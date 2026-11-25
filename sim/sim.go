package sim

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/20001172024/stormproof/storage"
)

// Process is one simulated server. The simulator calls it when its timer
// fires or a message arrives, and routes the messages it returns.
//
// A Process is the deterministic state machine plus the glue that persists
// its state to Env.Disk. It must do its work inside the call and not keep
// any goroutines or timers of its own. When a call returns an error the
// simulator treats it as the process failing and crashes the node; that is
// the right response to, for example, a failed fsync.
type Process interface {
	Tick() ([]Message, error)
	Step(m Message) ([]Message, error)
}

// Stater is implemented by processes that want their state in the trace.
// TraceState must return a struct (not a map), so it encodes the same way
// every time. A "state" record is written whenever it changes.
type Stater interface {
	TraceState() any
}

// Env is what a process gets when it starts. On a restart, Disk is the same
// disk as before the crash, holding whatever survived it.
type Env struct {
	ID    NodeID
	Peers []NodeID // every other node, sorted
	Disk  storage.FS
	Rand  *Rand // a fresh stream for each incarnation of the node
	// Now returns the current virtual time. It is for test workloads and
	// harness code; the system under test should be driven by Tick only.
	Now func() Time
}

// ProcessFactory starts a process, recovering any state from env.Disk.
// An error means recovery failed, which the simulator reports as a
// violation: the state the node left on disk can't be loaded.
type ProcessFactory func(env Env) (Process, error)

// Invariant is a property that must hold after every event.
type Invariant interface {
	Name() string
	// Check returns an error describing the problem if the property is
	// broken. It is called after every event, so it may keep history.
	Check(nodes []NodeView) error
}

// NodeView is what an invariant or final check can see of one node.
type NodeView struct {
	ID      NodeID
	Up      bool
	Process Process // nil while the node is down
	Client  bool    // only final checks see clients
}

// Config describes one simulation run.
type Config struct {
	Seed         uint64
	Nodes        int
	Duration     Time // stop when virtual time passes this
	TickInterval Time // base interval between a node's ticks
	Profile      Profile
	NewProcess   ProcessFactory
	// Clients is the number of client processes, created with NewClient.
	// They get IDs Nodes+1 .. Nodes+Clients, use the same network as the
	// servers (so their messages can be lost, delayed and duplicated), but
	// are never crashed or partitioned, and invariants don't see them. Their
	// Env.Peers lists the servers.
	Clients   int
	NewClient ProcessFactory

	// NewInvariants returns fresh invariants for this run. It is a function
	// because invariants may keep history, so each run needs new ones.
	NewInvariants func() []Invariant
	// Trace receives the JSON Lines trace. Nil means no trace.
	Trace io.Writer

	// Quiet stops injected faults for the final Quiet of the run, so the
	// cluster gets a chance to recover: no new crashes or partitions, and no
	// more fsync failures or slow disks. Partitions and downtime already
	// scheduled still run their course, and message loss and delay continue.
	Quiet Time
	// FinalCheck, if set, runs once when the run ends without a violation.
	// It is for liveness properties ("a leader was elected"), which can't be
	// checked after every event because they only have to hold eventually.
	// A non-nil error is reported as a violation named "final-check".
	FinalCheck func(nodes []NodeView) error
}

// Result is the outcome of a run.
type Result struct {
	Seed      uint64
	Steps     int  // events processed
	End       Time // virtual time when the run stopped
	Violation *Violation
	Stats     Stats
}

// Violation describes the first invariant that failed.
type Violation struct {
	Step      int
	T         Time
	Invariant string
	Detail    string
}

func (v *Violation) String() string {
	return fmt.Sprintf("step %d at %v: %s: %s", v.Step, v.T, v.Invariant, v.Detail)
}

// Stats counts what happened during a run.
type Stats struct {
	Sent, Delivered, Dropped int
	Crashes, Restarts        int
	Partitions               int
}

type node struct {
	id           NodeID
	client       bool // a client: never crashed or partitioned, not seen by invariants
	disk         *Disk
	tickInterval Time
	proc         Process // nil while down
	incarnation  int
	lastState    []byte
}

// simulation holds the state of one run. Everything happens on the calling
// goroutine.
type simulation struct {
	cfg        Config
	now        Time
	step       int
	q          queue
	ids        []NodeID
	nodes      []*node // nodes[id-1]
	net        *network
	faults     *Rand
	clock      *Rand
	partition  map[Link]bool // links currently blocked
	quiet      bool          // the quiet period has started
	invariants []Invariant
	trace      *traceWriter
	res        Result
	err        error // a harness error that stops the run
}

// Run runs one simulation. A failed invariant is reported in
// Result.Violation, not as an error; an error means the simulation itself
// could not run (bad config, unwritable trace).
func Run(cfg Config) (Result, error) {
	if err := validate(cfg); err != nil {
		return Result{}, err
	}
	s := &simulation{
		cfg:       cfg,
		net:       &network{cfg: cfg.Profile.Net, rng: NewRand(cfg.Seed, "network")},
		faults:    NewRand(cfg.Seed, "faults"),
		clock:     NewRand(cfg.Seed, "clock"),
		partition: map[Link]bool{},
		trace:     newTraceWriter(cfg.Trace),
		res:       Result{Seed: cfg.Seed},
	}
	if cfg.NewInvariants != nil {
		s.invariants = cfg.NewInvariants()
	}

	s.trace.write(Header{
		Type: "header", Version: TraceVersion, Seed: cfg.Seed, Nodes: cfg.Nodes, Clients: cfg.Clients,
		Duration: cfg.Duration, TickInterval: cfg.TickInterval, Profile: cfg.Profile,
	})

	skew := Time(float64(cfg.TickInterval) * cfg.Profile.ClockSkew)
	for i := 1; i <= cfg.Nodes; i++ {
		id := NodeID(i)
		s.ids = append(s.ids, id)
		s.nodes = append(s.nodes, &node{
			id:           id,
			disk:         NewDisk(cfg.Profile.Disk, NewRand(cfg.Seed, fmt.Sprintf("disk/%d", id))),
			tickInterval: cfg.TickInterval + s.clock.Between(-skew, skew+1),
		})
	}
	// Clients come after the servers, so server IDs are 1..Nodes.
	for i := 1; i <= cfg.Clients; i++ {
		id := NodeID(cfg.Nodes + i)
		s.nodes = append(s.nodes, &node{
			id:           id,
			client:       true,
			disk:         NewDisk(DiskConfig{}, NewRand(cfg.Seed, fmt.Sprintf("disk/%d", id))),
			tickInterval: cfg.TickInterval,
		})
	}
	for _, n := range s.nodes {
		s.start(n)
	}
	if cfg.Profile.Faults.Interval > 0 {
		s.q.push(&event{at: cfg.Profile.Faults.Interval, kind: evFaultCheck})
	}
	if cfg.Quiet > 0 {
		s.q.push(&event{at: cfg.Duration - cfg.Quiet, kind: evQuiet})
	}

	for s.q.len() > 0 && s.res.Violation == nil && s.err == nil {
		e := s.q.pop()
		if e.at > cfg.Duration {
			break
		}
		s.now = e.at
		s.step++
		s.handle(e)
		if s.res.Violation == nil && s.err == nil {
			s.checkInvariants()
		}
	}

	if s.res.Violation == nil && s.err == nil && cfg.FinalCheck != nil {
		if err := cfg.FinalCheck(s.views(true)); err != nil {
			s.violate("final-check", err.Error())
		}
	}

	s.res.Steps = s.step
	s.res.End = s.now
	if err := s.trace.close(); err != nil && s.err == nil {
		s.err = fmt.Errorf("writing trace: %w", err)
	}
	return s.res, s.err
}

func validate(cfg Config) error {
	switch {
	case cfg.Nodes < 2:
		return errors.New("sim: need at least 2 nodes")
	case cfg.TickInterval <= 0:
		return errors.New("sim: TickInterval must be positive")
	case cfg.Duration <= 0:
		return errors.New("sim: Duration must be positive")
	case cfg.NewProcess == nil:
		return errors.New("sim: NewProcess is required")
	case cfg.Clients < 0 || (cfg.Clients > 0 && cfg.NewClient == nil):
		return errors.New("sim: Clients needs NewClient")
	case cfg.Profile.ClockSkew < 0 || cfg.Profile.ClockSkew >= 1:
		return errors.New("sim: ClockSkew must be in [0, 1)")
	case cfg.Quiet < 0 || cfg.Quiet > cfg.Duration:
		return errors.New("sim: Quiet must be between 0 and Duration")
	}
	return nil
}

func (s *simulation) node(id NodeID) *node { return s.nodes[id-1] }

func (s *simulation) handle(e *event) {
	switch e.kind {
	case evTick:
		n := s.node(e.node)
		if n.proc == nil || n.incarnation != e.incarnation {
			return // the node crashed (and maybe restarted) since this was scheduled
		}
		msgs, err := s.call(n, n.proc.Tick)
		if s.res.Violation != nil {
			return // it panicked
		}
		s.afterCall(n, msgs, err)
		if n.proc != nil && n.incarnation == e.incarnation {
			s.q.push(&event{at: s.now + n.tickInterval, kind: evTick, node: n.id, incarnation: n.incarnation})
		}

	case evDeliver:
		s.deliver(e.msg)

	case evFaultCheck:
		s.rollFaults()
		s.q.push(&event{at: s.now + s.cfg.Profile.Faults.Interval, kind: evFaultCheck})

	case evRestart:
		s.start(s.node(e.node))

	case evHeal:
		s.partition = map[Link]bool{}
		s.trace.write(Record{Step: s.step, T: s.now, Type: RecHeal})

	case evQuiet:
		s.quiet = true
		for _, n := range s.nodes {
			n.disk.StopFaults()
		}
		s.trace.write(Record{Step: s.step, T: s.now, Type: RecQuiet})
	}
}

// start starts (or restarts) a node, recovering from its disk.
func (s *simulation) start(n *node) {
	n.incarnation++
	if n.incarnation > 1 {
		s.res.Stats.Restarts++
	}
	var peers []NodeID // a server's other servers, or a client's servers
	for _, id := range s.ids {
		if id != n.id {
			peers = append(peers, id)
		}
	}
	newProc := s.cfg.NewProcess
	if n.client {
		newProc = s.cfg.NewClient
	}
	proc, err := newProc(Env{
		ID:    n.id,
		Peers: peers,
		Disk:  n.disk,
		Rand:  NewRand(s.cfg.Seed, fmt.Sprintf("node/%d/%d", n.id, n.incarnation)),
		Now:   func() Time { return s.now },
	})
	if err != nil {
		s.violate("recovery", fmt.Sprintf("node %d failed to start: %v", n.id, err))
		return
	}
	n.proc = proc
	s.trace.write(Record{Step: s.step, T: s.now, Type: RecStart, Node: n.id})
	s.recordState(n)
	// Start each node's timer at a random phase so nodes don't tick in lockstep.
	first := s.now + s.clock.Between(0, n.tickInterval)
	s.q.push(&event{at: first, kind: evTick, node: n.id, incarnation: n.incarnation})
}

// crash stops a node. If machine is true it also loses unsynced disk data.
func (s *simulation) crash(n *node, machine bool, reason string) {
	n.proc = nil
	n.lastState = nil
	n.disk.Crash(machine)
	kind := "process"
	if machine {
		kind = "machine"
	}
	s.res.Stats.Crashes++
	s.trace.write(Record{Step: s.step, T: s.now, Type: RecCrash, Node: n.id, Kind: kind, Reason: reason})
	f := s.cfg.Profile.Faults
	s.q.push(&event{at: s.now + s.faults.Between(f.MinDowntime, f.MaxDowntime+1), kind: evRestart, node: n.id})
}

// call runs one call into a process. A panic in the system under test is a
// bug the simulator found, so it is recorded as a violation (named
// "panic") instead of killing the whole sweep, and the seed replays it like
// any other.
func (s *simulation) call(n *node, f func() ([]Message, error)) (msgs []Message, err error) {
	defer func() {
		if r := recover(); r != nil {
			s.violate("panic", fmt.Sprintf("node %d panicked: %v", n.id, r))
		}
	}()
	return f()
}

// afterCall handles the result of calling into a process.
func (s *simulation) afterCall(n *node, msgs []Message, err error) {
	delay := n.disk.takeDelay()
	if err != nil {
		// A process that hits an error (such as a failed fsync) stops.
		// After a failed fsync the OS may already have dropped the dirty
		// data, so this is treated as a machine crash.
		s.crash(n, true, err.Error())
		return
	}
	s.recordState(n)
	for _, m := range msgs {
		if m.To < 1 || int(m.To) > len(s.nodes) || m.To == n.id {
			s.err = fmt.Errorf("sim: node %d sent a message to invalid node %d", n.id, m.To)
			return
		}
		m.From = n.id
		s.send(m, s.now+delay)
	}
}

func (s *simulation) send(m Message, at Time) {
	s.res.Stats.Sent++
	s.trace.write(Record{Step: s.step, T: s.now, Type: RecSend, From: m.From, To: m.To, Kind: m.Body.Kind(), Msg: m.Body})
	delays := s.net.fate()
	if delays == nil {
		s.res.Stats.Dropped++
		s.trace.write(Record{Step: s.step, T: s.now, Type: RecDrop, From: m.From, To: m.To, Kind: m.Body.Kind(), Reason: "lost"})
		return
	}
	for _, d := range delays {
		s.q.push(&event{at: at + d, kind: evDeliver, msg: m})
	}
}

// deliver hands a message to its destination. Partitions are checked here,
// at delivery time, so messages already in flight when a partition starts
// are cut off too.
func (s *simulation) deliver(m Message) {
	n := s.node(m.To)
	reason := ""
	switch {
	case s.partition[Link{m.From, m.To}]:
		reason = "partition"
	case n.proc == nil:
		reason = "node down"
	}
	if reason != "" {
		s.res.Stats.Dropped++
		s.trace.write(Record{Step: s.step, T: s.now, Type: RecDrop, From: m.From, To: m.To, Kind: m.Body.Kind(), Reason: reason})
		return
	}
	s.res.Stats.Delivered++
	s.trace.write(Record{Step: s.step, T: s.now, Type: RecDeliver, From: m.From, To: m.To, Kind: m.Body.Kind()})
	msgs, err := s.call(n, func() ([]Message, error) { return n.proc.Step(m) })
	if s.res.Violation != nil {
		return // it panicked
	}
	s.afterCall(n, msgs, err)
}

// rollFaults is called every Faults.Interval to maybe crash a node and maybe
// start a partition.
func (s *simulation) rollFaults() {
	if s.quiet {
		return
	}
	f := s.cfg.Profile.Faults
	if s.faults.Chance(f.CrashProb) {
		var up []*node
		for _, n := range s.nodes {
			if n.proc != nil && !n.client {
				up = append(up, n)
			}
		}
		if len(up) > 0 {
			victim := up[s.faults.Intn(len(up))]
			s.crash(victim, s.faults.Chance(f.MachineCrashProb), "injected")
		}
	}
	if len(s.partition) == 0 && s.faults.Chance(f.PartitionProb) {
		kind, links := randomPartition(s.faults, s.ids)
		for _, l := range links {
			s.partition[l] = true
		}
		s.res.Stats.Partitions++
		s.trace.write(Record{Step: s.step, T: s.now, Type: RecPartition, Kind: kind, Links: links})
		s.q.push(&event{at: s.now + s.faults.Between(f.MinPartition, f.MaxPartition+1), kind: evHeal})
	}
}

// recordState writes a state record if the node's traced state changed.
func (s *simulation) recordState(n *node) {
	st, ok := n.proc.(Stater)
	if !ok {
		return
	}
	b, err := json.Marshal(st.TraceState())
	if err != nil {
		s.err = fmt.Errorf("sim: encoding state of node %d: %w", n.id, err)
		return
	}
	if bytes.Equal(b, n.lastState) {
		return
	}
	n.lastState = b
	s.trace.write(Record{Step: s.step, T: s.now, Type: RecState, Node: n.id, State: b})
}

func (s *simulation) checkInvariants() {
	if len(s.invariants) == 0 {
		return
	}
	views := s.views(false)
	for _, inv := range s.invariants {
		if err := inv.Check(views); err != nil {
			s.violate(inv.Name(), err.Error())
			return
		}
	}
}

// views describes the nodes for invariants (servers only) or for the final
// check (servers, then clients).
func (s *simulation) views(withClients bool) []NodeView {
	var views []NodeView
	for _, n := range s.nodes {
		if n.client && !withClients {
			continue
		}
		views = append(views, NodeView{ID: n.id, Up: n.proc != nil, Process: n.proc, Client: n.client})
	}
	return views
}

func (s *simulation) violate(name, detail string) {
	s.res.Violation = &Violation{Step: s.step, T: s.now, Invariant: name, Detail: detail}
	s.trace.write(Record{Step: s.step, T: s.now, Type: RecViolation, Kind: name, Reason: detail})
}
