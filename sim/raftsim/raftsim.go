// Package raftsim runs raft.Node inside the simulator.
//
// It is the simulator's version of the glue the real server will have
// (M7): it drives the node with Tick and Step, saves the node's HardState
// and log to its write-ahead log before sending any of its messages,
// applies committed entries, and recovers from the WAL on restart.
//
// Until the KV store and real clients arrive (M5), the leader also
// generates its own workload: on some ticks it proposes a unique command.
package raftsim

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/20001172024/stormproof/kv"
	"github.com/20001172024/stormproof/raft"
	"github.com/20001172024/stormproof/sim"
	"github.com/20001172024/stormproof/storage/wal"
)

// Options configures the simulated Raft nodes.
type Options struct {
	ElectionTicks  int
	HeartbeatTicks int

	// ProposeProb is the chance that a leader proposes a new command on a
	// tick. ProposeUntil stops proposals at that virtual time, so the
	// cluster can finish replicating before the run ends; 0 means proposals
	// never stop.
	ProposeProb  float64
	ProposeUntil sim.Time

	// SkipSync plants a durability bug for testing the simulator: the WAL
	// runs with wal.SyncNever, so a machine crash can make a node forget
	// its term, its vote and its log.
	SkipSync bool

	// SegmentSize is the WAL segment size; 0 means 1 KiB. That is tiny on
	// purpose: a 15 s simulated run writes only a few KiB, and at 16 KiB no
	// segment ever rotated, leaving rotation untested.
	SegmentSize int64

	// Knobs are passed to raft to switch off individual safety rules.
	Knobs raft.TestingKnobs

	// Planted bugs in the KV layer, for checking that the linearizability
	// checker notices. LocalReads answers a Get straight from the local
	// state on any node that believes it is the leader, skipping ReadIndex.
	// NoDedup switches off duplicate detection for retried writes.
	LocalReads bool
	NoDedup    bool
}

// DefaultOptions: with a 10 ms tick, heartbeats every 10 ms, election
// timeouts drawn from 100-200 ms, and a leader proposing on about one tick
// in ten (about 10 commands per second).
var DefaultOptions = Options{ElectionTicks: 10, HeartbeatTicks: 1, ProposeProb: 0.1}

// New returns a factory for simulated Raft nodes.
func New(o Options) sim.ProcessFactory {
	return func(env sim.Env) (sim.Process, error) { return open(env, o) }
}

// Process is one simulated Raft server.
type Process struct {
	env  sim.Env
	opts Options
	node *raft.Node

	wal *wal.WAL

	applied  []raft.Entry // every entry applied since this process started
	proposed int

	// The KV service.
	store         *kv.Store
	pendingWrites map[uint64]pendingWrite // by log index
	pendingReads  map[uint64]pendingGet   // by ReadIndex id, awaiting confirmation
	confirmed     []confirmedGet          // confirmed, waiting for the store to catch up
	nextReadID    uint64
	appliedIndex  uint64
	out           []sim.Message // client responses waiting for handleReady
	// logVersion goes up whenever the log changes. Every change (append or
	// truncation) reaches Ready.Entries, because it has to be persisted, so
	// counting non-empty Ready.Entries catches them all. Invariants use it to
	// skip re-checking logs that haven't changed.
	logVersion uint64
}

type pendingWrite struct {
	client    sim.NodeID
	requestID uint64
	term      raft.Term // the term it was proposed in
}

type pendingGet struct {
	client sim.NodeID
	cmd    kv.Command
}

type confirmedGet struct {
	pendingGet
	index uint64
}

// body wraps a raft.Message so the simulator can carry it. Embedding the
// struct means its fields appear directly in the trace JSON.
type body struct {
	raft.Message
}

func (b body) Kind() string { return b.Type.String() }

func open(env sim.Env, o Options) (*Process, error) {
	wo := wal.Options{SegmentSize: o.SegmentSize, Sync: wal.SyncAlways}
	if wo.SegmentSize == 0 {
		wo.SegmentSize = 1 << 10
	}
	if o.SkipSync {
		wo.Sync = wal.SyncNever
	}
	w, st, err := wal.Open(env.Disk, wo)
	if err != nil {
		return nil, err
	}
	peers := make([]raft.NodeID, len(env.Peers))
	for i, p := range env.Peers {
		peers[i] = raft.NodeID(p)
	}
	node, err := raft.NewNode(raft.Config{
		ID:             raft.NodeID(env.ID),
		Peers:          peers,
		ElectionTicks:  o.ElectionTicks,
		HeartbeatTicks: o.HeartbeatTicks,
		Rand:           env.Rand,
		HardState:      st.HardState,
		Entries:        st.Entries,
		Knobs:          o.Knobs,
	})
	if err != nil {
		return nil, err
	}
	store := kv.NewStore()
	if o.NoDedup {
		store = kv.NewStoreWithoutDedup()
	}
	return &Process{
		env: env, opts: o, node: node, wal: w, store: store,
		pendingWrites: map[uint64]pendingWrite{}, pendingReads: map[uint64]pendingGet{},
	}, nil
}

// Status returns the node's state, for invariants.
func (p *Process) Status() raft.Status { return p.node.Status() }

// Entries returns the node's log. Read-only; see raft.Node.Entries.
func (p *Process) Entries() []raft.Entry { return p.node.Entries() }

// LogVersion changes whenever the node's log changes.
func (p *Process) LogVersion() uint64 { return p.logVersion }

// Applied returns the entries applied since this process started, in order.
func (p *Process) Applied() []raft.Entry { return p.applied }

// TraceState implements sim.Stater.
func (p *Process) TraceState() any { return p.node.Status() }

// Tick implements sim.Process.
func (p *Process) Tick() ([]sim.Message, error) {
	p.node.Tick()
	p.maybePropose()
	return p.handleReady()
}

// Step implements sim.Process.
func (p *Process) Step(m sim.Message) ([]sim.Message, error) {
	switch b := m.Body.(type) {
	case body:
		p.node.Step(b.Message)
	case clientRequest:
		p.handleClient(m.From, b.Cmd)
	default:
		return nil, fmt.Errorf("unexpected message %T", m.Body)
	}
	return p.handleReady()
}

func (p *Process) reply(to sim.NodeID, requestID uint64, status string, r kv.Result) {
	resp := clientResponse{RequestID: requestID, Status: status, Result: r}
	if status == statusNotLeader {
		resp.Leader = sim.NodeID(p.node.Status().Leader)
	}
	p.out = append(p.out, sim.Message{To: to, Body: resp})
}

// handleClient starts serving one client request. Only the leader serves
// requests; others send the client to the leader they know of.
func (p *Process) handleClient(from sim.NodeID, cmd kv.Command) {
	if p.node.Status().Role != raft.Leader {
		p.reply(from, cmd.RequestID, statusNotLeader, kv.Result{})
		return
	}
	if cmd.Op == kv.OpGet {
		if p.opts.LocalReads {
			p.reply(from, cmd.RequestID, statusOK, p.store.Get(cmd.Key)) // the planted stale-read bug
			return
		}
		p.nextReadID++
		switch err := p.node.ReadIndex(p.nextReadID); {
		case errors.Is(err, raft.ErrReadNotReady):
			p.reply(from, cmd.RequestID, statusRetry, kv.Result{})
		case err != nil:
			p.reply(from, cmd.RequestID, statusNotLeader, kv.Result{})
		default:
			p.pendingReads[p.nextReadID] = pendingGet{client: from, cmd: cmd}
		}
		return
	}
	idx, err := p.node.Propose(cmd.Encode())
	if err != nil {
		p.reply(from, cmd.RequestID, statusNotLeader, kv.Result{})
		return
	}
	p.pendingWrites[idx] = pendingWrite{client: from, requestID: cmd.RequestID, term: p.node.Status().Term}
}

// maybePropose is the internal workload for runs without clients: the leader
// sometimes proposes a Put to a unique key ("n<node>.t<term>.<count>"), so
// that if two nodes ever apply different commands at the same index, it
// shows.
func (p *Process) maybePropose() {
	// The cut-off is virtual time, not a count of this process's ticks: a
	// node that restarted would start counting from zero and keep proposing
	// long after the others stopped. (The first version did exactly that.)
	if p.opts.ProposeUntil > 0 && p.env.Now() >= p.opts.ProposeUntil {
		return
	}
	if !p.env.Rand.Chance(p.opts.ProposeProb) {
		return
	}
	st := p.node.Status()
	if st.Role != raft.Leader {
		return
	}
	p.proposed++
	cmd := kv.Command{Op: kv.OpPut, Key: fmt.Sprintf("n%d.t%d.%d", st.ID, st.Term, p.proposed), Value: []byte("x")}
	if _, err := p.node.Propose(cmd.Encode()); err != nil {
		panic(err) // can't happen: we just checked we are the leader
	}
}

// handleReady does what raft.Ready asks, in the required order: persist
// first, then release messages, then apply committed entries.
func (p *Process) handleReady() ([]sim.Message, error) {
	rd := p.node.Ready()
	if rd.HardState != nil || len(rd.Entries) > 0 {
		if err := p.wal.Save(rd.HardState, rd.Entries); err != nil {
			// Send nothing: the messages may depend on state that isn't
			// durable. The simulator crashes the node.
			return nil, err
		}
	}
	out := make([]sim.Message, len(rd.Messages))
	for i, m := range rd.Messages {
		out[i] = sim.Message{To: sim.NodeID(m.To), Body: body{m}}
	}
	if len(rd.Entries) > 0 {
		p.logVersion++
	}
	p.applied = append(p.applied, rd.CommittedEntries...)
	for _, e := range rd.CommittedEntries {
		p.apply(e)
	}
	for _, rs := range rd.ReadStates {
		if g, ok := p.pendingReads[rs.ID]; ok {
			delete(p.pendingReads, rs.ID)
			p.confirmed = append(p.confirmed, confirmedGet{pendingGet: g, index: rs.Index})
		}
	}
	p.node.Advance()
	p.serveConfirmedReads()
	if p.node.Status().Role != raft.Leader {
		p.failPending()
	}
	out = append(out, p.out...)
	p.out = nil
	return out, nil
}

// apply applies one committed entry to the store and answers the client
// that proposed it, if this node is the one that did.
func (p *Process) apply(e raft.Entry) {
	p.appliedIndex = e.Index
	if e.Data == nil {
		return // a leader's no-op
	}
	cmd, err := kv.Decode(e.Data)
	if err != nil {
		// Only this code proposes entries, so this is a bug; the simulator
		// records the panic as a violation.
		panic(fmt.Sprintf("entry %d does not decode: %v", e.Index, err))
	}
	result := p.store.Apply(cmd)
	w, ok := p.pendingWrites[e.Index]
	if !ok {
		return
	}
	delete(p.pendingWrites, e.Index)
	if w.term != e.Term {
		// A different entry committed at this index: ours was overwritten
		// after we lost leadership. It may still have been committed
		// elsewhere in the log, so the outcome is unknown; the client
		// retries with the same request ID and dedup sorts it out.
		p.reply(w.client, w.requestID, statusRetry, kv.Result{})
		return
	}
	p.reply(w.client, w.requestID, statusOK, result)
}

// serveConfirmedReads answers reads whose ReadIndex has been confirmed, once
// the store has applied everything up to the read index.
func (p *Process) serveConfirmedReads() {
	n := 0
	for _, g := range p.confirmed {
		if g.index <= p.appliedIndex {
			p.reply(g.client, g.cmd.RequestID, statusOK, p.store.Get(g.cmd.Key))
		} else {
			p.confirmed[n] = g
			n++
		}
	}
	p.confirmed = p.confirmed[:n]
}

// failPending tells clients to retry requests this node can no longer
// answer because it isn't the leader any more. Confirmed reads stay: their
// leadership check already passed. Maps are walked in sorted key order so
// responses go out in the same order on every run.
func (p *Process) failPending() {
	for _, idx := range slices.Sorted(maps.Keys(p.pendingWrites)) {
		w := p.pendingWrites[idx]
		p.reply(w.client, w.requestID, statusRetry, kv.Result{})
		delete(p.pendingWrites, idx)
	}
	for _, id := range slices.Sorted(maps.Keys(p.pendingReads)) {
		g := p.pendingReads[id]
		p.reply(g.client, g.cmd.RequestID, statusNotLeader, kv.Result{})
		delete(p.pendingReads, id)
	}
}
