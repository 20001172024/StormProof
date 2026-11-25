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
	"fmt"

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
	// logVersion goes up whenever the log changes. Every change (append or
	// truncation) reaches Ready.Entries, because it has to be persisted, so
	// counting non-empty Ready.Entries catches them all. Invariants use it to
	// skip re-checking logs that haven't changed.
	logVersion uint64
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
	return &Process{env: env, opts: o, node: node, wal: w}, nil
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
	b, ok := m.Body.(body)
	if !ok {
		return nil, fmt.Errorf("unexpected message %T", m.Body)
	}
	p.node.Step(b.Message)
	return p.handleReady()
}

// maybePropose is the stand-in workload: the leader sometimes proposes a
// command. Commands are unique ("n<node>.t<term>.<count>") so that if two
// nodes ever apply different commands at the same index, it shows.
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
	cmd := fmt.Sprintf("n%d.t%d.%d", st.ID, st.Term, p.proposed)
	if _, err := p.node.Propose([]byte(cmd)); err != nil {
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
	p.node.Advance()
	return out, nil
}
