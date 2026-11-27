// Package raftsim runs StormProof servers inside the simulator.
//
// A server is a host.Host, the same code the real server runs; this package
// only adapts it to the simulator: it turns simulated messages into Raft
// messages and client requests, and back. It also provides the simulated
// KV clients (client.go), the admin that changes membership (admin.go), the
// invariants checked after every event and the end-of-run checks
// (invariants.go), and, for runs without clients, an internal workload in
// which the leader proposes commands itself.
package raftsim

import (
	"errors"
	"fmt"

	"github.com/20001172024/stormproof/kv"
	"github.com/20001172024/stormproof/raft"
	"github.com/20001172024/stormproof/server/host"
	"github.com/20001172024/stormproof/sim"
)

// Options configures the simulated Raft nodes.
type Options struct {
	ElectionTicks  int
	HeartbeatTicks int
	// PreVote turns on Raft's pre-vote round; on by default.
	PreVote bool

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

	// SnapshotEvery takes a snapshot once this many entries have been
	// applied since the last one; 0 never snapshots. The default, 50, is
	// small on purpose, so that snapshots, compaction and InstallSnapshot
	// happen many times in every run.
	SnapshotEvery uint64
	// SnapshotDropsSessions plants a bug: snapshots leave out the client
	// sessions, so a node restored from one can apply a retried write twice.
	SnapshotDropsSessions bool

	// InitialVoters is how many servers (IDs 1..InitialVoters) form the
	// initial configuration; the rest start with none and wait to be added.
	// 0 means all servers.
	InitialVoters int
}

// DefaultOptions: with a 10 ms tick, heartbeats every 10 ms, election
// timeouts drawn from 100-200 ms, and a leader proposing on about one tick
// in ten (about 10 commands per second).
var DefaultOptions = Options{ElectionTicks: 10, HeartbeatTicks: 1, PreVote: true, ProposeProb: 0.1, SnapshotEvery: 50}

// New returns a factory for simulated Raft nodes.
func New(o Options) sim.ProcessFactory {
	return func(env sim.Env) (sim.Process, error) { return open(env, o) }
}

// Process is one simulated Raft server: a host.Host plus the internal
// workload.
type Process struct {
	env      sim.Env
	opts     Options
	host     *host.Host
	proposed int
}

// body wraps a raft.Message so the simulator can carry it. Embedding the
// struct means its fields appear directly in the trace JSON.
type body struct {
	raft.Message
}

func (b body) Kind() string { return b.Type.String() }

func open(env sim.Env, o Options) (*Process, error) {
	initial := o.InitialVoters
	if initial == 0 {
		initial = len(env.Peers) + 1
	}
	var peers []raft.NodeID
	joining := int(env.ID) > initial
	if !joining {
		for i := 1; i <= initial; i++ {
			if sim.NodeID(i) != env.ID {
				peers = append(peers, raft.NodeID(i))
			}
		}
	}
	segment := o.SegmentSize
	if segment == 0 {
		segment = 1 << 10
	}
	h, err := host.Open(host.Config{
		ID:             raft.NodeID(env.ID),
		Peers:          peers,
		Joining:        joining,
		ElectionTicks:  o.ElectionTicks,
		HeartbeatTicks: o.HeartbeatTicks,
		PreVote:        o.PreVote,
		FS:             env.Disk,
		Rand:           env.Rand,
		SegmentSize:    segment,
		UnsafeNoSync:   o.SkipSync,
		SnapshotEvery:  o.SnapshotEvery,
		KeepHistory:    true, // for the state machine safety invariant
		Knobs:          o.Knobs,
		KVKnobs:        kv.TestingKnobs{NoDedup: o.NoDedup, SnapshotDropsSessions: o.SnapshotDropsSessions},
		LocalReads:     o.LocalReads,
	})
	if err != nil {
		return nil, err
	}
	return &Process{env: env, opts: o, host: h}, nil
}

// Introspection, delegated to the host, for invariants.

// Status returns the node's state.
func (p *Process) Status() raft.Status { return p.host.Status() }

// Entries returns the node's log after its snapshot. Read-only.
func (p *Process) Entries() []raft.Entry { return p.host.Entries() }

// LogVersion changes whenever the node's log changes.
func (p *Process) LogVersion() uint64 { return p.host.LogVersion() }

// Applied returns the entries applied since this process started.
func (p *Process) Applied() []raft.Entry { return p.host.Applied() }

// AppliedIndex is the index of the last entry reflected in the store.
func (p *Process) AppliedIndex() uint64 { return p.host.AppliedIndex() }

// Snapshots returns every snapshot this process took or installed.
func (p *Process) Snapshots() []raft.Snapshot { return p.host.Snapshots() }

// States returns the store's state hash after every applied index.
func (p *Process) States() []host.IndexedState { return p.host.States() }

// TermRun is a run of consecutive log entries with the same term: entries
// From .. From+Count-1. A log is traced as its runs, which keeps traces small
// (a log of hundreds of entries is usually only a few terms) while still
// letting the visualizer draw every entry.
type TermRun struct {
	From  uint64    `json:"from"`
	Term  raft.Term `json:"term"`
	Count int       `json:"count"`
}

// traceState is what the trace records for a server: its status, the
// index applied to its store, and its log after the snapshot as term runs.
type traceState struct {
	raft.Status
	Applied uint64    `json:"applied"`
	Log     []TermRun `json:"log"`
}

// termRuns run-length encodes a log by term.
func termRuns(entries []raft.Entry) []TermRun {
	runs := []TermRun{}
	for _, e := range entries {
		if n := len(runs); n > 0 && runs[n-1].Term == e.Term {
			runs[n-1].Count++
			continue
		}
		runs = append(runs, TermRun{From: e.Index, Term: e.Term, Count: 1})
	}
	return runs
}

// TraceState implements sim.Stater.
func (p *Process) TraceState() any {
	return traceState{Status: p.host.Status(), Applied: p.host.AppliedIndex(), Log: termRuns(p.host.Entries())}
}

// Tick implements sim.Process.
func (p *Process) Tick() ([]sim.Message, error) {
	out, err := p.host.Tick()
	if err != nil {
		return nil, err
	}
	msgs := toSim(out)
	more, err := p.maybePropose()
	if err != nil {
		return nil, err
	}
	return append(msgs, more...), nil
}

// Step implements sim.Process.
func (p *Process) Step(m sim.Message) ([]sim.Message, error) {
	var out host.Output
	var err error
	switch b := m.Body.(type) {
	case body:
		out, err = p.host.Step(b.Message)
	case clientRequest:
		out, err = p.host.Handle(host.Request{Client: uint64(m.From), Cmd: b.Cmd, Conf: b.Conf})
	default:
		return nil, fmt.Errorf("unexpected message %T", m.Body)
	}
	if err != nil {
		return nil, err
	}
	return toSim(out), nil
}

var statusNames = map[host.Status]string{
	host.StatusOK: statusOK, host.StatusNotLeader: statusNotLeader, host.StatusRetry: statusRetry,
}

// toSim turns a host's output into simulated messages: Raft messages to
// other servers, then responses to clients.
func toSim(out host.Output) []sim.Message {
	msgs := make([]sim.Message, 0, len(out.Messages)+len(out.Responses))
	for _, m := range out.Messages {
		msgs = append(msgs, sim.Message{To: sim.NodeID(m.To), Body: body{m}})
	}
	for _, r := range out.Responses {
		msgs = append(msgs, sim.Message{To: sim.NodeID(r.Client), Body: clientResponse{
			RequestID: r.RequestID, Status: statusNames[r.Status], Leader: sim.NodeID(r.Leader),
			Result: r.Result, Voters: r.Voters,
		}})
	}
	return msgs
}

// maybePropose is the internal workload for runs without clients: the leader
// sometimes proposes a Put to a unique key ("n<node>.t<term>.<count>"), so
// that if two nodes ever apply different commands at the same index, it
// shows.
func (p *Process) maybePropose() ([]sim.Message, error) {
	// The cut-off is virtual time, not a count of this process's ticks: a
	// node that restarted would start counting from zero and keep proposing
	// long after the others stopped. (The first version did exactly that.)
	if p.opts.ProposeUntil > 0 && p.env.Now() >= p.opts.ProposeUntil {
		return nil, nil
	}
	if !p.env.Rand.Chance(p.opts.ProposeProb) {
		return nil, nil
	}
	st := p.host.Status()
	if st.Role != raft.Leader {
		return nil, nil
	}
	p.proposed++
	cmd := kv.Command{Op: kv.OpPut, Key: fmt.Sprintf("n%d.t%d.%d", st.ID, st.Term, p.proposed), Value: []byte("x")}
	out, err := p.host.Propose(cmd)
	if errors.Is(err, raft.ErrNotLeader) {
		panic(err) // can't happen: we just checked we are the leader
	}
	if err != nil {
		return nil, err
	}
	return toSim(out), nil
}
