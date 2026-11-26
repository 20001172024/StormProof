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
	"hash/fnv"
	"maps"
	"slices"

	"github.com/20001172024/stormproof/kv"
	"github.com/20001172024/stormproof/raft"
	"github.com/20001172024/stormproof/sim"
	"github.com/20001172024/stormproof/storage/snap"
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
var DefaultOptions = Options{ElectionTicks: 10, HeartbeatTicks: 1, ProposeProb: 0.1, SnapshotEvery: 50}

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
	appliedTerm   raft.Term
	snapshots     []raft.Snapshot // every snapshot taken or installed since this process started
	// states records a hash of the whole store after each applied index
	// (and after loading a snapshot), so invariants can check that every
	// node's state machine is identical at the same index.
	states []IndexedState
	out    []sim.Message // client responses waiting for handleReady
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
	p := &Process{
		env: env, opts: o, wal: w, store: kv.NewStore(),
		pendingWrites: map[uint64]pendingWrite{}, pendingReads: map[uint64]pendingGet{},
	}
	// The WAL says which snapshot the log starts after; load it into the
	// store. A missing or corrupt snapshot file is a recovery failure.
	var snapshot *raft.Snapshot
	if m := st.Snapshot; m.Index > 0 {
		s, err := snap.Load(env.Disk, m.Index)
		if err != nil {
			return nil, err
		}
		if s.Term != m.Term {
			return nil, fmt.Errorf("snapshot %d has term %d, the WAL says %d", m.Index, s.Term, m.Term)
		}
		if p.store, err = kv.Restore(s.Data); err != nil {
			return nil, err
		}
		p.appliedIndex, p.appliedTerm = s.Index, s.Term
		snapshot = &s
		p.recordState()
	}
	p.store.SetTestingKnobs(kv.TestingKnobs{NoDedup: o.NoDedup, SnapshotDropsSessions: o.SnapshotDropsSessions})
	p.node, err = raft.NewNode(raft.Config{
		ID:             raft.NodeID(env.ID),
		Peers:          peers,
		Joining:        joining,
		ElectionTicks:  o.ElectionTicks,
		HeartbeatTicks: o.HeartbeatTicks,
		Rand:           env.Rand,
		HardState:      st.HardState,
		Snapshot:       snapshot,
		Entries:        st.Entries,
		Knobs:          o.Knobs,
	})
	if err != nil {
		return nil, err
	}
	return p, nil
}

// Status returns the node's state, for invariants.
func (p *Process) Status() raft.Status { return p.node.Status() }

// Entries returns the node's log. Read-only; see raft.Node.Entries.
func (p *Process) Entries() []raft.Entry { return p.node.Entries() }

// LogVersion changes whenever the node's log changes.
func (p *Process) LogVersion() uint64 { return p.logVersion }

// Applied returns the entries applied since this process started, in order.
// Entries covered by a snapshot the process loaded are not in it.
func (p *Process) Applied() []raft.Entry { return p.applied }

// IndexedState is a hash of the store's full state after applying Index.
type IndexedState struct {
	Index uint64
	Hash  uint64
}

// States returns the store's state hash after every index this process
// applied or loaded.
func (p *Process) States() []IndexedState { return p.states }

func (p *Process) recordState() {
	h := fnv.New64a()
	_, _ = h.Write(p.store.Snapshot()) // hash.Hash.Write never fails
	p.states = append(p.states, IndexedState{Index: p.appliedIndex, Hash: h.Sum64()})
}

// AppliedIndex is the index of the last entry reflected in the store.
func (p *Process) AppliedIndex() uint64 { return p.appliedIndex }

// Snapshots returns every snapshot this process took or installed.
func (p *Process) Snapshots() []raft.Snapshot { return p.snapshots }

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
		p.handleClient(m.From, b.Cmd, b.Conf)
	default:
		return nil, fmt.Errorf("unexpected message %T", m.Body)
	}
	return p.handleReady()
}

// handleConfChange starts a membership change for the admin.
func (p *Process) handleConfChange(from sim.NodeID, requestID uint64, cc raft.ConfChange) {
	idx, err := p.node.ProposeConfChange(cc)
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		p.reply(from, requestID, statusNotLeader, kv.Result{})
	case errors.Is(err, raft.ErrConfChangeInProgress), errors.Is(err, raft.ErrConfChangeNotReady):
		p.reply(from, requestID, statusRetry, kv.Result{})
	case err != nil:
		// Nothing to do (already a voter, not a voter, last voter): answer
		// with the configuration as it is.
		p.out = append(p.out, sim.Message{To: from, Body: clientResponse{
			RequestID: requestID, Status: statusOK, Voters: p.node.Status().Voters}})
	default:
		p.pendingWrites[idx] = pendingWrite{client: from, requestID: requestID, term: p.node.Status().Term}
	}
}

// answerConfChange answers the admin once its configuration entry commits.
func (p *Process) answerConfChange(e raft.Entry) {
	w, ok := p.pendingWrites[e.Index]
	if !ok {
		return
	}
	delete(p.pendingWrites, e.Index)
	if w.term != e.Term {
		p.reply(w.client, w.requestID, statusRetry, kv.Result{})
		return
	}
	voters, err := raft.DecodeVoters(e.Data)
	if err != nil {
		panic(fmt.Sprintf("configuration entry %d does not decode: %v", e.Index, err))
	}
	p.out = append(p.out, sim.Message{To: w.client, Body: clientResponse{
		RequestID: w.requestID, Status: statusOK, Voters: voters}})
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
func (p *Process) handleClient(from sim.NodeID, cmd kv.Command, conf *raft.ConfChange) {
	if p.node.Status().Role != raft.Leader {
		p.reply(from, cmd.RequestID, statusNotLeader, kv.Result{})
		return
	}
	if conf != nil {
		p.handleConfChange(from, cmd.RequestID, *conf)
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
	if rd.Snapshot != nil {
		// A snapshot from the leader. Persisting it also persists the
		// current HardState and whatever log remains after it.
		if err := p.installSnapshot(*rd.Snapshot); err != nil {
			return nil, err
		}
	} else if rd.HardState != nil || len(rd.Entries) > 0 {
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
	if err := p.maybeSnapshot(); err != nil {
		return nil, err
	}
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
	p.appliedIndex, p.appliedTerm = e.Index, e.Term
	if e.Type == raft.EntryConfChange {
		p.recordState()
		p.answerConfChange(e)
		return
	}
	if e.Data == nil {
		p.recordState()
		return // a leader's no-op
	}
	cmd, err := kv.Decode(e.Data)
	if err != nil {
		// Only this code proposes entries, so this is a bug; the simulator
		// records the panic as a violation.
		panic(fmt.Sprintf("entry %d does not decode: %v", e.Index, err))
	}
	result := p.store.Apply(cmd)
	p.recordState()
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

func (p *Process) hardState() raft.HardState {
	st := p.node.Status()
	return raft.HardState{Term: st.Term, Vote: st.Vote}
}

// persistSnapshot makes a snapshot durable and compacts the WAL to start
// after it, keeping the given entries. The snapshot file is written first:
// until the WAL's compaction record is in place, recovery keeps using the
// old WAL and simply ignores the new file.
func (p *Process) persistSnapshot(s raft.Snapshot, after []raft.Entry) error {
	sync := !p.opts.SkipSync
	if err := snap.Save(p.env.Disk, s, sync); err != nil {
		return err
	}
	if err := p.wal.Compact(wal.SnapshotMeta{Index: s.Index, Term: s.Term}, p.hardState(), after); err != nil {
		return err
	}
	p.snapshots = append(p.snapshots, s)
	p.logVersion++
	return snap.Prune(p.env.Disk, s.Index)
}

// installSnapshot persists a snapshot received from the leader and loads it
// into the store.
func (p *Process) installSnapshot(s raft.Snapshot) error {
	if err := p.persistSnapshot(s, p.node.Entries()); err != nil {
		return err
	}
	store, err := kv.Restore(s.Data)
	if err != nil {
		// The leader made this snapshot with the same code, so this is a bug.
		panic(fmt.Sprintf("snapshot %d from the leader does not decode: %v", s.Index, err))
	}
	store.SetTestingKnobs(kv.TestingKnobs{NoDedup: p.opts.NoDedup, SnapshotDropsSessions: p.opts.SnapshotDropsSessions})
	p.store = store
	p.appliedIndex, p.appliedTerm = s.Index, s.Term
	p.recordState()
	return nil
}

// maybeSnapshot takes a snapshot once SnapshotEvery entries have been
// applied since the last one: the node discards the entries it covers and
// returns the snapshot (with the configuration at that index), which is
// then saved and the WAL compacted.
//
// Compacting the node first is safe: it only changes memory. If saving
// then fails, the node crashes and recovers from the WAL as it was.
func (p *Process) maybeSnapshot() error {
	every := p.opts.SnapshotEvery
	if every == 0 || p.appliedIndex < p.node.Status().SnapIndex+every {
		return nil
	}
	s, err := p.node.Compact(p.appliedIndex, p.store.Snapshot())
	if err != nil {
		panic(fmt.Sprintf("compacting to %d: %v", p.appliedIndex, err)) // a bug in this code
	}
	return p.persistSnapshot(s, p.node.Entries())
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
