// Package host runs one StormProof server: a raft.Node with its write-ahead
// log, snapshots and key-value store, answering client requests.
//
// It is the glue between the pure Raft state machine and the outside world,
// and the same code is used in both modes:
//
//   - the simulator (sim/raftsim) drives it with simulated time, network and
//     disk, on one goroutine, under fault injection;
//   - the real server (package server) drives it from one goroutine fed by a
//     ticker, gRPC and real files.
//
// So what the simulator tests is exactly what runs for real. Host itself is
// deterministic: it does no I/O except through its storage.FS, starts no
// goroutines and reads no clock.
//
// Every call (Tick, Step, Handle, Propose) does its work and then handles the
// node's Ready in the order Raft requires: persist (WAL, snapshot files),
// then release messages, then apply committed entries and answer clients.
// An error means storage failed and the server must stop: it can no longer
// promise that what it acknowledged is durable.
package host

import (
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"slices"

	"github.com/20001172024/stormproof/kv"
	"github.com/20001172024/stormproof/raft"
	"github.com/20001172024/stormproof/storage"
	"github.com/20001172024/stormproof/storage/snap"
	"github.com/20001172024/stormproof/storage/wal"
)

// Config configures a Host.
type Config struct {
	ID raft.NodeID
	// Peers is the other voters of the initial configuration; Joining means
	// this server starts with none and waits to be added (see raft.Config).
	Peers   []raft.NodeID
	Joining bool

	ElectionTicks  int
	HeartbeatTicks int
	// PreVote turns on Raft's pre-vote round (see raft.Config.PreVote).
	PreVote bool

	FS   storage.FS
	Rand raft.Rand

	// SegmentSize is the WAL segment size (0: the WAL's default).
	SegmentSize int64
	// UnsafeNoSync turns off every fsync. A machine crash can then lose
	// acknowledged state. Only for measuring fsync cost, and as a planted
	// bug in the simulator.
	UnsafeNoSync bool
	// SnapshotEvery takes a snapshot once this many entries have been
	// applied since the last one; 0 never snapshots.
	SnapshotEvery uint64
	// KeepHistory keeps every applied entry, every snapshot, and a hash of
	// the whole key-value store after every applied entry (see Applied,
	// Snapshots and States), for the simulator's state machine safety
	// check. The lists only grow and each hash costs time proportional to
	// the store's size, so only the simulator turns it on.
	KeepHistory bool

	// Planted bugs for testing the checkers; never set them otherwise.
	Knobs      raft.TestingKnobs
	KVKnobs    kv.TestingKnobs
	LocalReads bool // answer Get from local state on any self-believed leader, skipping ReadIndex
}

// Status says how a request ended.
type Status int

const (
	StatusOK        Status = iota + 1
	StatusNotLeader        // try Response.Leader, if set, or another server
	StatusRetry            // outcome unknown or not ready yet: send the same request again
)

// Request is one client request. Client is an opaque token the caller uses
// to route the Response back; Host never interprets it.
type Request struct {
	Client uint64
	Cmd    kv.Command       // the operation, and the request's identity (ClientID, RequestID)
	Conf   *raft.ConfChange // a membership change instead of a KV operation
}

// Response answers a Request.
type Response struct {
	Client    uint64
	RequestID uint64
	Status    Status
	Leader    raft.NodeID   // with StatusNotLeader: the leader this server knows of, if any
	Result    kv.Result     // with StatusOK, for a KV operation
	Voters    []raft.NodeID // with StatusOK, for a membership change: the configuration
}

// Output is what a call produced: Raft messages to send to other servers,
// and responses to clients. Both may be sent as soon as the call returns:
// everything they depend on is already durable.
type Output struct {
	Messages  []raft.Message
	Responses []Response
}

// Host is one server. It is not safe for concurrent use.
type Host struct {
	cfg  Config
	node *raft.Node
	wal  *wal.WAL

	store         *kv.Store
	pendingWrites map[uint64]pendingWrite // by log index
	pendingReads  map[uint64]pendingGet   // by ReadIndex id, awaiting confirmation
	confirmed     []confirmedGet          // confirmed, waiting for the store to catch up
	nextReadID    uint64
	appliedIndex  uint64
	appliedTerm   raft.Term
	out           []Response

	// History, kept for the simulator's invariants.
	applied    []raft.Entry    // every entry applied since this Host started
	snapshots  []raft.Snapshot // every snapshot taken or installed
	states     []IndexedState  // the store's state hash after each applied index
	logVersion uint64          // goes up whenever the log changes
}

type pendingWrite struct {
	client    uint64
	requestID uint64
	term      raft.Term // the term it was proposed in
}

type pendingGet struct {
	client uint64
	cmd    kv.Command
}

type confirmedGet struct {
	pendingGet
	index uint64
}

// IndexedState is a hash of the store's full state after applying Index.
type IndexedState struct {
	Index uint64
	Hash  uint64
}

// Open recovers a server from its storage: the WAL says which snapshot the
// log starts after; that snapshot is loaded into the store, and the node is
// built from the snapshot, the HardState and the entries after it.
func Open(cfg Config) (*Host, error) {
	wo := wal.Options{SegmentSize: cfg.SegmentSize, Sync: wal.SyncAlways}
	if cfg.UnsafeNoSync {
		wo.Sync = wal.SyncNever
	}
	w, st, err := wal.Open(cfg.FS, wo)
	if err != nil {
		return nil, err
	}
	h := &Host{
		cfg: cfg, wal: w, store: kv.NewStore(),
		pendingWrites: map[uint64]pendingWrite{}, pendingReads: map[uint64]pendingGet{},
	}
	var snapshot *raft.Snapshot
	if m := st.Snapshot; m.Index > 0 {
		s, err := snap.Load(cfg.FS, m.Index)
		if err != nil {
			return nil, err
		}
		if s.Term != m.Term {
			return nil, fmt.Errorf("snapshot %d has term %d, the WAL says %d", m.Index, s.Term, m.Term)
		}
		if h.store, err = kv.Restore(s.Data); err != nil {
			return nil, err
		}
		h.appliedIndex, h.appliedTerm = s.Index, s.Term
		snapshot = &s
		h.recordState()
	}
	h.store.SetTestingKnobs(cfg.KVKnobs)
	h.node, err = raft.NewNode(raft.Config{
		ID:             cfg.ID,
		Peers:          cfg.Peers,
		Joining:        cfg.Joining,
		ElectionTicks:  cfg.ElectionTicks,
		HeartbeatTicks: cfg.HeartbeatTicks,
		PreVote:        cfg.PreVote,
		Rand:           cfg.Rand,
		HardState:      st.HardState,
		Snapshot:       snapshot,
		Entries:        st.Entries,
		Knobs:          cfg.Knobs,
	})
	if err != nil {
		return nil, err
	}
	return h, nil
}

// Close closes the WAL.
func (h *Host) Close() error { return h.wal.Close() }

// Tick advances the node's clock by one tick.
func (h *Host) Tick() (Output, error) {
	h.node.Tick()
	return h.handleReady()
}

// Step delivers a message from another server.
func (h *Host) Step(m raft.Message) (Output, error) {
	h.node.Step(m)
	return h.handleReady()
}

// Handle starts serving a client request. The response comes in this
// Output if it can be answered straight away (not the leader, a no-op
// change), otherwise in the Output of a later call, once it commits or its
// read is confirmed.
func (h *Host) Handle(req Request) (Output, error) {
	h.handleClient(req)
	return h.handleReady()
}

// Batch is several events handed to the node together. Processing them
// with one Ready means one write to the WAL, one fsync, and one send of the
// resulting messages, instead of one of each per event: under load that is
// the difference between an fsync per request and an fsync per batch.
type Batch struct {
	Ticks    int
	Messages []raft.Message
	Requests []Request
}

// Process handles a batch: its ticks, then its messages, then its requests,
// then one Ready. The events arrived concurrently, so any order is one they
// could have arrived in. Raft allows any number of steps between Readys;
// what matters is that their state is persisted before their messages go
// out, which handleReady does as for a single event.
func (h *Host) Process(b Batch) (Output, error) {
	for range b.Ticks {
		h.node.Tick()
	}
	for _, m := range b.Messages {
		h.node.Step(m)
	}
	for _, r := range b.Requests {
		h.handleClient(r)
	}
	return h.handleReady()
}

// Propose proposes a KV command that no client is waiting for (the
// simulator's internal workload). It returns raft.ErrNotLeader if this
// server is not the leader.
func (h *Host) Propose(cmd kv.Command) (Output, error) {
	if _, err := h.node.Propose(cmd.Encode()); err != nil {
		return Output{}, err
	}
	return h.handleReady()
}

// Introspection, for invariants, traces and metrics.

// Status returns the node's state.
func (h *Host) Status() raft.Status { return h.node.Status() }

// Entries returns the node's log after its snapshot. Read-only; see
// raft.Node.Entries.
func (h *Host) Entries() []raft.Entry { return h.node.Entries() }

// LogVersion changes whenever the node's log changes. Every change reaches
// Ready.Entries (it has to be persisted) or comes with a snapshot, so
// counting those catches them all.
func (h *Host) LogVersion() uint64 { return h.logVersion }

// Applied returns the entries applied since this Host started, in order.
// Entries covered by a snapshot it loaded are not in it.
// Only with Config.KeepHistory.
func (h *Host) Applied() []raft.Entry { return h.applied }

// AppliedIndex is the index of the last entry reflected in the store.
func (h *Host) AppliedIndex() uint64 { return h.appliedIndex }

// Snapshots returns every snapshot this Host took or installed. Only with
// Config.KeepHistory.
func (h *Host) Snapshots() []raft.Snapshot { return h.snapshots }

// States returns the store's state hash after every index this Host applied
// or loaded. Only with Config.KeepHistory.
func (h *Host) States() []IndexedState { return h.states }

func (h *Host) recordState() {
	if !h.cfg.KeepHistory {
		return
	}
	f := fnv.New64a()
	_, _ = f.Write(h.store.Snapshot()) // hash.Hash.Write never fails
	h.states = append(h.states, IndexedState{Index: h.appliedIndex, Hash: f.Sum64()})
}

func (h *Host) reply(client, requestID uint64, status Status, r kv.Result) {
	resp := Response{Client: client, RequestID: requestID, Status: status, Result: r}
	if status == StatusNotLeader {
		resp.Leader = h.node.Status().Leader
	}
	h.out = append(h.out, resp)
}

// handleClient starts serving one client request. Only the leader serves
// requests; others send the client to the leader they know of.
func (h *Host) handleClient(req Request) {
	cmd := req.Cmd
	if h.node.Status().Role != raft.Leader {
		h.reply(req.Client, cmd.RequestID, StatusNotLeader, kv.Result{})
		return
	}
	if req.Conf != nil {
		h.handleConfChange(req.Client, cmd.RequestID, *req.Conf)
		return
	}
	if cmd.Op == kv.OpGet {
		if h.cfg.LocalReads {
			h.reply(req.Client, cmd.RequestID, StatusOK, h.store.Get(cmd.Key)) // the planted stale-read bug
			return
		}
		h.nextReadID++
		switch err := h.node.ReadIndex(h.nextReadID); {
		case errors.Is(err, raft.ErrReadNotReady):
			h.reply(req.Client, cmd.RequestID, StatusRetry, kv.Result{})
		case err != nil:
			h.reply(req.Client, cmd.RequestID, StatusNotLeader, kv.Result{})
		default:
			h.pendingReads[h.nextReadID] = pendingGet{client: req.Client, cmd: cmd}
		}
		return
	}
	idx, err := h.node.Propose(cmd.Encode())
	if err != nil {
		h.reply(req.Client, cmd.RequestID, StatusNotLeader, kv.Result{})
		return
	}
	h.pendingWrites[idx] = pendingWrite{client: req.Client, requestID: cmd.RequestID, term: h.node.Status().Term}
}

// handleConfChange starts a membership change.
func (h *Host) handleConfChange(client, requestID uint64, cc raft.ConfChange) {
	idx, err := h.node.ProposeConfChange(cc)
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		h.reply(client, requestID, StatusNotLeader, kv.Result{})
	case errors.Is(err, raft.ErrConfChangeInProgress), errors.Is(err, raft.ErrConfChangeNotReady):
		h.reply(client, requestID, StatusRetry, kv.Result{})
	case err != nil:
		// Nothing to do (already a voter, not a voter, last voter): answer
		// with the configuration as it is.
		h.out = append(h.out, Response{Client: client, RequestID: requestID, Status: StatusOK, Voters: h.node.Status().Voters})
	default:
		h.pendingWrites[idx] = pendingWrite{client: client, requestID: requestID, term: h.node.Status().Term}
	}
}

// handleReady does what raft.Ready asks, in the required order: persist
// first, then release messages, then apply committed entries.
func (h *Host) handleReady() (Output, error) {
	rd := h.node.Ready()
	if rd.Snapshot != nil {
		// A snapshot from the leader. Persisting it also persists the
		// current HardState and whatever log remains after it.
		if err := h.installSnapshot(*rd.Snapshot); err != nil {
			return Output{}, err
		}
	} else if rd.HardState != nil || len(rd.Entries) > 0 {
		if err := h.wal.Save(rd.HardState, rd.Entries); err != nil {
			// Send nothing: the messages may depend on state that isn't
			// durable.
			return Output{}, err
		}
	}
	if len(rd.Entries) > 0 {
		h.logVersion++
	}
	if h.cfg.KeepHistory {
		h.applied = append(h.applied, rd.CommittedEntries...)
	}
	for _, e := range rd.CommittedEntries {
		h.apply(e)
	}
	for _, rs := range rd.ReadStates {
		if g, ok := h.pendingReads[rs.ID]; ok {
			delete(h.pendingReads, rs.ID)
			h.confirmed = append(h.confirmed, confirmedGet{pendingGet: g, index: rs.Index})
		}
	}
	h.node.Advance()
	if err := h.maybeSnapshot(); err != nil {
		return Output{}, err
	}
	h.serveConfirmedReads()
	if h.node.Status().Role != raft.Leader {
		h.failPending()
	}
	out := Output{Messages: rd.Messages, Responses: h.out}
	h.out = nil
	return out, nil
}

// apply applies one committed entry to the store and answers the client
// that proposed it, if this server is the one that did.
func (h *Host) apply(e raft.Entry) {
	h.appliedIndex, h.appliedTerm = e.Index, e.Term
	if e.Type == raft.EntryConfChange {
		h.recordState()
		h.answerConfChange(e)
		return
	}
	if e.Data == nil {
		h.recordState()
		return // a leader's no-op
	}
	cmd, err := kv.Decode(e.Data)
	if err != nil {
		// Only this code proposes entries, so this is a bug. (The simulator
		// records the panic as a violation.)
		panic(fmt.Sprintf("entry %d does not decode: %v", e.Index, err))
	}
	result := h.store.Apply(cmd)
	h.recordState()
	w, ok := h.pendingWrites[e.Index]
	if !ok {
		return
	}
	delete(h.pendingWrites, e.Index)
	if w.term != e.Term {
		// A different entry committed at this index: ours was overwritten
		// after we lost leadership. It may still have been committed
		// elsewhere in the log, so the outcome is unknown; the client
		// retries with the same request ID and dedup sorts it out.
		h.reply(w.client, w.requestID, StatusRetry, kv.Result{})
		return
	}
	h.reply(w.client, w.requestID, StatusOK, result)
}

// answerConfChange answers whoever asked for a membership change once its
// configuration entry commits.
func (h *Host) answerConfChange(e raft.Entry) {
	w, ok := h.pendingWrites[e.Index]
	if !ok {
		return
	}
	delete(h.pendingWrites, e.Index)
	if w.term != e.Term {
		h.reply(w.client, w.requestID, StatusRetry, kv.Result{})
		return
	}
	voters, err := raft.DecodeVoters(e.Data)
	if err != nil {
		panic(fmt.Sprintf("configuration entry %d does not decode: %v", e.Index, err))
	}
	h.out = append(h.out, Response{Client: w.client, RequestID: w.requestID, Status: StatusOK, Voters: voters})
}

func (h *Host) hardState() raft.HardState {
	st := h.node.Status()
	return raft.HardState{Term: st.Term, Vote: st.Vote}
}

// persistSnapshot makes a snapshot durable and compacts the WAL to start
// after it, keeping the given entries. The snapshot file is written first:
// until the WAL's compaction record is in place, recovery keeps using the
// old WAL and simply ignores the new file.
func (h *Host) persistSnapshot(s raft.Snapshot, after []raft.Entry) error {
	if err := snap.Save(h.cfg.FS, s, !h.cfg.UnsafeNoSync); err != nil {
		return err
	}
	if err := h.wal.Compact(wal.SnapshotMeta{Index: s.Index, Term: s.Term}, h.hardState(), after); err != nil {
		return err
	}
	if h.cfg.KeepHistory {
		h.snapshots = append(h.snapshots, s)
	}
	h.logVersion++
	return snap.Prune(h.cfg.FS, s.Index)
}

// installSnapshot persists a snapshot received from the leader and loads it
// into the store.
func (h *Host) installSnapshot(s raft.Snapshot) error {
	if err := h.persistSnapshot(s, h.node.Entries()); err != nil {
		return err
	}
	store, err := kv.Restore(s.Data)
	if err != nil {
		// The leader made this snapshot with the same code, so this is a bug.
		panic(fmt.Sprintf("snapshot %d from the leader does not decode: %v", s.Index, err))
	}
	store.SetTestingKnobs(h.cfg.KVKnobs)
	h.store = store
	h.appliedIndex, h.appliedTerm = s.Index, s.Term
	h.recordState()
	return nil
}

// maybeSnapshot takes a snapshot once SnapshotEvery entries have been
// applied since the last one: the node discards the entries it covers and
// returns the snapshot (with the configuration at that index), which is
// then saved and the WAL compacted.
//
// Compacting the node first is safe: it only changes memory. If saving
// then fails, the server stops and recovers from the WAL as it was.
func (h *Host) maybeSnapshot() error {
	every := h.cfg.SnapshotEvery
	if every == 0 || h.appliedIndex < h.node.Status().SnapIndex+every {
		return nil
	}
	s, err := h.node.Compact(h.appliedIndex, h.store.Snapshot())
	if err != nil {
		panic(fmt.Sprintf("compacting to %d: %v", h.appliedIndex, err)) // a bug in this code
	}
	return h.persistSnapshot(s, h.node.Entries())
}

// serveConfirmedReads answers reads whose ReadIndex has been confirmed, once
// the store has applied everything up to the read index.
func (h *Host) serveConfirmedReads() {
	n := 0
	for _, g := range h.confirmed {
		if g.index <= h.appliedIndex {
			h.reply(g.client, g.cmd.RequestID, StatusOK, h.store.Get(g.cmd.Key))
		} else {
			h.confirmed[n] = g
			n++
		}
	}
	h.confirmed = h.confirmed[:n]
}

// failPending tells clients to retry requests this server can no longer
// answer because it isn't the leader any more. Confirmed reads stay: their
// leadership check already passed. Maps are walked in sorted key order so
// responses go out in the same order every time.
func (h *Host) failPending() {
	for _, idx := range slices.Sorted(maps.Keys(h.pendingWrites)) {
		w := h.pendingWrites[idx]
		h.reply(w.client, w.requestID, StatusRetry, kv.Result{})
		delete(h.pendingWrites, idx)
	}
	for _, id := range slices.Sorted(maps.Keys(h.pendingReads)) {
		g := h.pendingReads[id]
		h.reply(g.client, g.cmd.RequestID, StatusNotLeader, kv.Result{})
		delete(h.pendingReads, id)
	}
}
