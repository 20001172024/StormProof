package raft

import (
	"errors"
	"slices"
)

// ErrNotLeader is returned by Propose and ReadIndex on a node that is not
// the leader.
var ErrNotLeader = errors.New("raft: not the leader")

// ErrReadNotReady is returned by ReadIndex on a new leader that has not yet
// committed an entry in its own term. Until it has, its commit index may be
// behind entries that an earlier leader already committed, so it can't
// promise a read index. The no-op appended on election clears this within
// one round trip.
var ErrReadNotReady = errors.New("raft: leader has not committed an entry in its term yet")

// Config configures a node.
type Config struct {
	ID NodeID
	// Peers is the other voters in the initial configuration. A server
	// joining an existing cluster sets Joining instead: it starts with no
	// configuration and learns it from the leader.
	Peers   []NodeID
	Joining bool

	// ElectionTicks is the base election timeout. Each time the timer is
	// reset a new timeout is drawn at random from
	// [ElectionTicks, 2*ElectionTicks). The randomness is what keeps
	// elections from repeatedly splitting the vote (§5.2).
	ElectionTicks int
	// HeartbeatTicks is how often a leader sends AppendEntries even when it
	// has nothing new. It must be well below ElectionTicks, or followers time
	// out between heartbeats (§5.6: broadcastTime << electionTimeout << MTBF).
	HeartbeatTicks int
	// MaxEntriesPerMsg caps the entries in one AppendEntries, so a follower
	// that is far behind is caught up in pieces. 0 means 64.
	MaxEntriesPerMsg int

	Rand Rand

	// HardState, Snapshot and Entries are the state recovered from storage
	// after a restart; zero values for a brand new node. Entries must be
	// contiguous and start right after the snapshot (at index 1 without
	// one). The caller has already loaded the snapshot into its state
	// machine.
	HardState HardState
	Snapshot  *Snapshot
	Entries   []Entry

	// Knobs deliberately break Raft rules, so tests can check that the
	// simulator catches the resulting bugs. Never set them outside tests.
	// PreVote makes a server whose election timer fires first check, with
	// a pre-vote round that changes no terms, that a majority would vote
	// for it, and only then start a real election (dissertation §9.6). It
	// stops a server that can't win, such as one whose log is behind, from
	// deposing a working leader by raising terms over and over.
	PreVote bool

	Knobs TestingKnobs
}

// TestingKnobs each switch off one safety rule. They exist so the
// simulator can be shown to find the bug the rule prevents.
type TestingKnobs struct {
	// CommitPreviousTermsByCounting lets a leader commit an entry from an
	// earlier term once it is stored on a majority. Raft forbids this
	// (§5.4.2, Figure 8): such an entry can still be overwritten.
	CommitPreviousTermsByCounting bool
	// NoNoopOnElection stops a new leader from appending an empty entry in
	// its term. The no-op is what normally lets entries from earlier terms
	// commit promptly; without it they wait for the next client command.
	NoNoopOnElection bool
	// ConfChangeBeforeOwnTermCommit lets a leader start a membership change
	// before it has committed an entry in its own term: the 2015 bug in
	// single-server changes, which can elect two leaders.
	ConfChangeBeforeOwnTermCommit bool
	// ConcurrentConfChanges lets a leader start a membership change while
	// an earlier one is still uncommitted.
	ConcurrentConfChanges bool
	// NoVoteStickiness makes servers grant votes even when they have heard
	// from a current leader recently, so removed servers can disrupt the
	// cluster (§4.2.3).
	NoVoteStickiness bool
	// ReadIndexWithoutQuorum confirms ReadIndex requests straight away
	// instead of waiting for a majority to acknowledge this node as leader.
	// A leader that has been replaced without knowing it then serves stale
	// reads.
	ReadIndexWithoutQuorum bool
}

func (c Config) validate() error {
	switch {
	case c.ID == None:
		return errors.New("raft: ID must not be 0")
	case slices.Contains(c.Peers, c.ID):
		return errors.New("raft: Peers must not include the node itself")
	case c.Joining && len(c.Peers) > 0:
		return errors.New("raft: a joining server has no initial Peers")
	case c.Snapshot != nil && len(c.Snapshot.Voters) == 0 && !c.Joining:
		return errors.New("raft: a snapshot must carry its configuration")
	case c.ElectionTicks <= 0 || c.HeartbeatTicks <= 0:
		return errors.New("raft: ElectionTicks and HeartbeatTicks must be positive")
	case c.HeartbeatTicks >= c.ElectionTicks:
		return errors.New("raft: HeartbeatTicks must be less than ElectionTicks")
	case c.MaxEntriesPerMsg < 0:
		return errors.New("raft: MaxEntriesPerMsg must not be negative")
	case c.Rand == nil:
		return errors.New("raft: Rand is required")
	}
	var first uint64 = 1
	if c.Snapshot != nil {
		first = c.Snapshot.Index + 1
	}
	for i, e := range c.Entries {
		if e.Index != first+uint64(i) {
			return errors.New("raft: recovered entries must be contiguous and start right after the snapshot")
		}
		if i > 0 && e.Term < c.Entries[i-1].Term {
			return errors.New("raft: recovered entries have decreasing terms")
		}
	}
	return nil
}

// Node is one Raft server's state machine. It does no I/O and has no
// goroutines or clocks: it is driven entirely by Tick, Step and Propose,
// and hands its work back through Ready. All methods must be called from
// one goroutine.
type Node struct {
	id  NodeID
	cfg Config

	// Configuration (membership.go). voters is the current configuration,
	// sorted; confIndex is the index of the entry that set it, or 0 if it
	// comes from baseVoters: the configuration in the latest snapshot, or
	// the initial one.
	voters     []NodeID
	baseVoters []NodeID
	confIndex  uint64

	// Persistent state (Figure 2). HardState is saved via Ready.HardState,
	// the log via Ready.Entries.
	term Term   // currentTerm
	vote NodeID // votedFor
	log  raftLog

	// snapshot is the latest snapshot, kept to send to followers that need
	// entries this node has compacted away. Nil until there is one.
	snapshot *Snapshot

	// Volatile state on all servers (Figure 2).
	commitIndex uint64
	applied     uint64 // lastApplied: highest entry handed out in CommittedEntries

	role   Role
	leader NodeID

	// votes holds the nodes that granted this candidate a vote in the
	// current term, including itself. Only its size is used, so Go's
	// random map iteration order never matters.
	votes map[NodeID]bool

	// Volatile state on leaders (Figure 2), reinitialized after election.
	// The maps are only read by key or collected and sorted, never
	// iterated in an order that matters.
	nextIndex  map[NodeID]uint64 // next entry to send to each follower
	matchIndex map[NodeID]uint64 // highest entry known to be replicated on each follower

	// ReadIndex (dissertation §6.4). readSeq numbers each read; heartbeats
	// carry the latest number as their Context. pendingReads wait for a
	// majority to acknowledge a heartbeat sent after they began; readStates
	// are confirmed and wait for the caller.
	readSeq      uint64
	pendingReads []pendingRead
	readStates   []ReadState

	// Timers, counted in ticks.
	electionElapsed  int
	electionTimeout  int // randomized, see resetElectionTimer
	heartbeatElapsed int

	// Output waiting for the caller, and what the last Ready handed out.
	msgs        []Message
	newSnapshot *Snapshot // installed from the leader, not yet handed out
	savedHard   HardState // last HardState the caller confirmed saving
	readyHard   HardState // HardState in the Ready awaiting Advance
	unstable    uint64    // first log index not yet handed out in Ready.Entries
	readyStable uint64    // value of unstable once the pending Ready is advanced
	readyApply  uint64    // value of applied once the pending Ready is advanced
}

type pendingRead struct {
	ReadState
	seq  uint64
	acks map[NodeID]bool // only its size is used
}

// NewNode creates a node as a follower, from its recovered state.
func NewNode(cfg Config) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.MaxEntriesPerMsg == 0 {
		cfg.MaxEntriesPerMsg = 64
	}
	n := &Node{
		id:        cfg.ID,
		cfg:       cfg,
		term:      cfg.HardState.Term,
		vote:      cfg.HardState.Vote,
		savedHard: cfg.HardState,
	}
	if !cfg.Joining {
		n.baseVoters = append(slices.Clone(cfg.Peers), cfg.ID)
		slices.Sort(n.baseVoters) // fixed order, so messages always go out in the same order
	}
	if s := cfg.Snapshot; s != nil {
		n.log.restore(s.Index, s.Term)
		n.snapshot = s
		n.baseVoters = slices.Clone(s.Voters)
		// A snapshot only ever holds committed entries, and the caller has
		// loaded it into its state machine.
		n.commitIndex, n.applied = s.Index, s.Index
	}
	n.log.entries = slices.Clone(cfg.Entries)
	n.unstable = n.log.lastIndex() + 1 // recovered entries are already durable
	n.role = Follower
	n.refreshConfig()
	n.resetElectionTimer()
	return n, nil
}

// Tick advances the node's logical clock by one tick.
func (n *Node) Tick() {
	if n.role == Leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatTicks {
			n.heartbeatElapsed = 0
			n.broadcastAppend()
		}
		return
	}
	// Figure 2, Followers and Candidates: if the election timeout elapses
	// without hearing from a current leader or granting a vote, start an
	// election. Only voters do: a server outside the configuration (new and
	// not yet added, or removed) can't win and would only disrupt.
	n.electionElapsed++
	if n.electionElapsed >= n.electionTimeout {
		if n.isVoter(n.id) && n.cfg.PreVote {
			n.preCampaign()
		} else if n.isVoter(n.id) {
			n.campaign()
		} else {
			n.resetElectionTimer()
		}
	}
}

// Propose asks the leader to append data to the log. It returns the index
// the entry will have if it commits. Being appended is no guarantee: the
// entry commits only if it reaches a majority before this node loses
// leadership, and the caller learns that from Ready.CommittedEntries.
func (n *Node) Propose(data []byte) (uint64, error) {
	if n.role != Leader {
		return 0, ErrNotLeader
	}
	e := Entry{Index: n.log.lastIndex() + 1, Term: n.term, Data: data}
	n.log.append(e)
	n.maybeCommit() // a one-node cluster commits straight away
	if n.role == Leader {
		n.broadcastAppend()
	}
	return e.Index, nil
}

// ReadIndex starts a linearizable read (dissertation §6.4). The read is
// confirmed in a later Ready.ReadStates, with the caller's id, once a
// majority has acknowledged this node as leader after the call. The caller
// then waits until it has applied up to ReadState.Index and serves the read
// from its state machine.
//
// Why each step is needed:
//   - Recording the commit index now: every write that completed before
//     the read started is committed, so is at or below this index.
//   - Confirming leadership afterwards: a leader cut off by a partition
//     may have been replaced, and the new leader may have committed writes
//     this node has never heard of. A majority still accepting it as
//     leader rules that out.
//   - Waiting to apply up to the index: the local state machine must
//     include every write the read has to see.
func (n *Node) ReadIndex(id uint64) error {
	if n.role != Leader {
		return ErrNotLeader
	}
	if t, _ := n.log.term(n.commitIndex); t != n.term {
		return ErrReadNotReady
	}
	rs := ReadState{ID: id, Index: n.commitIndex}
	if (n.quorum() == 1 && n.isVoter(n.id)) || n.cfg.Knobs.ReadIndexWithoutQuorum {
		n.readStates = append(n.readStates, rs)
		return nil
	}
	n.readSeq++
	n.pendingReads = append(n.pendingReads, pendingRead{
		ReadState: rs, seq: n.readSeq, acks: map[NodeID]bool{n.id: true},
	})
	n.broadcastAppend() // ask for acknowledgements now, rather than at the next heartbeat
	return nil
}

// ackRead records that a follower acknowledged this node as leader in a
// response to a heartbeat with Context ctx. That confirms every read that
// began before that heartbeat was sent (seq <= ctx). Reads are confirmed in
// order: an earlier read always has at least the acknowledgements a later
// one has.
func (n *Node) ackRead(from NodeID, ctx uint64) {
	for _, p := range n.pendingReads {
		if p.seq <= ctx {
			p.acks[from] = true
		}
	}
	for len(n.pendingReads) > 0 && n.countVoters(n.pendingReads[0].acks) >= n.quorum() {
		n.readStates = append(n.readStates, n.pendingReads[0].ReadState)
		n.pendingReads = n.pendingReads[1:]
	}
}

// Step processes one incoming message.
func (n *Node) Step(m Message) {
	// §4.2.3: a server that has heard from a current leader within the
	// minimum election timeout ignores RequestVote, without even adopting
	// its term. Otherwise a removed server, which never hears it was removed
	// and keeps timing out, would depose a perfectly healthy leader. (A
	// leader counts itself as a current leader.)
	if m.Type == MsgVote && m.Term > n.term && n.leader != None &&
		n.electionElapsed < n.cfg.ElectionTicks && !n.cfg.Knobs.NoVoteStickiness {
		return
	}

	// Figure 2, All Servers: "If RPC request or response contains term
	// T > currentTerm: set currentTerm = T, convert to follower (§5.1)."
	// Doing this first, for every message type, means the handlers below
	// only ever see messages from our own term or older ones.
	//
	// Pre-votes are the exception: a PreVote's term, and a granted
	// response's, is the term the candidate *would* use, not one anyone is
	// in, so it must change nothing (§9.6). A rejection carries the
	// responder's real term and goes through the rule as usual.
	preVoteTerm := m.Type == MsgPreVote || (m.Type == MsgPreVoteResp && m.VoteGranted)
	if m.Term > n.term && !preVoteTerm {
		leader := None
		if m.Type == MsgApp {
			leader = m.From // only a leader sends AppendEntries
		}
		n.becomeFollower(m.Term, leader)
	}

	switch m.Type {
	case MsgVote:
		n.handleRequestVote(m)
	case MsgVoteResp:
		n.handleVoteResponse(m)
	case MsgPreVote:
		n.handlePreVote(m)
	case MsgPreVoteResp:
		n.handlePreVoteResponse(m)
	case MsgApp:
		n.handleAppendEntries(m)
	case MsgAppResp:
		n.handleAppendResponse(m)
	case MsgSnap:
		n.handleSnapshot(m)
	}
}

// Ready returns the work accumulated since the last Advance. See Ready for
// what the caller must do with it.
func (n *Node) Ready() Ready {
	var rd Ready
	hs := HardState{Term: n.term, Vote: n.vote}
	if hs != n.savedHard {
		rd.HardState = &hs
	}
	rd.Entries = n.log.slice(n.unstable, n.log.lastIndex()+1)
	rd.Messages = n.msgs
	rd.CommittedEntries = n.log.slice(n.applied+1, n.commitIndex+1)
	rd.ReadStates = n.readStates
	rd.Snapshot = n.newSnapshot

	n.readyHard = hs
	n.readyStable = n.log.lastIndex() + 1
	n.readyApply = n.commitIndex
	return rd
}

// Advance tells the node that the last Ready has been fully handled.
func (n *Node) Advance() {
	n.savedHard = n.readyHard
	n.unstable = n.readyStable
	n.applied = n.readyApply
	n.msgs = nil
	n.readStates = nil
	n.newSnapshot = nil
}

// Status returns a snapshot of the node's state.
func (n *Node) Status() Status {
	return Status{
		ID: n.id, Role: n.role, Term: n.term, Vote: n.vote, Leader: n.leader,
		LastIndex: n.log.lastIndex(), LastTerm: n.log.lastTerm(), Commit: n.commitIndex,
		SnapIndex: n.log.snapIndex, SnapTerm: n.log.snapTerm,
		Voters: slices.Clone(n.voters),
	}
}

// ErrCompacted is returned by Compact for an index already compacted.
var ErrCompacted = errors.New("raft: index already compacted")

// ErrUnapplied is returned by Compact for an index not yet applied.
var ErrUnapplied = errors.New("raft: can't compact entries that haven't been applied")

// Compact records that the caller has durably saved a snapshot of its state
// machine as of entry index, and discards the log up to and including it
// (§7). data is the snapshot itself, kept to send to followers that are too
// far behind to catch up from the log.
//
// index must already have been applied: a snapshot is the result of
// applying entries, so it can't cover entries the state machine hasn't
// seen. The caller's storage must also stop needing those entries, which
// is its business, not the node's.
func (n *Node) Compact(index uint64, data []byte) (Snapshot, error) {
	if index <= n.log.snapIndex {
		return Snapshot{}, ErrCompacted
	}
	if index > n.applied {
		return Snapshot{}, ErrUnapplied
	}
	t, _ := n.log.term(index)
	voters := slices.Clone(n.configAt(index))
	n.log.compact(index)
	n.baseVoters = voters // the configuration entries up to index are gone
	n.refreshConfig()
	n.snapshot = &Snapshot{Index: index, Term: t, Voters: voters, Data: data}
	n.unstable = max(n.unstable, n.log.firstIndex())
	return *n.snapshot, nil
}

// Entries returns the log after the latest snapshot (see Status.SnapIndex
// for where it starts). The slice is shared with the node, so the caller
// must not modify it, and must not keep it past the next call into the
// node.
func (n *Node) Entries() []Entry { return n.log.entries }

// becomeFollower switches to follower in the given term. Moving to a new
// term clears the vote: votedFor applies to one term only.
func (n *Node) becomeFollower(term Term, leader NodeID) {
	if term > n.term {
		n.term = term
		n.vote = None
	}
	n.role = Follower
	n.leader = leader
	n.votes = nil
	n.nextIndex, n.matchIndex = nil, nil
	// Reads still waiting for confirmation can't be confirmed any more: the
	// caller sees this node is no longer leader and has them retried.
	// Already confirmed ones (readStates) stay valid.
	n.pendingReads = nil
	n.resetElectionTimer()
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leader = n.id
	n.votes = nil
	n.heartbeatElapsed = 0

	// Figure 2, volatile state on leaders: "nextIndex: initialized to leader
	// last log index + 1; matchIndex: initialized to 0".
	n.nextIndex = map[NodeID]uint64{}
	n.matchIndex = map[NodeID]uint64{}
	for _, p := range n.others() {
		n.nextIndex[p] = n.log.lastIndex() + 1
		n.matchIndex[p] = 0
	}

	// Append a no-op in the new term (dissertation §6.4). The leader can
	// only commit entries of its own term by counting replicas, so until
	// one of those commits, entries left over from earlier terms can't
	// commit either. The no-op gets that done straight away.
	if !n.cfg.Knobs.NoNoopOnElection {
		n.log.append(Entry{Index: n.log.lastIndex() + 1, Term: n.term})
	}
	n.maybeCommit() // a one-node cluster commits straight away
	if n.role != Leader {
		return
	}

	// Figure 2, Leaders: "Upon election: send initial empty AppendEntries
	// RPCs (heartbeat) to each server". Ours carry the no-op.
	n.broadcastAppend()
}

// resetElectionTimer restarts the election countdown with a fresh random
// timeout in [ElectionTicks, 2*ElectionTicks).
func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	n.electionTimeout = n.cfg.ElectionTicks + n.cfg.Rand.Intn(n.cfg.ElectionTicks)
}

// send queues a message from this node, stamped with the current term.
func (n *Node) send(m Message) {
	m.Term = n.term
	n.sendAs(m)
}

// sendAs queues a message with the term already set in m: only pre-votes,
// whose term is the one the candidate would campaign in.
func (n *Node) sendAs(m Message) {
	m.From = n.id
	n.msgs = append(n.msgs, m)
}
