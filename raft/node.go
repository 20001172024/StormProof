package raft

import (
	"errors"
	"slices"
)

// ErrNotLeader is returned by Propose on a node that is not the leader.
var ErrNotLeader = errors.New("raft: not the leader")

// Config configures a node.
type Config struct {
	ID    NodeID
	Peers []NodeID // every other node in the cluster

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

	// HardState and Entries are the state recovered from storage after a
	// restart; zero values for a brand new node. Entries must start at
	// index 1 and be contiguous.
	HardState HardState
	Entries   []Entry

	// Knobs deliberately break Raft rules, so tests can check that the
	// simulator catches the resulting bugs. Never set them outside tests.
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
}

func (c Config) validate() error {
	switch {
	case c.ID == None:
		return errors.New("raft: ID must not be 0")
	case slices.Contains(c.Peers, c.ID):
		return errors.New("raft: Peers must not include the node itself")
	case c.ElectionTicks <= 0 || c.HeartbeatTicks <= 0:
		return errors.New("raft: ElectionTicks and HeartbeatTicks must be positive")
	case c.HeartbeatTicks >= c.ElectionTicks:
		return errors.New("raft: HeartbeatTicks must be less than ElectionTicks")
	case c.MaxEntriesPerMsg < 0:
		return errors.New("raft: MaxEntriesPerMsg must not be negative")
	case c.Rand == nil:
		return errors.New("raft: Rand is required")
	}
	for i, e := range c.Entries {
		if e.Index != uint64(i+1) {
			return errors.New("raft: recovered entries must start at index 1 and be contiguous")
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
	id    NodeID
	peers []NodeID
	cfg   Config

	// Persistent state (Figure 2). HardState is saved via Ready.HardState,
	// the log via Ready.Entries.
	term Term   // currentTerm
	vote NodeID // votedFor
	log  raftLog

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

	// Timers, counted in ticks.
	electionElapsed  int
	electionTimeout  int // randomized, see resetElectionTimer
	heartbeatElapsed int

	// Output waiting for the caller, and what the last Ready handed out.
	msgs        []Message
	savedHard   HardState // last HardState the caller confirmed saving
	readyHard   HardState // HardState in the Ready awaiting Advance
	unstable    uint64    // first log index not yet handed out in Ready.Entries
	readyStable uint64    // value of unstable once the pending Ready is advanced
	readyApply  uint64    // value of applied once the pending Ready is advanced
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
		peers:     slices.Clone(cfg.Peers),
		cfg:       cfg,
		term:      cfg.HardState.Term,
		vote:      cfg.HardState.Vote,
		savedHard: cfg.HardState,
	}
	n.log.entries = slices.Clone(cfg.Entries)
	n.unstable = n.log.lastIndex() + 1 // recovered entries are already durable
	slices.Sort(n.peers)               // fixed order, so messages always go out in the same order
	n.role = Follower
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
	// election.
	n.electionElapsed++
	if n.electionElapsed >= n.electionTimeout {
		n.campaign()
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
	n.broadcastAppend()
	return e.Index, nil
}

// Step processes one incoming message.
func (n *Node) Step(m Message) {
	// Figure 2, All Servers: "If RPC request or response contains term
	// T > currentTerm: set currentTerm = T, convert to follower (§5.1)."
	// Doing this first, for every message type, means the handlers below
	// only ever see messages from our own term or older ones.
	if m.Term > n.term {
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
	case MsgApp:
		n.handleAppendEntries(m)
	case MsgAppResp:
		n.handleAppendResponse(m)
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
}

// Status returns a snapshot of the node's state.
func (n *Node) Status() Status {
	return Status{
		ID: n.id, Role: n.role, Term: n.term, Vote: n.vote, Leader: n.leader,
		LastIndex: n.log.lastIndex(), LastTerm: n.log.lastTerm(), Commit: n.commitIndex,
	}
}

// Entries returns the whole log. The slice is shared with the node, so the
// caller must not modify it, and must not keep it past the next call into
// the node. It is meant for invariant checks in tests.
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
	n.resetElectionTimer()
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leader = n.id
	n.votes = nil
	n.heartbeatElapsed = 0

	// Figure 2, volatile state on leaders: "nextIndex: initialized to leader
	// last log index + 1; matchIndex: initialized to 0".
	n.nextIndex = make(map[NodeID]uint64, len(n.peers))
	n.matchIndex = make(map[NodeID]uint64, len(n.peers))
	for _, p := range n.peers {
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

// quorum is the number of nodes that make a majority of the whole cluster,
// this node included.
func (n *Node) quorum() int { return (len(n.peers)+1)/2 + 1 }

// send queues a message from this node, stamped with the current term.
func (n *Node) send(m Message) {
	m.From = n.id
	m.Term = n.term
	n.msgs = append(n.msgs, m)
}
