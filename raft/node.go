package raft

import (
	"errors"
	"slices"
)

// Config configures a node.
type Config struct {
	ID    NodeID
	Peers []NodeID // every other node in the cluster

	// ElectionTicks is the base election timeout. Each time the timer is
	// reset a new timeout is drawn at random from
	// [ElectionTicks, 2*ElectionTicks). The randomness is what keeps
	// elections from repeatedly splitting the vote (§5.2).
	ElectionTicks int
	// HeartbeatTicks is how often a leader sends heartbeats. It must be
	// well below ElectionTicks, or followers time out between heartbeats
	// (§5.6: broadcastTime << electionTimeout << MTBF).
	HeartbeatTicks int

	Rand Rand

	// HardState is the state recovered from storage after a restart; the
	// zero value for a brand new node.
	HardState HardState
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
	case c.Rand == nil:
		return errors.New("raft: Rand is required")
	}
	return nil
}

// Node is one Raft server's state machine. It does no I/O and has no
// goroutines or clocks: it is driven entirely by Tick and Step, and hands
// its work back through Ready. All methods must be called from one
// goroutine.
type Node struct {
	id    NodeID
	peers []NodeID
	cfg   Config

	// Persistent state (Figure 2). Saved through Ready.HardState.
	term Term   // currentTerm
	vote NodeID // votedFor

	// Volatile state.
	role   Role
	leader NodeID

	// votes holds the nodes that granted this candidate a vote in the
	// current term, including itself. Only its size is used, so Go's
	// random map iteration order never matters.
	votes map[NodeID]bool

	// Timers, counted in ticks.
	electionElapsed  int
	electionTimeout  int // randomized, see resetElectionTimer
	heartbeatElapsed int

	// Output waiting for the caller.
	msgs      []Message
	savedHard HardState // the HardState last handed out by Ready
	readyHard HardState // the HardState in the Ready awaiting Advance
}

// NewNode creates a node as a follower in its recovered term.
func NewNode(cfg Config) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	n := &Node{
		id:        cfg.ID,
		peers:     slices.Clone(cfg.Peers),
		cfg:       cfg,
		term:      cfg.HardState.Term,
		vote:      cfg.HardState.Vote,
		savedHard: cfg.HardState,
	}
	slices.Sort(n.peers) // fixed order, so messages always go out in the same order
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
			n.broadcastHeartbeat()
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
		// Nothing to do until log replication (M3). A response with a higher
		// term has already been handled above.
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
	rd.Messages = n.msgs
	n.readyHard = hs
	return rd
}

// Advance tells the node that the last Ready has been fully handled.
func (n *Node) Advance() {
	n.savedHard = n.readyHard
	n.msgs = nil
}

// Status returns a snapshot of the node's state.
func (n *Node) Status() Status {
	return Status{ID: n.id, Role: n.role, Term: n.term, Vote: n.vote, Leader: n.leader}
}

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
	n.resetElectionTimer()
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leader = n.id
	n.votes = nil
	n.heartbeatElapsed = 0
	// Figure 2, Leaders: "Upon election: send initial empty AppendEntries
	// RPCs (heartbeat) to each server", so the other nodes stop their
	// elections and learn who the leader is straight away.
	n.broadcastHeartbeat()
}

// resetElectionTimer restarts the election countdown with a fresh random
// timeout in [ElectionTicks, 2*ElectionTicks).
func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	n.electionTimeout = n.cfg.ElectionTicks + n.cfg.Rand.Intn(n.cfg.ElectionTicks)
}

// quorum is the number of votes needed to win: a strict majority of the
// whole cluster, this node included.
func (n *Node) quorum() int { return (len(n.peers)+1)/2 + 1 }

// send queues a message from this node, stamped with the current term.
func (n *Node) send(m Message) {
	m.From = n.id
	m.Term = n.term
	n.msgs = append(n.msgs, m)
}
