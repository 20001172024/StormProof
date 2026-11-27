package raft

// NodeID identifies a server in the cluster. 0 means "nobody" (see None).
type NodeID uint64

// None is the NodeID used for "no node": no vote cast, no known leader.
const None NodeID = 0

// Term is a Raft term number (§5.1). Terms act as a logical clock: each
// election starts a new, higher term, and every message carries the
// sender's term so out-of-date nodes find out they are behind.
type Term uint64

// Role is the role a node currently plays (§5.1, Figure 4).
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
	// PreCandidate is a follower whose election timer fired and that is
	// asking, without changing its term, whether it could win an election
	// (pre-vote, Raft dissertation §9.6). Only with Config.PreVote.
	PreCandidate
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	case PreCandidate:
		return "pre-candidate"
	}
	return "unknown"
}

// MarshalText makes roles appear as words ("leader") in JSON traces.
func (r Role) MarshalText() ([]byte, error) { return []byte(r.String()), nil }

// MessageType is the kind of a Message. Each RPC in the paper is a pair of
// message types: a request and its response.
type MessageType int

const (
	MsgVote        MessageType = iota + 1 // RequestVote RPC (§5.2)
	MsgVoteResp                           // reply to RequestVote
	MsgApp                                // AppendEntries RPC (§5.3); in M2 only empty heartbeats
	MsgAppResp                            // reply to AppendEntries
	MsgSnap                               // InstallSnapshot RPC (§7); answered with MsgAppResp
	MsgPreVote                            // pre-vote request (dissertation §9.6), for term+1
	MsgPreVoteResp                        // reply to a pre-vote
)

func (t MessageType) String() string {
	switch t {
	case MsgVote:
		return "RequestVote"
	case MsgVoteResp:
		return "RequestVoteResponse"
	case MsgApp:
		return "AppendEntries"
	case MsgAppResp:
		return "AppendEntriesResponse"
	case MsgSnap:
		return "InstallSnapshot"
	case MsgPreVote:
		return "PreVote"
	case MsgPreVoteResp:
		return "PreVoteResponse"
	}
	return "unknown"
}

// MarshalText makes message types appear by name in JSON traces.
func (t MessageType) MarshalText() ([]byte, error) { return []byte(t.String()), nil }

// Message is every message Raft nodes exchange. Rather than one Go type per
// RPC, there is one struct and each Type uses a documented subset of the
// fields (the same approach as etcd's raft). That keeps Step a single entry
// point and the transport a single message type.
//
// The fields map to the RPC arguments and results in Figure 2:
//
//	RequestVote            Term, From (candidateId), LastLogIndex, LastLogTerm
//	RequestVoteResponse    Term, VoteGranted
//	AppendEntries          Term, From (leaderId), PrevLogIndex, PrevLogTerm,
//	                       Entries, LeaderCommit
//	AppendEntriesResponse  Term, Success, and either MatchIndex (on success)
//	                       or ConflictTerm and ConflictIndex (on failure)
//	PreVote                as RequestVote, but Term is the term the sender
//	                       would campaign in (its own plus one)
//	PreVoteResponse        VoteGranted; Term is the request's Term if
//	                       granted, else the responder's own term
type Message struct {
	Type MessageType `json:"type"`
	From NodeID      `json:"from"`
	To   NodeID      `json:"to"`
	Term Term        `json:"term"`

	// RequestVote and PreVote
	LastLogIndex uint64 `json:"last_log_index,omitempty"`
	LastLogTerm  Term   `json:"last_log_term,omitempty"`
	VoteGranted  bool   `json:"vote_granted,omitempty"`

	// AppendEntries
	PrevLogIndex uint64  `json:"prev_log_index,omitempty"`
	PrevLogTerm  Term    `json:"prev_log_term,omitempty"`
	Entries      []Entry `json:"entries,omitempty"`
	LeaderCommit uint64  `json:"leader_commit,omitempty"`

	// AppendEntriesResponse
	Success bool `json:"success,omitempty"`
	// MatchIndex is the index of the last entry the follower now knows
	// matches the leader's log: PrevLogIndex + len(Entries) of the request.
	// Replying with it, rather than the leader working it out, keeps
	// responses correct even when requests are duplicated or reordered.
	MatchIndex uint64 `json:"match_index,omitempty"`
	// ConflictTerm and ConflictIndex are the fast-backtracking hint on
	// failure (§5.3): the term of the follower's entry at PrevLogIndex and
	// the first index it has for that term, or ConflictTerm 0 and
	// ConflictIndex = its last index + 1 if its log is too short.
	ConflictTerm  Term   `json:"conflict_term,omitempty"`
	ConflictIndex uint64 `json:"conflict_index,omitempty"`

	// InstallSnapshot
	Snapshot *Snapshot `json:"snapshot,omitempty"`

	// Context is carried by AppendEntries and echoed back in its response.
	// It is the leader's latest ReadIndex sequence number, so a response
	// proves the follower still accepted this node as leader after that
	// read began (dissertation §6.4).
	Context uint64 `json:"context,omitempty"`
}

// HardState is the part of Figure 2's "persistent state on all servers"
// other than the log. It must be on stable storage before the node sends
// any message that depends on it: a node that votes, replies, crashes and
// forgets its vote could vote again in the same term and help elect a
// second leader.
type HardState struct {
	Term Term   `json:"term"` // currentTerm
	Vote NodeID `json:"vote"` // votedFor in Term, or None
}

// Ready is the work a node hands back to its caller. The caller must:
//
//  1. persist HardState (if non-nil) and Entries, and wait for that to finish
//  2. only then send Messages
//  3. apply CommittedEntries to the state machine, in order
//  4. call Advance
//
// Persisting before sending is what lets a reply mean what it says: "I
// voted for you" or "I have these entries" must survive a crash.
type Ready struct {
	HardState *HardState // nil if unchanged since the last Ready

	// Entries must be written to stable storage. If the first one has an
	// index the storage already holds, everything from that index on is
	// replaced: the log was truncated after a conflict with the leader.
	Entries []Entry

	Messages []Message

	// CommittedEntries are committed and not yet applied. They are always
	// already durable on this node (they were in an earlier Ready, or in
	// Entries of this one).
	CommittedEntries []Entry

	// Snapshot, if non-nil, came from the leader (InstallSnapshot) and
	// replaces the state machine. The caller must persist it durably,
	// together with the fact that the log now starts after it, before doing
	// anything else with this Ready; then load it into the state machine
	// before applying CommittedEntries, which follow on from it.
	Snapshot *Snapshot

	// ReadStates are ReadIndex requests whose leadership check has passed.
	// Each may be answered from the local state machine once every entry up
	// to its Index has been applied.
	ReadStates []ReadState
}

// Snapshot is a compacted prefix of the log (§7): the state machine's
// state after applying every entry up to Index, the term of that entry,
// and the state itself, which only the application understands.
type Snapshot struct {
	Index  uint64   `json:"index"`
	Term   Term     `json:"term"`
	Voters []NodeID `json:"voters"` // the configuration in effect at Index
	Data   []byte   `json:"data,omitempty"`
}

// ReadState is a confirmed ReadIndex request: the caller's ID for it, and
// the commit index that the state machine must reach before the read is
// served.
type ReadState struct {
	ID    uint64 `json:"id"`
	Index uint64 `json:"index"`
}

// Status is a read-only snapshot of a node, for tests, invariants, the
// trace and (later) metrics.
type Status struct {
	ID     NodeID `json:"id"`
	Role   Role   `json:"role"`
	Term   Term   `json:"term"`
	Vote   NodeID `json:"vote"`
	Leader NodeID `json:"leader"` // the leader of Term as far as this node knows, or None

	LastIndex uint64 `json:"last_index"`
	LastTerm  Term   `json:"last_term"`
	Commit    uint64 `json:"commit"` // commitIndex

	// SnapIndex and SnapTerm describe the latest snapshot: entries up to
	// SnapIndex are no longer in the log. Both 0 without a snapshot.
	SnapIndex uint64 `json:"snap_index,omitempty"`
	SnapTerm  Term   `json:"snap_term,omitempty"`

	// Voters is the current configuration, sorted.
	Voters []NodeID `json:"voters"`
}

// Rand is the randomness a node needs: picking election timeouts. It is
// passed in so the simulator can supply a seeded generator; sim.Rand
// satisfies it.
type Rand interface {
	// Intn returns a uniform random int in [0, n).
	Intn(n int) int
}
