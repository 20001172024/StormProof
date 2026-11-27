package raft

import (
	"encoding/binary"
	"errors"
	"slices"
)

// Cluster membership changes, one server at a time (Raft dissertation §4).
//
// A configuration is the set of voters: the servers whose votes count in
// elections and whose copies count towards committing. It changes through
// special log entries (Type EntryConfChange) whose Data is the complete new
// voter list.
//
// The rules:
//
//   - A server uses the newest configuration in its log, committed or not,
//     as soon as the entry is appended (§4.1). If the entry is later
//     truncated away, it goes back to the one before.
//   - Only one server is added or removed per change, and a leader won't
//     start a change while an earlier one is uncommitted. Any majority of
//     the old configuration then overlaps any majority of the new one, so
//     two leaders can't be elected in the same term (§4.2).
//   - A leader may only start a change once it has committed an entry in
//     its own term. Without this the single-server algorithm can elect two
//     leaders; Ongaro found and fixed it in 2015 (raft-dev, "bug in
//     single-server membership changes"). The no-op every leader appends
//     on election makes it cheap.
//   - A leader that removes itself keeps leading, without counting itself
//     towards majorities, until the change commits, then steps down
//     (§4.2.2).
//   - A server that has heard from a current leader within the minimum
//     election timeout ignores RequestVote (§4.2.3). Removed servers never
//     learn they were removed and keep starting elections; this stops them
//     from disrupting the cluster.
//
// New servers join as full voters immediately, without the catch-up phase
// of §4.2.1. That is safe but can cost availability while a slow new server
// catches up.

// ConfChangeOp is the kind of membership change.
type ConfChangeOp int

const (
	AddVoter ConfChangeOp = iota + 1
	RemoveVoter
)

func (o ConfChangeOp) String() string {
	switch o {
	case AddVoter:
		return "add"
	case RemoveVoter:
		return "remove"
	}
	return "unknown"
}

// MarshalText makes ops appear by name in JSON traces.
func (o ConfChangeOp) MarshalText() ([]byte, error) { return []byte(o.String()), nil }

// ConfChange is a request to add or remove one voter.
type ConfChange struct {
	Op   ConfChangeOp `json:"op"`
	Node NodeID       `json:"node"`
}

// Errors from ProposeConfChange.
var (
	ErrConfChangeInProgress = errors.New("raft: an earlier configuration change is not committed yet")
	ErrConfChangeNotReady   = errors.New("raft: leader has not committed an entry in its term yet")
	ErrConfChangeNoop       = errors.New("raft: configuration already has that change")
	ErrConfChangeInvalid    = errors.New("raft: invalid configuration change")
)

// EncodeVoters is the Data of a configuration entry: a u32 count, then each
// voter as a u64, little-endian, sorted.
func EncodeVoters(voters []NodeID) []byte {
	b := binary.LittleEndian.AppendUint32(nil, uint32(len(voters)))
	for _, v := range voters {
		b = binary.LittleEndian.AppendUint64(b, uint64(v))
	}
	return b
}

// DecodeVoters parses the Data of a configuration entry.
func DecodeVoters(b []byte) ([]NodeID, error) {
	if len(b) < 4 {
		return nil, errors.New("raft: malformed configuration")
	}
	n := binary.LittleEndian.Uint32(b)
	b = b[4:]
	if uint64(len(b)) != uint64(n)*8 {
		return nil, errors.New("raft: malformed configuration")
	}
	vs := make([]NodeID, n)
	for i := range vs {
		vs[i] = NodeID(binary.LittleEndian.Uint64(b[8*i:]))
	}
	return vs, nil
}

// isVoter reports whether id is in the current configuration.
func (n *Node) isVoter(id NodeID) bool {
	_, ok := slices.BinarySearch(n.voters, id)
	return ok
}

// others is the voters other than this node, in sorted order: who a leader
// replicates to and a candidate asks for votes.
func (n *Node) others() []NodeID {
	out := make([]NodeID, 0, len(n.voters))
	for _, v := range n.voters {
		if v != n.id {
			out = append(out, v)
		}
	}
	return out
}

// quorum is the number of voters that make a majority.
func (n *Node) quorum() int { return len(n.voters)/2 + 1 }

// countVoters counts how many members of set are voters, so votes and
// acknowledgements from servers outside the configuration never count.
func (n *Node) countVoters(set map[NodeID]bool) int {
	c := 0
	for id := range set {
		if n.isVoter(id) {
			c++
		}
	}
	return c
}

// configFrom returns the configuration in effect at the end of entries: the
// newest configuration entry among them, or base if there is none.
func configFrom(entries []Entry, base []NodeID) ([]NodeID, uint64) {
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].Type == EntryConfChange {
			vs, err := DecodeVoters(entries[i].Data)
			if err != nil {
				panic("raft: unreadable configuration entry in the log")
			}
			return vs, entries[i].Index
		}
	}
	return base, 0
}

// configAppended updates the configuration after entries were appended to
// the log, with nothing truncated: the newest configuration entry among
// them, if any, becomes the configuration (§4.1: a server uses the latest
// configuration in its log, committed or not). Equivalent to refreshConfig
// in that case, without scanning the whole log, which refreshConfig does
// when the log holds no configuration entry and which made every append
// cost time proportional to the log's length. Only followers append this
// way, so there is no leader bookkeeping to update.
func (n *Node) configAppended(es []Entry) {
	for i := len(es) - 1; i >= 0; i-- {
		if es[i].Type == EntryConfChange {
			n.voters, n.confIndex = configFrom(es[i:i+1], n.baseVoters)
			return
		}
	}
}

// refreshConfig recomputes the current configuration from the log. It is
// called whenever the log changes, since appending a configuration entry
// changes the configuration immediately and truncating one undoes it.
func (n *Node) refreshConfig() {
	n.voters, n.confIndex = configFrom(n.log.entries, n.baseVoters)
	if n.role != Leader {
		return
	}
	// A leader tracks progress for exactly the other voters. A new voter's
	// nextIndex starts optimistic, as after an election; backtracking (or a
	// snapshot) brings it up to date.
	for _, v := range n.others() {
		if _, ok := n.nextIndex[v]; !ok {
			n.nextIndex[v], n.matchIndex[v] = n.log.lastIndex()+1, 0
			n.inflight[v], n.sentUpTo[v] = false, 0
		}
	}
	for id := range n.nextIndex {
		if !n.isVoter(id) {
			delete(n.nextIndex, id)
			delete(n.matchIndex, id)
			delete(n.inflight, id)
			delete(n.sentUpTo, id)
		}
	}
}

// configAt returns the configuration in effect after entry index, which
// must be in the log.
func (n *Node) configAt(index uint64) []NodeID {
	vs, _ := configFrom(n.log.slice(n.log.firstIndex(), index+1), n.baseVoters)
	return vs
}

// ProposeConfChange asks the leader to add or remove one voter, returning
// the index of the configuration entry. The change takes effect on each
// server as soon as the entry reaches its log; the caller learns it
// committed from Ready.CommittedEntries.
func (n *Node) ProposeConfChange(cc ConfChange) (uint64, error) {
	if n.role != Leader {
		return 0, ErrNotLeader
	}
	k := n.cfg.Knobs
	if t, _ := n.log.term(n.commitIndex); t != n.term && !k.ConfChangeBeforeOwnTermCommit {
		return 0, ErrConfChangeNotReady
	}
	if n.confIndex > n.commitIndex && !k.ConcurrentConfChanges {
		return 0, ErrConfChangeInProgress
	}
	next := slices.Clone(n.voters)
	switch cc.Op {
	case AddVoter:
		if n.isVoter(cc.Node) {
			return 0, ErrConfChangeNoop
		}
		next = append(next, cc.Node)
		slices.Sort(next)
	case RemoveVoter:
		if !n.isVoter(cc.Node) {
			return 0, ErrConfChangeNoop
		}
		if len(next) == 1 {
			return 0, ErrConfChangeInvalid // can't remove the last voter
		}
		next = slices.DeleteFunc(next, func(v NodeID) bool { return v == cc.Node })
	default:
		return 0, ErrConfChangeInvalid
	}
	e := Entry{Index: n.log.lastIndex() + 1, Term: n.term, Type: EntryConfChange, Data: EncodeVoters(next)}
	n.log.append(e)
	n.refreshConfig()
	n.maybeCommit()
	if n.role == Leader { // committing may have removed us (one-node case)
		n.broadcastAppend()
	}
	return e.Index, nil
}

// stepDownIfRemoved makes a leader that is not in the configuration step
// down once that configuration has committed (§4.2.2).
func (n *Node) stepDownIfRemoved() {
	if n.role == Leader && !n.isVoter(n.id) && n.commitIndex >= n.confIndex {
		n.becomeFollower(n.term, None)
	}
}
