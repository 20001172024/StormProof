package raftsim

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"github.com/20001172024/stormproof/check"
	"github.com/20001172024/stormproof/raft"
	"github.com/20001172024/stormproof/server/host"
	"github.com/20001172024/stormproof/sim"
)

// Invariants returns fresh copies of the election invariants. They keep
// history across the whole run, so each run needs new ones.
func Invariants() []sim.Invariant {
	return []sim.Invariant{
		&electionSafety{leaders: map[raft.Term]raft.NodeID{}},
		&termMonotonic{highest: map[sim.NodeID]raft.Term{}},
		&oneVotePerTerm{votes: map[voteKey]raft.NodeID{}},
		&logMatching{seen: map[sim.NodeID]version{}},
		&leaderCompleteness{seen: map[sim.NodeID]version{}},
		&singleServerChanges{seen: map[sim.NodeID]version{}},
		&stateMachineSafety{snaps: map[uint64][]byte{}, states: map[uint64]stateSeen{}, seen: map[sim.NodeID]inspectable{},
			checked: map[sim.NodeID]int{}, checkedSnaps: map[sim.NodeID]int{}, checkedState: map[sim.NodeID]int{}},
	}
}

// inspectable is what the invariants need to see of a server. *Process
// implements it; tests check the invariants against fakes.
type inspectable interface {
	Status() raft.Status
	Entries() []raft.Entry
	LogVersion() uint64
	Applied() []raft.Entry
	AppliedIndex() uint64
	Snapshots() []raft.Snapshot
	States() []host.IndexedState
}

func proc(nv sim.NodeView) inspectable { return nv.Process.(inspectable) }

// servers drops the clients from a final check's node list.
func servers(nodes []sim.NodeView) []sim.NodeView {
	var out []sim.NodeView
	for _, nv := range nodes {
		if !nv.Client {
			out = append(out, nv)
		}
	}
	return out
}

// Linearizable is the end-of-run check that the clients' combined history
// is linearizable (checked with Porcupine). It needs no quiet period: an
// operation still waiting for an answer counts as "may or may not have
// happened".
func Linearizable(nodes []sim.NodeView) error {
	var ops []check.Op
	for _, nv := range nodes {
		if c, ok := nv.Process.(*Client); ok {
			ops = append(ops, c.History()...)
		}
	}
	if !check.Linearizable(ops) {
		return fmt.Errorf("linearizability: the history of %d client operations is not linearizable", len(ops))
	}
	return nil
}

// SettledAndLinearizable runs both end-of-run checks.
func SettledAndLinearizable(nodes []sim.NodeView) error {
	if err := Settled(nodes); err != nil {
		return err
	}
	return Linearizable(nodes)
}

// version identifies the state of a node's log: which process (a restart
// makes a new one), how many times its log has changed, and its commit
// index. If it is the same as at the last check, the log is unchanged.
type version struct {
	p      inspectable
	log    uint64
	commit uint64
	extra  int // checker-specific, e.g. how many committed entries were known
}

func versionOf(nv sim.NodeView) version {
	p := proc(nv)
	return version{p: p, log: p.LogVersion(), commit: p.Status().Commit}
}

func sameEntry(a, b raft.Entry) bool {
	return a.Index == b.Index && a.Term == b.Term && bytes.Equal(a.Data, b.Data)
}

func status(nv sim.NodeView) raft.Status { return proc(nv).Status() }

// electionSafety: "at most one leader can be elected in a given term"
// (Figure 3). It remembers the leader of every term for the whole run, so a
// second leader is caught even if the first one has crashed meanwhile.
type electionSafety struct {
	leaders map[raft.Term]raft.NodeID
}

func (*electionSafety) Name() string { return "election-safety" }

func (e *electionSafety) Check(nodes []sim.NodeView) error {
	for _, nv := range nodes {
		if !nv.Up {
			continue
		}
		st := status(nv)
		if st.Role != raft.Leader {
			continue
		}
		if prev, ok := e.leaders[st.Term]; ok && prev != st.ID {
			return fmt.Errorf("two leaders in term %d: node %d and node %d", st.Term, prev, st.ID)
		}
		e.leaders[st.Term] = st.ID
	}
	return nil
}

// termMonotonic: a node's currentTerm never decreases, even across a
// restart. If it does, the node lost persisted state.
type termMonotonic struct {
	highest map[sim.NodeID]raft.Term
}

func (*termMonotonic) Name() string { return "term-monotonic" }

func (m *termMonotonic) Check(nodes []sim.NodeView) error {
	for _, nv := range nodes {
		if !nv.Up {
			continue
		}
		term := status(nv).Term
		if term < m.highest[nv.ID] {
			return fmt.Errorf("node %d term went back from %d to %d", nv.ID, m.highest[nv.ID], term)
		}
		m.highest[nv.ID] = term
	}
	return nil
}

// oneVotePerTerm: a node never votes for two different candidates in the
// same term, even across a restart. This is what Raft's vote persistence
// exists to guarantee; breaking it is how two leaders get elected.
type oneVotePerTerm struct {
	votes map[voteKey]raft.NodeID
}

type voteKey struct {
	node sim.NodeID
	term raft.Term
}

func (*oneVotePerTerm) Name() string { return "one-vote-per-term" }

func (o *oneVotePerTerm) Check(nodes []sim.NodeView) error {
	for _, nv := range nodes {
		if !nv.Up {
			continue
		}
		st := status(nv)
		if st.Vote == raft.None {
			continue
		}
		k := voteKey{nv.ID, st.Term}
		if prev, ok := o.votes[k]; ok && prev != st.Vote {
			return fmt.Errorf("node %d voted for %d and then %d in term %d", nv.ID, prev, st.Vote, st.Term)
		}
		o.votes[k] = st.Vote
	}
	return nil
}

// logView is one node's log as the invariants see it: the snapshot it
// starts after, and the entries after that.
type logView struct {
	snapIndex uint64
	snapTerm  raft.Term
	entries   []raft.Entry
}

func viewOf(nv sim.NodeView) logView {
	st := status(nv)
	return logView{snapIndex: st.SnapIndex, snapTerm: st.SnapTerm, entries: proc(nv).Entries()}
}

func (l logView) first() uint64 { return l.snapIndex + 1 }
func (l logView) last() uint64  { return l.snapIndex + uint64(len(l.entries)) }

// at returns the entry at index i, which must be in [first, last].
func (l logView) at(i uint64) raft.Entry { return l.entries[i-l.snapIndex-1] }

// logMatching: "if two logs contain an entry with the same index and term,
// then the logs are identical in all entries up through the given index"
// (Figure 3). For each pair of running nodes it looks at the indexes both
// still hold (snapshots cut off the start), finds the highest one where
// their terms agree, and checks everything both hold up to there is
// identical. Pairs where neither log changed since the last check are
// skipped.
type logMatching struct {
	seen map[sim.NodeID]version
}

func (*logMatching) Name() string { return "log-matching" }

func (lm *logMatching) Check(nodes []sim.NodeView) error {
	changed := map[sim.NodeID]bool{}
	for _, nv := range nodes {
		if nv.Up {
			v := versionOf(nv)
			changed[nv.ID] = lm.seen[nv.ID] != v
			lm.seen[nv.ID] = v
		}
	}
	for i, a := range nodes {
		if !a.Up {
			continue
		}
		for _, b := range nodes[i+1:] {
			if !b.Up || (!changed[a.ID] && !changed[b.ID]) {
				continue
			}
			la, lb := viewOf(a), viewOf(b)
			lo, k := max(la.first(), lb.first()), min(la.last(), lb.last())
			for k >= lo && la.at(k).Term != lb.at(k).Term {
				k--
			}
			for j := lo; j <= k && k >= lo; j++ {
				if !sameEntry(la.at(j), lb.at(j)) {
					return fmt.Errorf("nodes %d and %d agree on entry %d (term %d) but differ at entry %d: %v vs %v",
						a.ID, b.ID, k, la.at(k).Term, j, la.at(j), lb.at(j))
				}
			}
		}
	}
	return nil
}

// leaderCompleteness: "if a log entry is committed in a given term, then
// that entry will be present in the logs of the leaders for all higher-
// numbered terms" (Figure 3).
//
// The checker keeps every entry any node has reported as committed, with
// the term of the node that first reported it. A committed entry must never
// change (that alone catches the Figure 8 failure), and every leader of a
// later term must hold it, either in its log or covered by its snapshot.
//
// "Later" matters. A leader cut off by a partition can still believe it
// leads an old term while the rest of the cluster commits entries in newer
// terms. It doesn't have those entries, and Raft doesn't promise it will:
// it can't commit anything, and it steps down as soon as it hears the new
// term. (An earlier version of this checker required every current leader
// to hold every committed entry, and flagged exactly that situation; see
// docs/bugs/003.)
//
// Snapshots mean a node may report a commit index without still holding
// all the entries up to it, so the record has gaps: known[i] says whether
// entry i+1 has been seen.
type leaderCompleteness struct {
	committed  []raft.Entry // committed[i] is the committed entry at index i+1, if known[i]
	known      []bool
	reportedIn []raft.Term // term of the node that first reported committed[i]
	// seen skips nodes whose log, commit index and known-committed count
	// are unchanged since they were last checked.
	seen map[sim.NodeID]version
}

func (*leaderCompleteness) Name() string { return "leader-completeness" }

func (c *leaderCompleteness) record(i uint64, e raft.Entry, term raft.Term) error {
	for uint64(len(c.committed)) < i {
		c.committed = append(c.committed, raft.Entry{})
		c.known = append(c.known, false)
		c.reportedIn = append(c.reportedIn, 0)
	}
	if !c.known[i-1] {
		c.committed[i-1], c.known[i-1], c.reportedIn[i-1] = e, true, term
		return nil
	}
	if prev := c.committed[i-1]; !sameEntry(prev, e) {
		return fmt.Errorf("committed entry %d changed: was %v, now %v", i, prev, e)
	}
	return nil
}

func (c *leaderCompleteness) Check(nodes []sim.NodeView) error {
	before := len(c.committed)
	for _, nv := range nodes {
		if !nv.Up {
			continue
		}
		v := versionOf(nv)
		if c.seen[nv.ID].p == v.p && c.seen[nv.ID].log == v.log && c.seen[nv.ID].commit == v.commit {
			continue // nothing this node reports has changed
		}
		st, l := status(nv), viewOf(nv)
		if st.Commit > l.last() {
			return fmt.Errorf("node %d has commit index %d but its log ends at %d: it deleted committed entries", nv.ID, st.Commit, l.last())
		}
		if l.snapIndex > 0 && l.snapIndex <= uint64(len(c.committed)) && c.known[l.snapIndex-1] &&
			c.committed[l.snapIndex-1].Term != l.snapTerm {
			return fmt.Errorf("node %d has a snapshot up to %d in term %d, but entry %d was committed in term %d",
				nv.ID, l.snapIndex, l.snapTerm, l.snapIndex, c.committed[l.snapIndex-1].Term)
		}
		for i := l.first(); i <= st.Commit; i++ {
			if err := c.record(i, l.at(i), st.Term); err != nil {
				return fmt.Errorf("node %d: %w", nv.ID, err)
			}
		}
	}
	for _, nv := range nodes {
		if !nv.Up {
			continue
		}
		st := status(nv)
		if st.Role != raft.Leader {
			continue
		}
		v := versionOf(nv)
		v.extra = len(c.committed)
		if c.seen[nv.ID] == v && len(c.committed) == before {
			continue // same log, nothing newly committed: already checked
		}
		l := viewOf(nv)
		for i, want := range c.committed {
			idx := uint64(i + 1)
			// The entry was committed by a leader of a term no later than the
			// reporting node's term, so leaders of any later term must have it.
			if !c.known[i] || st.Term <= c.reportedIn[i] || idx < l.snapIndex {
				continue // (entries before the snapshot index are checked via its term below)
			}
			if idx == l.snapIndex {
				if l.snapTerm != want.Term {
					return fmt.Errorf("leader %d of term %d has a snapshot ending at %d in term %d; the committed entry there has term %d",
						nv.ID, st.Term, idx, l.snapTerm, want.Term)
				}
				continue
			}
			if idx > l.last() || !sameEntry(l.at(idx), want) {
				return fmt.Errorf("leader %d of term %d is missing entry %d %v, committed by term %d",
					nv.ID, st.Term, idx, want, c.reportedIn[i])
			}
		}
	}
	for _, nv := range nodes {
		if nv.Up {
			v := versionOf(nv)
			v.extra = len(c.committed)
			c.seen[nv.ID] = v
		}
	}
	return nil
}

// stateMachineSafety: "if a server has applied a log entry at a given
// index to its state machine, no other server will ever apply a different
// log entry for the same index" (Figure 3).
//
// Snapshots replace applying entries one by one, so this checks the result
// directly too: after applying index i (or loading a snapshot ending at i),
// every node's whole store must be identical, compared by hash. That catches
// state that diverges without any client noticing, such as a snapshot that
// loses part of the state. Every snapshot at a given index must also be
// byte-for-byte the same on every node.
type stateMachineSafety struct {
	applied []raft.Entry // the first entry applied at each index, by any node
	have    []bool
	snaps   map[uint64][]byte // snapshot data by index
	states  map[uint64]stateSeen
	// Only new events are checked each time; a restarted node is a new
	// process with its own history.
	seen         map[sim.NodeID]inspectable
	checked      map[sim.NodeID]int
	checkedSnaps map[sim.NodeID]int
	checkedState map[sim.NodeID]int
}

type stateSeen struct {
	hash uint64
	node sim.NodeID
}

func (*stateMachineSafety) Name() string { return "state-machine-safety" }

func (s *stateMachineSafety) Check(nodes []sim.NodeView) error {
	for _, nv := range nodes {
		if !nv.Up {
			continue
		}
		p := proc(nv)
		if s.seen[nv.ID] != p {
			s.seen[nv.ID], s.checked[nv.ID], s.checkedSnaps[nv.ID], s.checkedState[nv.ID] = p, 0, 0, 0
		}
		for _, st := range p.States()[s.checkedState[nv.ID]:] {
			if prev, ok := s.states[st.Index]; ok && prev.hash != st.Hash {
				return fmt.Errorf("node %d's store after index %d differs from node %d's", nv.ID, st.Index, prev.node)
			}
			s.states[st.Index] = stateSeen{st.Hash, nv.ID}
		}
		s.checkedState[nv.ID] = len(p.States())
		for _, sn := range p.Snapshots()[s.checkedSnaps[nv.ID]:] {
			if prev, ok := s.snaps[sn.Index]; ok && !bytes.Equal(prev, sn.Data) {
				return fmt.Errorf("node %d has a different snapshot at index %d from another node's", nv.ID, sn.Index)
			}
			s.snaps[sn.Index] = sn.Data
		}
		s.checkedSnaps[nv.ID] = len(p.Snapshots())
		for _, e := range p.Applied()[s.checked[nv.ID]:] {
			for uint64(len(s.applied)) < e.Index {
				s.applied = append(s.applied, raft.Entry{})
				s.have = append(s.have, false)
			}
			if j := e.Index - 1; s.have[j] {
				if !sameEntry(s.applied[j], e) {
					return fmt.Errorf("node %d applied %v at index %d, but %v was applied there before", nv.ID, e, e.Index, s.applied[j])
				}
			} else {
				s.applied[j], s.have[j] = e, true
			}
		}
		s.checked[nv.ID] = len(p.Applied())
	}
	return nil
}

// Settled is the end-of-run liveness check, for after a quiet period with
// the workload stopped: there is one stable leader (StableLeader), and every
// server in the configuration has a log ending at the same entry as the
// leader's, everything in it committed, and all of it applied.
func Settled(nodes []sim.NodeView) error {
	nodes, err := members(nodes)
	if err != nil {
		return err
	}
	var leader raft.Status
	for _, nv := range nodes {
		if st := status(nv); st.Role == raft.Leader {
			leader = st
		}
	}
	for _, nv := range nodes {
		st := status(nv)
		if st.LastIndex != leader.LastIndex || st.LastTerm != leader.LastTerm {
			return fmt.Errorf("node %d log ends at %d (term %d), the leader's at %d (term %d)",
				nv.ID, st.LastIndex, st.LastTerm, leader.LastIndex, leader.LastTerm)
		}
		if st.Commit != st.LastIndex {
			return fmt.Errorf("node %d committed %d of %d entries", nv.ID, st.Commit, st.LastIndex)
		}
		if a := proc(nv).AppliedIndex(); a != st.LastIndex {
			return fmt.Errorf("node %d applied up to %d of %d", nv.ID, a, st.LastIndex)
		}
	}
	return nil
}

// StableLeader is a liveness check for the end of a run, after a quiet
// period: among the servers in the current configuration, all are up, they
// agree on the same term, and exactly one of them leads it and is known as
// leader by all. Servers outside the configuration (never added, or
// removed) are left out: a removed server is never told it was removed.
func StableLeader(nodes []sim.NodeView) error {
	_, err := members(nodes)
	return err
}

// members returns the servers in the current configuration, after checking
// StableLeader. The current configuration is the one the newest leader has.
func members(nodes []sim.NodeView) ([]sim.NodeView, error) {
	nodes = servers(nodes)
	var leader raft.Status
	for _, nv := range nodes {
		if nv.Up {
			if st := status(nv); st.Role == raft.Leader && st.Term > leader.Term {
				leader = st
			}
		}
	}
	if leader.ID == raft.None {
		return nil, errors.New("no leader")
	}
	var out []sim.NodeView
	for _, nv := range nodes {
		if !slices.Contains(leader.Voters, raft.NodeID(nv.ID)) {
			continue
		}
		if !nv.Up {
			return nil, fmt.Errorf("node %d is still down", nv.ID)
		}
		st := status(nv)
		if st.Role == raft.Leader && st.ID != leader.ID {
			return nil, fmt.Errorf("nodes %d and %d both think they lead", leader.ID, st.ID)
		}
		if st.Term != leader.Term || st.Leader != leader.ID {
			return nil, fmt.Errorf("node %d is in term %d following %d; leader %d is in term %d",
				st.ID, st.Term, st.Leader, leader.ID, leader.Term)
		}
		out = append(out, nv)
	}
	return out, nil
}

// singleServerChanges: consecutive configurations in any server's log
// differ by exactly one voter. That is the property that makes any majority
// of one overlap any majority of the next (dissertation §4.1).
type singleServerChanges struct {
	seen map[sim.NodeID]version
}

func (*singleServerChanges) Name() string { return "single-server-changes" }

func (c *singleServerChanges) Check(nodes []sim.NodeView) error {
	for _, nv := range nodes {
		if !nv.Up {
			continue
		}
		v := versionOf(nv)
		if c.seen[nv.ID] == v {
			continue
		}
		c.seen[nv.ID] = v
		var prev []raft.NodeID
		for _, e := range proc(nv).Entries() {
			if e.Type != raft.EntryConfChange {
				continue
			}
			cur, err := raft.DecodeVoters(e.Data)
			if err != nil {
				return fmt.Errorf("node %d: entry %d: %w", nv.ID, e.Index, err)
			}
			if prev != nil && symmetricDifference(prev, cur) != 1 {
				return fmt.Errorf("node %d: configuration at entry %d %v follows %v: not a single-server change", nv.ID, e.Index, cur, prev)
			}
			prev = cur
		}
	}
	return nil
}

func symmetricDifference(a, b []raft.NodeID) int {
	d := 0
	for _, x := range a {
		if !slices.Contains(b, x) {
			d++
		}
	}
	for _, x := range b {
		if !slices.Contains(a, x) {
			d++
		}
	}
	return d
}
