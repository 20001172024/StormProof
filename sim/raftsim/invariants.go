package raftsim

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/20001172024/stormproof/raft"
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
		&stateMachineSafety{seen: map[sim.NodeID]*Process{}, checked: map[sim.NodeID]int{}},
	}
}

func proc(nv sim.NodeView) *Process { return nv.Process.(*Process) }

// version identifies the state of a node's log: which process (a restart
// makes a new one), how many times its log has changed, and its commit
// index. If it is the same as at the last check, the log is unchanged.
type version struct {
	p      *Process
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

func status(nv sim.NodeView) raft.Status { return nv.Process.(*Process).Status() }

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

// logMatching: "if two logs contain an entry with the same index and term,
// then the logs are identical in all entries up through the given index"
// (Figure 3). For each pair of running nodes it finds the highest index
// where their terms agree and checks everything up to there is identical.
// Pairs where neither log changed since the last check are skipped.
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
			la, lb := proc(a).Entries(), proc(b).Entries()
			k := min(len(la), len(lb))
			for k > 0 && la[k-1].Term != lb[k-1].Term {
				k--
			}
			for j := 0; j < k; j++ {
				if !sameEntry(la[j], lb[j]) {
					return fmt.Errorf("nodes %d and %d agree on entry %d (term %d) but differ at entry %d: %v vs %v",
						a.ID, b.ID, k, la[k-1].Term, j+1, la[j], lb[j])
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
// later term must hold it.
//
// "Later" matters. A leader cut off by a partition can still believe it
// leads an old term while the rest of the cluster commits entries in newer
// terms. It doesn't have those entries, and Raft doesn't promise it will:
// it can't commit anything, and it steps down as soon as it hears the new
// term. (An earlier version of this checker required every current leader
// to hold every committed entry, and flagged exactly that situation.)
type leaderCompleteness struct {
	committed  []raft.Entry // committed[i] is the committed entry at index i+1
	reportedIn []raft.Term  // term of the node that first reported committed[i]
	// seen skips nodes whose log, commit index and known-committed count
	// are unchanged since they were last checked.
	seen map[sim.NodeID]version
}

func (*leaderCompleteness) Name() string { return "leader-completeness" }

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
		st, log := status(nv), proc(nv).Entries()
		if st.Commit > uint64(len(log)) {
			return fmt.Errorf("node %d has commit index %d but only %d entries: it deleted committed entries", nv.ID, st.Commit, len(log))
		}
		for i := uint64(1); i <= st.Commit; i++ {
			e := log[i-1]
			if i <= uint64(len(c.committed)) {
				if prev := c.committed[i-1]; !sameEntry(prev, e) {
					return fmt.Errorf("committed entry %d changed: was %v, node %d now has %v committed", i, prev, nv.ID, e)
				}
			} else {
				c.committed = append(c.committed, e)
				c.reportedIn = append(c.reportedIn, st.Term)
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
		log := proc(nv).Entries()
		for i, want := range c.committed {
			// The entry was committed by a leader of a term no later than the
			// reporting node's term, so leaders of any later term must have it.
			if st.Term <= c.reportedIn[i] {
				continue
			}
			if i >= len(log) || !sameEntry(log[i], want) {
				return fmt.Errorf("leader %d of term %d is missing entry %d %v, committed by term %d",
					nv.ID, st.Term, i+1, want, c.reportedIn[i])
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
type stateMachineSafety struct {
	applied []raft.Entry // first entry applied at each index, by any node
	// Only newly applied entries are checked each time: checked counts how
	// many of each process's applied entries have been seen. A restarted
	// node is a new process, which re-applies from the start.
	seen    map[sim.NodeID]*Process
	checked map[sim.NodeID]int
}

func (*stateMachineSafety) Name() string { return "state-machine-safety" }

func (s *stateMachineSafety) Check(nodes []sim.NodeView) error {
	for _, nv := range nodes {
		if !nv.Up {
			continue
		}
		p := proc(nv)
		if s.seen[nv.ID] != p {
			s.seen[nv.ID], s.checked[nv.ID] = p, 0
		}
		applied := p.Applied()
		for j := s.checked[nv.ID]; j < len(applied); j++ {
			e := applied[j]
			if e.Index != uint64(j+1) {
				return fmt.Errorf("node %d applied entry %d as its entry number %d: out of order", nv.ID, e.Index, j+1)
			}
			if j < len(s.applied) {
				if !sameEntry(s.applied[j], e) {
					return fmt.Errorf("node %d applied %v at index %d, but %v was applied there before", nv.ID, e, j+1, s.applied[j])
				}
			} else {
				s.applied = append(s.applied, e)
			}
		}
		s.checked[nv.ID] = len(applied)
	}
	return nil
}

// Settled is the end-of-run liveness check, for after a quiet period with
// the workload stopped: there is one stable leader (StableLeader), every
// node's log is identical to the leader's and fully committed, and every
// node has applied all of it.
func Settled(nodes []sim.NodeView) error {
	if err := StableLeader(nodes); err != nil {
		return err
	}
	var leader []raft.Entry
	for _, nv := range nodes {
		if status(nv).Role == raft.Leader {
			leader = proc(nv).Entries()
		}
	}
	for _, nv := range nodes {
		st, log := status(nv), proc(nv).Entries()
		if len(log) != len(leader) || (len(log) > 0 && !sameEntry(log[len(log)-1], leader[len(leader)-1])) {
			return fmt.Errorf("node %d log (last index %d) differs from the leader's (last index %d)", nv.ID, len(log), len(leader))
		}
		if st.Commit != uint64(len(log)) {
			return fmt.Errorf("node %d committed %d of %d entries", nv.ID, st.Commit, len(log))
		}
		if n := len(proc(nv).Applied()); n != len(log) {
			return fmt.Errorf("node %d applied %d of %d entries", nv.ID, n, len(log))
		}
	}
	return nil
}

// StableLeader is a liveness check for the end of a run, after a quiet
// period: every node is up, they all agree on the same term, and exactly
// one of them is the leader of it and known as leader by all.
func StableLeader(nodes []sim.NodeView) error {
	var leader raft.NodeID
	var term raft.Term
	for _, nv := range nodes {
		if !nv.Up {
			return fmt.Errorf("node %d is still down", nv.ID)
		}
		st := status(nv)
		if st.Role == raft.Leader {
			if leader != raft.None {
				return fmt.Errorf("nodes %d and %d both think they lead", leader, st.ID)
			}
			leader, term = st.ID, st.Term
		}
	}
	if leader == raft.None {
		return errors.New("no leader")
	}
	for _, nv := range nodes {
		if st := status(nv); st.Term != term || st.Leader != leader {
			return fmt.Errorf("node %d is in term %d following %d; leader %d is in term %d", st.ID, st.Term, st.Leader, leader, term)
		}
	}
	return nil
}
