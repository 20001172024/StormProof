package raftsim

import (
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
	}
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
