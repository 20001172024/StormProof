package raft

import (
	"errors"
	"testing"
)

func TestReadIndexRequiresLeader(t *testing.T) {
	c := newCluster(t, 3)
	c.elect(1)
	if err := c.nodes[2].ReadIndex(1); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower: err = %v, want ErrNotLeader", err)
	}
}

// A new leader can't serve reads until it has committed an entry in its own
// term: before that its commit index may lag what an earlier leader
// committed.
func TestReadIndexWaitsForOwnTermCommit(t *testing.T) {
	c := newCluster(t, 3)
	n := c.nodes[1]
	for n.role != Candidate {
		n.Tick()
	}
	n.Step(Message{Type: MsgVoteResp, From: 2, Term: n.term, VoteGranted: true}) // leader, no-op not yet replicated
	if err := n.ReadIndex(1); !errors.Is(err, ErrReadNotReady) {
		t.Fatalf("err = %v, want ErrReadNotReady before the no-op commits", err)
	}
	c.settle() // replicate and commit the no-op
	if err := n.ReadIndex(1); err != nil {
		t.Fatalf("after the no-op committed: %v", err)
	}
}

func TestReadIndexConfirmedByMajority(t *testing.T) {
	c := newCluster(t, 3)
	c.elect(1)
	c.propose(1, "x")
	commit := c.nodes[1].commitIndex

	// Nobody answers: the read must not be confirmed.
	c.drop = func(m Message) bool { return m.Type == MsgAppResp }
	if err := c.nodes[1].ReadIndex(42); err != nil {
		t.Fatal(err)
	}
	c.settle()
	if len(c.reads[1]) != 0 {
		t.Fatalf("read confirmed with no acknowledgements: %+v", c.reads[1])
	}

	// One follower answers the next heartbeat: with the leader, a majority.
	c.drop = func(m Message) bool { return m.Type == MsgAppResp && m.From == 3 }
	c.heartbeat(1)
	if got := c.reads[1]; len(got) != 1 || got[0] != (ReadState{ID: 42, Index: commit}) {
		t.Fatalf("reads = %+v, want read 42 at index %d", got, commit)
	}
}

// A response to a heartbeat sent before the read began proves nothing
// about leadership after it began, so it must not confirm the read.
func TestReadIndexIgnoresOlderHeartbeats(t *testing.T) {
	c := newCluster(t, 3)
	c.elect(1)
	n := c.nodes[1]
	if err := n.ReadIndex(1); err != nil { // read 1: seq 1
		t.Fatal(err)
	}
	c.settle()                             // confirms read 1
	if err := n.ReadIndex(2); err != nil { // read 2: seq 2
		t.Fatal(err)
	}
	drain(n)
	// A delayed response to a heartbeat with Context 1.
	n.Step(Message{Type: MsgAppResp, From: 2, Term: n.term, Success: true, MatchIndex: n.log.lastIndex(), Context: 1})
	if rd := n.Ready(); len(rd.ReadStates) != 0 {
		t.Fatalf("read 2 confirmed by a response to an older heartbeat: %+v", rd.ReadStates)
	}
}

func TestReadIndexOrderAndStepDown(t *testing.T) {
	c := newCluster(t, 3)
	c.elect(1)
	n := c.nodes[1]
	c.drop = func(m Message) bool { return m.Type == MsgAppResp }
	for id := uint64(1); id <= 3; id++ {
		if err := n.ReadIndex(id); err != nil {
			t.Fatal(err)
		}
	}
	c.settle()
	c.drop = nil
	c.heartbeat(1)
	got := c.reads[1]
	if len(got) != 3 || got[0].ID != 1 || got[1].ID != 2 || got[2].ID != 3 {
		t.Fatalf("reads = %+v, want 1, 2, 3 in order", got)
	}

	// Pending reads are dropped when the leader steps down.
	c.drop = func(m Message) bool { return m.Type == MsgAppResp }
	if err := n.ReadIndex(4); err != nil {
		t.Fatal(err)
	}
	n.Step(Message{Type: MsgApp, From: 2, Term: n.term + 1})
	c.drop = nil
	c.settle()
	for _, r := range c.reads[1] {
		if r.ID == 4 {
			t.Fatal("a read pending when the leader stepped down was confirmed")
		}
	}
}

func TestReadIndexSingleNode(t *testing.T) {
	n := newTestNode(t, 1, nil, HardState{})
	tickN(n, testElection)
	if err := n.ReadIndex(9); err != nil {
		t.Fatal(err)
	}
	if rd := n.Ready(); len(rd.ReadStates) != 1 || rd.ReadStates[0].ID != 9 {
		t.Fatalf("ReadStates = %+v, want read 9 immediately", rd.ReadStates)
	}
}

func TestReadIndexWithoutQuorumKnob(t *testing.T) {
	c := newCluster(t, 3)
	c.elect(1)
	c.nodes[1].cfg.Knobs.ReadIndexWithoutQuorum = true
	c.drop = func(m Message) bool { return true } // nobody can answer
	if err := c.nodes[1].ReadIndex(5); err != nil {
		t.Fatal(err)
	}
	c.settle()
	if len(c.reads[1]) != 1 {
		t.Fatal("with the knob, the read should be confirmed with no acknowledgements")
	}
}

// The deposed-leader stale read, scripted (the random simulator rarely sets
// it up; see docs/design.md). Node 1 leads, is cut off, and node 2 is
// elected and commits a write. Node 1 still believes it leads. A read at
// node 1 must not be confirmed: if it were, node 1 would answer from a
// state machine that lacks the write. TestingKnobs.ReadIndexWithoutQuorum
// shows exactly that happening.
func TestReadIndexDeposedLeader(t *testing.T) {
	for _, unsafe := range []bool{false, true} {
		c := newCluster(t, 3)
		c.elect(1)
		cutOff := map[NodeID]bool{1: true}
		c.drop = func(m Message) bool { return cutOff[m.From] || cutOff[m.To] }
		c.campaign(2)
		c.propose(2, "the write")
		c.heartbeat(2)
		written := c.nodes[2].commitIndex

		old := c.nodes[1]
		old.cfg.Knobs.ReadIndexWithoutQuorum = unsafe
		if old.role != Leader {
			t.Fatal("setup: node 1 should still believe it leads")
		}
		if err := old.ReadIndex(77); err != nil {
			t.Fatal(err)
		}
		c.settle()
		confirmed := len(c.reads[1]) == 1
		if confirmed != unsafe {
			t.Fatalf("unsafe=%v: read confirmed = %v", unsafe, confirmed)
		}
		if confirmed && c.reads[1][0].Index >= written {
			t.Fatalf("setup: the stale read index %d already covers the write at %d", c.reads[1][0].Index, written)
		}
	}
}
