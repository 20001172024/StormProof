package raft

import (
	"fmt"
	"testing"
)

// TestFigure8 replays the scenario in Figure 8 of the Raft paper, step by
// step, on five nodes. It shows why a leader must not commit an entry from
// an earlier term just because a majority stores it (§5.4.2).
//
// The random simulator did not find this in 12,000 runs: it needs five
// unlikely events in a row. So it is scripted here instead.
//
// With CommitPreviousTermsByCounting on, node 1 commits entry 2 (term 2) in
// step (c), and in step (d) node 5 is elected anyway and overwrites it on
// the majority: a committed entry is lost. With the real rule, node 1 never
// commits entry 2, and the overwrite in (d) is perfectly legal.
//
// NoNoopOnElection is on in both runs so the logs match the paper's figure
// (the no-op would add an entry from each new leader's term).
func TestFigure8(t *testing.T) {
	for _, unsafe := range []bool{false, true} {
		t.Run(fmt.Sprintf("CommitPreviousTermsByCounting=%v", unsafe), func(t *testing.T) {
			committedThenLost := figure8(t, TestingKnobs{CommitPreviousTermsByCounting: unsafe, NoNoopOnElection: true})
			if committedThenLost != unsafe {
				t.Fatalf("committed entry overwritten = %v, want %v", committedThenLost, unsafe)
			}
		})
	}
}

// figure8 runs the scenario and reports whether an entry that node 1 had
// committed ended up overwritten on a majority.
func figure8(t *testing.T, knobs TestingKnobs) bool {
	c := newCluster(t, 5)
	// Every node starts with entry 1 from term 1, as in the figure.
	for _, id := range c.ids {
		c.restartWith(id, HardState{Term: 1}, []Entry{{Index: 1, Term: 1}}, knobs)
	}
	down := map[NodeID]bool{}
	var block func(Message) bool // extra drops for one step
	c.drop = func(m Message) bool {
		return down[m.From] || down[m.To] || (block != nil && block(m))
	}
	// crash stops a node; it comes back as a follower from the state it had
	// persisted (in this harness everything is persisted at once).
	crash := func(id NodeID) {
		n := c.nodes[id]
		c.restartWith(id, HardState{Term: n.term, Vote: n.vote}, n.log.entries, knobs)
		down[id] = true
	}

	// (a) Node 1 leads term 2 and replicates entry 2 to node 2 only.
	c.campaign(1)
	block = func(m Message) bool { return m.Type == MsgApp && m.From == 1 && m.To != 2 }
	c.propose(1, "X")
	block = nil
	if got := c.nodes[2].log.lastTerm(); got != 2 {
		t.Fatalf("(a) node 2 last term %d, want 2", got)
	}

	// (b) Node 1 crashes. Node 5 is elected in term 3 by nodes 3, 4 and 5
	// (node 2 refuses: its log is more up-to-date), appends entry 2 in term
	// 3, and crashes before sending it to anyone.
	crash(1)
	c.campaign(5)
	if c.nodes[5].term != 3 {
		t.Fatalf("(b) node 5 leads term %d, want 3", c.nodes[5].term)
	}
	block = func(m Message) bool { return m.From == 5 }
	c.propose(5, "Y")
	block = nil
	crash(5)

	// (c) Node 1 restarts and is elected in term 4 by nodes 1, 2 and 3 (its
	// last term, 2, beats their 1 and 2). It replicates entry 2 (term 2) to
	// nodes 2 and 3: a majority now stores it.
	down[1] = false
	c.campaign(1)
	if c.nodes[1].term != 4 {
		t.Fatalf("(c) node 1 leads term %d, want 4", c.nodes[1].term)
	}
	block = func(m Message) bool { return m.To == 4 || m.From == 4 }
	c.heartbeat(1)
	block = nil
	for _, id := range []NodeID{1, 2, 3} {
		if e := c.nodes[id].log.entries; len(e) != 2 || e[1].Term != 2 {
			t.Fatalf("(c) node %d log %v, want entry 2 from term 2", id, terms(e))
		}
	}
	committedX := c.nodes[1].commitIndex >= 2

	// (d) Node 1 crashes again. Node 5 restarts and is elected in term 5 by
	// nodes 2, 3 and 4: its last term, 3, beats their 2. It overwrites
	// entry 2 on all of them with its own.
	crash(1)
	down[5] = false
	c.campaign(5)
	c.heartbeat(5)
	for _, id := range []NodeID{2, 3, 4} {
		if e := c.nodes[id].log.entries; len(e) != 2 || e[1].Term != 3 {
			t.Fatalf("(d) node %d log %v, want entry 2 replaced by term 3", id, terms(e))
		}
	}
	return committedX
}

// restartWith is restart with testing knobs.
func (c *cluster) restartWith(id NodeID, hs HardState, entries []Entry, knobs TestingKnobs) {
	c.restart(id, hs, entries)
	c.nodes[id].cfg.Knobs = knobs
}

// campaign makes id run elections until it wins (a first try can fail,
// for example just to learn that a newer term exists).
func (c *cluster) campaign(id NodeID) {
	c.t.Helper()
	n := c.nodes[id]
	for tries := 0; tries < 5 && n.role != Leader; tries++ {
		start := n.term
		for ticks := 0; n.term == start; ticks++ { // until it starts a new election
			if ticks > 10*testElection {
				c.t.Fatalf("node %d never started an election", id)
			}
			n.Tick()
		}
		c.settle()
	}
	if n.role != Leader {
		c.t.Fatalf("node %d could not win an election: %+v", id, n.Status())
	}
}
