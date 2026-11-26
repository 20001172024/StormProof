package raft

import (
	"errors"
	"slices"
	"testing"
)

// memberCluster has servers 1..size; the first `voters` of them form the
// initial configuration and the rest are joining (no configuration).
func memberCluster(t *testing.T, size, voters int) *cluster {
	t.Helper()
	c := newCluster(t, size)
	for i := 1; i <= size; i++ {
		id := NodeID(i)
		cfg := Config{ID: id, ElectionTicks: testElection, HeartbeatTicks: testHeartbeat, Rand: fixedRand(0)}
		if i <= voters {
			for j := 1; j <= voters; j++ {
				if j != i {
					cfg.Peers = append(cfg.Peers, NodeID(j))
				}
			}
		} else {
			cfg.Joining = true
		}
		n, err := NewNode(cfg)
		if err != nil {
			t.Fatal(err)
		}
		c.nodes[id] = n
	}
	return c
}

func (c *cluster) changeConf(id NodeID, cc ConfChange) {
	c.t.Helper()
	if _, err := c.nodes[id].ProposeConfChange(cc); err != nil {
		c.t.Fatalf("%v %d: %v", cc.Op, cc.Node, err)
	}
	c.settle()
	c.heartbeat(id) // spread the new commit index
}

func voters(n *Node) []NodeID { return n.Status().Voters }

func TestAddVoter(t *testing.T) {
	c := memberCluster(t, 4, 3)
	c.elect(1)
	c.propose(1, "before")
	if len(voters(c.nodes[4])) != 0 {
		t.Fatal("a joining server should start with no configuration")
	}
	c.changeConf(1, ConfChange{Op: AddVoter, Node: 4})
	for _, id := range c.ids {
		if got := voters(c.nodes[id]); !slices.Equal(got, []NodeID{1, 2, 3, 4}) {
			t.Fatalf("node %d voters %v", id, got)
		}
	}
	// Node 4 caught up from the start of the log, and now counts: with 4
	// voters a write needs 3 copies, so with 2 and 3 cut off it can't commit.
	if c.nodes[4].commitIndex != c.nodes[1].commitIndex {
		t.Fatal("new voter did not catch up")
	}
	c.drop = func(m Message) bool { return m.To == 2 || m.To == 3 }
	c.propose(1, "needs three of four")
	before := c.nodes[1].commitIndex
	if c.nodes[1].log.lastIndex() == before {
		t.Fatal("setup: entry should be uncommitted")
	}
	c.drop = nil
	c.heartbeat(1)
	if c.nodes[1].commitIndex == before {
		t.Fatal("entry did not commit once a majority of the new configuration had it")
	}
}

func TestRemoveFollower(t *testing.T) {
	c := memberCluster(t, 3, 3)
	c.elect(1)
	c.changeConf(1, ConfChange{Op: RemoveVoter, Node: 3})
	if got := voters(c.nodes[1]); !slices.Equal(got, []NodeID{1, 2}) {
		t.Fatalf("voters %v", got)
	}
	// Now 2 of 2 are needed; node 3 no longer counts or receives entries.
	c.drop = func(m Message) bool { return m.To == 2 }
	c.propose(1, "x")
	if c.nodes[1].commitIndex == c.nodes[1].log.lastIndex() {
		t.Fatal("committed without node 2, the only other voter")
	}
	c.drop = nil
	c.heartbeat(1)
	if c.nodes[3].log.lastIndex() == c.nodes[1].log.lastIndex() {
		t.Fatal("the removed server is still being sent entries")
	}
}

// §4.2.2: a leader that removes itself keeps leading until the change
// commits, without counting itself, and then steps down.
func TestRemoveLeader(t *testing.T) {
	c := memberCluster(t, 3, 3)
	c.elect(1)
	leader := c.nodes[1]
	if _, err := leader.ProposeConfChange(ConfChange{Op: RemoveVoter, Node: 1}); err != nil {
		t.Fatal(err)
	}
	if leader.role != Leader {
		t.Fatal("stepped down before the change committed")
	}
	c.settle()
	if leader.role == Leader {
		t.Fatal("still leading after its own removal committed")
	}
	if got := voters(c.nodes[2]); !slices.Equal(got, []NodeID{2, 3}) {
		t.Fatalf("voters %v", got)
	}
	c.campaign(2)
	c.propose(2, "after")
	c.heartbeat(2)
	if c.nodes[3].commitIndex != c.nodes[2].commitIndex || c.nodes[2].commitIndex < c.nodes[2].log.lastIndex() {
		t.Fatal("the remaining two can't make progress")
	}
	// The removed server never campaigns.
	tickN(leader, 5*testElection)
	if leader.role == Candidate {
		t.Fatal("a removed server started an election")
	}
}

func TestOneChangeAtATime(t *testing.T) {
	for _, knob := range []bool{false, true} {
		c := memberCluster(t, 5, 3)
		c.elect(1)
		c.nodes[1].cfg.Knobs.ConcurrentConfChanges = knob
		c.drop = func(m Message) bool { return m.Type == MsgAppResp } // nothing commits
		if _, err := c.nodes[1].ProposeConfChange(ConfChange{Op: AddVoter, Node: 4}); err != nil {
			t.Fatal(err)
		}
		_, err := c.nodes[1].ProposeConfChange(ConfChange{Op: AddVoter, Node: 5})
		if knob != (err == nil) || (!knob && !errors.Is(err, ErrConfChangeInProgress)) {
			t.Fatalf("ConcurrentConfChanges=%v: second change err = %v", knob, err)
		}
	}
}

// The 2015 fix: a new leader must commit an entry in its own term before
// it changes the configuration.
func TestConfChangeWaitsForOwnTermCommit(t *testing.T) {
	c := memberCluster(t, 4, 3)
	n := c.nodes[1]
	for n.role != Candidate {
		n.Tick()
	}
	n.Step(Message{Type: MsgVoteResp, From: 2, Term: n.term, VoteGranted: true}) // leader; no-op uncommitted
	if _, err := n.ProposeConfChange(ConfChange{Op: AddVoter, Node: 4}); !errors.Is(err, ErrConfChangeNotReady) {
		t.Fatalf("err = %v, want ErrConfChangeNotReady", err)
	}
	c.settle()
	if _, err := n.ProposeConfChange(ConfChange{Op: AddVoter, Node: 4}); err != nil {
		t.Fatalf("after the no-op committed: %v", err)
	}
}

func TestConfChangeErrors(t *testing.T) {
	c := memberCluster(t, 3, 3)
	if _, err := c.nodes[1].ProposeConfChange(ConfChange{Op: AddVoter, Node: 4}); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower: %v", err)
	}
	c.elect(1)
	for _, cc := range []ConfChange{{Op: AddVoter, Node: 2}, {Op: RemoveVoter, Node: 9}} {
		if _, err := c.nodes[1].ProposeConfChange(cc); !errors.Is(err, ErrConfChangeNoop) {
			t.Errorf("%v %d: err = %v, want ErrConfChangeNoop", cc.Op, cc.Node, err)
		}
	}
	single := memberCluster(t, 1, 1)
	tickN(single.nodes[1], testElection)
	single.settle()
	if _, err := single.nodes[1].ProposeConfChange(ConfChange{Op: RemoveVoter, Node: 1}); !errors.Is(err, ErrConfChangeInvalid) {
		t.Fatalf("removing the last voter: %v", err)
	}
}

// A configuration takes effect when appended, committed or not, and goes
// back to the previous one if the entry is truncated away (§4.1).
func TestConfigFollowsTheLog(t *testing.T) {
	f := follower(t, 2, 1, 1)
	cc := Entry{Index: 3, Term: 2, Type: EntryConfChange, Data: EncodeVoters([]NodeID{1, 2, 3, 4})}
	f.Step(Message{Type: MsgApp, From: 2, To: 1, Term: 2, PrevLogIndex: 2, PrevLogTerm: 1, Entries: []Entry{cc}})
	if got := voters(f); !slices.Equal(got, []NodeID{1, 2, 3, 4}) {
		t.Fatalf("after appending (uncommitted): %v", got)
	}
	// A new leader overwrites entry 3.
	f.Step(Message{Type: MsgApp, From: 3, To: 1, Term: 3, PrevLogIndex: 2, PrevLogTerm: 1,
		Entries: []Entry{{Index: 3, Term: 3}}})
	if got := voters(f); !slices.Equal(got, []NodeID{1, 2, 3}) {
		t.Fatalf("after truncation: %v, want the old configuration back", got)
	}
}

// Votes from servers outside the configuration don't count.
func TestVotesFromNonVotersDontCount(t *testing.T) {
	n := newTestNode(t, 1, []NodeID{2, 3}, HardState{})
	tickN(n, testElection)
	n.Step(Message{Type: MsgVoteResp, From: 9, Term: n.term, VoteGranted: true})
	if n.role == Leader {
		t.Fatal("won with a vote from a non-voter")
	}
	n.Step(Message{Type: MsgVoteResp, From: 2, Term: n.term, VoteGranted: true})
	if n.role != Leader {
		t.Fatal("did not win with a majority of voters")
	}
}

// The configuration survives compaction and a restart from the snapshot.
func TestConfigInSnapshot(t *testing.T) {
	c := memberCluster(t, 4, 3)
	c.elect(1)
	c.changeConf(1, ConfChange{Op: AddVoter, Node: 4})
	c.propose(1, "x")
	n := c.nodes[1]
	s, err := n.Compact(n.applied, []byte("state"))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(s.Voters, []NodeID{1, 2, 3, 4}) || !slices.Equal(voters(n), s.Voters) {
		t.Fatalf("snapshot voters %v, node voters %v", s.Voters, voters(n))
	}
	r := newTestNodeWith(t, Config{HardState: HardState{Term: n.term}, Snapshot: &s})
	if !slices.Equal(voters(r), []NodeID{1, 2, 3, 4}) {
		t.Fatalf("restored from snapshot: voters %v", voters(r))
	}
}

func TestEncodeVoters(t *testing.T) {
	vs := []NodeID{1, 5, 9}
	got, err := DecodeVoters(EncodeVoters(vs))
	if err != nil || !slices.Equal(got, vs) {
		t.Fatalf("round trip: %v, %v", got, err)
	}
	if _, err := DecodeVoters([]byte{1, 0, 0, 0, 7}); err == nil {
		t.Fatal("decoded a truncated configuration")
	}
}

// crashNode stops a node in a scripted test: it comes back as a follower
// from its persisted state (everything is persisted in this harness), and
// is cut off until revived.
func (c *cluster) crashNode(id NodeID, down map[NodeID]bool) {
	n := c.nodes[id]
	cfg := n.cfg
	cfg.HardState = HardState{Term: n.term, Vote: n.vote}
	cfg.Entries = slices.Clone(n.log.entries)
	cfg.Snapshot = n.snapshot
	r, err := NewNode(cfg)
	if err != nil {
		c.t.Fatal(err)
	}
	c.nodes[id] = r
	down[id] = true
}

// The bug Ongaro found in single-server changes in 2015, scripted. Initial
// configuration C = {1,2,3,4}.
//
//	(a) Leader 1 (term 1) starts adding 5: C1 = {1..5} reaches only node 5.
//	(b) 1 crashes. 2 is elected in term 2 by 2, 3, 4 under C, and, before
//	    committing anything in its own term, starts removing 4:
//	    C2 = {1,2,3}. Its entries reach only 3, which is a majority of C2,
//	    so C2 commits.
//	(c) 2 crashes and 1 comes back. Under C1, 1 needs 3 of 5 votes and gets
//	    them from 1, 4 and 5 (none of them has 2's term-2 entries).
//	(d) 1 overwrites 3's log with its own: the committed C2 is gone.
//
// The fix is the rule that a leader must commit an entry in its own term
// before changing the configuration. Then in (b) 2's no-op would need 3 of
// C's 4 servers, so 4 would have it and refuse to vote for 1 in (c).
func TestOngaro2015Bug(t *testing.T) {
	for _, unsafe := range []bool{false, true} {
		t.Run(map[bool]string{false: "with the fix", true: "without the fix"}[unsafe], func(t *testing.T) {
			c := memberCluster(t, 5, 4)
			for _, n := range c.nodes {
				n.cfg.Knobs.ConfChangeBeforeOwnTermCommit = unsafe
			}
			down := map[NodeID]bool{}
			var block func(Message) bool
			c.drop = func(m Message) bool { return down[m.From] || down[m.To] || (block != nil && block(m)) }

			// (a)
			c.elect(1)
			block = func(m Message) bool { return m.From == 1 && m.To != 5 }
			if _, err := c.nodes[1].ProposeConfChange(ConfChange{Op: AddVoter, Node: 5}); err != nil {
				t.Fatal(err)
			}
			c.settle()
			block = nil

			// (b)
			c.crashNode(1, down)
			// 4 votes for 2 but never receives 2's entries.
			block = func(m Message) bool {
				return (m.From == 2 && m.To == 4 && m.Type != MsgVote) || (m.From == 4 && m.To == 2 && m.Type != MsgVoteResp)
			}
			c.campaign(2)
			idx, err := c.nodes[2].ProposeConfChange(ConfChange{Op: RemoveVoter, Node: 4})
			if !unsafe {
				if !errors.Is(err, ErrConfChangeNotReady) {
					t.Fatalf("err = %v, want ErrConfChangeNotReady", err)
				}
				return // the dangerous change never happens
			}
			if err != nil {
				t.Fatal(err)
			}
			c.settle()
			c.heartbeat(2)
			if c.nodes[2].commitIndex < idx {
				t.Fatal("setup: C2 should have committed with 2 and 3")
			}
			committed := c.nodes[3].log.entries[idx-1]
			block = nil

			// (c)
			c.crashNode(2, down)
			delete(down, 1)
			c.campaign(1)

			// (d)
			c.heartbeat(1)
			if got, ok := c.nodes[3].log.term(idx); ok && got == committed.Term {
				t.Fatalf("node 3 still has the committed C2 at %d", idx)
			}
		})
	}
}

// Two membership changes in flight at once, scripted. Configuration
// {1,2,3}; leader 1 adds 4 and then 5 without waiting, and only 4 and 5 hear
// of it. 1 now uses {1..5}, where 1, 4 and 5 are a majority, while 2 and 3
// still use {1,2,3}, where they are a majority. Both sides commit different
// entries at the same index. With the one-change-at-a-time rule the second
// change is refused.
func TestConcurrentConfChanges(t *testing.T) {
	for _, unsafe := range []bool{false, true} {
		c := memberCluster(t, 5, 3)
		for _, n := range c.nodes {
			n.cfg.Knobs.ConcurrentConfChanges = unsafe
		}
		c.elect(1)
		toNew := func(m Message) bool {
			return (m.From == 2 || m.From == 3 || m.To == 2 || m.To == 3) && (m.From == 1 || m.To == 1)
		}
		c.drop = toNew
		if _, err := c.nodes[1].ProposeConfChange(ConfChange{Op: AddVoter, Node: 4}); err != nil {
			t.Fatal(err)
		}
		_, err := c.nodes[1].ProposeConfChange(ConfChange{Op: AddVoter, Node: 5})
		if !unsafe {
			if !errors.Is(err, ErrConfChangeInProgress) {
				t.Fatalf("err = %v, want ErrConfChangeInProgress", err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		c.settle()
		c.propose(1, "committed by 1, 4, 5")
		idx := c.nodes[1].log.lastIndex()
		if c.nodes[1].commitIndex < idx {
			t.Fatal("setup: 1 should commit with 4 and 5")
		}

		// 2 and 3 never heard of either change, and elect 2.
		oldSide := func(id NodeID) bool { return id == 2 || id == 3 }
		c.drop = func(m Message) bool { return !oldSide(m.From) || !oldSide(m.To) }
		c.campaign(2)
		for c.nodes[2].log.lastIndex() < idx {
			c.propose(2, "committed by 2, 3")
		}
		c.heartbeat(2)
		a, b := c.nodes[1].log.entries[idx-1], c.nodes[2].log.entries[idx-1]
		if c.nodes[2].commitIndex < idx || a.Term == b.Term {
			t.Fatalf("expected two different committed entries at %d: %v and %v (2 committed to %d)", idx, a, b, c.nodes[2].commitIndex)
		}
	}
}
