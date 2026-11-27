package raft

import (
	"errors"
	"fmt"
	"testing"
)

// cluster is a tiny synchronous test network: messages are delivered in
// FIFO order until none are left. Tests drive elections by ticking one node.
type cluster struct {
	t       testing.TB
	ids     []NodeID
	nodes   map[NodeID]*Node
	applied map[NodeID][]Entry
	reads   map[NodeID][]ReadState
	snaps   map[NodeID][]Snapshot
	// drop, if set, decides which messages are lost.
	drop func(Message) bool
	// preVote is passed to nodes built by restart.
	preVote bool
}

func newCluster(t testing.TB, size int) *cluster {
	t.Helper()
	c := &cluster{t: t, nodes: map[NodeID]*Node{}, applied: map[NodeID][]Entry{}, reads: map[NodeID][]ReadState{}, snaps: map[NodeID][]Snapshot{}}
	for i := 1; i <= size; i++ {
		c.ids = append(c.ids, NodeID(i))
	}
	for _, id := range c.ids {
		c.restart(id, HardState{}, nil)
	}
	return c
}

// restart replaces a node with a fresh one built from the given state.
func (c *cluster) restart(id NodeID, hs HardState, entries []Entry) {
	var peers []NodeID
	for _, p := range c.ids {
		if p != id {
			peers = append(peers, p)
		}
	}
	n, err := NewNode(Config{
		ID: id, Peers: peers, ElectionTicks: testElection, HeartbeatTicks: testHeartbeat,
		Rand: fixedRand(0), HardState: hs, Entries: entries, PreVote: c.preVote,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.nodes[id] = n
}

// settle delivers messages until the network is quiet.
func (c *cluster) settle() {
	var queue []Message
	collect := func() {
		for _, id := range c.ids {
			rd := c.nodes[id].Ready()
			c.applied[id] = append(c.applied[id], rd.CommittedEntries...)
			c.reads[id] = append(c.reads[id], rd.ReadStates...)
			if rd.Snapshot != nil {
				c.snaps[id] = append(c.snaps[id], *rd.Snapshot)
			}
			queue = append(queue, rd.Messages...)
			c.nodes[id].Advance()
		}
	}
	collect()
	for steps := 0; len(queue) > 0; steps++ {
		if steps > 10_000 {
			c.t.Fatal("network did not settle")
		}
		m := queue[0]
		queue = queue[1:]
		if c.drop == nil || !c.drop(m) {
			c.nodes[m.To].Step(m)
		}
		collect()
	}
}

// elect makes id the leader by running out its election timer.
func (c *cluster) elect(id NodeID) {
	c.t.Helper()
	n := c.nodes[id]
	for n.role != Candidate && n.role != PreCandidate {
		n.Tick()
	}
	c.settle()
	if n.role != Leader {
		c.t.Fatalf("node %d did not win the election: %+v", id, n.Status())
	}
}

// heartbeat makes the leader send one round of AppendEntries.
func (c *cluster) heartbeat(id NodeID) {
	c.nodes[id].Tick()
	c.settle()
}

func (c *cluster) propose(id NodeID, data string) {
	c.t.Helper()
	if _, err := c.nodes[id].Propose([]byte(data)); err != nil {
		c.t.Fatal(err)
	}
	c.settle()
}

func terms(es []Entry) []Term {
	out := make([]Term, len(es))
	for i, e := range es {
		out[i] = e.Term
	}
	return out
}

func TestReplicateAndCommit(t *testing.T) {
	c := newCluster(t, 3)
	c.elect(1)
	c.propose(1, "a")
	c.propose(1, "b")
	c.heartbeat(1) // tell followers the new commit index

	for _, id := range c.ids {
		st := c.nodes[id].Status()
		// Entry 1 is the leader's no-op, then "a" and "b".
		if st.LastIndex != 3 || st.Commit != 3 {
			t.Errorf("node %d: last %d commit %d, want 3 and 3", id, st.LastIndex, st.Commit)
		}
		got := c.applied[id]
		if len(got) != 3 || got[0].Data != nil || string(got[1].Data) != "a" || string(got[2].Data) != "b" {
			t.Errorf("node %d applied %+v, want no-op, a, b", id, got)
		}
	}
}

func TestProposeOnFollowerFails(t *testing.T) {
	c := newCluster(t, 3)
	c.elect(1)
	if _, err := c.nodes[2].Propose([]byte("x")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("err = %v, want ErrNotLeader", err)
	}
}

// Ready.Entries hands out new entries exactly once, and only after
// recovery does not re-persist entries that came from storage.
func TestReadyEntries(t *testing.T) {
	n := newTestNode(t, 1, nil, HardState{}) // one-node cluster
	tickN(n, testElection)                   // elects itself, appends no-op
	if _, err := n.Propose([]byte("x")); err != nil {
		t.Fatal(err)
	}
	rd := n.Ready()
	if len(rd.Entries) != 2 || len(rd.CommittedEntries) != 2 {
		t.Fatalf("Entries %d CommittedEntries %d, want 2 and 2", len(rd.Entries), len(rd.CommittedEntries))
	}
	n.Advance()
	if rd := n.Ready(); len(rd.Entries) != 0 || len(rd.CommittedEntries) != 0 {
		t.Fatal("entries handed out twice")
	}

	recovered := newTestNodeWith(t, Config{Entries: []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}})
	if rd := recovered.Ready(); len(rd.Entries) != 0 {
		t.Fatalf("recovered node asked to persist %d entries it loaded from storage", len(rd.Entries))
	}
}

func newTestNodeWith(t *testing.T, cfg Config) *Node {
	t.Helper()
	cfg.ID, cfg.Peers = 1, []NodeID{2, 3}
	cfg.ElectionTicks, cfg.HeartbeatTicks, cfg.Rand = testElection, testHeartbeat, fixedRand(0)
	n, err := NewNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// follower builds node 1 in term `term`, following leader 2, with the given
// log, and returns it with its pending output cleared.
func follower(t *testing.T, term Term, logTerms ...Term) *Node {
	t.Helper()
	var es []Entry
	for i, lt := range logTerms {
		es = append(es, Entry{Index: uint64(i + 1), Term: lt, Data: []byte(fmt.Sprint(i + 1))})
	}
	return newTestNodeWith(t, Config{HardState: HardState{Term: term}, Entries: es})
}

func appendMsg(term Term, prevIndex uint64, prevTerm Term, commit uint64, entryTerms ...Term) Message {
	m := Message{Type: MsgApp, From: 2, To: 1, Term: term, PrevLogIndex: prevIndex, PrevLogTerm: prevTerm, LeaderCommit: commit}
	for i, et := range entryTerms {
		m.Entries = append(m.Entries, Entry{Index: prevIndex + uint64(i) + 1, Term: et})
	}
	return m
}

// Figure 2, AppendEntries receiver rules 2-5.
func TestHandleAppendEntries(t *testing.T) {
	cases := []struct {
		name      string
		log       []Term
		msg       Message
		wantOK    bool
		wantLog   []Term
		wantHint  [2]uint64 // ConflictTerm, ConflictIndex on failure
		wantMatch uint64    // MatchIndex on success
	}{
		{
			name: "rule 2: log too short", log: []Term{1, 1},
			msg: appendMsg(3, 4, 3, 0, 3), wantOK: false, wantLog: []Term{1, 1},
			wantHint: [2]uint64{0, 3},
		},
		{
			name: "rule 2: term mismatch at prevLogIndex, hint is start of that term", log: []Term{1, 2, 2, 2},
			msg: appendMsg(3, 4, 3, 0, 3), wantOK: false, wantLog: []Term{1, 2, 2, 2},
			wantHint: [2]uint64{2, 2},
		},
		{
			name: "rule 4: append to an empty log", log: nil,
			msg: appendMsg(1, 0, 0, 0, 1, 1), wantOK: true, wantLog: []Term{1, 1}, wantMatch: 2,
		},
		{
			name: "rule 3: conflicting entry and everything after it are replaced", log: []Term{1, 1, 2, 2},
			msg: appendMsg(3, 2, 1, 0, 3), wantOK: true, wantLog: []Term{1, 1, 3}, wantMatch: 3,
		},
		{
			name: "rule 4: a delayed prefix must not truncate newer matching entries", log: []Term{1, 1, 1, 1},
			msg: appendMsg(1, 1, 1, 0, 1), wantOK: true, wantLog: []Term{1, 1, 1, 1}, wantMatch: 2,
		},
		{
			name: "heartbeat with no entries", log: []Term{1, 1},
			msg: appendMsg(1, 2, 1, 0), wantOK: true, wantLog: []Term{1, 1}, wantMatch: 2,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n := follower(t, c.msg.Term, c.log...)
			n.Step(c.msg)
			msgs := drain(n)
			if len(msgs) != 1 || msgs[0].Type != MsgAppResp {
				t.Fatalf("replies %+v, want one AppendEntriesResponse", msgs)
			}
			r := msgs[0]
			if r.Success != c.wantOK {
				t.Fatalf("Success = %v, want %v", r.Success, c.wantOK)
			}
			if c.wantOK && r.MatchIndex != c.wantMatch {
				t.Errorf("MatchIndex = %d, want %d", r.MatchIndex, c.wantMatch)
			}
			if !c.wantOK && (uint64(r.ConflictTerm) != c.wantHint[0] || r.ConflictIndex != c.wantHint[1]) {
				t.Errorf("hint = term %d index %d, want term %d index %d", r.ConflictTerm, r.ConflictIndex, c.wantHint[0], c.wantHint[1])
			}
			if got := terms(n.Entries()); fmt.Sprint(got) != fmt.Sprint(c.wantLog) {
				t.Errorf("log terms = %v, want %v", got, c.wantLog)
			}
		})
	}
}

// After a truncation, Ready.Entries starts at the truncation point so the
// caller's storage replaces the old tail.
func TestTruncationIsPersisted(t *testing.T) {
	n := follower(t, 3, 1, 1, 2, 2)
	n.Step(appendMsg(3, 2, 1, 0, 3))
	rd := n.Ready()
	if len(rd.Entries) != 1 || rd.Entries[0].Index != 3 || rd.Entries[0].Term != 3 {
		t.Fatalf("Ready.Entries = %+v, want just the new entry 3 (term 3)", rd.Entries)
	}
}

// Rule 5: commit follows the leader, but only as far as entries we know
// match, and never backwards.
func TestFollowerCommitIndex(t *testing.T) {
	n := follower(t, 1, 1, 1, 1, 1)
	n.Step(appendMsg(1, 1, 1, 9)) // leader has committed 9, but we only know 1 matches
	if c := n.Status().Commit; c != 1 {
		t.Fatalf("commit = %d, want 1 (limited to the last entry known to match)", c)
	}
	n.Step(appendMsg(1, 4, 1, 3))
	if c := n.Status().Commit; c != 3 {
		t.Fatalf("commit = %d, want 3", c)
	}
	n.Step(appendMsg(1, 1, 1, 9)) // delayed old message
	if c := n.Status().Commit; c != 3 {
		t.Fatalf("commit went back to %d", c)
	}
}

// A follower with a long divergent log is repaired, and fast backtracking
// gets there in a few round trips instead of one per entry.
func TestBacktrackingRepairsDivergentLog(t *testing.T) {
	c := newCluster(t, 3)
	// Node 3 has 30 entries from an old term 1 that nobody else has.
	var stale []Entry
	for i := 1; i <= 30; i++ {
		stale = append(stale, Entry{Index: uint64(i), Term: 1})
	}
	c.restart(3, HardState{Term: 1}, stale)
	// Nodes 1 and 2 agree on 3 entries from term 2 (as if 3 had been away).
	shared := []Entry{{Index: 1, Term: 2}, {Index: 2, Term: 2}, {Index: 3, Term: 2}}
	c.restart(1, HardState{Term: 2}, shared)
	c.restart(2, HardState{Term: 2}, shared)

	rejections := 0
	c.drop = func(m Message) bool {
		if m.Type == MsgAppResp && m.From == 3 && !m.Success {
			rejections++
		}
		return false
	}
	// Node 3's log is longer but from an older term, so node 1 (later last
	// term) still wins the election (§5.4.1).
	c.elect(1)
	c.propose(1, "x")
	c.heartbeat(1)

	want := terms(c.nodes[1].Entries())
	if got := terms(c.nodes[3].Entries()); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("node 3 log terms %v, want the leader's %v", got, want)
	}
	if rejections > 2 {
		t.Fatalf("took %d rejected round trips to repair 30 stale entries; fast backtracking should need 1-2", rejections)
	}
}

// §5.4.1: a node votes only for candidates whose log is at least as
// up-to-date as its own.
func TestElectionRestriction(t *testing.T) {
	c := newCluster(t, 3)
	c.restart(1, HardState{Term: 2}, []Entry{{Index: 1, Term: 1}})                      // behind
	c.restart(2, HardState{Term: 2}, []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}) // ahead
	c.restart(3, HardState{Term: 2}, []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}) // ahead
	n := c.nodes[1]
	for n.role != Candidate {
		n.Tick()
	}
	c.settle()
	if n.role == Leader {
		t.Fatal("a node missing entry 2 was elected")
	}
}

// §5.4.2: a leader does not commit an entry from an earlier term just
// because a majority has it. TestingKnobs.CommitPreviousTermsByCounting
// removes the rule.
func TestCurrentTermCommitRule(t *testing.T) {
	for _, unsafe := range []bool{false, true} {
		t.Run(fmt.Sprintf("unsafe=%v", unsafe), func(t *testing.T) {
			// Node 1 has an entry from term 1 that the others don't have.
			n, err := NewNode(Config{
				ID: 1, Peers: []NodeID{2, 3}, ElectionTicks: testElection, HeartbeatTicks: testHeartbeat,
				Rand: fixedRand(0), HardState: HardState{Term: 1}, Entries: []Entry{{Index: 1, Term: 1}},
				Knobs: TestingKnobs{CommitPreviousTermsByCounting: unsafe, NoNoopOnElection: true},
			})
			if err != nil {
				t.Fatal(err)
			}
			tickN(n, testElection) // candidate in term 2
			n.Step(Message{Type: MsgVoteResp, From: 2, Term: 2, VoteGranted: true})
			drain(n)
			// Node 2 now stores entry 1 (term 1): it is on a majority.
			n.Step(Message{Type: MsgAppResp, From: 2, Term: 2, Success: true, MatchIndex: 1})
			committed := n.Status().Commit == 1
			if committed != unsafe {
				t.Fatalf("entry from term 1 on a majority: committed = %v, want %v", committed, unsafe)
			}
		})
	}
}
