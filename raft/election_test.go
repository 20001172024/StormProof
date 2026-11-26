package raft

import "testing"

// fixedRand always returns the same number, so election timeouts in tests
// are exactly ElectionTicks + v.
type fixedRand int

func (r fixedRand) Intn(n int) int { return int(r) % n }

const (
	testElection  = 10
	testHeartbeat = 1
)

func newTestNode(t *testing.T, id NodeID, peers []NodeID, hs HardState) *Node {
	t.Helper()
	n, err := NewNode(Config{
		ID: id, Peers: peers,
		ElectionTicks: testElection, HeartbeatTicks: testHeartbeat,
		Rand: fixedRand(0), HardState: hs,
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// drain returns the node's pending messages and acknowledges them.
func drain(n *Node) []Message {
	rd := n.Ready()
	n.Advance()
	return rd.Messages
}

func tickN(n *Node, ticks int) {
	for i := 0; i < ticks; i++ {
		n.Tick()
	}
}

// Figure 2, RequestVote receiver rules. Each case starts a follower
// (node 1 of {1,2,3}) in some state and delivers one RequestVote.
func TestHandleRequestVote(t *testing.T) {
	cases := []struct {
		name     string
		start    HardState
		req      Message
		wantVote bool
		wantHard HardState // state after handling
	}{
		{
			name:     "rule 1: candidate's term is older, reject",
			start:    HardState{Term: 5},
			req:      Message{Type: MsgVote, From: 2, Term: 4},
			wantVote: false,
			wantHard: HardState{Term: 5},
		},
		{
			name:     "rule 2: no vote yet this term, grant",
			start:    HardState{Term: 5},
			req:      Message{Type: MsgVote, From: 2, Term: 5},
			wantVote: true,
			wantHard: HardState{Term: 5, Vote: 2},
		},
		{
			name:     "rule 2: already voted for someone else this term, reject",
			start:    HardState{Term: 5, Vote: 3},
			req:      Message{Type: MsgVote, From: 2, Term: 5},
			wantVote: false,
			wantHard: HardState{Term: 5, Vote: 3},
		},
		{
			name:     "rule 2: already voted for this candidate, grant again (retry)",
			start:    HardState{Term: 5, Vote: 2},
			req:      Message{Type: MsgVote, From: 2, Term: 5},
			wantVote: true,
			wantHard: HardState{Term: 5, Vote: 2},
		},
		{
			name:     "all servers: newer term resets the vote, then grant",
			start:    HardState{Term: 5, Vote: 3},
			req:      Message{Type: MsgVote, From: 2, Term: 6},
			wantVote: true,
			wantHard: HardState{Term: 6, Vote: 2},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n := newTestNode(t, 1, []NodeID{2, 3}, c.start)
			n.Step(c.req)
			msgs := drain(n)
			if len(msgs) != 1 || msgs[0].Type != MsgVoteResp || msgs[0].To != c.req.From {
				t.Fatalf("got %+v, want one RequestVoteResponse to %d", msgs, c.req.From)
			}
			resp := msgs[0]
			if resp.VoteGranted != c.wantVote {
				t.Errorf("VoteGranted = %v, want %v", resp.VoteGranted, c.wantVote)
			}
			if resp.Term != c.wantHard.Term {
				t.Errorf("reply term = %d, want %d (our current term)", resp.Term, c.wantHard.Term)
			}
			if got := (HardState{Term: n.term, Vote: n.vote}); got != c.wantHard {
				t.Errorf("state = %+v, want %+v", got, c.wantHard)
			}
		})
	}
}

// A candidate or leader has voted for itself, so it rejects other
// candidates of the same term.
func TestCandidateAndLeaderRejectOthersInSameTerm(t *testing.T) {
	n := newTestNode(t, 1, []NodeID{2, 3}, HardState{})
	tickN(n, testElection) // becomes candidate in term 1
	drain(n)
	n.Step(Message{Type: MsgVote, From: 2, Term: 1})
	if msgs := drain(n); msgs[0].VoteGranted {
		t.Fatal("candidate voted for another candidate in its own term")
	}

	n.Step(Message{Type: MsgVoteResp, From: 3, Term: 1, VoteGranted: true}) // wins
	drain(n)
	if n.role != Leader {
		t.Fatalf("role = %v, want leader", n.role)
	}
	n.Step(Message{Type: MsgVote, From: 2, Term: 1})
	if msgs := drain(n); msgs[0].VoteGranted {
		t.Fatal("leader voted for another candidate in its own term")
	}
}

func TestLogUpToDate(t *testing.T) {
	cases := []struct {
		candTerm  Term
		candIndex uint64
		ourTerm   Term
		ourIndex  uint64
		want      bool
	}{
		{3, 5, 2, 9, true},  // later last term wins, even with a shorter log
		{2, 9, 3, 5, false}, // earlier last term loses, even with a longer log
		{3, 5, 3, 5, true},  // identical: "at least as" up-to-date
		{3, 6, 3, 5, true},  // same term, longer wins
		{3, 4, 3, 5, false}, // same term, shorter loses
		{0, 0, 0, 0, true},  // both empty
	}
	for _, c := range cases {
		if got := logUpToDate(c.candTerm, c.candIndex, c.ourTerm, c.ourIndex); got != c.want {
			t.Errorf("logUpToDate(cand term %d idx %d, ours term %d idx %d) = %v, want %v",
				c.candTerm, c.candIndex, c.ourTerm, c.ourIndex, got, c.want)
		}
	}
}

// Figure 2, Followers: timeout → candidate; Candidates: increment term,
// vote for self, send RequestVote to all.
func TestElectionTimeoutStartsCampaign(t *testing.T) {
	n := newTestNode(t, 1, []NodeID{3, 2}, HardState{Term: 4})
	tickN(n, testElection-1)
	if n.role != Follower || len(drain(n)) != 0 {
		t.Fatal("campaigned before the election timeout")
	}
	n.Tick()
	if n.role != Candidate || n.term != 5 || n.vote != 1 {
		t.Fatalf("status %+v, want candidate in term 5 voting for itself", n.Status())
	}
	rd := n.Ready()
	if rd.HardState == nil || *rd.HardState != (HardState{Term: 5, Vote: 1}) {
		t.Fatalf("Ready.HardState = %v, want the new term and self-vote to persist", rd.HardState)
	}
	if len(rd.Messages) != 2 || rd.Messages[0].To != 2 || rd.Messages[1].To != 3 {
		t.Fatalf("messages %+v, want RequestVote to 2 then 3 (sorted)", rd.Messages)
	}
	for _, m := range rd.Messages {
		if m.Type != MsgVote || m.Term != 5 {
			t.Fatalf("message %+v, want RequestVote in term 5", m)
		}
	}
}

func TestCandidateWinsWithMajority(t *testing.T) {
	// 5 nodes: quorum is 3, self-vote plus two more.
	n := newTestNode(t, 1, []NodeID{2, 3, 4, 5}, HardState{})
	tickN(n, testElection)
	drain(n)

	n.Step(Message{Type: MsgVoteResp, From: 2, Term: 1, VoteGranted: true})
	n.Step(Message{Type: MsgVoteResp, From: 2, Term: 1, VoteGranted: true}) // duplicate
	n.Step(Message{Type: MsgVoteResp, From: 3, Term: 1, VoteGranted: false})
	if n.role != Candidate {
		t.Fatal("won with only 2 distinct votes out of 5")
	}
	n.Step(Message{Type: MsgVoteResp, From: 4, Term: 1, VoteGranted: true})
	if n.role != Leader || n.leader != 1 {
		t.Fatalf("status %+v, want leader after 3 votes", n.Status())
	}
	msgs := drain(n)
	if len(msgs) != 4 {
		t.Fatalf("new leader sent %d messages, want an immediate heartbeat to each of 4 peers", len(msgs))
	}
	for _, m := range msgs {
		if m.Type != MsgApp || m.Term != 1 {
			t.Fatalf("message %+v, want AppendEntries heartbeat in term 1", m)
		}
	}
}

// A vote granted for an earlier election must not count in a later one.
func TestStaleVoteResponseIgnored(t *testing.T) {
	n := newTestNode(t, 1, []NodeID{2, 3}, HardState{})
	tickN(n, testElection) // term 1
	tickN(n, testElection) // split vote: new election, term 2
	drain(n)
	n.Step(Message{Type: MsgVoteResp, From: 2, Term: 1, VoteGranted: true})
	if n.role != Candidate || n.term != 2 {
		t.Fatalf("status %+v: a term-1 vote counted in term 2", n.Status())
	}
}

// Figure 2, All Servers: any message with a higher term makes even a leader
// step down. RequestVote is the exception (§4.2.3, see TestVoteStickiness).
func TestHigherTermStepsDown(t *testing.T) {
	for _, typ := range []MessageType{MsgVoteResp, MsgApp, MsgAppResp} {
		n := newTestNode(t, 1, []NodeID{2, 3}, HardState{})
		tickN(n, testElection)
		n.Step(Message{Type: MsgVoteResp, From: 2, Term: 1, VoteGranted: true})
		drain(n)
		if n.role != Leader {
			t.Fatal("setup: not leader")
		}
		n.Step(Message{Type: typ, From: 3, Term: 7})
		if n.role != Follower || n.term != 7 {
			t.Errorf("%v with higher term: status %+v, want follower in term 7", typ, n.Status())
		}
	}
}

// Figure 2, Candidates: an AppendEntries from the new leader of the same
// term means we lost; step down.
func TestCandidateStepsDownForLeaderOfSameTerm(t *testing.T) {
	n := newTestNode(t, 1, []NodeID{2, 3}, HardState{})
	tickN(n, testElection)
	drain(n)
	n.Step(Message{Type: MsgApp, From: 2, Term: 1})
	if n.role != Follower || n.leader != 2 || n.vote != 1 {
		t.Fatalf("status %+v, want follower of leader 2, keeping its own vote", n.Status())
	}
	if msgs := drain(n); len(msgs) != 1 || !msgs[0].Success {
		t.Fatalf("reply %+v, want a successful AppendEntriesResponse", msgs)
	}
}

func TestStaleAppendEntriesRejected(t *testing.T) {
	n := newTestNode(t, 1, []NodeID{2, 3}, HardState{Term: 5})
	n.Step(Message{Type: MsgApp, From: 2, Term: 3})
	msgs := drain(n)
	if len(msgs) != 1 || msgs[0].Success || msgs[0].Term != 5 {
		t.Fatalf("reply %+v, want failure carrying term 5", msgs)
	}
	if n.leader != None {
		t.Fatal("accepted a stale leader")
	}
}

// Heartbeats from the leader keep a follower from starting an election.
func TestHeartbeatsPreventElection(t *testing.T) {
	n := newTestNode(t, 1, []NodeID{2, 3}, HardState{Term: 1})
	for i := 0; i < 5*testElection; i++ {
		n.Tick()
		if i%3 == 0 {
			n.Step(Message{Type: MsgApp, From: 2, Term: 1})
		}
	}
	if n.role != Follower || n.term != 1 {
		t.Fatalf("status %+v: started an election despite heartbeats", n.Status())
	}
}

func TestLeaderSendsHeartbeatsEveryInterval(t *testing.T) {
	n := newTestNode(t, 1, []NodeID{2, 3}, HardState{})
	tickN(n, testElection)
	n.Step(Message{Type: MsgVoteResp, From: 2, Term: 1, VoteGranted: true})
	drain(n)
	for i := 0; i < 3; i++ {
		n.Tick()
		if msgs := drain(n); len(msgs) != 2 || msgs[0].Type != MsgApp {
			t.Fatalf("tick %d: got %+v, want a heartbeat to each peer", i, msgs)
		}
	}
}

func TestSingleNodeElectsItself(t *testing.T) {
	n := newTestNode(t, 1, nil, HardState{})
	tickN(n, testElection)
	if n.role != Leader {
		t.Fatalf("role = %v, want a one-node cluster to elect itself", n.role)
	}
}

// Ready reports HardState only when it changed, so the caller doesn't fsync
// on every tick.
func TestReadyHardStateOnlyWhenChanged(t *testing.T) {
	n := newTestNode(t, 1, []NodeID{2, 3}, HardState{Term: 2, Vote: 3})
	if rd := n.Ready(); rd.HardState != nil {
		t.Fatalf("fresh node from recovered state reported HardState %+v", *rd.HardState)
	}
	n.Advance()
	n.Step(Message{Type: MsgApp, From: 3, Term: 2}) // same term: no change
	if rd := n.Ready(); rd.HardState != nil {
		t.Fatal("heartbeat in the same term reported a HardState change")
	}
	n.Advance()
	n.Step(Message{Type: MsgApp, From: 3, Term: 3})
	rd := n.Ready()
	if rd.HardState == nil || *rd.HardState != (HardState{Term: 3}) {
		t.Fatalf("HardState = %v, want term 3 with no vote", rd.HardState)
	}
	// Without Advance, the same change is reported again: the caller has not
	// confirmed it was saved.
	if again := n.Ready(); again.HardState == nil {
		t.Fatal("HardState change lost before Advance")
	}
	n.Advance()
	if rd := n.Ready(); rd.HardState != nil || len(rd.Messages) != 0 {
		t.Fatal("Advance did not clear the Ready")
	}
}

func TestConfigValidation(t *testing.T) {
	base := Config{ID: 1, Peers: []NodeID{2}, ElectionTicks: 10, HeartbeatTicks: 1, Rand: fixedRand(0)}
	cases := map[string]func(*Config){
		"zero id":             func(c *Config) { c.ID = 0 },
		"self in peers":       func(c *Config) { c.Peers = []NodeID{1, 2} },
		"heartbeat too slow":  func(c *Config) { c.HeartbeatTicks = 10 },
		"no election timeout": func(c *Config) { c.ElectionTicks = 0 },
		"no rand":             func(c *Config) { c.Rand = nil },
	}
	for name, mutate := range cases {
		cfg := base
		mutate(&cfg)
		if _, err := NewNode(cfg); err == nil {
			t.Errorf("%s: NewNode accepted an invalid config", name)
		}
	}
	if _, err := NewNode(base); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}

// §4.2.3: a server that has heard from a current leader within the minimum
// election timeout ignores RequestVote, even from a higher term, so a
// removed server can't depose a healthy leader. Once the timeout has passed
// without hearing from a leader, votes are granted again.
func TestVoteStickiness(t *testing.T) {
	for _, knob := range []bool{false, true} {
		f := newTestNode(t, 1, []NodeID{2, 3}, HardState{Term: 1})
		f.cfg.Knobs.NoVoteStickiness = knob
		f.Step(Message{Type: MsgApp, From: 2, Term: 1}) // hears from leader 2
		drain(f)
		f.Step(Message{Type: MsgVote, From: 3, Term: 5})
		msgs := drain(f)
		ignored := len(msgs) == 0 && f.term == 1
		if ignored == knob {
			t.Fatalf("NoVoteStickiness=%v: vote ignored = %v (messages %+v, term %d)", knob, ignored, msgs, f.term)
		}
	}

	f := newTestNode(t, 1, []NodeID{2, 3}, HardState{Term: 1})
	f.Step(Message{Type: MsgApp, From: 2, Term: 1})
	drain(f)
	tickN(f, testElection-1) // still within the timeout
	f.Step(Message{Type: MsgVote, From: 3, Term: 5})
	if len(drain(f)) != 0 {
		t.Fatal("vote answered within the election timeout")
	}
	f.electionElapsed = testElection // the leader has been silent for a full timeout
	f.Step(Message{Type: MsgVote, From: 3, Term: 5})
	if msgs := drain(f); len(msgs) != 1 || !msgs[0].VoteGranted {
		t.Fatalf("after the timeout: %+v, want the vote granted", msgs)
	}

	// A leader counts itself as a current leader.
	c := newCluster(t, 3)
	c.elect(1)
	c.nodes[1].Step(Message{Type: MsgVote, From: 3, Term: 9})
	if c.nodes[1].role != Leader {
		t.Fatal("a leader was deposed by a RequestVote")
	}
}
