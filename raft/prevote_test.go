package raft

import "testing"

// preVoter builds node 1 of {1,2,3} with pre-vote on, in term 2, with a
// log ending at index 2, term 2.
func preVoter(t *testing.T) *Node {
	t.Helper()
	return newTestNodeWith(t, Config{
		PreVote:   true,
		HardState: HardState{Term: 2, Vote: 2},
		Entries:   []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}},
	})
}

// The pre-vote receiver (dissertation §9.6): grant only for a later term,
// an up-to-date log, and no leader heard from recently; and whatever the
// answer, change nothing.
func TestHandlePreVote(t *testing.T) {
	cases := []struct {
		name       string
		heardFrom  bool // heard from leader 2 just before
		term       Term
		lastIndex  uint64
		lastTerm   Term
		wantGrant  bool
		wantInTerm Term // the response's term
	}{
		{"later term, log as long", false, 3, 2, 2, true, 3},
		{"later term, newer log", false, 3, 1, 3, true, 3},
		{"already voted in our term: irrelevant for a later one", false, 3, 5, 2, true, 3},
		{"not a later term", false, 2, 9, 2, false, 2},
		{"older term", false, 1, 9, 9, false, 2},
		{"log behind: shorter", false, 3, 1, 2, false, 2},
		{"log behind: older last term", false, 9, 9, 1, false, 2},
		{"leader heard from recently", true, 3, 9, 9, false, 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n := preVoter(t)
			if c.heardFrom {
				n.Step(appendMsg(2, 2, 2, 0))
			}
			drain(n)
			before := n.Status()
			elapsed := n.electionElapsed
			n.Step(Message{Type: MsgPreVote, From: 3, To: 1, Term: c.term, LastLogIndex: c.lastIndex, LastLogTerm: c.lastTerm})
			msgs := drain(n)
			if len(msgs) != 1 || msgs[0].Type != MsgPreVoteResp || msgs[0].To != 3 {
				t.Fatalf("got %+v, want one PreVoteResponse to 3", msgs)
			}
			if msgs[0].VoteGranted != c.wantGrant || msgs[0].Term != c.wantInTerm {
				t.Fatalf("granted=%v term=%d, want granted=%v term=%d", msgs[0].VoteGranted, msgs[0].Term, c.wantGrant, c.wantInTerm)
			}
			after := n.Status()
			if after.Term != before.Term || after.Vote != before.Vote || after.Role != before.Role || n.electionElapsed != elapsed {
				t.Fatalf("a pre-vote changed state: %+v -> %+v (elapsed %d -> %d)", before, after, elapsed, n.electionElapsed)
			}
		})
	}
}

// A node whose timer fires asks first, without raising its term, and only
// with a majority of pre-votes becomes a candidate in term+1.
func TestPreCandidate(t *testing.T) {
	n := preVoter(t)
	drain(n)
	tickN(n, testElection)
	if n.role != PreCandidate || n.term != 2 || n.vote != 2 {
		t.Fatalf("after the timeout: role %v term %d vote %d, want pre-candidate, term 2, vote unchanged", n.role, n.term, n.vote)
	}
	rd := n.Ready()
	if rd.HardState != nil {
		t.Fatalf("a pre-vote round asked to persist %+v; it must change no durable state", *rd.HardState)
	}
	msgs := drain(n)
	if len(msgs) != 2 {
		t.Fatalf("sent %d messages, want a PreVote to each of 2 peers", len(msgs))
	}
	for _, m := range msgs {
		if m.Type != MsgPreVote || m.Term != 3 || m.LastLogIndex != 2 || m.LastLogTerm != 2 {
			t.Fatalf("sent %+v, want PreVote for term 3 with last log 2/2", m)
		}
	}
	// A granted response carries term 3, but must not move us to term 3.
	n.Step(Message{Type: MsgPreVoteResp, From: 2, Term: 3, VoteGranted: true})
	if n.role != Candidate || n.term != 3 || n.vote != 1 {
		t.Fatalf("with a majority of pre-votes: role %v term %d vote %d, want candidate in term 3 voting for itself", n.role, n.term, n.vote)
	}
	msgs = drain(n)
	if len(msgs) != 2 || msgs[0].Type != MsgVote || msgs[0].Term != 3 {
		t.Fatalf("sent %+v, want RequestVotes for term 3", msgs)
	}
}

func TestPreVoteResponses(t *testing.T) {
	// A rejection from a later term is real news: become a follower in it.
	n := preVoter(t)
	tickN(n, testElection)
	drain(n)
	n.Step(Message{Type: MsgPreVoteResp, From: 2, Term: 7})
	if n.role != Follower || n.term != 7 {
		t.Fatalf("after a rejection from term 7: role %v term %d", n.role, n.term)
	}

	// A granted response that isn't for our current round counts for
	// nothing and changes nothing.
	n = preVoter(t)
	n.Step(Message{Type: MsgPreVoteResp, From: 2, Term: 3, VoteGranted: true}) // not campaigning
	if n.role != Follower || n.term != 2 {
		t.Fatalf("an unsolicited pre-vote changed role %v term %d", n.role, n.term)
	}
	tickN(n, testElection)
	n.Step(Message{Type: MsgPreVoteResp, From: 2, Term: 5, VoteGranted: true}) // for another term
	if n.role != PreCandidate || n.term != 2 {
		t.Fatalf("a pre-vote for the wrong term counted: role %v term %d", n.role, n.term)
	}
	n.Step(Message{Type: MsgPreVoteResp, From: 3, Term: 2}) // a rejection from our own term
	if n.role != PreCandidate || n.term != 2 {
		t.Fatalf("a rejection from our term changed role %v term %d", n.role, n.term)
	}

	// A leader is a current leader to itself, so it never grants.
	c := newCluster(t, 3)
	c.preVote = true
	for _, id := range c.ids {
		c.restart(id, HardState{}, nil)
	}
	c.elect(1)
	c.nodes[1].Step(Message{Type: MsgPreVote, From: 3, Term: 9, LastLogIndex: 99, LastLogTerm: 99})
	msgs := drain(c.nodes[1])
	if c.nodes[1].role != Leader || len(msgs) != 1 || msgs[0].VoteGranted {
		t.Fatalf("leader answered a pre-vote with %+v (role %v)", msgs, c.nodes[1].role)
	}
}

func TestPreVoteSingleNode(t *testing.T) {
	n, err := NewNode(Config{ID: 1, ElectionTicks: testElection, HeartbeatTicks: testHeartbeat, Rand: fixedRand(0), PreVote: true})
	if err != nil {
		t.Fatal(err)
	}
	tickN(n, testElection)
	if n.role != Leader || n.term != 1 {
		t.Fatalf("single node: role %v term %d, want leader in term 1", n.role, n.term)
	}
}

// The case the simulator found (docs/bugs/005): a server cut off from the
// cluster, whose log falls behind, keeps timing out. Without pre-vote each
// timeout raises its term, and when it can talk again its higher term
// deposes the working leader, though it can never win itself. With
// pre-vote its term never moves, the leader stays, and it catches up.
func TestPreVoteStopsDisruptiveServer(t *testing.T) {
	for _, preVote := range []bool{false, true} {
		c := newCluster(t, 3)
		c.preVote = preVote
		for _, id := range c.ids {
			c.restart(id, HardState{}, nil)
		}
		c.elect(1)
		c.propose(1, "a")

		// Cut node 3 off. The others carry on; node 3's timer keeps firing.
		c.drop = func(m Message) bool { return m.From == 3 || m.To == 3 }
		c.propose(1, "b")
		c.propose(1, "c")
		for range 5 {
			tickN(c.nodes[3], 2*testElection)
			c.settle()
		}
		term3 := c.nodes[3].term

		// Reconnect, and let node 3 run out its timer once more before the
		// leader's next heartbeat reaches it.
		c.drop = nil
		tickN(c.nodes[3], 2*testElection)
		c.settle()
		c.heartbeat(1)

		leader := c.nodes[1]
		if preVote {
			c.propose(1, "d")
			c.heartbeat(1) // followers learn the new commit index from the next AppendEntries
			if term3 != 1 {
				t.Errorf("pre-vote: the cut-off server raised its term to %d", term3)
			}
			if leader.role != Leader || leader.term != 1 {
				t.Errorf("pre-vote: leader deposed (role %v, term %d)", leader.role, leader.term)
			}
			s3, sl := c.nodes[3].Status(), leader.Status()
			if s3.LastIndex != sl.LastIndex || s3.Commit != sl.Commit {
				t.Errorf("pre-vote: server 3 at last %d commit %d, the leader at %d/%d: it did not catch up", s3.LastIndex, s3.Commit, sl.LastIndex, sl.Commit)
			}
		} else {
			if term3 <= 1 {
				t.Fatalf("without pre-vote the cut-off server should raise its term; it is %d", term3)
			}
			if leader.role == Leader && leader.term == 1 {
				t.Errorf("without pre-vote the leader of term 1 should have been deposed")
			}
		}
	}
}
