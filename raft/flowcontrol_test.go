package raft

import "testing"

// leaderOf3 elects node 1 of a settled 3-node cluster and returns the
// cluster with the leader's output cleared.
func leaderOf3(t *testing.T) (*cluster, *Node) {
	t.Helper()
	c := newCluster(t, 3)
	c.elect(1)
	c.heartbeat(1) // commit the no-op everywhere
	l := c.nodes[1]
	drain(l)
	return c, l
}

func appendsTo(msgs []Message, to NodeID) []Message {
	var out []Message
	for _, m := range msgs {
		if m.To == to && m.Type == MsgApp {
			out = append(out, m)
		}
	}
	return out
}

func mustPropose(t *testing.T, n *Node, data string) {
	t.Helper()
	if _, err := n.Propose([]byte(data)); err != nil {
		t.Fatal(err)
	}
}

// While a batch is outstanding, new proposals send nothing; when it is
// acknowledged, one AppendEntries carries everything that piled up.
func TestOneBatchOutstanding(t *testing.T) {
	c, l := leaderOf3(t)
	mustPropose(t, l, "a")
	first := appendsTo(drain(l), 2)
	if len(first) != 1 || len(first[0].Entries) != 1 {
		t.Fatalf("first proposal sent %+v to node 2, want one AppendEntries with one entry", first)
	}
	for _, d := range []string{"b", "c", "d"} {
		mustPropose(t, l, d)
	}
	if got := appendsTo(drain(l), 2); len(got) != 0 {
		t.Fatalf("with a batch outstanding, proposals sent %d more AppendEntries to node 2", len(got))
	}

	// Node 2 receives the first batch and acknowledges it.
	c.nodes[2].Step(first[0])
	for _, resp := range drain(c.nodes[2]) {
		l.Step(resp)
	}
	next := appendsTo(drain(l), 2)
	if len(next) != 1 || len(next[0].Entries) != 3 {
		t.Fatalf("after the acknowledgement: %+v, want one AppendEntries carrying the 3 waiting entries", next)
	}
}

// A lost batch is resent by the next heartbeat.
func TestHeartbeatResendsLostBatch(t *testing.T) {
	_, l := leaderOf3(t)
	mustPropose(t, l, "a")
	drain(l) // lost
	tickN(l, testHeartbeat)
	got := appendsTo(drain(l), 3)
	if len(got) != 1 || len(got[0].Entries) != 1 || string(got[0].Entries[0].Data) != "a" {
		t.Fatalf("heartbeat sent %+v to node 3, want the lost entry again", got)
	}
}

// A read sends a check with no entries, even with entries outstanding; its
// acknowledgement confirms the read but doesn't settle the batch.
func TestReadCheckCarriesNoEntries(t *testing.T) {
	c, l := leaderOf3(t)
	mustPropose(t, l, "a")
	batch := appendsTo(drain(l), 2)[0]
	if err := l.ReadIndex(7); err != nil {
		t.Fatal(err)
	}
	checks := drain(l)
	if len(checks) != 2 {
		t.Fatalf("read sent %d messages, want a check to each follower", len(checks))
	}
	for _, m := range checks {
		if m.Type != MsgApp || len(m.Entries) != 0 || m.Context == 0 {
			t.Fatalf("read check %+v: want an AppendEntries with no entries and the read's context", m)
		}
	}

	// Node 2 answers the check (not the batch): the read is confirmed with
	// the leader's own acknowledgement, and the batch is still outstanding.
	c.nodes[2].Step(appendsTo(checks, 2)[0])
	for _, resp := range drain(c.nodes[2]) {
		l.Step(resp)
	}
	rd := l.Ready()
	if len(rd.ReadStates) != 1 || rd.ReadStates[0].ID != 7 {
		t.Fatalf("read states %+v, want read 7 confirmed", rd.ReadStates)
	}
	l.Advance()
	mustPropose(t, l, "b")
	if got := appendsTo(drain(l), 2); len(got) != 0 {
		t.Fatalf("a read check's acknowledgement settled the batch: proposal sent %+v", got)
	}

	// The batch's own acknowledgement settles it, and the waiting entry goes.
	c.nodes[2].Step(batch)
	for _, resp := range drain(c.nodes[2]) {
		l.Step(resp)
	}
	if got := appendsTo(drain(l), 2); len(got) != 1 || len(got[0].Entries) != 1 {
		t.Fatalf("after the batch's acknowledgement: %+v, want the waiting entry", got)
	}
}

// A rejection settles the batch and the leader retries at once, from the
// follower's hint but never below what the follower already acknowledged
// (index 1, the no-op).
func TestRejectionSettlesBatch(t *testing.T) {
	_, l := leaderOf3(t)
	mustPropose(t, l, "a")
	drain(l)
	l.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: l.term, Success: false, ConflictIndex: 1})
	got := appendsTo(drain(l), 2)
	if len(got) != 1 || got[0].PrevLogIndex != 1 || len(got[0].Entries) != 1 || got[0].Entries[0].Index != 2 {
		t.Fatalf("after a rejection: %+v, want an immediate retry of entry 2", got)
	}
}

// End to end: many proposals in a row reach every node, and settling
// needs only a few messages rather than one per entry per follower.
func TestBatchingEndToEnd(t *testing.T) {
	c, l := leaderOf3(t)
	sent := 0
	for i := range 50 {
		mustPropose(t, l, string(rune('a'+i%26)))
		// Nothing is delivered yet: everything piles up behind batch 1.
		sent += len(appendsTo(drain(l), 2))
	}
	if sent != 1 {
		t.Fatalf("50 proposals sent %d AppendEntries to node 2 before any acknowledgement, want 1", sent)
	}
	c.heartbeat(1)
	c.heartbeat(1)
	for _, id := range c.ids {
		if got := c.nodes[id].Status().Commit; got != l.Status().LastIndex {
			t.Fatalf("node %d committed %d, want %d", id, got, l.Status().LastIndex)
		}
	}
}
