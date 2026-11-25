package raft

// broadcastHeartbeat sends an empty AppendEntries to every peer. In M2 that
// is all AppendEntries does: it tells followers the leader is alive, which
// resets their election timers. Log entries arrive in M3.
func (n *Node) broadcastHeartbeat() {
	for _, p := range n.peers {
		n.send(Message{Type: MsgApp, To: p})
	}
}

// handleAppendEntries handles a heartbeat from a leader.
func (n *Node) handleAppendEntries(m Message) {
	// Figure 2, AppendEntries receiver rule 1: "Reply false if term <
	// currentTerm." This is an old leader; the reply's term tells it to step
	// down.
	if m.Term < n.term {
		n.send(Message{Type: MsgAppResp, To: m.From, Success: false})
		return
	}
	// m.Term == n.term: m.From is the leader of our term. A candidate that
	// hears from it has lost this election and steps down (Figure 2,
	// Candidates). Hearing from the leader resets the election timer.
	n.becomeFollower(m.Term, m.From)
	n.send(Message{Type: MsgAppResp, To: m.From, Success: true})
}
