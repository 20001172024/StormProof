package raft

import "slices"

// broadcastAppend sends AppendEntries to every follower: whatever entries
// each is missing, or an empty heartbeat if it has them all.
func (n *Node) broadcastAppend() {
	for _, p := range n.peers {
		n.sendAppend(p)
	}
}

// sendAppend sends one AppendEntries to a follower, starting at its
// nextIndex. Every AppendEntries also acts as a heartbeat.
//
// Nothing is pipelined: until the follower replies, the next heartbeat
// resends the same entries. Simple and obviously correct; the receiver
// ignores entries it already has (rule 4), so the repeats are harmless.
func (n *Node) sendAppend(to NodeID) {
	next := n.nextIndex[to]
	prev := next - 1
	prevTerm, _ := n.log.term(prev) // prev <= lastIndex, so it exists
	n.send(Message{
		Type:         MsgApp,
		To:           to,
		PrevLogIndex: prev,
		PrevLogTerm:  prevTerm,
		Entries:      n.log.slice(next, next+uint64(n.cfg.MaxEntriesPerMsg)),
		LeaderCommit: n.commitIndex,
	})
}

// handleAppendEntries implements the AppendEntries receiver (Figure 2).
func (n *Node) handleAppendEntries(m Message) {
	// Rule 1: "Reply false if term < currentTerm (§5.1)." The reply's term
	// tells the old leader to step down.
	if m.Term < n.term {
		n.send(Message{Type: MsgAppResp, To: m.From, Success: false})
		return
	}

	// m.Term == n.term (Step has already handled higher terms), so m.From is
	// the leader of our term. A candidate that hears from it has lost this
	// election and steps down (Figure 2, Candidates). Hearing from the leader
	// also resets the election timer.
	n.becomeFollower(m.Term, m.From)

	// Rule 2: "Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)." This consistency check is the
	// induction step behind the Log Matching property: an entry is only
	// accepted if the entry before it already matches the leader's.
	//
	// The rejection includes a hint so the leader can skip back a whole term
	// at a time instead of one entry per round trip (fast backtracking).
	if m.PrevLogIndex > n.log.lastIndex() {
		// Our log is too short: tell the leader where it ends.
		n.send(Message{Type: MsgAppResp, To: m.From, Success: false,
			ConflictIndex: n.log.lastIndex() + 1})
		return
	}
	if t, _ := n.log.term(m.PrevLogIndex); t != m.PrevLogTerm {
		// We have an entry there but from a different term. Point the leader
		// at the start of that term: if it lacks the term entirely, everything
		// from there on has to be replaced.
		n.send(Message{Type: MsgAppResp, To: m.From, Success: false,
			ConflictTerm: t, ConflictIndex: n.log.firstIndexOfTerm(m.PrevLogIndex)})
		return
	}

	// Rules 3 and 4: "If an existing entry conflicts with a new one (same
	// index but different terms), delete the existing entry and all that
	// follow it. Append any new entries not already in the log."
	//
	// Entries we already have are skipped, not rewritten. That matters: a
	// delayed or duplicated AppendEntries carries a prefix of what we may
	// already hold, and blindly truncating to it would erase newer entries
	// (possibly committed ones) that arrived in a later message.
	for i, e := range m.Entries {
		t, ok := n.log.term(e.Index)
		if ok && t == e.Term {
			continue // already have it
		}
		if ok {
			n.log.truncateFrom(e.Index) // conflict: drop it and everything after
			n.unstable = min(n.unstable, e.Index)
		}
		n.log.append(m.Entries[i:]...)
		break
	}

	// Rule 5: "If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)." We can only commit as far
	// as we know our log matches the leader's, which is lastNew. The extra
	// "> commitIndex" check stops a delayed message with fewer entries from
	// moving commitIndex backwards.
	lastNew := m.PrevLogIndex + uint64(len(m.Entries))
	if c := min(m.LeaderCommit, lastNew); c > n.commitIndex {
		n.commitIndex = c
	}

	n.send(Message{Type: MsgAppResp, To: m.From, Success: true, MatchIndex: lastNew})
}

// handleAppendResponse updates the leader's view of a follower.
func (n *Node) handleAppendResponse(m Message) {
	// Ignore responses unless we are still the leader of the term they are
	// for. Responses with a higher term were handled in Step.
	if n.role != Leader || m.Term != n.term {
		return
	}
	from := m.From

	if m.Success {
		// Figure 2, Leaders: "If successful: update nextIndex and matchIndex
		// for follower." Only move forward: a reordered older response must
		// not undo progress from a newer one.
		if m.MatchIndex > n.matchIndex[from] {
			n.matchIndex[from] = m.MatchIndex
			n.maybeCommit()
		}
		n.nextIndex[from] = max(n.nextIndex[from], n.matchIndex[from]+1)
		if n.nextIndex[from] <= n.log.lastIndex() {
			n.sendAppend(from) // more to send: keep going without waiting for the heartbeat
		}
		return
	}

	// Figure 2, Leaders: "If AppendEntries fails because of log
	// inconsistency: decrement nextIndex and retry." Using the follower's
	// hint, we skip back a whole term instead of one entry:
	//   - follower's log too short (ConflictTerm 0): continue from its end
	//   - we have entries of ConflictTerm: continue after our last one, since
	//     our logs agree up to there
	//   - we have no entries of ConflictTerm: all of the follower's entries
	//     of that term must go, so continue from where they start
	next := m.ConflictIndex
	if m.ConflictTerm != 0 {
		if last := n.log.lastIndexOfTerm(m.ConflictTerm); last > 0 {
			next = last + 1
		}
	}
	// Never go back past what the follower is known to have, nor forward
	// past our own log.
	next = max(next, n.matchIndex[from]+1)
	next = min(next, n.log.lastIndex()+1)
	n.nextIndex[from] = next
	n.sendAppend(from)
}

// maybeCommit advances commitIndex (Figure 2, Leaders): "If there exists an
// N such that N > commitIndex, a majority of matchIndex[i] >= N, and
// log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4)."
func (n *Node) maybeCommit() {
	if n.role != Leader {
		return
	}
	// The highest index stored on a majority: sort every node's match index
	// (the leader's is its whole log) from high to low and take the one at
	// position quorum-1.
	matches := []uint64{n.log.lastIndex()}
	for _, p := range n.peers {
		matches = append(matches, n.matchIndex[p])
	}
	slices.Sort(matches)
	slices.Reverse(matches)
	N := matches[n.quorum()-1]
	if N <= n.commitIndex {
		return
	}

	// The current-term restriction (§5.4.2, Figure 8). An entry from an
	// earlier term can be on a majority and still be overwritten by a
	// future leader, so counting replicas proves nothing about it. Once an
	// entry from the current term commits, every entry before it is
	// committed too (Log Matching), which is how older entries get
	// committed: indirectly.
	if t, _ := n.log.term(N); t != n.term && !n.cfg.Knobs.CommitPreviousTermsByCounting {
		return
	}
	n.commitIndex = N
}
