package raft

import "slices"

// Flow control. Figure 2 says what an AppendEntries carries, not when to
// send one; that only affects speed, never safety, because every message
// passes the log consistency check on arrival. The policy here is one
// outstanding batch per follower:
//
//   - New entries go to a follower only when it has no batch outstanding
//     (broadcastAppend, after a proposal). Otherwise they wait in the log,
//     and the next batch carries all of them: under load, batches grow by
//     themselves (dissertation §10.2.1 describes batching and pipelining).
//   - A batch is settled when the follower acknowledges up to its last
//     entry, or rejects it; then whatever has piled up goes out at once.
//   - Heartbeats (broadcastHeartbeat, every HeartbeatTicks) send from
//     nextIndex regardless, so a lost batch is resent within a heartbeat.
//
// Before this, every proposal and every acknowledgement that found more to
// send resent all unacknowledged entries. Under steady load those chains of
// send, acknowledge, send piled up faster than they finished, and write
// throughput fell as clients were added.

// broadcastAppend sends new entries to every follower without a batch
// outstanding.
func (n *Node) broadcastAppend() {
	for _, p := range n.others() {
		n.maybeSendAppend(p)
	}
}

// broadcastHeartbeat sends AppendEntries to every follower, from its
// nextIndex, whether or not a batch is outstanding: the heartbeat (Figure 2,
// Leaders) and the retransmission of lost batches.
func (n *Node) broadcastHeartbeat() {
	for _, p := range n.others() {
		n.sendAppend(p)
	}
}

// broadcastReadCheck asks every follower to acknowledge this node as leader
// now, for ReadIndex, with an AppendEntries that carries no entries: just
// the check that the follower holds nextIndex-1, which it acknowledges
// either way. Sending entries here too would resend every unacknowledged
// entry for every read.
func (n *Node) broadcastReadCheck() {
	for _, p := range n.others() {
		prev := n.nextIndex[p] - 1
		if prev < n.log.snapIndex {
			// The follower needs a snapshot. Don't resend it for every read: the
			// next heartbeat carries the read's sequence number too.
			n.maybeSendAppend(p)
			continue
		}
		prevTerm, _ := n.log.term(prev)
		n.send(Message{Type: MsgApp, To: p, PrevLogIndex: prev, PrevLogTerm: prevTerm,
			LeaderCommit: n.commitIndex, Context: n.readSeq})
	}
}

// maybeSendAppend sends to a follower unless a batch is outstanding.
func (n *Node) maybeSendAppend(to NodeID) {
	if !n.inflight[to] {
		n.sendAppend(to)
	}
}

// sendAppend sends one AppendEntries to a follower, starting at its
// nextIndex, and records the batch as outstanding if it carries entries.
// Every AppendEntries also acts as a heartbeat. nextIndex only moves when
// the follower answers, so until then a resend repeats the same entries;
// the receiver skips entries it already has (rule 4).
func (n *Node) sendAppend(to NodeID) {
	next := n.nextIndex[to]
	if next <= n.log.snapIndex {
		// The follower needs entries we have compacted away: send the
		// snapshot instead (§7). It is resent with every heartbeat until the
		// follower answers, like AppendEntries.
		n.send(Message{Type: MsgSnap, To: to, Snapshot: n.snapshot, Context: n.readSeq})
		n.inflight[to], n.sentUpTo[to] = true, n.log.snapIndex
		return
	}
	prev := next - 1
	prevTerm, _ := n.log.term(prev) // prev <= lastIndex, so it exists
	entries := n.log.slice(next, next+uint64(n.cfg.MaxEntriesPerMsg))
	n.send(Message{
		Type:         MsgApp,
		To:           to,
		PrevLogIndex: prev,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
		Context:      n.readSeq,
	})
	if len(entries) > 0 {
		n.inflight[to], n.sentUpTo[to] = true, entries[len(entries)-1].Index
	}
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

	// Entries up to our snapshot are committed, so they match the leader's
	// (leader completeness); only what comes after can be checked or
	// appended. Skip the part of the message the snapshot already covers,
	// and check from the snapshot's last entry instead.
	if m.PrevLogIndex < n.log.snapIndex {
		skip := min(uint64(len(m.Entries)), n.log.snapIndex-m.PrevLogIndex)
		m.Entries = m.Entries[skip:]
		m.PrevLogIndex, m.PrevLogTerm = n.log.snapIndex, n.log.snapTerm
	}

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
			ConflictIndex: n.log.lastIndex() + 1, Context: m.Context})
		return
	}
	if t, _ := n.log.term(m.PrevLogIndex); t != m.PrevLogTerm {
		// We have an entry there but from a different term. Point the leader
		// at the start of that term: if it lacks the term entirely, everything
		// from there on has to be replaced.
		n.send(Message{Type: MsgAppResp, To: m.From, Success: false,
			ConflictTerm: t, ConflictIndex: n.log.firstIndexOfTerm(m.PrevLogIndex), Context: m.Context})
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
		n.refreshConfig() // the new entries may change the configuration, or undo a change
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

	n.send(Message{Type: MsgAppResp, To: m.From, Success: true, MatchIndex: lastNew, Context: m.Context})
}

// handleAppendResponse updates the leader's view of a follower.
func (n *Node) handleAppendResponse(m Message) {
	// Ignore responses unless we are still the leader of the term they are
	// for. Responses with a higher term were handled in Step.
	if n.role != Leader || m.Term != n.term {
		return
	}
	from := m.From

	// Any response in our term, success or not, shows the follower still
	// accepts us as leader: it would have answered with a higher term
	// otherwise. That is what ReadIndex needs.
	if m.Context > 0 {
		n.ackRead(from, m.Context)
	}

	if m.Success {
		// Figure 2, Leaders: "If successful: update nextIndex and matchIndex
		// for follower." Only move forward: a reordered older response must
		// not undo progress from a newer one.
		if m.MatchIndex > n.matchIndex[from] {
			n.matchIndex[from] = m.MatchIndex
			n.maybeCommit()
			if n.role != Leader {
				return // the commit removed us from the configuration (§4.2.2)
			}
		}
		n.nextIndex[from] = max(n.nextIndex[from], n.matchIndex[from]+1)
		// The batch is settled once the follower has everything it carried.
		// (Acknowledgements of heartbeats and read checks, which carry
		// nothing new, don't settle it.)
		if n.matchIndex[from] >= n.sentUpTo[from] {
			n.inflight[from] = false
		}
		if n.nextIndex[from] <= n.log.lastIndex() {
			n.maybeSendAppend(from) // more to send: keep going without waiting for the heartbeat
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
	n.inflight[from] = false // a rejection settles the batch: retry from the new nextIndex
	n.sendAppend(from)
}

// maybeCommit advances commitIndex (Figure 2, Leaders): "If there exists an
// N such that N > commitIndex, a majority of matchIndex[i] >= N, and
// log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4)."
func (n *Node) maybeCommit() {
	if n.role != Leader {
		return
	}
	// The highest index stored on a majority of voters: sort every voter's
	// match index (the leader's is its whole log, if it is a voter) from high
	// to low and take the one at position quorum-1.
	var matches []uint64
	for _, v := range n.voters {
		if v == n.id {
			matches = append(matches, n.log.lastIndex())
		} else {
			matches = append(matches, n.matchIndex[v])
		}
	}
	if len(matches) == 0 {
		return
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
	n.stepDownIfRemoved()
}

// handleSnapshot implements the InstallSnapshot receiver (Figure 13).
func (n *Node) handleSnapshot(m Message) {
	// Rule 1: "Reply immediately if term < currentTerm."
	if m.Term < n.term {
		n.send(Message{Type: MsgAppResp, To: m.From, Success: false, Context: m.Context})
		return
	}
	n.becomeFollower(m.Term, m.From) // m.From leads our term
	s := m.Snapshot
	reply := func(match uint64) {
		n.send(Message{Type: MsgAppResp, To: m.From, Success: true, MatchIndex: match, Context: m.Context})
	}

	// A snapshot we already have, or one that arrived late, after we had
	// caught up through the log: everything it covers is committed here
	// already. Ignore it. Installing it would throw away newer entries.
	if s.Index <= n.commitIndex {
		reply(n.commitIndex)
		return
	}

	// Rules 6 and 7: "If existing log entry has same index and term as
	// snapshot's last included entry, retain log entries following it and
	// reply. Discard the entire log." Either way the state machine is reset
	// from the snapshot (rule 8), and everything up to s.Index is committed.
	if t, ok := n.log.term(s.Index); ok && t == s.Term {
		n.log.compact(s.Index)
	} else {
		n.log.restore(s.Index, s.Term)
	}
	n.baseVoters = slices.Clone(s.Voters)
	n.refreshConfig()
	n.snapshot = s
	n.newSnapshot = s
	n.commitIndex = s.Index
	n.applied = s.Index // the caller loads the snapshot instead of applying entries
	n.unstable = n.log.lastIndex() + 1
	reply(s.Index)
}
