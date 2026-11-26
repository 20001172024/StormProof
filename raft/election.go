package raft

// campaign starts an election (Figure 2, Candidates, "On conversion to
// candidate"): increment currentTerm, vote for self, reset the election
// timer, and send RequestVote to every other server.
func (n *Node) campaign() {
	n.term++
	n.vote = n.id
	n.role = Candidate
	n.leader = None
	n.votes = map[NodeID]bool{n.id: true}
	n.pendingReads = nil
	n.resetElectionTimer()

	if n.countVoters(n.votes) >= n.quorum() {
		n.becomeLeader() // a one-node cluster elects itself
		return
	}
	for _, p := range n.others() {
		n.send(Message{
			Type:         MsgVote,
			To:           p,
			LastLogIndex: n.lastLogIndex(),
			LastLogTerm:  n.lastLogTerm(),
		})
	}
}

// handleRequestVote decides whether to vote for a candidate. It implements
// the RequestVote receiver rules in Figure 2.
//
// Step has already applied the all-servers rule before calling this, so if
// the candidate's term was higher than ours we have moved into that term,
// as a follower, with no vote cast in it yet.
func (n *Node) handleRequestVote(m Message) {
	// Rule 1: "Reply false if term < currentTerm (§5.1)." The candidate is
	// from an old term. Our reply carries our term, so it learns that it is
	// behind and steps down.
	if m.Term < n.term {
		n.send(Message{Type: MsgVoteResp, To: m.From, VoteGranted: false})
		return
	}

	// From here on m.Term == n.term.
	//
	// Rule 2: "If votedFor is null or candidateId, and candidate's log is at
	// least as up-to-date as receiver's log, grant vote (§5.2, §5.4)."
	//
	// Each node votes at most once per term, first come first served; that
	// alone guarantees at most one candidate can win a majority in a term.
	// Voting for the same candidate again is allowed, because its first
	// request or our first reply may have been lost or duplicated.
	// A candidate or leader of this term has already voted for itself, so
	// it rejects everyone else here.
	canVote := n.vote == None || n.vote == m.From
	if canVote && logUpToDate(m.LastLogTerm, m.LastLogIndex, n.lastLogTerm(), n.lastLogIndex()) {
		n.vote = m.From
		// Figure 2, Followers: granting a vote resets the election timer,
		// giving the candidate time to win before we start our own election.
		n.resetElectionTimer()
		n.send(Message{Type: MsgVoteResp, To: m.From, VoteGranted: true})
		return
	}
	n.send(Message{Type: MsgVoteResp, To: m.From, VoteGranted: false})
}

// handleVoteResponse counts votes (Figure 2, Candidates: "If votes received
// from majority of servers: become leader").
func (n *Node) handleVoteResponse(m Message) {
	// Ignore responses unless we are still campaigning in the term they are
	// for. A late response from an earlier election must not count now.
	if n.role != Candidate || m.Term != n.term || !m.VoteGranted {
		return
	}
	// A map, not a counter, so a duplicated response is only counted once.
	n.votes[m.From] = true
	if n.countVoters(n.votes) >= n.quorum() {
		n.becomeLeader()
	}
}

// logUpToDate reports whether a candidate's log, ending with an entry of
// term candTerm at index candIndex, is at least as up-to-date as ours
// (§5.4.1): "If the logs have last entries with different terms, then the
// log with the later term is more up-to-date. If the logs end with the same
// term, then whichever log is longer is more up-to-date."
//
// This check is what makes sure a node missing committed entries can never
// win an election (leader completeness): a committed entry is on a
// majority, any majority of voters includes one of those nodes, and that
// node refuses to vote for a candidate whose log is behind its own.
func logUpToDate(candTerm Term, candIndex uint64, ourTerm Term, ourIndex uint64) bool {
	if candTerm != ourTerm {
		return candTerm > ourTerm
	}
	return candIndex >= ourIndex
}

// lastLogIndex and lastLogTerm describe the last entry in the log.
func (n *Node) lastLogIndex() uint64 { return n.log.lastIndex() }
func (n *Node) lastLogTerm() Term    { return n.log.lastTerm() }
