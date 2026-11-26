package transport

import (
	raftv1 "github.com/20001172024/stormproof/proto/stormproof/raft/v1"
	"github.com/20001172024/stormproof/raft"
)

// ToProto converts a Raft message for the wire.
func ToProto(m raft.Message) *raftv1.Message {
	p := &raftv1.Message{
		Type:          uint32(m.Type),
		From:          uint64(m.From),
		To:            uint64(m.To),
		Term:          uint64(m.Term),
		LastLogIndex:  m.LastLogIndex,
		LastLogTerm:   uint64(m.LastLogTerm),
		VoteGranted:   m.VoteGranted,
		PrevLogIndex:  m.PrevLogIndex,
		PrevLogTerm:   uint64(m.PrevLogTerm),
		LeaderCommit:  m.LeaderCommit,
		Success:       m.Success,
		MatchIndex:    m.MatchIndex,
		ConflictTerm:  uint64(m.ConflictTerm),
		ConflictIndex: m.ConflictIndex,
		Context:       m.Context,
	}
	for _, e := range m.Entries {
		p.Entries = append(p.Entries, &raftv1.Entry{Index: e.Index, Term: uint64(e.Term), Type: uint32(e.Type), Data: e.Data})
	}
	if s := m.Snapshot; s != nil {
		ps := &raftv1.Snapshot{Index: s.Index, Term: uint64(s.Term), Data: s.Data}
		for _, v := range s.Voters {
			ps.Voters = append(ps.Voters, uint64(v))
		}
		p.Snapshot = ps
	}
	return p
}

// FromProto converts a message from the wire.
func FromProto(p *raftv1.Message) raft.Message {
	m := raft.Message{
		Type:          raft.MessageType(p.GetType()),
		From:          raft.NodeID(p.GetFrom()),
		To:            raft.NodeID(p.GetTo()),
		Term:          raft.Term(p.GetTerm()),
		LastLogIndex:  p.GetLastLogIndex(),
		LastLogTerm:   raft.Term(p.GetLastLogTerm()),
		VoteGranted:   p.GetVoteGranted(),
		PrevLogIndex:  p.GetPrevLogIndex(),
		PrevLogTerm:   raft.Term(p.GetPrevLogTerm()),
		LeaderCommit:  p.GetLeaderCommit(),
		Success:       p.GetSuccess(),
		MatchIndex:    p.GetMatchIndex(),
		ConflictTerm:  raft.Term(p.GetConflictTerm()),
		ConflictIndex: p.GetConflictIndex(),
		Context:       p.GetContext(),
	}
	for _, e := range p.GetEntries() {
		m.Entries = append(m.Entries, raft.Entry{
			Index: e.GetIndex(), Term: raft.Term(e.GetTerm()), Type: raft.EntryType(e.GetType()), Data: nilIfEmpty(e.GetData()),
		})
	}
	if ps := p.GetSnapshot(); ps != nil {
		s := &raft.Snapshot{Index: ps.GetIndex(), Term: raft.Term(ps.GetTerm()), Data: nilIfEmpty(ps.GetData())}
		for _, v := range ps.GetVoters() {
			s.Voters = append(s.Voters, raft.NodeID(v))
		}
		m.Snapshot = s
	}
	return m
}

// nilIfEmpty keeps the in-memory meaning of "no data" (nil, as for a
// leader's no-op) across the wire, where protobuf can't tell nil from empty.
func nilIfEmpty(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
}
