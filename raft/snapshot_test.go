package raft

import (
	"errors"
	"fmt"
	"testing"
)

// snapLog is a log compacted up to snapIndex (term snapTerm), followed by
// entries with the given terms.
func snapLog(snapIndex uint64, snapTerm Term, terms ...Term) *raftLog {
	l := &raftLog{snapIndex: snapIndex, snapTerm: snapTerm}
	for i, t := range terms {
		l.append(Entry{Index: snapIndex + uint64(i) + 1, Term: t})
	}
	return l
}

func TestLogWithSnapshot(t *testing.T) {
	l := snapLog(10, 3, 3, 4, 4) // entries 11, 12, 13
	if l.firstIndex() != 11 || l.lastIndex() != 13 || l.lastTerm() != 4 {
		t.Fatalf("first %d last %d lastTerm %d", l.firstIndex(), l.lastIndex(), l.lastTerm())
	}
	for _, c := range []struct {
		i      uint64
		want   Term
		wantOK bool
	}{{9, 0, false}, {10, 3, true}, {11, 3, true}, {13, 4, true}, {14, 0, false}} {
		if got, ok := l.term(c.i); got != c.want || ok != c.wantOK {
			t.Errorf("term(%d) = %d, %v; want %d, %v", c.i, got, ok, c.want, c.wantOK)
		}
	}
	if got := l.slice(5, 13); len(got) != 2 || got[0].Index != 11 {
		t.Errorf("slice from before the snapshot should start at the first entry: %+v", got)
	}
	if got := l.firstIndexOfTerm(11); got != 11 {
		t.Errorf("firstIndexOfTerm(11) = %d: the run can't go back into the snapshot", got)
	}
	if got := l.firstIndexOfTerm(10); got != 10 {
		t.Errorf("firstIndexOfTerm(snapshot index) = %d, want 10", got)
	}
	if got := snapLog(10, 3).lastIndexOfTerm(3); got != 10 {
		t.Errorf("lastIndexOfTerm should find the snapshot's own term: got %d", got)
	}
	if got := l.lastIndexOfTerm(3); got != 11 {
		t.Errorf("lastIndexOfTerm(3) = %d, want 11", got)
	}

	empty := snapLog(10, 3)
	if empty.lastIndex() != 10 || empty.lastTerm() != 3 {
		t.Error("a log that is all snapshot should report the snapshot's last index and term")
	}

	l.compact(12)
	if l.firstIndex() != 13 || l.snapTerm != 4 || len(l.entries) != 1 {
		t.Fatalf("after compact(12): first %d snapTerm %d entries %d", l.firstIndex(), l.snapTerm, len(l.entries))
	}
	l.restore(20, 5)
	if l.lastIndex() != 20 || len(l.entries) != 0 {
		t.Fatal("restore should leave only the snapshot")
	}
}

func TestTruncateIntoSnapshotPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("truncating at a compacted index did not panic")
		}
	}()
	snapLog(10, 3, 3).truncateFrom(10)
}

func TestCompact(t *testing.T) {
	n := newTestNode(t, 1, nil, HardState{}) // one node: commits immediately
	tickN(n, testElection)
	for i := 0; i < 5; i++ {
		if _, err := n.Propose([]byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := n.Compact(3, []byte("state")); !errors.Is(err, ErrUnapplied) {
		t.Fatalf("Compact before applying: err = %v, want ErrUnapplied", err)
	}
	drain(n) // applies 1..6
	s, err := n.Compact(4, []byte("state at 4"))
	if err != nil {
		t.Fatal(err)
	}
	if s.Index != 4 || s.Term != 1 || n.Status().SnapIndex != 4 || n.Entries()[0].Index != 5 {
		t.Fatalf("snapshot %+v, status %+v, first entry %d", s, n.Status(), n.Entries()[0].Index)
	}
	if _, err := n.Compact(4, nil); !errors.Is(err, ErrCompacted) {
		t.Fatalf("compacting again: err = %v, want ErrCompacted", err)
	}
	// The node keeps working after compaction.
	if _, err := n.Propose([]byte("after")); err != nil {
		t.Fatal(err)
	}
	if rd := n.Ready(); len(rd.CommittedEntries) != 1 || rd.CommittedEntries[0].Index != 7 {
		t.Fatalf("committed after compaction: %+v", rd.CommittedEntries)
	}
}

func TestRecoverFromSnapshot(t *testing.T) {
	snap := &Snapshot{Index: 10, Term: 2, Voters: []NodeID{1, 2, 3}, Data: []byte("s")}
	n := newTestNodeWith(t, Config{
		HardState: HardState{Term: 3}, Snapshot: snap,
		Entries: []Entry{{Index: 11, Term: 3}, {Index: 12, Term: 3}},
	})
	st := n.Status()
	if st.SnapIndex != 10 || st.Commit != 10 || st.LastIndex != 12 {
		t.Fatalf("status %+v", st)
	}
	if rd := n.Ready(); len(rd.CommittedEntries) != 0 || rd.Snapshot != nil || len(rd.Entries) != 0 {
		t.Fatalf("a recovered node should have nothing to do: %+v", rd)
	}
	_, err := NewNode(Config{
		ID: 1, Peers: []NodeID{2}, ElectionTicks: 10, HeartbeatTicks: 1, Rand: fixedRand(0),
		Snapshot: snap, Entries: []Entry{{Index: 12, Term: 3}},
	})
	if err == nil {
		t.Fatal("entries that don't start right after the snapshot were accepted")
	}
}

// Figure 13, InstallSnapshot receiver.
func TestHandleSnapshot(t *testing.T) {
	snap := func(term Term, index uint64, snapTerm Term) Message {
		return Message{Type: MsgSnap, From: 2, To: 1, Term: term,
			Snapshot: &Snapshot{Index: index, Term: snapTerm, Voters: []NodeID{1, 2, 3}, Data: []byte("state")}}
	}
	cases := []struct {
		name       string
		log        []Term
		commit     uint64
		msg        Message
		wantOK     bool
		wantSnap   bool   // Ready.Snapshot is set
		wantLast   uint64 // last index afterwards
		wantCommit uint64
	}{
		{name: "rule 1: stale term", log: []Term{1}, msg: snap(1, 5, 1), wantOK: false, wantLast: 1},
		{name: "rule 6: matching entry, keep what follows", log: []Term{1, 1, 2, 2, 2}, msg: snap(3, 3, 2),
			wantOK: true, wantSnap: true, wantLast: 5, wantCommit: 3},
		{name: "rule 7: no matching entry, discard the log", log: []Term{1, 1, 1, 1, 1}, msg: snap(3, 3, 2),
			wantOK: true, wantSnap: true, wantLast: 3, wantCommit: 3},
		{name: "rule 7: log shorter than the snapshot", log: []Term{1}, msg: snap(3, 8, 2),
			wantOK: true, wantSnap: true, wantLast: 8, wantCommit: 8},
		{name: "late snapshot already covered by commit: ignored", log: []Term{1, 1, 1, 1, 1}, commit: 5, msg: snap(2, 3, 1),
			wantOK: true, wantSnap: false, wantLast: 5, wantCommit: 5},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n := follower(t, 2, c.log...)
			if c.commit > 0 {
				n.Step(appendMsg(2, c.commit, c.log[c.commit-1], c.commit))
				drain(n)
			}
			n.Step(c.msg)
			rd := n.Ready()
			n.Advance()
			resp := rd.Messages[len(rd.Messages)-1]
			if resp.Type != MsgAppResp || resp.Success != c.wantOK {
				t.Fatalf("response %+v, want success %v", resp, c.wantOK)
			}
			if (rd.Snapshot != nil) != c.wantSnap {
				t.Fatalf("Ready.Snapshot = %v, want set: %v", rd.Snapshot, c.wantSnap)
			}
			st := n.Status()
			if st.LastIndex != c.wantLast || (c.wantOK && st.Commit != c.wantCommit) {
				t.Fatalf("last %d commit %d, want %d and %d", st.LastIndex, st.Commit, c.wantLast, c.wantCommit)
			}
			if c.wantSnap && len(rd.CommittedEntries) != 0 {
				t.Fatalf("entries covered by the snapshot were handed out to apply: %+v", rd.CommittedEntries)
			}
		})
	}
}

// AppendEntries reaching back into a follower's snapshot: the covered part
// is skipped and the rest checked against the snapshot's last entry.
func TestAppendEntriesOverlappingSnapshot(t *testing.T) {
	n := newTestNodeWith(t, Config{HardState: HardState{Term: 2}, Snapshot: &Snapshot{Index: 5, Term: 2, Voters: []NodeID{1, 2, 3}}})
	n.Step(appendMsg(2, 3, 1, 6, 2, 2, 2, 2)) // entries 4..7
	msgs := drain(n)
	if len(msgs) != 1 || !msgs[0].Success || msgs[0].MatchIndex != 7 {
		t.Fatalf("reply %+v, want success matching 7", msgs)
	}
	if st := n.Status(); st.LastIndex != 7 || st.Commit != 6 {
		t.Fatalf("status %+v", st)
	}
}

// A follower that falls behind a leader that has compacted its log is
// brought up to date with InstallSnapshot, and then by AppendEntries.
func TestLaggingFollowerGetsSnapshot(t *testing.T) {
	c := newCluster(t, 3)
	c.elect(1)
	c.drop = func(m Message) bool { return m.To == 3 || m.From == 3 }
	for i := 0; i < 10; i++ {
		c.propose(1, fmt.Sprint(i))
	}
	leader := c.nodes[1]
	snap, err := leader.Compact(leader.applied, []byte("leader state"))
	if err != nil {
		t.Fatal(err)
	}
	c.drop = nil
	c.heartbeat(1)
	c.propose(1, "after")
	c.heartbeat(1)

	got := c.snaps[3]
	if len(got) != 1 || got[0].Index != snap.Index || string(got[0].Data) != "leader state" {
		t.Fatalf("node 3 snapshots %+v, want the leader's at %d", got, snap.Index)
	}
	f := c.nodes[3]
	if st := f.Status(); st.LastIndex != leader.log.lastIndex() || st.Commit != leader.commitIndex {
		t.Fatalf("node 3 %+v, leader last %d commit %d", st, leader.log.lastIndex(), leader.commitIndex)
	}
	applied := c.applied[3]
	if len(applied) != 1 || string(applied[0].Data) != "after" {
		t.Fatalf("node 3 applied %+v after the snapshot, want just the new entry", applied)
	}
}
