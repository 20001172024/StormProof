package transport

import (
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	raftv1 "github.com/20001172024/stormproof/proto/stormproof/raft/v1"
	"github.com/20001172024/stormproof/raft"
)

func TestConvertRoundTrip(t *testing.T) {
	msgs := []raft.Message{
		{Type: raft.MsgVote, From: 1, To: 2, Term: 3, LastLogIndex: 9, LastLogTerm: 2},
		{Type: raft.MsgVoteResp, From: 2, To: 1, Term: 3, VoteGranted: true},
		{Type: raft.MsgApp, From: 1, To: 3, Term: 3, PrevLogIndex: 4, PrevLogTerm: 2, LeaderCommit: 4, Context: 7,
			Entries: []raft.Entry{{Index: 5, Term: 3}, {Index: 6, Term: 3, Data: []byte("x")},
				{Index: 7, Term: 3, Type: raft.EntryConfChange, Data: raft.EncodeVoters([]raft.NodeID{1, 2, 3, 4})}}},
		{Type: raft.MsgAppResp, From: 3, To: 1, Term: 3, Success: true, MatchIndex: 7, Context: 7},
		{Type: raft.MsgAppResp, From: 3, To: 1, Term: 3, ConflictTerm: 2, ConflictIndex: 3},
		{Type: raft.MsgSnap, From: 1, To: 2, Term: 4, Snapshot: &raft.Snapshot{Index: 50, Term: 4, Voters: []raft.NodeID{1, 2, 3}, Data: []byte("state")}},
	}
	for _, m := range msgs {
		if got := FromProto(ToProto(m)); !reflect.DeepEqual(got, m) {
			t.Errorf("round trip of %v:\n got %+v\nwant %+v", m.Type, got, m)
		}
	}
}

// Two transports talking over real gRPC on localhost.
func TestSendReceive(t *testing.T) {
	var mu sync.Mutex
	var got []raft.Message
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	raftv1.RegisterRaftServiceServer(srv, NewService(func(m raft.Message) {
		mu.Lock()
		got = append(got, m)
		mu.Unlock()
	}))
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	sent := 0
	tr, err := New(map[raft.NodeID]string{2: lis.Addr().String()}, Options{
		OnSend: func(raft.NodeID, time.Duration, error) { sent++ },
	})
	if err != nil {
		t.Fatal(err)
	}
	var want []raft.Message
	for i := uint64(1); i <= 200; i++ {
		want = append(want, raft.Message{Type: raft.MsgApp, From: 1, To: 2, Term: 1, LeaderCommit: i})
	}
	tr.Send(want)
	tr.Send([]raft.Message{{Type: raft.MsgApp, From: 1, To: 9}}) // unknown peer: dropped
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n == len(want) || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	tr.Close()
	mu.Lock()
	defer mu.Unlock()
	if len(got) != len(want) {
		t.Fatalf("received %d of %d messages", len(got), len(want))
	}
	for i := range want {
		if got[i].LeaderCommit != want[i].LeaderCommit {
			t.Fatalf("message %d out of order: %+v", i, got[i])
		}
	}
	if d := tr.Dropped(); d[9] != 1 {
		t.Fatalf("dropped = %v, want 1 to unknown peer 9", d)
	}
	if sent == 0 || sent >= len(want) {
		t.Fatalf("%d RPCs for %d messages: expected batching", sent, len(want))
	}
}

// A dead peer doesn't block Send: its queue fills and the rest is dropped.
func TestDeadPeerDoesNotBlock(t *testing.T) {
	tr, err := New(map[raft.NodeID]string{2: "127.0.0.1:1"}, Options{QueueSize: 4, Timeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			tr.Send([]raft.Message{{Type: raft.MsgApp, To: 2}})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Send blocked on a dead peer")
	}
	if tr.Dropped()[2] == 0 {
		t.Fatal("nothing was dropped for the dead peer")
	}
}
