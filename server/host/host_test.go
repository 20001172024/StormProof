package host

import (
	"testing"

	"github.com/20001172024/stormproof/kv"
	"github.com/20001172024/stormproof/raft"
	"github.com/20001172024/stormproof/sim"
)

func testConfig(disk *sim.Disk, id raft.NodeID, peers ...raft.NodeID) Config {
	return Config{
		ID: id, Peers: peers, ElectionTicks: 10, HeartbeatTicks: 1,
		FS: disk, Rand: sim.NewRand(1, "host-test"), SegmentSize: 1 << 10, SnapshotEvery: 50,
	}
}

func sameEntry(a, b raft.Entry) bool {
	return a.Index == b.Index && a.Term == b.Term && a.Type == b.Type && string(a.Data) == string(b.Data)
}

// State saved through the WAL, including a truncation, comes back after a
// machine crash.
func TestRecoveryThroughWAL(t *testing.T) {
	disk := sim.NewDisk(sim.DiskConfig{}, sim.NewRand(1, "disk"))
	h, err := Open(testConfig(disk, 1, 2, 3))
	if err != nil {
		t.Fatal(err)
	}
	hs := raft.HardState{Term: 7, Vote: 3}
	es := []raft.Entry{{Index: 1, Term: 2}, {Index: 2, Term: 7, Data: []byte("x")}, {Index: 3, Term: 7, Data: []byte("y")}}
	if err := h.wal.Save(&hs, es); err != nil {
		t.Fatal(err)
	}
	// Replace the tail from index 2, as after a conflict with a new leader.
	if err := h.wal.Save(nil, []raft.Entry{{Index: 2, Term: 8, Data: []byte("z")}}); err != nil {
		t.Fatal(err)
	}
	disk.Crash(true)
	h2, err := Open(testConfig(disk, 1, 2, 3))
	if err != nil {
		t.Fatal(err)
	}
	if st := h2.Status(); st.Term != 7 || st.Vote != 3 {
		t.Fatalf("recovered %+v, want term 7 vote 3", st)
	}
	want := []raft.Entry{{Index: 1, Term: 2}, {Index: 2, Term: 8, Data: []byte("z")}}
	got := h2.Entries()
	if len(got) != len(want) || !sameEntry(got[0], want[0]) || !sameEntry(got[1], want[1]) {
		t.Fatalf("recovered log %+v, want %+v", got, want)
	}
}

// A server whose WAL is corrupt refuses to start.
func TestCorruptWALRefusesToStart(t *testing.T) {
	disk := sim.NewDisk(sim.DiskConfig{}, sim.NewRand(1, "disk"))
	// A complete record (length 17, so the bytes are all there) whose
	// checksum is wrong.
	f, _ := disk.Create("wal-0000000000000001.log")
	_, _ = f.Write(append([]byte{17, 0, 0, 0, 0xde, 0xad, 0xbe, 0xef, 1}, make([]byte, 16)...))
	_ = f.Sync()
	_ = disk.SyncDir()
	if _, err := Open(testConfig(disk, 1, 2)); err == nil {
		t.Fatal("started from a corrupt WAL")
	}
}

// A one-server cluster end to end through the Host API: elect, write, read,
// restart from disk, read again.
func TestSingleServer(t *testing.T) {
	disk := sim.NewDisk(sim.DiskConfig{}, sim.NewRand(1, "disk"))
	h, err := Open(testConfig(disk, 1))
	if err != nil {
		t.Fatal(err)
	}
	for h.Status().Role != raft.Leader {
		if _, err := h.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	// The no-op must commit before reads; a tick or two does it.
	call := func(req Request) Response {
		t.Helper()
		out, err := h.Handle(req)
		for i := 0; err == nil && len(out.Responses) == 0 && i < 20; i++ {
			out, err = h.Tick()
		}
		if err != nil || len(out.Responses) != 1 {
			t.Fatalf("%+v: responses %+v, err %v", req, out.Responses, err)
		}
		return out.Responses[0]
	}
	put := call(Request{Client: 42, Cmd: kv.Command{ClientID: 7, RequestID: 1, Op: kv.OpPut, Key: "k", Value: []byte("v")}})
	if put.Status != StatusOK || put.Client != 42 || put.RequestID != 1 {
		t.Fatalf("put: %+v", put)
	}
	get := call(Request{Client: 42, Cmd: kv.Command{ClientID: 7, RequestID: 2, Op: kv.OpGet, Key: "k"}})
	if get.Status != StatusOK || string(get.Result.Value) != "v" {
		t.Fatalf("get: %+v", get)
	}
	_ = h.Close()
	disk.Crash(true)

	h, err = Open(testConfig(disk, 1))
	if err != nil {
		t.Fatal(err)
	}
	for h.Status().Role != raft.Leader {
		if _, err := h.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	if got := call(Request{Client: 1, Cmd: kv.Command{ClientID: 8, RequestID: 1, Op: kv.OpGet, Key: "k"}}); string(got.Result.Value) != "v" {
		t.Fatalf("after restart: %+v", got)
	}
}

func TestNotLeader(t *testing.T) {
	disk := sim.NewDisk(sim.DiskConfig{}, sim.NewRand(1, "disk"))
	h, err := Open(testConfig(disk, 1, 2, 3))
	if err != nil {
		t.Fatal(err)
	}
	out, err := h.Handle(Request{Client: 5, Cmd: kv.Command{RequestID: 3, Op: kv.OpGet, Key: "k"}})
	if err != nil || len(out.Responses) != 1 || out.Responses[0].Status != StatusNotLeader || out.Responses[0].Client != 5 {
		t.Fatalf("%+v, %v", out.Responses, err)
	}
}
