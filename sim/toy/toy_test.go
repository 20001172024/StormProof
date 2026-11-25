package toy

import (
	"encoding/binary"
	"testing"

	"github.com/20001172024/stormproof/sim"
)

func testEnv(seed uint64, disk *sim.Disk) sim.Env {
	return sim.Env{ID: 1, Peers: []sim.NodeID{2, 3}, Disk: disk, Rand: sim.NewRand(seed, "toy-test")}
}

func newDisk() *sim.Disk { return sim.NewDisk(sim.DiskConfig{}, sim.NewRand(1, "disk")) }

// tickUntilIncrement ticks n until its counter goes up and returns the
// messages from that tick.
func tickUntilIncrement(t *testing.T, n *Node) []sim.Message {
	t.Helper()
	before := n.value
	for i := 0; i < 1000; i++ {
		msgs, err := n.Tick()
		if err != nil {
			t.Fatal(err)
		}
		if n.value != before {
			return msgs
		}
	}
	t.Fatal("counter never incremented")
	return nil
}

func TestFreshStart(t *testing.T) {
	n, err := open(testEnv(1, newDisk()), false)
	if err != nil {
		t.Fatal(err)
	}
	if n.value != 0 {
		t.Fatalf("fresh node value = %d, want 0", n.value)
	}
}

func TestIncrementBroadcastsToPeers(t *testing.T) {
	n, _ := open(testEnv(1, newDisk()), false)
	msgs := tickUntilIncrement(t, n)
	if len(msgs) != 2 || msgs[0].To != 2 || msgs[1].To != 3 {
		t.Fatalf("messages = %+v, want one to each peer", msgs)
	}
	if c := msgs[0].Body.(Counter); c.Value != 1 {
		t.Fatalf("broadcast value %d, want 1", c.Value)
	}
}

// The correct node's state survives a machine crash, every time.
func TestStateSurvivesMachineCrash(t *testing.T) {
	for seed := uint64(0); seed < 100; seed++ {
		disk := sim.NewDisk(sim.DiskConfig{}, sim.NewRand(seed, "disk"))
		n, _ := open(testEnv(seed, disk), false)
		tickUntilIncrement(t, n)
		tickUntilIncrement(t, n)
		disk.Crash(true)
		n2, err := open(testEnv(seed, disk), false)
		if err != nil {
			t.Fatal(err)
		}
		if n2.value != 2 {
			t.Fatalf("seed %d: recovered %d, want 2", seed, n2.value)
		}
	}
}

func TestStepAdoptsLargerValuesOnly(t *testing.T) {
	disk := newDisk()
	n, _ := open(testEnv(1, disk), false)
	for _, c := range []struct{ in, want uint64 }{{5, 5}, {3, 5}, {9, 9}} {
		if _, err := n.Step(sim.Message{From: 2, To: 1, Body: Counter{Value: c.in}}); err != nil {
			t.Fatal(err)
		}
		if n.value != c.want {
			t.Fatalf("after receiving %d: value %d, want %d", c.in, n.value, c.want)
		}
	}
	disk.Crash(true)
	if n2, _ := open(testEnv(1, disk), false); n2.value != 9 {
		t.Fatalf("recovered %d, want 9", n2.value)
	}
}

func TestCorruptStateFileRefusesToStart(t *testing.T) {
	disk := newDisk()
	f, _ := disk.Create(stateFile)
	_, _ = f.Write([]byte{1, 2, 3})
	if _, err := open(testEnv(1, disk), false); err == nil {
		t.Fatal("correct node started from a 3-byte state file")
	}
	if _, err := open(testEnv(1, disk), true); err != nil {
		t.Fatalf("buggy node should start (wrongly) from a damaged file: %v", err)
	}
}

func TestLeftoverTempFileIsRemoved(t *testing.T) {
	disk := newDisk()
	f, _ := disk.Create(tempFile)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], 99)
	_, _ = f.Write(buf[:])
	n, err := open(testEnv(1, disk), false)
	if err != nil {
		t.Fatal(err)
	}
	if n.value != 0 {
		t.Fatalf("value = %d: an unrenamed temp file must not count", n.value)
	}
	if names, _ := disk.List(); len(names) != 0 {
		t.Fatalf("files after recovery = %v, want none", names)
	}
}

func TestMonotonicInvariant(t *testing.T) {
	inv := Monotonic()
	node := &Node{value: 5}
	view := []sim.NodeView{{ID: 1, Up: true, Process: node}}
	if err := inv.Check(view); err != nil {
		t.Fatal(err)
	}
	if err := inv.Check([]sim.NodeView{{ID: 1, Up: false}}); err != nil {
		t.Fatalf("down nodes must be skipped: %v", err)
	}
	node.value = 4
	if err := inv.Check(view); err == nil {
		t.Fatal("counter went from 5 to 4 and the invariant did not fire")
	}
}
