package snap

import (
	"errors"
	"io/fs"
	"slices"
	"testing"

	"github.com/20001172024/stormproof/raft"
	"github.com/20001172024/stormproof/sim"
)

func newDisk(seed uint64) *sim.Disk {
	return sim.NewDisk(sim.DiskConfig{}, sim.NewRand(seed, "snap-test"))
}

func TestSaveLoad(t *testing.T) {
	d := newDisk(1)
	want := raft.Snapshot{Index: 42, Term: 7, Data: []byte("state")}
	if err := Save(d, want, true); err != nil {
		t.Fatal(err)
	}
	got, err := Load(d, 42)
	if err != nil {
		t.Fatal(err)
	}
	if got.Index != 42 || got.Term != 7 || string(got.Data) != "state" {
		t.Fatalf("got %+v", got)
	}
	if _, err := Load(d, 41); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing snapshot: err = %v", err)
	}
}

func TestCorruptionDetected(t *testing.T) {
	d := newDisk(1)
	_ = Save(d, raft.Snapshot{Index: 1, Term: 1, Data: []byte("state")}, true)
	b, _ := d.ReadFile(Name(1))
	b[len(b)-1] ^= 0xff
	f, _ := d.Create(Name(1))
	_, _ = f.Write(b)
	if _, err := Load(d, 1); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
}

// A saved snapshot survives any machine crash; one whose Save was cut short
// by a crash is either complete or absent, never partial.
func TestSaveIsAtomic(t *testing.T) {
	for seed := uint64(0); seed < 200; seed++ {
		d := newDisk(seed)
		if err := Save(d, raft.Snapshot{Index: 1, Term: 1, Data: []byte("durable")}, true); err != nil {
			t.Fatal(err)
		}
		// A second Save without syncing stands in for one interrupted by the
		// crash before its fsyncs.
		_ = Save(d, raft.Snapshot{Index: 2, Term: 1, Data: []byte("interrupted")}, false)
		d.Crash(true)
		if s, err := Load(d, 1); err != nil || string(s.Data) != "durable" {
			t.Fatalf("seed %d: synced snapshot lost: %+v, %v", seed, s, err)
		}
		if s, err := Load(d, 2); err == nil && string(s.Data) != "interrupted" {
			t.Fatalf("seed %d: partial snapshot loaded: %+v", seed, s)
		} else if err != nil && !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("seed %d: interrupted save left a corrupt file under the real name: %v", seed, err)
		}
	}
}

func TestPrune(t *testing.T) {
	d := newDisk(1)
	for _, i := range []uint64{1, 2, 3} {
		_ = Save(d, raft.Snapshot{Index: i, Term: 1}, true)
	}
	f, _ := d.Create(Name(4) + ".tmp")
	_ = f.Close()
	f, _ = d.Create("wal-0000000000000001.log") // not ours: left alone
	_ = f.Close()
	if err := Prune(d, 2); err != nil {
		t.Fatal(err)
	}
	names, _ := d.List()
	if want := []string{Name(2), "wal-0000000000000001.log"}; !slices.Equal(names, want) {
		t.Fatalf("files = %v, want %v", names, want)
	}
}
