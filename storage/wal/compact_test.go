package wal

import (
	"fmt"
	"testing"

	"github.com/20001172024/stormproof/raft"
	"github.com/20001172024/stormproof/sim"
	"github.com/20001172024/stormproof/storage"
)

func entries(from, to uint64, term raft.Term) []raft.Entry {
	var es []raft.Entry
	for i := from; i <= to; i++ {
		es = append(es, ent(i, term, fmt.Sprint("e", i)))
	}
	return es
}

func TestCompactAndRecover(t *testing.T) {
	d := newDisk(1)
	w, _ := mustOpen(t, d, Options{SegmentSize: 200})
	if err := w.Save(hs(2, 1), entries(1, 30, 2)); err != nil {
		t.Fatal(err)
	}
	segsBefore, _ := d.List()
	if err := w.Compact(SnapshotMeta{Index: 20, Term: 2}, *hs(2, 1), entries(21, 30, 2)); err != nil {
		t.Fatal(err)
	}
	if err := w.Save(hs(3, 0), entries(31, 32, 3)); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()

	names, _ := d.List()
	for _, old := range segsBefore {
		for _, n := range names {
			if n == old {
				t.Fatalf("segment %s from before the compaction still exists: %v", old, names)
			}
		}
	}
	_, st := mustOpen(t, d, Options{SegmentSize: 200})
	if st.Snapshot != (SnapshotMeta{Index: 20, Term: 2}) || st.HardState != *hs(3, 0) {
		t.Fatalf("recovered snapshot %+v hardstate %+v", st.Snapshot, st.HardState)
	}
	if len(st.Entries) != 12 || st.Entries[0].Index != 21 || st.Entries[11].Index != 32 {
		t.Fatalf("recovered %d entries starting at %d", len(st.Entries), st.Entries[0].Index)
	}
}

// The snapshot record follows the InstallSnapshot rule: keep the entries
// after it if the log has its last entry, otherwise drop them.
func TestSnapshotRecordReplay(t *testing.T) {
	for _, c := range []struct {
		name string
		m    SnapshotMeta
		keep int
	}{
		{"log has the snapshot's last entry", SnapshotMeta{Index: 3, Term: 1}, 2},
		{"log has a different entry there", SnapshotMeta{Index: 3, Term: 2}, 0},
		{"log is shorter than the snapshot", SnapshotMeta{Index: 9, Term: 1}, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			st := State{Entries: entries(1, 5, 1)}
			if err := apply(&st, recSnapshot, encodeSnapshot(c.m)); err != nil {
				t.Fatal(err)
			}
			if len(st.Entries) != c.keep || (c.keep > 0 && st.Entries[0].Index != c.m.Index+1) {
				t.Fatalf("kept %d entries %+v, want %d", len(st.Entries), st.Entries, c.keep)
			}
		})
	}
}

// Crash at each step of Compact: before the new segment is renamed into
// place, and after it but before the old segments are deleted.
func TestCompactIsCrashSafe(t *testing.T) {
	setup := func() (*sim.Disk, *WAL) {
		d := newDisk(1)
		w, _ := mustOpen(t, d, Options{SegmentSize: 100})
		if err := w.Save(hs(1, 1), entries(1, 10, 1)); err != nil {
			t.Fatal(err)
		}
		return d, w
	}
	full := State{HardState: *hs(1, 1), Entries: entries(1, 10, 1)}
	compacted := State{HardState: *hs(1, 1), Snapshot: SnapshotMeta{Index: 6, Term: 1}, Entries: entries(7, 10, 1)}

	t.Run("crash before the rename", func(t *testing.T) {
		d, w := setup()
		buf := appendRecord(nil, recSnapshot, encodeSnapshot(compacted.Snapshot))
		f, _ := d.Create(segmentName(w.seq+1) + ".tmp")
		_, _ = f.Write(buf)
		_ = f.Sync()
		_ = d.SyncDir()
		d.Crash(true)
		_, got := mustOpen(t, d, Options{SegmentSize: 100})
		if !equalState(got, full) {
			t.Fatalf("recovered %+v, want the state from before the compaction", got)
		}
		if names, _ := d.List(); len(names) == 0 || names[len(names)-1] == segmentName(w.seq+1)+".tmp" {
			t.Fatalf("temp file not cleaned up: %v", names)
		}
	})
	t.Run("crash before old segments are deleted", func(t *testing.T) {
		d, w := setup()
		before, _ := d.List()
		buf := appendRecord(nil, recSnapshot, encodeSnapshot(compacted.Snapshot))
		buf = appendRecord(buf, recHardState, encodeHardState(compacted.HardState))
		buf = appendRecord(buf, recEntries, encodeEntries(compacted.Entries))
		if err := w.rewrite(segmentName(w.seq+1), buf); err != nil {
			t.Fatal(err)
		}
		d.Crash(true)
		_, got := mustOpen(t, d, Options{SegmentSize: 100})
		if !equalState(got, compacted) {
			t.Fatalf("recovered %+v, want the compacted state", got)
		}
		if after, _ := d.List(); len(after) != 1 {
			t.Fatalf("old segments %v not removed on recovery (before: %v)", after, before)
		}
	})
}

// Saves and compactions in random order, then a machine crash: everything
// acknowledged survives.
func TestCompactionSurvivesMachineCrash(t *testing.T) {
	for seed := uint64(0); seed < 200; seed++ {
		d := newDisk(seed)
		r := sim.NewRand(seed, "ops")
		w, _ := mustOpen(t, d, Options{SegmentSize: 64})
		var want State
		next := uint64(1)
		for op := r.Intn(30); op > 0; op-- {
			if r.Chance(0.2) && next > want.Snapshot.Index+1 {
				idx := want.Snapshot.Index + 1 + uint64(r.Intn(int(next-want.Snapshot.Index-1)))
				want.Entries = want.Entries[idx-want.Snapshot.Index:]
				want.Snapshot = SnapshotMeta{Index: idx, Term: 1}
				if err := w.Compact(want.Snapshot, want.HardState, want.Entries); err != nil {
					t.Fatal(err)
				}
				continue
			}
			e := ent(next, 1, "x")
			want.HardState = *hs(raft.Term(next), 1)
			if err := w.Save(&want.HardState, []raft.Entry{e}); err != nil {
				t.Fatal(err)
			}
			want.Entries = append(want.Entries, e)
			next++
		}
		d.Crash(true)
		_, got, err := Open(d, Options{SegmentSize: 64})
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if !equalState(got, want) || got.Snapshot != want.Snapshot {
			t.Fatalf("seed %d: recovered snapshot %+v with %d entries, want %+v with %d",
				seed, got.Snapshot, len(got.Entries), want.Snapshot, len(want.Entries))
		}
	}
}

// crashingFS passes calls through to a sim.Disk until a set number of
// changes (writes, syncs, creates, renames, removes) have happened, then
// crashes the disk (losing unsynced data) and fails every later call. It
// lets a test crash the machine at every step inside an operation.
type crashingFS struct {
	*sim.Disk
	left    int // changes allowed before the crash
	crashed bool
}

var errMachineDown = fmt.Errorf("machine crashed")

func (c *crashingFS) step() error {
	if c.crashed {
		return errMachineDown
	}
	if c.left == 0 {
		c.crashed = true
		c.Crash(true)
		return errMachineDown
	}
	c.left--
	return nil
}

type crashingFile struct {
	storage.File
	fs *crashingFS
}

func (f crashingFile) Write(p []byte) (int, error) {
	if err := f.fs.step(); err != nil {
		return 0, err
	}
	return f.File.Write(p)
}

func (f crashingFile) Sync() error {
	if err := f.fs.step(); err != nil {
		return err
	}
	return f.File.Sync()
}

func (c *crashingFS) Create(name string) (storage.File, error) {
	if err := c.step(); err != nil {
		return nil, err
	}
	f, err := c.Disk.Create(name)
	return crashingFile{f, c}, err
}

func (c *crashingFS) Append(name string) (storage.File, error) {
	if err := c.step(); err != nil {
		return nil, err
	}
	f, err := c.Disk.Append(name)
	return crashingFile{f, c}, err
}

func (c *crashingFS) Rename(a, b string) error {
	if err := c.step(); err != nil {
		return err
	}
	return c.Disk.Rename(a, b)
}

func (c *crashingFS) Remove(name string) error {
	if err := c.step(); err != nil {
		return err
	}
	return c.Disk.Remove(name)
}

func (c *crashingFS) SyncDir() error {
	if err := c.step(); err != nil {
		return err
	}
	return c.Disk.SyncDir()
}

// Crash the machine at every step inside Compact. Recovery must always
// find either the whole state from before the compaction or the whole
// state after it.
func TestCrashAtEveryStepOfCompact(t *testing.T) {
	before := State{HardState: *hs(1, 1), Entries: entries(1, 10, 1)}
	after := State{HardState: *hs(1, 1), Snapshot: SnapshotMeta{Index: 6, Term: 1}, Entries: entries(7, 10, 1)}
	sawBefore, sawAfter := false, false
	for crashAt := 0; ; crashAt++ {
		d := newDisk(uint64(crashAt))
		w, _ := mustOpen(t, d, Options{SegmentSize: 100})
		if err := w.Save(&before.HardState, before.Entries); err != nil {
			t.Fatal(err)
		}
		cfs := &crashingFS{Disk: d, left: crashAt}
		w.fs = cfs
		err := w.Compact(after.Snapshot, after.HardState, after.Entries)
		if !cfs.crashed {
			if err != nil {
				t.Fatal(err)
			}
			break // Compact finished before reaching crashAt changes: all steps covered
		}
		_, got, err := Open(d, Options{SegmentSize: 100})
		if err != nil {
			t.Fatalf("crash after %d steps: recovery failed: %v", crashAt, err)
		}
		switch {
		case equalState(got, before) && got.Snapshot == before.Snapshot:
			sawBefore = true
		case equalState(got, after) && got.Snapshot == after.Snapshot:
			sawAfter = true
		default:
			t.Fatalf("crash after %d steps: recovered snapshot %+v with %d entries: neither before nor after",
				crashAt, got.Snapshot, len(got.Entries))
		}
	}
	if !sawBefore || !sawAfter {
		t.Fatalf("saw before=%v after=%v: expected crash points on both sides", sawBefore, sawAfter)
	}
}
