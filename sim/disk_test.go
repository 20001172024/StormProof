package sim

import (
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"testing"

	"github.com/20001172024/stormproof/storage"
)

func newTestDisk(cfg DiskConfig) *Disk { return NewDisk(cfg, NewRand(1, "disk-test")) }

// create makes a file and syncs the directory, so its name survives crashes.
func create(t *testing.T, d *Disk, name string) storage.File {
	t.Helper()
	f, err := d.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.SyncDir(); err != nil {
		t.Fatal(err)
	}
	return f
}

func exists(d *Disk, name string) bool {
	_, err := d.ReadFile(name)
	return err == nil
}

func write(t *testing.T, f storage.File, s string) {
	t.Helper()
	if _, err := f.Write([]byte(s)); err != nil {
		t.Fatal(err)
	}
}

func read(t *testing.T, d *Disk, name string) string {
	t.Helper()
	b, err := d.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestDiskCrashKeepsSyncedData(t *testing.T) {
	for _, lose := range []bool{false, true} {
		d := newTestDisk(DiskConfig{})
		f := create(t, d, "a")
		write(t, f, "hello")
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
		d.Crash(lose)
		if got := read(t, d, "a"); got != "hello" {
			t.Errorf("loseUnsynced=%v: got %q, want %q", lose, got, "hello")
		}
	}
}

func TestDiskProcessCrashKeepsUnsyncedData(t *testing.T) {
	d := newTestDisk(DiskConfig{})
	f, _ := d.Create("a")
	write(t, f, "not synced")
	d.Crash(false)
	if got := read(t, d, "a"); got != "not synced" {
		t.Fatalf("got %q, want all data kept after a process crash", got)
	}
}

// After a machine crash a file must hold its synced bytes followed by a
// prefix of the unsynced ones: never garbage, never reordered. Over many
// seeds we should see all three outcomes: all lost, torn, all kept.
func TestDiskMachineCrashLeavesPrefix(t *testing.T) {
	const synced, unsynced = "durable|", "0123456789"
	seen := map[string]bool{}
	for seed := uint64(0); seed < 500; seed++ {
		d := NewDisk(DiskConfig{}, NewRand(seed, "disk-test"))
		f := create(t, d, "a")
		write(t, f, synced)
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
		write(t, f, unsynced)
		d.Crash(true)

		got := read(t, d, "a")
		want := synced + unsynced
		if len(got) < len(synced) || got != want[:len(got)] {
			t.Fatalf("seed %d: got %q, want a prefix of %q at least %q long", seed, got, want, synced)
		}
		switch len(got) {
		case len(synced):
			seen["lost"] = true
		case len(want):
			seen["kept"] = true
		default:
			seen["torn"] = true
		}
	}
	for _, outcome := range []string{"lost", "torn", "kept"} {
		if !seen[outcome] {
			t.Errorf("never saw outcome %q in 500 crashes", outcome)
		}
	}
}

// What survives one crash is durable: a second crash must not lose it.
func TestDiskSurvivorsAreDurable(t *testing.T) {
	d := newTestDisk(DiskConfig{})
	f := create(t, d, "a")
	write(t, f, "0123456789")
	d.Crash(true)
	after1 := read(t, d, "a")
	d.Crash(true)
	if after2 := read(t, d, "a"); after2 != after1 {
		t.Fatalf("second crash changed file from %q to %q", after1, after2)
	}
}

func TestDiskSyncFailureLeavesDataUnsynced(t *testing.T) {
	d := newTestDisk(DiskConfig{SyncFailProb: 1})
	f, _ := d.Create("a")
	write(t, f, "data")
	if err := f.Sync(); !errors.Is(err, ErrSyncFailed) {
		t.Fatalf("Sync error = %v, want ErrSyncFailed", err)
	}
	if d.files["a"].synced != 0 {
		t.Fatal("failed Sync marked data as durable")
	}
}

func TestDiskSlowSyncAddsDelay(t *testing.T) {
	d := newTestDisk(DiskConfig{SlowSyncProb: 1, SlowSyncMax: 10 * Millisecond})
	f, _ := d.Create("a")
	for i := 0; i < 20; i++ {
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	total := d.takeDelay()
	if total <= 0 || total >= 20*10*Millisecond {
		t.Fatalf("delay after 20 slow syncs = %v, want in (0, 200ms)", total)
	}
	if d.takeDelay() != 0 {
		t.Fatal("takeDelay did not reset")
	}
}

func TestDiskHandleUnusableAfterCrash(t *testing.T) {
	d := newTestDisk(DiskConfig{})
	f, _ := d.Create("a")
	d.Crash(false)
	if _, err := f.Write([]byte("x")); err == nil {
		t.Fatal("Write on a pre-crash handle succeeded")
	}
}

func TestDiskCloseThenWrite(t *testing.T) {
	d := newTestDisk(DiskConfig{})
	f, _ := d.Create("a")
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("x")); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("Write after Close: err = %v, want fs.ErrClosed", err)
	}
}

func TestDiskFileOperations(t *testing.T) {
	d := newTestDisk(DiskConfig{})

	f, _ := d.Append("log")
	write(t, f, "one,")
	f, _ = d.Append("log")
	write(t, f, "two")
	if got := read(t, d, "log"); got != "one,two" {
		t.Fatalf("Append: got %q", got)
	}

	f, _ = d.Create("log")
	write(t, f, "fresh")
	if got := read(t, d, "log"); got != "fresh" {
		t.Fatalf("Create should truncate: got %q", got)
	}

	f, _ = d.Create("tmp")
	write(t, f, "new")
	if err := d.Rename("tmp", "log"); err != nil {
		t.Fatal(err)
	}
	if got := read(t, d, "log"); got != "new" {
		t.Fatalf("Rename: got %q", got)
	}

	for _, name := range []string{"c", "b"} {
		if _, err := d.Create(name); err != nil {
			t.Fatal(err)
		}
	}
	names, _ := d.List()
	if want := []string{"b", "c", "log"}; !slices.Equal(names, want) {
		t.Fatalf("List = %v, want %v", names, want)
	}

	if err := d.Remove("b"); err != nil {
		t.Fatal(err)
	}
	for _, err := range []error{
		d.Remove("b"),
		d.Rename("missing", "x"),
		func() error { _, err := d.ReadFile("missing"); return err }(),
	} {
		if !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("err = %v, want fs.ErrNotExist", err)
		}
	}
}

// Two disks with the same seed and the same writes must end up identical
// after a machine crash. With many files holding unsynced data, this fails
// if Crash ever walks the file map in Go's (deliberately random) map order.
// The end-to-end determinism test can't catch that, because the toy node
// only has one file at a time.
func TestDiskCrashIsDeterministic(t *testing.T) {
	snapshot := func() map[string]string {
		d := NewDisk(DiskConfig{}, NewRand(9, "disk-test"))
		for i := 0; i < 30; i++ {
			f := create(t, d, fmt.Sprintf("file-%02d", i))
			write(t, f, "0123456789abcdef")
		}
		d.Crash(true)
		out := map[string]string{}
		names, _ := d.List()
		for _, name := range names {
			out[name] = read(t, d, name)
		}
		return out
	}
	want := snapshot()
	for run := 0; run < 20; run++ {
		got := snapshot()
		for name, w := range want {
			if got[name] != w {
				t.Fatalf("run %d: file %s holds %q after crash, first run had %q", run, name, got[name], w)
			}
		}
	}
}

func TestDiskStopFaults(t *testing.T) {
	d := newTestDisk(DiskConfig{SyncFailProb: 1, SlowSyncProb: 1, SlowSyncMax: Second})
	f, _ := d.Create("a")
	d.StopFaults()
	for i := 0; i < 10; i++ {
		if err := f.Sync(); err != nil {
			t.Fatalf("Sync failed after StopFaults: %v", err)
		}
	}
	if d.takeDelay() != 0 {
		t.Fatal("slow syncs after StopFaults")
	}
}

// Directory changes survive a machine crash only after SyncDir; a process
// crash keeps them.
func TestDiskDirectoryDurability(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		d := newTestDisk(DiskConfig{})
		f, _ := d.Create("a")
		write(t, f, "x")
		_ = f.Sync()
		d.Crash(false)
		if !exists(d, "a") {
			t.Fatal("process crash lost a new file")
		}
		d2 := newTestDisk(DiskConfig{})
		f, _ = d2.Create("a")
		write(t, f, "x")
		_ = f.Sync() // data synced, but not the directory
		d2.Crash(true)
		if exists(d2, "a") {
			t.Fatal("machine crash kept a file whose creation was never synced")
		}
	})
	t.Run("rename reverts", func(t *testing.T) {
		d := newTestDisk(DiskConfig{})
		old := create(t, d, "state")
		write(t, old, "old")
		_ = old.Sync()
		tmp := create(t, d, "tmp")
		write(t, tmp, "new")
		_ = tmp.Sync()
		if err := d.Rename("tmp", "state"); err != nil {
			t.Fatal(err)
		}
		d.Crash(true) // before SyncDir
		if got := read(t, d, "state"); got != "old" {
			t.Fatalf("state = %q after crash before SyncDir, want the old contents", got)
		}
	})
	t.Run("rename durable after SyncDir", func(t *testing.T) {
		d := newTestDisk(DiskConfig{})
		tmp := create(t, d, "tmp")
		write(t, tmp, "new")
		_ = tmp.Sync()
		_ = d.Rename("tmp", "state")
		_ = d.SyncDir()
		d.Crash(true)
		if got := read(t, d, "state"); got != "new" || exists(d, "tmp") {
			t.Fatalf("after SyncDir and crash: state = %q, tmp exists = %v", got, exists(d, "tmp"))
		}
	})
	t.Run("remove reverts", func(t *testing.T) {
		d := newTestDisk(DiskConfig{})
		f := create(t, d, "a")
		write(t, f, "x")
		_ = f.Sync()
		_ = d.Remove("a")
		d.Crash(true)
		if !exists(d, "a") {
			t.Fatal("a remove that was never synced survived a machine crash")
		}
	})
	t.Run("truncate is in place", func(t *testing.T) {
		d := newTestDisk(DiskConfig{})
		f := create(t, d, "a")
		write(t, f, "durable")
		_ = f.Sync()
		_, _ = d.Create("a") // truncate; no sync of anything
		d.Crash(true)
		if got := read(t, d, "a"); got != "" {
			t.Fatalf("a = %q: truncation should hit the file in place, not be undone", got)
		}
	})
}
