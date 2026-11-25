package sim

import (
	"errors"
	"io/fs"
	"sort"

	"github.com/20001172024/stormproof/storage"
)

// ErrSyncFailed is returned by Sync when the simulator injects an fsync
// failure. The data written since the last successful Sync is still not
// durable.
var ErrSyncFailed = errors.New("sim: injected fsync failure")

// errCrashed is returned when a file handle opened before a crash is used
// after it. A real process can't do this (it died), so seeing this error
// means a bug in the simulator wiring.
var errCrashed = errors.New("sim: file handle from before a crash")

// DiskConfig sets the fault probabilities for a simulated disk.
type DiskConfig struct {
	SyncFailProb float64 // chance each Sync fails
	SlowSyncProb float64 // chance each Sync is slow
	SlowSyncMax  Time    // a slow Sync adds a random delay in [0, SlowSyncMax)
}

// Disk is an in-memory implementation of storage.FS that models what
// survives a crash.
//
// Each file remembers how many of its bytes are durable (synced). Writes go
// to the in-memory view straight away, like an OS page cache, but only Sync
// makes them durable. Metadata operations (create, rename, remove) are
// durable immediately; this is a simplification, since real file systems
// also need the directory to be fsynced. It is noted in docs/design.md and
// may be tightened in M4.
type Disk struct {
	cfg   DiskConfig
	rng   *Rand
	files map[string]*memFile
	gen   int  // incremented on every crash, to invalidate old handles
	delay Time // slow-sync delay accumulated since the last takeDelay
}

type memFile struct {
	data   []byte
	synced int // data[:synced] is durable
}

var _ storage.FS = (*Disk)(nil) // compile-time check that Disk implements FS

// NewDisk returns an empty disk. rng drives its faults.
func NewDisk(cfg DiskConfig, rng *Rand) *Disk {
	return &Disk{cfg: cfg, rng: rng, files: make(map[string]*memFile)}
}

func notExist(op, name string) error {
	return &fs.PathError{Op: op, Path: name, Err: fs.ErrNotExist}
}

// Create implements storage.FS.
func (d *Disk) Create(name string) (storage.File, error) {
	f := &memFile{}
	d.files[name] = f
	return &memHandle{disk: d, file: f, gen: d.gen}, nil
}

// Append implements storage.FS.
func (d *Disk) Append(name string) (storage.File, error) {
	f, ok := d.files[name]
	if !ok {
		f = &memFile{}
		d.files[name] = f
	}
	return &memHandle{disk: d, file: f, gen: d.gen}, nil
}

// ReadFile implements storage.FS. It returns what a process would read:
// all written data, durable or not.
func (d *Disk) ReadFile(name string) ([]byte, error) {
	f, ok := d.files[name]
	if !ok {
		return nil, notExist("read", name)
	}
	return append([]byte(nil), f.data...), nil
}

// Rename implements storage.FS.
func (d *Disk) Rename(oldName, newName string) error {
	f, ok := d.files[oldName]
	if !ok {
		return notExist("rename", oldName)
	}
	delete(d.files, oldName)
	d.files[newName] = f
	return nil
}

// Remove implements storage.FS.
func (d *Disk) Remove(name string) error {
	if _, ok := d.files[name]; !ok {
		return notExist("remove", name)
	}
	delete(d.files, name)
	return nil
}

// List implements storage.FS.
func (d *Disk) List() ([]string, error) {
	return d.sortedNames(), nil
}

// sortedNames returns the file names in sorted order. Go deliberately
// randomizes map iteration order, so any loop over d.files that affects
// behaviour (like Crash, which draws random numbers per file) must go
// through this, or the same seed would give different results.
func (d *Disk) sortedNames() []string {
	names := make([]string, 0, len(d.files))
	for name := range d.files {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Crash models the node crashing. All open handles become unusable.
//
// If loseUnsynced is false (the process died but the machine kept running,
// like kill -9), the OS still writes out everything in its page cache, so
// nothing is lost. If it is true (power loss or kernel crash), each file
// keeps its durable bytes plus a random-length prefix of the bytes written
// after them: anywhere from none of them (lost) to all of them (lucky),
// with partial writes (torn) in between.
func (d *Disk) Crash(loseUnsynced bool) {
	d.gen++
	d.delay = 0
	for _, name := range d.sortedNames() {
		f := d.files[name]
		if loseUnsynced {
			unsynced := len(f.data) - f.synced
			keep := d.rng.Intn(unsynced + 1)
			f.data = f.data[:f.synced+keep]
		}
		f.synced = len(f.data) // whatever survived is now on disk
	}
}

// takeDelay returns the slow-sync delay accumulated since the last call
// and resets it. The simulator adds it to the send time of the messages the
// node produced, modelling a node that was stuck waiting on its disk.
func (d *Disk) takeDelay() Time {
	t := d.delay
	d.delay = 0
	return t
}

type memHandle struct {
	disk   *Disk
	file   *memFile
	gen    int
	closed bool
}

func (h *memHandle) check() error {
	if h.closed {
		return fs.ErrClosed
	}
	if h.gen != h.disk.gen {
		return errCrashed
	}
	return nil
}

func (h *memHandle) Write(p []byte) (int, error) {
	if err := h.check(); err != nil {
		return 0, err
	}
	h.file.data = append(h.file.data, p...)
	return len(p), nil
}

func (h *memHandle) Sync() error {
	if err := h.check(); err != nil {
		return err
	}
	d := h.disk
	if d.rng.Chance(d.cfg.SyncFailProb) {
		return ErrSyncFailed
	}
	if d.rng.Chance(d.cfg.SlowSyncProb) {
		d.delay += d.rng.Between(0, d.cfg.SlowSyncMax)
	}
	h.file.synced = len(h.file.data)
	return nil
}

func (h *memHandle) Close() error {
	if err := h.check(); err != nil {
		return err
	}
	h.closed = true
	return nil
}
