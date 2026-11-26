// Package wal is StormProof's write-ahead log: the durable record of a Raft
// node's HardState and log entries.
//
// Every raft.Ready is saved as one or two records (HardState, Entries) and
// a single fsync. Records are only ever appended. A later Entries record
// whose first index is already in the log replaces everything from that
// index on, which is how a truncation after a conflict with the leader is
// recorded, so there is no separate "truncate" record. This is the same
// contract as raft.Ready.Entries.
//
// The log is split into segment files, wal-<seq>.log, started afresh once
// the current one passes Options.SegmentSize.
//
// After a snapshot, Compact starts a new segment that begins with a
// snapshot record and holds everything still needed (the HardState and the
// entries after the snapshot), and deletes the older segments. Recovery
// starts from the newest segment that begins with a snapshot record and
// ignores anything older, so a crash partway through deleting old segments
// is harmless.
package wal

import (
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"

	"github.com/20001172024/stormproof/raft"
	"github.com/20001172024/stormproof/storage"
)

// SyncPolicy says whether Save waits for the disk.
type SyncPolicy int

const (
	// SyncAlways fsyncs every Save (and the directory when a segment is
	// created). This is the only safe setting: Raft's correctness depends on
	// a node never forgetting what it acknowledged.
	SyncAlways SyncPolicy = iota
	// SyncNever never fsyncs. A machine crash can lose acknowledged state,
	// which can break Raft's guarantees. It exists only to measure what the
	// fsyncs cost (M10).
	SyncNever
)

// DefaultSegmentSize is the size at which a new segment file is started.
const DefaultSegmentSize = 64 << 20

// Options configures a WAL.
type Options struct {
	SegmentSize int64 // 0 means DefaultSegmentSize
	Sync        SyncPolicy
}

// SnapshotMeta identifies a snapshot: the index and term of the last entry
// it covers. The snapshot's data lives in its own file (package snap).
type SnapshotMeta struct {
	Index uint64
	Term  raft.Term
}

// State is what recovery found on disk. Entries start right after
// Snapshot.Index (at 1 if there is no snapshot).
type State struct {
	HardState raft.HardState
	Snapshot  SnapshotMeta
	Entries   []raft.Entry
}

// WAL is an open write-ahead log. It is not safe for concurrent use.
type WAL struct {
	fs   storage.FS
	opts Options
	f    storage.File // the segment being appended to
	seq  uint64       // its sequence number
	size int64        // its size in bytes
	err  error        // first write error; the WAL refuses all use after one
}

const segmentPrefix, segmentSuffix = "wal-", ".log"

func segmentName(seq uint64) string {
	return fmt.Sprintf("%s%016x%s", segmentPrefix, seq, segmentSuffix)
}

func parseSegmentName(name string) (uint64, bool) {
	hex, ok := strings.CutPrefix(name, segmentPrefix)
	if !ok {
		return 0, false
	}
	hex, ok = strings.CutSuffix(hex, segmentSuffix)
	if !ok || len(hex) != 16 {
		return 0, false
	}
	seq, err := strconv.ParseUint(hex, 16, 64)
	return seq, err == nil
}

// Open recovers the log from fsys and opens it for appending.
//
// Recovery replays every record in order. The one kind of damage it
// repairs is a torn tail: the last segment ending partway through a record,
// which is what a crash during an append leaves behind. That record's
// fsync never completed, so nothing was acknowledged on the strength of it,
// and it is cut off. Anything else (a bad checksum anywhere, or a cut-short
// record in any segment but the last) is real corruption, and Open returns
// an error wrapping ErrCorrupt instead of guessing.
func Open(fsys storage.FS, opts Options) (*WAL, State, error) {
	if opts.SegmentSize == 0 {
		opts.SegmentSize = DefaultSegmentSize
	}
	w := &WAL{fs: fsys, opts: opts}

	names, err := fsys.List()
	if err != nil {
		return nil, State{}, err
	}
	var seqs []uint64
	for _, name := range names {
		if seq, ok := parseSegmentName(name); ok {
			seqs = append(seqs, seq) // List is sorted, and fixed-width hex sorts numerically
		} else if strings.HasPrefix(name, segmentPrefix) && strings.HasSuffix(name, ".tmp") {
			// Left over from a torn-tail repair interrupted by a crash; the
			// segment it would have replaced is still intact.
			if err := fsys.Remove(name); err != nil {
				return nil, State{}, err
			}
		}
	}
	// Start from the newest segment written by Compact: everything before it
	// is obsolete, and may be half deleted.
	start, err := latestCompaction(fsys, seqs)
	if err != nil {
		return nil, State{}, err
	}
	for _, seq := range seqs[:start] {
		if err := fsys.Remove(segmentName(seq)); err != nil {
			return nil, State{}, err
		}
	}
	seqs = seqs[start:]
	for i := 1; i < len(seqs); i++ {
		if seqs[i] != seqs[i-1]+1 {
			return nil, State{}, fmt.Errorf("%w: segment %s is missing", ErrCorrupt, segmentName(seqs[i-1]+1))
		}
	}

	var st State
	if len(seqs) == 0 {
		if err := w.startSegment(1); err != nil {
			return nil, State{}, err
		}
		return w, st, nil
	}

	for i, seq := range seqs {
		name := segmentName(seq)
		data, err := fsys.ReadFile(name)
		if err != nil {
			return nil, State{}, err
		}
		last := i == len(seqs)-1
		valid, err := replay(&st, data, last)
		if err != nil {
			return nil, State{}, fmt.Errorf("%s: %w", name, err)
		}
		if valid < len(data) {
			// Only possible for the last segment: cut off the torn record.
			if err := w.rewrite(name, data[:valid]); err != nil {
				return nil, State{}, err
			}
		}
		if last {
			if w.f, err = fsys.Append(name); err != nil {
				return nil, State{}, err
			}
			w.seq, w.size = seq, int64(valid)
		}
	}
	return w, st, nil
}

// replay applies the records in one segment to st and returns how many
// bytes of data were valid. A torn record is accepted only at the end of
// the last segment.
func replay(st *State, data []byte, last bool) (int, error) {
	off := 0
	for off < len(data) {
		typ, payload, n, err := readRecord(data[off:])
		if errors.Is(err, errTorn) && last {
			return off, nil // torn tail: everything before it is good
		}
		if errors.Is(err, errTorn) {
			return 0, fmt.Errorf("%w: record cut short at offset %d of a segment that is not the last", ErrCorrupt, off)
		}
		if err != nil {
			return 0, fmt.Errorf("%w at offset %d", err, off)
		}
		if err := apply(st, typ, payload); err != nil {
			return 0, fmt.Errorf("%w at offset %d", err, off)
		}
		off += n
	}
	return off, nil
}

func apply(st *State, typ recordType, payload []byte) error {
	switch typ {
	case recHardState:
		hs, err := decodeHardState(payload)
		if err != nil {
			return err
		}
		st.HardState = hs
	case recSnapshot:
		m, err := decodeSnapshot(payload)
		if err != nil {
			return err
		}
		if m.Index <= st.Snapshot.Index {
			return fmt.Errorf("%w: snapshot at %d after one at %d", ErrCorrupt, m.Index, st.Snapshot.Index)
		}
		// The same rule as raft's InstallSnapshot: keep the entries after
		// the snapshot if the log has its last entry, otherwise drop them all.
		if off := m.Index - st.Snapshot.Index; off <= uint64(len(st.Entries)) && st.Entries[off-1].Term == m.Term {
			st.Entries = append([]raft.Entry(nil), st.Entries[off:]...)
		} else {
			st.Entries = nil
		}
		st.Snapshot = m
	case recEntries:
		es, err := decodeEntries(payload)
		if err != nil {
			return err
		}
		for _, e := range es {
			// An entry may replace the tail of the log (a truncation) but
			// never leave a gap after it, nor reach back into the snapshot.
			first := st.Snapshot.Index + 1
			if e.Index < first || e.Index > first+uint64(len(st.Entries)) {
				return fmt.Errorf("%w: entry %d after a log of %d entries from %d", ErrCorrupt, e.Index, len(st.Entries), first)
			}
			st.Entries = append(st.Entries[:e.Index-first], e)
		}
	default:
		return fmt.Errorf("%w: unknown record type %d", ErrCorrupt, typ)
	}
	return nil
}

// Save durably appends a HardState (if hs is non-nil) and entries, with a
// single fsync. When it returns nil (under SyncAlways), both survive any
// crash. After a failed Save the WAL's state on disk is unknown, so every
// later call fails too: the caller should treat the node as crashed.
func (w *WAL) Save(hs *raft.HardState, entries []raft.Entry) error {
	if w.err != nil {
		return w.err
	}
	var buf []byte
	if hs != nil {
		buf = appendRecord(buf, recHardState, encodeHardState(*hs))
	}
	if len(entries) > 0 {
		buf = appendRecord(buf, recEntries, encodeEntries(entries))
	}
	if len(buf) == 0 {
		return nil
	}
	if err := w.write(buf); err != nil {
		w.err = err
		return err
	}
	if w.size >= w.opts.SegmentSize {
		if err := w.rotate(); err != nil {
			w.err = err
			return err
		}
	}
	return nil
}

func (w *WAL) write(buf []byte) error {
	if _, err := w.f.Write(buf); err != nil {
		return err
	}
	w.size += int64(len(buf))
	if w.opts.Sync == SyncAlways {
		return w.f.Sync()
	}
	return nil
}

// rotate closes the current segment (already synced by the last Save) and
// starts the next one.
func (w *WAL) rotate() error {
	if err := w.f.Close(); err != nil {
		return err
	}
	return w.startSegment(w.seq + 1)
}

// startSegment creates segment seq and makes its existence durable before
// anything is written to it. Without the directory sync, a machine crash
// could make the whole file vanish, taking acknowledged records with it.
func (w *WAL) startSegment(seq uint64) error {
	f, err := w.fs.Create(segmentName(seq))
	if err != nil {
		return err
	}
	if w.opts.Sync == SyncAlways {
		if err := w.fs.SyncDir(); err != nil {
			_ = f.Close()
			return err
		}
	}
	w.f, w.seq, w.size = f, seq, 0
	return nil
}

// rewrite atomically replaces a segment with data: write a temp file, sync
// it, rename it over the segment, sync the directory.
func (w *WAL) rewrite(name string, data []byte) error {
	tmp := name + ".tmp"
	f, err := w.fs.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if w.opts.Sync == SyncAlways {
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := w.fs.Rename(tmp, name); err != nil {
		return err
	}
	if w.opts.Sync == SyncAlways {
		return w.fs.SyncDir()
	}
	return nil
}

// latestCompaction returns the position in seqs of the newest segment that
// begins with a snapshot record (a segment written by Compact), or 0.
func latestCompaction(fsys storage.FS, seqs []uint64) (int, error) {
	for i := len(seqs) - 1; i > 0; i-- {
		data, err := fsys.ReadFile(segmentName(seqs[i]))
		if err != nil {
			return 0, err
		}
		if typ, _, _, err := readRecord(data); err == nil && typ == recSnapshot {
			return i, nil
		}
	}
	return 0, nil
}

// Compact records a snapshot and drops everything it makes obsolete. The
// snapshot itself must already be saved (package snap). hs is the current
// HardState and entries are the log entries after the snapshot.
//
// The order is what makes it crash-safe:
//
//  1. Write a new segment, under a temporary name, holding a snapshot
//     record, the HardState and the entries, and fsync it.
//  2. Rename it into place as the next segment and fsync the directory.
//     From this moment recovery starts here.
//  3. Delete the older segments.
//
// A crash before step 2 leaves the old segments in charge, and the temp
// file is removed on Open; a crash during step 3 leaves some old segments
// behind, which recovery ignores and deletes.
func (w *WAL) Compact(m SnapshotMeta, hs raft.HardState, entries []raft.Entry) error {
	if w.err != nil {
		return w.err
	}
	if err := w.compact(m, hs, entries); err != nil {
		w.err = err
		return err
	}
	return nil
}

func (w *WAL) compact(m SnapshotMeta, hs raft.HardState, entries []raft.Entry) error {
	buf := appendRecord(nil, recSnapshot, encodeSnapshot(m))
	buf = appendRecord(buf, recHardState, encodeHardState(hs))
	if len(entries) > 0 {
		buf = appendRecord(buf, recEntries, encodeEntries(entries))
	}
	seq := w.seq + 1
	name := segmentName(seq)
	if err := w.rewrite(name, buf); err != nil { // steps 1 and 2
		return err
	}
	if err := w.f.Close(); err != nil {
		return err
	}
	for old := seq - 1; old >= 1; old-- { // step 3, newest first
		err := w.fs.Remove(segmentName(old))
		if errors.Is(err, fs.ErrNotExist) {
			break // deleted by an earlier compaction
		}
		if err != nil {
			return err
		}
	}
	f, err := w.fs.Append(name)
	if err != nil {
		return err
	}
	w.f, w.seq, w.size = f, seq, int64(len(buf))
	return nil
}

// Close closes the current segment.
func (w *WAL) Close() error {
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}
