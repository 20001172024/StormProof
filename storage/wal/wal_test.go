package wal

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/20001172024/stormproof/raft"
	"github.com/20001172024/stormproof/sim"
)

func newDisk(seed uint64) *sim.Disk {
	return sim.NewDisk(sim.DiskConfig{}, sim.NewRand(seed, "wal-test"))
}

func mustOpen(t *testing.T, d *sim.Disk, opts Options) (*WAL, State) {
	t.Helper()
	w, st, err := Open(d, opts)
	if err != nil {
		t.Fatal(err)
	}
	return w, st
}

func ent(i uint64, t raft.Term, data string) raft.Entry {
	e := raft.Entry{Index: i, Term: t}
	if data != "" {
		e.Data = []byte(data)
	}
	return e
}

// save is one call to Save, and the state expected after it.
type save struct {
	hs      *raft.HardState
	entries []raft.Entry
}

func hs(term raft.Term, vote raft.NodeID) *raft.HardState {
	return &raft.HardState{Term: term, Vote: vote}
}

// script is a sequence of saves including a truncation, used by several
// tests, with the expected state after each.
func script() ([]save, []State) {
	saves := []save{
		{hs: hs(1, 1)},
		{entries: []raft.Entry{ent(1, 1, ""), ent(2, 1, "a")}},
		{hs: hs(2, 0), entries: []raft.Entry{ent(3, 2, "b")}},
		{entries: []raft.Entry{ent(4, 2, "c"), {Index: 5, Term: 2, Type: raft.EntryConfChange, Data: raft.EncodeVoters([]raft.NodeID{1, 2})}}},
		{hs: hs(3, 3), entries: []raft.Entry{ent(3, 3, "replaces b, c, d")}}, // truncation
		{entries: []raft.Entry{ent(4, 3, "e")}},
	}
	var states []State
	var st State
	for _, s := range saves {
		if s.hs != nil {
			st.HardState = *s.hs
		}
		for _, e := range s.entries {
			st.Entries = append(append([]raft.Entry(nil), st.Entries[:e.Index-1]...), e)
		}
		states = append(states, State{HardState: st.HardState, Entries: append([]raft.Entry(nil), st.Entries...)})
	}
	return saves, states
}

func equalState(a, b State) bool {
	if a.HardState != b.HardState || len(a.Entries) != len(b.Entries) {
		return false
	}
	for i := range a.Entries {
		if a.Entries[i].Index != b.Entries[i].Index || a.Entries[i].Term != b.Entries[i].Term ||
			!bytes.Equal(a.Entries[i].Data, b.Entries[i].Data) {
			return false
		}
	}
	return true
}

func TestOpenEmpty(t *testing.T) {
	d := newDisk(1)
	w, st := mustOpen(t, d, Options{})
	if !reflect.DeepEqual(st, State{}) {
		t.Fatalf("empty WAL recovered %+v", st)
	}
	if names, _ := d.List(); len(names) != 1 || names[0] != segmentName(1) {
		t.Fatalf("files = %v, want just the first segment", names)
	}
	_ = w.Close()
}

func TestRoundTripWithTruncation(t *testing.T) {
	saves, states := script()
	d := newDisk(1)
	w, _ := mustOpen(t, d, Options{})
	for _, s := range saves {
		if err := w.Save(s.hs, s.entries); err != nil {
			t.Fatal(err)
		}
	}
	_ = w.Close()
	_, got := mustOpen(t, d, Options{})
	if want := states[len(states)-1]; !equalState(got, want) {
		t.Fatalf("recovered %+v\nwant %+v", got, want)
	}
}

// Appending after a reopen continues the same log.
func TestReopenAndContinue(t *testing.T) {
	saves, states := script()
	d := newDisk(1)
	for _, s := range saves {
		w, _ := mustOpen(t, d, Options{})
		if err := w.Save(s.hs, s.entries); err != nil {
			t.Fatal(err)
		}
		_ = w.Close()
	}
	if _, got := mustOpen(t, d, Options{}); !equalState(got, states[len(states)-1]) {
		t.Fatalf("recovered %+v", got)
	}
}

func TestRotation(t *testing.T) {
	d := newDisk(1)
	w, _ := mustOpen(t, d, Options{SegmentSize: 100})
	var want State
	for i := uint64(1); i <= 50; i++ {
		e := ent(i, 1, fmt.Sprintf("entry %d", i))
		if err := w.Save(hs(1, 1), []raft.Entry{e}); err != nil {
			t.Fatal(err)
		}
		want.Entries = append(want.Entries, e)
	}
	want.HardState = *hs(1, 1)
	_ = w.Close()
	names, _ := d.List()
	if len(names) < 10 {
		t.Fatalf("only %d segments after 50 saves with a 100-byte limit", len(names))
	}
	if _, got := mustOpen(t, d, Options{SegmentSize: 100}); !equalState(got, want) {
		t.Fatalf("recovered %d entries, want %d", len(got.Entries), len(want.Entries))
	}
}

// The central recovery property. Cut the log at every byte offset, as a
// crash during an append could: recovery must succeed and return exactly
// the state after the last record that is complete. Never an error, never
// garbage.
func TestTornTailAtEveryOffset(t *testing.T) {
	saves, states := script()
	d := newDisk(1)
	w, _ := mustOpen(t, d, Options{})
	var ends []int // byte offset where each save's records end
	for _, s := range saves {
		if err := w.Save(s.hs, s.entries); err != nil {
			t.Fatal(err)
		}
		ends = append(ends, int(w.size))
	}
	_ = w.Close()
	full, _ := d.ReadFile(segmentName(1))

	for cut := 0; cut <= len(full); cut++ {
		td := newDisk(1)
		f, _ := td.Create(segmentName(1))
		_, _ = f.Write(full[:cut])
		_ = f.Sync()
		_ = td.SyncDir()

		w2, got, err := Open(td, Options{})
		if err != nil {
			t.Fatalf("cut at %d of %d: %v", cut, len(full), err)
		}
		// Expected: the state after the last save whose records all fit.
		// (A save's HardState record can survive without its Entries
		// record; handle that by checking against the record boundary.)
		want := State{}
		for i, end := range ends {
			if end <= cut {
				want = states[i]
			}
		}
		if !equalState(got, want) && !partialSaveOK(got, want, saves, states, ends, cut) {
			t.Fatalf("cut at %d: recovered %+v\nwant %+v", cut, got, want)
		}
		// The torn bytes are gone, and the WAL keeps working afterwards.
		if err := w2.Save(hs(9, 9), nil); err != nil {
			t.Fatalf("cut at %d: Save after recovery: %v", cut, err)
		}
		_ = w2.Close()
		if _, again, err := Open(td, Options{}); err != nil || again.HardState != *hs(9, 9) {
			t.Fatalf("cut at %d: reopen after repair: %+v, %v", cut, again, err)
		}
	}
}

// partialSaveOK accepts the one legitimate in-between state: a save with
// both a HardState and Entries record cut between the two, so the new
// HardState is there but not the entries.
func partialSaveOK(got, want State, saves []save, states []State, ends []int, cut int) bool {
	for i, s := range saves {
		start := 0
		if i > 0 {
			start = ends[i-1]
		}
		if s.hs == nil || len(s.entries) == 0 || cut <= start || cut >= ends[i] {
			continue
		}
		prev := State{}
		if i > 0 {
			prev = states[i-1]
		}
		return got.HardState == *s.hs && equalState(State{HardState: *s.hs, Entries: prev.Entries}, got)
	}
	return false
}

func writeSegments(t *testing.T, d *sim.Disk, segs ...[]byte) {
	t.Helper()
	for i, b := range segs {
		f, _ := d.Create(segmentName(uint64(i + 1)))
		_, _ = f.Write(b)
		_ = f.Sync()
	}
	_ = d.SyncDir()
}

func TestCorruptionIsRefused(t *testing.T) {
	rec1 := appendRecord(nil, recHardState, encodeHardState(*hs(1, 1)))
	rec2 := appendRecord(nil, recEntries, encodeEntries([]raft.Entry{ent(1, 1, "x")}))
	flip := func(b []byte, i int) []byte {
		c := append([]byte(nil), b...)
		c[i] ^= 0xff
		return c
	}
	cases := map[string][][]byte{
		"bad checksum in the middle":              {append(flip(rec1, 10), rec2...)},
		"bad checksum in the last record":         {append(rec1, flip(rec2, len(rec2)-1)...)},
		"torn record in a segment before last":    {rec1[:5], rec2},
		"unknown record type":                     {appendRecord(nil, 99, []byte("?"))},
		"entry leaves a gap":                      {appendRecord(nil, recEntries, encodeEntries([]raft.Entry{ent(5, 1, "")}))},
		"impossible length":                       {{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0}},
		"malformed payload with a valid checksum": {appendRecord(nil, recHardState, []byte{1, 2, 3})},
	}
	for name, segs := range cases {
		t.Run(name, func(t *testing.T) {
			d := newDisk(1)
			writeSegments(t, d, segs...)
			if _, _, err := Open(d, Options{}); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("err = %v, want ErrCorrupt", err)
			}
		})
	}
	t.Run("missing segment", func(t *testing.T) {
		d := newDisk(1)
		for _, seq := range []uint64{1, 3} {
			f, _ := d.Create(segmentName(seq))
			_, _ = f.Write(rec1)
		}
		if _, _, err := Open(d, Options{}); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("err = %v, want ErrCorrupt", err)
		}
	})
}

// Under SyncAlways, whatever Save acknowledged survives a machine crash,
// across segment rotations, for many crash points and random tear lengths.
func TestAcknowledgedSavesSurviveMachineCrash(t *testing.T) {
	for seed := uint64(0); seed < 200; seed++ {
		d := newDisk(seed)
		r := sim.NewRand(seed, "crash-point")
		w, _ := mustOpen(t, d, Options{SegmentSize: 64})
		var want State
		crashAfter := r.Intn(40)
		for i := uint64(1); i <= uint64(crashAfter); i++ {
			e := ent(i, 1, "data")
			if err := w.Save(hs(raft.Term(i), 1), []raft.Entry{e}); err != nil {
				t.Fatal(err)
			}
			want.HardState = *hs(raft.Term(i), 1)
			want.Entries = append(want.Entries, e)
		}
		d.Crash(true)
		_, got, err := Open(d, Options{SegmentSize: 64})
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if !equalState(got, want) {
			t.Fatalf("seed %d: lost acknowledged data: recovered %d entries, want %d", seed, len(got.Entries), len(want.Entries))
		}
	}
}

// SyncNever really is unsafe: some machine crash loses acknowledged data.
func TestSyncNeverCanLoseData(t *testing.T) {
	for seed := uint64(0); seed < 200; seed++ {
		d := newDisk(seed)
		w, _ := mustOpen(t, d, Options{Sync: SyncNever})
		for i := uint64(1); i <= 10; i++ {
			if err := w.Save(nil, []raft.Entry{ent(i, 1, "data")}); err != nil {
				t.Fatal(err)
			}
		}
		d.Crash(true)
		_, got, err := Open(d, Options{Sync: SyncNever})
		if err != nil || len(got.Entries) < 10 {
			return // lost data (or the whole segment): as expected
		}
	}
	t.Fatal("SyncNever never lost data in 200 machine crashes")
}

func TestSaveFailureIsSticky(t *testing.T) {
	d := sim.NewDisk(sim.DiskConfig{SyncFailProb: 1}, sim.NewRand(1, "x"))
	w, _, err := Open(d, Options{Sync: SyncNever}) // open without syncing...
	if err != nil {
		t.Fatal(err)
	}
	w.opts.Sync = SyncAlways // ...then make every Save fsync, which fails
	if err := w.Save(hs(1, 1), nil); !errors.Is(err, sim.ErrSyncFailed) {
		t.Fatalf("err = %v, want the injected fsync failure", err)
	}
	if err := w.Save(hs(2, 2), nil); err == nil {
		t.Fatal("Save succeeded after an earlier failure")
	}
}

func TestLeftoverTempFileRemoved(t *testing.T) {
	d := newDisk(1)
	f, _ := d.Create(segmentName(1) + ".tmp")
	_, _ = f.Write([]byte("half-finished repair"))
	w, _ := mustOpen(t, d, Options{})
	_ = w.Close()
	if names, _ := d.List(); len(names) != 1 || names[0] != segmentName(1) {
		t.Fatalf("files = %v", names)
	}
}

func TestSegmentNames(t *testing.T) {
	for _, seq := range []uint64{1, 255, 1 << 40} {
		got, ok := parseSegmentName(segmentName(seq))
		if !ok || got != seq {
			t.Errorf("round trip of %d gave %d, %v", seq, got, ok)
		}
	}
	for _, bad := range []string{"wal-1.log", "wal-000000000000000g.log", "x.log", "wal-0000000000000001.tmp"} {
		if _, ok := parseSegmentName(bad); ok {
			t.Errorf("parsed %q as a segment", bad)
		}
	}
}

// FuzzReadRecord: arbitrary bytes must never panic the decoder, and any
// record it accepts must re-encode to exactly the same bytes.
func FuzzReadRecord(f *testing.F) {
	f.Add(appendRecord(nil, recHardState, encodeHardState(*hs(3, 2))))
	f.Add(appendRecord(nil, recEntries, encodeEntries([]raft.Entry{ent(1, 1, ""), ent(2, 1, "x")})))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		typ, payload, n, err := readRecord(b)
		if err != nil {
			return
		}
		if again := appendRecord(nil, typ, payload); !bytes.Equal(again, b[:n]) {
			t.Fatalf("accepted record does not re-encode to the same bytes")
		}
		switch typ {
		case recHardState:
			if h, err := decodeHardState(payload); err == nil && !bytes.Equal(encodeHardState(h), payload) {
				t.Fatal("hardstate round trip mismatch")
			}
		case recEntries:
			if es, err := decodeEntries(payload); err == nil && !bytes.Equal(encodeEntries(es), payload) {
				t.Fatal("entries round trip mismatch")
			}
		}
	})
}

// FuzzOpen: recovering from an arbitrary segment must never panic. It may
// return an error, but if it succeeds the WAL must accept further writes.
func FuzzOpen(f *testing.F) {
	saves, _ := script()
	d := newDisk(1)
	w, _, _ := Open(d, Options{})
	for _, s := range saves {
		_ = w.Save(s.hs, s.entries)
	}
	full, _ := d.ReadFile(segmentName(1))
	f.Add(full)
	f.Add(full[:len(full)/2])
	f.Fuzz(func(t *testing.T, b []byte) {
		d := newDisk(1)
		writeSegments(t, d, b)
		w, _, err := Open(d, Options{})
		if err != nil {
			return
		}
		if err := w.Save(hs(1, 1), []raft.Entry{ent(1, 1, "x")}); err != nil {
			t.Fatalf("Save after a successful Open: %v", err)
		}
	})
}
