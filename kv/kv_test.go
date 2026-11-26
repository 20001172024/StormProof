package kv

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	cases := []Command{
		{ClientID: 1, RequestID: 2, Op: OpPut, Key: "k", Value: []byte("v")},
		{ClientID: 3, RequestID: 4, Op: OpCAS, Key: "k", Value: []byte("new"), Expected: []byte("old")},
		{Op: OpCAS, Key: "k", Value: []byte("x"), ExpectAbsent: true},
		{Op: OpDelete, Key: ""},
		{Op: OpGet, Key: "key with spaces"},
	}
	for _, c := range cases {
		got, err := Decode(c.Encode())
		if err != nil {
			t.Fatalf("%+v: %v", c, err)
		}
		if !reflect.DeepEqual(got, c) {
			t.Fatalf("round trip: got %+v, want %+v", got, c)
		}
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	good := Command{Op: OpPut, Key: "k", Value: []byte("v")}.Encode()
	bad := map[string][]byte{
		"empty":         nil,
		"short":         good[:10],
		"bad op":        append(append(append([]byte(nil), good[:16]...), 99), good[17:]...),
		"truncated":     good[:len(good)-1],
		"trailing junk": append(append([]byte(nil), good...), 0),
	}
	for name, b := range bad {
		if _, err := Decode(b); err == nil {
			t.Errorf("%s: decoded without error", name)
		}
	}
}

func FuzzDecode(f *testing.F) {
	f.Add(Command{ClientID: 1, RequestID: 1, Op: OpCAS, Key: "k", Value: []byte("v"), Expected: []byte("e")}.Encode())
	f.Fuzz(func(t *testing.T, b []byte) {
		c, err := Decode(b)
		if err != nil {
			return
		}
		if !bytes.Equal(c.Encode(), b) {
			t.Fatal("decoded command does not re-encode to the same bytes")
		}
	})
}

func TestApply(t *testing.T) {
	s := NewStore()
	put := func(k, v string) Command { return Command{Op: OpPut, Key: k, Value: []byte(v)} }
	steps := []struct {
		cmd  Command
		want Result
	}{
		{Command{Op: OpGet, Key: "a"}, Result{}},
		{put("a", "1"), Result{}},
		{Command{Op: OpGet, Key: "a"}, Result{Found: true, Value: []byte("1")}},
		{Command{Op: OpCAS, Key: "a", Expected: []byte("2"), Value: []byte("3")}, Result{Found: true, Value: []byte("1")}},
		{Command{Op: OpCAS, Key: "a", Expected: []byte("1"), Value: []byte("3")}, Result{Found: true, Value: []byte("1"), Swapped: true}},
		{Command{Op: OpCAS, Key: "a", ExpectAbsent: true, Value: []byte("x")}, Result{Found: true, Value: []byte("3")}},
		{Command{Op: OpCAS, Key: "b", ExpectAbsent: true, Value: []byte("x")}, Result{Swapped: true}},
		{Command{Op: OpDelete, Key: "a"}, Result{Existed: true}},
		{Command{Op: OpDelete, Key: "a"}, Result{}},
		{Command{Op: OpGet, Key: "a"}, Result{}},
	}
	for i, st := range steps {
		if got := s.Apply(st.cmd); !reflect.DeepEqual(got, st.want) {
			t.Fatalf("step %d %v %q: got %+v, want %+v", i, st.cmd.Op, st.cmd.Key, got, st.want)
		}
	}
}

// A retried request is applied once; the retry gets the original result.
func TestDedup(t *testing.T) {
	cas := Command{ClientID: 7, RequestID: 1, Op: OpCAS, Key: "k", ExpectAbsent: true, Value: []byte("mine")}

	s := NewStore()
	first := s.Apply(cas)
	retry := s.Apply(cas)
	if !first.Swapped || !retry.Swapped {
		t.Fatalf("first %+v, retry %+v: the retry must report the original success", first, retry)
	}
	if got := s.Get("k"); string(got.Value) != "mine" {
		t.Fatalf("value %q", got.Value)
	}
	// A late duplicate of an even older request changes nothing.
	s.Apply(Command{ClientID: 7, RequestID: 2, Op: OpPut, Key: "k", Value: []byte("second")})
	s.Apply(Command{ClientID: 7, RequestID: 1, Op: OpPut, Key: "k", Value: []byte("stale duplicate")})
	if got := s.Get("k"); string(got.Value) != "second" {
		t.Fatalf("a stale duplicate was applied: value %q", got.Value)
	}

	// The planted bug: without dedup the retry runs again and fails.
	bad := NewStoreWithoutDedup()
	bad.Apply(cas)
	if r := bad.Apply(cas); r.Swapped {
		t.Fatal("NewStoreWithoutDedup still deduplicated")
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	s := NewStore()
	s.Apply(Command{ClientID: 2, RequestID: 5, Op: OpPut, Key: "b", Value: []byte("2")})
	s.Apply(Command{ClientID: 1, RequestID: 9, Op: OpCAS, Key: "a", ExpectAbsent: true, Value: []byte("1")})
	s.Apply(Command{Op: OpPut, Key: "empty"})
	snap := s.Snapshot()

	r, err := Restore(snap)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(r.Snapshot(), snap) {
		t.Fatal("a restored store does not snapshot to the same bytes")
	}
	if got := r.Get("a"); string(got.Value) != "1" {
		t.Fatalf("a = %q", got.Value)
	}
	// Sessions survive: a retry of client 1's CAS is not applied again.
	if res := r.Apply(Command{ClientID: 1, RequestID: 9, Op: OpCAS, Key: "a", ExpectAbsent: true, Value: []byte("1")}); !res.Swapped {
		t.Fatal("after restore, a retried CAS was re-run instead of returning its saved result")
	}
}

// Two stores with the same state snapshot to identical bytes, however their
// maps happen to be laid out.
func TestSnapshotIsDeterministic(t *testing.T) {
	build := func(order []int) []byte {
		s := NewStore()
		for _, i := range order {
			s.Apply(Command{ClientID: uint64(i + 1), RequestID: 1, Op: OpPut, Key: fmt.Sprint("k", i), Value: []byte{byte(i)}})
		}
		return s.Snapshot()
	}
	want := build([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	for trial := 0; trial < 20; trial++ {
		if got := build([]int{9, 3, 7, 1, 5, 0, 8, 2, 6, 4}); !bytes.Equal(got, want) {
			t.Fatal("same state, different snapshot bytes")
		}
	}
}

func TestBrokenSnapshotsDropSessions(t *testing.T) {
	s := NewStoreWithBrokenSnapshots()
	cas := Command{ClientID: 1, RequestID: 1, Op: OpCAS, Key: "a", ExpectAbsent: true, Value: []byte("x")}
	s.Apply(cas)
	r, err := Restore(s.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if res := r.Apply(cas); res.Swapped {
		t.Fatal("the broken snapshot kept the session")
	}
}

func FuzzRestore(f *testing.F) {
	s := NewStore()
	s.Apply(Command{ClientID: 1, RequestID: 1, Op: OpPut, Key: "k", Value: []byte("v")})
	f.Add(s.Snapshot())
	f.Fuzz(func(t *testing.T, b []byte) {
		r, err := Restore(b)
		if err != nil {
			return
		}
		if !bytes.Equal(r.Snapshot(), b) {
			t.Fatal("restored snapshot does not re-encode to the same bytes")
		}
	})
}
