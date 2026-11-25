package kv

import (
	"bytes"
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
