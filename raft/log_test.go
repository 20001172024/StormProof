package raft

import "testing"

// logOf builds a log whose entry i has term terms[i-1].
func logOf(terms ...Term) *raftLog {
	l := &raftLog{}
	for i, t := range terms {
		l.append(Entry{Index: uint64(i + 1), Term: t})
	}
	return l
}

func TestLogTerm(t *testing.T) {
	l := logOf(1, 1, 2)
	cases := []struct {
		i      uint64
		want   Term
		wantOK bool
	}{
		{0, 0, true}, // the empty prefix
		{1, 1, true},
		{3, 2, true},
		{4, 0, false},
	}
	for _, c := range cases {
		got, ok := l.term(c.i)
		if got != c.want || ok != c.wantOK {
			t.Errorf("term(%d) = %d, %v; want %d, %v", c.i, got, ok, c.want, c.wantOK)
		}
	}
	if l.lastIndex() != 3 || l.lastTerm() != 2 {
		t.Errorf("last = %d/%d, want 3/2", l.lastIndex(), l.lastTerm())
	}
	if e := (&raftLog{}); e.lastIndex() != 0 || e.lastTerm() != 0 {
		t.Error("empty log should have last index and term 0")
	}
}

func TestLogSlice(t *testing.T) {
	l := logOf(1, 2, 3, 4)
	if got := l.slice(2, 4); len(got) != 2 || got[0].Index != 2 || got[1].Index != 3 {
		t.Fatalf("slice(2,4) = %+v", got)
	}
	if got := l.slice(3, 99); len(got) != 2 {
		t.Fatalf("slice past the end should be clamped, got %+v", got)
	}
	if got := l.slice(5, 6); got != nil {
		t.Fatalf("slice beyond the log = %+v, want nil", got)
	}
}

// Slices handed out before a truncation must not change when new entries
// are appended afterwards (they may be sitting in a Ready or a message).
func TestTruncateDoesNotCorruptEarlierSlices(t *testing.T) {
	l := logOf(1, 1, 1)
	handedOut := l.slice(1, 4)
	l.truncateFrom(2)
	l.append(Entry{Index: 2, Term: 9})
	if handedOut[1].Term != 1 {
		t.Fatalf("earlier slice changed: entry 2 now has term %d", handedOut[1].Term)
	}
	if l.lastIndex() != 2 || l.lastTerm() != 9 {
		t.Fatalf("log after truncate+append: last %d/%d, want 2/9", l.lastIndex(), l.lastTerm())
	}
}

func TestAppendRejectsGap(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("appending index 3 to a 1-entry log did not panic")
		}
	}()
	logOf(1).append(Entry{Index: 3, Term: 1})
}

func TestTermSearch(t *testing.T) {
	l := logOf(1, 1, 2, 2, 2, 4)
	if got := l.firstIndexOfTerm(5); got != 3 {
		t.Errorf("firstIndexOfTerm(5) = %d, want 3", got)
	}
	if got := l.firstIndexOfTerm(1); got != 1 {
		t.Errorf("firstIndexOfTerm(1) = %d, want 1", got)
	}
	for _, c := range []struct {
		term Term
		want uint64
	}{{2, 5}, {4, 6}, {1, 2}, {3, 0}, {5, 0}} {
		if got := l.lastIndexOfTerm(c.term); got != c.want {
			t.Errorf("lastIndexOfTerm(%d) = %d, want %d", c.term, got, c.want)
		}
	}
}
