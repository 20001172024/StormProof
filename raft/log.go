package raft

// Entry is one entry in the replicated log (§5.3). Index starts at 1.
// An entry with no Data is a no-op, appended by each new leader.
type Entry struct {
	Index uint64 `json:"index"`
	Term  Term   `json:"term"`
	Data  []byte `json:"data,omitempty"`
}

// raftLog is the in-memory log, after the latest snapshot.
//
// Entries up to and including snapIndex have been compacted into a snapshot
// (§7) and are gone; entries[i] has Index snapIndex+1+i. Without a snapshot
// snapIndex is 0 and the log starts at index 1.
//
// The snapshot's last index and term stay known. That is what lets the
// AppendEntries consistency check work for the first entry after the
// snapshot: its "previous entry" is the snapshot's last one.
//
// Entry.Data slices are never modified after an entry is created, so they
// are shared rather than copied.
type raftLog struct {
	snapIndex uint64
	snapTerm  Term
	entries   []Entry
}

// firstIndex is the index of the first entry still in the log.
func (l *raftLog) firstIndex() uint64 { return l.snapIndex + 1 }

// lastIndex returns the index of the last entry, or the snapshot's last
// index if there are no entries after it (0 for an empty log).
func (l *raftLog) lastIndex() uint64 { return l.snapIndex + uint64(len(l.entries)) }

// lastTerm returns the term of the last entry, or of the snapshot's last
// entry if there are none after it.
func (l *raftLog) lastTerm() Term {
	if len(l.entries) == 0 {
		return l.snapTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// term returns the term of the entry at index i. ok is false if the log
// can't say: i is past the end, or before the snapshot (compacted away).
// The snapshot's own last index answers with the snapshot's term, and with
// no snapshot that is index 0, term 0: the empty prefix before the first
// entry, which is what makes a leader's very first AppendEntries work.
func (l *raftLog) term(i uint64) (t Term, ok bool) {
	switch {
	case i == l.snapIndex:
		return l.snapTerm, true
	case i < l.snapIndex || i > l.lastIndex():
		return 0, false
	}
	return l.entries[i-l.snapIndex-1].Term, true
}

// slice returns the entries with indexes in [lo, hi), clamped to what the
// log holds. The result shares memory with the log, so callers must not
// modify it.
func (l *raftLog) slice(lo, hi uint64) []Entry {
	lo = max(lo, l.firstIndex())
	hi = min(hi, l.lastIndex()+1)
	if lo >= hi {
		return nil
	}
	return l.entries[lo-l.firstIndex() : hi-l.firstIndex()]
}

// append adds entries to the end of the log. Their indexes must follow on
// from the last entry.
func (l *raftLog) append(es ...Entry) {
	for _, e := range es {
		if e.Index != l.lastIndex()+1 {
			panic("raft: appended entry does not follow the end of the log")
		}
		l.entries = append(l.entries, e)
	}
}

// truncateFrom deletes the entry at index i and everything after it.
// Compacted entries are committed and can never be truncated.
func (l *raftLog) truncateFrom(i uint64) {
	if i <= l.snapIndex {
		panic("raft: truncating into the snapshot")
	}
	if i <= l.lastIndex() {
		// Copy rather than reslice, so slices handed out earlier (in Ready or
		// in messages) keep their contents when we append over this space.
		l.entries = append([]Entry(nil), l.entries[:i-l.firstIndex()]...)
	}
}

// compact discards the entries up to and including index, which must be in
// the log, recording its term as the snapshot's.
func (l *raftLog) compact(index uint64) {
	t, ok := l.term(index)
	if !ok || index < l.snapIndex {
		panic("raft: compacting to an index the log doesn't have")
	}
	l.entries = append([]Entry(nil), l.entries[index-l.snapIndex:]...)
	l.snapIndex, l.snapTerm = index, t
}

// restore replaces the whole log with a snapshot ending at (index, term)
// and nothing after it.
func (l *raftLog) restore(index uint64, t Term) {
	l.entries = nil
	l.snapIndex, l.snapTerm = index, t
}

// firstIndexOfTerm returns the first index of the run of entries with the
// same term as the entry at index i, which must be in the log or be the
// snapshot's last index. The run can't be followed back into the snapshot,
// so the answer is never below the snapshot index. Used by a follower to
// tell the leader where a conflicting term starts (fast backtracking).
func (l *raftLog) firstIndexOfTerm(i uint64) uint64 {
	t, _ := l.term(i)
	for i > l.firstIndex() {
		if pt, _ := l.term(i - 1); pt != t {
			break
		}
		i--
	}
	return i
}

// lastIndexOfTerm returns the index of the last entry with term t, or 0 if
// the log has no entry with that term. The snapshot's last entry counts.
// Used by a leader during backtracking.
func (l *raftLog) lastIndexOfTerm(t Term) uint64 {
	for i := l.lastIndex(); i >= l.firstIndex(); i-- {
		switch et, _ := l.term(i); {
		case et == t:
			return i
		case et < t:
			return 0 // terms only increase along a log, so t isn't here
		}
	}
	if l.snapIndex > 0 && l.snapTerm == t {
		return l.snapIndex
	}
	return 0
}
