package raft

// Entry is one entry in the replicated log (§5.3). Index starts at 1.
// An entry with no Data is a no-op, appended by each new leader.
type Entry struct {
	Index uint64 `json:"index"`
	Term  Term   `json:"term"`
	Data  []byte `json:"data,omitempty"`
}

// raftLog is the in-memory log. entries[i] has Index i+1: the log always
// starts at index 1, because there is no compaction until snapshots (M6).
//
// Entry.Data slices are never modified after an entry is created, so they
// are shared rather than copied.
type raftLog struct {
	entries []Entry
}

// lastIndex returns the index of the last entry, or 0 if the log is empty.
func (l *raftLog) lastIndex() uint64 { return uint64(len(l.entries)) }

// lastTerm returns the term of the last entry, or 0 if the log is empty.
func (l *raftLog) lastTerm() Term {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

// term returns the term of the entry at index i. Index 0 is the empty
// prefix before the first entry and has term 0, which is what makes the
// consistency check work for a leader's very first AppendEntries. ok is
// false if there is no entry at i.
func (l *raftLog) term(i uint64) (t Term, ok bool) {
	if i == 0 {
		return 0, true
	}
	if i > l.lastIndex() {
		return 0, false
	}
	return l.entries[i-1].Term, true
}

// slice returns the entries with indexes in [lo, hi). The result shares
// memory with the log, so callers must not modify it.
func (l *raftLog) slice(lo, hi uint64) []Entry {
	if lo < 1 {
		lo = 1
	}
	if hi > l.lastIndex()+1 {
		hi = l.lastIndex() + 1
	}
	if lo >= hi {
		return nil
	}
	return l.entries[lo-1 : hi-1]
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
func (l *raftLog) truncateFrom(i uint64) {
	if i <= l.lastIndex() {
		// Copy rather than reslice, so slices handed out earlier (in Ready or
		// in messages) keep their contents when we append over this space.
		l.entries = append([]Entry(nil), l.entries[:i-1]...)
	}
}

// firstIndexOfTerm returns the first index of the run of entries with the
// same term as the entry at index i, which must exist. Used by a follower
// to tell the leader where a conflicting term starts (fast backtracking).
func (l *raftLog) firstIndexOfTerm(i uint64) uint64 {
	t := l.entries[i-1].Term
	for i > 1 && l.entries[i-2].Term == t {
		i--
	}
	return i
}

// lastIndexOfTerm returns the index of the last entry with term t, or 0 if
// the log has no entry with that term. Used by a leader during backtracking.
func (l *raftLog) lastIndexOfTerm(t Term) uint64 {
	for i := l.lastIndex(); i > 0; i-- {
		switch et := l.entries[i-1].Term; {
		case et == t:
			return i
		case et < t:
			return 0 // terms only increase along a log, so t isn't here
		}
	}
	return 0
}
