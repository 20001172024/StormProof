package kv

import "bytes"

// Store is the replicated key-value state machine. Every node applies the
// same committed commands in the same order, so every node's Store ends up
// identical; nothing in it depends on time, randomness or map order.
type Store struct {
	data     map[string][]byte
	sessions map[uint64]session

	// Planted bugs, for testing that the checkers notice; never set them
	// outside tests. noDedup switches off duplicate detection;
	// snapshotDropsSessions leaves the session table out of snapshots.
	noDedup               bool
	snapshotDropsSessions bool
}

// session is what the store remembers about one client: the last request it
// applied and that request's result (dissertation §6.3).
//
// Sessions are never expired, so the table grows with the number of clients
// ever seen. A real deployment needs expiry, and the expiry decision has to
// be made through the log too, so every node expires the same sessions.
type session struct {
	lastRequest uint64
	result      Result
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{data: map[string][]byte{}, sessions: map[uint64]session{}}
}

// NewStoreWithoutDedup returns a store with duplicate detection switched off.
// Only for tests that check the checker.
func NewStoreWithoutDedup() *Store {
	s := NewStore()
	s.noDedup = true
	return s
}

// TestingKnobs plant bugs in a store, for tests that check the checkers.
type TestingKnobs struct {
	NoDedup               bool // switch off duplicate detection
	SnapshotDropsSessions bool // leave the session table out of snapshots
}

// SetTestingKnobs plants the given bugs. Never call it outside tests.
func (s *Store) SetTestingKnobs(k TestingKnobs) {
	s.noDedup, s.snapshotDropsSessions = k.NoDedup, k.SnapshotDropsSessions
}

// NewStoreWithBrokenSnapshots returns a store whose snapshots leave out the
// client sessions. Only for tests that check the checker.
func NewStoreWithBrokenSnapshots() *Store {
	s := NewStore()
	s.snapshotDropsSessions = true
	return s
}

// Apply applies a committed write and returns its result.
//
// Exactly-once: if this client's request was already applied (a retry whose
// first attempt did commit), the saved result is returned and nothing is
// applied again. Without this, a retried CompareAndSwap that had succeeded
// the first time would run again, fail, and report failure to a client
// whose swap in fact happened.
//
// A request older than the client's last one can only be a stale duplicate
// that arrived very late (clients have one request outstanding at a time),
// and it is ignored. Its result goes nowhere: the client has moved on.
func (s *Store) Apply(c Command) Result {
	if c.ClientID != 0 && !s.noDedup {
		if sess, ok := s.sessions[c.ClientID]; ok && c.RequestID <= sess.lastRequest {
			return sess.result
		}
	}

	var r Result
	switch c.Op {
	case OpPut:
		s.data[c.Key] = c.Value
	case OpDelete:
		_, r.Existed = s.data[c.Key]
		delete(s.data, c.Key)
	case OpCAS:
		cur, ok := s.data[c.Key]
		r.Found, r.Value = ok, cur
		if (c.ExpectAbsent && !ok) || (!c.ExpectAbsent && ok && bytes.Equal(cur, c.Expected)) {
			s.data[c.Key] = c.Value
			r.Swapped = true
		}
	case OpGet:
		// Reads are served with ReadIndex rather than through the log, but a
		// Get in the log is still well defined.
		r.Value, r.Found = s.data[c.Key]
	}

	if c.ClientID != 0 {
		s.sessions[c.ClientID] = session{lastRequest: c.RequestID, result: r}
	}
	return r
}

// Get reads a key from the local state. It is only linearizable if the
// caller has made sure this node is up to date (ReadIndex).
func (s *Store) Get(key string) Result {
	v, ok := s.data[key]
	return Result{Found: ok, Value: v}
}
