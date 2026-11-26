package kv

import (
	"encoding/binary"
	"errors"
	"slices"
	"sort"
)

// ErrBadSnapshot is returned when bytes don't decode to a store snapshot.
var ErrBadSnapshot = errors.New("kv: malformed snapshot")

const snapshotVersion = 1

// Snapshot returns the store's whole state: the data and the client
// sessions.
//
// The sessions are part of the state machine, not a cache. If a snapshot
// left them out, a node restored from it would forget which requests it had
// already applied, and a client's retry of a write that committed before
// the snapshot would be applied a second time.
//
// Keys and client IDs are written in sorted order, so two nodes with the
// same state produce byte-identical snapshots. (Go randomizes map
// iteration order, so writing them in map order would not.)
//
// Layout, little-endian: version u8; data count u32, then for each key in
// order key and value as u32-length-prefixed bytes; session count u32, then
// for each client in order: client u64, last request u64, result flags u8
// (found, existed, swapped), result value as length-prefixed bytes.
func (s *Store) Snapshot() []byte {
	b := []byte{snapshotVersion}
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(keys)))
	for _, k := range keys {
		b = appendBytes(b, []byte(k))
		b = appendBytes(b, s.data[k])
	}

	sessions := s.sessions
	if s.snapshotDropsSessions {
		sessions = nil // the planted bug
	}
	clients := make([]uint64, 0, len(sessions))
	for c := range sessions {
		clients = append(clients, c)
	}
	slices.Sort(clients)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(clients)))
	for _, c := range clients {
		sess := sessions[c]
		b = binary.LittleEndian.AppendUint64(b, c)
		b = binary.LittleEndian.AppendUint64(b, sess.lastRequest)
		var flags byte
		if sess.result.Found {
			flags |= 1
		}
		if sess.result.Existed {
			flags |= 2
		}
		if sess.result.Swapped {
			flags |= 4
		}
		b = append(b, flags)
		b = appendBytes(b, sess.result.Value)
	}
	return b
}

func appendBytes(b, v []byte) []byte {
	b = binary.LittleEndian.AppendUint32(b, uint32(len(v)))
	return append(b, v...)
}

// Restore builds a store from a snapshot.
func Restore(b []byte) (*Store, error) {
	r := reader{b: b}
	if v := r.byte(); r.err == nil && v != snapshotVersion {
		return nil, ErrBadSnapshot
	}
	s := NewStore()
	// Keys and clients must be strictly increasing: that rules out
	// duplicates (which a map would silently merge) and accepts only the one
	// layout Snapshot writes. The fuzzer found the duplicate-key case.
	var prevKey string
	for i, n := 0, r.u32(); r.err == nil && uint32(i) < n; i++ {
		k, v := string(r.bytes()), r.bytes()
		if i > 0 && k <= prevKey {
			r.err = ErrBadSnapshot
		}
		prevKey = k
		s.data[k] = v
	}
	var prevClient uint64
	for i, n := 0, r.u32(); r.err == nil && uint32(i) < n; i++ {
		c, req, flags := r.u64(), r.u64(), r.byte()
		if i > 0 && c <= prevClient {
			r.err = ErrBadSnapshot
		}
		prevClient = c
		val := r.bytes()
		if flags&^7 != 0 {
			r.err = ErrBadSnapshot
		}
		s.sessions[c] = session{lastRequest: req, result: Result{
			Found: flags&1 != 0, Existed: flags&2 != 0, Swapped: flags&4 != 0, Value: val,
		}}
	}
	if r.err != nil || len(r.b) != 0 {
		return nil, ErrBadSnapshot
	}
	return s, nil
}

// reader decodes the snapshot layout, remembering the first error so the
// decoding code above doesn't have to check after every field.
type reader struct {
	b   []byte
	err error
}

func (r *reader) take(n int) []byte {
	if r.err != nil || len(r.b) < n {
		r.err = ErrBadSnapshot
		return nil
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}

func (r *reader) byte() byte {
	if v := r.take(1); v != nil {
		return v[0]
	}
	return 0
}

func (r *reader) u32() uint32 {
	if v := r.take(4); v != nil {
		return binary.LittleEndian.Uint32(v)
	}
	return 0
}

func (r *reader) u64() uint64 {
	if v := r.take(8); v != nil {
		return binary.LittleEndian.Uint64(v)
	}
	return 0
}

func (r *reader) bytes() []byte {
	n := r.u32()
	if uint64(n) > uint64(len(r.b)) {
		r.err = ErrBadSnapshot
		return nil
	}
	v := r.take(int(n))
	if len(v) == 0 {
		return nil
	}
	return append([]byte(nil), v...)
}
