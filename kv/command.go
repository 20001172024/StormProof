package kv

import (
	"encoding/binary"
	"errors"
)

// Op is a key-value operation.
type Op byte

const (
	OpGet Op = iota + 1
	OpPut
	OpDelete
	OpCAS // CompareAndSwap
)

func (o Op) String() string {
	switch o {
	case OpGet:
		return "get"
	case OpPut:
		return "put"
	case OpDelete:
		return "delete"
	case OpCAS:
		return "cas"
	}
	return "unknown"
}

// MarshalText makes ops appear by name in JSON traces.
func (o Op) MarshalText() ([]byte, error) { return []byte(o.String()), nil }

// Command is one operation as it is stored in the Raft log (writes) or
// served through ReadIndex (Get).
//
// ClientID and RequestID identify the request for exactly-once semantics: a
// client numbers its requests 1, 2, 3, ... and reuses the same RequestID
// when it retries.
type Command struct {
	ClientID  uint64 `json:"client"`
	RequestID uint64 `json:"request"`
	Op        Op     `json:"op"`
	Key       string `json:"key"`
	Value     []byte `json:"value,omitempty"` // Put, and CAS's new value

	// CAS only: swap if the current value equals Expected, or, with
	// ExpectAbsent, if the key does not exist.
	Expected     []byte `json:"expected,omitempty"`
	ExpectAbsent bool   `json:"expect_absent,omitempty"`
}

// Result is the outcome of applying a command.
type Result struct {
	Found   bool   `json:"found,omitempty"`   // Get: the key exists; CAS: the key existed
	Value   []byte `json:"value,omitempty"`   // Get: its value; CAS: the value before the CAS
	Existed bool   `json:"existed,omitempty"` // Delete: the key existed
	Swapped bool   `json:"swapped,omitempty"` // CAS: the swap happened
}

// ErrBadCommand is returned when bytes don't decode to a valid command.
var ErrBadCommand = errors.New("kv: malformed command")

// Encoding, little-endian: client u64, request u64, op u8, flags u8
// (bit 0: ExpectAbsent), then three length-prefixed (u32) byte strings:
// key, value, expected.

// Encode returns the command's binary form.
func (c Command) Encode() []byte {
	b := binary.LittleEndian.AppendUint64(nil, c.ClientID)
	b = binary.LittleEndian.AppendUint64(b, c.RequestID)
	var flags byte
	if c.ExpectAbsent {
		flags |= 1
	}
	b = append(b, byte(c.Op), flags)
	for _, s := range [][]byte{[]byte(c.Key), c.Value, c.Expected} {
		b = binary.LittleEndian.AppendUint32(b, uint32(len(s)))
		b = append(b, s...)
	}
	return b
}

// Decode parses the binary form of a command.
func Decode(b []byte) (Command, error) {
	var c Command
	if len(b) < 18 {
		return c, ErrBadCommand
	}
	c.ClientID = binary.LittleEndian.Uint64(b[0:8])
	c.RequestID = binary.LittleEndian.Uint64(b[8:16])
	c.Op = Op(b[16])
	flags := b[17]
	if c.Op < OpGet || c.Op > OpCAS || flags&^1 != 0 {
		return Command{}, ErrBadCommand
	}
	c.ExpectAbsent = flags&1 != 0
	b = b[18:]
	var fields [3][]byte
	for i := range fields {
		if len(b) < 4 {
			return Command{}, ErrBadCommand
		}
		n := binary.LittleEndian.Uint32(b)
		b = b[4:]
		if uint64(len(b)) < uint64(n) {
			return Command{}, ErrBadCommand
		}
		if n > 0 {
			fields[i] = append([]byte(nil), b[:n]...)
		}
		b = b[n:]
	}
	if len(b) != 0 {
		return Command{}, ErrBadCommand
	}
	c.Key, c.Value, c.Expected = string(fields[0]), fields[1], fields[2]
	return c, nil
}
