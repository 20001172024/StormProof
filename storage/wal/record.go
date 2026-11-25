package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"

	"github.com/20001172024/stormproof/raft"
)

// Record layout, all integers little-endian:
//
//	+-----------+-----------+------+-----------------+
//	| length u32| crc32c u32| type | payload ...     |
//	+-----------+-----------+------+-----------------+
//	                         \_______ length bytes ___/
//
// The CRC covers the type byte and the payload. CRC-32C (Castagnoli) is the
// variant with hardware support on modern CPUs, and what etcd, RocksDB and
// many other storage engines use.
const headerSize = 8

// maxRecordSize bounds a record's length field. A length larger than this
// can only come from corruption, and refusing it early avoids trying to
// allocate gigabytes for a garbage value.
const maxRecordSize = 64 << 20

type recordType byte

const (
	recHardState recordType = 1
	recEntries   recordType = 2
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

var (
	// ErrCorrupt means the log holds data that is complete but wrong: a bad
	// checksum, an impossible length or a malformed payload. Recovery
	// refuses to continue rather than guess what the data should have been.
	ErrCorrupt = errors.New("wal: corrupt record")

	// errTorn means the data ends partway through a record. At the very end
	// of the log that is what a crash during an append leaves behind.
	errTorn = errors.New("wal: incomplete record")
)

// appendRecord appends one encoded record to buf.
func appendRecord(buf []byte, typ recordType, payload []byte) []byte {
	body := make([]byte, 0, 1+len(payload))
	body = append(body, byte(typ))
	body = append(body, payload...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(body)))
	buf = binary.LittleEndian.AppendUint32(buf, crc32.Checksum(body, castagnoli))
	return append(buf, body...)
}

// readRecord decodes the record at the start of b and returns its type,
// payload, and total size in bytes. It returns errTorn if b ends before the
// record does, and ErrCorrupt if the record is complete but invalid.
func readRecord(b []byte) (recordType, []byte, int, error) {
	if len(b) < headerSize {
		return 0, nil, 0, errTorn
	}
	length := binary.LittleEndian.Uint32(b[0:4])
	sum := binary.LittleEndian.Uint32(b[4:8])
	if length == 0 || length > maxRecordSize {
		return 0, nil, 0, ErrCorrupt
	}
	if uint64(len(b)-headerSize) < uint64(length) {
		return 0, nil, 0, errTorn
	}
	body := b[headerSize : headerSize+int(length)]
	if crc32.Checksum(body, castagnoli) != sum {
		return 0, nil, 0, ErrCorrupt
	}
	return recordType(body[0]), body[1:], headerSize + int(length), nil
}

func encodeHardState(hs raft.HardState) []byte {
	b := binary.LittleEndian.AppendUint64(nil, uint64(hs.Term))
	return binary.LittleEndian.AppendUint64(b, uint64(hs.Vote))
}

func decodeHardState(p []byte) (raft.HardState, error) {
	if len(p) != 16 {
		return raft.HardState{}, ErrCorrupt
	}
	return raft.HardState{
		Term: raft.Term(binary.LittleEndian.Uint64(p[0:8])),
		Vote: raft.NodeID(binary.LittleEndian.Uint64(p[8:16])),
	}, nil
}

// Entries payload: count u32, then per entry index u64, term u64,
// data length u32, data.
func encodeEntries(es []raft.Entry) []byte {
	b := binary.LittleEndian.AppendUint32(nil, uint32(len(es)))
	for _, e := range es {
		b = binary.LittleEndian.AppendUint64(b, e.Index)
		b = binary.LittleEndian.AppendUint64(b, uint64(e.Term))
		b = binary.LittleEndian.AppendUint32(b, uint32(len(e.Data)))
		b = append(b, e.Data...)
	}
	return b
}

func decodeEntries(p []byte) ([]raft.Entry, error) {
	if len(p) < 4 {
		return nil, ErrCorrupt
	}
	n := binary.LittleEndian.Uint32(p)
	p = p[4:]
	// Each entry takes at least 20 bytes, which bounds a corrupt count
	// before anything is allocated for it.
	if uint64(n)*20 > uint64(len(p)) {
		return nil, ErrCorrupt
	}
	es := make([]raft.Entry, 0, n)
	for i := uint32(0); i < n; i++ {
		if len(p) < 20 {
			return nil, ErrCorrupt
		}
		e := raft.Entry{
			Index: binary.LittleEndian.Uint64(p[0:8]),
			Term:  raft.Term(binary.LittleEndian.Uint64(p[8:16])),
		}
		dlen := binary.LittleEndian.Uint32(p[16:20])
		p = p[20:]
		if uint64(len(p)) < uint64(dlen) {
			return nil, ErrCorrupt
		}
		if dlen > 0 {
			e.Data = append([]byte(nil), p[:dlen]...)
		}
		p = p[dlen:]
		es = append(es, e)
	}
	if len(p) != 0 {
		return nil, ErrCorrupt
	}
	return es, nil
}
