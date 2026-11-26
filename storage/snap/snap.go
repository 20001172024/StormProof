// Package snap stores state-machine snapshots as files, one per snapshot,
// named snap-<index>.snap.
//
// A snapshot file is written atomically: to a temp file, fsynced, renamed
// into place, and the directory fsynced. A crash leaves either the complete
// file or none at all, never a half-written one under the real name.
//
// Layout, little-endian: CRC-32C u32 of everything after it, then index
// u64, term u64, and the snapshot data.
package snap

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"

	"github.com/20001172024/stormproof/raft"
	"github.com/20001172024/stormproof/storage"
)

// ErrCorrupt means a snapshot file exists but fails its checksum or
// doesn't describe the snapshot its name says.
var ErrCorrupt = errors.New("snap: corrupt snapshot file")

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

const prefix, suffix = "snap-", ".snap"

// Name is the file name for the snapshot at index.
func Name(index uint64) string { return fmt.Sprintf("%s%016x%s", prefix, index, suffix) }

func parseName(name string) (uint64, bool) {
	hex, ok := strings.CutPrefix(name, prefix)
	if !ok {
		return 0, false
	}
	hex, ok = strings.CutSuffix(hex, suffix)
	if !ok || len(hex) != 16 {
		return 0, false
	}
	i, err := strconv.ParseUint(hex, 16, 64)
	return i, err == nil
}

// Save durably writes a snapshot. If sync is false nothing is fsynced; that
// is unsafe and only for measuring the cost of syncing.
func Save(fsys storage.FS, s raft.Snapshot, sync bool) error {
	body := binary.LittleEndian.AppendUint64(nil, s.Index)
	body = binary.LittleEndian.AppendUint64(body, uint64(s.Term))
	body = append(body, s.Data...)
	b := binary.LittleEndian.AppendUint32(nil, crc32.Checksum(body, castagnoli))
	b = append(b, body...)

	name := Name(s.Index)
	tmp := name + ".tmp"
	f, err := fsys.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if sync {
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := fsys.Rename(tmp, name); err != nil {
		return err
	}
	if sync {
		return fsys.SyncDir()
	}
	return nil
}

// Load reads and verifies the snapshot at index.
func Load(fsys storage.FS, index uint64) (raft.Snapshot, error) {
	b, err := fsys.ReadFile(Name(index))
	if err != nil {
		return raft.Snapshot{}, err
	}
	if len(b) < 20 || crc32.Checksum(b[4:], castagnoli) != binary.LittleEndian.Uint32(b) {
		return raft.Snapshot{}, fmt.Errorf("%w: %s", ErrCorrupt, Name(index))
	}
	s := raft.Snapshot{
		Index: binary.LittleEndian.Uint64(b[4:12]),
		Term:  raft.Term(binary.LittleEndian.Uint64(b[12:20])),
	}
	if len(b) > 20 {
		s.Data = append([]byte(nil), b[20:]...)
	}
	if s.Index != index {
		return raft.Snapshot{}, fmt.Errorf("%w: %s holds the snapshot at %d", ErrCorrupt, Name(index), s.Index)
	}
	return s, nil
}

// Prune removes every snapshot file except the one at keep, and any temp
// files left by a Save interrupted by a crash.
func Prune(fsys storage.FS, keep uint64) error {
	names, err := fsys.List()
	if err != nil {
		return err
	}
	for _, name := range names {
		i, ok := parseName(name)
		stale := ok && i != keep
		tmp := strings.HasPrefix(name, prefix) && strings.HasSuffix(name, suffix+".tmp")
		if stale || tmp {
			if err := fsys.Remove(name); err != nil {
				return err
			}
		}
	}
	return nil
}
