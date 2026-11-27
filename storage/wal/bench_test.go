package wal

import (
	"fmt"
	"testing"

	"github.com/20001172024/stormproof/raft"
	"github.com/20001172024/stormproof/storage"
)

// BenchmarkSave measures appending entries to a WAL on the real file
// system: one Save per iteration with `batch` 100-byte entries, with and
// without fsync. The fsync cases show what batching saves: the cost per
// Save is mostly the fsync, whatever the batch size.
//
//	go test ./storage/wal -run '^$' -bench Save
func BenchmarkSave(b *testing.B) {
	for _, sync := range []SyncPolicy{SyncNever, SyncAlways} {
		for _, batch := range []int{1, 16, 128} {
			name := fmt.Sprintf("sync=%v/batch=%d", sync == SyncAlways, batch)
			b.Run(name, func(b *testing.B) {
				fs, err := storage.NewOSFS(b.TempDir())
				if err != nil {
					b.Fatal(err)
				}
				w, _, err := Open(fs, Options{Sync: sync})
				if err != nil {
					b.Fatal(err)
				}
				defer func() { _ = w.Close() }()
				data := make([]byte, 100)
				next := uint64(1)
				entries := make([]raft.Entry, batch)
				b.SetBytes(int64(batch * len(data)))
				b.ResetTimer()
				for range b.N {
					for i := range entries {
						entries[i] = raft.Entry{Index: next, Term: 1, Data: data}
						next++
					}
					if err := w.Save(nil, entries); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(b.N*batch)/b.Elapsed().Seconds(), "entries/s")
			})
		}
	}
}
