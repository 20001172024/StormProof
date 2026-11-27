package raft

import "testing"

// BenchmarkReplicate measures the Raft state machine alone, with no disk or
// network: a 3-node cluster in memory, one proposal at a time, delivered
// until it is committed everywhere. It shows how cheap the protocol logic
// is next to an fsync or a network round trip.
func BenchmarkReplicate(b *testing.B) {
	c := newCluster(b, 3)
	c.elect(1)
	l := c.nodes[1]
	data := make([]byte, 100)
	b.ResetTimer()
	for range b.N {
		if _, err := l.Propose(data); err != nil {
			b.Fatal(err)
		}
		c.settle()
	}
	b.StopTimer()
	if got := l.Status().Commit; got < uint64(b.N) {
		b.Fatalf("committed %d of %d", got, b.N)
	}
}
