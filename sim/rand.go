package sim

import (
	"hash/fnv"
	"math/rand/v2"
)

// Rand is a seeded random number generator. Every random decision in a
// simulation comes from a Rand derived from the run's seed, which is what
// makes a seed replayable.
//
// Each concern (network faults, crashes, each node, ...) gets its own
// stream, named by a string. Streams are independent: drawing one more
// number from the "network" stream does not shift the numbers the "faults"
// stream produces. Without that, a small code change in one place (say, the
// workload drawing an extra number) would change every later decision
// everywhere, and an old seed would stop reproducing an old bug.
//
// Rand is not safe for concurrent use; the simulator is single-threaded.
type Rand struct {
	r *rand.Rand
}

// NewRand returns the stream called name for the given seed. The same
// (seed, name) pair always produces the same sequence of numbers, as long
// as the Go version stays the same.
func NewRand(seed uint64, name string) *Rand {
	h := fnv.New64a()
	_, _ = h.Write([]byte(name)) // hash.Hash.Write never returns an error
	return &Rand{r: rand.New(rand.NewPCG(seed, h.Sum64()))}
}

// Intn returns a uniform int in [0, n). It panics if n <= 0.
func (r *Rand) Intn(n int) int {
	return r.r.IntN(n)
}

// Chance returns true with probability p. It always consumes exactly one
// draw, whatever p is, so turning a fault off does not shift the stream.
func (r *Rand) Chance(p float64) bool {
	return r.r.Float64() < p
}

// Between returns a uniform Time in [lo, hi). If hi <= lo it returns lo
// without consuming a draw.
func (r *Rand) Between(lo, hi Time) Time {
	if hi <= lo {
		return lo
	}
	return lo + Time(r.r.Int64N(int64(hi-lo)))
}

// Shuffle randomly permutes n elements using swap.
func (r *Rand) Shuffle(n int, swap func(i, j int)) {
	r.r.Shuffle(n, swap)
}
