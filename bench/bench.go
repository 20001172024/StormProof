// Package bench is StormProof's load generator: concurrent clients issuing
// a configurable mix of reads and writes against a cluster, measuring
// throughput and latency.
//
// Each client is closed-loop: it sends its next request as soon as the
// previous one returns. Throughput therefore depends on the number of
// clients as well as on the cluster, and every result records both.
package bench

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"sync"
	"time"
)

// KV is the part of a client the benchmark uses. *client.Client has it.
type KV interface {
	Get(ctx context.Context, key string) (value []byte, found bool, err error)
	Put(ctx context.Context, key string, value []byte) error
}

// Config describes a run.
type Config struct {
	Clients   int           // concurrent clients, each with its own KV
	Duration  time.Duration // measured time, after the warm-up
	Warmup    time.Duration // time before measuring, to fill caches and settle the leader
	Reads     float64       // fraction of operations that are Gets (linearizable)
	Keys      int           // keys are k0..k(Keys-1), chosen uniformly
	ValueSize int           // bytes per Put
	OpTimeout time.Duration // give up on one operation after this long
	Seed      uint64        // for the workload's choices (not the cluster's)
}

func (c Config) validate() error {
	switch {
	case c.Clients < 1:
		return errors.New("bench: need at least one client")
	case c.Duration <= 0:
		return errors.New("bench: duration must be positive")
	case c.Warmup < 0:
		return errors.New("bench: warm-up must not be negative")
	case c.Reads < 0 || c.Reads > 1:
		return errors.New("bench: reads must be between 0 and 1")
	case c.Keys < 1:
		return errors.New("bench: need at least one key")
	case c.ValueSize < 0:
		return errors.New("bench: value size must not be negative")
	case c.OpTimeout <= 0:
		return errors.New("bench: op timeout must be positive")
	}
	return nil
}

// Op names, as they appear in results and timelines.
const (
	OpGet = "get"
	OpPut = "put"
)

// Sample is one completed operation.
type Sample struct {
	Start   time.Time
	Op      string
	Latency time.Duration
	Err     bool
}

// Stats summarizes one kind of operation over the measured window.
type Stats struct {
	Count  int     `json:"count"`
	Errors int     `json:"errors"`
	P50Ms  float64 `json:"p50_ms"`
	P95Ms  float64 `json:"p95_ms"`
	P99Ms  float64 `json:"p99_ms"`
	MaxMs  float64 `json:"max_ms"`
	MeanMs float64 `json:"mean_ms"`
}

// Result is a run's outcome.
type Result struct {
	Config    Config           `json:"config"`
	Measured  time.Duration    `json:"measured_ns"`
	Ops       int              `json:"ops"` // successful operations in the measured window
	OpsPerSec float64          `json:"ops_per_sec"`
	ByOp      map[string]Stats `json:"by_op"`
}

// Run runs the benchmark. newKV is called once per client. If timeline is
// not nil, every operation (warm-up included) is written to it as CSV:
// start time in Unix nanoseconds, op, latency in microseconds, ok.
func Run(ctx context.Context, cfg Config, newKV func(i int) (KV, error), timeline io.Writer) (Result, error) {
	if err := cfg.validate(); err != nil {
		return Result{}, err
	}
	kvs := make([]KV, cfg.Clients)
	for i := range kvs {
		kv, err := newKV(i)
		if err != nil {
			return Result{}, fmt.Errorf("bench: client %d: %w", i, err)
		}
		kvs[i] = kv
	}

	value := make([]byte, cfg.ValueSize)
	for i := range value {
		value[i] = byte('a' + i%26)
	}

	start := time.Now()
	measureFrom := start.Add(cfg.Warmup)
	end := measureFrom.Add(cfg.Duration)
	runCtx, cancel := context.WithDeadline(ctx, end)
	defer cancel()

	samples := make([][]Sample, cfg.Clients)
	var wg sync.WaitGroup
	for i, kv := range kvs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(cfg.Seed, uint64(i)))
			for runCtx.Err() == nil {
				key := fmt.Sprintf("k%d", rng.IntN(cfg.Keys))
				op := OpPut
				if rng.Float64() < cfg.Reads {
					op = OpGet
				}
				opCtx, opCancel := context.WithTimeout(runCtx, cfg.OpTimeout)
				t0 := time.Now()
				var err error
				if op == OpGet {
					_, _, err = kv.Get(opCtx, key)
				} else {
					err = kv.Put(opCtx, key, value)
				}
				lat := time.Since(t0)
				opCancel()
				// An operation cut off by the end of the run says nothing about
				// the cluster: leave it out rather than count it as an error.
				if err != nil && runCtx.Err() != nil {
					break
				}
				samples[i] = append(samples[i], Sample{Start: t0, Op: op, Latency: lat, Err: err != nil})
			}
		}()
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	all := slices.Concat(samples...)
	slices.SortFunc(all, func(a, b Sample) int { return a.Start.Compare(b.Start) })
	if timeline != nil {
		if err := writeTimeline(timeline, all); err != nil {
			return Result{}, err
		}
	}
	return summarize(cfg, all, measureFrom, end), nil
}

// summarize computes the result from the samples that started inside the
// measured window [from, to).
func summarize(cfg Config, all []Sample, from, to time.Time) Result {
	lat := map[string][]time.Duration{}
	errs := map[string]int{}
	ok := 0
	for _, s := range all {
		if s.Start.Before(from) || !s.Start.Before(to) {
			continue
		}
		if s.Err {
			errs[s.Op]++
			continue
		}
		ok++
		lat[s.Op] = append(lat[s.Op], s.Latency)
	}
	res := Result{Config: cfg, Measured: to.Sub(from), Ops: ok, ByOp: map[string]Stats{}}
	res.OpsPerSec = float64(ok) / res.Measured.Seconds()
	for _, op := range []string{OpGet, OpPut} {
		if len(lat[op]) == 0 && errs[op] == 0 {
			continue
		}
		st := stats(lat[op])
		st.Errors = errs[op]
		res.ByOp[op] = st
	}
	return res
}

// stats computes exact percentiles by sorting every latency: a run has at
// most a few hundred thousand, so no histogram is needed.
func stats(lat []time.Duration) Stats {
	if len(lat) == 0 {
		return Stats{}
	}
	slices.Sort(lat)
	var sum time.Duration
	for _, l := range lat {
		sum += l
	}
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	return Stats{
		Count:  len(lat),
		P50Ms:  ms(Percentile(lat, 50)),
		P95Ms:  ms(Percentile(lat, 95)),
		P99Ms:  ms(Percentile(lat, 99)),
		MaxMs:  ms(lat[len(lat)-1]),
		MeanMs: ms(sum / time.Duration(len(lat))),
	}
}

// Percentile returns the p-th percentile of sorted, by the nearest-rank
// method: the smallest value with at least p% of the values at or below it.
func Percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(p / 100 * float64(len(sorted)))
	if float64(rank) < p/100*float64(len(sorted)) {
		rank++ // ceil
	}
	rank = max(rank, 1)
	return sorted[min(rank, len(sorted))-1]
}

func writeTimeline(w io.Writer, all []Sample) error {
	if _, err := fmt.Fprintln(w, "start_unix_ns,op,latency_us,ok"); err != nil {
		return err
	}
	for _, s := range all {
		if _, err := fmt.Fprintf(w, "%d,%s,%d,%t\n", s.Start.UnixNano(), s.Op, s.Latency.Microseconds(), !s.Err); err != nil {
			return err
		}
	}
	return nil
}
