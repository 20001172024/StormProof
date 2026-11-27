package bench

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	ms := func(xs ...int) []time.Duration {
		out := make([]time.Duration, len(xs))
		for i, x := range xs {
			out[i] = time.Duration(x) * time.Millisecond
		}
		return out
	}
	hundred := make([]time.Duration, 100)
	for i := range hundred {
		hundred[i] = time.Duration(i+1) * time.Millisecond
	}
	cases := []struct {
		name   string
		sorted []time.Duration
		p      float64
		want   time.Duration
	}{
		{"empty", nil, 50, 0},
		{"one", ms(7), 99, 7 * time.Millisecond},
		{"median of 4 is the 2nd", ms(1, 2, 3, 4), 50, 2 * time.Millisecond},
		{"p50 of 1..100", hundred, 50, 50 * time.Millisecond},
		{"p99 of 1..100", hundred, 99, 99 * time.Millisecond},
		{"p99.9 of 1..100 is the max", hundred, 99.9, 100 * time.Millisecond},
		{"p0 is the min", hundred, 0, 1 * time.Millisecond},
	}
	for _, c := range cases {
		if got := Percentile(c.sorted, c.p); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

// fakeKV answers instantly; every failEvery-th Put fails.
type fakeKV struct {
	mu        sync.Mutex
	data      map[string][]byte
	puts      int
	failEvery int
}

func (f *fakeKV) Get(_ context.Context, key string) ([]byte, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.data[key]
	time.Sleep(50 * time.Microsecond)
	return v, ok, nil
}

func (f *fakeKV) Put(_ context.Context, key string, value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts++
	time.Sleep(50 * time.Microsecond)
	if f.failEvery > 0 && f.puts%f.failEvery == 0 {
		return errors.New("injected")
	}
	f.data[key] = value
	return nil
}

func TestRun(t *testing.T) {
	kv := &fakeKV{data: map[string][]byte{}, failEvery: 10}
	cfg := Config{Clients: 4, Duration: 300 * time.Millisecond, Warmup: 100 * time.Millisecond,
		Reads: 0.75, Keys: 10, ValueSize: 8, OpTimeout: time.Second, Seed: 1}
	var tl bytes.Buffer
	res, err := Run(context.Background(), cfg, func(int) (KV, error) { return kv, nil }, &tl)
	if err != nil {
		t.Fatal(err)
	}
	get, put := res.ByOp[OpGet], res.ByOp[OpPut]
	if get.Count == 0 || put.Count == 0 {
		t.Fatalf("no gets or puts measured: %+v", res.ByOp)
	}
	if res.Ops != get.Count+put.Count {
		t.Fatalf("ops %d != gets %d + puts %d", res.Ops, get.Count, put.Count)
	}
	readFrac := float64(get.Count) / float64(get.Count+put.Count+put.Errors)
	if readFrac < 0.65 || readFrac > 0.85 {
		t.Errorf("read fraction %.2f, want about 0.75", readFrac)
	}
	if put.Errors == 0 || get.Errors != 0 {
		t.Errorf("errors: get %d put %d, want only put errors", get.Errors, put.Errors)
	}
	if want := res.Ops; res.OpsPerSec < float64(want)/0.31 || res.OpsPerSec > float64(want)/0.29 {
		t.Errorf("ops/sec %.0f for %d ops in 300 ms", res.OpsPerSec, want)
	}
	if !(get.P50Ms <= get.P95Ms && get.P95Ms <= get.P99Ms && get.P99Ms <= get.MaxMs) {
		t.Errorf("percentiles out of order: %+v", get)
	}

	// The timeline has every operation, warm-up included, so more than the
	// measured window.
	lines := strings.Split(strings.TrimSpace(tl.String()), "\n")
	if lines[0] != "start_unix_ns,op,latency_us,ok" {
		t.Fatalf("timeline header %q", lines[0])
	}
	if n := len(lines) - 1; n <= res.Ops+put.Errors {
		t.Errorf("timeline has %d operations, the measured window %d: warm-up missing", n, res.Ops+put.Errors)
	}
	if !strings.Contains(tl.String(), ",put,") || !strings.Contains(tl.String(), ",false") {
		t.Errorf("timeline lacks puts or failures")
	}
}

func TestRunValidates(t *testing.T) {
	bad := []Config{
		{},
		{Clients: 1, Duration: time.Second, Reads: 2, Keys: 1, OpTimeout: time.Second},
		{Clients: 1, Duration: time.Second, Reads: 0.5, Keys: 0, OpTimeout: time.Second},
		{Clients: 1, Duration: time.Second, Reads: 0.5, Keys: 1},
	}
	for _, c := range bad {
		if _, err := Run(context.Background(), c, nil, nil); err == nil {
			t.Errorf("config %+v accepted", c)
		}
	}
}

func TestSummarizeWindow(t *testing.T) {
	t0 := time.Unix(1000, 0)
	at := func(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }
	all := []Sample{
		{Start: at(50), Op: OpPut, Latency: time.Millisecond},                 // warm-up: excluded
		{Start: at(100), Op: OpPut, Latency: 2 * time.Millisecond},            // first in window
		{Start: at(150), Op: OpGet, Latency: 3 * time.Millisecond},            //
		{Start: at(180), Op: OpGet, Latency: 9 * time.Millisecond, Err: true}, // counted as an error only
		{Start: at(200), Op: OpPut, Latency: 4 * time.Millisecond},            // at the end: excluded
	}
	res := summarize(Config{}, all, at(100), at(200))
	if res.Ops != 2 || res.ByOp[OpPut].Count != 1 || res.ByOp[OpGet].Count != 1 || res.ByOp[OpGet].Errors != 1 {
		t.Fatalf("got %+v", res)
	}
	if res.ByOp[OpPut].MaxMs != 2 || res.OpsPerSec != 20 {
		t.Fatalf("got %+v", res)
	}
}
