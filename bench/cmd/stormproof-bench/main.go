// Command stormproof-bench runs a load against a StormProof cluster and
// reports throughput and latency.
//
//	stormproof-bench -clients 32 -reads 0.9 -duration 30s -out result.json
//
// Each client is closed-loop (its next request goes out when the previous
// one returns) and has its own session, so writes are exactly-once. Gets
// are linearizable (ReadIndex). -timeline writes every operation as CSV,
// which bench/recovery.sh uses to measure how long writes stall when the
// leader dies. -endpoints defaults to $STORMPROOF_ENDPOINTS.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/20001172024/stormproof/bench"
	"github.com/20001172024/stormproof/client"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "stormproof-bench:", err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	fl := flag.NewFlagSet("stormproof-bench", flag.ContinueOnError)
	endpoints := fl.String("endpoints", os.Getenv("STORMPROOF_ENDPOINTS"), "comma-separated server addresses")
	cfg := bench.Config{}
	fl.IntVar(&cfg.Clients, "clients", 16, "concurrent clients")
	fl.DurationVar(&cfg.Duration, "duration", 30*time.Second, "measured time")
	fl.DurationVar(&cfg.Warmup, "warmup", 5*time.Second, "unmeasured time first")
	fl.Float64Var(&cfg.Reads, "reads", 0.5, "fraction of operations that are (linearizable) Gets")
	fl.IntVar(&cfg.Keys, "keys", 1000, "number of distinct keys")
	fl.IntVar(&cfg.ValueSize, "value-size", 100, "bytes per Put")
	fl.DurationVar(&cfg.OpTimeout, "op-timeout", 5*time.Second, "give up on one operation after this long")
	fl.Uint64Var(&cfg.Seed, "seed", 1, "seed for the workload's choices of operation and key")
	attempt := fl.Duration("attempt-timeout", time.Second, "client: give up on one server after this long and try another")
	label := fl.String("label", "", "free text stored in the result, e.g. the cluster setup")
	outPath := fl.String("out", "", "also write the result as JSON to this file")
	timelinePath := fl.String("timeline", "", "write every operation as CSV to this file")
	if err := fl.Parse(args); err != nil {
		return err
	}
	if fl.NArg() > 0 {
		return fmt.Errorf("unexpected arguments: %v", fl.Args())
	}
	if *endpoints == "" {
		return errors.New("no endpoints: use -endpoints or STORMPROOF_ENDPOINTS")
	}
	eps := strings.Split(*endpoints, ",")

	var timeline io.Writer
	if *timelinePath != "" {
		f, err := os.Create(*timelinePath)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		timeline = f
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	var clients []*client.Client
	defer func() {
		for _, c := range clients {
			_ = c.Close()
		}
	}()
	fmt.Fprintf(out, "%d clients, %.0f%% reads, %d keys, %d-byte values: %v warm-up, then %v measured\n",
		cfg.Clients, cfg.Reads*100, cfg.Keys, cfg.ValueSize, cfg.Warmup, cfg.Duration)
	res, err := bench.Run(ctx, cfg, func(int) (bench.KV, error) {
		c, err := client.New(eps, client.Options{AttemptTimeout: *attempt})
		if err != nil {
			return nil, err
		}
		clients = append(clients, c)
		return c, nil
	}, timeline)
	if err != nil {
		return err
	}

	fmt.Fprintf(out, "%.0f ops/sec (%d ops)\n", res.OpsPerSec, res.Ops)
	fmt.Fprintf(out, "%-4s %8s %7s %8s %8s %8s %8s %8s\n", "op", "count", "errors", "p50 ms", "p95 ms", "p99 ms", "max ms", "mean ms")
	for _, op := range []string{bench.OpPut, bench.OpGet} {
		if s, ok := res.ByOp[op]; ok {
			fmt.Fprintf(out, "%-4s %8d %7d %8.2f %8.2f %8.2f %8.2f %8.2f\n", op, s.Count, s.Errors, s.P50Ms, s.P95Ms, s.P99Ms, s.MaxMs, s.MeanMs)
		}
	}
	if *outPath != "" {
		b, err := json.MarshalIndent(struct {
			Label string    `json:"label,omitempty"`
			When  time.Time `json:"when"`
			bench.Result
		}{*label, time.Now().UTC(), res}, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(*outPath, append(b, '\n'), 0o644); err != nil {
			return err
		}
	}
	return nil
}
