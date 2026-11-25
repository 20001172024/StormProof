// Command stormproof-sim runs StormProof simulations.
//
// Run many seeds and report failures:
//
//	stormproof-sim -seeds 1000 -profile chaos -trace-dir traces/
//
// Replay one seed and write its trace:
//
//	stormproof-sim -seed 17 -profile chaos -trace seed-17.jsonl
//
// -node picks what runs on each simulated server: "raft" (the default) or
// "toy", the replicated counter used to test the simulator itself. -buggy
// plants a known durability bug in either, to see the simulator find it.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/20001172024/stormproof/sim"
	"github.com/20001172024/stormproof/sim/raftsim"
	"github.com/20001172024/stormproof/sim/toy"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// Exit codes.
const (
	exitOK        = 0
	exitViolation = 1 // at least one seed broke an invariant
	exitError     = 2 // bad flags, or the simulator itself failed
)

type options struct {
	seed, start, seeds uint64
	profiles           []sim.Profile
	nodes              int
	node               string
	duration, tick     sim.Time
	quiet              sim.Time
	trace, traceDir    string
	buggy              bool
}

// run is main without the os.Exit, so tests can call it.
func run(args []string, stdout, stderr io.Writer) int {
	opts, err := parseFlags(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		fmt.Fprintln(stderr, "stormproof-sim:", err)
		return exitError
	}

	if opts.seed != 0 {
		return replay(opts, stdout, stderr)
	}
	return sweep(opts, stdout, stderr)
}

func parseFlags(args []string, stderr io.Writer) (options, error) {
	fl := flag.NewFlagSet("stormproof-sim", flag.ContinueOnError)
	fl.SetOutput(stderr)
	var (
		o        options
		profile  = fl.String("profile", "chaos", `fault profile, or "all" to cycle through every profile`)
		duration = fl.Duration("duration", 15*time.Second, "virtual time per run")
		quiet    = fl.Duration("quiet", 5*time.Second, "no new faults in the last part of each run; raft must then settle on one leader with every log identical and applied (0 disables)")
		tick     = fl.Duration("tick", 10*time.Millisecond, "base tick interval")
	)
	fl.Uint64Var(&o.seed, "seed", 0, "run just this seed (replay); 0 means run a range with -start and -seeds")
	fl.Uint64Var(&o.start, "start", 1, "first seed of the range")
	fl.Uint64Var(&o.seeds, "seeds", 100, "number of seeds in the range")
	fl.IntVar(&o.nodes, "nodes", 3, "cluster size")
	fl.StringVar(&o.node, "node", "raft", `what runs on each node: "raft" or "toy"`)
	fl.StringVar(&o.trace, "trace", "", "with -seed: write the trace to this file")
	fl.StringVar(&o.traceDir, "trace-dir", "", "without -seed: write a trace for each failing seed into this directory")
	fl.BoolVar(&o.buggy, "buggy", false, "plant the toy node's durability bug")
	if err := fl.Parse(args); err != nil {
		return o, err
	}
	if fl.NArg() > 0 {
		return o, fmt.Errorf("unexpected arguments: %v", fl.Args())
	}

	if o.node != "raft" && o.node != "toy" {
		return o, fmt.Errorf(`-node must be "raft" or "toy", not %q`, o.node)
	}
	if *profile == "all" {
		o.profiles = sim.Profiles
	} else {
		p, err := sim.ProfileByName(*profile)
		if err != nil {
			return o, err
		}
		o.profiles = []sim.Profile{p}
	}
	// The simulator works in virtual microseconds.
	o.duration = sim.Time(duration.Microseconds())
	o.tick = sim.Time(tick.Microseconds())
	o.quiet = sim.Time(quiet.Microseconds())
	if o.quiet > o.duration {
		return o, errors.New("-quiet must not be longer than -duration")
	}
	return o, nil
}

// config builds the simulation for one seed. With several profiles, seeds
// are spread across them in turn, so a seed always maps to the same profile.
func (o options) config(seed uint64, trace io.Writer) sim.Config {
	cfg := sim.Config{
		Seed:         seed,
		Nodes:        o.nodes,
		Duration:     o.duration,
		Quiet:        o.quiet,
		TickInterval: o.tick,
		Profile:      o.profiles[seed%uint64(len(o.profiles))],
		Trace:        trace,
	}
	switch o.node {
	case "raft":
		ro := raftsim.DefaultOptions
		ro.SkipSync = o.buggy
		if o.quiet > 0 {
			// Stop proposing partway through the quiet period, so the cluster
			// can finish replicating, then require it to have settled.
			ro.ProposeUntil = o.duration - min(o.quiet, 3*sim.Second)
			cfg.FinalCheck = raftsim.Settled
		}
		cfg.NewProcess = raftsim.New(ro)
		cfg.NewInvariants = raftsim.Invariants
	case "toy":
		cfg.NewProcess = toy.New(o.buggy)
		cfg.NewInvariants = func() []sim.Invariant { return []sim.Invariant{toy.Monotonic()} }
	}
	return cfg
}

func replay(o options, stdout, stderr io.Writer) int {
	var trace *os.File
	if o.trace != "" {
		f, err := os.Create(o.trace)
		if err != nil {
			fmt.Fprintln(stderr, "stormproof-sim:", err)
			return exitError
		}
		trace = f
	}
	cfg := o.config(o.seed, nil)
	if trace != nil {
		cfg.Trace = trace
	}
	res, err := sim.Run(cfg)
	if trace != nil {
		// A failed Close can mean the end of the trace never reached the
		// disk, so it is an error, not something to ignore.
		if cerr := trace.Close(); err == nil {
			err = cerr
		}
	}
	if err != nil {
		fmt.Fprintln(stderr, "stormproof-sim:", err)
		return exitError
	}
	st := res.Stats
	fmt.Fprintf(stdout, "seed %d, profile %s: %d steps, %v virtual\n", res.Seed, cfg.Profile.Name, res.Steps, res.End)
	fmt.Fprintf(stdout, "  messages: %d sent, %d delivered, %d dropped\n", st.Sent, st.Delivered, st.Dropped)
	fmt.Fprintf(stdout, "  faults:   %d crashes, %d restarts, %d partitions\n", st.Crashes, st.Restarts, st.Partitions)
	if o.trace != "" {
		fmt.Fprintf(stdout, "  trace:    %s\n", o.trace)
	}
	if res.Violation != nil {
		fmt.Fprintf(stdout, "VIOLATION: %v\n", res.Violation)
		return exitViolation
	}
	fmt.Fprintln(stdout, "ok")
	return exitOK
}

func sweep(o options, stdout, stderr io.Writer) int {
	if o.seeds == 0 {
		fmt.Fprintln(stderr, "stormproof-sim: -seeds must be at least 1")
		return exitError
	}
	if o.traceDir != "" {
		if err := os.MkdirAll(o.traceDir, 0o755); err != nil {
			fmt.Fprintln(stderr, "stormproof-sim:", err)
			return exitError
		}
	}

	begin := time.Now() // wall-clock time, only to report speed
	var failed int
	for seed := o.start; seed < o.start+o.seeds; seed++ {
		cfg := o.config(seed, nil) // no trace: fast
		res, err := sim.Run(cfg)
		if err != nil {
			fmt.Fprintf(stderr, "stormproof-sim: seed %d: %v\n", seed, err)
			return exitError
		}
		if res.Violation == nil {
			continue
		}
		failed++
		fmt.Fprintf(stdout, "seed %d (%s): %v\n", seed, cfg.Profile.Name, res.Violation)
		if o.traceDir != "" {
			if code := saveTrace(o, seed, res.Violation, stdout, stderr); code != exitOK {
				return code
			}
		}
	}

	elapsed := time.Since(begin)
	fmt.Fprintf(stdout, "%d seeds (%d to %d), %d failed, in %v (%.0f seeds/min)\n",
		o.seeds, o.start, o.start+o.seeds-1, failed, elapsed.Round(time.Millisecond),
		float64(o.seeds)/elapsed.Minutes())
	if failed > 0 {
		return exitViolation
	}
	return exitOK
}

// saveTrace re-runs a failing seed with tracing on. Getting the same
// violation again is itself a determinism check; if it differs, the
// simulator is broken and we say so loudly.
func saveTrace(o options, seed uint64, want *sim.Violation, stdout, stderr io.Writer) int {
	path := filepath.Join(o.traceDir, fmt.Sprintf("seed-%d.jsonl", seed))
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintln(stderr, "stormproof-sim:", err)
		return exitError
	}
	res, err := sim.Run(o.config(seed, f))
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		fmt.Fprintf(stderr, "stormproof-sim: seed %d: %v\n", seed, err)
		return exitError
	}
	if res.Violation == nil || *res.Violation != *want {
		fmt.Fprintf(stderr, "stormproof-sim: NONDETERMINISM: seed %d gave %v first and %v on replay\n", seed, want, res.Violation)
		return exitError
	}
	fmt.Fprintf(stdout, "  trace: %s\n", path)
	return exitOK
}
