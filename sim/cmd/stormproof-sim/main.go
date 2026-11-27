// Command stormproof-sim runs StormProof simulations.
//
// Run many seeds and report failures:
//
//	stormproof-sim -seeds 1000 -profile chaos -trace-dir traces/
//
// Seeds run -parallel at a time (by default, one per CPU). Each seed's run
// is unaffected, and results are printed in seed order, so the output is
// the same for any -parallel. Each failing seed is printed with the command
// that replays it.
//
// Replay one seed and write its trace:
//
//	stormproof-sim -seed 17 -profile chaos -trace seed-17.jsonl
//
// -node picks what runs on each simulated server: "raft" (the default) or
// "toy", the replicated counter used to test the simulator itself. Raft runs
// have -clients simulated KV clients, and end by checking the cluster
// settled and the clients' history is linearizable.
//
// -bug plants a known bug, to see the simulator find it: "fsync" (both node
// kinds; -buggy is short for it), and for raft "dedup", "local-reads",
// "read-quorum", "snapshot-sessions" and "no-prevote" (pre-vote off: not a
// safety bug, but under -profile storm it costs liveness).
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/20001172024/stormproof/sim"
	"github.com/20001172024/stormproof/sim/raftsim"
	"github.com/20001172024/stormproof/sim/toy"
)

func main() {
	// A simulation's live heap is only a few MB, but it allocates fast, so
	// with Go's default GOGC=100 the garbage collector runs almost all the
	// time and parallel workers mostly wait for it: 4 workers ran only 1.9
	// times as fast as 1. At 400 they run 3.6 times as fast, for about
	// 10 MB more memory. GOGC in the environment still wins.
	if os.Getenv("GOGC") == "" {
		debug.SetGCPercent(400)
	}
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
	bug                string
	clients            int
	membership         bool
	parallel           int
	batch              bool
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
	buggy := fl.Bool("buggy", false, `short for -bug fsync`)
	fl.StringVar(&o.bug, "bug", "", `plant a bug: "fsync", or for raft "dedup", "local-reads", "read-quorum", "snapshot-sessions", "no-prevote"`)
	fl.IntVar(&o.clients, "clients", raftsim.DefaultClients, "simulated KV clients (raft only)")
	fl.IntVar(&o.parallel, "parallel", runtime.NumCPU(), "without -seed: seeds to run at once (results don't depend on it)")
	fl.BoolVar(&o.batch, "batch", false, "raft only: process events in batches, as a busy server does (each event waits for the node's next one)")
	fl.BoolVar(&o.membership, "membership", false, "raft only: add two spare servers and an admin that adds and removes servers during the run (-nodes is the initial configuration)")
	if err := fl.Parse(args); err != nil {
		return o, err
	}
	if fl.NArg() > 0 {
		return o, fmt.Errorf("unexpected arguments: %v", fl.Args())
	}
	if *buggy {
		o.bug = "fsync"
	}
	switch {
	case o.bug == "" || o.bug == "fsync":
	case o.node == "raft" && (o.bug == "dedup" || o.bug == "local-reads" || o.bug == "read-quorum" || o.bug == "snapshot-sessions" || o.bug == "no-prevote"):
	default:
		return o, fmt.Errorf("unknown -bug %q for -node %s", o.bug, o.node)
	}

	if o.parallel < 1 {
		return o, errors.New("-parallel must be at least 1")
	}
	if o.node != "raft" && o.node != "toy" {
		return o, fmt.Errorf(`-node must be "raft" or "toy", not %q`, o.node)
	}
	if *profile == "all" {
		o.profiles = sim.Profiles // the heavier profiles in sim.HeavyProfiles only run when named
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
	servers := o.nodes
	if o.membership && o.node == "raft" {
		servers = o.nodes + 2
	}
	cfg := sim.Config{
		Seed:         seed,
		Nodes:        servers,
		Duration:     o.duration,
		Quiet:        o.quiet,
		TickInterval: o.tick,
		Profile:      o.profiles[seed%uint64(len(o.profiles))],
		Trace:        trace,
	}
	switch o.node {
	case "raft":
		ro := raftsim.DefaultOptions
		ro.Batch = o.batch
		switch o.bug {
		case "fsync":
			ro.SkipSync = true
		case "dedup":
			ro.NoDedup = true
		case "local-reads":
			ro.LocalReads = true
		case "read-quorum":
			ro.Knobs.ReadIndexWithoutQuorum = true
		case "snapshot-sessions":
			ro.SnapshotDropsSessions = true
		case "no-prevote":
			ro.PreVote = false
		}
		co := raftsim.DefaultClientOptions
		ad := raftsim.DefaultAdminOptions
		if o.quiet > 0 {
			// Stop new work partway through the quiet period so the cluster
			// can finish replicating, then require it to have settled.
			stop := o.duration - min(o.quiet, 3*sim.Second)
			ro.ProposeUntil = stop
			co.Until, co.RetryUntil = stop, stop+(o.duration-stop)/2
			ad.Until, ad.RetryUntil = co.Until, co.RetryUntil
			cfg.FinalCheck = raftsim.Settled
		}
		var admin *raftsim.AdminOptions
		if o.membership {
			ro.InitialVoters = o.nodes
			admin = &ad
		}
		if o.clients > 0 || admin != nil {
			ro.ProposeProb = 0 // the clients are the workload
			cfg.Clients = o.clients
			if admin != nil {
				cfg.Clients++
			}
			cfg.NewClient = raftsim.NewClients(co, admin, o.nodes)
			cfg.FinalCheck = raftsim.Linearizable
			if o.quiet > 0 {
				cfg.FinalCheck = raftsim.SettledAndLinearizable
			}
		}
		cfg.NewProcess = raftsim.New(ro)
		cfg.NewInvariants = raftsim.Invariants
	case "toy":
		cfg.NewProcess = toy.New(o.bug == "fsync")
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

// result is the outcome of one seed in a sweep.
type result struct {
	seed    uint64
	profile string
	res     sim.Result
	err     error
}

// sweep runs a range of seeds on o.parallel workers. Each simulation is
// still single-threaded and seeded only by its seed, so running several at
// once changes nothing about any one of them; results are reported in seed
// order, so the output doesn't depend on the number of workers either.
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
	seeds := make(chan uint64)
	results := make(chan result)
	stop := make(chan struct{}) // closed to make the workers give up early
	var wg sync.WaitGroup
	for range o.parallel {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seed := range seeds {
				cfg := o.config(seed, nil) // no trace: fast
				res, err := sim.Run(cfg)
				select {
				case results <- result{seed, cfg.Profile.Name, res, err}:
				case <-stop:
					return
				}
			}
		}()
	}
	go func() {
		defer close(seeds)
		for seed := o.start; seed < o.start+o.seeds; seed++ {
			select {
			case seeds <- seed:
			case <-stop:
				return
			}
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()
	defer close(stop)

	// Results arrive in any order; hold them until every earlier seed has
	// been reported.
	pending := map[uint64]result{}
	next := o.start
	var failed, done uint64
	for r := range results {
		pending[r.seed] = r
		for {
			r, ok := pending[next]
			if !ok {
				break
			}
			delete(pending, next)
			next++
			done++
			if r.err != nil {
				fmt.Fprintf(stderr, "stormproof-sim: seed %d: %v\n", r.seed, r.err)
				return exitError
			}
			if r.res.Violation != nil {
				failed++
				fmt.Fprintf(stdout, "seed %d (%s): %v\n", r.seed, r.profile, r.res.Violation)
				fmt.Fprintf(stdout, "  replay: %s\n", o.replayCommand(r.seed, r.profile))
				if o.traceDir != "" {
					if code := saveTrace(o, r.seed, r.res.Violation, stdout, stderr); code != exitOK {
						return code
					}
				}
			}
			if o.seeds >= 1000 && done%(o.seeds/10) == 0 && done < o.seeds {
				fmt.Fprintf(stdout, "  ... %d/%d seeds, %d failed, %v\n", done, o.seeds, failed, time.Since(begin).Round(time.Second))
			}
		}
	}

	elapsed := time.Since(begin)
	fmt.Fprintf(stdout, "%d seeds (%d to %d), %d failed, in %v (%.0f seeds/min, %d workers)\n",
		o.seeds, o.start, o.start+o.seeds-1, failed, elapsed.Round(time.Millisecond),
		float64(o.seeds)/elapsed.Minutes(), o.parallel)
	if failed > 0 {
		return exitViolation
	}
	return exitOK
}

// replayCommand is the command that re-runs one seed of this sweep on its
// own, with the flags that shape the run. The profile is the seed's own
// profile: with -profile all, a seed maps to one of them, and replaying
// with just that profile picks it again.
func (o options) replayCommand(seed uint64, profile string) string {
	args := []string{"go run ./sim/cmd/stormproof-sim", fmt.Sprintf("-seed %d", seed), "-profile " + profile}
	if o.node != "raft" {
		args = append(args, "-node "+o.node)
	}
	if o.nodes != 3 {
		args = append(args, fmt.Sprintf("-nodes %d", o.nodes))
	}
	if o.membership {
		args = append(args, "-membership")
	}
	if o.batch {
		args = append(args, "-batch")
	}
	if o.bug != "" {
		args = append(args, "-bug "+o.bug)
	}
	if o.node == "raft" && o.clients != raftsim.DefaultClients {
		args = append(args, fmt.Sprintf("-clients %d", o.clients))
	}
	dur := func(t sim.Time) time.Duration { return time.Duration(t) * time.Microsecond }
	if d := dur(o.duration); d != 15*time.Second {
		args = append(args, "-duration "+d.String())
	}
	if d := dur(o.quiet); d != 5*time.Second {
		args = append(args, "-quiet "+d.String())
	}
	if d := dur(o.tick); d != 10*time.Millisecond {
		args = append(args, "-tick "+d.String())
	}
	return strings.Join(args, " ") + fmt.Sprintf(" -trace seed-%d.jsonl", seed)
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
