package raftsim

import (
	"strings"
	"testing"

	"github.com/20001172024/stormproof/raft"
	"github.com/20001172024/stormproof/sim"
)

// kvConfig is a 15 s run with DefaultClients clients: they start operations
// until 12 s, retry until 13.5 s, and the run must end settled and with a
// linearizable history.
func kvConfig(seed uint64, profile string, o Options) sim.Config {
	o.ProposeProb = 0 // the clients are the workload
	cfg := config(seed, profile, o)
	co := DefaultClientOptions
	co.Until, co.RetryUntil = 12*sim.Second, 13500*sim.Millisecond
	cfg.Clients = DefaultClients
	cfg.NewClient = NewClient(co)
	cfg.FinalCheck = SettledAndLinearizable
	return cfg
}

func TestKVLinearizableUnderAllProfiles(t *testing.T) {
	for _, p := range sim.Profiles {
		t.Run(p.Name, func(t *testing.T) {
			for seed := uint64(1); seed <= 10; seed++ {
				if res := run(t, kvConfig(seed, p.Name, testOptions())); res.Violation != nil {
					t.Fatalf("seed %d: %v", seed, res.Violation)
				}
			}
		})
	}
}

// The clients really do work: most operations finish, and every kind of
// operation appears.
func TestKVWorkload(t *testing.T) {
	cfg := kvConfig(1, "calm", testOptions())
	var total, done int
	ops := map[string]int{}
	cfg.FinalCheck = func(nodes []sim.NodeView) error {
		for _, nv := range nodes {
			if nv.Client {
				for _, op := range nv.Process.(*Client).History() {
					total++
					ops[op.Cmd.Op.String()]++
					if op.Done {
						done++
					}
				}
			}
		}
		return SettledAndLinearizable(nodes)
	}
	if res := run(t, cfg); res.Violation != nil {
		t.Fatal(res.Violation)
	}
	if total < 500 || done < total*9/10 {
		t.Fatalf("%d operations, %d finished", total, done)
	}
	for _, op := range []string{"get", "put", "cas", "delete"} {
		if ops[op] == 0 {
			t.Errorf("no %s operations", op)
		}
	}
}

// findViolation runs seeds until one fails, and checks it replays.
func findViolation(t *testing.T, profile string, o Options, seeds uint64) *sim.Result {
	t.Helper()
	for seed := uint64(1); seed <= seeds; seed++ {
		res := run(t, kvConfig(seed, profile, o))
		if res.Violation == nil {
			continue
		}
		if again := run(t, kvConfig(seed, profile, o)); *again.Violation != *res.Violation {
			t.Fatalf("seed %d did not replay: %v then %v", seed, res.Violation, again.Violation)
		}
		return &res
	}
	return nil
}

// Without deduplication a retried write is applied twice, and the checker
// sees it.
func TestNoDedupIsNotLinearizable(t *testing.T) {
	o := testOptions()
	o.NoDedup = true
	res := findViolation(t, "flaky-net", o, 10)
	if res == nil || !strings.Contains(res.Violation.Detail, "linearizab") {
		t.Fatalf("got %v, want a linearizability violation within 10 seeds", res)
	}
	if fixed := run(t, kvConfig(res.Seed, "flaky-net", testOptions())); fixed.Violation != nil {
		t.Fatalf("seed %d also fails with dedup on: %v", res.Seed, fixed.Violation)
	}
}

// Serving reads from local state without ReadIndex returns stale data, and
// the checker sees it. (It needs the chaos profile and several seeds: see
// docs/design.md for why stale reads are hard to provoke.)
func TestLocalReadsAreNotLinearizable(t *testing.T) {
	for _, bug := range []struct {
		name string
		set  func(*Options)
	}{
		{"local reads", func(o *Options) { o.LocalReads = true }},
		{"ReadIndex without quorum", func(o *Options) { o.Knobs = raft.TestingKnobs{ReadIndexWithoutQuorum: true} }},
	} {
		o := testOptions()
		bug.set(&o)
		res := findViolation(t, "chaos", o, 50)
		if res == nil || !strings.Contains(res.Violation.Detail, "linearizab") {
			t.Errorf("%s: got %v, want a linearizability violation within 50 chaos seeds", bug.name, res)
		}
	}
}
