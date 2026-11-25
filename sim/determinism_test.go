package sim_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/20001172024/stormproof/sim"
	"github.com/20001172024/stormproof/sim/toy"
)

func toyConfig(seed uint64, profile sim.Profile, buggy bool) sim.Config {
	return sim.Config{
		Seed:          seed,
		Nodes:         3,
		Duration:      10 * sim.Second,
		TickInterval:  10 * sim.Millisecond,
		Profile:       profile,
		NewProcess:    toy.New(buggy),
		NewInvariants: func() []sim.Invariant { return []sim.Invariant{toy.Monotonic()} },
	}
}

// runTraced runs cfg and returns its result and full trace.
func runTraced(t *testing.T, cfg sim.Config) (sim.Result, []byte) {
	t.Helper()
	var buf bytes.Buffer
	cfg.Trace = &buf
	res, err := sim.Run(cfg)
	if err != nil {
		t.Fatalf("seed %d: %v", cfg.Seed, err)
	}
	return res, buf.Bytes()
}

// firstDiff describes the first line where two traces differ.
func firstDiff(a, b []byte) string {
	la, lb := bytes.Split(a, []byte("\n")), bytes.Split(b, []byte("\n"))
	for i := 0; i < len(la) && i < len(lb); i++ {
		if !bytes.Equal(la[i], lb[i]) {
			return fmt.Sprintf("line %d:\n  run 1: %s\n  run 2: %s", i+1, la[i], lb[i])
		}
	}
	return fmt.Sprintf("one trace is a prefix of the other (%d vs %d lines)", len(la), len(lb))
}

// TestDeterminism is the test the whole project rests on: the same seed
// must produce a byte-identical trace. It runs 40 seeds in every profile
// (200 runs, most with faults), each twice. This test must always pass.
func TestDeterminism(t *testing.T) {
	for _, p := range sim.Profiles {
		t.Run(p.Name, func(t *testing.T) {
			for seed := uint64(1); seed <= 40; seed++ {
				_, a := runTraced(t, toyConfig(seed, p, false))
				_, b := runTraced(t, toyConfig(seed, p, false))
				if !bytes.Equal(a, b) {
					t.Fatalf("seed %d is not deterministic; first difference at %s", seed, firstDiff(a, b))
				}
			}
		})
	}
}

// If the seed were accidentally ignored, every run would be identical and
// TestDeterminism would pass for the wrong reason. Different seeds must
// give different traces.
func TestSeedsChangeTheRun(t *testing.T) {
	p, _ := sim.ProfileByName("chaos")
	seen := map[string]uint64{}
	for seed := uint64(1); seed <= 20; seed++ {
		_, tr := runTraced(t, toyConfig(seed, p, false))
		// Drop the header line, which contains the seed itself.
		body := string(tr[bytes.IndexByte(tr, '\n')+1:])
		if prev, ok := seen[body]; ok {
			t.Fatalf("seeds %d and %d produced the same trace", prev, seed)
		}
		seen[body] = seed
	}
}

// The correct toy node must survive every fault profile.
func TestToySurvivesAllProfiles(t *testing.T) {
	for _, p := range sim.Profiles {
		t.Run(p.Name, func(t *testing.T) {
			for seed := uint64(1); seed <= 100; seed++ {
				res, err := sim.Run(toyConfig(seed, p, false))
				if err != nil {
					t.Fatal(err)
				}
				if res.Violation != nil {
					t.Fatalf("seed %d: %v", seed, res.Violation)
				}
			}
		})
	}
}

// End to end: the simulator finds the planted durability bug, and replaying
// the failing seed reproduces exactly the same violation and trace.
func TestFindsAndReplaysPlantedBug(t *testing.T) {
	p, _ := sim.ProfileByName("crashes")
	var failing *sim.Result
	for seed := uint64(1); seed <= 50 && failing == nil; seed++ {
		res, err := sim.Run(toyConfig(seed, p, true))
		if err != nil {
			t.Fatal(err)
		}
		if res.Violation != nil {
			failing = &res
		}
	}
	if failing == nil {
		t.Fatal("the simulator did not find the planted bug in 50 seeds")
	}
	seed := failing.Seed
	if failing.Violation.Invariant != "counter-monotonic" {
		t.Fatalf("seed %d: unexpected violation %v", seed, failing.Violation)
	}

	replay, tr1 := runTraced(t, toyConfig(seed, p, true))
	_, tr2 := runTraced(t, toyConfig(seed, p, true))
	if *replay.Violation != *failing.Violation {
		t.Fatalf("replay of seed %d gave %v, original gave %v", seed, replay.Violation, failing.Violation)
	}
	if !bytes.Equal(tr1, tr2) {
		t.Fatalf("replays of seed %d differ at %s", seed, firstDiff(tr1, tr2))
	}
	lines := bytes.Split(bytes.TrimSpace(tr1), []byte("\n"))
	if last := lines[len(lines)-1]; !bytes.Contains(last, []byte(`"type":"violation"`)) {
		t.Fatalf("last trace line is not the violation: %s", last)
	}
	// The same seed with the bug fixed must pass: the fault schedule is the
	// same, so this shows the bug, not the faults, caused the violation.
	if res, _ := sim.Run(toyConfig(seed, p, false)); res.Violation != nil {
		t.Fatalf("seed %d also fails with the bug fixed: %v", seed, res.Violation)
	}
}
