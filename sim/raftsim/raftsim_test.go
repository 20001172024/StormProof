package raftsim

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/20001172024/stormproof/raft"
	"github.com/20001172024/stormproof/sim"
)

func config(seed uint64, profile string, o Options) sim.Config {
	p, err := sim.ProfileByName(profile)
	if err != nil {
		panic(err)
	}
	return sim.Config{
		Seed:          seed,
		Nodes:         3,
		Duration:      15 * sim.Second,
		Quiet:         5 * sim.Second,
		TickInterval:  10 * sim.Millisecond,
		Profile:       p,
		NewProcess:    New(o),
		NewInvariants: Invariants,
		FinalCheck:    StableLeader,
	}
}

func run(t *testing.T, cfg sim.Config) sim.Result {
	t.Helper()
	res, err := sim.Run(cfg)
	if err != nil {
		t.Fatalf("seed %d: %v", cfg.Seed, err)
	}
	return res
}

// Election safety, persistence invariants and the end-of-run liveness check
// must hold in every profile, for 3- and 5-node clusters.
func TestElectionUnderAllProfiles(t *testing.T) {
	for _, p := range sim.Profiles {
		for _, nodes := range []int{3, 5} {
			t.Run(fmt.Sprintf("%s/%d-nodes", p.Name, nodes), func(t *testing.T) {
				for seed := uint64(1); seed <= 40; seed++ {
					cfg := config(seed, p.Name, DefaultOptions)
					cfg.Nodes = nodes
					if res := run(t, cfg); res.Violation != nil {
						t.Fatalf("seed %d: %v", seed, res.Violation)
					}
				}
			})
		}
	}
}

// With no faults a leader must be elected quickly: the first election
// timeout is at most 200 ms, so 1 s leaves room for a split vote or two.
func TestLeaderElectedQuicklyWhenCalm(t *testing.T) {
	for seed := uint64(1); seed <= 100; seed++ {
		cfg := config(seed, "calm", DefaultOptions)
		cfg.Duration = 1 * sim.Second
		cfg.Quiet = 0
		if res := run(t, cfg); res.Violation != nil {
			t.Fatalf("seed %d: %v", seed, res.Violation)
		}
	}
}

func TestDeterminism(t *testing.T) {
	for _, p := range sim.Profiles {
		for seed := uint64(1); seed <= 10; seed++ {
			traces := make([][]byte, 2)
			for i := range traces {
				var buf bytes.Buffer
				cfg := config(seed, p.Name, DefaultOptions)
				cfg.Trace = &buf
				run(t, cfg)
				traces[i] = buf.Bytes()
			}
			if !bytes.Equal(traces[0], traces[1]) {
				t.Fatalf("%s seed %d: traces differ", p.Name, seed)
			}
		}
	}
}

// The reason Figure 2 requires votedFor to be persisted before replying:
// with the fsync skipped, a machine crash makes a node forget its vote, vote
// again in the same term, and two leaders get elected. Only election safety
// is checked here, to show the bug reaches it (the persistence invariants
// would catch it earlier).
func TestLostVoteCausesTwoLeaders(t *testing.T) {
	buggy := DefaultOptions
	buggy.SkipSync = true
	onlySafety := func() []sim.Invariant {
		for _, inv := range Invariants() {
			if inv.Name() == "election-safety" {
				return []sim.Invariant{inv}
			}
		}
		panic("election-safety invariant missing")
	}

	var failing *sim.Result
	for seed := uint64(1); seed <= 50 && failing == nil; seed++ {
		cfg := config(seed, "crashes", buggy)
		cfg.NewInvariants = onlySafety
		cfg.FinalCheck = nil
		if res := run(t, cfg); res.Violation != nil {
			failing = &res
		}
	}
	if failing == nil {
		t.Fatal("two leaders never occurred in 50 seeds with the vote not fsynced")
	}
	if failing.Violation.Invariant != "election-safety" {
		t.Fatalf("unexpected violation %v", failing.Violation)
	}

	cfg := config(failing.Seed, "crashes", buggy)
	cfg.NewInvariants = onlySafety
	cfg.FinalCheck = nil
	if replay := run(t, cfg); *replay.Violation != *failing.Violation {
		t.Fatalf("replay gave %v, first run gave %v", replay.Violation, failing.Violation)
	}
	// Same seed, same faults, bug fixed: no violation.
	if res := run(t, config(failing.Seed, "crashes", DefaultOptions)); res.Violation != nil {
		t.Fatalf("seed %d fails with the bug fixed too: %v", failing.Seed, res.Violation)
	}
}

func TestHardStateRoundTrip(t *testing.T) {
	disk := sim.NewDisk(sim.DiskConfig{}, sim.NewRand(1, "disk"))
	env := sim.Env{ID: 1, Peers: []sim.NodeID{2, 3}, Disk: disk, Rand: sim.NewRand(1, "node")}
	p, err := open(env, DefaultOptions)
	if err != nil {
		t.Fatal(err)
	}
	want := raft.HardState{Term: 7, Vote: 3}
	if err := p.save(want); err != nil {
		t.Fatal(err)
	}
	disk.Crash(true)
	p2, err := open(env, DefaultOptions)
	if err != nil {
		t.Fatal(err)
	}
	if st := p2.Status(); st.Term != 7 || st.Vote != 3 {
		t.Fatalf("recovered %+v, want term 7 vote 3", st)
	}
}

func TestCorruptHardStateRefusesToStart(t *testing.T) {
	disk := sim.NewDisk(sim.DiskConfig{}, sim.NewRand(1, "disk"))
	f, _ := disk.Create(stateFile)
	_, _ = f.Write([]byte{1, 2, 3})
	env := sim.Env{ID: 1, Peers: []sim.NodeID{2}, Disk: disk, Rand: sim.NewRand(1, "node")}
	if _, err := open(env, DefaultOptions); err == nil {
		t.Fatal("started from a 3-byte hardstate file")
	}
}

func TestInvariantsDetectViolations(t *testing.T) {
	proc := func(st raft.Status) *Process {
		// A node built from the given HardState; role is forced afterwards.
		disk := sim.NewDisk(sim.DiskConfig{}, sim.NewRand(1, "disk"))
		env := sim.Env{ID: sim.NodeID(st.ID), Peers: []sim.NodeID{9}, Disk: disk, Rand: sim.NewRand(1, "n")}
		p, err := open(env, DefaultOptions)
		if err != nil {
			t.Fatal(err)
		}
		p.node, _ = raft.NewNode(raft.Config{
			ID: st.ID, Peers: []raft.NodeID{9}, ElectionTicks: 10, HeartbeatTicks: 1,
			Rand: sim.NewRand(1, "n"), HardState: raft.HardState{Term: st.Term, Vote: st.Vote},
		})
		if st.Role == raft.Leader {
			for p.node.Status().Role != raft.Leader {
				p.node.Tick()
				p.node.Step(raft.Message{Type: raft.MsgVoteResp, From: 9, Term: p.node.Status().Term, VoteGranted: true})
			}
		}
		return p
	}
	view := func(id sim.NodeID, p *Process) sim.NodeView { return sim.NodeView{ID: id, Up: true, Process: p} }
	find := func(name string) sim.Invariant {
		for _, inv := range Invariants() {
			if inv.Name() == name {
				return inv
			}
		}
		t.Fatalf("no invariant %q", name)
		return nil
	}

	t.Run("two leaders in one term", func(t *testing.T) {
		inv := find("election-safety")
		a, b := proc(raft.Status{ID: 1, Role: raft.Leader}), proc(raft.Status{ID: 2, Role: raft.Leader})
		if a.Status().Term != b.Status().Term {
			t.Fatal("setup: leaders in different terms")
		}
		if err := inv.Check([]sim.NodeView{view(1, a)}); err != nil {
			t.Fatal(err)
		}
		// Node 1 has since crashed; node 2 leads the same term.
		if err := inv.Check([]sim.NodeView{{ID: 1}, view(2, b)}); err == nil {
			t.Fatal("second leader of the same term not detected")
		}
	})
	t.Run("term goes back", func(t *testing.T) {
		inv := find("term-monotonic")
		_ = inv.Check([]sim.NodeView{view(1, proc(raft.Status{ID: 1, Term: 5}))})
		if err := inv.Check([]sim.NodeView{view(1, proc(raft.Status{ID: 1, Term: 4}))}); err == nil {
			t.Fatal("term 5 -> 4 not detected")
		}
	})
	t.Run("second vote in a term", func(t *testing.T) {
		inv := find("one-vote-per-term")
		_ = inv.Check([]sim.NodeView{view(1, proc(raft.Status{ID: 1, Term: 5, Vote: 2}))})
		if err := inv.Check([]sim.NodeView{view(1, proc(raft.Status{ID: 1, Term: 5, Vote: 3}))}); err == nil {
			t.Fatal("vote for 2 then 3 in term 5 not detected")
		}
	})
}
