package raftsim

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/20001172024/stormproof/raft"
	"github.com/20001172024/stormproof/server/host"
	"github.com/20001172024/stormproof/sim"
)

// testOptions stops proposals at 12 s, leaving the last 3 s of a 15 s run
// to finish replicating.
func testOptions() Options {
	o := DefaultOptions
	o.ProposeUntil = 12 * sim.Second
	return o
}

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
		FinalCheck:    Settled,
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
					cfg := config(seed, p.Name, testOptions())
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
		cfg := config(seed, "calm", testOptions())
		cfg.Duration = 1 * sim.Second
		cfg.Quiet = 0
		cfg.FinalCheck = StableLeader // proposals are still in flight at 1 s
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
				cfg := config(seed, p.Name, testOptions())
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

// The workload actually commits commands, under every profile.
func TestWorkloadCommits(t *testing.T) {
	for _, p := range sim.Profiles {
		cfg := config(1, p.Name, testOptions())
		var commands int
		cfg.FinalCheck = func(nodes []sim.NodeView) error {
			if err := Settled(nodes); err != nil {
				return err
			}
			for _, e := range proc(nodes[0]).Applied() {
				if e.Data != nil {
					commands++
				}
			}
			return nil
		}
		if res := run(t, cfg); res.Violation != nil {
			t.Fatalf("%s: %v", p.Name, res.Violation)
		}
		if commands < 20 {
			t.Errorf("%s: only %d commands committed in a 15 s run", p.Name, commands)
		}
	}
}

// The reason Figure 2 requires votedFor to be persisted before replying:
// with the fsync skipped, a machine crash makes a node forget its vote, vote
// again in the same term, and two leaders get elected. Only election safety
// is checked here, to show the bug reaches it (the persistence invariants
// would catch it earlier).
func TestLostVoteCausesTwoLeaders(t *testing.T) {
	buggy := testOptions()
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
	if res := run(t, config(failing.Seed, "crashes", testOptions())); res.Violation != nil {
		t.Fatalf("seed %d fails with the bug fixed too: %v", failing.Seed, res.Violation)
	}
}

func TestInvariantsDetectViolations(t *testing.T) {
	proc := func(st raft.Status) *fakeServer {
		// A node built from the given HardState; role is forced afterwards.
		n, err := raft.NewNode(raft.Config{
			ID: st.ID, Peers: []raft.NodeID{9}, ElectionTicks: 10, HeartbeatTicks: 1,
			Rand: sim.NewRand(1, "n"), HardState: raft.HardState{Term: st.Term, Vote: st.Vote},
		})
		if err != nil {
			t.Fatal(err)
		}
		if st.Role == raft.Leader {
			for n.Status().Role != raft.Leader {
				n.Tick()
				n.Step(raft.Message{Type: raft.MsgVoteResp, From: 9, Term: n.Status().Term, VoteGranted: true})
			}
		}
		return &fakeServer{node: n}
	}
	view := func(id sim.NodeID, p *fakeServer) sim.NodeView { return sim.NodeView{ID: id, Up: true, Process: p} }
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

// fakeServer is a stand-in for a Process with a node of the test's
// choosing, for exercising invariants directly.
type fakeServer struct {
	node    *raft.Node
	applied []raft.Entry
}

func (f *fakeServer) Status() raft.Status                     { return f.node.Status() }
func (f *fakeServer) Entries() []raft.Entry                   { return f.node.Entries() }
func (f *fakeServer) LogVersion() uint64                      { return 0 }
func (f *fakeServer) Applied() []raft.Entry                   { return f.applied }
func (f *fakeServer) AppliedIndex() uint64                    { return 0 }
func (f *fakeServer) Snapshots() []raft.Snapshot              { return nil }
func (f *fakeServer) States() []host.IndexedState             { return nil }
func (f *fakeServer) Tick() ([]sim.Message, error)            { return nil, nil }
func (f *fakeServer) Step(sim.Message) ([]sim.Message, error) { return nil, nil }

// fakeProc builds a fake server whose node has the given log and commit
// index.
func fakeProc(t *testing.T, id sim.NodeID, term raft.Term, log []raft.Entry, commitTo uint64) *fakeServer {
	t.Helper()
	n, err := raft.NewNode(raft.Config{
		ID: raft.NodeID(id), Peers: []raft.NodeID{99}, ElectionTicks: 10, HeartbeatTicks: 1,
		Rand: sim.NewRand(1, "x"), HardState: raft.HardState{Term: term}, Entries: log,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Advance the commit index through the real AppendEntries path.
	if commitTo > 0 {
		n.Step(raft.Message{Type: raft.MsgApp, From: 99, To: raft.NodeID(id), Term: term,
			PrevLogIndex: commitTo, PrevLogTerm: log[commitTo-1].Term, LeaderCommit: commitTo})
		n.Advance()
	}
	return &fakeServer{node: n}
}

func e(i uint64, t raft.Term, data string) raft.Entry {
	return raft.Entry{Index: i, Term: t, Data: []byte(data)}
}

func TestLogInvariantsDetectViolations(t *testing.T) {
	find := func(name string) sim.Invariant {
		for _, inv := range Invariants() {
			if inv.Name() == name {
				return inv
			}
		}
		t.Fatalf("no invariant %q", name)
		return nil
	}
	up := func(id sim.NodeID, p sim.Process) sim.NodeView { return sim.NodeView{ID: id, Up: true, Process: p} }

	t.Run("log matching", func(t *testing.T) {
		inv := find("log-matching")
		ok := []sim.NodeView{
			up(1, fakeProc(t, 1, 3, []raft.Entry{e(1, 1, "a"), e(2, 2, "b")}, 0)),
			up(2, fakeProc(t, 2, 3, []raft.Entry{e(1, 1, "a"), e(2, 3, "c")}, 0)), // diverge after a match: fine
		}
		if err := inv.Check(ok); err != nil {
			t.Fatalf("legal divergence flagged: %v", err)
		}
		bad := []sim.NodeView{
			up(1, fakeProc(t, 1, 3, []raft.Entry{e(1, 1, "a"), e(2, 2, "b")}, 0)),
			up(2, fakeProc(t, 2, 3, []raft.Entry{e(1, 1, "z"), e(2, 2, "b")}, 0)), // agree at 2, differ at 1
		}
		if err := inv.Check(bad); err == nil {
			t.Fatal("logs agreeing at index 2 but differing at 1 not detected")
		}
	})

	t.Run("committed entry changes", func(t *testing.T) {
		inv := find("leader-completeness")
		if err := inv.Check([]sim.NodeView{up(1, fakeProc(t, 1, 2, []raft.Entry{e(1, 1, "a"), e(2, 2, "b")}, 2))}); err != nil {
			t.Fatal(err)
		}
		if err := inv.Check([]sim.NodeView{up(2, fakeProc(t, 2, 3, []raft.Entry{e(1, 1, "a"), e(2, 3, "c")}, 2))}); err == nil {
			t.Fatal("entry 2 committed as b and then as c: not detected")
		}
	})

	t.Run("state machine safety", func(t *testing.T) {
		inv := find("state-machine-safety")
		a := fakeProc(t, 1, 2, nil, 0)
		a.applied = []raft.Entry{e(1, 1, "a"), e(2, 2, "b")}
		b := fakeProc(t, 2, 2, nil, 0)
		b.applied = []raft.Entry{e(1, 1, "a"), e(2, 2, "DIFFERENT")}
		if err := inv.Check([]sim.NodeView{up(1, a), up(2, b)}); err == nil {
			t.Fatal("two nodes applying different entries at index 2 not detected")
		}
	})
}
