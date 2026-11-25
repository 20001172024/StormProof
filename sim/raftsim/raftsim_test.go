package raftsim

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/20001172024/stormproof/raft"
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

// State saved through the WAL, including a truncation, comes back after a
// machine crash.
func TestRecoveryThroughWAL(t *testing.T) {
	disk := sim.NewDisk(sim.DiskConfig{}, sim.NewRand(1, "disk"))
	env := sim.Env{ID: 1, Peers: []sim.NodeID{2, 3}, Disk: disk, Rand: sim.NewRand(1, "node"), Now: func() sim.Time { return 0 }}
	p, err := open(env, DefaultOptions)
	if err != nil {
		t.Fatal(err)
	}
	hs := raft.HardState{Term: 7, Vote: 3}
	es := []raft.Entry{{Index: 1, Term: 2}, {Index: 2, Term: 7, Data: []byte("x")}, {Index: 3, Term: 7, Data: []byte("y")}}
	if err := p.wal.Save(&hs, es); err != nil {
		t.Fatal(err)
	}
	// Replace the tail from index 2, as after a conflict with a new leader.
	if err := p.wal.Save(nil, []raft.Entry{{Index: 2, Term: 8, Data: []byte("z")}}); err != nil {
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
	want := []raft.Entry{{Index: 1, Term: 2}, {Index: 2, Term: 8, Data: []byte("z")}}
	got := p2.Entries()
	if len(got) != len(want) || !sameEntry(got[0], want[0]) || !sameEntry(got[1], want[1]) {
		t.Fatalf("recovered log %+v, want %+v", got, want)
	}
}

// A node whose WAL is corrupt refuses to start, which the simulator reports
// as a recovery violation.
func TestCorruptWALRefusesToStart(t *testing.T) {
	disk := sim.NewDisk(sim.DiskConfig{}, sim.NewRand(1, "disk"))
	// A complete record (length 17, so the bytes are all there) whose
	// checksum is wrong.
	f, _ := disk.Create("wal-0000000000000001.log")
	_, _ = f.Write(append([]byte{17, 0, 0, 0, 0xde, 0xad, 0xbe, 0xef, 1}, make([]byte, 16)...))
	_ = f.Sync()
	_ = disk.SyncDir()
	env := sim.Env{ID: 1, Peers: []sim.NodeID{2}, Disk: disk, Rand: sim.NewRand(1, "node")}
	if _, err := open(env, DefaultOptions); err == nil {
		t.Fatal("started from a corrupt WAL")
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

// fakeProc builds a Process whose node has the given log and commit index,
// for exercising invariants directly.
func fakeProc(t *testing.T, id sim.NodeID, term raft.Term, log []raft.Entry, commitTo uint64) *Process {
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
	return &Process{node: n}
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
	up := func(id sim.NodeID, p *Process) sim.NodeView { return sim.NodeView{ID: id, Up: true, Process: p} }

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
