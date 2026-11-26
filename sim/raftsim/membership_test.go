package raftsim

import (
	"slices"
	"testing"

	"github.com/20001172024/stormproof/raft"
	"github.com/20001172024/stormproof/sim"
)

// memberConfig is kvConfig with five server slots, three of them in the
// initial configuration, and an admin adding and removing servers about
// once a second alongside the KV clients.
func memberConfig(seed uint64, profile string, o Options) sim.Config {
	cfg := kvConfig(seed, profile, o)
	cfg.Nodes = 5
	o.InitialVoters = 3
	o.ProposeProb = 0
	cfg.NewProcess = New(o)
	ad := DefaultAdminOptions
	ad.Until, ad.RetryUntil = 12*sim.Second, 13500*sim.Millisecond
	co := DefaultClientOptions
	co.Until, co.RetryUntil = 12*sim.Second, 13500*sim.Millisecond
	cfg.Clients = DefaultClients + 1
	cfg.NewClient = NewClients(co, &ad, 3)
	return cfg
}

func TestMembershipUnderAllProfiles(t *testing.T) {
	for _, p := range sim.Profiles {
		t.Run(p.Name, func(t *testing.T) {
			changes := 0
			for seed := uint64(1); seed <= 10; seed++ {
				cfg := memberConfig(seed, p.Name, testOptions())
				final := cfg.FinalCheck
				cfg.FinalCheck = func(nodes []sim.NodeView) error {
					for _, nv := range nodes {
						if a, ok := nv.Process.(*Admin); ok {
							changes += a.Changes
						}
					}
					return final(nodes)
				}
				if res := run(t, cfg); res.Violation != nil {
					t.Fatalf("seed %d: %v", seed, res.Violation)
				}
			}
			if changes < 20 {
				t.Fatalf("only %d membership changes in 10 runs", changes)
			}
		})
	}
}

// Servers outside the configuration are left out of the end-of-run check,
// and the configuration at the end really can differ from the start.
func TestMembershipEndsInADifferentConfiguration(t *testing.T) {
	changed := false
	for seed := uint64(1); seed <= 10 && !changed; seed++ {
		cfg := memberConfig(seed, "calm", testOptions())
		final := cfg.FinalCheck
		cfg.FinalCheck = func(nodes []sim.NodeView) error {
			for _, nv := range servers(nodes) {
				if st := status(nv); st.Role == raft.Leader && !slices.Equal(st.Voters, []raft.NodeID{1, 2, 3}) {
					changed = true
				}
			}
			return final(nodes)
		}
		if res := run(t, cfg); res.Violation != nil {
			t.Fatalf("seed %d: %v", seed, res.Violation)
		}
	}
	if !changed {
		t.Fatal("every run ended with the initial configuration")
	}
}

func TestSingleServerChangesInvariant(t *testing.T) {
	conf := func(i uint64, vs ...raft.NodeID) raft.Entry {
		return raft.Entry{Index: i, Term: 1, Type: raft.EntryConfChange, Data: raft.EncodeVoters(vs)}
	}
	for _, c := range []struct {
		name string
		log  []raft.Entry
		ok   bool
	}{
		{"one at a time", []raft.Entry{conf(1, 1, 2, 3), conf(2, 1, 2, 3, 4), conf(3, 1, 3, 4)}, true},
		{"two added at once", []raft.Entry{conf(1, 1, 2, 3), conf(2, 1, 2, 3, 4, 5)}, false},
		{"one swapped for another", []raft.Entry{conf(1, 1, 2, 3), conf(2, 1, 2, 4)}, false},
	} {
		inv := &singleServerChanges{seen: map[sim.NodeID]version{}}
		p := fakeProc(t, 1, 1, c.log, 0)
		err := inv.Check([]sim.NodeView{{ID: 1, Up: true, Process: p}})
		if (err == nil) != c.ok {
			t.Errorf("%s: err = %v", c.name, err)
		}
	}
}
