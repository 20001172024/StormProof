package sim

import (
	"sort"
	"testing"
)

func TestNetworkFate(t *testing.T) {
	cases := []struct {
		name       string
		cfg        NetworkConfig
		wantCopies int
	}{
		{"always delivered once", NetworkConfig{MinDelay: 1, MaxDelay: 10}, 1},
		{"always dropped", NetworkConfig{DropProb: 1}, 0},
		{"always duplicated", NetworkConfig{MinDelay: 1, MaxDelay: 10, DuplicateProb: 1}, 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n := &network{cfg: c.cfg, rng: NewRand(1, "net-test")}
			for i := 0; i < 100; i++ {
				delays := n.fate()
				if len(delays) != c.wantCopies {
					t.Fatalf("got %d copies, want %d", len(delays), c.wantCopies)
				}
				for _, d := range delays {
					if d < c.cfg.MinDelay || d >= c.cfg.MaxDelay {
						t.Fatalf("delay %v outside [%v, %v)", d, c.cfg.MinDelay, c.cfg.MaxDelay)
					}
				}
			}
		})
	}
}

// Check the shape of every kind of partition over many random draws, for
// clusters of 3 and 5 nodes.
func TestRandomPartition(t *testing.T) {
	for _, n := range []int{3, 5} {
		ids := make([]NodeID, n)
		for i := range ids {
			ids[i] = NodeID(i + 1)
		}
		seen := map[string]bool{}
		for seed := uint64(0); seed < 300; seed++ {
			kind, links := randomPartition(NewRand(seed, "partition-test"), ids)
			seen[kind] = true

			if !sort.SliceIsSorted(links, func(i, j int) bool {
				return links[i].From < links[j].From ||
					(links[i].From == links[j].From && links[i].To < links[j].To)
			}) {
				t.Fatalf("links not sorted: %v", links)
			}
			blocked := map[Link]bool{}
			for _, l := range links {
				if l.From == l.To {
					t.Fatalf("self link %v", l)
				}
				if blocked[l] {
					t.Fatalf("duplicate link %v", l)
				}
				blocked[l] = true
			}

			switch kind {
			case PartitionIsolate, PartitionSplit:
				for l := range blocked {
					if !blocked[Link{l.To, l.From}] {
						t.Fatalf("%s partition is not symmetric: %v blocked but not its reverse", kind, l)
					}
				}
				// A minority of size m blocks 2*m*(n-m) links, with m <= n/2.
				m := 0
				for size := 1; size <= n/2; size++ {
					if len(links) == 2*size*(n-size) {
						m = size
					}
				}
				if m == 0 {
					t.Fatalf("%s partition of %d nodes blocks %d links: not a clean group split", kind, n, len(links))
				}
			case PartitionOneWay:
				if len(links) != n-1 {
					t.Fatalf("one-way partition blocks %d links, want %d", len(links), n-1)
				}
				for _, l := range links {
					if l.From != links[0].From || blocked[Link{l.To, l.From}] {
						t.Fatalf("one-way partition should block only one node's outgoing links: %v", links)
					}
				}
			default:
				t.Fatalf("unknown partition kind %q", kind)
			}
		}
		for _, k := range []string{PartitionIsolate, PartitionSplit, PartitionOneWay} {
			if !seen[k] {
				t.Errorf("n=%d: never generated a %q partition", n, k)
			}
		}
	}
}
