package sim

import "sort"

// NetworkConfig sets how the simulated network treats messages.
type NetworkConfig struct {
	MinDelay, MaxDelay Time    // each delivery takes a random delay in [MinDelay, MaxDelay)
	DropProb           float64 // chance a message is lost
	DuplicateProb      float64 // chance a message is delivered twice
}

// network decides the fate of each message. It never reorders on purpose:
// reordering happens naturally, because each delivery gets its own random
// delay and a later message can overtake an earlier one.
type network struct {
	cfg NetworkConfig
	rng *Rand
}

// fate returns the delays after which copies of a message arrive: none if
// the message is lost, two if it is duplicated. Each copy draws its own
// delay, so duplicates can arrive far apart.
func (n *network) fate() []Time {
	if n.rng.Chance(n.cfg.DropProb) {
		return nil
	}
	copies := 1
	if n.rng.Chance(n.cfg.DuplicateProb) {
		copies = 2
	}
	delays := make([]Time, copies)
	for i := range delays {
		delays[i] = n.rng.Between(n.cfg.MinDelay, n.cfg.MaxDelay)
	}
	return delays
}

// Link is a one-way connection between two nodes.
type Link struct {
	From NodeID `json:"from"`
	To   NodeID `json:"to"`
}

// Kinds of partition the fault scheduler creates.
const (
	PartitionIsolate = "isolate" // one node cut off in both directions
	PartitionSplit   = "split"   // a minority group cut off from the majority
	PartitionOneWay  = "one-way" // one node can receive but its messages are lost
)

// randomPartition picks a partition of the given nodes and returns its kind
// and the sorted list of one-way links it blocks. Blocking individual
// one-way links covers symmetric, asymmetric and group splits with a single
// mechanism. ids must be sorted and hold at least two nodes.
func randomPartition(rng *Rand, ids []NodeID) (string, []Link) {
	var kind string
	var links []Link
	cut := func(a, b []NodeID, bothWays bool) {
		for _, x := range a {
			for _, y := range b {
				links = append(links, Link{x, y})
				if bothWays {
					links = append(links, Link{y, x})
				}
			}
		}
	}
	others := func(v NodeID) []NodeID {
		var out []NodeID
		for _, id := range ids {
			if id != v {
				out = append(out, id)
			}
		}
		return out
	}

	switch rng.Intn(3) {
	case 0:
		kind = PartitionIsolate
		v := ids[rng.Intn(len(ids))]
		cut([]NodeID{v}, others(v), true)
	case 1:
		kind = PartitionSplit
		shuffled := append([]NodeID(nil), ids...)
		rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		minority := 1 + rng.Intn(len(ids)/2) // between 1 and half the nodes
		cut(shuffled[:minority], shuffled[minority:], true)
	default:
		kind = PartitionOneWay
		v := ids[rng.Intn(len(ids))]
		cut([]NodeID{v}, others(v), false)
	}

	sort.Slice(links, func(i, j int) bool {
		if links[i].From != links[j].From {
			return links[i].From < links[j].From
		}
		return links[i].To < links[j].To
	})
	return kind, links
}
