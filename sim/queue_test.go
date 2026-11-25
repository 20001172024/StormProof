package sim

import (
	"sort"
	"testing"
)

func TestQueueOrdersByTime(t *testing.T) {
	var q queue
	for _, at := range []Time{30, 10, 20} {
		q.push(&event{at: at})
	}
	var got []Time
	for q.len() > 0 {
		got = append(got, q.pop().at)
	}
	want := []Time{10, 20, 30}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

// Events at the same time must come out in the order they were pushed.
// Compare against a stable sort over many random inputs, with lots of
// duplicate times so ties are common.
func TestQueueTiesKeepInsertionOrder(t *testing.T) {
	r := NewRand(1, "queue-test")
	for trial := 0; trial < 200; trial++ {
		var q queue
		var pushed []*event
		for i := 0; i < 50; i++ {
			e := &event{at: Time(r.Intn(5)), node: NodeID(i)}
			q.push(e)
			pushed = append(pushed, e)
		}
		want := append([]*event(nil), pushed...)
		sort.SliceStable(want, func(i, j int) bool { return want[i].at < want[j].at })

		for i, w := range want {
			got := q.pop()
			if got != w {
				t.Fatalf("trial %d pop %d: got event (at=%v node=%d), want (at=%v node=%d)",
					trial, i, got.at, got.node, w.at, w.node)
			}
		}
	}
}
