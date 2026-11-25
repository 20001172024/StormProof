package sim

import "container/heap"

type eventKind int

const (
	evTick       eventKind = iota + 1 // a node's timer fires
	evDeliver                         // a message arrives at its destination
	evFaultCheck                      // the fault scheduler rolls the dice
	evRestart                         // a crashed node comes back up
	evHeal                            // the current partition ends
)

// event is one thing scheduled to happen at a point in virtual time.
type event struct {
	at   Time
	seq  uint64 // set by queue.push; breaks ties between events at the same time
	kind eventKind

	node        NodeID  // evTick, evRestart
	incarnation int     // evTick: ignore the tick if the node restarted since
	msg         Message // evDeliver
}

// eventHeap implements heap.Interface (Go's standard binary heap, in
// container/heap). In Go you get a priority queue by giving a slice these
// five methods; heap.Push and heap.Pop then keep it ordered.
type eventHeap []*event

func (h eventHeap) Len() int { return len(h) }

// Less orders events by time, then by the order they were scheduled.
// The seq tie-break matters: without it, two events at the same time would
// come out in whatever order the heap's internal layout happens to give,
// which can change when unrelated code schedules one more event.
func (h eventHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].seq < h[j].seq
}

func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *eventHeap) Push(x any) { *h = append(*h, x.(*event)) }

func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil // let the garbage collector reclaim the event
	*h = old[:n-1]
	return e
}

// queue is the simulator's event queue: earliest event first, ties broken
// by insertion order.
type queue struct {
	h       eventHeap
	nextSeq uint64
}

func (q *queue) push(e *event) {
	e.seq = q.nextSeq
	q.nextSeq++
	heap.Push(&q.h, e)
}

func (q *queue) pop() *event { return heap.Pop(&q.h).(*event) }

func (q *queue) len() int { return q.h.Len() }
