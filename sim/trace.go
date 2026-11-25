package sim

import (
	"bufio"
	"encoding/json"
	"io"
)

// TraceVersion is bumped whenever the trace format changes incompatibly.
const TraceVersion = 1

// Trace record types.
const (
	RecStart     = "start"     // a node started or restarted
	RecCrash     = "crash"     // a node crashed
	RecSend      = "send"      // a node sent a message
	RecDeliver   = "deliver"   // a message reached its destination
	RecDrop      = "drop"      // a message was lost (see Reason)
	RecState     = "state"     // a node's traced state changed
	RecPartition = "partition" // a network partition started
	RecHeal      = "heal"      // the partition ended
	RecQuiet     = "quiet"     // the quiet period started: no more injected faults
	RecViolation = "violation" // an invariant failed; always the last record
)

// Header is the first line of every trace. It records everything needed to
// reproduce the run.
type Header struct {
	Type         string  `json:"type"` // always "header"
	Version      int     `json:"version"`
	Seed         uint64  `json:"seed"`
	Nodes        int     `json:"nodes"`
	Duration     Time    `json:"duration"`
	TickInterval Time    `json:"tick_interval"`
	Profile      Profile `json:"profile"`
}

// Record is one line of a trace after the header. Which fields are set
// depends on Type. Records are Go structs rather than maps so the JSON
// field order is fixed and traces are byte-for-byte reproducible.
type Record struct {
	Step   int             `json:"step"` // index of the event that produced this record
	T      Time            `json:"t"`    // virtual time in microseconds
	Type   string          `json:"type"`
	Node   NodeID          `json:"node,omitempty"`
	From   NodeID          `json:"from,omitempty"`
	To     NodeID          `json:"to,omitempty"`
	Kind   string          `json:"kind,omitempty"` // message kind, partition kind, or crash kind
	Msg    Body            `json:"msg,omitempty"`
	Links  []Link          `json:"links,omitempty"`
	State  json.RawMessage `json:"state,omitempty"`
	Reason string          `json:"reason,omitempty"`
}

// traceWriter writes JSON Lines. A nil *traceWriter discards everything, so
// runs without a trace pay almost nothing for it. The first write error is
// kept and reported at the end, rather than checked at every call site.
type traceWriter struct {
	w   *bufio.Writer
	err error
}

func newTraceWriter(w io.Writer) *traceWriter {
	if w == nil {
		return nil
	}
	return &traceWriter{w: bufio.NewWriter(w)}
}

func (t *traceWriter) write(v any) {
	if t == nil || t.err != nil {
		return
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.err = err
		return
	}
	b = append(b, '\n')
	_, t.err = t.w.Write(b)
}

func (t *traceWriter) close() error {
	if t == nil {
		return nil
	}
	if t.err != nil {
		return t.err
	}
	return t.w.Flush()
}
