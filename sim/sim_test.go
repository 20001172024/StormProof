package sim

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// ping is a minimal Process for testing the event loop: every tick it pings
// its peers, and it answers each ping with a pong.
type ping struct {
	env      Env
	received int
	failTick bool // return an error from the first tick
}

type pingBody struct {
	Pong bool `json:"pong"`
}

func (b pingBody) Kind() string {
	if b.Pong {
		return "pong"
	}
	return "ping"
}

func (p *ping) Tick() ([]Message, error) {
	if p.failTick {
		p.failTick = false
		return nil, errors.New("boom")
	}
	var out []Message
	for _, peer := range p.env.Peers {
		out = append(out, Message{To: peer, Body: pingBody{}})
	}
	return out, nil
}

func (p *ping) Step(m Message) ([]Message, error) {
	p.received++
	if m.Body.(pingBody).Pong {
		return nil, nil
	}
	return []Message{{To: m.From, Body: pingBody{Pong: true}}}, nil
}

func (p *ping) TraceState() any { return struct{ Received int }{p.received} }

func pingConfig(profile string) Config {
	p, err := ProfileByName(profile)
	if err != nil {
		panic(err)
	}
	return Config{
		Seed: 1, Nodes: 3, Duration: 2 * Second, TickInterval: 10 * Millisecond,
		Profile:    p,
		NewProcess: func(env Env) (Process, error) { return &ping{env: env}, nil },
	}
}

func readTrace(t *testing.T, b []byte) (Header, []Record) {
	t.Helper()
	sc := bufio.NewScanner(bytes.NewReader(b))
	sc.Buffer(nil, 1<<20)
	var h Header
	var recs []Record
	for i := 0; sc.Scan(); i++ {
		var err error
		if i == 0 {
			err = json.Unmarshal(sc.Bytes(), &h)
		} else {
			var r struct {
				Record
				Msg json.RawMessage `json:"msg"` // Body is an interface; skip decoding it
			}
			err = json.Unmarshal(sc.Bytes(), &r)
			recs = append(recs, r.Record)
		}
		if err != nil {
			t.Fatalf("line %d: %v", i+1, err)
		}
	}
	return h, recs
}

func countType(recs []Record, typ string) int {
	n := 0
	for _, r := range recs {
		if r.Type == typ {
			n++
		}
	}
	return n
}

func TestRunCalm(t *testing.T) {
	var buf bytes.Buffer
	cfg := pingConfig("calm")
	cfg.Trace = &buf
	res, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	st := res.Stats
	if st.Sent == 0 || st.Delivered == 0 {
		t.Fatalf("no traffic: %+v", st)
	}
	if st.Dropped != 0 || st.Crashes != 0 || st.Partitions != 0 {
		t.Fatalf("calm profile injected faults: %+v", st)
	}
	if res.End > cfg.Duration {
		t.Fatalf("ran past the duration: %v", res.End)
	}

	h, recs := readTrace(t, buf.Bytes())
	if h.Type != "header" || h.Seed != 1 || h.Nodes != 3 || h.Profile.Name != "calm" {
		t.Fatalf("bad header: %+v", h)
	}
	if got := countType(recs, RecStart); got != 3 {
		t.Fatalf("%d start records, want 3", got)
	}
	if got := countType(recs, RecSend); got != st.Sent {
		t.Fatalf("%d send records, Stats.Sent = %d", got, st.Sent)
	}
	for i := 1; i < len(recs); i++ {
		if recs[i].T < recs[i-1].T || recs[i].Step < recs[i-1].Step {
			t.Fatalf("records out of order at %d: %+v then %+v", i, recs[i-1], recs[i])
		}
	}
}

type maxReceived struct{ limit int }

func (maxReceived) Name() string { return "max-received" }

func (m maxReceived) Check(nodes []NodeView) error {
	for _, n := range nodes {
		if n.Up && n.Process.(*ping).received >= m.limit {
			return fmt.Errorf("node %d received %d messages", n.ID, m.limit)
		}
	}
	return nil
}

// A failing invariant stops the run, and the violation is the last record.
func TestRunStopsOnViolation(t *testing.T) {
	var buf bytes.Buffer
	cfg := pingConfig("calm")
	cfg.Trace = &buf
	cfg.NewInvariants = func() []Invariant { return []Invariant{maxReceived{limit: 5}} }
	res, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if res.Violation == nil || res.Violation.Invariant != "max-received" {
		t.Fatalf("Violation = %v, want max-received", res.Violation)
	}
	_, recs := readTrace(t, buf.Bytes())
	last := recs[len(recs)-1]
	if last.Type != RecViolation || last.Step != res.Violation.Step {
		t.Fatalf("last record = %+v, want the violation at step %d", last, res.Violation.Step)
	}
}

// A process that returns an error is crashed and later restarted with the
// same disk.
func TestRunCrashesFailingProcess(t *testing.T) {
	cfg := pingConfig("calm")
	cfg.Profile.Faults.MinDowntime = 10 * Millisecond
	cfg.Profile.Faults.MaxDowntime = 20 * Millisecond
	starts := map[NodeID]int{}
	cfg.NewProcess = func(env Env) (Process, error) {
		starts[env.ID]++
		return &ping{env: env, failTick: env.ID == 2 && starts[env.ID] == 1}, nil
	}
	res, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if res.Stats.Crashes != 1 || res.Stats.Restarts != 1 || starts[2] != 2 {
		t.Fatalf("stats %+v, starts %v: want node 2 crashed and restarted once", res.Stats, starts)
	}
}

func TestRunRecoveryFailureIsViolation(t *testing.T) {
	cfg := pingConfig("calm")
	cfg.NewProcess = func(env Env) (Process, error) { return nil, errors.New("corrupt") }
	res, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if res.Violation == nil || res.Violation.Invariant != "recovery" {
		t.Fatalf("Violation = %v, want recovery", res.Violation)
	}
}

func TestRunInjectsFaults(t *testing.T) {
	var buf bytes.Buffer
	cfg := pingConfig("chaos")
	cfg.Duration = 20 * Second
	cfg.Trace = &buf
	res, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	st := res.Stats
	if st.Crashes == 0 || st.Restarts == 0 || st.Partitions == 0 || st.Dropped == 0 {
		t.Fatalf("chaos profile over 20s injected too few faults: %+v", st)
	}
	_, recs := readTrace(t, buf.Bytes())
	reasons := map[string]bool{}
	for _, r := range recs {
		if r.Type == RecDrop {
			reasons[r.Reason] = true
		}
	}
	for _, want := range []string{"lost", "partition", "node down"} {
		if !reasons[want] {
			t.Errorf("no drops with reason %q", want)
		}
	}
	if !strings.Contains(buf.String(), `"type":"heal"`) {
		t.Error("no partition ever healed")
	}
}

func TestRunValidatesConfig(t *testing.T) {
	cases := map[string]func(*Config){
		"one node":       func(c *Config) { c.Nodes = 1 },
		"no tick":        func(c *Config) { c.TickInterval = 0 },
		"no duration":    func(c *Config) { c.Duration = 0 },
		"no process":     func(c *Config) { c.NewProcess = nil },
		"skew too high":  func(c *Config) { c.Profile.ClockSkew = 1 },
		"quiet too long": func(c *Config) { c.Quiet = c.Duration + 1 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := pingConfig("calm")
			mutate(&cfg)
			if _, err := Run(cfg); err == nil {
				t.Fatal("Run accepted an invalid config")
			}
		})
	}
}

// With a quiet period, no fault starts in the final stretch of the run.
func TestRunQuietPeriod(t *testing.T) {
	var buf bytes.Buffer
	cfg := pingConfig("chaos")
	cfg.Duration = 20 * Second
	cfg.Quiet = 10 * Second
	cfg.Trace = &buf
	if _, err := Run(cfg); err != nil {
		t.Fatal(err)
	}
	_, recs := readTrace(t, buf.Bytes())
	sawQuiet := false
	for _, r := range recs {
		sawQuiet = sawQuiet || r.Type == RecQuiet
		if (r.Type == RecCrash || r.Type == RecPartition) && r.T >= cfg.Duration-cfg.Quiet {
			t.Fatalf("fault during the quiet period: %+v", r)
		}
	}
	if !sawQuiet {
		t.Fatal("no quiet record in the trace")
	}
}

func TestRunFinalCheck(t *testing.T) {
	cfg := pingConfig("calm")
	calls := 0
	cfg.FinalCheck = func(nodes []NodeView) error {
		calls++
		if len(nodes) != 3 {
			t.Errorf("FinalCheck got %d nodes", len(nodes))
		}
		return errors.New("no leader")
	}
	res, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || res.Violation == nil || res.Violation.Invariant != "final-check" {
		t.Fatalf("calls=%d violation=%v, want one call reported as final-check", calls, res.Violation)
	}
}

type panicky struct{ ping }

func (p *panicky) Tick() ([]Message, error) { panic("boom") }

func TestRunReportsPanicAsViolation(t *testing.T) {
	cfg := pingConfig("calm")
	cfg.NewProcess = func(env Env) (Process, error) { return &panicky{ping{env: env}}, nil }
	res, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if res.Violation == nil || res.Violation.Invariant != "panic" || !strings.Contains(res.Violation.Detail, "boom") {
		t.Fatalf("Violation = %v, want a panic violation mentioning boom", res.Violation)
	}
}

// Clients get IDs after the servers, talk to the servers over the network,
// are never crashed, and are hidden from invariants but shown to the final
// check.
func TestRunClients(t *testing.T) {
	var buf bytes.Buffer
	cfg := pingConfig("chaos")
	cfg.Duration = 10 * Second
	cfg.Trace = &buf
	cfg.Clients = 2
	clientPeers := map[NodeID][]NodeID{}
	cfg.NewClient = func(env Env) (Process, error) {
		clientPeers[env.ID] = env.Peers
		return &ping{env: env}, nil
	}
	sawClientInInvariant := false
	cfg.NewInvariants = func() []Invariant { return []Invariant{viewSpy{saw: &sawClientInInvariant}} }
	var final []NodeView
	cfg.FinalCheck = func(nodes []NodeView) error { final = nodes; return nil }
	res, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if res.Violation != nil {
		t.Fatal(res.Violation)
	}
	if len(clientPeers) != 2 || len(clientPeers[4]) != 3 || len(clientPeers[5]) != 3 {
		t.Fatalf("client peers = %v, want clients 4 and 5 each seeing servers 1-3", clientPeers)
	}
	if sawClientInInvariant {
		t.Fatal("an invariant was shown a client")
	}
	if len(final) != 5 || !final[3].Client || !final[4].Client || final[0].Client {
		t.Fatalf("final check views = %+v, want 3 servers then 2 clients", final)
	}
	_, recs := readTrace(t, buf.Bytes())
	for _, r := range recs {
		if r.Type == RecCrash && r.Node > 3 {
			t.Fatalf("a client crashed: %+v", r)
		}
		if r.Type == RecDeliver && r.To > 3 && r.Kind == "pong" {
			return // a server answered a client: done
		}
	}
	t.Fatal("no server ever answered a client")
}

type viewSpy struct{ saw *bool }

func (viewSpy) Name() string { return "view-spy" }

func (v viewSpy) Check(nodes []NodeView) error {
	for _, n := range nodes {
		if n.Client {
			*v.saw = true
		}
	}
	return nil
}

// An injected fsync failure while a node starts is a crash, not a recovery
// violation: the node is restarted later (bug 004).
func TestRunSyncFailureDuringStartIsACrash(t *testing.T) {
	cfg := pingConfig("calm")
	cfg.Profile.Faults.MinDowntime = 10 * Millisecond
	cfg.Profile.Faults.MaxDowntime = 20 * Millisecond
	starts := map[NodeID]int{}
	cfg.NewProcess = func(env Env) (Process, error) {
		starts[env.ID]++
		if env.ID == 2 && starts[env.ID] == 1 {
			return nil, fmt.Errorf("repairing the log: %w", ErrSyncFailed)
		}
		return &ping{env: env}, nil
	}
	res, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if res.Violation != nil {
		t.Fatalf("got violation %v, want a crash and restart", res.Violation)
	}
	if starts[2] != 2 || res.Stats.Crashes != 1 {
		t.Fatalf("node 2 started %d times, %d crashes; want 2 and 1", starts[2], res.Stats.Crashes)
	}
}
