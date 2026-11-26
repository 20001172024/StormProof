package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/20001172024/stormproof/check"
	"github.com/20001172024/stormproof/client"
	"github.com/20001172024/stormproof/kv"
	"github.com/20001172024/stormproof/raft"
)

// testCluster runs real servers in this process: real gRPC on localhost,
// real files in temp directories, real time. Ticks are 10 ms to keep the
// tests quick.
type testCluster struct {
	t       *testing.T
	addrs   map[raft.NodeID]string
	dirs    map[raft.NodeID]string
	servers map[raft.NodeID]*Server
	metrics map[raft.NodeID]string // metrics HTTP address of each running server
}

func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	c := &testCluster{t: t, addrs: map[raft.NodeID]string{}, dirs: map[raft.NodeID]string{}, servers: map[raft.NodeID]*Server{}, metrics: map[raft.NodeID]string{}}
	listeners := map[raft.NodeID]net.Listener{}
	for i := 1; i <= n; i++ {
		id := raft.NodeID(i)
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[id], c.addrs[id], c.dirs[id] = lis, lis.Addr().String(), t.TempDir()
	}
	for id, lis := range listeners {
		c.start(id, lis)
	}
	t.Cleanup(func() {
		for _, s := range c.servers {
			s.Stop()
		}
	})
	return c
}

func (c *testCluster) start(id raft.NodeID, lis net.Listener) {
	c.t.Helper()
	mlis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		c.t.Fatal(err)
	}
	c.metrics[id] = mlis.Addr().String()
	s, err := New(Config{
		ID: id, Cluster: c.addrs, DataDir: c.dirs[id], Tick: 10 * time.Millisecond,
		SnapshotEvery: 100, Listener: lis, MetricsListener: mlis,
		Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		c.t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		c.t.Fatal(err)
	}
	c.servers[id] = s
}

func (c *testCluster) stop(id raft.NodeID) {
	c.servers[id].Stop()
	delete(c.servers, id)
}

// restart starts a stopped server again on its old address and data.
func (c *testCluster) restart(id raft.NodeID) {
	c.t.Helper()
	var lis net.Listener
	var err error
	for i := 0; i < 50; i++ {
		if lis, err = net.Listen("tcp", c.addrs[id]); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		c.t.Fatal(err)
	}
	c.start(id, lis)
}

// leader waits until one running server leads and every running server
// agrees, and returns it.
func (c *testCluster) leader() raft.NodeID {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var leader raft.NodeID
		agreed := true
		for _, s := range c.servers {
			st := s.Status()
			if st.Role == raft.Leader {
				leader = st.ID
			}
			if st.Leader == raft.None || (leader != raft.None && st.Leader != leader) {
				agreed = false
			}
		}
		if leader != raft.None && agreed {
			return leader
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("no stable leader within 10 s")
	return raft.None
}

func (c *testCluster) client() *client.Client {
	c.t.Helper()
	var eps []string
	for i := 1; i <= len(c.addrs); i++ {
		eps = append(eps, c.addrs[raft.NodeID(i)])
	}
	cl, err := client.New(eps, client.Options{AttemptTimeout: 500 * time.Millisecond})
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() { _ = cl.Close() })
	return cl
}

func ctx(t *testing.T) context.Context {
	cx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	return cx
}

func TestClusterOperations(t *testing.T) {
	c := newTestCluster(t, 3)
	cl := c.client()
	cx := ctx(t)
	if err := cl.Put(cx, "k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := cl.Get(cx, "k"); err != nil || !ok || string(v) != "v1" {
		t.Fatalf("get: %q %v %v", v, ok, err)
	}
	r, err := cl.CompareAndSwap(cx, "k", []byte("v1"), false, []byte("v2"))
	if err != nil || !r.Swapped {
		t.Fatalf("cas: %+v %v", r, err)
	}
	if r, err := cl.CompareAndSwap(cx, "k", []byte("v1"), false, []byte("v3")); err != nil || r.Swapped || string(r.Current) != "v2" {
		t.Fatalf("failing cas: %+v %v", r, err)
	}
	if existed, err := cl.Delete(cx, "k"); err != nil || !existed {
		t.Fatalf("delete: %v %v", existed, err)
	}
	if _, ok, err := cl.Get(cx, "k"); err != nil || ok {
		t.Fatalf("get after delete: %v %v", ok, err)
	}
}

// Stop the leader: a new one is elected and writes go on. Bring the old
// leader back: it catches up from the others.
func TestLeaderFailover(t *testing.T) {
	c := newTestCluster(t, 3)
	cl := c.client()
	cx := ctx(t)
	if err := cl.Put(cx, "a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	old := c.leader()
	c.stop(old)
	start := time.Now()
	if err := cl.Put(cx, "a", []byte("2")); err != nil {
		t.Fatalf("write after the leader stopped: %v", err)
	}
	t.Logf("first write after stopping leader %d succeeded %v later", old, time.Since(start).Round(time.Millisecond))
	if c.leader() == old {
		t.Fatal("the stopped server is still the leader")
	}

	c.restart(old)
	deadline := time.Now().Add(10 * time.Second)
	for c.servers[old].Status().Commit < c.servers[c.leader()].Status().Commit && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if v, _, err := cl.Get(cx, "a"); err != nil || string(v) != "2" {
		t.Fatalf("get: %q %v", v, err)
	}
}

// Stop every server and start them again: everything written survives,
// recovered from the WAL and snapshot files on disk.
func TestRestartRecoversFromDisk(t *testing.T) {
	c := newTestCluster(t, 3)
	cl := c.client()
	cx := ctx(t)
	for i := 0; i < 250; i++ { // more than SnapshotEvery, so recovery includes a snapshot
		if err := cl.Put(cx, fmt.Sprint("k", i), []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	for id := range c.addrs {
		c.stop(id)
	}
	for id := range c.addrs {
		c.restart(id)
	}
	for i := 0; i < 250; i += 25 {
		if v, ok, err := cl.Get(cx, fmt.Sprint("k", i)); err != nil || !ok || string(v) != fmt.Sprint(i) {
			t.Fatalf("k%d after restart: %q %v %v", i, v, ok, err)
		}
	}
	for _, s := range c.servers {
		if s.Status().SnapIndex == 0 {
			t.Errorf("node %d never took a snapshot", s.Status().ID)
		}
	}
}

// The same check the simulator does, on a real cluster: concurrent clients
// on a few keys while the leader is stopped and restarted, then Porcupine
// on the history, with real timestamps.
func TestLinearizableOnRealCluster(t *testing.T) {
	c := newTestCluster(t, 3)
	var mu sync.Mutex
	var ops []check.Op
	record := func(op check.Op) {
		mu.Lock()
		ops = append(ops, op)
		mu.Unlock()
	}
	stopAt := time.Now().Add(3 * time.Second)
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		cl := c.client()
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), 1))
			for n := 0; time.Now().Before(stopAt); n++ {
				key := fmt.Sprint("k", r.IntN(2))
				val := []byte(fmt.Sprintf("w%d.%d", w, n))
				cx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				op := check.Op{ClientID: w, Call: time.Now().UnixNano()}
				var err error
				switch p := r.IntN(10); {
				case p < 4:
					op.Cmd = kv.Command{Op: kv.OpGet, Key: key}
					var v []byte
					var found bool
					v, found, err = cl.Get(cx, key)
					op.Result = kv.Result{Found: found, Value: v}
				case p < 7:
					op.Cmd = kv.Command{Op: kv.OpPut, Key: key, Value: val}
					err = cl.Put(cx, key, val)
				case p < 9:
					op.Cmd = kv.Command{Op: kv.OpCAS, Key: key, ExpectAbsent: true, Value: val}
					var res client.CASResult
					res, err = cl.CompareAndSwap(cx, key, nil, true, val)
					op.Result = kv.Result{Swapped: res.Swapped, Found: res.Found, Value: res.Current}
				default:
					op.Cmd = kv.Command{Op: kv.OpDelete, Key: key}
					var existed bool
					existed, err = cl.Delete(cx, key)
					op.Result = kv.Result{Existed: existed}
				}
				cancel()
				op.Return, op.Done = time.Now().UnixNano(), err == nil
				record(op)
			}
		}(w)
	}
	time.Sleep(1 * time.Second)
	leader := c.leader()
	c.stop(leader)
	time.Sleep(800 * time.Millisecond)
	c.restart(leader)
	wg.Wait()

	done := 0
	for _, op := range ops {
		if op.Done {
			done++
		}
	}
	t.Logf("%d operations, %d answered", len(ops), done)
	if done < 50 {
		t.Fatalf("only %d operations answered", done)
	}
	if !check.Linearizable(ops) {
		t.Fatal("the real cluster's history is not linearizable")
	}
}

// scrape fetches a server's /metrics and returns each sample's value, keyed
// by the metric name with its labels (as Prometheus prints it).
func scrape(t *testing.T, addr string) map[string]float64 {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	out := map[string]float64{}
	for _, line := range strings.Split(string(body), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, val, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		if v, err := strconv.ParseFloat(val, 64); err == nil {
			out[name] = v
		}
	}
	return out
}

func probe(t *testing.T, addr, path string) int {
	t.Helper()
	resp, err := http.Get("http://" + addr + path)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

func TestMetricsAndProbes(t *testing.T) {
	c := newTestCluster(t, 3)
	cl := c.client()
	cx := ctx(t)
	for i := 0; i < 20; i++ {
		if err := cl.Put(cx, "k", []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := cl.Get(cx, "k"); err != nil {
		t.Fatal(err)
	}
	leader := c.leader()
	time.Sleep(50 * time.Millisecond) // a few ticks for followers to apply the last entries
	elections := 0.0
	for id, addr := range c.metrics {
		m := scrape(t, addr)
		node := fmt.Sprintf(`{node="%d"}`, id)
		want := map[string]float64{"stormproof_raft_voters": 3, "stormproof_raft_apply_lag": 0}
		if id == leader {
			want["stormproof_raft_is_leader"], want["stormproof_raft_role"] = 1, 2
		} else {
			want["stormproof_raft_is_leader"], want["stormproof_raft_role"] = 0, 0
		}
		for name, v := range want {
			if got, ok := m[name+node]; !ok || got != v {
				t.Errorf("node %d: %s = %v (present %v), want %v", id, name, got, ok, v)
			}
		}
		if m["stormproof_raft_term"+node] < 1 || m["stormproof_raft_commit_index"+node] < 21 {
			t.Errorf("node %d: term %v, commit %v", id, m["stormproof_raft_term"+node], m["stormproof_raft_commit_index"+node])
		}
		if m["stormproof_raft_leader_changes_total"+node] < 1 {
			t.Errorf("node %d never saw a leader", id)
		}
		elections += m["stormproof_raft_elections_started_total"+node]
		if probe(t, addr, "/healthz") != http.StatusOK || probe(t, addr, "/readyz") != http.StatusOK {
			t.Errorf("node %d: probes not OK", id)
		}
	}
	if elections < 1 {
		t.Error("no server counted an election")
	}
	lm := scrape(t, c.metrics[leader])
	if n := lm[fmt.Sprintf(`stormproof_client_request_duration_seconds_count{code="OK",node="%d",op="put"}`, leader)]; n < 20 {
		t.Errorf("leader counted %v successful puts, want at least 20", n)
	}
	sends := 0.0
	for name, v := range lm {
		if strings.HasPrefix(name, "stormproof_raft_send_duration_seconds_count") && strings.Contains(name, `result="ok"`) {
			sends += v
		}
	}
	if sends == 0 {
		t.Error("leader recorded no successful Raft sends")
	}
}
