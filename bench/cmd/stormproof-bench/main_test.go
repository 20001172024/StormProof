package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/20001172024/stormproof/raft"
	"github.com/20001172024/stormproof/server"
)

// startCluster runs a 3-server cluster in this process and returns its
// client endpoints.
func startCluster(t *testing.T) string {
	t.Helper()
	addrs := map[raft.NodeID]string{}
	lis := map[raft.NodeID]net.Listener{}
	for id := raft.NodeID(1); id <= 3; id++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lis[id], addrs[id] = l, l.Addr().String()
	}
	var eps []string
	for id, l := range lis {
		s, err := server.New(server.Config{
			ID: id, Cluster: addrs, DataDir: t.TempDir(), Tick: 10 * time.Millisecond,
			Listener: l, Logger: slog.New(slog.DiscardHandler),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Stop)
		eps = append(eps, addrs[id])
	}
	return strings.Join(eps, ",")
}

func TestBenchAgainstRealCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a real cluster")
	}
	eps := startCluster(t)
	dir := t.TempDir()
	out, tl := filepath.Join(dir, "r.json"), filepath.Join(dir, "t.csv")
	var stdout bytes.Buffer
	err := run([]string{"-endpoints", eps, "-clients", "4", "-warmup", "500ms", "-duration", "1s",
		"-reads", "0.5", "-label", "test", "-out", out, "-timeline", tl}, &stdout)
	if err != nil {
		t.Fatalf("%v\n%s", err, &stdout)
	}
	var res struct {
		Label     string  `json:"label"`
		Ops       int     `json:"ops"`
		OpsPerSec float64 `json:"ops_per_sec"`
		ByOp      map[string]struct {
			Count  int `json:"count"`
			Errors int `json:"errors"`
		} `json:"by_op"`
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &res); err != nil {
		t.Fatal(err)
	}
	if res.Label != "test" || res.Ops == 0 || res.ByOp["get"].Count == 0 || res.ByOp["put"].Count == 0 {
		t.Fatalf("result %s\nstdout:\n%s", b, &stdout)
	}
	if e := res.ByOp["get"].Errors + res.ByOp["put"].Errors; e > 0 {
		t.Fatalf("%d errors against a healthy cluster:\n%s", e, &stdout)
	}
	if fi, err := os.Stat(tl); err != nil || fi.Size() == 0 {
		t.Fatal("no timeline written")
	}
	t.Logf("%s", &stdout)
}

func TestFlags(t *testing.T) {
	var out bytes.Buffer
	t.Setenv("STORMPROOF_ENDPOINTS", "")
	for _, args := range [][]string{{}, {"-endpoints", "x:1", "extra"}, {"-endpoints", "x:1", "-reads", "2"}} {
		if err := run(args, &out); err == nil {
			t.Errorf("%v accepted", args)
		}
	}
}
