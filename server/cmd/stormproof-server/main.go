// Command stormproof-server runs one StormProof server.
//
//	stormproof-server -id 1 -cluster 1=localhost:7001,2=localhost:7002,3=localhost:7003 -data /var/lib/stormproof
//
// Every server gets the same -cluster list; it listens on its own entry.
// The data directory holds the write-ahead log and snapshots; restarting
// with the same directory recovers the server's state.
package main

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/20001172024/stormproof/raft"
	"github.com/20001172024/stormproof/server"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "stormproof-server:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fl := flag.NewFlagSet("stormproof-server", flag.ContinueOnError)
	var (
		id       = fl.Uint64("id", 0, "this server's ID (must appear in -cluster)")
		cluster  = fl.String("cluster", "", "every server as id=host:port, comma-separated")
		data     = fl.String("data", "", "data directory (WAL and snapshots)")
		tick     = fl.Duration("tick", 50*time.Millisecond, "Raft tick")
		election = fl.Int("election-ticks", 10, "base election timeout in ticks (actual: random in [n, 2n))")
		hb       = fl.Int("heartbeat-ticks", 1, "heartbeat interval in ticks")
		snapshot = fl.Uint64("snapshot-every", 10_000, "take a snapshot every this many applied entries")
		verbose  = fl.Bool("v", false, "debug logging")
	)
	if err := fl.Parse(args); err != nil {
		return err
	}
	members, err := parseCluster(*cluster)
	if err != nil {
		return err
	}
	if *data == "" {
		return errors.New("-data is required")
	}
	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	s, err := server.New(server.Config{
		ID: raft.NodeID(*id), Cluster: members, DataDir: *data,
		Tick: *tick, ElectionTicks: *election, HeartbeatTicks: *hb, SnapshotEvery: *snapshot,
		Logger: log,
	})
	if err != nil {
		return err
	}
	if err := s.Start(); err != nil {
		return err
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case sig := <-stop:
			log.Info("shutting down", "signal", sig.String())
			s.Stop()
			return nil
		case <-ticker.C:
			if err := s.Err(); err != nil {
				s.Stop()
				return err
			}
		}
	}
}

// parseCluster reads "1=host:port,2=host:port".
func parseCluster(spec string) (map[raft.NodeID]string, error) {
	if spec == "" {
		return nil, errors.New("-cluster is required")
	}
	out := map[raft.NodeID]string{}
	for _, part := range strings.Split(spec, ",") {
		idStr, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || addr == "" {
			return nil, fmt.Errorf("bad -cluster entry %q: want id=host:port", part)
		}
		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("bad server ID %q in -cluster", idStr)
		}
		if _, dup := out[raft.NodeID(id)]; dup {
			return nil, fmt.Errorf("server %d appears twice in -cluster", id)
		}
		out[raft.NodeID(id)] = addr
	}
	return out, nil
}
