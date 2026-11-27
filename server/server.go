package server

import (
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"

	kvv1 "github.com/20001172024/stormproof/proto/stormproof/kv/v1"
	raftv1 "github.com/20001172024/stormproof/proto/stormproof/raft/v1"
	"github.com/20001172024/stormproof/raft"
	"github.com/20001172024/stormproof/server/host"
	"github.com/20001172024/stormproof/storage"
	"github.com/20001172024/stormproof/transport"
)

// Config configures a server.
type Config struct {
	ID raft.NodeID
	// Cluster maps every server's ID to its address, this one included. All
	// of them form the initial configuration. (Membership changes are not
	// available in real mode yet: they need a way to learn new servers'
	// addresses.)
	Cluster map[raft.NodeID]string
	// DataDir holds the WAL and snapshot files.
	DataDir string

	// Tick is the length of one Raft tick; 0 means 50 ms. With the default
	// ElectionTicks (10) and HeartbeatTicks (1) that gives heartbeats every
	// 50 ms and election timeouts of 0.5-1 s. The heartbeat has to be well
	// below the election timeout even with real network jitter, or
	// followers start needless elections.
	Tick           time.Duration
	ElectionTicks  int
	HeartbeatTicks int
	// SnapshotEvery takes a snapshot every this many applied entries; 0
	// means 10,000.
	SnapshotEvery uint64

	// Listener, if set, is used instead of listening on Cluster[ID]. Tests
	// use it to pick free ports.
	Listener net.Listener
	Logger   *slog.Logger

	// MetricsAddr, if set, serves Prometheus metrics at /metrics and the
	// /healthz and /readyz probes over HTTP. MetricsListener, if set, is
	// used instead.
	MetricsAddr     string
	MetricsListener net.Listener
}

// Server is one StormProof server: a host.Host driven by real time, real
// files and gRPC.
//
// Concurrency: one goroutine, run, owns the Host and is the only one that
// ever touches it. Everything else talks to it through channels: the
// ticker, Raft messages from the gRPC RaftService, and client calls from
// the KV service, which wait on a channel for their response. The Host
// stays exactly as single-threaded as it is in the simulator.
type Server struct {
	cfg  Config
	log  *slog.Logger
	host *host.Host
	tr   *transport.Transport
	grpc *grpc.Server
	lis  net.Listener

	metrics *metrics
	http    *http.Server

	inbox chan raft.Message
	calls chan call
	stop  chan struct{}
	done  chan struct{}

	mu     sync.Mutex
	status raft.Status // copied after every event, for Status and the KV service's leader hints
	err    error       // why the server stopped, if it failed

	stopOnce sync.Once
}

type call struct {
	req  host.Request
	resp chan host.Response // buffered, so the loop never blocks delivering
}

// New opens a server's storage and prepares it, without starting it.
func New(cfg Config) (*Server, error) {
	if cfg.Tick == 0 {
		cfg.Tick = 50 * time.Millisecond
	}
	if cfg.ElectionTicks == 0 {
		cfg.ElectionTicks = 10
	}
	if cfg.HeartbeatTicks == 0 {
		cfg.HeartbeatTicks = 1
	}
	if cfg.SnapshotEvery == 0 {
		cfg.SnapshotEvery = 10_000
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if _, ok := cfg.Cluster[cfg.ID]; !ok {
		return nil, fmt.Errorf("server: ID %d is not in the cluster", cfg.ID)
	}
	fs, err := storage.NewOSFS(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	var peers []raft.NodeID
	others := map[raft.NodeID]string{}
	for id, addr := range cfg.Cluster {
		if id != cfg.ID {
			peers = append(peers, id)
			others[id] = addr
		}
	}
	h, err := host.Open(host.Config{
		ID: cfg.ID, Peers: peers,
		ElectionTicks: cfg.ElectionTicks, HeartbeatTicks: cfg.HeartbeatTicks, PreVote: true,
		FS: fs, Rand: newRand(), SnapshotEvery: cfg.SnapshotEvery,
	})
	if err != nil {
		return nil, fmt.Errorf("server: recovering from %s: %w", cfg.DataDir, err)
	}
	s := &Server{
		cfg: cfg, log: cfg.Logger.With("node", cfg.ID), host: h,
		inbox: make(chan raft.Message, 4096),
		calls: make(chan call, 1024),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	s.metrics = newMetrics(cfg.ID, func() float64 {
		var n uint64
		for _, d := range s.tr.Dropped() {
			n += d
		}
		return float64(n)
	})
	if s.tr, err = transport.New(others, transport.Options{OnSend: s.metrics.observeSend}); err != nil {
		_ = h.Close()
		return nil, err
	}
	s.status = h.Status()
	s.grpc = grpc.NewServer()
	raftv1.RegisterRaftServiceServer(s.grpc, transport.NewService(s.deliver))
	kvv1.RegisterKVServiceServer(s.grpc, &kvService{s: s})
	return s, nil
}

// newRand returns a generator for election timeouts, seeded from
// crypto/rand so servers started at the same moment don't pick the same
// timeouts. (In the simulator the seed comes from the run's seed instead.)
func newRand() raft.Rand {
	var seed [16]byte
	if _, err := cryptorand.Read(seed[:]); err != nil {
		panic(err) // crypto/rand.Read doesn't fail on supported platforms
	}
	return pcg{rand.New(rand.NewPCG(binary.LittleEndian.Uint64(seed[:8]), binary.LittleEndian.Uint64(seed[8:])))}
}

type pcg struct{ r *rand.Rand }

func (p pcg) Intn(n int) int { return p.r.IntN(n) }

// Start starts serving gRPC and the event loop.
func (s *Server) Start() error {
	lis := s.cfg.Listener
	if lis == nil {
		var err error
		if lis, err = net.Listen("tcp", s.cfg.Cluster[s.cfg.ID]); err != nil {
			return err
		}
	}
	s.lis = lis
	go func() {
		if err := s.grpc.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			s.log.Error("grpc serve", "err", err)
		}
	}()
	if mlis := s.cfg.MetricsListener; mlis != nil || s.cfg.MetricsAddr != "" {
		if mlis == nil {
			var err error
			if mlis, err = net.Listen("tcp", s.cfg.MetricsAddr); err != nil {
				s.grpc.Stop()
				return err
			}
		}
		s.http = &http.Server{Handler: s.handler(), ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := s.http.Serve(mlis); err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.log.Error("metrics serve", "err", err)
			}
		}()
	}
	// Log before the loop starts: from then on it owns s.status (the race
	// detector caught an earlier version reading it here unlocked).
	s.log.Info("started", "addr", lis.Addr().String(), "term", s.Status().Term)
	go s.run()
	return nil
}

// Addr is the address the server listens on.
func (s *Server) Addr() string { return s.lis.Addr().String() }

// Status returns the node's state as of its last event.
func (s *Server) Status() raft.Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

// Err returns why the server stopped itself, or nil.
func (s *Server) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Stop stops the server: the event loop, gRPC, the transport and storage.
// It is like a crash, as far as the cluster can tell, except that
// everything acknowledged is already durable so nothing is lost.
func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		close(s.stop)
		<-s.done
		if s.http != nil {
			_ = s.http.Close()
		}
		s.grpc.Stop()
		s.tr.Close()
		if err := s.host.Close(); err != nil {
			s.log.Error("closing storage", "err", err)
		}
	})
}

// deliver hands a Raft message from another server to the loop. If the
// loop is backed up, the message is dropped: Raft tolerates loss, and
// blocking here would stall the sender's RPC instead.
func (s *Server) deliver(m raft.Message) {
	select {
	case s.inbox <- m:
	default:
	}
}

// maxBatch caps the events processed together, so one batch's latency
// stays bounded under heavy load.
const maxBatch = 512

// run is the event loop. It waits for an event, then takes every other
// event already waiting (up to maxBatch) and hands them to the Host as one
// batch: one Ready, so one WAL write and one fsync for all of them. Under
// light load a batch is a single event, as before; under heavy load the
// fsync cost is shared. The output is then sent out: Raft messages through
// the transport, responses to the client calls waiting for them.
func (s *Server) run() {
	defer close(s.done)
	ticker := time.NewTicker(s.cfg.Tick)
	defer ticker.Stop()

	waiting := map[uint64]chan host.Response{}
	var nextToken uint64
	addCall := func(b *host.Batch, c call) {
		nextToken++
		waiting[nextToken] = c.resp
		c.req.Client = nextToken
		b.Requests = append(b.Requests, c.req)
	}
	for {
		var b host.Batch
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			b.Ticks++
		case m := <-s.inbox:
			b.Messages = append(b.Messages, m)
		case c := <-s.calls:
			addCall(&b, c)
		}
	drain:
		for n := 1; n < maxBatch; n++ {
			select {
			case m := <-s.inbox:
				b.Messages = append(b.Messages, m)
			case c := <-s.calls:
				addCall(&b, c)
			case <-ticker.C:
				b.Ticks++
			default:
				break drain
			}
		}
		out, err := s.host.Process(b)
		s.metrics.observeBatch(len(b.Messages) + len(b.Requests) + b.Ticks)
		if err != nil {
			// Storage failed. Nothing more can be promised durable, so stop:
			// the safe response to, for example, a failed fsync.
			s.log.Error("storage failure: stopping", "err", err)
			s.mu.Lock()
			s.err = err
			s.mu.Unlock()
			return
		}
		s.tr.Send(out.Messages)
		for _, r := range out.Responses {
			if ch, ok := waiting[r.Client]; ok {
				ch <- r
				delete(waiting, r.Client)
			}
		}
		st := s.host.Status()
		s.metrics.observe(s.status, st, s.host.AppliedIndex())
		s.mu.Lock()
		if st.Role != s.status.Role || st.Term != s.status.Term {
			s.log.Info("role change", "role", st.Role.String(), "term", st.Term, "leader", st.Leader)
		}
		s.status = st
		s.mu.Unlock()
	}
}
