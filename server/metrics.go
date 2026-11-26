package server

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/20001172024/stormproof/raft"
)

// metrics are the server's Prometheus metrics. Each server has its own
// registry rather than using the global one, so several servers can run in
// one process (as the tests do) without their metrics colliding.
//
// The gauges are set by the event loop after every event, from the node's
// status, so they cost nothing between scrapes and can't race with Raft.
type metrics struct {
	reg *prometheus.Registry

	term, role, commit, applied, applyLag, lastIndex, snapIndex, voters prometheus.Gauge
	isLeader                                                            prometheus.Gauge

	elections     prometheus.Counter // times this server started an election
	leaderChanges prometheus.Counter // times the leader this server knows of changed

	clientRequests *prometheus.HistogramVec // by op and gRPC code
	raftSends      *prometheus.HistogramVec // Send RPCs to each peer, by peer and result
}

func newMetrics(id raft.NodeID, dropped func() float64) *metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	labels := prometheus.Labels{"node": strconv.FormatUint(uint64(id), 10)}
	gauge := func(name, help string) prometheus.Gauge {
		g := prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "stormproof", Name: name, Help: help, ConstLabels: labels})
		reg.MustRegister(g)
		return g
	}
	counter := func(name, help string) prometheus.Counter {
		c := prometheus.NewCounter(prometheus.CounterOpts{Namespace: "stormproof", Name: name, Help: help, ConstLabels: labels})
		reg.MustRegister(c)
		return c
	}
	m := &metrics{
		reg:       reg,
		term:      gauge("raft_term", "Current Raft term."),
		role:      gauge("raft_role", "Current role: 0 follower, 1 candidate, 2 leader."),
		isLeader:  gauge("raft_is_leader", "1 if this server is the leader, else 0."),
		commit:    gauge("raft_commit_index", "Highest log index known to be committed."),
		applied:   gauge("raft_applied_index", "Highest log index applied to the key-value store."),
		applyLag:  gauge("raft_apply_lag", "Committed entries not yet applied (commit index minus applied index)."),
		lastIndex: gauge("raft_last_index", "Index of the last entry in the log."),
		snapIndex: gauge("raft_snapshot_index", "Last index covered by the latest snapshot."),
		voters:    gauge("raft_voters", "Number of voters in the current configuration."),

		elections:     counter("raft_elections_started_total", "Elections this server has started (times it became a candidate)."),
		leaderChanges: counter("raft_leader_changes_total", "Times the leader known to this server changed to a new server."),

		clientRequests: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "stormproof", Name: "client_request_duration_seconds",
			Help:        "Client requests handled by this server, by operation and gRPC result code.",
			ConstLabels: labels,
			Buckets:     prometheus.ExponentialBuckets(0.0005, 2, 14), // 0.5 ms to about 4 s
		}, []string{"op", "code"}),
		raftSends: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "stormproof", Name: "raft_send_duration_seconds",
			Help:        "Raft Send RPCs to each peer, by peer and result.",
			ConstLabels: labels,
			Buckets:     prometheus.ExponentialBuckets(0.0001, 2, 14), // 0.1 ms to about 0.8 s
		}, []string{"peer", "result"}),
	}
	reg.MustRegister(m.clientRequests, m.raftSends)
	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: "stormproof", Name: "raft_messages_dropped_total",
		Help:        "Raft messages dropped because a peer's send queue was full or the peer unknown.",
		ConstLabels: labels,
	}, dropped))
	return m
}

// observe updates the gauges from the node's state after an event, and
// counts elections and leader changes by comparing with the previous state.
func (m *metrics) observe(prev, cur raft.Status, applied uint64) {
	m.term.Set(float64(cur.Term))
	m.role.Set(float64(cur.Role))
	if cur.Role == raft.Leader {
		m.isLeader.Set(1)
	} else {
		m.isLeader.Set(0)
	}
	m.commit.Set(float64(cur.Commit))
	m.applied.Set(float64(applied))
	m.applyLag.Set(float64(cur.Commit) - float64(applied))
	m.lastIndex.Set(float64(cur.LastIndex))
	m.snapIndex.Set(float64(cur.SnapIndex))
	m.voters.Set(float64(len(cur.Voters)))
	if cur.Role == raft.Candidate && (prev.Role != raft.Candidate || cur.Term != prev.Term) {
		m.elections.Inc()
	}
	if cur.Leader != raft.None && cur.Leader != prev.Leader {
		m.leaderChanges.Inc()
	}
}

func (m *metrics) observeRequest(op, code string, d time.Duration) {
	m.clientRequests.WithLabelValues(op, code).Observe(d.Seconds())
}

func (m *metrics) observeSend(to raft.NodeID, d time.Duration, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	m.raftSends.WithLabelValues(strconv.FormatUint(uint64(to), 10), result).Observe(d.Seconds())
}

// handler serves /metrics, plus two probes for orchestrators:
//
//   - /healthz: 200 while the event loop is running; 503 once the server
//     has stopped itself (for example after a storage failure).
//   - /readyz: 200 once this server knows a leader, so requests sent to it
//     will be served or redirected; 503 before that.
func (s *Server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(s.metrics.reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if s.Err() != nil {
			http.Error(w, s.Err().Error(), http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if st := s.Status(); s.Err() != nil || st.Leader == raft.None {
			http.Error(w, "no known leader", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})
	return mux
}
