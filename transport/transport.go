package transport

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	raftv1 "github.com/20001172024/stormproof/proto/stormproof/raft/v1"
	"github.com/20001172024/stormproof/raft"
)

// Options configures a Transport.
type Options struct {
	// QueueSize is how many messages may wait for each peer. When a peer's
	// queue is full, new messages to it are dropped: Raft tolerates lost
	// messages, and it stops one slow or dead peer from holding up the
	// others or the node itself. 0 means 1024.
	QueueSize int
	// BatchSize caps the messages per Send RPC. 0 means 64.
	BatchSize int
	// Timeout bounds each Send RPC. 0 means one second.
	Timeout time.Duration
	// OnSend, if set, is called after every Send RPC with its duration and
	// error, for metrics.
	OnSend func(to raft.NodeID, d time.Duration, err error)
}

// Transport sends Raft messages to the other servers over gRPC.
//
// Each peer has a queue and one sender goroutine that drains it in
// batches, so sending never blocks the node's loop and one unreachable
// peer can't slow down the others.
type Transport struct {
	opts    Options
	mu      sync.Mutex
	peers   map[raft.NodeID]*peer
	dropped map[raft.NodeID]uint64
	closed  bool
}

type peer struct {
	conn  *grpc.ClientConn
	queue chan raft.Message
	done  chan struct{}
}

// New returns a transport for the given peer addresses (ID to host:port).
// Connections are made lazily, when there is something to send.
func New(addrs map[raft.NodeID]string, o Options) (*Transport, error) {
	if o.QueueSize == 0 {
		o.QueueSize = 1024
	}
	if o.BatchSize == 0 {
		o.BatchSize = 64
	}
	if o.Timeout == 0 {
		o.Timeout = time.Second
	}
	t := &Transport{opts: o, peers: map[raft.NodeID]*peer{}, dropped: map[raft.NodeID]uint64{}}
	for id, addr := range addrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Close()
			return nil, err
		}
		p := &peer{conn: conn, queue: make(chan raft.Message, o.QueueSize), done: make(chan struct{})}
		t.peers[id] = p
		go t.run(id, p)
	}
	return t, nil
}

// Send queues messages for their destinations without blocking. Messages
// to unknown peers, or to peers whose queue is full, are dropped and
// counted.
func (t *Transport) Send(msgs []raft.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	for _, m := range msgs {
		p, ok := t.peers[m.To]
		if !ok {
			t.dropped[m.To]++
			continue
		}
		select {
		case p.queue <- m:
		default:
			t.dropped[m.To]++
		}
	}
}

// Dropped returns how many messages to each peer have been dropped.
func (t *Transport) Dropped() map[raft.NodeID]uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[raft.NodeID]uint64, len(t.dropped))
	for id, n := range t.dropped {
		out[id] = n
	}
	return out
}

// run is a peer's sender goroutine: it waits for a message, takes whatever
// else is queued (up to a batch), and sends them in one RPC. A failed batch
// is dropped, not retried: by the time a retry went out, Raft would have
// newer messages to send anyway.
func (t *Transport) run(id raft.NodeID, p *peer) {
	defer close(p.done)
	client := raftv1.NewRaftServiceClient(p.conn)
	for m := range p.queue {
		batch := []*raftv1.Message{ToProto(m)}
	more:
		for len(batch) < t.opts.BatchSize {
			select {
			case m, ok := <-p.queue:
				if !ok {
					break more
				}
				batch = append(batch, ToProto(m))
			default:
				break more
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), t.opts.Timeout)
		start := time.Now()
		_, err := client.Send(ctx, &raftv1.SendRequest{Messages: batch})
		cancel()
		if t.opts.OnSend != nil {
			t.opts.OnSend(id, time.Since(start), err)
		}
		if err != nil {
			slog.Debug("raft send failed", "to", id, "messages", len(batch), "err", err)
		}
	}
}

// Close stops the senders and closes the connections.
func (t *Transport) Close() {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	for _, p := range t.peers {
		close(p.queue)
	}
	t.mu.Unlock()
	for _, p := range t.peers {
		<-p.done
		_ = p.conn.Close()
	}
}

// Handler receives messages from other servers.
type Handler func(raft.Message)

// Service implements the RaftService gRPC service, passing every message it
// receives to a Handler.
type Service struct {
	raftv1.UnimplementedRaftServiceServer
	handle Handler
}

// NewService returns a RaftService that delivers messages to h.
func NewService(h Handler) *Service { return &Service{handle: h} }

// ErrNoMessages is returned for an empty Send.
var ErrNoMessages = errors.New("transport: empty send")

// Send implements raftv1.RaftServiceServer.
func (s *Service) Send(_ context.Context, req *raftv1.SendRequest) (*raftv1.SendResponse, error) {
	if len(req.GetMessages()) == 0 {
		return nil, ErrNoMessages
	}
	for _, m := range req.GetMessages() {
		s.handle(FromProto(m))
	}
	return &raftv1.SendResponse{}, nil
}
