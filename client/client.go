package client

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	kvv1 "github.com/20001172024/stormproof/proto/stormproof/kv/v1"
)

// leaderAddrTrailer must match server.LeaderAddrTrailer. (It is repeated
// here so the client doesn't depend on the server package.)
const leaderAddrTrailer = "stormproof-leader-addr"

// Options configures a Client.
type Options struct {
	// AttemptTimeout bounds each attempt at one server; 0 means one second.
	AttemptTimeout time.Duration
}

// Client talks to a StormProof cluster. It finds the leader by following
// the hints servers give when they aren't it, and retries an operation
// until it succeeds or the caller's context ends.
//
// Every write carries this client's ID and a request number, and a retry
// reuses the same number, so a write whose first attempt did commit is not
// applied twice (exactly-once).
//
// A Client runs one operation at a time: concurrent calls wait their turn.
// That isn't only simplicity. The server remembers the latest request per
// client and treats anything older as a duplicate; if one client had
// requests 5 and 6 in flight and 6 was applied first, 5 would be mistaken
// for an old duplicate. For concurrency, use several Clients.
type Client struct {
	opts      Options
	clientID  uint64
	mu        sync.Mutex // one operation at a time; guards everything below
	endpoints []string
	conns     map[string]*grpc.ClientConn
	target    int // index into endpoints of the server we think leads
	nextReq   uint64
}

// New returns a client for the given server addresses.
func New(endpoints []string, o Options) (*Client, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("client: no endpoints")
	}
	if o.AttemptTimeout == 0 {
		o.AttemptTimeout = time.Second
	}
	var id [8]byte
	if _, err := cryptorand.Read(id[:]); err != nil {
		return nil, err
	}
	c := &Client{
		opts: o, clientID: binary.LittleEndian.Uint64(id[:]) | 1, // never 0, which means "no session"
		endpoints: slices.Clone(endpoints), conns: map[string]*grpc.ClientConn{},
	}
	return c, nil
}

// Close closes the client's connections.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var errs []error
	for _, conn := range c.conns {
		errs = append(errs, conn.Close())
	}
	return errors.Join(errs...)
}

func (c *Client) conn(addr string) (kvv1.KVServiceClient, error) {
	conn, ok := c.conns[addr]
	if !ok {
		var err error
		if conn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials())); err != nil {
			return nil, err
		}
		c.conns[addr] = conn
	}
	return kvv1.NewKVServiceClient(conn), nil
}

// header starts a new write request.
func (c *Client) header() *kvv1.RequestHeader {
	c.nextReq++
	return &kvv1.RequestHeader{ClientId: c.clientID, RequestId: c.nextReq}
}

// do runs one attempt function until it succeeds or ctx ends, moving to the
// leader the servers point to, or to the next server when one is down,
// cut off or not ready.
func (c *Client) do(ctx context.Context, attempt func(context.Context, kvv1.KVServiceClient, ...grpc.CallOption) error) error {
	backoff := 10 * time.Millisecond
	for {
		addr := c.endpoints[c.target]
		kvc, err := c.conn(addr)
		if err != nil {
			return err
		}
		actx, cancel := context.WithTimeout(ctx, c.opts.AttemptTimeout)
		var trailer metadata.MD
		err = attempt(actx, kvc, grpc.Trailer(&trailer))
		cancel()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		switch status.Code(err) {
		case codes.FailedPrecondition: // not the leader
			if hint := trailer.Get(leaderAddrTrailer); len(hint) > 0 {
				c.goTo(hint[0])
				continue // straight to the leader, no backoff
			}
			c.next()
		case codes.Unavailable:
			// Either down/unreachable, or the leader said "retry" (outcome
			// unknown, or not ready yet). In the second case the same server
			// is right, but moving on costs at most one hop.
			c.next()
		case codes.DeadlineExceeded:
			c.next()
		default:
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, 200*time.Millisecond)
	}
}

func (c *Client) next() { c.target = (c.target + 1) % len(c.endpoints) }

func (c *Client) goTo(addr string) {
	if i := slices.Index(c.endpoints, addr); i >= 0 {
		c.target = i
		return
	}
	c.endpoints = append(c.endpoints, addr)
	c.target = len(c.endpoints) - 1
}

// Get reads a key. Reads are linearizable: the leader confirms it is still
// the leader before answering (ReadIndex).
func (c *Client) Get(ctx context.Context, key string) (value []byte, found bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	err = c.do(ctx, func(ctx context.Context, kvc kvv1.KVServiceClient, opts ...grpc.CallOption) error {
		r, err := kvc.Get(ctx, &kvv1.GetRequest{Key: []byte(key)}, opts...)
		if err == nil {
			value, found = r.GetValue(), r.GetFound()
		}
		return err
	})
	return value, found, err
}

// Put sets a key.
func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	h := c.header()
	return c.do(ctx, func(ctx context.Context, kvc kvv1.KVServiceClient, opts ...grpc.CallOption) error {
		_, err := kvc.Put(ctx, &kvv1.PutRequest{Header: h, Key: []byte(key), Value: value}, opts...)
		return err
	})
}

// Delete removes a key and reports whether it existed.
func (c *Client) Delete(ctx context.Context, key string) (existed bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	h := c.header()
	err = c.do(ctx, func(ctx context.Context, kvc kvv1.KVServiceClient, opts ...grpc.CallOption) error {
		r, err := kvc.Delete(ctx, &kvv1.DeleteRequest{Header: h, Key: []byte(key)}, opts...)
		if err == nil {
			existed = r.GetExisted()
		}
		return err
	})
	return existed, err
}

// CASResult is the outcome of CompareAndSwap.
type CASResult struct {
	Swapped bool
	Found   bool   // the key existed before
	Current []byte // its value before
}

// CompareAndSwap sets key to newValue if its value is expected, or, with
// expectAbsent, if it doesn't exist.
func (c *Client) CompareAndSwap(ctx context.Context, key string, expected []byte, expectAbsent bool, newValue []byte) (CASResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	h := c.header()
	var res CASResult
	err := c.do(ctx, func(ctx context.Context, kvc kvv1.KVServiceClient, opts ...grpc.CallOption) error {
		r, err := kvc.CompareAndSwap(ctx, &kvv1.CompareAndSwapRequest{
			Header: h, Key: []byte(key), ExpectedValue: expected, ExpectAbsent: expectAbsent, NewValue: newValue,
		}, opts...)
		if err == nil {
			res = CASResult{Swapped: r.GetSwapped(), Found: r.GetFound(), Current: r.GetCurrentValue()}
		}
		return err
	})
	return res, err
}
