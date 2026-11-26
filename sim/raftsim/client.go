package raftsim

import (
	"fmt"

	"github.com/20001172024/stormproof/check"
	"github.com/20001172024/stormproof/kv"
	"github.com/20001172024/stormproof/sim"
)

// Messages between clients and servers.

type clientRequest struct {
	Cmd kv.Command `json:"cmd"`
}

func (clientRequest) Kind() string { return "ClientRequest" }

// Response statuses.
const (
	statusOK        = "ok"
	statusNotLeader = "not-leader" // try Leader, if set, or another server
	statusRetry     = "retry"      // outcome unknown or not ready: send the same request again
)

type clientResponse struct {
	RequestID uint64     `json:"request"`
	Status    string     `json:"status"`
	Leader    sim.NodeID `json:"leader,omitempty"`
	Result    kv.Result  `json:"result"`
}

func (clientResponse) Kind() string { return "ClientResponse" }

// ClientOptions configures the simulated clients' workload.
type ClientOptions struct {
	Keys    int      // operations pick from keys k0..k<Keys-1>: few keys, so operations conflict
	OpProb  float64  // chance per tick that an idle client starts an operation
	Timeout sim.Time // resend a request that has had no answer for this long
	// Until stops new operations; RetryUntil stops retrying the last one, so
	// the cluster can settle before the run ends. An operation still open at
	// RetryUntil is recorded with an unknown outcome.
	Until      sim.Time
	RetryUntil sim.Time
}

// DefaultClientOptions has each idle client start an operation on about one
// tick in five, on just two keys. Few keys means many operations on the
// same key overlap, which is what exposes stale reads: with five keys and
// three clients, a planted stale-read bug was caught in 0-10 of 50 seeds
// per profile, with two keys and six clients in up to 13.
var DefaultClientOptions = ClientOptions{Keys: 2, OpProb: 0.2, Timeout: 300 * sim.Millisecond}

// DefaultClients is the number of simulated clients used by tests and the
// CLI.
const DefaultClients = 6

// NewClient returns a factory for simulated KV clients.
func NewClient(o ClientOptions) sim.ProcessFactory {
	return func(env sim.Env) (sim.Process, error) {
		return &Client{env: env, opts: o, target: env.Peers[0], lastSeen: map[string][]byte{}}, nil
	}
}

// Client is a simulated KV client with one request outstanding at a time.
// It finds the leader by following "not leader" hints, resends a request
// that times out to the next server, and records every operation it made.
type Client struct {
	env    sim.Env
	opts   ClientOptions
	target sim.NodeID // the server it believes is the leader

	nextRequest uint64
	cur         *outstanding
	history     []check.Op
	// lastSeen is the last value this client read for each key, so a CAS
	// can expect a value that is plausibly current and sometimes succeed.
	lastSeen map[string][]byte
}

type outstanding struct {
	cmd    kv.Command
	call   sim.Time
	sentAt sim.Time
}

// History returns every operation the client made, finished or not.
func (c *Client) History() []check.Op {
	h := c.history
	if c.cur != nil {
		h = append(h, check.Op{ClientID: c.clientIndex(), Cmd: c.cur.cmd, Call: int64(c.cur.call)})
	}
	return h
}

// clientIndex is a zero-based client number, as Porcupine likes them.
func (c *Client) clientIndex() int { return int(c.env.ID) - len(c.env.Peers) - 1 }

// TraceState implements sim.Stater.
func (c *Client) TraceState() any {
	type state struct {
		Op      kv.Op      `json:"op,omitempty"`
		Key     string     `json:"key,omitempty"`
		Request uint64     `json:"request,omitempty"`
		Target  sim.NodeID `json:"target"`
		Done    int        `json:"done"`
	}
	st := state{Target: c.target, Done: len(c.history)}
	if c.cur != nil {
		st.Op, st.Key, st.Request = c.cur.cmd.Op, c.cur.cmd.Key, c.cur.cmd.RequestID
	}
	return st
}

// Tick implements sim.Process.
func (c *Client) Tick() ([]sim.Message, error) {
	now := c.env.Now()
	if c.cur != nil {
		if now >= c.opts.RetryUntil && c.opts.RetryUntil > 0 {
			return nil, nil // give up: recorded as an unknown outcome
		}
		if now-c.cur.sentAt >= c.opts.Timeout {
			c.nextServer() // that server may be down or cut off
			return c.send(now), nil
		}
		return nil, nil
	}
	if (c.opts.Until > 0 && now >= c.opts.Until) || !c.env.Rand.Chance(c.opts.OpProb) {
		return nil, nil
	}
	c.nextRequest++
	c.cur = &outstanding{cmd: c.newCommand(), call: now}
	return c.send(now), nil
}

// newCommand picks a random operation: 40% Get, 25% Put, 25% CAS, 10%
// Delete. Written values are unique, so the checker can tell them apart.
func (c *Client) newCommand() kv.Command {
	r := c.env.Rand
	cmd := kv.Command{
		ClientID:  uint64(c.env.ID),
		RequestID: c.nextRequest,
		Key:       fmt.Sprintf("k%d", r.Intn(c.opts.Keys)),
	}
	value := []byte(fmt.Sprintf("c%d.%d", c.env.ID, c.nextRequest))
	switch p := r.Intn(100); {
	case p < 40:
		cmd.Op = kv.OpGet
	case p < 65:
		cmd.Op, cmd.Value = kv.OpPut, value
	case p < 90:
		cmd.Op, cmd.Value = kv.OpCAS, value
		if seen, ok := c.lastSeen[cmd.Key]; ok && seen != nil {
			cmd.Expected = seen
		} else {
			cmd.ExpectAbsent = true
		}
	default:
		cmd.Op = kv.OpDelete
	}
	return cmd
}

func (c *Client) send(now sim.Time) []sim.Message {
	c.cur.sentAt = now
	return []sim.Message{{To: c.target, Body: clientRequest{Cmd: c.cur.cmd}}}
}

func (c *Client) nextServer() {
	for i, p := range c.env.Peers {
		if p == c.target {
			c.target = c.env.Peers[(i+1)%len(c.env.Peers)]
			return
		}
	}
	c.target = c.env.Peers[0]
}

// Step implements sim.Process.
func (c *Client) Step(m sim.Message) ([]sim.Message, error) {
	resp, ok := m.Body.(clientResponse)
	if !ok {
		return nil, fmt.Errorf("client: unexpected message %T", m.Body)
	}
	if c.cur == nil || resp.RequestID != c.cur.cmd.RequestID {
		return nil, nil // a late or duplicated answer to an earlier request
	}
	now := c.env.Now()
	if c.opts.RetryUntil > 0 && now >= c.opts.RetryUntil {
		return nil, nil
	}
	switch resp.Status {
	case statusOK:
		c.history = append(c.history, check.Op{
			ClientID: c.clientIndex(), Cmd: c.cur.cmd, Call: int64(c.cur.call),
			Return: int64(now), Done: true, Result: resp.Result,
		})
		c.remember(c.cur.cmd, resp.Result)
		c.cur = nil
		return nil, nil
	case statusNotLeader:
		if resp.Leader != 0 && resp.Leader != m.From {
			c.target = resp.Leader
		} else {
			c.nextServer()
		}
	case statusRetry:
		// Same server: it is the leader, just not able to answer yet.
	}
	return c.send(now), nil
}

// remember tracks the value this client last saw for a key.
func (c *Client) remember(cmd kv.Command, r kv.Result) {
	switch cmd.Op {
	case kv.OpGet:
		if r.Found {
			c.lastSeen[cmd.Key] = r.Value
		} else {
			delete(c.lastSeen, cmd.Key)
		}
	case kv.OpPut:
		c.lastSeen[cmd.Key] = cmd.Value
	case kv.OpCAS:
		if r.Swapped {
			c.lastSeen[cmd.Key] = cmd.Value
		} else if r.Found {
			c.lastSeen[cmd.Key] = r.Value
		} else {
			delete(c.lastSeen, cmd.Key)
		}
	case kv.OpDelete:
		delete(c.lastSeen, cmd.Key)
	}
}
