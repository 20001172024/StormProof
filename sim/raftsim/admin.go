package raftsim

import (
	"fmt"
	"slices"

	"github.com/20001172024/stormproof/raft"
	"github.com/20001172024/stormproof/sim"
)

// AdminOptions configures the simulated admin, which changes the cluster's
// membership while the run's faults are going on.
type AdminOptions struct {
	ChangeProb float64  // chance per tick that an idle admin starts a change
	MinVoters  int      // never remove below this many voters
	Timeout    sim.Time // resend a request with no answer after this long
	RetryDelay sim.Time // wait this long before resending after "retry"
	Until      sim.Time // no new changes after this; 0 means none
	RetryUntil sim.Time // stop retrying the last one; 0 means never stop
}

// DefaultAdminOptions starts about one change a second, keeping at least
// three voters.
var DefaultAdminOptions = AdminOptions{
	ChangeProb: 0.01, MinVoters: 3, Timeout: 300 * sim.Millisecond, RetryDelay: 50 * sim.Millisecond,
}

// Admin is a simulated operator that adds and removes servers one at a time.
// Like the KV clients it finds the leader by following hints and resends
// requests that get no answer. It keeps its own idea of the configuration,
// updated from every answer, to choose a sensible change: add a server that
// isn't a voter, or remove one that is.
type Admin struct {
	env    sim.Env
	opts   AdminOptions
	target sim.NodeID
	voters []raft.NodeID // the configuration as of the last answer

	nextRequest uint64
	cur         *raft.ConfChange
	sentAt      sim.Time
	Changes     int // changes acknowledged
}

// newAdmin returns an admin that believes the voters are initialVoters.
func newAdmin(env sim.Env, o AdminOptions, initialVoters int) *Admin {
	a := &Admin{env: env, opts: o, target: env.Peers[0]}
	for i := 1; i <= initialVoters; i++ {
		a.voters = append(a.voters, raft.NodeID(i))
	}
	return a
}

// TraceState implements sim.Stater.
func (a *Admin) TraceState() any {
	return struct {
		Voters  []raft.NodeID    `json:"voters"`
		Pending *raft.ConfChange `json:"pending,omitempty"`
		Changes int              `json:"changes"`
	}{a.voters, a.cur, a.Changes}
}

// Tick implements sim.Process.
func (a *Admin) Tick() ([]sim.Message, error) {
	now := a.env.Now()
	if a.cur != nil {
		if a.opts.RetryUntil > 0 && now >= a.opts.RetryUntil {
			return nil, nil
		}
		if now-a.sentAt >= a.opts.Timeout {
			a.nextServer()
			return a.send(now), nil
		}
		return nil, nil
	}
	if (a.opts.Until > 0 && now >= a.opts.Until) || !a.env.Rand.Chance(a.opts.ChangeProb) {
		return nil, nil
	}
	a.cur = a.pickChange()
	a.nextRequest++
	return a.send(now), nil
}

// pickChange adds a non-voter or removes a voter, at random, keeping at
// least MinVoters.
func (a *Admin) pickChange() *raft.ConfChange {
	var outside []raft.NodeID
	for _, p := range a.env.Peers {
		if !slices.Contains(a.voters, raft.NodeID(p)) {
			outside = append(outside, raft.NodeID(p))
		}
	}
	r := a.env.Rand
	if len(outside) > 0 && (len(a.voters) <= a.opts.MinVoters || r.Chance(0.5)) {
		return &raft.ConfChange{Op: raft.AddVoter, Node: outside[r.Intn(len(outside))]}
	}
	return &raft.ConfChange{Op: raft.RemoveVoter, Node: a.voters[r.Intn(len(a.voters))]}
}

func (a *Admin) send(now sim.Time) []sim.Message {
	a.sentAt = now
	return []sim.Message{{To: a.target, Body: clientRequest{
		Cmd:  kvCommandID(uint64(a.env.ID), a.nextRequest),
		Conf: a.cur,
	}}}
}

func (a *Admin) nextServer() {
	i := slices.Index(a.env.Peers, a.target)
	a.target = a.env.Peers[(i+1)%len(a.env.Peers)]
}

// Step implements sim.Process.
func (a *Admin) Step(m sim.Message) ([]sim.Message, error) {
	resp, ok := m.Body.(clientResponse)
	if !ok {
		return nil, fmt.Errorf("admin: unexpected message %T", m.Body)
	}
	if a.cur == nil || resp.RequestID != a.nextRequest {
		return nil, nil
	}
	now := a.env.Now()
	switch resp.Status {
	case statusOK:
		a.voters = resp.Voters
		a.cur = nil
		a.Changes++
		return nil, nil
	case statusNotLeader:
		if resp.Leader != 0 && resp.Leader != m.From {
			a.target = resp.Leader
		} else {
			a.nextServer()
		}
		return a.send(now), nil
	default: // retry: the leader is busy with another change; try again shortly
		a.sentAt = now - a.opts.Timeout + a.opts.RetryDelay
		return nil, nil
	}
}
