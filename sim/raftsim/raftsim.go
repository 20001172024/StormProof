// Package raftsim runs raft.Node inside the simulator.
//
// It is the simulator's version of the glue the real server will have
// (M7): it drives the node with Tick and Step, saves the node's HardState
// to disk before sending any of its messages, and reloads it on restart.
// The write-ahead log in M4 will replace the single HardState file.
package raftsim

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"

	"github.com/20001172024/stormproof/raft"
	"github.com/20001172024/stormproof/sim"
)

const (
	stateFile = "hardstate"
	tempFile  = "hardstate.tmp"
)

// Options configures the simulated Raft nodes.
type Options struct {
	ElectionTicks  int
	HeartbeatTicks int

	// SkipSync plants a durability bug for testing the simulator: the
	// HardState is written but never fsynced, and a damaged state file is
	// silently treated as a fresh start. A machine crash can then make a
	// node forget its term and vote.
	SkipSync bool
}

// DefaultOptions: with a 10 ms tick, heartbeats every 10 ms and election
// timeouts drawn from 100-200 ms.
var DefaultOptions = Options{ElectionTicks: 10, HeartbeatTicks: 1}

// New returns a factory for simulated Raft nodes.
func New(o Options) sim.ProcessFactory {
	return func(env sim.Env) (sim.Process, error) { return open(env, o) }
}

// Process is one simulated Raft server.
type Process struct {
	env  sim.Env
	opts Options
	node *raft.Node
}

// body wraps a raft.Message so the simulator can carry it. Embedding the
// struct means its fields appear directly in the trace JSON.
type body struct {
	raft.Message
}

func (b body) Kind() string { return b.Type.String() }

func open(env sim.Env, o Options) (*Process, error) {
	if err := env.Disk.Remove(tempFile); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	hs, err := loadHardState(env, o.SkipSync)
	if err != nil {
		return nil, err
	}
	peers := make([]raft.NodeID, len(env.Peers))
	for i, p := range env.Peers {
		peers[i] = raft.NodeID(p)
	}
	node, err := raft.NewNode(raft.Config{
		ID:             raft.NodeID(env.ID),
		Peers:          peers,
		ElectionTicks:  o.ElectionTicks,
		HeartbeatTicks: o.HeartbeatTicks,
		Rand:           env.Rand,
		HardState:      hs,
	})
	if err != nil {
		return nil, err
	}
	return &Process{env: env, opts: o, node: node}, nil
}

func loadHardState(env sim.Env, lenient bool) (raft.HardState, error) {
	b, err := env.Disk.ReadFile(stateFile)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return raft.HardState{}, nil
	case err != nil:
		return raft.HardState{}, err
	case len(b) == 16:
		return raft.HardState{
			Term: raft.Term(binary.BigEndian.Uint64(b[0:8])),
			Vote: raft.NodeID(binary.BigEndian.Uint64(b[8:16])),
		}, nil
	case lenient:
		return raft.HardState{}, nil // the planted bug: see Options.SkipSync
	default:
		return raft.HardState{}, fmt.Errorf("hardstate file is %d bytes, want 16", len(b))
	}
}

// Status returns the node's state, for invariants.
func (p *Process) Status() raft.Status { return p.node.Status() }

// TraceState implements sim.Stater.
func (p *Process) TraceState() any { return p.node.Status() }

// Tick implements sim.Process.
func (p *Process) Tick() ([]sim.Message, error) {
	p.node.Tick()
	return p.handleReady()
}

// Step implements sim.Process.
func (p *Process) Step(m sim.Message) ([]sim.Message, error) {
	b, ok := m.Body.(body)
	if !ok {
		return nil, fmt.Errorf("unexpected message %T", m.Body)
	}
	p.node.Step(b.Message)
	return p.handleReady()
}

// handleReady does what raft.Ready asks, in the required order: persist the
// HardState first, and only then release the messages.
func (p *Process) handleReady() ([]sim.Message, error) {
	rd := p.node.Ready()
	if rd.HardState != nil {
		if err := p.save(*rd.HardState); err != nil {
			// Don't send anything: the messages may depend on state that
			// isn't durable. The simulator crashes the node.
			return nil, err
		}
	}
	out := make([]sim.Message, len(rd.Messages))
	for i, m := range rd.Messages {
		out[i] = sim.Message{To: sim.NodeID(m.To), Body: body{m}}
	}
	p.node.Advance()
	return out, nil
}

// save durably replaces the HardState file: write a temp file, fsync it,
// rename it into place.
func (p *Process) save(hs raft.HardState) error {
	f, err := p.env.Disk.Create(tempFile)
	if err != nil {
		return err
	}
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(hs.Term))
	binary.BigEndian.PutUint64(buf[8:16], uint64(hs.Vote))
	if _, err := f.Write(buf[:]); err != nil {
		_ = f.Close()
		return err
	}
	if !p.opts.SkipSync {
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	return p.env.Disk.Rename(tempFile, stateFile)
}
