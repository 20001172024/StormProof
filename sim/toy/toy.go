// Package toy is a tiny replicated counter used to test the simulator
// before Raft exists. It has the same outside shape as the Raft node will
// have (driven by Tick and Step, persisting to a storage.FS), and one
// property that depends on getting durability right:
//
//	A node's counter never goes backwards, even across crashes.
//
// Each node sometimes increments its counter on a tick and tells its peers,
// and adopts any larger value it hears about. Before a new value is used,
// it is saved with the safe-write pattern: write a temp file, fsync it,
// then rename it over the real file.
//
// New(true) builds a deliberately buggy node that skips the fsync. The
// rename can then reach the disk before the data does, so a machine crash
// can leave a short or empty state file, and the buggy recovery treats that
// as a fresh start at 0. The simulator should find this, which proves the
// fault injection, invariant checking and seed replay all work.
package toy

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"

	"github.com/20001172024/stormproof/sim"
)

const (
	stateFile = "counter"
	tempFile  = "counter.tmp"

	// incrementProb is the chance that a tick increments the counter.
	incrementProb = 0.2
)

// Counter is the message a node sends when its counter changes.
type Counter struct {
	Value uint64 `json:"value"`
}

// Kind implements sim.Body.
func (Counter) Kind() string { return "counter" }

// Node is one toy process.
type Node struct {
	env   sim.Env
	value uint64
	buggy bool
}

// New returns a sim.ProcessFactory for toy nodes. If buggy is true the
// nodes skip fsync and recover carelessly (see the package comment).
func New(buggy bool) sim.ProcessFactory {
	return func(env sim.Env) (sim.Process, error) { return open(env, buggy) }
}

// open starts a node, recovering its counter from disk.
func open(env sim.Env, buggy bool) (*Node, error) {
	n := &Node{env: env, buggy: buggy}

	// A crash between writing the temp file and renaming it leaves the temp
	// file behind. It never replaced the real state, so it is safe to drop.
	if err := env.Disk.Remove(tempFile); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}

	b, err := env.Disk.ReadFile(stateFile)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		// First start: the counter begins at 0.
	case err != nil:
		return nil, err
	case len(b) == 8:
		n.value = binary.BigEndian.Uint64(b)
	case buggy:
		// BUG (deliberate): a damaged state file is treated as a fresh
		// start. Combined with the missing fsync, this loses data.
	default:
		// The correct node never leaves a damaged file, so if one shows up
		// something is badly wrong. Refuse to start rather than guess.
		return nil, fmt.Errorf("state file is %d bytes, want 8", len(b))
	}
	return n, nil
}

// Value returns the node's current counter.
func (n *Node) Value() uint64 { return n.value }

// TraceState implements sim.Stater.
func (n *Node) TraceState() any { return Counter{Value: n.value} }

// Tick implements sim.Process.
func (n *Node) Tick() ([]sim.Message, error) {
	if !n.env.Rand.Chance(incrementProb) {
		return nil, nil
	}
	next := n.value + 1
	// Save first, then update memory and tell peers. If the save fails we
	// return the error, the simulator crashes the node, and nothing has
	// claimed a value that might not be on disk.
	if err := n.save(next); err != nil {
		return nil, err
	}
	n.value = next
	out := make([]sim.Message, 0, len(n.env.Peers))
	for _, p := range n.env.Peers {
		out = append(out, sim.Message{To: p, Body: Counter{Value: next}})
	}
	return out, nil
}

// Step implements sim.Process.
func (n *Node) Step(m sim.Message) ([]sim.Message, error) {
	c, ok := m.Body.(Counter)
	if !ok {
		return nil, fmt.Errorf("unexpected message %T", m.Body)
	}
	if c.Value <= n.value {
		return nil, nil
	}
	if err := n.save(c.Value); err != nil {
		return nil, err
	}
	n.value = c.Value
	return nil, nil
}

// save durably replaces the state file with v: write a temp file, fsync it,
// then rename it over the real file. Rename replaces the file atomically, so
// after a crash the state file holds either the old value or the new one,
// but only if the data was fsynced before the rename.
func (n *Node) save(v uint64) error {
	f, err := n.env.Disk.Create(tempFile)
	if err != nil {
		return err
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	if _, err := f.Write(buf[:]); err != nil {
		_ = f.Close()
		return err
	}
	if !n.buggy {
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	return n.env.Disk.Rename(tempFile, stateFile)
}

// Monotonic returns the invariant that no node's counter ever decreases,
// including across a crash and restart.
func Monotonic() sim.Invariant {
	return &monotonic{highest: map[sim.NodeID]uint64{}}
}

type monotonic struct {
	highest map[sim.NodeID]uint64 // highest value each node has held
}

func (*monotonic) Name() string { return "counter-monotonic" }

func (m *monotonic) Check(nodes []sim.NodeView) error {
	for _, nv := range nodes {
		if !nv.Up {
			continue
		}
		v := nv.Process.(*Node).value
		if v < m.highest[nv.ID] {
			return fmt.Errorf("node %d counter went back from %d to %d", nv.ID, m.highest[nv.ID], v)
		}
		m.highest[nv.ID] = v
	}
	return nil
}
