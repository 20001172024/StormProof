// Package raftsim runs raft.Node inside the simulator.
//
// It is the simulator's version of the glue the real server will have
// (M7): it drives the node with Tick and Step, saves the node's HardState
// and log to disk before sending any of its messages, applies committed
// entries, and reloads everything on restart.
//
// Until the KV store and real clients arrive (M5), the leader also
// generates its own workload: on some ticks it proposes a unique command.
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
	stateFile = "raftstate"
	tempFile  = "raftstate.tmp"
)

// Options configures the simulated Raft nodes.
type Options struct {
	ElectionTicks  int
	HeartbeatTicks int

	// ProposeProb is the chance that a leader proposes a new command on a
	// tick. ProposeUntil stops proposals at that virtual time, so the
	// cluster can finish replicating before the run ends; 0 means proposals
	// never stop.
	ProposeProb  float64
	ProposeUntil sim.Time

	// SkipSync plants a durability bug for testing the simulator: state is
	// written but never fsynced, and a damaged state file is silently
	// treated as a fresh start. A machine crash can then make a node forget
	// its term, its vote and its log.
	SkipSync bool

	// Knobs are passed to raft to switch off individual safety rules.
	Knobs raft.TestingKnobs
}

// DefaultOptions: with a 10 ms tick, heartbeats every 10 ms, election
// timeouts drawn from 100-200 ms, and a leader proposing on about one tick
// in ten (about 10 commands per second).
var DefaultOptions = Options{ElectionTicks: 10, HeartbeatTicks: 1, ProposeProb: 0.1}

// New returns a factory for simulated Raft nodes.
func New(o Options) sim.ProcessFactory {
	return func(env sim.Env) (sim.Process, error) { return open(env, o) }
}

// Process is one simulated Raft server.
type Process struct {
	env  sim.Env
	opts Options
	node *raft.Node

	// stored mirrors what is on disk, so a save can rewrite the whole file.
	storedHard    raft.HardState
	storedEntries []raft.Entry

	applied  []raft.Entry // every entry applied since this process started
	proposed int
	// logVersion goes up whenever the log changes. Every change (append or
	// truncation) reaches Ready.Entries, because it has to be persisted, so
	// counting non-empty Ready.Entries catches them all. Invariants use it to
	// skip re-checking logs that haven't changed.
	logVersion uint64
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
	hs, entries, err := load(env, o.SkipSync)
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
		Entries:        entries,
		Knobs:          o.Knobs,
	})
	if err != nil {
		return nil, err
	}
	return &Process{env: env, opts: o, node: node, storedHard: hs, storedEntries: entries}, nil
}

// Status returns the node's state, for invariants.
func (p *Process) Status() raft.Status { return p.node.Status() }

// Entries returns the node's log. Read-only; see raft.Node.Entries.
func (p *Process) Entries() []raft.Entry { return p.node.Entries() }

// LogVersion changes whenever the node's log changes.
func (p *Process) LogVersion() uint64 { return p.logVersion }

// Applied returns the entries applied since this process started, in order.
func (p *Process) Applied() []raft.Entry { return p.applied }

// TraceState implements sim.Stater.
func (p *Process) TraceState() any { return p.node.Status() }

// Tick implements sim.Process.
func (p *Process) Tick() ([]sim.Message, error) {
	p.node.Tick()
	p.maybePropose()
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

// maybePropose is the stand-in workload: the leader sometimes proposes a
// command. Commands are unique ("n<node>.t<term>.<count>") so that if two
// nodes ever apply different commands at the same index, it shows.
func (p *Process) maybePropose() {
	// The cut-off is virtual time, not a count of this process's ticks: a
	// node that restarted would start counting from zero and keep proposing
	// long after the others stopped. (The first version did exactly that.)
	if p.opts.ProposeUntil > 0 && p.env.Now() >= p.opts.ProposeUntil {
		return
	}
	if !p.env.Rand.Chance(p.opts.ProposeProb) {
		return
	}
	st := p.node.Status()
	if st.Role != raft.Leader {
		return
	}
	p.proposed++
	cmd := fmt.Sprintf("n%d.t%d.%d", st.ID, st.Term, p.proposed)
	if _, err := p.node.Propose([]byte(cmd)); err != nil {
		panic(err) // can't happen: we just checked we are the leader
	}
}

// handleReady does what raft.Ready asks, in the required order: persist
// first, then release messages, then apply committed entries.
func (p *Process) handleReady() ([]sim.Message, error) {
	rd := p.node.Ready()
	if rd.HardState != nil || len(rd.Entries) > 0 {
		if err := p.save(rd); err != nil {
			// Send nothing: the messages may depend on state that isn't
			// durable. The simulator crashes the node.
			return nil, err
		}
	}
	out := make([]sim.Message, len(rd.Messages))
	for i, m := range rd.Messages {
		out[i] = sim.Message{To: sim.NodeID(m.To), Body: body{m}}
	}
	if len(rd.Entries) > 0 {
		p.logVersion++
	}
	p.applied = append(p.applied, rd.CommittedEntries...)
	p.node.Advance()
	return out, nil
}

// save applies the Ready's HardState and Entries to the stored copy and
// durably rewrites the whole state file (temp file, fsync, rename).
//
// Rewriting everything on every change is slow but obviously correct; the
// write-ahead log in M4 replaces it. The point here is the contract: when
// Entries starts at an index we already hold, the stored log is cut there
// first.
func (p *Process) save(rd raft.Ready) error {
	hs := p.storedHard
	if rd.HardState != nil {
		hs = *rd.HardState
	}
	entries := p.storedEntries
	if len(rd.Entries) > 0 {
		first := rd.Entries[0].Index
		if first > uint64(len(entries))+1 {
			return fmt.Errorf("raftsim: Ready.Entries starts at %d but only %d are stored", first, len(entries))
		}
		// The three-index slice caps capacity, so append copies instead of
		// overwriting entries that p.storedEntries still shares.
		entries = append(entries[:first-1:first-1], rd.Entries...)
	}

	f, err := p.env.Disk.Create(tempFile)
	if err != nil {
		return err
	}
	if _, err := f.Write(encode(hs, entries)); err != nil {
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
	if err := p.env.Disk.Rename(tempFile, stateFile); err != nil {
		return err
	}
	if !p.opts.SkipSync {
		if err := p.env.Disk.SyncDir(); err != nil {
			return err
		}
	}
	p.storedHard, p.storedEntries = hs, entries
	return nil
}

// The state file is: term, vote, entry count (8 bytes each, big-endian),
// then for each entry its index, term (8 bytes each), data length (4 bytes)
// and data. There is no checksum yet; the M4 write-ahead log adds CRCs.
func encode(hs raft.HardState, entries []raft.Entry) []byte {
	b := binary.BigEndian.AppendUint64(nil, uint64(hs.Term))
	b = binary.BigEndian.AppendUint64(b, uint64(hs.Vote))
	b = binary.BigEndian.AppendUint64(b, uint64(len(entries)))
	for _, e := range entries {
		b = binary.BigEndian.AppendUint64(b, e.Index)
		b = binary.BigEndian.AppendUint64(b, uint64(e.Term))
		b = binary.BigEndian.AppendUint32(b, uint32(len(e.Data)))
		b = append(b, e.Data...)
	}
	return b
}

var errCorrupt = errors.New("state file is truncated or corrupt")

func decode(b []byte) (raft.HardState, []raft.Entry, error) {
	u64 := func() (uint64, bool) {
		if len(b) < 8 {
			return 0, false
		}
		v := binary.BigEndian.Uint64(b)
		b = b[8:]
		return v, true
	}
	term, ok1 := u64()
	vote, ok2 := u64()
	count, ok3 := u64()
	if !ok1 || !ok2 || !ok3 {
		return raft.HardState{}, nil, errCorrupt
	}
	hs := raft.HardState{Term: raft.Term(term), Vote: raft.NodeID(vote)}
	var entries []raft.Entry
	for i := uint64(0); i < count; i++ {
		idx, ok1 := u64()
		et, ok2 := u64()
		if !ok1 || !ok2 || len(b) < 4 {
			return raft.HardState{}, nil, errCorrupt
		}
		n := binary.BigEndian.Uint32(b)
		b = b[4:]
		if uint32(len(b)) < n {
			return raft.HardState{}, nil, errCorrupt
		}
		var data []byte
		if n > 0 {
			data = append([]byte(nil), b[:n]...)
		}
		b = b[n:]
		entries = append(entries, raft.Entry{Index: idx, Term: raft.Term(et), Data: data})
	}
	if len(b) != 0 {
		return raft.HardState{}, nil, errCorrupt
	}
	return hs, entries, nil
}

func load(env sim.Env, lenient bool) (raft.HardState, []raft.Entry, error) {
	b, err := env.Disk.ReadFile(stateFile)
	if errors.Is(err, fs.ErrNotExist) {
		return raft.HardState{}, nil, nil
	}
	if err != nil {
		return raft.HardState{}, nil, err
	}
	hs, entries, err := decode(b)
	if err != nil {
		if lenient {
			return raft.HardState{}, nil, nil // the planted bug: see Options.SkipSync
		}
		return raft.HardState{}, nil, fmt.Errorf("%w (%d bytes)", err, len(b))
	}
	return hs, entries, nil
}
