// Package raft is a deterministic implementation of the Raft consensus
// algorithm (Ongaro & Ousterhout, "In Search of an Understandable Consensus
// Algorithm", 2014).
//
// The package is a pure state machine: it has no goroutines, no clocks, no
// I/O and no global randomness. It is driven only by three calls:
//
//   - Step(msg): deliver an incoming message
//   - Tick(): advance logical time by one tick
//   - Propose(cmd): submit a client command
//
// After each call the caller collects a Ready batch (messages to send,
// entries to persist, committed entries to apply, snapshot to save), acts
// on it, and then calls Advance. Everything that touches the outside world
// lives outside this package, which is what lets the simulator replay any
// run exactly from its seed.
//
// The purity rules are enforced by depguard (see .golangci.yml) and by
// purity_test.go. See docs/design.md.
package raft
