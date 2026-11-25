// Package server wires raft, kv, storage and transport together into a
// runnable node: it runs the loop that calls Tick and Step, persists and
// sends each Ready batch, and applies committed entries.
package server
