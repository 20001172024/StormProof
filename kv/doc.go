// Package kv is the replicated key-value state machine that sits on top of
// the Raft log: Get, Put, Delete and CompareAndSwap, plus client sessions
// that deduplicate retried writes for exactly-once semantics.
//
// Like package raft it must be deterministic: applying the same sequence of
// committed entries must always produce the same state.
package kv
