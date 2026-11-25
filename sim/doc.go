// Package sim runs a whole StormProof cluster inside one goroutine with a
// logical clock, an in-memory network and an in-memory disk, injects faults
// from a seeded RNG, checks invariants after every step, and writes a JSON
// trace that the visualizer can replay.
package sim
