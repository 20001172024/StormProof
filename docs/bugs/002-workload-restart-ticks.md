# 002: A restarted leader kept proposing after the workload should have stopped

- **Where:** test workload (`sim/raftsim`)
- **Found:** M3, while adding the end-of-run convergence check
- **Seed:** `crashes` profile, seed 14, 3 nodes
- **Fixed in:** 4349306 (before the workload was first committed)

## Symptom

```
step 9926 at 15.000000s: final-check: node 1 committed 125 of 126 entries
```

## Root cause

The trace showed the leader proposing `n2.t9.55` at 14.989 s, three seconds
after proposals were meant to stop. The workload stopped after a node had
ticked `ProposeTicks` times, counting ticks in the `Process`. A restart
creates a new `Process`, so a node that had crashed started counting from
zero and kept proposing almost to the end of the run.

## Fix

The workload stops at a virtual time (`Options.ProposeUntil`), read through
a new `sim.Env.Now`. Time is only exposed to the test workload; the Raft
node is still driven by `Tick` alone.
