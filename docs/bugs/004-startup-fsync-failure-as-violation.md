# 004: An fsync failure during startup was reported as a broken state

- **Where:** simulator (`sim`)
- **Found:** M5, 1,000-seed sweep with 5 nodes and simulated clients
- **Seeds:** `stormproof-sim -nodes 5 -profile all -seed 572` (also 224 and 987)
- **Fixed in:** 498ae44

## Symptom

```
seed 572 (crashes): step 24328 at 7.772179s: recovery: node 2 failed to start: sim: injected fsync failure
```

3 of 1,000 seeds failed; the 3-node sweep was clean.

## Root cause

The trace showed node 2 crashing at 7.52 s from an injected fsync failure.
That is treated as a machine crash, so its WAL was left with a torn tail.
When it restarted at 7.77 s, WAL recovery cut the torn record off by
rewriting the segment (temp file, fsync, rename, directory fsync), and the
simulated disk injected another fsync failure into that rewrite.

The WAL did the right thing and returned the error. The simulator treated
any error from starting a node as a `recovery` violation, which is meant to
say "the state this node left on disk can't be loaded". A transient disk
failure while starting is not that; it is just another fault.

It needs a restart that has to sync (only a torn-tail repair or a brand-new
WAL do) and an injected failure on that particular sync, which is why it
took 5 nodes and 1,000 seeds to show up.

## Fix

If starting a node fails with `sim.ErrSyncFailed`, the simulator crashes it
again and schedules another restart. Any other startup error is still a
`recovery` violation. `TestRunSyncFailureDuringStartIsACrash` covers it.
