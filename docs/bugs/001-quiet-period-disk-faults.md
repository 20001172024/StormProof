# 001: The quiet period didn't stop injected fsync failures

- **Where:** simulator (`sim`)
- **Found:** M3, while adding the end-of-run convergence check
- **Seed:** `crashes` profile, seed 4, 3 nodes, 15 s run with a 5 s quiet period
- **Fixed in:** 955f4b8

## Symptom

```
step 9403 at 15.000000s: final-check: node 1 committed 136 of 137 entries
```

The run ended with one follower a single commit behind the leader, even
though no faults were supposed to happen in the last 5 seconds.

## Root cause

The trace showed node 3 crashing at 14.80 s with reason
`sim: injected fsync failure`. `Config.Quiet` stopped the fault scheduler
from starting new crashes and partitions, but disk faults are injected by
the simulated disk itself on every `Sync`, and nothing told the disks the
quiet period had started. Node 3 restarted at 14.9 s, the cluster was still
catching up at 15 s, and the end-of-run check failed.

## Fix

An `evQuiet` event at `Duration - Quiet` calls `Disk.StopFaults()` on every
disk and writes a `quiet` record to the trace. `TestDiskStopFaults` and
`TestRunQuietPeriod` cover it.
