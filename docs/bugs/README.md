# Bugs found by the simulator

One file per bug, with the seed that showed it, the symptom, the root
cause, the fix and the commit. Only real bugs are listed: the bugs planted
on purpose to test the simulator (`-buggy`, `TestingKnobs`) are not.

| # | Where | Summary | Found by |
|---|---|---|---|
| [001](001-quiet-period-disk-faults.md) | simulator | The quiet period didn't stop injected fsync failures | `crashes` seed 4, end-of-run check |
| [002](002-workload-restart-ticks.md) | test workload | A restarted leader kept proposing after the workload should have stopped | `crashes` seed 14, end-of-run check |
| [003](003-leader-completeness-stale-leader.md) | invariant checker | Leader completeness flagged stale leaders that Raft makes no promise about | `partitions` seed 1, leader completeness |

So far none of the bugs is in the Raft implementation itself. All three
are in the testing infrastructure, which is a reminder that the checker
needs testing as much as the system it checks.
