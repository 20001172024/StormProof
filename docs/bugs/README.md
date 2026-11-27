# Bugs found by the simulator

One file per bug, with the seed that showed it, the symptom, the root
cause, the fix and the commit. Only real bugs are listed: the bugs planted
on purpose to test the simulator (`-buggy`, `TestingKnobs`) are not.

| # | Where | Summary | Found by |
|---|---|---|---|
| [001](001-quiet-period-disk-faults.md) | simulator | The quiet period didn't stop injected fsync failures | `crashes` seed 4, end-of-run check |
| [002](002-workload-restart-ticks.md) | test workload | A restarted leader kept proposing after the workload should have stopped | `crashes` seed 14, end-of-run check |
| [003](003-leader-completeness-stale-leader.md) | invariant checker | Leader completeness flagged stale leaders that Raft makes no promise about | `partitions` seed 1, leader completeness |
| [004](004-startup-fsync-failure-as-violation.md) | simulator | An injected fsync failure while a node restarted was reported as unrecoverable state | 5-node sweep, seeds 224, 572, 987 |
| [005](005-disruptive-candidate-liveness.md) | Raft (liveness) | A server that couldn't win kept raising its term and deposing the leader; fixed with pre-vote | `storm` seeds 133, 259, 334, 390, end-of-run check |

The first four are in the testing infrastructure, a reminder that the
checker needs testing as much as the system it checks. The fifth is the
first in the Raft implementation itself, and a liveness problem rather
than a safety one: no invariant ever failed, but under heavy faults the
cluster could end a run without a working leader.
