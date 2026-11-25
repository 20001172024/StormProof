# 003: Leader completeness flagged stale leaders

- **Where:** invariant checker (`sim/raftsim`)
- **Found:** M3, first run of the new invariants
- **Seed:** `partitions` profile, seed 1, 3 nodes
- **Fixed in:** 4349306 (before the checker was first committed)

## Symptom

```
step 3899 at 5.142214s: leader-completeness: leader 3 of term 1 is missing committed entry 53 {53 2 []}
```

## Root cause

Not a Raft bug. Node 3 was partitioned off while leading term 1 and still
believed it was the leader. Meanwhile the majority elected a leader in term
2 and committed entries. The checker required *every* current leader to hold
every committed entry, but Figure 3 promises less: a committed entry appears
in the logs of leaders of **higher-numbered** terms. A cut-off leader of an
older term doesn't have them, can't commit anything, and steps down as soon
as it hears the newer term.

## Fix

The checker records, for each committed entry, the term of the node that
first reported it as committed, and only requires leaders of a later term
to hold it. That term is never earlier than the term the entry was really
committed in, so the check stays sound.
