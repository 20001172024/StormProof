# 005: A server that couldn't win kept deposing the leader

- **Where:** Raft (`raft/`): a liveness problem, not a safety one
- **Found:** M9, the first sweep with the new `storm` profile
- **Seeds:** `storm` profile, 3 nodes: 4 of seeds 1-400 (133, 259, 334,
  390), and 33 of seeds 1-2000, while `storm` had 100 ms maximum delays
- **Fixed in:** 605e897 (pre-vote)

## Symptom

```
seed 133 (storm): step 13568 at 15.000000s: final-check: no leader
```

Every safety invariant held. The failures were all the end-of-run check:
after 5 s with no new faults, the cluster must have one leader and
identical, fully applied logs. It didn't: no leader, or one server still
behind.

## Root cause

Replaying seed 133 and listing each server's role changes over the last
5 s showed the pattern:

- S3 restarts after a crash with a log far behind the others' (last entry
  1/1, against 26/69).
- It never hears from a leader in time, so it times out and starts an
  election, again and again: 27 elections in 5 s, its term going from 49
  to 78.
- It can't win: its log is behind, so nobody votes for it (§5.4.1).
- But each RequestVote carries a higher term. Vote stickiness (§4.2.3)
  should make the others ignore it while they hear from a leader. However,
  just after an election the voters don't yet know who won: the first
  AppendEntries takes up to 100 ms to reach them. In that window they adopt
  S3's term, and their next reply deposes the new leader. S1 and S2 elected
  five leaders in those 5 s; none lasted long enough to bring S3 up to date.

The other three seeds were the same: one server started 27 to 44 elections
in the quiet period, and in three of them its log was behind.

This is a known weakness of basic Raft, and the reason the dissertation
adds pre-vote (§9.6). It shows only when messages are slow compared with
the election timeout. Failures by maximum delay, 2,000 seeds each:

| `storm` max delay | failed |
|---|---|
| 100 ms | 33 |
| 50 ms | 3 |
| 30 ms | 0 |

With 100 ms delays a round trip can take 200 ms, longer than the 100-200 ms
election timeout. Raft only promises progress when the broadcast time is
well below the election timeout (§5.6). So part of what `storm` showed at
100 ms is outside what any Raft implementation guarantees. The disruptive
server, though, is a real weakness: it also failed 3 seeds in 2,000 at
50 ms, within the timing requirement.

## Fix

Pre-vote. A server whose timer fires first runs a pre-vote round: it asks
whether the others *would* vote for it in term+1, without changing its own
term or anyone else's. They say yes only if its log is up to date and they
haven't heard from a leader within the minimum election timeout. Only with
a majority of yeses does it start a real election. S3 never gets one, so
its term never moves, the leader is never deposed, and S3 catches up.

`TestPreVoteStopsDisruptiveServer` reproduces the case in a scripted test,
with and without pre-vote.

## Results after the fix

With pre-vote, 2,000 seeds per row:

| Run | before (no pre-vote) | after |
|---|---|---|
| `storm`, 100 ms delays | 33 failed | 0 failed |
| `storm -membership`, 100 ms delays | 160 failed | 4 failed |
| `-profile all`, `-membership`, `-nodes 5`, `storm -nodes 5` | | 0 failed each |

The 4 remaining `storm -membership` failures at 100 ms were the timing
limit, not a disruptive server. For example, in seed 878 the configuration
had 4 voters, one of them just added and still empty. Every election then
needed all three of the others, through 20% message loss, with round trips
as long as the election timeout.

`storm` now keeps delays at 50 ms or less, as `chaos` does: a round trip is
then at most the minimum election timeout. At 50 ms, over 4,000 seeds each:
`storm` 0 failures with pre-vote (4 without), `storm -membership` 0 (20
without). So the nightly `storm` runs still catch pre-vote breaking.
`-bug no-prevote` switches it off to watch the problem happen; the
visualizer's examples include seed 4 of `partitions` both ways.
