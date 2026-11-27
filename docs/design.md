# StormProof design

This document describes how StormProof is put together and, above all, how it
stays **deterministic**: the same simulator seed must always produce the same
run, byte for byte. Everything else in the project (finding bugs, replaying
them, the visualizer) depends on that one property.

Sections marked *(planned)* describe design decisions made up front for code
that later milestones will write. They are updated as that code lands.

## 1. Big picture

```
                 ┌──────────────────────────── node ────────────────────────────┐
 client ──gRPC──▶│ server loop                                                  │
                 │   │  Tick() / Step(msg) / Propose(cmd)                       │
                 │   ▼                                                          │
                 │ ┌──────────┐  Ready{Messages, Entries, Committed, Snapshot}  │
                 │ │ raft     │ ───────────────────────────────────────────┐    │
                 │ │ (pure)   │ ◀── Advance() ──┐                          │    │
                 │ └──────────┘                 │                          ▼    │
                 │                     1. persist Entries/HardState  ─▶ Disk    │
                 │                     2. send Messages              ─▶ Network │
                 │                     3. apply Committed            ─▶ kv      │
                 └──────────────────────────────────────────────────────────────┘
```

The node is split in two:

- **`raft`**: a pure state machine. It decides *what* should happen (become a
  candidate, send this AppendEntries, these entries are now committed) but
  never *does* any of it.
- **The driver** (`server` in real mode, `sim` in simulation): owns the clock,
  the network, the disk and the RNG. It feeds events into `raft`, carries out
  what `raft` asked for, then tells `raft` it's done.

This pattern is borrowed in spirit from etcd's `raft` package. The payoff is
that the same Raft code runs unchanged in production and in the simulator, so
bugs the simulator finds are bugs in the real code path.

## 2. The Raft interface

`raft` exposes only these entry points:

| Call | Meaning |
|---|---|
| `Tick()` | One unit of logical time has passed. Drives election and heartbeat timeouts. |
| `Step(msg)` | A message from a peer (or a local event) has arrived. |
| `Propose(cmd)` | A client wants `cmd` appended to the log (leader only). |
| `Ready() Ready` | Collect everything the node wants done since the last `Advance`. |
| `Advance()` | The driver has finished acting on the last `Ready`. |

A `Ready` batch holds:

- `HardState`: current term and vote, if changed (must be persisted)
- `Entries`: log entries to persist; if the first one's index is already
  stored, the stored log is cut there first (a truncation)
- `Messages`: messages to send to peers
- `CommittedEntries`: entries safe to apply to the state machine
- *(M6)* `Snapshot`: a snapshot to save, if any

Messages are one Go struct, `raft.Message`, with a `Type` and the fields
each RPC uses, rather than generated protobuf types. That keeps `raft` free
of dependencies; the gRPC transport converts at the edge (M7). Randomness
comes in through a one-method `raft.Rand` interface, which `sim.Rand`
satisfies, so `raft` never imports the simulator.

### Ordering rule

The driver must **persist `HardState` and `Entries` before sending
`Messages`**. Raft's safety argument assumes a node never forgets a vote or an
acknowledged entry: if a follower replies "I have entry 7" and then crashes
before writing it, a leader may count it toward a majority that no longer
exists. Doing the fsync first is what makes the reply true. (Raft paper,
Figure 2: "Persistent state on all servers ... updated on stable storage
before responding to RPCs".)

### Why logical ticks instead of durations

`raft` never sees a `time.Duration`. The driver decides how much real time one
tick is (e.g. 10 ms in real mode). Election timeouts are "a random number of
ticks in `[T, 2T)`", with the random number drawn from an RNG the driver
injects. This keeps time out of the state machine entirely, so the simulator
can run thousands of simulated seconds per real second.

## 3. Injected dependencies

Each source of non-determinism comes in from outside the state machine:

| Concern | How it gets in | Real | Simulated (`sim/`) |
|---|---|---|---|
| Time | `Tick()` calls; the node never reads a clock | ticker goroutine (M7) | `sim.Time`, virtual microseconds advanced only by the event loop |
| Network | the node *returns* messages; it never sends them itself | gRPC (`transport`, M7) | the event loop routes them, with drop / delay / duplicate / partition |
| Disk | `storage.FS`, a small flat file-system interface | files on disk (M4) | `sim.Disk`, in memory, with crash, torn-write, fsync-failure and slow-disk faults |
| Randomness | a `*sim.Rand` handed to the node at start | seeded from `crypto/rand` (M7) | streams derived from the run's seed |

In real mode the seed is random and logged, which keeps behaviour realistic.
In simulation the seed is the input.

**Random streams.** The seed is not used to make one random generator but
many independent ones, each named: `network`, `faults`, `clock`,
`disk/<node>`, and `node/<node>/<incarnation>`. Each is PCG from
`math/rand/v2`, seeded with the run seed and a hash of the name. Drawing an
extra number from one stream does not shift any other, so a code change in
one part of the simulator does not silently change every fault decision and
make old seeds stop reproducing old bugs.

**Disk model.** Each simulated file tracks how many of its bytes are
durable. Writes are visible straight away (like the OS page cache), but
only `Sync` makes them durable. Crashes come in two kinds:

- **Process crash** (like `kill -9`): the machine keeps running, so the OS
  still writes out everything; nothing is lost.
- **Machine crash** (power loss): each file keeps its durable bytes plus a
  random-length prefix of the rest. That covers losing all unsynced data,
  keeping all of it, and torn writes in between. The directory also reverts
  to its last `SyncDir`, so a file created, renamed or removed since then
  can be undone (a classic source of real bugs: the data was fsynced, but
  the file's name never was). Truncating a file with `Create` happens in
  place and is not undone.

A failed `Sync` leaves the data non-durable. A process that gets an error
from its disk returns it, and the simulator crashes the node as a machine
crash, because after a failed fsync the OS may already have thrown the dirty
data away (the PostgreSQL "fsyncgate" lesson): the only safe response is to
stop and recover from what is really on disk.

A slow `Sync` delays the messages the node sends in that step, modelling a
node stuck waiting on its disk.

**Clock skew.** Each node's tick interval is the base interval plus or
minus up to `ClockSkew` (for example ±10%), fixed per node. Nodes' timers
really do drift apart, which is the clock failure that matters for election
timeouts and, later, leader leases.

## 4. The simulator event loop

The simulator (`sim.Run`) runs **every node and the network on one
goroutine**. Its core is a priority queue of events, ordered by
`(time, seq)`, where `seq` is the order the event was scheduled in:

| Event | What happens |
|---|---|
| tick | call the node's `Tick`, schedule its next tick |
| deliver | if the link is partitioned or the node is down, drop the message; otherwise call `Step` |
| fault check | every `Faults.Interval`: maybe crash a random running node, maybe start a partition |
| restart | start the node again on its post-crash disk |
| heal | end the current partition |

After every event the loop runs the invariant checks. The first failure is
recorded as a violation, written to the trace, and stops the run.

The `seq` tie-break matters: without it, two events at the same time would
come out in an order decided by the heap's internal layout, which can shift
when unrelated code schedules one more event.

Messages are routed when sent: the network draws whether each is lost or
duplicated and how long each copy takes. Reordering is not injected
directly; it happens because each copy gets its own random delay.
Partitions are checked **at delivery**, so messages already in flight when
a partition starts are cut off too. A partition is a set of blocked one-way
links, which covers isolating one node, splitting a minority from the
majority, and one-way (asymmetric) failures with a single mechanism.

Fault profiles (`sim.Profiles`) bundle these settings: `calm`, `flaky-net`,
`crashes`, `partitions` and `chaos`.

### The toy node

Until Raft exists, the simulator runs `sim/toy`: a replicated counter whose
one property, "a node's counter never goes backwards, even across a crash",
depends on saving state correctly (write a temp file, fsync it, rename it
over the real one). A deliberately buggy variant skips the fsync. The
simulator finds that bug in most seeds of the profiles with machine crashes,
and the failing seed replays to an identical trace. This is a planted test
bug, not a real one, so it is not recorded in `docs/bugs/`.

## 5. Sources of non-determinism, and the rule for each

These are the ways a Go program can quietly behave differently on two runs.
Each one is either forbidden in deterministic code or has a rule that tames it.

| Source | Why it's a problem | Rule |
|---|---|---|
| Wall-clock time (`time.Now`, timers) | Differs every run | `raft` imports no `time`; sim uses the logical clock. Enforced by depguard. |
| Global / unseeded randomness | Differs every run | Only the injected, seeded RNG. `math/rand` and `crypto/rand` banned in `raft`. |
| Goroutines | The scheduler picks the interleaving | No `go` statements in `raft` (`purity_test.go`); sim runs on one goroutine. |
| `select` with several ready cases | Go picks one **at random** on purpose | No `select` in `raft` (`purity_test.go`). |
| **Map iteration order** | Go randomizes it **on purpose**, every iteration | Never let map order leak into behaviour or output. Iterate peers via a sorted slice of IDs; sort keys before looping when the loop draws random numbers or writes output (see `Disk.sortedNames`). No linter catches this, so it relies on review and the determinism tests. |
| Direct I/O (files, sockets) | Timing and failures come from the OS | `raft` imports no `os` / `net`; sim uses in-memory fakes. |
| Pointer addresses (`%p`, printing pointers) | Differ between runs | Never put addresses in traces or logs that feed traces. |
| Unordered heap ties | Order depends on heap internals | Every event carries a `seq` tie-breaker. |

The last line of defence is the **determinism test** (`TestDeterminism`):
run 40 seeds in every profile, each twice, and require byte-identical
traces. It must always pass.

It only covers paths the workload actually exercises. When a map-order bug
was planted in `Disk.Crash` on purpose, `TestDeterminism` did *not* catch
it, because the toy node never has more than one file at crash time. So
components with their own random behaviour also get a determinism test of
their own (`TestDiskCrashIsDeterministic` crashes 30 files and compares).
A map-order bug planted in crash-victim selection, which the toy workload
does exercise, was caught by `TestDeterminism` straight away.

## 6. Invariants checked by the simulator

After every event the simulator checks the Raft safety properties (Raft paper,
Figure 3). All live in `sim/raftsim`. From M2:

- **Election safety:** at most one leader per term. The checker remembers
  the leader of every term for the whole run, so a second leader is caught
  even if the first has crashed in between.
- **Term never decreases** on a node, even across a restart.
- **One vote per term** on a node, even across a restart.

The last two are not in Figure 3. They check the persistence rule directly:
breaking either means a node lost state it had promised to keep.

Liveness can't be checked after every event, because it only has to hold
eventually. Runs instead end with a quiet period (no new injected faults),
after which a final check runs once.

From M3:

- **Log matching:** if two logs have an entry with the same index and term,
  the logs are identical up to that index.
- **Leader completeness:** a committed entry never changes, and is in the
  log of every leader of a later term. ("Later" matters: see
  [bug 003](bugs/003-leader-completeness-stale-leader.md).)
- **State machine safety:** no two nodes apply different commands at the same
  index. Since M6a it also compares every node's whole store, by hash, after
  each applied index, and requires snapshots at the same index to be
  identical (section 12).

The checks skip nodes whose log hasn't changed since the last event (every
log change passes through `Ready.Entries`, so a counter there is enough),
which cut the cost of a run by more than half.

The final check is `Settled`: every node up, one stable leader, and every
log identical, fully committed and applied.

From M5, at the end of each run with clients: the full client history is
checked for **linearizability** with Porcupine (section 11).

## 7. Trace format

A trace is a JSON Lines file: one JSON object per line, in the order the
event loop produced them. The first line is a header recording everything
needed to reproduce the run (seed, cluster size, duration, tick interval
and the full fault profile).

```json
{"type":"header","version":2,"seed":999,"nodes":3,"duration":10000000,"tick_interval":10000,"profile":{"name":"chaos",...}}
{"step":0,"t":0,"type":"start","node":1}
{"step":12,"t":43120,"type":"send","id":31,"from":2,"to":1,"kind":"counter","msg":{"value":3}}
{"step":12,"t":43120,"type":"send","id":32,"from":2,"to":3,"kind":"counter","msg":{"value":3}}
{"step":12,"t":43120,"type":"drop","id":32,"from":2,"to":3,"kind":"counter","reason":"lost"}
{"step":267,"t":700000,"type":"crash","node":2,"kind":"machine","reason":"injected"}
{"step":355,"t":1018522,"type":"state","node":2,"state":{"value":0}}
{"step":355,"t":1018522,"type":"violation","kind":"counter-monotonic","reason":"node 2 counter went back from 22 to 0"}
```

- `step` is the index of the event that produced the record; `t` is virtual
  time in microseconds.
- Record types: `start`, `crash`, `send`, `deliver`, `drop` (with reason
  `lost`, `partition` or `node down`), `state`, `partition`, `heal` and
  `violation`, which is always last.
- Every sent message gets an `id`, repeated on each `deliver` or `drop` of
  it. Messages are delayed by random amounts and sometimes duplicated, so
  without it a delivery couldn't be matched to its send.
- Ticks are not traced on their own (there are a hundred per node per
  virtual second); their effects show up as `state` and `send` records.
- A `state` record is written whenever a node's traced state changes. The
  visualizer can rebuild each node's state at any step from these. For a
  Raft server it is the node's status plus its applied index and its log,
  run-length encoded by term (`"log":[{"from":1,"term":1,"count":40},...]`),
  which is all the visualizer needs to draw the log.

JSON Lines (rather than one big JSON array) means a run that dies still
leaves a readable trace up to that point, and the file can be streamed.
Records are Go structs, never maps, so field order is fixed and traces are
byte-stable. The visualizer (M8) reads this format.

## Known simplifications

- **Torn writes keep a clean prefix.** A machine crash keeps a prefix of
  the unsynced bytes, never garbage after them. Real disks can also leave a
  torn sector with garbage in it; the WAL would report that as corruption
  rather than as a torn tail (see section 10).
- **Seeds reproduce within one Go version.** The random streams come from
  `math/rand/v2`, whose algorithms Go could in principle change between
  releases. Traces recorded under one Go version are not guaranteed to
  replay under another.
- **Disk slowness only delays sends.** A slow fsync delays the messages sent
  in that step, but the node can still process the next event straight
  away.

## 8. Leader election (M2): what the simulator showed

All numbers below are from actual runs with a 10 ms tick, 10 ms heartbeats
and election timeouts drawn from 100–200 ms.

**Safety and liveness.** Over 1,000 seeds across all five profiles, with 15 s
runs ending in a 5 s quiet period, there were no violations of any invariant
or of the final stable-leader check, with either 3 or 5 nodes.

**Time to first leader** in the `calm` profile over 500 seeds: for 3 nodes,
p50 115 ms, p99 172 ms, max 179 ms; for 5 nodes, max 260 ms.

**Why the vote must be persisted.** With the HardState fsync deliberately
skipped (`raftsim.Options.SkipSync`), the simulator reproduces the exact
failure Figure 2's persistence rule prevents. In `crashes` seed 1, node 1
votes for node 3 in term 1, a machine crash loses that unsynced vote, node 1
restarts with no memory of voting, votes for itself in the same term, and
wins: two leaders in term 1. With only election safety checked, this
happened in 128 of 1,000 `crashes` seeds and 114 of 1,000 `chaos` seeds;
the persistence invariants catch the lost state earlier, in almost every
seed with a machine crash. This is a planted bug, so it is not in
`docs/bugs/`; `TestLostVoteCausesTwoLeaders` keeps it as a regression test
for the simulator.

**Term inflation under partitions.** Over 200 seeds of 15 s:

| Profile | final term (p50 / max) | leader elections (p50 / max) |
|---|---|---|
| calm | 1 / 1 | 1 / 1 |
| flaky-net | 1 / 3 | 1 / 2 |
| partitions | 20 / 47 | 5 / 9 |
| chaos | 26 / 50 | 7 / 13 |

Under partitions roughly three terms in four are elections nobody wins. That
fits a cut-off node that keeps timing out and campaigning in ever higher
terms. This is the problem pre-vote (Raft dissertation §9.6) addresses.
These numbers show the inflation; they do not yet show how many needless
leader changes it causes when a partition heals, which is what actually
costs availability. Pre-vote is deferred until that is measured.
(It was measured, and added, in M9: see section 17.)

**Known gap: no check-quorum.** A leader cut off from the majority keeps
believing it is leader until it hears a higher term. This is safe (it can't
commit anything without a majority), but it will matter for serving reads
in M5, where ReadIndex has to confirm leadership first.

## 9. Log replication (M3): what the simulator showed

**Design choices.** The node keeps the log in memory and hands changes out
through `Ready.Entries`. In M3 the simulator adapter rewrote one state
file (HardState plus the whole log) per change, which was slow but obviously
correct; M4 replaced it with the WAL. Leaders send at most 64 entries
per AppendEntries and don't pipeline: until a follower replies, the next
heartbeat resends the same entries, and the receiver skips entries it
already has. Each new leader appends a no-op. Until real clients exist
(M5), the leader itself proposes a unique command on about one tick in ten.

**Safety and liveness.** 1,000 seeds across all five profiles, 15 s runs
ending in a 5 s quiet period: no violations of any invariant or of
`Settled`, with 3 nodes and again with 5.

**Do the invariants catch real Raft bugs?** Each bug below was planted in
the Raft code by hand, one at a time, and run against 300 seeds across all
profiles:

| Planted bug | Seeds failing | Caught by |
|---|---|---|
| Skip the AppendEntries consistency check (rule 2) | 20 / 300 | log matching |
| Truncate matching entries too (rules 3–4) | 138 / 300 | leader completeness (committed entries deleted), Settled |
| No election restriction (§5.4.1) | 119 / 300 | leader completeness |
| Commit earlier-term entries by counting (§5.4.2) | 0 / 300 | — |
| Follower commits past the entries it verified (rule 5) | 0 / 300 | — (unit test `TestFollowerCommitIndex` catches it) |

The second row first crashed the simulator with a panic inside the checker.
Panics are now recorded as violations, and the checker reports "commit
index beyond the end of the log" instead of indexing past it.

**Figure 8 is out of reach of random faults.** With the current-term commit
rule switched off (`TestingKnobs.CommitPreviousTermsByCounting`), 12,000
random runs (3 and 5 nodes; `crashes`, `partitions` and `chaos`; with and
without the no-op) never produced the failure. It needs five unlikely
events in a row, in the right order. `TestFigure8` scripts it step by step
instead: with the knob, node 1 commits an entry from term 2 and node 5 then
overwrites it on a majority; with the real rule, the entry is never
committed. Random search finding shallow bugs well and deep multi-step ones
poorly is a known limit of this kind of testing, and a reason to add
targeted fault scenarios later.

**Why the no-op.** With `NoNoopOnElection`, 7 of 1,000 `chaos` seeds ended
with entries from earlier terms still uncommitted after the quiet period:
with proposals stopped, no current-term entry came along to commit them.
With the no-op, 0 of 1,000.

## 10. Persistence (M4): the write-ahead log

**Format.** `storage/wal` writes records of `length u32 | CRC-32C u32 |
type | payload`. Each `raft.Ready` becomes at most two records (HardState,
Entries) followed by a single fsync, which is the durability point. Records
are only appended: an Entries record whose first index is already in the
log replaces everything from that index on, which is how a truncation is
recorded. That is the `Ready.Entries` contract from M3, so there is no
"truncate" record. The log is split into segment files `wal-<seq>.log`; a
new segment's directory entry is fsynced before anything is written to it.
Old segments are deleted by compaction after a snapshot (section 12).

**Recovery.** Records are replayed in order. A record cut short at the end
of the **last** segment is a torn tail: a crash during an append, whose
fsync never completed, so nothing was acknowledged on the strength of it.
It is cut off by atomically rewriting the segment without it. Anything
else, such as a bad checksum anywhere, a cut-short record in an earlier
segment, a missing segment or an entry that leaves a gap, is corruption,
and the node refuses to start rather than guess.

The trade-off: a complete record with a bad checksum at the very end of the
log is treated as corruption, not as a torn tail. That is the strict
choice. It can refuse to start after a torn sector that left garbage behind
(which the simulated disk doesn't model), but it never silently drops a
record that was acknowledged and later damaged.

**fsync policy.** `SyncAlways` (the default) fsyncs every Save.
`SyncNever` exists only to measure what the fsyncs cost (M10); a machine
crash can then lose acknowledged state, which can break Raft's guarantees.
`TestSyncNeverCanLoseData` checks that it really does.

**The real file system.** `storage.OSFS` implements the same `storage.FS`
interface on a directory, with `SyncDir` fsyncing the directory itself.

**Testing the WAL itself.**

- Cutting the log at **every byte offset** must recover exactly the state
  of the last complete record, and leave a WAL that keeps working.
- Each kind of corruption is refused.
- 200 machine crashes at random points with random tear lengths never lose
  an acknowledged Save, across segment rotations.
- Two fuzz targets: the record decoder (any accepted record re-encodes to
  the same bytes) and `Open` on arbitrary segment contents (never panics).
  Each ran for 45 s locally with no failures; CI runs their seed corpora as
  ordinary tests.
- Five bugs planted in the WAL by hand were all caught by the unit tests.
  The first attempt at "skip the checksum check" seemed to survive, but
  only because the mutation stopped the package compiling.

**What the simulator showed.** The simulator's WAL segments are 1 KiB. At
the original 16 KiB, a 15 s run never rotated a segment, so rotation went
untested; at 1 KiB every run rotates several times. With the WAL, 1,000
seeds across all profiles had no violations, with 3 nodes and again with 5.
Bugs planted in the WAL, 300 seeds each (only the 120 seeds on profiles
with machine crashes can expose them):

| Planted WAL bug | Seeds failing | How it showed up |
|---|---|---|
| Never fsync | 119 | a node refuses to start (torn record mid-log), or forgets its term |
| No directory sync for a new segment | 120 | term goes back to 0: the first segment vanished |
| Don't cut off a torn tail | 36 | the next recovery finds garbage mid-log |
| Treat a torn tail as corruption | 46 | a node refuses to start after an ordinary crash |

## 11. The KV store and linearizability (M5)

**The state machine** (`kv/`) applies `Put`, `Delete` and `CompareAndSwap`
from the log and answers `Get`. Commands have a fixed binary encoding,
which is fuzzed.

**Exactly-once writes** (dissertation §6.3). Each client numbers its
requests and reuses the number when it retries. The store keeps, per client,
the last request it applied and its result; a retry of an applied request
gets that result back instead of being applied again. Without this, a
retried CompareAndSwap that had succeeded runs again, fails, and reports
failure for a swap that happened. Sessions are never expired, which is a
known leak: expiry has to go through the log too, so every node expires the
same ones.

**Linearizable reads with ReadIndex** (dissertation §6.4). A Get is not put
in the log. The leader:

1. refuses if it hasn't yet committed an entry in its own term (its commit
   index might be behind what an earlier leader committed; the no-op clears
   this within a round trip);
2. records its commit index as the read index;
3. sends a heartbeat tagged with a read sequence number and waits for a
   majority to answer it, which proves nobody has replaced it as leader
   since the read began (responses to older heartbeats don't count);
4. waits until its state machine has applied up to the read index, then
   answers from it.

Leader leases would skip step 3 by trusting that no new leader can be
elected before the old one's lease runs out. That saves a round trip per
read but makes correctness depend on bounded clock drift, which is exactly
what the simulator's clock skew attacks. Not implemented.

**Simulated clients.** Six clients, separate from the servers, share the
faulty network. Each has one request at a time on one of two keys (40% Get,
25% Put, 25% CAS, 10% Delete; written values are unique). It follows
"not leader" hints, resends a request that has had no answer for 300 ms to
the next server with the same request ID, and records the call and return
time of every operation in virtual time. A server answers a write once it
has applied it; if a different entry committed at that index, or it lost
leadership first, it tells the client to retry, and deduplication makes the
retry safe.

**Checking.** At the end of every run, Porcupine checks the combined
history against a per-key model of the store. An operation that never got
an answer is placed after every other one, which covers both "it took
effect" and "it didn't". The history is split by key, which keeps the
check fast: a whole 15 s run with six clients, check included, took about
47 ms (one run, measured on the development container).

**Results.** 1,000 seeds across all profiles, with 3 nodes and again with 5:
no violations of any invariant, of `Settled`, or of linearizability. That
was after fixing [bug 004](bugs/004-startup-fsync-failure-as-violation.md),
which the first 5-node sweep found. Bugs planted to check the checker,
300 seeds across the five profiles (60 per profile):

| Planted bug | Seeds failing | Notes |
|---|---|---|
| No deduplication | 138 / 300 | 60/60 `flaky-net`, 58/60 `chaos`: lost replies cause retries |
| Reads from local state, no ReadIndex | 13 / 300 | mostly `chaos` |
| ReadIndex without the leadership check | 8 / 300 | `chaos` and `partitions` |
| Skip fsync (as before) | 120 / 300 | every seed on a profile with machine crashes |

**Stale reads are hard to provoke at random.** The classic case is a leader
cut off by a partition that keeps serving reads after a new leader has
committed a write. For it to show, one client must complete a write through
the new leader and then another must read the same key from the old one.
But a client talking to the old leader moves away at its first write,
which times out, so the window is small. With five keys and three clients
the leadership-check bug was caught in 0 of 200 seeds (50 in each of the
four fault profiles). Two changes were
tried: letting partitions cut off clients too made no measurable difference
and was dropped; six clients on two keys (more overlap on the same key)
raised it to the numbers above and became the default.
`TestReadIndexDeposedLeader` scripts the scenario step by step, as
`TestFigure8` does, so the leadership check is covered deterministically.

## 12. Snapshots and log compaction (M6a)

**The rules (Raft paper §7, Figure 13).** Each server snapshots on its own:
the state machine's state after its last applied entry, plus that entry's
index and term, after which the entries it covers are discarded. A
follower that needs entries the leader has discarded gets the snapshot
instead (InstallSnapshot). The receiver rejects an old term; if its log
has the snapshot's last entry (same index and term) it keeps what follows,
otherwise it throws the whole log away; either way its state machine is
reset from the snapshot.

**In the Raft node.** The log keeps the snapshot's last index and term, so
the AppendEntries consistency check works for the first entry after it.
`Compact(index, data)` discards entries the caller has applied and saved a
snapshot of. A leader sends InstallSnapshot when a follower's `nextIndex`
falls at or below its snapshot, resending it with each heartbeat until the
follower answers, like AppendEntries. Two cases needed care:

- A snapshot that arrives late, after the follower has already caught up
  through the log, is ignored if its index is at or below the commit index.
  Installing it would throw away newer entries.
- AppendEntries that reach back into a follower's snapshot: the covered
  part is committed, so it is skipped and the rest checked against the
  snapshot's last entry.

InstallSnapshot sends the whole snapshot in one message rather than in
chunks. That is fine for the simulator's tiny state; a real deployment with
large state would need chunking.

**The KV snapshot** holds the data and the client sessions, written in
sorted order so equal states give identical bytes. Without the sessions, a
node restored from a snapshot would forget which requests it had applied
and could apply a retried write twice. `Restore` accepts only that one
canonical layout: fuzzing found it silently merging duplicate keys.

**Storage.** A snapshot goes to its own file, `snap-<index>.snap` (CRC,
written to a temp file, fsynced, renamed, directory fsynced). Then the WAL
is compacted: a new segment holding a snapshot record, the HardState and
the entries after the snapshot is written under a temp name, fsynced,
renamed into place, the directory fsynced, and only then are older
segments deleted. Recovery starts from the newest segment that begins with
a snapshot record and loads the snapshot file it names; anything older is
ignored, so a crash at any point leaves either the old state or the new.
`TestCrashAtEveryStepOfCompact` crashes the disk at every single file
system step inside `Compact` to check that; it caught a deliberately
reversed step order that a test crashing only after `Compact` returned
had missed.

**In the simulator** a node snapshots every 50 applied entries, so in a
15 s run snapshots, compaction and InstallSnapshot happen many times: in
100 runs (20 per profile) the nodes ended up holding hundreds of snapshots
between them, and InstallSnapshot was delivered thousands of times, most
under `partitions`.

**A stronger state machine safety check.** Planting a snapshot that drops
the client sessions went unnoticed in 30 `flaky-net` seeds: the divergence
it causes is only visible to clients if a retry happens at just the wrong
moment, and snapshot files are only comparable if two nodes snapshot at
the same index. So each node now also records a hash of its whole store
after every applied index (and after loading a snapshot), and state
machine safety requires every node's hash at the same index to match.

**Results.** 1,000 seeds across all profiles, with 3 nodes and again with 5:
no violations of any invariant, `Settled` or linearizability (52 s and
1 min 53 s; faster than before snapshots, since logs stay short). Bugs
planted to check the checks, 300 seeds each:

| Planted bug | Seeds failing | Caught by |
|---|---|---|
| Snapshot leaves out client sessions | 82 | state machine safety (store hashes) |
| Never send InstallSnapshot | 158 | mostly `Settled`: the cluster can't catch followers up |
| WAL: delete old segments before the new one is in place | 5 | term goes backwards after a crash in that window |
| Install a snapshot older than the commit index | 2 | end-of-run checks |
| Always discard the log on InstallSnapshot (ignore rule 6) | 1 | end-of-run checks |
| Don't skip AppendEntries entries covered by a snapshot | 0 | — |

The last four are subtle, and random faults rarely line up to show them:
they need a crash in a narrow window, or a snapshot to cross with log
entries in flight. Each one is caught by a unit test instead
(`TestCrashAtEveryStepOfCompact`, `TestHandleSnapshot`,
`TestAppendEntriesOverlappingSnapshot`), which is why both kinds of test
exist.

## 13. Membership changes (M6b)

**The rules (Raft dissertation §4, one server at a time).** A configuration
is the set of voters. It changes through log entries holding the complete
new voter list, and each server uses the newest configuration in its log
as soon as the entry is appended, committed or not; if the entry is
truncated away, the previous one comes back. Only one server is added or
removed per change, and a leader starts no change while an earlier one is
uncommitted: then any majority of the old configuration overlaps any
majority of the new one, so two leaders can't win the same term.

Three refinements matter in practice:

- **The 2015 fix.** A leader must commit an entry in its own term before it
  starts a change. Without it the single-server algorithm can still elect
  two leaders (found by Ongaro in 2015, after the dissertation). The no-op
  every leader appends on election makes this cheap.
- **Removing the leader** (§4.2.2): it keeps leading, without counting
  itself towards majorities, until the change commits, then steps down.
- **Disruptive servers** (§4.2.3): a removed server is never told it was
  removed (the leader stops sending to it), so it keeps timing out and
  starting elections in ever higher terms. A server that has heard from a
  current leader within the minimum election timeout therefore ignores
  RequestVote entirely, without even adopting the candidate's term. A
  leader counts itself as a current leader, so a higher-term RequestVote no
  longer deposes it; a leader cut off from the majority is still replaced,
  because the rest of the cluster stops hearing from it.

Majorities, votes and ReadIndex acknowledgements count only voters; a
server outside the configuration never starts an election. Snapshots carry
the configuration at their index, and the WAL records each entry's type.

**Simplifications.** New servers join as full voters immediately, without
the catch-up rounds of §4.2.1: that is safe, but a slow new server can stall
commits until it catches up. A removed server that is later added back may
have raised its term while it was out; its first reply then makes the leader
step down and costs an election. Pre-vote (dissertation §9.6) would fix both
that and the term inflation measured in section 8; it is still not
implemented.

**In the simulator** (`-membership`), there are five server slots: the
first three form the initial configuration, the other two start with none.
An admin client adds and removes servers about once a second, keeping at
least three voters, following leader hints and retrying like the KV
clients, while the usual faults and client workload run. End-of-run checks
consider only servers in the leader's current configuration. A new
invariant checks that consecutive configurations in any log differ by
exactly one server.

**Results.** In 200 runs (40 per profile) the admin had about 1,950 changes
acknowledged, roughly ten per run. 1,000 seeds across all profiles with
membership changes: no violations (2 min 59 s). Fixed membership, also
1,000 seeds each: no violations with 3 nodes (1 min 28 s) or 5 (3 min
35 s). Those are slower than the M6a sweeps (52 s and 1 min 53 s); the
cause has not been investigated yet.

Bugs planted in the membership rules, 200 seeds each with membership
changes:

| Planted bug | Seeds failing |
|---|---|
| Change before committing in own term (the 2015 bug) | 0 |
| Two changes in flight at once | 0 |
| No vote stickiness (§4.2.3) | 10, all end-of-run liveness failures |

The first two are out of reach of random testing for different reasons.
The 2015 bug needs a precise five-step sequence, like Figure 8. Concurrent
changes can't even happen with this admin, which waits for each change to
commit before starting the next, so the leader's check is never exercised.
Both are scripted as deterministic tests instead (`TestOngaro2015Bug`,
`TestConcurrentConfChanges`): without the fixes, a committed configuration
entry is overwritten, and two majorities commit different entries at the
same index. All seven membership rules were also checked by planting each
bug and confirming a unit test fails.

## 14. Real mode (M7a)

**One code path for both modes.** The glue between the pure Raft node and
the world (persist via the WAL and snapshot files, release messages, apply
committed entries, answer clients, take and install snapshots) lives in
`server/host`. The simulator wraps a `host.Host` in a `sim.Process`; the
real server wraps the same `host.Host` with goroutines, gRPC and real
files. Everything the simulator found and checked applies to the code that
runs for real. Moving the code out of `sim/raftsim` changed no behaviour:
the 1,000-seed sweeps (fixed and changing membership) still had no
failures and every planted bug was still caught at about the same rate.
A back-to-back timing found the move about 10% slower (40-43 s against
37 s for 300 seeds), plausibly from building output slices per call.

**Concurrency.** The Host is as single-threaded in production as in the
simulator: one goroutine, `Server.run`, owns it and is the only one that
touches it. A `select` loop takes one event at a time from a ticker,
incoming Raft messages (from the RaftService) and client calls (from the KV
service), calls the Host, then hands Raft messages to the transport and
responses to the client calls waiting for them. gRPC handlers never touch
Raft state; they send a request into the loop on a channel and wait on a
reply channel, which is buffered so the loop never blocks delivering.

**Transport.** `raft.proto` mirrors `raft.Message` field for field. Each
peer has a bounded queue and a sender goroutine that sends in batches; when
a queue is full the message is dropped and counted. Raft already copes
with lost messages, and this way a dead or slow peer can never hold up the
leader or the other peers. Incoming messages go into the loop's inbox, also
dropped if it is full.

**Timing.** A tick is 50 ms by default, heartbeats every tick and election
timeouts 10-20 ticks (0.5-1 s). The simulator's 10 ms tick works in virtual
time; over a real network, the heartbeat must stay well below the election
timeout even with jitter and GC pauses, or followers start needless
elections. (etcd's defaults are in the same range: 100 ms heartbeat, 1 s
election timeout.)

**Client protocol.** A non-leader answers `FailedPrecondition` with the
leader's address in the `stormproof-leader-addr` trailer; the client goes
straight there. An unknown outcome (the leader lost leadership before the
entry committed, or a timeout) is `Unavailable`, and the client retries
with the same request ID, which deduplication makes safe. The Go client
runs one operation at a time: the server keeps only the newest request per
client, so with two in flight an older one applied later would be taken for
a duplicate. Concurrent callers should use separate clients.

**What was checked, on real processes** (localhost, real gRPC, files and
time; tests use 10 ms ticks):

- `server` tests run three servers in one process: all operations, leader
  failover (first write after stopping the leader succeeded 160-320 ms
  later across runs), a full stop and restart recovered from disk
  including a snapshot, and four concurrent clients across a leader stop and
  restart whose history of 1,973 operations Porcupine found linearizable.
  The suite passed six times in a row with the race detector, which on the
  first run caught a data race in `Server.Start` (an unlocked read of the
  status the loop updates).
- By hand, with three `stormproof-server` processes and `stormproof-cli`
  (50 ms ticks): every CLI command worked; after `kill -9` of the leader the
  next write succeeded 526 ms later; the restarted server recovered from
  disk and served the write made while it was down.

**Not yet in real mode.** Membership changes (they need a way to learn a new
server's address; planned with Kubernetes in M11); TLS or authentication.

## 15. Metrics and containers (M7b)

**Metrics.** `stormproof-server -metrics :9090` serves Prometheus metrics
at `/metrics` from a per-server registry (so several servers can share one
process, as in the tests):

| Metric | Kind | Meaning |
|---|---|---|
| `stormproof_raft_term`, `_role`, `_is_leader` | gauge | current term; 0 follower, 1 candidate, 2 leader |
| `stormproof_raft_commit_index`, `_applied_index`, `_apply_lag` | gauge | commit and applied index, and their difference |
| `stormproof_raft_last_index`, `_snapshot_index`, `_voters` | gauge | log end, last snapshot, configuration size |
| `stormproof_raft_elections_started_total` | counter | times this server became a candidate |
| `stormproof_raft_leader_changes_total` | counter | times the leader it knows of changed |
| `stormproof_raft_messages_dropped_total` | counter | Raft messages dropped at a full send queue |
| `stormproof_client_request_duration_seconds` | histogram | client requests, by op and gRPC code |
| `stormproof_raft_send_duration_seconds` | histogram | Send RPCs to each peer, by peer and result |

The event loop sets the gauges after every event, from the node's status,
so they never race with Raft and cost nothing between scrapes. Two probes
go with them: `/healthz` is 200 while the event loop runs (503 after the
server stopped itself, for example on a storage failure); `/readyz` is 200
once the server knows a leader.

**Image.** A multi-stage Dockerfile builds static binaries and copies them
onto alpine (44.7 MB); alpine rather than distroless because the compose
health check needs `wget` to call `/readyz`. Behind a TLS-intercepting
proxy (as in the development container) the build takes the proxy's CA as
an optional build secret, used only for downloading modules and never
stored in the image.

**docker-compose.** `deploy/docker/docker-compose.yml` (3 servers) and
`docker-compose.5.yml` (5) each run Prometheus too, scraping every server.
The CLI runs as a compose service, because servers point clients at the
leader by its address inside the compose network. `deploy/docker/smoke.sh`
starts a cluster, writes and reads, kills the leader's container, writes
again, restarts it, and checks it caught up. It also runs in CI, which
pushes the image to the GitHub Container Registry on merges to main.

Run in the development container: both clusters passed; the first write
after killing the leader took 586 ms (3 servers) and 513 ms (5), counting
the time to start the CLI container; Prometheus scraped all five targets of
the 5-server cluster, with exactly one reporting `is_leader`.

## 16. The visualizer (M8)

`visualizer/` is a static React + TypeScript site (built with Vite) that
loads a trace and lets you scrub through it. There is no backend: a trace
is read from a file you drop on the page, or fetched from the example
traces shipped with the site. GitHub Pages publishes it on every merge to
main that touches it.

**What the trace had to gain.** Two things were missing for drawing a run,
both added on the Go side (trace version 2, section 7):

- *Message IDs.* A `deliver` record named only sender, receiver and kind.
  With random delays and duplication, two RequestVotes from S1 to S2 can
  arrive in either order, so a delivery couldn't be tied to its send (and
  to its contents, or its send time for animating it). Each send now gets
  an ID, repeated on every delivery or drop of it.
- *Each server's log.* State records had the log's last index and term,
  not the entries' terms, which are what show logs diverging and
  converging. The full log is long and mostly runs of one term, so it is
  run-length encoded: `[{"from":1,"term":1,"count":40},...]`.

**Rebuilding a moment.** The trace only records changes, so the cluster at
step *f* is every record up to *f* applied in order. Replaying from the
start on every scrub would be slow on long traces, so the replayer keeps a
checkpoint every 256 steps and replays forward from the nearest one; a
test checks it agrees with a replay from the start on every example trace.
Messages on the wire are found without a scan of every message: a copy is
in flight from its send to its arrival, and no copy is in flight longer
than the longest delay seen, so only sends that recent are looked at.
While playing, virtual time advances smoothly and messages are drawn
partway along their link (sent at *t₀*, arriving at *t₁*, drawn at
(*t* − *t₀*)/(*t₁* − *t₀*)), so they glide rather than jump between steps.

**Views.** The cluster as a network (role colors, crashed servers dashed,
non-members faded, cut links as red dashed arrows, one per blocked
direction so one-way partitions are visible as such, drops flashing as red
crosses); a card per server (term, vote, leader, commit and applied index,
snapshot, configuration, and the whole log drawn to a common scale); the
last 18 log indexes of every server side by side, each entry labelled with
its term and dashed until committed; the event list around the current
step; and a timeline marking new leaders, configuration changes, crashes,
restarts, partitions, the quiet period and the violation, with a button
(and the V key) to jump to the violation.

**Example traces.** `visualizer/scripts/make-examples.sh` regenerates them
from the simulator: a leader crashing twice, a leader cut off by a
partition (two leaders in different terms until the heal), an isolated
follower disrupting the leader when it rejoins (the problem pre-vote
solves), membership changes, and the planted fsync bug ending in its
term-monotonic violation. The descriptions on the site were checked
against the traces.

Picking them turned up a pitfall in the simulator's quiet period rather
than in Raft: the quiet period stops new faults but doesn't heal a
partition already in progress. With a quiet period shorter than the
longest partition plus an election (here, 1 s against up to 2 s), a
partition started just before it can outlast it, and the final check
reports "no leader". The defaults (5 s quiet, partitions of at most 2 s)
leave enough time, and the examples use 2.5 s.

## 17. CI at scale and pre-vote (M9)

**Running seeds in parallel.** Seeds are independent, and each simulation
runs on one goroutine with all its randomness drawn from its own seed. So
`-parallel N` (default: one worker per CPU) runs N seeds at once without
changing any of them, and prints results in seed order, so the output is
identical for any N; a test checks exactly that. The first measurement
scaled poorly: 4 workers did only 1.9× the seeds of 1. The cause was the
garbage collector, not contention between seeds: a simulation's live heap
is a few MB but it allocates quickly, so at Go's default `GOGC=100` the
collector ran almost constantly and the workers waited on it. The CLI
now sets `GOGC=400` unless the environment says otherwise: 4 workers then
run 3.5× as fast as 1 (577 to 2,161 seeds/min on 4 CPUs, `-profile all`),
for about 10 MB more peak memory (14 MB to 25 MB).

**Per push and nightly.** CI runs 1,000 seeds of every profile on each
push, once with a fixed cluster and once with servers added and removed.
`nightly.yml` runs five jobs of 10,000 seeds each:

| Job | Flags |
|---|---|
| all-profiles | `-profile all` |
| five-servers | `-profile all -nodes 5` |
| membership | `-profile all -membership` |
| storm | `-profile storm` |
| storm-membership | `-profile storm -membership` |

Each night starts at seed (days since 1970) × 10,000 + 1, so every night
explores seeds no earlier night ran, and a failing seed's number says which
night found it. Failing seeds' traces are kept as artifacts for 30 days,
and each job's summary lists failures with the command that replays them.
Before the first nightly run, 2,000 seeds of each job, on the first
night's range (207,840,001 onwards), ran here with no failures.

**The `storm` profile** is harsher than `chaos`: 20% message loss and 10%
duplication, crashes and partitions twice as often, outages up to 2 s and
partitions up to 3 s, fsync failures five times as often and 20% clock
skew. It runs only at night, so the per-push sweep keeps its size. Its
delays stay at most 50 ms, like `chaos`: Raft only promises progress when
messages are fast compared with the election timeout (§5.6), and above
that the end-of-run "settled" check measures the network, not the code.

**Pre-vote.** The first `storm` sweeps found a liveness problem in Raft
(docs/bugs/005): a server that could never win an election, because its
log was behind, kept timing out and raising its term, and each higher term
deposed a working leader. Section 8 had predicted this kind of term
inflation and deferred pre-vote until its cost was measured. This is that
measurement: with 100 ms delays, 33 of 2,000 `storm` runs ended without a
leader. Pre-vote (dissertation §9.6) fixed it.

How it works: a follower whose election timer fires becomes a
*pre-candidate* and sends PreVote requests carrying the term it would
campaign in (its own plus one). Nothing changes on either side: no term is
adopted, no vote recorded, no timer reset. A server answers yes only if
the term would be newer than its own, the candidate's log is at least as
up-to-date (§5.4.1), and it hasn't heard from a leader within the minimum
election timeout (§4.2.3). With a majority of yeses, the pre-candidate
starts a real election. A no carries the responder's real term, so a
pre-candidate that is behind learns the current term and becomes a
follower. The one subtlety is in `Step`: Figure 2's rule "if a message
carries a higher term, adopt it" must skip PreVotes and granted
PreVoteResponses, whose term is hypothetical.

Pre-vote costs one extra round trip per election, which slows elections
when messages are slow. With 100 ms delays and changing membership, 4 of
2,000 `storm` runs still failed with pre-vote (160 without). Those were
the timing limit, not a disruptive server: for example, a configuration
of four voters with one still empty needed all three others to agree,
through 20% loss, with round trips as long as the election timeout.

Section 8 asked what term inflation costs in leader changes. Measured over
200 seeds of 15 s runs (3 servers, KV clients), counting each server's
final term and how many distinct leaders (server and term) were elected:

| Profile | final term, p50 / max | leaders elected, p50 / max |
|---|---|---|
| partitions, without pre-vote | 23 / 60 | 5 / 8 |
| partitions, with pre-vote | 3 / 7 | 3 / 7 |
| chaos, without pre-vote | 31 / 59 | 7 / 11 |
| chaos, with pre-vote | 7 / 14 | 5 / 10 |

Pre-vote cuts the median term by a factor of 4 to 8. It also cuts the
median number of leaders a run goes through, from 5 to 3 under partitions
and from 7 to 5 under chaos: those are leader changes, each a gap in
availability, caused only by servers that couldn't win. (Without pre-vote
the figures are close to section 8's, which were measured on M2's code.)

## 18. Performance (M10): what benchmarking found

The load generator (`bench/`) and the measurements are described in
[benchmarks.md](benchmarks.md). Four problems turned up; none affected
correctness, and the simulator sweeps passed before and after each fix.

**1. The server kept the simulator's history.** `server/host` kept every
applied entry, every snapshot, and a hash of the whole key-value store
after every applied entry, because the simulator's state machine safety
invariant reads them. The real server did too: its memory grew forever,
and hashing the store on every write took 20% of CPU in a profile. Now
`host.Config.KeepHistory` turns it on, and only the simulator sets it.

**2. Replication resent everything, all the time.** The first benchmarks
showed write throughput falling as clients were added: one client got 582
writes/s, eight got 330. A CPU profile pointed at messages, and the code
said why. A leader sent AppendEntries from `nextIndex` after every
proposal, and again after every acknowledgement that found more to send.
Until a follower acknowledged, each of those resent every unacknowledged
entry. With proposals arriving all the time, each acknowledgement set off
another send, so chains of send, acknowledge, send piled up faster than
they finished. An experiment that removed the resend on acknowledgement
gave 3.3 times the throughput at 4 clients; but that resend is what lets a
lagging follower catch up between heartbeats, so the fix had to keep its
effect without the pile-up.

Figure 2 says what an AppendEntries carries, not when to send one. When
to send only affects speed: every message passes the consistency check on
arrival, so no sending policy can break safety. The dissertation (§10.2.1)
suggests batching entries and pipelining messages. StormProof does the
simpler half: **one outstanding batch per follower**.

- A leader tracks, per follower, whether a batch of entries is outstanding
  (`inflight`) and the last index it carried (`sentUpTo`).
- A proposal sends only to followers with no batch outstanding. For the
  others the entry waits in the log.
- The batch is settled when the follower acknowledges up to `sentUpTo`, or
  rejects it. Then everything that piled up goes in one message, so under
  load batches grow by themselves.
- Heartbeats still send from `nextIndex` regardless, so a lost batch is
  resent within one heartbeat interval.
- ReadIndex used to call the same broadcast, resending every
  unacknowledged entry for every read. It now sends an AppendEntries with
  no entries: just the consistency check at `nextIndex - 1`, whose
  acknowledgement (or rejection) is the confirmation it needs.

Acknowledgements of those empty messages carry `MatchIndex = nextIndex - 1`,
below `sentUpTo`, so they don't settle a batch by mistake;
`TestReadCheckCarriesNoEntries` covers this. Full pipelining (several
batches in flight, `nextIndex` advanced optimistically) would add one more
state per follower and a rule for going back to probing after a loss; one
batch was enough to remove the collapse.

**3. One fsync per event.** The server's event loop gave the Host one
event at a time, so each client request and each Raft message cost a WAL
write and an fsync. It now takes every event already queued (up to 512)
and hands them over together (`host.Process`): Raft steps through all of
them, then one Ready persists everything once and sends the messages.
Raft allows any number of steps between Readys; the rule is only that
state is persisted before the messages depending on it are sent, and the
single Ready does that for all of them. The simulator's `-batch` holds
each delivered event until the node's next one, so these multi-step
Readys meet every injected fault; per-push CI runs 1,000 such seeds.

**4. A full-log scan on every append.** Followers recomputed the
configuration after appending entries by scanning back from the end of the
log for a configuration entry. With none in the log, that is the whole
log, every time: a micro-benchmark round took 4.8 us at 1,000 entries and
80 us at 50,000. Only the appended entries can change the configuration
when nothing was truncated, so only they are checked now (a truncation,
which can undo a change, still recomputes from the whole log);
`TestConfigAppendedMatchesFullScan` checks the two agree.

Effect on the 3-server cluster, write-heavy, 32 clients: 656 ops/s with a
median Put of 47 ms before, 5,735 ops/s at 5.2 ms after. The full tables
are in benchmarks.md.

## 19. Deployment (M11, first part)

The real cluster runs on Kubernetes; the details are in
`deploy/k8s/README.md` and `docs/aws.md`. The choices that follow from
Raft:

- **A StatefulSet, not a Deployment.** A Raft server's identity and log
  must survive restarts: if server 2 came back with an empty log under its
  old ID, it could vote again in a term it already voted in. A StatefulSet
  gives each pod a stable name (stormproof-0..2), a stable DNS name and its
  own volume, reattached wherever the pod is rescheduled. The server takes
  its Raft ID from the pod's ordinal (`-id-from-hostname`), so the pods
  share one spec.
- **All pods start at once** (`podManagementPolicy: Parallel`). The default
  starts pod N+1 only once pod N is ready, but a server is only ready
  (`/readyz`) once it knows a leader, which needs a majority up: one at a
  time would never get past the first.
- **Peers find each other before they are ready.** The headless Service
  publishes not-ready pods (`publishNotReadyAddresses`), for the same
  reason. Readiness still matters for clients: the client Service only
  routes to servers that know a leader, so a partitioned server drops out.
- **A PodDisruptionBudget of minAvailable 2** stops node drains and
  upgrades from taking down a majority at once.
- **Spread over machines.** A preferred anti-affinity rule puts the three
  servers on different nodes, so losing one machine costs one server.

On AWS the cluster is three t3.small instances running k3s, provisioned by
Terraform: cheap (about $0.09 an hour), and k3s enforces NetworkPolicy, so
the chaos demo can partition a server for real. The Kubernetes API isn't
exposed; it is reached through an SSH tunnel, and SSH is open only to one
address.
