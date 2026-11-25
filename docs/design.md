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

## 2. The Raft interface *(planned, M2–M3)*

`raft` exposes only these entry points:

| Call | Meaning |
|---|---|
| `Tick()` | One unit of logical time has passed. Drives election and heartbeat timeouts. |
| `Step(msg)` | A message from a peer (or a local event) has arrived. |
| `Propose(cmd)` | A client wants `cmd` appended to the log (leader only). |
| `Ready() Ready` | Collect everything the node wants done since the last `Advance`. |
| `Advance()` | The driver has finished acting on the last `Ready`. |

A `Ready` batch holds:

- `HardState`: current term, vote, commit index, if changed (must be persisted)
- `Entries`: new log entries to append to stable storage
- `Messages`: messages to send to peers
- `CommittedEntries`: entries safe to apply to the state machine
- `Snapshot`: a snapshot to save, if any (M6)

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
  keeping all of it, and torn writes in between.

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

## 6. Invariants checked by the simulator *(planned, M2–M5)*

After every event the simulator checks the Raft safety properties (Raft paper,
Figure 3):

- **Election safety:** at most one leader per term.
- **Log matching:** if two logs have an entry with the same index and term,
  the logs are identical up to that index.
- **Leader completeness:** a committed entry is present in the log of every
  leader of a later term.
- **State machine safety:** no two nodes apply different commands at the same
  index.

At the end of each run, the full client history is checked for
**linearizability** with Porcupine (M5).

## 7. Trace format

A trace is a JSON Lines file: one JSON object per line, in the order the
event loop produced them. The first line is a header recording everything
needed to reproduce the run (seed, cluster size, duration, tick interval
and the full fault profile).

```json
{"type":"header","version":1,"seed":999,"nodes":3,"duration":10000000,"tick_interval":10000,"profile":{"name":"chaos",...}}
{"step":0,"t":0,"type":"start","node":1}
{"step":12,"t":43120,"type":"send","from":2,"to":1,"kind":"counter","msg":{"value":3}}
{"step":12,"t":43120,"type":"drop","from":2,"to":3,"kind":"counter","reason":"lost"}
{"step":267,"t":700000,"type":"crash","node":2,"kind":"machine","reason":"injected"}
{"step":355,"t":1018522,"type":"state","node":2,"state":{"value":0}}
{"step":355,"t":1018522,"type":"violation","kind":"counter-monotonic","reason":"node 2 counter went back from 22 to 0"}
```

- `step` is the index of the event that produced the record; `t` is virtual
  time in microseconds.
- Record types: `start`, `crash`, `send`, `deliver`, `drop` (with reason
  `lost`, `partition` or `node down`), `state`, `partition`, `heal` and
  `violation`, which is always last.
- Ticks are not traced on their own (there are a hundred per node per
  virtual second); their effects show up as `state` and `send` records.
- A `state` record is written whenever a node's traced state changes. The
  visualizer can rebuild each node's state at any step from these.

JSON Lines (rather than one big JSON array) means a run that dies still
leaves a readable trace up to that point, and the file can be streamed.
Records are Go structs, never maps, so field order is fixed and traces are
byte-stable. The visualizer (M8) reads this format.

## Known simplifications

- **File metadata is durable immediately.** In the simulated disk, create,
  rename and remove survive any crash. On a real file system they also need
  the directory to be fsynced. This may be tightened in M4, when the real
  WAL needs it.
- **Seeds reproduce within one Go version.** The random streams come from
  `math/rand/v2`, whose algorithms Go could in principle change between
  releases. Traces recorded under one Go version are not guaranteed to
  replay under another.
- **Disk slowness only delays sends.** A slow fsync delays the messages sent
  in that step, but the node can still process the next event straight
  away.

## 8. Real mode *(planned, M7)*

In real mode the `server` package plays the driver's role: a ticker goroutine
calls `Tick` every tick interval, a gRPC handler calls `Step` for each
incoming message, and a single loop goroutine owns the `raft` node and
serializes all calls into it. The concurrency lives in the driver, never in
`raft`, which is why `raft` needs no locks.
