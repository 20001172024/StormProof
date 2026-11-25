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

## 3. Injected dependencies *(planned, M1)*

Each source of non-determinism is an interface with two implementations:

| Concern | Interface owns | Real | Simulated |
|---|---|---|---|
| Time | current time, timers | wall clock + `time.Timer` | logical clock advanced by the event loop |
| Network | send/receive between nodes | gRPC (`transport`) | in-memory queue with drop / delay / reorder / duplicate / partition |
| Disk | WAL append, fsync, snapshot files | files on disk (`storage`) | in-memory files with torn writes, fsync failure, slow I/O |
| Randomness | election timeouts, workload choices | RNG seeded from `crypto/rand` | RNG seeded from the run's seed |

In real mode the seed is random and logged, which keeps behaviour realistic.
In simulation the seed is the input.

## 4. The simulator event loop *(planned, M1)*

The simulator runs **every node, every client and the network on one
goroutine**. Its core is a priority queue of events:

```
event = { at: logical time, seq: insertion counter, kind, payload }
```

The loop repeatedly pops the earliest event, advances the logical clock to
`event.at`, handles it (deliver a message → `Step`, timer fires → `Tick`,
client issues an operation → `Propose`, fault fires → crash / partition /
heal), drains every node's `Ready`, schedules the resulting work as new
events, and runs the invariant checks.

Two events can have the same `at`. Ties are broken by `seq`, the order in
which the events were scheduled, so there is never an ambiguous ordering.
Without the tie-break, the order would depend on heap internals and could
shift with unrelated code changes.

Fault decisions (does this message get dropped? delayed by how much? does
node 2 crash now?) are drawn from the seeded RNG at the moment the event is
scheduled, in a fixed order. The fault *schedule* is therefore a pure function
of the seed.

## 5. Sources of non-determinism, and the rule for each

These are the ways a Go program can quietly behave differently on two runs.
Each one is either forbidden in deterministic code or has a rule that tames it.

| Source | Why it's a problem | Rule |
|---|---|---|
| Wall-clock time (`time.Now`, timers) | Differs every run | `raft` imports no `time`; sim uses the logical clock. Enforced by depguard. |
| Global / unseeded randomness | Differs every run | Only the injected, seeded RNG. `math/rand` and `crypto/rand` banned in `raft`. |
| Goroutines | The scheduler picks the interleaving | No `go` statements in `raft` (`purity_test.go`); sim runs on one goroutine. |
| `select` with several ready cases | Go picks one **at random** on purpose | No `select` in `raft` (`purity_test.go`). |
| **Map iteration order** | Go randomizes it **on purpose**, every iteration | Never let map order leak into behaviour or output. Iterate peers via a sorted slice of IDs; sort keys before writing traces. This one is not caught by a linter, so it needs review and the determinism test. |
| Direct I/O (files, sockets) | Timing and failures come from the OS | `raft` imports no `os` / `net`; sim uses in-memory fakes. |
| Pointer addresses (`%p`, printing pointers) | Differ between runs | Never put addresses in traces or logs that feed traces. |
| Unordered heap ties | Order depends on heap internals | Every event carries a `seq` tie-breaker. |

The last line of defence is the **determinism test** (M1): run the same seed
twice and assert the two trace files are byte-identical. If any rule above is
broken somewhere, that test is what catches it. It must always pass.

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

## 7. Trace format *(draft, finalised in M1)*

A trace is a JSON Lines file: one JSON object per line, one line per event, in
the order the event loop processed them. A header line records everything
needed to reproduce the run.

```json
{"type":"header","version":1,"seed":42,"nodes":[1,2,3],"profile":"partitions"}
{"t":0,"seq":0,"type":"tick","node":1}
{"t":13,"seq":7,"type":"role","node":2,"role":"candidate","term":1}
{"t":13,"seq":8,"type":"send","from":2,"to":1,"msg":{"kind":"RequestVote","term":1}}
{"t":15,"seq":9,"type":"drop","from":2,"to":3,"reason":"partition"}
{"t":40,"seq":31,"type":"violation","invariant":"election_safety","detail":"..."}
```

JSON Lines (rather than one big JSON array) means a crashed run still leaves a
readable trace up to the crash, and the file can be streamed. Field order is
fixed by Go struct definitions, never by maps, so traces are byte-stable.
The visualizer (M8) reads this format.

## 8. Real mode *(planned, M7)*

In real mode the `server` package plays the driver's role: a ticker goroutine
calls `Tick` every tick interval, a gRPC handler calls `Step` for each
incoming message, and a single loop goroutine owns the `raft` node and
serializes all calls into it. The concurrency lives in the driver, never in
`raft`, which is why `raft` needs no locks.
