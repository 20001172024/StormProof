# Benchmarks

Every number here comes from a run of the code in this repository; each
section says what was run, on what, and how to repeat it. The raw results
(one JSON file per run, plus the recovery timings) are in
[benchmarks/results/](benchmarks/results/).

## Setup

**Machine.** One cloud VM: 4 vCPUs (Intel Xeon @ 2.10 GHz), 16 GB of RAM,
one virtual disk (ext4), Linux 6.18, Docker 29.9, Go 1.24.7. An fsync of a
small write on a Docker volume there takes about 0.26 ms (p50; 0.6 ms
p99), the same as on the host's file system.

**Cluster.** The docker-compose clusters in `deploy/docker`: 3 or 5
servers in containers on that one VM, with the defaults (50 ms ticks,
election timeout 0.5-1 s, heartbeats every tick, fsync on every WAL
write, snapshots every 10,000 entries, pre-vote on). The load generator
runs as one more container on the same compose network.

**What that means for the numbers.** All servers, the load generator and
Prometheus share 4 CPUs and one disk, and network hops never leave the
machine. The results compare configurations and changes fairly with each
other; they are not what dedicated machines would do. The AWS runs in M11
will give those.

**Load generator** (`bench/cmd/stormproof-bench`): closed-loop clients,
each with its own session (writes are exactly-once) and one request at a
time; keys chosen uniformly from 1,000; 100-byte values; Gets are
linearizable (ReadIndex). Latency is measured at the client, per
operation, and percentiles are exact (every latency is kept and sorted).
Each configuration ran 3 times for 10 s after a 3 s warm-up; tables show
the median of the 3 runs for each figure.

Exactly what was run for the tables below (the script's defaults are 20 s
runs at 32 clients only):

```bash
docker build -f deploy/docker/Dockerfile -t stormproof:latest .
LABEL=final-3 CLIENTS="1 8 32" REPS=3 DURATION=10s WARMUP=3s bench/run.sh
NODES=5 LABEL=final-5 CLIENTS="1 8 32" REPS=3 DURATION=10s WARMUP=3s bench/run.sh
go run ./bench/cmd/benchtable deploy/docker/results/final-3
NODES=3 REPS=7 bench/recovery.sh
NODES=5 REPS=7 bench/recovery.sh
```

For the before-and-after table, the same matrix ran against images built
from each earlier commit (`git worktree add`, then `docker build` there).

## Throughput and latency (final code, f335ecb)

**3 servers**

| Workload | Clients | ops/sec | Put p50 ms | Put p99 ms | Get p50 ms | Get p99 ms | Errors |
|---|---|---|---|---|---|---|---|
| 10% reads | 1 | 594 | 1.64 | 4.15 | 0.63 | 1.52 | 0 |
| 10% reads | 8 | 2,488 | 2.99 | 7.56 | 2.33 | 5.82 | 0 |
| 10% reads | 32 | 5,735 | 5.21 | 13.43 | 3.65 | 10.30 | 0 |
| 50% reads | 1 | 843 | 1.60 | 3.89 | 0.60 | 1.49 | 0 |
| 50% reads | 8 | 2,946 | 2.95 | 7.50 | 2.06 | 5.75 | 0 |
| 50% reads | 32 | 6,750 | 5.04 | 13.04 | 3.63 | 10.47 | 0 |
| 90% reads | 1 | 1,484 | 1.55 | 3.77 | 0.52 | 1.34 | 0 |
| 90% reads | 8 | 4,606 | 2.42 | 6.60 | 1.44 | 4.83 | 0 |
| 90% reads | 32 | 7,955 | 4.94 | 12.22 | 3.52 | 10.13 | 0 |

**5 servers**

| Workload | Clients | ops/sec | Put p50 ms | Put p99 ms | Get p50 ms | Get p99 ms | Errors |
|---|---|---|---|---|---|---|---|
| 10% reads | 1 | 505 | 1.97 | 4.48 | 0.93 | 2.25 | 0 |
| 10% reads | 8 | 1,831 | 4.14 | 9.87 | 3.08 | 7.80 | 0 |
| 10% reads | 32 | 4,739 | 6.41 | 16.45 | 4.47 | 13.07 | 0 |
| 50% reads | 1 | 694 | 1.86 | 4.85 | 0.80 | 2.22 | 0 |
| 50% reads | 8 | 2,123 | 4.06 | 10.66 | 2.85 | 7.93 | 0 |
| 50% reads | 32 | 5,121 | 6.76 | 17.40 | 4.79 | 14.23 | 0 |
| 90% reads | 1 | 1,007 | 1.95 | 5.12 | 0.76 | 2.22 | 0 |
| 90% reads | 8 | 2,945 | 3.52 | 10.50 | 2.26 | 7.46 | 0 |
| 90% reads | 32 | 6,542 | 5.86 | 16.05 | 4.22 | 13.28 | 0 |

How to read these:

- **A write costs two fsyncs one after the other** (the leader saves the
  entry before sending it, then a follower saves it before acknowledging)
  plus the network hops: one client at a time gets a Put in about 1.6 ms. A linearizable Get skips the
  disk but still needs a round of acknowledgements from a majority
  (ReadIndex), about 0.6 ms.
- **Throughput grows with clients** because concurrent requests share
  fsyncs and messages (see the next section). At 32 clients, latency grows
  too: the 4 CPUs are busy.
- **5 servers are slower than 3**: every entry goes to twice as many
  followers, and here all of them share the same 4 CPUs and one disk. On
  separate machines the gap would be smaller.

## What batching and flow control did

Benchmarking first showed write throughput *falling* as clients were
added. Three changes fixed that; each was measured on its own, on the same
machine, with the same matrix (3 servers, ops/sec, median of 3 runs):

| Workload | Clients | before (b48de01) | + flow control (0cb8322) | + server batching (a872cd6) | + config fix (f335ecb) |
|---|---|---|---|---|---|
| 10% reads | 1 | 582 | 578 | 630 | 594 |
| 10% reads | 8 | 330 | 2,053 | 2,538 | 2,488 |
| 10% reads | 32 | 656 | 2,173 | 5,763 | 5,735 |
| 50% reads | 1 | 879 | 837 | 832 | 843 |
| 50% reads | 8 | 1,265 | 2,608 | 2,908 | 2,946 |
| 50% reads | 32 | 894 | 3,404 | 6,144 | 6,750 |
| 90% reads | 1 | 1,372 | 1,292 | 946 | 1,484 |
| 90% reads | 8 | 3,970 | 3,862 | 3,816 | 4,606 |
| 90% reads | 32 | 3,585 | 6,771 | 7,720 | 7,955 |

1. **Flow control in Raft (0cb8322).** Every proposal, and every
   acknowledgement that found more to send, resent all unacknowledged
   entries. Under steady load these chains of send, acknowledge, send
   piled up faster than they finished: before the fix, 8 write-heavy
   clients got 330 ops/s, fewer than one client's 582. Now each follower
   has at most one batch of entries outstanding, and entries proposed
   meanwhile go together in the next batch. Write-heavy throughput at 8
   clients went from 330 to 2,053 ops/s (6.2 times), with median Put
   latency from 23.6 ms to 3.8 ms. (Found by profiling, then confirmed by
   an experiment that removed the resend; see design.md section 18.)
2. **Batching in the server (a872cd6).** The event loop did one Raft
   Ready, so one fsync, per event. It now handles everything already
   queued (up to 512 events) with one Ready. That mostly helps at high
   concurrency, where many requests are queued: write-heavy at 32 clients
   went from 2,173 to 5,763 ops/s (2.7 times).
3. **No full-log scan on every append (f335ecb).** Followers looked for the
   latest configuration entry by scanning the whole log after every
   append. Its cost grows with the log (up to the 10,000 entries between
   snapshots). In this matrix the last column differs from the one before
   by -2% to +21%, which this VM's run-to-run noise could explain, so these
   runs don't show a gain either way; the micro-benchmark below shows the
   effect directly.

Single-client numbers barely move, as expected: one client never has more
than one request to batch. Differences of up to about 10% between runs of
the same code are noise on this shared VM; the read-heavy, 1-client cell of
the a872cd6 column (946 ops/s, with one run's Put p99 at 45 ms) is likely
such a hiccup.

Two more problems turned up on the way, neither in Raft itself:

- **The real server kept the simulator's history.** It stored every
  applied entry and every snapshot, and hashed the whole store after every
  write, for the simulator's state machine safety check. Memory grew
  forever and each write cost time proportional to the store's size (20%
  of CPU in a profile). Now only the simulator keeps it (10d9fa9).
- **The garbage collector held back the simulator** when it ran seeds in
  parallel (M9): see design.md section 17.

## Recovery from a leader crash

`bench/recovery.sh`: under a steady write load (4 clients, writes only),
kill the leader's container and measure from the moment the kill
returns: how long until another server reports being leader, and how long
until a write started after the kill succeeds. Repeated 7 times, restarting
the killed server each time. Measured with the image from a872cd6 (the
configuration fix after it doesn't touch elections).

| Servers | election, median (range) | writes succeed again, median (range) |
|---|---|---|
| 3 | 595 ms (418-696) | 694 ms (519-736) |
| 5 | 396 ms (360-624) | 523 ms (331-558) |

- The kill itself took 218-281 ms (`docker kill`). The leader died at some
  point in that window, so recovery could be up to that much longer than
  shown; the figures are lower bounds by at most about 0.25 s.
- The election time is detected by polling each server's `/metrics` in
  turn, so it can lag the real election; twice with 5 servers, writes were
  back before the poll saw the new leader.
- Followers wait an election timeout of 0.5-1 s (10 ticks of 50 ms,
  randomized up to double) after the last heartbeat before starting an
  election, then a pre-vote round and a vote round. With 5 servers the
  election came sooner. The likely reason: four followers draw random
  timeouts instead of two, and the earliest of four is earlier on average.
- Writes come back shortly after the election: the client library retries
  a dead server after 300 ms (`-attempt-timeout` here) and follows the
  servers' hints to the new leader.

## Micro-benchmarks

`go test ./storage/wal ./kv ./raft -run '^$' -bench . -benchtime 2s`, same
machine:

| Benchmark | Result |
|---|---|
| WAL Save, 1 entry, no fsync | 977 ns per Save |
| WAL Save, 1 entry, fsync | 310 us per Save (3,229 entries/s) |
| WAL Save, 16 entries, fsync | 361 us per Save (44,279 entries/s) |
| WAL Save, 128 entries, fsync | 467 us per Save (274,053 entries/s) |
| KV Apply, Put | 189 ns |
| KV Apply, Get | 162 ns |
| Raft replication round, 3 nodes in memory (propose to committed everywhere) | about 5.5 us, flat from 10,000 to 50,000 entries |

The fsync is nearly the whole cost of a Save, whatever the batch size:
128 entries cost 1.5 times one entry. That is why batching (above) pays.
The protocol logic itself is cheap next to a disk or network round trip.
Before the configuration fix, the in-memory replication round grew with
the log: 4.8 us at 1,000 entries, 14.6 us at 10,000, 80 us at 50,000.

## Simulator throughput

Seeds per minute on the same 4-vCPU machine, 15 s simulated runs with KV
clients, `-parallel 4` (the default):

| Run | seeds/min |
|---|---|
| `-profile all` (3 servers) | 2,186 |
| `-profile all -membership` | 1,086 |
| `-profile all -nodes 5` | 879 |
| `-profile storm` | 4,561 |
| `-profile all -parallel 1` | 561 |

So one CPU core simulates about 9 seeds of 15 s each per second, roughly
140 seconds of cluster time per second. Fewer messages made the simulator
faster too: before flow control, `-profile all` ran at about 1,000
seeds/min on 4 workers (measured on the previous machine, a 2.30 GHz VM).
The per-push CI sweeps run 1,000 seeds of each of three modes on GitHub's
4-CPU runners; their job summaries report the measured rate.

