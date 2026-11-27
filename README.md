# StormProof

[![ci](https://github.com/20001172024/StormProof/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/20001172024/StormProof/actions/workflows/ci.yml)

A replicated key-value store built on a from-scratch implementation of the
Raft consensus algorithm, tested by a **deterministic simulator** that injects
crashes, partitions, message loss and disk faults, and a **time-travel
visualizer** for replaying failures step by step.

> **Status:** early development. Done: project setup (M0), the simulator
> foundation (M1), Raft leader election (M2), log replication (M3), a
> crash-safe write-ahead log (M4), the KV store with sessions, ReadIndex
> reads and Porcupine linearizability checking (M5), snapshots with log
> compaction (M6a) and single-server membership changes (M6b), all tested
> in the simulator; real mode (M7): the same server code running over
> gRPC, with a CLI, Prometheus metrics and docker-compose clusters; and
> the trace visualizer (M8). CI at scale (M9) is next.

## Why it's built this way

The Raft node is a pure state machine: no goroutines, clocks, I/O or global
randomness. Time, network, disk and randomness are interfaces with a real and
a simulated implementation. In simulation the whole cluster runs on one
goroutine from a single random seed, so any bug the simulator finds can be
replayed exactly. See [docs/design.md](docs/design.md).

## Layout

| Path | What it holds |
|---|---|
| `raft/` | Deterministic Raft state machine (no I/O) |
| `kv/` | Key-value state machine, client sessions, dedup |
| `storage/` | File-system interface (real and simulated), the write-ahead log (`storage/wal`) and snapshot files (`storage/snap`) |
| `transport/` | gRPC transport between nodes |
| `server/` | The real server: `server/host` (shared with the simulator), the gRPC event loop, and the `stormproof-server` binary |
| `client/` | Go client library and the `stormproof-cli` command |
| `sim/` | Simulator: event loop, fake clock/network/disk, faults, invariants, traces |
| `check/` | Linearizability checking: a Porcupine model of the KV store |
| `proto/` | Protocol Buffers definitions |
| `visualizer/` | React app for stepping through simulator traces, with example traces |
| `docs/` | Design notes, bug write-ups, benchmarks |

## Development

Requires Go 1.24+ and golangci-lint v2.

```bash
make test   # go test -race ./...
make lint   # golangci-lint run ./...
make proto  # regenerate Go code from proto/ (installs pinned buf into ./bin)
```

Run a real three-server cluster on one machine:

```bash
go build -o bin/ ./server/cmd/stormproof-server ./client/cmd/stormproof-cli
C=1=localhost:7001,2=localhost:7002,3=localhost:7003
bin/stormproof-server -id 1 -cluster $C -data /tmp/sp1 &
bin/stormproof-server -id 2 -cluster $C -data /tmp/sp2 &
bin/stormproof-server -id 3 -cluster $C -data /tmp/sp3 &
export STORMPROOF_ENDPOINTS=localhost:7001,localhost:7002,localhost:7003
bin/stormproof-cli put greeting hello
bin/stormproof-cli get greeting
bin/stormproof-cli cas greeting hello hi
```

Or with Docker (3 servers plus Prometheus on :9090; `docker-compose.5.yml`
for 5):

```bash
docker compose -f deploy/docker/docker-compose.yml up -d --build --wait
docker compose -f deploy/docker/docker-compose.yml run --rm cli put greeting hello
docker compose -f deploy/docker/docker-compose.yml down -v
deploy/docker/smoke.sh   # start, kill the leader, check writes continue, tear down
```

Run the simulator:

```bash
go run ./sim/cmd/stormproof-sim -seeds 1000 -profile all                     # sweep seeds: Raft + KV clients
go run ./sim/cmd/stormproof-sim -seeds 300 -bug dedup -trace-dir traces/     # plant a bug, save failing traces
go run ./sim/cmd/stormproof-sim -seed 7 -bug fsync -trace seed-7.jsonl       # replay one seed
go run ./sim/cmd/stormproof-sim -seeds 1000 -membership                      # servers added and removed during the run
go run ./sim/cmd/stormproof-sim -node toy -seeds 1000                        # the toy counter used to test the simulator
```

Step through a trace in the visualizer (needs Node 22):

```bash
cd visualizer && npm ci && npm run dev   # then open the printed URL and drop a .jsonl trace on it
```

The visualizer is also published with GitHub Pages, preloaded with example
traces (a leader crash, partitions, membership changes, a planted bug).

## License

MIT. See [LICENSE](LICENSE).
