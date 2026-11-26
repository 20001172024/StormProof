# StormProof

[![ci](https://github.com/20001172024/StormProof/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/20001172024/StormProof/actions/workflows/ci.yml)

A replicated key-value store built on a from-scratch implementation of the
Raft consensus algorithm, tested by a **deterministic simulator** that injects
crashes, partitions, message loss and disk faults, and a **time-travel
visualizer** for replaying failures step by step.

> **Status:** early development. Done: project setup (M0), the simulator
> foundation (M1), Raft leader election (M2), log replication (M3), a
> crash-safe write-ahead log (M4), the KV store with sessions, ReadIndex
> reads and Porcupine linearizability checking (M5), and snapshots with log
> compaction (M6a), all in the simulator. Membership changes (M6b) are
> next. Nothing runs outside the simulator yet: the gRPC server comes in M7.

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
| `server/` | Node binary wiring everything together |
| `client/` | Go client library and CLI |
| `sim/` | Simulator: event loop, fake clock/network/disk, faults, invariants, traces |
| `check/` | Linearizability checking: a Porcupine model of the KV store |
| `proto/` | Protocol Buffers definitions |
| `docs/` | Design notes, bug write-ups, benchmarks |

## Development

Requires Go 1.24+ and golangci-lint v2.

```bash
make test   # go test -race ./...
make lint   # golangci-lint run ./...
make proto  # regenerate Go code from proto/ (installs pinned buf into ./bin)
```

Run the simulator:

```bash
go run ./sim/cmd/stormproof-sim -seeds 1000 -profile all                     # sweep seeds: Raft + KV clients
go run ./sim/cmd/stormproof-sim -seeds 300 -bug dedup -trace-dir traces/     # plant a bug, save failing traces
go run ./sim/cmd/stormproof-sim -seed 7 -bug fsync -trace seed-7.jsonl       # replay one seed
go run ./sim/cmd/stormproof-sim -node toy -seeds 1000                      # the toy counter used to test the simulator
```

## License

MIT. See [LICENSE](LICENSE).
