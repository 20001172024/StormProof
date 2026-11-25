# StormProof

A replicated key-value store built on a from-scratch implementation of the
Raft consensus algorithm, tested by a **deterministic simulator** that injects
crashes, partitions, message loss and disk faults, and a **time-travel
visualizer** for replaying failures step by step.

> **Status:** early development. Done: project setup (M0) and the simulator
> foundation (M1), which currently drives a toy replicated counter. Raft
> itself starts in M2.

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
| `storage/` | Write-ahead log and snapshot files |
| `transport/` | gRPC transport between nodes |
| `server/` | Node binary wiring everything together |
| `client/` | Go client library and CLI |
| `sim/` | Simulator: event loop, fake clock/network/disk, faults, invariants, traces |
| `check/` | Linearizability checking with Porcupine |
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
go run ./sim/cmd/stormproof-sim -seeds 1000 -profile all                   # sweep seeds
go run ./sim/cmd/stormproof-sim -seeds 1000 -buggy -trace-dir traces/      # plant a bug, save failing traces
go run ./sim/cmd/stormproof-sim -seed 999 -buggy -trace seed-999.jsonl     # replay one seed
```
