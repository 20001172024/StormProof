# StormProof — Project Brief for Claude Code

## 1. What this project is

StormProof is a **replicated key-value store built on my own implementation of the Raft consensus algorithm**, together with a **deterministic simulation testing framework** and a **time-travel visualizer**.

- **The store:** a cluster of nodes that agree on a replicated log via Raft, exposing a linearizable key-value API over gRPC.
- **The simulator:** runs the entire cluster inside one process with a fake clock, fake network and fake disk. It injects crashes, network partitions, dropped / delayed / reordered / duplicated messages, and slow or failing disks, then checks that correctness still holds. Every run is driven by a single random seed, so **any bug found can be replayed exactly**.
- **The visualizer:** a web UI that loads a simulation trace and lets me scrub forward and backward through a failure step by step, watching leader elections, log replication and commits as they happened.
- **The cloud deployment:** the real (non-simulated) cluster runs on AWS, provisioned with Terraform, orchestrated with Kubernetes, and monitored with Prometheus and Grafana.

**The simulator and visualizer are the core of this project, not add-ons.** Raft itself is a well-known exercise; what makes this project stand out is the testing framework and the real bugs it finds.

**Owner:** Swarnima Arora (B.Tech CSE-AI, IGDTUW). This is a portfolio project for SWE/SDE roles. I must be able to explain every design decision and every line of the Raft logic in interviews.

---

## 2. How I want you (Claude Code) to work

- **Work one milestone at a time** (Section 9). Don't jump ahead or generate the whole project at once.
- **Before writing code for a milestone, propose a short plan** (files, approach, trade-offs) and wait for my OK.
- **Raft core logic is a learning priority.** For the Raft state machine (elections, replication, commit rules, snapshots, membership), explain the relevant rule from the Raft paper (cite the section / Figure 2 rule) before implementing it. Where it helps me learn, let me attempt a piece first and then review it rather than writing it all yourself.
- **Explain non-obvious choices** in a few sentences (e.g. election timeout ranges, batching, how linearizable reads are served).
- **Never invent results.** Benchmark numbers, bug counts and test outcomes come only from actually running the code. If a bug is found by the simulator, record it honestly in `docs/bugs/` (seed, symptom, root cause, fix, commit).
- **Ask before adding any dependency** not listed in Section 4.
- **Write tests for every module.** Keep commits small and meaningful (`feat(raft): add RequestVote handling`, `fix(raft): reject stale term AppendEntries`).
- Prefer simple, readable code over clever code. No premature optimization before correctness is proven by the simulator.
- **Cloud cost safety:** never leave cloud resources running without telling me. Every Terraform setup must have a documented `terraform destroy` step, and default to the cheapest viable instance types.

---

## 3. Key design principle: determinism

The simulator only works if the Raft node is **deterministic**. Follow this design (similar in spirit to etcd's raft library):

- The Raft node is a **pure state machine** with no goroutines, no real clocks, no direct I/O and no global randomness inside it. It is driven only by:
  - `Step(msg)` — deliver an incoming message
  - `Tick()` — advance logical time by one tick
  - `Propose(cmd)` — submit a client command
- After each call, the node returns a `Ready` batch: messages to send, entries to persist, entries committed and ready to apply, snapshot to save.
- **Time, network, disk and randomness are interfaces**, with two implementations each:
  - **Real:** wall clock, gRPC transport, file-based WAL, `crypto/rand`-seeded RNG
  - **Simulated:** logical clock, in-memory network with fault injection, in-memory disk with fault injection, RNG seeded from the run's seed
- In simulation mode the whole cluster runs on **one goroutine in a single event loop**, so the same seed always produces the exact same sequence of events.
- **Determinism check:** a test runs the same seed twice and asserts the traces are byte-identical. This test must always pass.

---

## 4. Tech stack

| Area | Choice | Notes |
|---|---|---|
| Language | **Go 1.22+** | Standard for distributed systems; Porcupine is a Go library. (I know Java/C++; explain Go idioms when they first appear.) |
| RPC | gRPC + Protocol Buffers | Client API and node-to-node transport in real mode |
| Persistence | Custom write-ahead log (WAL) + snapshot files | CRC checksums per record, fsync policy configurable |
| Correctness checking | [Porcupine](https://github.com/anishathalye/porcupine) linearizability checker | Run on client histories from every simulation |
| Testing | Go `testing`, table-driven tests, fuzzing (`go test -fuzz`) where useful | |
| Benchmarks | Go benchmark harness + custom load generator | |
| Visualizer | React + Vite + TypeScript, D3 or a simple SVG/canvas timeline | Reads JSON trace files; static site, no backend needed |
| Metrics | Prometheus client for Go | `/metrics` endpoint on every node |
| Dashboards | Grafana | Dashboards stored as JSON in repo |
| Containers | Docker, docker-compose for local multi-node runs | |
| Orchestration | Kubernetes (StatefulSet + headless Service) | Local: `kind`. Cloud: see below |
| Infrastructure as code | Terraform | AWS provider |
| Cloud | AWS | Default: **k3s on 3 small EC2 instances** (cheaper than EKS). EKS as an optional documented variant. |
| CI | GitHub Actions | |

---

## 5. Features to implement

**Raft core**
- Leader election (randomized election timeouts, pre-vote optional extension)
- Log replication with consistency check and fast log backtracking
- Commit rules (only commit entries from the current term by counting replicas)
- Snapshots / log compaction and `InstallSnapshot`
- Cluster membership changes (single-server changes, as in the Raft dissertation; joint consensus optional)

**Key-value store**
- Operations: `Get`, `Put`, `Delete`, `CompareAndSwap`
- **Linearizable reads** via ReadIndex (and optionally leader leases, with the clock-assumption trade-off documented)
- Client sessions with request IDs for **exactly-once semantics** (deduplicate retried writes)
- gRPC client API plus a small CLI client (`stormproof-cli put foo bar`)

**Persistence**
- Write-ahead log with per-record CRC, crash-safe append, and recovery on restart
- Snapshot files with atomic write (write temp file, fsync, rename)
- Simulated disk faults: torn writes, fsync failure, slow disk

**Simulator & fault injection**
- Node crash and restart (with and without losing unsynced data)
- Network partitions (symmetric, asymmetric, isolating the leader, minority/majority splits)
- Message drop, delay, reorder and duplication with configurable probabilities
- Clock skew between nodes
- Random client workloads (concurrent clients issuing mixed operations)
- Invariant checks after every step:
  - **Election safety:** at most one leader per term
  - **Log matching:** same index + term implies identical prefixes
  - **Leader completeness:** committed entries appear in all future leaders' logs
  - **State machine safety:** no two nodes apply different commands at the same index
- **Linearizability check** with Porcupine on the full client history at the end of each run
- On failure: save the seed, the minimized fault schedule if possible, and a full trace file

**Visualizer**
- Load a trace JSON file (from the simulator, or from a failing CI run artifact)
- Timeline scrubber: step forward/back, jump to event, play/pause
- Per-node view: role (follower/candidate/leader), current term, log contents, commit index
- Messages drawn as arrows between nodes; dropped messages shown distinctly
- Partitions and crashes highlighted on the timeline
- "Jump to invariant violation" button

---

## 6. Repository structure

```
stormproof/
├── CLAUDE.md
├── README.md
├── go.mod
├── proto/                     # .proto definitions (client API + raft messages)
├── raft/                      # deterministic Raft state machine (no I/O)
│   ├── node.go, log.go, election.go, replication.go,
│   ├── snapshot.go, membership.go, readindex.go
│   └── *_test.go
├── kv/                        # KV state machine, sessions, dedup
├── storage/                   # WAL + snapshot files (real) and interfaces
├── transport/                 # gRPC transport (real)
├── server/                    # wires raft + kv + storage + transport into a node binary
├── client/                    # Go client library + CLI
├── sim/                       # simulator: event loop, fake clock/net/disk, fault scheduler,
│   │                          # workloads, invariants, trace writer
│   └── cmd/stormproof-sim/  # CLI: run N seeds, replay a seed
├── check/                     # Porcupine models + history recording
├── bench/                     # load generator and benchmark scripts
├── visualizer/                # React app
├── deploy/
│   ├── docker/                # Dockerfile, docker-compose.yml
│   ├── k8s/                   # StatefulSet, Services, ConfigMaps, kustomize overlays
│   ├── terraform/             # AWS: VPC, EC2 (k3s), security groups, outputs
│   └── monitoring/            # Prometheus config, Grafana dashboards (JSON)
├── docs/
│   ├── design.md              # architecture + determinism design
│   ├── bugs/                  # one file per bug found by the simulator
│   └── benchmarks.md
└── .github/workflows/
    ├── ci.yml                 # lint, unit tests, determinism test, 1,000+ sim seeds
    └── nightly.yml            # 10,000+ sim seeds, uploads failing traces as artifacts
```

---

## 7. Benchmarks to report

Run on both local docker-compose and the AWS cluster; record hardware and configuration with every result.

- **Throughput:** ops/sec for write-heavy, read-heavy and mixed workloads, at 3 and 5 nodes
- **Latency:** p50 / p95 / p99 for `Put` and linearizable `Get`
- **Recovery time:** time from leader crash to new leader elected and to client writes succeeding again
- **Effect of batching / pipelining** on throughput (before vs after, if implemented)
- **Simulator throughput:** simulated seeds per minute in CI

Results go in `docs/benchmarks.md` and a summary table in the README, **only from real runs**.

---

## 8. CI requirements

- **On every push / PR (`ci.yml`):** `go vet`, `staticcheck`/`golangci-lint`, unit tests with `-race`, the determinism test, and **at least 1,000 simulated seeds** across varied fault profiles. Must finish in a reasonable time (target under 10 minutes).
- **Nightly (`nightly.yml`):** **10,000+ seeds** with heavier fault profiles. Any failing seed's trace file is uploaded as a build artifact so I can load it in the visualizer.
- Build and push Docker images on merge to main.
- Visualizer builds and deploys as a static site (GitHub Pages), preloaded with a few interesting example traces.

---

## 9. Milestones (do these in order)

**M0 — Setup.** Go module, repo skeleton, protobuf generation, linting, CI skeleton, `docs/design.md` describing the deterministic design from Section 3.

**M1 — Simulator foundation.** Interfaces for clock, network, disk and RNG; simulated implementations; single-threaded event loop; trace writer (JSON); determinism test. Build this *before* Raft so Raft is tested in the simulator from day one.

**M2 — Leader election.** RequestVote, terms, randomized timeouts, election-safety invariant. Simulator tests with partitions and crashes.

**M3 — Log replication & commit.** AppendEntries, consistency check, fast backtracking, commit index, log-matching / leader-completeness / state-machine-safety invariants.

**M4 — Persistence.** WAL with CRC, recovery on restart, simulated disk faults, crash-restart scenarios in the simulator.

**M5 — KV store & linearizability.** KV state machine, client sessions and dedup, ReadIndex reads, client history recording, Porcupine checking at the end of each sim run.

**M6 — Snapshots & membership changes.** Log compaction, InstallSnapshot, add/remove server, simulator scenarios that change membership during faults.

**M7 — Real mode.** gRPC transport, file WAL, node binary, CLI client, docker-compose 3- and 5-node clusters, Prometheus `/metrics` (term, role, commit index, apply lag, RPC latency, election count).

**M8 — Visualizer.** React app loading traces, timeline scrubber, node and message views, jump-to-violation, GitHub Pages deploy with example traces.

**M9 — CI at scale.** 1,000+ seeds per commit, 10,000+ nightly, failing-trace artifacts.

**M10 — Benchmarks.** Load generator, benchmark runs, `docs/benchmarks.md`.

**M11 — Cloud & DevOps.**
- Terraform for AWS: VPC, subnets, security groups, 3 EC2 instances running k3s (EKS as an optional documented variant)
- Kubernetes manifests: StatefulSet with persistent volumes, headless Service for peer discovery, client Service, readiness/liveness probes
- Prometheus + Grafana deployed in the cluster, with a dashboard showing leader changes, commit rate, latency and replication lag
- A **live chaos demo**: script that kills the leader pod / applies a network policy partition while the load generator runs, with Grafana showing failover
- Documented `terraform apply` / `terraform destroy`, cost estimate, and a reminder to set an AWS billing alarm
- Benchmarks re-run on AWS

**M12 — Polish.** README with architecture diagram, demo GIF of the visualizer, Grafana screenshot from the AWS chaos demo, benchmark table, list of bugs found by the simulator, limitations section.

---

## 10. Definition of done

- All Raft features in Section 5 implemented and passing the simulator with all invariants and Porcupine checks
- Determinism test passes: same seed → identical trace
- CI runs 1,000+ seeds per commit and 10,000+ nightly
- At least a few real bugs found by the simulator are documented honestly in `docs/bugs/`
- Visualizer live on GitHub Pages with example traces
- Cluster deployed on AWS via Terraform + Kubernetes, with Prometheus/Grafana monitoring and a recorded chaos demo
- Benchmarks from real runs in README
- All cloud resources destroyed when not in use
- I can explain every Raft rule, the determinism design, and every module without looking at the code
