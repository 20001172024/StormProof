# StormProof documentation

| Document | What it covers |
|---|---|
| [design.md](design.md) | How everything works and why, milestone by milestone. Start with sections 1-7 (the deterministic design, the Raft interface, the simulator, invariants, traces); the rest records what the simulator and the benchmarks showed for each feature. |
| [bugs/](bugs/) | Every bug the simulator found: seed, symptom, root cause, fix and commit. |
| [benchmarks.md](benchmarks.md) | Throughput, latency, leader-crash recovery, micro-benchmarks and simulator speed, with the setup, the exact commands and the raw results ([benchmarks/results/](benchmarks/results/)). |
| [aws.md](aws.md) | Running the cluster on AWS with Terraform and k3s: cost, steps, the chaos demo, and `terraform destroy`. |
| [../deploy/k8s/README.md](../deploy/k8s/README.md) | The Kubernetes manifests, monitoring and chaos demo. |

Where to find each Raft rule in design.md:

| Topic | Section |
|---|---|
| Leader election, terms, randomized timeouts (§5.2) | 8 |
| Pre-vote (dissertation §9.6) | 17 |
| Vote stickiness (§4.2.3), removed servers | 13 |
| Log replication, consistency check, fast backtracking, commit rule and Figure 8 (§5.3, §5.4) | 9 |
| Flow control and batching (dissertation §10.2.1) | 18 |
| Persistence, the write-ahead log, crash recovery | 10 |
| Client sessions, exactly-once, ReadIndex, linearizability checking (§6.3, §6.4) | 11 |
| Snapshots, log compaction, InstallSnapshot (§7) | 12 |
| Single-server membership changes (dissertation §4.1-4.3) | 13 |
