# StormProof on Kubernetes

```
deploy/k8s/
  base/               StatefulSet (3 servers), peer and client Services, PodDisruptionBudget
  overlays/local/     a local cluster with a locally built image (+ monitoring)
  overlays/aws/       the k3s cluster from deploy/terraform, image from ghcr.io (+ monitoring)
  jobs/bench.yaml     the load generator as a Job
  jobs/isolate.yaml   a NetworkPolicy that partitions pods labelled chaos=isolated
  chaos.sh            live demo: kill the leader, then partition the new one, under load
deploy/monitoring/    Prometheus and Grafana (dashboard JSON in grafana/dashboards/)
```

## How it fits together

- **StatefulSet.** Pods `stormproof-0..2` keep their names, DNS names and
  volumes across restarts, which is what a Raft server needs: its ID and
  its log must survive. Every pod gets the same arguments;
  `-id-from-hostname` turns `stormproof-N` into server N+1. All pods start
  at once (`podManagementPolicy: Parallel`), since none can become ready
  before a majority is up to elect a leader.
- **Peer discovery** is a headless Service, `stormproof-peers`, giving
  each pod a DNS name (`stormproof-0.stormproof-peers`). It publishes
  pods before they are ready, because they only get ready after finding
  each other.
- **Probes.** Readiness is `/readyz` (the server knows a leader), so the
  client Service only routes to servers that can serve or redirect a
  request; a partitioned server drops out. Liveness is `/healthz` (the
  event loop runs); a server that stopped itself after a storage failure
  gets restarted.
- **PodDisruptionBudget**: at most one server down at a time for voluntary
  disruptions, so a majority always remains.
- **Monitoring**: Prometheus scrapes each pod by its DNS name every 2 s;
  Grafana has the StormProof dashboard preloaded (leader and term, leader
  changes, elections, commit rate, requests and latency, replication and
  apply lag, batch sizes, dropped messages).

## Running it

On a local cluster (for example [k3d](https://k3d.io) or kind on a machine
with cgroup v2), build and load the image, then apply:

```bash
docker build -f deploy/docker/Dockerfile -t stormproof:latest .
kind load docker-image stormproof:latest          # or: k3d image import stormproof:latest
kubectl apply -k deploy/k8s/overlays/local
kubectl rollout status statefulset/stormproof
kubectl port-forward svc/grafana 3000:3000        # http://localhost:3000
IMAGE=stormproof:latest deploy/k8s/chaos.sh
kubectl delete -k deploy/k8s/overlays/local && kubectl delete pvc -l app=stormproof
```

On AWS, see [docs/aws.md](../../docs/aws.md).

## What has been tested

All objects (both overlays, the Job and the NetworkPolicy) were accepted by
a real Kubernetes API server (k3s v1.34.13), with `kubectl apply` and
server-side dry runs. Running the pods was not possible in the development
container: its kernel doesn't let nested containers set their OOM score,
which every pod needs. So the deployment and `chaos.sh` are untested on a
running cluster until the first run on AWS (M11).
