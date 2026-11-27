#!/usr/bin/env bash
# Live chaos demo on a Kubernetes cluster running StormProof
# (deploy/k8s/overlays/*). While the load generator runs as a Job, it
#
#   1. deletes the leader's pod, and reports how long until another server
#      leads and the pod is back and caught up;
#   2. partitions the new leader with a NetworkPolicy (jobs/isolate.yaml)
#      for 20 s, reports the election among the others, then heals it.
#
# Watch it in Grafana (kubectl port-forward svc/grafana 3000:3000) while it
# runs. At the end it prints the load generator's results: errors should be
# zero, since the client library retries through failovers.
#
#   deploy/k8s/chaos.sh                   # image from the aws overlay (ghcr.io)
#   IMAGE=stormproof:latest deploy/k8s/chaos.sh   # a locally loaded image
set -euo pipefail
cd "$(dirname "$0")"

IMAGE=${IMAGE:-ghcr.io/20001172024/stormproof:latest}
PARTITION=${PARTITION:-20}
K=${KUBECTL:-kubectl}

ts() { date -u +%H:%M:%S; }
now_ms() { date +%s%3N; }
metric() { # metric POD NAME: a gauge from the pod's /metrics
	$K exec "$1" -c server -- wget -qO- localhost:9090/metrics 2>/dev/null | awk -v m="$2" '$1 ~ "^"m"[{ ]" || $1 == m { print $2; exit }'
}
leader() { # the pod reporting is_leader 1, excluding $1
	for i in 0 1 2; do
		p=stormproof-$i
		[ "$p" = "${1:-}" ] && continue
		if [ "$(metric "$p" stormproof_raft_is_leader)" = "1" ]; then echo "$p"; return; fi
	done
}
wait_leader() { # wait_leader EXCLUDE: until a pod other than EXCLUDE leads
	for _ in $(seq 1 300); do
		l=$(leader "${1:-}")
		if [ -n "$l" ]; then echo "$l"; return; fi
		sleep 0.1
	done
	echo "no leader after 30 s" >&2
	return 1
}

echo "$(ts) == starting the load generator (job/stormproof-bench)"
$K delete job stormproof-bench --ignore-not-found >/dev/null
sed "s|image: stormproof$|image: $IMAGE|" jobs/bench.yaml | $K apply -f - >/dev/null
$K wait --for=condition=Ready pod -l job-name=stormproof-bench --timeout=60s >/dev/null
sleep 10

L=$(wait_leader)
echo "$(ts) == 1. leader is $L (term $(metric "$L" stormproof_raft_term)); deleting its pod"
start=$(now_ms)
$K delete pod "$L" --grace-period=0 --force >/dev/null 2>&1
N=$(wait_leader "$L")
echo "$(ts)    $N leads term $(metric "$N" stormproof_raft_term), $(( $(now_ms) - start )) ms after the delete"
$K wait --for=condition=Ready "pod/$L" --timeout=120s >/dev/null
echo "$(ts)    $L is back and ready, $(( $(now_ms) - start )) ms after the delete"
sleep 10

L=$N
echo "$(ts) == 2. partitioning the leader, $L, for ${PARTITION}s"
$K label pod "$L" chaos=isolated --overwrite >/dev/null
$K apply -f jobs/isolate.yaml >/dev/null
start=$(now_ms)
N=$(wait_leader "$L")
echo "$(ts)    $N leads term $(metric "$N" stormproof_raft_term), $(( $(now_ms) - start )) ms after the partition"
echo "$(ts)    $L still thinks it leads: is_leader=$(metric "$L" stormproof_raft_is_leader), term $(metric "$L" stormproof_raft_term)"
sleep "$PARTITION"
echo "$(ts)    healing the partition"
$K delete -f jobs/isolate.yaml >/dev/null
$K label pod "$L" chaos- >/dev/null
for _ in $(seq 1 100); do
	[ "$(metric "$L" stormproof_raft_is_leader)" = "0" ] && break
	sleep 0.1
done
echo "$(ts)    $L: is_leader=$(metric "$L" stormproof_raft_is_leader), term $(metric "$L" stormproof_raft_term), commit $(metric "$L" stormproof_raft_commit_index) (leader's $(metric "$N" stormproof_raft_commit_index))"

echo "$(ts) == waiting for the load generator to finish"
$K wait --for=condition=Complete job/stormproof-bench --timeout=180s >/dev/null || true
$K logs job/stormproof-bench
