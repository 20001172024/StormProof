#!/usr/bin/env bash
# Runs the benchmark matrix against a docker-compose cluster: for each
# workload and client count, REPS runs, each result saved as JSON under
# deploy/docker/results/<label>/. Starts the cluster fresh and tears it
# down (volumes included) at the end.
#
#   bench/run.sh                         # 3 servers, default matrix
#   NODES=5 bench/run.sh                 # 5 servers
#   LABEL=before-batching CLIENTS="1 8 32" WORKLOADS="write" bench/run.sh
#
# Workloads: write (10% reads), mixed (50%), read (90%). Reads are
# linearizable Gets. Summarize with: go run ./bench/cmd/benchtable deploy/docker/results/<label>
set -euo pipefail
cd "$(dirname "$0")/../deploy/docker"

NODES=${NODES:-3}
LABEL=${LABEL:-${NODES}-servers}
CLIENTS=${CLIENTS:-32}
WORKLOADS=${WORKLOADS:-"write mixed read"}
REPS=${REPS:-3}
DURATION=${DURATION:-20s}
WARMUP=${WARMUP:-5s}

case $NODES in
3) FILE=docker-compose.yml ;;
5) FILE=docker-compose.5.yml ;;
*) echo "NODES must be 3 or 5" >&2; exit 1 ;;
esac
dc() { docker compose -f "$FILE" "$@"; }
cleanup() { dc down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

reads() {
	case $1 in
	write) echo 0.1 ;;
	mixed) echo 0.5 ;;
	read) echo 0.9 ;;
	*) echo "unknown workload $1" >&2; exit 1 ;;
	esac
}

mkdir -p "results/$LABEL"
echo "== starting $NODES servers"
cleanup
dc up -d --wait $(seq -f 'node%g' 1 "$NODES") >/dev/null

for w in $WORKLOADS; do
	for c in $CLIENTS; do
		for r in $(seq 1 "$REPS"); do
			name="$w-c$c-r$r"
			echo "== $LABEL: $name"
			dc run --rm -T bench -clients "$c" -reads "$(reads "$w")" -duration "$DURATION" -warmup "$WARMUP" \
				-label "$LABEL $name" -out "$LABEL/$name.json" | tail -n 3
		done
	done
done
echo "== results in deploy/docker/results/$LABEL"
