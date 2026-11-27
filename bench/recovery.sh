#!/usr/bin/env bash
# Measures recovery from a leader crash on a docker-compose cluster: REPS
# times, under a steady write load, kill the leader's container and record
#
#   election: time until another server reports being leader (its /metrics,
#             polled every 10 ms)
#   writes:   time until a client write that started after the kill
#             completes successfully (from the load generator's
#             per-operation timeline). Writes started before the kill
#             don't count: their replies may have left the old leader just
#             before it died.
#
# both measured from the kill. The killed server is restarted between
# repetitions. Containers share the host's clock, so the timestamps compare.
#
#   bench/recovery.sh            # 3 servers, 5 repetitions
#   NODES=5 REPS=10 bench/recovery.sh
set -euo pipefail
cd "$(dirname "$0")/../deploy/docker"

NODES=${NODES:-3}
REPS=${REPS:-5}
LABEL=${LABEL:-recovery-$NODES}
case $NODES in
3) FILE=docker-compose.yml ;;
5) FILE=docker-compose.5.yml ;;
*) echo "NODES must be 3 or 5" >&2; exit 1 ;;
esac
dc() { docker compose -f "$FILE" "$@"; }
cleanup() { dc down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

is_leader() { wget -qO- "http://localhost:$((9100 + $1))/metrics" 2>/dev/null | grep -q "^stormproof_raft_is_leader{node=\"$1\"} 1"; }
leader() {
	for _ in $(seq 1 100); do
		for i in $(seq 1 "$NODES"); do
			if is_leader "$i"; then echo "$i"; return; fi
		done
		sleep 0.1
	done
	echo "no leader" >&2
	return 1
}
now_ns() { date +%s%N; }

mkdir -p "results/$LABEL"
echo "== starting $NODES servers"
cleanup
dc up -d --wait $(seq -f 'node%g' 1 "$NODES") >/dev/null

out="results/$LABEL/summary.csv"
echo "rep,killed,new_leader,election_ms,writes_ms" >"$out"
for r in $(seq 1 "$REPS"); do
	L=$(leader)
	tl="$LABEL/timeline-$r.csv"
	# Writes only, 4 clients, long enough to cover the kill and the recovery.
	dc run --rm -T bench -clients 4 -reads 0 -warmup 1s -duration 12s -op-timeout 10s \
		-attempt-timeout 300ms -label "$LABEL rep $r" -timeline "$tl" >/dev/null &
	bench=$!
	sleep 5

	kill_ns=$(now_ns)
	dc kill "node$L" >/dev/null 2>&1
	new=""
	while [ -z "$new" ]; do
		for i in $(seq 1 "$NODES"); do
			if [ "$i" != "$L" ] && is_leader "$i"; then new=$i; break; fi
		done
		[ -n "$new" ] || sleep 0.01
	done
	elected_ns=$(now_ns)
	wait "$bench"

	# The first write started after the kill to succeed: start + latency.
	writes_ns=$(awk -F, -v k="$kill_ns" 'NR > 1 && $4 == "true" && $1 >= k { done = $1 + $3 * 1000; if (min == "" || done < min) min = done }
		END { if (min == "") print "never"; else printf "%d", min }' "results/$tl")
	election_ms=$(( (elected_ns - kill_ns) / 1000000 ))
	if [ "$writes_ns" = never ]; then writes_ms=never; else writes_ms=$(( (writes_ns - kill_ns) / 1000000 )); fi
	echo "== rep $r: killed node$L, node$new elected after ${election_ms} ms, writes back after ${writes_ms} ms"
	echo "$r,$L,$new,$election_ms,$writes_ms" >>"$out"

	dc start "node$L" >/dev/null
	dc up -d --wait "node$L" >/dev/null
done
echo "== results in deploy/docker/$out"
