#!/usr/bin/env bash
# Smoke test for the docker-compose cluster: start it, write and read
# through the CLI, kill the leader's container, check writes still work,
# bring it back and check it caught up, then tear everything down.
#
#   deploy/docker/smoke.sh                  # 3 servers
#   deploy/docker/smoke.sh docker-compose.5.yml
#   KEEP=1 deploy/docker/smoke.sh           # leave the cluster running
set -euo pipefail
cd "$(dirname "$0")"
FILE=${1:-docker-compose.yml}
N=$(grep -c '^  node[0-9]*:$' "$FILE")
dc() { docker compose -f "$FILE" "$@"; }
cli() { dc run --rm -T cli "$@"; }
# leader prints the ID of the server whose metrics say it leads. There can
# briefly be none (during an election), so it retries for up to 10 s.
leader() {
	for _ in $(seq 1 50); do
		for i in $(seq 1 "$N"); do
			if wget -qO- "http://localhost:$((9100 + i))/metrics" 2>/dev/null | grep -q "^stormproof_raft_is_leader{node=\"$i\"} 1"; then
				echo "$i"
				return
			fi
		done
		sleep 0.2
	done
	echo "no server reports being the leader" >&2
	return 1
}
cleanup() { if [ -z "${KEEP:-}" ]; then dc down -v --remove-orphans >/dev/null 2>&1 || true; fi; }
trap cleanup EXIT

echo "== starting $N servers"
dc up -d --wait $(seq -f 'node%g' 1 "$N")
echo "== write and read"
cli put smoke hello
[ "$(cli get smoke)" = hello ] || { echo "read back the wrong value"; exit 1; }

L=$(leader) || exit 1
echo "== leader is node$L; killing its container"
dc kill "node$L" >/dev/null
start=$(date +%s%N)
cli put smoke after-kill
echo "   first write after the kill took $(( ($(date +%s%N) - start) / 1000000 )) ms (includes starting the CLI container)"
echo "   new leader: node$(leader)"

echo "== restarting node$L"
dc start "node$L" >/dev/null
dc up -d --wait "node$L" >/dev/null
[ "$(cli get smoke)" = after-kill ] || { echo "read back the wrong value"; exit 1; }
echo "== OK"
