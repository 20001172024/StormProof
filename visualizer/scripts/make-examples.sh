#!/usr/bin/env bash
# Regenerates the example traces the visualizer site ships with, from the
# simulator at the current commit. Run from the repository root:
#
#   visualizer/scripts/make-examples.sh
#
# Each trace is a real simulator run; the seeds were picked because they
# show something worth watching. Short runs keep the files around 1 MB.
set -euo pipefail

out=visualizer/public/traces
mkdir -p "$out"
sim() { go run ./sim/cmd/stormproof-sim -duration 5s -quiet 2.5s -clients 2 "$@"; }

# The simulator exits non-zero when it finds a violation, which the last
# example is meant to do.
sim -seed 5 -profile crashes -trace "$out/leader-crash.jsonl"
sim -seed 35 -profile partitions -trace "$out/leader-isolated.jsonl"
sim -seed 4 -profile partitions -bug no-prevote -trace "$out/follower-isolated.jsonl"
sim -seed 4 -profile partitions -trace "$out/follower-isolated-prevote.jsonl"
sim -seed 2 -profile calm -membership -trace "$out/membership.jsonl"
sim -seed 13 -profile crashes -bug fsync -trace "$out/bug-fsync.jsonl" || true
