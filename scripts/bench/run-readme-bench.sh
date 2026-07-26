#!/usr/bin/env bash
# Append one full, environment-stamped benchmark pass to
# docs/bench/results.csv (append-only history — never truncated) and
# distill this run's rows into docs/bench/current-snapshot.csv
# (gitignored) for README table generation.
#
# Usage:
#   scripts/bench/run-readme-bench.sh [BROKER_HOST:PORT]
#
# BROKER defaults to 127.0.0.1:9092 and must be a running single broker —
# it hosts the RF=1 baseline and flush-policy scenarios. The RF=3
# scenarios spin up an in-process loopback 3-broker cluster per
# invocation; set BENCH_BOOTSTRAP=h:p,h:p,h:p to target an external
# cluster instead (the bench refuses to run when fewer brokers are
# reachable than the replication factor requires).
#
# Env knobs:
#   BENCH_DURATION_S  measured seconds per scenario (default 30; warmup
#                     of max(10s, 20%) is added on top and excluded)
#   BENCH_WARMUP_S    warmup override — plumbing smokes only; published
#                     rows must keep the default warmup
#   BENCH_BOOTSTRAP   external cluster endpoints for the RF=3 scenarios
#
# Disk: the high-throughput scenarios write log segments at hundreds of
# MB/s. Script-created topics carry retention.bytes=2147483648 so each
# converges to ~2 GiB on disk, but the broker's data volume still needs
# real headroom (the broker refuses produces below its low-space
# watermark).
#
# Expects `bench/build/install/bench/bin/bench` to be installed — run
# `./gradlew :bench:installDist` first.
#
# This script writes measured rows; it bakes in no expected numbers.
# Humans read the CSV — percentile cells the harness could not back with
# enough samples are empty, with the reason on stderr.

set -euo pipefail

BROKER="${1:-127.0.0.1:9092}"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CSV="$REPO_ROOT/docs/bench/results.csv"
SNAPSHOT="$REPO_ROOT/docs/bench/current-snapshot.csv"
BENCH="$REPO_ROOT/bench/build/install/bench/bin/bench"
DURATION="${BENCH_DURATION_S:-30}"
SIZES=(256 1024 4096)
# Bounds each bench topic's on-disk footprint; segments beyond the budget
# are deleted in the background, which is also the realistic production
# configuration.
RETENTION=(--config retention.bytes=2147483648)
WARMUP_ARGS=()
if [[ -n "${BENCH_WARMUP_S:-}" ]]; then
    WARMUP_ARGS=(--warmup-s "$BENCH_WARMUP_S")
fi

if [[ ! -x "$BENCH" ]]; then
    echo "bench binary not found at $BENCH — run ./gradlew :bench:installDist first" >&2
    exit 2
fi

mkdir -p "$(dirname "$CSV")"
export BENCH_GIT_SHA="$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
RUN_START="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

# Bench stdout carries only summaries (broker/gRPC logging goes to
# stderr at WARN); any non-zero exit — unreachable broker, refused
# topology — aborts the whole pass via set -e.
run() {
    "$BENCH" "$@"
}

# ---- (a) single-broker baseline: 1 partition, RF=1 ----------------------
for size in "${SIZES[@]}"; do
    topic="bench-p${size}"
    echo "--- baseline payload ${size}B (topic ${topic}) ---"
    run create-topic --broker "$BROKER" --topic "$topic" --partitions 1 --rf 1 "${RETENTION[@]}"
    run producer       --broker "$BROKER" --topic "$topic" --partition 0 \
        --payload-size "$size" --duration-s "$DURATION" "${WARMUP_ARGS[@]}" --csv "$CSV"
    run batch-producer --broker "$BROKER" --topic "$topic" --partition 0 \
        --payload-size "$size" --duration-s "$DURATION" "${WARMUP_ARGS[@]}" --csv "$CSV"
    run produce-batch  --broker "$BROKER" --topic "$topic" --partition 0 \
        --payload-size "$size" --duration-s "$DURATION" "${WARMUP_ARGS[@]}" --csv "$CSV"
    run consumer       --broker "$BROKER" --topic "$topic" --partition 0 \
        --duration-s "$DURATION" "${WARMUP_ARGS[@]}" --csv "$CSV"
done

# ---- (b) replicated path: 3 partitions, RF=3 ----------------------------
for size in "${SIZES[@]}"; do
    echo "--- cluster payload ${size}B (3 partitions, RF=3) ---"
    run acks-all --acks 1   --partitions 3 \
        --payload-size "$size" --duration-s "$DURATION" "${WARMUP_ARGS[@]}" --csv "$CSV"
    run acks-all --acks all --partitions 3 \
        --payload-size "$size" --duration-s "$DURATION" "${WARMUP_ARGS[@]}" --csv "$CSV"
    run acks-all --acks all --min-insync 2 --partitions 3 \
        --payload-size "$size" --duration-s "$DURATION" "${WARMUP_ARGS[@]}" --csv "$CSV"
done
echo "--- replication catch-up ---"
run replication --csv "$CSV"

# ---- (c) flush-policy comparison: eager fsync vs default, 1024B ---------
echo "--- flush policy: flush.messages=1 vs default (1024B) ---"
run create-topic --broker "$BROKER" --topic bench-flush-default --partitions 1 --rf 1 "${RETENTION[@]}"
run create-topic --broker "$BROKER" --topic bench-flush-every --partitions 1 --rf 1 \
    "${RETENTION[@]}" --config flush.messages=1
run producer --broker "$BROKER" --topic bench-flush-default --partition 0 \
    --payload-size 1024 --duration-s "$DURATION" "${WARMUP_ARGS[@]}" --csv "$CSV"
run producer --broker "$BROKER" --topic bench-flush-every --partition 0 \
    --payload-size 1024 --flush-messages 1 --duration-s "$DURATION" "${WARMUP_ARGS[@]}" --csv "$CSV"

# ---- snapshot: header + only this run's rows ----------------------------
head -n1 "$CSV" > "$SNAPSHOT"
awk -F, -v start="$RUN_START" 'NR > 1 && $1 >= start' "$CSV" >> "$SNAPSHOT"

echo
echo "--- $SNAPSHOT ---"
# Display only: mark empty cells so column alignment survives; the file
# itself keeps true empty cells.
awk -F, -v OFS=, '{for (i = 1; i <= NF; i++) if ($i == "") $i = "-"; print}' "$SNAPSHOT" | column -s, -t
