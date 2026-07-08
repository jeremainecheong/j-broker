#!/usr/bin/env bash
# Chaos with load — the capstone durability soak.
#
# Steady idempotent acks=all producer against the docker-compose 3-broker
# cluster while a chaos loop `docker kill`s a broker every KILL_INTERVAL
# seconds (compose's `restart: unless-stopped` revives it, so each cycle
# is a real SIGKILL + cold rejoin). After DURATION seconds the verifier
# re-reads every partition from offset 0 and asserts the invariant:
#
#   every ACKED record consumed exactly once; NO record consumed twice.
#
# Records whose ack never reached the producer (broker died mid-produce)
# may appear at most once — the producer retries them with the same
# idempotent (pid, partition, sequence), so the broker's dedup window is
# what keeps retries from duplicating.
#
# Usage:
#   scripts/chaos/scenario-chaos-with-load.sh [DURATION_SECONDS]
#
#   DURATION_SECONDS  soak length (default 600)
#   KILL_INTERVAL     seconds between broker kills (default 30)
#   RATE              produced records/second across partitions (default 200)
#
# Expects `./gradlew :bench:installDist :broker-app:installDist` to have
# run. Starts the compose cluster if it isn't already up (builds the
# image on first use).

set -euo pipefail

DURATION_SECONDS="${1:-600}"
KILL_INTERVAL="${KILL_INTERVAL:-30}"
RATE="${RATE:-200}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BENCH="$REPO_ROOT/bench/build/install/bench/bin/bench"
BROKER_APP="$REPO_ROOT/broker-app/build/install/broker-app/bin/broker-app"
BROKERS="localhost:9092,localhost:9093,localhost:9094"

if [[ ! -x "$BENCH" || ! -x "$BROKER_APP" ]]; then
    echo "bench/broker-app binaries missing — run ./gradlew :bench:installDist :broker-app:installDist" >&2
    exit 2
fi

echo "== scenario-chaos-with-load: ${DURATION_SECONDS}s, kill every ${KILL_INTERVAL}s, ${RATE} rec/s =="

# --- cluster up -------------------------------------------------------
if ! docker image inspect jbroker-broker:local >/dev/null 2>&1; then
    echo "-- building broker image (first run)"
    (cd "$REPO_ROOT" && docker compose build)
fi
# --force-recreate: long-running containers from an older image/build
# otherwise survive `up -d` untouched and the soak tests stale code.
(cd "$REPO_ROOT" && docker compose up -d --force-recreate broker1 broker2 broker3)

echo "-- waiting for all three brokers to serve metadata"
for port in 9092 9093 9094; do
    deadline=$((SECONDS + 120))
    until "$BROKER_APP" topics list --broker "localhost:$port" >/dev/null 2>&1; do
        if ((SECONDS > deadline)); then
            echo "broker on :$port did not become ready within 120s" >&2
            exit 3
        fi
        sleep 2
    done
done

# --- topic + producer -------------------------------------------------
# createTopic must land on the Raft leader; rotate until one accepts.
TOPIC="soak-$(date +%s)"
PARTITIONS=3
created=""
for attempt in 1 2 3 4 5 6; do
    for port in 9092 9093 9094; do
        if "$BROKER_APP" topics create --broker "localhost:$port" \
            --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor 3 >/dev/null 2>&1; then
            created="localhost:$port"
            break 2
        fi
    done
    sleep 5
done
if [[ -z "$created" ]]; then
    echo "createTopic failed on every broker across 6 rounds" >&2
    exit 4
fi
echo "-- topic $TOPIC created via $created"

ACKED_FILE="$(mktemp -t soak-acked)"
"$BENCH" soak-produce \
    --brokers "$BROKERS" --topic "$TOPIC" --partitions "$PARTITIONS" \
    --duration-seconds "$DURATION_SECONDS" --rate "$RATE" \
    --acked-file "$ACKED_FILE" &
PRODUCER_PID=$!

# --- chaos loop -------------------------------------------------------
# Round-robin SIGKILL one broker at a time, leave it dead for
# DEAD_WINDOW seconds, then `docker start` it explicitly — Docker
# ignores the restart policy for manually killed containers, so
# without the start the cluster loses quorum by the second kill.
# KILL_INTERVAL spacing keeps at most one broker down at a time.
DEAD_WINDOW="${DEAD_WINDOW:-10}"
victim=1
kills=0
while kill -0 "$PRODUCER_PID" 2>/dev/null; do
    for ((i = 0; i < KILL_INTERVAL; i++)); do
        kill -0 "$PRODUCER_PID" 2>/dev/null || break 2
        sleep 1
    done
    echo "-- chaos: docker kill jbroker-broker$victim (kill #$((++kills)))"
    docker kill "jbroker-broker$victim" >/dev/null || true
    sleep "$DEAD_WINDOW"
    docker start "jbroker-broker$victim" >/dev/null || true
    victim=$((victim % 3 + 1))
done

wait "$PRODUCER_PID"
echo "-- producer finished after $kills broker kills; letting replication settle"
sleep 10

# --- verify -----------------------------------------------------------
"$BENCH" soak-verify \
    --brokers "$BROKERS" --topic "$TOPIC" --partitions "$PARTITIONS" \
    --acked-file "$ACKED_FILE" --settle-seconds 90

echo "== scenario-chaos-with-load: PASS (${DURATION_SECONDS}s, $kills kills, acked file: $ACKED_FILE) =="
