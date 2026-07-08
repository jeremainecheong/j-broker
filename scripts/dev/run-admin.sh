#!/usr/bin/env bash
# Start the admin-app against a local broker at 127.0.0.1:9092 (override with $BROKERS).
# Assumes `j-broker server` is already running; admin-app dials broker(s) via gRPC.
set -euo pipefail

BROKERS="${BROKERS:-localhost:9092}"
PORT="${PORT:-9090}"

cd "$(dirname "$0")/../.."
./gradlew :admin-app:bootRun \
  --args="--jbroker.admin.brokers=${BROKERS} --server.port=${PORT}"
