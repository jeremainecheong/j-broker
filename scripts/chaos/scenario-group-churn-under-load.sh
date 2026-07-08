#!/usr/bin/env bash
# Compound chaos scenario.
#
# Runs group churn + produce load concurrently — the consumer-group-
# rebalance flavour of compound chaos (rolling rebalance while the
# producer hammers partitions). The broker-kill-under-load flavour is
# its own scenario: `scenario-chaos-with-load.sh`, which
# SIGKILLs a broker every 30s under sustained idempotent acks=all load
# and asserts zero lost / zero duplicated records.
#
set -euo pipefail

DURATION_SECONDS="${1:-600}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "== scenario-group-churn-under-load: ${DURATION_SECONDS}s =="
"${SCRIPT_DIR}/group-churn.sh" "${DURATION_SECONDS}"
