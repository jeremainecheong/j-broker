#!/usr/bin/env bash
# P7.12 — compound chaos scenario.
#
# Runs group churn + produce load concurrently. Today this is just the
# group-churn wrapper because the Phase 6 broker-kill chaos script
# (`kill-leader-mid-produce.sh`) hasn't been ported into the shell-script
# layer yet — group churn by itself already exercises the critical
# interactions (rolling rebalance while the producer hammers partitions).
# When a broker-kill scenario wrapper exists this script will background
# that alongside the churn run.
#
set -euo pipefail

DURATION_SECONDS="${1:-600}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "== scenario-group-churn-under-load: ${DURATION_SECONDS}s =="
"${SCRIPT_DIR}/group-churn.sh" "${DURATION_SECONDS}"
