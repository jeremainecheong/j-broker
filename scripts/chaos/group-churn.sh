#!/usr/bin/env bash
# Local 10-minute consumer-group churn sign-off.
#
# Drives `GroupChurnIT` at the spec-mandated 10-minute duration so a
# real dev can observe sustained rebalance + produce behaviour end-to-end.
# Excluded from default CI (the `chaos` JUnit tag gates it); invoke this
# script directly before merging consumer-group changes.
#
# Override duration via the first positional argument (in seconds):
#   ./scripts/chaos/group-churn.sh          # 600s (10 min)
#   ./scripts/chaos/group-churn.sh 60       # 1 min smoke
#
# Assertions (inside the IT):
#   - producer path runs clean (no RuntimeExceptions)
#   - brokers stay Role.LEADER/FOLLOWER/CANDIDATE throughout
#   - every produced record is consumed by at least one member
#
set -euo pipefail

DURATION_SECONDS="${1:-600}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

echo "== group-churn: ${DURATION_SECONDS}s =="
cd "${REPO_ROOT}"
JBROKER_CHURN_DURATION_SECONDS="${DURATION_SECONDS}" \
    ./gradlew :integration-tests:chaosTest \
    --tests "jbroker.it.GroupChurnIT.clusterSurvivesJoinLeaveChurnUnderSustainedProduceLoad" \
    --rerun-tasks
