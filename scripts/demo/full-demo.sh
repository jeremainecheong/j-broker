#!/usr/bin/env bash
# One command, four acts: bring up the 3-broker cluster, run plain and
# transactional workloads against it, kill a partition leader mid-flow, and
# audit the exactly-once contract at the end.
#
#   scripts/demo/full-demo.sh
#
# Optional environment:
#   DEMO_MONITORING=1     also start Prometheus + Grafana (monitoring profile)
#   DEMO_LOG_DIR=DIR      where demo client logs land (default /tmp/jbroker-demo)
#   DEMO_EVENTS_COUNT/DEMO_EVENTS_RATE       Act 1 workload size and pace
#   DEMO_PIPELINE_COUNT/DEMO_PIPELINE_RATE   Act 2 workload size and pace
#
# The demo is rerunnable: it clears its own topics, consumer groups, and any
# stray demo client processes from a previous run, and heals a broker left
# stopped by an interrupted run (docker compose up restarts it). The cluster
# is left running at the end. Tear down with:  docker compose down -v
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

ADMIN=${ADMIN:-http://localhost:15672}
BOOTSTRAP=${BOOTSTRAP:-localhost:9092,localhost:9093,localhost:9094}
CLI="$ROOT/broker-app/build/install/broker-app/bin/broker-app"
LOG_DIR=${DEMO_LOG_DIR:-/tmp/jbroker-demo}
EVENTS_COUNT=${DEMO_EVENTS_COUNT:-60000}
EVENTS_RATE=${DEMO_EVENTS_RATE:-200}
PIPELINE_COUNT=${DEMO_PIPELINE_COUNT:-1000}
PIPELINE_RATE=${DEMO_PIPELINE_RATE:-40}

COMPOSE=(docker compose)
if [ "${DEMO_MONITORING:-0}" = "1" ]; then
  # The overlay repoints Prometheus at the compose admin-app on host port
  # 15672 (scripts/monitoring/prometheus.yml targets an admin-app running
  # straight on the host instead).
  COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.monitoring.yml
    -f scripts/demo/monitoring-demo.override.yml --profile monitoring)
fi

# ---------------------------------------------------------------- helpers

banner() {
  echo
  echo "================================================================================"
  echo "$1"
  echo "================================================================================"
}

say() { echo "  $*"; }

indent() { sed 's/^/    /'; }

api() { curl -sS --max-time 10 "$@"; }

# wait_for <description> <timeout-seconds> <predicate...>
wait_for() {
  local desc=$1 timeout=$2
  shift 2
  local deadline=$(($(date +%s) + timeout))
  until "$@"; do
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "error: timed out after ${timeout}s waiting for: $desc" >&2
      exit 1
    fi
    sleep 2
  done
}

admin_healthy() {
  api -o /dev/null -w '%{http_code}' "$ADMIN/actuator/health" 2>/dev/null | grep -q 200
}

alive_brokers() {
  api "$ADMIN/api/v1/cluster" 2>/dev/null | python3 -c '
import json, sys
print(sum(1 for n in json.load(sys.stdin)["nodes"] if n["alive"]))' 2>/dev/null || echo 0
}

all_brokers_alive() { [ "$(alive_brokers)" = "3" ]; }

controller_id() {
  api "$ADMIN/api/v1/cluster" | python3 -c '
import json, sys
print(json.load(sys.stdin)["controller_id"])'
}

cluster_view() {
  api "$ADMIN/api/v1/cluster" | python3 -c '
import json, sys
d = json.load(sys.stdin)
for n in sorted(d["nodes"], key=lambda x: x["broker_id"]):
    state = "alive" if n["alive"] else "DOWN"
    print("broker %d  %s:%d  %s" % (n["broker_id"], n["host"], n["port"], state))'
}

demo_topics_absent() {
  api "$ADMIN/api/v1/topics" 2>/dev/null | python3 -c '
import json, sys
names = {t["name"] for t in json.load(sys.stdin)}
sys.exit(1 if names & {"demo-events", "demo-source", "demo-sink"} else 0)' 2>/dev/null
}

topic_ready() { # <topic> — every partition has a leader and a full ISR
  api "$ADMIN/api/v1/topics/$1" 2>/dev/null | python3 -c '
import json, sys
d = json.load(sys.stdin)
ps = d.get("partition_states", [])
ok = bool(ps) and all(p["leader"] > 0 and len(p["isr"]) == 3 for p in ps)
sys.exit(0 if ok else 1)' 2>/dev/null
}

leader_of() { # <topic> <partition>
  api "$ADMIN/api/v1/topics/$1" | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(next(p['leader'] for p in d['partition_states'] if p['partition'] == $2))"
}

isr_full() { # <topic> <partition>
  api "$ADMIN/api/v1/topics/$1" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
isr = next(p['isr'] for p in d['partition_states'] if p['partition'] == $2)
sys.exit(0 if len(isr) == 3 else 1)" 2>/dev/null
}

create_topic() { # <name> <partitions>
  api -X POST -H 'Content-Type: application/json' \
    -d "{\"name\":\"$1\",\"partitions\":$2,\"replication_factor\":3}" \
    -o /dev/null -w "    create $1: HTTP %{http_code}\n" "$ADMIN/api/v1/topics"
}

delete_group() { # <group> — retries while a stale session from a previous run expires
  local group=$1 code
  local deadline=$(($(date +%s) + 90))
  while :; do
    code=$(api -X DELETE -o /dev/null -w '%{http_code}' "$ADMIN/api/v1/consumer-groups/$group")
    case "$code" in
      2* | 404) return 0 ;;
    esac
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "error: could not delete consumer group $group (HTTP $code)" >&2
      exit 1
    fi
    say "group $group still has a session from a previous run (HTTP $code); waiting for it to expire..."
    sleep 5
  done
}

pipeline_total() {
  grep -o 'total [0-9]*' "$LOG_DIR/pipeline.log" 2>/dev/null | tail -1 | awk '{print $2}' | grep . || echo 0
}

drain_total() {
  grep -o 'total [0-9]* records' "$LOG_DIR/drain-events.log" 2>/dev/null | tail -1 | awk '{print $2}' | grep . || echo 0
}

drain_moving() { [ "$(drain_total)" -gt 0 ]; }

pipeline_started() { grep -q 'committed' "$LOG_DIR/pipeline.log" 2>/dev/null; }

pipeline_progressed() { [ "$(pipeline_total)" -ge "$1" ]; }

process_gone() { ! kill -0 "$1" 2>/dev/null; }

CLIENT_PIDS=""
cleanup() {
  for pid in $CLIENT_PIDS; do kill "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT

# ---------------------------------------------------------------- prologue

banner "j-broker full demo: replicated cluster, transactions, a broker kill, exactly-once"
say "Repo:        $ROOT"
say "Client logs: $LOG_DIR"

for cmd in docker python3 curl; do
  command -v "$cmd" >/dev/null || {
    echo "error: $cmd is required" >&2
    exit 1
  }
done
docker info >/dev/null 2>&1 || {
  echo "error: the Docker daemon is not reachable" >&2
  exit 1
}

rm -rf "$LOG_DIR"
mkdir -p "$LOG_DIR"

# Stray demo clients from an interrupted previous run would fight over the
# consumer groups this run is about to reset.
pkill -f 'jbroker.app.BrokerApp demo' 2>/dev/null || true

say "Building the CLI (./gradlew :broker-app:installDist; incremental after the first run)..."
./gradlew -q :broker-app:installDist
[ -x "$CLI" ] || {
  echo "error: CLI missing at $CLI after build" >&2
  exit 1
}

say "Starting the cluster (docker compose up -d --build; cached after the first run)..."
"${COMPOSE[@]}" up -d --build
wait_for "the admin UI at $ADMIN" 600 admin_healthy
wait_for "all 3 brokers alive" 180 all_brokers_alive
say "Cluster up: 3 brokers alive, admin UI answering at $ADMIN."

say "Resetting demo topics and consumer groups from any previous run..."
for group in demo-consumers demo-pipeline demo-audit-src demo-audit-sink; do
  delete_group "$group"
done
for topic in demo-events demo-source demo-sink; do
  api -X DELETE -o /dev/null -w "    delete $topic: HTTP %{http_code}\n" "$ADMIN/api/v1/topics/$topic"
done
wait_for "old demo topics to be gone" 60 demo_topics_absent
create_topic demo-events 3
create_topic demo-source 1
create_topic demo-sink 1
for topic in demo-events demo-source demo-sink; do
  wait_for "$topic to have leaders and a full ISR" 60 topic_ready "$topic"
done
say "Topics ready: demo-events (3 partitions), demo-source, demo-sink (1 each), all rf=3."

# ---------------------------------------------------------------- act 1

banner "Act 1: steady flow - a producer and a consumer group on demo-events"
say "Producer: $EVENTS_COUNT records at ~$EVENTS_RATE/s, idempotent acks=all, spread over 3 partitions."
say "Consumer: group demo-consumers, committing offsets as it goes."
"$CLI" demo feed --bootstrap "$BOOTSTRAP" --topic demo-events \
  --count "$EVENTS_COUNT" --rate "$EVENTS_RATE" --partitions 3 --prefix event \
  >"$LOG_DIR/feed-events.log" 2>&1 &
FEED_EVENTS_PID=$!
CLIENT_PIDS="$CLIENT_PIDS $FEED_EVENTS_PID"
"$CLI" demo drain --bootstrap "$BOOTSTRAP" --group demo-consumers --topic demo-events \
  >"$LOG_DIR/drain-events.log" 2>&1 &
DRAIN_PID=$!
CLIENT_PIDS="$CLIENT_PIDS $DRAIN_PID"
wait_for "group demo-consumers to start consuming" 90 drain_moving
say "Both run in the background for the rest of the demo. Current throughput lines:"
tail -n 2 "$LOG_DIR/feed-events.log" | indent
tail -n 2 "$LOG_DIR/drain-events.log" | indent

# ---------------------------------------------------------------- act 2

banner "Act 2: transactional consume-transform-produce - demo-source to demo-sink"
say "Feeding $PIPELINE_COUNT records into demo-source at ~$PIPELINE_RATE/s."
"$CLI" demo feed --bootstrap "$BOOTSTRAP" --topic demo-source \
  --count "$PIPELINE_COUNT" --rate "$PIPELINE_RATE" --partitions 1 --prefix order \
  >"$LOG_DIR/feed-source.log" 2>&1 &
FEED_SOURCE_PID=$!
CLIENT_PIDS="$CLIENT_PIDS $FEED_SOURCE_PID"
say "Pipeline: read_committed consume from demo-source, transform, transactional produce to"
say "demo-sink; the source offsets commit inside the same transaction. The loop body has no"
say "error handling - retries and aborts belong to the transactional client."
"$CLI" demo pipeline --bootstrap "$BOOTSTRAP" --source demo-source --sink demo-sink \
  --group demo-pipeline --txn-id demo-pipeline-txn --expected "$PIPELINE_COUNT" \
  >"$LOG_DIR/pipeline.log" 2>&1 &
PIPELINE_PID=$!
CLIENT_PIDS="$CLIENT_PIDS $PIPELINE_PID"
wait_for "the first committed transaction" 90 pipeline_started
say "First transactions committed:"
grep 'committed' "$LOG_DIR/pipeline.log" | head -n 3 | indent

# ---------------------------------------------------------------- act 3

banner "Act 3: kill the leader of the transactional sink partition mid-flow"
LEADER=$(leader_of demo-sink 0)
CONTROLLER=$(controller_id)
say "demo-sink-0 is led by broker $LEADER (container jbroker-broker$LEADER)."
if [ "$LEADER" = "$CONTROLLER" ]; then
  say "Broker $LEADER is also the current Raft leader, so a metadata election runs too."
fi
P_BEFORE=$(pipeline_total)
D_BEFORE=$(drain_total)
say "Progress before the kill: pipeline $P_BEFORE/$PIPELINE_COUNT moved, demo-consumers $D_BEFORE consumed."
say "Running: docker kill jbroker-broker$LEADER"
docker kill "jbroker-broker$LEADER" >/dev/null
say "Broker $LEADER is down (SIGKILL, no graceful handoff). Waiting for the pipeline to keep"
say "committing on the two survivors (leader failover + acks=all on the remaining ISR)..."
wait_for "the pipeline to move 100+ records past the kill" 240 pipeline_progressed $((P_BEFORE + 100))
say "Cluster view during the outage:"
cluster_view | indent
say "Progress now: pipeline $(pipeline_total)/$PIPELINE_COUNT moved, demo-consumers $(drain_total) consumed."
say "Both workloads kept moving without broker $LEADER."
say "Running: docker start jbroker-broker$LEADER"
docker start "jbroker-broker$LEADER" >/dev/null
wait_for "broker $LEADER to rejoin the cluster" 180 all_brokers_alive
wait_for "demo-sink-0's ISR to regain all 3 replicas" 240 isr_full demo-sink 0
say "Broker $LEADER caught up from the new leader's log and rejoined demo-sink-0's ISR."

# ---------------------------------------------------------------- act 4

banner "Act 4: the exactly-once audit"
say "Waiting for the pipeline to finish all $PIPELINE_COUNT records..."
wait_for "the source feed to finish" 240 process_gone "$FEED_SOURCE_PID"
wait "$FEED_SOURCE_PID" || {
  echo "error: the demo-source feed failed; log tail:" >&2
  tail -n 10 "$LOG_DIR/feed-source.log" >&2
  exit 1
}
wait_for "the pipeline to finish" 300 process_gone "$PIPELINE_PID"
wait "$PIPELINE_PID" || {
  echo "error: the pipeline failed; log tail:" >&2
  tail -n 10 "$LOG_DIR/pipeline.log" >&2
  exit 1
}
tail -n 1 "$LOG_DIR/pipeline.log" | indent
say "Auditing: re-read demo-source and demo-sink (read_committed) end to end and compare."
if ! "$CLI" demo verify --bootstrap "$BOOTSTRAP" --source demo-source --sink demo-sink \
  --group demo-pipeline --expected "$PIPELINE_COUNT" | indent; then
  echo "error: exactly-once audit FAILED" >&2
  exit 1
fi
say "Every source record reached the sink exactly once, in order, across the broker kill."

say "Stopping the Act 1 clients..."
kill "$FEED_EVENTS_PID" "$DRAIN_PID" 2>/dev/null || true
wait "$FEED_EVENTS_PID" 2>/dev/null || true
wait "$DRAIN_PID" 2>/dev/null || true
say "Act 1 final counts:"
tail -n 1 "$LOG_DIR/feed-events.log" | indent
grep 'total' "$LOG_DIR/drain-events.log" | tail -n 1 | indent

# ---------------------------------------------------------------- epilogue

banner "Demo complete - the cluster is still running"
say "Admin UI: $ADMIN"
say "  /        cluster overview - topology with broker $LEADER back, controller badge, events rail"
say "  /topics  demo-events, demo-source, demo-sink - per-partition leader, ISR, high watermark"
say "  /groups  demo-pipeline (transactional commits, lag 0) and demo-consumers"
say "  /raft    Raft term, commit index, and per-broker roles after the failover"
say "  /metrics produce/fetch throughput sparklines across the demo"
if [ "${DEMO_MONITORING:-0}" = "1" ]; then
  say "Grafana:    http://localhost:3000 (anonymous admin; two provisioned dashboards)"
  say "Prometheus: http://localhost:9091"
else
  say "Rerun with DEMO_MONITORING=1 to add Prometheus + Grafana."
fi
say "Demo client logs: $LOG_DIR"
say "Tear down everything: docker compose down -v"
