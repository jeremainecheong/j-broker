#!/usr/bin/env bash
# Seed a freshly-booted docker-compose cluster with realistic traffic for
# README screenshots. Creates three demo topics (orders, prices, audit-log),
# starts a producer loop + a consumer group, and prints a one-liner snapshot
# of cluster state once it's ready.
#
# Expects:
#   docker compose up -d
# already run from the repo root. (Chaos endpoints are not needed here;
# to enable them separately, set JBROKER_CHAOS_ENABLED=true plus
# JBROKER_CHAOS_PORT and JBROKER_CHAOS_TOKEN.)
#
# Tear down at end with:
#   docker stop jb-consumer jb-producer jb-prices && docker rm jb-consumer jb-producer jb-prices
#   docker compose down
set -euo pipefail

ADMIN=${ADMIN:-http://localhost:15672}

echo "waiting for admin @ ${ADMIN}…"
until curl -sS -o /dev/null -w '%{http_code}' "${ADMIN}/actuator/health" | grep -q 200; do sleep 2; done
echo "admin up"

echo "pruning old user topics (keeping __* internal)…"
for t in $(curl -sS "${ADMIN}/api/v1/topics" | python3 -c "import json,sys; [print(x['name']) for x in json.load(sys.stdin) if not x['name'].startswith('__')]"); do
  curl -sS -X DELETE "${ADMIN}/api/v1/topics/${t}" -o /dev/null -w "  delete ${t}: %{http_code}\n"
done

echo "creating demo topics…"
curl -sS -X POST -H 'Content-Type: application/json' \
  -d '{"name":"orders","partitions":6,"replication_factor":3,"config":{"retention.ms":"604800000"}}' \
  "${ADMIN}/api/v1/topics" -w 'orders: %{http_code}\n' -o /dev/null
curl -sS -X POST -H 'Content-Type: application/json' \
  -d '{"name":"prices","partitions":3,"replication_factor":3,"config":{"cleanup.policy":"compact","retention.ms":"86400000"}}' \
  "${ADMIN}/api/v1/topics" -w 'prices: %{http_code}\n' -o /dev/null
curl -sS -X POST -H 'Content-Type: application/json' \
  -d '{"name":"audit-log","partitions":3,"replication_factor":3,"config":{"retention.ms":"259200000"}}' \
  "${ADMIN}/api/v1/topics" -w 'audit-log: %{http_code}\n' -o /dev/null

echo "starting producer (orders + prices) on jbroker-net…"
docker rm -f jb-producer jb-prices jb-consumer 2>/dev/null || true

docker run -d --name jb-producer --network jbroker-net --entrypoint sh jbroker-broker:local -c '
  for i in $(seq 1 600); do
    for p in 0 1 2 3 4 5; do echo "order-$i-$p-$(date +%N)"; done \
      | /opt/jbroker/bin/broker-app produce --broker broker2:9092 --topic orders --partition $((i % 6)) > /dev/null 2>&1
    sleep 1
  done
' > /dev/null

docker run -d --name jb-prices --network jbroker-net --entrypoint sh jbroker-broker:local -c '
  for i in $(seq 1 400); do
    echo "BTC $(awk -v s=$RANDOM "BEGIN{srand(s); printf \"%.2f\",60000+rand()*5000}")" \
      | /opt/jbroker/bin/broker-app produce --broker broker2:9092 --topic prices --partition $((i % 3)) > /dev/null 2>&1
    echo "ETH $(awk -v s=$RANDOM "BEGIN{srand(s); printf \"%.2f\",3000+rand()*200}")" \
      | /opt/jbroker/bin/broker-app produce --broker broker2:9092 --topic prices --partition $((i % 3)) > /dev/null 2>&1
    sleep 1.5
  done
' > /dev/null

echo "starting consumer group checkout-processor subscribed to [orders, prices]…"
docker run -d --name jb-consumer --network jbroker-net --entrypoint sh jbroker-broker:local -c '
  /opt/jbroker/bin/broker-app consume --broker broker2:9092 --group checkout-processor --topic orders --topic prices 2>&1
' > /dev/null

echo "waiting for cluster to reach Stable state with non-zero throughput…"
deadline=$(($(date +%s) + 60))
while [ $(date +%s) -lt $deadline ]; do
  state=$(curl -sS "${ADMIN}/api/v1/consumer-groups/checkout-processor" | python3 -c "import json,sys; print(json.load(sys.stdin).get('state', ''))" 2>/dev/null || true)
  tput=$(curl -sS "${ADMIN}/api/v1/metrics/throughput" | python3 -c "import json,sys; d=json.load(sys.stdin); print(int(d.get('produce_bytes_per_sec') or 0))" 2>/dev/null || echo 0)
  if [ "${state}" = "Stable" ] && [ "${tput}" -gt 0 ]; then
    echo "  Stable group + ${tput} B/s produce — ready"
    break
  fi
  sleep 2
done

echo
echo "--- snapshot ---"
curl -sS "${ADMIN}/api/v1/cluster" | python3 -m json.tool | head -6
echo
curl -sS "${ADMIN}/api/v1/consumer-groups/checkout-processor" | python3 -c "
import json,sys
d = json.load(sys.stdin)
print(f\"group checkout-processor: state={d['state']} members={len(d['members'])} gen={d['generation']}\")
print('partitions:')
for p in sorted(d['partitions'], key=lambda x:(x['topic'],x['partition'])):
    print(f\"  {p['topic']}/{p['partition']}: committed={p['committed_offset']} hwm={p['high_watermark']} lag={p['lag']}\")
"
echo
echo "For LAG>0 screenshots: docker stop jb-consumer  (lag grows, Stable heartbeat ~30s)"
echo "Resume:                 docker start jb-consumer"
