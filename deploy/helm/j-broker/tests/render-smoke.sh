#!/usr/bin/env bash
#
# Helm chart smoke tests. Runs `helm template` across a matrix of
# values combinations and greps the rendered manifest for structural
# invariants. No live Kubernetes required.
#
# Run from the repo root:
#   ./deploy/helm/j-broker/tests/render-smoke.sh
#
# Exits non-zero on first failed assertion. Intended to run in CI before
# any helm chart change lands.
#
set -euo pipefail

CHART_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RELEASE="test"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

pass() {
    echo "  ok: $*"
}

# must_match — grep pattern must match in rendered output.
must_match() {
    local pattern="$1"
    local rendered="$2"
    local label="$3"
    if ! grep -Eq "$pattern" <<<"$rendered"; then
        echo "--- rendered output ---" >&2
        echo "$rendered" >&2
        fail "$label — pattern not found: $pattern"
    fi
    pass "$label"
}

# must_not_match — grep pattern must NOT appear (negative assertion).
must_not_match() {
    local pattern="$1"
    local rendered="$2"
    local label="$3"
    if grep -Eq "$pattern" <<<"$rendered"; then
        echo "--- rendered output ---" >&2
        echo "$rendered" >&2
        fail "$label — unexpected pattern found: $pattern"
    fi
    pass "$label"
}

run_template() {
    helm template "$RELEASE" "$CHART_DIR" "$@" 2>&1
}

echo "==> [1/8] defaults produce a 3-broker StatefulSet + admin Deployment"
defaults=$(run_template)
must_match 'kind: StatefulSet' "$defaults" "StatefulSet exists"
must_match 'kind: Deployment' "$defaults" "Deployment exists"
must_match '^  replicas: 3' "$defaults" "broker replicas=3 by default"
must_match 'clusterIP: None' "$defaults" "headless service rendered"
must_match 'volumeClaimTemplates:' "$defaults" "PVCs rendered by default"
must_match 'accessModes' "$defaults" "PVC accessModes present"
must_match 'storage: "10Gi"' "$defaults" "default PVC size 10Gi"
must_match 'podManagementPolicy: Parallel' "$defaults" "parallel pod management"
must_match 'kind: PodDisruptionBudget' "$defaults" "broker PDB rendered by default"
must_match 'maxUnavailable: 1' "$defaults" "PDB caps evictions at 1"
must_match 'startupProbe' "$defaults" "broker startup probe present"
must_match '"helm.sh/hook": test' "$defaults" "helm test hook pod rendered"
must_match 'preferredDuringSchedulingIgnoredDuringExecution' "$defaults" "soft anti-affinity by default"
must_match 'topologyKey: kubernetes.io/hostname' "$defaults" "anti-affinity spreads across nodes"
must_not_match 'kind: Ingress' "$defaults" "no ingress by default"
must_not_match 'kind: NetworkPolicy' "$defaults" "no NetworkPolicy by default"
must_not_match 'kind: Secret' "$defaults" "no secret created by default (bring your own)"
must_not_match 'JBROKER_REDIS_URL' "$defaults" "no redis wiring when disabled"
must_not_match 'tls-enabled' "$defaults" "no --tls flags when tls disabled"
must_not_match 'mountPath: /etc/jbroker/tls' "$defaults" "no TLS volume when disabled"

echo "==> [2/8] PDB + anti-affinity toggles remove their resources"
bare=$(run_template \
    --set broker.podDisruptionBudget.enabled=false \
    --set broker.podAntiAffinity.enabled=false)
must_not_match 'kind: PodDisruptionBudget' "$bare" "PDB gone when disabled"
must_not_match 'podAntiAffinity' "$bare" "anti-affinity gone when disabled"

echo "==> [3/8] replica count override propagates into voters + admin brokers"
five=$(run_template --set broker.replicaCount=5)
must_match 'replicas: 5' "$five" "broker replicas=5 override"
must_match '5@test-j-broker-broker-4' "$five" "5th voter present in JBROKER_VOTERS"
must_match 'test-j-broker-broker-4.test-j-broker-broker-headless' "$five" "5th admin broker URL present"

echo "==> [4/8] TLS enabled wires cert paths + mounts TLS secret"
tls=$(run_template --set tls.enabled=true --set tls.secretName=my-bundle)
must_match 'name: JBROKER_ADMIN_TLS_ENABLED' "$tls" "admin TLS enabled env var"
must_match 'mountPath: /etc/jbroker/tls' "$tls" "TLS volume mounted into containers"
must_match 'secretName: "my-bundle"' "$tls" "custom secretName propagated"
must_match '\-\-tls-enabled' "$tls" "broker --tls-enabled flag"
must_match '\-\-tls-cert /etc/jbroker/tls/tls.crt' "$tls" "broker --tls-cert flag"
must_match '\-\-tls-key /etc/jbroker/tls/tls.key' "$tls" "broker --tls-key flag"
must_match '\-\-tls-trust /etc/jbroker/tls/ca.crt' "$tls" "broker --tls-trust flag"

echo "==> [5/8] advertised-listeners template renders from sprintf"
adv=$(run_template --set broker.advertisedHostTemplate='broker%d.example.com' --set broker.advertisedPort=9443)
must_match '1=broker1.example.com:9443' "$adv" "advertised listener id=1"
must_match '2=broker2.example.com:9443' "$adv" "advertised listener id=2"
must_match '3=broker3.example.com:9443' "$adv" "advertised listener id=3"
must_match '\-\-advertised-listeners' "$adv" "--advertised-listeners CLI flag emitted"

echo "==> [6/8] Redis toggle emits Deployment + admin URL env var"
redis=$(run_template --set redis.enabled=true)
must_match 'name: test-j-broker-redis' "$redis" "redis Deployment rendered"
must_match 'value: "redis://test-j-broker-redis:6379"' "$redis" "admin sees redis URL"

echo "==> [7/8] NetworkPolicy toggle locks broker/admin/redis ingress"
netpol=$(run_template --set networkPolicy.enabled=true --set redis.enabled=true \
    --set broker.chaosPort=9100 --set broker.chaosTokenSecret=chaos-token)
count=$(grep -c 'kind: NetworkPolicy' <<<"$netpol" || true)
[ "$count" -eq 3 ] || fail "expected 3 NetworkPolicies (broker/admin/redis), got $count"
pass "3 NetworkPolicies rendered"
must_match 'port: 9192' "$netpol" "raft port allowed between brokers"
must_match 'port: 9100' "$netpol" "chaos port opened to admin when bound"
must_match 'port: 6379' "$netpol" "redis locked to admin app"
restricted=$(run_template --set networkPolicy.enabled=true \
    --set 'networkPolicy.clientFrom[0].namespaceSelector.matchLabels.team=data')
must_match 'team: data' "$restricted" "clientFrom peers propagate into the broker policy"

echo "==> [8/8] monitoring toggles gate ServiceMonitor + PrometheusRule"
must_not_match 'kind: ServiceMonitor' "$defaults" "no ServiceMonitor by default"
must_not_match 'kind: PrometheusRule' "$defaults" "no PrometheusRule by default"
mon=$(run_template \
    --set metrics.serviceMonitor.enabled=true \
    --set metrics.prometheusRule.enabled=true)
must_match 'kind: ServiceMonitor' "$mon" "ServiceMonitor rendered when enabled"
must_match 'path: /actuator/prometheus' "$mon" "scrape path targets the actuator exposition"
must_match 'kind: PrometheusRule' "$mon" "PrometheusRule rendered when enabled"
must_match 'alert: JBrokerUnderReplicatedPartitions' "$mon" "under-replication alert present"
must_match 'alert: JBrokerReplicationLagHigh' "$mon" "follower-lag alert present"
must_match 'alert: JBrokerReplicationStalled' "$mon" "stuck-watermark alert present"
must_match 'alert: JBrokerRaftTermFlapping' "$mon" "raft-flapping alert present"
must_match 'alert: JBrokerAdminMetricsDown' "$mon" "scrape-down alert present with ServiceMonitor"
must_match 'jbroker_replication_lag_records\{namespace="default"\} > 1000' "$mon" "lag threshold value propagates"
rules_only=$(run_template --set metrics.prometheusRule.enabled=true)
must_not_match 'alert: JBrokerAdminMetricsDown' "$rules_only" "scrape-down alert absent without the ServiceMonitor"

# Strict YAML parse of the monitoring objects (helm template already
# splits documents, but multi-line PromQL blocks deserve a real parser).
if python3 -c 'import yaml' >/dev/null 2>&1; then
    printf '%s' "$mon" \
        | python3 -c 'import sys, yaml; list(yaml.safe_load_all(sys.stdin))' \
        || fail "rendered monitoring manifests are not valid YAML"
    pass "monitoring manifests parse as YAML"
else
    echo "  skip: python3 + PyYAML unavailable — strict YAML parse skipped"
fi

# Rule-syntax check when promtool is on PATH: extract the rendered
# PrometheusRule spec (a valid Prometheus rule file) and lint it.
if command -v promtool >/dev/null 2>&1 && python3 -c 'import yaml' >/dev/null 2>&1; then
    rules_file=$(mktemp)
    run_template \
        --set metrics.serviceMonitor.enabled=true \
        --set metrics.prometheusRule.enabled=true \
        -s templates/prometheusrule.yaml \
        | python3 -c 'import sys, yaml
docs = [d for d in yaml.safe_load_all(sys.stdin) if d]
yaml.safe_dump(docs[0]["spec"], sys.stdout)' > "$rules_file"
    promtool check rules "$rules_file" || fail "promtool rejected the rendered alert rules"
    rm -f "$rules_file"
    pass "promtool check rules passed"
else
    echo "  skip: promtool unavailable — rule syntax check skipped"
fi

echo
echo "All helm smoke-test assertions passed."
