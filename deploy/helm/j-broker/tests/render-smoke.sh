#!/usr/bin/env bash
#
# P15.3 — helm chart smoke tests. Runs `helm template` across a matrix of
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

echo "==> [1/5] defaults produce a 3-broker StatefulSet + admin Deployment"
defaults=$(run_template)
must_match 'kind: StatefulSet' "$defaults" "StatefulSet exists"
must_match 'kind: Deployment' "$defaults" "Deployment exists"
must_match '^  replicas: 3' "$defaults" "broker replicas=3 by default"
must_match 'clusterIP: None' "$defaults" "headless service rendered"
must_match 'volumeClaimTemplates:' "$defaults" "PVCs rendered by default"
must_match 'accessModes' "$defaults" "PVC accessModes present"
must_match 'storage: "10Gi"' "$defaults" "default PVC size 10Gi"
must_match 'podManagementPolicy: Parallel' "$defaults" "parallel pod management"
must_not_match 'kind: Ingress' "$defaults" "no ingress by default"
must_not_match 'kind: Secret' "$defaults" "no secret created by default (bring your own)"
must_not_match 'JBROKER_REDIS_URL' "$defaults" "no redis wiring when disabled"
must_not_match 'tls-enabled' "$defaults" "no --tls flags when tls disabled"
must_not_match 'mountPath: /etc/jbroker/tls' "$defaults" "no TLS volume when disabled"

echo "==> [2/5] replica count override propagates into voters + admin brokers"
five=$(run_template --set broker.replicaCount=5)
must_match 'replicas: 5' "$five" "broker replicas=5 override"
must_match '5@test-j-broker-broker-4' "$five" "5th voter present in JBROKER_VOTERS"
must_match 'test-j-broker-broker-4.test-j-broker-broker-headless' "$five" "5th admin broker URL present"

echo "==> [3/5] TLS enabled wires cert paths + mounts TLS secret"
tls=$(run_template --set tls.enabled=true --set tls.secretName=my-bundle)
must_match 'name: JBROKER_ADMIN_TLS_ENABLED' "$tls" "admin TLS enabled env var"
must_match 'mountPath: /etc/jbroker/tls' "$tls" "TLS volume mounted into containers"
must_match 'secretName: "my-bundle"' "$tls" "custom secretName propagated"
must_match '\-\-tls-enabled' "$tls" "broker --tls-enabled flag"
must_match '\-\-tls-cert /etc/jbroker/tls/tls.crt' "$tls" "broker --tls-cert flag"
must_match '\-\-tls-key /etc/jbroker/tls/tls.key' "$tls" "broker --tls-key flag"
must_match '\-\-tls-trust /etc/jbroker/tls/ca.crt' "$tls" "broker --tls-trust flag"

echo "==> [4/5] advertised-listeners template renders from sprintf"
adv=$(run_template --set broker.advertisedHostTemplate='broker%d.example.com' --set broker.advertisedPort=9443)
must_match '1=broker1.example.com:9443' "$adv" "advertised listener id=1"
must_match '2=broker2.example.com:9443' "$adv" "advertised listener id=2"
must_match '3=broker3.example.com:9443' "$adv" "advertised listener id=3"
must_match '\-\-advertised-listeners' "$adv" "--advertised-listeners CLI flag emitted"

echo "==> [5/5] Redis toggle emits Deployment + admin URL env var"
redis=$(run_template --set redis.enabled=true)
must_match 'name: test-j-broker-redis' "$redis" "redis Deployment rendered"
must_match 'value: "redis://test-j-broker-redis:6379"' "$redis" "admin sees redis URL"

echo
echo "All helm smoke-test assertions passed."
