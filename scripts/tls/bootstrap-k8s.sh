#!/usr/bin/env bash
#
# mTLS bundle generator for Kubernetes installs of the Helm chart.
# Differs from bootstrap-ca.sh (the Compose/dev generator) in one
# essential way: the broker server cert carries wildcard SANs for the
# StatefulSet's headless-service DNS names, so pod-to-pod and
# admin-to-broker dials verify. One shared server cert works for every
# broker pod because the SAN is the wildcard, not an ordinal name.
#
# Output layout (under $OUTDIR):
#   ca.key / ca.crt       — CA pair (keep ca.key offline)
#   server.key / server.crt — broker server cert, SANs:
#                             *.<headless>.<namespace>.svc.cluster.local
#                             <headless>.<namespace>.svc.cluster.local
#                             localhost, 127.0.0.1
#   admin.key / admin.crt — admin-app client cert (CN=admin)
#
# Usage:
#   scripts/tls/bootstrap-k8s.sh OUTDIR HEADLESS_SERVICE [NAMESPACE]
# Example (release "jb", chart defaults):
#   scripts/tls/bootstrap-k8s.sh .tls-k8s jb-j-broker-broker-headless default
#
# Then create the secret the chart mounts:
#   kubectl -n NAMESPACE create secret generic jbroker-tls \
#       --from-file=tls.crt=OUTDIR/server.crt \
#       --from-file=tls.key=OUTDIR/server.key \
#       --from-file=ca.crt=OUTDIR/ca.crt
#
set -euo pipefail

OUTDIR="${1:?usage: bootstrap-k8s.sh OUTDIR HEADLESS_SERVICE [NAMESPACE]}"
HEADLESS="${2:?usage: bootstrap-k8s.sh OUTDIR HEADLESS_SERVICE [NAMESPACE]}"
NAMESPACE="${3:-default}"

mkdir -p "$OUTDIR"
cd "$OUTDIR"

DOMAIN="${HEADLESS}.${NAMESPACE}.svc.cluster.local"

echo "==> Generating k8s mTLS bundle for *.${DOMAIN} at $(pwd)"

if [[ ! -f ca.key ]]; then
    openssl genrsa -out ca.key 2048
fi
if [[ ! -f ca.crt ]]; then
    openssl req -x509 -new -key ca.key -sha256 -days 3650 \
        -subj "/CN=j-broker k8s CA" \
        -out ca.crt
fi

openssl genrsa -out server.key 2048
openssl req -new -key server.key -subj "/CN=broker" -out server.csr
cat > server.ext <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:*.${DOMAIN},DNS:${DOMAIN},DNS:localhost,IP:127.0.0.1
EOF
openssl x509 -req -in server.csr \
    -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out server.crt -days 825 -sha256 \
    -extfile server.ext
rm server.csr server.ext

openssl genrsa -out admin.key 2048
openssl req -new -key admin.key -subj "/CN=admin" -out admin.csr
cat > admin.ext <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=clientAuth
EOF
openssl x509 -req -in admin.csr \
    -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out admin.crt -days 825 -sha256 \
    -extfile admin.ext
rm admin.csr admin.ext

# gRPC's SslContextBuilder wants PKCS#8 keys.
for k in ca.key server.key admin.key; do
    if head -1 "$k" | grep -q "BEGIN RSA PRIVATE KEY"; then
        openssl pkcs8 -topk8 -nocrypt -in "$k" -out "${k}.pkcs8"
        mv "${k}.pkcs8" "$k"
    fi
done

chmod 600 *.key
chmod 644 *.crt

echo "==> Done. Artifacts in $(pwd)"
ls -la *.crt *.key
