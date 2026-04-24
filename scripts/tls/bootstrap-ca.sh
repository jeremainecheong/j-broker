#!/usr/bin/env bash
#
# dev-only mTLS CA + broker server certs + admin-app client cert
# generator. Produces PEM artifacts directly consumable by j-broker's
# TlsConfig (certChain/privateKey/trustStore). Idempotent: re-runs regenerate
# certs in place. Do NOT use in production — keys are unencrypted and SANs
# hardcode localhost + Docker service names.
#
# Output layout (under $OUTDIR):
#   ca.key          — 2048-bit RSA CA private key (keep offline)
#   ca.crt          — self-signed CA cert (distribute as truststore)
#   broker1.key     — broker 1 server private key
#   broker1.crt     — broker 1 server cert, signed by CA
#   broker2.key / broker2.crt
#   broker3.key / broker3.crt
#   admin.key       — admin-app client private key
#   admin.crt       — admin-app client cert
#
# Server cert SANs cover DNS:brokerN (Docker Compose service), DNS:localhost,
# IP:127.0.0.1 so both `docker compose up` and host-side TLS clients validate.
#
# Usage:
#   scripts/tls/bootstrap-ca.sh [OUTDIR]     # default: .tls/
#   scripts/tls/bootstrap-ca.sh --clean      # wipe + regenerate
#
set -euo pipefail

OUTDIR="${1:-.tls}"
if [[ "${1:-}" == "--clean" ]]; then
    OUTDIR="${2:-.tls}"
    rm -rf "$OUTDIR"
fi

mkdir -p "$OUTDIR"
cd "$OUTDIR"

echo "==> Generating dev CA at $(pwd)"

# --- Root CA ---
if [[ ! -f ca.key ]]; then
    openssl genrsa -out ca.key 2048
fi
if [[ ! -f ca.crt ]]; then
    openssl req -x509 -new -key ca.key -sha256 -days 3650 \
        -subj "/CN=j-broker dev CA" \
        -out ca.crt
fi

# --- Server certs: broker1 / broker2 / broker3 ---
for n in 1 2 3; do
    name="broker${n}"
    if [[ -f "${name}.crt" && "${2:-}" != "--force" ]]; then
        continue
    fi

    openssl genrsa -out "${name}.key" 2048
    openssl req -new -key "${name}.key" \
        -subj "/CN=${name}" \
        -out "${name}.csr"

    cat > "${name}.ext" <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:${name},DNS:localhost,IP:127.0.0.1
EOF

    openssl x509 -req -in "${name}.csr" \
        -CA ca.crt -CAkey ca.key -CAcreateserial \
        -out "${name}.crt" -days 825 -sha256 \
        -extfile "${name}.ext"

    rm "${name}.csr" "${name}.ext"
done

# --- Admin-app client cert ---
if [[ ! -f admin.crt || "${2:-}" == "--force" ]]; then
    openssl genrsa -out admin.key 2048
    openssl req -new -key admin.key \
        -subj "/CN=admin" \
        -out admin.csr

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
fi

# --- PKCS#8 key re-encoding ---
# gRPC's SslContextBuilder expects PKCS#8 PEM keys. `openssl genrsa` emits
# PKCS#1 — convert in place so files are ready for consumption.
for k in ca.key broker1.key broker2.key broker3.key admin.key; do
    if head -1 "$k" | grep -q "BEGIN RSA PRIVATE KEY"; then
        openssl pkcs8 -topk8 -nocrypt -in "$k" -out "${k}.pkcs8"
        mv "${k}.pkcs8" "$k"
    fi
done

chmod 600 *.key
chmod 644 *.crt

echo "==> Done. Artifacts in $(pwd)"
ls -la *.crt *.key
