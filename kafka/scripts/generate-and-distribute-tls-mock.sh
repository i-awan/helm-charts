#!/usr/bin/env bash
# Generates ONE self-signed CA + ONE server cert covering every mocked
# region's namespace, then creates the SAME `kafka-tls` secret in all
# three namespaces on this one cluster.
#
# Uses CFK's PEM-format TLS secret (fullchain.pem/privkey.pem/cacerts.pem)
# rather than JKS/PKCS12 — deliberately avoids keystore/truststore
# generation entirely, since OpenSSL 3.x's default PKCS12 encryption is
# often unreadable by Java's keystore provider (surfaces as
# "trustAnchors parameter must be non-empty" when Kafka starts). Plain
# PEM certs have no such compatibility issue, and no password is needed.
#
# Dev/test only. For production, get certs issued by your real PKI —
# the "same secret in all namespaces" distribution step stays the same.
set -euo pipefail

# -not_before/-not_after (used below to avoid clock-skew issues) need
# OpenSSL 3.0+. macOS ships LibreSSL as /usr/bin/openssl by default,
# which doesn't support these flags — install real OpenSSL via
# `brew install openssl@3` and ensure it's earlier in PATH if this check
# fails on Mac.
if ! openssl version | grep -q "OpenSSL 3\."; then
  echo "ERROR: this script needs OpenSSL 3.x (found: $(openssl version))." >&2
  echo "On macOS: brew install openssl@3, then put it first in PATH, e.g.:" >&2
  echo '  export PATH="$(brew --prefix openssl@3)/bin:$PATH"' >&2
  exit 1
fi

NAMESPACES=("kafka-region-a" "kafka-region-b" "kafka-region-05dc")

WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT
cd "$WORKDIR"

# SANs cover all three namespaces' internal service DNS.
SAN=""
for ns in "${NAMESPACES[@]}"; do
  SAN="${SAN}DNS:kafka.${ns}.svc.cluster.local,DNS:*.${ns}.svc.cluster.local,"
done
SAN="${SAN%,}"

echo "==> Generating self-signed CA"
openssl genrsa -out ca-key.pem 4096
openssl req -x509 -new -nodes -key ca-key.pem -sha256 \
  -out ca-cert.pem -subj "/CN=kafka-mock-ca" \
  -not_before 20200101000000Z -not_after 21000101000000Z

echo "==> Generating server key + cert signed by that CA"
openssl genrsa -out server-key.pem 2048
openssl req -new -key server-key.pem -out server.csr -subj "/CN=kafka"
openssl x509 -req -in server.csr -CA ca-cert.pem -CAkey ca-key.pem \
  -CAcreateserial -out server-cert.pem -sha256 \
  -not_before 20200101000000Z -not_after 21000101000000Z \
  -extfile <(echo "subjectAltName=$SAN")

echo "==> Applying the SAME PEM-format secret to all three namespaces (one cluster)"
for ns in "${NAMESPACES[@]}"; do
  echo "  -> namespace: $ns"
  kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f -
  kubectl create secret generic kafka-tls \
    --from-file=fullchain.pem=server-cert.pem \
    --from-file=privkey.pem=server-key.pem \
    --from-file=cacerts.pem=ca-cert.pem \
    -n "$ns" --dry-run=client -o yaml | kubectl apply -f -
done

echo "Done. 'kafka-tls' (PEM format) is now identical in: ${NAMESPACES[*]}"
