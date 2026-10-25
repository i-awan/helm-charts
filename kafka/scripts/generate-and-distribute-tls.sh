#!/usr/bin/env bash
# Generates ONE self-signed CA + ONE server cert covering every region's
# Kafka/controller endpoints, then creates the SAME `kafka-tls` secret in
# all three OpenShift clusters. This is what "shared TLS identity across
# regions" from the README actually means in practice.
#
# Uses CFK's PEM-format TLS secret (fullchain.pem/privkey.pem/cacerts.pem)
# rather than JKS/PKCS12 — deliberately avoids keystore/truststore
# generation entirely, since OpenSSL 3.x's default PKCS12 encryption is
# often unreadable by Java's keystore provider (surfaces as
# "trustAnchors parameter must be non-empty" when Kafka starts). Plain
# PEM certs have no such compatibility issue, and no password is needed.
#
# Dev/test only. For production, get the cert issued by your real PKI
# (internal CA, cert-manager + an actual ClusterIssuer, etc.) instead of
# the openssl steps below — the distribution loop at the bottom is the
# part that stays the same either way.
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

SECRET_NAME="${SECRET_NAME:-kafka-tls}"
# Context -> namespace pairs. Update these if your kube-context names or
# per-region namespace names differ from the chart's defaults
# (values-region-a/b.yaml, values-05dc.yaml).
CONTEXTS=("region-a" "region-b" "region-05dc")
NAMESPACES=("kafka-region-a" "kafka-region-b" "kafka-region-05dc")

WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT
cd "$WORKDIR"

# SANs must cover EVERY region's internal service DNS and external domain
# — this one cert has to be valid no matter which region's broker a client
# connects to, since it's one logical cluster's identity, not three.
SAN=""
for ns in "${NAMESPACES[@]}"; do
  SAN="${SAN}DNS:kafka.${ns}.svc.cluster.local,DNS:*.${ns}.svc.cluster.local,"
done
SAN="${SAN}DNS:kafka-region-a.example.internal,DNS:kafka-region-b.example.internal"

echo "==> Generating self-signed CA"
openssl genrsa -out ca-key.pem 4096
openssl req -x509 -new -nodes -key ca-key.pem -sha256 \
  -out ca-cert.pem -subj "/CN=kafka-2.5dc-ca" \
  -not_before 20200101000000Z -not_after 21000101000000Z

echo "==> Generating server key + cert signed by that CA"
openssl genrsa -out server-key.pem 2048
openssl req -new -key server-key.pem -out server.csr -subj "/CN=kafka"
openssl x509 -req -in server.csr -CA ca-cert.pem -CAkey ca-key.pem \
  -CAcreateserial -out server-cert.pem -sha256 \
  -not_before 20200101000000Z -not_after 21000101000000Z \
  -extfile <(echo "subjectAltName=$SAN")

echo "==> Applying the SAME PEM-format secret to all three region clusters"
for i in "${!CONTEXTS[@]}"; do
  ctx="${CONTEXTS[$i]}"
  ns="${NAMESPACES[$i]}"
  echo "  -> context: $ctx, namespace: $ns"
  kubectl create secret generic "$SECRET_NAME" \
    --from-file=fullchain.pem=server-cert.pem \
    --from-file=privkey.pem=server-key.pem \
    --from-file=cacerts.pem=ca-cert.pem \
    -n "$ns" --context "$ctx" \
    --dry-run=client -o yaml | kubectl apply -f - --context "$ctx"
done

echo "Done. '$SECRET_NAME' (PEM format) is now identical across: ${CONTEXTS[*]}"
echo "Re-run this script (with the same generated certs saved somewhere"
echo "safe) any time you rotate — CFK detects the secret change and does"
echo "a safe one-broker-at-a-time rolling restart automatically."
