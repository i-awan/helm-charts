#!/usr/bin/env bash
# Generates ONE self-signed CA + ONE server cert covering every region's
# Kafka/controller endpoints, then creates the SAME `kafka-tls` secret in
# all three OpenShift clusters. This is what "shared TLS identity across
# regions" from the README actually means in practice.
#
# Dev/test only. For production, get the cert issued by your real PKI
# (internal CA, cert-manager + an actual ClusterIssuer, etc.) instead of
# the openssl steps below — the distribution loop at the bottom is the
# part that stays the same either way.
set -euo pipefail

NAMESPACE="${NAMESPACE:-confluent}"
SECRET_NAME="${SECRET_NAME:-kafka-tls}"
JKS_PASSWORD="${JKS_PASSWORD:-changeit}"   # override via env var, don't commit real passwords
CONTEXTS=("region-a" "region-b" "region-05dc")

WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT
cd "$WORKDIR"

# SANs must cover EVERY region's internal service DNS and external domain
# — this one cert has to be valid no matter which region's broker a client
# connects to, since it's one logical cluster's identity, not three.
SAN="DNS:kafka.${NAMESPACE}.svc.cluster.local,\
DNS:*.${NAMESPACE}.svc.cluster.local,\
DNS:kafka-region-a.example.internal,\
DNS:kafka-region-b.example.internal"

echo "==> Generating self-signed CA"
openssl genrsa -out ca-key.pem 4096
openssl req -x509 -new -nodes -key ca-key.pem -sha256 -days 3650 \
  -out ca-cert.pem -subj "/CN=kafka-2.5dc-ca"

echo "==> Generating server key + cert signed by that CA"
openssl genrsa -out server-key.pem 2048
openssl req -new -key server-key.pem -out server.csr -subj "/CN=kafka"
openssl x509 -req -in server.csr -CA ca-cert.pem -CAkey ca-key.pem \
  -CAcreateserial -out server-cert.pem -days 3650 -sha256 \
  -extfile <(echo "subjectAltName=$SAN")

echo "==> Building PKCS12 keystore (CFK TLS Group 2 format)"
openssl pkcs12 -export -in server-cert.pem -inkey server-key.pem \
  -certfile ca-cert.pem -out keystore.jks -name kafka \
  -password "pass:${JKS_PASSWORD}"

echo "==> Building truststore"
keytool -importcert -noprompt -alias ca -file ca-cert.pem \
  -keystore truststore.jks -storepass "${JKS_PASSWORD}" -storetype PKCS12

printf 'jksPassword=%s' "${JKS_PASSWORD}" > jksPassword.txt

echo "==> Applying the SAME secret to all three region clusters"
for ctx in "${CONTEXTS[@]}"; do
  echo "  -> context: $ctx"
  kubectl create secret generic "$SECRET_NAME" \
    --from-file=keystore.jks=keystore.jks \
    --from-file=truststore.jks=truststore.jks \
    --from-file=jksPassword.txt=jksPassword.txt \
    -n "$NAMESPACE" --context "$ctx" \
    --dry-run=client -o yaml | kubectl apply -f - --context "$ctx"
done

echo "Done. '$SECRET_NAME' is now identical in: ${CONTEXTS[*]}"
echo "Re-run this script (with the same generated certs saved somewhere"
echo "safe) any time you rotate — CFK detects the secret change and does"
echo "a safe one-broker-at-a-time rolling restart automatically."
