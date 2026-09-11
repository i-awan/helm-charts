# kafka-2.5dc

Helm chart deploying one logical Kafka cluster stretched across a
**2.5DC topology**: two full regions (brokers + controller) plus one
lightweight witness region (controller only). Uses Confluent for
Kubernetes (CFK). Full rationale and troubleshooting history: `README.md`.

## Quick start

| Mode | Values files | Kubernetes scope |
|---|---|---|
| Mock 2.5DC (start here) | `values-mock-region-a/b.yaml`, `values-mock-05dc.yaml` | 1 cluster, 3 namespaces |
| Real 2.5DC | `values-region-a/b.yaml`, `values-05dc.yaml` | 3 clusters |

Both follow the same steps below — differences noted inline.

## Why 2.5DC

KRaft controllers need a **majority vote** (Raft) before any metadata
change commits. Two consequences:
- **Controller count must be odd** — an even split can partition into
  two equal halves, neither with a majority, halting the cluster.
- **Exactly 2 regions can never be made safe** — whichever region ends
  up holding the majority is a single point of failure for the whole
  cluster's control plane, no matter how you split it.

2.5DC fixes this at minimum cost: 2 full regions + 1 controller-only
witness = 3 locations, odd quorum, no region ever holds the deciding
majority alone.

## Connectivity prerequisites (real clusters only)

- Non-overlapping pod CIDRs across all three clusters
- Cross-cluster DNS resolution between all three
- Nodes labeled `topology.kubernetes.io/region=<name>` in each cluster
- Stable, low-latency link between region-a ↔ region-b specifically
  (carries replication traffic); the 0.5DC link just needs to be
  reachable (controller traffic only)

The mock setup sidesteps all of this (one cluster's networking is
already flat) — it validates every *Kafka-level* mechanic below, but
not these cross-cluster prerequisites.

## The critical finding: `clusterID` alone does not form a quorum

Matching `cluster.clusterID` across regions is necessary but **not
sufficient**. Verified via:
```bash
kubectl exec -it kraftcontroller-region-a-0 -n kafka-region-a -- \
  kafka-metadata-quorum --bootstrap-controller localhost:9074 describe --status
```
Without the fix below, only region-a ever appeared as a voter —
region-b and the 0.5DC never joined, not even as observers. `clusterID`
is an identity check performed *after* controllers already connect —
it isn't a discovery mechanism.

**Fix — explicit static voter list, identical on every region**
(`staticQuorumVoters` in each values file), rendered via
`configOverrides.server` (not CFK's structured
`listeners.controllerQuorumVoters` field — that was silently rejected
as an "unknown field" by the CRD version tested):
```yaml
- controller.quorum.voters=100@kraftcontroller-region-a-0...:9074,200@kraftcontroller-region-b-0...:9074,300@kraftcontroller-region-05dc-0...:9074
```
**Always check `helm install`/`upgrade` output for `Warning: unknown
field` lines** — a structured field your CRD version doesn't recognize
is silently dropped, not rejected. Two other fields hit this on the
tested cluster: `podTemplate.initContainers` (cosmetic) and
`podTemplate.nodeSelector` (real — node-label scheduling doesn't
currently work regardless of `nodeSelector.enabled`).

**`clusterID` propagation is still a separate, required step** — the
voter list fixes discovery, not identity. Region-a must still be
installed first (generates the ID), fetched, and pasted into region-b/
05dc's values before installing those two. Leaving it blank on all
three lets each generate its own UUID and fail to join.

**Quorum-topology changes aren't safe to apply in place** on a cluster
with existing committed metadata — wipe (`helm uninstall` + delete
PVCs) and reinstall fresh if retrofitting this.

## `clusterID` vs Kubernetes cluster identity

Two unrelated things: the Kafka `clusterID` (shared by every controller/
broker in one logical cluster) and the Kubernetes cluster's own identity
(unrelated — you could rebuild the K8s cluster entirely and Kafka
wouldn't notice, as long as the PVCs and `clusterID` carry over).
Separately, every controller/broker needs its own unique **node ID**
(`controllerIdOffset`/`brokerIdOffset`) — never shared, never
overlapping even within one region (CFK enforces a ≥100 minimum on
controller offsets specifically).

## TLS

`tls.enabled: true` encrypts every connection (client↔broker,
broker↔broker, broker↔controller, controller↔controller) via one
shared `kafka-tls` secret. **Uses PEM format**
(`fullchain.pem`/`privkey.pem`/`cacerts.pem`), not JKS/PKCS12 — OpenSSL
3.x's default PKCS12 cipher is frequently unreadable by Java's keystore
provider, surfacing as a cryptic `trustAnchors` SSL error at Kafka
startup. PEM has no such issue and needs no password.

TLS here proves server identity only — it does **not** authenticate
clients. `authorization.enabled` (simple ACLs) needs real client auth
(mTLS certs or SASL) to mean anything; without it, every client presents
the same anonymous identity. Both are commonly left `false` during
mechanics validation.

Scripts require OpenSSL 3.x — macOS's bundled `/usr/bin/openssl` is
LibreSSL; `brew install openssl@3` if the script's version check fails.

## Deploying

**0. Operator.** Mock: one operator, `namespaced=false`, watching all
three namespaces (`helm upgrade --install cfk-operator ... --set
namespaced=false -n confluent`). Real: one operator *per cluster*,
`namespaced: true` (default) — installed **into the same namespace as
that cluster's workload** (`-n kafka-region-a`, etc.), or it won't watch it.

**1. Label nodes.**
```bash
oc label node <node> topology.kubernetes.io/region=region-a   # etc. per region
```
No privileges? Set `nodeSelector.enabled: false` — skips the
requirement and softens pod anti-affinity, at the cost of real node
separation.

**2. CRDs** (once per cluster):
```bash
helm repo add confluentinc https://packages.confluent.io/helm && helm repo update
helm pull confluentinc/confluent-for-kubernetes --untar
kubectl apply --server-side -f confluent-for-kubernetes/crds/
```

**3. TLS** (if enabled): `./scripts/generate-and-distribute-tls-mock.sh`
(mock) or `./scripts/generate-and-distribute-tls.sh` (real, add
`--kube-context` per cluster).

**4. Bootstrap region-a, fetch `clusterID`:**
```bash
helm install kafka-region-a . -f values-mock-region-a.yaml -n kafka-region-a
kubectl get kraftcontroller kraftcontroller-region-a -n kafka-region-a -o jsonpath='{.status.clusterID}'
```
Paste into `cluster.clusterID` in region-b/05dc's values files, then:
```bash
helm install kafka-region-b . -f values-mock-region-b.yaml -n kafka-region-b
helm install kafka-05dc . -f values-mock-05dc.yaml -n kafka-region-05dc
```
(Real clusters: same commands, add `-n <region-namespace> --kube-context <ctx>`.)

**5. Verify quorum:**
```bash
kubectl exec -it kraftcontroller-region-a-0 -n kafka-region-a -- \
  kafka-metadata-quorum --bootstrap-controller localhost:9074 describe --status
```
`CurrentVoters` must list all three node IDs.

**6. Prove replication:**
```bash
kubectl exec -it kafka-0 -n kafka-region-a -- kafka-topics \
  --bootstrap-server localhost:9092 --create --topic orders3 \
  --partitions 3 --replication-factor 3 --config min.insync.replicas=2
```
`--describe` should show `Replicas:` spanning both region-a and region-b brokers.

**7. The one test that actually matters — cross-region produce/consume.**
A same-region round trip proves nothing about multi-region; this does:
```bash
kubectl exec -it kafka-0 -n kafka-region-a -- kafka-console-producer \
  --bootstrap-server localhost:9092 --topic orders3   # produce here, Ctrl+D to exit
kubectl exec -it kafka-0 -n kafka-region-b -- kafka-console-consumer \
  --bootstrap-server kafka.kafka-region-b.svc.cluster.local:9092 \
  --topic orders3 --from-beginning   # consume here
```
If it shows up, replication genuinely crossed the region boundary.

## Schema Registry

`schemas/<name>/<name>-schema.yaml` files ship inside the chart;
`templates/schemas.yaml` auto-discovers them (`.Files.Glob
"schemas/**/*.yaml"`) and renders, per file, a `ConfigMap` (raw schema
JSON, inert until read) + a `Schema` CR (subject/format/ConfigMap ref).
The CFK operator watches `Schema` CRs and calls Schema Registry's REST
API to register them — already done for the bundled `payment` schema
the moment `helm install` ran; no extra step needed.

Add your own the same way:
```bash
mkdir -p schemas/shipments
cat > schemas/shipments/shipments-schema.yaml << 'EOF'
name: shipments
subjects: shipments-value
format: avro
schema: |
  { "type": "record", "name": "Shipment", "namespace": "io.example.shipments",
    "fields": [ { "name": "shipment_id", "type": "string" },
                { "name": "order_id", "type": "string" },
                { "name": "status", "type": "string" } ] }
EOF
```

**Cross-region Avro test** — the same valuable test as step 7, now with
schema enforcement:
```bash
kubectl exec -it kafka-0 -n kafka-region-a -- kafka-topics \
  --bootstrap-server localhost:9092 --create --topic payment \
  --partitions 3 --replication-factor 3 --config min.insync.replicas=2

kubectl exec -it schemaregistry-0 -n kafka-region-a -- bash
curl -s http://localhost:8081/subjects/payment-value/versions/latest   # note the id
LOG_DIR=/tmp kafka-avro-console-producer --broker-list kafka.kafka-region-a.svc.cluster.local:9092 \
  --topic payment --property schema.registry.url=http://localhost:8081 \
  --property value.schema.id=<id>
# type: {"payment_id":"pay-1","order_id":"order-101","amount":49.99,"status":"created"}  then Ctrl+D

kubectl exec -it schemaregistry-0 -n kafka-region-b -- bash
LOG_DIR=/tmp kafka-avro-console-consumer --bootstrap-server kafka.kafka-region-b.svc.cluster.local:9092 \
  --topic payment --property schema.registry.url=http://localhost:8081 --from-beginning
```
Region-b decoding a record it never registered proves both replication
*and* the shared `_schemas` topic genuinely span the cluster.
`LOG_DIR=/tmp` avoids a log4j permission crash on these images; use
`Ctrl+D`, not `Ctrl+C`, to exit the producer cleanly.

## Key gotchas (condensed)

- `image.application`/`image.init` — one combined `image:tag` string,
  not split `repository`/`tag`.
- Controller/broker ID ranges must never overlap, even within one
  region — plan `controllerIdOffset`/`brokerIdOffset` spacing upfront;
  can't change after creation.
- CRDs and the operator are separate installs — CRDs alone create nothing.
- `Pending`/`0/1 Running` for a minute or two on first boot is normal —
  a fresh controller has to format its metadata log and self-elect.
- Adding brokers never rebalances existing topics automatically — new
  brokers only get used by *new* topics/partitions.
- No key on a record ≠ even partition spread — the sticky partitioner
  batches a whole producer session onto one partition at a time.
