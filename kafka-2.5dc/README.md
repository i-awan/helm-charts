# kafka-2.5dc

A Helm chart that renders the CFK (Confluent for Kubernetes) `KRaftController`
and `Kafka` custom resources needed to run Kafka on Kubernetes — usable
either as a **single-cluster standalone deployment** (for local dev/test)
or as **one logical Kafka cluster stretched across three Kubernetes
clusters** in a 2.5DC topology (for production multi-region HA).

This document summarizes the reasoning behind the design, not just the
commands — see the inline `# comments` in each `values-*.yaml` for
field-level detail.

---

## Quick start — do these in order

Pick the row matching what you're deploying. **Node labeling is step 1
wherever it appears — do it before installing anything, or pods will sit
unschedulable with no useful error on the CR itself.**

| Mode | Values file(s) | Order of operations |
|---|---|---|
| **Standalone** (1 cluster, fastest test) | `values-standalone.yaml` | 1. CRDs → 2. operator → 3. `helm install` — no node labels needed (`standalone: true` skips `nodeSelector`) |
| **Standalone, multi-broker** (real replication/quorum on 1 cluster) | `values-standalone-multibroker.yaml` | Same as above, plus create `kafka-tls` secret first if TLS is on |
| **Mock multi-region** (3 namespaces, 1 real cluster) | `values-mock-region-a/b.yaml`, `values-mock-05dc.yaml` | **0. Reinstall the CFK operator with `namespaced=false` (see below) — do this first, or CRs in the new namespaces silently never reconcile** → 1. Label real, distinct nodes per region → 2. `generate-and-distribute-tls-mock.sh` → 3. install region-a, fetch `clusterID` → 4. install region-b/05dc with that ID |
| **Real multi-region** (3 separate clusters) | `values-region-a/b.yaml`, `values-05dc.yaml` | **1. Label nodes in each cluster — do this first** → 2. verify cross-cluster networking prerequisites → 3. CRDs+operator per cluster → 4. `generate-and-distribute-tls.sh` → 5. install region-a, fetch `clusterID` → 6. install region-b/05dc with that ID |

**The node-labeling command** (mock and real multi-region modes only):
```bash
kubectl get nodes   # pick real node names first
oc label node <node1> topology.kubernetes.io/region=region-a
oc label node <node2> topology.kubernetes.io/region=region-b
oc label node <node3> topology.kubernetes.io/region=region-05dc
```

**Why this step exists, and why it has to come first:** two separate
mechanisms in this chart depend on it —

1. **Scheduling** — `nodeSelector: topology.kubernetes.io/region: <name>`
   on each region's pods (`templates/kafka.yaml`,
   `templates/kraftcontroller.yaml`) forces that region's pods onto only
   nodes carrying that exact label. Skip this and Kubernetes will happily
   schedule "region-a" and "region-b" pods onto the *same* node — a
   single node failure could then take out replicas from multiple
   regions at once, defeating the entire point of spreading them.
2. **Replica placement** — `broker.rack=<region.name>` (set via
   `configOverrides` on the `Kafka` CR) tells Kafka's own replica
   placement algorithm to spread a partition's copies across regions.
   This only means anything if brokers are *actually* running in the
   regions their rack value claims — which depends entirely on point 1
   being enforced correctly.

So this isn't just "do this or pods stay `Pending`" (though that's the
symptom you'll see immediately if you skip it) — without real, distinct
nodes backing each region, you'd have no actual fault isolation even if
pods somehow scheduled anyway, just labels asserting a separation that
isn't really there.

**Why this still applies once you move to real separate clusters per
region, even though cluster boundaries already prevent cross-region
scheduling:** Kubernetes scheduling never crosses cluster boundaries — a
pod submitted to region-a's cluster can only ever land on a node that's
part of region-a's cluster. So the specific "pods from two regions land
on the same node" risk genuinely can't happen once each region is truly
a separate cluster. What still requires manual labeling is *where the
label itself comes from*: on public cloud (EKS/GKE/AKS), the cloud
provider auto-stamps `topology.kubernetes.io/region` onto every node at
creation time, using the real cloud region — no manual step needed. On
**on-prem/bare-metal clusters** (no cloud controller manager doing that
stamping), the label simply doesn't exist until a human applies it. In
that case, skipping this step doesn't risk cross-region blending (cluster
boundaries already prevent that) — it risks the pod having nothing to
schedule onto at all (empty `nodeSelector` match), and `broker.rack`
having no consistent, agreed-upon value to report.

Full detail and rationale for each row above is in the matching section
further down this document.

**No privileges to label nodes?** Set `nodeSelector.enabled: false` (a
top-level value, independent of `standalone`) in your values file. This
skips only the `nodeSelector` requirement, while keeping everything else
about that mode intact (real broker/controller counts, hard
anti-affinity, `mode: full`, etc.) — useful if you can get a cluster/
namespace to install into but can't get `oc label node` run against it.
Be clear-eyed about the tradeoff: without real node separation, Kubernetes
is free to schedule this region's pods anywhere, including onto the same
node another region's pods land on (in the mock/single-cluster case) —
so `broker.rack`'s value stops being a reliable description of physical
placement. Fine for proving the chart/config logic works; not something
to carry into an actual production rollout.

---

## Why 2.5DC, and why "2.5" specifically

A genuinely stretched Kafka cluster (one logical cluster, not independent
clusters + async replication) needs its KRaft controllers to reach
**quorum** — a majority vote — before any metadata change (leader
election, ISR update, new topic) is considered committed. This is what
prevents split-brain: two controllers can never simultaneously believe
they're in charge of the same partition, because two disjoint majority
groups can never exist out of the same voter set.

**This is why the controller count must be odd.** With an even number,
a network partition can split the controllers into two equal halves,
neither of which has a majority — the cluster freezes even though every
individual node is healthy. Odd numbers make an even split mathematically
impossible; there's always a decider.

**This is also why exactly 2 regions can never safely host a stretched
cluster.** However you split an odd controller count across 2 physical
locations, one location ends up holding the majority by itself — making
that specific location a single point of failure for the *entire*
cluster's control plane. Losing the *other* region is fine; losing the
majority-holding region halts the cluster even though the surviving
region is completely healthy. There's no split of controllers across 2
locations that fixes this — it's a structural property of majority-vote
systems, not a configuration problem.

**2.5DC is the minimal fix:** two full regions (running brokers + a
controller each) plus one lightweight "0.5" region that runs *only* a
single tiebreaker controller — no brokers, no partition data, minimal
footprint. Three physical locations means an odd, evenly-distributable
quorum: any *one* location can go down and the other two still hold a
majority. No region is ever a single point of failure.

Confluent's own guidance: use 2.5DC when extremely high availability is
paramount and the sites are connected by a stable, low-latency network —
not for sites that are geographically distant or connected by an
unstable/high-latency link (see "Connectivity requirements" below).

## Connectivity required between the three sites

All three OpenShift/Kubernetes clusters — including the lightweight 0.5DC
— need to satisfy CFK's cross-cluster networking prerequisites, since
Kubernetes has no built-in concept of "these three clusters are one
Kafka cluster":

- **Non-overlapping pod CIDRs** across all three clusters
- **Cross-cluster DNS resolution** — pods in region-a need to resolve and
  route to pods in region-b and the 0.5DC (and vice versa) for internal
  listeners
- **Node labels applied per region** — `topology.kubernetes.io/region=<name>`
  on every node, which this chart's `nodeSelector` and `broker.rack`
  config both depend on (see `oc label node ...` in the prerequisites)
- **Stable, low latency between region-a and region-b** — this is the
  link that carries actual partition replication traffic, and it's the
  most latency-sensitive part of the whole design. Confluent's guidance
  is explicit that 2.5DC isn't meant for distant/high-latency sites.
- **Reachability to the 0.5DC** — lighter requirement than the region-a
  ↔ region-b link, since it only carries controller/quorum traffic, not
  partition replication, but it still needs to be reliably reachable —
  an unreachable tiebreaker is as good as no tiebreaker.

None of this is Kafka-specific — it's infrastructure that has to exist
*before* any of the Kafka-level clusterID/quorum mechanics (below) can
work at all.

## One chart, two deployment shapes

The chart uses a single `standalone` boolean plus per-file values to
switch between two shapes without changing any template logic:

| | `values-standalone*.yaml` | `values-region-*.yaml` / `values-05dc.yaml` |
|---|---|---|
| `standalone` | `true` | `false` |
| Kubernetes clusters involved | 1 | 3 (one `helm install` each, separate `--kube-context`) |
| `nodeSelector` (region node labels) | skipped — not needed on a single test cluster | applied — required so pods land on the right region's nodes |
| Pod anti-affinity | soft (`preferred...`) — a 1-2 node test cluster may have nowhere else to schedule | hard (`required...`) — real fault isolation across nodes matters in production |
| `region.mode` | `full` (renders both CRs) | `full` for region-a/b, `light` for the 0.5DC (renders only `KRaftController`, no `Kafka` CR — no brokers) |

**Helm has no multi-cluster concept** — there's no single command that
installs "across" three clusters. Multi-region is three separate
`helm install` invocations, each with a different `--kube-context` and a
different region's values file. What ties those three separate releases
into *one* Kafka cluster isn't Helm or Kubernetes at all — it's the
shared `clusterID` (below) plus the network connectivity above.

## `clusterID`: glues the *Kafka* cluster, not the Kubernetes cluster

Two IDs that sound similar but are completely unrelated layers:

- **Kafka `clusterID`** — one UUID shared by every controller and broker
  that considers itself part of the same logical Kafka cluster. Generated
  by KRaft on the *first* controller's first boot, then explicitly reused
  by every other controller/broker you add — including ones in entirely
  different Kubernetes clusters. This is the actual mechanism that makes
  three separate `helm install`s into one Kafka cluster.
- **Kubernetes cluster identity** (e.g. a k3s/OpenShift cluster) — a
  completely separate concept with no relationship to the above. You
  could tear down the underlying Kubernetes cluster entirely, reattach
  the same PVCs to a fresh one, and — as far as Kafka is concerned —
  it's still "the same" cluster, because the `clusterID` (and the
  metadata log on those PVCs) never changed. Kubernetes has no
  comparable single "cluster ID" and Kafka has no awareness Kubernetes
  exists.

**Bootstrap sequence** (this is *why* region-a always goes first):
1. Install region-a with `cluster.clusterID: ""` — CFK/KRaft generates one
2. Fetch it: `kubectl get kraftcontroller <name> -n confluent -o jsonpath='{.status.clusterID}'`
3. Paste that value into region-b's and the 0.5DC's values files before
   installing them — they join the *existing* cluster identity rather
   than generating their own

Separately, every controller/broker also needs its own unique **node ID**
(`controllerIdOffset` / `brokerIdOffset`) — the opposite requirement from
`clusterID`: this must be *different* everywhere, never shared, and
critically must never overlap between controllers and brokers even
within the same region (CFK enforces a hard minimum of 100 on controller
offsets specifically, to keep the ranges apart). See the offset table in
`values.yaml`.

## TLS / mTLS

`tls.enabled: true` turns on encryption for **every** connection type in
the cluster via one shared secret (`kafka-tls`), for producer/consumer →
broker, broker → broker (replication), broker → controller, and
controller → controller (the Raft quorum traffic itself).

**Secret format: PEM, not JKS/PKCS12** — `fullchain.pem` / `privkey.pem`
/ `cacerts.pem`. CFK supports both formats and auto-detects which one a
secret uses based on which keys are present; this chart's scripts
deliberately use PEM. Reason: building a JKS/PKCS12 keystore requires an
extra conversion step (`openssl pkcs12 -export`), and OpenSSL 3.x's
default PKCS12 encryption is frequently unreadable by Java's own keystore
provider — surfaces at Kafka startup as
`InvalidAlgorithmParameterException: the trustAnchors parameter must be
non-empty`, since Java silently loads zero certs from a keystore it
can't actually parse. Plain PEM certs have no such compatibility layer
to get wrong, and need no password at all.

**Because it's one logical cluster, the same cert material must exist in
all three regions' namespaces** — `scripts/generate-and-distribute-tls.sh`
generates a single self-signed CA + cert (with SANs covering every
region's endpoints) and applies the identical secret to all three
`--kube-context`s in one run. Swap the generation step for your real PKI
in production; the "same secret everywhere" distribution requirement
stays the same either way.

**What this setup currently does *not* give you: client authentication.**
TLS as configured here proves *the server's* identity to the client (and
encrypts the traffic) — it does not by itself prove *the client's*
identity to the broker. That distinction matters directly for
`authorization.enabled` (simple ACLs): ACL grants are tied to a
`User:<principal>` identity, and without some form of client
authentication — mutual TLS (client certificates) or SASL — Kafka has no
real way to know which principal is connecting. Enabling ACLs without
also wiring up mTLS or SASL means every client effectively presents the
same anonymous/default identity, which defeats the purpose of the ACL
layer. Adding real mTLS (distinct client certs per principal, `ssl.client.auth=required`
on the listener) or SASL is the natural next step before relying on the
authorization block for anything real.

## Deploying: standalone (single cluster, for dev/test)

```bash
# 1. CRDs (from the CFK operator chart, applied separately — not
#    auto-installed the normal Helm crds/ way due to CRD size)
helm repo add confluentinc https://packages.confluent.io/helm
helm repo update
helm pull confluentinc/confluent-for-kubernetes --untar
kubectl apply --server-side -f confluent-for-kubernetes/crds/
kubectl get crd | grep platform.confluent.io   # confirm

# 2. The operator itself (CRDs alone create nothing — this is the
#    controller-manager that actually reconciles your CRs into pods)
helm install cfk-operator confluentinc/confluent-for-kubernetes \
  -n confluent --create-namespace
kubectl get pods -n confluent   # confirm it's Running

# 3. (Only if tls.enabled: true in your values) create the kafka-tls
#    secret in this one namespace before installing — see the TLS
#    section above for the openssl steps, or adapt
#    scripts/generate-and-distribute-tls.sh to a single context.

# 4. The cluster itself
helm install kafka-test . -f values-standalone.yaml -n confluent
kubectl get pods -n confluent -w
```

Two standalone values files, depending on what you're validating:
- `values-standalone.yaml` — fastest possible bring-up: 1 broker,
  1 controller, RF=1, TLS/ACLs off. Good for "does the chart even work."
- `values-standalone-multibroker.yaml` — 3 brokers, 3 controllers, RF=3,
  min.insync.replicas=2, TLS on. Good for validating real replication,
  ISR, and quorum survival (kill a pod, watch re-election) before ever
  touching multi-cluster infrastructure. Needs its own fresh `kafka-tls`
  secret and a fresh `clusterID` (uninstall the RF=1 release first —
  they're not compatible/continuous with each other).

## Deploying: multi-region (2.5DC, three real clusters)

Repeat steps 1-3 above **in each of the three Kubernetes clusters** (own
`--kube-context` each time), then:

```bash
# Region A — bootstrap first, no clusterID set yet
helm install kafka-region-a . -f values-region-a.yaml \
  -n confluent --kube-context region-a

kubectl get kraftcontroller kraftcontroller-region-a -n confluent \
  --kube-context region-a -o jsonpath='{.status.clusterID}'

# Paste that clusterID into values-region-b.yaml and values-05dc.yaml,
# then:
helm install kafka-region-b . -f values-region-b.yaml \
  -n confluent --kube-context region-b

helm install kafka-05dc . -f values-05dc.yaml \
  -n confluent --kube-context region-05dc   # KRaftController only, no brokers

# Verify the quorum formed across all three
kubectl get kraftcontroller -A --context region-a
kubectl get kafka -A --context region-a
```

Grant topic ACLs afterward with `scripts/example-acls.sh` (ACLs are a
runtime CLI action against the live cluster, not a CFK custom resource).

## Other things worth knowing (lessons from getting this running)

- **`image.application` / `image.init`, not `repository`/`tag`** — the
  CRD schema wants a single combined `image:tag` string per field
  (`confluentinc/cp-server:7.9.0`), and validates this strictly; a split
  repo/tag shape fails CRD admission entirely.
- **The node-ID-offset minimum (100) applies to `KRaftController`
  specifically**, and controller/broker ID ranges must never overlap
  *even within the same region* — Kafka's own startup validation
  rejects a broker node ID that's also a controller quorum voter ID.
  This can't be changed after the cluster is created, so plan spacing
  generously (`values.yaml` documents the convention used here).
- **CRDs and the operator are two separate installs** — applying the
  CRDs makes Kubernetes *accept* `Kafka`/`KRaftController` objects, but
  nothing reconciles them into actual pods until the operator itself is
  also installed and running.
- **`Pending`/`0/1 Running` isn't necessarily broken** — a fresh KRaft
  controller has to format its own metadata log and elect itself leader
  of its quorum on first boot; give it a minute or two, especially on
  modest hardware, before assuming something's actually stuck.
- **Storage class defaults differ per platform** — leave `storage.class: ""`
  to use the cluster's default StorageClass (`local-path` on k3s,
  whatever your Ceph/OCS class is named on OpenShift) rather than
  hardcoding one that may not exist on every cluster you test against.
- **Confluent's images have arm64 builds on newer Confluent Platform
  versions** — worth confirming (`docker manifest inspect`) before
  debugging what looks like a crash but is actually an architecture
  mismatch, particularly on Apple Silicon.
- **Adding brokers never rebalances existing topics automatically** —
  Kafka has no built-in auto-rebalancer; new brokers only get used for
  *new* topics/partitions unless you explicitly run
  `kafka-reassign-partitions` (or use Confluent's Self-Balancing
  Clusters feature on `cp-server`, not wired into this chart).
- **Unkeyed produces are not evenly distributed** — the sticky
  partitioner batches a whole producer session onto one partition at a
  time, not round-robin per record. Don't rely on "no key" for even
  spread; key by whatever field needs ordering (e.g. `order_id`) if
  partition placement matters to you.

## Schema Registry: end-to-end walkthrough (setup → register → produce/consume → conformance test)

This is the validated sequence — reflects what actually worked when tested
against a real standalone cluster, including the gotchas hit along the way.

### 1. Deploy Schema Registry (depends on Kafka already being up)

```bash
helm upgrade --install kafka-test . -f values-standalone.yaml -n confluent
kubectl get pods -n confluent
```

Confirm `schemaregistry-0` reaches `1/1 Running` — the `wait-for-kafka`
init container (see `templates/schemaregistry.yaml`) handles waiting for
Kafka automatically, no manual ordering needed.

### 2. Define your schema, using the `schemas/<name>/<name>-schema.yaml` convention

```yaml
name: payment
subjects: payment-value
format: avro
schema: |
  {
    "type": "record",
    "name": "Payment",
    "namespace": "io.example.payment",
    "fields": [
      { "name": "payment_id", "type": "string" },
      { "name": "order_id", "type": "string" },
      { "name": "amount", "type": "double" },
      { "name": "status", "type": "string" }
    ]
  }
```

### 3. Apply it — creates the ConfigMap + Schema CR, registered via REST by the operator

```bash
helm upgrade --install kafka-test . -f values-standalone.yaml -n confluent
kubectl get schema -n confluent
```

### 4. Confirm registration and note the schema ID

```bash
kubectl exec -it schemaregistry-0 -n confluent -- \
  curl -s http://localhost:8081/subjects/payment-value/versions/latest
```

Note the `id` field — reference it directly in later steps rather than
restating the full schema.

### 5. Create the target topic explicitly — don't assume auto-create

```bash
kubectl exec -it kafka-0 -n confluent -- kafka-topics \
  --bootstrap-server localhost:9092 --create --topic payment \
  --partitions 3 --replication-factor 1
```

### 6. Produce a conforming message

```bash
kubectl exec -it schemaregistry-0 -n confluent -- bash
LOG_DIR=/tmp kafka-avro-console-producer --broker-list kafka.confluent.svc.cluster.local:9092 --topic payment \
  --property schema.registry.url=http://localhost:8081 \
  --property value.schema.id=<id-from-step-4>
```

Type, then `Ctrl+D`:
```json
{"payment_id": "pay-1", "order_id": "order-101", "amount": 49.99, "status": "created"}
```

`LOG_DIR=/tmp` avoids a log4j permission crash on this image's default log
path (`/usr/bin/../logs/schema-registry.log`, not writable under a
non-root SCC/security-policy UID) — without it, the producer/consumer CLI
can die *before* actually sending or reading anything, which looks like a
silent failure if you don't redirect logging first.

### 7. Consume and confirm it decodes cleanly

```bash
LOG_DIR=/tmp kafka-avro-console-consumer --bootstrap-server kafka.confluent.svc.cluster.local:9092 --topic payment \
  --property schema.registry.url=http://localhost:8081 \
  --from-beginning
```

Expected output — proves the full round trip (schema-validated produce →
binary storage on the broker → schema-validated decode on consume):
```json
{"payment_id":"pay-1","order_id":"order-101","amount":49.99,"status":"created"}
```

### 8. Produce a non-conforming message, to see enforcement actually reject it

Same producer command as step 6, but with a payload that violates the
schema — e.g. a missing required field and the wrong type on `amount`:
```json
{"payment_id": "pay-2", "amount": "not-a-number", "status": "created"}
```

Expect Avro to reject this **client-side, before it reaches the broker** —
a `SerializationException`/schema-mismatch error in the producer's own
output, not a silent write. (Run this fresh against an already-existing
topic — in initial testing, the first non-conforming attempt was
confounded by the topic not existing yet, which produces a different,
unrelated `UNKNOWN_TOPIC_OR_PARTITION` error instead.)

### Troubleshooting notes from getting this working

- **`kafka-avro-console-producer`/`-consumer` live on the `cp-schema-registry`
  image (`schemaregistry-0`), not on the broker image (`kafka-0`)** — the
  broker image only ships plain, non-Avro-aware `kafka-console-producer`/
  `-consumer`. Run Avro-aware commands from `schemaregistry-0`.
- **`which` may not exist in these minimal images** — use `type` or
  `command -v` instead when checking whether a CLI tool is present.
- **`kafka-run-class kafka.tools.GetOffsetShell` no longer exists** on
  current Kafka versions — use `kafka-topics --describe` or a plain
  `kafka-console-consumer --from-beginning --timeout-ms 5000` instead to
  sanity-check whether records exist on a topic.
- **A plain (non-Avro) consumer will happily print Avro-encoded records**
  as garbled binary text rather than erroring — that's not corruption,
  it's just binary bytes force-printed as text. Useful as a quick "did
  *anything* get written" check without needing Schema Registry reachable,
  but not a substitute for the real Avro-aware consumer to verify
  conformance/decoding.
- **The whole enforcement mechanism is opt-in per client** — none of this
  applies to any producer that doesn't use an Avro-aware serializer. A
  plain `kafka-console-producer` (or a misconfigured app) can still write
  arbitrary, non-conforming bytes straight into `payment`, completely
  bypassing every check above.

## Mocking the 2.5DC regions as namespaces (before real multi-cluster infra)

A cheaper intermediate step between the single-cluster multi-broker test
and the real 3-cluster rollout: mock each "region" as its own **namespace**
on one real, multi-node cluster (e.g. your existing 7-node OpenShift
cluster), rather than three separate Kubernetes clusters.

**Prerequisite — the CFK operator must be watching all three namespaces,
not just its own.** By default CFK installs with `namespaced: true`,
meaning it only reconciles CRs in the single namespace it was installed
into. If your operator was originally installed for the single-namespace
`confluent` setup, it has **zero visibility** into `kafka-region-a/b/05dc`
— CRs applied there will sit with no `status`, no StatefulSet, no events,
silently unreconciled, since the operator never sees them at all. Fix:
```bash
helm upgrade --install cfk-operator confluentinc/confluent-for-kubernetes \
  --set namespaced=false -n confluent
kubectl delete pod -n confluent -l app=confluent-operator   # restart to pick up the change
```
Do this before installing anything into the 3 mock namespaces below.

**What this genuinely validates:** the Kafka-level mechanics — the
`clusterID` bootstrap flow across independently-installed releases,
`broker.rack`/rack-aware replica placement, `controllerIdOffset`/
`brokerIdOffset` spacing, node-label-driven scheduling via `nodeSelector`.

**What this does NOT validate:** the actual cross-cluster networking
prerequisites from earlier in this README — non-overlapping pod CIDRs,
cross-cluster DNS resolution. Those don't apply within a single cluster's
already-flat networking, so this step can't exercise them. Treat this as
a de-risking stage, not a substitute for the real rollout.

### 1. Label real, distinct nodes per mocked region

Node labels are what actually create fault-domain separation here — a
namespace alone doesn't. Pick non-overlapping subsets of your real nodes:

```bash
oc label node <node1> <node2> topology.kubernetes.io/region=region-a
oc label node <node3> <node4> topology.kubernetes.io/region=region-b
oc label node <node5> topology.kubernetes.io/region=region-05dc
```

### 2. Generate and distribute TLS across the three namespaces

```bash
JKS_PASSWORD=<your-password> ./scripts/generate-and-distribute-tls-mock.sh
```

(Creates the three namespaces if they don't already exist, and applies
the identical `kafka-tls` secret to each — same cert material requirement
as the real multi-cluster case, just across namespaces instead of
clusters.)

### 3. Bootstrap region-a's namespace first

```bash
helm install kafka-region-a . -f values-mock-region-a.yaml -n kafka-region-a
kubectl get kraftcontroller kraftcontroller-region-a -n kafka-region-a \
  -o jsonpath='{.status.clusterID}'
```

### 4. Paste that clusterID into the other two mock values files, then install

```bash
helm install kafka-region-b . -f values-mock-region-b.yaml -n kafka-region-b
helm install kafka-05dc . -f values-mock-05dc.yaml -n kafka-region-05dc
```

### 5. Verify the quorum formed across all three namespaces

```bash
kubectl get kraftcontroller -A | grep region
kubectl get kafka -A | grep region
```

All `KRaftController` resources should report the same `clusterID`. From
here, the same reassign/kill-a-pod/leader-election tests from the
single-cluster multi-broker walkthrough apply — just now genuinely
spanning distinct node labels and namespaces rather than one flat pool.

**Graduating to real clusters afterward:** the `values-region-a/b.yaml`
and `values-05dc.yaml` files (not the `-mock-` ones) are what you'd use
for that — same shape, but namespace defaults to the shared `confluent`
namespace (fine once each region is truly a separate cluster/context) and
`externalAccess` is wired on for region-a/b. The mock files exist purely
to prove the Kafka-level logic first, cheaply, before standing up the
real infrastructure and its networking prerequisites.
