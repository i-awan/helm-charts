# kafka-2.5dc

A Helm chart that renders the CFK (Confluent for Kubernetes) `KRaftController`,
`Kafka`, `SchemaRegistry`, and `Schema` custom resources needed to run one
logical Kafka cluster stretched across a **2.5-datacenter (2.5DC) topology**:
two full regions running brokers and a controller each, plus one lightweight
witness region running only a KRaft controller.

This document summarizes the reasoning behind the design, not just the
commands — see the inline `# comments` in each `values-*.yaml` for
field-level detail.

---

## Quick start — do these in order

Two ways to run this chart, depending on what infrastructure you have.
**Node labeling is step 1 wherever it appears — do it before installing
anything, or pods will sit unschedulable with no useful error on the CR
itself.**

| Mode | Values file(s) | Order of operations |
|---|---|---|
| **Mock 2.5DC** (3 namespaces, 1 real cluster — start here) | `values-mock-region-a/b.yaml`, `values-mock-05dc.yaml` | **0. Reinstall the CFK operator with `namespaced=false` — do this first, or CRs in the new namespaces silently never reconcile** → 1. Label real, distinct nodes per region → 2. `generate-and-distribute-tls-mock.sh` (if using TLS) → 3. install region-a, fetch its `clusterID` → 4. paste into region-b/05dc's values files, install those two — the static voter list fixes quorum *discovery*, but `clusterID` still needs this one manual propagation step |
| **Real 2.5DC** (3 separate clusters) | `values-region-a/b.yaml`, `values-05dc.yaml` | **1. Label nodes in each cluster** → 2. verify cross-cluster networking prerequisites → 3. CRDs+operator per cluster (into the same namespace as that cluster's workload) → 4. `generate-and-distribute-tls.sh` → 5. install region-a, fetch its `clusterID` → 6. paste into region-b/05dc's values files, install those two |

**The node-labeling command:**
```bash
kubectl get nodes   # pick real node names first
oc label node <node1> topology.kubernetes.io/region=region-a
oc label node <node2> topology.kubernetes.io/region=region-b
oc label node <node3> topology.kubernetes.io/region=region-05dc
```

**Why this step exists:** two separate mechanisms depend on it —
`nodeSelector` (Kubernetes-level scheduling onto the right region's
nodes) and `broker.rack` (Kafka-level replica placement across
regions). **Caveat confirmed during validation:** `spec.podTemplate.nodeSelector`
was silently rejected as an unknown field on the CRD version tested —
see "Cross-namespace/cross-region KRaft quorum" below. Confirm this
works on your own CFK/CRD version before relying on it for real fault
isolation; `broker.rack` is unaffected either way, since it's set via a
config override, not the structured field.

**No privileges to label nodes?** Set `nodeSelector.enabled: false` in
your values file — skips only the `nodeSelector` requirement and
switches pod anti-affinity from hard to soft, while keeping everything
else about that mode intact. Without real node separation, Kubernetes
is free to schedule a region's pods anywhere, so `broker.rack`'s value
stops being a reliable description of physical placement — fine for
proving the chart/config logic, not for production.

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
  config both depend on (see the node-labeling caveat above)
- **Stable, low latency between region-a and region-b** — this is the
  link that carries actual partition replication traffic, and it's the
  most latency-sensitive part of the whole design. Confluent's guidance
  is explicit that 2.5DC isn't meant for distant/high-latency sites.
- **Reachability to the 0.5DC** — lighter requirement than the region-a
  ↔ region-b link, since it only carries controller/quorum traffic, not
  partition replication, but it still needs to be reliably reachable —
  an unreachable tiebreaker is as good as no tiebreaker.

**Note:** the mock (namespace-based) setup below sidesteps all of the
above, since one Kubernetes cluster's networking is already flat and
non-overlapping by definition. It validates everything Kafka-level in
this document; it does **not** validate any of the bullets above — that
only happens on the real 3-cluster rollout.

## Cross-namespace/cross-region KRaft quorum: the real bootstrapping requirement

This is the single most important finding from building this chart —
read this before doing a real install.

**Matching `cluster.clusterID` across regions is NOT sufficient to form
one quorum.** It's a necessary label, but nowhere close to sufficient.
Proof, captured live on a real cluster:

```bash
kubectl exec -it kraftcontroller-region-a-0 -n kafka-region-a -- \
  kafka-metadata-quorum --bootstrap-controller localhost:9074 describe --status
```
Before the fix below, this showed **only region-a's own controller** as
a voter, and region-a's own two brokers as observers — region-b's and
the 0.5DC's controllers and brokers didn't appear at all, not even as
non-voting observers. Three `KRaftController` CRs sharing one `clusterID`
string were, in practice, three completely isolated single-node quorums
that happened to agree on a label.

**Why:** nothing was telling any controller *where* the others actually
live on the network. `clusterID` is just an identity check performed
once two controllers already talk to each other — it was never a
discovery mechanism.

**The fix — an explicit static quorum voter list, identical on every
region:**
```yaml
staticQuorumVoters:
  - brokerEndpoint: kraftcontroller-region-a-0.kraftcontroller-region-a.kafka-region-a.svc.cluster.local:9074
    nodeId: 100
  - brokerEndpoint: kraftcontroller-region-b-0.kraftcontroller-region-b.kafka-region-b.svc.cluster.local:9074
    nodeId: 200
  - brokerEndpoint: kraftcontroller-region-05dc-0.kraftcontroller-region-05dc.kafka-region-05dc.svc.cluster.local:9074
    nodeId: 300
```
This same list — every controller, every region — must be set
identically in every region's values file, not just a self-reference.
It's already wired into `values-mock-region-a/b.yaml` and
`values-mock-05dc.yaml`. For a real 3-cluster rollout, swap the internal
per-namespace DNS names above for whatever externally-reachable
addresses each region's controller advertises across the cluster
boundary.

**A second, independent gotcha found while fixing the first one: CRD
schema version mismatch.** CFK's own structured field for this
(`spec.listeners.controllerQuorumVoters`, per Confluent's current docs)
was silently rejected as `unknown field` by this cluster's installed CRD
version — the value never took effect at all, with no hard error, just
a warning easy to miss. Two other structured fields hit the exact same
silent-rejection pattern: `spec.podTemplate.initContainers` (a
Schema Registry startup-ordering nicety — cosmetic, not blocking) and
`spec.podTemplate.nodeSelector` (node-label-based pod scheduling doesn't
currently work on this CRD version regardless of the `nodeSelector.enabled`
setting).

**The working fix bypasses the broken structured field entirely**, using
`configOverrides.server` — the same raw-Kafka-property passthrough
mechanism already used for ACL settings, which isn't subject to CRD
schema validation the same way:
```yaml
configOverrides:
  server:
    - controller.quorum.voters=100@kraftcontroller-region-a-0...:9074,200@kraftcontroller-region-b-0...:9074,300@kraftcontroller-region-05dc-0...:9074
```
This is what `templates/kraftcontroller.yaml` actually renders — built
automatically from `staticQuorumVoters` via a Helm range, not something
you write by hand.

**Practical implication for every install you run:** always check
`helm install`/`upgrade` output for `Warning: unknown field` lines. A
structured CR field silently doing nothing is far more dangerous than an
outright error — the install "succeeds," the pod runs, and the actual
behavior you configured simply never happens. When in doubt, prefer
`configOverrides` (raw properties) over a newer structured field if
you're unsure your installed CRD version supports it.

**A quorum-topology change like this is not safe to apply in place** on
a cluster that already has committed metadata — a controller that
already bootstrapped itself as a lone voter doesn't cleanly reconcile
into a multi-voter static list via a config change alone. If you're
retrofitting this onto an existing cluster, do a clean wipe (`helm
uninstall` + delete PVCs across all regions) and reinstall fresh with
the static voter list already in place from the start, rather than
patching it into a running cluster.

**The static voter list fixes quorum *discovery*, not `clusterID`
propagation — those remain two separate steps.** Even with the voter
list in place, region-a still has to be installed first (its
`KRaftController` generates the shared `clusterID` on first boot), then
that value fetched and pasted into region-b's and the 0.5DC's values
files before installing those two — see "Deploying: mock 2.5DC" below
for the exact commands. Leaving `clusterID` blank on all three and
installing simultaneously would let each generate its own independent
random UUID, and a controller refuses to join a quorum whose
`clusterID` doesn't match its own already-formatted storage — a
different failure mode than the quorum-discovery problem this section
is otherwise about, but one that would block the cluster from forming
just as effectively.

**Confirmed working, end to end, after the fix:**
```
CurrentVoters: [{id:100,...}, {id:200,...}, {id:300,...}]   # all three
CurrentObservers: [{id:210,...}, {id:110,...}, {id:111,...}]  # all brokers, cluster-wide
```
Followed by: a 3-partition, RF=3, `min.insync.replicas=2` topic created
successfully across all 3 brokers; every partition's `Replicas:` showing
`110,111,210`; and a record produced via `kafka-avro-console-producer`
against region-a's broker, successfully decoded via the Avro-aware
consumer pointed at region-b's broker — real, physical cross-namespace
replication, not just matching config.

## Rack-aware fetching: keeping consumers local to their region

Producers can never be guaranteed to stay in-region — a producer must
write to whichever broker currently **leads** the target partition,
and leadership rotates across all brokers regardless of region to
spread load. There's no "write to a local follower" option; only the
leader accepts writes at all.

**Consumers are different — followers hold valid, readable data too.**
`rackAwareFetching.enabled: true` (default) sets
`replica.selector.class=RackAwareReplicaSelector` on every broker,
which lets a consumer that sets `client.rack=<region>` on its *own*
config fetch from a local replica instead of always crossing to the
leader. It's a preference, not a guarantee — falls back to the leader
if no local replica exists — and it only affects consume; produce
behavior is unaffected either way. The consuming application has to
set `client.rack` itself; nothing server-side can force this on an
unconfigured client.

## `clusterID`: glues the *Kafka* cluster, not the Kubernetes cluster

Two IDs that sound similar but are completely unrelated layers:

- **Kafka `clusterID`** — one UUID shared by every controller and broker
  that considers itself part of the same logical Kafka cluster. As
  established above, this alone does **not** form the quorum — it's a
  necessary identity check, layered on top of the static voter list.
- **Kubernetes cluster identity** (e.g. a k3s/OpenShift cluster) — a
  completely separate concept with no relationship to the above. You
  could tear down the underlying Kubernetes cluster entirely, reattach
  the same PVCs to a fresh one, and — as far as Kafka is concerned —
  it's still "the same" cluster, because the `clusterID` (and the
  metadata log on those PVCs) never changed. Kubernetes has no
  comparable single "cluster ID" and Kafka has no awareness Kubernetes
  exists.

Separately, every controller/broker also needs its own unique **node
ID** (`controllerIdOffset` / `brokerIdOffset`) — the opposite
requirement from `clusterID`: this must be *different* everywhere, never
shared, and critically must never overlap between controllers and
brokers even within the same region (CFK enforces a hard minimum of 100
on controller offsets specifically, to keep the ranges apart). See the
offset table in `values.yaml`.

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
all three regions' namespaces** — `scripts/generate-and-distribute-tls-mock.sh`
(3 namespaces, 1 cluster) or `scripts/generate-and-distribute-tls.sh` (3
real clusters) generates a single self-signed CA + cert (with SANs
covering every region's endpoints, and `-not_before`/`-not_after` set
explicitly to avoid clock-skew "not yet valid" failures) and applies the
identical secret everywhere in one run. Swap the generation step for
your real PKI in production; the "same secret everywhere" distribution
requirement stays the same either way. Both scripts require OpenSSL 3.x
— macOS's bundled `/usr/bin/openssl` is LibreSSL and will fail the
version check; install real OpenSSL via `brew install openssl@3`.

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
layer. Adding real mTLS (distinct client certs per principal,
`ssl.client.auth=required` on the listener) or SASL is the natural next
step before relying on the authorization block for anything real.

**Both TLS and authorization are commonly parked (`false`/`false`)
during initial mechanics validation** — see the mock values files — to
isolate quorum/replication testing from security-layer variables. Turn
both back on once the multi-region mechanics are proven; nothing about
re-enabling them depends on anything else in this document.

## Deploying: mock 2.5DC (3 namespaces, 1 real cluster)

The cheapest way to validate the real quorum/replication mechanics
before touching multi-cluster infrastructure.

### 0. CFK operator must watch all three namespaces

By default CFK installs with `namespaced: true`, reconciling only the
namespace it was installed into. Fix:
```bash
helm upgrade --install cfk-operator confluentinc/confluent-for-kubernetes \
  --set namespaced=false -n confluent
kubectl delete pod -n confluent -l app=confluent-operator   # restart to pick up the change
```

### 1. Label real, distinct nodes per mocked region

```bash
oc label node <node1> topology.kubernetes.io/region=region-a
oc label node <node2> topology.kubernetes.io/region=region-b
oc label node <node3> topology.kubernetes.io/region=region-05dc
```

### 2. CRDs (once per cluster)

```bash
helm repo add confluentinc https://packages.confluent.io/helm
helm repo update
helm pull confluentinc/confluent-for-kubernetes --untar
kubectl apply --server-side -f confluent-for-kubernetes/crds/
kubectl get crd | grep platform.confluent.io   # confirm
```

### 3. TLS (only if `tls.enabled: true` in your values)

No password needed — this script uses PEM, not JKS/PKCS12:
```bash
./scripts/generate-and-distribute-tls-mock.sh
```

### 4. Bootstrap region-a first, then capture and propagate its clusterID

The static voter list solves *quorum discovery* (who's in the cluster),
but every controller still needs to agree on the same `clusterID` (an
explicit identity check, separate from discovery) — leaving it blank on
all three would let each generate its own random UUID independently, and
a controller refuses to join a quorum whose `clusterID` doesn't match
its own already-formatted storage. So this part still isn't fully
simultaneous, even with the static voter list in place:

```bash
helm install kafka-region-a . -f values-mock-region-a.yaml -n kafka-region-a
kubectl get pods -n kafka-region-a -w   # wait for kraftcontroller-region-a-0 to reach 1/1
```

Fetch the generated `clusterID`:
```bash
kubectl get kraftcontroller kraftcontroller-region-a -n kafka-region-a \
  -o jsonpath='{.status.clusterID}'
```

Paste that value into `cluster.clusterID` in **both**
`values-mock-region-b.yaml` and `values-mock-05dc.yaml`, replacing the
`REPLACE_WITH_CLUSTER_ID_FROM_REGION_A` placeholder, then install the
remaining two:
```bash
helm install kafka-region-b . -f values-mock-region-b.yaml -n kafka-region-b
helm install kafka-05dc . -f values-mock-05dc.yaml -n kafka-region-05dc
```

### 5. Verify the quorum actually formed

```bash
kubectl exec -it kraftcontroller-region-a-0 -n kafka-region-a -- \
  kafka-metadata-quorum --bootstrap-controller localhost:9074 describe --status
```
`CurrentVoters` should list all three node IDs (`100`, `200`, `300`).

### 6. Prove real cross-region replication — create the topic

```bash
kubectl exec -it kafka-0 -n kafka-region-a -- kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --topic orders3 --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2

kubectl exec -it kafka-0 -n kafka-region-a -- kafka-topics \
  --bootstrap-server localhost:9092 --describe --topic orders3
```
Every partition's `Replicas:` should include brokers from **both**
region-a and region-b.

### 7. The valuable test: produce in region-a, consume in region-b

This is the test that actually proves cross-region replication is
physically real, not just correct on paper — a same-region test could
pass even on a broken, effectively single-region deployment.

**Produce, against region-a's broker:**
```bash
kubectl exec -it kafka-0 -n kafka-region-a -- kafka-console-producer \
  --bootstrap-server localhost:9092 --topic orders3
```
Type a record, then **`Ctrl+D`** (not `Ctrl+C` — a hard kill can skip
flushing the record before the process exits):
```
order-847 created
```

**Consume, against region-b's broker — a different namespace, a
different broker, that never received the write directly:**
```bash
kubectl exec -it kafka-0 -n kafka-region-b -- kafka-console-consumer \
  --bootstrap-server kafka.kafka-region-b.svc.cluster.local:9092 \
  --topic orders3 --from-beginning
```
If the record shows up here, that's real, physical confirmation the
record's bytes replicated across the namespace boundary — see the
Schema Registry walkthrough below for the Avro-aware version of this
same test, with a fuller explanation of exactly what it proves.

### 8. The schema-file convention — how `payment-schema.yaml` was authored

`schemas/payment/payment-schema.yaml` already ships with this chart
(that's why step 9 below needs no registration step) — this is the
convention used to author it, useful when you want to add your own
schema alongside it. The file lives inside the chart directory itself;
`templates/schemas.yaml` discovers anything matching this pattern
automatically via `.Files.Glob "schemas/**/*.yaml"`. It isn't a
Kubernetes object at this point, just a file on disk:

```bash
mkdir -p schemas/shipments
cat > schemas/shipments/shipments-schema.yaml << 'EOF'
name: shipments
subjects: shipments-value
format: avro
schema: |
  {
    "type": "record",
    "name": "Shipment",
    "namespace": "io.example.shipments",
    "fields": [
      { "name": "shipment_id", "type": "string" },
      { "name": "order_id", "type": "string" },
      { "name": "status", "type": "string" }
    ]
  }
EOF
```

Dropping in a file like this and running `helm upgrade` gets it
rendered and registered automatically — no template changes needed. The
next step shows exactly what that rendering and registration produced
for the `payment` schema already bundled with the chart.

### 9. Schema Registry was already populated — no `helm upgrade` needed

Unlike the from-scratch walkthrough later in this document, nothing
extra needs installing here. `schemas/payment/payment-schema.yaml`
ships with this chart, and `schemaRegistry.enabled`/`schemas.enabled`
both default to `true` — so the moment `helm install` ran in step 4,
the `payment` schema was already rendered and registered. Confirm it:

```bash
kubectl exec -it schemaregistry-0 -n kafka-region-a -- \
  curl -s http://localhost:8081/subjects/payment-value/versions/latest
```
This should already return a registered schema — nothing to create.

**What actually happened under the hood, back at step 4, worth being
explicit about:**

1. **`templates/schemas.yaml`** found `schemas/payment/payment-schema.yaml`
   inside the chart (via `.Files.Glob "schemas/**/*.yaml"`) and rendered
   two Kubernetes objects from it: a `ConfigMap` named
   `payment-schema-config` (holding the raw Avro JSON as plain text —
   Kubernetes doesn't parse or validate it, it's just inert data at this
   point) and a `Schema` custom resource named `payment` (holding
   `spec.name: payment-value`, `spec.data.format: avro`, and a reference
   to that ConfigMap).

   Concretely, this is the actual `ConfigMap` that got created in
   `kafka-region-a` during step 4 — confirm it yourself with
   `kubectl get configmap payment-schema-config -n kafka-region-a -o yaml`:
   ```yaml
   apiVersion: v1
   kind: ConfigMap
   metadata:
     name: payment-schema-config
     namespace: kafka-region-a
   data:
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
   Notice this is just your `schemas/payment/payment-schema.yaml`
   file's `schema:` block, copied verbatim into a Kubernetes object —
   nothing added, nothing transformed. The `Schema` CR (below) is the
   part that actually turns this inert text into a real registration.
2. **The CFK operator**, watching for `Schema` CRs, picked this one up,
   read the referenced ConfigMap's content, and made a real REST call —
   `POST /subjects/payment-value/versions` — against the `SchemaRegistry`
   instance this same chart install had just stood up.
3. **Schema Registry** validated the schema, assigned it a version and a
   globally unique schema ID, and durably persisted that registration as
   an actual Kafka record in its internal `_schemas` compacted topic —
   on the same underlying Kafka cluster, not a separate store.
4. **The operator wrote the result back** into the `Schema` CR's
   `.status` field — which is exactly what the `kubectl get schema
   payment -n kafka-region-a -o yaml` command would show you, if you
   want to see the registered version/ID reflected there too.

None of this required a topic to exist, a producer to run, or any
manual `curl`/CLI step — it's the same declarative, operator-reconciled
pattern as the `Kafka` and `KRaftController` CRs themselves, just aimed
at Schema Registry's REST API instead of the Kafka Admin API.

### 10. Produce via region-a, consume via region-b — using the bundled schema

Create the target topic (not auto-created):
```bash
kubectl exec -it kafka-0 -n kafka-region-a -- kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --topic payment --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2
```

Fetch the schema ID (from step 9's `curl` output, or re-run it), then
produce from **region-a**:
```bash
kubectl exec -it schemaregistry-0 -n kafka-region-a -- bash
LOG_DIR=/tmp kafka-avro-console-producer --broker-list kafka.kafka-region-a.svc.cluster.local:9092 --topic payment \
  --property schema.registry.url=http://localhost:8081 \
  --property value.schema.id=<id-from-step-8>
```
Type, then **`Ctrl+D`**:
```json
{"payment_id": "pay-1", "order_id": "order-101", "amount": 49.99, "status": "created"}
```

Consume from **region-b** — a different namespace, a different
broker, a different (never-registered-anything-itself) Schema Registry
instance:
```bash
kubectl exec -it schemaregistry-0 -n kafka-region-b -- bash
LOG_DIR=/tmp kafka-avro-console-consumer --bootstrap-server kafka.kafka-region-b.svc.cluster.local:9092 --topic payment \
  --property schema.registry.url=http://localhost:8081 \
  --from-beginning
```
Expected output — decoded correctly on region-b's side, proving the
replication and the shared `_schemas` topic both genuinely span the
cluster, not just region-a in isolation:
```json
{"payment_id":"pay-1","order_id":"order-101","amount":49.99,"status":"created"}
```

## Deploying: real 2.5DC (3 separate clusters)

Same shape as above, but each region is a genuinely separate Kubernetes
cluster (own `--kube-context`), so the cross-cluster networking
prerequisites from earlier actually apply and must be verified first.

**Namespace naming differs from the mock setup in one important way.**
Each cluster's operator defaults to `namespaced: true` (unlike the
mock's `namespaced: false`) — meaning it only watches the single
namespace it was installed into. So the operator must be installed
**into the same namespace as that cluster's workload**, per cluster:
`kafka-region-a` on the region-a cluster, `kafka-region-b` on the
region-b cluster, `kafka-region-05dc` on the 0.5DC cluster — matching
the `namespace:` value already set in `values-region-a/b.yaml` and
`values-05dc.yaml`.

Repeat, **in each of the three clusters**:

```bash
# On the region-a cluster:
helm upgrade --install cfk-operator confluentinc/confluent-for-kubernetes \
  -n kafka-region-a --create-namespace --kube-context region-a
kubectl apply --server-side -f confluent-for-kubernetes/crds/ --context region-a
# (repeat with -n kafka-region-b --kube-context region-b, and
#  -n kafka-region-05dc --kube-context region-05dc, on those clusters)
```

Then TLS (if `tls.enabled: true`):
```bash
./scripts/generate-and-distribute-tls.sh
```

Then bootstrap region-a first and propagate its `clusterID` the same
way as the mock setup (see step 4 above) — the static voter list
resolves quorum *discovery*, but every controller still needs an
identical `clusterID`, which only region-a's first boot generates:
```bash
helm install kafka-region-a . -f values-region-a.yaml -n kafka-region-a --kube-context region-a
kubectl get kraftcontroller kraftcontroller-region-a -n kafka-region-a --context region-a \
  -o jsonpath='{.status.clusterID}'
```
Paste that into `cluster.clusterID` in `values-region-b.yaml` and
`values-05dc.yaml`, then:
```bash
helm install kafka-region-b . -f values-region-b.yaml -n kafka-region-b --kube-context region-b
helm install kafka-05dc . -f values-05dc.yaml -n kafka-region-05dc --kube-context region-05dc
```

Verify and test the same way as the mock setup (steps 5-7 above),
adding `--context <context>` to each command as appropriate. Grant
topic ACLs afterward with `scripts/example-acls.sh` once
`authorization.enabled: true`.

## Schema Registry: end-to-end walkthrough (setup → register → produce/consume → conformance test)

This is the validated sequence — reflects what actually worked when
tested against the `kafka-region-a` namespace, including the gotchas hit
along the way. Swap the namespace/service DNS if you're running this
against a real cluster instead — the commands are otherwise identical.

### 1. Confirm Schema Registry is up (depends on Kafka already being up)

```bash
kubectl get pods -n kafka-region-a
```
Confirm `schemaregistry-0` reaches `1/1 Running`.

### 2. Apply it — creates the ConfigMap + Schema CR, registered via REST by the operator

```bash
helm upgrade --install kafka-region-a . -f values-mock-region-a.yaml -n kafka-region-a
kubectl get schema -n kafka-region-a
```

### 3. Confirm registration and note the schema ID

```bash
kubectl exec -it schemaregistry-0 -n kafka-region-a -- \
  curl -s http://localhost:8081/subjects/payment-value/versions/latest
```
Note the `id` field — reference it directly in later steps rather than
restating the full schema.

### 4. Create the target topic explicitly — don't assume auto-create

```bash
kubectl exec -it kafka-0 -n kafka-region-a -- kafka-topics \
  --bootstrap-server localhost:9092 --create --topic payment \
  --partitions 3 --replication-factor 3 --config min.insync.replicas=2
```

### 5. Produce a conforming message

```bash
kubectl exec -it schemaregistry-0 -n kafka-region-a -- bash
LOG_DIR=/tmp kafka-avro-console-producer --broker-list kafka.kafka-region-a.svc.cluster.local:9092 --topic payment \
  --property schema.registry.url=http://localhost:8081 \
  --property value.schema.id=<id-from-step-3>
```
Type, then **`Ctrl+D`** (not `Ctrl+C` — a hard kill can skip flushing
the record before the process exits):
```json
{"payment_id": "pay-1", "order_id": "order-101", "amount": 49.99, "status": "created"}
```

`LOG_DIR=/tmp` avoids a log4j permission crash on this image's default
log path (not writable under a non-root SCC/security-policy UID) —
without it, the producer/consumer CLI can die *before* actually sending
or reading anything.

### 6. The valuable test: consume via region-b, not region-a — proving replication is physically real

```bash
kubectl exec -it schemaregistry-0 -n kafka-region-b -- bash
LOG_DIR=/tmp kafka-avro-console-consumer --bootstrap-server kafka.kafka-region-b.svc.cluster.local:9092 --topic payment \
  --property schema.registry.url=http://localhost:8081 \
  --from-beginning
```
Expected output:
```json
{"payment_id":"pay-1","order_id":"order-101","amount":49.99,"status":"created"}
```

**Why this specific test matters, and what a same-region test would have
missed:** consuming from `kafka-region-a` after producing there proves
almost nothing about the *multi-region* claim — a broken, single-region
Kafka install could pass that test just as easily. What actually needs
proving is that a record written through region-a's broker is physically
retrievable through a **completely different broker, in a different
namespace, that never received the write directly** — that's the entire
point of RF=3 replication and the whole reason this chart exists.

This one test exercises the full cross-region chain in a single pass:
- The record replicated from region-a's broker to region-b's broker over
  the network — real bytes crossing the namespace boundary, not just a
  topic's metadata *claiming* replicas exist in both regions.
- Region-b's **own, independently-running** Schema Registry instance
  correctly resolved the schema ID and decoded the record — even though
  *it never registered that schema itself*. It only succeeded because
  both Schema Registry instances are backed by the same shared `_schemas`
  compacted topic on the same underlying stretched Kafka cluster.
- The shared `clusterID` and static quorum voter list (see "Cross-namespace/cross-region
  KRaft quorum" above) are doing real work — none of this is possible if
  region-a and region-b were actually two separate, merely
  similarly-labeled Kafka clusters rather than one genuine logical
  cluster.

If you only run one validation step after building this chart, make it
this one — a same-region produce/consume test can pass even when the
multi-region wiring is completely broken; this test cannot.

### 7. Produce a non-conforming message, to see enforcement actually reject it

Same producer command as step 5, but with a payload that violates the
schema — e.g. a missing required field and the wrong type on `amount`:
```json
{"payment_id": "pay-2", "amount": "not-a-number", "status": "created"}
```
Expect Avro to reject this **client-side, before it reaches the broker**
— a `SerializationException`/schema-mismatch error in the producer's own
output, not a silent write.

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
  *anything* get written" check, not a substitute for the real Avro-aware
  consumer to verify conformance/decoding.
- **The whole enforcement mechanism is opt-in per client** — none of this
  applies to any producer that doesn't use an Avro-aware serializer. A
  plain `kafka-console-producer` (or a misconfigured app) can still write
  arbitrary, non-conforming bytes straight into `payment`, completely
  bypassing every check above.

## Other things worth knowing (lessons from getting this running)

- **Always check `helm install`/`upgrade` output for `Warning: unknown field`
  lines** — a structured CR field that your installed CRD version doesn't
  recognize gets silently dropped, not rejected. Hit this three separate
  times on the same cluster (`controllerQuorumVoters`,
  `podTemplate.initContainers`, `podTemplate.nodeSelector`) — see the
  quorum section above for the full story and the working
  `configOverrides`-based fallback.
- **A single boolean flag driving two unrelated Kubernetes mechanisms
  can regress silently when only one caller path is fixed** —
  `nodeSelector.enabled` gates both the `nodeSelector` block and
  soft-vs-hard pod anti-affinity across three separate templates
  (`Kafka`, `KRaftController`, `SchemaRegistry`). Missing the override in
  even one values file silently reintroduces hard anti-affinity and
  unschedulable pods on a small test cluster. Worth grepping all values
  files for the setting after any change to this logic, rather than
  assuming it propagated everywhere.
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
  to use the cluster's default StorageClass rather than hardcoding one
  that may not exist on every cluster you test against.
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