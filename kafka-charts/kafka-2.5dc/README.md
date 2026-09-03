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
the cluster via one shared secret (`kafka-tls`, containing
`keystore.jks` / `truststore.jks` / `jksPassword.txt` — CFK's expected
format): producer/consumer → broker, broker → broker (replication),
broker → controller, and controller → controller (the Raft quorum
traffic itself).

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
