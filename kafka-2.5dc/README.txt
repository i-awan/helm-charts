# Kafka 2.5 DC Multi-Region Cluster on OpenShift

A single **stretched KRaft Kafka cluster** (one cluster ID, one controller quorum) spread across three OpenShift namespaces/regions, deployed with **Confluent for Kubernetes (CFK)**.

It is a **"2.5 DC"** topology: two full data regions plus one controller-only region acting as a quorum tiebreaker — the "0.5".

![2.5 DC architecture](docs/architecture.png)

---

## Table of contents

- [Architecture](#architecture)
  - [The design in one page](#the-design-in-one-page)
  - [Key design decisions](#key-design-decisions)
  - [Node ID scheme](#node-id-scheme)
- [Durability model](#durability-model)
  - [RF, ISR, minISR](#rf-isr-minisr)
  - [Leader loss vs replica loss](#leader-loss-vs-replica-loss)
  - [Region-failure survival](#region-failure-survival)
  - [Uneven region loss](#uneven-region-loss)
  - [Why minISR=1 is wrong](#why-minisr1-is-the-wrong-fix)
  - [Alternative: RF=5 for two regions](#alternative-rf5-for-two-regions)
- [Configuration](#configuration)
  - [Controller quorum (static voters)](#controller-quorum-static-voters)
  - [Replication-factor defaults](#replication-factor-defaults)
  - [Cross-region replica placement](#cross-region-replica-placement)
  - [Endpoints (always FQDN)](#endpoints-always-fqdn)
- [Cross-region networking](#cross-region-networking)
- [Deployment](#deployment)
- [Operations](#operations)
  - [Reassignment Job](#reassignment-job)
  - [Troubleshooting runbook](#troubleshooting-runbook)
- [Recommendations](#recommendations)

---

## Architecture

### The design in one page

| Plane | Layout |
|---|---|
| **Metadata** | 3 controllers per region × 3 regions = a **9-voter KRaft quorum**. Majority is 5, so metadata survives losing any single region. |
| **Data** | Brokers in region **A and B only** (region C has none). RF=3 places replicas **2+1** across the two data regions. |
| **Applications** | Control Center, Schema Registry, Connect, REST Proxy run in **region A only**; topic CRs are managed from region A as cluster-wide objects. |
| **Networking** | Cross-region traffic must be **explicitly allowed** per namespace; broker, replication, and controller ports carry data and metadata between regions. |

### Key design decisions

| Decision | Why | Trade-off |
|---|---|---|
| 3-region controller quorum | Metadata survives a full region loss | Metadata commits need a cross-region majority — latency-sensitive |
| Region C controller-only (the 0.5) | 3rd failure domain for the quorum without paying for a 3rd data copy | Data plane still only spans 2 regions |
| RF=3, minISR=2 | Standard durability floor; survives a broker failure | On 2 data regions, RF=3 is 2+1 — a region loss makes some partitions read-only |
| Apps in region A only | Single place to manage C3 / SR / topics | Region A is the app SPOF; those apps must reach brokers in every region |

### Node ID scheme

Each region gets a disjoint ID block via per-region offsets; controllers and brokers occupy non-overlapping ranges. **Every node ID must be globally unique across all regions** — collisions cause quorum and registration failures.

| Region | Controllers (voters) | Brokers (observers) |
|---|---|---|
| A — `kafka-region-a` | 100, 101, 102 | 110, 111, 112 |
| B — `kafka-region-b` | 200, 201, 202 | 210, 211, 212 |
| C — `kafka-region-05dc` | 300, 301, 302 | _none (controller-only)_ |

> IDs are fixed on disk in `meta.properties` at format time. Changing an offset does **not** renumber a node that already has state — that requires a fresh volume.

---

## Durability model

### RF, ISR, minISR

- **Replication factor (RF)** — a count of distinct brokers holding each partition. RF can never exceed the brokers available in the placement scope.
- **ISR** — the set of replicas currently caught up to the leader.
- **`min.insync.replicas` (minISR)** — the durability floor. With `acks=all`, a write is only acknowledged when at least minISR replicas are in sync; below it, writes are rejected with `NOT_ENOUGH_REPLICAS`.

> **minISR does not create replicas — RF does.** minISR only gates writes. Setting `minISR=2` on a topic with 1 replica doesn't add a second; it guarantees that topic can never accept writes.

The standard pairing is **RF=3 with minISR=2**: lose one replica and keep writing, lose a second and still not lose data.

### Leader loss vs replica loss

A partition has one leader, but the leader is **not** a single point of failure — if it dies, a caught-up follower is promoted automatically. What stops writes is the **ISR count falling below minISR**, which depends on how many replicas survive, not on where the leader was. If a region holds a **majority** of a partition's replicas (2 of 3) and goes down, that partition goes read-only regardless of leader location.

### Region-failure survival

Three replicas can't split evenly across two regions — the split is **2 + 1**. Lose the region holding 2 and only 1 survives (below minISR → read-only). With three regions the split is **1 + 1 + 1**, so any single region can be lost while two in-sync replicas remain.

![Region-failure behaviour: 2 vs 3 regions](docs/region-failure.png)

| Layout | Replicas / region | Left after region loss | Writes survive? |
|---|---|---|---|
| **3 brokers × 2 regions (this cluster)** | 2 + 1 | 1 | ❌ No |
| 1 broker × 3 regions | 1 + 1 + 1 | 2 | ✅ Yes |
| 3 brokers × 3 regions (rack-aware) | 1 + 1 + 1 | 2 | ✅ Yes |

> Survival is a function of **failure domains (regions)**, not broker count. This cluster's 6 brokers over 2 regions tolerate a broker failure but **not** a full data-region loss.

### Uneven region loss

Impact is per-partition: which region holds a partition's 2-replica majority varies, so a region loss leaves **roughly half the partitions writable and half read-only** — a patchy outage. A single topic can be simultaneously up and down; with keyed partitioning, some keys' writes fail deterministically. Three regions removes this: every partition keeps 2 in-sync replicas, so the failure mode is uniform.

### Why minISR=1 is the wrong fix

`min.insync.replicas=1` stops write-blocking but trades away durability **permanently, during stable operation** — not just during an outage. A write can be acked on the leader alone; if it then crashes before a follower copies it, the acknowledged write is silently lost. It also makes routine rolling restarts risky (no floor) and masks single-copy degradation.

Reserve minISR=1 for loss-tolerant data (replayable logs, metrics), set **per-topic, never as the cluster default** — the default would weaken internal topics like `_schemas` and `__consumer_offsets` too.

### Alternative: RF=5 for two regions

If a third **data** region isn't available, **RF=5 across two regions** is the one arrangement that survives a region loss within two regions. Five replicas split 3+2, so the worst case (losing the 3-replica region) still leaves 2 — exactly minISR.

- Requires **rack-aware 3+2 placement** (else it may land 4+1 or 5+0).
- Needs ≥5 brokers (aim for 6, as 3+3).
- Caps you at minISR=2; costs 5× storage.

| Aspect | RF=5 across 2 regions | RF=3 across 3 regions |
|---|---|---|
| Data regions needed | 2 (existing) | 3 (new broker region) |
| Copies / storage | 5× (higher) | 3× (lower) |
| Acks in degraded case | minority (2 of 5) | majority (2 of 3) |
| Survives a region loss | Yes (3+2 rack placement) | Yes (1+1+1) |

---

## Configuration

### Controller quorum (static voters)

With 3 controllers per region, the static voter list has **9 entries** — all controllers, on the controller listener (9074), identical in every region. Brokers are never listed.

```
controller.quorum.voters=100@<a-ctrl-0>:9074,101@<a-ctrl-1>:9074, ... ,302@<c-ctrl-2>:9074
```

Each endpoint is the controller pod's stable StatefulSet FQDN:

```
kraftcontroller-<region>-<ordinal>.kraftcontroller-<region>.<namespace>.svc.cluster.local:9074
```

Verify:

```bash
oc exec <broker-pod> -n kafka-region-a -- \
  kafka-metadata-quorum --bootstrap-server localhost:9071 describe --status
# CurrentVoters must list all 9 IDs with one leader; observers list the brokers
```

### Replication-factor defaults

RF for internal topics is set **per-component at creation time** — they do not inherit `default.replication.factor`, and existing topics keep their creation-time RF. Set these **before first boot**.

**Kafka CR (`configOverrides.server`):**

```yaml
configOverrides:
  server:
    - default.replication.factor=3
    - offsets.topic.replication.factor=3
    - transaction.state.log.replication.factor=3
    - confluent.license.topic.replication.factor=3
    - min.insync.replicas=2
```

**SchemaRegistry CR:**

```yaml
configOverrides:
  server:
    - kafkastore.topic.replication.factor=3
```

**ControlCenter CR** (note: C3 uses `.replication`, **not** `.replication.factor`):

```yaml
configOverrides:
  server:
    - confluent.controlcenter.internal.topics.replication=3
    - confluent.controlcenter.command.topic.replication=3
    - confluent.monitoring.interceptor.topic.replication=3
    - confluent.metrics.topic.replication=3
```

### Cross-region replica placement

RF sets how many replicas exist; it does **not** control *where* they land. Without region awareness, Kafka assigns replicas by broker-ID order. Because region-A IDs (110–112) sort before region-B (210–212), RF=3 lands **2 replicas in A + 1 in B on every partition** — uniformly. This is dangerous:

- Lose region A → *every* partition drops to 1 replica → **the whole cluster goes read-only** (not "half").
- Leadership also concentrates in region A.

**Preferred fix — `broker.rack` via config** (node-free; makes placement region-aware and automatic at creation time):

```yaml
# region-a Kafka CR
configOverrides:
  server:
    - broker.rack=region-a
# region-b Kafka CR
configOverrides:
  server:
    - broker.rack=region-b
```

> ⚠️ **Version-dependent** — some CFK versions manage `broker.rack` themselves and may override a manual value. Verify it took:
> ```bash
> oc exec <broker> -n <ns> -- grep broker.rack /opt/confluentinc/etc/kafka/kafka.properties
> ```

The label-based `rackAssignment.nodeLabels` mechanism reads a **node label** and needs node RBAC + labelled nodes — **not available** in this environment.

| | `broker.rack` (config) | [Reassignment Job](#reassignment-job) |
|---|---|---|
| Fixes existing topics | ❌ No | ✅ Yes |
| Fixes future topics | ✅ **Yes, automatically** | ❌ No (re-run per topic) |
| Needs node access | No | No |
| Availability | Version-dependent (verify) | Works everywhere |
| Effort | One-time config + rolling restart | Recurring |

**Recommended combination:** use `broker.rack` (if honoured) so future topics balance automatically, and run the reassignment Job **once** to fix already-clumped existing topics, then retire it.

Caveats: rack awareness does not retroactively move existing topics; `broker.rack` is immutable once set (needs a rolling restart); it still can't beat the two-region math (RF=3 over 2 racks is 2+1). After reassigning, run a preferred-leader election so leadership balances:

```bash
kafka-leader-election --bootstrap-server localhost:9071 \
  --election-type preferred --all-topic-partitions
```

### Endpoints (always FQDN)

Every component's `bootstrapEndpoint` and C3's `connectUrl` must use the fully-qualified form, never a bare `kafka:9071`:

```
kafka.<namespace>.svc.cluster.local:9071
```

A bare name resolves in the **caller's** namespace, so it silently points at the wrong cluster (or fails) from another region. This cluster is **plaintext** — endpoints are `http://` / port 9071, TLS off.

---

## Cross-region networking

**The most important operational lesson.** On a stretched cluster, brokers, controllers, and components must reach each other **across namespaces**. NetworkPolicy is **default-deny per namespace**, so each destination namespace needs its own ingress rule. A policy that allows only the external client port (9092) lets the cluster form at the metadata level but **blocks data replication between regions** — surfacing as misleading license, placement, and registration errors.

**Metadata plane vs. data plane** — membership and data access travel on different ports and fail independently. A controller quorum showing another region's brokers as members (`describe --status`) proves membership, **not** that a broker there can read topic data. That fetch is a separate connection to the partition leader on the broker/replication listener.

### Ports to allow cross-namespace

| Port | Purpose | Cross-namespace |
|---|---|---|
| **9071** | Internal broker listener — inter-broker replication & data fetch | ✅ Required (both directions) |
| **9072** | Replication listener | ✅ Required (both directions) |
| **9074** | Controller listener (KRaft quorum) | ✅ Required (all regions) |
| 9092 | External client listener | ⚠️ Insufficient alone |

Also open **component → broker** paths: Control Center and Schema Registry live in region A but manage/read the whole cluster, so they must reach brokers in **every** region. Because policy is per destination namespace, allowing them into region B requires a rule in **region B's** namespace — scope it to the whole region-A namespace (not just the C3 pod) so Schema Registry is covered when a `_schemas` leader lands in region B.

**Verify** (works even if brokers crash-loop — run from a throwaway pod):

```bash
oc run nettest -n kafka-region-b --image=registry.access.redhat.com/ubi9/ubi-minimal \
  --restart=Never --rm -it -- \
  sh -c 'for p in 9071 9072 9074; do timeout 3 bash -c "</dev/tcp/kafka.kafka-region-a.svc.cluster.local/$p" && echo "$p OPEN" || echo "$p BLOCKED"; done'
```

> Keep these NetworkPolicies in the Helm chart / IaC — a namespace rebuild otherwise reintroduces the client-port-only rule. NetworkPolicies are a security control; widening them is the platform team's call.

Example (region-a namespace, allowing inter-broker + controller traffic from B and C):

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-cross-region-kafka
  namespace: kafka-region-a
spec:
  podSelector: {}
  ingress:
    - from:
        - namespaceSelector: { matchLabels: { kubernetes.io/metadata.name: kafka-region-b } }
        - namespaceSelector: { matchLabels: { kubernetes.io/metadata.name: kafka-region-05dc } }
      ports:
        - { protocol: TCP, port: 9071 }
        - { protocol: TCP, port: 9072 }
        - { protocol: TCP, port: 9074 }
```

Apply reciprocal policies in **every** region namespace.

---

## Deployment

Region A is installed first to mint the cluster ID, which is then supplied to the other regions.

```bash
# 1. RBAC / NetworkPolicies per namespace (apply first)
oc apply -f networkpolicy-region-a.yaml
oc apply -f networkpolicy-region-b.yaml
oc apply -f networkpolicy-region-05dc.yaml

# 2. Region A (mints cluster ID)
helm upgrade --install <release> -n kafka-region-a \
  -f values-common.yaml -f values-region-a.yaml

# 3. Regions B and C with the shared cluster ID
helm upgrade --install <release> -n kafka-region-b \
  -f values-common.yaml -f values-region-b.yaml
helm upgrade --install <release> -n kafka-region-05dc \
  -f values-common.yaml -f values-region-05dc.yaml
```

> Because region A is up first, internal topics are initially created with all replicas in region A. See [Cross-region replica placement](#cross-region-replica-placement) and the [Reassignment Job](#reassignment-job) to balance them 2+1.

**Post-deploy verification:**

```bash
# all 6 brokers registered, quorum healthy (9 voters)
oc exec <broker-pod> -n kafka-region-a -- \
  kafka-metadata-quorum --bootstrap-server localhost:9071 describe --status

# broker.rack set per region (if using the config path)
oc exec <broker-pod> -n kafka-region-a -- grep broker.rack /opt/confluentinc/etc/kafka/kafka.properties
oc exec <broker-pod> -n kafka-region-b -- grep broker.rack /opt/confluentinc/etc/kafka/kafka.properties

# internal topics spread across both regions
for t in _confluent-command _schemas __consumer_offsets; do
  oc exec <broker-pod> -n kafka-region-a -- \
    kafka-topics --bootstrap-server localhost:9071 --describe --topic $t
done
```

---

## Operations

### Reassignment Job

A Helm **post-upgrade** hook that re-places internal topics **2+1 across the two data regions**, generating the assignment per-partition (not hardcoded JSON) and alternating the majority region for balance. See [`reassign-internal-topics-job.yaml`](reassign-internal-topics-job.yaml).

Values it reads:

```yaml
reassign:
  regionABrokers: [110, 111, 112]
  regionBBrokers: [210, 211, 212]
  throttleBytesPerSec: "50000000"   # 50 MB/s inter-broker throttle
  topics:
    - _confluent-command
    - _schemas
    - __consumer_offsets
    - __transaction_state
    - _confluent-monitoring
    - _confluent-metrics
    - connect-configs
    - connect-offsets
    - connect-status
```

**Reassign vs. recreate** — some internal topics hold irreplaceable state and must be reassigned, never deleted:

| Topic | Holds real state? | Action |
|---|---|---|
| `_schemas` | Yes (schemas) | **Reassign** — never delete |
| `__consumer_offsets` | Yes (offsets) | **Reassign** — never delete |
| `__transaction_state` | Yes (txn state) | Reassign if EOS/txns used |
| `connect-offsets` | Yes (connector offsets) | **Reassign** |
| `connect-configs` / `connect-status` | Configs/status | Reassign (or recreate if empty) |
| `_confluent-command` | Regenerable | Either |
| `_confluent-monitoring` / `-metrics` | Regenerable | Recreate or reassign |

> - **Post-upgrade only** — topics must already exist; never runs on a fresh install.
> - **Throttled** (default 50 MB/s) because moving `__consumer_offsets` / `__transaction_state` (50 partitions each) shifts real data. Re-run `--verify` later to clear the throttle.
> - It is an **imperative repair**. The permanent fix is [`broker.rack`](#cross-region-replica-placement) + RF defaults so topics are born correct.

### Troubleshooting runbook

> Recurring meta-lesson: **suspect stale on-disk node state or the cross-region network path before the Kafka config.**

| Symptom | Root cause | Fix |
|---|---|---|
| `INCONSISTENT_CLUSTER_ID` (104) on fetch/vote | Node booted on a stale volume with a different cluster ID, or cluster ID not shared across regions | Ensure the same cluster ID across regions; clear the odd node's PVC so it reformats |
| Node ID appears as both voter and observer | Identical ID offsets across regions, or stale `meta.properties` | Disjoint per-region offsets; clear stale volumes so nodes reformat under new IDs |
| Controller: *"node 201 must be in the set of voters"* | Scaled controllers to 3/region but static voter list still had 1/region | List all 9 controllers in `controller.quorum.voters`, identical in every region |
| Control Center: *"failed to get bootstrap cluster id"* | Bare `bootstrapEndpoint` (`kafka:9071`) resolved wrong from its namespace | Use FQDN `kafka.<ns>.svc.cluster.local:9071` |
| Schema Registry: *"failed to write Noop record"* | `_schemas` ISR < minISR (too few brokers / clumped replicas) | Ensure enough brokers; RF=3 with a replica in each region |
| Broker: *"valid license must be configured"* (region B) | Brokers can't read `_confluent-command` — replicas all in A **and** 9071 blocked cross-region | Open 9071/9072 cross-region; reassign the topic to include a region-B broker |
| Reassign: *"unknown broker 210"* | Target broker crash-looping → not a registered/eligible target | Fix the crash first (networking) so the broker stays up, then reassign |
| Internal topic has all replicas in region A | Topic created while only region-A brokers existed (A installed first) | Reassign 2+1 once all brokers healthy; set RF defaults before creation |
| Every topic uniformly 2-in-A / 1-in-B | No region awareness — replicas placed by broker-ID order; losing A makes the whole cluster read-only | Set `broker.rack` per region; reassign existing topics to alternating 2+1 + preferred-leader election |
| Init container `ErrImagePull`: certificate expired/not yet valid | Node clock skew (TLS cert validation fails) | Fix node time sync (chrony/NTP) — a node/infra action, not a Kafka change |

---

## Recommendations

- Keep **RF=3 / minISR=2** as the durability standard; set the internal-topic RF defaults **before first boot**.
- Bake the **cross-region NetworkPolicies into IaC** (9071/9072/9074 both directions, all region pairs, plus component→broker) so a rebuild can't reintroduce the client-port-only rule.
- Always use **FQDN endpoints**; never bare service names in a multi-namespace cluster.
- Make placement region-aware: prefer **`broker.rack` via `configOverrides`** (node-free; verify your CFK version honours it). Fall back to the reassignment Job for existing topics and if the config path is overridden.
- Know the ceiling: with two data regions, RF=3 is 2+1 and a full region loss makes some partitions read-only. For region-loss **write availability**, add a third **data** region (RF=3, 1+1+1) or use **RF=5** rack-aware 3+2.
- Manage **topic CRs from one region only** — topics are cluster-wide; duplicate CRs across namespaces fight over the same object.

---

_Node IDs, endpoints, and policies here are examples — confirm against your CFK version and actual topology before applying._
