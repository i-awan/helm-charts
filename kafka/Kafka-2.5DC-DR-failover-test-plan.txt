# Kafka 2.5DC - DR failover test plan

## Goal and topology

This test shows whether the cluster keeps serving when region-a is lost, loses no data, and recovers cleanly when region-a returns. Region-a is "failed" by scaling its pods to 0, not by deleting a namespace.

| Region | Namespace | Pods | Node IDs |
| --- | --- | --- | --- |
| region-a | kafka-region-a | kafka-0, kraftcontroller-region-a-0, schemaregistry-0, controlcenter-0 | broker 110, controller 100 |
| region-b | kafka-region-b | kafka-0, kraftcontroller-region-b-0, schemaregistry-0 | broker 210, controller 200 |
| region-05dc | kafka-region-05dc | kraftcontroller-region-05dc-0 | controller 300 |

With region-a down, controllers 200 and 300 remain: 2 of 3 voters, so the quorum survives. Every partition has replication factor 2, one replica per region, so broker 210 holds a full copy and becomes the leader.

**Key caveat:** the cluster currently runs with `min.insync.replicas=2`. With one broker left, producers using `acks=all` are rejected with `NOT_ENOUGH_REPLICAS`. Consumer offset commits and Schema Registry writes are affected the same way, because their internal topics inherit the setting. The plan therefore first records that behavior, then lowers `min.insync.replicas` to 1 at runtime so writes can continue, and restores it at the end. Reads work in both cases.

Assumed names: StatefulSets `kafka`, `kraftcontroller-region-a`, `schemaregistry`, `controlcenter`. Confirm with `oc get sts -n kafka-region-a`.

## Prerequisites and test preparation

1. All pods in all three namespaces are `Running` and `Ready`. Fix node clock skew first, or the restarted pods may fail image pulls or certificate checks.
2. Set variables once per shell:

```bash
NS_A=kafka-region-a
NS_B=kafka-region-b
NS_T=kafka-region-05dc
BOOT=kafka.kafka-region-b.svc.cluster.local:9071
SR=https://schemaregistry.kafka-region-b.svc.cluster.local:8081
```

**3. Copy the TLS certs into the pods.** The Kafka CLI tools (`kafka-topics`, `kafka-configs`, `kafka-metadata-quorum`, the plain console producer and consumer) are in the **broker pod** (`kafka-0`), not the Schema Registry pod. The Avro console tools are only in the **Schema Registry pod**. So both pods in region-b need the cert files. Region-b is used for all test traffic because region-a will be down. The client identity is the Schema Registry cert from the secret `kafka-schemaregistry-tls`; its CN (`region-b-schemaregistry`) is a super user. Run this on your workstation:

```bash
mkdir -p /tmp/drcerts && cd /tmp/drcerts
for k in ca.crt tls.crt tls.key; do
  oc get secret kafka-schemaregistry-tls -n $NS_B -o jsonpath="{.data.${k//./\\.}}" | base64 -d > $k
done

# Kafka's PEM keystore needs a PKCS#8 key; convert it, then join cert and key in one file
openssl pkcs8 -topk8 -nocrypt -in tls.key -out tls-pkcs8.key
cat tls.crt tls-pkcs8.key > keystore.pem

# Check the subject (should be CN=region-b-schemaregistry) and expiry
openssl x509 -in tls.crt -noout -subject -enddate

# Client config used by every Kafka command in this plan
cat > client.properties <<EOF
security.protocol=SSL
ssl.truststore.type=PEM
ssl.truststore.location=/tmp/ca.crt
ssl.keystore.type=PEM
ssl.keystore.location=/tmp/keystore.pem
EOF
```

Copy the three files into `/tmp` of both pods. This streams them through `oc exec`, so it works even when the image has no `tar` for `oc cp`:

```bash
for p in kafka-0 schemaregistry-0; do
  for f in ca.crt keystore.pem client.properties; do
    oc exec -i $p -n $NS_B -- sh -c "cat > /tmp/$f" < $f
  done
  oc exec $p -n $NS_B -- ls -l /tmp/ca.crt /tmp/keystore.pem /tmp/client.properties
done
```

Open the broker shell. Every command in this plan runs here unless it says otherwise:

```bash
oc exec -it kafka-0 -n $NS_B -- bash
export LOG_DIR=/tmp
```

The `/tmp` path is writable by the pod user, unlike the mounted cert directory. Delete the key material when the test ends (see Cleanup).

4. Create the test topic with RF 2 and 3 partitions:

```bash
export BOOT=kafka.kafka-region-b.svc.cluster.local:9071
kafka-topics --bootstrap-server $BOOT --command-config /tmp/client.properties \
  --create --topic dr-test --partitions 3 --replication-factor 2
kafka-topics --bootstrap-server $BOOT --command-config /tmp/client.properties \
  --describe --topic dr-test
```

Expected: each partition lists two replicas (110 and 210) and `Isr: 110,210` or `210,110`.

## Phase 1: Baseline (both regions healthy)

Produce 10 records tagged `pre-N`, using plain strings so no schema is needed:

```bash
for i in $(seq 1 10); do echo "pre-$i"; done | kafka-console-producer \
  --bootstrap-server $BOOT --producer.config /tmp/client.properties \
  --topic dr-test --producer-property acks=all
```

Consume them all back from the beginning and count:

```bash
kafka-console-consumer --bootstrap-server $BOOT --consumer.config /tmp/client.properties \
  --topic dr-test --from-beginning --timeout-ms 15000 | sort | uniq | wc -l
```

Expected: `10`.

Record the starting state, then continue only if all three controllers are voters:

```bash
export BOOT=kafka.kafka-region-b.svc.cluster.local:9071
kafka-metadata-quorum --bootstrap-server $BOOT --command-config /tmp/client.properties describe --status
kafka-metadata-quorum --bootstrap-server $BOOT --command-config /tmp/client.properties describe --replication
```

Expected: `CurrentVoters` 100, 200, 300 and a leader among them. Note the `LeaderId` and each voter's lag.

## Phase 2: Fail region-a

Run from your workstation, not from inside a pod. Note the time. If Helm or CFK manages these resources, scaling the StatefulSet directly may be reverted by the operator, so scale the CFK custom resources instead:

```bash
date -u
oc scale kafka/kafka -n $NS_A --replicas=0
oc scale kraftcontroller/kraftcontroller-region-a -n $NS_A --replicas=0
oc scale schemaregistry/schemaregistry -n $NS_A --replicas=0
oc scale controlcenter/controlcenter -n $NS_A --replicas=0
```

If `oc scale` is rejected for a custom resource, patch it instead, for example `oc patch kafka kafka -n $NS_A --type merge -p '{"spec":{"replicas":0}}'`. As a last resort scale the StatefulSets and confirm the operator does not undo it.

Watch the pods go away:

```bash
oc get pods -n $NS_A -w
```

Then check from region-b what the cluster sees (inside the region-b broker pod shell):

```bash
oc exec -it kafka-0 -n $NS_B -- bash
export BOOT=kafka.kafka-region-b.svc.cluster.local:9071
kafka-metadata-quorum --bootstrap-server $BOOT --command-config /tmp/client.properties describe --status
kafka-topics --bootstrap-server $BOOT --command-config /tmp/client.properties --describe --topic dr-test
```

Expected: controller leader is 200 or 300 and voter 100 shows as lagging or offline. Every partition leader is now 210 and `Isr` is `210` only. Partitions with `Isr` below `min.insync.replicas` are under-min-ISR, which is the cause of the write failures in the next phase.

## Phase 3: Produce and consume during the outage

All commands run in the region-b broker pod shell (kafka-0), except 3e.

**3a. Reads still work.** The 10 baseline records must be readable from region-b alone:

```bash
export BOOT=kafka.kafka-region-b.svc.cluster.local:9071
for p in 0 1 2; do
  kafka-console-consumer --bootstrap-server $BOOT --consumer.config /tmp/client.properties \
    --topic dr-test --partition $p --offset earliest --timeout-ms 15000 2>/dev/null
done | sort | uniq | wc -l
```

Expected: `10`. A group-less console consumer needs no offset commit, so it works even with min ISR violated.

**3b. Writes are rejected with min.insync.replicas=2 (expected failure).**

```bash
for i in $(seq 1 5); do echo "out-$i"; done | kafka-console-producer \
  --bootstrap-server $BOOT --producer.config /tmp/client.properties \
  --topic dr-test --producer-property acks=all --producer-property delivery.timeout.ms=20000
```

Expected: `NOT_ENOUGH_REPLICAS` errors or a timeout. This is the correct, safe behavior for a 2-broker cluster: it refuses writes it cannot replicate.

**3c. Allow writes on one broker (explicit DR decision).** This trades durability for availability, so do it only on purpose:

```bash
kafka-configs --bootstrap-server $BOOT --command-config /tmp/client.properties \
  --alter --entity-type topics --entity-name dr-test --add-config min.insync.replicas=1
```

If you also want internal topics such as `__consumer_offsets` and the Schema Registry `_schemas` topic to accept writes, apply the same setting to them (`--entity-name __consumer_offsets`, `--entity-name _schemas`). Note that the cluster-wide default is not changed here.

**3d. Produce during the outage and verify.**

```bash
for i in $(seq 1 10); do echo "out-$i"; done | kafka-console-producer \
  --bootstrap-server $BOOT --producer.config /tmp/client.properties \
  --topic dr-test --producer-property acks=all

kafka-get-offsets --bootstrap-server $BOOT --command-config /tmp/client.properties --topic dr-test

dr-test:0:10
dr-test:1:0
dr-test:2:5

for p in 0 1 2; do
  kafka-console-consumer --bootstrap-server $BOOT --consumer.config /tmp/client.properties \
    --topic dr-test --partition $p --offset earliest --timeout-ms 15000 2>/dev/null
done | sort | uniq | wc -l
```

Expected: `20` (10 `pre-` plus 10 `out-`).

```bash
# remove the override so the topic falls back to min ISR 2
kafka-configs --bootstrap-server $BOOT --command-config /tmp/client.properties \
  --alter --entity-type topics --entity-name dr-test --delete-config min.insync.replicas

# 3b: expect NOT_ENOUGH_REPLICAS or a timeout; capture the output
for i in $(seq 1 5); do echo "rejected-$i"; done | kafka-console-producer \
  --bootstrap-server $BOOT --producer.config /tmp/client.properties \
  --topic dr-test --producer-property acks=all --producer-property delivery.timeout.ms=20000 2>&1 | tail -5

# 3c: re-apply the override
kafka-configs --bootstrap-server $BOOT --command-config /tmp/client.properties \
  --alter --entity-type topics --entity-name dr-test --add-config min.insync.replicas=1

# 3d: produce the real outage records with unique names
for i in $(seq 1 10); do echo "outage-$i"; done | kafka-console-producer \
  --bootstrap-server $BOOT --producer.config /tmp/client.properties \
  --topic dr-test --producer-property acks=all

kafka-get-offsets --bootstrap-server $BOOT --command-config /tmp/client.properties --topic dr-test

```
**3e. Avro through Schema Registry, optional.** Region-b's Schema Registry stays up but is a separate instance with its own election. If it was configured to use the shared `_schemas` topic, it can still read schemas; registering a new one needs `_schemas` writable (3c) and its leader election to work. Run this one in the region-b Schema Registry pod shell (`oc exec -it schemaregistry-0 -n $NS_B -- bash`, then `export LOG_DIR=/tmp`). Test a produce with the existing `payment` schema:

```bash
echo '{"id":"p-dr-1", ...}' | kafka-avro-console-producer \
  --bootstrap-server $BOOT --producer.config /tmp/client.properties \
  --topic payment --property schema.registry.url=$SR \
  --property value.schema.file=/tmp/payment.avsc
```

Replace the JSON with a valid `payment` record and pass SR TLS settings with `--property schema.registry.ssl.*` as in the earlier produce test.

## Phase 4: Failback and verification

Bring region-a back, controller first so it rejoins the quorum before the broker starts:

```bash
oc scale kraftcontroller/kraftcontroller -n $NS_A --replicas=1
oc wait --for=condition=Ready pod/kraftcontroller-region-a-0 -n $NS_A --timeout=300s
oc scale kafka/kafka -n $NS_A --replicas=1
oc wait --for=condition=Ready pod/kafka-0 -n $NS_A --timeout=600s
oc scale schemaregistry/schemaregistry -n $NS_A --replicas=1
oc scale controlcenter/controlcenter -n $NS_A --replicas=1
```

If you scaled StatefulSets directly in Phase 2, scale the same StatefulSets back to 1.

Wait for the broker to catch up, then check that both replicas are in sync again:

```bash
kafka-topics --bootstrap-server $BOOT --command-config /tmp/client.properties \
  --describe --topic dr-test
kafka-topics --bootstrap-server $BOOT --command-config /tmp/client.properties \
  --describe --under-replicated-partitions
kafka-metadata-quorum --bootstrap-server $BOOT --command-config /tmp/client.properties describe --replication
```

Expected: `Isr: 110,210` for every partition, no under-replicated partitions, and voter 100 with a small `Lag`.

**Verify no data loss and no divergence.** Read through region-a's own broker to prove it holds the outage records too:

```bash
# Copy the certs into region-a's broker pod first (workstation, from /tmp/drcerts).
# The region-a SR cert works here too, or reuse the region-b one: both are signed by the same CA.
for f in ca.crt keystore.pem client.properties; do
  oc exec -i kafka-0 -n $NS_A -- sh -c "cat > /tmp/$f" < /tmp/drcerts/$f
done

oc exec -it kafka-0 -n $NS_A -- bash
export LOG_DIR=/tmp
kafka-console-consumer \
  --bootstrap-server kafka.kafka-region-a.svc.cluster.local:9071 \
  --consumer.config /tmp/client.properties \
  --topic dr-test --from-beginning --timeout-ms 15000 | sort | uniq | wc -l
```

Expected: `20`, with the same set `pre-1`..`pre-10` and `out-1`..`out-10` as on region-b.

**Restore durability.** Remove the temporary override so the topic returns to the cluster default:

```bash
kafka-configs --bootstrap-server $BOOT --command-config /tmp/client.properties \
  --alter --entity-type topics --entity-name dr-test --delete-config min.insync.replicas
```

Repeat for `__consumer_offsets` and `_schemas` if you changed them. Finally confirm Schema Registry and Control Center are healthy again in region-a (`oc get pods -n $NS_A`, port-forward to 9021).

## Results checklist

Fill in during the test.

| Step | Expected | Actual | Pass |
| --- | --- | --- | --- |
| 1. Baseline read | 10 records, voters 100/200/300 | | |
| 2. Region-a scaled to 0 | Leader 210, controller quorum 200+300 | | |
| 3a. Read during outage | 10 records | | |
| 3b. Write, min ISR 2 | NOT_ENOUGH_REPLICAS or timeout | | |
| 3d. Write, min ISR 1 | 10 more records accepted, total 20 | | |
| 4. Region-a back | Isr 110,210, no under-replicated partitions | | |
| 4. Read via region-a | 20 records, identical set | | |
| Time to recover | Minutes from scale-up to full ISR | | |

## Optional scenarios

Run each from a healthy state, one at a time, and restore before the next.

- **Fail region-b instead.** Same steps with the namespaces swapped. Region-a then holds the only broker and the Schema Registry and Control Center keep running there, so this also tests that the UI side survives.
- **Lose the tiebreaker only.** `oc scale kraftcontroller/kraftcontroller -n $NS_T --replicas=0`. Quorum is 100 and 200, so everything keeps working, including `min.insync.replicas=2` writes. Restore afterwards.
- **Lose a controller plus the tiebreaker.** Scale region-a's controller and region-05dc's controller to 0. Only 1 of 3 voters remains, so there is no quorum: metadata changes stop and brokers cannot elect leaders. Existing leaders may keep serving briefly. Restore both and confirm recovery.
- **Network partition, not scale-down.** Apply a NetworkPolicy that blocks traffic between `kafka-region-a` and the other namespaces, without stopping pods. This tests split-brain behavior, which scaling down cannot show. Delete the policy to heal.
- **Continuous load across the failover.** Run the `kafka-test-client` producer with a rate limit through the whole test, then count gaps with the consumer afterwards.

## Cleanup

```bash
kafka-topics --bootstrap-server $BOOT --command-config /tmp/client.properties --delete --topic dr-test

# Remove the private key material from the pods and your workstation
for ns in $NS_A $NS_B; do
  oc exec kafka-0 -n $ns -- rm -f /tmp/ca.crt /tmp/keystore.pem /tmp/client.properties
done
oc exec schemaregistry-0 -n $NS_B -- rm -f /tmp/ca.crt /tmp/keystore.pem /tmp/client.properties
rm -rf /tmp/drcerts
```

Confirm every pod in all three namespaces is `Running` and `Ready`, and that any `min.insync.replicas` overrides you added are removed (`kafka-configs ... --describe --entity-type topics --entity-name <topic>`).
