# kafka-test-client

Helm chart that runs a producer and/or consumer **inside the same Kubernetes/OpenShift cluster as Kafka**.
The producer generates random records that **conform to a schema you pass in** (Avro or JSON Schema).
The consumer reads them back and validates each one.

Status: written and checked with a stand-in template renderer (valid YAML for TLS, SCRAM, OAuth, no-SR variants and the
failure guards) and the Python client tested against a fake Schema Registry. **Not yet run with real `helm` or a real broker.**
Expect to fix small things on first install.

## 1. Build the image once

```bash
docker build -t <registry>/kafka-test-client:0.1.0 image/
docker push <registry>/kafka-test-client:0.1.0
```

The script (`files/client.py`) is not baked into the image; the chart mounts it from a ConfigMap, so edits only need `helm upgrade`.

## 2. Install

Plain in-cluster listener, Avro schema, Schema Registry:

```bash
helm install kt ./kafka-test-client -n <ns> \
  --set image.repository=<registry>/kafka-test-client --set image.tag=0.1.0 \
  --set kafka.bootstrapServers=kafka.<ns>.svc.cluster.local:9071 \
  --set schemaRegistry.url=http://schemaregistry.<ns>.svc.cluster.local:8081 \
  --set topic.name=orders \
  --set-file schema.inline=examples/order.avsc
```

JSON Schema instead: add `--set schema.format=jsonschema --set-file schema.inline=examples/order.schema.json`.

TLS with the CFK/cert-manager secret (keys `ca.crt`, `tls.crt`, `tls.key`):

```bash
  --set kafka.securityProtocol=SSL \
  --set kafka.tls.existingSecret=kafka-tls-combined \
  --set kafka.tls.mutual=false
```

SASL: `kafka.securityProtocol=SASL_SSL`, `kafka.sasl.mechanism=PLAIN|SCRAM-SHA-256|SCRAM-SHA-512|OAUTHBEARER`,
`kafka.sasl.existingSecret=<secret with username/password>` (for OAUTHBEARER: client id/secret keys plus
`kafka.sasl.oauth.tokenEndpointUrl`). If the Keycloak/IdP CA is private, put it in the same TLS secret.

No Schema Registry: `--set schemaRegistry.enabled=false`. Records are then raw Avro binary or plain JSON.

A schema already in a ConfigMap: `--set schema.existingConfigMap=<name> --set schema.existingConfigMapKey=<key>`.

## 3. Use

```bash
helm test kt -n <ns> --logs       # preflight, produce N, read exactly those N back, validate each
kubectl logs -n <ns> -l app.kubernetes.io/instance=kt,app.kubernetes.io/component=producer
kubectl logs -n <ns> -l app.kubernetes.io/instance=kt,app.kubernetes.io/component=consumer
```

Useful values: `producer.kind` (Job|Deployment), `producer.messageCount`, `producer.ratePerSecond`, `producer.key`
(`none|uuid|sequence|field:<f>|fixed:<v>`), `producer.seed` (reproducible data), `consumer.groupId`,
`consumer.maxMessages`, `consumer.idleTimeoutSeconds`, `consumer.failOnInvalid`, `topic.create`, `logSamples`.
A Job producer needs `messageCount > 0`; a Job consumer needs `maxMessages` or `idleTimeoutSeconds`.
Each `helm upgrade` creates a new Job (`-r<revision>`) because Job pod templates are immutable.

Preview the generated data with no Kafka:

```bash
pip install -r image/requirements.txt
SCHEMA_PATH=examples/order.avsc SCHEMA_FORMAT=avro python files/client.py sample
```

## 4. Limits of the generator

- Avro: all types, named-type references, unions, maps, arrays, enums, logical types (decimal, uuid, date, time, timestamp).
- JSON Schema: type, enum, const, properties/required, items, min/max, pattern (via `exrex`), common formats, allOf/anyOf/oneOf,
  local `$ref`. Remote `$ref`s are not supported; `not` and `if/then/else` are ignored when generating (records are still validated, so they may be retried or rejected). Every record is validated and regenerated up to 30
  times; a schema that cannot be satisfied fails with a clear error.
- Records are random but schema-valid; they are not business-realistic.
- With Schema Registry, `schemaRegistry.autoRegister=true` registers the schema; set it false (and `useLatestVersion=true`) if the
  principal may not register. RBAC needs `Write` on the topic and subject `<topic>-value` (and `Read` + group for the consumer).
