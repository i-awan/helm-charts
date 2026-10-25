
## How TLS is configured per component (CFK)

Every component both **accepts** connections (server) and **opens** connections
(client). CFK configures the two roles in different places, even when the same
certificate is used for both.

| | Inbound (component as server) | Outbound (component as client) |
|---|---|---|
| Question answered | "Who am I?" | "Do I trust who I'm calling?" |
| Needs | Cert, key, SANs | CA for the server; client cert if the server demands mTLS |
| CFK field | `spec.tls`, `spec.listeners.<name>.tls` | `spec.dependencies.<service>.tls` and `.authentication` |
| Fails as | Callers can't verify this service (hostname/SAN errors) | `PKIX path building failed` (missing CA), or `bad_certificate`/handshake rejected (missing client cert) |

Examples:

- **Kafka broker:** inbound is the certificate it presents to clients and peers. With
  mTLS it also demands a certificate back (`ssl.client.auth=required`).
- **Schema Registry:** inbound is its HTTPS API certificate. Outbound is
  `dependencies.kafka`, which holds the CA it trusts and, with mTLS, its client cert.
- **Control Center:** inbound (`spec.tls`) is the certificate browsers see. Outbound
  is `dependencies.kafka` and `dependencies.schemaRegistry`. For Kafka with mTLS,
  `authentication.type: mtls` makes C3 present its `tls.crt`/`tls.key` as a client
  certificate, and the broker maps its CN to a principal.

A component can work inbound while failing outbound, and the reverse. For example, the
C3 UI can load in a browser while C3 cannot reach Kafka because it presents no client
certificate.

### `spec.tls` versus `spec.listeners.<name>.tls`

- **`spec.tls`** is the component-wide default: the certificate used unless a
  listener overrides it. CFK also treats it as "TLS is configured" (the
  `missing .spec.tls. in spec` InputError means this block is absent).
- **`spec.listeners.<name>.tls`** is per listener. A component can have several (Kafka
  and Schema Registry have `internal` and `external`), each able to switch TLS on or
  off and use its own `secretRef`. The per-listener setting wins for that listener; a
  listener with no `secretRef` falls back to `spec.tls`.

Not every CRD has `spec.listeners`. ControlCenter does not (checked with
`oc explain controlcenter.spec.listeners`), so its inbound TLS is set only through
`spec.tls`. Unsupported fields are silently dropped by CFK and show up only as a
`unknown field "spec.listeners"` warning at install.

With one certificate per component `spec.tls` is enough. Per-listener blocks matter
when listeners need different certificates, or TLS on for only one of them.

### Shared versus per-service certificates

By default Kafka, KRaft, Schema Registry and Control Center share one certificate
(`tls.secretRef`). With `tls.certManager.serviceCerts.enabled: true`, Schema
Registry and C3 get their own (CN `<region>-schemaregistry` /
`<region>-controlcenter`), signed by the same issuer so truststores don't change.
Separate certificates give each service its own principal for ACLs.
