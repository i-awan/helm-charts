# Confluent Kafka 2.5DC / 2.5 MRC — OIDC and Keycloak Architecture

## 1. Architecture overview

```text
                        ┌─────────────────────────────┐
                        │          End Users          │
                        │    Browser / AD Identity    │
                        └──────────────┬──────────────┘
                                       │
                                       │ 1. Access C3
                                       ▼
             ┌─────────────────────────────────────────────┐
             │             REGION A — PRIMARY             │
             │                                             │
             │   ┌─────────────────────────────────────┐   │
             │   │     Confluent Control Center (C3)   │   │
             │   │       OIDC / SSO Authentication     │   │
             │   └──────────────────┬──────────────────┘   │
             │                      │                      │
             │                      ▼                      │
             │   ┌─────────────────────────────────────┐   │
             │   │         Kafka MDS / RBAC            │   │
             │   │  mds-token                          │   │
             │   │  mds-oidc-secret                    │   │
             │   └──────────────────┬──────────────────┘   │
             │                      ▼                      │
             │   ┌─────────────────────────────────────┐   │
             │   │       Kafka Cluster — Region A     │   │
             │   └─────────────────────────────────────┘   │
             └──────────────────────┬──────────────────────┘
                                    │
                                    │ Multi-region replication / failover
                                    │
             ┌──────────────────────▼──────────────────────┐
             │             REGION B — SECONDARY            │
             │   ┌──────────────────────────────────────┐   │
             │   │         Kafka MDS / RBAC             │   │
             │   │  Same mds-token material              │   │
             │   │  Same mds-oidc-secret material        │   │
             │   └──────────────────┬───────────────────┘   │
             │                      ▼                       │
             │   ┌──────────────────────────────────────┐   │
             │   │       Kafka Cluster — Region B      │   │
             │   └──────────────────────────────────────┘   │
             │   OIDC configuration available for standby  │
             └──────────────────────────────────────────────┘


                    ┌───────────────────────────────┐
                    │       KEYCLOAK SERVER         │
                    │   Shared Realm               │
                    │   One OIDC Client            │
                    │   Client ID / Credentials    │
                    │   User Authentication        │
                    └───────────────┬───────────────┘
                                    ▲
                                    │ OIDC authentication
                                    │ and token exchange
                              C3 / MDS OIDC integration
```

## 2. Key design decisions

| Component | Design |
|---|---|
| **Control Center (C3)** | One C3 instance, deployed in Region A |
| **Keycloak** | One shared Keycloak server and realm |
| **Keycloak OIDC client** | One client used by the setup |
| **Region A** | Primary region, with C3 and Kafka/MDS |
| **Region B** | Secondary region, with its own Kafka/MDS and OIDC configuration |
| **`mds-token`** | Matching token-signing material shared across both regions |
| **`mds-oidc-secret`** | Matching OIDC client-secret material in both regions |
| **Authentication** | Keycloak OIDC / SSO |
| **Authorization** | Confluent MDS and RBAC |
| **Failover** | Region B is configured to support the secondary role if Region A fails |

## 3. OIDC authentication flow

```text
User
 │
 │ 1. Opens Control Center
 ▼
C3 — Region A
 │
 │ 2. Redirects browser to Keycloak
 ▼
Keycloak
 │
 │ 3. Authenticates user
 │
 │ 4. Returns authorization response
 ▼
C3 — Region A
 │
 │ 5. Establishes authenticated session
 │
 │ 6. Uses MDS integration for authorization
 ▼
Kafka MDS / RBAC
 │
 │ 7. Evaluates user permissions
 ▼
Kafka Cluster
```

**Important distinction:** Keycloak authenticates the user; Confluent MDS/RBAC determines what the authenticated user can access in Kafka and Control Center. Successful SSO alone does not grant permission to view brokers, cluster information, or topics.

## 4. Failover considerations

```text
NORMAL OPERATION
──────────────────────────────────────────────
Region A: C3 + Kafka + MDS  ──► Keycloak
Region B: Kafka + MDS       ──► Keycloak


REGION A FAILURE
──────────────────────────────────────────────
Region A: Unavailable

Region B: Kafka + MDS remains available
         Matching MDS token and OIDC secret
         OIDC configuration already present

         C3 availability depends on how C3 itself
         is recovered or redeployed.
```

The critical point is that **configuring OIDC in Region B does not, by itself, provide a running Control Center there**. Since this design has only one C3 instance, C3 recovery or redeployment must be addressed separately if Region A is lost.

The Kubernetes `KeycloakClient` custom resource and the client stored in Keycloak are distinct things. The Keycloak-side client is what the identity provider uses; the CR manages that client declaratively. For failover resilience, ensure the authoritative client configuration is not dependent solely on a resource that disappears with Region A.

## 5. Summary

**One Keycloak → one OIDC client → one C3 in Region A → two Kafka/MDS regions, with matching MDS token and OIDC secret material.**

This is the architecture overview for the 2.5DC / 2.5 MRC deployment.
