# ADR-0001: HeteroCloud and Flow Service Boundary

- Status: Accepted
- Date: 2026-07-31

## Context

HeteroCloud owns public customer identity, organization, project, IAM, quota,
billing, and the resource control plane. Flow is a separately deployable
managed realtime service. HeteroNetwork owns VPN connectivity, public-address
eligibility, and Kubernetes load-balancer routing.

LiveKit provides a mature SFU, room routing, and media transport. It is written
in Go. coturn, PostgreSQL, and Redis are also established infrastructure
components. Reimplementing these data planes in Rust would reduce reliability
and would not strengthen the service ownership boundary.

## Decision

All Flow-authored backend control-plane processes are Rust:

- Axum API
- PostgreSQL store
- matchmaking worker
- WebSocket P2P signaling
- HeteroCloud Ed25519 provider-command verification
- data-plane principal-context verification
- LiveKit participant token issuance
- TURN REST credential issuance
- audit and usage event production

LiveKit is a fixed-version third-party Go SFU data plane and is the explicit
exception to the Rust backend rule. coturn, PostgreSQL, and Redis Sentinel are
third-party infrastructure dependencies. Their credentials remain in
Kubernetes Secrets and are never exposed through the Flow API.

HeteroCloud reconciles managed instances with a short-lived Ed25519/EdDSA JWT.
The internal endpoint verifies the exact provider issuer, audience, action,
generation, key ID, service-instance scope, lifetime, and replay identifier.
Provider command authentication does not use HMAC.

Public Flow operations use a separately named HMAC-signed principal context.
HeteroCloud, not the end user, holds the HMAC key and returns a precomputed
three-header bearer context. Flow binds the signed header timestamp to the
context's `issued_at` and accepts reuse until the signed, bounded `expires_at`;
the context UUID is written to audit records.
Flow does not query or mutate HeteroCloud IAM state and does not contain
customer login UI. It applies organization, project, and service-instance scope
on every durable data-plane operation.

Flow exposes versioned provider resources and does not depend on HeteroCloud
internal database types. HeteroNetwork integration is limited to Kubernetes
`loadBalancerClass` and traffic-mode contracts.

## Consequences

- Flow can be released, scaled, and failed independently.
- Compromise of a LiveKit participant token is bounded to one room and a short
  lifetime; it does not expose the LiveKit API secret.
- HeteroCloud remains the source of truth for authorization. Flow performs
  defense-in-depth permission and managed-instance isolation checks.
- Existing sessions on a failed SFU, TURN pod, signaling socket, or public
  gateway reconnect rather than migrate transparently.
- Provider API versioning and delegated-principal key rotation must be
  coordinated across repositories.
