# HeteroCloud Flow

HeteroCloud Flow is the first managed service provider for HeteroCloud. It
owns realtime session, matchmaking, signaling, and usage state. It does not
own customer accounts, organizations, projects, billing, or IAM policy.

## Components

| Component | Responsibility |
| --- | --- |
| `flow-api` | Authenticated room, matchmaking, participant token, and TURN credential API |
| `flow-matchmaker` | HA queue worker using PostgreSQL group/row locks and expiring reservations |
| `flow-signaling` | P2P WebSocket signaling over Redis Pub/Sub |
| PostgreSQL | Durable ticket, room, assignment, audit, and usage state |
| Redis Sentinel | Cross-replica signaling and LiveKit distributed coordination |
| LiveKit | Third-party SFU media data plane |
| coturn | STUN/TURN data plane for P2P and relay fallback |

All Flow-authored backend services are Rust. LiveKit is an upstream Go
data-plane dependency and coturn, PostgreSQL, and Redis are upstream
infrastructure dependencies. They are intentionally not reimplemented. See
[ADR-0001](docs/adr/0001-service-boundary.md).

## Trust Boundary

Flow has two deliberately separate authentication boundaries.

HeteroCloud's provider worker is the only caller of
`PUT /internal/v1/service-instances/{service_instance_id}`. It sends an EdDSA
JWT signed by the HeteroCloud provider's Ed25519 private key. Flow selects a
configured public key by the required JWT `kid`; no provider HMAC secret exists
in Flow. The exact claims contract is:

```json
{
  "iss": "heterocloud",
  "aud": "heterocloud-flow",
  "sub": "principal-uuid",
  "organization_id": "organization-uuid",
  "project_id": "project-uuid",
  "service_instance_id": "service-instance-uuid",
  "action": "service-instance.reconcile",
  "generation": 1,
  "iat": 1700000000,
  "nbf": 1699999995,
  "exp": 1700000060,
  "jti": "request-uuid"
}
```

The token must use EdDSA, `nbf=iat-5`, and a lifetime no longer than 60
seconds. The request uses `Idempotency-Key: <jti>` and body
`{"generation":1,"name":"...","spec":{...}}`. Flow rejects unknown body fields,
claim/path/generation mismatch, untrusted keys, altered reuse of a `jti`, and
stale generations. An exact retry returns the original `operation_id`.
Every success response requires both fields and mirrors the persisted status:
`{"operation_id":"...","status":{"phase":"ready","observed_generation":1,"operation_id":"..."}}`.

Public room, matchmaking, TURN, and signaling operations do not accept that
provider JWT. They require `X-Flow-Principal`, `X-Flow-Timestamp`, and
`X-Flow-Signature`. The first header is base64url JSON containing
`organization_id`, `project_id`, `service_instance_id`, `principal_id`,
permissions, issuer, audience, issuance/expiration, and a context UUID. Its
signature is base64url HMAC-SHA256 over
`<X-Flow-Timestamp>.<X-Flow-Principal>`. This data-plane HMAC is an independent
delegation mechanism and cannot authenticate internal reconciliation.
Its production contract is `iss=heterocloud` and
`aud=heterocloud-flow-data`; the provider command audience remains
`heterocloud-flow`.

HeteroCloud returns this precomputed header set to the client; the client never
receives the HMAC secret. Flow first verifies the MAC, then requires
`X-Flow-Timestamp` to equal the signed `issued_at`. The complete header set is a
short-lived bearer context and may be reused until its signed `expires_at`.
Production limits the signed lifetime to 300 seconds and applies only a small
clock skew at the issuance/expiration boundaries; there is no per-request
15-second freshness check. The signed `context_id` is retained as
`principal_context_id` on public API and signaling audit records.

Every durable data-plane row and query includes `service_instance_id` as well
as organization and project. Two managed Flow instances in one project cannot
share queues, tickets, rooms, audit events, or usage idempotency keys.

## API

The generated OpenAPI 3.1 schema is served at `/openapi.json`, and the vendored
interactive documentation is served at `/docs/`. The schema contains only the
customer data-plane API; provider-only `/internal/*` operations are excluded.
Its security requirement models all three `X-Flow-*` headers as mandatory.

| Method and path | Permission | Result |
| --- | --- | --- |
| `PUT /internal/v1/service-instances/{id}` | EdDSA provider command | Idempotently persist desired generation/spec/status |
| `POST /v1/queues/{queue}/tickets` | `flow.queue.write` | Queue a P2P or SFU ticket |
| `GET /v1/queues/{queue}/tickets` | `flow.queue.read` | List scoped tickets |
| `GET /v1/tickets/{id}` | `flow.queue.read` | Read ticket and assignment |
| `DELETE /v1/tickets/{id}` | `flow.queue.write` | Cancel the caller's queued ticket |
| `POST /v1/rooms` | `flow.room.create` | Create P2P or LiveKit SFU room |
| `GET /v1/rooms` | `flow.room.read` | List scoped rooms |
| `GET /v1/rooms/{id}` | `flow.room.read` | Read a scoped room |
| `POST /v1/rooms/{id}/join` | `flow.room.join` | Issue mode-specific connection data |
| `POST /v1/turn-credentials` | `flow.turn.issue` | Issue short-lived coturn REST credentials |

All public REST requests and P2P WebSocket upgrade attempts share a Redis-backed
deployment source-IP ceiling across every API and signaling replica.
Authenticated calls additionally use a `service instance + source IP` bucket
configured in the HeteroCloud console. Services default to 20 requests per
second with a burst of 40. Successful REST responses expose
`X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `X-RateLimit-Reset`; rejected
requests return `429` with the same fields and `Retry-After`. Redis failure is
fail-closed with `503`, and readiness also verifies the limiter backend.

`X-Forwarded-For` is accepted only when the immediate peer belongs to
`FLOW_TRUSTED_PROXY_CIDRS`. The rightmost forwarded address is used so a client
cannot prepend a forged address. Direct callers and untrusted proxies are
limited by their socket peer address.

`p2p` join responses contain an ordered `connection.urls` array of WSS Flow
signaling URLs, with `/v1/signal/{room_id}` appended to every origin. `sfu`
responses contain an ordered array of public LiveKit WSS URLs, a short-lived
participant JWT, and TURN credentials. In both cases clients try the first URL
as primary and the remaining URLs in order after connection failure. TURN
credentials contain every configured UDP/TCP endpoint. LiveKit keys and TURN
secrets are shared by replicas, but neither secret is ever returned.

The first P2P WebSocket frame must carry the separate signed principal context:

```json
{
  "type":"signed_context",
  "principal_context":"...",
  "timestamp":"1700000000",
  "signature":"..."
}
```

Subsequent frames are targeted `offer`, `answer`, `ice_candidate`,
`renegotiate`, or `leave` messages:

```json
{
  "type":"offer",
  "target":"peer-principal-uuid",
  "payload":{"sdp":"v=0..."}
}
```

## Local Development

Start PostgreSQL and Redis, then set the environment variables shown in
[`deploy/env.example`](deploy/env.example). Run migrations and services:

```bash
cargo run -p flow-api -- migrate
cargo run -p flow-api
cargo run -p flow-matchmaker
cargo run -p flow-signaling
```

Run verification:

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
helm dependency build deploy/helm/heterocloud-flow
helm lint deploy/helm/heterocloud-flow -f deploy/helm/heterocloud-flow/ci/test-values.yaml
helm template flow deploy/helm/heterocloud-flow \
  -f deploy/helm/heterocloud-flow/ci/test-values.yaml
```

PostgreSQL integration tests run when `TEST_DATABASE_URL` is set and otherwise
skip without failing unit tests.

## Kubernetes

The Helm chart deploys each Rust service with three replicas, a disruption
budget, required host spreading, probes, restricted security contexts, and
NetworkPolicies. It also deploys three LiveKit replicas and three coturn
replicas on HeteroNetwork public-ingress nodes.

`coturn.relayAddress.mode` explicitly separates local public-address binding
from 1:1 NAT mapping. The HeteroNetwork overlay uses `direct-public`: the VPN
`InternalIP` selects a mapping and coturn binds the corresponding public IPv4
address directly. The chart restricts scheduling to the mapped node set and
fails pod startup when the current host IP is not mapped.

[`deploy/environments/heteronet/values.yaml`](deploy/environments/heteronet/values.yaml)
enables host networking so each Rust process reaches the node-local PostgreSQL
HAProxy at `127.0.0.1:25432`. Its pools are API `8 x 3`, matchmaker `4 x 3`,
and signaling `6 x 3`, for 54 steady-state Flow connections against the
`heterocloud_flow` role's limit of 90. Migrations run inside the host-networked
API pods and consume their existing pools; the separate migration Job is
disabled. Flow-owned workloads select the three public-IP control-plane nodes;
Redis remains independently scheduled by its subchart. Redis Sentinel uses
ephemeral `emptyDir` storage because the cluster has no `StorageClass`.

Create the required Secret first. See
[`deploy/helm/heterocloud-flow/README.md`](deploy/helm/heterocloud-flow/README.md)
for keys and installation commands. The release workflow runs only when a
GitHub Release is published; ordinary pushes and pull requests do not trigger
Actions.

## HA Limits

- API, matching, and signaling accept one replica loss without reducing below
  two replicas when scheduled across at least three Kubernetes nodes.
- Match reservations expire and return to the queue if a worker dies.
- Redis Sentinel and the optional PostgreSQL HA subchart require three
  independent nodes and durable volumes.
- A LiveKit room is hosted by one SFU node. New rooms survive a node loss, but
  participants in a room on the failed node must reconnect.
- Existing WebSocket, UDP, TURN allocation, and WebRTC sessions do not migrate
  transparently when their serving pod or public gateway dies.
- New P2P and SFU joins return three ordered WSS endpoints. Clients must fail
  over to an alternate URL when the primary endpoint cannot be reached.
- HeteroNetwork direct mode requires at least three reachable public-IP-owning
  nodes for the default replica count.
