# HeteroCloud Flow Helm Chart

## Prerequisites

- Kubernetes 1.29 or newer on at least three nodes
- HeteroNetwork controller with `heteronetwork.io/public`
  `loadBalancerClass`
- At least three eligible public-ingress nodes for direct RTC/TURN
- External PostgreSQL 15+ HA, or `postgresql-ha.enabled=true`
- Per-node managed HeteroNetwork Caddy for browser-facing HTTPS/WSS

The chart runs three replicas each of `flow-api`, `flow-matchmaker`,
`flow-signaling`, LiveKit, and coturn. Required hostname anti-affinity and
`DoNotSchedule` topology spread prevent two replicas of one component from
sharing a node.

## Secret

Flow trusts HeteroCloud provider commands through an Ed25519 public-key set. The
JSON file is an object from JWT `kid` to PEM public key:

```json
{
  "provider-key-2026-07": "-----BEGIN PUBLIC KEY-----\n...\n-----END PUBLIC KEY-----\n"
}
```

Create the production Secret outside Helm:

```bash
deploy/helm/heterocloud-flow/scripts/create-secret.sh \
  heterocloud \
  'postgres://heterocloud_flow:...@127.0.0.1:25432/heterocloud_flow' \
  ./heterocloud-provider-public-keys.json
```

The helper creates:

- `database-url`
- `heterocloud-provider-public-keys.json`
- `flow-principal-context-hmac-secret`
- `livekit-api-key` and `livekit-api-secret`
- `livekit-keys.yaml`
- `turn-shared-secret`

There is no shared provider JWT secret. The HMAC key is only for the separate
public data-plane principal context and cannot authorize
`/internal/v1/service-instances/*`.
HeteroCloud keeps that HMAC key and gives clients a precomputed signed header
set. The set is a bearer context reusable until its signed expiration (at most
300 seconds in this environment); `x-flow-timestamp` is bound to signed
`issued_at`, not to each request's wall-clock time.

Set PostgreSQL TLS parameters in `database-url` according to the local HAProxy
configuration. Do not use `secrets.create=true` in production; it stores
plaintext values in Helm release state.

## HeteroNet Environment

Use the supplied environment overlay:

```bash
helm dependency build deploy/helm/heterocloud-flow
helm upgrade --install flow deploy/helm/heterocloud-flow \
  --namespace heterocloud \
  --create-namespace \
  -f deploy/environments/heteronet/values.yaml
```

The checked-in overlay contains the current `flow-a/b/c`, `rtc-a/b/c`, and
`turn-a/b/c` sslip hostnames. Verify their public-IP mapping before installation.
The overlay sets:

- `hostNetwork: true` and `ClusterFirstWithHostNet` for API, matchmaker, and
  signaling; LiveKit and coturn are host-network media pods as well
- the top-level control-plane node selector for Flow API, matchmaker,
  signaling, LiveKit, and coturn; the Redis subchart remains independently
  distributed
- node-local PostgreSQL URL at `127.0.0.1:25432`
- pools of API `8 x 3`, matchmaker `4 x 3`, signaling `6 x 3`: 54 steady-state
  maximum connections
- API startup migrations enabled and the standalone migration Job disabled, so
  migration uses the same local proxy and API connection pools
- data-plane principal context `iss=heterocloud` and
  `aud=heterocloud-flow-data`; provider commands remain
  `aud=heterocloud-flow`
- Redis primary plus two replicas with persistence disabled, which uses
  `emptyDir` and requires no `StorageClass`
- three ordered HTTPS/WSS public endpoints while backend listeners remain HTTP
- immutable Redis and Sentinel image digests; LiveKit `v1.13.5` and coturn
  `4.16.0`

The 54-connection total stays below the `heterocloud_flow` role limit of 90 and
leaves capacity in the PostgreSQL cluster's global 200-connection budget for
HeteroCloud and Keycloak.

## Public Endpoints

| Service | Traffic mode | Policy | Purpose |
| --- | --- | --- | --- |
| `*-api-public` (optional) | `forwarded` | `Cluster` | Plain HTTP API behind a trusted TLS tier |
| `*-signaling-public` (optional) | `forwarded` | `Cluster` | Flow P2P WebSocket behind a trusted TLS tier |
| `*-livekit-signal-public` (optional) | `forwarded` | `Cluster` | LiveKit API/WebSocket behind a trusted TLS tier |
| `*-livekit-rtc` | `direct` | `Local` | LiveKit TCP/UDP media |
| `*-turn` | `direct` | `Local` | coturn UDP/TCP listener |

The internal API, signaling, and LiveKit signal ClusterIPs always remain
available. Enable `api.publicService`, `signaling.publicService`, or
`livekit.signal.publicService` only when a trusted TLS tier fronts that L4
service and denies `/internal/*`. The HeteroNet deployment keeps all three
disabled and exposes them only through per-node Caddy HTTPS/WSS listeners.

The production browser endpoints are:

| Role | Ordered endpoints |
| --- | --- |
| Flow API and P2P signaling | `flow-a.163-220-236-51.sslip.io`, `flow-b.163-220-236-52.sslip.io`, `flow-c.163-220-236-53.sslip.io` |
| LiveKit signaling | `rtc-a.163-220-236-51.sslip.io`, `rtc-b.163-220-236-52.sslip.io`, `rtc-c.163-220-236-53.sslip.io` |
| TURN | `turn-a.163-220-236-51.sslip.io`, `turn-b.163-220-236-52.sslip.io`, `turn-c.163-220-236-53.sslip.io`, each over UDP and TCP |

Flow emits only `wss://` signaling and LiveKit URLs in join responses. The first
entry is primary and the other two are client failover endpoints. The room path
is appended to all Flow signaling origins. Shared LiveKit keys make one room
token valid at every LiveKit signaling endpoint.

For the normal HeteroNet deployment, install
[`flow.Caddyfile.example`](../../environments/heteronet/flow.Caddyfile.example)
on every eligible node through
`HETERONETWORK_AGENT_PUBLIC_WEB_GATEWAY_EXTRA_CADDYFILE`. Caddy terminates TLS
and uses local-first health failover across `10.250.0.4`, `10.250.0.5`, and
`10.250.0.6` on ports `8080`, `8082`, and `7880`. Install the matching `a`, `b`,
or `c` host pair on its public node. The example returns 404 for `/internal/*`
before any API proxy. Raw backend ports must remain private. Certificate
issuance, firewall rules, and stable multi-A DNS records are deployment
responsibilities outside this chart.

coturn remains credentialed `turn:` over UDP/TCP. Open the configured TURN
listener and relay range, and the LiveKit RTC ports, on every eligible public
node.

## External Redis

To use an existing Sentinel deployment:

```yaml
redis:
  enabled: false
externalRedis:
  sentinelUrls:
    - redis://sentinel-a.internal:26379
    - redis://sentinel-b.internal:26379
    - redis://sentinel-c.internal:26379
  sentinelMaster: flowmaster
  passwordSecretKey: redis-password
  sentinelPasswordSecretKey: redis-sentinel-password
```

If Redis authentication is enabled, provide a complete LiveKit config through
`livekit.existingConfigSecret`; it must contain matching Redis credentials.

## Failure Semantics

- PostgreSQL advisory and row locks let all matchmakers work concurrently
  without splitting a match.
- A dead matchmaker's reservation expires and its tickets return to the queue.
- Redis failover closes affected signaling sockets; reconnecting clients
  resolve the new primary. Loss of every ephemeral Redis pod loses transient
  coordination, not durable Flow state.
- A LiveKit room remains assigned to one SFU node. New rooms can use another
  replica, while participants on a failed node reconnect.
- coturn allocations and active UDP/TCP flows reconnect after pod or gateway
  loss.
