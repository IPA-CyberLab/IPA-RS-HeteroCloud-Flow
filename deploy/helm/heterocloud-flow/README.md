# HeteroCloud Flow Helm Chart

## Prerequisites

- Kubernetes 1.29 or newer on at least three nodes
- HeteroNetwork controller with `heteronetwork.io/public`
  `loadBalancerClass`
- At least three eligible public-ingress nodes for direct TURN
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
Flow stores provider-issued `principal-context.revoke` commands in PostgreSQL.
REST checks are fail-closed, and P2P WebSockets re-check at each 15-second
heartbeat. LiveKit and TURN credentials already issued before revocation remain
valid only for their context-bounded residual TTL; those external credential
formats do not support a database-backed immediate revocation check.

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

The checked-in overlay uses the unified `flow.heterocloud.mizuame.app` public
name. Its multi-address A record must contain every public ingress node. The
overlay sets:

- `hostNetwork: true` and `ClusterFirstWithHostNet` for API, matchmaker, and
  signaling; LiveKit and coturn are host-network media pods as well
- five replicas of each forwarded workload, spread one per Kubernetes node;
  coturn alone is selected onto the three `public-ingress=true` nodes
- explicit coturn `direct-public` relay addressing; each VPN `status.hostIP`
  selects a public IPv4 address that is locally assigned on the same host, and
  coturn binds its relay socket directly to that public address
- node-local PostgreSQL URL at `127.0.0.1:25432`
- pools of API `6 x 5`, matchmaker `3 x 5`, signaling `4 x 5`: 65 steady-state
  maximum connections
- API startup migrations enabled and the standalone migration Job disabled, so
  migration uses the same local proxy and API connection pools
- data-plane principal context `iss=heterocloud` and
  `aud=heterocloud-flow-data`; provider commands remain
  `aud=heterocloud-flow`
- Redis primary plus two replicas and Sentinel quorum two on the three always-on
  control-plane nodes, with persistence disabled; this uses `emptyDir` and
  requires no `StorageClass`
- one stable HTTPS/WSS public endpoint while backend listeners remain HTTP
- a Redis-backed deployment source-IP ceiling shared by API and signaling
  replicas, plus lower per-service buckets managed from HeteroCloud; forwarding
  metadata is trusted only from the three managed Caddy addresses
- immutable Redis and Sentinel image digests; LiveKit `v1.13.5` and coturn
  `4.16.0`
- five Prometheus replicas that scrape Kubernetes resource/cAdvisor,
  HeteroNetwork, LiveKit, and coturn metrics every 15 seconds and independently
  retain 15 days or 10 GB
- five Grafana replicas backed by shared PostgreSQL, with a provisioned
  Prometheus datasource and Flow/VPN capacity dashboard

The 65-connection total stays below the `heterocloud_flow` role limit of 90 and
leaves capacity in the PostgreSQL cluster's global 200-connection budget for
HeteroCloud and Keycloak.

## Public Endpoints

| Service | Traffic mode | Policy | Purpose |
| --- | --- | --- | --- |
| `*-api-public` (optional) | `forwarded` | `Cluster` | Plain HTTP API behind a trusted TLS tier |
| `*-signaling-public` (optional) | `forwarded` | `Cluster` | Flow P2P WebSocket behind a trusted TLS tier |
| `*-livekit-signal-public` (optional) | `forwarded` | `Cluster` | LiveKit API/WebSocket behind a trusted TLS tier |
| `*-livekit-rtc` | `forwarded` | `Cluster` | LiveKit TCP/UDP media through redundant public ingress |
| `*-turn` | `direct` | `Local` | coturn UDP/TCP listener |

These modes are fixed by the chart. Only coturn is scheduled as a direct
workload; API, signaling, LiveKit, Prometheus, and Grafana use forwarded public
Services. Supplying the traffic-mode annotation through values is rejected.
This Kubernetes traffic policy is separate from WebRTC ICE selection. LiveKit
advertises STUN-discovered external candidates and enables TCP/TURN fallback.
Clients use a directly reachable path when ICE checks succeed and select the
direct coturn relay only when NAT or firewall checks reject every direct path.
There is no chart or API setting that forces relay-only client transport.

The internal API, signaling, and LiveKit signal ClusterIPs always remain
available. Enable `api.publicService`, `signaling.publicService`, or
`livekit.signal.publicService` only when a trusted TLS tier fronts that L4
service and denies `/internal/*`. The HeteroNet deployment enables all three as
forwarded Services, restricts their source ranges to its three Caddy hosts, and
uses the Caddy HTTPS/WSS listeners as the only Internet-facing entry points.

The production browser endpoints are:

| Role | Endpoint |
| --- | --- |
| Flow API and P2P signaling | `flow.heterocloud.mizuame.app` |
| LiveKit signaling | `flow.heterocloud.mizuame.app` |
| STUN/TURN | `flow.heterocloud.mizuame.app`, over UDP and TCP |

Flow emits only secure signaling and LiveKit URLs in join responses. DNS selects
an available gateway for the unified hostname. The room path is appended to the
Flow signaling origin. Shared LiveKit keys make one room token valid at every
LiveKit signaling replica.

For the normal HeteroNet deployment, install the node-specific public gateway
configuration from the HeteroCloud repository. Caddy terminates TLS, preserves
the direct client address in `X-Forwarded-For`, routes API, signaling, and
LiveKit paths to the local data-plane replica, and returns 404 for `/internal/*`
before any API proxy. Raw backend ports must remain private. Certificate
issuance, firewall rules, and stable multi-A DNS records are deployment
responsibilities outside this chart.

coturn remains credentialed `turn:` over UDP/TCP. Relay address behavior is
never auto-detected; select one of these explicit modes:

- `host`: coturn selects a host address itself; `mappings` must be empty
- `direct-public`: `privateIp` identifies the scheduled node through the
  Downward API `status.hostIP`, while coturn binds `--relay-ip=publicIp`;
  `publicIp` must be assigned directly to a local host interface
- `one-to-one-nat`: coturn binds `--relay-ip=privateIp` and advertises
  `--external-ip=publicIp/privateIp`; use only where same-port 1:1 NAT exists

The HeteroNet environment uses `direct-public` because its public addresses are
local VLAN interface addresses, while `10.250.0.x` addresses are `/32` overlay
selection keys:

```yaml
coturn:
  relayAddress:
    mode: direct-public
    mappings:
      - nodeName: turn-node-a
        publicIp: 192.0.2.10
        privateIp: 10.250.0.10
      - nodeName: turn-node-b
        publicIp: 192.0.2.11
        privateIp: 10.250.0.11
      - nodeName: turn-node-c
        publicIp: 192.0.2.12
        privateIp: 10.250.0.12
```

Required node affinity limits the Deployment to the named Kubernetes Nodes, while hostname
anti-affinity keeps one replica per node. A pod may therefore be rescheduled
onto any mapped node without changing its template. At startup it selects the
single mapping whose `nodeName` equals `spec.nodeName` and whose `privateIp`
equals `status.hostIP`. It also verifies that the selected relay address is
assigned to a local host interface, and exits instead of starting coturn if
either check fails. In `direct-public` mode there is no
`--external-ip` argument: binding the local public address makes
`XOR-RELAYED-ADDRESS` public without relying on NAT.

The mapping list must contain at least `coturn.replicaCount` unique node names,
public addresses, and private addresses. Before replacing a node or changing
its VPN address, update this list in the same Helm rollout. The Kubernetes Node
name must match `nodeName`, and `status.addresses[InternalIP]` must match
`privateIp`. Open the TURN listener and the complete relay port range on every
mapped public node. The NAT path must preserve relay ports one-to-one; for
example, public UDP port `49162` must reach private UDP port `49162` on the
mapped node. This NAT requirement applies only to `one-to-one-nat`; in
`direct-public`, open the relay range on the directly assigned public address.

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

## Prometheus Monitoring

Prometheus is disabled by default and enabled by the HeteroNet environment
overlay. It uses read-only Kubernetes discovery and node-proxy permissions to
collect:

- node, pod, and container CPU and memory from kubelet resource metrics
- root-interface and container network counters from cAdvisor
- HeteroNetwork Agent path, lazy-connect, relay, and packet-flow metrics
- current coturn allocations and aggregate packet/byte counters
- LiveKit room, participant, packet, process, and Go runtime metrics

Unbounded coturn per-credential series are discarded at ingestion; aggregate
`turn_total_*` counters remain available. Recording rules publish node CPU,
memory, network rates, HeteroNetwork VPN-interface transfer rates, and TURN
allocation utilization. Kubernetes node discovery automatically follows every
HeteroNetwork Agent at its node InternalIP and VPN Web UI metrics port. Alerting
rules are visible in Prometheus, but notifications require a separately
configured Alertmanager.

By default the service is `ClusterIP` only. The HeteroNetwork deployment runs
three Prometheus replicas on distinct control-plane nodes. Each process binds
to its node's VPN InternalIP, while a `heteronetwork.io/public` LoadBalancer
Service in `forwarded` mode can route any selected ingress gateway to any Ready
replica. `loadBalancerSourceRanges` is mandatory for this monitoring Service;
the production values admit only the HeteroNetwork VPN CIDR instead of exposing
the unauthenticated Prometheus UI to the Internet.

List the redundant VPN listeners and forwarded Service ingress addresses:

```bash
kubectl -n heterocloud-flow get pod \
  -l app.kubernetes.io/component=prometheus \
  -o custom-columns='POD:.metadata.name,NODE:.spec.nodeName,VPN_IP:.status.hostIP,READY:.status.containerStatuses[0].ready'
kubectl -n heterocloud-flow get service heterocloud-flow-prometheus -o wide
```

The cluster has no shared `StorageClass`, so every replica stores a complete
TSDB on its own node-local `hostPath`. Required pod anti-affinity, rolling
updates with at most one unavailable replica, and a `minAvailable: 2` PDB keep
two independent copies available during a single-node failure. Long-term
retention beyond simultaneous loss of the three monitoring nodes still
requires remote-write or object storage.

## Grafana Dashboards

Grafana is disabled in the chart defaults and enabled by the HeteroNet
environment overlay. The production topology runs three replicas on distinct
control-plane nodes with required anti-affinity, rolling updates with at most
one unavailable replica, and a `minAvailable: 2` PDB. All replicas use the same
PostgreSQL URL and security key from `monitoring.grafana.existingSecret`; local
SQLite is not used. This keeps users, sessions, and dashboard metadata
consistent after a pod or node failure. Replicas wait up to five minutes for
the database migration lock so a concurrent startup does not fail while one
replica upgrades the shared schema.

The provisioned `HeteroCloud Flow and VPN` dashboard covers node CPU and
memory, total and HeteroNetwork-interface bandwidth, LiveKit rooms and
participants, TURN allocations and transfer rates, VPN peer/path state,
LazyConnect probes, and monitoring target availability. Prometheus also
scrapes each Grafana replica and raises `GrafanaReplicaMissing` when fewer than
the configured replica count are healthy.

Production Grafana binds to each control-plane node's VPN InternalIP. Its
`heteronetwork.io/public` LoadBalancer runs in `forwarded` mode with three
ingress replicas, and both `loadBalancerSourceRanges` and NetworkPolicy admit
only `10.250.0.0/16`. Anonymous access is Viewer-only within that VPN boundary;
administrative credentials remain in the existing Secret.

The Secret must contain `database-url`, `admin-user`, `admin-password`, and a
shared `secret-key`. The database URL must use Grafana's `postgres://` scheme,
not `postgresql://`. List the redundant listeners and Service addresses with:

```bash
kubectl -n heterocloud-flow get pod \
  -l app.kubernetes.io/component=grafana \
  -o custom-columns='POD:.metadata.name,NODE:.spec.nodeName,VPN_IP:.status.hostIP,READY:.status.containerStatuses[0].ready'
kubectl -n heterocloud-flow get service heterocloud-flow-grafana -o wide
```

## Failure Semantics

- PostgreSQL advisory and row locks let all matchmakers work concurrently
  without splitting a match.
- A dead matchmaker's reservation expires and its tickets return to the queue.
- Matchmakers share PostgreSQL activity claims and remove P2P or SFU rooms
  after ten minutes with no participants; another replica resumes scans after
  a worker failure.
- Redis failover closes affected signaling sockets; reconnecting clients
  resolve the new primary. Loss of every ephemeral Redis pod loses transient
  coordination, not durable Flow state.
- A LiveKit room remains assigned to one SFU node. New rooms can use another
  replica, while participants on a failed node reconnect.
- coturn allocations and active UDP/TCP flows reconnect after pod or gateway
  loss.
