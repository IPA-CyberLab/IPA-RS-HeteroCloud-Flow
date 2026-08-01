# Flow LiveKit image

`Dockerfile.livekit` builds the upstream LiveKit v1.13.5 commit pinned in the
Dockerfile and applies `service-metrics.patch` before compilation.

The patch publishes `livekit_service_packet_bytes` with bounded labels for the
Flow service ID, direction, and transmission type. Flow room names carry the
service ID, allowing the API to report SFU ingress and egress without exposing
room names as Prometheus labels.

These Prometheus counters reset when a LiveKit process restarts. They are
suitable for the management console's operational totals, but a billing system
must persist sampled deltas in an idempotent external store.
