# Service usage measurements

`GET /v1/service-overview` adds service-scoped coturn and LiveKit counters to
the database counters. A successful response means every configured metrics
target was read successfully. A DNS, connection, HTTP, or parsing failure returns
HTTP 503 with error code `usage_unavailable`; no fresh `measured_at` or misleading
zero/partial byte total is returned.

The chart defaults to ready-Pod discovery through separate headless metrics
Services for LiveKit and every TURN pool. The API re-resolves these names on
each request and scrapes **all** returned addresses, with bounded concurrency.
It selects IPv4 for dual-stack discovery (IPv6 when there are no IPv4 records)
to avoid counting one Pod twice. An empty discovery result is an error.
Pending Pods are not traffic-serving targets and are not published by these
Services. Replica availability should also be monitored independently.

For external deployments, use either `COTURN_METRICS_URLS` /
`LIVEKIT_METRICS_URLS` for individual static endpoints, or the corresponding
`*_METRICS_DISCOVERY_URLS` for headless DNS names. Static URLs and discovery URLs
cannot be combined for the same component. Ordinary load-balanced Services
must not be used as discovery names, because they hide individual replicas.
In Helm, explicit `coturn.metrics.urls` / `livekit.metrics.urls` take precedence
over discovery. To explicitly disable a component's metrics, use an empty URL
list and set its `metrics.discovery` to `false`.

These are live process counters, not a durable billing ledger. coturn reports
traffic from completed TURN sessions; active sessions may not appear until
they finish. Restarting a metrics process can reset its counters. Preserve
observations before maintenance and do not interpret this endpoint as a
lifetime or invoice total across restarts.
