# TURN placement and external verification, 2026-11-07

Flow's two TURN pools previously each had one ready replica, both on
`ichikawap1`. The other configured replicas could not run on the dedicated
control-plane nodes. The HeteroNetwork admission controller also required every
direct Pod to use the HTTPS ingress selector, which excluded `uc-k8sp5` even
though its TURN ports were reachable.

HeteroNetwork now admits a separate, operator-managed `turn` placement class.
Terraform and Argo CD designate only the two application workers for that
class; the HTTPS ingress label on `uc-k8sp5` remains false. Flow runs one
instance of each pool on each worker, and issues the existing hostname plus
the additional TURN-only IPv4 address. The shared HTTPS DNS record remains
unchanged. The second pool reserves 100m CPU per replica rather than 250m, with
no CPU limit, so it can be scheduled on the constrained worker. This reduces
the CPU share guaranteed to that pool under contention.

## Verified

- HeteroNetwork controller source `d5a5f32d`, image
  `sha256:47a7a1c8e0b33814b63fe48134e37263d3abda70bde40870f115d2a9e44b3ad1`;
  [build and tests](https://github.com/IPA-CyberLab/IPA-RS-HeteroNetwork/actions/runs/42356480088).
- Kubernetes server-side admission dry runs verified the original public
  selector, the separate TURN selector, and rejection of invalid classes and
  conflicting selectors. The controller is ready on both workers.
- Each TURN pool reached two ready replicas, one on each worker. Flow API and
  LiveKit reached seven ready replicas each after their rolling updates.
- From an external host, two authenticated TURN allocations exchanged data
  bidirectionally between the workers. UDP and TCP to ports 3478 and 3479 all
  passed in three consecutive runs, using `scripts/verify_turn_relay_pair.py`.
- Headless Chromium used **only** the new worker as its ICE server. Relay-only
  WebRTC data channels exchanged messages in both directions for UDP and TCP
  on both ports; the selected local and remote candidates were relay types.
  This uses an operator-generated short-lived credential and does not exercise
  HeteroCloud sign-in or Flow's credential-issuance API.
- The initial relay probe used asynchronous sends without waiting for cleanup;
  one TCP attempt timed out. The probe now awaits its sends and allocation
  cleanup. The subsequent repeated probes passed; the initial timeout's cause
  has not been independently established.
- The rendered `FlowMetricsTargetMissing` alert now detects all targets
  disappearing as well as a single failed target. Nine scenarios passed using
  real `promtool`, including disappearance of either or both media jobs and
  complete loss of the Grafana or Prometheus scrape targets. Prometheus pods
  now restart when the rendered configuration or alert rules change.

## Remaining limits

This checks representative relay allocations, not every port in both relay
ranges or throughput under load. The test selects one TURN host directly; it
does not shut down a worker or verify automatic recovery during a live call.
No recurring external relay probe or external alert notification is configured.
The shared Flow HTTPS hostname still resolves only to `ichikawap1`, so losing
that host can prevent new clients from obtaining credentials even though the
other TURN server remains available. A separate HTTPS ingress and an additional
independent worker are still needed for stronger availability. Storage volume
degradation is also independent of this change and remains unresolved.
