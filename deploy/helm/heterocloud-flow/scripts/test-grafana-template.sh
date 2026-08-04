#!/usr/bin/env bash
set -euo pipefail

chart_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
repo_dir=$(cd "$chart_dir/../../.." && pwd)
environment_values="$repo_dir/deploy/environments/heteronet/values.yaml"
work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' EXIT

helm template heterocloud-flow "$chart_dir" \
  --namespace heterocloud-flow \
  --values "$environment_values" \
  --show-only templates/grafana.yaml > "$work_dir/grafana.yaml"

grep -q 'replicas: 5' "$work_dir/grafana.yaml"
grep -q 'hostNetwork: true' "$work_dir/grafana.yaml"
grep -q 'containerPort: 33000' "$work_dir/grafana.yaml"
grep -q 'grafana/grafana:13.1.0@sha256:121a7a9ece6dc10b969f1f96eed64b4f07dfac0d0b8abc070f7cb83bbde86f63' "$work_dir/grafana.yaml"
grep -q 'kind: PodDisruptionBudget' "$work_dir/grafana.yaml"
grep -q 'minAvailable: 3' "$work_dir/grafana.yaml"
grep -q 'loadBalancerClass: "heteronetwork.io/public"' "$work_dir/grafana.yaml"
grep -q 'networking.heteronetwork.io/traffic-mode: "forwarded"' "$work_dir/grafana.yaml"
grep -q 'networking.heteronetwork.io/ingress-replicas: "3"' "$work_dir/grafana.yaml"
grep -q '10.250.0.0/16' "$work_dir/grafana.yaml"
grep -q 'name: heterocloud-flow-grafana' "$work_dir/grafana.yaml"
grep -q 'key: database-url' "$work_dir/grafana.yaml"
grep -q 'key: secret-key' "$work_dir/grafana.yaml"
grep -q 'name: GF_DATABASE_LOCKING_ATTEMPT_TIMEOUT_SEC' "$work_dir/grafana.yaml"
grep -q 'value: "300"' "$work_dir/grafana.yaml"
grep -q 'name: GF_PLUGINS_PREINSTALL_DISABLED' "$work_dir/grafana.yaml"
grep -q 'path: /etc/grafana/dashboards' "$work_dir/grafana.yaml"

awk '
  /^  heterocloud-flow.json: \|$/ { emit = 1; next }
  emit && /^---$/ { emit = 0 }
  emit { sub(/^    /, ""); print }
' "$work_dir/grafana.yaml" > "$work_dir/dashboard.json"

jq -e '
  .uid == "heterocloud-flow-overview" and
  .title == "HeteroCloud Flow and VPN" and
  (.panels | length) >= 13 and
  ([.panels[].title] | index("HeteroNetwork VPN bandwidth") != null) and
  ([.panels[].title] | index("TURN throughput") != null) and
  ([.panels[].title] | index("Node CPU utilization") != null)
' "$work_dir/dashboard.json" >/dev/null

grep -q 'heteronetwork:node_vpn_receive_bytes_per_second' "$work_dir/dashboard.json"
grep -q 'flow:node_cpu_utilization:ratio' "$work_dir/dashboard.json"
grep -q 'turn_total_traffic_rcvb' "$work_dir/dashboard.json"

helm template heterocloud-flow "$chart_dir" \
  --namespace heterocloud-flow \
  --values "$environment_values" \
  --show-only templates/networkpolicy.yaml > "$work_dir/networkpolicy.yaml"
grep -q 'name: heterocloud-flow-grafana' "$work_dir/networkpolicy.yaml"
grep -q 'port: 33000' "$work_dir/networkpolicy.yaml"
grep -q 'cidr: "10.250.0.0/16"' "$work_dir/networkpolicy.yaml"

if helm template heterocloud-flow "$chart_dir" \
  --namespace heterocloud-flow \
  --values "$environment_values" \
  --set monitoring.grafana.replicaCount=1 \
  --show-only templates/grafana.yaml > /dev/null 2>&1; then
  echo "Grafana template accepted a PDB minAvailable greater than replicaCount" >&2
  exit 1
fi
