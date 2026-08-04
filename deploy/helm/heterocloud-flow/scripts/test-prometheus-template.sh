#!/usr/bin/env bash
set -euo pipefail

chart_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
repo_dir=$(cd "$chart_dir/../../.." && pwd)
environment_values="$repo_dir/deploy/environments/heteronet/values.yaml"
work_dir=$(mktemp -d)
container=
cleanup() {
  if [[ -n "$container" ]]; then
    docker container rm "$container" >/dev/null 2>&1 || true
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT

helm template heterocloud-flow "$chart_dir" \
  --namespace heterocloud-flow \
  --values "$environment_values" \
  --show-only templates/prometheus.yaml > "$work_dir/rendered.yaml"

grep -q 'job_name: heteronetwork-agents' "$work_dir/rendered.yaml"
grep -q 'job_name: grafana' "$work_dir/rendered.yaml"
grep -q 'alert: GrafanaReplicaMissing' "$work_dir/rendered.yaml"
grep -q 'record: heteronetwork:node_vpn_receive_bytes_per_second' "$work_dir/rendered.yaml"
grep -q 'hostNetwork: true' "$work_dir/rendered.yaml"
grep -q -- '--web.listen-address=$(PROMETHEUS_LISTEN_ADDRESS):9090' "$work_dir/rendered.yaml"
grep -q 'replicas: 5' "$work_dir/rendered.yaml"
grep -q 'kind: PodDisruptionBudget' "$work_dir/rendered.yaml"
grep -q 'minAvailable: 3' "$work_dir/rendered.yaml"
grep -q 'loadBalancerClass: "heteronetwork.io/public"' "$work_dir/rendered.yaml"
grep -q 'networking.heteronetwork.io/traffic-mode: "forwarded"' "$work_dir/rendered.yaml"
grep -q 'networking.heteronetwork.io/ingress-replicas: "3"' "$work_dir/rendered.yaml"
grep -q '10.250.0.0/16' "$work_dir/rendered.yaml"
grep -q 'job_name: kubernetes-services-annotated' "$work_dir/rendered.yaml"
grep -q -- '- argocd' "$work_dir/rendered.yaml"
grep -q -- '- endpoints' "$work_dir/rendered.yaml"
grep -q -- '- services' "$work_dir/rendered.yaml"

awk '
  /^  prometheus.yml: \|$/ { emit = 1; next }
  /^  rules.yml: \|$/ { emit = 0 }
  emit { sub(/^    /, ""); print }
' "$work_dir/rendered.yaml" > "$work_dir/prometheus.yml"

awk '
  /^  rules.yml: \|$/ { emit = 1; next }
  emit && /^---$/ { emit = 0 }
  emit { sub(/^    /, ""); print }
' "$work_dir/rendered.yaml" > "$work_dir/rules.yml"

sed -i \
  -e 's#/etc/prometheus/rules.yml#/tmp/rules.yml#' \
  -e 's#/var/run/secrets/kubernetes.io/serviceaccount/token#/tmp/token#' \
  -e 's#/var/run/secrets/kubernetes.io/serviceaccount/ca.crt#/tmp/ca.crt#' \
  "$work_dir/prometheus.yml"
printf 'template-test-token' > "$work_dir/token"
cp /etc/ssl/certs/ca-certificates.crt "$work_dir/ca.crt"

image=$(awk '/image: prom\/prometheus:v.*@/{print $2; exit}' "$work_dir/rendered.yaml")
test -n "$image"

container=$(docker create \
  --entrypoint /bin/promtool \
  "$image" \
  check config /tmp/prometheus.yml)
for file in prometheus.yml rules.yml token ca.crt; do
  docker cp "$work_dir/$file" "$container:/tmp/$file"
done
docker start --attach "$container"
