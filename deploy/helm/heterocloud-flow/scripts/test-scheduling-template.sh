#!/usr/bin/env bash
set -euo pipefail

chart_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
repo_dir=$(cd "$chart_dir/../../.." && pwd)
environment_values="$repo_dir/deploy/environments/heteronet/values.yaml"
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

for component in api matchmaker signaling livekit coturn prometheus grafana pdb; do
  helm template flow "$chart_dir" -f "$environment_values" \
    --namespace heterocloud-flow \
    --show-only "templates/${component}.yaml" >"$tmp_dir/${component}.yaml"
done

for component in api matchmaker signaling livekit; do
  grep -q '^  replicas: 6$' "$tmp_dir/${component}.yaml"
done

for component in prometheus grafana; do
  grep -q '^  replicas: 5$' "$tmp_dir/${component}.yaml"
done

for component in api matchmaker signaling grafana; do
  grep -q '^      nodeSelector:$' "$tmp_dir/${component}.yaml"
  grep -q 'database.heteronetwork.io/proxy-ready: "true"' "$tmp_dir/${component}.yaml"
done

for component in livekit prometheus; do
  ! grep -q '^      nodeSelector:' "$tmp_dir/${component}.yaml"
done

test "$(grep -c '^  replicas: 3$' "$tmp_dir/coturn.yaml")" -eq 2
grep -q '^      nodeSelector:$' "$tmp_dir/coturn.yaml"
grep -q 'networking.heteronetwork.io/public-ingress: "true"' "$tmp_dir/coturn.yaml"

test "$(grep -c '^  minAvailable: 3$' "$tmp_dir/pdb.yaml")" -eq 5
! grep -q '^  minAvailable: 2$' "$tmp_dir/pdb.yaml"

helm template flow "$chart_dir" -f "$environment_values" \
  --namespace heterocloud-flow >"$tmp_dir/all.yaml"
test "$(grep -c '^  replicas: 6$' "$tmp_dir/all.yaml")" -eq 4
test "$(grep -c '^  replicas: 5$' "$tmp_dir/all.yaml")" -eq 2
grep -Eq 'sentinel monitor flowmaster .* 6379 2' "$tmp_dir/all.yaml"
for index in 0 1 2; do
  grep -Fq "redis://heterocloud-flow-redis-node-${index}.heterocloud-flow-redis-headless.heterocloud-flow.svc.cluster.local:26379" "$tmp_dir/all.yaml"
  grep -Fq "heterocloud-flow-redis-node-${index}.heterocloud-flow-redis-headless.heterocloud-flow.svc.cluster.local:26379" "$tmp_dir/all.yaml"
done

helm template flow "$chart_dir" \
  --namespace heterocloud-flow \
  --set-string 'database.nodeSelector.database\.example\.com/proxy-ready=true' \
  --show-only templates/migration-job.yaml >"$tmp_dir/migration.yaml"
grep -q '^      nodeSelector:$' "$tmp_dir/migration.yaml"
grep -q 'database.example.com/proxy-ready: "true"' "$tmp_dir/migration.yaml"

printf 'Flow six-node scheduling tests passed\n'
