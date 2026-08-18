#!/usr/bin/env bash
set -euo pipefail

chart_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
repo_dir=$(cd "$chart_dir/../../.." && pwd)
environment_values="$repo_dir/deploy/environments/heteronet/values.yaml"
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

for component in api matchmaker signaling livekit coturn prometheus grafana pdb database-proxy; do
  helm template flow "$chart_dir" -f "$environment_values" \
    --namespace heterocloud-flow \
    --show-only "templates/${component}.yaml" >"$tmp_dir/${component}.yaml"
done

for component in api matchmaker signaling livekit; do
  grep -q '^  replicas: 7$' "$tmp_dir/${component}.yaml"
  grep -q '^          nodeTaintsPolicy: Honor$' "$tmp_dir/${component}.yaml"
done

for component in prometheus grafana; do
  grep -q '^  replicas: 6$' "$tmp_dir/${component}.yaml"
done

for component in api matchmaker signaling; do
  ! grep -q '^      nodeSelector:$' "$tmp_dir/${component}.yaml"
  grep -q '^          requiredDuringSchedulingIgnoredDuringExecution:$' "$tmp_dir/${component}.yaml"
  for node in ichikawap1 uc-k8s3p uc-k8sp1 uc-k8sp2 uc-k8sv1; do
    grep -q -- "- \"$node\"" "$tmp_dir/${component}.yaml"
  done
  ! grep -q -- '- "mizuame-nucboxg5"' "$tmp_dir/${component}.yaml"
  ! grep -q -- '- "mizuame"' "$tmp_dir/${component}.yaml"
done

for component in livekit prometheus grafana; do
  ! grep -q '^      nodeSelector:' "$tmp_dir/${component}.yaml"
done

test "$(grep -c '^  replicas: 4$' "$tmp_dir/coturn.yaml")" -eq 2
test "$(grep -c '^          nodeTaintsPolicy: Honor$' "$tmp_dir/coturn.yaml")" -eq 2
grep -q '^      nodeSelector:$' "$tmp_dir/coturn.yaml"
grep -q 'networking.heteronetwork.io/public-ingress: "true"' "$tmp_dir/coturn.yaml"

test "$(grep -c '^  minAvailable: 4$' "$tmp_dir/pdb.yaml")" -eq 5
! grep -q '^  minAvailable: 2$' "$tmp_dir/pdb.yaml"

helm template flow "$chart_dir" -f "$environment_values" \
  --namespace heterocloud-flow >"$tmp_dir/all.yaml"
test "$(grep -c '^  replicas: 7$' "$tmp_dir/all.yaml")" -eq 4
test "$(grep -c '^  replicas: 6$' "$tmp_dir/all.yaml")" -eq 2
grep -Eq 'sentinel monitor flowmaster .* 6379 3' "$tmp_dir/all.yaml"
grep -Fq 'sentinel down-after-milliseconds flowmaster 2000' "$tmp_dir/all.yaml"
grep -Fq 'sentinel failover-timeout flowmaster 10000' "$tmp_dir/all.yaml"
grep -Fq 'sentinel parallel-syncs flowmaster 2' "$tmp_dir/all.yaml"
for index in 0 1 2 3 4; do
  grep -Fq "heterocloud-flow-redis-node-${index}.heterocloud-flow-redis-headless.heterocloud-flow.svc.cluster.local:26379" "$tmp_dir/all.yaml"
done
grep -q '^  internalTrafficPolicy: Local$' "$tmp_dir/all.yaml"
grep -q 'nodeName: "ichikawap1"' "$tmp_dir/all.yaml"
grep -q -- '- "10.250.0.8"' "$tmp_dir/database-proxy.yaml"
grep -q 'nodeName: "uc-k8sv1"' "$tmp_dir/database-proxy.yaml"
grep -q '^          preferredDuringSchedulingIgnoredDuringExecution:$' "$tmp_dir/livekit.yaml"
! grep -q '^          requiredDuringSchedulingIgnoredDuringExecution:$' "$tmp_dir/livekit.yaml"

helm template flow "$chart_dir" \
  --namespace heterocloud-flow \
  --set-string 'database.nodeSelector.database\.example\.com/proxy-ready=true' \
  --show-only templates/migration-job.yaml >"$tmp_dir/migration.yaml"
grep -q '^      nodeSelector:$' "$tmp_dir/migration.yaml"
grep -q 'database.example.com/proxy-ready: "true"' "$tmp_dir/migration.yaml"

printf 'Flow six-node scheduling tests passed\n'
