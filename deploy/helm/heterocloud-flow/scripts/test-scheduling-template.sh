#!/usr/bin/env bash
set -euo pipefail

chart_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
repo_dir=$(cd "$chart_dir/../../.." && pwd)
environment_values="$repo_dir/deploy/environments/heteronet/values.yaml"
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

for component in api matchmaker signaling livekit coturn prometheus grafana pdb; do
  helm template flow "$chart_dir" -f "$environment_values" \
    --show-only "templates/${component}.yaml" >"$tmp_dir/${component}.yaml"
done

for component in api matchmaker signaling livekit prometheus grafana; do
  grep -q '^  replicas: 5$' "$tmp_dir/${component}.yaml"
  ! grep -q '^      nodeSelector:' "$tmp_dir/${component}.yaml"
done

grep -q '^  replicas: 3$' "$tmp_dir/coturn.yaml"
grep -q '^      nodeSelector:$' "$tmp_dir/coturn.yaml"
grep -q 'networking.heteronetwork.io/public-ingress: "true"' "$tmp_dir/coturn.yaml"

test "$(grep -c '^  minAvailable: 3$' "$tmp_dir/pdb.yaml")" -eq 4
test "$(grep -c '^  minAvailable: 2$' "$tmp_dir/pdb.yaml")" -eq 1

helm template flow "$chart_dir" -f "$environment_values" >"$tmp_dir/all.yaml"
test "$(grep -c '^  replicas: 5$' "$tmp_dir/all.yaml")" -eq 6
grep -Eq 'sentinel monitor flowmaster .* 6379 2' "$tmp_dir/all.yaml"

printf 'Flow five-node scheduling tests passed\n'
