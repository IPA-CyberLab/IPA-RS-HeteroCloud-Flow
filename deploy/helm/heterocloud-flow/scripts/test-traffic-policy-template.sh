#!/usr/bin/env bash
set -euo pipefail

chart_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
repo_dir=$(cd "$chart_dir/../../.." && pwd)
test_values="$chart_dir/ci/test-values.yaml"
environment_values="$repo_dir/deploy/environments/heteronet/values.yaml"
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

helm template flow "$chart_dir" -f "$test_values" >"$tmp_dir/test.yaml"
helm template flow "$chart_dir" -f "$test_values" \
  --show-only templates/livekit.yaml >"$tmp_dir/livekit.yaml"
helm template flow "$chart_dir" -f "$test_values" \
  --show-only templates/coturn.yaml >"$tmp_dir/coturn.yaml"
helm template flow "$chart_dir" -f "$environment_values" >"$tmp_dir/heteronet.yaml"

test "$(grep -c 'networking.heteronetwork.io/traffic-mode: "forwarded"' "$tmp_dir/livekit.yaml")" -eq 2
! grep -q 'networking.heteronetwork.io/traffic-mode: direct' "$tmp_dir/livekit.yaml"
test "$(grep -c 'externalTrafficPolicy: Cluster' "$tmp_dir/livekit.yaml")" -eq 2
! grep -q 'externalTrafficPolicy: Local' "$tmp_dir/livekit.yaml"

grep -q 'networking.heteronetwork.io/traffic-mode: direct' "$tmp_dir/coturn.yaml"
grep -q 'networking.heteronetwork.io/traffic-mode: "direct"' "$tmp_dir/coturn.yaml"
grep -q 'externalTrafficPolicy: Local' "$tmp_dir/coturn.yaml"
! grep -q 'networking.heteronetwork.io/traffic-mode: "forwarded"' "$tmp_dir/coturn.yaml"

test "$(grep -c 'networking.heteronetwork.io/traffic-mode: direct' "$tmp_dir/test.yaml")" -eq 1
test "$(grep -c 'networking.heteronetwork.io/traffic-mode: "direct"' "$tmp_dir/test.yaml")" -eq 1
test "$(grep -c 'networking.heteronetwork.io/traffic-mode: direct' "$tmp_dir/heteronet.yaml")" -eq 1
test "$(grep -c 'networking.heteronetwork.io/traffic-mode: "direct"' "$tmp_dir/heteronet.yaml")" -eq 1
test "$(grep -c 'networking.heteronetwork.io/traffic-mode: "forwarded"' "$tmp_dir/heteronet.yaml")" -eq 6
test "$(grep -c 'externalTrafficPolicy: Cluster' "$tmp_dir/heteronet.yaml")" -eq 6
test "$(grep -c 'loadBalancerSourceRanges:' "$tmp_dir/heteronet.yaml")" -eq 5
grep -q 'use_external_ip: true' "$tmp_dir/test.yaml"
grep -q 'use_external_ip: true' "$tmp_dir/heteronet.yaml"
grep -q 'advertise_internal_ip: true' "$tmp_dir/test.yaml"
grep -q 'advertise_internal_ip: true' "$tmp_dir/heteronet.yaml"
grep -q 'allow_tcp_fallback: true' "$tmp_dir/test.yaml"
grep -q 'allow_tcp_fallback: true' "$tmp_dir/heteronet.yaml"
grep -q '"turn-a.example.test:3478"' "$tmp_dir/test.yaml"
grep -q '"flow.heterocloud.mizuame.app:3478"' "$tmp_dir/heteronet.yaml"
! grep -q 'use_external_ip: false' "$tmp_dir/test.yaml"
! grep -q 'use_external_ip: false' "$tmp_dir/heteronet.yaml"

expect_override_failure() {
  local values_file=$1
  local expected_path=$2
  if helm template flow "$chart_dir" -f "$test_values" -f "$values_file" \
    >"$tmp_dir/unexpected.yaml" 2>"$tmp_dir/error.log"; then
    printf 'expected a user-supplied traffic-mode annotation to fail\n' >&2
    exit 1
  fi
  grep -q "$expected_path must not set networking.heteronetwork.io/traffic-mode" "$tmp_dir/error.log"
}

cat >"$tmp_dir/api-override.yaml" <<'EOF'
api:
  publicService:
    annotations:
      networking.heteronetwork.io/traffic-mode: direct
EOF
cat >"$tmp_dir/signaling-override.yaml" <<'EOF'
signaling:
  publicService:
    annotations:
      networking.heteronetwork.io/traffic-mode: direct
EOF
cat >"$tmp_dir/livekit-signal-override.yaml" <<'EOF'
livekit:
  signal:
    publicService:
      annotations:
        networking.heteronetwork.io/traffic-mode: direct
EOF
cat >"$tmp_dir/livekit-rtc-override.yaml" <<'EOF'
livekit:
  rtc:
    annotations:
      networking.heteronetwork.io/traffic-mode: direct
EOF
cat >"$tmp_dir/coturn-override.yaml" <<'EOF'
coturn:
  annotations:
    networking.heteronetwork.io/traffic-mode: forwarded
EOF

expect_override_failure "$tmp_dir/api-override.yaml" api.publicService.annotations
expect_override_failure "$tmp_dir/signaling-override.yaml" signaling.publicService.annotations
expect_override_failure "$tmp_dir/livekit-signal-override.yaml" livekit.signal.publicService.annotations
expect_override_failure "$tmp_dir/livekit-rtc-override.yaml" livekit.rtc.annotations
expect_override_failure "$tmp_dir/coturn-override.yaml" coturn.annotations

printf 'Flow fixed traffic policy tests passed\n'
