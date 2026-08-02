#!/usr/bin/env bash
set -euo pipefail

chart_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
test_values="${chart_dir}/ci/test-values.yaml"
tmp_dir=$(mktemp -d)
trap 'rm -rf "${tmp_dir}"' EXIT

render() {
  helm template flow "${chart_dir}" \
    -f "${test_values}" \
    --show-only templates/coturn.yaml \
    "$@"
}

assert_contains() {
  local expected=$1
  local file=$2
  if ! grep -Fq -- "${expected}" "${file}"; then
    printf 'missing rendered value: %s\n' "${expected}" >&2
    exit 1
  fi
}

assert_not_contains() {
  local unexpected=$1
  local file=$2
  if grep -Fq -- "${unexpected}" "${file}"; then
    printf 'unexpected rendered value: %s\n' "${unexpected}" >&2
    exit 1
  fi
}

expect_render_failure() {
  local mode=$1
  local mappings=$2
  local expected_error=$3
  if render \
    --set-string "coturn.relayAddress.mode=${mode}" \
    --set-json "coturn.relayAddress.mappings=${mappings}" \
    >"${tmp_dir}/unexpected.yaml" 2>"${tmp_dir}/error.log"; then
    printf 'expected Helm rendering to fail\n' >&2
    exit 1
  fi
  assert_contains "${expected_error}" "${tmp_dir}/error.log"
}

render >"${tmp_dir}/coturn.yaml"
assert_contains 'fieldPath: status.hostIP' "${tmp_dir}/coturn.yaml"
assert_contains 'fieldPath: spec.nodeName' "${tmp_dir}/coturn.yaml"
assert_contains 'name: TURN_RELAY_ADDRESS_MODE' "${tmp_dir}/coturn.yaml"
assert_contains 'value: "direct-public"' "${tmp_dir}/coturn.yaml"
assert_contains 'name: TURN_RELAY_ADDRESS_MAPPINGS' "${tmp_dir}/coturn.yaml"
assert_contains 'value: "turn-node-a.example.test=192.0.2.10/10.0.0.10,turn-node-b.example.test=192.0.2.11/10.0.0.11,turn-node-c.example.test=192.0.2.12/10.0.0.12"' "${tmp_dir}/coturn.yaml"
assert_contains 'nodeAffinity:' "${tmp_dir}/coturn.yaml"
assert_contains 'matchFields:' "${tmp_dir}/coturn.yaml"
assert_contains 'key: metadata.name' "${tmp_dir}/coturn.yaml"
assert_contains '- "turn-node-a.example.test"' "${tmp_dir}/coturn.yaml"
assert_contains 'set -- "$@" "--relay-ip=${selected_public_ip}"' "${tmp_dir}/coturn.yaml"
assert_contains '"--external-ip=${selected_public_ip}/${selected_private_ip}"' "${tmp_dir}/coturn.yaml"
assert_contains 'has no matching coturn.relayAddress.mappings entry' "${tmp_dir}/coturn.yaml"
assert_contains 'is not assigned to a local interface' "${tmp_dir}/coturn.yaml"

awk '
  $0 == "          args:" { in_args = 1; next }
  in_args && $0 == "            - |" { in_script = 1; next }
  in_script && $0 == "          env:" { exit }
  in_script { sub(/^              /, ""); print }
' "${tmp_dir}/coturn.yaml" >"${tmp_dir}/start-coturn.sh"
mkdir "${tmp_dir}/bin"
cat >"${tmp_dir}/bin/turnserver" <<'EOF'
#!/bin/sh
printf '%s\n' "$@"
EOF
chmod 755 "${tmp_dir}/bin/turnserver"
cat >"${tmp_dir}/bin/hostname" <<'EOF'
#!/bin/sh
if [ "${1:-}" = -I ]; then
  printf '%s\n' "${LOCAL_IPS:-}"
  exit 0
fi
exec /usr/bin/hostname "$@"
EOF
chmod 755 "${tmp_dir}/bin/hostname"

run_startup() {
  local mode=$1
  local node_name=$2
  local host_ip=$3
  local mappings=$4
  local local_ips=$5
  PATH="${tmp_dir}/bin:${PATH}" \
  TURN_SHARED_SECRET=test-secret \
  NODE_NAME="${node_name}" \
  HOST_IP="${host_ip}" \
  LOCAL_IPS="${local_ips}" \
  TURN_RELAY_ADDRESS_MODE="${mode}" \
  TURN_RELAY_ADDRESS_MAPPINGS="${mappings}" \
    /bin/sh -ec "$(cat "${tmp_dir}/start-coturn.sh")"
}

mappings=turn-node-a.example.test=192.0.2.10/10.0.0.10,turn-node-b.example.test=192.0.2.11/10.0.0.11,turn-node-c.example.test=192.0.2.12/10.0.0.12

run_startup direct-public turn-node-b.example.test 10.0.0.11 "${mappings}" '10.0.0.11 192.0.2.11' >"${tmp_dir}/direct-public-args.log"
assert_contains '--relay-ip=192.0.2.11' "${tmp_dir}/direct-public-args.log"
assert_not_contains '--relay-ip=10.0.0.11' "${tmp_dir}/direct-public-args.log"
assert_not_contains '--external-ip=' "${tmp_dir}/direct-public-args.log"

run_startup one-to-one-nat turn-node-b.example.test 10.0.0.11 "${mappings}" '10.0.0.11' >"${tmp_dir}/nat-args.log"
assert_contains '--relay-ip=10.0.0.11' "${tmp_dir}/nat-args.log"
assert_contains '--external-ip=192.0.2.11/10.0.0.11' "${tmp_dir}/nat-args.log"

run_startup host turn-node-b.example.test 10.0.0.11 '' '' >"${tmp_dir}/host-args.log"
assert_not_contains '--relay-ip=' "${tmp_dir}/host-args.log"
assert_not_contains '--external-ip=' "${tmp_dir}/host-args.log"

if run_startup direct-public turn-node-b.example.test 10.0.0.99 "${mappings}" '10.0.0.99 192.0.2.11' \
  >"${tmp_dir}/unexpected-runtime.log" 2>"${tmp_dir}/runtime-error.log"; then
  printf 'expected unmapped host IP startup to fail\n' >&2
  exit 1
fi
assert_contains 'node turn-node-b.example.test with host IP 10.0.0.99 has no matching coturn.relayAddress.mappings entry' "${tmp_dir}/runtime-error.log"

if run_startup direct-public turn-node-a.example.test 10.0.0.10 "${mappings}" '10.0.0.10' \
  >"${tmp_dir}/unexpected-local-address.log" 2>"${tmp_dir}/local-address-error.log"; then
  printf 'expected missing local public address startup to fail\n' >&2
  exit 1
fi
assert_contains 'relay IP 192.0.2.10 for node turn-node-a.example.test is not assigned to a local interface' "${tmp_dir}/local-address-error.log"

if run_startup direct-public turn-node-c.example.test 10.0.0.11 "${mappings}" '10.0.0.11 192.0.2.11' \
  >"${tmp_dir}/unexpected-node-mapping.log" 2>"${tmp_dir}/node-mapping-error.log"; then
  printf 'expected mismatched node and host IP startup to fail\n' >&2
  exit 1
fi
assert_contains 'node turn-node-c.example.test with host IP 10.0.0.11 has no matching coturn.relayAddress.mappings entry' "${tmp_dir}/node-mapping-error.log"

expect_render_failure \
  direct-public \
  '[{"nodeName":"turn-node-a","publicIp":"192.0.2.10","privateIp":"10.0.0.10"},{"nodeName":"turn-node-b","publicIp":"192.0.2.11","privateIp":"10.0.0.11"}]' \
  'must contain at least coturn.replicaCount entries'

expect_render_failure \
  direct-public \
  '[{"nodeName":"turn-node-a","publicIp":"192.0.2.10","privateIp":"10.0.0.10"},{"nodeName":"turn-node-a","publicIp":"192.0.2.11","privateIp":"10.0.0.11"},{"nodeName":"turn-node-c","publicIp":"192.0.2.12","privateIp":"10.0.0.12"}]' \
  'duplicate nodeName'

expect_render_failure \
  direct-public \
  '[{"nodeName":"turn-node-a","publicIp":"192.0.2.10","privateIp":"10.0.0.10"},{"nodeName":"turn-node-b","publicIp":"192.0.2.10","privateIp":"10.0.0.11"},{"nodeName":"turn-node-c","publicIp":"192.0.2.12","privateIp":"10.0.0.12"}]' \
  'duplicate publicIp'

expect_render_failure \
  direct-public \
  '[{"nodeName":"turn-node-a","publicIp":"192.0.2.10","privateIp":"10.0.0.10"},{"nodeName":"turn-node-b","publicIp":"192.0.2.11","privateIp":"10.0.0.10"},{"nodeName":"turn-node-c","publicIp":"192.0.2.12","privateIp":"10.0.0.12"}]' \
  'duplicate privateIp'

expect_render_failure \
  direct-public \
  '[{"nodeName":"turn-node-a","publicIp":"999.0.2.10","privateIp":"10.0.0.10"},{"nodeName":"turn-node-b","publicIp":"192.0.2.11","privateIp":"10.0.0.11"},{"nodeName":"turn-node-c","publicIp":"192.0.2.12","privateIp":"10.0.0.12"}]' \
  "Does not match format 'ipv4'"

expect_render_failure \
  host \
  '[{"nodeName":"turn-node-a","publicIp":"192.0.2.10","privateIp":"10.0.0.10"}]' \
  'must be empty when mode is host'

if render --set coturn.replicaCount=17 \
  >"${tmp_dir}/unexpected-replica-count.yaml" 2>"${tmp_dir}/replica-count-error.log"; then
  printf 'expected excessive coturn replica count to fail\n' >&2
  exit 1
fi
assert_contains 'coturn.replicaCount must not exceed 16' "${tmp_dir}/replica-count-error.log"

render \
  --set-string coturn.relayAddress.mode=host \
  --set-json 'coturn.relayAddress.mappings=[]' \
  >"${tmp_dir}/coturn-host.yaml"
assert_not_contains 'nodeAffinity:' "${tmp_dir}/coturn-host.yaml"
assert_contains 'name: TURN_RELAY_ADDRESS_MAPPINGS' "${tmp_dir}/coturn-host.yaml"
assert_contains 'value: "host"' "${tmp_dir}/coturn-host.yaml"
assert_contains 'value: ""' "${tmp_dir}/coturn-host.yaml"

printf 'coturn Helm relay address tests passed\n'
