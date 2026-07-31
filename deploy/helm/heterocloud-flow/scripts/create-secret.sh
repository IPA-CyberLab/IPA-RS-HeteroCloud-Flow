#!/usr/bin/env sh
set -eu

NAMESPACE="${1:?usage: create-secret.sh NAMESPACE DATABASE_URL PROVIDER_PUBLIC_KEYS_JSON_FILE [SECRET_NAME]}"
DATABASE_URL="${2:?usage: create-secret.sh NAMESPACE DATABASE_URL PROVIDER_PUBLIC_KEYS_JSON_FILE [SECRET_NAME]}"
PROVIDER_PUBLIC_KEYS_FILE="${3:?usage: create-secret.sh NAMESPACE DATABASE_URL PROVIDER_PUBLIC_KEYS_JSON_FILE [SECRET_NAME]}"
SECRET_NAME="${4:-heterocloud-flow-secrets}"

command -v kubectl >/dev/null
command -v openssl >/dev/null
test -s "${PROVIDER_PUBLIC_KEYS_FILE}"

PRINCIPAL_CONTEXT_SECRET="$(openssl rand -base64 48 | tr -d '\n')"
LIVEKIT_SECRET="$(openssl rand -base64 48 | tr -d '\n')"
TURN_SECRET="$(openssl rand -base64 48 | tr -d '\n')"
LIVEKIT_KEY="flow-$(openssl rand -hex 8)"
KEY_FILE="$(mktemp)"
trap 'rm -f "${KEY_FILE}"' EXIT
printf '%s: %s\n' "${LIVEKIT_KEY}" "${LIVEKIT_SECRET}" >"${KEY_FILE}"

kubectl -n "${NAMESPACE}" create secret generic "${SECRET_NAME}" \
  --from-literal=database-url="${DATABASE_URL}" \
  --from-file=heterocloud-provider-public-keys.json="${PROVIDER_PUBLIC_KEYS_FILE}" \
  --from-literal=flow-principal-context-hmac-secret="${PRINCIPAL_CONTEXT_SECRET}" \
  --from-literal=livekit-api-key="${LIVEKIT_KEY}" \
  --from-literal=livekit-api-secret="${LIVEKIT_SECRET}" \
  --from-literal=turn-shared-secret="${TURN_SECRET}" \
  --from-file=livekit-keys.yaml="${KEY_FILE}" \
  --dry-run=client -o yaml |
  kubectl apply -f -
