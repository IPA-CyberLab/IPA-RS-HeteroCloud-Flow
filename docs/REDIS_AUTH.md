# Authenticated Redis And Sentinel

When enabling authentication on bundled Redis, provide an existing Secret:

```yaml
redis:
  enabled: true
  auth:
    enabled: true
    sentinel: true
    existingSecret: heterocloud-flow-dev-secrets
    existingSecretPasswordKey: redis-password
livekit:
  existingConfigSecret: heterocloud-flow-dev-livekit-config
```

The API and signaling Deployments consume this Secret through `REDIS_PASSWORD`
and, when Sentinel authentication is enabled, `REDIS_SENTINEL_PASSWORD`.
Credentials are not interpolated into URLs or literal environment values.
The bundled Bitnami chart consumes the same existing Secret/key.

The LiveKit Secret's `livekit.yaml` must independently contain matching Redis
and Sentinel passwords and the configured Sentinel master name and addresses.
The chart does not inspect or generate that Secret. Authenticated Redis without
`livekit.existingConfigSecret` fails rendering instead of generating an
unauthenticated LiveKit ConfigMap. Bundled authentication also requires an
explicit existing Secret to avoid independently generated client/server passwords.

For external Redis, the existing `externalRedis.passwordSecretKey` and
`externalRedis.sentinelPasswordSecretKey` fields still select keys from Flow's
Secret. They also require a separately provisioned LiveKit configuration Secret.
Unauthenticated chart defaults remain unchanged.

`scripts/test_redis_auth.py` uses PyYAML 6.0.3 and Helm for offline checks of both
client Deployments, secret references, Sentinel URLs and missing-Secret refusal.
It does not provision credentials, validate Secret contents, connect to Redis,
or prove persistence, quorum availability or failover.
