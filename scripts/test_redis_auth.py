"""Offline Helm authentication contracts; no Redis credentials or cluster access."""
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml

CHART = Path(__file__).resolve().parents[1] / "deploy/helm/heterocloud-flow"


def render(values, error=None):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as override:
        json.dump(values, override)
        override.flush()
        result = subprocess.run(["helm", "template", "flow", str(CHART),
            "-f", str(CHART / "ci/test-values.yaml"), "-f", override.name],
            capture_output=True, text=True, timeout=45)
    if error:
        if result.returncode == 0 or error not in result.stderr:
            raise AssertionError("Expected Helm rejection: " + error)
        return []
    if result.returncode:
        raise AssertionError(result.stderr)
    return [d for d in yaml.safe_load_all(result.stdout) if d]


def clients(documents):
    selected = {}
    for d in documents:
        if d.get("kind") not in ("Deployment", "Job"):
            continue
        for c in d["spec"]["template"]["spec"]["containers"]:
            if c["name"] in ("api", "signaling", "migrate"):
                selected[c["name"]] = {e["name"]: e for e in c["env"]}
    if set(selected) != {"api", "signaling", "migrate"}:
        raise AssertionError("Expected API, signaling and migration Redis configuration")
    return selected.values()


class RedisAuthTests(unittest.TestCase):
    def bundled(self, sentinel=True):
        return {"redis": {"auth": {"enabled": True, "sentinel": sentinel,
                "existingSecret": "dev-redis-auth", "existingSecretPasswordKey": "password"}},
                "livekit": {"existingConfigSecret": "dev-livekit-config"}}

    def test_bundled_authenticated_sentinel(self):
        documents = render(self.bundled())
        for env in clients(documents):
            for key in ("REDIS_PASSWORD", "REDIS_SENTINEL_PASSWORD"):
                self.assertEqual(env[key]["valueFrom"]["secretKeyRef"],
                                 {"name": "dev-redis-auth", "key": "password"})
                self.assertNotIn("value", env[key])
            self.assertEqual(len(env["REDIS_SENTINEL_URLS"]["value"].split(",")), 3)
            self.assertNotIn("REDIS_URL", env)
        self.assertFalse(any(d["kind"] == "ConfigMap" and
                             d["metadata"]["name"].endswith("livekit-config") for d in documents))

    def test_sentinel_without_auth_does_not_receive_password(self):
        for env in clients(render(self.bundled(False))):
            self.assertIn("REDIS_PASSWORD", env)
            self.assertNotIn("REDIS_SENTINEL_PASSWORD", env)

    def test_unauthenticated_default_unchanged(self):
        for env in clients(render({})):
            self.assertNotIn("REDIS_PASSWORD", env)
            self.assertNotIn("REDIS_SENTINEL_PASSWORD", env)

    def test_existing_external_secret_contract(self):
        values = {"redis": {"enabled": False}, "externalRedis": {
            "sentinelUrls": ["redis://sentinel.example.invalid:26379"],
            "passwordSecretKey": "redis-password", "sentinelPasswordSecretKey": "sentinel-password"},
            "livekit": {"existingConfigSecret": "dev-livekit-config"}}
        for env in clients(render(values)):
            self.assertEqual(env["REDIS_PASSWORD"]["valueFrom"]["secretKeyRef"]["key"], "redis-password")
            self.assertEqual(env["REDIS_SENTINEL_PASSWORD"]["valueFrom"]["secretKeyRef"]["key"], "sentinel-password")
        values["livekit"]["existingConfigSecret"] = ""
        render(values, "authenticated Redis requires livekit.existingConfigSecret")

    def test_missing_bundled_secrets_rejected(self):
        values = self.bundled()
        values["redis"]["auth"]["existingSecret"] = ""
        render(values, "authenticated bundled Redis requires redis.auth.existingSecret")
        values = self.bundled()
        values["livekit"]["existingConfigSecret"] = ""
        render(values, "authenticated Redis requires livekit.existingConfigSecret")

    def test_external_direct_redis_includes_migration(self):
        values = {"redis": {"enabled": False}, "externalRedis": {
            "address": "redis://redis.example.invalid:6379", "sentinelUrls": [],
            "passwordSecretKey": "redis-password"},
            "livekit": {"existingConfigSecret": "dev-livekit-config"}}
        for env in clients(render(values)):
            self.assertEqual(env["REDIS_URL"]["value"], "redis://redis.example.invalid:6379")
            self.assertNotIn("REDIS_SENTINEL_URLS", env)
            self.assertEqual(env["REDIS_PASSWORD"]["valueFrom"]["secretKeyRef"]["key"], "redis-password")


if __name__ == "__main__":
    unittest.main()
