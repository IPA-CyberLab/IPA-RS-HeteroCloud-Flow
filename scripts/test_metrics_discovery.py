"""Validate ready-Pod discovery instead of load-balanced or stale host scrapes."""

import pathlib
import subprocess
import unittest

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
CHART = ROOT / "deploy/helm/heterocloud-flow"


def render(*args):
    return list(yaml.safe_load_all(subprocess.check_output([
        "helm", "template", "heterocloud-flow", str(CHART),
        "-f", str(ROOT / "deploy/environments/heteronet/values.yaml"),
        *args,
    ], text=True)))


class MetricsDiscoveryTest(unittest.TestCase):
    def test_every_pool_and_ready_livekit_replica_is_discoverable(self):
        docs = render("--set-json", "coturn.metrics.urls=[]", "--set-json", "livekit.metrics.urls=[]")
        services = {doc["metadata"]["name"]: doc for doc in docs if doc and doc["kind"] == "Service"}
        deployment = next(doc for doc in docs if doc and doc["kind"] == "Deployment" and doc["metadata"]["name"] == "heterocloud-flow-api")
        env = {entry["name"]: entry.get("value") for entry in deployment["spec"]["template"]["spec"]["containers"][0]["env"]}
        self.assertEqual(env["COTURN_METRICS_URLS"], "")
        self.assertEqual(env["LIVEKIT_METRICS_URLS"], "")
        for component, suffix, port, pool in [
            ("coturn", "", 9641, "primary"),
            ("coturn", "-secondary", 9642, "secondary"),
            ("livekit", "", 6789, None),
        ]:
            name = f"heterocloud-flow-{component}{suffix}-metrics-discovery"
            spec = services[name]["spec"]
            self.assertEqual(spec["clusterIP"], "None")
            self.assertFalse(spec["publishNotReadyAddresses"])
            self.assertEqual(spec["selector"]["app.kubernetes.io/component"], component)
            self.assertEqual(spec["selector"].get("flow.heterocloud.io/turn-pool"), pool)
            self.assertEqual(spec["ports"][0]["port"], port)
            self.assertIn(f"http://{name}:{port}/metrics", env[f"{component.upper()}_METRICS_DISCOVERY_URLS"].split(","))

    def test_explicit_targets_preserve_static_scraping(self):
        docs = render("--set-json", 'coturn.metrics.urls=["http://192.0.2.1:9641/metrics"]')
        deployment = next(doc for doc in docs if doc and doc["kind"] == "Deployment" and doc["metadata"]["name"] == "heterocloud-flow-api")
        env = {entry["name"]: entry.get("value") for entry in deployment["spec"]["template"]["spec"]["containers"][0]["env"]}
        self.assertEqual(env["COTURN_METRICS_URLS"], "http://192.0.2.1:9641/metrics")
        self.assertEqual(env["COTURN_METRICS_DISCOVERY_URLS"], "")


if __name__ == "__main__":
    unittest.main()
