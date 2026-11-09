#!/usr/bin/env python3
"""Exercise the rendered Grafana readiness probe against real kubelet endpoints.

The fixture keeps /api/health responsive while deliberately slowing /login.
Run only against a disposable cluster: this creates and deletes a namespace.
"""
import copy
import json
import subprocess
import time
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
NAMESPACE = "grafana-readiness-e2e"
SERVER = r'''
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import time
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/login" and Path("/tmp/slow").exists():
            # A 1.6-second backend passes the old two-second readiness probe
            # but leaves too little of a three-second VPN/browser budget.
            time.sleep(1.6)
        if self.path not in ("/api/health", "/login"):
            self.send_error(404)
            return
        try:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        except (BrokenPipeError, ConnectionResetError):
            pass
    def log_message(self, *args):
        pass
ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
'''


def kubectl(*args, input_text=None):
    return subprocess.run(
        ["kubectl", *args], input=input_text, text=True, check=True,
        stdout=subprocess.PIPE,
    ).stdout


def await_endpoints(expected, timeout=100):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = json.loads(kubectl(
            "-n", NAMESPACE, "get", "endpointslices", "-l",
            "kubernetes.io/service-name=grafana", "-o", "json",
        ))
        ready = {
            endpoint["targetRef"]["name"]
            for item in result["items"]
            for endpoint in item.get("endpoints", [])
            if endpoint.get("conditions", {}).get("ready") is True
        }
        if ready == expected:
            print("ready endpoints:", ", ".join(sorted(ready)), flush=True)
            return
        time.sleep(1)
    raise RuntimeError(f"expected ready endpoints {sorted(expected)}, got {sorted(ready)}")


def main():
    rendered = subprocess.check_output([
        "helm", "template", "heterocloud-flow", str(ROOT / "deploy/helm/heterocloud-flow"),
        "--namespace", "heterocloud-flow", "--values",
        str(ROOT / "deploy/environments/heteronet/values.yaml"),
        "--show-only", "templates/grafana.yaml",
    ], text=True)
    deployments = [d for d in yaml.safe_load_all(rendered) if d and d.get("kind") == "Deployment"]
    assert len(deployments) == 1
    container = deployments[0]["spec"]["template"]["spec"]["containers"][0]
    probe = copy.deepcopy(container["readinessProbe"])
    assert probe["httpGet"]["path"] == "/login"
    assert probe["timeoutSeconds"] == 1 and probe["failureThreshold"] == 3
    assert probe["successThreshold"] == 2
    assert container["livenessProbe"]["httpGet"]["path"] == "/api/health"
    assert container["startupProbe"]["httpGet"]["path"] == "/api/health"
    probe["httpGet"]["port"] = 8080

    resources = [
        {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NAMESPACE}},
        {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "server", "namespace": NAMESPACE},
         "data": {"server.py": SERVER}},
        {"apiVersion": "v1", "kind": "Service", "metadata": {"name": "grafana", "namespace": NAMESPACE},
         "spec": {"selector": {"app": "grafana-readiness"}, "ports": [{"port": 8080}]}},
    ]
    for name in ("healthy", "slow"):
        resources.append({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "namespace": NAMESPACE, "labels": {"app": "grafana-readiness"}},
            "spec": {"containers": [{
                "name": "server", "image": "python:3.13-alpine", "command": ["python", "/app/server.py"],
                "ports": [{"containerPort": 8080}], "readinessProbe": probe,
                "volumeMounts": [{"name": "server", "mountPath": "/app", "readOnly": True}],
            }], "volumes": [{"name": "server", "configMap": {"name": "server"}}]},
        })
    try:
        kubectl("apply", "-f", "-", input_text=json.dumps({"apiVersion": "v1", "kind": "List", "items": resources}))
        kubectl("-n", NAMESPACE, "wait", "--for=condition=Ready", "pod", "-l", "app=grafana-readiness", "--timeout=150s")
        await_endpoints({"healthy", "slow"})
        kubectl("-n", NAMESPACE, "exec", "slow", "--", "touch", "/tmp/slow")
        # The previous readiness probe remains healthy even while UI requests stall.
        kubectl("-n", NAMESPACE, "exec", "slow", "--", "python", "-c",
                "import urllib.request; assert urllib.request.urlopen('http://127.0.0.1:8080/api/health', timeout=1).status == 200")
        await_endpoints({"healthy"})
        # If the fast replica goes away, the recovered peer must be usable.
        kubectl("-n", NAMESPACE, "delete", "pod", "healthy", "--wait=true")
        await_endpoints(set())
        kubectl("-n", NAMESPACE, "exec", "slow", "--", "rm", "-f", "/tmp/slow")
        await_endpoints({"slow"})
    finally:
        subprocess.run(["kubectl", "delete", "namespace", NAMESPACE, "--ignore-not-found", "--wait=false"], check=False)


if __name__ == "__main__":
    main()
