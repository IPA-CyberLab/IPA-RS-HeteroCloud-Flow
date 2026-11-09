"""Check the rendered node discovery policy and emit promtool cases."""
import pathlib
import subprocess
import sys

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[1]
rendered = subprocess.check_output([
    "helm", "template", "heterocloud-flow", str(ROOT / "deploy/helm/heterocloud-flow"),
    "-n", "heterocloud-flow", "-f", str(ROOT / "deploy/environments/heteronet/values.yaml"),
], text=True)
configmap = next(item for item in yaml.safe_load_all(rendered)
                 if item and item.get("kind") == "ConfigMap"
                 and item["metadata"]["name"].endswith("-prometheus"))
config = yaml.safe_load(configmap["data"]["prometheus.yml"])
scrapes = {item["job_name"]: item for item in config["scrape_configs"]}
for name in ("kubernetes-nodes-resource", "kubernetes-nodes-cadvisor", "heteronetwork-agents"):
    assert {"source_labels": ["__meta_kubernetes_node_label_heteronetwork_io_lifecycle"],
            "action": "drop", "regex": "stopped"} in scrapes[name]["relabel_configs"]
groups = yaml.safe_load(configmap["data"]["rules.yml"])["groups"]
rules = {rule["alert"]: rule["expr"] for group in groups for rule in group["rules"] if "alert" in rule}

series = [
    {"series": 'container_network_receive_errors_total{job="kubernetes-nodes-cadvisor",id="/",node="node-a",interface="ens2f0"}',
     "values": "0+120x10"},
    {"series": 'container_network_receive_packets_dropped_total{job="kubernetes-nodes-cadvisor",id="/",node="node-b",interface="enp6s0"}',
     "values": "0+120x10"},
    # Loopback and low-rate counters must not trigger.
    {"series": 'container_network_receive_errors_total{job="kubernetes-nodes-cadvisor",id="/",node="node-a",interface="lo"}',
     "values": "0+600x10"},
    {"series": 'container_network_receive_errors_total{job="kubernetes-nodes-cadvisor",id="/",node="node-b",interface="enp6s0"}',
     "values": "0+30x10"},
    {"series": 'up{job="kubernetes-nodes-cadvisor",node="node-a"}', "values": "0x10"},
    {"series": 'up{job="kubernetes-nodes-cadvisor",node="node-b"}', "values": "1x10"},
]
tests = [{"name": "sustained-node-receive-failures-and-missing-scrape", "interval": "1m",
          "input_series": series, "promql_expr_test": [
              {"expr": rules["HeteroNodeNetworkReceiveErrors"], "eval_time": "6m",
               "exp_samples": [{"labels": '{interface="ens2f0",node="node-a"}', "value": 2}]},
              {"expr": rules["HeteroNodeNetworkReceiveDrops"], "eval_time": "6m",
               "exp_samples": [{"labels": '{interface="enp6s0",node="node-b"}', "value": 2}]},
              {"expr": rules["HeteroNodeMetricsUnavailable"], "eval_time": "6m",
               "exp_samples": [{"labels": '{job="kubernetes-nodes-cadvisor",node="node-a"}', "value": 0}]},
          ]}]
yaml.safe_dump({"rule_files": [], "evaluation_interval": "1m", "tests": tests}, sys.stdout, sort_keys=False)
