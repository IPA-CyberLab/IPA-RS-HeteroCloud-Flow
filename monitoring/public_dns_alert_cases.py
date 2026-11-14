"""Emit promtool cases for DNS quorum, stale status, and path latency alerts."""
import pathlib
import subprocess
import sys

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[1]
rendered = subprocess.check_output([
    'helm', 'template', 'heterocloud-flow', str(ROOT/'deploy/helm/heterocloud-flow'),
    '-n', 'heterocloud-flow', '-f', str(ROOT/'deploy/environments/heteronet/values.yaml'),
    '--set', 'monitoring.publicDnsQuorum.enabled=true',
], text=True)
configmap = next(item for item in yaml.safe_load_all(rendered)
                 if item and item.get('kind') == 'ConfigMap'
                 and item['metadata']['name'] == 'heterocloud-flow-prometheus')
groups = yaml.safe_load(configmap['data']['rules.yml'])['groups']
rules = {rule['alert']: rule['expr'] for group in groups for rule in group['rules'] if 'alert' in rule}

tests = [
    {'name':'quorum-loss-unknown-origin-and-slow-link', 'interval':'1m',
     'input_series':[
         {'series':'heteronetwork_public_dns_fresh_voters{instance="a"}', 'values':'1x10'},
         {'series':'heteronetwork_public_dns_majority_required{instance="a"}', 'values':'2x10'},
         {'series':'heteronetwork_public_dns_origin_decision{origin="east",decision="unknown"}', 'values':'1x10'},
         {'series':'heteronetwork_public_dns_probe_duration_seconds{voter="west",origin="east"}', 'values':'3.5x10'},
         {'series':'heteronetwork_public_dns_probe_healthy{voter="west",origin="east"}', 'values':'1x10'},
         {'series':'heteronetwork_overlay_dns_probe_attempts_total{voter="west",server="east"}', 'values':'0+12x10'},
         {'series':'heteronetwork_overlay_dns_probe_failures_total{voter="west",server="east"}', 'values':'0+3x10'},
     ], 'promql_expr_test':[
         {'expr':rules['HeteroPublicDNSQuorumUnavailable'], 'eval_time':'6m',
          'exp_samples':[{'labels':'{}','value':1}]},
         {'expr':rules['HeteroPublicDNSOriginUnknown'], 'eval_time':'6m',
          'exp_samples':[{'labels':'{origin="east"}','value':1}]},
         {'expr':rules['HeteroPublicDNSProbeSlow'], 'eval_time':'6m',
          'exp_samples':[{'labels':'{origin="east",voter="west"}','value':3.5}]},
         {'expr':rules['HeteroOverlayDNSProbeFailures'], 'eval_time':'6m',
          'exp_samples':[{'labels':'{server="east",voter="west"}','value':0.25}]},
     ]},
    {'name':'healthy-quorum-does-not-alert', 'interval':'1m',
     'input_series':[
         {'series':'heteronetwork_public_dns_fresh_voters', 'values':'2x10'},
         {'series':'heteronetwork_public_dns_majority_required', 'values':'2x10'},
         {'series':'heteronetwork_public_dns_origin_decision{origin="east",decision="unknown"}', 'values':'0x10'},
         {'series':'heteronetwork_public_dns_probe_duration_seconds{voter="west",origin="east"}', 'values':'0.3x10'},
         {'series':'heteronetwork_public_dns_probe_healthy{voter="west",origin="east"}', 'values':'1x10'},
         {'series':'heteronetwork_overlay_dns_probe_attempts_total{voter="west",server="east"}', 'values':'0+12x10'},
         {'series':'heteronetwork_overlay_dns_probe_failures_total{voter="west",server="east"}', 'values':'0x10'},
     ], 'promql_expr_test':[
         {'expr':rules['HeteroPublicDNSQuorumUnavailable'], 'eval_time':'6m', 'exp_samples':[]},
         {'expr':rules['HeteroPublicDNSOriginUnknown'], 'eval_time':'6m', 'exp_samples':[]},
         {'expr':rules['HeteroPublicDNSProbeSlow'], 'eval_time':'6m', 'exp_samples':[]},
         {'expr':rules['HeteroOverlayDNSProbeFailures'], 'eval_time':'6m', 'exp_samples':[]},
     ]},
    {'name':'missing-metrics-are-not-healthy', 'interval':'1m', 'input_series':[],
     'promql_expr_test':[
         {'expr':rules['HeteroPublicDNSMetricsUnavailable'], 'eval_time':'6m',
          'exp_samples':[{'labels':'{}','value':0}]},
         {'expr':rules['HeteroPublicDNSStatusStale'], 'eval_time':'6m',
          'exp_samples':[{'labels':'{namespace="heterocloud-dns",service="public-dns-quorum-metrics"}','value':1}]},
     ]},
]
for case in tests:
    for item in case['input_series']:
        common = 'namespace="heterocloud-dns",service="public-dns-quorum-metrics"'
        item['series'] = (item['series'].replace('{', '{'+common+',', 1) if '{' in item['series']
                          else item['series']+'{'+common+'}')
yaml.safe_dump({'rule_files':[], 'evaluation_interval':'1m', 'tests':tests}, sys.stdout, sort_keys=False)
