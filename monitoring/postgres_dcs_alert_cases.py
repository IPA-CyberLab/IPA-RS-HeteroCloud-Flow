"""Exercise the rendered PostgreSQL DCS health alert expressions with promtool."""
from pathlib import Path
import subprocess
import sys

import yaml


root = Path(__file__).resolve().parents[1]
rendered = subprocess.check_output([
    'helm', 'template', 'heterocloud-flow', str(root / 'deploy/helm/heterocloud-flow'),
    '-n', 'heterocloud-flow', '-f', str(root / 'deploy/environments/heteronet/values.yaml'),
], text=True)
configmap = next(item for item in yaml.safe_load_all(rendered)
                 if item and item.get('kind') == 'ConfigMap'
                 and item['metadata']['name'].endswith('-prometheus'))
config = yaml.safe_load(configmap['data']['prometheus.yml'])
job = next(x for x in config['scrape_configs'] if x['job_name'] == 'heterocloud-postgres-dcs')
assert set(job['static_configs'][0]['targets']) == {'10.250.0.2:19181', '10.250.0.10:19181'}
groups = yaml.safe_load(configmap['data']['rules.yml'])['groups']
rules = {r['alert']: r['expr'] for g in groups for r in g['rules'] if 'alert' in r}
tests = [{
    'name': 'unavailable-node-and-degraded-quorum', 'interval': '1m',
    'input_series': [
        {'series': 'up{job="heterocloud-postgres-dcs",instance="a"}', 'values': '1x10'},
        {'series': 'up{job="heterocloud-postgres-dcs",instance="b"}', 'values': '0x10'},
        {'series': 'heteronetwork_postgres_dcs_has_leader{job="heterocloud-postgres-dcs",instance="a"}', 'values': '0x10'},
        {'series': 'heteronetwork_postgres_dcs_has_leader{job="heterocloud-postgres-dcs",instance="b"}', 'values': '1x10'},
        {'series': 'heteronetwork_postgres_dcs_active_peers{job="heterocloud-postgres-dcs",instance="a"}', 'values': '1x10'},
        {'series': 'heteronetwork_postgres_dcs_active_peers{job="heterocloud-postgres-dcs",instance="b"}', 'values': '2x10'},
        {'series': 'heteronetwork_postgres_dcs_leader_changes_total{job="heterocloud-postgres-dcs",instance="a"}', 'values': '0+1x10'},
        {'series': 'heteronetwork_postgres_dcs_leader_changes_total{job="heterocloud-postgres-dcs",instance="b"}', 'values': '0x10'},
    ],
    'promql_expr_test': [
        {'expr': rules['HeteroPostgresDCSMetricsUnavailable'], 'eval_time': '6m',
         'exp_samples': [{'labels': '{}', 'value': 1}]},
        {'expr': rules['HeteroPostgresDCSLeaderMissing'], 'eval_time': '6m',
         'exp_samples': [{'labels': '{__name__="heteronetwork_postgres_dcs_has_leader",instance="a",job="heterocloud-postgres-dcs"}', 'value': 0}]},
        {'expr': rules['HeteroPostgresDCSPeerMissing'], 'eval_time': '6m',
         'exp_samples': [{'labels': '{__name__="heteronetwork_postgres_dcs_active_peers",instance="a",job="heterocloud-postgres-dcs"}', 'value': 1}]},
        {'expr': '(' + rules['HeteroPostgresDCSFrequentElections'] + ') > bool 0', 'eval_time': '6m',
         'exp_samples': [{'labels': '{instance="a",job="heterocloud-postgres-dcs"}', 'value': 1}]},
    ]
}, {
    'name': 'missing-all-targets', 'interval': '1m', 'input_series': [],
    'promql_expr_test': [
        {'expr': rules['HeteroPostgresDCSMetricsUnavailable'], 'eval_time': '1m',
         'exp_samples': [{'labels': '{}', 'value': 0}]},
    ]
}]
yaml.safe_dump({'rule_files': [], 'evaluation_interval': '1m', 'tests': tests}, sys.stdout, sort_keys=False)
