"""Test the rendered media-target alert, including total target disappearance."""
import pathlib
import subprocess
import sys

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
rendered = subprocess.check_output([
    'helm', 'template', 'heterocloud-flow', str(ROOT / 'deploy/helm/heterocloud-flow'),
    '-n', 'heterocloud-flow', '-f', str(ROOT / 'deploy/environments/heteronet/values.yaml'),
], text=True)
objects = [value for value in yaml.safe_load_all(rendered) if value]
config = next(value for value in objects if value['kind'] == 'ConfigMap'
              and value['metadata']['name'].endswith('-prometheus'))
groups = yaml.safe_load(config['data']['rules.yml'])['groups']
expression = next(rule['expr'] for group in groups for rule in group['rules']
                  if rule.get('alert') == 'FlowMetricsTargetMissing')


def case(name, coturn, livekit, expected):
    series = []
    for job, values in [('flow-coturn', coturn), ('flow-livekit', livekit)]:
        series.extend({'series': f'up{{job="{job}",instance="{index}"}}',
                       'values': f'{value} {value}'} for index, value in enumerate(values))
    return {'name': name, 'interval': '1m', 'input_series': series, 'promql_expr_test': [{
        'expr': expression, 'eval_time': '1m',
        'exp_samples': [] if expected is None else [{'labels': '{}', 'value': expected}],
    }]}


tests = [
    case('all targets healthy', [1] * 4, [1] * 7, None),
    case('one TURN target down', [1, 1, 1, 0], [1] * 7, 3),
    case('all TURN targets disappeared', [], [1] * 7, 0),
    case('all LiveKit targets disappeared', [1] * 4, [], 0),
    case('all media targets disappeared', [], [], 0),
]
yaml.safe_dump({'rule_files': [], 'evaluation_interval': '1m', 'tests': tests}, sys.stdout, sort_keys=False)
