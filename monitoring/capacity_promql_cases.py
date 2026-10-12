"""Emit promtool cases against the actual provisioned dashboard expressions."""
import json
import pathlib
import sys
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
PANELS = {p['id']: p for p in json.loads((ROOT / 'deploy/helm/heterocloud-flow/files/cluster-resources.json').read_text())['panels']}
RESOURCES = [
    (11, 'cpu_capacity_cores', 'cpu_usage_cores', 'cpu_usage_cores', 1),
    (13, 'memory_capacity_bytes', 'memory_working_set_bytes', 'memory_working_set_bytes', 10**9),
    (15, 'disk_capacity_bytes', 'disk_usage_bytes', 'disk_observed_bytes', 10**9),
]


def case(panel_id, capacity, host, service, scale, scenario):
    series = []

    def add(metric, labels, value):
        # Both HA collectors report the same objects; this must not double usage.
        for replica in ('a', 'b'):
            parts = [f'{k}={json.dumps(v)}' for k, v in sorted({**labels, 'instance': replica}.items())]
            series.append({'series': metric + '{' + ','.join(parts) + '}', 'values': f'{value} {value}'})

    for name, pool in [('worker-a', 1), ('worker-b', 1), ('dedicated-master', 0)]:
        labels = {'node': name}
        add('heterocloud_node_workload_pool', labels, pool)
        if not (scenario == 'missing-capacity' and name == 'worker-b'):
            add('heterocloud_node_' + capacity, labels, (500 if pool else 5000) * scale)
        if scenario == 'missing-usage' and name == 'worker-b':
            continue
        used = 600 if scenario == 'host-above-capacity' else (25 if scenario == 'host-below-users' else 125)
        add('heterocloud_node_' + host, labels, (used if pool else 2000) * scale)
    if scenario != 'no-users':
        for sid, value in [('svc-a', 25), ('svc-b', 75)]:
            add('heterocloud_service_' + service,
                dict(service_id=sid, service_name=sid, organization_id='org', user_id='u1', user_name='Alice'),
                (600 if scenario == 'users-above-capacity' else value) * scale)
    valid = scenario not in ('missing-usage', 'missing-capacity', 'users-above-capacity', 'host-above-capacity')
    user = [] if not valid or scenario == 'no-users' else [{'labels': '{user_id="u1",user_name="Alice"}', 'value': 100 * scale}]
    system = 0 if scenario == 'host-below-users' else (250 if scenario == 'no-users' else 150)
    free = 900 if scenario == 'host-below-users' else 750
    expected = {'A': user, 'B': [{'labels': '{}', 'value': system * scale}] if valid else [],
                'C': [{'labels': '{}', 'value': free * scale}] if valid else []}
    checks = [{'expr': target['expr'], 'eval_time': '1m', 'exp_samples': expected[target['refId']]}
              for target in PANELS[panel_id]['targets']]
    # A user's 100 GB out of 1 TB must be 10%, including both unused capacity
    # and platform use, regardless of master-only capacity and HA replicas.
    if valid:
        targets = {t['refId']: t['expr'] for t in PANELS[panel_id]['targets']}
        total = f'(sum({targets["A"]}) or vector(0)) + ({targets["B"]}) + ({targets["C"]})'
        checks.append({'expr': total, 'eval_time': '1m', 'exp_samples': [{'labels': '{}', 'value': 1000 * scale}]})
        if scenario != 'no-users':
            checks.append({'expr': f'sum({targets["A"]}) / ({total})', 'eval_time': '1m',
                           'exp_samples': [{'labels': '{}', 'value': 0.1}]})
    return {'name': f'{capacity}/{scenario}', 'interval': '1m', 'input_series': series, 'promql_expr_test': checks}


def main():
    tests = [case(*resource, scenario) for resource in RESOURCES for scenario in
             ('normal', 'host-below-users', 'no-users', 'missing-usage', 'missing-capacity',
              'users-above-capacity', 'host-above-capacity')]
    yaml.safe_dump({'rule_files': [], 'evaluation_interval': '1m', 'tests': tests}, sys.stdout, sort_keys=False)


if __name__ == '__main__':
    main()
