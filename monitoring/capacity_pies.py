"""Regenerate the dashboard PromQL for partitions of Flash worker capacity."""

POOL = '(min by (node) (heterocloud_node_workload_pool) == 1)'


def node(metric):
    return f'(max by (node) (heterocloud_node_{metric}) and on(node) {POOL})'


def queries(capacity_metric, node_usage_metric, service_usage_metric):
    capacity = f'sum({node(capacity_metric)})'
    host_used = f'sum({node(node_usage_metric)})'
    users = ('sum by (user_id,user_name) (max by '
             '(service_id,service_name,organization_id,user_id,user_name) '
             f'(heterocloud_service_{service_usage_metric}))')
    # Zero users is valid only when the collector has a current node inventory.
    user_total = f'(sum({users}) or on() (0 * {capacity}))'
    system = f'clamp_min({host_used} - {user_total}, 0)'
    unused = f'clamp_min({capacity} - {user_total} - {system}, 0)'
    inventory_count = f'count({POOL})'
    # Missing nodes must not masquerade as free capacity. Contradictory samples
    # above capacity are omitted rather than silently changing the denominator.
    valid = (f'({inventory_count} == count({node(capacity_metric)})) and on() '
             f'({inventory_count} == count({node(node_usage_metric)})) and on() '
             f'({capacity} > 0) and on() ({capacity} >= {host_used}) and on() '
             f'({capacity} >= {user_total})')
    return {'A': f'({users}) and on() ({valid})',
            'B': f'({system}) and on() ({valid})',
            'C': f'({unused}) and on() ({valid})'}


if __name__ == '__main__':
    import json
    from pathlib import Path
    path = Path(__file__).resolve().parents[1] / 'deploy/helm/heterocloud-flow/files/cluster-resources.json'
    dashboard = json.loads(path.read_text())
    panels = {p['id']: p for p in dashboard['panels']}
    for cap, used, physical, user, cm, hm, um in [
        (1, 2, 10, 11, 'cpu_capacity_cores', 'cpu_usage_cores', 'cpu_usage_cores'),
        (3, 4, 12, 13, 'memory_capacity_bytes', 'memory_working_set_bytes', 'memory_working_set_bytes'),
        (5, 6, 14, 15, 'disk_capacity_bytes', 'disk_usage_bytes', 'disk_observed_bytes'),
    ]:
        panels[cap]['targets'][0]['expr'] = f'sum({node(cm)})'
        panels[used]['targets'][0]['expr'] = f'sum({node(hm)})'
        panels[physical]['targets'][0]['expr'] = node(cm)
        exprs = queries(cm, hm, um)
        for target in panels[user]['targets']:
            target['expr'] = exprs[target['refId']]
    panels[20]['targets'][0]['expr'] = f'sum(max by (node) (heterocloud_resource_node_scrape_success) and on(node) {POOL})'
    panels[21]['targets'][0]['expr'] = f'count({POOL})'
    path.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2) + '\n')
