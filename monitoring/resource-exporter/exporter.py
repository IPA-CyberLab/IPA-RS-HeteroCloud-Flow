"""Read-only cluster resource inventory. Never exports workload configuration."""
import concurrent.futures
import json
import logging
import os
import ssl
import threading
import time
import urllib.request
from collections import defaultdict
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

INSTANCE = 'flash.heterocloud.io/instance'
NAMESPACE = os.getenv('WORKLOAD_NAMESPACE', 'heterocloud-flash-workloads')
TOKEN_PATH = '/var/run/secrets/kubernetes.io/serviceaccount/token'
CA_PATH = '/var/run/secrets/kubernetes.io/serviceaccount/ca.crt'
API = os.getenv('KUBERNETES_API_URL', 'https://kubernetes.default.svc')
INTERVAL = int(os.getenv('COLLECTION_INTERVAL', '30'))
MAX_AGE = int(os.getenv('MAX_SAMPLE_AGE', '120'))


def quantity(value):
    value = str(value)
    for suffix, factor in [('Ki', 2**10), ('Mi', 2**20), ('Gi', 2**30), ('Ti', 2**40),
                           ('Pi', 2**50), ('K', 10**3), ('M', 10**6), ('G', 10**9),
                           ('T', 10**12), ('m', .001), ('u', .000001), ('n', .000000001)]:
        if value.endswith(suffix):
            return float(Decimal(value[:-len(suffix)]) * Decimal(str(factor)))
    return float(value)


def owners(rows):
    """Follow authenticated task roles, never tenant-supplied owner metadata.

    Ambiguous shared roles and cycles remain service-account attribution.
    """
    parents = defaultdict(list)
    for row in rows:
        if row['task_role_id']:
            parents[(row['organization_id'], row['task_role_id'])].append(row)

    def resolve(row, visited):
        sid = row['service_id']
        if row['creator_user_id']:
            return str(row['creator_user_id']), row['creator_user_name'], 'creator'
        principal = row['creator_principal_id']
        fallback = ('sa:' + str(principal), 'SA: ' + row['creator_name'], 'service_account') if principal else (
            'unattributed:' + row['organization_id'], '未帰属 / ' + row['organization_name'], 'unattributed')
        if sid in visited or len(visited) >= 16:
            return fallback
        roots = [resolve(parent, visited | {sid}) for parent in parents[(row['organization_id'], principal)]]
        if roots and len({root[0] for root in roots}) == 1 and all(root[2] in ('creator', 'task_role') for root in roots):
            return roots[0][0], roots[0][1], 'task_role'
        return fallback

    return {row['service_id']: resolve(row, set()) for row in rows}


def label_value(value):
    # Prometheus text format supports only backslash, quote and newline escapes.
    # Other control characters are normalized instead of emitting JSON \t / \r.
    value = ''.join(c if ord(c) >= 32 and ord(c) != 127 or c == '\n' else ' ' for c in str(value))
    return '"' + value.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n') + '"'


class Metrics:
    def __init__(self):
        self.lines = []
        self.names = set()

    def add(self, name, labels, value):
        if name not in self.names:
            self.lines += [f'# HELP {name} Current {name.removeprefix("heterocloud_").replace("_", " ")}.', f'# TYPE {name} gauge']
            self.names.add(name)
        escaped = ','.join(f'{key}={label_value(val)}' for key, val in sorted(labels.items()))
        self.lines.append(f'{name}{{{escaped}}} {float(value):.12g}')

    def render(self):
        return '\n'.join(self.lines) + '\n'


def workload_pool_node(node):
    """Configured Flash pool, not a transient Ready/cordon availability signal.

    A control-plane role alone is not a reason to exclude a mixed-role worker.
    Dedicated control-plane taints do exclude it, even if a stale ready label
    remains. Runtime readiness labels are maintained by HeteroNetwork setup.
    """
    labels = node['metadata'].get('labels', {})
    runtime_ready = (labels.get('flash.heterocloud.io/gvisor-ready') == 'true' or
                     (labels.get('flash.heterocloud.io/gpu-ready') == 'true' and
                      labels.get('nvidia.com/gpu.present') == 'true'))
    dedicated = {'heteronetwork.io/control-plane-only',
                 'node-role.kubernetes.io/control-plane', 'node-role.kubernetes.io/master'}
    return runtime_ready and not any(t.get('key') in dedicated and t.get('effect') in ('NoSchedule', 'NoExecute')
                                     for t in node.get('spec', {}).get('taints', []))


def aggregate(rows, nodes, pods, claims, summaries, now=None):
    """Build a fresh snapshot: deleted objects and failed measurements disappear."""
    now = time.time() if now is None else now
    metrics = Metrics()
    attribution = owners(rows)
    service_rows = {row['service_id']: row for row in rows}
    active_pods = {p['metadata']['uid']: p for p in pods if p.get('status', {}).get('phase') not in ('Failed', 'Succeeded')}
    observed_pods, volumes = {}, {}
    for node in nodes:
        name = node['metadata']['name']
        metrics.add('heterocloud_node_workload_pool', {'node': name}, workload_pool_node(node))
        capacity = node['status']['capacity']
        metrics.add('heterocloud_node_cpu_capacity_cores', {'node': name}, quantity(capacity['cpu']))
        metrics.add('heterocloud_node_memory_capacity_bytes', {'node': name}, quantity(capacity['memory']))
        summary = summaries.get(name)
        metrics.add('heterocloud_resource_node_scrape_success', {'node': name}, summary is not None)
        if summary is None:
            continue
        stats = summary['node']
        for path, metric, divisor in [('cpu.usageNanoCores', 'cpu_usage_cores', 1e9),
                                      ('memory.workingSetBytes', 'memory_working_set_bytes', 1),
                                      ('fs.capacityBytes', 'disk_capacity_bytes', 1),
                                      ('fs.usedBytes', 'disk_usage_bytes', 1)]:
            section, key = path.split('.')
            value = stats.get(section, {}).get(key)
            if value is not None:
                metrics.add('heterocloud_node_' + metric, {'node': name}, value / divisor)
        for pod_stats in summary.get('pods', []):
            if pod_stats['podRef'].get('namespace') != NAMESPACE:
                continue
            uid = pod_stats['podRef']['uid']
            if uid not in active_pods:
                continue
            observed_pods[uid] = pod_stats
            for volume in pod_stats.get('volume', []):
                ref = volume.get('pvcRef', {})
                if ref.get('namespace') == NAMESPACE and 'usedBytes' in volume:
                    key = ref['name']
                    volumes[key] = max(volumes.get(key, 0), volume['usedBytes'])
    by_service = defaultdict(list)
    claim_owners = defaultdict(set)
    for pod in active_pods.values():
        sid = pod['metadata'].get('labels', {}).get(INSTANCE)
        if sid not in service_rows:
            continue
        by_service[sid].append(pod)
        for volume in pod.get('spec', {}).get('volumes', []):
            claim = volume.get('persistentVolumeClaim', {}).get('claimName')
            if claim:
                claim_owners[claim].add(sid)
    claims_by_service = defaultdict(list)
    for claim in claims:
        name = claim['metadata']['name']
        sid = claim['metadata'].get('labels', {}).get(INSTANCE)
        # An unlabelled admin claim is attributed only if exactly one service uses it.
        if not sid and len(claim_owners[name]) == 1:
            sid = next(iter(claim_owners[name]))
        if sid in service_rows:
            claims_by_service[sid].append(claim)
    for sid, row in service_rows.items():
        user_id, user_name, source = attribution[sid]
        labels = dict(service_id=sid, service_name=row['service_name'], organization_id=row['organization_id'],
                      user_id=user_id, user_name=user_name)
        metrics.add('heterocloud_service_info', {**labels, 'attribution': source}, 1)
        usage = dict(cpu=0, memory=0, ephemeral_disk=0)
        complete = dict(cpu=True, memory=True, ephemeral_disk=True)
        for pod in by_service[sid]:
            if pod.get('status', {}).get('phase') != 'Running':
                # Pending Pods with running containers can consume resources too.
                if not any('running' in s.get('state', {}) for s in pod.get('status', {}).get('containerStatuses', [])):
                    continue
            stats = observed_pods.get(pod['metadata']['uid'], {})
            for resource, section, key, divisor in [('cpu', 'cpu', 'usageNanoCores', 1e9),
                                                   ('memory', 'memory', 'workingSetBytes', 1),
                                                   ('ephemeral_disk', 'ephemeral-storage', 'usedBytes', 1)]:
                value = stats.get(section, {}).get(key)
                if value is None:
                    complete[resource] = False
                else:
                    usage[resource] += value / divisor
        persistent, capacity, disk_complete = 0, 0, complete['ephemeral_disk']
        for claim in claims_by_service[sid]:
            name = claim['metadata']['name']
            capacity += quantity(claim.get('status', {}).get('capacity', {}).get('storage',
                                 claim['spec']['resources']['requests']['storage']))
            if name in volumes:
                persistent += volumes[name]
            else:
                disk_complete = False
        for resource, suffix in [('cpu', 'cpu_usage_cores'), ('memory', 'memory_working_set_bytes'),
                                 ('ephemeral_disk', 'ephemeral_disk_usage_bytes')]:
            if complete[resource]:
                metrics.add('heterocloud_service_' + suffix, labels, usage[resource])
        metrics.add('heterocloud_service_disk_observed_bytes', labels, persistent + usage['ephemeral_disk'])
        metrics.add('heterocloud_service_persistent_disk_capacity_bytes', labels, capacity)
        if disk_complete:
            metrics.add('heterocloud_service_disk_usage_bytes', labels, persistent + usage['ephemeral_disk'])
        for resource, available in [('cpu', complete['cpu']), ('memory', complete['memory']), ('disk', disk_complete)]:
            metrics.add('heterocloud_service_resource_available', {**labels, 'resource': resource}, available)
    metrics.add('heterocloud_resource_snapshot_timestamp_seconds', {}, now)
    return metrics.render()


def kget(path):
    request = urllib.request.Request(API + path, headers={'Authorization': 'Bearer ' + Path(TOKEN_PATH).read_text().strip()})
    with urllib.request.urlopen(request, context=ssl.create_default_context(cafile=CA_PATH), timeout=12) as response:
        return json.load(response)


def inventory():
    import psycopg
    from psycopg.rows import dict_row
    # The DB role can SELECT this projection only, never secrets, tokens or spec.
    with psycopg.connect(os.environ['DATABASE_URL'].strip(), connect_timeout=5,
                         options='-c default_transaction_read_only=on -c statement_timeout=10000', row_factory=dict_row) as connection:
        return connection.execute('SELECT * FROM monitoring.resource_services').fetchall()


def collect():
    rows = inventory()
    nodes = kget('/api/v1/nodes')['items']
    pods = kget('/api/v1/namespaces/' + NAMESPACE + '/pods')['items']
    claims = kget('/api/v1/namespaces/' + NAMESPACE + '/persistentvolumeclaims')['items']
    summaries = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(kget, '/api/v1/nodes/' + n['metadata']['name'] + '/proxy/stats/summary'): n['metadata']['name'] for n in nodes}
        for future in concurrent.futures.as_completed(futures):
            name = futures[future]
            try:
                summaries[name] = future.result()
            except Exception:
                logging.warning('Node resource sample unavailable: %s', name)
    return aggregate(rows, nodes, pods, claims, summaries)


class Snapshot:
    def __init__(self):
        self.lock = threading.Lock()
        self.body, self.updated, self.success = '', 0, False

    def refresh(self):
        while True:
            started = time.monotonic()
            try:
                body = collect()
                with self.lock:
                    self.body, self.updated, self.success = body, time.time(), True
            except Exception as error:
                # Do not log exception text: connection exceptions can contain credentials.
                logging.error('Resource collection failed: %s', type(error).__name__)
                with self.lock:
                    self.success = False
            time.sleep(max(1, INTERVAL - (time.monotonic() - started)))

    def read(self):
        with self.lock:
            fresh = self.success and time.time() - self.updated <= MAX_AGE
            return ('# HELP heterocloud_resource_exporter_success Whether the latest complete inventory snapshot is fresh.\n# TYPE heterocloud_resource_exporter_success gauge\nheterocloud_resource_exporter_success ' + str(int(fresh)) + '\n' + (self.body if fresh else '')), fresh


def main():
    logging.basicConfig(level=logging.INFO)
    state = Snapshot()
    threading.Thread(target=state.refresh, daemon=True).start()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            body, ready = state.read()
            if self.path not in ('/metrics', '/healthz', '/readyz'):
                self.send_error(404)
                return
            self.send_response(503 if self.path == '/readyz' and not ready else 200)
            self.send_header('Content-Type', 'text/plain; version=0.0.4; charset=utf-8')
            self.end_headers()
            self.wfile.write(body.encode() if self.path == '/metrics' else b'ok\n')

        def log_message(self, *_):
            pass

    ThreadingHTTPServer(('0.0.0.0', 9790), Handler).serve_forever()


if __name__ == '__main__':
    main()
