"""Emit promtool cases for the actual rendered storage alert expressions."""
import pathlib
import subprocess
import sys
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
output = subprocess.check_output([
    'helm', 'template', 'heterocloud-flow', str(ROOT / 'deploy/helm/heterocloud-flow'),
    '-n', 'heterocloud-flow', '-f', str(ROOT / 'deploy/environments/heteronet/values.yaml'),
    '--set', 'monitoring.resourceExporter.storageHealth.enabled=true',
], text=True)
objects = [value for value in yaml.safe_load_all(output) if value]
config = next(value for value in objects if value['kind'] == 'ConfigMap'
              and value['metadata']['name'].endswith('-prometheus'))
groups = yaml.safe_load(config['data']['rules.yml'])['groups']
rules = {rule['alert']: rule['expr'] for group in groups for rule in group['rules'] if 'alert' in rule}


def case(name, series, degraded, faulted, inventory, replica_inventory=None, replica_count=None,
         observation_unavailable=False):
    def result(labels, value):
        return [{'labels': labels, 'value': value}]
    expectations = {
        'HeteroCloudVolumeDegraded': result('{volume="disk"}', 1) if degraded else [],
        'HeteroCloudVolumeFaulted': result('{volume="disk"}', 1) if faulted else [],
        'HeteroCloudStorageInventoryUnavailable': result('{}', inventory) if inventory is not None else [],
        'HeteroCloudStorageReplicaInventoryUnavailable': result('{}', replica_inventory) if replica_inventory is not None else [],
        'HeteroCloudVolumeSingleReadWriteReplica': result('{volume="disk"}', replica_count) if replica_count is not None else [],
        'HeteroCloudVolumeReplicaObservationUnavailable': result('{volume="disk"}', 0) if observation_unavailable else [],
    }
    return {'name': name, 'interval': '1m', 'input_series': [
        {'series': labels, 'values': f'{value} {value}'} for labels, value in series
    ], 'promql_expr_test': [
        {'expr': rules[alert], 'eval_time': '1m', 'exp_samples': expected}
        for alert, expected in expectations.items()
    ]}


healthy = [('heterocloud_storage_inventory_success{instance="a"}', 1),
           ('heterocloud_storage_inventory_success{instance="b"}', 1),
           ('heterocloud_storage_replica_inventory_success{instance="a"}', 1),
           ('heterocloud_storage_replica_inventory_success{instance="b"}', 1)]
tests = [
    case('healthy', healthy + [('heterocloud_storage_volume_robustness{volume="disk",state="healthy"}', 1)], False, False, None),
    case('degraded-deduplicates-collectors', healthy + [
        ('heterocloud_storage_volume_robustness{volume="disk",state="degraded",instance="a"}', 1),
        ('heterocloud_storage_volume_robustness{volume="disk",state="degraded",instance="b"}', 1)], True, False, None),
    case('faulted', healthy + [('heterocloud_storage_volume_robustness{volume="disk",state="faulted"}', 1)], False, True, None),
    case('one-collector-failed', [healthy[0], healthy[2], ('heterocloud_storage_inventory_success{instance="b"}', 0)], False, False, None),
    case('all-collectors-failed', [('heterocloud_storage_inventory_success{instance="a"}', 0)], False, False, 0, 1),
    case('all-collectors-absent', [], False, False, 1, 1),
    case('single-readwrite-replica', healthy + [
        ('heterocloud_storage_volume_readwrite_replicas{volume="disk",instance="a"}', 1),
        ('heterocloud_storage_volume_readwrite_replicas{volume="disk",instance="b"}', 1)], False, False, None, replica_count=1),
    case('zero-readwrite-replicas', healthy + [('heterocloud_storage_volume_readwrite_replicas{volume="disk"}', 0)],
         False, False, None, replica_count=0),
    case('two-readwrite-replicas', healthy + [('heterocloud_storage_volume_readwrite_replicas{volume="disk"}', 2)],
         False, False, None),
    case('replica-observation-unavailable', healthy + [
        ('heterocloud_storage_volume_replica_observation_available{volume="disk"}', 0)],
         False, False, None, observation_unavailable=True),
    case('replica-inventory-failed', healthy[:2] + [('heterocloud_storage_replica_inventory_success{instance="a"}', 0)],
         False, False, None, replica_inventory=0),
]
yaml.safe_dump({'rule_files': [], 'evaluation_interval': '1m', 'tests': tests}, sys.stdout, sort_keys=False)
