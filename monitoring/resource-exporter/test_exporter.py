import copy
import time
import unittest
from unittest.mock import patch
import exporter as e


def row(sid='s1', **kwargs):
    return dict(service_id=sid, service_name='Service "test"', organization_id='org1', organization_name='Team',
                creator_principal_id='p1', creator_name='Alice', creator_user_id='u1', creator_user_name='Alice', task_role_id=None, **kwargs)


def sample():
    nodes = [{'metadata': {'name': 'n1'}, 'status': {'capacity': {'cpu': '8', 'memory': '16Gi'}}}]
    pods = [{'metadata': {'uid': 'pod1', 'labels': {e.INSTANCE: 's1'}}, 'status': {'phase': 'Running'}}]
    claims = [{'metadata': {'name': 'disk1', 'labels': {e.INSTANCE: 's1'}}, 'spec': {'resources': {'requests': {'storage': '10Gi'}}}}]
    stats = {'podRef': {'uid': 'pod1', 'namespace': e.NAMESPACE}, 'cpu': {'usageNanoCores': 1500000000},
             'memory': {'workingSetBytes': 4096}, 'ephemeral-storage': {'usedBytes': 1024},
             'volume': [{'pvcRef': {'name': 'disk1', 'namespace': e.NAMESPACE}, 'usedBytes': 2048}]}
    summaries = {'n1': {'node': {'cpu': {'usageNanoCores': 2000000000}, 'fs': {'capacityBytes': 10000, 'usedBytes': 6000}}, 'pods': [stats]}}
    return [row()], nodes, pods, claims, summaries


def values(text, metric):
    return [float(line.rsplit(' ', 1)[1]) for line in text.splitlines() if line.startswith(metric + '{')]


class ResourceTests(unittest.TestCase):
    def test_actual_usage_not_resource_limits(self):
        args = sample()
        args[2][0]['spec'] = {'containers': [{'resources': {'limits': {'memory': '48Gi'}}}]}
        result = e.aggregate(*args)
        self.assertEqual(values(result, 'heterocloud_service_memory_working_set_bytes'), [4096])
        self.assertEqual(values(result, 'heterocloud_service_cpu_usage_cores'), [1.5])
        self.assertEqual(values(result, 'heterocloud_service_disk_usage_bytes'), [3072])

    def test_replicas_sum_cpu_but_shared_pvc_counted_once(self):
        args = sample()
        pod = copy.deepcopy(args[2][0]); pod['metadata']['uid'] = 'pod2'; args[2].append(pod)
        stats = copy.deepcopy(args[4]['n1']['pods'][0]); stats['podRef']['uid'] = 'pod2'; args[4]['n1']['pods'].append(stats)
        result = e.aggregate(*args)
        self.assertEqual(values(result, 'heterocloud_service_cpu_usage_cores'), [3])
        self.assertEqual(values(result, 'heterocloud_service_disk_usage_bytes'), [4096])

    def test_missing_node_metrics_are_not_zero(self):
        args = sample(); args[4].clear()
        result = e.aggregate(*args)
        self.assertEqual(values(result, 'heterocloud_service_memory_working_set_bytes'), [])
        self.assertEqual(values(result, 'heterocloud_resource_node_scrape_success'), [0])
        self.assertIn('resource="disk"', result)

    def test_stopped_service_disk_unknown_capacity_preserved(self):
        args = sample(); args[2].clear()
        result = e.aggregate(*args)
        self.assertEqual(values(result, 'heterocloud_service_cpu_usage_cores'), [0])
        self.assertEqual(values(result, 'heterocloud_service_disk_usage_bytes'), [])
        self.assertEqual(values(result, 'heterocloud_service_persistent_disk_capacity_bytes'), [10 * 2**30])

    def test_deleted_service_and_old_pod_uid_not_resurrected(self):
        args = sample(); args[0].clear()
        self.assertEqual(values(e.aggregate(*args), 'heterocloud_service_info'), [])
        args = sample(); args[4]['n1']['pods'][0]['podRef']['uid'] = 'old-pod'
        self.assertEqual(values(e.aggregate(*args), 'heterocloud_service_cpu_usage_cores'), [])

    def test_deleted_node_not_emitted(self):
        args = sample(); args[1].clear()
        self.assertEqual(values(e.aggregate(*args), 'heterocloud_node_disk_capacity_bytes'), [])

    def test_terminated_pod_not_counted(self):
        args = sample(); args[2][0]['status']['phase'] = 'Failed'
        self.assertEqual(values(e.aggregate(*args), 'heterocloud_service_cpu_usage_cores'), [0])

    def test_labels_escaped(self):
        result = e.aggregate(*sample())
        self.assertIn('service_name="Service \\"test\\""', result)

    def test_task_role_inherits_parent_user_not_declared_metadata(self):
        parent = row('parent'); parent['task_role_id'] = 'role1'
        child = row('child'); child.update(creator_user_id=None, creator_user_name=None, creator_principal_id='role1', creator_name='controller')
        self.assertEqual(e.owners([parent, child])['child'], ('u1', 'Alice', 'task_role'))

    def test_cross_org_and_ambiguous_roles_do_not_inherit(self):
        parent = row('parent'); parent['task_role_id'] = 'role1'
        child = row('child'); child.update(creator_user_id=None, creator_principal_id='role1', organization_id='org2')
        self.assertEqual(e.owners([parent, child])['child'][2], 'service_account')
        child['organization_id'] = 'org1'
        other = copy.deepcopy(parent); other.update(service_id='other', creator_user_id='u2')
        self.assertEqual(e.owners([parent, other, child])['child'][2], 'service_account')

    def test_role_cycle_is_bounded(self):
        a = row('a'); a.update(creator_user_id=None, task_role_id='r1', creator_principal_id='r2')
        b = row('b'); b.update(creator_user_id=None, task_role_id='r2', creator_principal_id='r1')
        self.assertEqual(e.owners([a, b])['a'][2], 'service_account')

    def test_failed_or_stale_snapshot_drops_old_samples(self):
        s = e.Snapshot(); s.body = 'old 5\n'; s.updated = time.time(); s.success = True
        self.assertTrue(s.read()[1])
        s.success = False
        self.assertNotIn('old', s.read()[0])
        s.success = True; s.updated -= e.MAX_AGE + 1
        self.assertNotIn('old', s.read()[0])

    def test_control_characters_do_not_break_prometheus_labels(self):
        value = e.label_value('Alice\tteam\rtest')
        self.assertEqual(value, '"Alice team test"')
        self.assertNotIn('\\t', value)
        self.assertEqual(e.label_value('a\nb'), '"a\\nb"')

    def test_dedicated_masters_not_in_workload_pool(self):
        node = {'metadata': {'labels': {'flash.heterocloud.io/gvisor-ready': 'true'}},
                'spec': {'taints': [{'key': 'heteronetwork.io/control-plane-only', 'effect': 'NoSchedule'}]}}
        self.assertFalse(e.workload_pool_node(node))
        node['spec']['taints'][0]['key'] = 'node-role.kubernetes.io/control-plane'
        self.assertFalse(e.workload_pool_node(node))

    def test_mixed_role_runtime_ready_worker_is_included(self):
        node = {'metadata': {'labels': {'flash.heterocloud.io/gvisor-ready': 'true',
                                        'node-role.kubernetes.io/control-plane': ''}}}
        self.assertTrue(e.workload_pool_node(node))
        node['metadata']['labels'].pop('flash.heterocloud.io/gvisor-ready')
        self.assertFalse(e.workload_pool_node(node))
        node['metadata']['labels'].update({'flash.heterocloud.io/gpu-ready': 'true', 'nvidia.com/gpu.present': 'true'})
        self.assertTrue(e.workload_pool_node(node))

    def test_transient_maintenance_does_not_change_capacity_denominator(self):
        node = {'metadata': {'labels': {'flash.heterocloud.io/gvisor-ready': 'true'}},
                'spec': {'unschedulable': True, 'taints': [{'key': 'node.kubernetes.io/not-ready', 'effect': 'NoSchedule'}]}}
        self.assertTrue(e.workload_pool_node(node))

    def test_quantity(self):
        self.assertEqual(e.quantity('500m'), .5)
        self.assertEqual(e.quantity('8Gi'), 8 * 2**30)


if __name__ == '__main__':
    unittest.main()
