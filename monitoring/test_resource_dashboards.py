"""Contract tests for the rendered resource dashboards and read-only collector."""
import json
import pathlib
import subprocess
import unittest
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
CHART = ROOT / 'deploy/helm/heterocloud-flow'


class DashboardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        output = subprocess.check_output(['helm', 'template', 'heterocloud-flow', str(CHART), '-n', 'heterocloud-flow',
                                          '-f', str(ROOT / 'deploy/environments/heteronet/values.yaml')], text=True)
        cls.objects = [o for o in yaml.safe_load_all(output) if o]
        cls.dashboards = {key: json.loads(value) for obj in cls.objects if obj['kind'] == 'ConfigMap'
                          for key, value in obj.get('data', {}).items() if key.endswith('.json')}

    def test_six_pies_and_filtered_service_drilldown(self):
        dashboard = self.dashboards['cluster-resources.json']
        pies = [p for p in dashboard['panels'] if p['type'] == 'piechart']
        self.assertEqual(len(pies), 6)
        self.assertEqual(len({p['id'] for p in dashboard['panels']}), len(dashboard['panels']))
        for p in pies:
            self.assertEqual(p['options']['tooltip']['mode'], 'single')
            self.assertTrue(p['targets'][0]['instant'])
            if 'ユーザー' in p['title']:
                self.assertNotIn('links', p['fieldConfig']['defaults'])
                user_override = next(x for x in p['fieldConfig']['overrides'] if x['matcher'] == {'id': 'byFrameRefID', 'options': 'A'})
                links = next(x['value'] for x in user_override['properties'] if x['id'] == 'links')
                self.assertEqual(len(links), 1)
                self.assertIn('var-user=${__field.labels.user_id}', links[0]['url'])
                self.assertEqual([t['refId'] for t in p['targets']], ['A', 'B', 'C'])
                self.assertEqual([t['legendFormat'] for t in p['targets'][1:]], ['システム・未帰属', '未使用'])
                for override in p['fieldConfig']['overrides'][1:]:
                    self.assertIn({'id': 'links', 'value': []}, override['properties'])
        detail = self.dashboards['user-resources.json']
        for p in detail['panels']:
            for t in p.get('targets', []):
                self.assertIn('user_id=~"${user:regex}"', t['expr'])
        self.assertTrue(any(p['type'] == 'table' and p['fieldConfig']['defaults']['noValue'] == '未取得' for p in detail['panels']))

    def test_ha_collectors_deduplicated_and_no_missing_to_zero(self):
        for key in ('cluster-resources.json', 'user-resources.json'):
            for panel in self.dashboards[key]['panels']:
                for target in panel.get('targets', []):
                    self.assertTrue('max by' in target['expr'] or 'min by' in target['expr'])
                    self.assertNotIn('or vector(0)', target['expr'])

    def test_node_charts_exclude_dedicated_masters(self):
        for panel in self.dashboards['cluster-resources.json']['panels']:
            if panel['id'] == 22:  # Missing service disk measurements.
                continue
            for target in panel.get('targets', []):
                self.assertIn('heterocloud_node_workload_pool', target['expr'])

    def test_private_exporter_and_minimal_permissions(self):
        deployment = next(o for o in self.objects if o['kind'] == 'Deployment' and o['metadata']['name'].endswith('-resources'))
        spec = deployment['spec']['template']['spec']
        self.assertFalse(spec.get('hostNetwork', False))
        self.assertTrue(spec['containers'][0]['securityContext']['readOnlyRootFilesystem'])
        self.assertIn('@sha256:', spec['containers'][0]['image'])
        services = [o for o in self.objects if o['kind'] == 'Service' and o['metadata']['name'].endswith('-resources')]
        self.assertEqual(services[0]['spec']['type'], 'ClusterIP')
        for obj in self.objects:
            if obj['kind'] in ('Role', 'ClusterRole') and '-resources' in obj['metadata']['name']:
                for rule in obj['rules']:
                    self.assertLessEqual(set(rule['verbs']), {'get', 'list'})
                    self.assertNotIn('secrets', rule['resources'])
                    self.assertNotIn('pods/exec', rule['resources'])

    def test_legacy_dashboard_preserved(self):
        self.assertEqual(len(self.dashboards['heterocloud-flow.json']['panels']), 13)


if __name__ == '__main__':
    unittest.main()
