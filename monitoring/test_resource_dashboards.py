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
                links = p['fieldConfig']['defaults']['links']
                self.assertEqual(len(links), 1)
                self.assertIn('var-user=${__field.labels.user_id}', links[0]['url'])
        detail = self.dashboards['user-resources.json']
        for p in detail['panels']:
            for t in p.get('targets', []):
                self.assertIn('user_id=~"${user:regex}"', t['expr'])
        self.assertTrue(any(p['type'] == 'table' and p['fieldConfig']['defaults']['noValue'] == '未取得' for p in detail['panels']))

    def test_ha_collectors_deduplicated_and_no_missing_to_zero(self):
        for key in ('cluster-resources.json', 'user-resources.json'):
            for panel in self.dashboards[key]['panels']:
                for target in panel.get('targets', []):
                    self.assertIn('max by', target['expr'])
                    self.assertNotIn('or vector(0)', target['expr'])

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
