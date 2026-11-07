"""Prometheus pods must reload whenever rendered config or rules change."""
import pathlib
import shutil
import subprocess
import tempfile
import unittest

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
CHART = ROOT / 'deploy/helm/heterocloud-flow'
VALUES = ROOT / 'deploy/environments/heteronet/values.yaml'


def render(chart, *options):
    output = subprocess.check_output([
        'helm', 'template', 'heterocloud-flow', str(chart), '-n', 'heterocloud-flow',
        '-f', str(VALUES), '--show-only', 'templates/prometheus.yaml', *options,
    ], text=True)
    objects = [value for value in yaml.safe_load_all(output) if value]
    config = next(value for value in objects if value['kind'] == 'ConfigMap')
    deployment = next(value for value in objects if value['kind'] == 'Deployment')
    checksum = deployment['spec']['template']['metadata']['annotations']['checksum/config']
    return config['data'], checksum


class PrometheusReloadTests(unittest.TestCase):
    def test_replica_count_changes_rules_and_rollout_checksum(self):
        original, old_checksum = render(CHART)
        changed, new_checksum = render(CHART, '--set', 'livekit.replicaCount=6')
        self.assertNotEqual(original['rules.yml'], changed['rules.yml'])
        self.assertNotEqual(old_checksum, new_checksum)

    def test_template_rule_change_updates_checksum(self):
        original, old_checksum = render(CHART)
        with tempfile.TemporaryDirectory() as temporary:
            chart = pathlib.Path(temporary) / 'chart'
            shutil.copytree(CHART, chart)
            path = chart / 'templates/_prometheus-config.tpl'
            source = path.read_text()
            old = 'summary: A Flow media metrics target is unavailable'
            self.assertIn(old, source)
            path.write_text(source.replace(old, old + ' test change', 1))
            changed, new_checksum = render(chart)
        self.assertNotEqual(original['rules.yml'], changed['rules.yml'])
        self.assertNotEqual(old_checksum, new_checksum)

    def test_unrelated_turn_advertisement_does_not_reload_prometheus(self):
        original, old_checksum = render(CHART)
        changed, new_checksum = render(CHART, '--set-json', 'coturn.additionalPublicHosts=["192.0.2.99"]')
        self.assertEqual(original, changed)
        self.assertEqual(old_checksum, new_checksum)


if __name__ == '__main__':
    unittest.main()
