# Cluster and user resource usage

The provisioned Grafana home is **HeteroCloud / クラスターリソース** (`/d/heterocloud-cluster-resources/overview`). Each resource has two donut charts: capacity by Flash worker node, and actual Flash consumption by HeteroCloud user. Both use the same worker pool capacity as 100%, including unused capacity in the user chart. Hover a slice for its name, value and percentage. Click a user slice to open `/d/heterocloud-user-resources/usage?var-user=<user-id>` and inspect the user's individual services. The existing Flow/VPN dashboard remains linked at the top.

## Meaning of the measurements

- Dedicated masters (such as p1–p3) are excluded from all capacity totals, physical node charts and overview usage denominators. Membership comes from Flash runtime readiness labels and dedicated control-plane taints, not a hardcoded node list. A mixed-role control-plane/worker is included; temporary cordon/NotReady does not shrink the denominator.
- CPU capacity is logical cores from the Flash worker nodes. CPU consumption is kubelet's recent `usageNanoCores`, expressed in cores, not the configured request or limit.
- Memory capacity is node capacity; consumption is the working set, not the container memory ceiling.
- Node disk capacity and use come from kubelet `node.fs`, the primary node filesystem. Additional mounted filesystems and unmounted disks are outside this measure. This intentionally avoids counting Pod bind mounts, loop devices and replicated PVCs as extra physical capacity.
- Service disk use is Pod ephemeral storage (writable layers and logs) plus observed PVC filesystem use. A shared PVC is counted once per service, including across replicas/nodes. Dedicated unlabelled administrator PVCs are attributed when exactly one service mounts them. Shared infrastructure/image caches and storage replicas are not billed to a user here.
- The persistent disk **capacity** panel reports provisioned PVC capacity separately. Stopped/unmounted PVCs may have no current measurement; missing disk use is marked **未取得**, and the overview reports the number of incomplete services. Disk donut charts explicitly show the observed subtotal. They do not claim missing data is zero.
- Overview user charts partition worker capacity into **users**, **システム・未帰属** (host use minus measured users, clamped to zero), and **未使用** (remaining capacity). A user consuming 100 GB of a 1 TB pool occupies 10%, not 100% of the users' subtotal. Platform overhead, storage replicas and unmeasured user consumption can be in the residual. If host samples temporarily trail user samples, the residual is zero; capacity is unchanged. Missing node measurements or usage above total capacity suppress the partition instead of inventing free space. Only user slices link to a user detail page. That detail page breaks down the selected user's own consumption by service.

The collector refreshes every 30 seconds. Fresh snapshots replace prior inventory, so removed services/nodes disappear without maintaining a manual list. Collection failure removes stale usage instead of returning it as a new successful sample. Two collectors are deduplicated in every dashboard query. The dataset is administrative and uses the existing VPN-only Grafana access boundary.

## User attribution

`monitoring.resource_services` is a restricted SQL projection of generation-one creation events, service names, organization names and creator identities. Editing a service never changes its creator. Task-role children inherit a parent user's attribution only when all matching parents in the same organization resolve to one user. Cycles, ambiguous roles and standalone service accounts remain separate `SA:` entries; missing creators appear as **未帰属**. Coder/application-provided owner metadata is not trusted for HeteroCloud ownership.

The exporter has no Kubernetes write/exec/Secret permission and its SQL account can only select the projection. Service specs, environment values, credentials, emails and sessions are not exported. Bootstrap credentials through HeteroNetwork's `terraform_data.resource_monitoring` / `scripts/reconcile-resource-monitoring.sh`; the password is preserved on subsequent reconciliations. Deployment, RBAC, NetworkPolicy, Prometheus discovery and dashboards are managed by the Flow Helm chart and Argo CD.

## Verification

```sh
python3 -m unittest discover -s monitoring/resource-exporter -v
python3 -m unittest discover -s monitoring -v  # Helm and PyYAML required
bash deploy/helm/heterocloud-flow/scripts/test-grafana-template.sh
bash deploy/helm/heterocloud-flow/scripts/test-prometheus-template.sh  # Docker required
python3 monitoring/capacity_promql_cases.py | promtool test rules /dev/stdin
```

The Resource monitoring workflow runs these checks and publishes an exporter image. Production pins its digest. Unit coverage includes actual usage versus limits, shared-volume deduplication, missing/stopped services, deleted nodes, old Pod UIDs, task-role ownership, cross-organization isolation, cyclic roles, and stale collection failures. Promtool evaluates the actual dashboard expressions for CPU, memory and disk, including 100 GB / 1 TB = 10%, exclusion of dedicated-master capacity, duplicate HA collectors, empty users, missing samples and contradictory samples.

Read-only browser verification against a deployed Grafana (connect the VPN first):

```sh
npm ci --prefix monitoring/browser
npx --prefix monitoring/browser playwright install chromium
GRAFANA_BASE_URL=http://grafana.heteronetwork.internal:13000 \
  npm test --prefix monitoring/browser
```

This test uses actual mouse hover and clicks on all three resource donuts, checks worker-only node membership and the shared capacity denominator, verifies that unused slices have no user link, checks that the drilled-down table contains exactly the selected user's services, tests every user's filtered route, and verifies the Grafana default home. Screenshots and a JSON result are written under `test-results/resource-monitoring`. `ARTIFACT_DIR` can override this directory. It creates, restarts and deletes no services.

With host-network Prometheus and Flannel, remote Pod traffic originates from each monitoring host's `flannel.1` address. `metricsSourceCidrs` permits those host addresses as `/32`, in addition to VPN addresses; it does not grant all tenant Pod CIDRs access. When replacing a monitoring host, update this allowlist along with the monitoring pool. The collector's database connection uses the health-discovered `postgres-ha-connector` Service, not a static list of retired database hosts.
