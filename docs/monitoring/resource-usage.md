# Cluster and user resource usage

The provisioned Grafana home is **HeteroCloud / クラスターリソース** (`/d/heterocloud-cluster-resources/overview`). Each resource has two donut charts: capacity by Kubernetes node, and actual Flash consumption by HeteroCloud user. Hover a slice for its name, value and percentage. Click a user slice or its value to open `/d/heterocloud-user-resources/usage?var-user=<user-id>` and inspect the user's individual services. The existing Flow/VPN dashboard remains linked at the top.

## Meaning of the measurements

- CPU capacity is logical cores from the registered nodes. CPU consumption is kubelet's recent `usageNanoCores`, expressed in cores, not the configured request or limit.
- Memory capacity is node capacity; consumption is the working set, not the container memory ceiling.
- Node disk capacity and use come from kubelet `node.fs`, the primary node filesystem. Additional mounted filesystems and unmounted disks are outside this measure. This intentionally avoids counting Pod bind mounts, loop devices and replicated PVCs as extra physical capacity.
- Service disk use is Pod ephemeral storage (writable layers and logs) plus observed PVC filesystem use. A shared PVC is counted once per service, including across replicas/nodes. Dedicated unlabelled administrator PVCs are attributed when exactly one service mounts them. Shared infrastructure/image caches and storage replicas are not billed to a user here.
- The persistent disk **capacity** panel reports provisioned PVC capacity separately. Stopped/unmounted PVCs may have no current measurement; missing disk use is marked **未取得**, and the overview reports the number of incomplete services. Disk donut charts explicitly show the observed subtotal. They do not claim missing data is zero.
- Host consumption includes platform/OS overhead, while user charts cover Flash services. These totals have different meanings and need not match.

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
```

The Resource monitoring workflow runs these checks and publishes an exporter image. Production pins its digest. Unit coverage includes actual usage versus limits, shared-volume deduplication, missing/stopped services, deleted nodes, old Pod UIDs, task-role ownership, cross-organization isolation, cyclic roles, and stale collection failures.
