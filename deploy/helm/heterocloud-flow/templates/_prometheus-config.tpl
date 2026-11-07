{{- define "flow.prometheus.configmap" -}}
{{- $prometheus := .Values.monitoring.prometheus -}}
{{- $replicaCount := int $prometheus.replicaCount -}}
{{- $prometheusName := printf "%s-prometheus" (include "flow.fullname" .) -}}
{{- $coturnReplicas := int .Values.coturn.replicaCount -}}
{{- $relayPorts := add1 (sub (int .Values.coturn.relayPortMax) (int .Values.coturn.relayPortMin)) -}}
{{- $relayCapacity := mul $relayPorts $coturnReplicas -}}
{{- range $pool := .Values.coturn.additionalPools -}}
{{- $poolRelayPorts := add1 (sub (int $pool.relayPortMax) (int $pool.relayPortMin)) -}}
{{- $coturnReplicas = add $coturnReplicas (int $pool.replicaCount) -}}
{{- $relayCapacity = add $relayCapacity (mul $poolRelayPorts (int $pool.replicaCount)) -}}
{{- end -}}
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ $prometheusName }}
  labels:
    {{- include "flow.labels" . | nindent 4 }}
    app.kubernetes.io/component: prometheus
data:
  prometheus.yml: |
    global:
      scrape_interval: {{ $prometheus.scrapeInterval }}
      evaluation_interval: {{ $prometheus.evaluationInterval }}
      external_labels:
        {{- toYaml $prometheus.externalLabels | nindent 8 }}
    rule_files:
      - /etc/prometheus/rules.yml
    scrape_configs:
      {{- if .Values.monitoring.securityInventory.enabled }}
      {{- if empty .Values.monitoring.securityInventory.targets }}
      {{- fail "monitoring.securityInventory.targets is required when security inventory is enabled" }}
      {{- end }}
      - job_name: heterocloud-security
        static_configs:
          - targets: {{ .Values.monitoring.securityInventory.targets | toJson }}
        scrape_timeout: 5s
      {{- end }}
      {{- if .Values.monitoring.resourceExporter.enabled }}
      - job_name: heterocloud-resources
        kubernetes_sd_configs:
          - role: pod
            namespaces:
              names: [{{ .Release.Namespace | quote }}]
        relabel_configs:
          - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_instance]
            action: keep
            regex: {{ .Release.Name | quote }}
          - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_component]
            action: keep
            regex: resources
          - source_labels: [__meta_kubernetes_pod_container_port_name]
            action: keep
            regex: metrics
          - source_labels: [__meta_kubernetes_pod_name]
            target_label: collector
        scrape_timeout: 5s
      {{- end }}
      - job_name: prometheus
        kubernetes_sd_configs:
          - role: pod
            namespaces:
              names: [{{ .Release.Namespace | quote }}]
        relabel_configs:
          - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_instance]
            action: keep
            regex: {{ .Release.Name | quote }}
          - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_component]
            action: keep
            regex: prometheus
          - source_labels: [__meta_kubernetes_pod_container_port_name]
            action: keep
            regex: http
          - source_labels: [__meta_kubernetes_pod_node_name]
            target_label: node
          - source_labels: [__meta_kubernetes_pod_name]
            target_label: pod

      {{- if .Values.monitoring.grafana.enabled }}
      - job_name: grafana
        metrics_path: /metrics
        kubernetes_sd_configs:
          - role: pod
            namespaces:
              names: [{{ .Release.Namespace | quote }}]
        relabel_configs:
          - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_instance]
            action: keep
            regex: {{ .Release.Name | quote }}
          - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_component]
            action: keep
            regex: grafana
          - source_labels: [__meta_kubernetes_pod_container_port_name]
            action: keep
            regex: http
          - source_labels: [__meta_kubernetes_pod_node_name]
            target_label: node
          - source_labels: [__meta_kubernetes_pod_name]
            target_label: pod
      {{- end }}

      {{- if $prometheus.additionalScrapeNamespaces }}
      - job_name: kubernetes-services-annotated
        kubernetes_sd_configs:
          - role: endpoints
            namespaces:
              names:
                {{- toYaml $prometheus.additionalScrapeNamespaces | nindent 16 }}
        relabel_configs:
          - source_labels: [__meta_kubernetes_service_annotation_prometheus_io_scrape]
            action: keep
            regex: "true"
          - source_labels: [__meta_kubernetes_endpoint_port_name]
            action: keep
            regex: (http-)?metrics
          - source_labels: [__meta_kubernetes_service_annotation_prometheus_io_scheme]
            action: replace
            target_label: __scheme__
            regex: (https?)
          - source_labels: [__meta_kubernetes_service_annotation_prometheus_io_path]
            action: replace
            target_label: __metrics_path__
            regex: (.+)
          - source_labels: [__meta_kubernetes_namespace]
            target_label: namespace
          - source_labels: [__meta_kubernetes_service_name]
            target_label: service
          - source_labels: [__meta_kubernetes_pod_node_name]
            target_label: node
      {{- end }}

      - job_name: kubernetes-nodes-resource
        scheme: https
        authorization:
          credentials_file: /var/run/secrets/kubernetes.io/serviceaccount/token
        tls_config:
          ca_file: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
          server_name: kubernetes.default.svc
        kubernetes_sd_configs:
          - role: node
        relabel_configs:
          - source_labels: [__meta_kubernetes_node_name]
            target_label: node
          - target_label: __address__
            replacement: kubernetes.default.svc:443
          - source_labels: [__meta_kubernetes_node_name]
            target_label: __metrics_path__
            replacement: /api/v1/nodes/$1/proxy/metrics/resource

      - job_name: kubernetes-nodes-cadvisor
        scheme: https
        authorization:
          credentials_file: /var/run/secrets/kubernetes.io/serviceaccount/token
        tls_config:
          ca_file: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
          server_name: kubernetes.default.svc
        kubernetes_sd_configs:
          - role: node
        relabel_configs:
          - source_labels: [__meta_kubernetes_node_name]
            target_label: node
          - target_label: __address__
            replacement: kubernetes.default.svc:443
          - source_labels: [__meta_kubernetes_node_name]
            target_label: __metrics_path__
            replacement: /api/v1/nodes/$1/proxy/metrics/cadvisor

      {{- if $prometheus.heteronetwork.enabled }}
      - job_name: heteronetwork-agents
        metrics_path: /metrics
        kubernetes_sd_configs:
          - role: node
        relabel_configs:
          - source_labels: [__meta_kubernetes_node_address_InternalIP]
            action: keep
            regex: .+
          - source_labels: [__meta_kubernetes_node_address_InternalIP]
            target_label: __address__
            replacement: $1:{{ $prometheus.heteronetwork.agentMetricsPort }}
          - source_labels: [__meta_kubernetes_node_name]
            target_label: node
          - source_labels: [__meta_kubernetes_node_address_InternalIP]
            target_label: vpn_ip
      {{- end }}

      - job_name: flow-livekit
        kubernetes_sd_configs:
          - role: pod
            namespaces:
              names: [{{ .Release.Namespace | quote }}]
        relabel_configs:
          - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_instance]
            action: keep
            regex: {{ .Release.Name | quote }}
          - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_component]
            action: keep
            regex: livekit
          - source_labels: [__meta_kubernetes_pod_container_port_name]
            action: keep
            regex: metrics
          - source_labels: [__meta_kubernetes_pod_node_name]
            target_label: node
          - source_labels: [__meta_kubernetes_pod_name]
            target_label: pod

      - job_name: flow-coturn
        kubernetes_sd_configs:
          - role: pod
            namespaces:
              names: [{{ .Release.Namespace | quote }}]
        relabel_configs:
          - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_instance]
            action: keep
            regex: {{ .Release.Name | quote }}
          - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_component]
            action: keep
            regex: coturn
          - source_labels: [__meta_kubernetes_pod_container_port_name]
            action: keep
            regex: metrics
          - source_labels: [__meta_kubernetes_pod_node_name]
            target_label: node
          - source_labels: [__meta_kubernetes_pod_name]
            target_label: pod
        metric_relabel_configs:
          # Per-credential series are unbounded and are not required for
          # aggregate relay capacity or transfer-rate monitoring.
          - source_labels: [__name__]
            action: drop
            regex: turn_traffic_.*
  rules.yml: |
    groups:
      {{- if and .Values.monitoring.resourceExporter.enabled .Values.monitoring.resourceExporter.storageHealth.enabled }}
      - name: heterocloud-storage-health
        rules:
          - alert: HeteroCloudStorageInventoryUnavailable
            expr: absent(heterocloud_storage_inventory_success) or max(heterocloud_storage_inventory_success) == 0
            for: 2m
            labels:
              severity: warning
            annotations:
              summary: Storage volume health could not be collected
          - alert: HeteroCloudVolumeDegraded
            expr: max by (volume) (heterocloud_storage_volume_robustness{state="degraded"} == 1)
            for: 2m
            labels:
              severity: warning
            annotations:
              summary: A storage volume has lost replica redundancy
          - alert: HeteroCloudVolumeFaulted
            expr: max by (volume) (heterocloud_storage_volume_robustness{state="faulted"} == 1)
            for: 1m
            labels:
              severity: critical
            annotations:
              summary: A storage volume is faulted
      {{- end }}
      {{- if .Values.monitoring.securityInventory.enabled }}
      - name: heterocloud-security
        rules:
          - alert: HeteroCloudSecurityInventoryUnavailable
            expr: absent(up{job="heterocloud-security"}) or up{job="heterocloud-security"} == 0
            for: 2m
            labels:
              severity: warning
            annotations:
              summary: OSS vulnerability inventory is unavailable
          - alert: HeteroCloudSecurityInventoryIncomplete
            expr: heterocloud_security_scan_complete == 0 or heterocloud_security_scan_age_seconds > {{ .Values.monitoring.securityInventory.maxAgeSeconds }}
            for: 5m
            labels:
              severity: warning
            annotations:
              summary: OSS inventory contains unverified targets or stale results
          - alert: HeteroCloudCriticalVulnerabilityUnresolved
            expr: heterocloud_security_findings{severity="CRITICAL"} > 0
            for: 1m
            labels:
              severity: critical
            annotations:
              summary: Critical OSS vulnerabilities require remediation
          - alert: HeteroCloudRuntimeBinaryDrift
            expr: heterocloud_security_runtime_drift_processes > 0
            for: 5m
            labels:
              severity: warning
            annotations:
              summary: Running processes still use a superseded runtime binary
      {{- end }}
      - name: heterocloud-flow-capacity
        interval: {{ $prometheus.evaluationInterval }}
        rules:
          - record: flow:turn_allocations:sum
            expr: sum(turn_total_allocations{job="flow-coturn"})
          - record: flow:turn_allocation_utilization:ratio
            expr: flow:turn_allocations:sum / {{ $relayCapacity }}
          - record: flow:node_cpu_utilization:ratio
            expr: max by (node) ((rate(node_cpu_usage_seconds_total{job="kubernetes-nodes-resource"}[5m]) / on (node) machine_cpu_cores{job="kubernetes-nodes-cadvisor"}) or on (node) sum by (node) (0 * up{job="heteronetwork-agents"}))
          - record: flow:node_memory_working_set:ratio
            expr: max by (node) ((node_memory_working_set_bytes{job="kubernetes-nodes-resource"} / on (node) machine_memory_bytes{job="kubernetes-nodes-cadvisor"}) or on (node) sum by (node) (0 * up{job="heteronetwork-agents"}))
          - record: flow:node_network_receive_bytes_per_second
            expr: sum by (node, interface) (rate(container_network_receive_bytes_total{job="kubernetes-nodes-cadvisor",id="/"}[5m]))
          - record: flow:node_network_transmit_bytes_per_second
            expr: sum by (node, interface) (rate(container_network_transmit_bytes_total{job="kubernetes-nodes-cadvisor",id="/"}[5m]))
          {{- if $prometheus.heteronetwork.enabled }}
          - record: heteronetwork:node_vpn_receive_bytes_per_second
            expr: sum by (node) (rate(container_network_receive_bytes_total{job="kubernetes-nodes-cadvisor",id="/",interface={{ $prometheus.heteronetwork.interface | quote }}}[5m]))
          - record: heteronetwork:node_vpn_transmit_bytes_per_second
            expr: sum by (node) (rate(container_network_transmit_bytes_total{job="kubernetes-nodes-cadvisor",id="/",interface={{ $prometheus.heteronetwork.interface | quote }}}[5m]))
          - record: heteronetwork:active_peers:sum
            expr: sum(ipars_agent_active_peers{job="heteronetwork-agents"})
          - record: heteronetwork:pinned_peers:sum
            expr: sum(ipars_agent_pinned_peers{job="heteronetwork-agents"})
          - record: heteronetwork:relay_sessions:sum
            expr: sum(ipars_agent_relay_sessions{job="heteronetwork-agents"})
          {{- end }}
          - alert: FlowTurnAllocationCapacityHigh
            expr: flow:turn_allocation_utilization:ratio > 0.8
            for: 2m
            labels:
              severity: warning
            annotations:
              summary: TURN relay allocation capacity is above 80 percent
          - alert: FlowMetricsTargetMissing
            expr: (count(up{job="flow-coturn"} == 1) or vector(0)) < {{ $coturnReplicas }} or (count(up{job="flow-livekit"} == 1) or vector(0)) < {{ .Values.livekit.replicaCount }}
            for: 2m
            labels:
              severity: warning
            annotations:
              summary: A Flow media metrics target is unavailable
          - alert: FlowNodeCpuHigh
            expr: flow:node_cpu_utilization:ratio > 0.85
            for: 10m
            labels:
              severity: warning
            annotations:
              summary: Kubernetes node CPU utilization is above 85 percent
          - alert: FlowNodeMemoryHigh
            expr: flow:node_memory_working_set:ratio > 0.85
            for: 10m
            labels:
              severity: warning
            annotations:
              summary: Kubernetes node memory working set is above 85 percent
          {{- if $prometheus.heteronetwork.enabled }}
          - alert: HeteroNetworkAgentMetricsMissing
            expr: absent(up{job="heteronetwork-agents"}) or min(up{job="heteronetwork-agents"}) < 1
            for: 2m
            labels:
              severity: warning
            annotations:
              summary: At least one HeteroNetwork Agent metrics endpoint is unavailable
          {{- end }}
          - alert: PrometheusReplicaMissing
            expr: (count(up{job="prometheus"} == 1) or vector(0)) < {{ $replicaCount }}
            for: 2m
            labels:
              severity: warning
            annotations:
              summary: Fewer than {{ $replicaCount }} Prometheus replicas are available
          {{- if .Values.monitoring.grafana.enabled }}
          - alert: GrafanaReplicaMissing
            expr: (count(up{job="grafana"} == 1) or vector(0)) < {{ .Values.monitoring.grafana.replicaCount }}
            for: 2m
            labels:
              severity: warning
            annotations:
              summary: Fewer than {{ .Values.monitoring.grafana.replicaCount }} Grafana replicas are available
          {{- end }}
{{- end -}}
