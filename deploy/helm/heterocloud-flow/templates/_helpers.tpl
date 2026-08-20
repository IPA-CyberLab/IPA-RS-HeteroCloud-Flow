{{- define "flow.name" -}}
heterocloud-flow
{{- end }}

{{- define "flow.fullname" -}}
{{- .Values.fullnameOverride | default (printf "%s-%s" .Release.Name (include "flow.name" .)) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "flow.chart" -}}
{{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end }}

{{- define "flow.labels" -}}
helm.sh/chart: {{ include "flow.chart" . }}
app.kubernetes.io/name: {{ include "flow.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/part-of: heterocloud-flow
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "flow.selectorLabels" -}}
app.kubernetes.io/name: {{ include "flow.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/part-of: heterocloud-flow
{{- end }}

{{- define "flow.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- .Values.serviceAccount.name | default (include "flow.fullname" .) }}
{{- else }}
{{- .Values.serviceAccount.name | default "default" }}
{{- end }}
{{- end }}

{{- define "flow.secretName" -}}
{{- if .Values.secrets.create }}
{{- printf "%s-secrets" (include "flow.fullname" .) }}
{{- else }}
{{- required "secrets.existingSecret is required when secrets.create=false" .Values.secrets.existingSecret }}
{{- end }}
{{- end }}

{{- define "flow.image" -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) }}
{{- end }}

{{- define "flow.imageRef" -}}
{{- $reference := printf "%s:%s" .repository .tag -}}
{{- if .digest -}}
{{- printf "%s@%s" $reference .digest -}}
{{- else -}}
{{- $reference -}}
{{- end -}}
{{- end }}

{{- define "flow.redisSentinelUrls" -}}
{{- if .Values.redis.enabled -}}
{{- $root := . -}}
{{- $fullname := .Values.redis.fullnameOverride -}}
{{- $headless := printf "%s-headless" $fullname -}}
{{- $replicas := int .Values.redis.replica.replicaCount -}}
{{- $urls := list -}}
{{- range $index := until $replicas -}}
{{- $urls = append $urls (printf "redis://%s-node-%d.%s.%s.svc.cluster.local:26379" $fullname $index $headless $root.Release.Namespace) -}}
{{- end -}}
{{ join "," $urls }}
{{- else -}}
{{ join "," .Values.externalRedis.sentinelUrls }}
{{- end -}}
{{- end }}

{{- define "flow.redisSentinelMaster" -}}
{{- if .Values.redis.enabled -}}
{{ .Values.redis.sentinel.masterSet }}
{{- else -}}
{{ .Values.externalRedis.sentinelMaster }}
{{- end -}}
{{- end }}

{{- define "flow.databaseProxyHost" -}}
{{- printf "%s-postgres-proxy.%s.svc.cluster.local" (include "flow.fullname" .) .Release.Namespace -}}
{{- end }}

{{- define "flow.databaseProxyEnv" -}}
{{- if .Values.database.proxy.enabled }}
- name: DATABASE_PROXY_HOST
  value: {{ include "flow.databaseProxyHost" . | quote }}
- name: DATABASE_PROXY_PORT
  value: {{ .Values.database.proxy.port | quote }}
{{- end }}
{{- end }}

{{- define "flow.rustSecurityContext" -}}
allowPrivilegeEscalation: false
capabilities:
  drop:
    - ALL
readOnlyRootFilesystem: true
runAsNonRoot: true
runAsUser: 65532
runAsGroup: 65532
{{- end }}

{{- define "flow.podSecurityContext" -}}
runAsNonRoot: true
seccompProfile:
  type: RuntimeDefault
{{- end }}

{{- define "flow.topologySpread" -}}
- maxSkew: 1
  topologyKey: kubernetes.io/hostname
  whenUnsatisfiable: ScheduleAnyway
  # Unreachable nodes receive NoSchedule/NoExecute taints. Excluding those
  # domains lets replicas rebalance onto the remaining healthy nodes.
  nodeTaintsPolicy: Honor
  labelSelector:
    matchLabels:
      {{- include "flow.selectorLabels" .root | nindent 6 }}
      app.kubernetes.io/component: {{ .component }}
{{- end }}

{{- define "flow.requiredPodAntiAffinity" -}}
podAntiAffinity:
  preferredDuringSchedulingIgnoredDuringExecution:
    - weight: 100
      podAffinityTerm:
        topologyKey: kubernetes.io/hostname
        labelSelector:
          matchLabels:
            {{- include "flow.selectorLabels" .root | nindent 12 }}
            app.kubernetes.io/component: {{ .component }}
{{- end }}

{{- define "flow.databaseNodeAffinity" -}}
{{- if and .Values.database.proxy.enabled (gt (len .Values.database.proxy.endpointNodeNames) 0) -}}
{{- $nodes := list -}}
{{- range $_, $node := .Values.database.proxy.endpointNodeNames -}}
{{- $nodes = append $nodes $node -}}
{{- end -}}
nodeAffinity:
  requiredDuringSchedulingIgnoredDuringExecution:
    nodeSelectorTerms:
      - matchExpressions:
          - key: kubernetes.io/hostname
            operator: In
            values:
              {{- range sortAlpha (uniq $nodes) }}
              - {{ . | quote }}
              {{- end }}
{{- end -}}
{{- end }}

{{- define "flow.turnUrls" -}}
{{- $root := . -}}
{{- $urls := list -}}
{{- range $host := .Values.coturn.publicHosts -}}
{{- $urls = append $urls (printf "turn:%s:%v?transport=udp" $host $root.Values.coturn.servicePort) -}}
{{- $urls = append $urls (printf "turn:%s:%v?transport=tcp" $host $root.Values.coturn.servicePort) -}}
{{- range $pool := $root.Values.coturn.additionalPools -}}
{{- $urls = append $urls (printf "turn:%s:%v?transport=udp" $host $pool.servicePort) -}}
{{- $urls = append $urls (printf "turn:%s:%v?transport=tcp" $host $pool.servicePort) -}}
{{- end -}}
{{- end -}}
{{- join "," $urls -}}
{{- end }}

{{- define "flow.rejectTrafficModeAnnotation" -}}
{{- $annotations := .annotations | default dict -}}
{{- if hasKey $annotations "networking.heteronetwork.io/traffic-mode" -}}
{{- fail (printf "%s must not set networking.heteronetwork.io/traffic-mode; Flow fixes this policy by component" .path) -}}
{{- end -}}
{{- end }}
