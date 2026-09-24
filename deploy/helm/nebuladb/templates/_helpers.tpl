{{/*
Naming + label helpers. Keeping them narrow (no kitchen-sink
include) so every template file's YAML stays grep-able.
*/}}

{{- define "nebuladb.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "nebuladb.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{ .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{ .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else -}}
{{ printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "nebuladb.chart" -}}
{{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end -}}

{{/*
Common labels on every resource. `helm.sh/chart` helps operators
grep for "who deployed this" on a shared cluster.
*/}}
{{- define "nebuladb.labels" -}}
helm.sh/chart: {{ include "nebuladb.chart" . }}
{{ include "nebuladb.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "nebuladb.selectorLabels" -}}
app.kubernetes.io/name: {{ include "nebuladb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Server-specific selector label — adds a `component` so the server
and showcase can share the chart but land on distinct Pods via
their own Services.
*/}}
{{- define "nebuladb.server.selectorLabels" -}}
{{ include "nebuladb.selectorLabels" . }}
app.kubernetes.io/component: server
{{- end -}}

{{- define "nebuladb.follower.selectorLabels" -}}
{{ include "nebuladb.selectorLabels" . }}
app.kubernetes.io/component: server-follower
{{- end -}}

{{- define "nebuladb.regionB.selectorLabels" -}}
{{ include "nebuladb.selectorLabels" . }}
app.kubernetes.io/component: server-region-b
{{- end -}}

{{- define "nebuladb.showcase.selectorLabels" -}}
{{ include "nebuladb.selectorLabels" . }}
app.kubernetes.io/component: showcase
{{- end -}}

{{- define "nebuladb.examples.fullname" -}}
{{ printf "%s-examples" (include "nebuladb.fullname" .) }}
{{- end -}}

{{- define "nebuladb.examples.selectorLabels" -}}
{{ include "nebuladb.selectorLabels" . }}
app.kubernetes.io/component: examples
{{- end -}}

{{/* In-cluster DNS for the primary server Service. */}}
{{- define "nebuladb.server.fullname" -}}
{{ include "nebuladb.fullname" . }}
{{- end -}}

{{- define "nebuladb.follower.fullname" -}}
{{ printf "%s-follower" (include "nebuladb.fullname" .) }}
{{- end -}}

{{- define "nebuladb.regionB.fullname" -}}
{{ printf "%s-region-b" (include "nebuladb.fullname" .) }}
{{- end -}}

{{/*
Cross-region peers env value for the primary (region-a) leader.
Always includes the chart-managed region-b Service when deployPeerLeader.
*/}}
{{- define "nebuladb.crossRegion.peersPrimary" -}}
{{- $parts := list -}}
{{- if and .Values.ha.crossRegion.enabled .Values.ha.crossRegion.deployPeerLeader -}}
{{- $parts = append $parts (printf "%s=http://%s:%d" .Values.ha.crossRegion.peerRegion (include "nebuladb.regionB.fullname" .) (int .Values.server.service.grpcPort)) -}}
{{- end -}}
{{- range .Values.ha.crossRegion.peers -}}
{{- $parts = append $parts (printf "%s=%s" .region .grpcUrl) -}}
{{- end -}}
{{- join "," $parts -}}
{{- end -}}

{{/* Cross-region peers env for region-b (points back at primary). */}}
{{- define "nebuladb.crossRegion.peersRegionB" -}}
{{- printf "%s=http://%s:%d" .Values.ha.crossRegion.region (include "nebuladb.server.fullname" .) (int .Values.server.service.grpcPort) -}}
{{- end -}}

{{/* NEBULA_PEERS listing for cluster admin (REST base URLs). */}}
{{- define "nebuladb.cluster.peers" -}}
{{- $parts := list -}}
{{- if .Values.ha.withinRegion.enabled -}}
{{- $parts = append $parts (printf "follower=http://%s:%d" (include "nebuladb.follower.fullname" .) (int .Values.server.service.restPort)) -}}
{{- end -}}
{{- if and .Values.ha.crossRegion.enabled .Values.ha.crossRegion.deployPeerLeader -}}
{{- $parts = append $parts (printf "region-b=http://%s:%d" (include "nebuladb.regionB.fullname" .) (int .Values.server.service.restPort)) -}}
{{- end -}}
{{- join "," $parts -}}
{{- end -}}
{{- define "nebuladb.serviceAccountName" -}}
{{- if .Values.server.serviceAccount.create -}}
{{- default (include "nebuladb.fullname" .) .Values.server.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.server.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Redis URL resolution. Order:
  1. values.externalRedisUrl if set.
  2. `<release>-redis-master:6379` if the subchart is enabled.
  3. empty — server falls back to in-proc cache only.
*/}}
{{- define "nebuladb.redisUrl" -}}
{{- if .Values.externalRedisUrl -}}
{{ .Values.externalRedisUrl }}
{{- else if .Values.redis.enabled -}}
{{ printf "redis://%s-redis-master:6379" .Release.Name }}
{{- else -}}
{{- end -}}
{{- end -}}

{{/*
The showcase talks to the server via nginx proxy. It needs the
in-cluster DNS of the server Service.
*/}}
{{- define "nebuladb.showcase.serverUrl" -}}
{{- if .Values.showcase.nebulaServerUrl -}}
{{ .Values.showcase.nebulaServerUrl }}
{{- else -}}
{{ printf "http://%s:%d" (include "nebuladb.fullname" .) (int .Values.server.service.restPort) }}
{{- end -}}
{{- end -}}
