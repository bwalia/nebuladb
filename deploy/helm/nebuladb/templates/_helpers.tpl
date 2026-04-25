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

{{- define "nebuladb.showcase.selectorLabels" -}}
{{ include "nebuladb.selectorLabels" . }}
app.kubernetes.io/component: showcase
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
