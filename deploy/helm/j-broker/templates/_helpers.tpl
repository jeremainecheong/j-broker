{{/*
Expand the name of the chart.
*/}}
{{- define "jbroker.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Fully-qualified release name used as a prefix for every resource.
*/}}
{{- define "jbroker.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "jbroker.broker.fullname" -}}
{{- printf "%s-broker" (include "jbroker.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "jbroker.broker.headless" -}}
{{- printf "%s-broker-headless" (include "jbroker.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "jbroker.admin.fullname" -}}
{{- printf "%s-admin" (include "jbroker.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "jbroker.redis.fullname" -}}
{{- printf "%s-redis" (include "jbroker.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels applied to every resource.
*/}}
{{- define "jbroker.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "jbroker.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "jbroker.broker.selectorLabels" -}}
app.kubernetes.io/name: {{ include "jbroker.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: broker
{{- end -}}

{{- define "jbroker.admin.selectorLabels" -}}
app.kubernetes.io/name: {{ include "jbroker.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: admin
{{- end -}}

{{- define "jbroker.redis.selectorLabels" -}}
app.kubernetes.io/name: {{ include "jbroker.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: redis
{{- end -}}

{{/*
Render the --voters spec from the broker replica count. Each pod is
addressable at {pod-0}.{headless}.{ns}.svc.cluster.local — the service
fully-qualified name.
*/}}
{{- define "jbroker.voters" -}}
{{- $count := int .Values.broker.replicaCount -}}
{{- $headless := include "jbroker.broker.headless" . -}}
{{- $fullname := include "jbroker.broker.fullname" . -}}
{{- $ns := .Release.Namespace -}}
{{- $raft := .Values.broker.ports.raft -}}
{{- $grpc := .Values.broker.ports.grpc -}}
{{- $entries := list -}}
{{- range $i, $e := until $count -}}
  {{- $id := add $i 1 -}}
  {{- $host := printf "%s-%d.%s.%s.svc.cluster.local" $fullname $i $headless $ns -}}
  {{- $entry := printf "%d@%s:%d:%d" $id $host (int $raft) (int $grpc) -}}
  {{- $entries = append $entries $entry -}}
{{- end -}}
{{- join "," $entries -}}
{{- end -}}

{{/*
Advertised listeners spec, rendered when broker.advertisedHostTemplate is
set. Broker ids are 1-indexed; sprintf "%d" in the template receives the
1-indexed broker id.
*/}}
{{- define "jbroker.advertisedListeners" -}}
{{- if .Values.broker.advertisedHostTemplate -}}
{{- $count := int .Values.broker.replicaCount -}}
{{- $tmpl := .Values.broker.advertisedHostTemplate -}}
{{- $port := .Values.broker.advertisedPort -}}
{{- if le (int $port) 0 -}}
  {{- $port = .Values.broker.ports.grpc -}}
{{- end -}}
{{- $entries := list -}}
{{- range $i, $e := until $count -}}
  {{- $id := add $i 1 -}}
  {{- $host := printf $tmpl $id -}}
  {{- $entry := printf "%d=%s:%d" $id $host (int $port) -}}
  {{- $entries = append $entries $entry -}}
{{- end -}}
{{- join "," $entries -}}
{{- end -}}
{{- end -}}

{{/*
Service account name — created by the chart when serviceAccount.create.
*/}}
{{- define "jbroker.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "jbroker.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Admin-app broker list (Spring property jbroker.admin.brokers). Default:
derive from the broker headless service, one entry per replica.
*/}}
{{- define "jbroker.admin.brokers" -}}
{{- if .Values.admin.brokersOverride -}}
{{- .Values.admin.brokersOverride -}}
{{- else -}}
{{- $count := int .Values.broker.replicaCount -}}
{{- $headless := include "jbroker.broker.headless" . -}}
{{- $fullname := include "jbroker.broker.fullname" . -}}
{{- $ns := .Release.Namespace -}}
{{- $grpc := .Values.broker.ports.grpc -}}
{{- $entries := list -}}
{{- range $i, $e := until $count -}}
  {{- $host := printf "%s-%d.%s.%s.svc.cluster.local" $fullname $i $headless $ns -}}
  {{- $entry := printf "%s:%d" $host (int $grpc) -}}
  {{- $entries = append $entries $entry -}}
{{- end -}}
{{- join "," $entries -}}
{{- end -}}
{{- end -}}
