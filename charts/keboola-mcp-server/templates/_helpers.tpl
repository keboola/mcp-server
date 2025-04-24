{{/*
Expand the name of the chart.
*/}}
{{- define "keboola-mcp-server.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "keboola-mcp-server.fullname" -}}
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

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "keboola-mcp-server.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "keboola-mcp-server.labels" -}}
helm.sh/chart: {{ include "keboola-mcp-server.chart" . }}
{{ include "keboola-mcp-server.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels
*/}}
{{- define "keboola-mcp-server.selectorLabels" -}}
app.kubernetes.io/name: {{ include "keboola-mcp-server.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Create the name of the service account to use
*/}}
{{- define "keboola-mcp-server.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
    {{- default (include "keboola-mcp-server.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
    {{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Helper to get the KBC storage token value or reference
*/}}
{{- define "keboola-mcp-server.storageTokenValue" -}}
{{- if .Values.existingSecret -}}
valueFrom:
  secretKeyRef:
    name: {{ .Values.existingSecret }}
    key: KBC_STORAGE_TOKEN
{{- else if .Values.keboola.storageTokenSecretName -}}
valueFrom:
  secretKeyRef:
    name: {{ .Values.keboola.storageTokenSecretName }}
    key: {{ .Values.keboola.storageTokenSecretKey }}
{{- else -}}
# It's highly recommended to use a secret for the KBC_STORAGE_TOKEN
# value: "YOUR_KBC_STORAGE_TOKEN_HERE" # Placeholder - DO NOT commit sensitive data
value: ""
{{- end -}}
{{- end -}}

{{/*
Helper to get the Google Application Credentials value or reference
*/}}
{{- define "keboola-mcp-server.googleCredentialsValue" -}}
{{- if .Values.existingSecret -}}
valueFrom:
  secretKeyRef:
    name: {{ .Values.existingSecret }}
    key: GOOGLE_APPLICATION_CREDENTIALS
{{- else if .Values.keboola.googleCredentialsSecretName -}}
valueFrom:
  secretKeyRef:
    name: {{ .Values.keboola.googleCredentialsSecretName }}
    key: {{ .Values.keboola.googleCredentialsSecretKey }}
{{- else -}}
# Set 'useGoogleCredentials: true' and provide googleCredentialsSecretName if using BigQuery
value: ""
{{- end -}}
{{- end -}} 