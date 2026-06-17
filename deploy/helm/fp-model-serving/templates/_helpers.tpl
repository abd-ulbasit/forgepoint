{{/*
============================================================================
_helpers.tpl — named template helpers (the chart's "functions")
============================================================================
Helm renders every file in templates/, but files/blocks defined with
`{{- define "name" -}}` are NOT rendered to manifests; they are reusable
templates other files pull in with `{{ include "name" . }}`. Centralizing
names and labels here means the chart has ONE source of truth for them, so
this file is a near-verbatim copy of fp-auth's — only the chart name and the
component label (serving) differ.

`include` (not the builtin `template`) is used at call sites because only
`include` can be piped into `nindent`/`indent` for correct YAML indentation.
============================================================================
*/}}

{{/*
fp-model-serving.name — the chart's short name, overridable, truncated to the
63-char DNS label limit. trimSuffix "-" avoids a trailing dash if truncation
lands on one.
*/}}
{{- define "fp-model-serving.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
fp-model-serving.fullname — the fully-qualified resource name (release + chart).
WHY this exact dance (the canonical Helm idiom):
  - fullnameOverride wins outright (operator forced an exact name).
  - else if the release name already contains the chart name, don't double it
    (release "fp-model-serving" + chart "fp-model-serving" → "fp-model-serving",
    not "fp-model-serving-fp-model-serving").
  - else join release + chart.
Always trunc 63 / trimSuffix "-" for DNS-1123 compliance.
*/}}
{{- define "fp-model-serving.fullname" -}}
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
fp-model-serving.chart — "name-version" sanitized for the helm.sh/chart label
(+ → _ because '+' is illegal in a label value; appears in SemVer build metadata).
*/}}
{{- define "fp-model-serving.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
fp-model-serving.selectorLabels — the IMMUTABLE identity labels.
These go in BOTH a Deployment's selector.matchLabels AND its pod template
labels, and a Service's selector. They must NEVER change for a given workload:
a Deployment's selector is immutable after creation, so adding/removing one of
these forces a delete+recreate. Keep this set minimal and stable (name + instance).
*/}}
{{- define "fp-model-serving.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fp-model-serving.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
fp-model-serving.labels — the FULL recommended label set (app.kubernetes.io/*).
Superset of selectorLabels plus version/managed-by/part-of/component, which are
free to change across releases (they are metadata, not selectors). part-of groups
all Forgepoint services under one logical platform for dashboards and queries.
*/}}
{{- define "fp-model-serving.labels" -}}
helm.sh/chart: {{ include "fp-model-serving.chart" . }}
{{ include "fp-model-serving.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: forgepoint
app.kubernetes.io/component: serving
{{- end -}}

{{/*
fp-model-serving.serviceAccountName — the SA to attach to pods.
If serviceAccount.create, default the name to the fullname; otherwise honor an
explicit name, falling back to "default" so the pod spec is always valid.
*/}}
{{- define "fp-model-serving.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "fp-model-serving.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
fp-model-serving.secretName — the Secret the Deployment envFrom's.
When the chart creates the Secret it is named after the fullname; when using an
externally-managed Secret (External Secrets / Sealed Secrets), honor
secrets.existingSecret. Either way the Deployment references ONE stable name.
For model-serving the Secret carries the OBJECT-STORAGE credentials (MinIO/S3
access + secret key) the storage adapter uses to fetch the artifact — NOT a DB
DSN or JWT key (this service has neither).
*/}}
{{- define "fp-model-serving.secretName" -}}
{{- if .Values.secrets.create -}}
{{- include "fp-model-serving.fullname" . -}}
{{- else -}}
{{- required "secrets.create=false requires secrets.existingSecret to name the externally-managed Secret" .Values.secrets.existingSecret -}}
{{- end -}}
{{- end -}}

{{/*
fp-model-serving.image — assemble repository:tag with appVersion as the tag
fallback. Centralized so every manifest (and any future init container)
references the exact same image string.
*/}}
{{- define "fp-model-serving.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}
