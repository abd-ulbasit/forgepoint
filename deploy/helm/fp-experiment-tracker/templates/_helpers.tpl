{{/*
============================================================================
_helpers.tpl — named template helpers (the chart's "functions")
============================================================================
Helm renders every file in templates/, but files/blocks defined with
`{{- define "name" -}}` are NOT rendered to manifests; they are reusable
templates other files pull in with `{{ include "name" . }}`. Centralizing
names and labels here means the chart has ONE source of truth for them. This
file is a verbatim copy of the fp-auth reference helpers with the template name
prefix swapped to "fp-experiment-tracker"; every service's chart follows the
same pattern so only Chart.name and the component label differ.

`include` (not the builtin `template`) is used at call sites because only
`include` can be piped into `nindent`/`indent` for correct YAML indentation.
============================================================================
*/}}

{{/*
fp-experiment-tracker.name — the chart's short name, overridable, truncated to the 63-char
DNS label limit. trimSuffix "-" avoids a trailing dash if truncation lands on one.
*/}}
{{- define "fp-experiment-tracker.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
fp-experiment-tracker.fullname — the fully-qualified resource name (release + chart).
WHY this exact dance (the canonical Helm idiom):
  - fullnameOverride wins outright (operator forced an exact name).
  - else if the release name already contains the chart name, don't double it
    (release "fp-experiment-tracker" + chart "fp-experiment-tracker" →
    "fp-experiment-tracker", not "fp-experiment-tracker-fp-experiment-tracker").
  - else join release + chart.
Always trunc 63 / trimSuffix "-" for DNS-1123 compliance.
*/}}
{{- define "fp-experiment-tracker.fullname" -}}
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
fp-experiment-tracker.chart — "name-version" sanitized for the helm.sh/chart label
(+ → _ because '+' is illegal in a label value; appears in SemVer build metadata).
*/}}
{{- define "fp-experiment-tracker.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
fp-experiment-tracker.selectorLabels — the IMMUTABLE identity labels.
These go in BOTH a Deployment's selector.matchLabels AND its pod template
labels, and a Service's selector. They must NEVER change for a given workload:
a Deployment's selector is immutable after creation, so adding/removing one of
these forces a delete+recreate. Keep this set minimal and stable (name + instance).
*/}}
{{- define "fp-experiment-tracker.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fp-experiment-tracker.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
fp-experiment-tracker.labels — the FULL recommended label set (app.kubernetes.io/*).
Superset of selectorLabels plus version/managed-by/part-of/component, which are
free to change across releases (they are metadata, not selectors). part-of groups
all Forgepoint services under one logical platform for dashboards and queries.
*/}}
{{- define "fp-experiment-tracker.labels" -}}
helm.sh/chart: {{ include "fp-experiment-tracker.chart" . }}
{{ include "fp-experiment-tracker.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: forgepoint
app.kubernetes.io/component: experiment-tracker
{{- end -}}

{{/*
fp-experiment-tracker.serviceAccountName — the SA to attach to pods.
If serviceAccount.create, default the name to the fullname; otherwise honor an
explicit name, falling back to "default" so the pod spec is always valid.
*/}}
{{- define "fp-experiment-tracker.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "fp-experiment-tracker.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
fp-experiment-tracker.secretName — the Secret the Deployment envFrom's.
When the chart creates the Secret it is named after the fullname; when using an
externally-managed Secret (External Secrets / Sealed Secrets), honor
secrets.existingSecret. Either way the Deployment references ONE stable name.
*/}}
{{- define "fp-experiment-tracker.secretName" -}}
{{- if .Values.secrets.create -}}
{{- include "fp-experiment-tracker.fullname" . -}}
{{- else -}}
{{- required "secrets.create=false requires secrets.existingSecret to name the externally-managed Secret" .Values.secrets.existingSecret -}}
{{- end -}}
{{- end -}}

{{/*
fp-experiment-tracker.image — assemble repository:tag with appVersion as the tag fallback.
Centralized so every manifest (and any future init container) references the
exact same image string.
*/}}
{{- define "fp-experiment-tracker.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}
