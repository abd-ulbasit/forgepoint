{{/*
============================================================================
_helpers.tpl — named template helpers (the chart's "functions")
============================================================================
Helm renders every file in templates/, but files/blocks defined with
`{{- define "name" -}}` are NOT rendered to manifests; they are reusable
templates other files pull in with `{{ include "name" . }}`. Centralizing
names and labels here means the chart has ONE source of truth for them. This
file is a direct copy of fp-auth's helpers with the template names rebased to
`fp-inference-gateway.*` and the component label set to inference-gateway —
nothing else differs (that uniformity is the point).

`include` (not the builtin `template`) is used at call sites because only
`include` can be piped into `nindent`/`indent` for correct YAML indentation.
============================================================================
*/}}

{{/*
fp-inference-gateway.name — the chart's short name, overridable, truncated to the
63-char DNS label limit. trimSuffix "-" avoids a trailing dash if truncation
lands on one.
*/}}
{{- define "fp-inference-gateway.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
fp-inference-gateway.fullname — the fully-qualified resource name (release + chart).
WHY this exact dance (the canonical Helm idiom):
  - fullnameOverride wins outright (operator forced an exact name).
  - else if the release name already contains the chart name, don't double it
    (release "fp-inference-gateway" + chart "fp-inference-gateway" →
    "fp-inference-gateway", not "fp-inference-gateway-fp-inference-gateway").
  - else join release + chart.
Always trunc 63 / trimSuffix "-" for DNS-1123 compliance. NOTE: "fp-inference-
gateway" is 20 chars, so release+chart joins stay comfortably under 63 — the
trunc is defensive, mirroring the reference chart verbatim.
*/}}
{{- define "fp-inference-gateway.fullname" -}}
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
fp-inference-gateway.chart — "name-version" sanitized for the helm.sh/chart label
(+ → _ because '+' is illegal in a label value; appears in SemVer build metadata).
*/}}
{{- define "fp-inference-gateway.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
fp-inference-gateway.selectorLabels — the IMMUTABLE identity labels.
These go in BOTH a Deployment's selector.matchLabels AND its pod template
labels, and a Service's selector. They must NEVER change for a given workload:
a Deployment's selector is immutable after creation, so adding/removing one of
these forces a delete+recreate. Keep this set minimal and stable (name + instance).
*/}}
{{- define "fp-inference-gateway.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fp-inference-gateway.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
fp-inference-gateway.labels — the FULL recommended label set (app.kubernetes.io/*).
Superset of selectorLabels plus version/managed-by/part-of/component, which are
free to change across releases (they are metadata, not selectors). part-of groups
all Forgepoint services under one logical platform for dashboards and queries.
*/}}
{{- define "fp-inference-gateway.labels" -}}
helm.sh/chart: {{ include "fp-inference-gateway.chart" . }}
{{ include "fp-inference-gateway.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: forgepoint
app.kubernetes.io/component: inference-gateway
{{- end -}}

{{/*
fp-inference-gateway.serviceAccountName — the SA to attach to pods.
If serviceAccount.create, default the name to the fullname; otherwise honor an
explicit name, falling back to "default" so the pod spec is always valid.
*/}}
{{- define "fp-inference-gateway.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "fp-inference-gateway.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
fp-inference-gateway.secretName — the Secret the Deployment envFrom's.
When the chart creates the Secret it is named after the fullname; when using an
externally-managed Secret (External Secrets / Sealed Secrets), honor
secrets.existingSecret. Either way the Deployment references ONE stable name.
*/}}
{{- define "fp-inference-gateway.secretName" -}}
{{- if .Values.secrets.create -}}
{{- include "fp-inference-gateway.fullname" . -}}
{{- else -}}
{{- required "secrets.create=false requires secrets.existingSecret to name the externally-managed Secret" .Values.secrets.existingSecret -}}
{{- end -}}
{{- end -}}

{{/*
fp-inference-gateway.image — assemble repository:tag with appVersion as the tag
fallback. Centralized so every manifest (and any future init container)
references the exact same image string.
*/}}
{{- define "fp-inference-gateway.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}
