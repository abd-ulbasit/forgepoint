{{/*
============================================================================
_helpers.tpl — named template helpers (the chart's "functions")
============================================================================
Helm renders every file in templates/, but blocks defined with
`{{- define "name" -}}` are NOT rendered to manifests; they are reusable
templates other files pull in with `{{ include "name" . }}`. Centralizing
names and labels here gives the chart ONE source of truth for them. This file
is a direct copy of fp-auth's helpers with the template names rebased to
`fp-bff.*` and the component label set to `bff` — nothing else differs (that
uniformity is the point).

NOTE vs the reference chart: there is NO `fp-bff.secretName` helper — the BFF
owns NO Secret. It FORWARDS the caller's JWT to the downstream services (which
validate it with the shared HMAC key); it never mints or validates tokens, so
it needs no signing/verification key, no DB DSN, and no Secret at all.

`include` (not the builtin `template`) is used at call sites because only
`include` can be piped into `nindent`/`indent` for correct YAML indentation.
============================================================================
*/}}

{{/*
fp-bff.name — the chart's short name, overridable, truncated to the 63-char
DNS label limit. trimSuffix "-" avoids a trailing dash if truncation lands on one.
*/}}
{{- define "fp-bff.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
fp-bff.fullname — the fully-qualified resource name (release + chart). The
canonical Helm idiom:
  - fullnameOverride wins outright (operator forced an exact name).
  - else if the release name already contains the chart name, don't double it
    (release "fp-bff" + chart "fp-bff" → "fp-bff", not "fp-bff-fp-bff").
  - else join release + chart.
Always trunc 63 / trimSuffix "-" for DNS-1123 compliance.
*/}}
{{- define "fp-bff.fullname" -}}
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
fp-bff.chart — "name-version" sanitized for the helm.sh/chart label
(+ → _ because '+' is illegal in a label value; appears in SemVer build metadata).
*/}}
{{- define "fp-bff.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
fp-bff.selectorLabels — the IMMUTABLE identity labels. These go in BOTH a
Deployment's selector.matchLabels AND its pod template labels, and a Service's
selector. They must NEVER change for a given workload: a Deployment's selector is
immutable after creation, so adding/removing one forces a delete+recreate. Keep
this set minimal and stable (name + instance).
*/}}
{{- define "fp-bff.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fp-bff.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
fp-bff.labels — the FULL recommended label set (app.kubernetes.io/*). Superset of
selectorLabels plus version/managed-by/part-of/component, which are free to change
across releases (metadata, not selectors). part-of groups all Forgepoint services
under one logical platform for dashboards and queries.
*/}}
{{- define "fp-bff.labels" -}}
helm.sh/chart: {{ include "fp-bff.chart" . }}
{{ include "fp-bff.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: forgepoint
app.kubernetes.io/component: bff
{{- end -}}

{{/*
fp-bff.serviceAccountName — the SA to attach to pods. If serviceAccount.create,
default the name to the fullname; otherwise honor an explicit name, falling back
to "default" so the pod spec is always valid.
*/}}
{{- define "fp-bff.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "fp-bff.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
fp-bff.image — assemble repository:tag with appVersion as the tag fallback.
Centralized so every manifest references the exact same image string.
*/}}
{{- define "fp-bff.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}
