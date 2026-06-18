{{/*
============================================================================
_helpers.tpl — named template helpers (the chart's "functions")
============================================================================
Helm renders every file in templates/, but files/blocks defined with
`{{- define "name" -}}` are NOT rendered to manifests; they are reusable
templates other files pull in with `{{ include "name" . }}`. Centralizing
names and labels here means the chart has ONE source of truth for them. This
file is a direct copy of fp-auth's helpers with the template names rebased to
`fp-ai-gateway.*` and the component label set to ai-gateway —
nothing else differs (that uniformity is the point).

`include` (not the builtin `template`) is used at call sites because only
`include` can be piped into `nindent`/`indent` for correct YAML indentation.
============================================================================
*/}}

{{/*
fp-ai-gateway.name — the chart's short name, overridable, truncated to the
63-char DNS label limit. trimSuffix "-" avoids a trailing dash if truncation
lands on one.
*/}}
{{- define "fp-ai-gateway.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
fp-ai-gateway.fullname — the fully-qualified resource name (release + chart).
WHY this exact dance (the canonical Helm idiom):
  - fullnameOverride wins outright (operator forced an exact name).
  - else if the release name already contains the chart name, don't double it
    (release "fp-ai-gateway" + chart "fp-ai-gateway" →
    "fp-ai-gateway", not "fp-ai-gateway-fp-ai-gateway").
  - else join release + chart.
Always trunc 63 / trimSuffix "-" for DNS-1123 compliance. NOTE: "fp-inference-
gateway" is 20 chars, so release+chart joins stay comfortably under 63 — the
trunc is defensive, mirroring the reference chart verbatim.
*/}}
{{- define "fp-ai-gateway.fullname" -}}
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
fp-ai-gateway.chart — "name-version" sanitized for the helm.sh/chart label
(+ → _ because '+' is illegal in a label value; appears in SemVer build metadata).
*/}}
{{- define "fp-ai-gateway.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
fp-ai-gateway.selectorLabels — the IMMUTABLE identity labels.
These go in BOTH a Deployment's selector.matchLabels AND its pod template
labels, and a Service's selector. They must NEVER change for a given workload:
a Deployment's selector is immutable after creation, so adding/removing one of
these forces a delete+recreate. Keep this set minimal and stable (name + instance).
*/}}
{{- define "fp-ai-gateway.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fp-ai-gateway.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
fp-ai-gateway.labels — the FULL recommended label set (app.kubernetes.io/*).
Superset of selectorLabels plus version/managed-by/part-of/component, which are
free to change across releases (they are metadata, not selectors). part-of groups
all Forgepoint services under one logical platform for dashboards and queries.
*/}}
{{- define "fp-ai-gateway.labels" -}}
helm.sh/chart: {{ include "fp-ai-gateway.chart" . }}
{{ include "fp-ai-gateway.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: forgepoint
app.kubernetes.io/component: ai-gateway
{{- end -}}

{{/*
fp-ai-gateway.serviceAccountName — the SA to attach to pods.
If serviceAccount.create, default the name to the fullname; otherwise honor an
explicit name, falling back to "default" so the pod spec is always valid.
*/}}
{{- define "fp-ai-gateway.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "fp-ai-gateway.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
fp-ai-gateway.secretName — the Secret the Deployment envFrom's.
When the chart creates the Secret it is named after the fullname; when using an
externally-managed Secret (External Secrets / Sealed Secrets), honor
secrets.existingSecret. Either way the Deployment references ONE stable name.
*/}}
{{- define "fp-ai-gateway.secretName" -}}
{{- if .Values.secrets.create -}}
{{- include "fp-ai-gateway.fullname" . -}}
{{- else -}}
{{- required "secrets.create=false requires secrets.existingSecret to name the externally-managed Secret" .Values.secrets.existingSecret -}}
{{- end -}}
{{- end -}}

{{/*
fp-ai-gateway.image — assemble repository:tag with appVersion as the tag
fallback. Centralized so every manifest (and any future init container)
references the exact same image string.
*/}}
{{- define "fp-ai-gateway.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{/*
fp-ai-gateway.validateJwtSecret — fail-closed validation for the shared HMAC-SHA256 key.

WHY a helper (not a bare `required`): `required` only catches an empty/nil
value. The real security bug we are closing is a *predictable* key sneaking into
prod, so this also (a) rejects the committed dev-only placeholder string and
(b) enforces the >= 32-byte minimum pkg/auth demands for HS256. Centralizing it
here means every render path that needs the key gets the identical guard.

INTERVIEW NOTE: HS256 is symmetric — the same secret signs AND verifies. A leaked
or guessable key lets an attacker forge tokens for ANY identity, so the chart must
refuse to install with a weak/default key rather than ship a usable one. Returns
the validated key so call sites do `{{ include "fp-ai-gateway.validateJwtSecret" . | quote }}`.
*/}}
{{- define "fp-ai-gateway.validateJwtSecret" -}}
{{- $s := required "secrets.jwtSecret must be set (>=32 bytes; supply via --set, a non-committed -f, or secrets.existingSecret backed by an external secret store) and MUST match fp-auth's signing key" .Values.secrets.jwtSecret -}}
{{- if eq $s "dev-only-change-me-in-prod-min-32-bytes-of-entropy" -}}
{{- fail "secrets.jwtSecret is the committed dev-only placeholder; set a real, high-entropy >=32-byte HMAC-SHA256 key" -}}
{{- end -}}
{{- if lt (len $s) 32 -}}
{{- fail (printf "secrets.jwtSecret must be >=32 bytes for HS256 (got %d bytes)" (len $s)) -}}
{{- end -}}
{{- $s -}}
{{- end -}}
