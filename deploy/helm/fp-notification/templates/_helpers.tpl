{{/*
============================================================================
_helpers.tpl — named template helpers (the chart's "functions")
============================================================================
Helm renders every file in templates/, but files/blocks defined with
`{{- define "name" -}}` are NOT rendered to manifests; they are reusable
templates other files pull in with `{{ include "name" . }}`. Centralizing
names and labels here means the chart has ONE source of truth for them. This
file is fp-auth's _helpers.tpl copied verbatim with only the template names
re-prefixed to `fp-notification` and the component label set to `notification`.

`include` (not the builtin `template`) is used at call sites because only
`include` can be piped into `nindent`/`indent` for correct YAML indentation.
============================================================================
*/}}

{{/*
fp-notification.name — the chart's short name, overridable, truncated to the
63-char DNS label limit. trimSuffix "-" avoids a trailing dash if truncation
lands on one.
*/}}
{{- define "fp-notification.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
fp-notification.fullname — the fully-qualified resource name (release + chart).
WHY this exact dance (the canonical Helm idiom):
  - fullnameOverride wins outright (operator forced an exact name).
  - else if the release name already contains the chart name, don't double it
    (release "fp-notification" + chart "fp-notification" → "fp-notification").
  - else join release + chart.
Always trunc 63 / trimSuffix "-" for DNS-1123 compliance.
*/}}
{{- define "fp-notification.fullname" -}}
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
fp-notification.chart — "name-version" sanitized for the helm.sh/chart label
(+ → _ because '+' is illegal in a label value; appears in SemVer build metadata).
*/}}
{{- define "fp-notification.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
fp-notification.selectorLabels — the IMMUTABLE identity labels.
These go in BOTH a Deployment's selector.matchLabels AND its pod template
labels, and a Service's selector. They must NEVER change for a given workload:
a Deployment's selector is immutable after creation, so adding/removing one of
these forces a delete+recreate. Keep this set minimal and stable (name + instance).
*/}}
{{- define "fp-notification.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fp-notification.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
fp-notification.labels — the FULL recommended label set (app.kubernetes.io/*).
Superset of selectorLabels plus version/managed-by/part-of/component, which are
free to change across releases (they are metadata, not selectors). part-of groups
all Forgepoint services under one logical platform for dashboards and queries.
*/}}
{{- define "fp-notification.labels" -}}
helm.sh/chart: {{ include "fp-notification.chart" . }}
{{ include "fp-notification.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: forgepoint
app.kubernetes.io/component: notification
{{- end -}}

{{/*
fp-notification.serviceAccountName — the SA to attach to pods.
If serviceAccount.create, default the name to the fullname; otherwise honor an
explicit name, falling back to "default" so the pod spec is always valid.
*/}}
{{- define "fp-notification.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "fp-notification.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
fp-notification.secretName — the Secret the Deployment envFrom's.
When the chart creates the Secret it is named after the fullname; when using an
externally-managed Secret (External Secrets / Sealed Secrets), honor
secrets.existingSecret. Either way the Deployment references ONE stable name.
*/}}
{{- define "fp-notification.secretName" -}}
{{- if .Values.secrets.create -}}
{{- include "fp-notification.fullname" . -}}
{{- else -}}
{{- required "secrets.create=false requires secrets.existingSecret to name the externally-managed Secret" .Values.secrets.existingSecret -}}
{{- end -}}
{{- end -}}

{{/*
fp-notification.image — assemble repository:tag with appVersion as the tag
fallback. Centralized so every manifest (and any future init container)
references the exact same image string.
*/}}
{{- define "fp-notification.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{/*
fp-notification.validateJwtSecret — fail-closed validation for the shared HMAC-SHA256 key.

WHY a helper (not a bare `required`): `required` only catches an empty/nil
value. The real security bug we are closing is a *predictable* key sneaking into
prod, so this also (a) rejects the committed dev-only placeholder string and
(b) enforces the >= 32-byte minimum pkg/auth demands for HS256. Centralizing it
here means every render path that needs the key gets the identical guard.

WHY: HS256 is symmetric — the same secret signs AND verifies. A leaked
or guessable key lets an attacker forge tokens for ANY identity, so the chart must
refuse to install with a weak/default key rather than ship a usable one. Returns
the validated key so call sites do `{{ include "fp-notification.validateJwtSecret" . | quote }}`.
*/}}
{{- define "fp-notification.validateJwtSecret" -}}
{{- $s := required "secrets.jwtSecret must be set (>=32 bytes; supply via --set, a non-committed -f, or secrets.existingSecret backed by an external secret store) and MUST match fp-auth's signing key" .Values.secrets.jwtSecret -}}
{{- if eq $s "dev-only-change-me-in-prod-min-32-bytes-of-entropy" -}}
{{- fail "secrets.jwtSecret is the committed dev-only placeholder; set a real, high-entropy >=32-byte HMAC-SHA256 key" -}}
{{- end -}}
{{- if lt (len $s) 32 -}}
{{- fail (printf "secrets.jwtSecret must be >=32 bytes for HS256 (got %d bytes)" (len $s)) -}}
{{- end -}}
{{- $s -}}
{{- end -}}
