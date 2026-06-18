// allowlist.go — the MODEL / PROVIDER ALLOW-LIST: policy-as-config at the data plane.
//
// ============================================================================
// WHAT THIS IS (AI governance, runtime tier — M7/L5)
// ============================================================================
//
// An allow-list is the gateway's RUNTIME governance gate: a config-driven set of
// the model names and/or provider kinds a deployment is PERMITTED to serve. Before
// the gateway routes a ChatCompletion to any backend, it checks the requested
// model+provider against this list and REJECTS anything not on it
// (codes.PermissionDenied, captured by the audit interceptor as a DENY).
//
// WHY an allow-list — three governance forces it satisfies (interview framing):
//
//  1. COST CONTROL. LLM spend is unbounded per request and varies 100x across
//     models (a frontier model can cost orders of magnitude more per token than a
//     local 135M one). Letting any caller name any model is an open cost spigot.
//     An allow-list caps the BLAST RADIUS to models the org has priced and budgeted
//     for — the same reason a cloud account restricts which instance types a team
//     can launch.
//
//  2. APPROVED-MODELS-ONLY COMPLIANCE. Regulated orgs must attest that production
//     traffic only ever touches models that passed review (licensing, data-
//     residency, safety eval, vendor DPA). The allow-list is the ENFORCEMENT POINT
//     for that attestation: a model that wasn't approved literally cannot be called,
//     and every attempt to call one is audited.
//
//  3. BLOCKING SHADOW MODELS. Without a gate, a team can quietly point a request at
//     an unvetted/experimental model (a "shadow model") that never went through
//     governance — invisible until an incident. The allow-list makes the set of
//     callable models EXPLICIT and DENIES the rest by default, so shadow usage
//     surfaces as audited DENYs instead of silent production traffic.
//
// ============================================================================
// WHERE IT IS ENFORCED — and WHY in the DOMAIN, not the handler/interceptor
// ============================================================================
//
// The check lives in the GatewayService use-case (ChatCompletion), BEFORE routing,
// returning a typed sentinel (ErrModelNotAllowed) that the handler maps 1:1 to
// codes.PermissionDenied. Two consequences fall out for free:
//
//   - The audit interceptor (wired into the gRPC chain) sees the PermissionDenied
//     code and records a DENY with the real actor — so an attempt to call a
//     disallowed model is AUDITED with no extra code in the use-case. Governance
//     denial and audit capture compose through the existing layers.
//   - The policy is unit-testable with zero infra (no HTTP, no Redis): construct an
//     AllowList, call Permits, assert. This file is pure (stdlib only), like every
//     other domain type.
//
// It is NOT an interceptor because the decision needs the request's MODEL/PROVIDER
// (business fields the generic interceptor doesn't decode), and it is NOT in the
// handler because "which models may run" is a domain POLICY, not wire mapping.
//
// ============================================================================
// EMPTY = ALLOW ALL (the dev default) — a deliberate, documented choice
// ============================================================================
//
// An EMPTY allow-list permits everything. This keeps local dev / CI frictionless
// (the stub + Ollama answer any model with no config), and matches the platform's
// other "0 = unlimited" knobs (the team token budget). The TRADEOFF — fail-OPEN by
// default — is acceptable for the data plane because production tightens it via
// config (FP_AI_ALLOWED_MODELS / FP_AI_ALLOWED_PROVIDERS) AND a Kyverno policy
// (deploy/policies) flags a wildcard-allow deployment in non-dev namespaces. So the
// permissive default is a dev convenience the K8s layer governs away in prod, not a
// silent prod hole. (Contrast the AUTH interceptor, which is default-DENY: an open
// model is a cost/compliance issue an operator opts to tighten; an open door is not.)
package domain

import "strings"

// AllowList is the runtime governance gate over which models/providers may be
// served. It is IMMUTABLE after construction (built once at wiring from config), so
// it needs no lock — Permits is a pure read over fixed sets, safe for concurrent use
// on the hot path.
//
// Two independent dimensions, BOTH must pass when configured:
//   - models:    permitted model NAMES (exact match, case-insensitive).
//   - providers: permitted provider KINDS (the pinned provider, when the caller pins).
//
// Either set empty = that dimension is unconstrained. Both empty = allow all.
type AllowList struct {
	models    map[string]struct{} // lower-cased model names; empty = any model.
	providers map[ProviderKind]struct{}
}

// NewAllowList builds the gate from the configured model names and provider kinds.
// Inputs are normalized (trimmed, lower-cased for models) and de-duplicated into
// sets for O(1) membership checks. Blank entries are dropped so a trailing comma in
// the env var (FP_AI_ALLOWED_MODELS="a,b,") doesn't create an empty "" entry that
// would never match anything. Passing two empty slices yields the ALLOW-ALL gate.
func NewAllowList(models []string, providers []ProviderKind) AllowList {
	a := AllowList{
		models:    make(map[string]struct{}, len(models)),
		providers: make(map[ProviderKind]struct{}, len(providers)),
	}
	for _, m := range models {
		m = strings.ToLower(strings.TrimSpace(m))
		if m == "" {
			continue
		}
		a.models[m] = struct{}{}
	}
	for _, p := range providers {
		if p == ProviderKindUnspecified {
			continue // "unspecified" is not a real provider to allow-list.
		}
		a.providers[p] = struct{}{}
	}
	return a
}

// AllowsAll reports whether the gate is the ALLOW-ALL gate (both dimensions empty).
// Wiring uses it to LOG the governance posture at boot ("allow-list: ALLOW ALL
// (dev)") so an operator can see at a glance whether the gateway is governed — the
// same fact the Kyverno policy checks at the K8s layer.
func (a AllowList) AllowsAll() bool {
	return len(a.models) == 0 && len(a.providers) == 0
}

// Permits reports whether a request for (model, provider) may be served.
//
// THE RULE (each dimension is an independent gate; an unconfigured dimension is a
// pass):
//
//	model    permitted  ⇔  models set empty            OR  model ∈ models
//	provider permitted  ⇔  providers set empty         OR  pinned == Unspecified
//	                                                   OR  pinned ∈ providers
//
// and the request Permits ⇔ BOTH dimensions permit. Notes on the edges:
//
//   - An EMPTY model with a non-empty models set is DENIED: if a deployment declares
//     an approved-models list, a request that names no model can't be implicitly
//     blessed (it would route to a provider's default model, side-stepping the gate).
//     The caller must name an approved model. (With no models configured, an empty
//     model is fine — the provider applies its default, as before.)
//   - provider == Unspecified means "no pin — route via the default failover order".
//     We do NOT reject that on the provider dimension: the MODEL gate still applies,
//     and the chosen backend is the operator-configured failover order (itself
//     governed by which providers are registered + the Kyverno key-from-Secret
//     policy). We only constrain a provider the CALLER explicitly pinned.
func (a AllowList) Permits(model string, provider ProviderKind) bool {
	return a.permitsModel(model) && a.permitsProvider(provider)
}

// permitsModel applies the model dimension. Case-insensitive exact match against the
// configured set; empty set = any model passes.
func (a AllowList) permitsModel(model string) bool {
	if len(a.models) == 0 {
		return true // model dimension unconstrained.
	}
	_, ok := a.models[strings.ToLower(strings.TrimSpace(model))]
	return ok
}

// permitsProvider applies the provider dimension. An unpinned request (Unspecified)
// passes — routing then follows the configured failover order. A pinned provider must
// be in the configured set; empty set = any pinned provider passes.
func (a AllowList) permitsProvider(provider ProviderKind) bool {
	if provider == ProviderKindUnspecified {
		return true // unpinned: the model gate + the registered failover order govern.
	}
	if len(a.providers) == 0 {
		return true // provider dimension unconstrained.
	}
	_, ok := a.providers[provider]
	return ok
}
