// errors.go — sentinel errors owned by the Inference Gateway domain.
//
// ============================================================================
// WHY EACH ERROR NAMES A RESILIENCE PATTERN FIRING
// ============================================================================
//
// Unlike a CRUD service whose errors are mostly "not found / invalid input",
// this gateway's failure vocabulary IS its resilience story: each sentinel
// corresponds to one pattern deciding to reject, and each maps to a distinct
//   (a) operational response,
//   (b) gRPC status code at the edge, and
//   (c) events.InferenceFailureReason on the async bus.
//
// The handler is the single translation point. The mapping is:
//
//	ErrNoRoute       → NOT_FOUND          → NO_ROUTE        (model not routable)
//	ErrRateLimited   → RESOURCE_EXHAUSTED → RATE_LIMITED    (token bucket empty)
//	ErrQuotaExceeded → RESOURCE_EXHAUSTED → QUOTA_EXCEEDED  (team quota blocked)
//	ErrCircuitOpen   → UNAVAILABLE        → CIRCUIT_OPEN    (breaker tripped)
//	ErrUpstream      → UNAVAILABLE        → UPSTREAM_ERROR  (backend returned error)
//	ErrTimeout       → DEADLINE_EXCEEDED  → TIMEOUT         (backend too slow)
//	ErrInvalidInput  → INVALID_ARGUMENT   → INVALID_INPUT   (bad tensors/shape)
//
// WHY sentinels (not ad-hoc fmt.Errorf strings): callers (the handler, the
// metrics layer, the event publisher) must branch on the CLASS of failure with
// errors.Is — a free-form string can't be matched reliably and a typo silently
// mis-routes the status code. Each is wrapped with %w where a specific message
// is useful, so callers still get detail AND a matchable identity.
package domain

import "errors"

var (
	// ErrNoRoute means the model has no eligible (active) target — it isn't
	// routable. Could be never-deployed, fully drained, archived, or every
	// target quarantined by its breaker. Edge: NOT_FOUND.
	ErrNoRoute = errors.New("inference: no route for model")

	// ErrRateLimited means the per-principal token bucket had no token available.
	// This is the RATE LIMITING pattern protecting shared capacity from a single
	// noisy tenant. Edge: RESOURCE_EXHAUSTED (HTTP 429). The caller should back
	// off and retry; it is a transient, self-clearing condition.
	ErrRateLimited = errors.New("inference: rate limited")

	// ErrQuotaExceeded means the caller's TEAM is flagged over-quota in the
	// (eventually consistent) quota cache flipped by the QuotaExceeded event.
	// Distinct from ErrRateLimited: rate limiting is short-term throughput
	// shaping; quota is a billing-period cap. Both map to RESOURCE_EXHAUSTED at
	// the edge but to different InferenceFailureReason values for billing/alerts.
	ErrQuotaExceeded = errors.New("inference: team quota exceeded")

	// ErrCircuitOpen means the chosen backend's circuit breaker is OPEN (or
	// HALF_OPEN and the probe slot was taken) — the gateway fails fast WITHOUT
	// touching a known-sick backend. This is the CIRCUIT BREAKER pattern. Edge:
	// UNAVAILABLE. An operator reads this as "that version's backend is unhealthy".
	ErrCircuitOpen = errors.New("inference: circuit open")

	// ErrUpstream means the model-serving backend was reached but returned an
	// error (it ran and failed, or returned a malformed result). Counts as a
	// breaker failure. Edge: UNAVAILABLE → UPSTREAM_ERROR.
	ErrUpstream = errors.New("inference: upstream backend error")

	// ErrTimeout means the backend call exceeded its deadline. Counts as a
	// breaker failure (a hung backend is as bad as a failing one). Edge:
	// DEADLINE_EXCEEDED → TIMEOUT. Distinguished from ErrUpstream because a
	// bulk-scoring client's per-item retry policy differs (retry a TIMEOUT,
	// be cautious re-running a possibly-side-effecting UPSTREAM_ERROR).
	ErrTimeout = errors.New("inference: upstream timeout")

	// ErrInvalidInput means the request failed VALIDATION before any backend
	// call — missing model name, empty inputs, a tensor whose byte length does
	// not match product(shape)*elemsize. Does NOT count as a breaker failure
	// (the backend is innocent; the client sent garbage). Edge: INVALID_ARGUMENT.
	ErrInvalidInput = errors.New("inference: invalid input")

	// ErrRouteValidation means a control-plane write (UpsertRoute / SetTrafficSplit)
	// violated a routing invariant — weights don't sum to 10000, a referenced
	// version doesn't exist, a duplicate version, or a negative/over-cap weight.
	// Edge: INVALID_ARGUMENT. Kept separate from ErrInvalidInput (a DATA-plane
	// fault) so control-plane validation failures read clearly in audit logs.
	ErrRouteValidation = errors.New("inference: route validation failed")
)
