// errors.go — the AI Gateway domain's sentinel errors.
//
// These are TYPED SENTINELS the use-case returns and the handler maps 1:1 to gRPC
// status codes (the service's failure contract). Defining them as package-level
// vars (not fmt.Errorf strings) lets callers errors.Is() them and the handler do a
// single switch — the same discipline as inference-gateway's domain errors. The
// domain never imports google.golang.org/grpc; the handler owns the code mapping.
package domain

import "errors"

var (
	// ErrInvalidInput — the request is malformed (no messages, etc.). → InvalidArgument.
	ErrInvalidInput = errors.New("ai-gateway: invalid input")

	// ErrBudgetExceeded — the caller's TEAM is over its token budget for the window.
	// → ResourceExhausted. This is the budget gate firing BEFORE any provider call,
	// so the platform stops doing unbillable LLM work promptly.
	ErrBudgetExceeded = errors.New("ai-gateway: team token budget exceeded")

	// ErrNoProvider — no configured provider could serve (the pinned provider is
	// unknown/disabled, or the default order is empty). → FailedPrecondition. Distinct
	// from ErrAllProvidersFailed: here we never even attempted a live call.
	ErrNoProvider = errors.New("ai-gateway: no eligible provider")

	// ErrAllProvidersFailed — every eligible provider was skipped (circuit OPEN) or
	// errored. The stream's terminal frame carries FinishReasonError; the unary
	// caller sees → Unavailable. This is the FAILOVER-exhausted outcome.
	ErrAllProvidersFailed = errors.New("ai-gateway: all providers failed")

	// ErrModelNotAllowed — the requested model/provider is not on the runtime
	// governance ALLOW-LIST (see allowlist.go). → PermissionDenied. The handler maps
	// it to codes.PermissionDenied, which the audit interceptor records as a DENY —
	// so a request for a disallowed (e.g. shadow / unpriced / unapproved) model is
	// REJECTED before any provider call AND audited with the real actor. Distinct from
	// ErrNoProvider (a routing/availability miss): this is a POLICY denial — the model
	// COULD be served, but governance forbids it.
	ErrModelNotAllowed = errors.New("ai-gateway: model or provider not permitted by allow-list")
)
