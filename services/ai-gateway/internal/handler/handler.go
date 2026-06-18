// Package handler implements the gRPC server side of the AI Gateway. It is the
// outermost layer in Clean Architecture: it speaks proto (the wire format) and
// delegates all business logic to the domain.GatewayService interface.
//
// ============================================================================
// HANDLER RESPONSIBILITIES (the anti-corruption layer)
// ============================================================================
//
// Exactly three jobs, no business logic:
//
//  1. PROTO → DOMAIN: extract + VALIDATE request fields, build domain inputs.
//     CRITICAL SECURITY STEP for THIS service: the TEAM (the tenancy + budget key)
//     is taken from the VERIFIED auth claims in the context (injected by the auth
//     interceptor), NEVER from a request field — a client-supplied team would be
//     budget-theft (spend another team's allowance) or budget-evasion (claim a
//     team with a fat budget). See teamFromContext.
//
//  2. CALL the domain service (GatewayService) — the handler holds the INTERFACE,
//     never a concrete impl, so tests inject a fake with no Redis/NATS/HTTP.
//
//  3. DOMAIN → PROTO + ERROR MAPPING: stream domain deltas to the gRPC stream and
//     map domain sentinel errors to gRPC status codes (the failure contract):
//     ErrInvalidInput    → InvalidArgument
//     ErrBudgetExceeded  → ResourceExhausted
//     ErrNoProvider      → FailedPrecondition
//     ErrAllProvidersFailed → handled IN-STREAM (terminal error frame), the RPC
//     itself returns nil (the client already got a clean
//     terminal frame with finish_reason=ERROR).
//
// ============================================================================
// EMBEDDING UnimplementedAIGatewayServiceServer
// ============================================================================
//
// We embed aiv1.UnimplementedAIGatewayServiceServer so the handler satisfies the
// full server interface immediately and is forward-compatible. The four prompt RPCs
// (CreatePrompt/GetPrompt/ListPrompts/RenderPrompt) are now IMPLEMENTED (L3,
// prompt_rpcs.go) but remain conditionally available: when the prompt registry is
// wired (a database is configured) they serve real responses; when it is NOT wired
// (h.prompts == nil), each overriding method returns codes.Unimplemented itself —
// the SAME contract the embedded base would give. So the embed still buys forward-
// compatibility for any future RPC, while the prompt RPCs self-gate on the registry
// being present. ChatCompletion/ListProviders/GetUsage are implemented in rpcs.go.
package handler

import (
	aiv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/ai/v1"
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/prompt"
)

// AIGatewayHandler is the gRPC server implementation for the AI Gateway.
//
// It carries TWO domain services, each independently optional:
//   - svc (GatewayService): the hot-path chat/usage/providers logic. Always wired.
//   - prompts (prompt.PromptService): the L3 PROMPT REGISTRY, Postgres-backed. It is
//     wired ONLY when a database is configured (FP_DATABASE_URL). When nil, the four
//     prompt RPCs fall back to the embedded UnimplementedAIGatewayServiceServer base
//     (a real codes.Unimplemented), so the gateway runs chat-only with no database.
//     This is the DB SELF-GATING contract: a missing prompt DB degrades the prompt
//     surface, it never takes down the whole gateway.
type AIGatewayHandler struct {
	aiv1.UnimplementedAIGatewayServiceServer // embedded by value (forward-compat base).
	svc                                      domain.GatewayService
	prompts                                  prompt.PromptService // nil → prompt RPCs stay Unimplemented
}

// NewHandler builds the handler over the gateway service ONLY (no prompt registry).
// The four prompt RPCs return codes.Unimplemented via the embedded base. main.go uses
// this when no database is configured.
func NewHandler(svc domain.GatewayService) *AIGatewayHandler {
	return &AIGatewayHandler{svc: svc}
}

// NewHandlerWithPrompts builds the handler with BOTH the gateway service and the
// prompt registry live. main.go uses this when a database is configured. A nil
// prompts argument is equivalent to NewHandler (the prompt RPCs stay Unimplemented),
// so the wiring site can pass whatever it constructed without a branch.
func NewHandlerWithPrompts(svc domain.GatewayService, prompts prompt.PromptService) *AIGatewayHandler {
	return &AIGatewayHandler{svc: svc, prompts: prompts}
}
