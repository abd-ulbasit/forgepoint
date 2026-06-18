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
// full server interface immediately and is forward-compatible: the four prompt RPCs
// (CreatePrompt/GetPrompt/ListPrompts/RenderPrompt) are L3 work and return the
// embedded base's codes.Unimplemented until then — a real, correct gRPC error, never
// a panic. ChatCompletion/ListProviders/GetUsage are implemented below.
package handler

import (
	aiv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/ai/v1"
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/domain"
)

// AIGatewayHandler is the gRPC server implementation for the AI Gateway.
type AIGatewayHandler struct {
	aiv1.UnimplementedAIGatewayServiceServer // embedded by value (forward-compat base).
	svc                                      domain.GatewayService
}

// NewHandler builds the handler over a domain service. svc must be non-nil for the
// implemented RPCs; the embedded base answers the unimplemented prompt RPCs.
func NewHandler(svc domain.GatewayService) *AIGatewayHandler {
	return &AIGatewayHandler{svc: svc}
}
