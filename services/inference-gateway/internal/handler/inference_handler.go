// Package handler implements the gRPC server side of the Inference Gateway.
// It is the outermost layer in Clean Architecture: it speaks proto (wire format)
// and delegates all business logic to the domain.InferenceService interface.
//
// ============================================================================
// HANDLER LAYER RESPONSIBILITIES (the anti-corruption layer)
// ============================================================================
//
// Exactly three jobs, no business logic:
//
//  1. PROTO → DOMAIN: extract + VALIDATE request fields, build domain input
//     types. CRITICAL security step for THIS service: the principal
//     (api_key_id, team) is taken from the VERIFIED auth claims in the context
//     (injected by the auth interceptor), NEVER from a request field — a
//     client-supplied principal would be account-takeover-for-billing. Likewise
//     version_override is only forwarded for callers holding the elevated scope.
//
//  2. CALL the domain service (InferenceService) — the handler holds the
//     interface, never a concrete impl, so tests can inject a mock with no Redis,
//     no backend, no NATS.
//
//  3. DOMAIN → PROTO + ERROR MAPPING: map the domain result to the proto
//     Response, and map the domain sentinel errors to gRPC status codes. The
//     mapping is the service's failure contract (NO_ROUTE→NotFound,
//     RATE_LIMITED/QUOTA/BULKHEAD→ResourceExhausted, CIRCUIT_OPEN/UPSTREAM→
//     Unavailable, TIMEOUT→DeadlineExceeded, INVALID_INPUT→InvalidArgument).
//
// ============================================================================
// EMBEDDING UnimplementedInferenceGatewayServiceServer — WHY THIS IS CORRECT
// ============================================================================
//
// protoc-gen-go-grpc generates the server interface with a private method
// mustEmbedUnimplementedInferenceGatewayServiceServer(), forcing every impl to
// embed UnimplementedInferenceGatewayServiceServer. That base implements EVERY
// RPC returning codes.Unimplemented. Embedding it means InferenceHandler:
//
//  1. Satisfies inferencev1.InferenceGatewayServiceServer at compile time NOW —
//     the server registers and runs in this scaffold phase.
//  2. Returns codes.Unimplemented for any RPC we haven't written yet — a real,
//     correct gRPC error, never a panic.
//  3. Is forward-compatible: adding an RPC to the proto doesn't break this server
//     (the base handles the new method until we implement it).
//
// This is the IDIOMATIC Go/gRPC scaffold — a production-correct, running server,
// not a placeholder. Per-RPC implementations (Predict converting proto tensors ↔
// domain, the route admin RPCs, the circuit observability reads) land once the
// repository/event adapters exist to construct a real domain service.
//
// FORWARD COMPATIBILITY OF gRPC SERVICE SERVERS: the
// mustEmbed... private method forces embedding the Unimplemented base; a new RPC
// in the proto compiles fine for servers that embed it (returning Unimplemented)
// instead of breaking the build.
// ============================================================================
package handler

import (
	inferencev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/inference/v1"
	"github.com/abd-ulbasit/forgepoint/services/inference-gateway/internal/domain"
)

// InferenceHandler is the gRPC server implementation for the Inference Gateway.
//
// It embeds inferencev1.UnimplementedInferenceGatewayServiceServer to satisfy the
// full server interface immediately, with all RPCs returning codes.Unimplemented
// until they are individually implemented.
//
// svc is the domain.InferenceService holding all business logic (the resilience
// stack). It is nil during the scaffold phase (main.go passes nil); the embedded
// Unimplemented methods never dereference svc, so this is safe. Once the Redis
// route store, the model-serving gRPC client, the rate limiter, and the NATS
// publisher adapters exist, main.go constructs the real service and passes it
// here, and each RPC method below is overridden to call svc.
type InferenceHandler struct {
	inferencev1.UnimplementedInferenceGatewayServiceServer // embedded by value (see grpc-go note above)
	svc                                                    domain.InferenceService
}

// NewInferenceHandler creates an InferenceHandler with the given domain service.
//
// svc is nil during the scaffold phase because the adapters it depends on (Redis
// RouteStore + RateLimiter, the model-serving client, the NATS EventPublisher)
// are wired in later tasks. The nil is safe here: the embedded Unimplemented base
// answers every RPC without touching svc.
//
// When the per-RPC methods are implemented, each guards against a nil service per
// its kind:
//   - UNARY RPCs (Predict, BatchPredict, the route/circuit RPCs) return (resp,
//     err), so: if h.svc == nil { return nil, status.Error(codes.Internal, "service not wired") }
//   - the SERVER-STREAMING RPC (StreamPredict) returns only err (responses flow
//     via stream.Send), so: if h.svc == nil { return status.Error(codes.Internal, "service not wired") }
//
// During this scaffold phase the embedded Unimplemented base answers first, so
// svc is never dereferenced.
func NewInferenceHandler(svc domain.InferenceService) *InferenceHandler {
	return &InferenceHandler{svc: svc}
}
