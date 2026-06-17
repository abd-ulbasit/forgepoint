// Package handler implements the gRPC server side of the Model Serving service.
// It is the outermost layer in Clean Architecture: it speaks proto (wire format)
// and delegates all business logic to the domain.ServingService interface.
//
// ============================================================================
// HANDLER LAYER RESPONSIBILITIES (the anti-corruption layer)
// ============================================================================
//
// The handler has exactly three jobs, and NO business logic:
//
//  1. PROTO → DOMAIN: extract + validate fields from the incoming proto Request
//     and build domain input types (e.g. *servingv1.PredictRequest →
//     domain.PredictInput, including the proto TensorData → domain.Tensor
//     conversion). The domain never sees a proto type.
//
//  2. CALL THE DOMAIN SERVICE: invoke the matching ServingService method. The
//     handler holds a domain.ServingService INTERFACE — it never knows whether
//     the implementation is the real registry (backed by the ONNX runtime + MinIO
//     fetcher) or an in-memory test stub.
//
//  3. DOMAIN → PROTO: map the domain result back to a proto Response, and map
//     domain SENTINEL ERRORS to gRPC status codes (ErrModelNotReady →
//     FailedPrecondition, ErrModelNotFound → NotFound, ErrArtifactURINotAllowed
//     → PermissionDenied, ErrValidation → InvalidArgument, etc.).
//
// ============================================================================
// EMBEDDING UnimplementedModelServingServiceServer — WHY THIS IS CORRECT
// ============================================================================
//
// protoc-gen-go-grpc generates ModelServingServiceServer with a private method
// mustEmbedUnimplementedModelServingServiceServer(), forcing every
// implementation to embed UnimplementedModelServingServiceServer. That embedded
// base implements EVERY RPC to return codes.Unimplemented. Embedding it makes
// ServingHandler:
//
//  1. satisfy the ModelServingServiceServer interface AT COMPILE TIME — the
//     server can be registered and started RIGHT NOW (this scaffold phase), and
//  2. return a real, correct codes.Unimplemented for any RPC we have not yet
//     overridden (no panic), and
//  3. stay forward-compatible: a new RPC added to the proto is handled by the
//     embedded base (Unimplemented) until we implement it, instead of breaking
//     the build.
//
// This is the IDIOMATIC Go/gRPC scaffold — a production-correct running server,
// not a placeholder. The per-RPC implementations (Predict, LoadModel, …) land in
// the handler phase, each overriding a method on *ServingHandler and doing the
// proto↔domain conversion described above.
//
// INTERVIEW: "How does grpc-go give you forward-compatible service servers?"
//
//	The mustEmbed… private method forces embedding the Unimplemented base.
//	Adding an RPC to the proto can't break a server that embeds the base — the
//	base answers the new method with Unimplemented until you implement it. That
//	is gRPC's server-side backward-compatibility story.
//
// ============================================================================
package handler

import (
	servingv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/serving/v1"
	"github.com/abd-ulbasit/forgepoint/services/model-serving/internal/domain"
)

// ServingHandler is the gRPC server implementation for the Model Serving service.
//
// It embeds servingv1.UnimplementedModelServingServiceServer to satisfy the full
// servingv1.ModelServingServiceServer interface immediately, with all RPCs
// returning codes.Unimplemented until the handler phase fills them in.
//
// svc is the domain.ServingService holding the model-runtime-registry business
// logic. In the handler phase each RPC method will call svc.Predict,
// svc.LoadModel, etc. and convert between proto and domain types. svc is nil
// during this scaffold phase (main.go passes nil); the embedded Unimplemented
// methods never dereference svc, so this is safe.
type ServingHandler struct {
	servingv1.UnimplementedModelServingServiceServer // embedded by value (grpc-go requirement)
	svc                                              domain.ServingService
}

// NewServingHandler creates a ServingHandler with the given domain service.
//
// The svc parameter is nil during the scaffold phase because the domain service
// is wired only once the runtime (ONNX) and storage (MinIO fetcher) ADAPTERS
// exist to inject as its ports. Once those land, main.go constructs the real
// service:
//
//	eng := runtime.NewONNXEngine(...)
//	fetch := storage.NewMinIOFetcher(...)
//	svc := domain.NewServingService(domain.ServiceConfig{Engine: eng, Fetcher: fetch, Clock: realClock{}, ...})
//	servingv1.RegisterModelServingServiceServer(srv.GRPC, handler.NewServingHandler(svc))
//
// The nil svc is safe here because the embedded UnimplementedModelServingService
// Server handles all RPCs without touching svc. When the per-RPC methods are
// implemented, each will guard a nil service:
//   - UNARY RPCs (Predict, LoadModel, …) return (resp, err):
//     if h.svc == nil { return nil, status.Error(codes.Internal, "service not wired") }
//   - the STREAMING RPC (StreamPredict) returns only err (responses flow via
//     stream.Send), so its guard returns just the error:
//     if h.svc == nil { return status.Error(codes.Internal, "service not wired") }
//
// During this scaffold phase the embedded Unimplemented base answers first, so
// svc is never dereferenced.
func NewServingHandler(svc domain.ServingService) *ServingHandler {
	return &ServingHandler{svc: svc}
}
