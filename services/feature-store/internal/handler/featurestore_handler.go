// Package handler implements the gRPC server side of the Feature Store service.
// It is the OUTERMOST ring of Clean Architecture: it speaks proto (wire format)
// and delegates all business logic to the domain.FeatureStoreService interface.
//
// ============================================================================
// HANDLER LAYER RESPONSIBILITIES (the anti-corruption layer)
// ============================================================================
//
// When the per-RPC methods are filled in (the handler phase, after the Postgres
// EventLog + Redis/Postgres view stores exist), each will do exactly three things:
//
//  1. PROTO → DOMAIN: extract + validate request fields, convert the proto
//     google.protobuf.Value feature maps into domain.FeatureValue (the tagged
//     union), and extract the authenticated caller into a domain.Principal from
//     the auth interceptor's TokenClaims — NEVER from request fields (the
//     mass-assignment guard the whole service depends on).
//
//  2. CALL THE DOMAIN SERVICE: invoke svc.DefineFeatureView / WriteFeatures /
//     GetOnlineFeatures / GetHistoricalFeatures / DeleteFeatureView / RebuildViews.
//     The handler holds the INTERFACE — it never knows whether the impl is backed
//     by Postgres+Redis or an in-memory test double.
//
//  3. DOMAIN → PROTO: map the domain result (FeatureView, FeatureVector, …) back
//     to the proto Response, mapping domain.FeatureValueType → the proto enum and
//     domain.FeatureValue → google.protobuf.Value. For RebuildViews (server-
//     streaming), each domain.RebuildProgress frame becomes a stream.Send of a
//     featurestorev1.RebuildViewsResponse.
//
// ERROR MAPPING (also the handler's job): translate the domain sentinels to gRPC
// status codes — ErrValidation/ErrSchemaViolation/ErrAsOfRequired/ErrBatchTooLarge
// → InvalidArgument, ErrViewNotFound → NotFound, ErrViewDeleted/ErrViewNameConflict
// → FailedPrecondition. The domain never imports grpc/codes; this layer is the
// only place that translation happens.
//
// ============================================================================
// EMBEDDING UnimplementedFeatureStoreServiceServer — CORRECT, NOT A STUB
// ============================================================================
//
// protoc-gen-go-grpc generates FeatureStoreServiceServer with a private method
// mustEmbedUnimplementedFeatureStoreServiceServer(), forcing every implementation
// to embed UnimplementedFeatureStoreServiceServer. That base implements every RPC
// (including the server-streaming RebuildViews) to return codes.Unimplemented.
// Embedding it means FeatureStoreHandler:
//
//  1. Satisfies featurestorev1.FeatureStoreServiceServer at compile time RIGHT NOW
//     — the server can be registered and started in this scaffold phase.
//  2. Returns codes.Unimplemented for any RPC not yet overridden — a real, correct
//     gRPC error, never a panic.
//  3. Is forward-compatible: adding a new RPC to the proto doesn't break this
//     service (the embedded base handles it) until we implement it.
//
// This is the IDIOMATIC Go/gRPC scaffold, a production-correct running server —
// not a placeholder. Per-RPC implementations land in the handler phase.
//
// INTERVIEW: "How does grpc-go ensure forward compatibility of service servers?"
//
//	The mustEmbed… private method forces embedding the Unimplemented base; a new
//	RPC compiles against existing servers that embed it (returning Unimplemented)
//	instead of breaking the build. That is gRPC's server-side compatibility story.
package handler

import (
	featurestorev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/featurestore/v1"
	"github.com/abd-ulbasit/forgepoint/services/feature-store/internal/domain"
)

// FeatureStoreHandler is the gRPC server implementation for the Feature Store.
//
// It embeds featurestorev1.UnimplementedFeatureStoreServiceServer to satisfy the
// full FeatureStoreServiceServer interface immediately (all RPCs return
// Unimplemented until the handler phase overrides them).
//
// svc is the domain.FeatureStoreService holding all business logic — the event-
// sourcing engine. In the handler phase each RPC method calls svc.* and converts
// proto↔domain. The svc field is nil during this scaffold phase (main.go passes
// nil); the embedded Unimplemented methods never dereference it, so this is safe.
type FeatureStoreHandler struct {
	featurestorev1.UnimplementedFeatureStoreServiceServer // embedded BY VALUE (grpc-go requirement)
	svc                                                   domain.FeatureStoreService
}

// NewFeatureStoreHandler constructs the handler with the given domain service.
//
// The svc parameter is nil during the scaffold phase because the domain service's
// real ADAPTERS (Postgres EventLog, Redis online store, Postgres offline store,
// the NATS publisher) are a later phase. Once those land, main.go constructs the
// real service and passes it here:
//
//	svc := domain.NewFeatureStoreService(eventLog, onlineStore, offlineStore, clock, idgen)
//	featurestorev1.RegisterFeatureStoreServiceServer(srv.GRPC, handler.NewFeatureStoreHandler(svc))
//
// The nil svc is safe now because the embedded UnimplementedFeatureStoreServiceServer
// answers every RPC without touching svc. When the per-RPC methods are written,
// each will guard a nil service — the guard differs by RPC kind:
//   - UNARY RPCs return (resp, err):
//     if h.svc == nil { return nil, status.Error(codes.Internal, "service not wired") }
//   - the SERVER-STREAMING RebuildViews returns only err (responses flow via
//     stream.Send), so there is no resp to return:
//     if h.svc == nil { return status.Error(codes.Internal, "service not wired") }
func NewFeatureStoreHandler(svc domain.FeatureStoreService) *FeatureStoreHandler {
	return &FeatureStoreHandler{svc: svc}
}
