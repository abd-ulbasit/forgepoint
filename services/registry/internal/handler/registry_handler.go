// Package handler implements the gRPC server side of the Model Registry service.
// It is the outermost layer in Clean Architecture: it speaks proto (wire format)
// and delegates all business logic to the domain.RegistryService interface.
//
// ============================================================================
// HANDLER LAYER RESPONSIBILITIES (filled in the handler phase)
// ============================================================================
//
// The handler has exactly three jobs, none of which is business logic:
//
//  1. PROTO → DOMAIN: extract the CLIENT-OWNED fields from the request, build the
//     domain *Input, and SEPARATELY build the server-authoritative domain.Actor
//     from the validated auth claims in the context (NOT from the request body).
//     Keeping the Actor out of the Input is the structural mass-assignment guard:
//     a request can never carry an owner/team into a command.
//     Example: *registryv1.RegisterModelRequest → domain.RegisterModelInput + Actor.
//
//  2. CALL THE DOMAIN SERVICE: invoke the matching RegistryService method. The
//     handler holds the INTERFACE — it never knows the impl is Postgres-write /
//     Redis-read; it could be a test stub.
//
//  3. DOMAIN → PROTO + ERROR MAPPING: map the domain result back to a proto
//     Response, and translate domain sentinel errors to gRPC status codes:
//     ErrValidation             → codes.InvalidArgument
//     ErrModelNotFound /
//     ErrVersionNotFound       → codes.NotFound
//     ErrModelNameTaken /
//     ErrVersionExists         → codes.AlreadyExists
//     ErrModelArchived /
//     ErrIllegalTransition /
//     ErrVersionNotReady /
//     ErrInvalidStatusTransition → codes.FailedPrecondition
//     Also map the domain ModelStage/VersionStatus enums to/from the registryv1
//     proto enums EXPLICITLY (not by casting), per the decoupling discipline the
//     proto header documents — so the API enum can evolve independently of the
//     domain enum.
//
// ============================================================================
// EMBEDDING UnimplementedRegistryServiceServer — WHY THIS IS CORRECT, NOT A STUB
// ============================================================================
//
// protoc-gen-go-grpc generates RegistryServiceServer with a private method
// mustEmbedUnimplementedRegistryServiceServer(), which forces every implementation
// to embed UnimplementedRegistryServiceServer. That base implements EVERY RPC to
// return codes.Unimplemented. By embedding it, RegistryHandler:
//
//  1. Satisfies registryv1.RegistryServiceServer at compile time RIGHT NOW — the
//     server can be registered and started this phase.
//  2. Returns a real, correct codes.Unimplemented gRPC error (not a panic) for any
//     RPC we haven't overridden yet.
//  3. Is forward-compatible: adding a new RPC to the proto doesn't break this
//     service — the embedded base answers it until we implement it.
//
// This is the idiomatic Go/gRPC scaffold, a production-correct running server. The
// per-RPC method bodies (RegisterModel, PromoteVersion, GetModel, ...) are added in
// the handler phase by overriding each method on *RegistryHandler.
//
// FORWARD COMPATIBILITY OF gRPC SERVICE SERVERS:
//
//	The mustEmbedUnimplemented... private method forces embedding the Unimplemented
//	base; a new proto RPC then compiles fine on services that embed the base
//	(returning Unimplemented) instead of breaking them. That is gRPC's server-side
//	backward-compatibility story.
//
// ============================================================================
package handler

import (
	registryv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/registry/v1"
	"github.com/abd-ulbasit/forgepoint/services/registry/internal/domain"
)

// RegistryHandler is the gRPC server implementation for the Model Registry.
//
// It embeds registryv1.UnimplementedRegistryServiceServer to satisfy the full
// registryv1.RegistryServiceServer interface immediately, with all RPCs returning
// codes.Unimplemented until the handler phase fills them in.
//
// svc is the domain.RegistryService holding all business logic. In the handler
// phase each RPC method calls svc.RegisterModel/svc.PromoteVersion/etc. and
// converts between proto and domain types (plus building the Actor from auth
// claims). svc is nil during this scaffold phase (main.go passes nil); the embedded
// UnimplementedRegistryServiceServer methods never dereference svc, so this is safe.
type RegistryHandler struct {
	registryv1.UnimplementedRegistryServiceServer // embedded by VALUE (grpc-go requirement)
	svc                                           domain.RegistryService
}

// NewRegistryHandler creates a RegistryHandler with the given domain service.
//
// svc is nil during the scaffold phase (the Postgres WriteStore + Redis ReadStore +
// NATS emitter that NewRegistryService needs arrive in later phases). The nil is
// safe because the embedded UnimplementedRegistryServiceServer answers every RPC
// without touching svc. Once those adapters land, main.go constructs the real
// service and passes it here:
//
//	svc := domain.NewRegistryService(writeStore, readStore, emitter, domain.NewRealClock(), domain.NewUUIDGenerator())
//	registryv1.RegisterRegistryServiceServer(srv.GRPC, handler.NewRegistryHandler(svc))
//
// When the per-RPC methods are implemented, each will guard a nil service:
//   - UNARY RPCs (all of RegistryService's RPCs are unary) return (resp, err):
//     if h.svc == nil { return nil, status.Error(codes.Internal, "service not wired") }
//     During this scaffold phase the embedded base answers first, so svc is never
//     dereferenced.
func NewRegistryHandler(svc domain.RegistryService) *RegistryHandler {
	return &RegistryHandler{svc: svc}
}
