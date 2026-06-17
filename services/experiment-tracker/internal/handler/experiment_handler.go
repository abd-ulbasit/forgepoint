// Package handler implements the gRPC server side of the Experiment Tracker
// service. It is the outermost layer in Clean Architecture: it speaks proto
// (wire format) and delegates all business logic to the domain.ExperimentService
// interface.
//
// ============================================================================
// HANDLER LAYER RESPONSIBILITIES (the anti-corruption boundary)
// ============================================================================
//
// The handler has exactly three jobs (filled in a LATER phase, RPC by RPC):
//
//  1. PROTO → DOMAIN: extract + validate fields from the incoming proto Request,
//     lift the SERVER-AUTHORITATIVE identity (Actor{UserID, Team}) from the auth
//     interceptor's TokenClaims (NOT from request fields — mass-assignment
//     guard), and build the domain input struct (e.g. *experimentv1.StartRunRequest
//     → domain.StartRunInput + Actor).
//
//  2. CALL DOMAIN SERVICE: invoke the ExperimentService method. The handler holds
//     an INTERFACE — it never knows whether the impl is backed by Postgres+NATS
//     or an in-memory test stub.
//
//  3. DOMAIN → PROTO: map the domain result back to a proto Response, converting
//     domain enums (RunStatus/RunSource) to their proto mirrors and time.Time to
//     google.protobuf.Timestamp. Translate domain sentinel errors to gRPC status
//     codes (ErrValidation→InvalidArgument, ErrRunNotFound→NotFound,
//     ErrRunNotRunning/ErrInvalidStatusTransition/ErrParamConflict→
//     FailedPrecondition, ErrExperimentNameExists→AlreadyExists,
//     ErrBatchTooLarge/ErrTooManyRuns→InvalidArgument).
//
// WHAT THE HANDLER DOES NOT DO: business logic (domain), SQL (repository), NATS
// publish/consume (events), buffering/back-pressure (events adapter).
//
// ============================================================================
// EMBEDDING UnimplementedExperimentTrackerServiceServer — CORRECT, NOT A STUB
// ============================================================================
//
// protoc-gen-go-grpc generates the server interface with a private method
// mustEmbedUnimplementedExperimentTrackerServiceServer(), which FORCES every
// implementation to embed UnimplementedExperimentTrackerServiceServer. That base
// implements every RPC to return codes.Unimplemented. Embedding it means this
// handler:
//
//  1. Satisfies experimentv1.ExperimentTrackerServiceServer at COMPILE TIME — the
//     server registers and runs RIGHT NOW (the scaffold is a real, running
//     server, not a placeholder).
//  2. Returns a correct codes.Unimplemented gRPC error for any RPC not yet
//     overridden — a real error to the client, never a panic.
//  3. Is FORWARD-COMPATIBLE: adding a new RPC to the proto doesn't break this
//     service — the embedded base answers it (Unimplemented) until we implement.
//
// INTERVIEW: "How does grpc-go guarantee forward compatibility of servers?" The
// mustEmbed… private method forces embedding the Unimplemented base; new RPCs
// added to the proto are handled (Unimplemented) by the base, so the service
// keeps compiling and serving. That is gRPC's server-side compatibility story.
//
// ============================================================================
package handler

import (
	experimentv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/experiment/v1"
	"github.com/abd-ulbasit/forgepoint/services/experiment-tracker/internal/domain"
)

// ExperimentHandler is the gRPC server implementation for the Experiment Tracker.
//
// It embeds experimentv1.UnimplementedExperimentTrackerServiceServer to satisfy
// the full server interface immediately, with every RPC returning
// codes.Unimplemented until later phases override them. svc is the
// domain.ExperimentService holding all business logic; each future RPC method
// converts proto↔domain and calls svc.
//
// The svc field is nil during the scaffold phase (main.go passes nil). The
// embedded Unimplemented methods NEVER dereference svc, so this is safe. Once the
// repositories, idempotency store, and event publisher exist, main.go constructs
// the real service and passes it here.
type ExperimentHandler struct {
	experimentv1.UnimplementedExperimentTrackerServiceServer // embedded by value (grpc-go convention)
	svc                                                      domain.ExperimentService
}

// NewExperimentHandler creates an ExperimentHandler with the given domain
// service.
//
// svc is nil during the scaffold phase because the Postgres repositories, the
// idempotency store, and the NATS event publisher are wired in later phases.
// Once those land, main.go passes the real service:
//
//	svc := domain.NewExperimentService(expRepo, runRepo, idemStore, publisher, domain.NewRealClock())
//	experimentv1.RegisterExperimentTrackerServiceServer(srv.GRPC, handler.NewExperimentHandler(svc))
//
// The nil svc is safe here because the embedded Unimplemented base answers all
// RPCs without touching svc. When the per-RPC methods are added, each UNARY
// method guards a nil service with:
//
//	if h.svc == nil { return nil, status.Error(codes.Internal, "service not wired") }
//
// (All Experiment Tracker RPCs are unary, so the streaming guard form — returning
// only the error — is not needed here.)
func NewExperimentHandler(svc domain.ExperimentService) *ExperimentHandler {
	return &ExperimentHandler{svc: svc}
}
