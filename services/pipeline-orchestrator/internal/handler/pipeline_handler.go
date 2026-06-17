// Package handler implements the gRPC server side of the Pipeline Orchestrator.
// It is the OUTERMOST adapter in Clean Architecture: it speaks proto (the wire
// format) and delegates ALL business logic to the domain.PipelineService.
//
// ============================================================================
// HANDLER LAYER RESPONSIBILITIES (proto ⇄ domain only)
// ============================================================================
//
// Three jobs, no more:
//
//  1. PROTO → DOMAIN: extract + validate fields from the incoming Request, build
//     domain input types (CreatePipelineInput, TriggerInput, ...) and the Actor
//     (from the auth interceptor's token claims — NEVER from request fields).
//
//  2. CALL THE DOMAIN SERVICE: invoke the PipelineService method. The handler
//     holds the INTERFACE, so it never knows whether a Postgres-backed engine or
//     a test stub is behind it.
//
//  3. DOMAIN → PROTO: map the domain result (PipelineDefinition, Execution) back
//     to the proto Response, and map domain sentinel errors to gRPC status codes
//     (ErrValidation → InvalidArgument, ErrPipelineNotFound → NotFound, ...).
//
// WHAT THE HANDLER DOES NOT DO: business logic (domain), the saga/DAG scheduling
// (domain engine), SQL (repository), or NATS publishing (events adapter).
//
// ============================================================================
// EMBEDDING UnimplementedPipelineOrchestratorServiceServer — CORRECT, NOT A STUB
// ============================================================================
//
// protoc-gen-go-grpc emits a private marker method
// (mustEmbedUnimplementedPipelineOrchestratorServiceServer) that forces every
// implementation to embed UnimplementedPipelineOrchestratorServiceServer. That
// base implements EVERY RPC to return codes.Unimplemented. Embedding it means
// PipelineHandler:
//
//  1. Satisfies pipelinev1.PipelineOrchestratorServiceServer at COMPILE TIME —
//     the server registers and runs RIGHT NOW (this scaffold phase).
//  2. Returns a real codes.Unimplemented gRPC error (not a panic) for any RPC we
//     have not written yet.
//  3. Is FORWARD-COMPATIBLE: adding a new RPC to the proto doesn't break this
//     service — the embedded base answers it until we implement it.
//
// This is the idiomatic Go/gRPC scaffold, a production-correct running server.
// The per-RPC method bodies (CreatePipeline, TriggerExecution, WatchExecution,
// ...) land in the handler phase once the Postgres repos + executor adapters
// exist; each will guard a nil svc and translate proto ⇄ domain.
//
// WatchExecution is the one SERVER-STREAMING RPC: its signature is
//
//	WatchExecution(*WatchExecutionRequest, grpc.ServerStreamingServer[WatchExecutionResponse]) error
//
// (no response return — updates flow via stream.Send). The embedded base returns
// Unimplemented for it too until the handler phase bridges the domain's execution
// state to the stream.
// ============================================================================
package handler

import (
	pipelinev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/pipeline/v1"
	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/domain"
)

// PipelineHandler is the gRPC server implementation for the Pipeline
// Orchestrator. It embeds the generated Unimplemented base (by VALUE, per the
// grpc-go contract) to satisfy the full service interface immediately, and holds
// the domain.PipelineService that the per-RPC bodies will delegate to.
//
// svc is nil during the scaffold phase (main.go passes nil): the embedded
// Unimplemented methods never dereference it, so a running server returns
// codes.Unimplemented for every RPC. Once the domain engine is wired with real
// repos/executors, main constructs the service and passes it here.
type PipelineHandler struct {
	pipelinev1.UnimplementedPipelineOrchestratorServiceServer // embedded by value (grpc-go contract)
	svc                                                       domain.PipelineService
}

// NewPipelineHandler builds a PipelineHandler around the given domain service.
//
// The svc parameter is nil during the scaffold phase — safe because the embedded
// UnimplementedPipelineOrchestratorServiceServer answers every RPC without
// touching svc. When the per-RPC methods are implemented they will guard svc:
//   - UNARY RPCs return (resp, err): if h.svc == nil { return nil, status.Error(codes.Internal, "service not wired") }
//   - STREAMING WatchExecution returns only err: if h.svc == nil { return status.Error(codes.Internal, "service not wired") }
//
// (All current RPCs except WatchExecution are unary; both guard forms are noted
// so the pattern is correct when streaming is implemented.)
func NewPipelineHandler(svc domain.PipelineService) *PipelineHandler {
	return &PipelineHandler{svc: svc}
}
