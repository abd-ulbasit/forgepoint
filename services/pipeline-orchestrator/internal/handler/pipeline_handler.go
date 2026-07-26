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
//  1. Satisfies pipelinev1.PipelineOrchestratorServiceServer at COMPILE TIME.
//  2. Is FORWARD-COMPATIBLE: adding a new RPC to the proto doesn't break this
//     service — the embedded base answers it until we implement it.
//
// Every RPC method below OVERRIDES that base. The first thing each does is the
// NIL-SVC GUARD (see below): if a binary is started before the real domain
// service is wired (the repo phase), the handler returns codes.Unimplemented
// rather than panicking on a nil-pointer dereference. Once main.go passes a real
// service, the guard is a no-op and the method runs for real.
//
// WatchExecution is the one SERVER-STREAMING RPC: its signature is
//
//	WatchExecution(*WatchExecutionRequest, grpc.ServerStreamingServer[WatchExecutionResponse]) error
//
// (no response return — updates flow via stream.Send). Its guard returns the
// status error directly (no nil response), and it bridges domain Execution state
// to the stream while honoring ctx cancellation.
// ============================================================================
package handler

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pipelinev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/pipeline/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/domain"
)

// PipelineHandler is the gRPC server implementation for the Pipeline
// Orchestrator. It embeds the generated Unimplemented base (by VALUE, per the
// grpc-go contract) to satisfy the full service interface, and holds the
// domain.PipelineService that the per-RPC bodies delegate to.
//
// svc may be nil during the scaffold phase (main.go passes nil): every RPC
// guards against that and returns codes.Unimplemented, so a not-yet-fully-wired
// binary is safe to run. Once the domain engine is wired with real
// repos/executors, main constructs the service and passes it here.
type PipelineHandler struct {
	pipelinev1.UnimplementedPipelineOrchestratorServiceServer // embedded by value (grpc-go contract)
	svc                                                       domain.PipelineService
}

// NewPipelineHandler builds a PipelineHandler around the given domain service.
//
// svc may be nil during the scaffold phase — safe because every RPC method
// starts with the nil-svc guard. When main wires the real service, the guard is
// inert.
func NewPipelineHandler(svc domain.PipelineService) *PipelineHandler {
	return &PipelineHandler{svc: svc}
}

// ============================================================================
// IDENTITY EXTRACTION — server-authoritative Actor from the auth interceptor
// ============================================================================
//
// actorFromContext builds the domain.Actor from the Claims the pkg/grpcutil auth
// interceptor stamped onto the context after validating the caller's JWT/API
// key. This is the SINGLE place identity enters the domain.
//
// WHY this matters (anti-mass-assignment / anti-IDOR):
//   - CreatedBy / TriggeredBy / Team are SERVER-AUTHORITATIVE. They come from the
//     verified token, NEVER from a request field. A client cannot author a
//     pipeline "as" another user or list another team's runs by passing a team
//     string — the request types don't even carry those fields, and the handler
//     never reads identity from the request.
//   - A request with no claims is UNAUTHENTICATED. We fail closed (return an
//     error) rather than fall through with an empty Actor, which would otherwise
//     read/write the "" team — a tenancy hole. The auth interceptor normally
//     rejects unauthenticated calls before we get here; this is defense in depth.
func actorFromContext(ctx context.Context) (domain.Actor, error) {
	claims, ok := grpcutil.ClaimsFromContext(ctx)
	if !ok || claims == nil {
		// No verified identity on the context → fail closed. We do NOT leak
		// whether auth is misconfigured vs. the token was missing; a flat
		// "unauthenticated" is all the client gets.
		return domain.Actor{}, status.Error(codes.Unauthenticated, "missing or invalid authentication")
	}
	// A token with no team cannot be tenancy-scoped; treat it as unauthenticated
	// rather than silently scoping to the empty team (which could match
	// untenanted rows). Subject (user id) is required for audit stamping.
	if claims.UserID == "" || claims.Team == "" {
		return domain.Actor{}, status.Error(codes.Unauthenticated, "authentication is missing required identity")
	}
	return domain.Actor{
		Subject: claims.UserID,
		Team:    claims.Team,
	}, nil
}

// ============================================================================
// TEMPLATE MANAGEMENT RPCs
// ============================================================================

// CreatePipeline authors a new pipeline template.
//
// FLOW: nil-svc guard → identity from claims → validate the request (anti-mass-
// assignment: we read ONLY name/type/steps/idempotency_key) → proto→domain →
// service → domain→proto / error mapping.
func (h *PipelineHandler) CreatePipeline(ctx context.Context, req *pipelinev1.CreatePipelineRequest) (*pipelinev1.CreatePipelineResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}

	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}

	// EDGE VALIDATION (cheap, transport-level): reject obviously-malformed input
	// here so the domain isn't entered for garbage. The domain ALSO validates the
	// step graph (cycles, dangling edges, caps) — that is business validation and
	// stays there; the handler only checks shape (required fields, known enums).
	pt, err := pipelineTypeToDomain(req.GetType())
	if err != nil {
		return nil, err
	}
	steps, err := stepsToDomain(req.GetSteps())
	if err != nil {
		return nil, err
	}

	input := domain.CreatePipelineInput{
		Name:           req.GetName(),
		Type:           pt,
		Steps:          steps,
		IdempotencyKey: req.GetIdempotencyKey(),
	}

	pipeline, err := h.svc.CreatePipeline(ctx, actor, input)
	if err != nil {
		return nil, mapDomainError(err)
	}
	return &pipelinev1.CreatePipelineResponse{Pipeline: pipelineToProto(pipeline)}, nil
}

// GetPipeline fetches one template by id, scoped to the caller's team.
func (h *PipelineHandler) GetPipeline(ctx context.Context, req *pipelinev1.GetPipelineRequest) (*pipelinev1.GetPipelineResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}

	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetPipelineId() == "" {
		return nil, status.Error(codes.InvalidArgument, "pipeline_id is required")
	}

	pipeline, err := h.svc.GetPipeline(ctx, actor, req.GetPipelineId())
	if err != nil {
		return nil, mapDomainError(err)
	}
	return &pipelinev1.GetPipelineResponse{Pipeline: pipelineToProto(pipeline)}, nil
}

// UpdatePipeline edits a template's name/steps in place. type is NOT accepted
// (immutable per the proto) — we read only the user-authorable fields.
func (h *PipelineHandler) UpdatePipeline(ctx context.Context, req *pipelinev1.UpdatePipelineRequest) (*pipelinev1.UpdatePipelineResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}

	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetPipelineId() == "" {
		return nil, status.Error(codes.InvalidArgument, "pipeline_id is required")
	}
	steps, err := stepsToDomain(req.GetSteps())
	if err != nil {
		return nil, err
	}

	input := domain.UpdatePipelineInput{
		ID:    req.GetPipelineId(),
		Name:  req.GetName(),
		Steps: steps,
	}

	pipeline, err := h.svc.UpdatePipeline(ctx, actor, input)
	if err != nil {
		return nil, mapDomainError(err)
	}
	return &pipelinev1.UpdatePipelineResponse{Pipeline: pipelineToProto(pipeline)}, nil
}

// DeletePipeline soft-deletes (archives) a template.
func (h *PipelineHandler) DeletePipeline(ctx context.Context, req *pipelinev1.DeletePipelineRequest) (*pipelinev1.DeletePipelineResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}

	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetPipelineId() == "" {
		return nil, status.Error(codes.InvalidArgument, "pipeline_id is required")
	}

	if err := h.svc.DeletePipeline(ctx, actor, req.GetPipelineId()); err != nil {
		return nil, mapDomainError(err)
	}
	// A named empty response (not google.protobuf.Empty) for forward compat.
	return &pipelinev1.DeletePipelineResponse{}, nil
}

// ListPipelines returns a tenancy-scoped, paginated page of templates.
//
// PAGE-SIZE CAP: we surface the raw requested page_size to the domain, which
// clamps it (default 20, max 100) — the cap is enforced server-side regardless
// of the client's request. A NEGATIVE page_size is nonsense from the wire, so we
// reject it here as InvalidArgument (the domain treats <= 0 as "use default",
// but a negative value is a malformed request worth surfacing).
func (h *PipelineHandler) ListPipelines(ctx context.Context, req *pipelinev1.ListPipelinesRequest) (*pipelinev1.ListPipelinesResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}

	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}

	// type_filter is OPTIONAL; UNSPECIFIED means "any type". Only reject a value
	// that is out of the enum's range (a garbage int the client invented).
	tf, err := pipelineTypeFilterToDomain(req.GetTypeFilter())
	if err != nil {
		return nil, err
	}
	pageSize, err := pageSizeToDomain(req.GetPagination())
	if err != nil {
		return nil, err
	}

	filter := domain.ListPipelinesFilter{
		Type: tf,
		List: domain.ListOptions{
			PageSize:  pageSize,
			PageToken: pageTokenFromProto(req.GetPagination()),
		},
		// Team is intentionally NOT set from the request — the domain service
		// stamps it from the Actor. We pass the Actor below.
	}

	pipelines, nextToken, err := h.svc.ListPipelines(ctx, actor, filter)
	if err != nil {
		return nil, mapDomainError(err)
	}

	out := make([]*pipelinev1.PipelineDefinition, 0, len(pipelines))
	for i := range pipelines {
		out = append(out, pipelineToProto(pipelines[i]))
	}
	return &pipelinev1.ListPipelinesResponse{
		Pipelines:  out,
		Pagination: paginationResponse(nextToken),
	}, nil
}

// ============================================================================
// EXECUTION RPCs
// ============================================================================

// TriggerExecution starts a new run of a pipeline and (per the domain contract)
// drives it to a terminal state, returning the settled Execution.
func (h *PipelineHandler) TriggerExecution(ctx context.Context, req *pipelinev1.TriggerExecutionRequest) (*pipelinev1.TriggerExecutionResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}

	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetPipelineId() == "" {
		return nil, status.Error(codes.InvalidArgument, "pipeline_id is required")
	}

	input := domain.TriggerInput{
		PipelineID:     req.GetPipelineId(),
		Input:          structToMap(req.GetInput()),
		IdempotencyKey: req.GetIdempotencyKey(),
	}

	execution, err := h.svc.TriggerExecution(ctx, actor, input)
	if err != nil {
		return nil, mapDomainError(err)
	}
	return &pipelinev1.TriggerExecutionResponse{Execution: executionToProto(execution)}, nil
}

// GetExecution returns the current state of a run (incl. its per-step timeline).
func (h *PipelineHandler) GetExecution(ctx context.Context, req *pipelinev1.GetExecutionRequest) (*pipelinev1.GetExecutionResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}

	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetExecutionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "execution_id is required")
	}

	execution, err := h.svc.GetExecution(ctx, actor, req.GetExecutionId())
	if err != nil {
		return nil, mapDomainError(err)
	}
	return &pipelinev1.GetExecutionResponse{Execution: executionToProto(execution)}, nil
}

// CancelExecution requests a graceful stop and returns the settled Execution.
func (h *PipelineHandler) CancelExecution(ctx context.Context, req *pipelinev1.CancelExecutionRequest) (*pipelinev1.CancelExecutionResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}

	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetExecutionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "execution_id is required")
	}

	execution, err := h.svc.CancelExecution(ctx, actor, req.GetExecutionId(), req.GetReason())
	if err != nil {
		return nil, mapDomainError(err)
	}
	return &pipelinev1.CancelExecutionResponse{Execution: executionToProto(execution)}, nil
}

// ListExecutions returns a tenancy-scoped, paginated page of runs.
func (h *PipelineHandler) ListExecutions(ctx context.Context, req *pipelinev1.ListExecutionsRequest) (*pipelinev1.ListExecutionsResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}

	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}

	sf, err := executionStatusFilterToDomain(req.GetStatusFilter())
	if err != nil {
		return nil, err
	}
	pageSize, err := pageSizeToDomain(req.GetPagination())
	if err != nil {
		return nil, err
	}

	filter := domain.ListExecutionsFilter{
		PipelineID: req.GetPipelineId(), // optional narrow within the authorized scope
		Status:     sf,
		List: domain.ListOptions{
			PageSize:  pageSize,
			PageToken: pageTokenFromProto(req.GetPagination()),
		},
		// Team set by the service from the Actor — never from the request.
	}

	executions, nextToken, err := h.svc.ListExecutions(ctx, actor, filter)
	if err != nil {
		return nil, mapDomainError(err)
	}

	out := make([]*pipelinev1.Execution, 0, len(executions))
	for i := range executions {
		out = append(out, executionToProto(executions[i]))
	}
	return &pipelinev1.ListExecutionsResponse{
		Executions: out,
		Pagination: paginationResponse(nextToken),
	}, nil
}

// ============================================================================
// WatchExecution — the SERVER-STREAMING RPC
// ============================================================================
//
// SHAPE: one request → many responses. The signature returns ONLY an error;
// updates flow through stream.Send. So the nil-svc guard returns the status
// error directly (no nil response value to return) — this is the streaming guard
// form noted in the package comment.
//
// WHY POLL-AND-DIFF (not an in-process subscription) HERE: the domain
// PipelineService interface deliberately has NO WatchExecution method — streaming
// is a transport concern (see pipeline_service.go). The handler bridges the
// domain's queryable state (GetExecution) to the gRPC stream by polling and
// emitting an update whenever the snapshot CHANGES. This keeps the domain
// transport-free. A future iteration can swap the poll loop for an in-process
// update channel (or a NATS subscription) without touching the proto or the
// domain — the stream contract (full-snapshot-per-update + monotonic sequence)
// is unchanged. Tradeoff: a poll has up to one interval of latency and reads the
// store on a cadence; for a saga with a handful of steps over seconds-to-minutes
// that is negligible, and it is the simplest correct bridge.
//
// CANCELLATION: the loop selects on stream.Context().Done() so a client that
// hangs up (or a server shutdown) stops the loop promptly — we never leak a
// goroutine polling for an execution nobody is watching.
//
// TERMINATION: when the execution reaches a terminal state (COMPLETED / FAILED /
// CANCELLED) we emit the final snapshot and return nil, which closes the stream
// cleanly — exactly the proto's documented behavior.
func (h *PipelineHandler) WatchExecution(req *pipelinev1.WatchExecutionRequest, stream pipelinev1.PipelineOrchestratorService_WatchExecutionServer) error {
	if h.svc == nil {
		// Streaming guard: return the status error directly (no nil response).
		return status.Error(codes.Unimplemented, "pipeline service is not wired")
	}
	if req == nil {
		return status.Error(codes.InvalidArgument, "request must not be nil")
	}

	ctx := stream.Context()
	actor, err := actorFromContext(ctx)
	if err != nil {
		return err
	}
	if req.GetExecutionId() == "" {
		return status.Error(codes.InvalidArgument, "execution_id is required")
	}

	return h.watchExecution(ctx, actor, req, stream)
}
