// ============================================================================
// Forgepoint Pipeline Orchestrator Proto Definitions
// ============================================================================
//
// WHY: The Pipeline Orchestrator is the "star service" of the platform — a
// GENERIC workflow engine that runs multi-step ML workflows. Two execution
// models live behind one API:
//
//   1. SAGA (orchestration + compensation) — for stateful, side-effecting
//      workflows like model DEPLOYMENT. Steps run in sequence; if a later step
//      fails, previously-completed steps are UNDONE in reverse order by running
//      their compensation steps. This is how we get "distributed transaction"
//      semantics without a 2-phase-commit coordinator.
//
//   2. DAG (directed acyclic graph) — for TRAINING workflows. Steps declare
//      `depends_on` edges; independent steps run in parallel (fan-out), and
//      barrier steps wait for all parents (fan-in). No global rollback —
//      failure propagates to dependents.
//
// PATTERN — Saga (Orchestration) vs Choreography:
//   ORCHESTRATION (this service): a CENTRAL coordinator (the orchestrator)
//   tells each participant what to do and decides when to compensate. The
//   control flow lives in ONE place — easy to reason about, easy to visualize,
//   easy to debug ("where is the saga stuck?"). Tradeoff: the orchestrator is
//   a coupling point and must be highly available + durable.
//
//   CHOREOGRAPHY (the Notification service uses this): no coordinator; each
//   service reacts to events and emits its own. Decoupled, but the workflow is
//   implicit — there is no single place that knows the whole flow, which makes
//   debugging and compensation ordering much harder.
//
//   WHY ORCHESTRATION FOR DEPLOYMENT: deployment compensation MUST run in a
//   strict reverse order (destroy serving instance only AFTER rolling back
//   traffic). A central orchestrator enforces that order deterministically;
//   choreography cannot guarantee it.
//
// REAL-WORLD COMPARISON:
//   - Temporal / Cadence: durable workflow engines — same "orchestrator owns
//     the state machine, persists every step" idea. Our saga is a hand-rolled,
//     domain-specific Temporal.
//   - AWS Step Functions: state machine of steps with retry/catch — closest
//     managed analogue to this service.
//   - Argo Workflows / Airflow: DAG executors for ML/data pipelines — analogous
//     to our DAG mode (TRAINING_DAG).
//   - Netflix Conductor: orchestration-based saga engine for microservices.
//
// DURABILITY: every StepExecution is persisted to Postgres
// BEFORE the step runs (write-ahead). If the orchestrator pod crashes mid-saga,
// on restart it loads the last persisted checkpoint and RESUMES — it does not
// restart the whole pipeline. The Helm chart runs this service with LEADER
// ELECTION (single active replica) so two pods never drive the same saga
// concurrently. The API here is the durable, queryable face of that state.
//
// CLOSED LOOP: this service is what makes Forgepoint a closed loop. The Model
// Monitor publishes fp.models.drift.detected (events.ModelDriftDetected); this
// service CONSUMES it and, on a CRITICAL report with auto_retrain armed, calls
// its OWN TriggerExecution on the model's retrain pipeline (retrain → canary →
// promote). That closes serve → monitor → retrain WITHOUT a synchronous callback
// — the orchestrator reacts to a fat-but-flat event, not a gRPC poke.
//
// EVENT CONTRACT (single source of truth): this service's NATS payloads are
// defined in forgepoint/events/v1/events.proto, NOT here. We deliberately do NOT
// re-declare event-payload messages in this API proto, and we do NOT even import
// events.proto into this file (the generated Go event types are imported by the
// Go publisher/consumer code instead — keeping the API-proto surface uncoupled
// from the event schema). WHY: an event is a PUBLISHED CONTRACT with its own
// lifecycle and its own buf-breaking guarantee, decoupled from the RPC types — a
// consumer (Notification, Experiment Tracker) depends only on the events package,
// never on this service's API proto. See the long DESIGN block in events.proto
// for the schema-registry reasoning. The handler maps this proto's domain enums
// (PipelineType, StepType) to/from the mirrored events enums at the publish
// boundary (a few lines of mechanical conversion — the price of decoupling).
//
//   PRODUCES (subject → events.v1 message):
//     fp.pipelines.started                  → events.PipelineStarted
//     fp.pipelines.step.completed           → events.StepCompleted
//     fp.pipelines.step.failed              → events.StepFailed
//     fp.pipelines.completed                → events.PipelineCompleted
//     fp.pipelines.failed                   → events.PipelineFailed
//     fp.pipelines.compensation.triggered   → events.CompensationTriggered
//     fp.pipelines.model.deployed           → events.ModelDeployed     (NEW — see below)
//     fp.pipelines.model.undeployed         → events.ModelUndeployed   (NEW — see below)
//
//   CONSUMES:
//     fp.models.drift.detected              → events.ModelDriftDetected
//       (on severity == CRITICAL && auto_retrain, TriggerExecution(retrain_pipeline_id))
//
// MODEL-DEPLOY LIFECYCLE OWNERSHIP (events.proto conflict #2): the DEPLOY /
// PROMOTE saga steps — and the compensation that rolls them back — are the
// AUTHORITATIVE source of "a model version is now (un)deployed". This service
// therefore OWNS and publishes events.ModelDeployed / events.ModelUndeployed
// under fp.pipelines.model.* (the deploy is a WORKFLOW outcome, so it lives in
// the pipelines domain even though it concerns a model — EventEnvelope.source
// records "pipeline-orchestrator" as the real producer). The Inference Gateway
// and Model Serving CONSUME these to add/remove routes and (un)load versions; a
// serving pod must NOT emit a competing ModelLoaded/Unloaded lifecycle event.
//   SSRF GUARD: events.ModelDeployed.endpoint is the serving
//   backend address. It is RESOLVED SERVER-SIDE by the DEPLOY executor (from the
//   model version + the K8s Service it created), NEVER taken from client-supplied
//   step `config`. Accepting a client URL as the route target would let a caller
//   point gateway traffic at an arbitrary internal host (SSRF). The executor only
//   ever emits an endpoint it constructed itself, against the fp-models namespace.
//
// VERSIONING: Package path includes v1 following Buf/Google convention.
// Breaking changes require a new forgepoint.pipeline.v2 package.
// ============================================================================

// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.6.2
// - protoc             (unknown)
// source: forgepoint/pipeline/v1/pipeline.proto

package pipelinev1

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	PipelineOrchestratorService_CreatePipeline_FullMethodName   = "/forgepoint.pipeline.v1.PipelineOrchestratorService/CreatePipeline"
	PipelineOrchestratorService_ListPipelines_FullMethodName    = "/forgepoint.pipeline.v1.PipelineOrchestratorService/ListPipelines"
	PipelineOrchestratorService_GetPipeline_FullMethodName      = "/forgepoint.pipeline.v1.PipelineOrchestratorService/GetPipeline"
	PipelineOrchestratorService_UpdatePipeline_FullMethodName   = "/forgepoint.pipeline.v1.PipelineOrchestratorService/UpdatePipeline"
	PipelineOrchestratorService_DeletePipeline_FullMethodName   = "/forgepoint.pipeline.v1.PipelineOrchestratorService/DeletePipeline"
	PipelineOrchestratorService_TriggerExecution_FullMethodName = "/forgepoint.pipeline.v1.PipelineOrchestratorService/TriggerExecution"
	PipelineOrchestratorService_GetExecution_FullMethodName     = "/forgepoint.pipeline.v1.PipelineOrchestratorService/GetExecution"
	PipelineOrchestratorService_WatchExecution_FullMethodName   = "/forgepoint.pipeline.v1.PipelineOrchestratorService/WatchExecution"
	PipelineOrchestratorService_CancelExecution_FullMethodName  = "/forgepoint.pipeline.v1.PipelineOrchestratorService/CancelExecution"
	PipelineOrchestratorService_ListExecutions_FullMethodName   = "/forgepoint.pipeline.v1.PipelineOrchestratorService/ListExecutions"
)

// PipelineOrchestratorServiceClient is the client API for PipelineOrchestratorService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
//
// ============================================================================
// PIPELINE ORCHESTRATOR SERVICE
// ============================================================================
//
// WHY ONE SERVICE WITH THESE RPCs: the service has two responsibilities —
// DEFINING workflows (CreatePipeline / ListPipelines) and RUNNING + OBSERVING
// them (Trigger / Get / Watch / Cancel / List executions). They share the same
// durable store and domain model, so they live behind one service boundary.
//
// STREAMING CHOICE: only WatchExecution is server-
// streaming; everything else is unary. WHY:
//   - WatchExecution: the orchestrator is the SINGLE source of truth for saga
//     state and PUSHES transitions as they happen. One client request →
//     many server messages = SERVER-STREAMING. Polling GetExecution in a loop
//     would be wasteful (most polls see no change) and laggy. Long-lived push
//     is the natural fit, and the BFF bridges it to SSE for the browser.
//   - Everything else is a single request/response → unary. We do NOT use
//     client-streaming or bidi anywhere here: the client never sends a stream of
//     data to the orchestrator; it issues discrete commands.
//
// LEADER ELECTION (deployment note): although the gRPC
// API can be served by many replicas, only the ELECTED LEADER actually drives
// sagas (runs steps, writes checkpoints). This prevents two pods from executing
// the same saga concurrently and double-applying side effects. Read RPCs
// (Get/List/Watch) can be served by any replica from the shared DB.
//
// CALL FLOW (closing the loop):
//
//	Model Monitor detects drift
//	     │ gRPC
//	     ▼
//	PipelineOrchestratorService.TriggerExecution(training pipeline, drift report)
//	     │  saga/DAG: retrain → evaluate → register → deploy → canary → promote
//	     ▼
//	emits fp.pipelines.* events → Notification + Experiment Tracker
//
// ============================================================================
type PipelineOrchestratorServiceClient interface {
	// CreatePipeline registers a new pipeline TEMPLATE (saga or DAG). The server
	// validates the step graph (known types, valid depends_on, no cycles, valid
	// compensation pointers) and assigns id/created_by/created_at server-side.
	CreatePipeline(ctx context.Context, in *CreatePipelineRequest, opts ...grpc.CallOption) (*CreatePipelineResponse, error)
	// ListPipelines returns a paginated list of pipeline templates within the
	// caller's team/RBAC scope, optionally filtered by type.
	ListPipelines(ctx context.Context, in *ListPipelinesRequest, opts ...grpc.CallOption) (*ListPipelinesResponse, error)
	// GetPipeline fetches one pipeline TEMPLATE by id (full step graph), scoped to
	// the caller's team/RBAC. Point lookup for the CLI/UI before triggering a run.
	GetPipeline(ctx context.Context, in *GetPipelineRequest, opts ...grpc.CallOption) (*GetPipelineResponse, error)
	// UpdatePipeline edits a template's name/steps in place (id and execution
	// history preserved). Re-validates the step graph; type is immutable.
	UpdatePipeline(ctx context.Context, in *UpdatePipelineRequest, opts ...grpc.CallOption) (*UpdatePipelineResponse, error)
	// DeletePipeline soft-deletes (archives) a template so it can no longer be
	// listed or triggered, while preserving past executions' lineage. Rejected if
	// a non-terminal execution of the pipeline is still in flight.
	DeletePipeline(ctx context.Context, in *DeletePipelineRequest, opts ...grpc.CallOption) (*DeletePipelineResponse, error)
	// TriggerExecution starts a new run of a pipeline. Supports an idempotency_key
	// so retries don't double-trigger (exactly-once from the caller's view). The
	// orchestrator owns the resulting Execution's lifecycle.
	TriggerExecution(ctx context.Context, in *TriggerExecutionRequest, opts ...grpc.CallOption) (*TriggerExecutionResponse, error)
	// GetExecution fetches the current state of a run (point-in-time), including
	// its per-step timeline. Cheap, stateless poll — use Watch for live updates.
	GetExecution(ctx context.Context, in *GetExecutionRequest, opts ...grpc.CallOption) (*GetExecutionResponse, error)
	// WatchExecution opens a SERVER-STREAMING feed of live updates for one run.
	// The orchestrator pushes a WatchExecutionResponse on every state transition until
	// the run reaches a terminal state (COMPLETED/FAILED/CANCELLED), then closes
	// the stream. With include_current_state=true the server first replays the
	// current state, avoiding a get-then-watch race.
	WatchExecution(ctx context.Context, in *WatchExecutionRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[WatchExecutionResponse], error)
	// CancelExecution requests a graceful stop. The run transitions to
	// COMPENSATING (undo completed steps in reverse) and then CANCELLED.
	// Returns the Execution so the caller sees the immediate transition.
	CancelExecution(ctx context.Context, in *CancelExecutionRequest, opts ...grpc.CallOption) (*CancelExecutionResponse, error)
	// ListExecutions returns a paginated, filterable list of runs (by pipeline
	// and/or status). page_size is capped server-side. List items omit per-step
	// detail; fetch GetExecution for the full timeline.
	ListExecutions(ctx context.Context, in *ListExecutionsRequest, opts ...grpc.CallOption) (*ListExecutionsResponse, error)
}

type pipelineOrchestratorServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewPipelineOrchestratorServiceClient(cc grpc.ClientConnInterface) PipelineOrchestratorServiceClient {
	return &pipelineOrchestratorServiceClient{cc}
}

func (c *pipelineOrchestratorServiceClient) CreatePipeline(ctx context.Context, in *CreatePipelineRequest, opts ...grpc.CallOption) (*CreatePipelineResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(CreatePipelineResponse)
	err := c.cc.Invoke(ctx, PipelineOrchestratorService_CreatePipeline_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *pipelineOrchestratorServiceClient) ListPipelines(ctx context.Context, in *ListPipelinesRequest, opts ...grpc.CallOption) (*ListPipelinesResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ListPipelinesResponse)
	err := c.cc.Invoke(ctx, PipelineOrchestratorService_ListPipelines_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *pipelineOrchestratorServiceClient) GetPipeline(ctx context.Context, in *GetPipelineRequest, opts ...grpc.CallOption) (*GetPipelineResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetPipelineResponse)
	err := c.cc.Invoke(ctx, PipelineOrchestratorService_GetPipeline_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *pipelineOrchestratorServiceClient) UpdatePipeline(ctx context.Context, in *UpdatePipelineRequest, opts ...grpc.CallOption) (*UpdatePipelineResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(UpdatePipelineResponse)
	err := c.cc.Invoke(ctx, PipelineOrchestratorService_UpdatePipeline_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *pipelineOrchestratorServiceClient) DeletePipeline(ctx context.Context, in *DeletePipelineRequest, opts ...grpc.CallOption) (*DeletePipelineResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(DeletePipelineResponse)
	err := c.cc.Invoke(ctx, PipelineOrchestratorService_DeletePipeline_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *pipelineOrchestratorServiceClient) TriggerExecution(ctx context.Context, in *TriggerExecutionRequest, opts ...grpc.CallOption) (*TriggerExecutionResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(TriggerExecutionResponse)
	err := c.cc.Invoke(ctx, PipelineOrchestratorService_TriggerExecution_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *pipelineOrchestratorServiceClient) GetExecution(ctx context.Context, in *GetExecutionRequest, opts ...grpc.CallOption) (*GetExecutionResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetExecutionResponse)
	err := c.cc.Invoke(ctx, PipelineOrchestratorService_GetExecution_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *pipelineOrchestratorServiceClient) WatchExecution(ctx context.Context, in *WatchExecutionRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[WatchExecutionResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &PipelineOrchestratorService_ServiceDesc.Streams[0], PipelineOrchestratorService_WatchExecution_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[WatchExecutionRequest, WatchExecutionResponse]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type PipelineOrchestratorService_WatchExecutionClient = grpc.ServerStreamingClient[WatchExecutionResponse]

func (c *pipelineOrchestratorServiceClient) CancelExecution(ctx context.Context, in *CancelExecutionRequest, opts ...grpc.CallOption) (*CancelExecutionResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(CancelExecutionResponse)
	err := c.cc.Invoke(ctx, PipelineOrchestratorService_CancelExecution_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *pipelineOrchestratorServiceClient) ListExecutions(ctx context.Context, in *ListExecutionsRequest, opts ...grpc.CallOption) (*ListExecutionsResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ListExecutionsResponse)
	err := c.cc.Invoke(ctx, PipelineOrchestratorService_ListExecutions_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// PipelineOrchestratorServiceServer is the server API for PipelineOrchestratorService service.
// All implementations must embed UnimplementedPipelineOrchestratorServiceServer
// for forward compatibility.
//
// ============================================================================
// PIPELINE ORCHESTRATOR SERVICE
// ============================================================================
//
// WHY ONE SERVICE WITH THESE RPCs: the service has two responsibilities —
// DEFINING workflows (CreatePipeline / ListPipelines) and RUNNING + OBSERVING
// them (Trigger / Get / Watch / Cancel / List executions). They share the same
// durable store and domain model, so they live behind one service boundary.
//
// STREAMING CHOICE: only WatchExecution is server-
// streaming; everything else is unary. WHY:
//   - WatchExecution: the orchestrator is the SINGLE source of truth for saga
//     state and PUSHES transitions as they happen. One client request →
//     many server messages = SERVER-STREAMING. Polling GetExecution in a loop
//     would be wasteful (most polls see no change) and laggy. Long-lived push
//     is the natural fit, and the BFF bridges it to SSE for the browser.
//   - Everything else is a single request/response → unary. We do NOT use
//     client-streaming or bidi anywhere here: the client never sends a stream of
//     data to the orchestrator; it issues discrete commands.
//
// LEADER ELECTION (deployment note): although the gRPC
// API can be served by many replicas, only the ELECTED LEADER actually drives
// sagas (runs steps, writes checkpoints). This prevents two pods from executing
// the same saga concurrently and double-applying side effects. Read RPCs
// (Get/List/Watch) can be served by any replica from the shared DB.
//
// CALL FLOW (closing the loop):
//
//	Model Monitor detects drift
//	     │ gRPC
//	     ▼
//	PipelineOrchestratorService.TriggerExecution(training pipeline, drift report)
//	     │  saga/DAG: retrain → evaluate → register → deploy → canary → promote
//	     ▼
//	emits fp.pipelines.* events → Notification + Experiment Tracker
//
// ============================================================================
type PipelineOrchestratorServiceServer interface {
	// CreatePipeline registers a new pipeline TEMPLATE (saga or DAG). The server
	// validates the step graph (known types, valid depends_on, no cycles, valid
	// compensation pointers) and assigns id/created_by/created_at server-side.
	CreatePipeline(context.Context, *CreatePipelineRequest) (*CreatePipelineResponse, error)
	// ListPipelines returns a paginated list of pipeline templates within the
	// caller's team/RBAC scope, optionally filtered by type.
	ListPipelines(context.Context, *ListPipelinesRequest) (*ListPipelinesResponse, error)
	// GetPipeline fetches one pipeline TEMPLATE by id (full step graph), scoped to
	// the caller's team/RBAC. Point lookup for the CLI/UI before triggering a run.
	GetPipeline(context.Context, *GetPipelineRequest) (*GetPipelineResponse, error)
	// UpdatePipeline edits a template's name/steps in place (id and execution
	// history preserved). Re-validates the step graph; type is immutable.
	UpdatePipeline(context.Context, *UpdatePipelineRequest) (*UpdatePipelineResponse, error)
	// DeletePipeline soft-deletes (archives) a template so it can no longer be
	// listed or triggered, while preserving past executions' lineage. Rejected if
	// a non-terminal execution of the pipeline is still in flight.
	DeletePipeline(context.Context, *DeletePipelineRequest) (*DeletePipelineResponse, error)
	// TriggerExecution starts a new run of a pipeline. Supports an idempotency_key
	// so retries don't double-trigger (exactly-once from the caller's view). The
	// orchestrator owns the resulting Execution's lifecycle.
	TriggerExecution(context.Context, *TriggerExecutionRequest) (*TriggerExecutionResponse, error)
	// GetExecution fetches the current state of a run (point-in-time), including
	// its per-step timeline. Cheap, stateless poll — use Watch for live updates.
	GetExecution(context.Context, *GetExecutionRequest) (*GetExecutionResponse, error)
	// WatchExecution opens a SERVER-STREAMING feed of live updates for one run.
	// The orchestrator pushes a WatchExecutionResponse on every state transition until
	// the run reaches a terminal state (COMPLETED/FAILED/CANCELLED), then closes
	// the stream. With include_current_state=true the server first replays the
	// current state, avoiding a get-then-watch race.
	WatchExecution(*WatchExecutionRequest, grpc.ServerStreamingServer[WatchExecutionResponse]) error
	// CancelExecution requests a graceful stop. The run transitions to
	// COMPENSATING (undo completed steps in reverse) and then CANCELLED.
	// Returns the Execution so the caller sees the immediate transition.
	CancelExecution(context.Context, *CancelExecutionRequest) (*CancelExecutionResponse, error)
	// ListExecutions returns a paginated, filterable list of runs (by pipeline
	// and/or status). page_size is capped server-side. List items omit per-step
	// detail; fetch GetExecution for the full timeline.
	ListExecutions(context.Context, *ListExecutionsRequest) (*ListExecutionsResponse, error)
	mustEmbedUnimplementedPipelineOrchestratorServiceServer()
}

// UnimplementedPipelineOrchestratorServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedPipelineOrchestratorServiceServer struct{}

func (UnimplementedPipelineOrchestratorServiceServer) CreatePipeline(context.Context, *CreatePipelineRequest) (*CreatePipelineResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method CreatePipeline not implemented")
}
func (UnimplementedPipelineOrchestratorServiceServer) ListPipelines(context.Context, *ListPipelinesRequest) (*ListPipelinesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method ListPipelines not implemented")
}
func (UnimplementedPipelineOrchestratorServiceServer) GetPipeline(context.Context, *GetPipelineRequest) (*GetPipelineResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetPipeline not implemented")
}
func (UnimplementedPipelineOrchestratorServiceServer) UpdatePipeline(context.Context, *UpdatePipelineRequest) (*UpdatePipelineResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method UpdatePipeline not implemented")
}
func (UnimplementedPipelineOrchestratorServiceServer) DeletePipeline(context.Context, *DeletePipelineRequest) (*DeletePipelineResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method DeletePipeline not implemented")
}
func (UnimplementedPipelineOrchestratorServiceServer) TriggerExecution(context.Context, *TriggerExecutionRequest) (*TriggerExecutionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method TriggerExecution not implemented")
}
func (UnimplementedPipelineOrchestratorServiceServer) GetExecution(context.Context, *GetExecutionRequest) (*GetExecutionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetExecution not implemented")
}
func (UnimplementedPipelineOrchestratorServiceServer) WatchExecution(*WatchExecutionRequest, grpc.ServerStreamingServer[WatchExecutionResponse]) error {
	return status.Error(codes.Unimplemented, "method WatchExecution not implemented")
}
func (UnimplementedPipelineOrchestratorServiceServer) CancelExecution(context.Context, *CancelExecutionRequest) (*CancelExecutionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method CancelExecution not implemented")
}
func (UnimplementedPipelineOrchestratorServiceServer) ListExecutions(context.Context, *ListExecutionsRequest) (*ListExecutionsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method ListExecutions not implemented")
}
func (UnimplementedPipelineOrchestratorServiceServer) mustEmbedUnimplementedPipelineOrchestratorServiceServer() {
}
func (UnimplementedPipelineOrchestratorServiceServer) testEmbeddedByValue() {}

// UnsafePipelineOrchestratorServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to PipelineOrchestratorServiceServer will
// result in compilation errors.
type UnsafePipelineOrchestratorServiceServer interface {
	mustEmbedUnimplementedPipelineOrchestratorServiceServer()
}

func RegisterPipelineOrchestratorServiceServer(s grpc.ServiceRegistrar, srv PipelineOrchestratorServiceServer) {
	// If the following call panics, it indicates UnimplementedPipelineOrchestratorServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&PipelineOrchestratorService_ServiceDesc, srv)
}

func _PipelineOrchestratorService_CreatePipeline_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreatePipelineRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PipelineOrchestratorServiceServer).CreatePipeline(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: PipelineOrchestratorService_CreatePipeline_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PipelineOrchestratorServiceServer).CreatePipeline(ctx, req.(*CreatePipelineRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _PipelineOrchestratorService_ListPipelines_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListPipelinesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PipelineOrchestratorServiceServer).ListPipelines(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: PipelineOrchestratorService_ListPipelines_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PipelineOrchestratorServiceServer).ListPipelines(ctx, req.(*ListPipelinesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _PipelineOrchestratorService_GetPipeline_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetPipelineRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PipelineOrchestratorServiceServer).GetPipeline(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: PipelineOrchestratorService_GetPipeline_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PipelineOrchestratorServiceServer).GetPipeline(ctx, req.(*GetPipelineRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _PipelineOrchestratorService_UpdatePipeline_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UpdatePipelineRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PipelineOrchestratorServiceServer).UpdatePipeline(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: PipelineOrchestratorService_UpdatePipeline_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PipelineOrchestratorServiceServer).UpdatePipeline(ctx, req.(*UpdatePipelineRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _PipelineOrchestratorService_DeletePipeline_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeletePipelineRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PipelineOrchestratorServiceServer).DeletePipeline(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: PipelineOrchestratorService_DeletePipeline_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PipelineOrchestratorServiceServer).DeletePipeline(ctx, req.(*DeletePipelineRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _PipelineOrchestratorService_TriggerExecution_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TriggerExecutionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PipelineOrchestratorServiceServer).TriggerExecution(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: PipelineOrchestratorService_TriggerExecution_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PipelineOrchestratorServiceServer).TriggerExecution(ctx, req.(*TriggerExecutionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _PipelineOrchestratorService_GetExecution_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetExecutionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PipelineOrchestratorServiceServer).GetExecution(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: PipelineOrchestratorService_GetExecution_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PipelineOrchestratorServiceServer).GetExecution(ctx, req.(*GetExecutionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _PipelineOrchestratorService_WatchExecution_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(WatchExecutionRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(PipelineOrchestratorServiceServer).WatchExecution(m, &grpc.GenericServerStream[WatchExecutionRequest, WatchExecutionResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type PipelineOrchestratorService_WatchExecutionServer = grpc.ServerStreamingServer[WatchExecutionResponse]

func _PipelineOrchestratorService_CancelExecution_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CancelExecutionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PipelineOrchestratorServiceServer).CancelExecution(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: PipelineOrchestratorService_CancelExecution_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PipelineOrchestratorServiceServer).CancelExecution(ctx, req.(*CancelExecutionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _PipelineOrchestratorService_ListExecutions_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListExecutionsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PipelineOrchestratorServiceServer).ListExecutions(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: PipelineOrchestratorService_ListExecutions_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PipelineOrchestratorServiceServer).ListExecutions(ctx, req.(*ListExecutionsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// PipelineOrchestratorService_ServiceDesc is the grpc.ServiceDesc for PipelineOrchestratorService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var PipelineOrchestratorService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "forgepoint.pipeline.v1.PipelineOrchestratorService",
	HandlerType: (*PipelineOrchestratorServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "CreatePipeline",
			Handler:    _PipelineOrchestratorService_CreatePipeline_Handler,
		},
		{
			MethodName: "ListPipelines",
			Handler:    _PipelineOrchestratorService_ListPipelines_Handler,
		},
		{
			MethodName: "GetPipeline",
			Handler:    _PipelineOrchestratorService_GetPipeline_Handler,
		},
		{
			MethodName: "UpdatePipeline",
			Handler:    _PipelineOrchestratorService_UpdatePipeline_Handler,
		},
		{
			MethodName: "DeletePipeline",
			Handler:    _PipelineOrchestratorService_DeletePipeline_Handler,
		},
		{
			MethodName: "TriggerExecution",
			Handler:    _PipelineOrchestratorService_TriggerExecution_Handler,
		},
		{
			MethodName: "GetExecution",
			Handler:    _PipelineOrchestratorService_GetExecution_Handler,
		},
		{
			MethodName: "CancelExecution",
			Handler:    _PipelineOrchestratorService_CancelExecution_Handler,
		},
		{
			MethodName: "ListExecutions",
			Handler:    _PipelineOrchestratorService_ListExecutions_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "WatchExecution",
			Handler:       _PipelineOrchestratorService_WatchExecution_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "forgepoint/pipeline/v1/pipeline.proto",
}
