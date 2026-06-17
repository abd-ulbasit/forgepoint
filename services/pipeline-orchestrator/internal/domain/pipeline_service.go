// pipeline_service.go — the PipelineService interface (the use-case contract).
//
// ============================================================================
// THE SERVICE INTERFACE AS THE PRIMARY PORT
// ============================================================================
//
// In Clean / Hexagonal Architecture this interface is the PRIMARY (driving)
// port: the handler holds a PipelineService and calls it; the concrete
// pipelineService (pipeline_service_impl.go) implements it. The handler never
// knows whether a real Postgres-backed engine or a test stub is behind the
// interface — dependency inversion. It also never sees a goroutine, a NATS
// connection, or a SQL row: those live behind the SECONDARY ports in ports.go.
//
// COMMAND/QUERY shape of this contract:
//   - Commands (mutate state): CreatePipeline, UpdatePipeline, DeletePipeline,
//     TriggerExecution, CancelExecution.
//   - Queries (read state):     GetPipeline, ListPipelines, GetExecution,
//     ListExecutions.
//
// WHY no WatchExecution here: WatchExecution is a SERVER-STREAMING transport
// concern. The domain exposes the state (GetExecution) and the engine drives
// transitions; bridging those to a gRPC stream is the handler's job (it can poll
// GetExecution or subscribe to an in-process update channel). Keeping the
// streaming mechanics out of the domain interface keeps the domain transport-free.
//
// IDENTITY IS A PARAMETER, NOT A REQUEST FIELD: every mutating method takes an
// explicit Actor (derived by the handler from the authenticated token claims),
// never a client-supplied "created_by"/"team". This is the anti-mass-assignment
// discipline made structural: the service literally cannot read identity from
// untrusted input because the input types (below) don't carry it.
// ============================================================================
package domain

import "context"

// Actor is the authenticated principal performing an operation. The handler
// builds it from the gRPC auth interceptor's TokenClaims (user id + team +
// optional service identity) — NEVER from request fields. The service stamps
// CreatedBy/TriggeredBy/Team from this, making ownership server-authoritative.
//
// WHY a struct and not two string params: it keeps the identity bundle in one
// place and lets us add fields (roles for RBAC, request id for audit) without
// changing every signature.
type Actor struct {
	// Subject is the user id (manual call) OR a service identity such as
	// "model-monitor" (the auto-retrain trigger that closes the serve→monitor→
	// retrain loop). Recorded as CreatedBy / TriggeredBy.
	Subject string
	// Team is the caller's team, used for tenancy scoping on every list/read and
	// stamped onto created pipelines. Server-derived, never client input.
	Team string
}

// CreatePipelineInput carries ONLY the user-authorable fields for a new
// pipeline. It deliberately omits ID/CreatedBy/CreatedAt/Team (server-set from
// the Actor) — the anti-mass-assignment boundary. The handler converts the proto
// CreatePipelineRequest into this; the service never sees proto types.
type CreatePipelineInput struct {
	Name           string           // unique within the actor's team
	Type           PipelineType     // saga / DAG / batch (immutable after create)
	Steps          []StepDefinition // validated: known types, valid edges, acyclic, <= cap
	IdempotencyKey string           // optional dedup key for the create itself
}

// UpdatePipelineInput carries the editable fields of an existing pipeline. Type
// is intentionally absent — changing a pipeline's execution model out from under
// in-flight runs is unsafe, so the service keeps it immutable (create a new
// pipeline instead). ID selects the target; it is not itself mutable.
type UpdatePipelineInput struct {
	ID    string           // target pipeline (selected, not mutated)
	Name  string           // new name (must stay unique within the team)
	Steps []StepDefinition // full REPLACE of the step graph (re-validated)
}

// TriggerInput carries what a run is started with. The Execution it produces is
// entirely server-owned; the client supplies only the pipeline id, an optional
// input payload, and an idempotency key.
type TriggerInput struct {
	PipelineID string         // the template to run
	Input      map[string]any // run input (e.g. {"model_id": ...}); no secrets
	// IdempotencyKey makes the trigger exactly-once from the caller's view: the
	// same key returns the SAME Execution instead of starting a second run. Vital
	// for automated triggers (CI, the Model Monitor auto-retrain) where a retry
	// after a network blip must not deploy twice. Empty = no dedup (interactive).
	IdempotencyKey string
}

// PipelineService is the primary domain interface for authoring and running
// pipelines. The handler depends on this; pipelineService implements it.
type PipelineService interface {
	// -----------------------------------------------------------------------
	// TEMPLATE MANAGEMENT (authoring)
	// -----------------------------------------------------------------------

	// CreatePipeline validates the step graph (known StepTypes, depends_on and
	// compensation pointers reference real steps, no cycles, <= MaxStepsPerPipeline,
	// non-empty) and persists a new template with server-assigned
	// id/created_by/created_at/team. Returns ErrValidation (wrapped) on a bad
	// graph. Idempotent on input.IdempotencyKey.
	CreatePipeline(ctx context.Context, actor Actor, input CreatePipelineInput) (PipelineDefinition, error)

	// GetPipeline returns one template by id, scoped to the actor's team (a caller
	// cannot read another team's pipeline by guessing its id). Returns
	// ErrPipelineNotFound if absent or out of scope.
	GetPipeline(ctx context.Context, actor Actor, id string) (PipelineDefinition, error)

	// UpdatePipeline re-validates and replaces a template's name/steps in place,
	// preserving id and execution history. Type is immutable. Scoped to the
	// actor's team. Returns ErrPipelineNotFound / ErrValidation.
	UpdatePipeline(ctx context.Context, actor Actor, input UpdatePipelineInput) (PipelineDefinition, error)

	// DeletePipeline soft-deletes (archives) a template so it can no longer be
	// listed or triggered, preserving past executions' lineage. Returns
	// ErrPipelineNotFound; a later layer rejects deletion while a non-terminal
	// execution of the pipeline is in flight (FAILED_PRECONDITION).
	DeletePipeline(ctx context.Context, actor Actor, id string) error

	// ListPipelines returns a tenancy-scoped, paginated page of templates,
	// optionally filtered by type. page size is capped server-side.
	ListPipelines(ctx context.Context, actor Actor, f ListPipelinesFilter) (pipelines []PipelineDefinition, nextToken string, err error)

	// -----------------------------------------------------------------------
	// EXECUTION (running + observing)
	// -----------------------------------------------------------------------

	// TriggerExecution starts a new run of a pipeline and DRIVES IT TO A TERMINAL
	// STATE (running the saga/DAG, compensating on failure). It returns the
	// settled Execution. Idempotent on input.IdempotencyKey: the same key returns
	// the SAME Execution. Returns ErrPipelineNotFound / ErrPipelineArchived.
	//
	// SYNCHRONOUS-IN-THE-DOMAIN, ASYNC-AT-THE-EDGE: the engine here runs the run
	// to completion synchronously (which makes it deterministically testable).
	// The transport layer (handler) decides whether to run it inline or hand it to
	// a worker and return the PENDING execution immediately; the domain contract
	// is "given a trigger, produce the run and execute it."
	TriggerExecution(ctx context.Context, actor Actor, input TriggerInput) (Execution, error)

	// GetExecution returns the current state of a run (incl. its per-step
	// timeline), scoped to the actor's team. Returns ErrExecutionNotFound.
	GetExecution(ctx context.Context, actor Actor, executionID string) (Execution, error)

	// CancelExecution requests a graceful stop of an in-flight run: it transitions
	// the run to COMPENSATING (undo completed steps in reverse) and then CANCELLED,
	// returning the settled Execution. Returns ErrExecutionNotCancellable if the
	// run is already terminal, ErrExecutionNotFound if absent.
	CancelExecution(ctx context.Context, actor Actor, executionID, reason string) (Execution, error)

	// ListExecutions returns a tenancy-scoped, paginated page of runs, optionally
	// filtered by pipeline and/or status. page size is capped server-side. List
	// items may omit per-step detail (fetch GetExecution for the full timeline).
	ListExecutions(ctx context.Context, actor Actor, f ListExecutionsFilter) (executions []Execution, nextToken string, err error)
}
