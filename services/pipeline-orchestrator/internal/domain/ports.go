// ports.go — the PORTS (interfaces) the Pipeline Orchestrator domain depends on.
//
// ============================================================================
// HEXAGONAL "CONSUMER-OWNED PORTS" — WHY THEY LIVE IN THE DOMAIN
// ============================================================================
//
// Per docs/design/service-architecture.md (learned the hard way in Auth): the
// port interfaces MUST live in the domain, not in repository/. The domain's
// service impl references them, and their signatures reference domain types, so
// putting them in repository/ creates a domain → repository → domain import
// CYCLE the moment the domain consumes them. "The consumer owns the port"
// (hexagonal) avoids it: the domain (the consumer of persistence and of step
// execution) declares the interfaces; the adapters (Postgres, the real K8s/HTTP
// step executors, the NATS publisher) implement them with a single inward arrow.
//
//	domain  (PipelineService + PORTS + saga engine + models)   ← stdlib + uuid
//	   ▲ implements
//	   ├── postgres adapter   (ExecutionRepository, PipelineRepository)
//	   ├── executor adapters  (StepExecutor per StepType — talk to K8s/Registry/GW)
//	   └── nats publisher      (EventPublisher)
//
// THE STAR PORT IS StepExecutor. The saga ENGINE (pipeline_service_impl.go) is
// pure: it sequences steps, runs compensations in reverse, and drives the state
// machine — but it never KNOWS how a DEPLOY actually creates a K8s Deployment or
// how a CANARY shifts traffic. That side-effecting work is behind the
// StepExecutor port. This is exactly why the engine is unit-testable with
// hand-written mock executors that simulate success/failure/panic without any
// real infrastructure — and why the saga logic is readable in isolation from
// the I/O.
// ============================================================================
package domain

import (
	"context"
	"errors"
	"time"
)

// ============================================================================
// STORAGE SENTINEL (the one outcome the repository ports signal structurally)
// ============================================================================

// ErrRepoNotFound is the STORAGE sentinel a repository port returns when a row
// does not exist. The service translates it into the right BUSINESS error
// (ErrPipelineNotFound or ErrExecutionNotFound). Keeping a distinct storage
// vocabulary means the handler never imports or knows about persistence.
var ErrRepoNotFound = errors.New("repository: not found")

// ============================================================================
// PAGINATION
// ============================================================================

// ListOptions carries cursor-based pagination for list ports. WHY cursor (not
// LIMIT/OFFSET): a cursor is stable under concurrent writes — OFFSET can skip or
// duplicate rows when the underlying set shifts between pages. PageSize is capped
// server-side by the service (default 20, max 100) regardless of what a client
// requests, so a caller can't demand an unbounded page.
type ListOptions struct {
	PageSize  int    // 0 = use the service default; values above the cap are clamped
	PageToken string // opaque cursor; empty = start from the beginning
}

// ============================================================================
// EXECUTION FILTERS (server-side, tenancy-scoped)
// ============================================================================

// ListExecutionsFilter narrows a ListExecutions query. NOTE the deliberate
// ABSENCE of a Team field: tenancy is enforced SERVER-SIDE from auth claims, not
// from a client-supplied field (anti-IDOR). The Team below is set by the service
// from the caller's token, never from the request — it is here so the repository
// can scope the SQL, but the SERVICE owns its value.
type ListExecutionsFilter struct {
	Team       string          // SERVER-SET from auth claims (tenancy scope) — never client input
	PipelineID string          // optional: only runs of this pipeline ("" = any)
	Status     ExecutionStatus // optional: only runs in this status (Unspecified = any)
	List       ListOptions
}

// ListPipelinesFilter narrows a ListPipelines query. Same tenancy discipline:
// Team is server-set; Archived pipelines are excluded unless explicitly asked.
type ListPipelinesFilter struct {
	Team string       // SERVER-SET from auth claims — never client input
	Type PipelineType // optional: only this type (Unspecified = any)
	List ListOptions
}

// ============================================================================
// REPOSITORY PORTS
// ============================================================================

// PipelineRepository is the persistence port for PipelineDefinition (templates).
// The Postgres adapter implements it; tests inject hand-written mocks.
//
// IDEMPOTENCY (Create): idempotencyKey lets a retried CreatePipeline return the
// SAME pipeline instead of a duplicate. The adapter stores key → pipeline_id and,
// on a repeat key, returns the original. Empty key = no dedup. This is the Stripe
// idempotency-key pattern, mirrored across the platform's mutating RPCs.
type PipelineRepository interface {
	// Create persists a new PipelineDefinition. If idempotencyKey is non-empty and
	// was seen before, returns the ORIGINAL pipeline (no duplicate). The caller
	// (service) has already set the server-authoritative fields (ID, CreatedBy,
	// CreatedAt, Team) before calling.
	Create(ctx context.Context, p PipelineDefinition, idempotencyKey string) (PipelineDefinition, error)

	// GetByID returns the pipeline with that id, or ErrRepoNotFound. Includes
	// archived pipelines (the service decides whether an archived template is
	// usable for the operation at hand).
	GetByID(ctx context.Context, id string) (PipelineDefinition, error)

	// Update replaces the editable fields (name, steps) of an existing pipeline,
	// preserving id/created_by/created_at/team. Returns ErrRepoNotFound if absent.
	Update(ctx context.Context, p PipelineDefinition) (PipelineDefinition, error)

	// Archive soft-deletes (sets archived) a pipeline so it can no longer be
	// listed or triggered. Returns ErrRepoNotFound if absent. WHY soft-delete:
	// past executions reference this id for audit/lineage.
	Archive(ctx context.Context, id string) error

	// List returns a tenancy-scoped page of pipelines (newest first) plus a
	// nextToken cursor. Archived pipelines are excluded.
	List(ctx context.Context, f ListPipelinesFilter) (pipelines []PipelineDefinition, nextToken string, err error)
}

// ExecutionRepository is the persistence port for Execution + StepExecution
// (runs). This is the DURABILITY backbone of the saga: every step checkpoint is
// written here BEFORE the step runs (write-ahead), so a crashed orchestrator can
// reload the last persisted state and RESUME rather than restart.
//
// WHY SaveStep is separate from Save(Execution): a step transition is the
// finest-grained durable unit — the engine persists each step state change
// (PENDING→RUNNING→COMPLETED/FAILED, and the compensation sub-states)
// individually so a crash between steps loses at most the in-flight step, not
// the whole run. Save(Execution) records the coarse execution-level transitions
// (RUNNING→COMPENSATING→FAILED) and the input/error fields.
type ExecutionRepository interface {
	// Create persists a new Execution in PENDING (with its PENDING StepExecutions
	// already populated as write-ahead checkpoints). If idempotencyKey is
	// non-empty and was seen, returns the ORIGINAL execution (exactly-once trigger
	// from the caller's view — a retry never double-runs a deployment saga).
	Create(ctx context.Context, e Execution, idempotencyKey string) (Execution, error)

	// Save persists execution-level changes (status, current_step, completed_at,
	// error). Used for the coarse state-machine transitions.
	Save(ctx context.Context, e Execution) error

	// SaveStep persists a single StepExecution transition — the write-ahead
	// checkpoint. Called before a step runs (→RUNNING) and after it settles
	// (→COMPLETED/FAILED/COMPENSATED/...). This row is what crash recovery reads.
	SaveStep(ctx context.Context, step StepExecution) error

	// GetByID returns the Execution (with its StepExecutions) or ErrRepoNotFound.
	GetByID(ctx context.Context, id string) (Execution, error)

	// List returns a tenancy-scoped page of executions (newest first) + cursor.
	// List items may omit per-step detail (the service documents that contract).
	List(ctx context.Context, f ListExecutionsFilter) (executions []Execution, nextToken string, err error)
}

// ============================================================================
// STEP EXECUTION PORT — the saga's effect boundary (THE star port)
// ============================================================================

// StepInput is the immutable bundle the engine hands a StepExecutor when running
// a step. It is constructed entirely by the engine from SERVER-AUTHORITATIVE
// data — never directly from client input — which is what makes the SSRF guard
// real rather than a comment.
type StepInput struct {
	ExecutionID string         // the run this step belongs to
	StepID      string         // template step id being executed
	StepType    StepType       // which kind of work (the executor is keyed on this)
	Config      map[string]any // the step's static config (UNTRUSTED — executor validates)
	Input       map[string]any // the run-level trigger input (echoed for the step)

	// UpstreamOutputs maps a completed dependency's step id → its Output. This is
	// the DAG data-flow channel: a REGISTER step reads the TRAIN step's
	// {"model_uri": ...} here. Only COMPLETED upstream steps appear.
	UpstreamOutputs map[string]map[string]any

	// Attempt is the 1-based attempt number for this run of the step (incremented
	// on each retry). Lets an executor make a retry idempotent (e.g. "if attempt
	// > 1, check whether the resource I was creating already exists").
	Attempt int
}

// StepResult is what a StepExecutor returns from a forward (Execute) run.
//
// SSRF GUARD: for DEPLOY/CANARY/PROMOTE, the serving
// Endpoint a downstream consumer (Inference Gateway / Model Serving) will route
// to is RESOLVED BY THE EXECUTOR ITSELF — constructed from the model version and
// the K8s Service the executor created in the fp-models namespace. It is NEVER
// read from the (client-supplied, untrusted) step Config. Accepting a client URL
// as the route target would let a caller point gateway traffic at an arbitrary
// internal host — a server-side request forgery. The port models this by having
// the EXECUTOR return the endpoint (an output it constructed) rather than the
// engine passing a client value in. The engine treats Endpoint as opaque and
// only forwards it to the (internal, trusted) ModelDeployed event.
type StepResult struct {
	// Output is the step's result, persisted on the StepExecution and fed to
	// dependent steps as UpstreamOutputs. No secrets (it is echoed in events).
	Output map[string]any

	// Endpoint is the server-RESOLVED serving address for DEPLOY/PROMOTE steps
	// (empty for steps that don't deploy). Populated by the executor from the K8s
	// Service it created — never from Config. Carried into the ModelDeployed event.
	Endpoint string
}

// StepExecutor performs the actual, side-effecting work of ONE step kind and
// knows how to UNDO it. There is one implementation per StepType (DeployExecutor,
// CanaryExecutor, TrainExecutor, ...), registered in the engine's
// ExecutorRegistry. The engine calls Execute on the forward pass and Compensate
// on rollback.
//
// THE COMPENSATION CONTRACT (the heart of the saga):
//
//   - Compensate UNDOES a previously-COMPLETED Execute. The engine only calls it
//     for steps that actually completed (a step that never ran has no effect to
//     undo) — see the engine's compensateCompleted loop.
//
//   - Compensate MUST BE IDEMPOTENT and best-effort. It may be retried, and it
//     may run against a partially-applied effect (the forward step crashed
//     mid-way). "Destroy the serving instance" must succeed (or be a no-op) even
//     if the instance was only half-created. A Compensate that returns an error
//     is NOT swallowed: the engine marks the step COMPENSATION_FAILED and the
//     saga ends FAILED with ErrCompensationFailed — a loud, paged "stuck saga".
//
//   - A step with no real effect (a read-only VALIDATE, or any step whose
//     StepDefinition has no CompensationStepID) need not be compensated; the
//     engine skips it. Compensate is only invoked for steps that have a
//     compensation defined.
//
// IF A COMPENSATION ITSELF FAILS: this is the
// worst case. Compensation is best-effort and idempotent by design; a failed
// compensation is surfaced as COMPENSATION_FAILED + an alert (NOT a silent
// state), because we may have leaked a real resource (an orphaned serving pod).
// The honest answer is "we make compensation idempotent so retries are safe, and
// we page a human when it still fails — we never pretend the rollback succeeded."
type StepExecutor interface {
	// Execute performs the forward action of the step. A returned error means the
	// step FAILED; the engine retries up to the step's MaxRetries (clamped), then,
	// in a saga, begins compensation of the already-completed steps in reverse.
	//
	// CONTEXT: the engine passes a context that is CANCELLED on CancelExecution
	// and on per-step Timeout. A well-behaved executor honors ctx.Done() and
	// stops promptly so cancellation is responsive.
	Execute(ctx context.Context, in StepInput) (StepResult, error)

	// Compensate UNDOES a previously-completed Execute. See the contract above:
	// idempotent, best-effort, only called for completed steps that have a
	// compensation. A returned error → COMPENSATION_FAILED (a stuck saga).
	Compensate(ctx context.Context, in StepInput) error
}

// ExecutorRegistry maps a StepType to the StepExecutor that runs it. The engine
// looks an executor up here for each step; a missing entry for a known type is a
// wiring bug surfaced as ErrNoExecutor. Modeled as a port so tests inject a map
// of mock executors and production injects the real K8s/Registry/Gateway-backed
// ones — the engine code is identical in both.
type ExecutorRegistry interface {
	// Executor returns the StepExecutor for the given type, or (nil, false) if
	// none is registered. The engine turns the false case into ErrNoExecutor.
	Executor(t StepType) (StepExecutor, bool)
}

// ============================================================================
// EVENT PUBLISHER PORT
// ============================================================================

// EventPublisher is the port through which the engine emits saga lifecycle
// signals (PipelineStarted, StepCompleted, StepFailed, CompensationTriggered,
// PipelineCompleted, PipelineFailed, ModelDeployed, ModelUndeployed). The NATS
// adapter (events/) implements it by marshaling the domain event into the
// canonical forgepoint/events/v1 payload inside an EventEnvelope.
//
// WHY a domain-level event type (StepEvent below) and not the proto event here:
// the dependency rule again. The engine emits framework-free StepEvent values;
// the adapter maps them to the wire payloads at the boundary. The engine never
// imports the generated event types.
//
// BEST-EFFORT / NON-BLOCKING contract: publishing a lifecycle event MUST NOT
// fail the saga. A NATS hiccup should not roll back a successful deployment. The
// adapter logs/retries internally; a Publish error is recorded but does not
// change the Execution's outcome. (Reliable delivery, if needed, is the Outbox
// pattern's job — the Billing service — not this best-effort lifecycle feed.)
type EventPublisher interface {
	// Publish emits one lifecycle event. The subject is derived from ev.Type by
	// the adapter (e.g. EventStepCompleted → "fp.pipelines.step.completed").
	// Returns an error only for the adapter's own bookkeeping; the engine does
	// not abort on a publish failure (see the best-effort contract above).
	Publish(ctx context.Context, ev StepEvent) error
}

// EventType enumerates the lifecycle events the engine emits. Kept as a domain
// enum (not the proto enum) so the engine stays framework-free; the publisher
// adapter maps each to its NATS subject + events.v1 payload.
type EventType int

const (
	EventPipelineStarted EventType = iota
	EventStepCompleted
	EventStepFailed
	EventCompensationTriggered
	EventPipelineCompleted
	EventPipelineFailed
	EventModelDeployed   // DEPLOY/PROMOTE saga step succeeded — gateway/serving consume it
	EventModelUndeployed // teardown / rollback compensation — gateway/serving consume it
)

// StepEvent is the framework-free payload the engine hands EventPublisher. It
// carries IDs + a snapshot of the essential fields (THIN-EVENT / FAT-READ: a
// consumer wanting the full timeline calls GetExecution). The model-deploy events
// are the exception — they carry Endpoint so the gateway can act with no callback.
type StepEvent struct {
	Type         EventType
	ExecutionID  string
	PipelineID   string
	PipelineType PipelineType
	StepID       string         // empty for execution-level events
	StepType     StepType       // zero for execution-level events
	TriggeredBy  string         // who/what started the run (for PipelineStarted)
	Error        string         // failing-step reason (StepFailed / PipelineFailed)
	Attempts     int            // attempts made (StepFailed)
	Endpoint     string         // server-resolved serving address (ModelDeployed only)
	Output       map[string]any // step output (StepCompleted)
	OccurredAt   time.Time      // producer clock
}

// ============================================================================
// CLOCK + ID PORTS (so the engine is deterministic under test)
// ============================================================================

// Clock abstracts "now" so tests can use a fixed/controlled time and assert on
// timestamps and ordering deterministically. Production injects a real clock.
// WHY a port and not time.Now() directly: the compensator orders by CompletedAt;
// a controllable clock lets a test prove the REVERSE ordering with exact stamps
// rather than relying on wall-clock flakiness.
type Clock interface {
	Now() time.Time
}

// IDGenerator abstracts UUID generation so tests can inject deterministic ids
// (making assertions on execution/step ids stable) while production uses
// crypto-random UUIDv4. (uuid is allowed in the domain per the task's dependency
// rules; the port exists for testability, not to avoid the import.)
type IDGenerator interface {
	NewID() string
}
