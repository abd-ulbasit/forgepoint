// experiment_service.go defines ExperimentService — the primary port through
// which the handler (and the async events adapter) reach the business logic.
//
// ============================================================================
// THE SERVICE INTERFACE AS A "PORT" (Hexagonal / Clean Architecture)
// ============================================================================
//
// The domain owns this interface; the handler depends on it; the concrete impl
// (experimentService in experiment_service_impl.go) lives in the same package
// but is reached only through this interface (NewExperimentService returns the
// interface, not the struct). This is dependency inversion: the handler — and
// the NATS batch consumer — call ExperimentService.LogMetrics without knowing or
// caring whether it's backed by Postgres or an in-memory test stub.
//
// WHY the input types are dedicated structs (not loose args): adding a field
// later (e.g. a new run attribute) is backward-compatible, and callers construct
// them with named fields so there's no risk of swapping two positional strings.
// The handler converts proto Request → these inputs; the service never sees a
// proto type. This is the anti-corruption boundary in DDD terms.
//
// SECURITY THREADING (caller identity): every mutating method takes an Actor —
// the server-authoritative identity resolved from the auth interceptor's claims.
// The service uses Actor.Team as the TENANCY boundary and Actor.UserID for
// OwnerID. Methods NEVER read owner/team from the input structs (those fields
// don't exist on the inputs precisely to make mass-assignment impossible).
package domain

import (
	"context"
	"time"
)

// Actor is the server-authoritative identity of the caller, lifted from the
// validated TokenClaims by the handler/interceptor and passed explicitly into
// the service. WHY pass it explicitly rather than read it from the context
// inside the domain: the domain must not import the gRPC/context-key machinery
// or know how identity was transported. An explicit Actor argument keeps the
// domain pure AND makes the security-relevant input obvious and testable.
type Actor struct {
	UserID string // becomes OwnerID on created resources
	Team   string // the tenancy boundary; all reads/writes are scoped to it
}

// CreateExperimentInput carries ONLY client-owned fields. Note what's ABSENT:
// id, owner, team, timestamps — all server-authoritative (the mass-assignment
// guard). Owner/Team come from the Actor, not from here.
type CreateExperimentInput struct {
	Name        string
	Description string
	Tags        map[string]string
}

// UpdateExperimentInput edits mutable fields via a field mask. UpdateFields
// lists which of {"name","description","tags"} to apply; an unknown name is
// rejected (ErrValidation). WHY a mask: a client editing only the description
// must not have to round-trip name/tags and risk clobbering a concurrent edit.
// Tags, when masked in, REPLACE the whole map (replace-not-merge so a tag can be
// deleted — merge semantics make deletion impossible).
type UpdateExperimentInput struct {
	ID           string
	Name         string
	Description  string
	Tags         map[string]string
	UpdateFields []string
}

// StartRunInput opens a new run. SERVER-AUTHORITATIVE and therefore ABSENT:
// id, status (always RUNNING), source (API on this path), owner, started_at,
// final_metrics. IdempotencyKey (optional) makes a retried StartRun return the
// SAME run instead of creating a duplicate.
type StartRunInput struct {
	ExperimentID   string
	DisplayName    string
	ModelVersionID string
	Params         []Param
	IdempotencyKey string
}

// LogMetricsInput is the batch-ingestion input — the heart of the event-driven
// pattern. Points carry client-supplied key/value/step; the service stamps the
// timestamp and dedups by (key, step). IdempotencyKey lets the highest-volume,
// most-retried mutation avoid double-writing a batch.
type LogMetricsInput struct {
	RunID          string
	Points         []MetricPoint
	IdempotencyKey string
}

// LogMetricsResult reports how many points were durably accepted by THIS call
// (post-dedup, post-conflict). It may be less than len(Points) on idempotent
// replay or intra-batch duplicates — batch APIs should tell the caller what
// landed so it can reconcile.
type LogMetricsResult struct {
	AcceptedCount int
}

// LogParamsInput appends write-once params to a RUNNING run.
type LogParamsInput struct {
	RunID  string
	Params []Param
}

// LogParamsResult reports how many params were NEWLY written (excludes
// idempotent no-op re-logs of an identical key/value).
type LogParamsResult struct {
	AcceptedCount int
}

// FinishRunInput transitions a run to a terminal state. WHY a dedicated input
// (and method) rather than a generic "UpdateRun": status is security-sensitive
// and state-machine-constrained, so it gets its own narrow, auditable mutation.
// TargetStatus must be terminal (FINISHED/FAILED/KILLED); the service rejects
// RUNNING/Unspecified and illegal transitions.
type FinishRunInput struct {
	RunID        string
	TargetStatus RunStatus
}

// SetArtifactsInput attaches free-form JSON-shaped artifacts to a run.
// Artifacts is size-capped server-side (MaxArtifactsBytes). IdempotencyKey lets
// a retried attach replay to the same effect.
type SetArtifactsInput struct {
	RunID          string
	Artifacts      map[string]any
	IdempotencyKey string
}

// CompareRunsInput asks for the metric series of several runs side-by-side. Run
// count is capped (MaxCompareRuns); MetricKeys narrows the payload (empty = all).
type CompareRunsInput struct {
	RunIDs     []string
	MetricKeys []string
}

// RunComparison is the per-run slice of a CompareRuns result: the run's metadata
// (params, final metrics) plus its requested metric series (the curves).
type RunComparison struct {
	Run    Run
	Series []MetricSeries
}

// GetMetricHistoryInput reads ONE run's full metric time-series, paginated, with
// optional key and step-window filters. HasMinStep/HasMaxStep distinguish a real
// bound of 0 from "no bound".
type GetMetricHistoryInput struct {
	RunID      string
	MetricKeys []string
	MinStep    int64
	MaxStep    int64
	HasMinStep bool
	HasMaxStep bool
	Pagination ListOptions
}

// ExperimentService is the primary domain interface. The handler holds a value
// of this interface; the NATS batch consumer holds the same value and calls
// LogMetrics/StartRun on it. The repository ports and the event publisher are
// injected into the concrete impl (NewExperimentService).
type ExperimentService interface {
	// ---- EXPERIMENTS ----

	// CreateExperiment creates a named grouping of runs. Owner/Team come from the
	// Actor (server-authoritative). Returns ErrExperimentNameExists on a
	// (team, name) collision, ErrValidation on bad input.
	CreateExperiment(ctx context.Context, actor Actor, in CreateExperimentInput) (Experiment, error)
	// GetExperiment fetches one experiment, scoped to the actor's team
	// (ErrExperimentNotFound if missing or not in the team — the same error for
	// both, to avoid leaking existence across tenants).
	GetExperiment(ctx context.Context, actor Actor, id string) (Experiment, error)
	// ListExperiments returns a page scoped to the actor's team. The page size is
	// capped server-side (DefaultPageSize / MaxPageSize).
	ListExperiments(ctx context.Context, actor Actor, includeArchived bool, opts ListOptions) (exps []Experiment, nextToken string, err error)
	// UpdateExperiment applies a field-masked edit to mutable fields and refreshes
	// UpdatedAt. Owner/team/timestamps are not editable.
	UpdateExperiment(ctx context.Context, actor Actor, in UpdateExperimentInput) (Experiment, error)
	// ArchiveExperiment SOFT-deletes an experiment (sets ArchivedAt) while
	// preserving its runs/metrics for lineage/audit. Idempotent: archiving an
	// already-archived experiment returns it unchanged.
	ArchiveExperiment(ctx context.Context, actor Actor, id string) (Experiment, error)

	// ---- RUN LIFECYCLE ----

	// StartRun opens a RUNNING run in an experiment (source = API). Honors
	// IdempotencyKey (a retry returns the same run). On success it PUBLISHES a
	// RunCreated event (best-effort). The experiment must exist and belong to the
	// actor's team.
	StartRun(ctx context.Context, actor Actor, in StartRunInput) (Run, error)
	// FinishRun transitions a run to a terminal state, validates the transition,
	// stamps EndedAt, computes FinalMetrics from the run's series, and PUBLISHES
	// a RunFinished fat event. This is the design doc's run-completion path.
	FinishRun(ctx context.Context, actor Actor, in FinishRunInput) (Run, error)
	// GetRun returns a run's metadata + params + headline metrics + artifacts
	// (NOT the full series — use GetMetricHistory).
	GetRun(ctx context.Context, actor Actor, id string) (Run, error)
	// ListRuns returns a page of runs in an experiment, optionally filtered by
	// status. Page size capped server-side.
	ListRuns(ctx context.Context, actor Actor, experimentID string, statusFilter RunStatus, opts ListOptions) (runs []Run, nextToken string, err error)
	// DeleteRun hard-deletes a run + its metrics/params. Idempotent via
	// IdempotencyKey. (The Registry-lineage guard described in the proto is a
	// cross-service check wired when the Registry client exists; the domain
	// enforces ownership/team and the idempotency replay here.)
	DeleteRun(ctx context.Context, actor Actor, runID, idempotencyKey string) error

	// ---- INGESTION (sync path; the async path calls the SAME methods) ----

	// LogMetrics is the high-throughput BATCH ingestion operation: validate, cap,
	// dedup by (key, step), server-stamp timestamps, and write the batch in one
	// transactional, idempotent INSERT. Returns the accepted count. The run must
	// be RUNNING. This single method backs BOTH the gRPC LogMetrics RPC and the
	// NATS batch consumer's flush — the whole point of modelling ingestion as a
	// transport-agnostic batch operation.
	LogMetrics(ctx context.Context, actor Actor, in LogMetricsInput) (LogMetricsResult, error)
	// LogParams appends write-once params to a RUNNING run. Re-logging an
	// identical key/value is an idempotent no-op; a changed value is
	// ErrParamConflict.
	LogParams(ctx context.Context, actor Actor, in LogParamsInput) (LogParamsResult, error)
	// SetRunArtifacts attaches size-capped, JSON-shaped artifacts to a run.
	SetRunArtifacts(ctx context.Context, actor Actor, in SetArtifactsInput) (Run, error)

	// ---- ANALYSIS / READ ----

	// GetMetricHistory returns ONE run's metric series, paginated, with optional
	// key/step filters. Page size capped server-side (DefaultMetricPageSize /
	// MaxMetricPageSize — higher than list RPCs because points are tiny).
	GetMetricHistory(ctx context.Context, actor Actor, in GetMetricHistoryInput) ([]MetricSeries, string, error)
	// CompareRuns returns several runs' metric series side-by-side — the data
	// behind "which run/model version wins?". Run count capped (MaxCompareRuns).
	CompareRuns(ctx context.Context, actor Actor, in CompareRunsInput) ([]RunComparison, error)
}

// Clock is injected so tests can supply a deterministic time instead of
// time.Now(). WHY a port for the clock: timestamps are SERVER-AUTHORITATIVE in
// this service (metric Timestamp, StartedAt, EndedAt), so they are part of the
// behavior under test. A fixed clock lets a test assert the EXACT stamped time
// rather than "some time near now". The production wiring passes a real clock.
type Clock interface {
	Now() time.Time
}

// realClock is the production Clock backed by the wall clock.
type realClock struct{}

// Now returns the current UTC time. WHY UTC: storing/serializing timestamps in
// UTC avoids ambiguity across pod time zones and makes partition-key math
// (UTC day boundaries) unambiguous.
func (realClock) Now() time.Time { return time.Now().UTC() }

// NewRealClock returns the wall-clock implementation for production wiring.
func NewRealClock() Clock { return realClock{} }
