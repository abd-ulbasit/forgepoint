// ports.go — the PORTS (data-access + event-publish interfaces) the Experiment
// Tracker domain depends on. These are defined IN the domain package, per the
// Hexagonal "consumer owns the port" rule.
//
// ============================================================================
// WHY THE PORTS LIVE IN THE DOMAIN (not in repository/)  — the cycle lesson
// ============================================================================
//
// The Auth service learned this the hard way (see services/auth/internal/domain/
// ports.go): if the port interfaces live in `repository` and reference domain
// types, then `repository` imports `domain` for the types — but the domain
// service impl (experiment_service_impl.go) must reference the ports, so it would
// import `repository`. That is `domain → repository → domain`, an import CYCLE
// Go rejects outright.
//
// The fix is the idiomatic Hexagonal one: "the CONSUMER defines the interface it
// needs." The domain is the consumer of persistence and of event publishing, so
// the PORTS belong here. The Postgres ADAPTER and the NATS ADAPTER (later phases)
// import `domain` and IMPLEMENT these ports — a single inward arrow, no cycle:
//
//	domain  (service + PORTS + models)         ← stdlib + uuid only
//	   ▲                 ▲
//	   │ implements      │ implements
//	postgres adapter   nats adapter            handler (proto ↔ domain)
//
// WHERE REPOSITORY INTERFACES BELONG: in Hexagonal Architecture
// the port is owned by the side that USES it (the domain). The adapter (DB/NATS)
// implements it. Defining the port on the adapter side creates a cycle the moment
// a domain consumer needs it.
package domain

import (
	"context"
	"errors"
	"time"
)

// ============================================================================
// STORAGE SENTINELS (outcomes the repository returns; the service translates
// them into the business errors in errors.go)
// ============================================================================

// ErrRepoNotFound is the STORAGE sentinel a repository port returns when a row
// does not exist. The service maps it to the right BUSINESS error
// (ErrExperimentNotFound / ErrRunNotFound) depending on what was being fetched.
// Keeping storage and business vocabularies separate means the handler never
// needs to know a database exists.
var ErrRepoNotFound = errors.New("repository: not found")

// ErrRepoConflict is the STORAGE sentinel for a unique-constraint violation
// (e.g. the (team, name) index on experiments, or the (run_id, key, step) index
// on metrics when an adapter chooses to surface a conflict rather than silently
// ignore it). The service maps it to ErrExperimentNameExists or treats it as an
// idempotent no-op on the metrics path, as appropriate.
var ErrRepoConflict = errors.New("repository: unique constraint violation")

// ============================================================================
// PAGINATION
// ============================================================================

// ListOptions carries cursor-based pagination parameters. Cursor (keyset)
// pagination is stable under concurrent writes where LIMIT/OFFSET can skip or
// duplicate rows — the same choice every Forgepoint list RPC makes. PageSize of
// 0 means "use the service default"; the service caps it before it ever reaches
// a repository (the cap is a domain invariant, not the adapter's job to enforce).
type ListOptions struct {
	PageSize  int    // 0 = service default; service caps the maximum
	PageToken string // opaque cursor; empty = first page
}

// MetricHistoryQuery bounds a metric-history read. Empty Keys = all keys on the
// run; a zero MinStep/MaxStep on either end means "no bound on that end". The
// page is cursor-paginated like everything else. The service constructs this
// from the validated request and hands it to the repository; the repository maps
// it to an indexed keyset scan over the time-partitioned metrics table.
type MetricHistoryQuery struct {
	RunID      string
	Keys       []string // empty = all metric keys on the run
	MinStep    int64    // inclusive lower bound; 0 with MaxStep 0 = no bound
	MaxStep    int64    // inclusive upper bound; 0 with MinStep 0 = no bound
	HasMinStep bool     // distinguishes "min step 0" from "no min bound"
	HasMaxStep bool     // distinguishes "max step 0" from "no max bound"
	Pagination ListOptions
}

// ============================================================================
// REPOSITORY PORTS
// ============================================================================

// ExperimentRepository is the persistence port for Experiment. The Postgres
// adapter implements it; domain tests inject hand-written mocks.
//
// All reads/writes are scoped by the caller's Team at the SERVICE layer — the
// repository methods take the already-resolved values; tenancy enforcement
// (deriving Team from auth claims, never from a request field) happens in the
// service before these are ever called. That keeps the IDOR/tenancy boundary in
// ONE place (the service) rather than smeared across every adapter method.
type ExperimentRepository interface {
	// Create persists a new Experiment (id/owner/team/timestamps already set by
	// the service). Returns ErrRepoConflict if (team, name) collides.
	Create(ctx context.Context, exp Experiment) (Experiment, error)
	// GetByID returns the Experiment, or ErrRepoNotFound. The service checks the
	// returned Team against the caller's claims (defense in depth) even though
	// the adapter may also filter by team.
	GetByID(ctx context.Context, id string) (Experiment, error)
	// Update persists mutable fields (name/description/tags/updated_at). The
	// service has already applied the field mask and refreshed UpdatedAt.
	Update(ctx context.Context, exp Experiment) (Experiment, error)
	// List returns a page of Experiments for a team (created_at DESC) plus a
	// nextToken cursor. includeArchived toggles soft-deleted rows.
	List(ctx context.Context, team string, includeArchived bool, opts ListOptions) (exps []Experiment, nextToken string, err error)
}

// RunRepository is the persistence port for Run + its params/metrics. A run, its
// params, and its metric points form one aggregate owned by this service; the
// metric points live in a separate time-partitioned table but are reached
// through the run.
type RunRepository interface {
	// CreateRun persists a new Run (server-authoritative fields already set).
	CreateRun(ctx context.Context, run Run) (Run, error)
	// GetRun returns the Run with params + final metrics + artifacts (NOT the
	// full metric series), or ErrRepoNotFound.
	GetRun(ctx context.Context, id string) (Run, error)
	// ListRuns returns a page of runs in an experiment, optionally filtered by
	// status (RunStatusUnspecified = no filter), plus a nextToken cursor.
	ListRuns(ctx context.Context, experimentID string, statusFilter RunStatus, opts ListOptions) (runs []Run, nextToken string, err error)
	// ListRunsByTeam returns a page of ALL runs owned by a team (across every
	// experiment), newest-first, optionally status-filtered. Used by the unscoped
	// runs view (no experiment_id). Team is the tenancy boundary: runs join their
	// parent experiment and only that team's runs are returned.
	ListRunsByTeam(ctx context.Context, team string, statusFilter RunStatus, opts ListOptions) (runs []Run, nextToken string, err error)
	// UpdateRunStatus transitions a run to a terminal state and stamps EndedAt
	// and the computed FinalMetrics in ONE write. The service has already
	// validated the transition and computed the finals. Returns the updated Run.
	UpdateRunStatus(ctx context.Context, runID string, status RunStatus, endedAt time.Time, finalMetrics []MetricPoint) (Run, error)
	// DeleteRun hard-deletes a run and its params/metrics. The service has
	// already enforced the lineage guard (no live model reference). Returns
	// ErrRepoNotFound if the run is already gone (the service may treat that as
	// idempotent success when an idempotency key matches).
	DeleteRun(ctx context.Context, runID string) error

	// AppendMetrics writes a batch of metric points for a run in one
	// transactional, idempotent multi-row INSERT. The points are ALREADY deduped
	// and timestamp-stamped by the service; the adapter relies on a UNIQUE
	// (run_id, key, step) index with ON CONFLICT DO NOTHING so a retried batch
	// double-writes nothing. Returns the count actually written (post-conflict),
	// which the service surfaces as accepted_count. This is the high-throughput
	// write the entire event-driven pattern optimizes: ONE INSERT per batch, not
	// one per point.
	AppendMetrics(ctx context.Context, runID string, points []MetricPoint) (written int, err error)
	// AppendParams writes write-once params. The service has resolved conflicts
	// (rejecting changed values, skipping identical re-logs); the adapter inserts
	// the new keys. Returns the count newly written.
	AppendParams(ctx context.Context, runID string, params []Param) (written int, err error)
	// GetRunParams returns a run's existing params — the service reads these to
	// enforce write-once semantics before AppendParams.
	GetRunParams(ctx context.Context, runID string) ([]Param, error)
	// GetMetricHistory returns the metric points matching the query (key/step
	// filters), paginated, ordered by (key, step, timestamp). The full series for
	// CompareRuns is read via repeated calls or a higher cap.
	GetMetricHistory(ctx context.Context, q MetricHistoryQuery) (points []MetricPoint, nextToken string, err error)

	// SetArtifacts replaces the run's free-form artifacts map (already
	// size-validated by the service) and returns the updated run.
	SetArtifacts(ctx context.Context, runID string, artifacts map[string]any) (Run, error)
}

// IdempotencyStore records (idempotency_key → result_id) for mutating RPCs so a
// retried request returns the SAME result instead of creating a duplicate. This
// is the Stripe Idempotency-Key pattern. It is a PORT (not baked into a repo)
// because the same store backs StartRun, LogMetrics, DeleteRun, and
// SetRunArtifacts — and could be Redis or a Postgres table depending on the
// deployment. An empty key means "no idempotency requested"; the service skips
// the store entirely in that case.
type IdempotencyStore interface {
	// Lookup returns the previously-recorded result id for a key on a given
	// operation, and whether it was found. The (operation, key) tuple scopes keys
	// per-RPC so the same client-generated UUID reused across different RPCs
	// doesn't collide.
	Lookup(ctx context.Context, operation, key string) (resultID string, found bool, err error)
	// Record stores (operation, key → resultID). Called after the mutation
	// succeeds so a later retry of the SAME key replays the SAME result.
	Record(ctx context.Context, operation, key, resultID string) error
}

// ============================================================================
// EVENT PUBLISHER PORT (the OUTBOUND async side)
// ============================================================================

// EventPublisher is the port the service uses to PUBLISH domain events to the
// platform bus. The NATS adapter (events layer) implements it by marshalling
// these domain payloads into the canonical events.* proto types and wrapping
// them in an EventEnvelope. Defining a SMALL, domain-shaped port here (rather
// than handing the service a *nats.Conn) keeps the domain free of NATS imports
// and makes publishing trivially mockable in tests ("did finishing a run emit a
// RunFinished with the right final metrics?").
//
// NOTE on the consume side: the high-volume INBOUND path (the NATS batch
// consumer that buffers and flushes platform events) is NOT a domain port — it
// is an ADAPTER that DECODES events into domain MetricPoints/Runs and calls the
// ordinary service methods (LogMetrics/StartRun). The domain doesn't model "a
// subscription"; it models the business operations a subscription ultimately
// invokes. So only the OUTBOUND publisher needs a port here.
type EventPublisher interface {
	// PublishRunCreated emits fp.experiments.run.created. Best-effort from the
	// service's perspective: a publish failure is logged but does NOT roll back
	// the run (the run is the source of truth; the event is a derived
	// notification). The adapter handles envelope/retry/DLQ concerns.
	PublishRunCreated(ctx context.Context, ev RunCreatedEvent) error
	// PublishRunFinished emits fp.experiments.run.finished — the FAT event
	// carrying final_metrics so a leaderboard/notification consumer ranks/alerts
	// without a GetRun callback (terminal-run metrics are final, so no staleness).
	PublishRunFinished(ctx context.Context, ev RunFinishedEvent) error
}

// RunCreatedEvent is the DOMAIN payload for fp.experiments.run.created. It is a
// plain domain struct, NOT the generated events.RunCreated proto — the events
// adapter maps domain→proto at the publish boundary (the same anti-corruption
// boundary the handler uses for the gRPC surface). This keeps the domain
// decoupled from the wire schema; if events.proto's field layout changes, only
// the adapter's mapping changes, not the business logic or its tests.
type RunCreatedEvent struct {
	RunID          string
	ExperimentID   string
	ModelVersionID string
	DisplayName    string
	StartedAt      time.Time
}

// RunFinishedEvent is the DOMAIN payload for fp.experiments.run.finished. It
// carries the terminal status and the denormalized FinalMetrics (the "fat
// event" — see EventPublisher.PublishRunFinished).
type RunFinishedEvent struct {
	RunID          string
	ExperimentID   string
	ModelVersionID string
	Status         RunStatus
	FinalMetrics   []MetricPoint
	EndedAt        time.Time
}
