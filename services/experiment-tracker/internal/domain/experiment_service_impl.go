// experiment_service_impl.go — the concrete ExperimentService implementation.
//
// ============================================================================
// CLEAN ARCHITECTURE PLACEMENT
// ============================================================================
//
// experimentService lives in the domain package and depends ONLY on:
//   - the repository/idempotency/publisher PORTS (interfaces, not adapters),
//   - the Clock port, and
//   - stdlib + github.com/google/uuid.
//
// It imports NO gRPC, NO NATS, NO database driver, NO generated proto. The
// handler injects the real Postgres/NATS adapters at wire time (main.go); tests
// inject mocks. This is dependency inversion: the business logic dictates the
// contracts; the infrastructure conforms.
//
// ============================================================================
// PATTERN — Event-Driven (Async Batch Ingestion): the domain's role
// ============================================================================
//
// LogMetrics is the centerpiece. It implements the per-batch BUSINESS rules that
// BOTH ingestion paths share:
//
//	validate → cap → DEDUP by (key,step) → server-stamp timestamps → idempotent
//	single transactional write → report accepted count
//
// The sync gRPC handler and the async NATS batch consumer both call THIS method.
// The consumer's buffering/flushing/back-pressure lives in the events adapter; it
// hands an already-assembled batch to LogMetrics. So the domain owns POLICY
// (what a valid, deduped batch is) and the adapter owns MECHANISM (when to flush,
// how to NAK under load). That separation is exactly why the pattern's hard part
// can be added later without touching these rules or their tests.
//
// INTERVIEW: "Walk me through what happens to a metric batch." Validate (run is
// RUNNING, batch ≤ cap, values finite) → dedup intra-batch by (key,step),
// first-wins → overwrite timestamps with the server clock (partition-key
// integrity) → write in one INSERT that the DB de-dups across batches via a
// UNIQUE (run,key,step) index → return how many actually landed.
package domain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/google/uuid"
)

// ============================================================================
// DOMAIN INVARIANTS / CAPS (server-enforced; clients cannot exceed them)
// ============================================================================

const (
	// MaxMetricsPerBatch caps points in a single LogMetrics call. WHY a cap: an
	// unbounded batch is both an OOM lever (the whole slice is held in memory)
	// and a single-transaction-too-large hazard (one giant INSERT can bloat WAL
	// and lock the partition). 1000 is generous for a training flush (an epoch's
	// worth of metrics) while keeping each transaction small. Clients split
	// larger flushes across calls — and the async consumer flushes in similar
	// chunks. This is a domain invariant, not the adapter's job, so the cap can't
	// be bypassed by a different transport.
	MaxMetricsPerBatch = 1000

	// MaxParamsPerCall caps params per LogParams call (params are tiny and
	// write-once; a few dozen is realistic, 200 is a safe ceiling).
	MaxParamsPerCall = 200

	// MaxCompareRuns caps how many runs CompareRuns will overlay at once.
	// Comparing hundreds of full curves is a payload/DoS hazard.
	MaxCompareRuns = 20

	// DefaultPageSize / MaxPageSize bound list RPCs (experiments, runs).
	DefaultPageSize = 20
	MaxPageSize     = 100

	// DefaultMetricPageSize / MaxMetricPageSize bound GetMetricHistory. The cap
	// is HIGHER than list RPCs because metric points are tiny and charts want
	// many — but it is still server-enforced so a page can't be unbounded.
	DefaultMetricPageSize = 1000
	MaxMetricPageSize     = 5000

	// MaxArtifactsBytes caps the serialized free-form artifacts attached to a
	// run. Large blobs belong in object storage with a URI referenced here, not
	// inline on NATS-adjacent state. 256 KiB matches the proto's stated cap.
	MaxArtifactsBytes = 256 * 1024

	// Idempotency operation namespaces — scope keys per-RPC so the same
	// client-generated UUID reused across different RPCs never collides.
	opStartRun        = "start_run"
	opLogMetrics      = "log_metrics"
	opDeleteRun       = "delete_run"
	opSetRunArtifacts = "set_run_artifacts"
)

// experimentService is the production implementation of ExperimentService.
// Unexported: callers receive it only through the interface returned by
// NewExperimentService, enforcing programming-to-the-interface.
type experimentService struct {
	expRepo ExperimentRepository
	runRepo RunRepository
	idem    IdempotencyStore
	pub     EventPublisher
	clock   Clock
}

// NewExperimentService wires the dependencies and returns the interface.
//
// A compile-time assertion below guarantees *experimentService satisfies the
// interface; if a method signature drifts, the package fails to build rather
// than failing mysteriously at a call site.
func NewExperimentService(
	expRepo ExperimentRepository,
	runRepo RunRepository,
	idem IdempotencyStore,
	pub EventPublisher,
	clock Clock,
) ExperimentService {
	return &experimentService{
		expRepo: expRepo,
		runRepo: runRepo,
		idem:    idem,
		pub:     pub,
		clock:   clock,
	}
}

var _ ExperimentService = (*experimentService)(nil)

// ============================================================================
// EXPERIMENTS
// ============================================================================

// CreateExperiment validates input, sets the server-authoritative fields from
// the Actor and clock, and persists. Owner/Team come from the AUTHENTICATED
// caller, never the input — the mass-assignment guard.
func (s *experimentService) CreateExperiment(ctx context.Context, actor Actor, in CreateExperimentInput) (Experiment, error) {
	name := strings.TrimSpace(in.Name)
	if name == "" {
		return Experiment{}, fmt.Errorf("%w: experiment name is required", ErrValidation)
	}
	if actor.Team == "" {
		// No team on the caller's claims => cannot scope tenancy. Fail closed.
		return Experiment{}, fmt.Errorf("%w: caller has no team", ErrValidation)
	}
	now := s.clock.Now()
	exp := Experiment{
		ID:          uuid.NewString(), // server-generated, immutable
		Name:        name,
		Description: strings.TrimSpace(in.Description),
		Tags:        in.Tags,
		OwnerID:     actor.UserID, // SERVER-set from claims
		Team:        actor.Team,   // SERVER-set from claims (tenancy)
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	created, err := s.expRepo.Create(ctx, exp)
	if err != nil {
		if errors.Is(err, ErrRepoConflict) {
			return Experiment{}, ErrExperimentNameExists
		}
		return Experiment{}, fmt.Errorf("create experiment: %w", err)
	}
	return created, nil
}

// GetExperiment fetches one experiment and enforces tenancy: an experiment in
// another team returns ErrExperimentNotFound (the SAME error as truly-missing)
// so a caller can't probe for the existence of other teams' experiments.
func (s *experimentService) GetExperiment(ctx context.Context, actor Actor, id string) (Experiment, error) {
	exp, err := s.fetchExperimentForTeam(ctx, id, actor.Team)
	if err != nil {
		return Experiment{}, err
	}
	return exp, nil
}

// fetchExperimentForTeam centralizes the load-then-tenancy-check used by every
// experiment-scoped operation. Defense in depth: even if an adapter forgets to
// filter by team, the service rejects a cross-tenant row here.
func (s *experimentService) fetchExperimentForTeam(ctx context.Context, id, team string) (Experiment, error) {
	if strings.TrimSpace(id) == "" {
		return Experiment{}, fmt.Errorf("%w: experiment id is required", ErrValidation)
	}
	exp, err := s.expRepo.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, ErrRepoNotFound) {
			return Experiment{}, ErrExperimentNotFound
		}
		return Experiment{}, fmt.Errorf("get experiment: %w", err)
	}
	if exp.Team != team {
		// Cross-tenant access — indistinguishable from "not found" to the caller.
		return Experiment{}, ErrExperimentNotFound
	}
	return exp, nil
}

// ListExperiments returns a team-scoped page. The page size is capped here, in
// the domain, so no transport can request an unbounded page.
func (s *experimentService) ListExperiments(ctx context.Context, actor Actor, includeArchived bool, opts ListOptions) ([]Experiment, string, error) {
	if actor.Team == "" {
		return nil, "", fmt.Errorf("%w: caller has no team", ErrValidation)
	}
	opts.PageSize = clampPageSize(opts.PageSize, DefaultPageSize, MaxPageSize)
	return s.expRepo.List(ctx, actor.Team, includeArchived, opts)
}

// UpdateExperiment applies a field-masked edit to mutable fields and refreshes
// UpdatedAt. Owner/team/timestamps/archived are NOT in the input and cannot be
// changed. An unknown field name in the mask is rejected.
func (s *experimentService) UpdateExperiment(ctx context.Context, actor Actor, in UpdateExperimentInput) (Experiment, error) {
	exp, err := s.fetchExperimentForTeam(ctx, in.ID, actor.Team)
	if err != nil {
		return Experiment{}, err
	}
	if len(in.UpdateFields) == 0 {
		return Experiment{}, fmt.Errorf("%w: update_fields must list at least one field", ErrValidation)
	}
	for _, f := range in.UpdateFields {
		switch f {
		case "name":
			name := strings.TrimSpace(in.Name)
			if name == "" {
				return Experiment{}, fmt.Errorf("%w: name cannot be cleared", ErrValidation)
			}
			exp.Name = name
		case "description":
			exp.Description = strings.TrimSpace(in.Description)
		case "tags":
			exp.Tags = in.Tags // replace-not-merge so a tag can be deleted
		default:
			return Experiment{}, fmt.Errorf("%w: unknown update field %q", ErrValidation, f)
		}
	}
	exp.UpdatedAt = s.clock.Now()
	updated, err := s.expRepo.Update(ctx, exp)
	if err != nil {
		if errors.Is(err, ErrRepoConflict) {
			return Experiment{}, ErrExperimentNameExists
		}
		return Experiment{}, fmt.Errorf("update experiment: %w", err)
	}
	return updated, nil
}

// ArchiveExperiment soft-deletes an experiment by stamping ArchivedAt. Idempotent:
// archiving an already-archived experiment returns it unchanged (no error) — a
// retried archive must not fail.
func (s *experimentService) ArchiveExperiment(ctx context.Context, actor Actor, id string) (Experiment, error) {
	exp, err := s.fetchExperimentForTeam(ctx, id, actor.Team)
	if err != nil {
		return Experiment{}, err
	}
	if exp.IsArchived() {
		return exp, nil // idempotent no-op
	}
	now := s.clock.Now()
	exp.ArchivedAt = &now
	exp.UpdatedAt = now
	updated, err := s.expRepo.Update(ctx, exp)
	if err != nil {
		return Experiment{}, fmt.Errorf("archive experiment: %w", err)
	}
	return updated, nil
}

// ============================================================================
// RUN LIFECYCLE
// ============================================================================

// StartRun opens a RUNNING run (source = API). It honors IdempotencyKey: a
// retried StartRun returns the SAME run (no duplicate, no second event). On
// first creation it publishes a RunCreated event (best-effort).
func (s *experimentService) StartRun(ctx context.Context, actor Actor, in StartRunInput) (Run, error) {
	// Idempotency replay FIRST: if this key already created a run, return it.
	if in.IdempotencyKey != "" {
		if runID, found, err := s.idem.Lookup(ctx, opStartRun, in.IdempotencyKey); err != nil {
			return Run{}, fmt.Errorf("idempotency lookup: %w", err)
		} else if found {
			return s.runRepo.GetRun(ctx, runID)
		}
	}

	// Parent experiment must exist and belong to the caller's team.
	if _, err := s.fetchExperimentForTeam(ctx, in.ExperimentID, actor.Team); err != nil {
		return Run{}, err
	}
	if err := validateParams(in.Params); err != nil {
		return Run{}, err
	}

	now := s.clock.Now()
	run := Run{
		ID:             uuid.NewString(), // server-generated
		ExperimentID:   in.ExperimentID,
		DisplayName:    strings.TrimSpace(in.DisplayName),
		Status:         RunStatusRunning, // server-authoritative: born RUNNING
		Source:         RunSourceAPI,     // sync path
		ModelVersionID: strings.TrimSpace(in.ModelVersionID),
		OwnerID:        actor.UserID, // SERVER-set from claims
		Params:         in.Params,
		StartedAt:      now,
	}
	created, err := s.runRepo.CreateRun(ctx, run)
	if err != nil {
		return Run{}, fmt.Errorf("create run: %w", err)
	}

	// Record idempotency AFTER the write succeeds so a key only ever maps to a
	// run that actually exists.
	if in.IdempotencyKey != "" {
		if err := s.idem.Record(ctx, opStartRun, in.IdempotencyKey, created.ID); err != nil {
			return Run{}, fmt.Errorf("idempotency record: %w", err)
		}
	}

	// Publish RunCreated — BEST-EFFORT. The run is the source of truth; a failed
	// publish must not fail the operation (we'd otherwise lose a committed run on
	// a transient NATS blip). The events adapter handles retry/DLQ.
	s.publishRunCreated(ctx, created)
	return created, nil
}

// FinishRun transitions a run to a terminal state. It validates the transition,
// computes FinalMetrics from the run's full series (the CQRS projection), stamps
// EndedAt, persists, and publishes the RunFinished FAT event (final_metrics
// included so consumers rank/alert without a callback).
func (s *experimentService) FinishRun(ctx context.Context, actor Actor, in FinishRunInput) (Run, error) {
	run, err := s.loadRunForTeam(ctx, in.RunID, actor.Team)
	if err != nil {
		return Run{}, err
	}
	// State-machine guard — the SINGLE rule lives in RunStatus.CanTransitionTo.
	if !run.Status.CanTransitionTo(in.TargetStatus) {
		return Run{}, fmt.Errorf("%w: %s -> %s", ErrInvalidStatusTransition, run.Status, in.TargetStatus)
	}

	// Compute the denormalized headline metrics from the FULL series.
	//
	// CORRECTNESS HAZARD (why this is a loop, not a single read): the repository
	// returns metric history PAGINATED and ordered by (key, step, timestamp). A
	// single run's curve can be "millions of points" (the proto's own words), and
	// the Postgres adapter caps a page at DefaultMetricPageSize/MaxMetricPageSize.
	// If we read only ONE page, we'd see a TRUNCATED PREFIX — and because the order
	// is ascending by step, the LATEST steps (the ones ComputeFinalMetrics must
	// pick as the headline) live on the LAST page. Reading one page would persist
	// and publish an EARLY-step value as "final accuracy", silently corrupting the
	// denormalized headline AND the fat RunFinished event leaderboards rank on.
	//
	// So we DRAIN every page: keep calling GetMetricHistory with the returned
	// nextToken until it comes back empty, accumulating all points, THEN fold them
	// with the pure ComputeFinalMetrics projection. We pass an EXPLICIT max page
	// size (MaxMetricPageSize) to minimise round-trips on huge series — fewer, full
	// pages instead of many default-sized ones.
	//
	// ALTERNATIVE (and the better one at extreme scale): a dedicated repo method
	// that computes finals server-side with a single
	//   SELECT DISTINCT ON (key) ... ORDER BY key, step DESC, timestamp DESC
	// so the DB returns just the latest point per key regardless of series size —
	// no full read at all. We keep the read-then-fold form here because it keeps the
	// projection rule (latest-step-wins) PURE and unit-testable without a database,
	// which is this service's learning goal; the loop makes it CORRECT at any size.
	// The trade-off (full read vs. server-side DISTINCT ON) is noted for the repo
	// phase. INTERVIEW: "Your finals were wrong for big runs — why?" Because we
	// folded a single capped page of an ascending series, so the tail (the real
	// final steps) was never read; the fix is to drain all pages (or push the
	// DISTINCT ON down to SQL).
	finals, err := s.computeFinalsFromHistory(ctx, run.ID)
	if err != nil {
		return Run{}, err
	}

	now := s.clock.Now()
	updated, err := s.runRepo.UpdateRunStatus(ctx, run.ID, in.TargetStatus, now, finals)
	if err != nil {
		return Run{}, fmt.Errorf("update run status: %w", err)
	}

	s.publishRunFinished(ctx, updated)
	return updated, nil
}

// GetRun returns a run (metadata + params + finals + artifacts), tenancy-checked.
func (s *experimentService) GetRun(ctx context.Context, actor Actor, id string) (Run, error) {
	return s.loadRunForTeam(ctx, id, actor.Team)
}

// ListRuns returns a team-scoped page of runs in an experiment, optionally
// filtered by status. The experiment's team is verified first.
func (s *experimentService) ListRuns(ctx context.Context, actor Actor, experimentID string, statusFilter RunStatus, opts ListOptions) ([]Run, string, error) {
	if _, err := s.fetchExperimentForTeam(ctx, experimentID, actor.Team); err != nil {
		return nil, "", err
	}
	opts.PageSize = clampPageSize(opts.PageSize, DefaultPageSize, MaxPageSize)
	return s.runRepo.ListRuns(ctx, experimentID, statusFilter, opts)
}

// DeleteRun hard-deletes a run + its metrics/params. Idempotent via the key: a
// retried delete after the run is already gone returns nil if the SAME key
// recorded the earlier success.
//
// NOTE: the Registry-lineage guard (reject delete if model_version_id is still
// referenced by a live version) is a CROSS-SERVICE check wired when the Registry
// client exists — flagged in the report. The domain enforces tenancy/ownership
// and idempotency here.
func (s *experimentService) DeleteRun(ctx context.Context, actor Actor, runID, idempotencyKey string) error {
	if idempotencyKey != "" {
		if _, found, err := s.idem.Lookup(ctx, opDeleteRun, idempotencyKey); err != nil {
			return fmt.Errorf("idempotency lookup: %w", err)
		} else if found {
			return nil // this key already deleted the run — replay succeeds
		}
	}
	run, err := s.loadRunForTeam(ctx, runID, actor.Team)
	if err != nil {
		return err
	}
	if err := s.runRepo.DeleteRun(ctx, run.ID); err != nil {
		if errors.Is(err, ErrRepoNotFound) {
			return ErrRunNotFound
		}
		return fmt.Errorf("delete run: %w", err)
	}
	if idempotencyKey != "" {
		if err := s.idem.Record(ctx, opDeleteRun, idempotencyKey, run.ID); err != nil {
			return fmt.Errorf("idempotency record: %w", err)
		}
	}
	return nil
}

// ============================================================================
// INGESTION — LogMetrics (the batch pattern), LogParams, SetRunArtifacts
// ============================================================================

// LogMetrics is the high-throughput batch ingestion operation. See the file
// header for the full pipeline. Both the gRPC handler and the NATS batch
// consumer call this exact method.
func (s *experimentService) LogMetrics(ctx context.Context, actor Actor, in LogMetricsInput) (LogMetricsResult, error) {
	// Idempotency replay FIRST: a retried batch (same key) must not double-write.
	// We short-circuit to accepted=0 (nothing NEW landed on this call). This is
	// the in-service half of the idempotency guarantee; the DB's UNIQUE index is
	// the backstop if a replay slips past (e.g. the key store lost the record).
	if in.IdempotencyKey != "" {
		if _, found, err := s.idem.Lookup(ctx, opLogMetrics, in.IdempotencyKey); err != nil {
			return LogMetricsResult{}, fmt.Errorf("idempotency lookup: %w", err)
		} else if found {
			return LogMetricsResult{AcceptedCount: 0}, nil
		}
	}

	// CAP before anything else — reject an oversize batch without allocating or
	// touching the run. This is the DoS guard.
	if len(in.Points) > MaxMetricsPerBatch {
		return LogMetricsResult{}, fmt.Errorf("%w: %d points (max %d)", ErrBatchTooLarge, len(in.Points), MaxMetricsPerBatch)
	}
	if len(in.Points) == 0 {
		return LogMetricsResult{}, fmt.Errorf("%w: batch is empty", ErrValidation)
	}

	// The run must exist, be in the caller's team, and be RUNNING (its metrics
	// aren't final yet). Logging to a terminal run would corrupt a finished
	// curve.
	run, err := s.loadRunForTeam(ctx, in.RunID, actor.Team)
	if err != nil {
		return LogMetricsResult{}, err
	}
	if run.Status != RunStatusRunning {
		return LogMetricsResult{}, fmt.Errorf("%w: run %s is %s", ErrRunNotRunning, run.ID, run.Status)
	}

	// Validate each point: non-empty key, FINITE value (reject NaN/±Inf — they
	// poison aggregations and break JSON serialization and DOUBLE PRECISION
	// math downstream). This is the "overflow-safe / finite math" security
	// default for a numeric ingestion path.
	for i := range in.Points {
		if strings.TrimSpace(in.Points[i].Key) == "" {
			return LogMetricsResult{}, fmt.Errorf("%w: metric point %d has empty key", ErrValidation, i)
		}
		if math.IsNaN(in.Points[i].Value) || math.IsInf(in.Points[i].Value, 0) {
			return LogMetricsResult{}, fmt.Errorf("%w: metric %q at step %d has non-finite value", ErrValidation, in.Points[i].Key, in.Points[i].Step)
		}
	}

	// DEDUP intra-batch by (key,step) (first wins) then SERVER-STAMP timestamps
	// (partition-key integrity — a client can't backdate). Order: dedup first so
	// we don't stamp points we're about to drop.
	deduped := DedupMetricPoints(in.Points)
	stamped := StampMetricTimestamps(deduped, s.clock.Now())

	// One transactional, idempotent multi-row write. The adapter's UNIQUE
	// (run,key,step) index + ON CONFLICT DO NOTHING gives CROSS-batch dedup; the
	// returned count is what actually landed.
	written, err := s.runRepo.AppendMetrics(ctx, run.ID, stamped)
	if err != nil {
		return LogMetricsResult{}, fmt.Errorf("append metrics: %w", err)
	}

	if in.IdempotencyKey != "" {
		if err := s.idem.Record(ctx, opLogMetrics, in.IdempotencyKey, run.ID); err != nil {
			return LogMetricsResult{}, fmt.Errorf("idempotency record: %w", err)
		}
	}
	return LogMetricsResult{AcceptedCount: written}, nil
}

// LogParams appends WRITE-ONCE params to a RUNNING run. Re-logging an identical
// key/value is an idempotent no-op (counted as 0 accepted); re-logging a key
// with a DIFFERENT value is ErrParamConflict (a run's config must not silently
// change mid-flight).
func (s *experimentService) LogParams(ctx context.Context, actor Actor, in LogParamsInput) (LogParamsResult, error) {
	if len(in.Params) == 0 {
		return LogParamsResult{}, fmt.Errorf("%w: no params provided", ErrValidation)
	}
	if len(in.Params) > MaxParamsPerCall {
		return LogParamsResult{}, fmt.Errorf("%w: %d params (max %d)", ErrValidation, len(in.Params), MaxParamsPerCall)
	}
	if err := validateParams(in.Params); err != nil {
		return LogParamsResult{}, err
	}
	run, err := s.loadRunForTeam(ctx, in.RunID, actor.Team)
	if err != nil {
		return LogParamsResult{}, err
	}
	if run.Status != RunStatusRunning {
		return LogParamsResult{}, fmt.Errorf("%w: run %s is %s", ErrRunNotRunning, run.ID, run.Status)
	}

	existing, err := s.runRepo.GetRunParams(ctx, run.ID)
	if err != nil {
		return LogParamsResult{}, fmt.Errorf("read existing params: %w", err)
	}
	have := make(map[string]string, len(existing))
	for _, p := range existing {
		have[p.Key] = p.Value
	}

	var toWrite []Param
	for _, p := range in.Params {
		if cur, ok := have[p.Key]; ok {
			if cur != p.Value {
				return LogParamsResult{}, fmt.Errorf("%w: key %q (%q != %q)", ErrParamConflict, p.Key, cur, p.Value)
			}
			continue // identical re-log => idempotent no-op
		}
		// Guard against a duplicate key WITHIN this same call too.
		have[p.Key] = p.Value
		toWrite = append(toWrite, p)
	}
	if len(toWrite) == 0 {
		return LogParamsResult{AcceptedCount: 0}, nil
	}
	written, err := s.runRepo.AppendParams(ctx, run.ID, toWrite)
	if err != nil {
		return LogParamsResult{}, fmt.Errorf("append params: %w", err)
	}
	return LogParamsResult{AcceptedCount: written}, nil
}

// SetRunArtifacts attaches size-capped, JSON-shaped artifacts to a run.
// Idempotent via key. The size cap rejects oversize blobs (they belong in object
// storage with a URI referenced here).
func (s *experimentService) SetRunArtifacts(ctx context.Context, actor Actor, in SetArtifactsInput) (Run, error) {
	if in.IdempotencyKey != "" {
		if runID, found, err := s.idem.Lookup(ctx, opSetRunArtifacts, in.IdempotencyKey); err != nil {
			return Run{}, fmt.Errorf("idempotency lookup: %w", err)
		} else if found {
			return s.runRepo.GetRun(ctx, runID)
		}
	}
	run, err := s.loadRunForTeam(ctx, in.RunID, actor.Team)
	if err != nil {
		return Run{}, err
	}
	if err := validateArtifactsSize(in.Artifacts); err != nil {
		return Run{}, err
	}
	updated, err := s.runRepo.SetArtifacts(ctx, run.ID, in.Artifacts)
	if err != nil {
		return Run{}, fmt.Errorf("set artifacts: %w", err)
	}
	if in.IdempotencyKey != "" {
		if err := s.idem.Record(ctx, opSetRunArtifacts, in.IdempotencyKey, run.ID); err != nil {
			return Run{}, fmt.Errorf("idempotency record: %w", err)
		}
	}
	return updated, nil
}

// ============================================================================
// ANALYSIS / READ
// ============================================================================

// GetMetricHistory returns ONE run's metric series, paginated, with optional
// key/step filters. Page size capped server-side.
func (s *experimentService) GetMetricHistory(ctx context.Context, actor Actor, in GetMetricHistoryInput) ([]MetricSeries, string, error) {
	run, err := s.loadRunForTeam(ctx, in.RunID, actor.Team)
	if err != nil {
		return nil, "", err
	}
	in.Pagination.PageSize = clampPageSize(in.Pagination.PageSize, DefaultMetricPageSize, MaxMetricPageSize)
	q := MetricHistoryQuery{
		RunID:      run.ID,
		Keys:       in.MetricKeys,
		MinStep:    in.MinStep,
		MaxStep:    in.MaxStep,
		HasMinStep: in.HasMinStep,
		HasMaxStep: in.HasMaxStep,
		Pagination: in.Pagination,
	}
	points, next, err := s.runRepo.GetMetricHistory(ctx, q)
	if err != nil {
		return nil, "", fmt.Errorf("get metric history: %w", err)
	}
	return GroupIntoSeries(points), next, nil
}

// CompareRuns returns several runs' metric series side-by-side, preserving the
// request order. Run count capped (MaxCompareRuns). Runs not visible to the
// caller's team are silently skipped (no existence leak); a fully-empty result
// is valid (the caller asked about runs it can't see).
func (s *experimentService) CompareRuns(ctx context.Context, actor Actor, in CompareRunsInput) ([]RunComparison, error) {
	if len(in.RunIDs) == 0 {
		return nil, fmt.Errorf("%w: no run ids provided", ErrValidation)
	}
	if len(in.RunIDs) > MaxCompareRuns {
		return nil, fmt.Errorf("%w: %d runs (max %d)", ErrTooManyRuns, len(in.RunIDs), MaxCompareRuns)
	}

	out := make([]RunComparison, 0, len(in.RunIDs))
	for _, id := range in.RunIDs {
		run, err := s.loadRunForTeam(ctx, id, actor.Team)
		if err != nil {
			if errors.Is(err, ErrRunNotFound) {
				continue // skip runs that don't exist / aren't visible
			}
			return nil, err
		}
		points, _, err := s.runRepo.GetMetricHistory(ctx, MetricHistoryQuery{RunID: run.ID, Keys: in.MetricKeys})
		if err != nil {
			return nil, fmt.Errorf("compare: history for run %s: %w", run.ID, err)
		}
		out = append(out, RunComparison{Run: run, Series: GroupIntoSeries(points)})
	}
	return out, nil
}

// ============================================================================
// INTERNAL HELPERS
// ============================================================================

// loadRunForTeam loads a run and enforces tenancy by checking the OWNING
// EXPERIMENT's team. WHY check the experiment (not a team on the run): the run
// inherits tenancy from its experiment; the experiment is the tenancy anchor. A
// run in another team's experiment returns ErrRunNotFound (no existence leak).
func (s *experimentService) loadRunForTeam(ctx context.Context, runID, team string) (Run, error) {
	if strings.TrimSpace(runID) == "" {
		return Run{}, fmt.Errorf("%w: run id is required", ErrValidation)
	}
	run, err := s.runRepo.GetRun(ctx, runID)
	if err != nil {
		if errors.Is(err, ErrRepoNotFound) {
			return Run{}, ErrRunNotFound
		}
		return Run{}, fmt.Errorf("get run: %w", err)
	}
	// Verify the parent experiment is in the caller's team. fetchExperimentForTeam
	// returns ErrExperimentNotFound for a cross-tenant parent; we re-map it to
	// ErrRunNotFound so the caller learns nothing about the run's existence.
	if _, err := s.fetchExperimentForTeam(ctx, run.ExperimentID, team); err != nil {
		return Run{}, ErrRunNotFound
	}
	return run, nil
}

// computeFinalsFromHistory drains the ENTIRE paginated metric history for a run
// and folds it into the denormalized headline metrics. It exists because the
// finals projection (latest-step-per-key) is only correct over the FULL series;
// reading a single capped page would truncate the ascending-by-step curve and
// pick an early-step value as "final". See the long note in FinishRun for the
// hazard and the server-side-DISTINCT-ON alternative.
//
// We page with an EXPLICIT MaxMetricPageSize and follow nextToken until it is
// empty. Two safety rails matter:
//   - A pageToken-not-advancing guard: if an adapter returns the SAME nextToken it
//     was given (a buggy cursor), we'd loop forever; we break instead. This is the
//     kind of defensive bound an interviewer probes ("what if the cursor never
//     terminates?").
//   - A hard page-count ceiling derived from the metric cap so a pathological
//     adapter that always returns a fresh token can't spin unbounded.
func (s *experimentService) computeFinalsFromHistory(ctx context.Context, runID string) ([]MetricPoint, error) {
	var all []MetricPoint
	pageToken := ""
	// maxPages is a generous ceiling: even a million-point run drained in
	// MaxMetricPageSize chunks is ~200 pages; 100_000 pages is an unreachable
	// runaway-guard, not a real limit on legitimate series.
	const maxPages = 100_000
	for pages := 0; pages < maxPages; pages++ {
		points, next, err := s.runRepo.GetMetricHistory(ctx, MetricHistoryQuery{
			RunID: runID,
			// Explicit page size: drain in the largest legal chunks to minimise
			// round-trips on huge series. (The service caps this on the read RPC
			// too; here we set it directly since this is an internal full scan.)
			Pagination: ListOptions{PageSize: MaxMetricPageSize, PageToken: pageToken},
		})
		if err != nil {
			return nil, fmt.Errorf("read metric history for finals: %w", err)
		}
		all = append(all, points...)
		// Empty token => no more pages. Also stop if the adapter failed to advance
		// the cursor (next == the token we just used) to avoid an infinite loop.
		if next == "" || next == pageToken {
			break
		}
		pageToken = next
	}
	// ComputeFinalMetrics is a PURE fold over the full accumulated series — the
	// single, testable rule that picks the latest-step point per key.
	return ComputeFinalMetrics(all), nil
}

// publishRunCreated emits RunCreated best-effort (a failure is swallowed — the
// run is already committed; the event is a derived notification). A real wiring
// logs the error via the events adapter; the domain stays free of a logger
// dependency by treating publish as fire-and-forget here.
func (s *experimentService) publishRunCreated(ctx context.Context, run Run) {
	_ = s.pub.PublishRunCreated(ctx, RunCreatedEvent{
		RunID:          run.ID,
		ExperimentID:   run.ExperimentID,
		ModelVersionID: run.ModelVersionID,
		DisplayName:    run.DisplayName,
		StartedAt:      run.StartedAt,
	})
}

// publishRunFinished emits the RunFinished FAT event best-effort, carrying the
// computed final metrics so downstream consumers (leaderboards, notification)
// rank/alert without a GetRun callback.
func (s *experimentService) publishRunFinished(ctx context.Context, run Run) {
	endedAt := run.StartedAt
	if run.EndedAt != nil {
		endedAt = *run.EndedAt
	}
	_ = s.pub.PublishRunFinished(ctx, RunFinishedEvent{
		RunID:          run.ID,
		ExperimentID:   run.ExperimentID,
		ModelVersionID: run.ModelVersionID,
		Status:         run.Status,
		FinalMetrics:   run.FinalMetrics,
		EndedAt:        endedAt,
	})
}

// validateParams rejects empty keys (params are display/group/equality keys; an
// empty key is meaningless and breaks the write-once map).
func validateParams(params []Param) error {
	for i := range params {
		if strings.TrimSpace(params[i].Key) == "" {
			return fmt.Errorf("%w: param %d has empty key", ErrValidation, i)
		}
	}
	return nil
}

// validateArtifactsSize enforces the serialized-size cap against the ACTUAL
// serialized form of the artifacts, not a per-entry heuristic.
//
// SECURITY HISTORY (why this is no longer a heuristic): the previous version
// summed key length + a flat 32 bytes for ANY non-string value. That counted a
// single key whose value was a 5-million-element array as ~40 bytes — so tens of
// megabytes serialized passed the cap. Since the domain is the ONLY guard until
// the handler's precise wire-byte check is wired (the SetRunArtifacts handler is
// still a stub), that heuristic was a trivially-bypassable unbounded-growth /
// OOM / DoS vector: oversized blobs would land in Postgres and ride NATS-adjacent
// state. The proto's SIZE note says such payloads MUST be rejected.
//
// The fix measures real bytes by marshalling to JSON (encoding/json is stdlib, so
// the domain stays pure — no framework import) and comparing len(bytes) to the
// cap. JSON is the right yardstick: artifacts are JSON-shaped (a proto Struct on
// the wire), so its byte length is a faithful, slightly-conservative proxy for
// the stored/transported size — and it counts NESTED arrays/maps in full, which
// is exactly what the heuristic missed.
//
// TRADEOFF: marshalling allocates the serialized form to measure it. That is
// acceptable here: SetRunArtifacts is a low-frequency, set-once operation (not the
// hot LogMetrics path), and we still reject anything over 256 KiB — the very thing
// that would otherwise bloat memory. (For an even tighter bound one could use a
// counting io.Writer with json.NewEncoder to stop early at the cap; the simple
// Marshal is clear and the cap keeps the transient allocation small.)
//
// INTERVIEW: "How do you stop a client smuggling a huge blob past a size cap?"
// Measure the SERIALIZED bytes, never a field-count heuristic — a heuristic that
// charges a flat cost per value is blind to nested growth and is the classic cap
// bypass.
func validateArtifactsSize(artifacts map[string]any) error {
	if len(artifacts) == 0 {
		return nil // nothing to attach; trivially within the cap
	}
	encoded, err := json.Marshal(artifacts)
	if err != nil {
		// Non-JSON-encodable artifacts (e.g. a channel, func, or NaN/Inf float that
		// json.Marshal rejects) are invalid input, not a server fault — the
		// artifacts contract is "JSON-shaped". Reject as validation.
		return fmt.Errorf("%w: artifacts are not JSON-serializable: %v", ErrValidation, err)
	}
	if len(encoded) > MaxArtifactsBytes {
		return fmt.Errorf("%w: artifacts serialize to %d bytes (max %d)", ErrValidation, len(encoded), MaxArtifactsBytes)
	}
	return nil
}

// clampPageSize applies the default (when 0) and the maximum (when too large).
// Centralizing this is the server-side cap that prevents an unbounded page from
// any transport — the cap is a domain invariant, not the handler's discretion.
func clampPageSize(requested, def, max int) int {
	if requested <= 0 {
		return def
	}
	if requested > max {
		return max
	}
	return requested
}
