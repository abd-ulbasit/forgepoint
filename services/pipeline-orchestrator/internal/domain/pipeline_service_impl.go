// pipeline_service_impl.go — the SAGA ORCHESTRATOR + DAG EXECUTOR.
//
// ============================================================================
// THIS FILE IS THE TEACHING CENTERPIECE OF THE SERVICE
// ============================================================================
//
// It implements PipelineService. Two execution strategies live behind one
// engine, selected by PipelineType:
//
//   1. SAGA (DeploymentSaga) — runStepsSaga: steps run SEQUENTIALLY in
//      topological order; on the FIRST failure, the engine COMPENSATES every
//      already-COMPLETED step in REVERSE completion order, then settles the run
//      to FAILED (or CANCELLED if the user cancelled). This is the distributed-
//      transaction-without-2PC pattern: instead of holding locks across
//      services, each step has an UNDO and we roll forward, compensating on
//      failure. (Garcia-Molina & Salem, 1987.)
//
//   2. DAG (TrainingDAG / BatchInference) — runStepsDAG: steps run in
//      topological LEVELS; independent steps within a level run in PARALLEL
//      (fan-out). A failed step PROPAGATES failure to its dependents, which are
//      marked SKIPPED. A DAG does NOT compensate (no global rollback) — training
//      steps are typically idempotent and re-runnable.
//
// THE STATE MACHINE (asserted exhaustively in the tests):
//
//	PENDING ─► RUNNING ─► COMPLETED
//	             │
//	             ├─ step fails    ─► COMPENSATING ─► FAILED
//	             └─ cancel         ─► COMPENSATING ─► CANCELLED
//
// DURABILITY (write-ahead): the engine calls ExecutionRepository.SaveStep BEFORE
// running a step (→RUNNING) and after it settles (→COMPLETED/FAILED/...). A
// crash between steps loses at most the in-flight step; on restart a recovery
// routine (a later layer) reloads the checkpoints and resumes. The IN-MEMORY
// Execution we mutate here is the same state we persist, so "what ran" and "what
// is stored" never diverge.
//
// PURITY: this file imports ONLY stdlib + github.com/google/uuid. All side
// effects (deploying, training, publishing) are behind ports. That is what makes
// the saga logic unit-testable without any infrastructure (see the *_test.go).
// ============================================================================
package domain

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ============================================================================
// CONSTRUCTION
// ============================================================================

// pipelineService is the concrete PipelineService. It holds the secondary ports
// (repositories, executor registry, event publisher) plus the Clock and
// IDGenerator. Everything it does flows through these interfaces — there is no
// direct I/O in this struct.
type pipelineService struct {
	pipelines  PipelineRepository
	executions ExecutionRepository
	registry   ExecutorRegistry
	publisher  EventPublisher
	clock      Clock
	ids        IDGenerator

	// cancels tracks in-flight executions so CancelExecution can signal the
	// running engine goroutine to stop. Keyed by execution id → cancel func.
	// WHY in-process: this single-leader engine drives a run to completion in one
	// process (leader election ensures one active replica). A cancel from another
	// replica is handled by the DB poll on recovery; the in-process map covers the
	// common case where the cancel hits the same leader running the saga.
	mu      sync.Mutex
	cancels map[string]context.CancelFunc

	// execMu serializes MUTATIONS of the in-memory *Execution while a DAG runs
	// steps in PARALLEL. The saga scheduler is sequential and needs no lock, but
	// the DAG fan-out has N goroutines all calling runOneStep on the SAME
	// Execution — each updating CurrentStep and a StepExecution in the shared
	// Steps slice. Without a lock that is a data race (caught by -race). We take
	// the coarse approach (one lock around every step-state write) rather than
	// per-step locks: the critical sections are tiny (a field assignment + a
	// SaveStep call) and contention is negligible at ML-pipeline fan-out widths
	// (tens of steps), so a single mutex is simplest and provably correct.
	//
	// MAKING THE PARALLEL DAG SCHEDULER SAFE: the executors
	// run concurrently (the expensive work), but every mutation of the shared
	// execution state is funneled through execMu, so the state machine transitions
	// are serialized even though the side-effecting work is parallel.
	execMu sync.Mutex
}

// NewPipelineService wires the engine with its ports. The Clock and IDGenerator
// are injected (never defaulted here) so the DOMAIN stays free of even the uuid
// import — the concrete time/uuid implementations are a COMPOSITION-ROOT concern
// (see cmd/server/main.go's systemClock / uuidGenerator). This keeps the domain's
// dependency footprint to the standard library alone, matching the Auth service's
// domain baseline. Tests inject deterministic doubles; main injects the real ones.
//
// PRECONDITION: clock and ids must be non-nil. Passing nil is a programmer error
// (a mis-wired composition root) and the engine would nil-panic on first use; we
// fail fast and loud here instead, at construction, where the stack trace points
// straight at the wiring bug.
func NewPipelineService(
	pipelines PipelineRepository,
	executions ExecutionRepository,
	registry ExecutorRegistry,
	publisher EventPublisher,
	clock Clock,
	ids IDGenerator,
) *pipelineService {
	if clock == nil || ids == nil {
		panic("domain.NewPipelineService: clock and ids ports are required (wire systemClock + uuidGenerator in main)")
	}
	return &pipelineService{
		pipelines:  pipelines,
		executions: executions,
		registry:   registry,
		publisher:  publisher,
		clock:      clock,
		ids:        ids,
		cancels:    make(map[string]context.CancelFunc),
	}
}

// Compile-time assertion that pipelineService satisfies the primary port.
var _ PipelineService = (*pipelineService)(nil)

// ============================================================================
// TEMPLATE MANAGEMENT
// ============================================================================

// CreatePipeline validates the step graph and persists a new template with
// SERVER-AUTHORITATIVE id/created_by/created_at/team (from the actor, never the
// input). Idempotent on input.IdempotencyKey.
func (s *pipelineService) CreatePipeline(ctx context.Context, actor Actor, input CreatePipelineInput) (PipelineDefinition, error) {
	// Validate the graph BEFORE assigning ids or touching storage — a malformed
	// template must never be persisted (so the engine can trust any stored graph).
	if err := validateGraph(input.Type, input.Steps); err != nil {
		return PipelineDefinition{}, err
	}

	// Clamp untrusted numeric config (anti-DoS): MaxRetries is bounded so a
	// definition cannot request an effectively-infinite retry loop.
	steps := cloneStepsClamped(input.Steps)

	p := PipelineDefinition{
		ID:        s.ids.NewID(),    // SERVER-AUTHORITATIVE
		Name:      input.Name,
		Type:      input.Type,
		Steps:     steps,
		CreatedBy: actor.Subject,    // SERVER-AUTHORITATIVE: from auth claims, NOT input
		CreatedAt: s.clock.Now(),    // SERVER-AUTHORITATIVE
		Team:      actor.Team,       // SERVER-AUTHORITATIVE: tenancy from claims
	}
	created, err := s.pipelines.Create(ctx, p, input.IdempotencyKey)
	if err != nil {
		return PipelineDefinition{}, fmt.Errorf("create pipeline: %w", err)
	}
	return created, nil
}

// GetPipeline returns one template by id, scoped to the actor's team.
func (s *pipelineService) GetPipeline(ctx context.Context, actor Actor, id string) (PipelineDefinition, error) {
	p, err := s.pipelines.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, ErrRepoNotFound) {
			return PipelineDefinition{}, ErrPipelineNotFound
		}
		return PipelineDefinition{}, fmt.Errorf("get pipeline: %w", err)
	}
	// Tenancy: a caller cannot read another team's pipeline. We return NotFound
	// (not PermissionDenied) so we don't confirm the id exists out of scope.
	if p.Team != actor.Team {
		return PipelineDefinition{}, ErrPipelineNotFound
	}
	return p, nil
}

// UpdatePipeline re-validates and replaces a template's name/steps, preserving
// id/created_by/created_at/team. Type is immutable (we keep the stored type).
func (s *pipelineService) UpdatePipeline(ctx context.Context, actor Actor, input UpdatePipelineInput) (PipelineDefinition, error) {
	existing, err := s.GetPipeline(ctx, actor, input.ID) // also enforces team scope
	if err != nil {
		return PipelineDefinition{}, err
	}
	// Validate against the EXISTING (immutable) type — saga rules vs DAG rules.
	if err := validateGraph(existing.Type, input.Steps); err != nil {
		return PipelineDefinition{}, err
	}
	existing.Name = input.Name
	existing.Steps = cloneStepsClamped(input.Steps)
	updated, err := s.pipelines.Update(ctx, existing)
	if err != nil {
		if errors.Is(err, ErrRepoNotFound) {
			return PipelineDefinition{}, ErrPipelineNotFound
		}
		return PipelineDefinition{}, fmt.Errorf("update pipeline: %w", err)
	}
	return updated, nil
}

// DeletePipeline soft-deletes (archives) a template, scoped to the actor's team.
func (s *pipelineService) DeletePipeline(ctx context.Context, actor Actor, id string) error {
	if _, err := s.GetPipeline(ctx, actor, id); err != nil { // team scope + existence
		return err
	}
	if err := s.pipelines.Archive(ctx, id); err != nil {
		if errors.Is(err, ErrRepoNotFound) {
			return ErrPipelineNotFound
		}
		return fmt.Errorf("archive pipeline: %w", err)
	}
	return nil
}

// ListPipelines returns a tenancy-scoped page. The team filter is set from the
// actor here — never from the request — so a client cannot list another team's
// pipelines. page size is capped server-side.
func (s *pipelineService) ListPipelines(ctx context.Context, actor Actor, f ListPipelinesFilter) ([]PipelineDefinition, string, error) {
	f.Team = actor.Team                          // SERVER-SET tenancy scope
	f.List.PageSize = clampPageSize(f.List.PageSize)
	return s.pipelines.List(ctx, f)
}

// ============================================================================
// EXECUTION — the engine entry point
// ============================================================================

// TriggerExecution starts a run of a pipeline and drives it to a terminal state.
// Idempotent on input.IdempotencyKey.
func (s *pipelineService) TriggerExecution(ctx context.Context, actor Actor, input TriggerInput) (Execution, error) {
	// 1. Load + authorize the template (team scope + archived guard).
	p, err := s.pipelines.GetByID(ctx, input.PipelineID)
	if err != nil {
		if errors.Is(err, ErrRepoNotFound) {
			return Execution{}, ErrPipelineNotFound
		}
		return Execution{}, fmt.Errorf("load pipeline: %w", err)
	}
	if p.Team != actor.Team {
		// Out-of-scope id → NotFound (no existence leak), same as GetPipeline.
		return Execution{}, ErrPipelineNotFound
	}
	if p.Archived {
		return Execution{}, ErrPipelineArchived
	}

	// 2. Build the PENDING execution with a write-ahead StepExecution per step.
	//    These PENDING rows are the durability checkpoints the recovery path reads.
	exec := s.newPendingExecution(actor, p, input)

	// 3. Persist it (idempotent). If the key was seen, we get the ORIGINAL run
	//    back and DO NOT re-execute — exactly-once trigger from the caller's view.
	stored, err := s.executions.Create(ctx, exec, input.IdempotencyKey)
	if err != nil {
		return Execution{}, fmt.Errorf("persist execution: %w", err)
	}
	if stored.ID != exec.ID || stored.Status != ExecutionStatusPending {
		// Dedup hit: a prior trigger with this key already created (and ran) this
		// execution. Return it untouched — re-running would double-apply effects.
		return stored, nil
	}

	// 4. Register a cancel hook so CancelExecution can stop this run mid-flight,
	//    then run the engine to a terminal state.
	runCtx, cancel := context.WithCancel(ctx)
	s.registerCancel(exec.ID, cancel)
	defer s.unregisterCancel(exec.ID)
	defer cancel()

	return s.run(runCtx, &p, &exec)
}

// GetExecution returns a run's current state, scoped to the actor's team.
func (s *pipelineService) GetExecution(ctx context.Context, actor Actor, executionID string) (Execution, error) {
	e, err := s.executions.GetByID(ctx, executionID)
	if err != nil {
		if errors.Is(err, ErrRepoNotFound) {
			return Execution{}, ErrExecutionNotFound
		}
		return Execution{}, fmt.Errorf("get execution: %w", err)
	}
	if err := s.authorizeExecution(ctx, actor, e); err != nil {
		return Execution{}, err
	}
	return e, nil
}

// CancelExecution requests a graceful stop. If the run is currently executing in
// THIS process, we trip its cancel func (so the running engine transitions it to
// COMPENSATING → CANCELLED). If it is already terminal, we reject.
func (s *pipelineService) CancelExecution(ctx context.Context, actor Actor, executionID, reason string) (Execution, error) {
	e, err := s.executions.GetByID(ctx, executionID)
	if err != nil {
		if errors.Is(err, ErrRepoNotFound) {
			return Execution{}, ErrExecutionNotFound
		}
		return Execution{}, fmt.Errorf("load execution: %w", err)
	}
	if err := s.authorizeExecution(ctx, actor, e); err != nil {
		return Execution{}, err
	}
	if e.Status.IsTerminal() {
		return Execution{}, ErrExecutionNotCancellable
	}

	// Signal the running engine goroutine (if this leader is driving the run).
	// The engine observes the cancelled context, stops scheduling new steps, and
	// runs compensation — finally settling the run to CANCELLED. We then return
	// the (eventually) settled execution by re-reading it.
	if cancel := s.lookupCancel(executionID); cancel != nil {
		cancel()
		// Give the engine a brief, bounded window to settle, then re-read. We poll
		// rather than block forever so a wedged engine can't hang the cancel RPC.
		settled := s.awaitTerminal(ctx, executionID)
		return settled, nil
	}

	// No in-process runner (e.g. a stale PENDING row, or a different replica owns
	// it). Mark it for cancellation at the execution level so the owner/recovery
	// path picks it up. We transition to COMPENSATING as the intent signal.
	e.Status = ExecutionStatusCompensating
	if saveErr := s.executions.Save(ctx, e); saveErr != nil {
		return Execution{}, fmt.Errorf("mark cancelling: %w", saveErr)
	}
	return e, nil
}

// ListExecutions returns a tenancy-scoped page. Team is set from the actor.
func (s *pipelineService) ListExecutions(ctx context.Context, actor Actor, f ListExecutionsFilter) ([]Execution, string, error) {
	f.Team = actor.Team                          // SERVER-SET tenancy scope
	f.List.PageSize = clampPageSize(f.List.PageSize)
	return s.executions.List(ctx, f)
}

// ============================================================================
// THE ENGINE — run(): strategy dispatch + terminal settlement
// ============================================================================

// run transitions the execution to RUNNING, dispatches to the saga or DAG
// scheduler based on pipeline type, and settles the execution to a terminal
// state. It returns the settled Execution. It returns a non-nil error ONLY for
// infrastructure problems (e.g. a missing executor); a STEP failure is NOT an
// error here — it is a normal saga outcome that yields a FAILED Execution.
//
// WHY a failed step is not a Go error: the saga's whole point is that failure is
// a first-class, handled outcome (compensate + settle FAILED). Surfacing it as
// an error would push saga control flow into the caller. The caller gets the
// Execution and reads its Status. (Infra errors — "no executor registered" — ARE
// errors, because they mean the engine is mis-wired, not that a step failed.)
func (s *pipelineService) run(ctx context.Context, p *PipelineDefinition, exec *Execution) (Execution, error) {
	exec.Status = ExecutionStatusRunning
	exec.StartedAt = s.clock.Now()
	if err := s.executions.Save(ctx, *exec); err != nil {
		return Execution{}, fmt.Errorf("save running: %w", err)
	}
	s.emit(ctx, StepEvent{
		Type: EventPipelineStarted, ExecutionID: exec.ID, PipelineID: p.ID,
		PipelineType: p.Type, TriggeredBy: exec.TriggeredBy, OccurredAt: s.clock.Now(),
	})

	// stepErrs collects checkpoint-persistence failures from every markStep/
	// markStepAttempt/recordSuccess across the whole run (Finding 2). The schedulers
	// thread it down to the step helpers; we fold its verdict into the outcome below.
	errs := &stepErrs{}

	var runErr error // a step failure (saga) or propagated DAG failure
	if p.Type.IsSaga() {
		runErr = s.runStepsSaga(ctx, p, exec, errs)
	} else {
		runErr = s.runStepsDAG(ctx, p, exec, errs)
	}

	// Settle. cancelled is distinguished from failed by the context: if the run
	// context was cancelled, the user asked to stop → CANCELLED; otherwise a step
	// genuinely failed → FAILED. Both run compensation first (done inside the
	// schedulers for the saga; the DAG does not compensate).
	settled, settleErr := s.settle(ctx, p, exec, runErr)
	if settleErr != nil {
		// The terminal Save itself failed — that is the most severe durability
		// breach (the row may still say RUNNING). It takes precedence over a
		// step-checkpoint gap, so return it directly.
		return settled, settleErr
	}
	// The run reached a clean terminal STATUS, but if any step checkpoint failed to
	// persist, surface ErrStepPersist so the caller/leader knows the durable step
	// history is incomplete (the status is trustworthy; the per-step audit is not).
	if stepErr := errs.err(); stepErr != nil {
		return settled, stepErr
	}
	return settled, nil
}

// settle performs the final state-machine transition and emits the terminal
// lifecycle event. It is the single place that decides COMPLETED vs FAILED vs
// CANCELLED, so the terminal rules live in one auditable spot.
//
// DURABILITY CONTEXT (Finding 1 — never settle on a cancelled ctx): the terminal
// Save is the single most important write of the entire run — it is the row a
// crash-recovery/leader scan reads to decide "is this saga done, or stuck and
// needing resume?". If the user cancelled, the incoming ctx is cancelled and a
// real pgx adapter would REJECT this write, leaving the durable row RUNNING while
// we return CANCELLED. So we DETACH the persistence context here
// (context.WithoutCancel + a bounded SettlementTimeout) and write the terminal
// state on THAT context. The cancel signal stopped us scheduling new steps; it
// must not also abort recording the outcome.
//
// DURABILITY ERROR (Finding 2 — never lie about the terminal state): the terminal
// Save error is no longer swallowed. We retry it with backoff (it is worth a few
// retries) and, if it STILL fails, we return a non-nil error wrapping
// ErrTerminalPersist. The engine then reports the run's durable state as UNKNOWN
// rather than confidently returning COMPLETED/FAILED/CANCELLED while the database
// still says RUNNING — which would cause double-execution on recovery or orphaned
// resources with no audit trail. The caller/leader can retry the settle.
func (s *pipelineService) settle(ctx context.Context, p *PipelineDefinition, exec *Execution, runErr error) (Execution, error) {
	now := s.clock.Now()
	exec.CompletedAt = &now

	// Detach from the (possibly cancelled) run context for the terminal write, but
	// keep it bounded so a wedged DB can't hang the run. ALL three branches below
	// persist on persistCtx, never on the user-cancellable ctx.
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), SettlementTimeout)
	defer cancel()

	var (
		evType EventType
		evErr  string
	)
	switch {
	case runErr == nil:
		exec.Status = ExecutionStatusCompleted
		evType, evErr = EventPipelineCompleted, ""

	case errors.Is(runErr, context.Canceled):
		// User cancellation: the schedulers already compensated completed steps.
		exec.Status = ExecutionStatusCancelled
		// CANCELLED is an expected operator action — emit PipelineFailed? No: we
		// emit a terminal signal but the CANCELLED status tells Notification not
		// to page. We reuse PipelineFailed's channel with the cancelled status in
		// the snapshot; the consumer routes on the (read-back) status.
		evType, evErr = EventPipelineFailed, "cancelled"

	default:
		exec.Status = ExecutionStatusFailed
		exec.Error = sanitize(runErr)
		evType, evErr = EventPipelineFailed, exec.Error
	}

	// Persist the terminal transition with retry; surface failure rather than
	// swallow it (the write-ahead-durability contract the whole service is built on).
	if err := s.saveTerminal(persistCtx, *exec); err != nil {
		return *exec, fmt.Errorf("%w: execution %s terminal state %s not durably persisted: %v",
			ErrTerminalPersist, exec.ID, exec.Status, err)
	}

	// Only emit the lifecycle event once the terminal state is durable — a consumer
	// must never see "PipelineCompleted" for a run whose COMPLETED row didn't land.
	s.emit(persistCtx, StepEvent{
		Type: evType, ExecutionID: exec.ID, PipelineID: p.ID,
		PipelineType: p.Type, Error: evErr, OccurredAt: now,
	})
	return *exec, nil
}

// saveTerminal persists the terminal Execution row with a small retry-with-backoff
// budget. The terminal Save is the run's most important durable write, so a
// transient repo hiccup should not immediately strand the run in an UNKNOWN state;
// we give it a few attempts on the DETACHED settlement context before giving up.
// Cancellation of that context (the SettlementTimeout firing) stops the retries
// promptly. Returns the last error if every attempt fails.
func (s *pipelineService) saveTerminal(ctx context.Context, exec Execution) error {
	const maxAttempts = 3
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := s.executions.Save(ctx, exec); err == nil {
			return nil
		} else {
			lastErr = err
		}
		// Don't sleep after the final attempt, and abort early if the bounded
		// settlement context is done (timeout fired) — no point retrying a dead ctx.
		if attempt < maxAttempts && ctx.Err() == nil {
			backoff := time.Duration(attempt) * 50 * time.Millisecond
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return lastErr
			case <-timer.C:
			}
		}
	}
	return lastErr
}

// ============================================================================
// SAGA SCHEDULER — sequential, compensation in reverse on failure
// ============================================================================

// runStepsSaga executes the saga steps in topological order, one at a time. On
// the first step failure (or a cancellation), it compensates the already-
// COMPLETED steps in REVERSE completion order and returns the cause (the step
// error, or context.Canceled). On success it returns nil.
//
// COMPENSATION ORDERING (the centerpiece): when step k fails, steps
// 0..k-1 have applied side effects. We must undo them in the OPPOSITE order they
// were applied — last-applied is undone first — because later steps may depend on
// the state earlier steps created (e.g. "shift traffic" before "destroy
// instance"). completedStepsInOrder() gives forward completion order; we iterate
// it BACKWARDS. This LIFO discipline is exactly a transaction rollback / an undo
// stack.
func (s *pipelineService) runStepsSaga(ctx context.Context, p *PipelineDefinition, exec *Execution, errs *stepErrs) error {
	order, err := topoOrder(p.Steps) // linear order honoring depends_on
	if err != nil {
		return err
	}
	byID := p.stepByID()

	for _, stepID := range order {
		// Honor cancellation between steps: stop scheduling new work the moment
		// the user cancels, then fall through to compensation. The user cancelled
		// BEFORE this step ran, so there is nothing to SKIP — go straight to
		// compensation (which detaches its own persistence context, Finding 1).
		if ctx.Err() != nil {
			return s.compensate(ctx, p, exec, byID, context.Canceled, errs)
		}

		def := byID[stepID]
		runErr := s.runOneStep(ctx, p, exec, def, errs)
		if runErr == nil {
			continue // step COMPLETED; persist already done in runOneStep
		}

		// Step failed (or was cancelled mid-run). Begin compensation of the
		// completed steps. The cause we propagate is context.Canceled if the
		// failure was due to cancellation, else the step error.
		cause := runErr
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(runErr, context.Canceled) {
			cause = context.Canceled
		}

		// DURABILITY CONTEXT (Finding 1): the SKIPPED markers are writes we are
		// OBLIGATED to persist even when the user cancelled (which cancels ctx). Detach
		// here — context.WithoutCancel + a bounded SettlementTimeout — so a real pgx
		// adapter does not reject the SKIPPED checkpoints with context.Canceled.
		// (compensate() detaches its OWN persistence context internally, so we hand it
		// the live ctx and it remains safe regardless of caller — see compensate.)
		// DURABILITY CONTEXT (Finding 1): the SKIPPED markers are writes we are
		// OBLIGATED to persist even when the user cancelled (which cancels ctx). Detach
		// here — context.WithoutCancel + a bounded SettlementTimeout — so a real pgx
		// adapter does not reject the SKIPPED checkpoints with context.Canceled.
		// (compensate() detaches its OWN persistence context internally, so we hand it
		// the live ctx and it remains safe regardless of caller — see compensate.)
		persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), SettlementTimeout)
		defer cancel()

		// Mark any not-yet-run steps as SKIPPED (they are after the failure).
		s.skipRemaining(persistCtx, exec, order, stepID, errs)
		return s.compensate(ctx, p, exec, byID, cause, errs)
	}
	return nil // all steps completed
}

// compensate transitions the execution to COMPENSATING, emits
// CompensationTriggered, then undoes every COMPLETED step in REVERSE completion
// order. A compensation that itself errors marks that step COMPENSATION_FAILED
// (a stuck saga) and is surfaced via the returned error wrapping
// ErrCompensationFailed — but we STILL attempt the remaining compensations
// (best-effort), because undoing the other steps is still valuable.
//
// Returns the original cause (so settle() can decide FAILED vs CANCELLED). If a
// compensation failed, the cause is wrapped with ErrCompensationFailed so the
// caller knows a human must reconcile — but the terminal status is still driven
// by the original cause (a cancelled saga whose compensation failed is still
// CANCELLED, just with a stuck step recorded).
func (s *pipelineService) compensate(ctx context.Context, p *PipelineDefinition, exec *Execution, byID map[string]*StepDefinition, cause error, errs *stepErrs) error {
	// DURABILITY CONTEXT (Finding 1): compensation is the cleanup we are OBLIGATED to
	// perform AND record even when the user cancelled (which cancels the incoming
	// ctx). Detach ONCE at the top — context.WithoutCancel + a bounded
	// SettlementTimeout — and use compCtx for EVERY write below: the exec-level
	// COMPENSATING Save, the CompensationTriggered emit, and each compensateOne.
	// Previously only compensateOne was detached, so the exec-level COMPENSATING Save
	// (here) ran on the cancelled ctx and a real pgx adapter would reject it with
	// context.Canceled — leaving the durable row RUNNING while we believed it was
	// COMPENSATING. Threading the SAME detached context through all of them closes
	// that gap.
	compCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), SettlementTimeout)
	defer cancel()

	exec.Status = ExecutionStatusCompensating
	// The COMPENSATING transition is a real state-machine checkpoint; record (don't
	// swallow) its persistence error so the run's outcome reflects a missed write
	// (Finding 2). It rides the step-error channel as the closest existing bucket.
	errs.record(s.executions.Save(compCtx, *exec))

	completed := exec.completedStepsInOrder() // forward completion order
	// Build the rollback PLAN (ids in reverse) for the CompensationTriggered event.
	plan := make([]string, 0, len(completed))
	for i := len(completed) - 1; i >= 0; i-- {
		plan = append(plan, completed[i].StepID)
	}
	s.emit(compCtx, StepEvent{
		Type: EventCompensationTriggered, ExecutionID: exec.ID, PipelineID: p.ID,
		PipelineType: p.Type, OccurredAt: s.clock.Now(),
	})

	var compFailed bool
	for i := len(completed) - 1; i >= 0; i-- { // REVERSE completion order (LIFO)
		se := completed[i]
		def := byID[se.StepID]
		// Only compensate steps that DEFINED a compensation. A read-only step
		// (no CompensationStepID) has no side effect to undo — skip it. It stays
		// COMPLETED (it ran and was never reversed).
		if def == nil || def.CompensationStepID == "" {
			continue
		}
		if err := s.compensateOne(compCtx, p, exec, def, &se, errs); err != nil {
			compFailed = true // recorded as COMPENSATION_FAILED; keep going (best-effort)
		}
	}

	if compFailed {
		// Wrap so the handler can surface "manual reconciliation required" while
		// the terminal status still reflects the original cause.
		return fmt.Errorf("%w (original cause: %v)", ErrCompensationFailed, cause)
	}
	return cause
}

// compensateOne undoes a single completed step. It transitions the step
// COMPLETED → COMPENSATING → COMPENSATED (or COMPENSATION_FAILED), persisting
// each transition. Returns an error only if the compensation itself failed.
func (s *pipelineService) compensateOne(ctx context.Context, p *PipelineDefinition, exec *Execution, def *StepDefinition, se *StepExecution, errs *stepErrs) error {
	executor, ok := s.registry.Executor(def.Type)
	if !ok {
		// No executor to undo with — treat as a failed compensation (stuck).
		errs.record(s.markStep(ctx, exec, se.StepID, StepStatusCompensationFailed, "no executor for compensation"))
		return ErrNoExecutor
	}

	errs.record(s.markStep(ctx, exec, se.StepID, StepStatusCompensating, ""))
	in := s.buildStepInput(exec, def, se.Attempt)
	if err := executor.Compensate(ctx, in); err != nil {
		// COMPENSATION_FAILED: the worst case. We could not undo a side effect.
		// Surfaced loudly (status + error) so an operator reconciles the orphaned
		// resource; never silently swallowed.
		errs.record(s.markStep(ctx, exec, se.StepID, StepStatusCompensationFailed, sanitize(err)))
		return err
	}
	errs.record(s.markStep(ctx, exec, se.StepID, StepStatusCompensated, ""))

	// If this was a deploy/promote step, its compensation tore down the serving
	// instance → emit ModelUndeployed so the gateway/serving drop the route.
	if isDeployStep(def.Type) {
		s.emit(ctx, StepEvent{
			Type: EventModelUndeployed, ExecutionID: exec.ID, PipelineID: p.ID,
			PipelineType: p.Type, StepID: def.ID, StepType: def.Type, OccurredAt: s.clock.Now(),
		})
	}
	return nil
}

// ============================================================================
// DAG SCHEDULER — parallel levels, failure → SKIPPED dependents, no compensation
// ============================================================================

// runStepsDAG executes the steps in topological LEVELS. All steps in a level are
// independent (no edges among them) so they run CONCURRENTLY (fan-out). If any
// step in a level fails, its transitive dependents are marked SKIPPED and the run
// fails — but a DAG does NOT compensate (training steps are re-runnable; there is
// no global rollback). Returns the first step error, or nil on full success.
//
// WHY levels and not a worker pool over a ready-set: levels make the parallelism
// structure obvious and the test deterministic (a level is a clean barrier). A
// ready-set scheduler is more efficient for sparse graphs but harder to reason
// about; for ML pipelines (tens of steps) the level approach is plenty and is
// far easier to explain.
func (s *pipelineService) runStepsDAG(ctx context.Context, p *PipelineDefinition, exec *Execution, errs *stepErrs) error {
	levels, err := topoLevels(p.Steps)
	if err != nil {
		return err
	}
	byID := p.stepByID()
	failed := map[string]bool{} // step ids that failed or were skipped

	// DURABILITY CONTEXT (Finding 1): SKIPPED is a checkpoint we want durable even
	// when a cancel races in (cancelling ctx). Detach a bounded context purely for
	// the SKIPPED markStep writes so they aren't rejected by a real DB after cancel.
	// The forward executors below still use the LIVE ctx — a cancel SHOULD abort the
	// expensive work in flight; only the bookkeeping write must survive.
	skipCtx, cancelSkip := context.WithTimeout(context.WithoutCancel(ctx), SettlementTimeout)
	defer cancelSkip()

	var firstErr error
	for _, level := range levels {
		// Partition this level into runnable (all parents succeeded) vs skipped
		// (a parent failed). Skipped steps never run; their dependents skip too.
		var runnable []string
		for _, id := range level {
			if anyParentFailed(byID[id], failed) {
				errs.record(s.markStep(skipCtx, exec, id, StepStatusSkipped, ""))
				failed[id] = true
				continue
			}
			runnable = append(runnable, id)
		}
		if len(runnable) == 0 {
			continue
		}

		// Run the runnable steps in this level concurrently. ctx cancellation
		// (user cancel) aborts in-flight executors via the shared context.
		var wg sync.WaitGroup
		var mu sync.Mutex
		results := make(map[string]error, len(runnable))
		for _, id := range runnable {
			wg.Add(1)
			go func(stepID string) {
				defer wg.Done()
				e := s.runOneStep(ctx, p, exec, byID[stepID], errs)
				mu.Lock()
				results[stepID] = e
				mu.Unlock()
			}(id)
		}
		wg.Wait()

		for id, e := range results {
			if e != nil {
				failed[id] = true
				if firstErr == nil {
					firstErr = fmt.Errorf("step %s: %w", id, e)
				}
			}
		}
		// Honor cancellation as a level barrier: if cancelled, skip the rest.
		if ctx.Err() != nil && firstErr == nil {
			firstErr = context.Canceled
		}
	}
	return firstErr
}

// anyParentFailed reports whether any of a step's depends_on parents is in the
// failed/skipped set — which means this step cannot run and must be SKIPPED.
func anyParentFailed(def *StepDefinition, failed map[string]bool) bool {
	if def == nil {
		return false
	}
	for _, parent := range def.DependsOn {
		if failed[parent] {
			return true
		}
	}
	return false
}

// ============================================================================
// SINGLE-STEP EXECUTION — with retries, idempotent attempt tracking
// ============================================================================

// runOneStep executes one step with retry: it tries Execute up to (MaxRetries+1)
// times, persisting each attempt's status. On success it records COMPLETED + the
// step output and (for deploy steps) emits ModelDeployed/StepCompleted. On
// exhausting retries it records FAILED + emits StepFailed and returns the error.
//
// IDEMPOTENT RETRY: the Attempt counter is passed to the
// executor in StepInput so a non-idempotent forward action can make itself safe
// across retries ("if attempt > 1, check whether I already created the resource").
// The engine guarantees attempts are sequential and the count is persisted.
func (s *pipelineService) runOneStep(ctx context.Context, p *PipelineDefinition, exec *Execution, def *StepDefinition, errs *stepErrs) error {
	// CurrentStep is a coarse "where are we" pointer; in a parallel DAG several
	// goroutines write it, so guard it with execMu (the value is racy-but-coarse
	// by design — the authoritative per-step state is in the Steps slice).
	s.execMu.Lock()
	exec.CurrentStep = def.ID
	s.execMu.Unlock()

	executor, ok := s.registry.Executor(def.Type)
	if !ok {
		errs.record(s.markStep(ctx, exec, def.ID, StepStatusFailed, "no executor registered"))
		return ErrNoExecutor
	}

	// DURABILITY CONTEXT (Finding 1): the FAILED-due-to-cancel checkpoint must land
	// durably even though a user-cancel has cancelled ctx — a real DB would reject a
	// write on the cancelled ctx. Prepare ONE detached, bounded context up front
	// (with a deferred cancel, so no context leak) and use it for the cancel-path
	// SaveStep writes below. The forward executor still runs on the LIVE ctx so a
	// cancel aborts the expensive work in flight; only the bookkeeping must survive.
	persistCtx, cancelPersist := context.WithTimeout(context.WithoutCancel(ctx), SettlementTimeout)
	defer cancelPersist()

	maxAttempts := def.MaxRetries + 1 // 1 initial try + MaxRetries retries
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Bail out promptly on cancellation rather than starting another attempt.
		if ctx.Err() != nil {
			errs.record(s.markStepAttempt(persistCtx, exec, def, StepStatusFailed, "cancelled", attempt))
			return ctx.Err()
		}

		errs.record(s.markStepAttempt(ctx, exec, def, StepStatusRunning, "", attempt))

		// Apply a per-step timeout if configured; the executor must honor ctx.
		stepCtx := ctx
		var cancel context.CancelFunc
		if def.Timeout > 0 {
			stepCtx, cancel = context.WithTimeout(ctx, def.Timeout)
		}
		result, err := executor.Execute(stepCtx, s.buildStepInput(exec, def, attempt))
		if cancel != nil {
			cancel()
		}

		if err == nil {
			// SUCCESS: record output + COMPLETED, emit StepCompleted, and (for a
			// deploy/promote step that resolved an endpoint) ModelDeployed.
			errs.record(s.recordSuccess(ctx, p, exec, def, attempt, result))
			return nil
		}

		lastErr = err
		// If cancelled, do not retry — propagate the cancellation immediately. The
		// FAILED-due-to-cancel checkpoint rides the detached persistCtx so it lands
		// even though the user-cancel killed ctx (Finding 1).
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			errs.record(s.markStepAttempt(persistCtx, exec, def, StepStatusFailed, "cancelled", attempt))
			return context.Canceled
		}
		// Otherwise it's a (possibly transient) failure: loop to retry if budget
		// remains. We persist the attempt so operators see the retry history.
	}

	// Retries exhausted → the step has FAILED. This is a genuine failure (not a
	// cancel), so ctx is live and the checkpoint persists normally.
	errs.record(s.markStepAttempt(ctx, exec, def, StepStatusFailed, sanitize(lastErr), maxAttempts))
	s.emit(ctx, StepEvent{
		Type: EventStepFailed, ExecutionID: exec.ID, PipelineID: p.ID, PipelineType: p.Type,
		StepID: def.ID, StepType: def.Type, Error: sanitize(lastErr),
		Attempts: maxAttempts, OccurredAt: s.clock.Now(),
	})
	return fmt.Errorf("step %s failed after %d attempts: %w", def.ID, maxAttempts, lastErr)
}

// recordSuccess persists a step's COMPLETED state + output and emits the success
// events. Split out so runOneStep reads as a clean retry loop. Returns the
// SaveStep error so the caller records it in the run's stepErrs (Finding 2): a
// COMPLETED step whose checkpoint is lost would be re-run (or, in a saga, never
// compensated) on recovery, so the gap must surface.
func (s *pipelineService) recordSuccess(ctx context.Context, p *PipelineDefinition, exec *Execution, def *StepDefinition, attempt int, result StepResult) error {
	now := s.clock.Now()
	s.execMu.Lock()
	se := exec.findStep(def.ID)
	if se != nil {
		se.Status = StepStatusCompleted
		se.Output = result.Output
		se.CompletedAt = &now
		se.Attempt = attempt
	}
	var snapshot StepExecution
	if se != nil {
		snapshot = *se
	}
	s.execMu.Unlock()
	var saveErr error
	if se != nil {
		saveErr = s.executions.SaveStep(ctx, snapshot)
	}
	s.emit(ctx, StepEvent{
		Type: EventStepCompleted, ExecutionID: exec.ID, PipelineID: p.ID, PipelineType: p.Type,
		StepID: def.ID, StepType: def.Type, Output: result.Output, OccurredAt: now,
	})
	// DEPLOY/PROMOTE that resolved a serving endpoint → ModelDeployed (fat-flat
	// event the gateway/serving consume to add the route). SSRF-safe: Endpoint was
	// RESOLVED BY THE EXECUTOR, never read from client config.
	if isDeployStep(def.Type) && result.Endpoint != "" {
		s.emit(ctx, StepEvent{
			Type: EventModelDeployed, ExecutionID: exec.ID, PipelineID: p.ID, PipelineType: p.Type,
			StepID: def.ID, StepType: def.Type, Endpoint: result.Endpoint, OccurredAt: now,
		})
	}
	return saveErr
}

// ============================================================================
// STEP STATE HELPERS — every transition persists a checkpoint (write-ahead)
// ============================================================================

// stepErrs accumulates SaveStep (step-checkpoint) failures across ONE run.
//
// WHY accumulate instead of abort-on-first or swallow (Finding 2): a step
// checkpoint that fails to persist must not be silently dropped (recovery would
// mis-resume), but it also should not abort the run mid-step — the remaining
// forward/compensation work is still worth doing, and the most important write
// (the terminal Save) is handled separately with its own retry. So we keep going,
// remember that AT LEAST ONE checkpoint did not land, and fold ErrStepPersist into
// the run's outcome at the end. We do NOT keep every error (that would be unbounded
// and leak internal detail); first + count is enough to alert + investigate.
//
// CONCURRENCY: the DAG scheduler runs steps in parallel, so several goroutines may
// call record concurrently → guard with a mutex (caught by -race otherwise).
type stepErrs struct {
	mu    sync.Mutex
	first error
	count int
}

// record folds one SaveStep result into the accumulator. A nil error is a no-op,
// so call sites can write errs.record(s.markStep(...)) without an `if`.
func (e *stepErrs) record(err error) {
	if err == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.count++
	if e.first == nil {
		e.first = err
	}
}

// err returns a non-nil error wrapping ErrStepPersist if any checkpoint failed to
// persist, else nil. The wrapped detail carries the count + the first cause so an
// operator can both alert (errors.Is ErrStepPersist) and investigate.
func (e *stepErrs) err() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.count == 0 {
		return nil
	}
	return fmt.Errorf("%w: %d checkpoint write(s) failed (first: %v)", ErrStepPersist, e.count, e.first)
}

// markStep transitions a step's status + error and persists it. Used for
// compensation sub-states and skips, where the attempt count does not change.
//
// Returns the SaveStep error (Finding 2). Callers record it into the run's
// stepErrs accumulator instead of dropping it: a lost checkpoint means crash
// recovery sees a stale step state. The transition is applied IN MEMORY regardless
// (the engine's authoritative state advances) — the returned error reports only
// that the DURABLE copy did not land.
func (s *pipelineService) markStep(ctx context.Context, exec *Execution, stepID string, status StepStatus, errMsg string) error {
	s.execMu.Lock()
	se := exec.findStep(stepID)
	if se == nil {
		s.execMu.Unlock()
		return nil
	}
	se.Status = status
	se.Error = errMsg
	if status == StepStatusCompensated || status == StepStatusCompensationFailed || status == StepStatusSkipped {
		now := s.clock.Now()
		se.CompletedAt = &now
	}
	snapshot := *se
	s.execMu.Unlock()
	return s.executions.SaveStep(ctx, snapshot)
}

// markStepAttempt transitions a step during the forward run, stamping started_at
// on the first RUNNING transition and recording the attempt number. Returns the
// SaveStep error so the caller can record it in the run's stepErrs (Finding 2).
func (s *pipelineService) markStepAttempt(ctx context.Context, exec *Execution, def *StepDefinition, status StepStatus, errMsg string, attempt int) error {
	s.execMu.Lock()
	se := exec.findStep(def.ID)
	if se == nil {
		s.execMu.Unlock()
		return nil
	}
	se.Status = status
	se.Error = errMsg
	se.Attempt = attempt
	if status == StepStatusRunning && se.StartedAt == nil {
		now := s.clock.Now()
		se.StartedAt = &now
	}
	if status == StepStatusFailed {
		now := s.clock.Now()
		se.CompletedAt = &now
	}
	snapshot := *se
	s.execMu.Unlock()
	return s.executions.SaveStep(ctx, snapshot)
}

// skipRemaining marks every step AFTER the failed step (in the linear saga order)
// as SKIPPED — they never ran because a predecessor failed.
//
// PERSISTENCE CONTEXT (Finding 1): this runs on the cancel/failure path, so it is
// given the DETACHED persistCtx by its caller — never the user-cancellable run
// ctx — or a real DB would reject the SKIPPED checkpoint writes with
// context.Canceled. SaveStep failures are recorded in stepErrs (Finding 2).
func (s *pipelineService) skipRemaining(ctx context.Context, exec *Execution, order []string, failedStepID string, errs *stepErrs) {
	past := false
	for _, id := range order {
		if id == failedStepID {
			past = true
			continue
		}
		if !past {
			continue
		}
		se := exec.findStep(id)
		if se != nil && se.Status == StepStatusPending {
			errs.record(s.markStep(ctx, exec, id, StepStatusSkipped, ""))
		}
	}
}

// ============================================================================
// INPUT CONSTRUCTION — server-authoritative, SSRF-safe
// ============================================================================

// buildStepInput assembles the StepInput from SERVER-AUTHORITATIVE data only:
// the persisted Config, the run input, and the COMPLETED upstream outputs. It
// never injects a client-supplied endpoint — the SSRF guard is structural (the
// engine doesn't even have a field for a client URL to flow through).
func (s *pipelineService) buildStepInput(exec *Execution, def *StepDefinition, attempt int) StepInput {
	// Reading a parent's status/output races with concurrent writers in a DAG, so
	// snapshot the upstream outputs under execMu.
	s.execMu.Lock()
	upstream := make(map[string]map[string]any)
	for _, parentID := range def.DependsOn {
		if pse := exec.findStep(parentID); pse != nil && pse.Status == StepStatusCompleted {
			upstream[parentID] = pse.Output
		}
	}
	s.execMu.Unlock()
	return StepInput{
		ExecutionID:     exec.ID,
		StepID:          def.ID,
		StepType:        def.Type,
		Config:          def.Config, // UNTRUSTED — the executor validates it
		Input:           exec.Input,
		UpstreamOutputs: upstream,
		Attempt:         attempt,
	}
}

// newPendingExecution builds the initial PENDING Execution with a write-ahead
// StepExecution (also PENDING) per template step. These rows are persisted by
// the caller (Create) and are the durability checkpoints crash recovery reads.
func (s *pipelineService) newPendingExecution(actor Actor, p PipelineDefinition, input TriggerInput) Execution {
	exec := Execution{
		ID:          s.ids.NewID(),    // SERVER-AUTHORITATIVE
		PipelineID:  p.ID,
		Status:      ExecutionStatusPending,
		TriggeredBy: actor.Subject,    // SERVER-AUTHORITATIVE: who/what triggered (user or service)
		StartedAt:   s.clock.Now(),
		Input:       input.Input,
	}
	for _, def := range p.Steps {
		exec.Steps = append(exec.Steps, StepExecution{
			ID:          s.ids.NewID(),
			ExecutionID: exec.ID,
			StepID:      def.ID,
			StepType:    def.Type,
			Status:      StepStatusPending,
		})
	}
	return exec
}

// ============================================================================
// AUTHORIZATION + PUBLISH HELPERS
// ============================================================================

// authorizeExecution enforces tenancy on a run: the actor's team must own the
// pipeline the execution belongs to. We resolve the pipeline's team (the
// execution doesn't store team directly — it's a property of the template) and
// compare. A mismatch returns ErrExecutionNotFound (no existence leak).
func (s *pipelineService) authorizeExecution(ctx context.Context, actor Actor, e Execution) error {
	p, err := s.pipelines.GetByID(ctx, e.PipelineID)
	if err != nil {
		// If the pipeline is gone, fall back to denying (can't prove ownership).
		return ErrExecutionNotFound
	}
	if p.Team != actor.Team {
		return ErrExecutionNotFound
	}
	return nil
}

// emit publishes a lifecycle event best-effort. A publish failure is logged by
// the adapter but NEVER fails the saga (see EventPublisher's contract). If no
// publisher is wired (scaffold/tests without events), this is a no-op.
func (s *pipelineService) emit(ctx context.Context, ev StepEvent) {
	if s.publisher == nil {
		return
	}
	_ = s.publisher.Publish(ctx, ev)
}

// ============================================================================
// CANCELLATION REGISTRY
// ============================================================================

func (s *pipelineService) registerCancel(execID string, cancel context.CancelFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancels[execID] = cancel
}

func (s *pipelineService) unregisterCancel(execID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.cancels, execID)
}

func (s *pipelineService) lookupCancel(execID string) context.CancelFunc {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cancels[execID]
}

// awaitTerminal polls the execution until it reaches a terminal state or the
// caller's context expires, then returns the latest state. Bounded so a wedged
// engine cannot hang the Cancel RPC forever.
func (s *pipelineService) awaitTerminal(ctx context.Context, execID string) Execution {
	deadline := time.Now().Add(5 * time.Second)
	for {
		e, err := s.executions.GetByID(ctx, execID)
		if err == nil && e.Status.IsTerminal() {
			return e
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			// Return whatever we last read (best-effort); the engine will still
			// settle it durably even if we stop waiting here.
			if err == nil {
				return e
			}
			return Execution{ID: execID, Status: ExecutionStatusCompensating}
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ============================================================================
// GRAPH VALIDATION + TOPOLOGICAL SORT (pure functions, directly unit-tested)
// ============================================================================

// validateGraph enforces every pipeline-graph invariant BEFORE persistence:
// non-empty, within the step cap, unique ids, known step types, every depends_on
// and compensation pointer references a real step, and (the big one) NO CYCLES.
// Each failure wraps the matching sentinel AND ErrValidation, so the handler maps
// to codes.InvalidArgument while a test can assert the specific cause.
func validateGraph(_ PipelineType, steps []StepDefinition) error {
	if len(steps) == 0 {
		// ErrEmptyPipeline already wraps ErrValidation (see errors.go), so this
		// satisfies errors.Is for both the specific and umbrella targets.
		return ErrEmptyPipeline
	}
	if len(steps) > MaxStepsPerPipeline {
		return fmt.Errorf("%w: %d steps exceeds cap %d", ErrTooManySteps, len(steps), MaxStepsPerPipeline)
	}

	// Unique ids + known types.
	ids := make(map[string]struct{}, len(steps))
	for _, st := range steps {
		if st.ID == "" {
			return fmt.Errorf("%w: a step has an empty id", ErrValidation)
		}
		if _, dup := ids[st.ID]; dup {
			return fmt.Errorf("%w: %q", ErrDuplicateStepID, st.ID)
		}
		ids[st.ID] = struct{}{}
		if !st.Type.IsKnown() {
			return fmt.Errorf("%w: step %q has type %v", ErrUnknownStepType, st.ID, st.Type)
		}
	}

	// Edges (depends_on + compensation pointers) must reference real steps.
	for _, st := range steps {
		for _, dep := range st.DependsOn {
			if _, ok := ids[dep]; !ok {
				return fmt.Errorf("%w: step %q depends_on unknown step %q", ErrDanglingDependency, st.ID, dep)
			}
			if dep == st.ID {
				return fmt.Errorf("%w: step %q depends on itself", ErrCycleDetected, st.ID)
			}
		}
		if st.CompensationStepID != "" {
			if _, ok := ids[st.CompensationStepID]; !ok {
				return fmt.Errorf("%w: step %q compensation references unknown step %q", ErrDanglingDependency, st.ID, st.CompensationStepID)
			}
		}
	}

	// Cycle detection via the topo sort (the authoritative check).
	if _, err := topoLevels(steps); err != nil {
		return err
	}
	return nil
}

// topoLevels performs a Kahn's-algorithm topological sort and groups the result
// into LEVELS: level 0 is every step with no unmet dependency, level 1 is every
// step whose dependencies are all in level 0, and so on. Steps within a level are
// mutually independent and therefore safe to run in PARALLEL (the DAG fan-out).
//
// KAHN'S ALGORITHM:
//  1. Compute the in-degree (number of unmet deps) of every node.
//  2. The current level = all nodes with in-degree 0.
//  3. "Remove" them, decrementing the in-degree of their dependents.
//  4. The next level = nodes that just reached in-degree 0. Repeat.
//  5. If we processed fewer nodes than exist, the leftover nodes form a CYCLE
//     (they can never reach in-degree 0) → ErrCycleDetected.
//
// WHY Kahn's (BFS) over DFS-with-colors: Kahn's naturally yields the LEVELS we
// want for parallel scheduling, and the "processed < total ⇒ cycle" check is a
// clean, single comparison. DFS detects cycles too but doesn't hand you the
// level structure for free.
func topoLevels(steps []StepDefinition) ([][]string, error) {
	indeg := make(map[string]int, len(steps))
	dependents := make(map[string][]string, len(steps)) // parent -> children
	order := make([]string, 0, len(steps))               // stable id order for determinism

	for _, st := range steps {
		if _, seen := indeg[st.ID]; !seen {
			order = append(order, st.ID)
		}
		indeg[st.ID] += len(st.DependsOn)
		for _, dep := range st.DependsOn {
			dependents[dep] = append(dependents[dep], st.ID)
		}
	}

	// Seed level 0 with in-degree-0 nodes, preserving input order for stable,
	// reproducible scheduling (helps make tests deterministic).
	var current []string
	for _, id := range order {
		if indeg[id] == 0 {
			current = append(current, id)
		}
	}

	var levels [][]string
	processed := 0
	for len(current) > 0 {
		levels = append(levels, current)
		var next []string
		for _, id := range current {
			processed++
			// Decrement dependents; any that hit 0 join the next level. We iterate
			// in input order to keep the next level deterministic.
			for _, child := range dependents[id] {
				indeg[child]--
				if indeg[child] == 0 {
					next = append(next, child)
				}
			}
		}
		current = next
	}

	if processed < len(indeg) {
		// Some nodes never reached in-degree 0 → they are in a cycle.
		return nil, fmt.Errorf("%w", ErrCycleDetected)
	}
	return levels, nil
}

// topoOrder flattens topoLevels into a single linear order. The saga scheduler
// uses this: it runs steps one at a time in dependency order. (For a saga the
// graph is typically a chain, but flattening the level order works for any DAG
// and keeps the saga and DAG schedulers sharing one sort.)
func topoOrder(steps []StepDefinition) ([]string, error) {
	levels, err := topoLevels(steps)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, lvl := range levels {
		out = append(out, lvl...)
	}
	return out, nil
}

// ============================================================================
// SMALL PURE HELPERS
// ============================================================================

// cloneStepsClamped deep-copies the step slice and clamps MaxRetries to the cap.
// Cloning ensures the engine owns its own copy (a caller mutating its input slice
// later can't change a persisted/running pipeline).
func cloneStepsClamped(steps []StepDefinition) []StepDefinition {
	out := make([]StepDefinition, len(steps))
	for i, st := range steps {
		out[i] = st
		out[i].DependsOn = append([]string(nil), st.DependsOn...)
		if out[i].MaxRetries < 0 {
			out[i].MaxRetries = 0 // negative is nonsense → fail fast (no retries)
		}
		if out[i].MaxRetries > MaxRetriesCap {
			out[i].MaxRetries = MaxRetriesCap // clamp absurd values (anti-DoS)
		}
	}
	return out
}

// isDeployStep reports whether a step kind (un)deploys a model — DEPLOY and
// PROMOTE both change serving routing, so both gate the ModelDeployed /
// ModelUndeployed events.
func isDeployStep(t StepType) bool {
	return t == StepTypeDeploy || t == StepTypePromote
}

// clampPageSize enforces the list page-size policy server-side (default 20, max
// 100) regardless of what a client requests — an unbounded page is a DoS vector.
func clampPageSize(requested int) int {
	const (
		def = 20
		max = 100
	)
	if requested <= 0 {
		return def
	}
	if requested > max {
		return max
	}
	return requested
}

// sanitize trims an internal error down to a safe, human-readable message for
// the Execution.Error field / events. It strips nothing structured today (the
// interceptor sanitizes gRPC status messages), but centralizing it gives one
// place to redact if an executor ever leaks sensitive detail into an error.
func sanitize(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// sortStepsByCompletedAt stable-sorts step executions by CompletedAt ascending.
// Declared here (used by Execution.completedStepsInOrder in models.go) so the
// sort import lives with the engine rather than the pure model file. A nil
// CompletedAt sorts first (defensive; a COMPLETED step always has one).
func sortStepsByCompletedAt(steps []StepExecution) {
	sort.SliceStable(steps, func(i, j int) bool {
		ti, tj := steps[i].CompletedAt, steps[j].CompletedAt
		switch {
		case ti == nil && tj == nil:
			return false
		case ti == nil:
			return true
		case tj == nil:
			return false
		default:
			return ti.Before(*tj)
		}
	})
}
