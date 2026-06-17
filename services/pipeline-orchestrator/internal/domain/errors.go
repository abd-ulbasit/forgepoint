// errors.go — sentinel errors owned by the Pipeline Orchestrator domain.
//
// ============================================================================
// WHY SENTINELS + errors.Is, AND THE TWO-VOCABULARY SPLIT
// ============================================================================
//
// As in the Auth service, the domain distinguishes STORAGE outcomes (the
// repository ports' ErrRepoNotFound in ports.go — "no such row") from BUSINESS
// outcomes (the errors below — "that pipeline does not exist"). The service is
// the single translation point: it catches a storage sentinel and re-expresses
// it as the matching business error, so the handler maps cleanly to a gRPC code
// and never needs to know storage exists.
//
// All errors here are package-level sentinels so callers can branch with
// errors.Is(err, ErrCycleDetected). Validation errors wrap ErrValidation with %w
// so a caller gets a SPECIFIC message ("step 'deploy' depends on unknown step
// 'buidl'") while still matching errors.Is(err, ErrValidation) for the
// codes.InvalidArgument mapping. This is the idiomatic Go error-taxonomy pattern.
// ============================================================================
package domain

import (
	"errors"
	"fmt"
)

var (
	// ErrValidation is the umbrella for any malformed pipeline/trigger input the
	// service rejects BEFORE persisting or executing anything. The handler maps it
	// to codes.InvalidArgument.
	//
	// WHY the specific validation sentinels below WRAP it (via %w): a caller can
	// then match EITHER the precise cause (errors.Is(err, ErrCycleDetected) for a
	// targeted message/test) OR the umbrella (errors.Is(err, ErrValidation) for the
	// single codes.InvalidArgument mapping in the handler). One taxonomy, two
	// granularities — the idiomatic Go error-chaining pattern. (We bake the wrap
	// into the sentinel itself with fmt.Errorf at package init so every return site
	// satisfies errors.Is(ErrValidation) without each one having to remember to
	// wrap.)
	ErrValidation = errors.New("pipeline: validation failed")

	// ErrCycleDetected is returned when a pipeline's depends_on edges form a cycle.
	// A DAG is acyclic by definition; a cycle means the topological sort can never
	// schedule all steps (a deadlock). We reject at CreatePipeline, not at run
	// time, so a malformed template never reaches the engine. Wraps ErrValidation.
	ErrCycleDetected = fmt.Errorf("%w: dependency cycle detected", ErrValidation)

	// ErrUnknownStepType is returned when a step's type is not one the engine can
	// execute (the zero/unspecified value, or an out-of-range value). The
	// ExecutorRegistry must be exhaustive, so an unrunnable step is never
	// persisted as runnable. Wraps ErrValidation.
	ErrUnknownStepType = fmt.Errorf("%w: unknown step type", ErrValidation)

	// ErrDanglingDependency is returned when a step's depends_on (or
	// compensation_step_id) references a step id that does not exist in the same
	// pipeline. A dangling edge would either deadlock the scheduler or silently
	// drop a compensation. Wraps ErrValidation.
	ErrDanglingDependency = fmt.Errorf("%w: dependency references unknown step", ErrValidation)

	// ErrDuplicateStepID is returned when two steps in one pipeline share an id.
	// Step ids are the keys for depends_on/compensation pointers; duplicates make
	// those references ambiguous. Wraps ErrValidation.
	ErrDuplicateStepID = fmt.Errorf("%w: duplicate step id", ErrValidation)

	// ErrTooManySteps is returned when a pipeline exceeds MaxStepsPerPipeline.
	// An unbounded graph is a DoS vector (a checkpoint row per step + a topo-sort
	// over the whole set). Wraps ErrValidation.
	ErrTooManySteps = fmt.Errorf("%w: too many steps", ErrValidation)

	// ErrEmptyPipeline is returned when a pipeline has zero steps. A pipeline with
	// no work is almost always a mistake and there is nothing to execute. Wraps
	// ErrValidation.
	ErrEmptyPipeline = fmt.Errorf("%w: must have at least one step", ErrValidation)

	// ErrPipelineNotFound is the BUSINESS translation of the repository's
	// "not found" for a PipelineDefinition (e.g. TriggerExecution on a missing or
	// archived pipeline). The handler maps it to codes.NotFound.
	ErrPipelineNotFound = errors.New("pipeline: pipeline not found")

	// ErrExecutionNotFound is the BUSINESS translation of "not found" for an
	// Execution (GetExecution/CancelExecution on a missing run). Maps to
	// codes.NotFound.
	ErrExecutionNotFound = errors.New("pipeline: execution not found")

	// ErrPipelineArchived is returned by TriggerExecution when the target pipeline
	// has been soft-deleted (archived). An archived template cannot start new runs
	// (its lineage is preserved only for past executions). Maps to
	// codes.FailedPrecondition.
	ErrPipelineArchived = errors.New("pipeline: pipeline is archived and cannot be triggered")

	// ErrExecutionNotCancellable is returned by CancelExecution when the run has
	// already reached a TERMINAL state (COMPLETED/FAILED/CANCELLED). You cannot
	// cancel a run that is already over — the request is a no-op the caller should
	// know about. Maps to codes.FailedPrecondition.
	//
	// INTERVIEW: cancellation is only meaningful for PENDING/RUNNING/COMPENSATING.
	// Returning an explicit error (rather than silently succeeding) tells an
	// automation it raced the run's natural completion.
	ErrExecutionNotCancellable = errors.New("pipeline: execution is already in a terminal state")

	// ErrCompensationFailed is returned by the engine when one or more
	// compensations FAILED during a rollback — the saga is "stuck" and a side
	// effect could not be undone. This is the worst case in any saga and is
	// surfaced loudly (an alert), never swallowed. The Execution still settles to
	// FAILED/CANCELLED, but this error tells the caller a human must reconcile
	// orphaned resources. Maps to codes.Internal (with a sanitized message).
	ErrCompensationFailed = errors.New("pipeline: compensation failed — manual reconciliation required")

	// ErrNoExecutor is returned by the engine when no StepExecutor is registered
	// for a step's type. It indicates a wiring bug (a known StepType with no
	// executor), distinct from ErrUnknownStepType (an invalid type at validation).
	// Maps to codes.Internal.
	ErrNoExecutor = errors.New("pipeline: no executor registered for step type")

	// ErrTerminalPersist is returned by the engine when it COULD NOT durably write
	// a run's TERMINAL state (COMPLETED/FAILED/CANCELLED) after retries. This breaks
	// the write-ahead-durability contract: the engine computed the outcome in memory
	// but the database may still say RUNNING, so the run's durable state is UNKNOWN.
	//
	// WHY surface it instead of swallowing (Finding 2): if we returned a clean
	// terminal status while the row stayed RUNNING, a crash-recovery/leader scan
	// would later see a "stuck RUNNING" saga and either re-run it (double-applying
	// side effects) or strand it. By returning this error the caller/leader knows to
	// RETRY the settle rather than trust an in-memory status that never hit disk.
	// Maps to codes.Internal (the run still needs durable reconciliation).
	ErrTerminalPersist = errors.New("pipeline: terminal state could not be durably persisted")

	// ErrStepPersist is returned by the engine when one or more STEP CHECKPOINT
	// writes (SaveStep) failed during a run. Step checkpoints are the write-ahead
	// records a crash-recovery routine reads to know how far a run got; a dropped
	// checkpoint means recovery could mis-resume (re-run a completed step, or fail
	// to reverse a completed one). We aggregate these per run rather than aborting
	// mid-step (the forward/compensation work may still be worth finishing), but we
	// no longer SWALLOW them — the run's terminal result carries this error so an
	// operator/leader knows the durable step history is incomplete. Maps to
	// codes.Internal.
	ErrStepPersist = errors.New("pipeline: one or more step checkpoints could not be durably persisted")
)
