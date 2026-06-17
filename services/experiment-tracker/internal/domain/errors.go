// errors.go — sentinel errors owned by the Experiment Tracker domain layer.
//
// ============================================================================
// WHY SENTINEL ERRORS (and why two vocabularies: business vs storage)
// ============================================================================
//
// The handler maps these business errors to gRPC status codes. Keeping them as
// package-level sentinels (not ad-hoc fmt.Errorf strings) means callers can use
// errors.Is(err, ErrRunNotFound) to branch on the *fact* without string-matching
// — robust to message wording changes.
//
// Like the Auth service, we keep STORAGE sentinels (returned by repository ports
// in ports.go — ErrRepoNotFound, ErrRepoConflict) distinct from BUSINESS
// sentinels (here). The domain service is the single translation point: it
// catches a storage outcome ("no row") and re-expresses it as the business fact
// the handler understands ("run not found"). This is what lets the handler stay
// blissfully ignorant that a database even exists.
//
// INTERVIEW: "Why not just return the raw DB error up the stack?"
//
//	Because the transport layer (gRPC) would then have to understand pgx error
//	codes — a leak of infrastructure into the API boundary. The translation also
//	lets us return a SAFE, non-leaky message (no SQL, no table names) to clients.
package domain

import "errors"

var (
	// ErrValidation is returned for malformed input the service rejects BEFORE
	// touching any repository (empty run id, batch over the cap, NaN metric
	// value, etc.). The handler maps it to codes.InvalidArgument. Wrapped with
	// %w so callers get a specific human message AND can errors.Is on it.
	ErrValidation = errors.New("experiment: validation failed")

	// ErrExperimentNotFound is returned when an operation targets an experiment
	// that does not exist (or is not visible to the caller's team). Maps to
	// codes.NotFound. It is the business translation of the storage sentinel
	// ErrRepoNotFound (ports.go).
	ErrExperimentNotFound = errors.New("experiment: experiment not found")

	// ErrRunNotFound is returned when an operation targets a run that does not
	// exist. Maps to codes.NotFound. Also the business form of ErrRepoNotFound.
	ErrRunNotFound = errors.New("experiment: run not found")

	// ErrExperimentNameExists is returned by CreateExperiment when the name is
	// already taken within the caller's team. The handler maps it to
	// codes.AlreadyExists. Business translation of the storage ErrRepoConflict
	// raised by a unique (team, name) constraint violation.
	ErrExperimentNameExists = errors.New("experiment: experiment name already exists in team")

	// ErrRunNotRunning is returned when a caller tries to log metrics/params to a
	// run that has already reached a terminal state. A FINISHED/FAILED/KILLED
	// run's metrics are FINAL — appending to them would corrupt the historical
	// record (and silently change a model's reported accuracy after the fact).
	// Maps to codes.FailedPrecondition (the resource is in the wrong state for
	// the operation — distinct from InvalidArgument, which is "your input is
	// malformed regardless of state").
	ErrRunNotRunning = errors.New("experiment: run is not RUNNING (its metrics are final)")

	// ErrInvalidStatusTransition is returned by FinishRun/UpdateRunStatus when
	// the requested target status is illegal for the run's current state — e.g.
	// trying to move a FINISHED run back to RUNNING, or transitioning to a
	// non-terminal target. The run lifecycle is a state machine (see models.go);
	// this guards its edges. Maps to codes.FailedPrecondition.
	ErrInvalidStatusTransition = errors.New("experiment: invalid run status transition")

	// ErrParamConflict is returned by LogParams when a param key is re-logged
	// with a DIFFERENT value than already recorded. Params are write-once per
	// key within a run (a run's configuration must not silently change
	// mid-flight). Re-logging the SAME value is idempotent (no error); only a
	// changed value is a conflict. Maps to codes.FailedPrecondition.
	ErrParamConflict = errors.New("experiment: param already set with a different value")

	// ErrBatchTooLarge is returned by LogMetrics when a single batch exceeds the
	// server-enforced cap (MaxMetricsPerBatch). An unbounded batch is both an
	// OOM lever and a single-transaction-too-big hazard. Maps to
	// codes.InvalidArgument. This is a DoS guard, not a business rule per se,
	// but it lives in the domain because the cap is a domain invariant the
	// handler must not be able to bypass.
	ErrBatchTooLarge = errors.New("experiment: metric batch exceeds the per-call cap")

	// ErrTooManyRuns is returned by CompareRuns when more run ids are requested
	// than MaxCompareRuns. Comparing hundreds of full metric curves at once is a
	// payload/DoS hazard. Maps to codes.InvalidArgument.
	ErrTooManyRuns = errors.New("experiment: too many runs requested for comparison")
)
