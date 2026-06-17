// errors.go — sentinel errors owned by the registry domain layer.
//
// ============================================================================
// TWO VOCABULARIES: STORAGE OUTCOMES vs BUSINESS OUTCOMES
// ============================================================================
//
// Like the Auth service, the registry separates STORAGE sentinels (returned by
// the repository PORTS in ports.go — "no such row", "unique violation") from
// BUSINESS sentinels (below — "model not found", "illegal stage transition").
// The SERVICE is the single translation point: it catches a storage sentinel and
// re-expresses it as the matching business error. The handler then maps the
// business error to a gRPC status code (NotFound, AlreadyExists, FailedPrecondition,
// InvalidArgument). The handler never needs to know storage exists, and the domain
// never imports gRPC's codes package — the mapping table lives in the handler.
//
// All errors are package-level vars wrapped with %w at the throw site where a
// specific message helps, so callers can both read a human message AND
// errors.Is(err, ErrFoo) to branch. This is the errors.Is-friendly contract every
// Forgepoint service follows.
// ============================================================================
package domain

import "errors"

var (
	// ErrValidation is returned for malformed input the service rejects BEFORE
	// touching any port (empty name, empty model id, a target stage that isn't a
	// real stage). The handler maps it to codes.InvalidArgument. Wrapped with %w so
	// the caller gets a specific message while still matching errors.Is.
	ErrValidation = errors.New("registry: validation failed")

	// ErrModelNotFound is returned when a command/query targets a model that does
	// not exist (or is not in the caller's team — we do NOT distinguish, to avoid
	// leaking the existence of other teams' models; see the team-scoping note in
	// the service). The handler maps it to codes.NotFound.
	ErrModelNotFound = errors.New("registry: model not found")

	// ErrVersionNotFound is returned when an operation targets a version id that
	// does not exist. Maps to codes.NotFound.
	ErrVersionNotFound = errors.New("registry: version not found")

	// ErrModelNameTaken is the BUSINESS translation of the write store's unique-
	// constraint sentinel (ErrWriteConflict on Create) for the (team, name) index.
	// RegisterModel returns it when the chosen name already exists in the caller's
	// team. The handler maps it to codes.AlreadyExists. NOTE: the idempotency path
	// short-circuits BEFORE this — a retried RegisterModel with the same
	// idempotency key returns the original model, not this error (Stripe-style).
	ErrModelNameTaken = errors.New("registry: model name already exists in team")

	// ErrVersionExists is returned when CreateVersion is asked to pin an explicit
	// version label that already exists for the model. Maps to codes.AlreadyExists.
	ErrVersionExists = errors.New("registry: version already exists for model")

	// ErrModelArchived is returned when a command tries to mutate a model that has
	// already been soft-deleted (e.g. CreateVersion or PromoteVersion on an archived
	// model). Maps to codes.FailedPrecondition — the request is well-formed but the
	// model's state forbids it.
	ErrModelArchived = errors.New("registry: model is archived")

	// ErrIllegalTransition is returned by PromoteVersion when the requested
	// target stage is not reachable from the version's current stage per
	// ModelStage.CanTransitionTo (e.g. DEV→PRODUCTION skipping STAGING, or any move
	// out of the terminal ARCHIVED). Maps to codes.FailedPrecondition. This is the
	// state-machine guard surfacing as a business error.
	ErrIllegalTransition = errors.New("registry: illegal stage transition")

	// ErrVersionNotReady is returned by PromoteVersion when promoting to a SERVING
	// stage (STAGING/PRODUCTION) a version whose artifact Status is not READY. You
	// cannot route traffic to weights that aren't physically present and verified.
	// Maps to codes.FailedPrecondition.
	ErrVersionNotReady = errors.New("registry: version artifact is not READY")

	// ErrInvalidStatusTransition is returned by MarkVersionReady when the version
	// is not in PENDING_UPLOAD (e.g. confirming an already-READY or FAILED version
	// in a non-idempotent way). The idempotent retry path returns the current state
	// instead; this fires only for a genuinely illegal status move. Maps to
	// codes.FailedPrecondition.
	ErrInvalidStatusTransition = errors.New("registry: invalid version status transition")
)
