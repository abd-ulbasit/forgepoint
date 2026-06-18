// errors.go — sentinel errors owned by the prompt-registry domain.
//
// ============================================================================
// TWO VOCABULARIES: STORAGE OUTCOMES vs BUSINESS OUTCOMES (mirrors the Registry)
// ============================================================================
//
// The repository PORT (ports.go) returns STORAGE sentinels — ErrRecordNotFound
// (no such row) and ErrWriteConflict (a unique-index violation). The SERVICE is
// the single translation point: it catches a storage sentinel and re-expresses it
// as a BUSINESS error below (ErrNotFound, ErrAlreadyExists, ...). The handler then
// maps the business error to a gRPC status code. The handler never needs to know
// storage exists, and this domain never imports gRPC's codes package.
//
// All errors are package-level vars, wrapped with %w at the throw site where a
// specific message helps, so callers can both read a human message AND
// errors.Is(err, ErrFoo) to branch — the errors.Is-friendly contract every
// Forgepoint service follows.
package prompt

import "errors"

var (
	// ---- BUSINESS sentinels (the service returns these; the handler maps them) ----

	// ErrValidation is returned for malformed input rejected BEFORE any I/O (empty
	// name, empty template, a missing render variable under the strict policy). The
	// handler maps it to codes.InvalidArgument. Wrapped with %w so the caller gets a
	// specific message while still matching errors.Is.
	ErrValidation = errors.New("prompt: validation failed")

	// ErrNotFound is returned when a Get/Render targets a prompt that does not exist
	// — OR is not in the caller's team. The domain DELIBERATELY does not distinguish
	// "absent" from "another team's" (it queries WHERE team = $caller, so a cross-
	// tenant prompt is simply not in the result set) — this is the team-isolation
	// guarantee: a caller cannot even learn that another team's prompt exists. Maps
	// to codes.NotFound.
	ErrNotFound = errors.New("prompt: not found")

	// ErrAlreadyExists is the business translation of the write store's unique-
	// constraint sentinel that the idempotency path did NOT absorb. In practice the
	// versioning loop turns a (team,name,version) collision into a RETRY (recompute
	// next version), so a caller almost never sees this; it surfaces only if the
	// retry budget is exhausted under pathological contention. Maps to
	// codes.AlreadyExists.
	ErrAlreadyExists = errors.New("prompt: already exists")

	// ---- STORAGE sentinels (the repository PORT returns these; the service maps) ----

	// ErrRecordNotFound is the STORAGE "no such row" the PromptRepository returns
	// (e.g. a pgx.ErrNoRows translated at the adapter boundary). The service catches
	// it and re-expresses it as the business ErrNotFound. Keeping a SEPARATE storage
	// sentinel means the domain never sees a driver error string and the service owns
	// the storage→business mapping.
	ErrRecordNotFound = errors.New("prompt: record not found")

	// ErrWriteConflict is the STORAGE "you violated a UNIQUE constraint" sentinel the
	// PromptRepository returns on a (team,name,version) or (team,idempotency_key)
	// collision (SQLSTATE 23505, translated at the adapter). The service's version-
	// assignment loop treats it as "lost the version race, recompute and retry"; a
	// persistent conflict becomes the business ErrAlreadyExists.
	ErrWriteConflict = errors.New("prompt: write conflict")
)
