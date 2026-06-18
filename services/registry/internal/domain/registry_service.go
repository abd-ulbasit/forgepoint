// registry_service.go — the RegistryService interface (the primary port).
//
// ============================================================================
// CLEAN ARCHITECTURE: THE SERVICE INTERFACE AS A PORT
// ============================================================================
//
// RegistryService is the use-case contract the handler depends on. The handler
// holds a RegistryService value and calls its methods; it never knows whether the
// implementation is the real registryService (Postgres write + Redis read) or a
// test stub. Defining the interface in the domain (not the handler) is dependency
// inversion: outer layers depend on this abstraction, and we can swap
// implementations without touching the handler.
//
// ============================================================================
// THE COMMAND / QUERY SPLIT IS VISIBLE IN THIS INTERFACE (CQRS at the seam)
// ============================================================================
//
// The methods are grouped into COMMANDS (mutate the write store, then emit a
// projection event) and QUERIES (read the eventually-consistent projection). This
// grouping mirrors the proto's COMMAND/QUERY/STORAGE sections and makes the CQRS
// boundary legible at the type level: a reader can see, from the interface alone,
// which calls change state and which only observe it. If we ever physically split
// read and write into two services, the cut line is already drawn here.
//
// INPUT/OUTPUT TYPES: every command takes a dedicated *Input struct rather than a
// long positional argument list. WHY: adding a field later is backward-compatible,
// and named fields prevent argument-order bugs (swapping two string params). The
// handler builds these from proto requests, stamping the server-authoritative
// Actor (from auth claims) separately from the client-supplied fields — so the
// mass-assignment guard is structural, not just convention.
//
// WHAT'S NOT HERE: the concrete registryService and its constructor
// NewRegistryService(writeStore, readStore, emitter, clock, idgen) live in
// registry_service_impl.go. Keeping the interface separate lets the handler
// scaffold compile against this contract while the impl evolves.
// ============================================================================
package domain

import (
	"context"
	"time"
)

// MaxPageSize is the platform-wide hard cap on a list page (the DoS guard the
// proto documents: default 20, max 100). The service clamps every query's
// PageSize to [1, MaxPageSize] so a store never does unbounded work and a response
// never balloons. Defined here (domain) because it is a BUSINESS rule, not a
// transport detail — the handler also references it when defaulting an unset size.
const MaxPageSize = 100

// DefaultPageSize is the page size used when the caller requests 0 (unset).
const DefaultPageSize = 20

// ----------------------------------------------------------------------------
// COMMAND INPUTS (client-supplied fields only; Actor carries server-auth identity)
// ----------------------------------------------------------------------------

// RegisterModelInput carries ONLY the client-owned fields of RegisterModel. Note
// the deliberate ABSENCE of id/owner/team/timestamps — those are server-derived
// (id/timestamps by the service, owner/team from the Actor). This is the mass-
// assignment guard expressed as a type: a client literally cannot supply an owner.
type RegisterModelInput struct {
	Name           string
	Description    string
	Framework      string
	TaskType       string
	Tags           map[string]string
	IdempotencyKey string // empty = one-shot (still protected by name uniqueness)
}

// CreateVersionInput carries the client-owned fields of CreateVersion. The
// artifact bytes are NOT here — they go to object storage out of band; the version
// lands at Status=PENDING_UPLOAD until MarkVersionReady confirms the upload.
type CreateVersionInput struct {
	ModelID        string             // target model (validated to exist + be in team + active)
	Version        string             // "" = service auto-assigns the next monotonic label
	Description    string             // changelog/notes
	Metrics        map[string]float64 // training/eval metrics
	IdempotencyKey string
}

// MarkVersionReadyInput drives the PENDING_UPLOAD→READY (or →FAILED) transition.
//
// SECURITY (mass-assignment, critical): the caller does NOT supply the artifact
// digest/size/path as authoritative values. Those are MEASURED by the storage
// layer and passed here by the SERVER-side caller of MarkVersionReady (the upload-
// confirmation handler that re-verified the object), never copied from a client
// request body. ExpectedDigest is an OPTIONAL client assertion the storage layer
// already cross-checked; if Success is false the version goes to FAILED and no
// ModelVersionReady event is emitted.
type MarkVersionReadyInput struct {
	VersionID      string
	Success        bool   // true → READY (artifact verified); false → FAILED
	ArtifactPath   string // server-measured location (only meaningful when Success)
	ArtifactDigest string // server-measured digest  (only meaningful when Success)
	SizeBytes      int64  // server-measured size     (only meaningful when Success)
	IdempotencyKey string
}

// PromoteVersionInput requests a stage transition. TargetStage is the ONLY place
// a client influences stage — and even here it is a REQUEST the service validates
// against the state machine and may reject, not a direct write.
type PromoteVersionInput struct {
	VersionID      string
	TargetStage    ModelStage
	IdempotencyKey string
}

// UpdateModelInput mutates the tiny mutable surface (description and/or tags) with
// explicit apply flags so an omitted field never silently clears stored data
// (the proto3-has-no-presence problem solved with update-mask-style booleans).
type UpdateModelInput struct {
	ModelID           string
	Description       string
	UpdateDescription bool // apply Description (even empty) only when true
	Tags              map[string]string
	ReplaceTags       bool // replace the stored tag map (empty clears) only when true
	IdempotencyKey    string
}

// ----------------------------------------------------------------------------
// COMMAND OUTPUTS (results that carry more than the primary entity)
// ----------------------------------------------------------------------------

// PromoteResult is PromoteVersion's output: the promoted version PLUS the version
// that was auto-demoted by the single-production invariant (Demoted.ID == "" when
// there was no prior production version). Returning both lets the caller and audit
// log record the exact atomic swap — and lets the handler populate
// PromoteVersionResponse.demoted_version.
type PromoteResult struct {
	Promoted ModelVersion
	Demoted  ModelVersion // zero-value (ID=="") when nothing was demoted
}

// ----------------------------------------------------------------------------
// QUERY INPUTS (team scoping is applied by the service from the Actor, not here)
// ----------------------------------------------------------------------------

// ListModelsInput is a query's filter + pagination. Team is intentionally absent —
// the service scopes to the Actor's team. The filter fields only narrow within it.
type ListModelsInput struct {
	Filter    ListModelsFilter
	PageSize  int
	PageToken string
}

// Page is the generic paginated result the queries return. NextToken == "" means
// the result set is exhausted. WHY a generic over the concrete []Model/[]ModelVersion:
// it keeps one consistent paginated shape across queries (the handler maps it to
// the proto PaginationResponse identically every time).
//
// Total is the COUNT of the full filtered result set (NOT just this page) — the value
// the proto's PaginationResponse.total_count carries and the BFF dashboard reads to
// render "N models". It is maintained on the read side (see ReadStore.CountModels) so
// the dashboard's cheap page_size=1 call still gets an accurate total without pulling
// every row. A query that does not compute a total (e.g. ListVersions, where no caller
// needs it yet) leaves it 0 — the documented "total unknown" sentinel.
type Page[T any] struct {
	Items     []T
	NextToken string
	Total     int
}

// ============================================================================
// RegistryService — the use-case contract.
// ============================================================================
//
// Every method takes the server-authoritative Actor as a separate parameter from
// the input. WHY split them: the Actor comes from validated auth claims and is the
// trust boundary; the Input comes (mostly) from the request body. Keeping them
// distinct parameters makes it impossible to accidentally let a request field
// populate an owner/team — the structural mass-assignment defense.
type RegistryService interface {
	// ----- COMMANDS (write store → emit projection event) -----

	// RegisterModel creates a new model (identity only, no versions). Stamps
	// id/owner/team/timestamps server-side (owner/team from actor), persists to the
	// write store, and emits EventModelRegistered. Idempotent via the input key.
	// Returns ErrModelNameTaken if the (team,name) is taken and no idempotency key
	// short-circuits it; ErrValidation on bad input.
	RegisterModel(ctx context.Context, actor Actor, in RegisterModelInput) (Model, error)

	// UpdateModel mutates description/tags using the explicit apply flags. Stamps
	// UpdatedAt, persists, and refreshes the projection (NO canonical lifecycle
	// event — description/tag edits are not a published platform fact). Returns
	// ErrModelNotFound / ErrModelArchived / ErrValidation as appropriate.
	UpdateModel(ctx context.Context, actor Actor, in UpdateModelInput) (Model, error)

	// CreateVersion cuts a new immutable version of an existing model. Validates
	// the target model (exists, in team, not archived), auto-assigns the next
	// version label when unset, stamps Stage=DEV/Status=PENDING_UPLOAD/CreatedBy,
	// persists, and emits EventVersionCreated. Idempotent. Returns ErrModelNotFound,
	// ErrModelArchived, ErrVersionExists (pinned label collision), ErrValidation.
	CreateVersion(ctx context.Context, actor Actor, in CreateVersionInput) (ModelVersion, error)

	// MarkVersionReady drives PENDING_UPLOAD→READY (Success) or →FAILED (!Success),
	// recording server-measured artifact facts on success and emitting
	// EventVersionReady ONLY on success. Idempotent (a repeat returns current state).
	// Returns ErrVersionNotFound, ErrInvalidStatusTransition.
	MarkVersionReady(ctx context.Context, actor Actor, in MarkVersionReadyInput) (ModelVersion, error)

	// PromoteVersion advances a version through the stage state machine, enforcing
	// legal transitions, the READY precondition for serving stages, and the SINGLE-
	// PRODUCTION invariant (atomic demotion of the prior prod version). Persists the
	// swap in one transaction and emits EventModelPromoted. Idempotent (promoting to
	// the current stage returns the current state, no event). Returns
	// ErrVersionNotFound, ErrIllegalTransition, ErrVersionNotReady.
	PromoteVersion(ctx context.Context, actor Actor, in PromoteVersionInput) (PromoteResult, error)

	// ArchiveModel soft-deletes a model and archives all its versions in one
	// transaction, then emits EventModelArchived. Idempotent (archiving an already-
	// archived model returns it without a second event). Returns ErrModelNotFound.
	ArchiveModel(ctx context.Context, actor Actor, modelID, idempotencyKey string) (Model, error)

	// ----- QUERIES (read projection; EVENTUALLY CONSISTENT) -----

	// GetModel returns a single model from the read projection by id (preferred) or
	// name, team-scoped to the actor. May briefly miss a just-registered model
	// (projection lag) → ErrModelNotFound. Provide exactly one of id/name.
	GetModel(ctx context.Context, actor Actor, id, name string) (Model, error)

	// ListModels returns a team-scoped, newest-first page from the projection, with
	// page size clamped to [1, MaxPageSize].
	ListModels(ctx context.Context, actor Actor, in ListModelsInput) (Page[Model], error)

	// GetVersion returns a single version from the projection, by id (preferred) or
	// (modelID, version label).
	GetVersion(ctx context.Context, actor Actor, id, modelID, version string) (ModelVersion, error)

	// ListVersions returns a model's versions newest-first from the projection,
	// optionally filtered to one stage (StageUnspecified = any). Page size clamped.
	ListVersions(ctx context.Context, actor Actor, modelID string, stageFilter ModelStage, pageSize int, pageToken string) (Page[ModelVersion], error)
}

// realClock is the production Clock: it delegates to time.Now(). Defined here
// (not the impl) so the constructor's default wiring is co-located with the port
// it satisfies. Tests inject a fixed-time fake instead.
type realClock struct{}

// Now returns the current wall-clock time.
func (realClock) Now() time.Time { return time.Now() }

// NewRealClock returns the production Clock. main.go passes this to
// NewRegistryService; tests pass a deterministic fake.
func NewRealClock() Clock { return realClock{} }
