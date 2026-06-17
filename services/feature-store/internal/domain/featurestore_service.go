// featurestore_service.go defines the FeatureStoreService interface — the primary
// PORT through which the gRPC handler reaches the business logic.
//
// ============================================================================
// THE SERVICE INTERFACE AS A "PORT" (Clean / Hexagonal Architecture)
// ============================================================================
//
// The handler depends on this INTERFACE, not the concrete impl. That gives
// dependency inversion (swap the real event-sourced impl for a test double
// without touching the handler) and a clean anti-corruption boundary: the handler
// converts proto↔domain and calls these methods; the domain never sees proto.
//
// INPUT/OUTPUT TYPES: every method takes/returns DOMAIN types (FeatureView,
// FeatureVector, FeatureRow, …), never proto. The dedicated input structs (rather
// than long positional parameter lists) make adding a field backward-compatible
// and prevent positional-argument mistakes (Auth uses the same convention).
//
// CALLER CONTEXT — the Principal:
//
//	Several methods need the authenticated caller's identity (owner_user_id) and
//	team (owner_team) to set SERVER-AUTHORITATIVE fields and to scope reads.
//	Those come from the auth interceptor's TokenClaims, NOT from request fields
//	(mass-assignment guard). The handler extracts them and passes a Principal —
//	the domain stays unaware of how identity was established (JWT vs API key).
package domain

import (
	"context"
	"time"
)

// Principal is the authenticated caller context the handler extracts from the
// auth interceptor's TokenClaims and passes into the service. It is the ONLY
// trusted source of owner identity + team — the domain uses it to stamp
// server-authoritative fields and to scope every read/write to the caller's team.
//
// WHY a domain type (not the auth package's TokenClaims): the domain must not
// import another service's types. The handler maps TokenClaims → Principal at the
// edge. Scopes is included so admin-gated operations (RebuildViews) can be checked
// in one place if the handler delegates the scope check; the handler may also
// enforce it via the interceptor — both are documented at the call sites.
type Principal struct {
	UserID string   // owner_user_id to stamp on defined views
	Team   string   // owner_team — the authorization/scoping boundary
	Scopes []string // credential scopes (e.g. "features:admin"); enforced for RebuildViews
}

// ----------------------------------------------------------------------------
// COMMAND / QUERY INPUT TYPES
// ----------------------------------------------------------------------------

// DefineFeatureViewInput is the validated input for DefineFeatureView (the
// schema-shaping fields only). Owner/team/version/timestamps/id are NOT here —
// they are server-assigned from the Principal/clock/sequence (mass-assignment guard).
type DefineFeatureViewInput struct {
	Name           string
	Description    string
	Entity         Entity
	Features       []FeatureSpec
	IdempotencyKey string // dedup a retried define (UUID v4 per logical attempt)
}

// WriteFeaturesInput is the validated input for WriteFeatures: the target view +
// the batch of rows to append + the idempotency key. event_time inside each row
// is caller-owned domain data (the producer knows when values became true); all
// other authority (version, append time) is server-assigned.
type WriteFeaturesInput struct {
	FeatureViewID  string
	Rows           []FeatureRow
	IdempotencyKey string
}

// WriteFeaturesResult confirms an append: how many rows landed and the highest
// log version assigned (so a producer can wait for the online projection to catch
// up to WrittenThroughVersion — read-your-writes against the eventually-consistent
// online view).
type WriteFeaturesResult struct {
	WrittenCount          int
	WrittenThroughVersion int64
	// AffectedEntityIDs are the entity ids touched by this batch — handed to the
	// event publisher (FeaturesWritten) so the Model Monitor can scope drift checks
	// to the changed entities. MAY be truncated for huge batches; WrittenCount is
	// authoritative.
	AffectedEntityIDs []string
}

// GetOnlineFeaturesInput targets the online "latest" projection for a set of
// entities, with an optional feature-name projection (return only these columns).
type GetOnlineFeaturesInput struct {
	FeatureViewID string
	EntityIDs     []string
	FeatureNames  []string // empty ⇒ all features in the view
}

// GetHistoricalFeaturesInput targets the offline "as-of" projection. AsOf is
// REQUIRED (a missing cutoff means "now", which is non-reproducible — rejected
// with ErrAsOfRequired).
type GetHistoricalFeaturesInput struct {
	FeatureViewID string
	EntityIDs     []string
	AsOf          time.Time
	FeatureNames  []string
	Pagination    ListOptions
}

// FeatureVectorPage is the paginated result of online/historical reads: the found
// vectors, the entity ids that had no value (returned explicitly so the caller can
// default/skip them rather than guess), and a cursor for the next page.
type FeatureVectorPage struct {
	Vectors          []FeatureVector
	MissingEntityIDs []string
	NextPageToken    string
}

// RebuildProgress is one frame of a RebuildViews replay, pushed to the caller via
// the emit callback (the domain stays transport-agnostic — the handler turns each
// frame into a gRPC stream Send). Mirrors the proto RebuildViewsResponse.
type RebuildProgress struct {
	EventsReplayed       int64
	TotalEvents          int64
	CurrentFeatureViewID string
	Done                 bool
}

// RebuildTarget selects which projection(s) RebuildViews regenerates. A pure
// domain enum (the handler maps the proto RebuildTarget at the edge).
type RebuildTarget int

const (
	// RebuildTargetAll is the zero value and the default: rebuild both projections.
	// (The proto's _UNSPECIFIED also means "all" — we collapse them here.)
	RebuildTargetAll RebuildTarget = iota
	RebuildTargetOnline
	RebuildTargetOffline
)

// RebuildViewsInput drives a replay-based recovery. Empty FeatureViewID = rebuild
// every view (full recovery); a specific id scopes the replay to one view (a cheap
// targeted bugfix replay).
type RebuildViewsInput struct {
	FeatureViewID string // empty ⇒ all views
	Target        RebuildTarget
}

// ----------------------------------------------------------------------------
// THE SERVICE INTERFACE
// ----------------------------------------------------------------------------

// FeatureStoreService is the primary domain interface for the Feature Store. The
// handler holds this interface; the concrete event-sourced impl
// (featurestore_service_impl.go) implements it; tests inject mocks of the PORTS
// it depends on (EventLog, view stores, Clock, IDGenerator).
//
// SECURITY CONTRACT honored by the impl across ALL methods:
//   - Owner identity + team come from Principal (TokenClaims), never request fields.
//   - Every read/write is TEAM-SCOPED: a cross-team view id returns ErrViewNotFound
//     (NOT a permission error) so a caller cannot probe which ids exist elsewhere.
//   - Batch caps (MaxBatchSize) are REJECTED; page sizes (MaxPageSize) are CLAMPED.
//   - Mutations are idempotent via the idempotency key (exactly-once EFFECT).
type FeatureStoreService interface {
	// DefineFeatureView creates a view or evolves its schema by APPENDING a
	// FeatureEventViewDefined event (never an UPDATE) and bumping SchemaVersion.
	// First call for a name creates (version 1); a later call with the same name
	// and a changed schema evolves (version +1). The Entity is identity: supplying
	// a different entity for an existing name is ErrViewNameConflict. Server-
	// assigns id/owner/team/version/timestamps from the Principal/clock. Idempotent.
	DefineFeatureView(ctx context.Context, p Principal, in DefineFeatureViewInput) (FeatureView, error)

	// GetFeatureView returns a single view's current schema/metadata (a fold of its
	// definition events), team-scoped. Exactly one handle is supplied — id (stable,
	// preferred for machines) or name (resolved within the team). A deleted view is
	// still returned (with DeletedAt set) so callers can see the terminal state.
	GetFeatureViewByID(ctx context.Context, p Principal, id string) (FeatureView, error)
	GetFeatureViewByName(ctx context.Context, p Principal, name string) (FeatureView, error)

	// ListFeatureViews pages the team's catalog (a projection over definition
	// events), optionally filtered by a name substring. page_size defaults to 20,
	// clamped to 100.
	ListFeatureViews(ctx context.Context, p Principal, nameFilter string, opts ListOptions) (views []FeatureView, nextToken string, err error)

	// WriteFeatures appends a batch of feature rows to the log (the core event-
	// sourcing write). Each row is validated against the view schema (name + type +
	// dimension); ANY mismatch fails the WHOLE batch (ErrSchemaViolation) so a
	// partially-corrupt write can't slip in. Refuses a deleted view (ErrViewDeleted)
	// and an over-cap batch (ErrBatchTooLarge). On success it updates the online
	// projection and returns the assigned version range. Idempotent.
	WriteFeatures(ctx context.Context, p Principal, in WriteFeaturesInput) (WriteFeaturesResult, error)

	// GetOnlineFeatures returns the LATEST values for entities from the online
	// projection (the inference hot path). Eventually consistent with writes; each
	// vector carries AsOfVersion for staleness detection. entity_ids capped at 1000.
	GetOnlineFeatures(ctx context.Context, p Principal, in GetOnlineFeaturesInput) (FeatureVectorPage, error)

	// GetHistoricalFeatures returns POINT-IN-TIME values: for each entity, the
	// latest event with event_time ≤ as_of. Powers reproducible training / prevents
	// label leakage. as_of is REQUIRED. entity_ids capped at 1000; result paginated.
	GetHistoricalFeatures(ctx context.Context, p Principal, in GetHistoricalFeaturesInput) (FeatureVectorPage, error)

	// DeleteFeatureView soft-retires a view by appending a TERMINAL
	// FeatureEventViewDeleted event (history preserved for reproducibility; online
	// projection torn down). Idempotent: re-deleting returns the same terminal view.
	DeleteFeatureView(ctx context.Context, p Principal, featureViewID, idempotencyKey string) (FeatureView, error)

	// RebuildViews REPLAYS the log to regenerate the materialized projections — the
	// event-sourcing recovery path (after a Redis flush, a projection bugfix, or a
	// read-model migration). The log is never touched; only the caches are rebuilt
	// FROM it, proving the log is the source of truth. ADMIN-gated ("features:admin").
	// Progress frames are pushed via emit (the handler streams them); emit returning
	// an error (client disconnected) aborts the rebuild.
	RebuildViews(ctx context.Context, p Principal, in RebuildViewsInput, emit func(RebuildProgress) error) error
}
