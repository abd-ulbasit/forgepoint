// ports.go — the PORTS (data-access interfaces) the Feature Store domain depends
// on, defined IN the domain per Hexagonal Architecture ("the consumer owns the
// port").
//
// ============================================================================
// WHY THE PORTS LIVE HERE (not in internal/repository) — the import-cycle lesson
// ============================================================================
//
// The service impl (featurestore_service_impl.go) lives in `domain` and must
// reference these interfaces. If the interfaces lived in `repository`, then
// `repository` would import `domain` (the port signatures reference domain types
// like FeatureEvent), and `domain` would import `repository` (the impl needs the
// ports) — a domain → repository → domain CYCLE that Go rejects at compile time.
// The Hexagonal fix is "the CONSUMER defines the interface it needs": the domain
// consumes persistence, so the ports belong in the domain. The Postgres/Redis
// ADAPTERS (a later phase) import `domain` and implement these — one inward arrow,
// no cycle. (Auth learned this the hard way; see services/auth/internal/domain/ports.go.)
//
// ============================================================================
// THE EVENT-SOURCING PORTS (what the write/read models need from infrastructure)
// ============================================================================
//
// The domain is written against THREE ports, mirroring the event-sourcing
// topology (one write model, two read models) — and a CLOCK + IDGEN so the pure
// domain stays deterministic in tests:
//
//	EventLog          — the append-only WRITE MODEL (the source of truth).
//	OnlineViewStore   — the Redis "latest" READ MODEL (low-latency hot path).
//	OfflineViewStore  — the Postgres "as-of" READ MODEL (point-in-time/catalog).
//
// WHY split the read side into two ports rather than one: the online and offline
// projections have genuinely different access patterns and backing stores (Redis
// key→hash vs Postgres scan), different consistency (online is eventually
// consistent, rebuilt on flush; offline is durable), and RebuildViews can target
// one without the other. Two ports keep those adapters independent. The write
// side is a SINGLE EventLog because there is exactly one log and it is the only
// thing that is authoritative — everything else is derived from it.
//
// IMPORTANT — these ports are the LATER LAYER'S contract. This task delivers the
// domain (pure business logic + the fold + the service impl that orchestrates
// these ports) and TDD against in-memory mocks. The real Postgres EventLog +
// Postgres/Redis view stores are a subsequent phase that implements exactly these
// interfaces. The teaching comments below specify the contract that phase fulfills.
package domain

import (
	"context"
	"errors"
	"time"
)

// ============================================================================
// STORAGE SENTINELS (port-level outcomes the service translates to business errors)
// ============================================================================

// ErrEventLogNotFound is the STORAGE sentinel an EventLog returns when no events
// exist for a requested view id. The service translates it to the BUSINESS error
// ErrViewNotFound. WHY distinct: "no rows in the log" (a storage fact) and "that
// feature view does not exist" (a business fact) are different vocabularies; the
// service is the single translation point so the handler never imports storage.
var ErrEventLogNotFound = errors.New("featurestore: event log empty for view")

// ============================================================================
// EventLog — the APPEND-ONLY WRITE MODEL (source of truth)
// ============================================================================
//
// This is the heart of event sourcing. The ONLY mutating operation is Append;
// there is deliberately NO Update and NO Delete on individual events — the log is
// immutable. "Deleting" a view is itself an Append (a FeatureEventViewDeleted
// event). Everything the service reads on the rebuild/definition path comes from
// Load/LoadView, which REPLAY the log.
type EventLog interface {
	// Append atomically writes a batch of events as one logical, ordered unit and
	// assigns each a monotonic, gap-free Version (the port reserves the next
	// versions under its write lock / a Postgres sequence + SELECT … FOR UPDATE).
	// It returns the events with their assigned Version + ID populated, and the
	// highest version written (writtenThrough) for read-your-writes.
	//
	// IDEMPOTENCY (exactly-once EFFECT under at-least-once delivery): a retried
	// command (lost response) must not append duplicate events. The adapter records
	// the idempotencyKey; a repeat with the SAME key is a no-op that returns the
	// ORIGINAL events/version (replayed=true). The service relies on this so a
	// network retry of WriteFeatures/DefineFeatureView/DeleteFeatureView does not
	// corrupt counts or history. An empty idempotencyKey disables dedup (callers
	// SHOULD always supply one for mutations).
	Append(ctx context.Context, idempotencyKey string, events []FeatureEvent) (appended []FeatureEvent, writtenThrough int64, replayed bool, err error)

	// Load returns ALL events for a view id in ascending Version order (the full
	// replay stream) — used by RebuildViews and by point-in-time/historical reads
	// when the offline store is being recomputed. Returns ErrEventLogNotFound if
	// the view has no events at all. For very large logs the adapter may stream/
	// page internally; the domain treats it as the ordered event slice.
	Load(ctx context.Context, featureViewID string) ([]FeatureEvent, error)

	// LoadView returns only the DEFINITION/DELETION events for a view (the subset
	// foldView needs to reconstruct the FeatureView), avoiding loading millions of
	// value events just to read a schema. Returns ErrEventLogNotFound if the view
	// was never defined.
	LoadView(ctx context.Context, featureViewID string) ([]FeatureEvent, error)

	// ResolveViewIDByName resolves a (team, name) to a view id, scoped to the team
	// for authorization (a caller cannot resolve another team's view name). Returns
	// ErrEventLogNotFound if no such view exists for the team. Used by
	// DefineFeatureView (to find an existing view to evolve) and GetFeatureView(name).
	ResolveViewIDByName(ctx context.Context, team, name string) (string, error)

	// ListViewIDs returns the view ids defined for a team, optionally filtered by a
	// case-insensitive name substring, paginated by an opaque cursor. The catalog
	// projection. nextToken is empty when the last page is reached.
	ListViewIDs(ctx context.Context, team, nameFilter string, opts ListOptions) (ids []string, nextToken string, err error)
}

// ============================================================================
// OnlineViewStore — the REDIS "latest" READ MODEL (eventually consistent cache)
// ============================================================================
//
// Updated just AFTER an Append (apply the new value events to the cache). It is a
// CACHE rebuildable from the log (RebuildViews online target replays ProjectLatest
// and Puts the result). The hot inference path reads it via GetLatest. Eventual
// consistency is the explicit tradeoff: a GetLatest right after a write may miss
// it until the projection catches up — surfaced via FeatureVector.AsOfVersion.
type OnlineViewStore interface {
	// GetLatest fetches the latest projected vectors for a set of entities in one
	// round-trip (batched to amortize the hot path). Entities with no online value
	// are simply absent from the returned map; the service reports them as missing.
	GetLatest(ctx context.Context, featureViewID string, entityIDs []string) (map[string]FeatureVector, error)

	// Put upserts the latest vectors for entities (called after Append with the new
	// values, and by RebuildViews with the full ProjectLatest result). Upsert (not
	// insert) because re-applying an event during a rebuild must be idempotent.
	Put(ctx context.Context, featureViewID string, vectors map[string]FeatureVector) error

	// Purge tears down the entire online projection for a view — used by
	// DeleteFeatureView (stop serving a retired view) and at the start of a
	// RebuildViews(online) so the rebuild starts clean.
	Purge(ctx context.Context, featureViewID string) error
}

// ============================================================================
// OfflineViewStore — the POSTGRES "as-of" READ MODEL (durable, point-in-time)
// ============================================================================
//
// Backs GetHistoricalFeatures. The adapter may compute the as-of projection in
// SQL (DISTINCT ON (entity_id) … WHERE event_time ≤ asOf ORDER BY entity_id,
// version DESC) for scale — but it MUST agree with the domain's ProjectAsOf fold,
// which is the reference semantics. RebuildViews(offline) recomputes it from the
// log. Exposed as a port (not done purely in-domain) because a real point-in-time
// scan over a large log belongs in the database, not in process memory.
type OfflineViewStore interface {
	// GetAsOf returns the point-in-time vectors for entities as of a timestamp,
	// paginated over the entity result set. Entities with no event at/before asOf
	// are absent from the map; the service reports them as missing. nextToken is
	// empty on the last page.
	GetAsOf(ctx context.Context, featureViewID string, entityIDs []string, asOf time.Time, opts ListOptions) (vectors map[string]FeatureVector, nextToken string, err error)

	// Rebuild replaces the offline projection for a view with a freshly-computed
	// one (from a replay of the log). Called by RebuildViews(offline). Idempotent.
	Rebuild(ctx context.Context, featureViewID string, vectors map[string]FeatureVector) error
}

// ============================================================================
// Clock & IDGenerator — determinism seams for the pure domain
// ============================================================================
//
// WHY inject these rather than call time.Now()/uuid.New() directly in the service:
// the service assigns SERVER-AUTHORITATIVE timestamps and ids (created_at,
// appended_at, view id, event id). If those came from real wall-clock/random
// inside the impl, tests could not assert on them deterministically. Injecting a
// Clock and IDGenerator lets tests pass a fixed clock and a counting id generator
// and assert exact values — while production wires time.Now and uuid.NewString.
// This is the standard "inject the impure edges, keep the core pure" technique.
type Clock interface {
	// Now returns the current time. Production: time.Now(). Tests: a fixed instant.
	Now() time.Time
}

// IDGenerator mints unique ids (UUID v4 in production). Injected for deterministic
// tests. SECURITY: production uses crypto-strong UUIDs (google/uuid is crypto/rand
// backed) so ids are unguessable — important because ids appear in URLs/logs and a
// guessable id is an enumeration handle.
type IDGenerator interface {
	NewID() string
}

// ============================================================================
// ListOptions — cursor-based pagination (uniform across the platform)
// ============================================================================
//
// Cursor (token) pagination, not LIMIT/OFFSET: a cursor is stable under
// concurrent appends (OFFSET can skip or duplicate rows when the underlying set
// shifts between pages). PageSize 0 means "use DefaultPageSize"; the service
// CLAMPS it to MaxPageSize (a too-large page is bounded silently — contrast the
// batch caps, which are rejected).
type ListOptions struct {
	PageSize  int    // 0 ⇒ DefaultPageSize; clamped to MaxPageSize
	PageToken string // opaque cursor; empty ⇒ first page
}
