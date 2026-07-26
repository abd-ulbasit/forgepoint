// Package domain is the innermost ring of the Feature Store's Clean Architecture
// onion. It holds the canonical business types AND the EVENT-SOURCING ENGINE:
// the immutable event types, and the pure FOLD that projects an event log into
// the materialized views (online "latest" and offline "as-of").
//
// ============================================================================
// CLEAN ARCHITECTURE — DOMAIN LAYER (zero framework imports)
// ============================================================================
//
// This package imports ONLY the standard library + github.com/google/uuid. NO
// gRPC, NO NATS, NO database driver, NO generated proto. That is verified in CI:
//
//	go list -deps ./internal/domain/ | grep -E "grpc|nats|database/sql|gen/go|protobuf"
//
// must be EMPTY. The handler (proto↔domain) and the Postgres/Redis adapters
// (port↔rows) live one ring out and depend INWARD on these types; the domain
// never knows they exist. This is what makes the event-sourcing fold below unit-
// testable with a hand-written in-memory log and no infrastructure at all.
//
// ============================================================================
// PATTERN — EVENT SOURCING (the teaching centerpiece of this service)
// ============================================================================
//
// The core inversion: we do NOT store current state and mutate it in place.
// Every change is an immutable FeatureEvent appended to an append-only log. The
// "current value" of any feature is a DERIVED PROJECTION — the result of FOLDING
// (replaying) the events. State is a CACHE of the log, never the source of truth.
//
//	commands ──► append-only EVENT LOG (the TRUTH) ──fold──► VIEWS (caches)
//	                                                          ├─ online: latest
//	                                                          └─ offline: as-of T
//
// The fold lives HERE, in the pure domain (ProjectLatest / ProjectAsOf below),
// for three reasons:
//
//  1. REPLAYABILITY / DETERMINISM. A projection is a pure function of the log:
//     same events in the same order ⇒ same view, every time, with no I/O. That is
//     exactly what makes RebuildViews (replay the whole log to regenerate Redis +
//     Postgres) a documented recovery path rather than a prayer. The Postgres
//     adapter can ALSO compute the offline projection in SQL (DISTINCT ON … ORDER
//     BY version DESC) for scale — but the domain fold is the REFERENCE semantics
//     those SQL projections must match, and the thing our tests pin.
//
//  2. POINT-IN-TIME CORRECTNESS. ProjectAsOf(T) reconstructs the view as it stood
//     at event_time ≤ T — the reproducibility workhorse that lets a training job
//     assemble the exact feature snapshot that existed when labels were observed
//     (no label leakage). With an UPDATE-in-place table this is impossible;
//     history is gone. With the log it is a pure filter+fold.
//
//  3. TESTABILITY. Because the fold is pure, "append then project" and "point-in-
//     time reconstruction" are ordinary table tests over in-memory slices.
//
// TRADEOFFS:
//   - STORAGE grows forever (the log is never compacted in place). Mitigation =
//     snapshots (periodic checkpoints so replay starts mid-log) + retention on
//     very old versions. Snapshots are a read-side optimization, NOT part of the
//     domain contract — the fold is defined over the full log; a snapshot is just
//     a precomputed prefix the caller may pass as the starting accumulator.
//   - COMPLEXITY: two read models to keep consistent with the write model, and
//     EVENTUAL CONSISTENCY on the online side (a read right after a write may miss
//     it). We surface that with AsOfVersion on every projected row so a caller can
//     detect staleness / do read-your-writes against WrittenThroughVersion.
//   - vs CRUD UPDATE: simplest, but destroys history → no reproducibility.
//     Disqualifying for a feature store (reproducibility IS the product).
//   - vs CDC/temporal tables: you keep history, but STATE is still primary and
//     events are a side effect; harder to publish typed domain events and to
//     rebuild arbitrary projections. We want events as first-class.
package domain

import (
	"sort"
	"time"
)

// ============================================================================
// SERVER-ENFORCED LIMITS (named constants ARE the contract — see the proto)
// ============================================================================
//
// proto3 has no native numeric bounds, so the caps the .proto documents are
// enforced HERE and named so the value is the single source of truth. The
// handler reads these; tests assert against them.
const (
	// MaxBatchSize caps WriteFeatures rows and GetOnline/GetHistorical entity_ids.
	// Over-cap requests are REJECTED (ErrBatchTooLarge), never truncated — see the
	// errors.go rationale (dropping rows/entities corrupts results).
	MaxBatchSize = 1000

	// DefaultPageSize / MaxPageSize bound the catalog + historical result pages.
	// Page sizes are CLAMPED (not rejected): a too-large page is harmless to bound
	// silently, unlike a too-large batch. An unset/zero page size becomes Default.
	DefaultPageSize = 20
	MaxPageSize     = 100
)

// ============================================================================
// FeatureValueType — the declared type of one feature in a view's schema
// ============================================================================
//
// WHY an enum (mirroring the proto FeatureValueType, but a PURE domain copy, not
// the generated type): the schema declares the expected type of each feature so
// the server can VALIDATE every written value against it. Catching type drift at
// the write boundary is what stops a string silently landing where a DOUBLE is
// declared and poisoning training data. We keep a domain-native enum (rather than
// importing the proto enum) to honor the zero-framework-imports rule; the handler
// maps proto↔domain at the edge.
type FeatureValueType int

const (
	// FeatureTypeUnspecified is the zero value — an INVALID schema declaration.
	// DefineFeatureView rejects a spec with this type (a feature with no declared
	// type cannot be validated). Having the zero value be "invalid" makes a
	// forgotten field fail loudly instead of silently accepting anything.
	FeatureTypeUnspecified FeatureValueType = iota
	FeatureTypeInt64                        // 64-bit signed integer
	FeatureTypeDouble                       // IEEE-754 double (most numeric features)
	FeatureTypeString                       // UTF-8 string
	FeatureTypeBool                         // boolean flag
	FeatureTypeTimestamp                    // wall-clock time feature
	FeatureTypeDoubleList                   // []float64 — the canonical embedding shape
	FeatureTypeStruct                       // arbitrary nested structure (escape hatch)
)

// String makes the enum log-friendly and gives test failures readable output.
func (t FeatureValueType) String() string {
	switch t {
	case FeatureTypeInt64:
		return "INT64"
	case FeatureTypeDouble:
		return "DOUBLE"
	case FeatureTypeString:
		return "STRING"
	case FeatureTypeBool:
		return "BOOL"
	case FeatureTypeTimestamp:
		return "TIMESTAMP"
	case FeatureTypeDoubleList:
		return "DOUBLE_LIST"
	case FeatureTypeStruct:
		return "STRUCT"
	default:
		return "UNSPECIFIED"
	}
}

// ============================================================================
// Entity — the THING features describe and the KEY they are looked up by
// ============================================================================
//
// e.g. "user" (join_key "user_id"), "merchant", "device". Every FeatureView is
// keyed by exactly one Entity; naming the join key is what lets multiple views
// compose at serving time (a model joins user_credit and user_activity on the
// shared `user` entity). Mirrors Feast's first-class Entity concept.
type Entity struct {
	Name        string // stable name within the team: "user", "merchant"
	JoinKey     string // logical join column: "user_id"
	Description string
}

// ============================================================================
// FeatureSpec — one feature's schema (name + type + optional shape)
// ============================================================================
//
// A FeatureView holds a list of these; the server validates every WriteFeatures
// payload against them. Dimension constrains DOUBLE_LIST length (e.g. 768 for an
// embedding); 0 = unconstrained.
type FeatureSpec struct {
	Name        string
	ValueType   FeatureValueType
	Description string
	Dimension   int // for DOUBLE_LIST: required vector length; 0 = unconstrained
}

// ============================================================================
// FeatureView — a NAMED, VERSIONED schema (a set of FeatureSpecs) over an Entity
// ============================================================================
//
// This is the unit of definition and serving (the design doc's "feature set").
// It is itself a PROJECTION: a FeatureView value is what you get by folding the
// FeatureViewDefined / FeatureViewDeleted events for one name. SchemaVersion
// increments on each definition event so historical reads can reason about which
// schema was in effect.
//
// SECURITY — SERVER-AUTHORITATIVE FIELDS: ID, OwnerUserID, OwnerTeam,
// SchemaVersion, CreatedAt, UpdatedAt, DeletedAt are all set by the SERVER from
// the authenticated principal / clock / sequence — NEVER from client input
// (mass-assignment guard). The domain constructs them; the handler never copies
// them off the request.
type FeatureView struct {
	ID            string
	Name          string
	Description   string
	Entity        Entity
	Features      []FeatureSpec
	SchemaVersion int64
	OwnerUserID   string
	OwnerTeam     string
	CreatedAt     time.Time
	UpdatedAt     time.Time
	// DeletedAt is the soft-delete marker. Zero while live; set to the retirement
	// time when a FeatureViewDeleted event is the latest definition. Presence =
	// deleted (a timestamp, not a bool, so we record WHEN — audit — and the view's
	// history stays queryable; only mutation + online serving are stopped).
	DeletedAt time.Time
}

// IsDeleted reports whether the view has been soft-retired. A pure predicate so
// the service impl reads as business prose (if view.IsDeleted() { return ErrViewDeleted }).
func (v FeatureView) IsDeleted() bool { return !v.DeletedAt.IsZero() }

// specByName returns the FeatureSpec for a feature name and whether it exists.
// Used by write-time schema validation. Linear scan is fine: a view has a handful
// of features, and building a map per validation would cost more than it saves.
func (v FeatureView) specByName(name string) (FeatureSpec, bool) {
	for _, s := range v.Features {
		if s.Name == name {
			return s, true
		}
	}
	return FeatureSpec{}, false
}

// ============================================================================
// FeatureValue — one feature's value, a tagged union over the supported types
// ============================================================================
//
// WHY a tagged union (a Kind + per-type fields) rather than `any`:
//   - It is SELF-DESCRIBING: validation compares Kind against the declared
//     FeatureValueType without reflection or type-switching on interface{}.
//   - It keeps the domain free of the proto google.protobuf.Value wire type (the
//     handler maps Value↔FeatureValue at the edge), honoring zero-framework-imports.
//   - It is comparable/printable in tests, so projection assertions read cleanly.
//
// Exactly one field is meaningful per Kind; the others are zero. This mirrors a
// proto `oneof` / a Rust enum, expressed in Go's structural style.
type FeatureValue struct {
	Kind FeatureValueType

	Int    int64
	Double float64
	Str    string
	Bool   bool
	Time   time.Time
	List   []float64 // DOUBLE_LIST (embeddings)
	// Struct carries the opaque escape-hatch value (FeatureTypeStruct). Modeled as
	// map[string]any so the domain stays proto-free; the handler converts to/from
	// google.protobuf.Struct. It is NOT validated field-by-field (that is what
	// "opaque" means) — only that the declared type is STRUCT.
	Struct map[string]any
}

// ============================================================================
// FeatureRow — a batch input row for WriteFeatures (a COMMAND, pre-event)
// ============================================================================
//
// This is what the producer sends: an entity instance + its name→value map + the
// EVENT TIME (when the values became true in the real world — caller-owned domain
// data, the timestamp point-in-time queries compare against; NOT the ingest
// clock). The service validates a FeatureRow against the view schema and, on
// success, turns it into an immutable FeatureEvent.
type FeatureRow struct {
	EntityID  string
	Values    map[string]FeatureValue
	EventTime time.Time // when the values became valid; defaults to ingest time if zero
}

// ============================================================================
// FeatureEvent — THE immutable, append-only log record (the source of truth)
// ============================================================================
//
// Everything else in this service is a projection of a stream of these. Once
// appended, a FeatureEvent is NEVER mutated or deleted — that immutability is the
// whole basis of reproducibility and replay. WHY one event type with an EventType
// discriminator (rather than a Go interface with concrete variants): the log is
// persisted as homogeneous rows (feature_events) and replayed in version order;
// a single struct with a discriminator maps 1:1 to that row shape and to a JSONB
// payload, and keeps the fold a simple switch. The richer "interface of events"
// modeling buys nothing here and complicates persistence.
type FeatureEvent struct {
	// ID is a unique event id (UUID v4), server-assigned. Doubles as a natural
	// idempotency anchor for the persistence layer.
	ID string

	// Type discriminates how the fold interprets Payload (see EventType).
	Type EventType

	// FeatureViewID ties the event to its view (the aggregate root). The log is
	// partitioned by this for replay.
	FeatureViewID string

	// Version is the MONOTONIC, GAP-FREE per-... ordering key the projection folds
	// in. It is assigned by the append (the persistence port reserves the next
	// version under the write lock). WHY a version and not just a timestamp:
	// wall-clock time is non-monotonic and can tie/collide; a sequence gives a
	// total order so "latest" is unambiguous and read-your-writes
	// (WrittenThroughVersion) is well-defined. event_time orders for POINT-IN-TIME
	// reads; version orders for LATEST and for replay.
	Version int64

	// EntityID is set for FeatureEventValuesWritten (which entity this row is for);
	// empty for view-definition/deletion events (those are view-scoped, not
	// entity-scoped).
	EntityID string

	// Values is the feature payload for FeatureEventValuesWritten; nil otherwise.
	Values map[string]FeatureValue

	// EventTime is when the values became true (FeatureEventValuesWritten). This is
	// what ProjectAsOf compares against. For definition/deletion events it carries
	// the definition/retirement time.
	EventTime time.Time

	// ViewDef carries the schema snapshot for FeatureEventViewDefined (so a replay
	// can reconstruct the FeatureView without a second source). nil otherwise.
	ViewDef *ViewDefinition

	// AppendedAt is the server commit time (ingest time) — distinct from EventTime.
	// Kept for audit; the fold never compares against it (point-in-time uses
	// EventTime, ordering uses Version).
	AppendedAt time.Time
}

// EventType discriminates the FeatureEvent variants. The three types mirror the
// design doc's append-only Created/Updated/Deleted lifecycle, specialized to a
// feature store: a view is Defined (and re-Defined to evolve), values are
// Written, and a view is Deleted (soft retire).
type EventType int

const (
	// FeatureEventViewDefined records a DefineFeatureView command — the view's
	// schema at a version. Re-defining the same view appends another of these and
	// bumps SchemaVersion (additive evolution). Carries ViewDef.
	FeatureEventViewDefined EventType = iota + 1

	// FeatureEventValuesWritten records a WriteFeatures row — an entity's feature
	// values at an EventTime. The high-volume event; the fold over these IS the
	// online/offline projection. Carries EntityID + Values + EventTime.
	FeatureEventValuesWritten

	// FeatureEventViewDeleted records a DeleteFeatureView command — a TERMINAL
	// definition event that soft-retires the view. The log/history is preserved;
	// only new writes + online serving stop. Carries the retirement time in
	// EventTime.
	FeatureEventViewDeleted
)

// ViewDefinition is the schema snapshot carried by a FeatureEventViewDefined
// event. It is the subset of FeatureView that a definition event needs to make a
// replay self-contained: identity (id/name/entity), the schema (features), owner,
// and the version this event represents. The full FeatureView is REBUILT from a
// stream of these by foldView (below).
type ViewDefinition struct {
	ID            string
	Name          string
	Description   string
	Entity        Entity
	Features      []FeatureSpec
	SchemaVersion int64
	OwnerUserID   string
	OwnerTeam     string
}

// ============================================================================
// FeatureVector — the READ-SIDE projection result (CQRS read model)
// ============================================================================
//
// WHY distinct from FeatureRow (the write shape): a served vector carries
// PROVENANCE the write side does not — which log Version produced it, that
// event's EventTime, and the SchemaVersion it was validated against. Returning
// the version that served the value lets callers (and the Model Monitor) detect
// staleness and reproduce a read. Separating the write command shape from the
// read projection shape is textbook CQRS — the natural companion to event
// sourcing.
type FeatureVector struct {
	EntityID      string
	Values        map[string]FeatureValue
	AsOfVersion   int64     // the log version of the event that produced these values
	EventTime     time.Time // the producing event's EventTime
	SchemaVersion int64     // the view schema_version these values were validated against
}

// ============================================================================
// THE FOLD — projecting an event log into views (PURE, the pattern's core)
// ============================================================================
//
// These functions are the reference semantics for the materialized views. They
// are deliberately allocation-simple and side-effect-free: given the SAME events
// they always produce the SAME result, which is what makes RebuildViews a sound
// recovery operation and what the TDD suite pins. A Postgres/Redis adapter may
// compute the same answers more cheaply (SQL DISTINCT ON, a Redis hash), but it
// must AGREE with these.

// ProjectLatest folds a view's value events into the ONLINE view: for each
// entity, the values from the HIGHEST-version event (the latest write). It is the
// "latest value now" projection the inference hot path serves.
//
// WHY version order (not event_time) for "latest": the online view answers "what
// is true NOW", and writes are totally ordered by Version. A late-arriving
// backfill (older event_time, but appended later → higher version) SHOULD become
// the current online value if it is the most recently written fact for that
// entity — versions capture "most recently written", which is the online
// contract. (Point-in-time reads use event_time instead; see ProjectAsOf.)
//
// The result maps entity_id → FeatureVector. Deleted-view handling is the
// caller's concern (the service refuses online reads on a deleted view); this
// pure fold just projects whatever value events it is given.
func ProjectLatest(events []FeatureEvent, schemaVersion int64) map[string]FeatureVector {
	// We want, per entity, the event with the greatest Version. Sorting once by
	// Version ascending and overwriting keeps the LAST (highest) per entity, which
	// is simpler and more obviously-correct than carrying a per-entity max.
	ordered := sortedByVersion(events)
	out := make(map[string]FeatureVector)
	for _, e := range ordered {
		if e.Type != FeatureEventValuesWritten {
			continue // definition/deletion events carry no entity values
		}
		out[e.EntityID] = FeatureVector{
			EntityID:      e.EntityID,
			Values:        e.Values,
			AsOfVersion:   e.Version,
			EventTime:     e.EventTime,
			SchemaVersion: schemaVersion,
		}
	}
	return out
}

// ProjectAsOf folds a view's value events into the OFFLINE point-in-time view:
// for each entity, the values from the latest event with EventTime ≤ asOf. This
// is the reproducibility workhorse — it reconstructs the feature snapshot exactly
// as it stood at a past instant, eliminating label leakage in training.
//
// TIE-BREAK: among an entity's events with EventTime ≤ asOf, we take the greatest
// EventTime; if two events share an EventTime (e.g. a correction backfilled at the
// same instant), the greater Version wins (it was written later → more recent
// fact). This makes the result DETERMINISTIC, which a point-in-time read must be.
//
// Events with EventTime AFTER asOf are invisible (the entity may not have existed
// yet, or that value had not become true) — that invisibility is precisely what
// prevents leaking future information into a past snapshot.
func ProjectAsOf(events []FeatureEvent, asOf time.Time, schemaVersion int64) map[string]FeatureVector {
	out := make(map[string]FeatureVector)
	// best tracks the chosen event per entity so we can apply the (EventTime, then
	// Version) tie-break in a single pass without sorting.
	best := make(map[string]FeatureEvent)
	for _, e := range events {
		if e.Type != FeatureEventValuesWritten {
			continue
		}
		// Strictly: include events AT or BEFORE asOf. After(asOf) is the future.
		if e.EventTime.After(asOf) {
			continue
		}
		cur, seen := best[e.EntityID]
		if !seen || moreRecentAsOf(e, cur) {
			best[e.EntityID] = e
		}
	}
	for id, e := range best {
		out[id] = FeatureVector{
			EntityID:      id,
			Values:        e.Values,
			AsOfVersion:   e.Version,
			EventTime:     e.EventTime,
			SchemaVersion: schemaVersion,
		}
	}
	return out
}

// moreRecentAsOf implements the point-in-time tie-break: later EventTime wins; on
// an EventTime tie, higher Version wins. Pulled out so the rule is named, tested,
// and identical wherever it is applied.
func moreRecentAsOf(candidate, current FeatureEvent) bool {
	if candidate.EventTime.After(current.EventTime) {
		return true
	}
	if candidate.EventTime.Equal(current.EventTime) {
		return candidate.Version > current.Version
	}
	return false
}

// foldView reconstructs a FeatureView from its ordered definition/deletion events
// (the view aggregate). It is the projection behind GetFeatureView/ListFeatureViews
// and the proof that the catalog, too, is "just a fold of the log". The last
// FeatureEventViewDefined establishes the schema; a trailing FeatureEventViewDeleted
// sets DeletedAt. Returns false if no defining event exists (the view was never
// defined).
//
// WHY CreatedAt comes from the FIRST defined event and UpdatedAt from the LAST:
// CreatedAt is immutable (first definition); UpdatedAt advances with each schema
// evolution — both are derivable from the log, never client-set.
func foldView(events []FeatureEvent) (FeatureView, bool) {
	ordered := sortedByVersion(events)
	var view FeatureView
	defined := false
	for _, e := range ordered {
		switch e.Type {
		case FeatureEventViewDefined:
			if e.ViewDef == nil {
				continue // defensive: a defined event must carry its schema
			}
			d := e.ViewDef
			if !defined {
				view.CreatedAt = e.EventTime // first definition = creation
				defined = true
			}
			view.ID = d.ID
			view.Name = d.Name
			view.Description = d.Description
			view.Entity = d.Entity
			view.Features = d.Features
			view.SchemaVersion = d.SchemaVersion
			view.OwnerUserID = d.OwnerUserID
			view.OwnerTeam = d.OwnerTeam
			view.UpdatedAt = e.EventTime // last definition = last update
		case FeatureEventViewDeleted:
			view.DeletedAt = e.EventTime
			view.UpdatedAt = e.EventTime
		case FeatureEventValuesWritten:
			// value events do not affect the view's schema projection
		}
	}
	return view, defined
}

// sortedByVersion returns a copy of events ordered by ascending Version. We COPY
// (rather than sort in place) so the fold never mutates the caller's slice — a
// projection must be free of observable side effects, including reordering the
// input. Version is the total order assigned at append time, so this is the
// canonical replay order.
func sortedByVersion(events []FeatureEvent) []FeatureEvent {
	out := make([]FeatureEvent, len(events))
	copy(out, events)
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out
}
