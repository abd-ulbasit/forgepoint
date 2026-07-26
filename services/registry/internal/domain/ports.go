// ports.go — the repository/external PORTS the registry domain depends on.
//
// ============================================================================
// PORTS LIVE IN THE DOMAIN (Hexagonal "consumer-owned ports")
// ============================================================================
//
// As in the Auth service, the interfaces the domain CONSUMES are defined here,
// in the domain package, NOT in repository/. The reason is the same import-cycle
// trap: the service impl (registry_service_impl.go) lives in `domain` and must
// reference these ports; if the ports lived in `repository`, then `repository`
// would import `domain` (for the types) AND `domain` would import `repository`
// (for the ports) — a cycle Go rejects at compile time. Consumer-owns-the-port
// breaks it: the Postgres/Redis ADAPTERS (later phases) import `domain` and
// implement these one-way. (A thin repository alias shim is optional sugar.)
//
// ============================================================================
// THE CQRS SPLIT — WHY *TWO* STORE PORTS, NOT ONE (the centerpiece)
// ============================================================================
//
// CQRS (Command Query Responsibility Segregation) separates the WRITE model from
// the READ model. This service realizes it with TWO distinct ports:
//
//	WriteStore  — the source of truth (Postgres in production). COMMANDS go here:
//	              RegisterModel/CreateVersion/PromoteVersion/MarkVersionReady/
//	              ArchiveModel persist normalized rows with strong consistency,
//	              transactions, and the (team,name) / (model,version) uniqueness
//	              the business invariants need. The write store is also where the
//	              IDEMPOTENCY ledger lives.
//
//	ReadStore   — the denormalized projection (Redis in production). QUERIES go
//	              here: GetModel/ListModels/GetVersion/ListVersions are O(1)/range
//	              reads of a shape PRE-COMPUTED for the query ("prod version of X"
//	              is a stored field, never a join at read time).
//
// THE LOOP that keeps them in sync (modeled, not yet implemented in this phase):
//
//	COMMAND ─► WriteStore.Save... (truth, in a tx) ──► returns the new state
//	                                 │
//	                                 ▼
//	                       ProjectionEmitter.Emit(event)   ← records the fact that
//	                                 │                        the read model must
//	                                 ▼                        reflect this change
//	                    (later phase) NATS event ─► projection consumer ─► ReadStore
//	                                                                           ▲
//	                           GetModel / ListModels / GetVersion ────────────┘
//	                                           (eventually consistent)
//
// WHY a ProjectionEmitter PORT rather than the service writing Redis directly:
//
//	Keeping the projection FED BY EVENTS (not by a direct dual-write inside the
//	command) is what makes this real CQRS rather than "two databases I update in a
//	loop". The command's only coupling to the read side is "emit a fact"; WHO
//	builds the projection from that fact (a NATS consumer) is a separate concern
//	that can be scaled, replayed, or rebuilt independently. It also means the SAME
//	emitted fact feeds BOTH the internal Redis projection AND external consumers
//	(serving, billing) — one event, many readers. In this phase the emitter is a
//	port with a hand-written mock; the NATS-backed adapter lands in the events phase.
//
// TRADEOFF we accept (and document on every query): EVENTUAL CONSISTENCY. After a
// command commits to the WriteStore and emits its event, there is a brief window
// before the ReadStore reflects it. The command RESPONSE returns the WriteStore
// truth (immediately consistent for the caller); only the separate QUERY RPCs read
// the lagging projection. A caller needing read-your-writes consumes the event
// rather than polling the read API.
//
// IS A DUAL WRITE TO POSTGRES+REDIS INSIDE ONE COMMAND UNSAFE?
//
//	Exactly — that is the dual-write problem (the second write can fail after the
//	first commits, leaving the stores inconsistent with no rollback). We avoid it:
//	the command writes ONE store (Postgres) and emits an event; the projection is
//	built from the event asynchronously and idempotently, so a transient Redis
//	failure just delays the projection, never corrupts the truth. (Production-
//	grade variants put the emit in the same tx via the Outbox pattern — which is
//	precisely what the Billing service demonstrates; here the emit port is the seam
//	that an outbox-backed adapter would slot into.)
//
// ============================================================================
package domain

import (
	"context"
	"errors"
	"time"
)

// ============================================================================
// STORAGE SENTINELS (storage outcomes the SERVICE translates to business errors)
// ============================================================================

// ErrRecordNotFound is the STORAGE sentinel a store port returns when a requested
// row/key is absent. The service translates it to the right BUSINESS error
// (ErrModelNotFound / ErrVersionNotFound) depending on what was being fetched.
// Keeping storage's "no row" distinct from the business's "model not found" means
// the service is the only place that knows the mapping.
var ErrRecordNotFound = errors.New("registry/store: record not found")

// ErrWriteConflict is the STORAGE sentinel the WriteStore returns when an insert
// violates a uniqueness constraint — the (team, name) model index or the
// (model_id, version) version index. The service maps it to ErrModelNameTaken or
// ErrVersionExists depending on which create was attempted. Surfacing it as a
// typed sentinel (rather than a raw pg error string) keeps the domain free of the
// driver vocabulary.
var ErrWriteConflict = errors.New("registry/store: unique constraint violation")

// ============================================================================
// PAGINATION (mirrors the platform-wide cursor contract)
// ============================================================================

// ListOptions carries cursor-based pagination into the read ports. Cursor (token)
// pagination is used over LIMIT/OFFSET because it is stable under concurrent
// writes — OFFSET can skip or duplicate rows when the underlying set changes
// between pages, whereas a cursor anchored on (created_at,id) does not.
//
// PageSize is the caller's request; the SERVICE clamps it to [1, MaxPageSize]
// (the DoS guard the proto documents — default 20, hard cap 100) BEFORE it ever
// reaches a store, so a store never sees an unbounded or zero page size.
type ListOptions struct {
	PageSize  int    // already clamped by the service; a store may trust it
	PageToken string // opaque cursor; empty = start from the beginning
}

// ListModelsFilter narrows a ListModels query WITHIN the caller's already team-
// scoped view. WHY these live in a struct passed to the port (not as loose args):
// the filter surface can grow (a new optional facet) without changing the port
// signature. Team is NOT here on purpose — team scoping is applied by the SERVICE
// from the Actor (auth claims), never a filter field a caller could set, so a
// caller can only ever NARROW their own team's view, never widen to another's.
type ListModelsFilter struct {
	TaskType        string // "" = any
	Framework       string // "" = any
	IncludeArchived bool   // false (default) hides soft-deleted models
}

// ============================================================================
// WRITE STORE PORT (the source of truth — COMMANDS)
// ============================================================================

// WriteStore is the normalized, strongly-consistent source of truth (Postgres in
// production). Every COMMAND method of the service persists through it. The
// adapter implements each method in a single transaction so the business
// invariants (uniqueness, single-production) hold atomically.
//
// IDEMPOTENCY — TWO LEDGERS, ONE CONTRACT:
//
//	(1) CREATE idempotency. The two genuinely-creating commands (RegisterModel,
//	    CreateVersion) take an idempotency key recorded INLINE with the created row
//	    (CreateModel/CreateVersion's key arg + the LookupBy*IdempotencyKey methods).
//	    A retry returns the ORIGINAL record instead of minting a duplicate.
//
//	(2) MUTATION idempotency. The three mutation commands the proto also promises
//	    key-dedup for — ConfirmVersionUpload (MarkVersionReady), PromoteVersion and
//	    DeleteModel (ArchiveModel) — do NOT create a row, so there is no row to hang
//	    the key on. They use a SEPARATE generic command-idempotency ledger keyed by
//	    (team, command, key) → the affected entity id. The service checks it FIRST
//	    (returning current entity state, no re-write, no re-emit) and records it on
//	    the write, mirroring the create path.
//
//	WHY a generic ledger rather than per-command columns: the proto guarantees
//	KEY-based exactly-once side effects (serving reload, billing re-meter) — a
//	guarantee state-based idempotency alone cannot give. State-based idempotency
//	("already READY → no-op") dedupes a SIMPLE retry, but a key whose first attempt
//	took a DIFFERENT branch (e.g. a confirm that FAILED, then a retry asserting
//	Success; or two same-key promotes to different stages) is only safely deduped by
//	remembering the key→outcome. The ledger is that memory. One generic table
//	(team, command, key) serves all three with no schema churn per command.
//	In production this is one INSERT ... ON CONFLICT DO NOTHING row written inside
//	the SAME transaction as the mutation, so the key and the side effect commit
//	atomically (no window where the mutation lands but the key doesn't).
//
//	The service checks the key FIRST, so a network retry is a clean success, not an
//	ALREADY_EXISTS error or a re-fired side effect (Stripe's contract).
type WriteStore interface {
	// CreateModel inserts a new model. Returns ErrWriteConflict if (Team, Name)
	// collides. The service has already stamped server-authoritative fields
	// (ID, OwnerID, Team, CreatedAt) before calling.
	CreateModel(ctx context.Context, m Model, idempotencyKey string) (Model, error)

	// LookupModelByIdempotencyKey returns the model previously created with this
	// key, or ErrRecordNotFound if none. Empty key always returns ErrRecordNotFound
	// (an empty key never matches — the caller opted out of idempotency).
	LookupModelByIdempotencyKey(ctx context.Context, team, key string) (Model, error)

	// GetModel loads a model by id for command-side validation (e.g. CreateVersion
	// must confirm the target model exists, is in the caller's team, and is not
	// archived). Commands read the TRUTH, not the projection, so they never act on
	// stale data. Returns ErrRecordNotFound if absent.
	GetModel(ctx context.Context, id string) (Model, error)

	// UpdateModel persists a mutated model (description/tags edit, or the
	// ArchivedAt stamp on soft-delete, or a denormalized pointer refresh). The
	// service sets UpdatedAt before calling.
	UpdateModel(ctx context.Context, m Model) (Model, error)

	// CreateVersion inserts a new version row. Returns ErrWriteConflict if
	// (ModelID, Version) collides. The service has stamped ID, Stage=DEV,
	// Status=PENDING_UPLOAD, CreatedBy, CreatedAt before calling.
	CreateVersion(ctx context.Context, v ModelVersion, idempotencyKey string) (ModelVersion, error)

	// LookupVersionByIdempotencyKey mirrors the model lookup for CreateVersion
	// retries. Scoped by modelID so keys are namespaced per model.
	LookupVersionByIdempotencyKey(ctx context.Context, modelID, key string) (ModelVersion, error)

	// GetVersion loads a version by id for command-side validation (PromoteVersion
	// and MarkVersionReady operate on the truth). Returns ErrRecordNotFound if absent.
	GetVersion(ctx context.Context, id string) (ModelVersion, error)

	// CountVersions returns how many versions a model has — used by the service to
	// auto-assign the next monotonic version label when the client doesn't pin one.
	CountVersions(ctx context.Context, modelID string) (int, error)

	// UpdateVersionStatus persists a version's status transition plus the server-
	// measured artifact facts (path/digest/size) on the PENDING_UPLOAD→READY edge.
	// Passing the artifact fields here (not letting them be set on create) keeps
	// them server-measured-only. Returns the updated version.
	UpdateVersionStatus(ctx context.Context, v ModelVersion) (ModelVersion, error)

	// PromoteVersionTx performs the stage swap as ONE atomic unit — the heart of
	// the SINGLE-PRODUCTION INVARIANT. The service has already validated the
	// transition and computed both ends:
	//   - promoted: the version moved to its new stage.
	//   - demoted:  the prior PRODUCTION version moved to ARCHIVED, or a zero-value
	//               ModelVersion (Demoted.ID == "") when there was no prior prod.
	// The adapter writes BOTH rows in a single transaction so there is never a
	// moment with two PRODUCTION versions of the same model. Returns the persisted
	// promoted + demoted versions.
	PromoteVersionTx(ctx context.Context, promoted ModelVersion, demoted ModelVersion) (newPromoted ModelVersion, newDemoted ModelVersion, err error)

	// FindProductionVersion returns the model's current PRODUCTION version, or
	// ErrRecordNotFound if the model has none. The service calls this before a
	// promote-to-PRODUCTION to find the version it must atomically demote.
	FindProductionVersion(ctx context.Context, modelID string) (ModelVersion, error)

	// ArchiveModelTx soft-deletes a model and archives ALL its versions in one
	// transaction (the aggregate is the consistency boundary). The service has set
	// the model's ArchivedAt. Returns the archived model.
	ArchiveModelTx(ctx context.Context, m Model) (Model, error)

	// LookupCommandIdempotency returns the entity id a prior invocation of the given
	// (team, command, key) recorded, or ErrRecordNotFound if this key has never been
	// seen for that command. It is the read half of the MUTATION idempotency ledger
	// (see the IDEMPOTENCY note above) — the service consults it FIRST on
	// MarkVersionReady / PromoteVersion / ArchiveModel and, on a hit, returns the
	// referenced entity's CURRENT state without re-writing or re-emitting. An empty
	// key always returns ErrRecordNotFound (the caller opted out of key dedup).
	LookupCommandIdempotency(ctx context.Context, team string, command Command, key string) (entityID string, err error)

	// RecordCommandIdempotency records that (team, command, key) produced the given
	// entity id, so a later retry with the same key is deduped by LookupCommandIdempotency.
	// An empty key is a no-op (nothing to remember). In production this row is written
	// in the SAME transaction as the mutation so the key and the side effect commit
	// atomically; here it is a separate call the in-memory fake performs after the
	// mutation persists. Returns ErrWriteConflict only if the SAME key is concurrently
	// recorded for a DIFFERENT entity (a genuine key-reuse race) — the service surfaces
	// that as the underlying mutation's conflict rather than corrupting the ledger.
	RecordCommandIdempotency(ctx context.Context, team string, command Command, key, entityID string) error
}

// Command names the mutation a MUTATION-idempotency ledger entry belongs to, so the
// same key may be reused across DIFFERENT commands without colliding (a promote key
// and an archive key that happen to be equal are independent). Modeled as a small
// typed enum rather than a free string so a typo can't silently fragment the ledger.
type Command int

const (
	// CommandMarkVersionReady scopes ConfirmVersionUpload (MarkVersionReady) keys.
	CommandMarkVersionReady Command = iota
	// CommandPromoteVersion scopes PromoteVersion keys.
	CommandPromoteVersion
	// CommandArchiveModel scopes DeleteModel (ArchiveModel) keys.
	CommandArchiveModel
)

// String renders the command for the ledger key + logs. Kept terse and stable
// (these strings are part of the persisted ledger key in production).
func (c Command) String() string {
	switch c {
	case CommandMarkVersionReady:
		return "mark_version_ready"
	case CommandPromoteVersion:
		return "promote_version"
	case CommandArchiveModel:
		return "archive_model"
	default:
		return "unknown"
	}
}

// ============================================================================
// READ STORE PORT (the denormalized projection — QUERIES)
// ============================================================================

// ReadStore is the eventually-consistent read model (Redis in production), built
// by a projection consumer from the events the commands emit. Every QUERY method
// of the service reads through it. Its shape is optimized for the queries:
// GetModel is a key lookup, ListModels/ListVersions are sorted-set range scans,
// and the production/latest pointers are stored fields (no read-time computation).
//
// WHY queries do NOT read the WriteStore: the whole point of CQRS is that the read
// side scales and is shaped independently. Reading Postgres on the hot "prod
// version of X" path (potentially thousands/sec on the inference path) is exactly
// what we are avoiding. The cost is projection lag (eventual consistency),
// documented on every query.
type ReadStore interface {
	// GetModelByID / GetModelByName fetch a single projected model. The query side
	// supports name lookup because callers usually know the human name, not the
	// UUID. Both are team-scoped by the service (the team arg comes from the Actor,
	// never the request). Return ErrRecordNotFound on a projection miss (which may
	// be transient projection lag for a just-registered model — the service surfaces
	// it as ErrModelNotFound and the caller may retry or consume the event).
	GetModelByID(ctx context.Context, team, id string) (Model, error)
	GetModelByName(ctx context.Context, team, name string) (Model, error)

	// ListModels returns a team-scoped, newest-first page plus a next-page cursor
	// ("" when exhausted). The filter only narrows within the team.
	ListModels(ctx context.Context, team string, filter ListModelsFilter, opts ListOptions) (models []Model, nextToken string, err error)

	// CountModels returns the TOTAL number of a team's models matching the filter —
	// independent of pagination. It is the read-model's accurate aggregate count: the
	// value ListModels' Page.Total carries, surfaced as the proto's total_count, which
	// the BFF dashboard reads to render "N models" (it calls ListModels with page_size=1
	// purely to read this count, NOT to fetch rows). Keeping a dedicated count method —
	// rather than len(page) of a single page — is what makes the dashboard tile agree
	// with the full list: a page is at most PageSize rows, but the count is the whole
	// filtered set. Same team-scoping + filter semantics as ListModels so the count and
	// the list can never disagree about which models are in scope (notably: archived
	// models are excluded unless filter.IncludeArchived, matching ListModels' default).
	CountModels(ctx context.Context, team string, filter ListModelsFilter) (int, error)

	// GetVersionByID / GetVersionByLabel fetch a single projected version, by its
	// own id or by the (modelID, label) pair — whichever the caller has on hand.
	GetVersionByID(ctx context.Context, id string) (ModelVersion, error)
	GetVersionByLabel(ctx context.Context, modelID, version string) (ModelVersion, error)

	// ListVersions returns a model's versions newest-first, optionally filtered to a
	// single stage. StageUnspecified means "any stage". Paginated like ListModels.
	ListVersions(ctx context.Context, modelID string, stageFilter ModelStage, opts ListOptions) (versions []ModelVersion, nextToken string, err error)
}

// ============================================================================
// PROJECTION EMITTER PORT (the CQRS sync seam — feeds the read model + consumers)
// ============================================================================

// ProjectionEvent is the domain's neutral description of a state change a command
// produced, recorded so the read projection (and external consumers) can react.
//
// WHY a domain-defined event type rather than the generated eventsv1.* proto:
//
//	The domain must not import gen/go (framework dependency). It speaks in its own
//	vocabulary; the EVENT ADAPTER (later phase) maps a ProjectionEvent to the
//	canonical eventsv1.* payload and the NATS subject. So the domain emits a pure
//	value and stays decoupled from the wire event schema — the same boundary the
//	proto header insists on.
type ProjectionEvent struct {
	// Kind names the change (see the ProjectionEventKind constants). The adapter
	// switches on it to pick the eventsv1 message + fp.models.* subject.
	Kind ProjectionEventKind
	// Model is the affected model's state AFTER the command (for Registered/
	// Archived/version events it carries identity + ownership the projection and
	// consumers need). Always populated.
	Model Model
	// Version is the affected version AFTER the command, for version-scoped events
	// (Created/Ready/Promoted). Its ID is "" for model-only events (Registered/Archived).
	Version ModelVersion
	// DemotedVersion carries the auto-demoted prior-production version on a
	// Promoted-to-PRODUCTION event (the single-prod swap), so the projection and
	// downstream consumers see the whole atomic change. Its ID is "" when no prior
	// production version existed.
	DemotedVersion ModelVersion
	// FromStage/ToStage describe a Promoted event's transition (self-describing for
	// audit and so a consumer can ignore transitions it doesn't care about). Zero
	// for non-promotion events.
	FromStage ModelStage
	ToStage   ModelStage
	// Actor is who performed the command (becomes the *_by audit field on the event).
	Actor Actor
	// OccurredAt is the server time the change committed (the event's producer clock).
	OccurredAt time.Time
}

// ProjectionEventKind enumerates the changes the registry emits. These map 1:1 to
// the five canonical events the proto documents (ModelRegistered,
// ModelVersionCreated, ModelVersionReady, ModelPromoted, ModelArchived). The
// adapter does the domain→eventsv1 mapping; the domain stays wire-agnostic.
type ProjectionEventKind int

const (
	// EventModelRegistered ← RegisterModel committed (→ fp.models.registered).
	EventModelRegistered ProjectionEventKind = iota
	// EventVersionCreated ← CreateVersion committed; status PENDING_UPLOAD
	// (→ fp.models.version.created).
	EventVersionCreated
	// EventVersionReady ← MarkVersionReady flipped PENDING_UPLOAD→READY
	// (→ fp.models.version.ready). NOT emitted on a →FAILED outcome.
	EventVersionReady
	// EventModelPromoted ← PromoteVersion committed the atomic stage swap
	// (→ fp.models.promoted). Carries the demoted version when one was swapped out.
	EventModelPromoted
	// EventModelArchived ← ArchiveModel soft-deleted the model + its versions
	// (→ fp.models.archived).
	EventModelArchived
)

// ProjectionEmitter is the CQRS sync seam: a command records its state change
// here so the read projection can be (re)built and external consumers can react.
//
// CONTRACT / FAILURE POSTURE: in this phase the emitter is a
// simple port. The production adapter publishes to NATS. The service emits AFTER
// the WriteStore commit succeeds — the write is the truth and must not be undone
// by an emit failure. An emit error is therefore logged and surfaced but does NOT
// roll back the command (the projection will catch up on the next event or a
// rebuild). The robust production form moves the emit INTO the write transaction
// via the Outbox pattern (see the Billing service) so the fact is committed
// atomically with the state and published by a relay — this port is the seam that
// outbox-backed adapter slots into without changing the service.
type ProjectionEmitter interface {
	// Emit records that a command produced the given change. Returns an error if
	// the change could not be recorded; the service logs it but does not fail the
	// already-committed command (see the posture note above).
	Emit(ctx context.Context, event ProjectionEvent) error
}

// ============================================================================
// SUPPORTING PORTS (clock + id generation — pure, but injected for determinism)
// ============================================================================

// Clock supplies the current time. WHY inject it rather than call time.Now()
// inside the service: tests can assert EXACT timestamps (CreatedAt == a known
// value) and the single-production swap's ordering deterministically, instead of
// racing the wall clock. Production wires a realClock that delegates to time.Now().
type Clock interface {
	Now() time.Time
}

// IDGenerator supplies new unique ids. WHY a port: tests inject a deterministic
// sequence ("model-1","version-1",...) so assertions can reference exact ids,
// while production wires a uuidGenerator backed by github.com/google/uuid (v4,
// the only external dep the domain is permitted). Centralizing id minting also
// means every server-generated id is created the same way — a caller can never
// supply one.
type IDGenerator interface {
	NewID() string
}
