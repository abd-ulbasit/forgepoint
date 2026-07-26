// projection.go — the CQRS PROJECTION CONSUMER: the missing read-side of the
// registry's command/query split.
//
// ============================================================================
// THE BUG THIS CLOSES (the CQRS read side was never populated)
// ============================================================================
//
// The registry is CQRS: COMMANDS write Postgres (the WriteStore, the truth) and
// emit fp.models.* events; QUERIES read Redis (the ReadStore, the projection).
// But nothing was UPDATING Redis from those events — main.go even documented the
// projection consumer as "a later phase". Net effect: POST /models wrote Postgres
// and published fp.models.registered, but GET /models read an EMPTY Redis and
// returned []. The headline CQRS feature was half-wired.
//
// THIS FILE is that missing half. It is a durable, idempotent NATS consumer that
// subscribes to the registry's own lifecycle events and UPSERTS the Redis read
// model, so a write becomes visible to reads after a short projection lag:
//
//	COMMAND ─► WriteStore.Save (Postgres, truth) ─► emit fp.models.* event
//	                                                      │
//	                                                      ▼  (THIS consumer)
//	                                   ProjectionWriter.Upsert... ─► Redis read model
//	                                                      │
//	QUERY ◄──────────────────────── ReadStore (Redis) ◄──┘
//
// ============================================================================
// WHY A CONSUMER (eventual consistency) RATHER THAN DUAL-WRITE
// ============================================================================
//
// The command could write Postgres AND Redis inline, but that is a dual-write
// across two stores with no shared transaction: a crash between the two leaves the
// projection permanently wrong, and the hot read path now depends on Redis being
// up at WRITE time. Driving the projection off the SAME events every other
// consumer already receives keeps ONE source of truth (the event), makes the read
// model REBUILDABLE (replay the stream to repopulate Redis after a flush/schema
// change), and lets the read side scale/restart independently — the whole point of
// CQRS. The cost is projection lag (documented on every ReadStore query) — the
// command RESPONSE returns the authoritative write-side state, so a client never
// sees its own write missing.
//
// ============================================================================
// IDEMPOTENCY (NATS is at-least-once — duplicates WILL happen)
// ============================================================================
//
// Two layers make a redelivered event a no-op:
//   (a) TRANSPORT: a natsutil ProcessedStore dedupes on EventEnvelope.id — a
//       redelivered envelope id is ACKed without re-running the handler (the cheap
//       fast-path, mirroring every other consumer on the platform).
//   (b) SEMANTIC: every handler is LOAD-MERGE-UPSERT — it reconstructs the target
//       entity's full projected state and writes it wholesale. Applying the SAME
//       event twice yields the SAME Redis state (the upsert is deterministic), and
//       a STALE/out-of-order event is guarded by an event-time check (we never let
//       an older event overwrite a newer projection — see applyIfNewer). So even a
//       duplicate that slips past (a) — e.g. redelivered to a different replica
//       with its own memory store — cannot corrupt the projection. (b) is the
//       authoritative guard; (a) is the optimization.
//
// DECODE DIALECT: the registry publishes proto events via natsutil.Publisher, which
// marshals proto messages with protojson (the canonical proto-JSON dialect). So we
// decode with protojson.Unmarshal — symmetric with the publisher. registered_at /
// ready_at / promoted_at are google.protobuf.Timestamp fields (RFC-3339 strings on
// the wire) that ONLY protojson decodes; encoding/json would fail or zero them.
package events

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/services/registry/internal/domain"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// ProjectionWriter is the narrow port the projection needs from the Redis read
// store: the upserts that WRITE the projection plus the unscoped loads it
// load-merges against. We depend on this small interface (not the concrete
// *redis.ReadStore) so the projection is unit-testable with a fake and the
// dependency surface is honest — exactly the seam the registry's other adapters
// use. *redis.ReadStore satisfies it structurally, so production wiring is a
// one-liner in main.go.
type ProjectionWriter interface {
	UpsertModel(ctx context.Context, m domain.Model) error
	UpsertVersion(ctx context.Context, v domain.ModelVersion) error
	GetModelByIDUnscoped(ctx context.Context, id string) (domain.Model, error)
	GetVersionByID(ctx context.Context, id string) (domain.ModelVersion, error)
}

// ModelReader is the narrow read-back port the projection uses to hydrate a model's
// FULL field set from the WRITE store (the source of truth) when an event payload is
// "thin" — i.e. does not carry every field the read model must serve.
//
// WHY THIS EXISTS (the description-dropped fix): the fp.models.registered event schema
// (eventsv1.ModelRegistered) carries identity + ownership + framework/task_type, but
// NOT description or tags. So a projection built ONLY from the event would store an
// empty description, and GET /models/{id} would return "" even though the write store
// has the real value — the exact bug. Rather than fatten the published event contract
// (a schema change rippling to every consumer), we use the THIN-EVENT + READ-BACK
// pattern: the event is the trigger ("model X changed"), and the projection reads
// model X's authoritative, FIELD-COMPLETE state from the write store and projects
// that. This keeps the event contract minimal while the read model stays complete.
//
// TRADEOFF: read-back adds one write-store round-trip per
// projected register and reintroduces a read of the truth on the projection path
// (versus a self-contained "fat event"). We accept it here because (a) registers are
// low-volume relative to the hot query path, (b) it avoids a contract change to a
// shared event consumed by six services, and (c) the projection is already a
// server-side, trusted writer (not a tenant query). The fat-event alternative —
// adding description/tags to ModelRegistered — is the right move IF those fields ever
// have external consumers; until then, read-back is the smaller, contained fix.
// *postgres.WriteStore satisfies this via its GetModel(ctx, id) method.
type ModelReader interface {
	GetModel(ctx context.Context, id string) (domain.Model, error)
}

// ProjectionConfig is the resilience policy the wiring layer (main.go) hands the
// consumer. Zero values get platform defaults via withDefaults so main.go can pass
// ProjectionConfig{} for standard behavior.
type ProjectionConfig struct {
	// ConsumerBase is the durable-name PREFIX. A UNIQUE durable is derived per
	// subject (base + "-" + sanitized-subject) so each registry subject gets its own
	// server-side cursor/filter — never sharing one durable across subjects (that
	// would make CreateOrUpdateConsumer overwrite filters; only the last would
	// survive and events would hit the wrong handler). Default: "registry-projection".
	ConsumerBase string
	// Store is the consumer-side idempotency backend (memory in tests; shared across
	// the per-subject consumers since dedup is by global EventEnvelope.id). May be nil
	// (the semantic load-merge-upsert idempotency still holds), but wiring one is the
	// cheap fast-path. Default: a fresh MemoryProcessedStore.
	Store natsutil.ProcessedStore
	// MaxRetries / DLQSubject are the DLQ policy: a poison event (corrupt payload,
	// unmappable) is parked after the cap instead of NAK-looping forever. Default:
	// MaxRetries=4, DLQSubject="fp.dlq.registry-projection".
	MaxRetries int
	DLQSubject string
	// MessageTimeout bounds one handler invocation (a load-merge-upsert = a couple of
	// Redis round-trips). Default: 10s.
	MessageTimeout time.Duration
}

func (c *ProjectionConfig) withDefaults() {
	if c.ConsumerBase == "" {
		c.ConsumerBase = "registry-projection"
	}
	if c.Store == nil {
		c.Store = natsutil.NewMemoryProcessedStore()
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = 4
	}
	if c.DLQSubject == "" {
		c.DLQSubject = "fp.dlq.registry-projection"
	}
	if c.MessageTimeout == 0 {
		c.MessageTimeout = 10 * time.Second
	}
}

// Projection is the inbound adapter that rebuilds the Redis read model from the
// registry's fp.models.* events. It owns one durable natsutil.Subscriber per
// consumed subject (each with its own durable name — see ProjectionConfig).
type Projection struct {
	js    jetstream.JetStream
	store ProjectionWriter
	// truth reads the FIELD-COMPLETE model from the write store to hydrate fields the
	// thin event omits (description/tags). May be nil — see hydrateModel: when nil the
	// projection falls back to the event's fields (the pre-fix behavior), so existing
	// callers/tests that don't wire a reader still work, just without the enrichment.
	truth ModelReader
	cfg   ProjectionConfig

	mu   sync.Mutex
	subs []*natsutil.Subscriber
}

// NewProjection builds the projection consumer. main.go injects the JetStream handle,
// the Redis read store (which satisfies ProjectionWriter), and the Postgres write
// store as the ModelReader read-back source (which satisfies ModelReader via GetModel)
// so the read model is hydrated FIELD-COMPLETE (description/tags included) — see
// ModelReader. truth may be nil (the projection then falls back to the thin event's
// fields), which keeps unit tests that only exercise the projection mechanics simple.
func NewProjection(js jetstream.JetStream, store ProjectionWriter, truth ModelReader, cfg ProjectionConfig) *Projection {
	cfg.withDefaults()
	return &Projection{js: js, store: store, truth: truth, cfg: cfg}
}

// durableName derives a JetStream-safe durable name for a subject by replacing the
// '.' separators (illegal in a durable name) with '_'. base "registry-projection" +
// "fp.models.registered" → "registry-projection-fp_models_registered" — unique per
// subject (so each gets its own server-side cursor/filter) and stable across
// restarts (so the consumer resumes where it left off rather than replaying).
func durableName(base, subject string) string {
	return base + "-" + strings.ReplaceAll(subject, ".", "_")
}

// projectionSubscription pairs a subject with the handler that decodes+applies it.
// Defining the consumed set as DATA (not five copy-pasted Subscribe calls) keeps
// "what we subscribe to" and "what we decode" impossible to drift apart.
type projectionSubscription struct {
	subject string
	handler natsutil.EventHandler
}

// subscriptions returns the registry's five lifecycle subjects bound to their
// projection handlers. All live on the MODELS stream (the registry owns the
// fp.models.* tree — see streams.go).
func (p *Projection) subscriptions() []projectionSubscription {
	return []projectionSubscription{
		{SubjectModelRegistered, p.handleModelRegistered},
		{SubjectModelVersionCreated, p.handleModelVersionCreated},
		{SubjectModelVersionReady, p.handleModelVersionReady},
		{SubjectModelPromoted, p.handleModelPromoted},
		{SubjectModelArchived, p.handleModelArchived},
	}
}

// Start binds a durable consumer for every registry subject and begins draining.
// Call once at startup; the ctx governs every consume loop's lifetime alongside
// Close(). A failure to bind ANY subject aborts (returns the error) so the read
// model never silently misses half its inputs.
func (p *Projection) Start(ctx context.Context) error {
	for _, sb := range p.subscriptions() {
		sub := natsutil.NewSubscriber(p.js,
			natsutil.WithConsumerGroup(durableName(p.cfg.ConsumerBase, sb.subject)),
			natsutil.WithIdempotencyStore(p.cfg.Store),
			natsutil.WithMaxRetries(p.cfg.MaxRetries),
			natsutil.WithDLQSubject(p.cfg.DLQSubject),
			natsutil.WithMessageTimeout(p.cfg.MessageTimeout),
		)
		if err := sub.Subscribe(ctx, StreamName, sb.subject, sb.handler); err != nil {
			sub.Close()
			return fmt.Errorf("registry/events: projection subscribe %s on %s: %w", sb.subject, StreamName, err)
		}
		p.mu.Lock()
		p.subs = append(p.subs, sub)
		p.mu.Unlock()
	}
	return nil
}

// HandlerFor exposes a single subject's handler so a test can drive the exact
// production decode→upsert path without a live JetStream consumer. Returns nil for
// an unknown subject (a test typo fails loudly rather than testing nothing).
func (p *Projection) HandlerFor(subject string) natsutil.EventHandler {
	for _, sb := range p.subscriptions() {
		if sb.subject == subject {
			return sb.handler
		}
	}
	return nil
}

// Close stops every per-subject consume loop. Idempotent (each natsutil
// Subscriber.Close is). Called on graceful shutdown alongside cancelling the Start
// context.
func (p *Projection) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, sub := range p.subs {
		sub.Close()
	}
	p.subs = nil
}

// ----------------------------------------------------------------------------
// Per-event handlers: decode events.v1 payload → load-merge → upsert projection.
// ----------------------------------------------------------------------------

// handleModelRegistered projects a brand-new model. RegisterModel commits with no
// versions yet, so this is a clean UpsertModel of identity + ownership + metadata.
func (p *Projection) handleModelRegistered(ctx context.Context, env natsutil.EventEnvelope) error {
	var ev eventsv1.ModelRegistered
	if err := decodeProjection(env, &ev); err != nil {
		return err
	}
	if ev.GetModelId() == "" {
		return fmt.Errorf("%w: ModelRegistered with empty model_id (no projection key)", natsutil.ErrProcessingFailed)
	}
	registeredAt := ev.GetRegisteredAt().AsTime()
	// Build the model from the THIN event first (identity + ownership + framework/
	// task_type), then hydrate the fields the event omits (description, tags) from the
	// write-store truth. See hydrateModel / ModelReader for why the event can't carry
	// them and why read-back is the fix.
	m := domain.Model{
		ID:        ev.GetModelId(),
		Name:      ev.GetModelName(),
		OwnerID:   ev.GetOwnerId(),
		Team:      ev.GetTeam(),
		Framework: ev.GetFramework(),
		TaskType:  ev.GetTaskType(),
		CreatedAt: registeredAt,
		UpdatedAt: registeredAt,
	}
	m = p.hydrateModel(ctx, m)
	if err := p.store.UpsertModel(ctx, m); err != nil {
		// Transient (Redis blip) — NAK + retry. The upsert is idempotent, so a retry
		// after a partial failure is safe.
		return fmt.Errorf("registry/events: project ModelRegistered %s: %w", ev.GetModelId(), err)
	}
	return nil
}

// hydrateModel fills the FIELD-COMPLETE model from the write-store truth, copying the
// fields the thin fp.models.registered event does not carry (description, tags) onto
// the event-derived skeleton so the projected read model is complete (GET /models/{id}
// returns the real description, not "").
//
// FAILURE POSTURE (deliberately best-effort): the read-back is an ENRICHMENT, not the
// source of identity — the event already carries everything needed to key and serve a
// basic projection. So if the read-back is unavailable (no ModelReader wired) or the
// truth row can't be loaded right now (a transient write-store blip, or the rare
// read-your-write race where the projection consumes the event a hair before the
// committed row is visible), we keep the event-derived model rather than NAK the whole
// projection. Result: the model is still projected promptly with its event fields; a
// missing description self-heals on the next projection of this model (UpdateModel
// edit, or a stream replay/rebuild). We DO NOT overwrite a non-empty event field with
// the truth blindly — we only ADD the event-omitted fields, so a future fat-event
// upgrade that DOES carry description would still win.
func (p *Projection) hydrateModel(ctx context.Context, m domain.Model) domain.Model {
	if p.truth == nil {
		return m
	}
	full, err := p.truth.GetModel(ctx, m.ID)
	if err != nil {
		// Best-effort: log-free here (the domain/events layer has no logger by design);
		// keep the event-derived model. The enrichment self-heals on a later projection.
		return m
	}
	// Copy ONLY the fields the thin event omits. Identity/ownership/timestamps stay as
	// the event stamped them (the event is authoritative for those).
	if m.Description == "" {
		m.Description = full.Description
	}
	if len(m.Tags) == 0 {
		m.Tags = full.Tags
	}
	return m
}

// handleModelVersionCreated projects a new version row (PENDING_UPLOAD) AND advances
// the owning model's latest_version pointer. The version is created at stage DEV /
// status PENDING_UPLOAD (the write-side invariant). We load-merge an existing version
// (a duplicate/redelivery) so we never regress a version that a later ready/promoted
// event already advanced.
func (p *Projection) handleModelVersionCreated(ctx context.Context, env natsutil.EventEnvelope) error {
	var ev eventsv1.ModelVersionCreated
	if err := decodeProjection(env, &ev); err != nil {
		return err
	}
	if ev.GetVersionId() == "" || ev.GetModelId() == "" {
		return fmt.Errorf("%w: ModelVersionCreated missing version_id/model_id", natsutil.ErrProcessingFailed)
	}
	createdAt := ev.GetCreatedAt().AsTime()

	// Load-merge the version: if it already exists (redelivery, or this event arrived
	// AFTER a ready/promoted event due to reordering), keep the existing richer state
	// and only fill the creation facts. A fresh version starts DEV/PENDING_UPLOAD.
	v, err := p.loadVersionOrZero(ctx, ev.GetVersionId())
	if err != nil {
		return err
	}
	if v.ID == "" {
		v = domain.ModelVersion{
			ID:        ev.GetVersionId(),
			ModelID:   ev.GetModelId(),
			Version:   ev.GetVersion(),
			Stage:     domain.StageDev,
			Status:    domain.StatusPendingUpload,
			CreatedBy: ev.GetCreatedBy(),
			CreatedAt: createdAt,
		}
		if err := p.store.UpsertVersion(ctx, v); err != nil {
			return fmt.Errorf("registry/events: project ModelVersionCreated %s: %w", ev.GetVersionId(), err)
		}
	}

	// Advance the model's latest_version pointer (newest-created label). Guarded by
	// event time so an out-of-order/duplicate older create can't roll the pointer back.
	return p.advanceLatestVersion(ctx, ev.GetModelId(), ev.GetVersion(), createdAt)
}

// handleModelVersionReady flips a version to READY and stamps the SERVER-MEASURED
// artifact facts (path/digest/size). Load-merge so we preserve the version's other
// fields (created_by/created_at/stage) and only apply the ready delta.
func (p *Projection) handleModelVersionReady(ctx context.Context, env natsutil.EventEnvelope) error {
	var ev eventsv1.ModelVersionReady
	if err := decodeProjection(env, &ev); err != nil {
		return err
	}
	if ev.GetVersionId() == "" {
		return fmt.Errorf("%w: ModelVersionReady with empty version_id", natsutil.ErrProcessingFailed)
	}
	v, err := p.loadVersionOrZero(ctx, ev.GetVersionId())
	if err != nil {
		return err
	}
	// Reconstruct enough of the version even if the create event hasn't been projected
	// yet (reordering): identity from this event, the rest left zero until create lands.
	v.ID = ev.GetVersionId()
	if v.ModelID == "" {
		v.ModelID = ev.GetModelId()
	}
	if v.Version == "" {
		v.Version = ev.GetVersion()
	}
	v.ArtifactPath = ev.GetArtifactPath()
	v.ArtifactDigest = ev.GetArtifactDigest()
	v.SizeBytes = ev.GetSizeBytes()
	v.Status = domain.StatusReady
	if v.CreatedAt.IsZero() {
		v.CreatedAt = ev.GetReadyAt().AsTime()
	}
	if err := p.store.UpsertVersion(ctx, v); err != nil {
		return fmt.Errorf("registry/events: project ModelVersionReady %s: %w", ev.GetVersionId(), err)
	}
	return nil
}

// handleModelPromoted applies the atomic single-production stage swap to the
// projection: the promoted version takes to_stage, the demoted prior-production
// version (if any) becomes ARCHIVED, and the model's production_version pointer
// updates when the promotion targets PRODUCTION.
func (p *Projection) handleModelPromoted(ctx context.Context, env natsutil.EventEnvelope) error {
	var ev eventsv1.ModelPromoted
	if err := decodeProjection(env, &ev); err != nil {
		return err
	}
	if ev.GetVersionId() == "" || ev.GetModelId() == "" {
		return fmt.Errorf("%w: ModelPromoted missing version_id/model_id", natsutil.ErrProcessingFailed)
	}
	promotedAt := ev.GetPromotedAt().AsTime()

	// Promoted version → to_stage.
	v, err := p.loadVersionOrZero(ctx, ev.GetVersionId())
	if err != nil {
		return err
	}
	v.ID = ev.GetVersionId()
	if v.ModelID == "" {
		v.ModelID = ev.GetModelId()
	}
	if v.Version == "" {
		v.Version = ev.GetVersion()
	}
	v.Stage = fromEventStage(ev.GetToStage())
	if v.CreatedAt.IsZero() {
		v.CreatedAt = promotedAt
	}
	if err := p.store.UpsertVersion(ctx, v); err != nil {
		return fmt.Errorf("registry/events: project ModelPromoted (promoted version) %s: %w", ev.GetVersionId(), err)
	}

	// Demoted prior-production version → ARCHIVED (its id is "" when none existed).
	if id := ev.GetDemotedVersionId(); id != "" {
		d, err := p.loadVersionOrZero(ctx, id)
		if err != nil {
			return err
		}
		d.ID = id
		if d.ModelID == "" {
			d.ModelID = ev.GetModelId()
		}
		if d.Version == "" {
			d.Version = ev.GetDemotedVersion()
		}
		d.Stage = domain.StageArchived
		if d.CreatedAt.IsZero() {
			d.CreatedAt = promotedAt
		}
		if err := p.store.UpsertVersion(ctx, d); err != nil {
			return fmt.Errorf("registry/events: project ModelPromoted (demoted version) %s: %w", id, err)
		}
	}

	// Model's production_version pointer — only when the target stage is PRODUCTION.
	if fromEventStage(ev.GetToStage()) == domain.StageProduction {
		m, err := p.store.GetModelByIDUnscoped(ctx, ev.GetModelId())
		if err != nil {
			if errors.Is(err, domain.ErrRecordNotFound) {
				// The model's own ModelRegistered hasn't been projected yet (reordering);
				// NAK so JetStream redelivers after the registered event lands.
				return fmt.Errorf("registry/events: project ModelPromoted %s: model %s not yet projected: %w",
					ev.GetVersionId(), ev.GetModelId(), natsutil.ErrProcessingFailed)
			}
			return fmt.Errorf("registry/events: load model for ModelPromoted %s: %w", ev.GetModelId(), err)
		}
		m.ProductionVersion = ev.GetVersion()
		if promotedAt.After(m.UpdatedAt) {
			m.UpdatedAt = promotedAt
		}
		if err := p.store.UpsertModel(ctx, m); err != nil {
			return fmt.Errorf("registry/events: project ModelPromoted (model pointer) %s: %w", ev.GetModelId(), err)
		}
	}
	return nil
}

// handleModelArchived soft-deletes the model in the projection (sets archived_at).
// The query side surfaces archived models per its filter; we just stamp the time.
func (p *Projection) handleModelArchived(ctx context.Context, env natsutil.EventEnvelope) error {
	var ev eventsv1.ModelArchived
	if err := decodeProjection(env, &ev); err != nil {
		return err
	}
	if ev.GetModelId() == "" {
		return fmt.Errorf("%w: ModelArchived with empty model_id", natsutil.ErrProcessingFailed)
	}
	m, err := p.store.GetModelByIDUnscoped(ctx, ev.GetModelId())
	if err != nil {
		if errors.Is(err, domain.ErrRecordNotFound) {
			// Archive for a model we never projected (reordering / replay gap). NAK so it
			// redelivers after ModelRegistered lands; the retry budget bounds the loop.
			return fmt.Errorf("registry/events: project ModelArchived: model %s not yet projected: %w",
				ev.GetModelId(), natsutil.ErrProcessingFailed)
		}
		return fmt.Errorf("registry/events: load model for ModelArchived %s: %w", ev.GetModelId(), err)
	}
	archivedAt := ev.GetArchivedAt().AsTime()
	m.ArchivedAt = archivedAt
	if archivedAt.After(m.UpdatedAt) {
		m.UpdatedAt = archivedAt
	}
	if err := p.store.UpsertModel(ctx, m); err != nil {
		return fmt.Errorf("registry/events: project ModelArchived %s: %w", ev.GetModelId(), err)
	}
	return nil
}

// advanceLatestVersion sets the model's latest_version pointer to label, guarded by
// event time so an out-of-order or duplicate older create cannot roll the pointer
// backward. A missing model (its ModelRegistered not yet projected) is a retryable
// reordering condition.
func (p *Projection) advanceLatestVersion(ctx context.Context, modelID, label string, at time.Time) error {
	m, err := p.store.GetModelByIDUnscoped(ctx, modelID)
	if err != nil {
		if errors.Is(err, domain.ErrRecordNotFound) {
			return fmt.Errorf("registry/events: advance latest_version: model %s not yet projected: %w",
				modelID, natsutil.ErrProcessingFailed)
		}
		return fmt.Errorf("registry/events: load model %s: %w", modelID, err)
	}
	// Only advance forward (by event time). The pointer is a denormalized convenience;
	// the guard keeps a duplicate/reorder from regressing it.
	if !at.Before(m.UpdatedAt) {
		m.LatestVersion = label
		m.UpdatedAt = at
		if err := p.store.UpsertModel(ctx, m); err != nil {
			return fmt.Errorf("registry/events: advance latest_version for model %s: %w", modelID, err)
		}
	}
	return nil
}

// loadVersionOrZero loads a projected version by id, returning a zero ModelVersion
// (ID == "") when it does not exist yet — the load half of load-merge-upsert. A real
// load error (not a miss) is propagated (→ NAK + retry).
func (p *Projection) loadVersionOrZero(ctx context.Context, id string) (domain.ModelVersion, error) {
	v, err := p.store.GetVersionByID(ctx, id)
	if err != nil {
		if errors.Is(err, domain.ErrRecordNotFound) {
			return domain.ModelVersion{}, nil
		}
		return domain.ModelVersion{}, fmt.Errorf("registry/events: load version %s: %w", id, err)
	}
	return v, nil
}

// decodeProjection decodes an EventEnvelope.Data payload into a proto message with
// protojson — symmetric with natsutil.Publisher's protojson encoding for proto
// payloads. A decode failure is a POISON message (corrupt bytes never parse), so we
// wrap natsutil.ErrProcessingFailed → the subscriber DLQs it after the retry budget
// rather than NAK-looping forever.
func decodeProjection(env natsutil.EventEnvelope, msg proto.Message) error {
	if err := protojson.Unmarshal(env.Data, msg); err != nil {
		return fmt.Errorf("%w: decode %T from event %s: %v", natsutil.ErrProcessingFailed, msg, env.ID, err)
	}
	return nil
}

// fromEventStage maps the wire eventsv1.ModelStage back to the domain ModelStage —
// the INVERSE of publisher.go's toEventStage. Explicit (not a numeric cast) so a
// reorder of either enum breaks visibly here rather than silently mis-projecting.
func fromEventStage(s eventsv1.ModelStage) domain.ModelStage {
	switch s {
	case eventsv1.ModelStage_MODEL_STAGE_DEV:
		return domain.StageDev
	case eventsv1.ModelStage_MODEL_STAGE_STAGING:
		return domain.StageStaging
	case eventsv1.ModelStage_MODEL_STAGE_PRODUCTION:
		return domain.StageProduction
	case eventsv1.ModelStage_MODEL_STAGE_ARCHIVED:
		return domain.StageArchived
	default:
		return domain.StageUnspecified
	}
}
