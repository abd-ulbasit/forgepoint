package events

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/services/billing/internal/domain"
)

// ============================================================================
// INFERENCE CONSUMER — meter usage by reacting to fp.inference.completed
// ============================================================================
//
// Billing does not poll the gateway for traffic; it REACTS to the single canonical
// inference fact (InferenceCompleted, owned by the inference-gateway — see the
// event contract's "one inference event" conflict resolution). For each completed
// prediction this consumer calls domain.RecordUsage, which prices it, writes the
// ledger row, AND enqueues a UsageRecorded outbox event in one tx — so the loop
// closes: consume InferenceCompleted → meter → (relay) publish UsageRecorded.
//
// WHAT WE METER per event:
//   - ALWAYS one METER_TYPE_INFERENCE_REQUEST, quantity 1 (the per-call fee).
//   - WHEN token_count > 0, ALSO METER_TYPE_INFERENCE_TOKENS, quantity = token_count
//     (the dominant LLM cost axis). 0 tokens → no token record (non-token models).
//   Each axis is a SEPARATE UsageRecord (the ledger atom is one meter), so a single
//   event can produce up to two RecordUsage calls.
//
// IDEMPOTENCY (every consumer must survive duplicate redelivery — the contract):
//   THREE layers guarantee a duplicate event meters AT MOST ONCE per axis:
//     (a) TRANSPORT: natsutil's ProcessedStore dedupes on EventEnvelope.id — a
//         redelivered envelope id is ACKed WITHOUT re-running the handler. Wired via
//         WithIdempotencyStore on the subscription.
//     (b) BUSINESS: each RecordUsage call carries an IdempotencyKey derived from the
//         event's request_id (the per-call key, and request_id+":tokens" for the
//         token axis). domain.RecordUsage dedupes on (team, idempotency_key): a
//         repeat returns the ORIGINAL record and inserts nothing. So even a
//         redelivery AFTER the dedup window — or to a different replica with a
//         separate ProcessedStore — meters the request exactly once per axis.
//     (c) The two axes use DISTINCT keys (request_id vs request_id+":tokens") so the
//         request fee and the token fee don't collide on one key and suppress each
//         other.
//   Belt-and-suspenders: (a) makes the common case cheap (no DB round-trip on a
//   dup); (b) makes correctness independent of the store's reach. This is the
//   textbook "idempotent consumer over at-least-once delivery" answer.
//
// PARTIAL-FAILURE / RETRY SAFETY: if the request-fee RecordUsage SUCCEEDS but the
// token RecordUsage then FAILS (DB blip), the handler returns an error → natsutil
// NAKs → the WHOLE event is redelivered. On redelivery the request fee is deduped
// by its idempotency key (no double-charge) and the token fee is retried. So a
// retry never double-bills the part that already committed — the business
// idempotency key makes the multi-write handler safe to replay.
//
// DLQ (poison messages): an event that can never be metered (corrupt payload,
// unresolvable api_key, an UNSPECIFIED-meter mapping bug) would otherwise loop
// forever. WithMaxRetries + WithDLQSubject park it on a dead-letter subject after
// the cap so redelivery stops and ops can inspect/replay. A genuinely TRANSIENT
// failure (DB down, team-resolver timeout) is returned UNWRAPPED so it NAKs and
// retries; only a permanently-unprocessable event is wrapped as poison.
// ============================================================================

// TeamResolver maps an inference api_key_id to the SERVER-AUTHORITATIVE billed team.
//
// WHY THIS IS A PORT (and why team can't come from the event as a plain field):
// the billed team is the account that PAYS — naming it is account-takeover-for-
// money, so it must be server-derived, never client-asserted. The gateway stamps
// api_key_id (a reference, never the secret) on InferenceCompleted; resolving that
// key to its owning team is an AUTH/identity concern billing doesn't own. We depend
// on this narrow interface (not a concrete auth client) so:
//   - the consumer is unit-testable with a fake resolver (no auth service needed),
//   - the resolution mechanism (an Auth gRPC GetAPIKey call, a cached lookup, a
//     read replica) is a wiring decision for main.go (next stage), not baked in.
//
// A resolver that returns ErrTeamNotFound for an unknown/revoked key makes the
// event POISON (we cannot bill an unknown principal) → DLQ, not an infinite retry.
type TeamResolver interface {
	// ResolveTeam returns the team that owns apiKeyID. It returns ErrTeamNotFound
	// when the key is unknown/revoked (a permanent condition → poison), or any other
	// error for a TRANSIENT failure (→ NAK + retry).
	ResolveTeam(ctx context.Context, apiKeyID string) (team string, err error)
}

// ErrTeamNotFound is the resolver's sentinel for "no team owns this api key"
// (unknown or revoked). The consumer treats it as a PERMANENT failure (poison →
// DLQ) — retrying will never make an unknown key resolve.
var ErrTeamNotFound = errors.New("events: team not found for api key")

// SubConfig tunes the consumer's resilience knobs. Zero values get platform-default
// values via withDefaults so main.go can pass SubConfig{} for standard behavior.
type SubConfig struct {
	// ConsumerGroup is the durable name; all billing replicas sharing it form a
	// consumer group (each event metered by exactly ONE replica — never double-
	// billed across replicas). Default: Source ("billing").
	ConsumerGroup string
	// MaxRetries is redeliveries before DLQ (total attempts = MaxRetries+1). Default: 4.
	MaxRetries int
	// DLQSubject parks poison events. Default: "fp.dlq.billing".
	DLQSubject string
	// MessageTimeout bounds one handler invocation (it may do up to two DB writes +
	// a resolver call). Default: 15s.
	MessageTimeout time.Duration
}

func (c *SubConfig) withDefaults() {
	if c.ConsumerGroup == "" {
		c.ConsumerGroup = Source
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = 4
	}
	if c.DLQSubject == "" {
		c.DLQSubject = "fp.dlq." + Source
	}
	if c.MessageTimeout == 0 {
		c.MessageTimeout = 15 * time.Second
	}
}

// InferenceConsumer subscribes to fp.inference.completed and meters each event.
type InferenceConsumer struct {
	svc   domain.BillingService
	teams TeamResolver
	store natsutil.ProcessedStore
	cfg   SubConfig
	js    jetstream.JetStream
	sub   *natsutil.Subscriber
}

// NewInferenceConsumer builds the consumer. The caller (main.go, next stage)
// provides the JetStream context, the domain BillingService (whose RecordUsage does
// the metering + outbox write), the TeamResolver (api_key → team), and the
// idempotency ProcessedStore (a shared Redis/Postgres store in prod; a memory store
// in tests).
func NewInferenceConsumer(
	js jetstream.JetStream,
	svc domain.BillingService,
	teams TeamResolver,
	store natsutil.ProcessedStore,
	cfg SubConfig,
) *InferenceConsumer {
	cfg.withDefaults()
	return &InferenceConsumer{svc: svc, teams: teams, store: store, cfg: cfg, js: js}
}

// Register starts the subscription. Call exactly once (calling twice in one process
// double-consumes locally). The ctx governs the consume loop's lifetime alongside
// Close().
func (c *InferenceConsumer) Register(ctx context.Context) error {
	sub := natsutil.NewSubscriber(c.js,
		natsutil.WithConsumerGroup(c.cfg.ConsumerGroup),
		natsutil.WithMaxRetries(c.cfg.MaxRetries),
		natsutil.WithDLQSubject(c.cfg.DLQSubject),
		natsutil.WithIdempotencyStore(c.store),
		natsutil.WithMessageTimeout(c.cfg.MessageTimeout),
	)
	if err := sub.Subscribe(ctx, StreamInference, SubjectInferenceCompleted, c.handleInferenceCompleted); err != nil {
		return fmt.Errorf("events: subscribe %s on %s: %w", SubjectInferenceCompleted, StreamInference, err)
	}
	c.sub = sub
	return nil
}

// Close stops the consume loop. Safe to call multiple times and before/after
// Register (a nil sub is a no-op).
func (c *InferenceConsumer) Close() {
	if c.sub != nil {
		c.sub.Close()
	}
}

// handleInferenceCompleted decodes the event, resolves the billed team, and meters
// the request fee (always) and the token fee (when token_count > 0).
func (c *InferenceConsumer) handleInferenceCompleted(ctx context.Context, env natsutil.EventEnvelope) error {
	var ev eventsv1.InferenceCompleted
	// protojson (NOT encoding/json): InferenceCompleted is a forgepoint/events/v1
	// PROTO message and the gateway publishes it via natsutil.Publisher, which now
	// marshals proto payloads with protojson (the canonical proto-JSON dialect).
	// Its completed_at is a google.protobuf.Timestamp — an RFC-3339 string on the
	// wire that ONLY protojson decodes; a plain json.Unmarshal would fail or
	// silently zero it. Symmetric publish/consume = no spurious DLQ.
	if err := protojson.Unmarshal(env.Data, &ev); err != nil {
		// Corrupt payload — redelivery can't fix it → poison → DLQ.
		return fmt.Errorf("%w: decode InferenceCompleted from event %s: %v",
			natsutil.ErrProcessingFailed, env.ID, err)
	}

	// request_id is the gateway-minted, SERVER-authoritative business idempotency
	// key. An event without one cannot be safely deduped → treat as poison rather
	// than risk double-metering a re-delivery.
	requestID := ev.GetRequestId()
	if requestID == "" {
		return fmt.Errorf("%w: InferenceCompleted with empty request_id (no idempotency anchor)",
			natsutil.ErrProcessingFailed)
	}

	// api_key_id is required to resolve the billed team. Absent → we cannot attribute
	// the charge → poison.
	apiKeyID := ev.GetApiKeyId()
	if apiKeyID == "" {
		return fmt.Errorf("%w: InferenceCompleted %s with empty api_key_id (cannot attribute charge)",
			natsutil.ErrProcessingFailed, requestID)
	}

	// Resolve the SERVER-AUTHORITATIVE team. ErrTeamNotFound is permanent (unknown/
	// revoked key) → poison; any other error is transient (resolver down) → NAK+retry
	// (returned unwrapped so it is NOT classified as poison).
	team, err := c.teams.ResolveTeam(ctx, apiKeyID)
	if err != nil {
		if errors.Is(err, ErrTeamNotFound) {
			return fmt.Errorf("%w: no team for api_key on InferenceCompleted %s",
				natsutil.ErrProcessingFailed, requestID)
		}
		return fmt.Errorf("events: resolve team for InferenceCompleted %s: %w", requestID, err)
	}

	occurredAt := ev.GetCompletedAt().AsTime() // zero-value timestamp → zero time → domain stamps now()

	// ----- METER 1: the per-call request fee (always) -----
	// idempotency key = request_id: a redelivery of this event re-attempts the same
	// key and domain.RecordUsage returns the original record (no double-charge).
	reqInput := domain.RecordUsageInput{
		MeterType:       domain.MeterTypeInferenceRequest,
		Quantity:        1,
		ModelID:         ev.GetModelId(),
		ModelVersion:    ev.GetVersion(),
		SourceRequestID: requestID,
		OccurredAt:      occurredAt,
		IdempotencyKey:  requestID,
	}
	if err := c.meter(ctx, team, reqInput); err != nil {
		return err
	}

	// ----- METER 2: tokens (only when the model reported a token count) -----
	// Non-token models report 0 → no token record. The token axis uses a DISTINCT
	// idempotency key (request_id + ":tokens") so it doesn't collide with the request
	// fee's key — both must be independently dedupable on redelivery.
	if tokens := ev.GetTokenCount(); tokens > 0 {
		tokInput := domain.RecordUsageInput{
			MeterType:       domain.MeterTypeInferenceTokens,
			Quantity:        tokens,
			ModelID:         ev.GetModelId(),
			ModelVersion:    ev.GetVersion(),
			SourceRequestID: requestID,
			OccurredAt:      occurredAt,
			IdempotencyKey:  requestID + ":tokens",
		}
		if err := c.meter(ctx, team, tokInput); err != nil {
			return err
		}
	}

	return nil
}

// meter calls domain.RecordUsage and classifies the outcome for the subscriber's
// ACK/NAK/DLQ machinery:
//   - nil / deduplicated      → success (ACK).
//   - a VALIDATION/pricing error that a retry can NEVER fix (unknown meter, no rate
//     plan for the team, quantity-too-large) → POISON (wrap ErrProcessingFailed →
//     DLQ). We can't bill this event no matter how many times we retry.
//   - any other error (DB down, tx failure) → TRANSIENT (return unwrapped → NAK +
//     retry).
//
// WHY map domain errors here (not let them all NAK): a NO-RATE-PLAN or
// UNKNOWN-METER condition is structural — retrying floods the logs and never
// succeeds. Routing it to the DLQ surfaces it for an operator (assign the team a
// plan, then replay) instead of an infinite NAK loop. A DB outage, by contrast, IS
// transient and SHOULD retry.
func (c *InferenceConsumer) meter(ctx context.Context, team string, input domain.RecordUsageInput) error {
	_, _, err := c.svc.RecordUsage(ctx, team, input)
	if err == nil {
		return nil
	}
	if isPermanentBillingError(err) {
		return fmt.Errorf("%w: meter %s for team=%s: %v",
			natsutil.ErrProcessingFailed, input.MeterType, team, err)
	}
	// Transient — NAK + retry (the request fee, if already committed on a prior
	// attempt, is deduped by its idempotency key, so a retry never double-bills).
	return fmt.Errorf("events: meter %s for team=%s: %w", input.MeterType, team, err)
}

// isPermanentBillingError reports whether a RecordUsage error is structurally
// unfixable by retry (→ poison → DLQ). These are the domain's
// InvalidArgument/FailedPrecondition-class sentinels: a corrupt/unbillable event,
// not a transient infra fault. ErrAmountOverflow is included as a belt-and-
// suspenders (it should be unreachable given the quantity bound, but if it ever
// fires it is deterministic — retrying won't help).
func isPermanentBillingError(err error) bool {
	return errors.Is(err, domain.ErrUnknownMeter) ||
		errors.Is(err, domain.ErrNegativeQuantity) ||
		errors.Is(err, domain.ErrQuantityTooLarge) ||
		errors.Is(err, domain.ErrRatePlanNotFound) ||
		errors.Is(err, domain.ErrValidation) ||
		errors.Is(err, domain.ErrAmountOverflow)
}

// ============================================================================
// STORAGE CONSUMER — meter STORAGE_BYTES by reacting to fp.models.version.ready
// ============================================================================
//
// The event contract (docs/design/event-contract.md) lists billing as a consumer
// of fp.models.version.ready for STORAGE metering, alongside fp.inference.completed
// for inference metering. Inference is the per-CALL money axis; storage is the
// per-ARTIFACT money axis. The registry publishes ModelVersionReady when an
// artifact upload is verified and the version flips PENDING_UPLOAD → READY,
// stamping the SERVER-MEASURED size_bytes — the authoritative storage fact. This
// consumer maps that size_bytes → one RecordUsage(STORAGE_BYTES) call.
//
// WHY A SEPARATE CONSUMER (not a branch in the inference consumer): the two
// subjects live on DIFFERENT JetStream streams (INFERENCE vs MODELS), so they need
// distinct durable consumers bound to distinct streams. They also differ in
// attribution: inference attributes by api_key_id (→ TeamResolver), whereas a model
// version's owning team is a different lookup. Splitting them keeps each consumer's
// failure/retry semantics independent (a registry outage can't block inference
// metering and vice-versa) and each handler small and single-purpose.
//
// TEAM ATTRIBUTION — the open seam: a model version's billed team is the team that
// OWNS the model. ModelVersionReady does NOT currently carry an owner_team field
// (it carries model_id/version_id/size_bytes — see the proto), so resolving the
// owning team is a registry/auth lookup the same way the inference path resolves
// api_key → team. We model that as a VersionTeamResolver port (mirroring
// TeamResolver) so this consumer is unit-testable with a fake and main.go decides
// the concrete resolver. Until the real resolver is wired, main.go injects a
// fail-closed placeholder (transient error → NAK + retry, NEVER guess a team) —
// the same money-safe posture as the inference path.
//
// IDEMPOTENCY: one ready-event = one storage record. The business idempotency key
// is version_id + ":storage" (a version becomes READY once; the +":storage" suffix
// keeps the storage axis from ever colliding with an inference key that happened to
// equal a version_id). A redelivery re-attempts the same key and domain.RecordUsage
// returns the original record — no double-charge. The transport ProcessedStore
// dedupes on the envelope id as the cheap fast-path, exactly as the inference path.

// VersionTeamResolver maps a model version to the SERVER-AUTHORITATIVE team that
// OWNS the model and therefore pays for its stored artifact.
//
// WHY A PORT (and why the team can't be a plain field on the event): the billed
// team is the account that PAYS — server-derived, never client/event-asserted (the
// same trust-boundary rule as TeamResolver). ModelVersionReady identifies the model
// (model_id/version_id); resolving that to the owning team is a registry/identity
// concern billing does not own. We depend on this narrow interface so the consumer
// is unit-testable with a fake and the resolution mechanism (a registry gRPC call,
// a cached lookup) is a wiring decision for main.go.
//
// A resolver that returns ErrTeamNotFound for an unknown model makes the event
// POISON (we cannot bill an unknown owner) → DLQ, not an infinite retry — exactly
// like the inference path's ErrTeamNotFound.
type VersionTeamResolver interface {
	// ResolveVersionTeam returns the team that owns the model identified by
	// modelID (versionID is passed for logging/lineage and future per-version
	// ownership). It returns ErrTeamNotFound when the model is unknown (permanent →
	// poison), or any other error for a TRANSIENT failure (→ NAK + retry).
	ResolveVersionTeam(ctx context.Context, modelID, versionID string) (team string, err error)
}

// StorageConsumer subscribes to fp.models.version.ready and meters STORAGE_BYTES.
type StorageConsumer struct {
	svc   domain.BillingService
	teams VersionTeamResolver
	store natsutil.ProcessedStore
	cfg   SubConfig
	js    jetstream.JetStream
	sub   *natsutil.Subscriber
}

// NewStorageConsumer builds the storage-metering consumer. Same shape as
// NewInferenceConsumer: the caller injects the JetStream context, the domain
// BillingService, the VersionTeamResolver (model → owning team), and the
// idempotency ProcessedStore. SubConfig zero-values get platform defaults via
// withDefaults (ConsumerGroup defaults to "billing"; we override it below so this
// consumer does not collide with the inference consumer's durable name).
func NewStorageConsumer(
	js jetstream.JetStream,
	svc domain.BillingService,
	teams VersionTeamResolver,
	store natsutil.ProcessedStore,
	cfg SubConfig,
) *StorageConsumer {
	// Distinct durable name from the inference consumer. Two durable consumers in
	// ONE process cannot share a name (they would fight over the same JetStream
	// consumer); the inference consumer defaults to Source ("billing"), so the
	// storage consumer defaults to "billing-storage". A caller may still override.
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = Source + "-storage"
	}
	cfg.withDefaults()
	return &StorageConsumer{svc: svc, teams: teams, store: store, cfg: cfg, js: js}
}

// Register starts the subscription on the MODELS stream filtered to
// fp.models.version.ready. Call exactly once.
func (c *StorageConsumer) Register(ctx context.Context) error {
	sub := natsutil.NewSubscriber(c.js,
		natsutil.WithConsumerGroup(c.cfg.ConsumerGroup),
		natsutil.WithMaxRetries(c.cfg.MaxRetries),
		natsutil.WithDLQSubject(c.cfg.DLQSubject),
		natsutil.WithIdempotencyStore(c.store),
		natsutil.WithMessageTimeout(c.cfg.MessageTimeout),
	)
	if err := sub.Subscribe(ctx, StreamModels, SubjectModelVersionReady, c.handleModelVersionReady); err != nil {
		return fmt.Errorf("events: subscribe %s on %s: %w", SubjectModelVersionReady, StreamModels, err)
	}
	c.sub = sub
	return nil
}

// Close stops the consume loop. Safe to call multiple times and before/after
// Register (a nil sub is a no-op).
func (c *StorageConsumer) Close() {
	if c.sub != nil {
		c.sub.Close()
	}
}

// handleModelVersionReady decodes the event, resolves the owning team, and meters
// the artifact's storage bytes.
func (c *StorageConsumer) handleModelVersionReady(ctx context.Context, env natsutil.EventEnvelope) error {
	var ev eventsv1.ModelVersionReady
	// protojson (NOT encoding/json): ModelVersionReady is a proto message the
	// registry publishes via natsutil.Publisher (now protojson-encoded). ready_at
	// is a google.protobuf.Timestamp (RFC-3339 string on the wire), so the decode
	// must be protojson to stay symmetric with the publisher.
	if err := protojson.Unmarshal(env.Data, &ev); err != nil {
		// Corrupt payload — redelivery can't fix it → poison → DLQ.
		return fmt.Errorf("%w: decode ModelVersionReady from event %s: %v",
			natsutil.ErrProcessingFailed, env.ID, err)
	}

	// version_id is the storage idempotency anchor (a version becomes READY once).
	// Absent → we cannot safely dedupe a redelivery → poison.
	versionID := ev.GetVersionId()
	if versionID == "" {
		return fmt.Errorf("%w: ModelVersionReady with empty version_id (no idempotency anchor)",
			natsutil.ErrProcessingFailed)
	}

	// model_id is required to attribute the storage charge to an owning team.
	modelID := ev.GetModelId()
	if modelID == "" {
		return fmt.Errorf("%w: ModelVersionReady %s with empty model_id (cannot attribute storage charge)",
			natsutil.ErrProcessingFailed, versionID)
	}

	// size_bytes is the metered quantity. A non-positive size is structurally
	// unbillable (there is nothing to meter, and a NEGATIVE size would be rejected
	// by the domain's non-negative-quantity invariant anyway). Treat <= 0 as a
	// no-op ACK rather than poison: an empty/zero artifact is not an ERROR, it just
	// has no storage to charge — looping or DLQ'ing it would be wrong. We ACK so the
	// event is consumed and not redelivered.
	sizeBytes := ev.GetSizeBytes()
	if sizeBytes <= 0 {
		return nil
	}

	// Resolve the SERVER-AUTHORITATIVE owning team. ErrTeamNotFound is permanent
	// (unknown model) → poison; any other error is transient (resolver down) →
	// NAK+retry (returned unwrapped so it is NOT classified as poison).
	team, err := c.teams.ResolveVersionTeam(ctx, modelID, versionID)
	if err != nil {
		if errors.Is(err, ErrTeamNotFound) {
			return fmt.Errorf("%w: no team for model on ModelVersionReady %s",
				natsutil.ErrProcessingFailed, versionID)
		}
		return fmt.Errorf("events: resolve team for ModelVersionReady %s: %w", versionID, err)
	}

	occurredAt := ev.GetReadyAt().AsTime() // zero-value timestamp → zero time → domain stamps now()

	// ----- METER: storage bytes -----
	// idempotency key = version_id + ":storage": a redelivery re-attempts the same
	// key and domain.RecordUsage returns the original record (no double-charge). The
	// ":storage" suffix guarantees the key never collides with an inference key.
	input := domain.RecordUsageInput{
		MeterType:       domain.MeterTypeStorageBytes,
		Quantity:        sizeBytes,
		ModelID:         modelID,
		ModelVersion:    ev.GetVersion(),
		SourceRequestID: versionID,
		OccurredAt:      occurredAt,
		IdempotencyKey:  versionID + ":storage",
	}
	return c.meter(ctx, team, input)
}

// meter calls domain.RecordUsage and classifies the outcome for the subscriber's
// ACK/NAK/DLQ machinery — IDENTICAL classification to the inference consumer's
// meter (permanent billing errors → poison/DLQ; transient infra faults → NAK +
// retry). Shared via the package-level isPermanentBillingError so both axes route
// errors the same way.
func (c *StorageConsumer) meter(ctx context.Context, team string, input domain.RecordUsageInput) error {
	_, _, err := c.svc.RecordUsage(ctx, team, input)
	if err == nil {
		return nil
	}
	if isPermanentBillingError(err) {
		return fmt.Errorf("%w: meter %s for team=%s: %v",
			natsutil.ErrProcessingFailed, input.MeterType, team, err)
	}
	return fmt.Errorf("events: meter %s for team=%s: %w", input.MeterType, team, err)
}
