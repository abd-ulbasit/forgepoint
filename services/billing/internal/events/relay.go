package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/services/billing/internal/domain"
)

// ============================================================================
// OUTBOX RELAY — the DB→NATS half of the transactional outbox (the centerpiece)
// ============================================================================
//
// THE PROBLEM the outbox solves (the dual-write problem): RecordUsage must both
// (1) persist a priced UsageRecord and (2) publish a UsageRecorded event. There is
// no distributed transaction spanning Postgres and NATS, so we cannot make those
// two atomic. If we published to NATS inside the request and then the DB commit
// failed, downstream would learn of a charge that never happened (a phantom
// event); if we committed the row and then the publish failed, the charge is
// invisible to the rest of the platform (a lost event). Either way the bus and the
// ledger disagree.
//
// THE FIX (this relay completes it): the write path commits a PUBLISH INTENT — an
// `outbox` row — in the SAME tx as the business row (see usage_store.go's
// RecordUsageTx). So after the tx commits, the event is GUARANTEED to exist as a
// durable row even though it hasn't hit NATS yet. This relay is the asynchronous
// loop that drains those rows:
//
//	┌─ relay tick (every PollInterval) ───────────────────────────────────────┐
//	│ 1. claim a batch of UNPUBLISHED rows, oldest-first                       │
//	│      SELECT … WHERE published_at IS NULL ORDER BY created_at LIMIT N     │
//	│ 2. for each row: rebuild the canonical events.v1 payload from the stored │
//	│    JSONB, publish it to the row's event_type subject (natsutil stamps    │
//	│    the envelope id = the OUTBOX ROW id — the consumer dedupe key)        │
//	│ 3. mark the row published   UPDATE … SET published_at = now()           │
//	└──────────────────────────────────────────────────────────────────────────┘
//
// DELIVERY SEMANTICS — AT-LEAST-ONCE, never at-most-once:
//   The publish (step 2) and the mark (step 3) are NOT atomic. If the process
//   crashes after publishing but before marking, the row stays unpublished and the
//   NEXT tick republishes it. That is a DUPLICATE, which is safe because:
//     (a) natsutil sets Nats-Msg-Id = the outbox row id, so a republish within
//         JetStream's dedup window is collapsed at the BROKER, and
//     (b) every consumer dedupes on EventEnvelope.id (== the outbox row id) via a
//         ProcessedStore, and additionally on a BUSINESS key (UsageRecorded carries
//         source_request_id; Billing's own RecordUsage dedupes on idempotency_key).
//   So at-least-once delivery + idempotent consumers = exactly-once IN EFFECT. We
//   deliberately mark AFTER a successful publish (publish-then-mark), which biases
//   to at-least-once; the reverse (mark-then-publish) would risk at-most-once (a
//   crash between mark and publish loses the event forever) — the unacceptable
//   direction for a billing event.
//
// WHY POLLING (not LISTEN/NOTIFY): a simple, crash-safe poll is the canonical
// outbox relay (Debezium-style CDC is the heavier alternative). Polling a PARTIAL
// index on the unpublished set (outbox_unpublished_idx, see migrations) is cheap —
// the index only contains rows still needing work, so the claim query stays O(batch).
// LISTEN/NOTIFY could lower latency but adds a missed-notification failure mode
// (a notify fires while no relay is connected → the row waits for the next poll
// anyway), so polling must back it regardless; we keep just the poll for simplicity.
//
// SINGLE-WRITER NOTE ("what if two relay replicas run?"): this
// relay claims with a plain SELECT, so two replicas could both publish the same
// row → two publishes. That is still SAFE (dedup as above) but wasteful. The
// production hardening is `FOR UPDATE SKIP LOCKED` on the claim (each replica grabs
// a disjoint batch) — documented on OutboxReader.ClaimUnpublished as the intended
// upgrade. For the platform's current single-relay deployment the plain claim is
// correct and simplest; the dedup guarantee means even the multi-replica race
// never DOUBLE-BILLS, it only double-PUBLISHES (which the consumer collapses).
// ============================================================================

// OutboxRow is the relay's view of one durable publish intent read back from the
// `outbox` table. It is intentionally a DIFFERENT shape from domain.OutboxEvent:
// the domain type carries a typed Payload (built at write time); the relay reads
// the row back as the discriminating event_type + the stored JSONB bytes, and
// reconstructs the typed payload itself. id becomes the EventEnvelope.id.
type OutboxRow struct {
	ID        string // outbox row id → EventEnvelope.id (consumer dedupe key)
	EventType string // "fp.billing.usage.recorded", … — both the discriminator AND the subject
	Payload   []byte // the JSONB body marshaled by the repository's marshalOutboxPayload
	CreatedAt time.Time
}

// OutboxReader is the relay's narrow persistence PORT: claim unpublished rows and
// mark a row published. The Postgres adapter (PgxOutboxReader, below) implements
// it; tests inject a fake to drive the relay without a database.
//
// WHY a port and not a direct *pgxpool.Pool dependency: the relay's LOGIC (rebuild
// payload → publish → mark) is the interesting, testable part; the SQL is a
// mechanical adapter. Depending on this interface lets a test assert "the relay
// publishes every claimed row to the right subject and marks exactly the published
// ones" with an in-memory fake — no container needed for the loop's logic — while
// the real wiring passes the pgx-backed reader.
type OutboxReader interface {
	// ClaimUnpublished returns up to `limit` unpublished rows, OLDEST FIRST (stable
	// FIFO-ish drain). PRODUCTION UPGRADE for multi-replica relays: add
	// `FOR UPDATE SKIP LOCKED` so concurrent relays claim disjoint batches; the
	// plain SELECT here is correct for a single relay and safe (never double-bills)
	// even if two run, by the dedup guarantee in the file header.
	ClaimUnpublished(ctx context.Context, limit int) ([]OutboxRow, error)
	// MarkPublished stamps published_at = now() for the row id, removing it from the
	// unpublished set (and from the partial index). Called only AFTER a successful
	// publish (publish-then-mark → at-least-once).
	MarkPublished(ctx context.Context, id string) error
}

// natsPublisher is the minimal slice of *natsutil.Publisher the relay needs.
// Depending on the interface (not the concrete type) lets tests inject a fake to
// capture (subject, payload) without a broker; real wiring passes a
// *natsutil.Publisher constructed with source = Source.
type natsPublisher interface {
	Publish(ctx context.Context, subject string, payload any) error
}

// RelayConfig tunes the relay loop. Zero values get sensible defaults via
// withDefaults so main.go can pass RelayConfig{} for the standard behavior.
type RelayConfig struct {
	// PollInterval is how often the relay scans for unpublished rows when the last
	// scan drained the table. Default: 1s — low enough that an event's publish
	// latency is sub-second in the common case, high enough not to hammer Postgres.
	PollInterval time.Duration
	// BatchSize bounds rows claimed per tick (and thus per transaction's worth of
	// publishes). Default: 100 — a balance between throughput and not starving the
	// pool / holding a long publish burst.
	BatchSize int
}

func (c *RelayConfig) withDefaults() {
	if c.PollInterval <= 0 {
		c.PollInterval = time.Second
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
}

// OutboxRelay is the running relay: it owns the reader port, the publisher, and
// the loop configuration.
type OutboxRelay struct {
	reader OutboxReader
	pub    natsPublisher
	cfg    RelayConfig
}

// NewOutboxRelay builds the relay. The caller (main.go, next stage) provides the
// pgx-backed OutboxReader (NewPgxOutboxReader over the shared pool) and a
// *natsutil.Publisher built with source = Source. Tests pass fakes.
func NewOutboxRelay(reader OutboxReader, pub natsPublisher, cfg RelayConfig) *OutboxRelay {
	cfg.withDefaults()
	return &OutboxRelay{reader: reader, pub: pub, cfg: cfg}
}

// Run drives the relay until ctx is cancelled. It is the long-lived goroutine the
// composition root starts (`go relay.Run(ctx)`). The loop is DRAIN-THEN-WAIT: each
// tick fully drains the backlog in BatchSize chunks until a partial batch signals
// the table is empty, then sleeps PollInterval. This keeps latency low under load
// (it doesn't sleep between full batches) without busy-spinning when idle.
//
// Run returns ctx.Err() on cancellation so the caller can distinguish a clean
// shutdown from a fatal loop exit (there is none — transient errors are logged and
// retried on the next tick, never fatal: a billing relay must self-heal, not die).
func (r *OutboxRelay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	for {
		// Drain the whole backlog before sleeping. processBatch returns the number
		// published; a batch smaller than BatchSize means we've reached the tail, so
		// we stop draining and wait for the next tick.
		for {
			n, err := r.processBatch(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return ctx.Err()
				}
				// Transient (DB blip, NATS down). Log and break to the wait — the rows
				// stay unpublished and the next tick retries them. Never fatal.
				slog.ErrorContext(ctx, "outbox relay batch failed; will retry next tick",
					slog.String("error", err.Error()))
				break
			}
			if n < r.cfg.BatchSize {
				break // tail reached — nothing left to drain this round
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// RunOnce drains a single batch and returns how many rows it published. Exposed
// for tests (deterministic: publish exactly the currently-pending rows once) and
// usable by a one-shot/CLI drain. The Run loop calls the same processBatch.
func (r *OutboxRelay) RunOnce(ctx context.Context) (int, error) {
	return r.processBatch(ctx)
}

// processBatch claims one batch and publishes+marks each row. It returns the count
// SUCCESSFULLY published-and-marked. A per-row failure is logged and the row is
// LEFT UNPUBLISHED (no mark) so the next tick retries it — one poison/failing row
// must not block the rest of the batch, and must not be silently dropped.
func (r *OutboxRelay) processBatch(ctx context.Context) (int, error) {
	rows, err := r.reader.ClaimUnpublished(ctx, r.cfg.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("claim unpublished outbox rows: %w", err)
	}

	published := 0
	for _, row := range rows {
		if err := r.publishRow(ctx, row); err != nil {
			// Leave the row unpublished (don't mark) → retried next tick. We log the
			// event TYPE + id, never the payload bytes (which can hold tenant/billing
			// data) — the same PII discipline natsutil uses on the consume side.
			slog.ErrorContext(ctx, "outbox relay: publish failed, row left for retry",
				slog.String("outbox.id", row.ID),
				slog.String("event.type", row.EventType),
				slog.String("error", err.Error()))
			continue
		}
		// Publish succeeded → mark published. If the MARK fails (DB blip), the row
		// stays unpublished and will be REPUBLISHED next tick — a duplicate the
		// consumer dedupes on the envelope id. We log and move on (do NOT count it as
		// failed-to-publish; it WAS published).
		if err := r.reader.MarkPublished(ctx, row.ID); err != nil {
			slog.ErrorContext(ctx, "outbox relay: published but failed to mark; will republish (consumer dedupes)",
				slog.String("outbox.id", row.ID),
				slog.String("event.type", row.EventType),
				slog.String("error", err.Error()))
			continue
		}
		published++
	}
	return published, nil
}

// publishRow rebuilds the canonical events.v1 payload from the stored JSONB and
// publishes it to the row's event_type subject. natsutil.Publisher wraps it in the
// EventEnvelope, sets the dedupe Nats-Msg-Id, and — crucially — we pass the OUTBOX
// ROW ID as the envelope id so the consumer dedupe key equals the durable row id.
//
// WHY we set the envelope id explicitly (PublishWithID) instead of letting natsutil
// mint a fresh UUID: a republish (crash before mark) must carry the SAME id both
// times, or the consumer would see two different ids and process the event twice.
// The outbox row id is the STABLE idempotency anchor across republishes — that is
// the whole reason the row's id "becomes EventEnvelope.id" (see the migration
// comment). A fresh per-publish UUID would defeat both broker- and consumer-side
// dedup.
func (r *OutboxRelay) publishRow(ctx context.Context, row OutboxRow) error {
	payload, err := decodeOutboxPayload(row.EventType, row.Payload)
	if err != nil {
		return err
	}
	// The publisher is constructed with source = Source ("billing"); it sets the
	// envelope id = row.ID so republishes are idempotent end-to-end.
	if pub, ok := r.pub.(idPublisher); ok {
		return pub.PublishWithID(ctx, row.EventType, row.ID, payload)
	}
	// Fallback for a publisher that only exposes Publish (e.g. a simple test fake):
	// still correct for the happy path; republish-idempotency then relies on the
	// JetStream dedup window + the business idempotency key. The real wiring always
	// satisfies idPublisher (see RelayPublisher), so production gets the strong path.
	return r.pub.Publish(ctx, row.EventType, payload)
}

// idPublisher is the optional capability of publishing with a CALLER-SUPPLIED
// envelope id (the outbox row id). *RelayPublisher (below) implements it over
// natsutil. Kept as a separate interface so the relay degrades gracefully against a
// bare Publish-only fake in unit tests while using the strong path in production.
type idPublisher interface {
	PublishWithID(ctx context.Context, subject, envelopeID string, payload any) error
}

// decodeOutboxPayload reconstructs the canonical events.v1 message from the stored
// JSONB body, dispatching on the event_type discriminator. The repository stored
// the FLAT domain payload (UsageRecordedPayload, …) as JSON; we decode into that
// domain struct and then map it to the wire message. This is the events-layer's
// anti-corruption mapping — the domain payload's field names/JSON tags are an
// internal detail, the events.v1 message is the published contract.
//
// A decode/unknown-type failure is a PERMANENT error for this row (corrupt bytes
// or a row written by a newer schema this relay doesn't understand). We return it;
// processBatch logs and leaves the row unpublished for an operator to inspect —
// the row is NEVER silently dropped (a dropped billing event is lost revenue
// signal). An alert on a row that stays unpublished past an SLA is the ops hook.
func decodeOutboxPayload(eventType string, body []byte) (any, error) {
	switch eventType {
	case domain.EventTypeUsageRecorded:
		var p domain.UsageRecordedPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, fmt.Errorf("decode UsageRecorded payload: %w", err)
		}
		return usageRecordedToProto(p), nil
	case domain.EventTypeQuotaExceeded:
		var p domain.QuotaExceededPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, fmt.Errorf("decode QuotaExceeded payload: %w", err)
		}
		return quotaExceededToProto(p), nil
	case domain.EventTypeInvoiceGenerated:
		var p domain.InvoiceGeneratedPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, fmt.Errorf("decode InvoiceGenerated payload: %w", err)
		}
		return invoiceGeneratedToProto(p), nil
	default:
		return nil, fmt.Errorf("unknown outbox event_type %q (no events.v1 mapping)", eventType)
	}
}

// ----------------------------------------------------------------------------
// DOMAIN PAYLOAD → events.v1 message  (the publish-side mapping, mirrored 1:1)
// ----------------------------------------------------------------------------
//
// Each mapper is the single, total translation from billing's internal outbox
// payload to the platform's canonical wire message. The domain payloads FLATTEN
// Money to micros+currency precisely so this mapping is a field copy (no Money
// dependency leaks onto the bus). MeterType is mapped through meterTypeToProto so
// the wire enum stays in lock-step with the domain's string-backed type.

func usageRecordedToProto(p domain.UsageRecordedPayload) *eventsv1.UsageRecorded {
	return &eventsv1.UsageRecorded{
		RecordId:        p.RecordID,
		Team:            p.Team,
		RatePlanId:      p.RatePlanID,
		MeterType:       meterTypeToProto(p.MeterType),
		Quantity:        p.Quantity,
		CostMicros:      p.CostMicros,
		CurrencyCode:    p.CurrencyCode,
		ModelId:         p.ModelID,
		ModelVersion:    p.ModelVersion,
		SourceRequestId: p.SourceRequestID,
		OccurredAt:      timestamppb.New(p.OccurredAt),
	}
}

func quotaExceededToProto(p domain.QuotaExceededPayload) *eventsv1.QuotaExceeded {
	return &eventsv1.QuotaExceeded{
		Team:         p.Team,
		RatePlanId:   p.RatePlanID,
		MeterType:    meterTypeToProto(p.MeterType),
		QuotaLimit:   p.QuotaLimit,
		CurrentUsage: p.CurrentUsage,
		OccurredAt:   timestamppb.New(p.OccurredAt),
	}
}

func invoiceGeneratedToProto(p domain.InvoiceGeneratedPayload) *eventsv1.InvoiceGenerated {
	return &eventsv1.InvoiceGenerated{
		InvoiceId:     p.InvoiceID,
		InvoiceNumber: p.InvoiceNumber,
		Team:          p.Team,
		RatePlanId:    p.RatePlanID,
		PeriodStart:   timestamppb.New(p.PeriodStart),
		PeriodEnd:     timestamppb.New(p.PeriodEnd),
		TotalMicros:   p.TotalMicros,
		CurrencyCode:  p.CurrencyCode,
		FinalizedAt:   timestamppb.New(p.FinalizedAt),
	}
}

// meterTypeToProto maps the domain's string-backed MeterType to the generated
// events.v1 enum. WHY an explicit switch and not a numeric cast: the two sets are
// independent (the domain mirrors the proto to stay free of gen/go imports), so a
// future reorder of either would SILENTLY mis-map a meter (the worst kind of
// billing bug — metering the wrong axis). The switch makes the mapping a
// compile-time-visible contract; the default guards an unmapped value as
// UNSPECIFIED (a consumer treats that as "unknown meter", never as a wrong one).
func meterTypeToProto(mt domain.MeterType) eventsv1.MeterType {
	switch mt {
	case domain.MeterTypeInferenceRequest:
		return eventsv1.MeterType_METER_TYPE_INFERENCE_REQUEST
	case domain.MeterTypeInferenceTokens:
		return eventsv1.MeterType_METER_TYPE_INFERENCE_TOKENS
	case domain.MeterTypeComputeSeconds:
		return eventsv1.MeterType_METER_TYPE_COMPUTE_SECONDS
	case domain.MeterTypeStorageBytes:
		return eventsv1.MeterType_METER_TYPE_STORAGE_BYTES
	default:
		return eventsv1.MeterType_METER_TYPE_UNSPECIFIED
	}
}
