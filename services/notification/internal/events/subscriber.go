package events

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/services/notification/internal/domain"
	"github.com/nats-io/nats.go/jetstream"
)

// ============================================================================
// REACTOR — the choreography consumer (the imperative shell around ReactToEvent)
// ============================================================================
//
// The Reactor is the INBOUND adapter that drives the domain's primary port. It is
// the "imperative shell" wrapped around the domain's "functional core":
//
//	NATS fp.>  ──►  Reactor.handle (THIS)  ──►  domain.ReactToEvent (pure)
//	                     │                            │ returns RoutingDecision
//	                     │  loads prefs (PreferenceLoader port)
//	                     │  executes decision (DecisionExecutor port):
//	                     │     • write inbox row
//	                     │     • fire Notifier per delivering channel
//	                     │     • record SUPPRESSED attempts for the rest
//	                     ▼
//	            publishes fp.notifications.{delivered,failed} (DeliveryHealthPublisher)
//
// THE FLOW per inbound envelope:
//   1. Build a domain.InboundEvent from the envelope — OPAQUELY. We read only the
//      envelope's metadata (id/type/source/timestamp) and the recipient + rendered
//      title/body the platform put in the envelope's data; we NEVER decode the
//      producer-specific payload schema (that is the whole point of choreography —
//      see the package doc). The raw payload bytes ride through verbatim.
//   2. DEDUP (idempotent consumer): NATS is at-least-once, so the same envelope can
//      be redelivered. Before any side effect we check the IdempotencyStore keyed on
//      the DOMAIN's per-(eventID, recipient) key (domain.EventDedupKey). A duplicate
//      is ACKed without re-creating the inbox row or re-firing a webhook.
//   3. ReactToEvent (pure) → RoutingDecision.
//   4. EXECUTE the decision via the DecisionExecutor port (DB writes + Notifier
//      calls live in the executor the main.go wiring provides — keeping THIS adapter
//      free of Postgres/HTTP and trivially testable with a fake).
//   5. For each external delivery the executor reports, publish a delivery-health
//      event (delivered/failed) so the platform can observe alert reachability.
//   6. RECORD the dedup key (MarkProcessed) so a later redelivery is recognized.
//
// ============================================================================
// IDEMPOTENCY (every consumer must survive duplicate redelivery — the contract)
// ============================================================================
//
// THREE layers guarantee a duplicate has a SINGLE effect:
//   (a) TRANSPORT (natsutil ProcessedStore on EventEnvelope.id): wired via
//       WithIdempotencyStore on the subscription — a redelivered envelope id is
//       ACKed WITHOUT re-running the handler. This is the cheap common-case guard.
//   (b) DOMAIN (per-(eventID, recipient) key via domain.EventDedupKey): the SAME
//       event may legitimately notify several recipients (one inbox row each), so
//       the business dedup unit is per-recipient, not per-event. We check this key
//       in handle() before executing, so even if the transport store is bypassed
//       (a redelivery after the dedup window, or a different replica with a separate
//       store), a (eventID, recipient) pair we already reacted to is skipped.
//   (c) EXECUTOR: the inbox write should be an idempotent upsert on
//       (event_id, recipient) — so even a torn execution (crash between executing
//       and MarkProcessed) converges. The executor owns that; the contract is
//       documented on DecisionExecutor.
//   Belt-and-suspenders: (a) makes the common case cheap, (b) makes correctness
//   per-recipient, (c) makes it independent of the store's reach. This is the
//   standard answer to "how do you avoid double-processing at-least-once delivery?".
//
// ============================================================================
// DLQ (poison messages)
// ============================================================================
//
// A message that can never be processed (a recipient we can't resolve, a corrupt
// envelope, a persistent executor bug) would otherwise loop forever:
// deliver→fail→NAK→deliver→… We wire WithMaxRetries + WithDLQSubject so after the
// cap the message is parked on a dead-letter subject for ops to inspect/replay and
// redelivery stops. A message natsutil can't even unmarshal into an EventEnvelope
// is Term'd immediately by natsutil (poison at the envelope layer); a well-formed
// envelope that fails our resolve/execute path goes through the retry→DLQ machinery
// when the handler wraps natsutil.ErrProcessingFailed (a permanent fault). A
// TRANSIENT fault (DB blip) returns a plain error so it is retried, not DLQ-rushed.

// ============================================================================
// PORTS the Reactor drives (provided by main.go next stage; faked in tests)
// ============================================================================

// PreferenceLoader loads the recipient's notification preferences so the pure
// ReactToEvent brain can be a function of (event, prefs) with zero I/O. The
// Postgres preference repo (behind a small cache) satisfies this in production; a
// test injects a map-backed fake. WHY a narrow port here (not domain.PreferenceRepository
// directly): the reactor needs ONLY "load for this user", and a missing user must
// degrade to defaults — so we keep a one-method port that already encodes that.
type PreferenceLoader interface {
	// LoadPreferences returns the user's preferences. It must NOT error on a
	// never-configured user — it returns sane defaults (IN_APP on, nothing external)
	// so the reactor always has something to route against. (The domain service's
	// GetPreferences already implements exactly this fall-back.)
	LoadPreferences(ctx context.Context, userID string) (domain.NotificationPreferences, error)
}

// ExecutedDelivery is the executor's report of ONE external channel's terminal
// outcome, which the Reactor turns into a delivery-health event. IN_APP and
// SUPPRESSED channels produce NO ExecutedDelivery (IN_APP is a DB write, not an
// external delivery; a SUPPRESSED channel was deliberately not attempted).
type ExecutedDelivery struct {
	Channel domain.NotificationChannel
	// Delivered is true if the channel succeeded; false means the retry budget was
	// exhausted (or the circuit was open / the SSRF guard rejected the target).
	Delivered bool
	// Attempts is how many tries the executor made (for the failed-event detail).
	Attempts int32
	// ErrorMessage is the terminal failure detail on a failure (empty on success) —
	// health detail only, never a secret/target.
	ErrorMessage string
}

// ExecutionResult is what the DecisionExecutor returns: the persisted inbox-entry
// id (so delivery-health events can join back to it) and the per-external-channel
// terminal outcomes.
type ExecutionResult struct {
	// NotificationID is the inbox row the executor created (the stable handle the
	// delivery-health events carry). Empty when the event was fully muted (no row).
	NotificationID string
	// Deliveries are the terminal outcomes of the EXTERNAL channels the decision
	// chose to deliver on (one per delivering WEBHOOK/SLACK/EMAIL channel).
	Deliveries []ExecutedDelivery
}

// DecisionExecutor performs the SIDE EFFECTS of a RoutingDecision: write the inbox
// row, fire the Notifier for each delivering channel (SSRF guard + retries live in
// the Notifier adapter), and record a SUPPRESSED delivery-log row for each held
// channel. It returns the inbox id + the external deliveries' outcomes.
//
// WHY this is a PORT in the events package (not a domain method): executing the
// decision is pure INFRASTRUCTURE orchestration (DB writes + HTTP POSTs) — the
// domain's job ended when ReactToEvent returned the pure decision. Keeping the
// executor behind a one-method port lets the Reactor be unit-tested against a fake
// (asserting decode + dedup + dispatch + delivery-health publishing) without a real
// Postgres or HTTP client, exactly mirroring the gateway's QuotaCacheWriter pattern.
//
// IDEMPOTENCY CONTRACT (layer (c) above): Execute MUST be idempotent on
// (decision.EventID, decision.RecipientUserID) — the inbox write is an upsert on
// that pair — so a redelivery that slips past the store-level dedup still produces
// a single row. The Reactor's own dedup (layer (b)) makes this the rare path, but
// the contract closes the crash-between-execute-and-mark window.
type DecisionExecutor interface {
	Execute(ctx context.Context, decision domain.RoutingDecision, event domain.InboundEvent) (ExecutionResult, error)
}

// DeliveryHealthPublisher is the outbound port the Reactor uses to announce
// delivery outcomes. *Publisher (publisher.go) implements it; a test injects a
// recording fake. Defined here (next to its consumer) per the hexagonal
// "consumer-owned port" rule.
type DeliveryHealthPublisher interface {
	PublishDelivered(ctx context.Context, in DeliveredInput) error
	PublishFailed(ctx context.Context, in FailedInput) error
}

// ============================================================================
// Reactor
// ============================================================================

// ReactorDeps are the collaborators the reactor drives. All are ports so the
// reactor is testable with fakes and free of Postgres/HTTP/gen-proto.
type ReactorDeps struct {
	// Service is the domain brain — the reactor calls ReactToEvent on it.
	Service domain.NotificationService
	// Router maps an opaque envelope → (recipient, rendered content, canonical type).
	// It is the platform's recipient-resolution policy (see resolve.go); the reactor
	// stays opaque regardless of how it is implemented.
	Router RecipientRouting
	// Prefs loads the recipient's preferences (with default fall-back).
	Prefs PreferenceLoader
	// Executor performs the decision's side effects (inbox + Notifier + suppressed log).
	Executor DecisionExecutor
	// Health publishes fp.notifications.{delivered,failed}. Optional: nil disables
	// the delivery-health feed (the reaction still happens; just no observability
	// events) — useful in a degraded mode where the bus is one-directional.
	Health DeliveryHealthPublisher
}

// ReactorConfig tunes the resilience knobs of the firehose subscription. Defaults
// (applied by Start when zero) match the platform conventions: a durable consumer
// group named after the service, a small retry budget, and a service-scoped DLQ.
type ReactorConfig struct {
	// ConsumerGroup is the durable name. All replicas sharing it form a consumer
	// group (each event handled by exactly one replica). Default: Source.
	ConsumerGroup string
	// MaxRetries is redeliveries before DLQ (total attempts = MaxRetries+1). Default: 4.
	MaxRetries int
	// DLQSubject is where poison messages are parked. Default: SubjectDLQ
	// ("fp_dlq.notification"). CRITICAL: this subject MUST NOT match the reactor's own
	// consumer filter (SubjectAllEvents = "fp.>") and MUST live in a stream OTHER than
	// the one the reactor consumes (StreamEvents). Otherwise a parked poison message
	// is fed straight back to the reactor's fp.> consumer and re-processed (the DLQ
	// re-consumption / amplification bug). The "fp_dlq." root token (underscore) keeps
	// it outside fp.>; main.go binds it to StreamDLQ. See SubjectDLQ for the full rationale.
	DLQSubject string
	// MessageTimeout bounds a single reaction (load prefs → react → execute →
	// publish). Default: 15s — generous, because the execute step does external
	// webhook deliveries with retries.
	MessageTimeout time.Duration
	// AckWait overrides the JetStream redelivery timeout. Default 0 → natsutil's 30s.
	AckWait time.Duration
}

func (c *ReactorConfig) withDefaults() {
	if c.ConsumerGroup == "" {
		c.ConsumerGroup = Source
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = 4
	}
	if c.DLQSubject == "" {
		// Default to the dedicated DLQ subject that is OUTSIDE the fp.> firehose the
		// reactor consumes (SubjectDLQ = "fp_dlq.notification"). Using "fp.dlq."+Source
		// here was the amplification bug: "fp.dlq.notification" matches fp.> and lands
		// in the same EVENTS stream, so the parked poison message was fed back to the
		// reactor and re-processed. See SubjectDLQ / StreamDLQ.
		c.DLQSubject = SubjectDLQ
	}
	if c.MessageTimeout == 0 {
		c.MessageTimeout = 15 * time.Second
	}
}

// Reactor owns the single fp.> subscription and its lifecycle.
type Reactor struct {
	deps   ReactorDeps
	cfg    ReactorConfig
	store  natsutil.ProcessedStore
	sub    *natsutil.Subscriber
	logger *slog.Logger
}

// NewReactor builds the reactor. store is the idempotency ProcessedStore (a shared
// Redis/Postgres-backed store in prod; a memory store in tests). The caller
// (main.go, next stage) provides the JetStream context, the domain service, the
// prefs loader, the executor, the health publisher, and the store.
func NewReactor(sub *natsutil.Subscriber, deps ReactorDeps, store natsutil.ProcessedStore, cfg ReactorConfig, logger *slog.Logger) *Reactor {
	cfg.withDefaults()
	if logger == nil {
		logger = slog.Default()
	}
	return &Reactor{deps: deps, cfg: cfg, store: store, sub: sub, logger: logger}
}

// Start begins consuming the fp.> firehose. The natsutil.Subscriber passed in must
// already be configured with the consumer group + idempotency store + retries + DLQ
// that match cfg (NewReactorSubscriber builds exactly that). ctx governs the
// consume loop's lifetime alongside the subscriber's Close().
//
// WHY a single Subscribe to StreamEvents/fp.>: the choreography reactor sees the
// WHOLE platform through ONE durable consumer — it does not enumerate subjects (see
// the package doc). That is what makes a new event type a zero-code change here.
func (r *Reactor) Start(ctx context.Context) error {
	if err := r.sub.Subscribe(ctx, StreamEvents, SubjectAllEvents, r.handle); err != nil {
		return fmt.Errorf("events: subscribe %s on %s: %w", SubjectAllEvents, StreamEvents, err)
	}
	return nil
}

// Close stops the consume loop. Safe to call multiple times (natsutil Subscriber.Close
// is idempotent).
func (r *Reactor) Close() {
	if r.sub != nil {
		r.sub.Close()
	}
}

// NewReactorSubscriber builds a natsutil.Subscriber wired with the reactor's
// resilience config: durable consumer group + idempotency store + bounded retries +
// DLQ + message timeout. Exposed so main.go (next stage) and the tests construct the
// subscriber the same way, and so the natsutil-level options live in ONE place.
func NewReactorSubscriber(js jetstream.JetStream, store natsutil.ProcessedStore, cfg ReactorConfig) *natsutil.Subscriber {
	cfg.withDefaults()
	opts := []natsutil.SubOption{
		natsutil.WithConsumerGroup(cfg.ConsumerGroup),
		natsutil.WithMaxRetries(cfg.MaxRetries),
		natsutil.WithDLQSubject(cfg.DLQSubject),
		natsutil.WithIdempotencyStore(store),
		natsutil.WithMessageTimeout(cfg.MessageTimeout),
	}
	if cfg.AckWait > 0 {
		opts = append(opts, natsutil.WithAckWait(cfg.AckWait))
	}
	return natsutil.NewSubscriber(js, opts...)
}

// ============================================================================
// handle — the per-message reaction
// ============================================================================

func (r *Reactor) handle(ctx context.Context, env natsutil.EventEnvelope) error {
	// (1) Build the domain InboundEvent OPAQUELY from the envelope. We interpret
	// ONLY envelope metadata + the platform-supplied recipient/rendered strings; the
	// producer-specific payload schema is never decoded (choreography). resolveEvent
	// returns ok=false when the platform gave us no recipient to notify — that is a
	// NORMAL, expected case (many platform events have no per-user recipient, e.g. a
	// fp.pipelines.step.completed). It is NOT an error: we ACK and move on.
	ev, ok := resolveEvent(env, r.deps.Router)
	if !ok {
		r.logger.DebugContext(ctx, "no recipient for event; skipping",
			slog.String("event.id", env.ID), slog.String("event.type", env.Type))
		return nil // ACK — nothing to do, not a failure
	}

	// (2) DOMAIN-LEVEL DEDUP (layer (b)): per-(eventID, recipient). Even though the
	// natsutil store already deduped on envelope.id at the transport layer, we re-check
	// at the BUSINESS unit so correctness is per-recipient and survives a store miss.
	dedupKey := domain.EventDedupKey(ev.EventID, ev.RecipientUserID)
	if r.store != nil {
		seen, err := r.store.IsProcessed(ctx, dedupKey)
		if err != nil {
			// Can't be sure → return a TRANSIENT error so natsutil NAKs and retries.
			return fmt.Errorf("events: idempotency check for %s: %w", dedupKey, err)
		}
		if seen {
			r.logger.DebugContext(ctx, "duplicate (event,recipient); already reacted",
				slog.String("event.id", ev.EventID), slog.String("recipient", ev.RecipientUserID))
			return nil // ACK — single effect already happened
		}
	}

	// (3) Load the recipient's preferences (default fall-back inside the loader) and
	// run the PURE routing brain. A prefs-load failure is TRANSIENT (DB blip) → retry.
	prefs, err := r.deps.Prefs.LoadPreferences(ctx, ev.RecipientUserID)
	if err != nil {
		return fmt.Errorf("events: load prefs for %s: %w", ev.RecipientUserID, err)
	}
	decision, err := r.deps.Service.ReactToEvent(ctx, ev, prefs)
	if err != nil {
		// ReactToEvent is pure and effectively never errors, but if it does it is a
		// logic fault, not transient — wrap ErrProcessingFailed so it heads to the DLQ
		// after the retry cap rather than NAK-looping forever.
		return fmt.Errorf("%w: react to event %s: %v", natsutil.ErrProcessingFailed, ev.EventID, err)
	}

	// (4) EXECUTE the decision (inbox write + Notifier + suppressed log) via the port.
	// A failure here is TRANSIENT (DB/HTTP) → return a plain error so it is retried;
	// the executor's idempotent upsert (layer (c)) makes the retry safe.
	res, err := r.deps.Executor.Execute(ctx, decision, ev)
	if err != nil {
		return fmt.Errorf("events: execute decision for event %s: %w", ev.EventID, err)
	}

	// (5) Publish a delivery-health event per external delivery the executor reported.
	// BEST-EFFORT: a publish failure does NOT fail the reaction (the inbox row + the
	// actual delivery already happened and are the durable record). We log it; we do
	// NOT return it, because returning would NAK and re-run the whole reaction →
	// double-deliver. This is the deliberate asymmetry between the durable side
	// effect (must succeed before we record dedup) and the observability feed.
	r.publishDeliveryHealth(ctx, res, ev)

	// (6) RECORD the domain dedup key AFTER a successful execute, so a later
	// redelivery of this (eventID, recipient) is recognized as already-reacted. We
	// record AFTER execute (not before) so a failed execute can be legitimately
	// retried with the same key. The natsutil store separately records envelope.id on
	// ACK; this records the per-recipient business key.
	if r.store != nil {
		if err := r.store.MarkProcessed(ctx, dedupKey); err != nil {
			// The reaction already happened; failing to record the key only risks ONE
			// duplicate inbox row IF this exact message is redelivered AND the executor's
			// upsert (layer (c)) is somehow bypassed. Return a transient error so the
			// store write is retried — but note the side effect is already durable.
			return fmt.Errorf("events: mark processed %s: %w", dedupKey, err)
		}
	}
	return nil // ACK
}

// publishDeliveryHealth emits one delivered/failed event per external delivery.
// Best-effort: errors are logged, never returned (see handle step 5).
func (r *Reactor) publishDeliveryHealth(ctx context.Context, res ExecutionResult, ev domain.InboundEvent) {
	if r.deps.Health == nil {
		return // delivery-health feed disabled
	}
	for _, d := range res.Deliveries {
		var perr error
		if d.Delivered {
			perr = r.deps.Health.PublishDelivered(ctx, DeliveredInput{
				NotificationID:  res.NotificationID,
				RecipientUserID: ev.RecipientUserID,
				Channel:         d.Channel,
				EventType:       ev.Type,
				DeliveredAt:     time.Now().UTC(),
			})
		} else {
			perr = r.deps.Health.PublishFailed(ctx, FailedInput{
				NotificationID:  res.NotificationID,
				RecipientUserID: ev.RecipientUserID,
				Channel:         d.Channel,
				EventType:       ev.Type,
				Attempts:        d.Attempts,
				ErrorMessage:    d.ErrorMessage,
				FailedAt:        time.Now().UTC(),
			})
		}
		if perr != nil {
			r.logger.WarnContext(ctx, "failed to publish delivery-health event (best-effort)",
				slog.String("event.id", ev.EventID),
				slog.Int("channel", int(d.Channel)),
				slog.Bool("delivered", d.Delivered),
				slog.String("error", perr.Error()),
			)
		}
	}
}
