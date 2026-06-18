package events

import (
	"context"
	"fmt"
	"time"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/services/inference-gateway/internal/domain"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// ============================================================================
// SUBSCRIBER ADAPTERS — the route table + quota cache as pure event reactors
// ============================================================================
//
// The gateway's control plane is EVENT-DRIVEN: it does not poll the registry or
// orchestrator, it REACTS to facts on the bus. Each consumer here:
//
//	1. decodes the canonical events.v1 payload from EventEnvelope.Data,
//	2. maps it to the domain's event-reaction input type,
//	3. dispatches to the matching domain method (ApplyModelDeployed, …) /
//	   QuotaCacheWriter.Block,
//	4. returns nil (ACK) or an error (NAK → retry → DLQ after the cap).
//
// IDEMPOTENCY (every consumer must survive duplicate redelivery — the contract):
//   Two layers guarantee a duplicate has a SINGLE effect:
//     (a) TRANSPORT: natsutil's ProcessedStore dedupes on EventEnvelope.id —
//         a redelivered envelope id is recognized and ACKed WITHOUT re-running
//         the handler. We wire WithIdempotencyStore on every subscription.
//     (b) DOMAIN: the Apply* methods are themselves idempotent by construction
//         (Upsert replaces a route; Delete of an absent route is a no-op;
//         Block SET is naturally idempotent). So even WITHOUT the store — e.g.
//         a redelivery after the dedup window, or a different replica with a
//         separate store — replaying the same event converges to the same state.
//   Belt-and-suspenders: (a) makes the common case cheap (no re-work); (b) makes
//   correctness independent of the store's reach. This is the standard answer to
//   "how do you avoid double-processing at-least-once delivery?".
//
// DLQ (poison messages): a payload that can never be decoded/applied (corrupt
// data, a bug) would otherwise loop forever: deliver→fail→NAK→deliver→… We wire
// WithMaxRetries + WithDLQSubject so after the cap the message is parked on a
// dead-letter subject for ops to inspect/replay, and redelivery stops. A message
// natsutil can't even unmarshal into an envelope is Term'd immediately (poison at
// the envelope layer); a payload that unmarshals but fails our decode/apply goes
// through the retry→DLQ path.
//
// WHY one Subscribers struct holding many natsutil.Subscribers: each
// natsutil.Subscriber owns one durable consumer config (group, retries, DLQ,
// store). The gateway needs consumers across THREE streams (PIPELINES, MODELS,
// BILLING) with the SAME resilience config, so we build per-subscription
// subscribers sharing that config and track them all for a single clean Close().
// ============================================================================

// QuotaCacheWriter is the events layer's narrow view of the quota cache's
// CONTROL-PLANE writer. The redis adapter's *QuotaChecker already exposes
// Block(ctx, team, ttl) (and Unblock) for exactly this; we depend on this small
// interface — not the concrete redis type — so the QuotaExceeded consumer is
// testable with a fake and the events package stays free of the redis key layout.
//
// WHY this is a PORT here and not a domain method: flipping the quota cache is a
// pure INFRASTRUCTURE state change (a Redis flag), not business logic — the
// domain's QuotaChecker port is deliberately READ-ONLY (IsBlocked) because the
// hot path only ever READS the flag. The WRITE side belongs to the adapter that
// owns the cache; the consumer is the adapter-side reactor that drives it.
type QuotaCacheWriter interface {
	// Block flags a team as over-quota. ttl bounds the block (e.g. time until the
	// billing period resets) so the flag self-clears even if a clear event is lost;
	// ttl <= 0 means a sticky flag. A repeated Block is naturally idempotent (SET).
	Block(ctx context.Context, team string, ttl time.Duration) error
}

// SubscriberDeps are the collaborators the consumers dispatch into: the domain
// service (for the route-table reactions) and the quota cache writer.
type SubscriberDeps struct {
	Service     domain.InferenceService
	QuotaWriter QuotaCacheWriter
}

// SubConfig tunes the resilience knobs shared by every gateway subscription.
// Defaults (applied by Register when zero) match the platform conventions:
// a durable consumer group named after the service, a small retry budget, and a
// service-scoped DLQ subject.
type SubConfig struct {
	// ConsumerGroup is the durable name. All replicas sharing it form a consumer
	// group (each event handled by exactly one replica). Default: "inference-gateway".
	ConsumerGroup string
	// MaxRetries is redeliveries before DLQ (total attempts = MaxRetries+1).
	// Default: 4.
	MaxRetries int
	// DLQSubject is where poison messages are parked. Default: "fp.dlq.inference-gateway".
	DLQSubject string
	// MessageTimeout bounds a single handler invocation. Default: 10s.
	MessageTimeout time.Duration
	// QuotaBlockTTL is how long a team's over-quota flag persists if no clear event
	// arrives (defense against a lost QuotaRestored). Default: 1h.
	QuotaBlockTTL time.Duration
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
		c.MessageTimeout = 10 * time.Second
	}
	if c.QuotaBlockTTL == 0 {
		c.QuotaBlockTTL = time.Hour
	}
}

// Subscribers owns all of the gateway's event subscriptions and their lifecycle.
type Subscribers struct {
	deps  SubscriberDeps
	cfg   SubConfig
	store natsutil.ProcessedStore

	js   jetstream.JetStream
	subs []*natsutil.Subscriber // tracked for a single Close()
}

// NewSubscribers builds the subscriber set. store is the idempotency
// ProcessedStore (a shared Redis/Postgres-backed store in prod; a memory store in
// tests). The caller (main.go, next stage) provides the JetStream context, the
// domain service, the quota writer, and the store.
func NewSubscribers(js jetstream.JetStream, deps SubscriberDeps, store natsutil.ProcessedStore, cfg SubConfig) *Subscribers {
	cfg.withDefaults()
	return &Subscribers{deps: deps, cfg: cfg, store: store, js: js}
}

// Register starts every gateway subscription. It is idempotent in the sense that
// JetStream's CreateOrUpdateConsumer reuses an existing durable consumer; calling
// Register twice in one process is a wiring bug (it would double-consume locally),
// so call it exactly once. The ctx governs the consume loops' lifetime alongside
// Close().
//
// THE FIVE SUBSCRIPTIONS (subject → stream → domain method):
//
//	fp.pipelines.model.deployed    PIPELINES  → ApplyModelDeployed
//	fp.pipelines.model.undeployed  PIPELINES  → ApplyModelUndeployed
//	fp.models.promoted             MODELS     → ApplyModelPromoted
//	fp.models.archived             MODELS     → ApplyModelArchived
//	fp.billing.quota.exceeded      BILLING    → QuotaWriter.Block
func (s *Subscribers) Register(ctx context.Context) error {
	type binding struct {
		stream  string
		subject string
		// durableSuffix makes the consumer's durable name UNIQUE per subscription.
		// WHY this matters: a JetStream durable name is scoped to a STREAM, and a
		// durable consumer has ONE FilterSubject. The gateway subscribes to TWO
		// subjects on the SAME stream (deployed + undeployed on PIPELINES), so they
		// MUST be distinct durables — otherwise the second CreateOrUpdateConsumer
		// reuses the first durable and overwrites its filter, silently breaking one
		// of the two subscriptions (the symptom: that subject's events never arrive).
		// The consumer GROUP (for multi-replica load balancing) is therefore
		// "{group}-{suffix}": replicas of THIS service share "{group}-deployed" for
		// the deployed subject, while deployed and undeployed stay separate durables.
		durableSuffix string
		handler       natsutil.EventHandler
	}
	bindings := []binding{
		{StreamPipelines, SubjectModelDeployed, "deployed", s.handleModelDeployed},
		{StreamPipelines, SubjectModelUndeployed, "undeployed", s.handleModelUndeployed},
		{StreamModels, SubjectModelPromoted, "promoted", s.handleModelPromoted},
		{StreamModels, SubjectModelArchived, "archived", s.handleModelArchived},
		{StreamBilling, SubjectQuotaExceeded, "quota-exceeded", s.handleQuotaExceeded},
	}

	for _, b := range bindings {
		sub := natsutil.NewSubscriber(s.js,
			natsutil.WithConsumerGroup(s.cfg.ConsumerGroup+"-"+b.durableSuffix),
			natsutil.WithMaxRetries(s.cfg.MaxRetries),
			natsutil.WithDLQSubject(s.cfg.DLQSubject),
			natsutil.WithIdempotencyStore(s.store),
			natsutil.WithMessageTimeout(s.cfg.MessageTimeout),
		)
		if err := sub.Subscribe(ctx, b.stream, b.subject, b.handler); err != nil {
			// Stop anything already started so a partial Register doesn't leak.
			s.Close()
			return fmt.Errorf("events: subscribe %s on %s: %w", b.subject, b.stream, err)
		}
		s.subs = append(s.subs, sub)
	}
	return nil
}

// Close stops all consume loops. Safe to call multiple times (each natsutil
// Subscriber.Close is idempotent) and safe to call after a partial Register.
func (s *Subscribers) Close() {
	for _, sub := range s.subs {
		sub.Close()
	}
}

// ----------------------------------------------------------------------------
// HANDLERS — decode events.v1 payload → domain reaction.
// ----------------------------------------------------------------------------
//
// DECODE NOTE: the producer (natsutil.Publisher) serialized the payload with
// encoding/json into EventEnvelope.Data, so we decode it the SAME way into the
// generated struct. The generated types carry json struct tags, so this is a
// faithful, symmetric round-trip (enums ride as their int values; timestamps via
// the well-known-type json tags). decodeData centralizes the unmarshal +
// error-wrapping so each handler reads as pure mapping logic.

// handleModelDeployed: ADD/update a route target from a ModelDeployed event. The
// endpoint and initial weight come from the EVENT (server-resolved by the deploy
// saga), never a client — the anti-SSRF source of truth for a backend address.
func (s *Subscribers) handleModelDeployed(ctx context.Context, env natsutil.EventEnvelope) error {
	var p eventsv1.ModelDeployed
	if err := decodeData(env, &p); err != nil {
		return err
	}
	return s.deps.Service.ApplyModelDeployed(ctx, domain.ModelDeployed{
		ModelName: p.GetModelName(),
		Version:   p.GetVersion(),
		Endpoint:  p.GetEndpoint(),
		WeightBps: int(p.GetWeightBps()),
	})
}

// handleModelUndeployed: REMOVE a route target (a saga removed a version from
// serving). If it was the model's last target the route becomes non-serving.
func (s *Subscribers) handleModelUndeployed(ctx context.Context, env natsutil.EventEnvelope) error {
	var p eventsv1.ModelUndeployed
	if err := decodeData(env, &p); err != nil {
		return err
	}
	return s.deps.Service.ApplyModelUndeployed(ctx, domain.ModelUndeployed{
		ModelName: p.GetModelName(),
		Version:   p.GetVersion(),
		Reason:    p.GetReason(), // logged, not logic — suppresses alerts on expected removals
	})
}

// handleModelPromoted: repoint traffic to the newly-promoted PRODUCTION version
// (100% to it, mark stable) and tear down the auto-demoted prior one. We pass the
// new version AND the demoted version so the domain performs the whole swap.
func (s *Subscribers) handleModelPromoted(ctx context.Context, env natsutil.EventEnvelope) error {
	var p eventsv1.ModelPromoted
	if err := decodeData(env, &p); err != nil {
		return err
	}
	return s.deps.Service.ApplyModelPromoted(ctx, domain.ModelPromoted{
		ModelName:      p.GetModelName(),
		Version:        p.GetVersion(),
		DemotedVersion: p.GetDemotedVersion(),
	})
}

// handleModelArchived: DROP the model's route entirely (the model is going away).
func (s *Subscribers) handleModelArchived(ctx context.Context, env natsutil.EventEnvelope) error {
	var p eventsv1.ModelArchived
	if err := decodeData(env, &p); err != nil {
		return err
	}
	return s.deps.Service.ApplyModelArchived(ctx, domain.ModelArchived{
		ModelName: p.GetModelName(),
	})
}

// handleQuotaExceeded: flip the team's quota cache to "blocked" so subsequent
// predicts are pre-flight rejected. Eventual consistency is acceptable here (the
// gateway is a soft pre-flight gate, not the billing source of truth). The TTL
// bounds the block so it self-clears at the next billing period even if a clear
// event is lost.
func (s *Subscribers) handleQuotaExceeded(ctx context.Context, env natsutil.EventEnvelope) error {
	var p eventsv1.QuotaExceeded
	if err := decodeData(env, &p); err != nil {
		return err
	}
	// team is server-authoritative on the producer side (derived from the
	// inference api_key / auth claims) — never a client value — so we trust it as
	// the cache key. An empty team is a malformed event; treat it as poison so it
	// goes to the DLQ rather than blocking the "" tenant.
	if p.GetTeam() == "" {
		return fmt.Errorf("%w: QuotaExceeded with empty team", natsutil.ErrProcessingFailed)
	}
	if err := s.deps.QuotaWriter.Block(ctx, p.GetTeam(), s.cfg.QuotaBlockTTL); err != nil {
		// A Redis write failure is TRANSIENT — return the error so natsutil NAKs and
		// redelivers (the block will be applied on retry). This is the right call
		// for a control-plane write (unlike the READ path, which fails open).
		return fmt.Errorf("events: block quota for team=%s: %w", p.GetTeam(), err)
	}
	return nil
}

// decodeData unmarshals the envelope's payload into the given events.v1 message
// using protojson — the CANONICAL proto-JSON dialect every producer now emits via
// natsutil.Publisher. WHY protojson, not encoding/json: these payloads are
// forgepoint/events/v1 PROTO messages carrying well-known types (e.g. a
// google.protobuf.Timestamp renders as an RFC-3339 string, an enum as its NAME);
// only protojson decodes that form. Using encoding/json here would fail to decode
// (or silently mis-decode) and DLQ a perfectly valid event. The parameter is a
// proto.Message (not any) so the protojson call is type-safe and every caller
// passes a *eventsv1.* pointer.
//
// A decode failure is a POISON message (redelivery will never fix corrupt bytes),
// so we wrap natsutil.ErrProcessingFailed — but the subscriber's retry→DLQ
// machinery still gives ops a chance to inspect it before it is parked, and a
// transient handler is distinguished from poison by NOT wrapping this sentinel for
// retryable infra errors (see handleQuotaExceeded).
func decodeData(env natsutil.EventEnvelope, msg proto.Message) error {
	if err := protojson.Unmarshal(env.Data, msg); err != nil {
		return fmt.Errorf("%w: decode %T from event %s: %v",
			natsutil.ErrProcessingFailed, msg, env.ID, err)
	}
	return nil
}
