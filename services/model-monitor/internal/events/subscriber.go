package events

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/domain"
	"github.com/nats-io/nats.go/jetstream"
)

// ============================================================================
// Subscriber — the consumer adapter (four durable consumers)
// ============================================================================
//
// It wires the four consumed subjects to the monitor domain, building one
// natsutil.Subscriber PER subject (each with its own durable name + the shared
// idempotency/DLQ options) so the durables don't collide. It depends on the
// domain.MonitorService INTERFACE (the driving port) and the MonitorResolver port
// (the authorized-binding boundary) — never concrete impls — so it is unit-testable
// against fakes and the same adapter serves the real service in production
// (dependency inversion).
type Subscriber struct {
	svc      domain.MonitorService
	resolver MonitorResolver
	js       jetstream.JetStream
	subOpts  []natsutil.SubOption // shared options (idempotency store, DLQ, timeouts)
	log      *slog.Logger

	// subs holds the per-subject natsutil.Subscribers created by Start, so Close
	// stops every consume loop on shutdown.
	subsMu sync.Mutex
	subs   []*natsutil.Subscriber
}

// NewSubscriber builds the consumer adapter.
//
// It takes the JetStream context, the domain service (driving port), the
// MonitorResolver (model → binding), a logger, plus the SHARED subscriber options
// the service standardizes on (an idempotency ProcessedStore via
// WithIdempotencyStore, a DLQ subject + MaxRetries via WithDLQSubject/
// WithMaxRetries, a per-message timeout). The adapter applies those options to
// EACH per-subject natsutil.Subscriber and appends a per-subject WithConsumerGroup
// durable name. Passing options (not a pre-built Subscriber) lets the adapter own
// the durable-per-subject requirement while the caller (next-stage main.go) owns
// the dedupe/DLQ policy.
func NewSubscriber(
	svc domain.MonitorService,
	resolver MonitorResolver,
	js jetstream.JetStream,
	log *slog.Logger,
	subOpts ...natsutil.SubOption,
) *Subscriber {
	if log == nil {
		log = slog.Default()
	}
	return &Subscriber{
		svc:      svc,
		resolver: resolver,
		js:       js,
		subOpts:  subOpts,
		log:      log,
	}
}

// Start binds all four subject consumers, each on its own durable. Each Subscribe
// spins a managed pull loop that runs until ctx is cancelled or Close() is called.
//
// WHY one consumer per subject (not a single fp.>): a JetStream durable consumer
// binds to ONE filter subject, and a wildcard would also deliver event types this
// service has no reaction for, wasting redelivery budget on ACK-only no-ops.
func (s *Subscriber) Start(ctx context.Context) error {
	type binding struct {
		stream  string
		subject string
		handler natsutil.EventHandler
	}
	bindings := []binding{
		{StreamInference, SubjectInferenceCompleted, s.handleInferenceCompleted},
		{StreamInference, SubjectInferenceFailed, s.handleInferenceFailed},
		{StreamModels, SubjectModelPromoted, s.handleModelPromoted},
		{StreamFeatures, SubjectFeaturesWritten, s.handleFeaturesWritten},
	}
	for _, b := range bindings {
		if err := s.subscribeOne(ctx, b.stream, b.subject, b.handler); err != nil {
			return fmt.Errorf("events: subscribe %s: %w", b.subject, err)
		}
	}
	return nil
}

// Close stops every per-subject consume loop. Safe to call multiple times.
func (s *Subscriber) Close() {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	for _, sub := range s.subs {
		sub.Close()
	}
	s.subs = nil
}

// subscribeOne builds a per-subject natsutil.Subscriber (shared options + a
// per-subject durable name) and starts its consume loop on the given subject.
func (s *Subscriber) subscribeOne(ctx context.Context, stream, subject string, handler natsutil.EventHandler) error {
	opts := make([]natsutil.SubOption, 0, len(s.subOpts)+1)
	opts = append(opts, s.subOpts...)
	opts = append(opts, natsutil.WithConsumerGroup(durableName(subject)))

	sub := natsutil.NewSubscriber(s.js, opts...)

	s.subsMu.Lock()
	s.subs = append(s.subs, sub)
	s.subsMu.Unlock()

	return sub.Subscribe(ctx, stream, subject, handler)
}

// durableName turns a subject into a JetStream-legal durable consumer name:
// "<prefix>-<subject>" with '.', '*', '>' (forbidden in durable names) replaced by
// '-'. e.g. "fp.inference.completed" → "model-monitor-fp-inference-completed".
func durableName(subject string) string {
	out := make([]byte, 0, len(consumerGroupPrefix)+1+len(subject))
	out = append(out, consumerGroupPrefix...)
	out = append(out, '-')
	for i := 0; i < len(subject); i++ {
		c := subject[i]
		if c == '.' || c == '*' || c == '>' {
			c = '-'
		}
		out = append(out, c)
	}
	return string(out)
}

// ============================================================================
// HANDLERS — wire → domain translation (one per consumed event)
// ============================================================================
//
// Each handler:
//  1. Decodes envelope.Data into its eventsv1 payload via the dual-format codec
//     (protojson-first, encoding/json fallback — see codec.go for why).
//  2. Validates the minimum fields it needs.
//  3. Resolves the model → monitor binding (the authorized-binding boundary).
//  4. Calls the idempotent domain reaction.
//
// Returning nil ACKs; returning an error NAKs (→ retry → DLQ after MaxRetries).
//
// ERROR CLASSIFICATION (the crux of a correct consumer):
//   - A STRUCTURALLY-INVALID payload (both decoders fail) is a POISON message: it
//     will never succeed on redelivery. We return an error wrapping
//     natsutil.ErrProcessingFailed so it flows through the retry budget to the DLQ
//     (ops can inspect it) rather than being silently dropped — and is bounded, not
//     looping forever.
//   - A MISSING-FIELD payload that parsed but is semantically empty (no model name)
//     is also non-retryable but is NOT operator-actionable as a dead letter; we log
//     and ACK (return nil) to drop it cheaply.
//   - A TRANSIENT failure (resolver lookup error, domain returns a storage error) is
//     RETRYABLE: return an error (NAK) so JetStream redelivers.

// handleInferenceCompleted folds an InferenceCompleted into the model's window:
// it resolves the monitor binding, maps the event's feature/prediction summaries
// to a domain.InferenceObservation, and calls ObserveInference (the streaming-
// aggregation + closed-loop heartbeat — fold, maybe close+score+act).
func (s *Subscriber) handleInferenceCompleted(ctx context.Context, env natsutil.EventEnvelope) error {
	var p eventsv1.InferenceCompleted
	if err := decodeCanonical(env.Data, &p); err != nil {
		return s.poison(ctx, env, "InferenceCompleted")
	}
	modelName := strings.TrimSpace(p.GetModelName())
	if modelName == "" {
		s.log.WarnContext(ctx, "drop InferenceCompleted missing model name",
			slog.String("event.id", env.ID))
		return nil // parsed but semantically empty — not a DLQ-worthy poison
	}

	binding, found, err := s.resolver.ResolveByModel(ctx, modelName)
	if err != nil {
		// Transient lookup failure — NAK so the binding can be resolved on retry.
		return fmt.Errorf("%w: resolve monitor for %q", natsutil.ErrProcessingFailed, modelName)
	}
	if !found {
		// Unmonitored model — folding its traffic is meaningless and would leak
		// memory. ACK and drop (the safe default; never fabricate a tenant).
		s.log.DebugContext(ctx, "InferenceCompleted for unmonitored model — skip",
			slog.String("event.id", env.ID), slog.String("model", modelName))
		return nil
	}

	obs := domain.InferenceObservation{
		MonitorID:    binding.MonitorID,
		OwnerTeam:    binding.OwnerTeam,
		RequestID:    p.GetRequestId(),
		ModelName:    modelName,
		ModelVersion: p.GetVersion(),
		Features:     featuresFromSummary(p.GetFeatureSummary()),
		PredictedTop: predictedTop(p.GetPredictionSummary()),
		ObservedAt:   timeOrNow(p.GetCompletedAt()),
	}

	if _, err := s.svc.ObserveInference(ctx, obs); err != nil {
		// A domain error here is a storage/scoring failure (load window, save report,
		// trigger retrain) — TRANSIENT. NAK so the observation is retried; the
		// domain's idempotency (request_id within window, window_id on the report)
		// makes the retry safe (no double-count, no double-emit).
		s.log.WarnContext(ctx, "ObserveInference failed — NAK/retry",
			slog.String("event.id", env.ID), slog.String("model", modelName))
		return fmt.Errorf("%w: observe inference for %q", natsutil.ErrProcessingFailed, modelName)
	}
	return nil
}

// handleInferenceFailed folds a FAILURE into the error-rate signal. A spiking
// failure rate is its own degradation signal (contract: fp.inference.failed →
// model-monitor error-rate). We model a failure as an observation with NO feature/
// prediction signal (there may be no prediction to summarize, and we avoid echoing
// a possibly-malformed input) but WITH the request_id + version, so it still counts
// toward the window's sample total and the failure is attributable to the version.
//
// WHY route it through ObserveInference (not a separate domain method): the window
// is the single aggregation point, and a failed call is still a sample the window
// saw — the domain's scorer derives the error-rate from the ratio of
// signal-bearing to total samples. Keeping one fold path keeps the consumer simple
// and the domain the single owner of what a "failure sample" means.
func (s *Subscriber) handleInferenceFailed(ctx context.Context, env natsutil.EventEnvelope) error {
	var p eventsv1.InferenceFailed
	if err := decodeCanonical(env.Data, &p); err != nil {
		return s.poison(ctx, env, "InferenceFailed")
	}
	modelName := strings.TrimSpace(p.GetModelName())
	if modelName == "" {
		s.log.WarnContext(ctx, "drop InferenceFailed missing model name",
			slog.String("event.id", env.ID))
		return nil
	}

	binding, found, err := s.resolver.ResolveByModel(ctx, modelName)
	if err != nil {
		return fmt.Errorf("%w: resolve monitor for %q", natsutil.ErrProcessingFailed, modelName)
	}
	if !found {
		s.log.DebugContext(ctx, "InferenceFailed for unmonitored model — skip",
			slog.String("event.id", env.ID), slog.String("model", modelName))
		return nil
	}

	obs := domain.InferenceObservation{
		MonitorID:    binding.MonitorID,
		OwnerTeam:    binding.OwnerTeam,
		RequestID:    p.GetRequestId(),
		ModelName:    modelName,
		ModelVersion: p.GetVersion(),
		// No Features / PredictedTop on a failure — a failure carries no clean
		// prediction signal. The empty observation still counts as a window sample.
		ObservedAt: timeOrNow(p.GetFailedAt()),
	}

	if _, err := s.svc.ObserveInference(ctx, obs); err != nil {
		s.log.WarnContext(ctx, "ObserveInference (failed call) errored — NAK/retry",
			slog.String("event.id", env.ID), slog.String("model", modelName))
		return fmt.Errorf("%w: observe inference-failed for %q", natsutil.ErrProcessingFailed, modelName)
	}
	return nil
}

// handleModelPromoted re-pins the drift baseline to the newly promoted PRODUCTION
// version. WHY: without it, every new deploy reads as drift vs a stale baseline.
// We act ONLY when to_stage == PRODUCTION (the contract: Monitor reacts only on the
// production transition — a DEV/STAGING promotion does not move the served baseline).
// The adapter resolves the model's OWNING TEAM (the event carries no auth claims)
// and passes it to ResetBaselineFromPromotion, reusing the same team-scoped path as
// the manual ResetBaseline. A promoted model that isn't monitored is a silent no-op
// (the domain returns nil for ErrMonitorNotFound; we also short-circuit on found=false).
func (s *Subscriber) handleModelPromoted(ctx context.Context, env natsutil.EventEnvelope) error {
	var p eventsv1.ModelPromoted
	if err := decodeCanonical(env.Data, &p); err != nil {
		return s.poison(ctx, env, "ModelPromoted")
	}

	// Only the PRODUCTION transition re-pins the served baseline.
	if p.GetToStage() != eventsv1.ModelStage_MODEL_STAGE_PRODUCTION {
		s.log.DebugContext(ctx, "ModelPromoted not to PRODUCTION — ignore for baseline",
			slog.String("event.id", env.ID),
			slog.String("model", p.GetModelName()),
			slog.String("to_stage", p.GetToStage().String()))
		return nil
	}

	modelName := strings.TrimSpace(p.GetModelName())
	newVersion := strings.TrimSpace(p.GetVersion())
	if modelName == "" || newVersion == "" {
		s.log.WarnContext(ctx, "drop ModelPromoted missing model name / version",
			slog.String("event.id", env.ID))
		return nil
	}

	binding, found, err := s.resolver.ResolveByModel(ctx, modelName)
	if err != nil {
		return fmt.Errorf("%w: resolve monitor for %q", natsutil.ErrProcessingFailed, modelName)
	}
	if !found {
		// A promoted model that isn't monitored — nothing to re-baseline. No-op.
		s.log.DebugContext(ctx, "ModelPromoted for unmonitored model — skip re-baseline",
			slog.String("event.id", env.ID), slog.String("model", modelName))
		return nil
	}

	if err := s.svc.ResetBaselineFromPromotion(ctx, binding.OwnerTeam, modelName, newVersion); err != nil {
		// Could be a transient store/baseline-provider error (NAK to retry) OR a
		// permanent baseline-unavailable for the new version. We treat it as
		// retryable: a freshly-promoted version's baseline may not be captured yet
		// (race between promote and baseline capture), and a redelivery after backoff
		// is the cheapest fix. ResetBaselineFromPromotion is idempotent (re-pinning to
		// the same version is harmless), so the retry is safe.
		s.log.WarnContext(ctx, "ResetBaselineFromPromotion failed — NAK/retry",
			slog.String("event.id", env.ID),
			slog.String("model", modelName), slog.String("version", newVersion))
		return fmt.Errorf("%w: re-baseline %q to %q", natsutil.ErrProcessingFailed, modelName, newVersion)
	}
	s.log.InfoContext(ctx, "baseline re-pinned from promotion",
		slog.String("event.id", env.ID),
		slog.String("model", modelName), slog.String("version", newVersion),
		slog.String("owner_team", binding.OwnerTeam))
	return nil
}

// handleFeaturesWritten reacts to fp.features.written. This is a THIN event (entity
// ids + a version range + a count — NOT the feature values). The contract's role
// for the monitor is to "scope drift checks / cache invalidation to the changed
// entities". The monitor's drift math is driven by the INFERENCE stream (the
// distributions it scores come from served predictions, not from feature writes),
// so a feature write does not itself change a window or fire a score. We therefore
// treat this as an OBSERVABILITY/scoping signal: log it (so an operator can
// correlate "features changed for entity X" with a later drift report) and ACK. A
// future enhancement could invalidate a cached baseline or annotate the next report
// with the changed-entity set; the domain exposes no such hook today, so wiring one
// in here would be inventing behavior the domain doesn't model (no TODO left in
// shipped code — this is the honest, complete reaction for the current domain).
func (s *Subscriber) handleFeaturesWritten(ctx context.Context, env natsutil.EventEnvelope) error {
	var p eventsv1.FeaturesWritten
	if err := decodeCanonical(env.Data, &p); err != nil {
		return s.poison(ctx, env, "FeaturesWritten")
	}
	s.log.DebugContext(ctx, "features written (drift-scoping signal)",
		slog.String("event.id", env.ID),
		slog.String("feature_view", p.GetFeatureViewName()),
		slog.Int("changed_entities", len(p.GetEntityIds())),
		slog.Int("written_count", int(p.GetWrittenCount())),
		slog.Int64("through_version", p.GetWrittenThroughVersion()))
	return nil
}

// poison logs an undecodable payload and returns a retryable error so the message
// flows through the retry budget to the DLQ (ops can inspect a dead letter), rather
// than being silently dropped. WHY route a structurally-bad payload to the DLQ at
// all (it will never decode): a poison message in the DLQ is OBSERVABLE — an alert
// on DLQ depth tells ops a producer is emitting malformed events; a silently
// dropped one is invisible. The natsutil subscriber bounds the redelivery to
// MaxRetries, so this does not loop forever.
func (s *Subscriber) poison(ctx context.Context, env natsutil.EventEnvelope, typ string) error {
	s.log.WarnContext(ctx, "undecodable event payload — route to DLQ",
		slog.String("event.id", env.ID),
		slog.String("event.type", env.Type),
		slog.String("payload.type", typ),
		slog.String("error.type", "payload_decode"))
	return fmt.Errorf("%w: decode %s payload (event %s)", natsutil.ErrProcessingFailed, typ, env.ID)
}
