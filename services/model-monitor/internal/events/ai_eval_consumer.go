package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/domain"
	"github.com/nats-io/nats.go/jetstream"
)

// ============================================================================
// AI-EVAL CONSUMER — M7/L4 LLM quality evaluation (fp.ai.completion.served)
// ============================================================================
//
// This is the DATA-plane entry point for L4. The ai-gateway emits one
// fp.ai.completion.served event after EVERY LLM completion (the SAME event billing
// meters). This consumer:
//
//  1. Decodes the (PLAIN-JSON) event.
//  2. Resolves the model → authorized monitor binding (the same authorized-binding
//     boundary the inference consumer uses — never derive a tenant from event data).
//  3. SAMPLES (1-in-N, configurable) to bound the judge's Ollama load.
//  4. For a sampled event, hands the completion to the QualityEvalService, which
//     judges it, records the eval, and (on quality drift) raises a drift report into
//     the EXISTING alert + retrain loop.
//
// ----------------------------------------------------------------------------
// PLAIN JSON, NOT protojson (the codec call-out)
// ----------------------------------------------------------------------------
//
// The other four monitor consumers decode forgepoint/events/v1 PROTO messages with
// the dual-format codec (protojson-first). The AI completion event is DIFFERENT:
// there is no events.v1 proto message for it (the events proto predates M7 and the
// hard rule forbids regen), so the gateway publishes it as a PLAIN Go struct that
// natsutil marshals with encoding/json. It carries NO google.protobuf.* well-known
// types — only strings/ints/bool/text — so plain encoding/json is the correct,
// symmetric codec. This mirrors billing's AIConsumer exactly.
//
// ----------------------------------------------------------------------------
// IDEMPOTENCY — durable + envelope-id + business request_id
// ----------------------------------------------------------------------------
//
//   - TRANSPORT: the natsutil Subscriber's ProcessedStore dedupes on EventEnvelope.id
//     (wired via WithIdempotencyStore on the shared options) — a redelivered envelope
//     is ACKed without re-running the handler.
//   - BUSINESS: EvalStore.Record is idempotent on request_id (INSERT … ON CONFLICT
//     DO NOTHING), and a quality-drift report is idempotent on the synthesized
//     window_id (llmq-<request_id>). So even a redelivery to a DIFFERENT replica
//     (separate transport store) records the eval at most once and emits at most one
//     drift event per completion.
//
// SAMPLING + IDEMPOTENCY ORDERING (subtle): we sample BEFORE judging but AFTER the
// transport dedup, so a redelivered already-processed envelope never even reaches the
// sampler (the subscriber short-circuits it). For a genuinely new event the sampler
// decides once; a NAK-then-redeliver of a SAMPLED event re-enters the handler, but the
// judge+record are idempotent (request_id), so a retry never double-judges in effect.
//
// ----------------------------------------------------------------------------
// TEAM ATTRIBUTION — gateway-resolved, but we STILL resolve the monitor binding
// ----------------------------------------------------------------------------
//
// The event's `team` is gateway-resolved and server-authoritative (like billing
// trusts it). But the monitor's tenancy + config (auto_retrain, pipeline) must come
// from the MONITOR row, not the event — so we resolve model → (monitor id, owner
// team) via the MonitorResolver and pass the monitor id down. The QualityEvalService
// loads the monitor by id for owner_team. We also cross-check that the event's team
// matches the resolved owner team; a mismatch is dropped (defense against a stray
// event naming a model another team monitors under the same name).
// ============================================================================

// aiCompletionServed is the monitor's read-model of the gateway's
// fp.ai.completion.served wire payload. The json tags MIRROR the producer's struct
// (ai-gateway/internal/events/publisher.go completionServedPayload) field-for-field:
// lowerCamelCase (proto3-JSON convention). We re-declare it (not import the gateway's
// unexported type from another module) — the events layer's anti-corruption boundary.
// promptText/responseText are present ONLY when the gateway runs with
// FP_AI_EVAL_INCLUDE_TEXT on (PII discipline); empty otherwise, in which case the
// judge degrades to UNSCORED and we note the limitation.
type aiCompletionServed struct {
	RequestID    string `json:"requestId"`
	Team         string `json:"team"`
	Model        string `json:"model"`
	Provider     string `json:"provider"`
	PromptTokens int32  `json:"promptTokens"`
	OutputTokens int32  `json:"outputTokens"`
	TotalTokens  int32  `json:"totalTokens"`
	CostMicroUSD int64  `json:"costMicroUsd"`
	CacheHit     bool   `json:"cacheHit"`
	LatencyMs    int64  `json:"latencyMs"`
	PromptText   string `json:"promptText,omitempty"`
	ResponseText string `json:"responseText,omitempty"`
}

// AIEvalConsumer subscribes to fp.ai.completion.served and drives L4 quality eval.
type AIEvalConsumer struct {
	svc      domain.QualityEvalService
	resolver MonitorResolver
	sampler  domain.Sampler
	js       jetstream.JetStream
	subOpts  []natsutil.SubOption
	log      *slog.Logger

	sub *natsutil.Subscriber
	// textSeen tracks whether we've EVER seen response text on the stream, so the
	// "judging blind (no text)" limitation is logged ONCE at warn, not per-event.
	loggedNoText bool
}

// NewAIEvalConsumer builds the L4 consumer. The caller (main.go) injects the
// JetStream context, the QualityEvalService (driving port), the MonitorResolver
// (model → authorized binding), the Sampler (1-in-N decimation), a logger, and the
// SHARED subscriber options the service standardizes on (idempotency store, DLQ,
// max-retries, per-message timeout). We append our OWN durable name so this consumer
// doesn't collide with the four drift consumers' durables.
func NewAIEvalConsumer(
	svc domain.QualityEvalService,
	resolver MonitorResolver,
	sampler domain.Sampler,
	js jetstream.JetStream,
	log *slog.Logger,
	subOpts ...natsutil.SubOption,
) *AIEvalConsumer {
	if log == nil {
		log = slog.Default()
	}
	return &AIEvalConsumer{
		svc:      svc,
		resolver: resolver,
		sampler:  sampler,
		js:       js,
		subOpts:  subOpts,
		log:      log,
	}
}

// Start binds the durable consumer on the AI stream filtered to
// fp.ai.completion.served and spins the managed pull loop. Call once.
func (c *AIEvalConsumer) Start(ctx context.Context) error {
	opts := make([]natsutil.SubOption, 0, len(c.subOpts)+1)
	opts = append(opts, c.subOpts...)
	// Distinct durable from the drift consumers (which prefix model-monitor-<subject>);
	// this one reads a different stream, so it needs its own durable name.
	opts = append(opts, natsutil.WithConsumerGroup(durableName(SubjectAICompletionServed)))

	sub := natsutil.NewSubscriber(c.js, opts...)
	if err := sub.Subscribe(ctx, StreamAI, SubjectAICompletionServed, c.handle); err != nil {
		return fmt.Errorf("events: subscribe %s on %s: %w", SubjectAICompletionServed, StreamAI, err)
	}
	c.sub = sub
	return nil
}

// Close stops the consume loop. Safe to call multiple times and before Start.
func (c *AIEvalConsumer) Close() {
	if c.sub != nil {
		c.sub.Close()
	}
}

// handle is the per-envelope reaction. Returning nil ACKs; returning an error NAKs
// (→ retry → DLQ after MaxRetries). See the error-classification block.
func (c *AIEvalConsumer) handle(ctx context.Context, env natsutil.EventEnvelope) error {
	var ev aiCompletionServed
	// encoding/json (NOT protojson): the gateway publishes a plain Go struct — match
	// the producer's codec. A corrupt payload is poison → DLQ.
	if err := json.Unmarshal(env.Data, &ev); err != nil {
		c.log.WarnContext(ctx, "undecodable AiCompletionServed — route to DLQ",
			slog.String("event.id", env.ID), slog.String("error.type", "payload_decode"))
		return fmt.Errorf("%w: decode AiCompletionServed (event %s): %v",
			natsutil.ErrProcessingFailed, env.ID, err)
	}

	model := strings.TrimSpace(ev.Model)
	if model == "" || strings.TrimSpace(ev.RequestID) == "" {
		// No model / no idempotency anchor — we can't attribute or dedup. Parsed-but-
		// empty: log + ACK (drop cheaply; not a DLQ-worthy poison).
		c.log.WarnContext(ctx, "drop AiCompletionServed missing model / request_id",
			slog.String("event.id", env.ID))
		return nil
	}

	// AUTHORIZED-BINDING boundary: resolve model → (monitor id, owner team) from the
	// monitor's OWN store. Never derive tenancy from the event.
	binding, found, err := c.resolver.ResolveByModel(ctx, model)
	if err != nil {
		// Transient lookup failure — NAK so the binding can be resolved on retry.
		return fmt.Errorf("%w: resolve monitor for %q", natsutil.ErrProcessingFailed, model)
	}
	if !found {
		// The completion's model isn't monitored — nothing to evaluate. ACK + drop (the
		// safe default; never fabricate a tenant). The vast majority of LLM traffic may
		// be for unmonitored models, so this is the common, cheap path.
		c.log.DebugContext(ctx, "AiCompletionServed for unmonitored model — skip",
			slog.String("event.id", env.ID), slog.String("model", model))
		return nil
	}

	// TENANCY CROSS-CHECK: the gateway-resolved team should match the monitor's owner
	// team. A mismatch means this event is for a model another team monitors under the
	// same name (the same-name caveat the resolver documents) — drop it rather than
	// evaluate it under the wrong tenant. Empty event team is tolerated (older gateway),
	// the monitor's owner team is authoritative downstream regardless.
	if ev.Team != "" && binding.OwnerTeam != "" && ev.Team != binding.OwnerTeam {
		c.log.DebugContext(ctx, "AiCompletionServed team != monitor owner team — skip (same-name across tenants)",
			slog.String("event.id", env.ID), slog.String("model", model),
			slog.String("event_team", ev.Team), slog.String("owner_team", binding.OwnerTeam))
		return nil
	}

	// SAMPLE: bound the judge's Ollama load to ~1-in-N. A non-sampled event is ACKed
	// WITHOUT judging (the completion still got served + billed elsewhere; we just don't
	// quality-grade every one). Sampling happens AFTER the cheap drops above so the
	// 1-in-N rate applies to MONITORED traffic, not the whole firehose.
	if !c.sampler.ShouldSample() {
		c.log.DebugContext(ctx, "AiCompletionServed not sampled — skip judge",
			slog.String("event.id", env.ID), slog.String("model", model))
		return nil
	}

	// GRACEFUL DEGRADATION: if the gateway ran with EvalIncludeText off, there is no
	// response text to judge — the QualityEvalService will record the eval UNSCORED.
	// Log the limitation ONCE (not per-event) so an operator knows quality scores will
	// be empty until FP_AI_EVAL_INCLUDE_TEXT is enabled on the gateway.
	if strings.TrimSpace(ev.ResponseText) == "" && !c.loggedNoText {
		c.loggedNoText = true
		c.log.WarnContext(ctx, "AiCompletionServed has no response text — judging will record UNSCORED; "+
			"enable FP_AI_EVAL_INCLUDE_TEXT on the ai-gateway to score quality",
			slog.String("model", model))
	}

	comp := domain.Completion{
		Team:      binding.OwnerTeam, // authoritative tenant (monitor owner), not the event field
		Model:     model,
		RequestID: ev.RequestID,
		Prompt:    ev.PromptText,
		Response:  ev.ResponseText,
	}

	if _, err := c.svc.EvaluateCompletion(ctx, binding.MonitorID, comp); err != nil {
		// A domain error here is a storage/scoring failure (record eval, save report,
		// trigger retrain) — TRANSIENT. NAK so it's retried; the eval's request_id
		// idempotency + the report's window_id idempotency make the retry safe (no
		// double-record, no double-emit). The judge itself NEVER returns an error that
		// reaches here (it degrades to Unscored), so a flaky judge can't NAK-storm.
		c.log.WarnContext(ctx, "EvaluateCompletion failed — NAK/retry",
			slog.String("event.id", env.ID), slog.String("model", model))
		return fmt.Errorf("%w: evaluate completion for %q", natsutil.ErrProcessingFailed, model)
	}
	return nil
}
