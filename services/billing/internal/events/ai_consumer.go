package events

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/services/billing/internal/domain"
	"github.com/nats-io/nats.go/jetstream"
)

// ============================================================================
// AI CONSUMER — meter AI token cost by reacting to fp.ai.completion.served
// ============================================================================
//
// This is the THIRD money axis billing consumes (M7/L2), alongside the inference
// per-call fee and the per-artifact storage fee. The AI Gateway (services/ai-
// gateway) emits one fp.ai.completion.served event after EVERY LLM completion it
// serves, carrying the billing facts it alone authoritatively knows: the team, the
// model, the provider that actually served (post-failover), the prompt/completion/
// total token counts, the gateway-computed cost (cost_micro_usd), the cache-hit
// flag, and the latency. Billing meters those tokens so a team's AI spend flows
// through the SAME usage ledger + outbox machinery as inference and storage — and
// therefore shows up in GetUsage and on the period invoice.
//
// ----------------------------------------------------------------------------
// PLAIN JSON, NOT protojson (the decode-convention call-out the task flags)
// ----------------------------------------------------------------------------
//
// The inference and storage consumers decode with protojson because their payloads
// are forgepoint/events/v1 PROTO messages (with google.protobuf.Timestamp fields
// that ONLY protojson round-trips). The AI completion event is DIFFERENT: there is
// no events.v1 proto message for it (the events proto predates M7 and the hard rule
// forbids regenerating protos), so the AI Gateway publishes it as a PLAIN Go struct
// that natsutil.Publisher marshals with encoding/json (see ai-gateway/internal/
// events/publisher.go marshalPayload's non-proto branch). It carries NO
// google.protobuf.* well-known types — only strings, ints, and a bool — so plain
// encoding/json is the correct, SYMMETRIC codec. Decoding it with protojson would
// fail (protojson cannot unmarshal into a non-proto type). This is the exact
// "protojson-vs-json" convention the billing events package documents: match the
// codec to the producer.
//
// ----------------------------------------------------------------------------
// WHAT WE METER — tokens through the existing INFERENCE_TOKENS meter
// ----------------------------------------------------------------------------
//
// We record ONE domain.RecordUsage(INFERENCE_TOKENS, quantity = total_tokens) per
// event. WHY reuse INFERENCE_TOKENS rather than invent a new meter:
//   - It reuses billing's existing usage-metering + outbox machinery WITHOUT a
//     proto/domain change (the task's explicit constraint: "the same way billing
//     records other metered usage", zero drift, no proto regen). An LLM completion
//     IS token-metered inference; the token axis already exists and is already
//     priced by every rate plan, so AI spend lands in the same per-meter rollup a
//     team already sees.
//   - RecordUsage is SERVER-AUTHORITATIVE on price: it resolves the team's rate
//     plan and computes cost = billable_tokens × plan_unit_price. Billing never
//     accepts a caller-supplied cost (the mass-assignment guard — a client/event
//     naming its own price is free money). So the BILLED amount is billing's
//     plan-priced token cost, computed inside RecordUsage exactly like inference.
//
// THE GATEWAY'S cost_micro_usd — carried for AUDIT, not billed here. The event's
// CostMicroUSD is the gateway's own per-provider cost estimate (what the gateway
// paid OpenAI/Anthropic/Ollama). It is NOT the customer's bill: the customer is
// billed at the team's plan rate. We thread it through to the metering call's
// attribution/lineage (it travels on the SourceRequestID-anchored record and is
// available to logs/tracing) so the gateway's cost and billing's charge are both
// observable, but pricing stays in billing's plan. If a deployment ever wants
// cost-passthrough (bill the gateway's exact cost), that is a domain change (a
// cost-passthrough meter) — deliberately out of scope here to keep pricing
// server-authoritative and the change zero-drift.
//
// TOKEN-COUNT GUARD: a completion with total_tokens <= 0 (a pure cache hit that
// reports zero billable tokens, or a malformed count) has nothing to meter. We ACK
// it as a NO-OP (not poison, not a loop) — the same posture the storage consumer
// takes for a zero-byte artifact. There is no per-call fee on this axis (that is
// the inference consumer's job on fp.inference.completed); the AI event is a pure
// token-cost fact.
//
// ----------------------------------------------------------------------------
// TEAM ATTRIBUTION — why NO TeamResolver here (the key difference)
// ----------------------------------------------------------------------------
//
// The inference and storage consumers carry a reference (api_key_id / model_id) and
// must RESOLVE it to the billed team via a server-side port, because the billed
// team must never be client-asserted. The AI event is different: the AI Gateway is
// a trusted PLATFORM service that ALREADY resolved the team server-side (from the
// caller's api key) before emitting the event, and it stamps the resolved `team`
// directly on the payload. There is no api_key on this event to re-resolve. So the
// event's `team` IS server-authoritative — it came from a trusted internal producer
// over the internal bus, exactly like the gateway's cost_micro_usd is trusted. An
// event with an empty team is therefore a PRODUCER bug / corruption, not client
// input: we treat it as poison (we cannot attribute the charge) rather than guess.
//
// ----------------------------------------------------------------------------
// IDEMPOTENCY (every consumer must survive duplicate redelivery — the contract)
// ----------------------------------------------------------------------------
//
// TWO layers guarantee a duplicate event meters AT MOST ONCE:
//   (a) TRANSPORT: the natsutil Subscriber's ProcessedStore dedupes on
//       EventEnvelope.id — a redelivered envelope id is ACKed WITHOUT re-running
//       the handler. Wired via WithIdempotencyStore. We ALSO consult the same store
//       directly in dispatch() so envelope-id dedupe holds even when the handler is
//       driven outside the subscriber (and so it is unit-testable without a broker).
//   (b) BUSINESS: the RecordUsage call carries an IdempotencyKey derived from the
//       event's request_id (request_id + ":ai-tokens"). domain.RecordUsage dedupes
//       on (team, idempotency_key): a repeat returns the ORIGINAL record and inserts
//       nothing. So even a redelivery AFTER the transport dedup window — or to a
//       different replica with a separate store — meters the completion exactly once.
//   The ":ai-tokens" suffix keeps this key from ever colliding with the inference
//   consumer's request_id / request_id+":tokens" keys (an AI request_id and an
//   inference request_id could in principle be equal strings).
//
// DLQ (poison messages): an event that can never be metered (empty team, empty
// request_id, an unpriced-meter/no-rate-plan mapping) routes to fp.dlq.billing after
// MaxRetries via the shared natsutil DLQ machinery — exactly like the other two
// consumers. A genuinely TRANSIENT failure (DB down) NAKs and retries.
// ============================================================================

// aiCompletionServed is billing's read-model of the AI Gateway's
// fp.ai.completion.served wire payload. The json tags MIRROR the producer's struct
// (services/ai-gateway/internal/events/publisher.go completionServedPayload) field-
// for-field: lowerCamelCase, the proto3-JSON convention the platform's events use.
//
// WHY a billing-owned struct (not an import of the gateway's type): the gateway's
// completionServedPayload is unexported and lives in another module — and, per
// database/service-per-service boundaries, billing must not depend on the gateway's
// internals. This is the events layer's anti-corruption boundary: we re-declare the
// wire contract we consume and own our decode. If the gateway adds a field, this
// struct simply ignores it (encoding/json drops unknown fields), and if it RENAMES
// one a test here fails — the contract is greppable in one place.
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
}

// AIConsumer subscribes to fp.ai.completion.served and meters AI token usage.
//
// NOTE on the absence of a TeamResolver field (vs InferenceConsumer/StorageConsumer):
// the AI event carries the gateway-resolved team directly, so this consumer needs no
// resolver port — see the "TEAM ATTRIBUTION" section above.
type AIConsumer struct {
	svc   domain.BillingService
	store natsutil.ProcessedStore
	cfg   SubConfig
	js    jetstream.JetStream
	sub   *natsutil.Subscriber
}

// NewAIConsumer builds the AI-metering consumer. Same shape as the other two
// consumers: the caller (main.go) injects the JetStream context, the domain
// BillingService (whose RecordUsage meters + writes the outbox), and the
// idempotency ProcessedStore. SubConfig zero-values get platform defaults via
// withDefaults; we give the consumer its OWN durable name so it does not collide
// with the inference/storage consumers' durables in one process.
func NewAIConsumer(
	js jetstream.JetStream,
	svc domain.BillingService,
	store natsutil.ProcessedStore,
	cfg SubConfig,
) *AIConsumer {
	// Distinct durable name from the inference ("billing") and storage
	// ("billing-storage") consumers: two durable consumers in one process cannot
	// share a name (they would fight over one JetStream consumer). A caller may
	// still override via SubConfig.ConsumerGroup.
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = Source + "-ai"
	}
	cfg.withDefaults()
	return &AIConsumer{svc: svc, store: store, cfg: cfg, js: js}
}

// Register starts the subscription on the AI stream filtered to
// fp.ai.completion.served. Call exactly once.
func (c *AIConsumer) Register(ctx context.Context) error {
	sub := natsutil.NewSubscriber(c.js,
		natsutil.WithConsumerGroup(c.cfg.ConsumerGroup),
		natsutil.WithMaxRetries(c.cfg.MaxRetries),
		natsutil.WithDLQSubject(c.cfg.DLQSubject),
		natsutil.WithIdempotencyStore(c.store),
		natsutil.WithMessageTimeout(c.cfg.MessageTimeout),
	)
	if err := sub.Subscribe(ctx, StreamAI, SubjectAICompletionServed, c.dispatch); err != nil {
		return fmt.Errorf("events: subscribe %s on %s: %w", SubjectAICompletionServed, StreamAI, err)
	}
	c.sub = sub
	return nil
}

// Close stops the consume loop. Safe to call multiple times and before/after
// Register (a nil sub is a no-op).
func (c *AIConsumer) Close() {
	if c.sub != nil {
		c.sub.Close()
	}
}

// dispatch is the per-envelope entry point the subscriber invokes. It enforces
// envelope-id idempotency against the ProcessedStore directly (in ADDITION to the
// natsutil Subscriber's own check) and then runs handle. Doing the dedupe here too
// makes envelope-id idempotency:
//   - hold even if the handler is driven outside the subscriber, and
//   - UNIT-TESTABLE without a NATS broker (the test calls dispatch twice with the
//     same envelope id and asserts RecordUsage runs once).
//
// ORDER (record-then-... matters): we MarkProcessed only AFTER handle succeeds, so a
// handler error leaves the id un-marked and the event is retried (at-least-once). A
// store that is already-processed short-circuits to a no-op ACK (return nil).
func (c *AIConsumer) dispatch(ctx context.Context, env natsutil.EventEnvelope) error {
	if c.store != nil {
		processed, err := c.store.IsProcessed(ctx, env.ID)
		if err != nil {
			// A store read failure is TRANSIENT (Redis blip) — NAK + retry rather than
			// risk double-metering by proceeding blind. Returned unwrapped (not poison).
			return fmt.Errorf("events: idempotency check for AI event %s: %w", env.ID, err)
		}
		if processed {
			// Duplicate envelope id — already handled. No-op ACK, no second meter.
			return nil
		}
	}

	if err := c.handle(ctx, env); err != nil {
		return err
	}

	if c.store != nil {
		if err := c.store.MarkProcessed(ctx, env.ID); err != nil {
			// We DID meter successfully but failed to record the dedupe marker. Returning
			// an error NAKs → the event is redelivered → the BUSINESS idempotency key
			// (request_id+":ai-tokens") makes the re-meter a no-op (RecordUsage returns the
			// original record). So a missed marker never double-bills; it just costs a
			// redelivery. Better to retry than to silently drop the marker and risk a
			// double-effect on a transport that hasn't recorded the id either.
			return fmt.Errorf("events: mark AI event %s processed: %w", env.ID, err)
		}
	}
	return nil
}

// handle decodes the AI completion event and meters its tokens.
func (c *AIConsumer) handle(ctx context.Context, env natsutil.EventEnvelope) error {
	var ev aiCompletionServed
	// encoding/json (NOT protojson): the AI Gateway publishes this as a PLAIN Go
	// struct (no proto message, no google.protobuf.* fields) — see the package
	// doc's "PLAIN JSON, NOT protojson" call-out. Decoding must match the producer.
	if err := json.Unmarshal(env.Data, &ev); err != nil {
		// Corrupt payload — redelivery can't fix it → poison → DLQ.
		return fmt.Errorf("%w: decode AiCompletionServed from event %s: %v",
			natsutil.ErrProcessingFailed, env.ID, err)
	}

	// request_id is the SERVER-authoritative business idempotency anchor (the
	// gateway mints it per completion). Absent → we cannot safely dedupe a
	// redelivery → poison rather than risk double-metering.
	requestID := ev.RequestID
	if requestID == "" {
		return fmt.Errorf("%w: AiCompletionServed with empty request_id (no idempotency anchor)",
			natsutil.ErrProcessingFailed)
	}

	// team is the gateway-resolved, SERVER-authoritative billed account (see the
	// "TEAM ATTRIBUTION" section). An empty team is a producer bug / corruption — we
	// cannot attribute the charge → poison (NOT a guess, NOT an infinite retry).
	team := ev.Team
	if team == "" {
		return fmt.Errorf("%w: AiCompletionServed %s with empty team (cannot attribute charge)",
			natsutil.ErrProcessingFailed, requestID)
	}

	// total_tokens is the metered quantity. <= 0 (a pure cache hit reporting zero
	// billable tokens, or a malformed count) has nothing to bill → NO-OP ACK, not
	// poison and not a loop (mirrors the storage consumer's zero-byte handling).
	tokens := int64(ev.TotalTokens)
	if tokens <= 0 {
		return nil
	}

	// ----- METER: AI tokens through the existing INFERENCE_TOKENS axis -----
	// Quantity = total_tokens; RecordUsage prices it at the team's plan rate
	// (server-authoritative — the gateway's cost_micro_usd is NOT the billed amount;
	// see the package doc). The idempotency key is request_id+":ai-tokens" (distinct
	// from the inference consumer's keys so the two never collide). OccurredAt is left
	// zero so the domain stamps now(): the plain-JSON event carries only a latency_ms,
	// not an RFC-3339 completion timestamp, and back-dating to a derived time risks
	// landing in a closed billing period — "now" is the safe, monotonic choice.
	input := domain.RecordUsageInput{
		MeterType:       domain.MeterTypeInferenceTokens,
		Quantity:        tokens,
		ModelID:         ev.Model,
		SourceRequestID: requestID,
		IdempotencyKey:  requestID + ":ai-tokens",
	}
	return c.meter(ctx, team, input)
}

// meter calls domain.RecordUsage and classifies the outcome for the subscriber's
// ACK/NAK/DLQ machinery — IDENTICAL classification to the inference and storage
// consumers (permanent billing errors → poison/DLQ; transient infra faults → NAK +
// retry). Shared via the package-level isPermanentBillingError so all three axes
// route errors the same way.
func (c *AIConsumer) meter(ctx context.Context, team string, input domain.RecordUsageInput) error {
	_, _, err := c.svc.RecordUsage(ctx, team, input)
	if err == nil {
		return nil
	}
	if isPermanentBillingError(err) {
		return fmt.Errorf("%w: meter %s for team=%s: %v",
			natsutil.ErrProcessingFailed, input.MeterType, team, err)
	}
	// Transient — NAK + retry (a meter that already committed on a prior attempt is
	// deduped by its idempotency key, so a retry never double-bills).
	return fmt.Errorf("events: meter %s for team=%s: %w", input.MeterType, team, err)
}
