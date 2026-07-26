// ports.go — the PORTS (interfaces) the AI Gateway domain depends on.
//
// ============================================================================
// HEXAGONAL PORTS: the domain is the CONSUMER, the adapters implement
// ============================================================================
//
// The domain consumes three outbound capabilities — calling a model backend
// (Provider), checking/deducting a team's token budget (BudgetStore), and
// emitting cost/warm events (EventPublisher). In Hexagonal Architecture the
// CONSUMER owns the port; the adapter (the Ollama HTTP client, the Redis budget
// store, the NATS publisher) implements it one layer out. Defining them here (not
// in internal/providers or internal/events) keeps a single inward dependency
// arrow and avoids the import cycle Go would reject (the port signatures
// reference domain types; the adapters import domain).
//
//	domain (Provider/BudgetStore/EventPublisher ports + the GatewayService) ← stdlib only
//	   ▲
//	   │ implements
//	providers (Ollama/Stub HTTP)   redis budget store   NATS publisher   (outer)
//
// ============================================================================
package domain

import "context"

// ============================================================================
// PROVIDER PORT — a model backend the gateway can route to
// ============================================================================

// Provider is one model backend (Ollama, the stub, later a cloud LLM). It is the
// unit the failover loop iterates over: the service asks each eligible provider,
// in order, to Chat until one streams successfully.
//
// THE STREAMING CONTRACT (why a channel, not a slice or a callback):
//
//	Chat returns a RECEIVE-ONLY channel of Delta. The provider runs the backend
//	call on its OWN goroutine and pushes frames as they arrive; the caller ranges
//	over the channel. This is the idiomatic Go way to model an unbounded,
//	incrementally-produced sequence (vs. a slice, which forces buffering the whole
//	completion before the first token reaches the browser — defeating streaming;
//	vs. a callback, which inverts control and tangles cancellation). The channel is
//	CLOSED by the provider after it sends the single terminal Delta (Done=true),
//	which is how the caller knows the stream ended cleanly.
//
//	CANCELLATION: the provider MUST stop producing and close the channel when ctx
//	is cancelled (client disconnect, gateway shutdown). The caller propagates the
//	gRPC stream's context, so a hung backend can't leak a goroutine past the RPC.
//
//	ERROR DELIVERY — IN BAND, not a second return value: a stream has no return
//	value once it begins, so a mid-stream backend error is delivered as a terminal
//	Delta{Done:true, FinishReason:FinishReasonError}. WHY also a synchronous error
//	from Chat itself: a provider can fail BEFORE producing any frame (connection
//	refused, circuit logic upstream) — that is the synchronous error, and it is the
//	signal the FAILOVER loop keys on to try the next provider. Once Chat returns a
//	nil error and a live channel, the provider OWNS the outcome and reports any
//	later failure in-band (the caller has already committed to this provider for
//	this attempt and streamed its early tokens). So: synchronous error => not
//	started, try the next provider; in-band error frame => started then failed,
//	surfaced to the client as a terminal error (no silent re-attempt mid-stream,
//	which would duplicate tokens).
type Provider interface {
	// Kind identifies this provider for routing, events, and the breaker key.
	Kind() ProviderKind
	// Models lists the model names this provider can serve (for ListProviders and
	// for matching a request's pinned Model). Empty = serves any model name.
	Models() []string
	// Chat starts a streamed completion. A nil error + a live channel means the
	// stream started (range it to the terminal Done frame). A non-nil error means
	// the provider never started (try the next one). See the contract above.
	Chat(ctx context.Context, req ChatRequest) (<-chan Delta, error)
}

// ============================================================================
// BUDGET STORE PORT — the per-team token budget (RATE LIMITER, token-denominated)
// ============================================================================

// BudgetStore is the per-TEAM token budget gate, the LLM analogue of the inference
// gateway's rate limiter — but the "tokens" here are LLM tokens, not request
// tokens, and the bucket is metered per TEAM (the tenant), not per api-key. It
// realizes the same TOKEN-BUCKET semantics: a team holds up to `budget` tokens per
// window and a chat both CHECKS availability before serving and DEDUCTS the tokens
// it actually used after serving.
//
// WHY two methods (Check pre-flight, Deduct post-serve) and not one atomic spend:
// an LLM call's token cost is UNKNOWN until the completion finishes (you don't know
// how many tokens the model will emit). So we cannot atomically reserve the exact
// amount up front. The honest two-phase shape is: (1) pre-flight reject a team that
// is ALREADY over budget (Check), then (2) after serving, subtract the real usage
// (Deduct). A team can therefore overshoot its budget by at most one in-flight
// request's worth of tokens — acceptable for a soft cost cap (Billing reconciles
// the exact ledger). This mirrors the inference gateway's quota checker rationale
// (eventually-consistent soft cap) adapted to token accounting.
//
// The TEAM key always comes from the verified claims, NEVER a request field.
type BudgetStore interface {
	// Check reports whether the team may proceed (it is not yet over budget) and the
	// remaining token allowance. An infrastructure error (Redis down) is returned so
	// the use-case applies its fail-open/closed policy (we fail OPEN for a soft cap:
	// a transient Redis blip must not block all LLM traffic platform-wide).
	Check(ctx context.Context, team string) (allowed bool, remaining int64, err error)
	// Deduct subtracts `tokens` from the team's window allowance AFTER a completion
	// served. Best-effort and off the response path: the client already has its
	// answer, so a deduct failure is logged, not surfaced. Returns the new remaining
	// for observability/GetUsage.
	Deduct(ctx context.Context, team string, tokens int64) (remaining int64, err error)
	// Usage returns the team's consumed tokens and configured budget for GetUsage.
	// consumed = budget - remaining (clamped at >= 0).
	Usage(ctx context.Context, team string) (consumed int64, budget int64, err error)
}

// ============================================================================
// USAGE STORE PORT — the per-team prompt/completion/total accumulator (the FIX)
// ============================================================================
//
// THE BUG THIS PORT FIXES: GetUsage reported a correct TOTAL but prompt=0,
// completion=0 in the breakdown (observed live: total 27, prompt/completion 0). WHY:
// the only per-team counter was the BudgetStore, whose Deduct takes a single
// `tokens int64` (the total) — it never saw the prompt/completion SPLIT, so the
// breakdown was always zero. The budget bucket is the right shape for a REFILLING
// rate cap, but the wrong shape for an accurate cumulative breakdown.
//
// THE FIX: a dedicated, MONOTONIC per-team usage accumulator that records BOTH parts
// of every served completion. It is separate from the budget on purpose:
//   - the budget REFILLS over a rolling window (it's a rate cap that goes back up);
//   - usage ACCUMULATES (a lifetime/window counter that only goes up) and must keep
//     prompt vs completion distinct for the GetUsage breakdown.
//
// Conflating them is exactly what zeroed the breakdown.
type UsageStore interface {
	// Add records one served completion's prompt + completion tokens for `team`
	// (total is derived = prompt + completion). Best-effort, post-serve: a record
	// failure is logged, not surfaced (the client already has its answer).
	Add(ctx context.Context, team string, promptTokens, completionTokens int32) error
	// Get returns the team's accumulated prompt/completion/total tokens for GetUsage.
	Get(ctx context.Context, team string) (promptTokens, completionTokens, totalTokens int64, err error)
}

// ============================================================================
// EVENT PUBLISHER PORT — the async outcome of every completion + the warm signal
// ============================================================================

// EventPublisher is the port the use-case calls to (a) publish the canonical
// fp.ai.completion.served cost/audit event after every completion and (b) publish
// the lightweight WARM signal that wakes a scaled-to-zero Ollama (the KEDA
// ScaledObject scales 0->1 on the AI_REQUESTS stream lag).
//
// WHY publishing is a PORT (not a direct NATS call in the use-case): the domain
// must not import NATS. The use-case decides WHAT business fact occurred and hands
// a pure payload to the port; the adapter owns the envelope, subject, protojson
// encoding, trace propagation, and at-least-once delivery.
//
// BEST-EFFORT, OFF THE RESPONSE PATH: a publish error must NOT fail a completion
// the client already received. The port returns the error so the use-case can
// log-and-continue.
//
// WARM-FIRST ORDERING: the use-case publishes the warm signal
// BEFORE it calls the provider (so a cold Ollama starts scaling up while the
// gateway retries with backoff), and publishes the served event AFTER. The served
// event drains nothing; the warm signal is acked by the ollama-warmer consumer once
// serving is up, draining the lag back toward zero.
type EventPublisher interface {
	// PublishCompletionServed emits fp.ai.completion.served (best-effort).
	PublishCompletionServed(ctx context.Context, ev CompletionServed) error
	// PublishWarmSignal emits a lightweight warm request to the AI_REQUESTS stream so
	// KEDA scales Ollama 0->1. Best-effort: if it fails, a warm Ollama still serves.
	PublishWarmSignal(ctx context.Context, ev WarmSignal) error
}

// CompletionServed is the domain payload for the cost/audit event. It carries the
// billing facts the gateway alone authoritatively knows — team, model, the provider
// that ACTUALLY served (post-failover), the token counts, the computed cost, the
// cache-hit flag, and the latency.
//
// PII DISCIPLINE — the optional text fields (M7/L4 quality evaluation):
//
//	By DEFAULT this event carries NO message content (PromptText/ResponseText empty)
//	— prompt/completion text never leaves the gateway on an event. That is the right
//	default: the cost/audit event is consumed by Billing (metering) which has no
//	business seeing raw prompts, and an event bus is a poor place for PII.
//
//	The two text fields exist ONLY for the model-monitor's LLM-as-judge quality
//	evaluation (L4): an OFFLINE judge cannot score relevance/coherence/safety without
//	the actual prompt+response. They are populated ONLY when the gateway is started
//	with FP_AI_EVAL_INCLUDE_TEXT=true (a deliberate, operator-owned opt-in for the
//	homelab quality-eval pipeline). When the flag is off the monitor's judge degrades
//	gracefully: it records the completion as UNSCORED and notes the limitation rather
//	than judging blind. Keeping the fields OPTIONAL (and the flag default-off) means
//	the privacy-preserving posture is the default and turning on quality eval is a
//	conscious decision, exactly the tradeoff a reviewer expects.
type CompletionServed struct {
	RequestID    string
	Team         string
	Model        string
	Provider     ProviderKind
	PromptTokens int32
	OutputTokens int32
	TotalTokens  int32
	CostMicroUSD int64
	CacheHit     bool
	LatencyMs    int64

	// PromptText / ResponseText are the raw turn content, populated ONLY when the
	// gateway's EvalIncludeText flag is on (see the PII note above). Empty otherwise.
	// They feed the model-monitor LLM-as-judge; no other consumer reads them.
	PromptText   string
	ResponseText string
}

// WarmSignal is the minimal payload that wakes Ollama. It only needs to create lag
// on the AI_REQUESTS stream; the model name is included for observability/routing
// but the KEDA scaler only counts pending messages.
type WarmSignal struct {
	RequestID string
	Team      string
	Model     string
	Provider  ProviderKind
}
