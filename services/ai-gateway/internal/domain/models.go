// models.go — the AI Gateway's pure domain types (ZERO framework imports).
//
// ============================================================================
// WHY A SEPARATE DOMAIN VOCABULARY (and not just the generated proto types)
// ============================================================================
//
// The handler speaks proto (aiv1.ChatCompletionRequest, ChatRole, ProviderKind);
// the domain speaks THESE types. The boundary mapping lives in the handler. This
// is the same anti-corruption discipline every Forgepoint service uses: the
// business logic never imports gen/go, so a proto regeneration can never ripple a
// type change into the failover/budget/cost logic — only the (thin, total)
// handler mapping changes. It also keeps these types trivially constructible in a
// unit test with no proto plumbing.
//
// The vocabulary mirrors the OpenAI / Ollama chat shape (messages with roles, a
// streamed delta, a terminal usage frame) because that is the lingua franca every
// LLM backend speaks — adopting it means a new provider adapter is a thin HTTP
// translation, not a re-modelling.
package domain

import "time"

// Role is a chat turn's author. Mirrors aiv1.ChatRole 1:1 (the handler maps).
type Role int

const (
	RoleUnspecified Role = iota
	RoleSystem           // instruction/persona; steers the assistant.
	RoleUser             // the human turn.
	RoleAssistant        // a prior model turn (multi-turn context).
)

// Message is one turn in a conversation. The smallest unit a provider consumes.
type Message struct {
	Role    Role
	Content string
}

// ProviderKind identifies a model backend. Mirrors aiv1.ProviderKind. OLLAMA +
// STUB ship built-in; the cloud kinds are reserved (not wired in this core) so the
// failover story works with the local model + the deterministic stub, no cloud
// account required.
type ProviderKind int

const (
	ProviderKindUnspecified ProviderKind = iota
	ProviderKindOllama                   // local, self-hosted (in-cluster serving).
	ProviderKindStub                     // deterministic fake; backs all CI/tests.
	ProviderKindOpenAI                   // reserved (not wired here).
	ProviderKindAnthropic                // reserved (not wired here).
)

// String gives a stable human label used in logs/events/metrics. Kept here (not a
// proto String()) so the domain never reaches for the generated enum.
func (k ProviderKind) String() string {
	switch k {
	case ProviderKindOllama:
		return "ollama"
	case ProviderKindStub:
		return "stub"
	case ProviderKindOpenAI:
		return "openai"
	case ProviderKindAnthropic:
		return "anthropic"
	default:
		return "unspecified"
	}
}

// FinishReason explains why a completion stopped. Mirrors aiv1.FinishReason.
type FinishReason int

const (
	FinishReasonUnspecified   FinishReason = iota
	FinishReasonStop                       // natural end / stop sequence.
	FinishReasonLength                     // hit max_tokens.
	FinishReasonContentFilter              // guardrail blocked it.
	FinishReasonError                      // provider/backend error after failover.
)

// ChatRequest is the domain form of a chat completion ask. The TEAM is NOT here —
// it is a separate, claims-derived value the SERVICE receives, never trusted from
// this struct. Keeping team out of the request type makes the "never trust the
// body" rule structurally enforced: there is no field to accidentally read.
type ChatRequest struct {
	// Model is the logical model name (e.g. "smollm2:135m"); empty = provider default.
	Model string
	// Messages is the conversation so far (system/user/assistant turns).
	Messages []Message
	// Temperature / MaxTokens are optional sampling controls (0 = provider default).
	Temperature float32
	MaxTokens   int32
	// Provider, when set, PINS a specific backend (the caller opts out of failover
	// routing). Unspecified = route through the configured default order with failover.
	Provider ProviderKind
	// RequestID is the gateway-minted correlation id, stamped on every frame and on
	// the served/warm events. Server-authoritative (never client-supplied).
	RequestID string
}

// TokenUsage is the cost accounting for one completion. Mirrors aiv1.TokenUsage.
// CostMicroUSD is computed by the service from a per-provider rate table — the
// gateway is the single place that attaches a price to tokens (the metering seam
// L2 reconciles against).
type TokenUsage struct {
	PromptTokens     int32
	CompletionTokens int32
	TotalTokens      int32
	CostMicroUSD     int64
}

// Delta is one frame of a streamed completion — the unit a Provider yields on its
// channel and the handler maps to an aiv1.ChatCompletionResponse frame.
//
// STREAM SHAPE (the contract every Provider MUST honor):
//   - Zero or more NON-terminal deltas: Done=false, Text carries incremental
//     tokens, Usage is nil, FinishReason is Unspecified.
//   - EXACTLY ONE terminal delta LAST: Done=true, Usage is the final accounting,
//     FinishReason explains why it stopped. Text on the terminal frame is empty
//     (the content already streamed). The channel is then closed.
//
// WHY a single terminal frame carries usage (and not a side return): a stream has
// no return value once it starts; the only way to deliver the final token counts
// is in-band on the last frame. This matches Ollama's NDJSON (the `done:true` line
// carries prompt_eval_count/eval_count) and OpenAI's SSE (the final chunk carries
// usage when requested) — so the domain shape is a faithful superset of both.
type Delta struct {
	Text         string
	Done         bool
	Usage        TokenUsage
	FinishReason FinishReason
}

// CircuitState is the breaker's externally-visible state, surfaced by ListProviders
// so an operator can see WHY a provider is (or isn't) taking traffic. Mirrors the
// inference-gateway's CircuitState taxonomy.
type CircuitState int

const (
	CircuitClosed CircuitState = iota
	CircuitOpen
	CircuitHalfOpen
)

// String renders the wire/observability label (CLOSED|OPEN|HALF_OPEN) the proto's
// Provider.circuit_state field expects.
func (s CircuitState) String() string {
	switch s {
	case CircuitOpen:
		return "OPEN"
	case CircuitHalfOpen:
		return "HALF_OPEN"
	default:
		return "CLOSED"
	}
}

// ProviderSnapshot is a read-only view of one configured backend for ListProviders:
// its kind/name/enabled flag, the models it serves, and its LIVE circuit state. It
// turns an invisible failure mode ("why is Ollama getting no traffic?") into an
// observable one.
type ProviderSnapshot struct {
	Kind         ProviderKind
	Name         string
	Enabled      bool
	Models       []string
	CircuitState CircuitState
}

// Completion is the terminal summary the SERVICE returns to the handler AFTER the
// stream drains: who actually served (post-failover), the final usage, and the
// finish reason. The handler already streamed the deltas; this is the metadata it
// needs for the terminal proto frame and the served/warm events.
type Completion struct {
	ServedBy     ProviderKind
	Usage        TokenUsage
	FinishReason FinishReason
	CacheHit     bool
	Latency      time.Duration
}
