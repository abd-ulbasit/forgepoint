// Package events is the AI Gateway's NATS adapter layer — the OUTER ring that
// bridges the pure domain to the async event bus. It implements the domain's
// EventPublisher port: it ensures the platform AI streams exist, publishes the
// canonical fp.ai.completion.served cost/audit event, and publishes the lightweight
// warm signal that wakes a scaled-to-zero Ollama via KEDA.
//
// ============================================================================
// WHERE THIS SITS (Clean Architecture)
// ============================================================================
//
//	internal/domain     defines EventPublisher + CompletionServed/WarmSignal   (pure)
//	     ▲
//	     │ implements
//	internal/events     Publisher (impl) · EnsureStreams                ← THIS PACKAGE
//	     │ uses
//	     ▼
//	pkg/natsutil        envelopes · dedup Msg-Id · trace propagation · JSON dialect
//
// The domain never imports NATS; this adapter owns the envelope, subject, encoding,
// and at-least-once delivery.
//
// ============================================================================
// THE TWO STREAMS THIS SERVICE OWNS
// ============================================================================
//
//	AI            owns fp.ai.>          — the cost/audit log (fp.ai.completion.served).
//	                                       L2 (Billing metering) consumes this.
//	AI_REQUESTS   owns fp.ai.warm.>     — the warm-signal log. The KEDA ScaledObject
//	                                       (deploy/llm/ollama-scaledobject.yaml) watches
//	                                       its `ollama-warmer` consumer's pending count
//	                                       and scales Ollama 0->1 when lag >= 1.
//
// WHY TWO STREAMS (not one): the warm signal is OPERATIONAL plumbing with a
// different retention/consumer story than the cost event — KEDA's scaler binds to a
// dedicated stream+consumer whose LAG is the scaling signal, and that lag must not
// be polluted by (or compete with) the durable cost/audit log that Billing reads at
// its own pace. Separate streams keep the scaler's lag a clean demand signal.
//
// In production these streams are provisioned once at platform bootstrap; this
// service ALSO ensures them on boot (idempotent CreateOrUpdateStream) so a fresh
// cluster / local run works without a separate bootstrap step.
package events

// ----------------------------------------------------------------------------
// CANONICAL SUBJECTS — the single source of truth for this service's wire names.
// ----------------------------------------------------------------------------
const (
	// Produced by the gateway after every completion (cost + audit metering).
	SubjectCompletionServed = "fp.ai.completion.served"

	// Produced by the gateway BEFORE serving to wake a scaled-to-zero Ollama. KEDA's
	// ollama-warmer consumer binds to this subject on the AI_REQUESTS stream.
	SubjectWarmRequest = "fp.ai.warm.requested"
)

// Source is stamped into EventEnvelope.source for every event this service
// publishes (WHO produced it; the subject says WHICH resource).
const Source = "ai-gateway"

// ----------------------------------------------------------------------------
// JETSTREAM STREAMS this service owns/produces into.
// ----------------------------------------------------------------------------
const (
	// StreamAI owns fp.ai.> EXCEPT the warm subjects (which live on AI_REQUESTS so
	// the KEDA scaler's lag is isolated). We bind StreamAI to the completion subject
	// explicitly rather than the fp.ai.> wildcard so the two streams don't both claim
	// the warm subject (a subject may belong to only one stream).
	StreamAI = "AI"
	// StreamAIRequests owns the warm subject; its lag is KEDA's 0->1 scaling signal.
	StreamAIRequests = "AI_REQUESTS"
)
