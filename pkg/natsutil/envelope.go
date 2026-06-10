package natsutil

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// ============================================================================
// EVENT ENVELOPE
// ============================================================================
//
// WHY: Every async event published to NATS is wrapped in a standard envelope.
// This ensures consistent metadata across all events regardless of which
// service publishes them. The envelope is the async equivalent of HTTP
// headers — it carries routing, deduplication, and tracing metadata.
//
// ENVELOPE vs RAW PAYLOAD:
//   Without envelope: each consumer must know the exact payload structure.
//   Adding metadata (correlation_id, timestamp) requires changing every
//   publisher and consumer.
//
//   With envelope: metadata is standardized. Consumers can inspect envelope
//   fields without knowing the payload type. New metadata fields are added
//   once (here) and all events get them automatically.
//
// FIELDS:
//   - ID: UUID v4. Two uses: (1) JetStream Nats-Msg-Id dedup on publish,
//     (2) consumer-side idempotency key (see ProcessedStore in subscriber.go).
//   - Type: Dot-notation event type (e.g., "registered", "user.created").
//   - Source: Service name that produced the event.
//   - Timestamp: When the event was produced (not consumed).
//   - CorrelationID: Links a chain of async events back to the originating
//     request. Reused across hops so one logical workflow shares an ID.
//   - TraceContext: W3C trace carrier (traceparent/baggage) so a distributed
//     trace continues across the async NATS boundary into the consumer.
//   - Data: Raw JSON payload — the actual event data.
//
// HOW IT'S USED:
//   Publisher:
//     pub.Publish(ctx, "fp.models.registered", ModelRegisteredEvent{...})
//     → serializes event, wraps in envelope, injects trace context, publishes
//
//   Subscriber:
//     sub.Subscribe(ctx, "MODELS", "fp.models.>", func(ctx, env) {
//         var event ModelRegisteredEvent
//         json.Unmarshal(env.Data, &event)
//     })
//     → extracts trace context into ctx, handler gets raw Data to unmarshal
// ============================================================================

// EventEnvelope wraps every NATS event with standard metadata.
type EventEnvelope struct {
	ID            string            `json:"id"`
	Type          string            `json:"type"`
	Source        string            `json:"source"`
	Timestamp     time.Time         `json:"timestamp"`
	CorrelationID string            `json:"correlation_id,omitempty"`
	TraceContext  map[string]string `json:"trace_context,omitempty"`
	Data          json.RawMessage   `json:"data"`
}

// NewEnvelope creates a new event envelope with a generated UUID and current
// timestamp. The eventType should be dot-notation (e.g., "model.registered").
//
// CorrelationID and TraceContext are populated separately by the Publisher from
// the request context — keeping NewEnvelope context-free makes it trivial to
// construct in tests.
func NewEnvelope(eventType, source string, data json.RawMessage) EventEnvelope {
	return EventEnvelope{
		ID:        uuid.New().String(),
		Type:      eventType,
		Source:    source,
		Timestamp: time.Now().UTC(),
		Data:      data,
	}
}

// ============================================================================
// CORRELATION ID + TRACE PROPAGATION
// ============================================================================
//
// These helpers are the glue that keeps observability working across async
// boundaries. Synchronous gRPC propagates trace context automatically via
// metadata; NATS does not, so we must carry it inside the envelope ourselves.
// ============================================================================

// correlationIDKey is the context key for the correlation ID.
type correlationIDKey struct{}

// ContextWithCorrelationID stores a correlation ID in the context so it is
// reused (not regenerated) when this request publishes further events.
func ContextWithCorrelationID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, correlationIDKey{}, id)
}

// CorrelationIDFromContext returns the correlation ID carried in ctx, if any.
func CorrelationIDFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(correlationIDKey{}).(string)
	return id, ok
}

// correlationIDForPublish picks the correlation ID to stamp on an outgoing
// event. Priority: an explicit ID already on the context (continue the chain) →
// the active trace ID (so events are still correlatable to the trace) → a fresh
// UUID (first hop with no tracing).
func correlationIDForPublish(ctx context.Context) string {
	if id, ok := CorrelationIDFromContext(ctx); ok {
		return id
	}
	if sc := trace.SpanContextFromContext(ctx); sc.HasTraceID() {
		return sc.TraceID().String()
	}
	return uuid.New().String()
}

// injectTraceContext writes the active span context into the envelope using the
// globally-configured OTel propagator (set in observability.Setup). This is
// what allows a trace to continue across the async NATS hop.
func injectTraceContext(ctx context.Context, env *EventEnvelope) {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if len(carrier) > 0 {
		env.TraceContext = carrier
	}
}

// extractTraceContext returns a context carrying the producer's trace context
// (extracted from the envelope), so spans created while handling the message
// link back to the producer's trace in Tempo.
func extractTraceContext(ctx context.Context, env EventEnvelope) context.Context {
	if len(env.TraceContext) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(env.TraceContext))
}
