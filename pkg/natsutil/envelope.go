package natsutil

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
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
//   - ID: UUID v4 for idempotent consumption (consumers track processed IDs)
//   - Type: Dot-notation event type (e.g., "model.registered")
//   - Source: Service name that produced the event
//   - Timestamp: When the event was produced (not consumed)
//   - CorrelationID: Links async events to the originating request's trace
//   - Data: Raw JSON payload — the actual event data
//
// HOW IT'S USED:
//   Publisher:
//     pub.Publish(ctx, "goml.models.registered", ModelRegisteredEvent{...})
//     → serializes event, wraps in envelope, publishes to NATS
//
//   Subscriber:
//     sub.Subscribe(ctx, "goml.models.>", func(ctx, env) {
//         var event ModelRegisteredEvent
//         json.Unmarshal(env.Data, &event)
//     })
//     → receives envelope, handler gets raw Data to unmarshal
// ============================================================================

// EventEnvelope wraps every NATS event with standard metadata.
type EventEnvelope struct {
	ID            string          `json:"id"`
	Type          string          `json:"type"`
	Source        string          `json:"source"`
	Timestamp     time.Time       `json:"timestamp"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	Data          json.RawMessage `json:"data"`
}

// NewEnvelope creates a new event envelope with a generated UUID and current
// timestamp. The eventType should be dot-notation (e.g., "model.registered").
func NewEnvelope(eventType, source string, data json.RawMessage) EventEnvelope {
	return EventEnvelope{
		ID:        uuid.New().String(),
		Type:      eventType,
		Source:    source,
		Timestamp: time.Now().UTC(),
		Data:      data,
	}
}
