package natsutil

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

// ============================================================================
// NATS PUBLISHER
// ============================================================================
//
// WHY a Publisher struct instead of raw JetStream.Publish:
//   1. Automatically wraps events in EventEnvelope (consistent metadata)
//   2. Sets the Nats-Msg-Id header for deduplication (JetStream feature)
//   3. Derives event type from subject for the envelope
//   4. Serializes payload to JSON (consistent serialization)
//
// Without Publisher:
//   data, _ := json.Marshal(event)
//   envelope := EventEnvelope{ID: uuid.New(), Type: "model.registered", ...}
//   bytes, _ := json.Marshal(envelope)
//   js.Publish(ctx, "goml.models.registered", bytes)
//   // Easy to forget envelope, forget dedup ID, use wrong type, etc.
//
// With Publisher:
//   pub.Publish(ctx, "goml.models.registered", event)
//   // Envelope, serialization, dedup all handled
//
// DEDUPLICATION:
//   JetStream deduplicates messages with the same Nats-Msg-Id within a
//   configurable window (default 2 minutes). This prevents duplicate events
//   when a publisher retries after a timeout (network blip: message was
//   delivered but ACK was lost → publisher retries → JetStream deduplicates).
// ============================================================================

// Publisher wraps JetStream publishing with automatic event envelope creation
// and deduplication.
type Publisher struct {
	js     jetstream.JetStream
	source string // service name for the envelope Source field
}

// NewPublisher creates a new Publisher.
// The source parameter identifies the publishing service (e.g., "auth", "registry").
func NewPublisher(js jetstream.JetStream, source string) *Publisher {
	return &Publisher{
		js:     js,
		source: source,
	}
}

// Publish serializes the payload, wraps it in an EventEnvelope, and publishes
// to the given NATS subject.
//
// The event type in the envelope is derived from the subject by stripping
// the "goml.{service}." prefix. For example:
//   subject "goml.models.registered" → type "model.registered"
//   subject "goml.auth.user.created" → type "user.created"
//
// This convention ensures event types are stable identifiers (not tied to
// NATS subject structure, which may change if we reorganize subjects).
func (p *Publisher) Publish(ctx context.Context, subject string, payload any) error {
	// Serialize the payload to JSON.
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("natsutil: marshal payload: %w", err)
	}

	// Derive event type from subject.
	// "goml.models.registered" → strip first two parts → "registered"
	// "goml.auth.user.created" → strip first two parts → "user.created"
	eventType := deriveEventType(subject)

	// Create envelope with standard metadata.
	envelope := NewEnvelope(eventType, p.source, data)

	// Serialize the complete envelope.
	envBytes, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("natsutil: marshal envelope: %w", err)
	}

	// Publish with deduplication ID.
	// The Nats-Msg-Id header tells JetStream to deduplicate: if a message
	// with the same ID was already stored (within the dedup window), this
	// publish is silently dropped. Prevents duplicate events on retries.
	_, err = p.js.Publish(ctx, subject, envBytes, jetstream.WithMsgID(envelope.ID))
	if err != nil {
		return fmt.Errorf("natsutil: publish to %s: %w", subject, err)
	}

	return nil
}

// deriveEventType extracts the event type from a NATS subject.
// "goml.models.registered" → "model.registered"
// "goml.auth.user.created" → "user.created"
func deriveEventType(subject string) string {
	// Strip "goml." prefix and the service segment
	parts := strings.SplitN(subject, ".", 3)
	if len(parts) >= 3 {
		return parts[2]
	}
	return subject
}
