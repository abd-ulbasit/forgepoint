package natsutil

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// ============================================================================
// NATS PUBLISHER
// ============================================================================
//
// WHY a Publisher struct instead of raw JetStream.Publish:
//   1. Automatically wraps events in EventEnvelope (consistent metadata)
//   2. Sets the Nats-Msg-Id header for deduplication (JetStream feature)
//   3. Derives event type from subject for the envelope
//   4. Serializes payload with the CANONICAL dialect (protojson for proto
//      messages, encoding/json for plain Go structs) so every publish/consume
//      pair is symmetric — see marshalPayload below.
//
// Without Publisher:
//   data, _ := json.Marshal(event)
//   envelope := EventEnvelope{ID: uuid.New(), Type: "model.registered", ...}
//   bytes, _ := json.Marshal(envelope)
//   js.Publish(ctx, "fp.models.registered", bytes)
//   // Easy to forget envelope, forget dedup ID, use wrong type, etc.
//
// With Publisher:
//   pub.Publish(ctx, "fp.models.registered", event)
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
// The event type in the envelope is derived from the subject by stripping the
// "fp.{service}." prefix (the first two segments). For example:
//
//	subject "fp.models.registered"  → type "registered"
//	subject "fp.auth.user.created"  → type "user.created"
//
// This convention ensures event types are stable identifiers (not tied to
// NATS subject structure, which may change if we reorganize subjects).
func (p *Publisher) Publish(ctx context.Context, subject string, payload any) error {
	// Serialize the payload with the CANONICAL dialect for its kind.
	data, err := marshalPayload(payload)
	if err != nil {
		return fmt.Errorf("natsutil: marshal payload: %w", err)
	}

	// Derive event type from subject (strip the "fp.{service}." prefix).
	eventType := deriveEventType(subject)

	// Create envelope with standard metadata, then enrich it from the request
	// context so observability survives the async hop:
	//   - CorrelationID continues the logical workflow across services.
	//   - TraceContext carries the W3C traceparent so the consumer's spans
	//     attach to this producer's trace in Tempo.
	envelope := NewEnvelope(eventType, p.source, data)
	envelope.CorrelationID = correlationIDForPublish(ctx)
	injectTraceContext(ctx, &envelope)

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

// marshalPayload serializes the event payload into EventEnvelope.Data using the
// CANONICAL JSON dialect for the payload's kind. This is the heart of the
// serialization-dialect fix.
//
// WHY protojson FOR proto.Message (and why plain encoding/json is WRONG here):
//
//	The platform's canonical domain events are forgepoint/events/v1 PROTO
//	messages. encoding/json and protojson are INCOMPATIBLE proto-JSON dialects:
//	  - Well-known types: google.protobuf.Timestamp renders as the Go struct
//	    {"seconds":..,"nanos":..} under encoding/json, but as an RFC-3339 STRING
//	    ("2026-06-18T12:00:00Z") under protojson. google.protobuf.Struct and
//	    Duration do not round-trip through encoding/json at all.
//	  - Field names: encoding/json uses the generated json tags; protojson uses
//	    lowerCamelCase (proto3 JSON mapping) by default.
//	  - Enums: encoding/json emits the integer; protojson emits the enum NAME.
//	A proto event published with encoding/json is DELIVERED but FAILS to decode in
//	any protojson consumer (and in any non-Go protojson runtime — there is now a
//	Python SDK). So we marshal proto.Message payloads with protojson, the single
//	canonical, cross-language encoding the event contract requires, and EVERY
//	consumer decodes the same proto event with protojson.Unmarshal — symmetric.
//
// WHY plain Go structs STAY on encoding/json:
//
//	Some payloads are genuinely NOT proto — e.g. the audit Record
//	(pkg/audit.Record), which is a hand-rolled Go struct. protojson cannot marshal
//	a non-proto value, and encoding/json is the natural, symmetric codec for a
//	plain struct (the audit consumer decodes it with encoding/json). The type
//	switch below routes each payload to the codec its consumer expects.
//
// PASSTHROUGH NOTE (no double-encoding): producers that PRE-MARSHAL their proto
// with protojson and hand us a json.RawMessage (pipeline-orchestrator,
// feature-store, experiment-tracker, model-monitor) are NOT proto.Message, so
// they fall to the json.Marshal branch — and json.Marshal of a json.RawMessage is
// the identity (it copies the bytes verbatim). Their canonical protojson bytes
// therefore land in EventEnvelope.Data unchanged, exactly as before.
func marshalPayload(payload any) (json.RawMessage, error) {
	// proto.Message → canonical proto-JSON (protojson). This is what makes the
	// proto event readable by every protojson consumer and every non-Go runtime.
	if pm, ok := payload.(proto.Message); ok {
		data, err := protojson.Marshal(pm)
		if err != nil {
			return nil, err
		}
		return json.RawMessage(data), nil
	}
	// Plain Go struct (or a pre-marshalled json.RawMessage) → encoding/json. For a
	// json.RawMessage this is the identity passthrough; for a struct (audit Record)
	// it is the symmetric codec the consumer uses.
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(data), nil
}

// deriveEventType extracts the event type from a NATS subject by stripping the
// first two segments ("fp.{service}.").
// "fp.models.registered" → "registered"
// "fp.auth.user.created" → "user.created"
func deriveEventType(subject string) string {
	// Strip "fp." prefix and the service segment
	parts := strings.SplitN(subject, ".", 3)
	if len(parts) >= 3 {
		return parts[2]
	}
	return subject
}
