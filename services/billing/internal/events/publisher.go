package events

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
)

// ============================================================================
// RELAY PUBLISHER — publish with a CALLER-SUPPLIED envelope id (the outbox row id)
// ============================================================================
//
// WHY THIS EXISTS (and isn't just natsutil.Publisher): natsutil.Publisher mints a
// FRESH random UUID for every Publish call (NewEnvelope). That is exactly right for
// a normal producer — but WRONG for an outbox relay. The relay may publish the SAME
// outbox row more than once (a crash between publish and mark; see relay.go's
// at-least-once contract). For the consumer's idempotency to collapse those
// republishes into a single effect, every republish of a row MUST carry the SAME
// EventEnvelope.id. The stable id we have is the OUTBOX ROW ID — so the relay needs
// to STAMP that id onto the envelope, not let a new one be generated each time.
//
// This adapter builds the envelope itself (reusing natsutil's EventEnvelope type +
// trace/correlation helpers so the wire shape is byte-identical to what every other
// producer emits) but sets ID = the supplied outbox row id, and also uses that id
// as the JetStream Nats-Msg-Id (broker-side dedup within the publish window). The
// result: a republish is deduped at BOTH layers (broker Msg-Id + consumer
// ProcessedStore) and the consumer's business-key dedupe is the final backstop.
//
// We keep this in the billing events package (rather than extending pkg/natsutil)
// because "publish with my own id" is an outbox-specific need; natsutil's
// general-producer API intentionally hides id generation. The envelope JSON shape
// is the shared contract (natsutil.EventEnvelope), so consumers using
// natsutil.Subscriber decode these identically to any other event.
// ============================================================================

// RelayPublisher publishes events to JetStream with a caller-supplied envelope id.
// It satisfies relay.go's idPublisher (PublishWithID) AND natsPublisher (Publish),
// so NewOutboxRelay always gets the strong, republish-idempotent path.
type RelayPublisher struct {
	js     jetstream.JetStream
	source string // stamped into EventEnvelope.source (== Source, "billing")
}

// NewRelayPublisher builds the publisher over a JetStream context. source should be
// events.Source so every billing event carries source="billing".
func NewRelayPublisher(js jetstream.JetStream, source string) *RelayPublisher {
	return &RelayPublisher{js: js, source: source}
}

// Compile-time proofs that the relay can use this publisher on both paths.
var (
	_ idPublisher   = (*RelayPublisher)(nil)
	_ natsPublisher = (*RelayPublisher)(nil)
)

// PublishWithID serializes payload, wraps it in an EventEnvelope whose ID is the
// supplied envelopeID (the outbox row id), enriches it with trace + correlation
// context (so the async hop stays observable), and publishes to subject using
// envelopeID ALSO as the Nats-Msg-Id for broker-side dedup.
//
// SUBJECT == EVENT TYPE for billing: the outbox row's event_type IS the canonical
// subject ("fp.billing.usage.recorded", …), so the relay passes it as both the NATS
// subject and (via deriveEventType inside the envelope) the envelope Type — exactly
// the convention natsutil.Publisher uses.
func (p *RelayPublisher) PublishWithID(ctx context.Context, subject, envelopeID string, payload any) error {
	// Serialize the payload with the SAME canonical dialect natsutil.Publisher uses:
	// protojson for proto messages, encoding/json for plain Go structs. The relay's
	// decodeOutboxPayload reconstructs a forgepoint/events/v1 PROTO (UsageRecorded /
	// QuotaExceeded / InvoiceGenerated) from the stored row, and those carry
	// google.protobuf.Timestamp fields (occurred_at, …). Under encoding/json a
	// Timestamp renders as the Go-only {seconds,nanos} shape that NO protojson
	// consumer can read — so this MUST be protojson to stay symmetric with the
	// billing consumers (experiment-tracker, notification) that protojson.Unmarshal
	// these events. This is the outbox half of the serialization-dialect fix: the
	// relay builds its own envelope (to stamp the outbox row id), so the protojson
	// switch lives here too, not only in natsutil.Publisher.
	data, err := marshalRelayPayload(payload)
	if err != nil {
		return fmt.Errorf("events: marshal payload for %s: %w", subject, err)
	}

	// Build the standard envelope, then OVERRIDE the id with the outbox row id so
	// republishes are idempotent. We reuse natsutil.NewEnvelope for the Type/Source/
	// Timestamp/Data fields (identical wire shape to every other producer) and then
	// replace the generated UUID — the one field that must be stable across
	// republishes — with envelopeID.
	env := natsutil.NewEnvelope(eventTypeFromSubject(subject), p.source, data)
	env.ID = envelopeID
	// Carry correlation + W3C trace context across the async boundary, mirroring
	// natsutil.Publisher's own enrichment (its internal helpers are unexported, so
	// we replicate the SAME logic here against the public natsutil + OTel APIs — the
	// resulting envelope is byte-identical in shape to a natsutil.Publisher one).
	env.CorrelationID = correlationIDForPublish(ctx)
	injectTraceContext(ctx, &env)

	envBytes, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("events: marshal envelope for %s: %w", subject, err)
	}

	// Nats-Msg-Id = the envelope id (outbox row id): a republish within the dedup
	// window is dropped at the broker. Beyond the window, the consumer's
	// ProcessedStore (also keyed on this id) is the backstop.
	if _, err := p.js.Publish(ctx, subject, envBytes, jetstream.WithMsgID(envelopeID)); err != nil {
		return fmt.Errorf("events: publish to %s (id=%s): %w", subject, envelopeID, err)
	}
	return nil
}

// Publish satisfies natsPublisher by delegating to the standard natsutil publisher
// semantics is NOT what we want here — the relay always wants the row id. So Publish
// simply errors to make misuse loud: the relay uses PublishWithID (it type-asserts
// idPublisher first), and nothing else should call this. Implementing it keeps the
// type usable wherever a bare natsPublisher is expected without silently dropping
// the id guarantee.
//
// In practice the relay's publishRow type-asserts idPublisher and calls
// PublishWithID, so this method is never hit in production; it exists only so
// *RelayPublisher satisfies the narrower interface for wiring flexibility.
func (p *RelayPublisher) Publish(_ context.Context, subject string, _ any) error {
	return fmt.Errorf("events: RelayPublisher requires an explicit envelope id; use PublishWithID (subject=%s)", subject)
}

// marshalRelayPayload serializes the relay payload into EventEnvelope.Data using the
// canonical dialect for its kind — IDENTICAL to natsutil.Publisher's marshalPayload:
// protojson for proto.Message (the events.v1 payloads the relay republishes carry
// well-known Timestamps that only protojson round-trips across languages),
// encoding/json for any plain struct. Kept here because the relay builds its own
// envelope (to control the id) and so cannot route through natsutil.Publisher.
func marshalRelayPayload(payload any) ([]byte, error) {
	if pm, ok := payload.(proto.Message); ok {
		return protojson.Marshal(pm)
	}
	return json.Marshal(payload)
}

// eventTypeFromSubject derives the envelope Type from the subject the SAME way
// natsutil does (strip the "fp.<domain>." prefix): "fp.billing.usage.recorded" →
// "usage.recorded". Re-implemented here (natsutil's deriveEventType is unexported)
// so the envelope Type matches what a natsutil.Publisher would have produced for
// the same subject — keeping billing's events indistinguishable on the wire.
func eventTypeFromSubject(subject string) string {
	// Strip the first two dot segments ("fp.<domain>.").
	first := indexByte(subject, '.')
	if first < 0 {
		return subject
	}
	second := indexByte(subject[first+1:], '.')
	if second < 0 {
		return subject
	}
	return subject[first+1+second+1:]
}

// indexByte is strings.IndexByte inlined to avoid importing strings for one call;
// returns the index of the first c in s, or -1.
func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// ----------------------------------------------------------------------------
// OBSERVABILITY HELPERS — replicate natsutil.Publisher's (unexported) enrichment
// ----------------------------------------------------------------------------
//
// natsutil keeps correlationIDForPublish / injectTraceContext unexported, so a
// caller building its OWN envelope (as the relay must, to control the id) can't
// reuse them. We replicate the identical logic over natsutil's PUBLIC correlation
// helpers and the global OTel propagator (the same propagator natsutil uses). The
// envelope a billing event carries is therefore observability-equivalent to one
// from any natsutil.Publisher.

// correlationIDForPublish picks the correlation id to stamp on an outgoing event,
// matching natsutil's priority: an explicit id already on ctx (continue the chain)
// → the active trace id (events stay correlatable to the trace) → a fresh UUID.
//
// RELAY NUANCE: the relay's Run loop ctx usually carries no correlation/trace (the
// originating request finished long before the relay publishes). The fresh-UUID
// fallback is correct then — the event still gets a stable correlation id. When the
// relay is driven within a traced context (tests, a one-shot drain), the trace id
// flows through.
func correlationIDForPublish(ctx context.Context) string {
	if id, ok := natsutil.CorrelationIDFromContext(ctx); ok {
		return id
	}
	if sc := trace.SpanContextFromContext(ctx); sc.HasTraceID() {
		return sc.TraceID().String()
	}
	return uuid.New().String()
}

// injectTraceContext writes the active span context into the envelope using the
// globally-configured OTel propagator (set in observability.Setup) — identical to
// natsutil's internal injection, so the consumer's extractTraceContext links its
// spans back to whatever produced the event.
func injectTraceContext(ctx context.Context, env *natsutil.EventEnvelope) {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if len(carrier) > 0 {
		env.TraceContext = carrier
	}
}
