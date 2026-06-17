// Package events is the Notification service's NATS adapter layer — the OUTER
// ring that bridges the pure domain (internal/domain) to the async event bus.
//
// ============================================================================
// WHERE THIS SITS IN CLEAN ARCHITECTURE
// ============================================================================
//
//	internal/domain   NotificationService (ReactToEvent) + the ports          (pure)
//	     ▲                                                                      │ calls
//	     │ implements / drives                                                 ▼
//	internal/events   Publisher (delivery-health) · Reactor (fp.> consumer)  ← THIS PACKAGE
//	     │ uses
//	     ▼
//	pkg/natsutil      envelopes · idempotency (ProcessedStore) · DLQ · trace
//	gen/go/.../events the canonical wire payloads (events.v1.*)
//
// This package is the ONLY place in the notification service where the generated
// events.v1 proto types meet the domain types — the async equivalent of the
// handler's proto↔domain anti-corruption layer. The domain never imports gen/go
// or NATS; this adapter owns both and maps between them at the boundary.
//
// ============================================================================
// THE NOTIFICATION SERVICE'S EVENT ROLE (grounded in docs/design/event-contract.md)
// ============================================================================
//
//	PRODUCES (so the platform can observe DELIVERY HEALTH):
//	  fp.notifications.delivered  (NotificationDelivered) — a channel succeeded
//	  fp.notifications.failed     (NotificationFailed)    — a channel exhausted retries
//
//	CONSUMES (the CHOREOGRAPHY firehose — a single wildcard subscription):
//	  fp.>  → every platform event. Notable members per the contract:
//	          fp.models.drift.detected, fp.pipelines.failed, fp.pipelines.step.failed,
//	          fp.pipelines.compensation.triggered, fp.pipelines.completed,
//	          fp.billing.quota.exceeded, fp.billing.invoice.generated,
//	          fp.experiments.run.created, fp.experiments.run.finished,
//	          fp.models.archived, fp.inference.failed, fp.auth.user.created,
//	          fp.auth.apikey.rotated, …
//
// ============================================================================
// WHY A SINGLE fp.> SUBSCRIPTION, NOT ONE CONSUMER PER EVENT TYPE
// ============================================================================
//
// This is the defining property of CHOREOGRAPHY (vs ORCHESTRATION) and the single
// most interview-relevant decision in this package. Every other consumer on the
// platform (gateway, monitor, orchestrator) subscribes to the SPECIFIC few
// subjects it understands and decodes each payload into a typed events.v1 message.
// Notification does the opposite: it subscribes to the WHOLE firehose and treats
// every event OPAQUELY — it pattern-matches on EventEnvelope.type and forwards the
// payload bytes verbatim. It NEVER decodes what a `fp.pipelines.failed` MEANS.
//
//	consequence: a brand-new event type the team ships next quarter needs ZERO
//	code change here. The producer has no awareness of us; a user just writes a
//	preference/mute pattern whose wildcard matches the new Type, and the reactor
//	already routes it. That is choreography in one sentence — "react to facts on
//	the bus, with no central coordinator and no per-event-type coupling".
//
// The events.proto NOTIFICATION block says exactly this: "The Notification service
// is a CHOREOGRAPHY leaf — it consumes the firehose but treats inbound events
// OPAQUELY (it never decodes their payloads; it pattern-matches on
// EventEnvelope.type). So there are no consumer-side payload types for it." We
// honor that: the Reactor decodes ONLY the envelope, never env.Data.
//
// ============================================================================
// CANONICAL ENCODING — protojson, not encoding/json, for the PRODUCED payloads
// ============================================================================
//
// pkg/natsutil.Publisher serializes whatever payload it is handed with
// encoding/json. The forgepoint.events.v1 messages are PROTOBUF messages, and
// encoding/json does NOT understand the well-known types they embed
// (google.protobuf.Timestamp → {"seconds","nanos"} instead of an RFC-3339 string;
// enums as ints instead of their string names). That would make the bus a Go-only,
// non-canonical contract — exactly what the events.proto DESIGN block forbids
// (the event is a PUBLISHED, cross-language schema, like an Avro record in a schema
// registry). So we marshal each produced payload OURSELVES with protojson into a
// json.RawMessage and hand THAT to natsutil.Publisher. Because json.RawMessage is a
// verbatim json.Marshal passthrough, the canonical proto-JSON bytes reach
// EventEnvelope.data exactly as protojson produced them.
//
// The CONSUMER side has no symmetric decode here on purpose: a choreography leaf
// never decodes env.Data, so there is nothing to protojson.Unmarshal — the Reactor
// only reads envelope metadata.
package events

import (
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// ============================================================================
// CANONICAL SUBJECTS + SOURCE (single source of truth; grounded in event-contract.md)
// ============================================================================
//
// Defined as constants — not scattered string literals — so the publisher, the
// reactor, and the tests all reference ONE source of truth and a subject typo is a
// single compile-time-greppable edit rather than a silent producer/consumer split.
const (
	// Produced by this service (EventEnvelope.source = Source).
	SubjectNotificationDelivered = "fp.notifications.delivered"
	SubjectNotificationFailed    = "fp.notifications.failed"

	// Consumed by this service — the CHOREOGRAPHY firehose. One wildcard filter
	// over the whole platform namespace. The reactor matches on EventEnvelope.type
	// and the recipient's preference patterns; it does not enumerate subjects.
	SubjectAllEvents = "fp.>"

	// SubjectDLQ is where the reactor parks POISON messages (a message that fails
	// processing past the retry cap). It is DELIBERATELY rooted at "fp_dlq." (an
	// UNDERSCORE, a distinct token) — NOT "fp.dlq." — so it does NOT match the
	// SubjectAllEvents "fp.>" wildcard the reactor's own consumer filters on. That
	// one-character difference is the whole fix:
	//
	//   "fp.dlq.notification"  matches  "fp.>"   ← the DLQ re-feeds the reactor (BUG)
	//   "fp_dlq.notification"  does NOT match "fp.>" ← the reactor never re-consumes it
	//
	// THE BUG THIS PREVENTS (DLQ poison re-consumption / amplification):
	// The reactor's single consumer filters on fp.> and shares ONE stream (EVENTS)
	// with the subject it parks poison on. If the DLQ subject is "fp.dlq.notification"
	// it ALSO matches fp.> and lands in the SAME stream — so the moment natsutil
	// publishes the DLQ copy, JetStream hands it right back to the reactor's fp.>
	// consumer. The reactor re-decodes the SAME envelope (same Type, same content),
	// re-fails, and re-DLQs. A single forever-poison message therefore drives the
	// executor through ~2x its retry budget (one extra full retry-round), and once a
	// real DecisionExecutor fires actual webhook/Slack sends a poison-classified
	// notification is DELIVERED TWICE before being parked. It is only bounded today by
	// natsutil giving the DLQ copy Nats-Msg-Id = envelope.ID+"-dlq", so the SECOND DLQ
	// publish is dropped by JetStream's ~2-minute publish-dedup window — a time-window
	// bound, not a structural one.
	//
	// THE FIX (option a — the DLQ must never be re-consumed by the producer's own
	// firehose consumer): route the DLQ to a subject OUTSIDE fp.> and give it its OWN
	// stream (StreamDLQ). A poison message then leaves the firehose for good; ops
	// inspect/replay it from the dedicated DLQ stream out-of-band. This is how mature
	// buses model dead-letter (Kafka's separate dead-letter TOPIC, SQS's separate
	// dead-letter QUEUE) — the DLQ is a SINK, never an input to the same consumer.
	//
	// WHY a token boundary, not a deny-filter on the consumer: JetStream subject
	// matching is token-wise on ".", and a consumer FilterSubject is a single allow
	// pattern with no built-in deny — so the robust, self-documenting way to keep the
	// DLQ out of fp.> is to give it a root token ("fp_dlq") that simply isn't under fp.
	SubjectDLQ = "fp_dlq.notification"
)

// Source is the value stamped into EventEnvelope.source for every event THIS
// service publishes. The subject says WHICH RESOURCE the event is about; source
// says WHO produced it. Consumers (experiment-tracker delivery-health dashboards,
// on-call escalation) and audit trails key off this.
const Source = "notification"

// ============================================================================
// JETSTREAM STREAMS this service interacts with.
// ============================================================================
//
// In JetStream a SUBJECT is bound to a STREAM (the durable log). A subscriber
// creates a consumer ON a stream filtered to a subject, so the stream must exist
// before Subscribe is called. In production these streams are provisioned once at
// platform bootstrap (a stream-bootstrap job / the infra Helm chart), NOT by each
// service — multiple services share the streams, so letting any one service own
// their config invites conflicting definitions.
//
// The CHOREOGRAPHY twist: a JetStream consumer filters WITHIN a single stream, but
// Notification must see fp.> (every domain). The platform therefore provisions a
// dedicated firehose stream — EVENTS — whose Subjects include fp.> (or, in a
// per-domain-stream topology, Notification runs one consumer per stream). We name
// the firehose stream here; main.go (next stage) and the tests provision exactly
// this so the reactor's single fp.> consumer has a stream to bind to.
const (
	// StreamEvents is the firehose: every fp.> subject lands here so the
	// choreography reactor's single consumer can see the whole platform. It also
	// carries this service's own fp.notifications.> output. It DELIBERATELY does NOT
	// carry the DLQ — see StreamDLQ and SubjectDLQ for why the dead-letter must live
	// in a SEPARATE stream outside the fp.> firehose.
	StreamEvents = "EVENTS"

	// StreamDLQ is the DEDICATED dead-letter stream that owns SubjectDLQ
	// ("fp_dlq.notification" — outside fp.>). It MUST be a different stream from
	// EVENTS, because a JetStream subject is bound to exactly ONE stream and the
	// reactor's fp.> consumer lives on EVENTS: keeping the DLQ subject out of EVENTS
	// is precisely what stops a parked poison message from being re-delivered to the
	// reactor. Ops drain/replay from this stream out-of-band; the reactor never
	// consumes it. main.go provisions both streams at boot.
	StreamDLQ = "NOTIFICATION_DLQ"
)

// ============================================================================
// CANONICAL MARSHALING (produced payloads only — see the package doc's rationale)
// ============================================================================

// marshalCanonical encodes a proto event payload to CANONICAL proto-JSON and
// returns it as a json.RawMessage. natsutil.Publisher will json.Marshal this
// RawMessage, which is a verbatim passthrough — so the bytes that reach
// EventEnvelope.data are exactly what protojson produced (RFC-3339 timestamps,
// string enum names), making the event a cross-language contract.
//
// WHY a tiny wrapper and not protojson.Marshal inline: both produced events use it,
// and centralizing the marshal options (defaults: canonical names, omit unpopulated
// scalars) means the encoding can never drift between the two event types.
func marshalCanonical(msg proto.Message) (json.RawMessage, error) {
	b, err := protojson.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("events: marshal %T: %w", msg, err)
	}
	return json.RawMessage(b), nil
}
