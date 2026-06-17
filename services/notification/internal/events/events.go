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
//	CONSUMES (the CHOREOGRAPHY set — one durable consumer PER subject, each bound
//	to the EXISTING per-domain stream that owns the subject; see ConsumedSubjects):
//	  fp.models.drift.detected, fp.models.archived            (stream MODELS)
//	  fp.pipelines.failed, fp.pipelines.step.failed,
//	  fp.pipelines.compensation.triggered, fp.pipelines.completed (stream PIPELINES)
//	  fp.inference.failed                                     (stream INFERENCE)
//	  fp.billing.quota.exceeded, fp.billing.invoice.generated (stream BILLING)
//	  fp.experiments.run.created, fp.experiments.run.finished (stream EXPERIMENTS)
//
// ============================================================================
// WHY A CURATED SUBJECT SET BOUND TO PER-DOMAIN STREAMS (NOT ONE fp.> CONSUMER)
// ============================================================================
//
// Notification is still a CHOREOGRAPHY leaf — it treats every inbound event
// OPAQUELY (it pattern-matches on EventEnvelope.type via the recipient router and
// forwards the payload bytes verbatim; it NEVER decodes what a `fp.pipelines.failed`
// MEANS). What changed is the TOPOLOGY of HOW it subscribes, forced by a hard
// JetStream constraint we hit in the k3s deploy:
//
//	A JetStream STREAM owns a SUBJECT TREE, and a stream's subjects MUST NOT
//	OVERLAP another stream's. The platform already provisions NARROW per-domain
//	streams — MODELS=fp.models.>, PIPELINES=fp.pipelines.>, BILLING=fp.billing.>,
//	INFERENCE=fp.inference.>, EXPERIMENTS=fp.experiments.>, … (each owning service
//	creates its own at boot). A single firehose stream over "fp.>" would OVERLAP
//	all of them, so CreateStream/CreateOrUpdateStream fails with JetStream err
//	10065 ("subjects overlap with an existing stream") and notification CrashLoops.
//
// And a JetStream CONSUMER binds to exactly ONE stream — so there is no single
// consumer that can span fp.models.>, fp.pipelines.>, fp.billing.>, … living in
// DIFFERENT streams. The "one fp.> consumer on a self-owned EVENTS stream" design
// is therefore structurally impossible alongside the per-domain-stream topology
// the rest of the platform uses.
//
// THE FIX (the mature, idiomatic shape — what model-monitor already does): run ONE
// durable consumer PER consumed subject, and BIND each to the EXISTING stream that
// owns that subject (resolved at boot via js.StreamNameBySubject — see
// subscriber.go's Start). Notification CREATES NO domain stream of its own; it is a
// pure CONSUMER that attaches to streams the producers provision. The only stream
// it owns is its private DLQ (StreamDLQ on fp_dlq.*, outside every fp.<domain>.>
// tree, so it overlaps nothing).
//
// WHAT WE GIVE UP vs a true fp.> firehose: a brand-new event DOMAIN (a new
// fp.<newdomain>.> tree) is no longer auto-consumed — it must be added to
// ConsumedSubjects. That is the deliberate, contract-anchored tradeoff: the
// subjects notification reacts to are exactly the ones the event-contract lists
// with notification as a consumer, so they are an enumerable, reviewable set, not
// an open firehose. Within an already-consumed domain a new event subject just
// needs its narrow subject added here (still no payload decode — choreography
// intact). The reactor body is UNCHANGED: it still decodes ONLY the envelope.
//
// INTERVIEW FRAMING: "How does a choreography consumer span many domains when each
// domain is a separate JetStream stream and consumers bind to one stream?" Answer:
// you DON'T use one wildcard consumer (that needs one stream whose subjects overlap
// every domain stream — forbidden). You run one durable consumer per subject, each
// bound to its owning stream, and resolve the owning stream by subject at boot. The
// opacity (no payload decode) is orthogonal to the subscription topology.
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

	// ---- CONSUMED subjects (the curated choreography set) -------------------
	//
	// These are the EXACT subjects the canonical event-contract (docs/design/
	// event-contract.md) lists with `notification` as a consumer. Each is a NARROW
	// subject (no wildcard) so a durable consumer for it binds to the one EXISTING
	// per-domain stream that owns its tree (MODELS / PIPELINES / INFERENCE /
	// BILLING / EXPERIMENTS) — never a self-created fp.> stream that would overlap.
	// The reactor still treats every payload OPAQUELY; these names only decide WHICH
	// streams' events reach the (opaque) handler.

	// fp.models.* — owned by stream MODELS (producer: registry; drift by model-monitor).
	SubjectModelDriftDetected = "fp.models.drift.detected"
	SubjectModelArchived      = "fp.models.archived"

	// fp.pipelines.* — owned by stream PIPELINES (producer: pipeline-orchestrator).
	SubjectPipelineFailed        = "fp.pipelines.failed"
	SubjectPipelineStepFailed    = "fp.pipelines.step.failed"
	SubjectPipelineCompensation  = "fp.pipelines.compensation.triggered"
	SubjectPipelineCompleted     = "fp.pipelines.completed"

	// fp.inference.* — owned by stream INFERENCE (producer: inference-gateway).
	SubjectInferenceFailed = "fp.inference.failed"

	// fp.billing.* — owned by stream BILLING (producer: billing).
	SubjectBillingQuotaExceeded     = "fp.billing.quota.exceeded"
	SubjectBillingInvoiceGenerated  = "fp.billing.invoice.generated"

	// fp.experiments.* — owned by stream EXPERIMENTS (producer: experiment-tracker).
	SubjectExperimentRunCreated  = "fp.experiments.run.created"
	SubjectExperimentRunFinished = "fp.experiments.run.finished"

	// SubjectDLQ is where the reactor parks POISON messages (a message that fails
	// processing past the retry cap). It is DELIBERATELY rooted at "fp_dlq." (an
	// UNDERSCORE, a distinct token) — NOT "fp.dlq." and NOT under any consumed
	// fp.<domain>.> tree — so it lands ONLY in its own dedicated StreamDLQ and is
	// never re-consumed by one of the reactor's per-subject consumers:
	//
	//   "fp.dlq.notification"  is under fp.>  → could land in a domain stream (BUG)
	//   "fp_dlq.notification"  is under NO fp.<domain> tree → its own stream only
	//
	// THE BUG THIS PREVENTS (DLQ poison re-consumption / amplification):
	// If the DLQ subject lived under a tree a reactor consumer reads (e.g.
	// "fp.pipelines.dlq.notification" under fp.pipelines.>), the moment natsutil
	// publishes the DLQ copy JetStream would hand it back to that domain consumer.
	// The reactor would re-decode the SAME envelope, re-fail, and re-DLQ — driving the
	// executor through extra retry rounds and, once a real DecisionExecutor fires
	// actual webhook/Slack sends, DELIVERING a poison-classified notification twice
	// before parking it. Rooting the DLQ at its own "fp_dlq." token in its own stream
	// makes the dead-letter a pure SINK, structurally un-re-consumable.
	//
	// This is how mature buses model dead-letter (Kafka's separate dead-letter TOPIC,
	// SQS's separate dead-letter QUEUE) — the DLQ is a SINK, never an input to the
	// same consumer. WHY a token boundary, not a deny-filter on the consumer:
	// JetStream subject matching is token-wise on "." and a consumer FilterSubject is
	// a single allow pattern with no built-in deny, so the robust, self-documenting
	// way to keep the DLQ out of every domain tree is a root token ("fp_dlq") that
	// simply isn't under fp.<domain>.
	SubjectDLQ = "fp_dlq.notification"
)

// ConsumedSubjects is the curated, contract-anchored set of subjects the reactor
// reacts to — one durable consumer is created PER entry (see subscriber.go Start).
// It is the single source of truth for the subscription set: subscriber.go iterates
// it and the tests assert against it, so adding a subject is a one-line edit here.
//
// Ordering is not significant (each subject gets an independent durable consumer);
// duplicates would create duplicate consumers, so keep it a set.
var ConsumedSubjects = []string{
	SubjectModelDriftDetected,
	SubjectModelArchived,
	SubjectPipelineFailed,
	SubjectPipelineStepFailed,
	SubjectPipelineCompensation,
	SubjectPipelineCompleted,
	SubjectInferenceFailed,
	SubjectBillingQuotaExceeded,
	SubjectBillingInvoiceGenerated,
	SubjectExperimentRunCreated,
	SubjectExperimentRunFinished,
}

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
// creates a CONSUMER on a stream filtered to a subject, so the stream must exist
// before Subscribe is called. The per-domain streams notification consumes from
// (MODELS, PIPELINES, INFERENCE, BILLING, EXPERIMENTS) are OWNED and provisioned by
// the producing services (registry, pipeline-orchestrator, …) at their boot — and,
// at platform scale, by a stream-bootstrap job / the infra Helm chart. Notification
// does NOT create any of them; it RESOLVES the owning stream for each consumed
// subject at boot via js.StreamNameBySubject (subscriber.go Start) and binds a
// durable consumer there. Creating an fp.> stream of its own would OVERLAP every
// per-domain stream (JetStream err 10065) — the exact CrashLoop this design fixes.
//
// The ONE stream notification owns is its private DLQ: a SINK rooted at "fp_dlq.*"
// (outside every fp.<domain>.> tree, so it overlaps nothing). Ops drain/replay from
// it out-of-band; no reactor consumer reads it.
const (
	// StreamDLQ is the DEDICATED dead-letter stream that owns SubjectDLQ
	// ("fp_dlq.notification" — outside every fp.<domain>.> tree). Because a JetStream
	// subject is bound to exactly ONE stream, keeping the DLQ subject in its own
	// stream (and out of the domain streams the reactor's consumers read) is precisely
	// what stops a parked poison message from being re-delivered to a reactor consumer.
	// notification provisions ONLY this stream at boot (main.go); the domain streams it
	// consumes from are provisioned by their producers.
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
