// Package events is the NATS adapter layer for the Pipeline Orchestrator.
//
// ============================================================================
// HEXAGONAL OUTBOUND/INBOUND ADAPTERS — WHERE THE WIRE MEETS THE DOMAIN
// ============================================================================
//
// The domain (internal/domain) is framework-free: it emits framework-free
// StepEvent values through the EventPublisher PORT and never imports NATS or the
// generated proto event types. THIS package is the ADAPTER that implements that
// port (Publisher, below) and the inbound adapters (subscribers) that decode the
// canonical wire payloads and DRIVE the domain's primary port (PipelineService).
//
//	wire (NATS + forgepoint.events.v1 protojson)
//	   ▲ Publisher implements domain.EventPublisher  (outbound)
//	   │
//	domain  (StepEvent / PipelineService — pure Go)
//	   │
//	   ▼ Subscribers call PipelineService.TriggerExecution  (inbound)
//	wire (NATS ModelDriftDetected → auto-retrain saga)
//
// THE SINGLE DESIGN DECISION WORTH DEFENDING IN AN INTERVIEW — protojson, not
// encoding/json, for the event payloads:
//
//   pkg/natsutil.Publisher serializes whatever payload it is handed with
//   encoding/json. The forgepoint.events.v1 messages are PROTOBUF messages, and
//   encoding/json does NOT understand the well-known types they embed:
//     - google.protobuf.Timestamp would marshal as {"seconds":..,"nanos":..}
//       instead of the canonical RFC-3339 string,
//     - enums would marshal as integers instead of their string names,
//     - google.protobuf.Struct would not round-trip at all.
//   That would make the event bus a Go-only, non-canonical contract — exactly
//   what the events.proto DESIGN block forbids (the event is a PUBLISHED, cross-
//   language schema, like an Avro record in a schema registry).
//
//   So we marshal each payload OURSELVES with protojson into a json.RawMessage
//   and hand THAT to natsutil.Publisher. Because json.RawMessage implements
//   json.Marshaler as a verbatim passthrough, the canonical proto-JSON bytes
//   survive natsutil's inner json.Marshal untouched and land in
//   EventEnvelope.data exactly as protojson produced them. Consumers read
//   env.Data (a json.RawMessage) and protojson.Unmarshal it back into the same
//   generated type — one canonical contract, both ends of every pipe.
//
//   INTERVIEW framing: "Why not just let the JSON publisher marshal the proto?"
//   → Proto well-known types (Timestamp/Struct/enum) don't serialize correctly
//   under encoding/json; protojson is the canonical, cross-language encoding the
//   published event contract requires. We pre-encode and pass the raw bytes
//   through.
package events

import (
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// ============================================================================
// CANONICAL SUBJECTS (grounded in docs/design/event-contract.md)
// ============================================================================
//
// The subject is the RESOURCE domain, not the producer name (see the event
// contract's OWNERSHIP NOTE). This service PRODUCES all eight fp.pipelines.*
// subjects below and CONSUMES fp.models.drift.detected (a model-domain subject
// produced by model-monitor — the loop-closer). We name them as constants so the
// publisher, the subscribers, and the tests all reference one source of truth and
// a typo can't silently split a producer from its consumer.
const (
	// Produced by this service (EventEnvelope.source = "pipeline-orchestrator").
	SubjectPipelineStarted       = "fp.pipelines.started"
	SubjectStepCompleted         = "fp.pipelines.step.completed"
	SubjectStepFailed            = "fp.pipelines.step.failed"
	SubjectPipelineCompleted     = "fp.pipelines.completed"
	SubjectPipelineFailed        = "fp.pipelines.failed"
	SubjectCompensationTriggered = "fp.pipelines.compensation.triggered"
	SubjectModelDeployed         = "fp.pipelines.model.deployed"
	SubjectModelUndeployed       = "fp.pipelines.model.undeployed"

	// Consumed by this service — produced by model-monitor. Lives under
	// fp.models.* because it is ABOUT a model; source records the real producer.
	SubjectModelDriftDetected = "fp.models.drift.detected"
)

// Source is the EventEnvelope.source this adapter stamps on every event it
// publishes — the ACTUAL producing service, independent of the subject's
// resource domain. (For ModelDeployed/Undeployed the subject is fp.pipelines.*
// and the source is this service; for a consumed drift event the source is
// "model-monitor", which our subscriber does not set — it only reads.)
const Source = "pipeline-orchestrator"

// ServiceIdentity is the principal recorded as TriggeredBy/CreatedBy when THIS
// service (not a human) starts a run — specifically the auto-retrain saga the
// drift consumer triggers. It matches the Actor.Subject the domain documents for
// the model-monitor-initiated loop closer, so Notification can suppress paging on
// an automated retrain and audit can see "who" started it.
const ServiceIdentity = "model-monitor"

// marshalCanonical encodes a proto event payload to CANONICAL proto-JSON and
// returns it as a json.RawMessage. The natsutil.Publisher will json.Marshal this
// RawMessage, which is a verbatim passthrough — so the bytes that reach
// EventEnvelope.data are exactly what protojson produced (RFC-3339 timestamps,
// string enum names, real Structs), making the event a cross-language contract.
//
// WHY a tiny wrapper and not protojson.Marshal inline: every produced event uses
// it, and centralizing the marshal options (we keep defaults: emit canonical
// names, omit unpopulated scalars) means the encoding can never drift between
// event types.
func marshalCanonical(msg proto.Message) (json.RawMessage, error) {
	b, err := protojson.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("events: marshal %T: %w", msg, err)
	}
	return json.RawMessage(b), nil
}

// unmarshalCanonical decodes a canonical proto-JSON payload (an EventEnvelope's
// Data field) back into the given proto message. DiscardUnknown is intentional:
// a forward-compatible consumer must not reject an event a NEWER producer
// enriched with a field this binary doesn't know yet (additive evolution is the
// whole point of versioning the event contract).
func unmarshalCanonical(data json.RawMessage, msg proto.Message) error {
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := opts.Unmarshal(data, msg); err != nil {
		return fmt.Errorf("events: unmarshal %T: %w", msg, err)
	}
	return nil
}
