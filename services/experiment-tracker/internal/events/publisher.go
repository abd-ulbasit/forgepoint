// Package events is the NATS ADAPTER ring of the Experiment Tracker's Clean
// Architecture onion. It is the OUTERMOST layer on the async axis: it knows
// about NATS (pkg/natsutil), the canonical wire schema (gen/.../events/v1), and
// the proto3-JSON marshalling rules — none of which the domain may ever import.
//
// ============================================================================
// WHAT LIVES HERE (two halves of the async nervous system)
// ============================================================================
//
//  1. PUBLISHER (this file) — implements the domain's EventPublisher PORT.
//     The domain hands it plain domain structs (RunCreatedEvent /
//     RunFinishedEvent); this adapter maps them to the canonical events.v1
//     proto payloads, wraps them in the standard EventEnvelope, and publishes
//     to the canonical subjects (fp.experiments.run.created / .finished).
//
//  2. SUBSCRIBERS (subscriber.go) — the INBOUND sink. Experiment Tracker is the
//     platform's WIDE event-driven consumer: it subscribes to a broad set of
//     lifecycle events from other services and records each as a LINEAGE entry
//     (a model was registered, a version went ready, an inference happened, a
//     pipeline step completed, …). The subscriber is an ADAPTER, not a domain
//     port (see the NOTE in domain/ports.go): it DECODES events and calls
//     ordinary domain operations. For the wide sink it writes through a small
//     adapter-owned LineageRecorder port.
//
// ============================================================================
// THE SERIALIZATION BRIDGE — why protojson + json.RawMessage
// ============================================================================
//
// The canonical event contract (proto forgepoint.events.v1) defines every
// payload as a proto MESSAGE. pkg/natsutil's Publisher, however, serializes the
// payload with encoding/json and stores it as the EventEnvelope.Data
// (json.RawMessage). Two facts make the bridge necessary:
//
//   - encoding/json does NOT understand proto well-known types. json.Marshal on
//     an events.v1.RunCreated would emit the timestamppb.Timestamp as its raw
//     {seconds, nanos} struct and would choke on the proto's unexported
//     internal state fields. The proto3 JSON mapping (RFC3339 timestamps, enum
//     names, camelCase) is implemented by google.golang.org/protobuf/encoding/protojson.
//
//   - So we marshal the proto payload OURSELVES with protojson to get canonical,
//     cross-language event JSON, then pass those bytes to natsutil.Publisher as a
//     json.RawMessage. json.RawMessage implements json.Marshaler by returning its
//     bytes verbatim, so the Publisher embeds our protojson bytes UNCHANGED as
//     EventEnvelope.Data — it never re-encodes them. The consumer side reverses
//     this with protojson.Unmarshal(envelope.Data, &msg).
//
// This keeps ONE wire contract (proto3 JSON) on both ends of every pipe while
// still reusing all of natsutil's envelope/dedup/trace/DLQ machinery. How proto
// events ride the JSON envelope is answered exactly here: protojson for the
// payload, RawMessage to splice it into the envelope without double-encoding.
package events

import (
	"context"
	"encoding/json"
	"fmt"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/services/experiment-tracker/internal/domain"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ServiceName is the EventEnvelope.source for every event this service emits.
// It MUST match the producer column in docs/design/event-contract.md
// ("experiment-tracker") so cross-service audit/trace correlation lines up.
const ServiceName = "experiment-tracker"

// Canonical PRODUCED subjects (the only two this service publishes). Grounded in
// the event-contract subject registry:
//
//	fp.experiments.run.created  → RunCreated
//	fp.experiments.run.finished → RunFinished
//
// They are constants (not derived) so a typo is a compile-time-adjacent failure
// (one place to read, one place the tests assert against) rather than a silent
// publish to a dead subject no consumer is bound to.
const (
	SubjectRunCreated  = "fp.experiments.run.created"
	SubjectRunFinished = "fp.experiments.run.finished"
)

// natsPublisher is the concrete EventPublisher backed by pkg/natsutil.Publisher.
// It is unexported: callers receive it as the domain.EventPublisher interface
// (constructor returns the interface), so the domain stays unaware that NATS —
// or proto, or JSON — is the transport.
type natsPublisher struct {
	pub *natsutil.Publisher
}

// NewPublisher builds the EventPublisher adapter over an already-constructed
// natsutil.Publisher. The natsutil.Publisher carries the source ("experiment-
// tracker") and the JetStream handle; main.go wires it once at startup. Returning
// the domain interface (not *natsPublisher) is the dependency-inversion seam:
// the service holds an EventPublisher and never sees this struct.
func NewPublisher(pub *natsutil.Publisher) domain.EventPublisher {
	return &natsPublisher{pub: pub}
}

// PublishRunCreated emits fp.experiments.run.created.
//
// Maps the domain RunCreatedEvent → events.v1.RunCreated, marshals it with
// protojson, and publishes via natsutil (which wraps it in the EventEnvelope,
// stamps the dedup Msg-Id, and propagates correlation/trace from ctx). The
// domain treats this as best-effort (a publish failure does not roll back the
// run), but the adapter still RETURNS the error so the service can log it — the
// port contract is "tell me if it failed", not "swallow it".
func (p *natsPublisher) PublishRunCreated(ctx context.Context, ev domain.RunCreatedEvent) error {
	payload := &eventsv1.RunCreated{
		RunId:          ev.RunID,
		ExperimentId:   ev.ExperimentID,
		ModelVersionId: ev.ModelVersionID,
		DisplayName:    ev.DisplayName,
		StartedAt:      timestamppb.New(ev.StartedAt),
	}
	return p.publishProto(ctx, SubjectRunCreated, payload)
}

// PublishRunFinished emits fp.experiments.run.finished — the FAT event carrying
// final_metrics so a leaderboard/notification consumer ranks or alerts WITHOUT a
// GetRun callback (terminal-run metrics are final, so there is no staleness).
//
// The domain RunStatus (a typed string: "FINISHED"/"FAILED"/"KILLED") is mapped
// to the events.v1.RunStatus enum at THIS boundary — exactly the "service maps
// its own enum to/from the event enum at the publish/consume boundary" rule the
// contract mandates (the event schema must not import the domain's enum).
func (p *natsPublisher) PublishRunFinished(ctx context.Context, ev domain.RunFinishedEvent) error {
	payload := &eventsv1.RunFinished{
		RunId:          ev.RunID,
		ExperimentId:   ev.ExperimentID,
		ModelVersionId: ev.ModelVersionID,
		Status:         runStatusToProto(ev.Status),
		FinalMetrics:   metricPointsToProto(ev.FinalMetrics),
		EndedAt:        timestamppb.New(ev.EndedAt),
	}
	return p.publishProto(ctx, SubjectRunFinished, payload)
}

// publishProto is the shared marshal-and-publish path. It is the single place
// the protojson↔RawMessage bridge lives, so both produced events (and any future
// ones) serialize identically.
func (p *natsPublisher) publishProto(ctx context.Context, subject string, msg proto.Message) error {
	// protojson gives canonical proto3 JSON (RFC3339 timestamps, enum NAMES,
	// camelCase fields) — the cross-language wire form. We do NOT let natsutil's
	// encoding/json touch the proto struct (it would mangle well-known types).
	data, err := protojson.Marshal(msg)
	if err != nil {
		return fmt.Errorf("events: marshal %s payload: %w", subject, err)
	}
	// json.RawMessage round-trips through encoding/json verbatim, so natsutil's
	// internal json.Marshal(payload) embeds OUR protojson bytes unchanged as
	// EventEnvelope.Data. This is the splice that lets proto payloads ride the
	// JSON envelope without double-encoding.
	if err := p.pub.Publish(ctx, subject, json.RawMessage(data)); err != nil {
		return fmt.Errorf("events: publish %s: %w", subject, err)
	}
	return nil
}

// ----------------------------------------------------------------------------
// Domain → event-proto mappers (the anti-corruption boundary, publish side)
// ----------------------------------------------------------------------------

// runStatusToProto maps the domain's RunStatus (a typed string the domain and DB
// use) to the events.v1.RunStatus enum. WHY the explicit switch rather than a
// map: an unrecognized/zero status falls through to RUN_STATUS_UNSPECIFIED,
// which is the proto's required zero value — a defined, safe default rather than
// a panic. Only terminal statuses are ever published on RunFinished, but the
// mapper handles all values so it can't silently drop one if the caller changes.
func runStatusToProto(s domain.RunStatus) eventsv1.RunStatus {
	switch s {
	case domain.RunStatusRunning:
		return eventsv1.RunStatus_RUN_STATUS_RUNNING
	case domain.RunStatusFinished:
		return eventsv1.RunStatus_RUN_STATUS_FINISHED
	case domain.RunStatusFailed:
		return eventsv1.RunStatus_RUN_STATUS_FAILED
	case domain.RunStatusKilled:
		return eventsv1.RunStatus_RUN_STATUS_KILLED
	default:
		return eventsv1.RunStatus_RUN_STATUS_UNSPECIFIED
	}
}

// metricPointsToProto converts the domain's denormalized final metrics into the
// events.v1.MetricPoint repeated field carried on the fat RunFinished event. The
// domain MetricPoint.Timestamp is server-authoritative (set on ingest), so it is
// carried as-is — a consumer plotting the headline number trusts the server clock.
func metricPointsToProto(points []domain.MetricPoint) []*eventsv1.MetricPoint {
	if len(points) == 0 {
		return nil
	}
	out := make([]*eventsv1.MetricPoint, len(points))
	for i, p := range points {
		out[i] = &eventsv1.MetricPoint{
			Key:       p.Key,
			Value:     p.Value,
			Step:      p.Step,
			Timestamp: timestamppb.New(p.Timestamp),
		}
	}
	return out
}
