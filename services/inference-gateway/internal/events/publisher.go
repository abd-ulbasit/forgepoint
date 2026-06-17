package events

import (
	"context"
	"fmt"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/services/inference-gateway/internal/domain"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ============================================================================
// PUBLISHER ADAPTER — implements domain.EventPublisher
// ============================================================================
//
// WHY THIS EXISTS: the Predict use-case (domain) decides WHAT business fact
// occurred (an inference completed / failed) and hands a pure domain payload to
// the EventPublisher port. It must not know about NATS, envelopes, subjects, or
// proto. This adapter owns all of that: it maps the domain payload → the
// canonical events.v1 wire message, hands it to natsutil.Publisher (which wraps
// it in the EventEnvelope, sets the dedupe Msg-Id, injects trace/correlation,
// and publishes), and targets the canonical subject.
//
// BEST-EFFORT, OFF THE RESPONSE PATH (a deliberate contract — see ports.go):
// the client already has its prediction; a publish failure must NOT fail the
// request. The port returns the error so the USE-CASE can log-and-continue; we
// surface a wrapped error here rather than swallowing it so the caller decides.
// We never block the hot path: natsutil.Publisher.Publish is a single async-ish
// JetStream publish.
//
// IDEMPOTENCY: natsutil stamps the envelope's UUID as the JetStream Nats-Msg-Id,
// so a retried publish (network blip: stored-but-ACK-lost) is de-duped inside the
// dedup window at the BROKER. Separately, InferenceCompleted carries request_id,
// the BUSINESS dedupe handle Billing keys on — so even across the dedup window a
// consumer never double-meters a request_id. Two layers: transport dedupe +
// business dedupe = exactly-once IN EFFECT over at-least-once delivery.
// ============================================================================

// natsPublisher is the minimal slice of *natsutil.Publisher this adapter needs.
// Depending on this interface (not the concrete type) lets the unit-ish tests
// inject a fake to assert subject+payload without a real broker, while the real
// wiring (main.go, next stage) passes a *natsutil.Publisher built over JetStream.
type natsPublisher interface {
	Publish(ctx context.Context, subject string, payload any) error
}

// Publisher is the events-layer adapter that satisfies domain.EventPublisher.
type Publisher struct {
	pub natsPublisher
}

// NewPublisher builds the adapter over a natsutil.Publisher. The natsutil
// publisher must have been constructed with source = Source so EventEnvelope.source
// is stamped "inference-gateway" (the producer identity for this service).
func NewPublisher(pub natsPublisher) *Publisher {
	return &Publisher{pub: pub}
}

// Compile-time proof we satisfy the port. If the domain interface changes, this
// line fails to build — the cheapest possible contract test.
var _ domain.EventPublisher = (*Publisher)(nil)

// PublishCompleted maps the domain success payload to events.v1.InferenceCompleted
// and publishes it to fp.inference.completed.
//
// WHAT TRAVELS (and what deliberately does NOT): IDs + the served version +
// latency + the billed api_key_id reference. NEVER raw input/output tensors and
// NEVER an end-user identity — PII discipline for an event that leaves the
// gateway and fans out to Billing, Experiment Tracker, and Model Monitor. The
// statistical prediction/feature summaries the canonical event CAN carry are
// computed by a later layer from the tensors; the use-case provides only the
// routing/billing facts it alone authoritatively knows, so those summary fields
// are left nil here (a consumer treats an absent summary as "no signal this call").
func (p *Publisher) PublishCompleted(ctx context.Context, ev domain.InferenceCompleted) error {
	payload := &eventsv1.InferenceCompleted{
		// request_id is SERVER-authoritative: Billing's business idempotency key
		// and Monitor's join key for delayed ground truth. Never client-supplied.
		RequestId: ev.RequestID,
		ModelName: ev.ModelName,
		// version that ACTUALLY served, post traffic-split — the A/B + per-version
		// metering key.
		Version:  ev.Version,
		ApiKeyId: ev.APIKeyID,
		IsCanary: ev.IsCanary,
		// Carry BOTH the typed Duration (precise) and the whole-ms convenience the
		// proto denormalizes for simple Grafana panels — derived from the same value
		// so they can never disagree.
		Latency:     durationpb.New(ev.Latency),
		LatencyMs:   ev.Latency.Milliseconds(),
		CompletedAt: timestamppb.New(ev.CompletedAt),
		// ModelId, TokenCount, PredictionSummary, FeatureSummary: the use-case does
		// not know these (registry id resolution + tensor summarization are a later
		// layer); left zero/nil intentionally rather than fabricated.
	}

	if err := p.pub.Publish(ctx, SubjectInferenceCompleted, payload); err != nil {
		return fmt.Errorf("events: publish InferenceCompleted (request_id=%s): %w", ev.RequestID, err)
	}
	return nil
}

// PublishFailed maps the domain failure payload to events.v1.InferenceFailed and
// publishes it to fp.inference.failed.
//
// WHY no summaries on failure: there may be no prediction to summarize, and we
// avoid echoing a possibly-malformed input. We carry the failure CLASSIFICATION
// (which resilience pattern fired) — that is exactly what drives Model Monitor's
// error-rate signal and Notification's routing (a capacity problem vs a backend-
// health problem call for different reactions). The domain's FailureReason is
// mapped 1:1 to the generated enum at this boundary (see failureReasonToProto).
func (p *Publisher) PublishFailed(ctx context.Context, ev domain.InferenceFailed) error {
	payload := &eventsv1.InferenceFailed{
		RequestId: ev.RequestID,
		ModelName: ev.ModelName,
		// version attempted; empty when it failed before version selection (NO_ROUTE).
		Version:  ev.Version,
		ApiKeyId: ev.APIKeyID,
		Reason:   failureReasonToProto(ev.Reason),
		FailedAt: timestamppb.New(ev.FailedAt),
		// Error (common.ErrorDetail) is omitted: the domain payload carries a
		// human-readable Message but the structured ErrorDetail (code/details) is a
		// handler-layer concern; Reason is the actionable classification consumers
		// route on. ModelId omitted for the same reason as the success path.
	}

	if err := p.pub.Publish(ctx, SubjectInferenceFailed, payload); err != nil {
		return fmt.Errorf("events: publish InferenceFailed (request_id=%s): %w", ev.RequestID, err)
	}
	return nil
}

// failureReasonToProto maps the domain's resilience-failure taxonomy to the
// generated events.v1 enum. The two sets are intentionally identical (the domain
// mirrors the enum in ports.go to stay free of gen/go imports); this is the
// single, total mapping the boundary owns.
//
// WHY a switch and not a numeric cast: even though the iota order currently
// matches the proto values 1:1, relying on that coincidence is fragile — a future
// reorder of either set would silently mis-map (a SILENT, hard-to-find bug that
// bills/alerts the wrong reason). An explicit switch makes the mapping a
// compile-time-visible contract and the default guards an unmapped value.
func failureReasonToProto(r domain.FailureReason) eventsv1.InferenceFailureReason {
	switch r {
	case domain.FailureReasonNoRoute:
		return eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_NO_ROUTE
	case domain.FailureReasonRateLimited:
		return eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_RATE_LIMITED
	case domain.FailureReasonBulkheadFull:
		return eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_BULKHEAD_FULL
	case domain.FailureReasonCircuitOpen:
		return eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_CIRCUIT_OPEN
	case domain.FailureReasonUpstreamError:
		return eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_UPSTREAM_ERROR
	case domain.FailureReasonTimeout:
		return eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_TIMEOUT
	case domain.FailureReasonInvalidInput:
		return eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_INVALID_INPUT
	case domain.FailureReasonQuotaExceeded:
		return eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_QUOTA_EXCEEDED
	default:
		// FailureReasonUnspecified and any future-unmapped value both land here.
		return eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_UNSPECIFIED
	}
}
