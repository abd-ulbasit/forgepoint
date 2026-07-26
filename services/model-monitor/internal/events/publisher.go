package events

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/domain"
)

// ============================================================================
// PUBLISHER ADAPTER — implements domain.DriftPublisher
// ============================================================================
//
// The domain (applyPolicy in monitor_service_impl.go) decides WHAT business fact
// occurred — a window breached a threshold — and hands this adapter a
// transport-neutral domain.DriftEvent. The adapter owns everything wire-shaped:
// the events.v1 payload, the canonical subject (fp.models.drift.detected), the
// FLATTENING of the DriftReport + control fields into the fat-but-flat event, the
// domain-enum → event-enum conversion, and the protojson encoding. The domain
// never imports NATS or the events proto (Clean Architecture: the domain owns the
// DriftPublisher PORT, this adapter is the driven ADAPTER).
//
// EXACTLY-ONCE-IN-EFFECT: the domain calls PublishDrift ONLY on a TRUE report
// insert (Save returned inserted=true, idempotent on window_id), so the event
// fires exactly once per window even under stream redelivery. natsutil then stamps
// the envelope UUID as the JetStream Nats-Msg-Id, so a retried PUBLISH (stored-but-
// ACK-lost) is deduped at the broker too. The payload also carries report_id — the
// business dedupe handle the orchestrator keys its retrain saga on. Three layers:
// domain insert-once + broker msg-id dedupe + consumer report_id idempotency.

// natsPublisher is the minimal slice of *natsutil.Publisher this adapter needs.
// Depending on the interface (not the concrete type) lets the publisher unit test
// inject a fake to assert subject+payload without a broker, while production wiring
// (main.go, next stage) passes a *natsutil.Publisher built with source=SourceName.
type natsPublisher interface {
	Publish(ctx context.Context, subject string, payload any) error
}

// DriftEventPublisher is the events-layer adapter that satisfies
// domain.DriftPublisher. It maps domain.DriftEvent → events.v1.ModelDriftDetected
// and publishes it to fp.models.drift.detected.
type DriftEventPublisher struct {
	pub natsPublisher
}

// Compile-time proof we satisfy the domain port. If DriftPublisher changes, this
// line fails to build — the cheapest possible contract test.
var _ domain.DriftPublisher = (*DriftEventPublisher)(nil)

// NewPublisher builds the adapter over a natsutil.Publisher. The natsutil
// publisher MUST have been constructed with source = SourceName so
// EventEnvelope.source is stamped "model-monitor" (the producer identity).
func NewPublisher(pub natsPublisher) *DriftEventPublisher {
	return &DriftEventPublisher{pub: pub}
}

// PublishDrift maps the domain DriftEvent to events.v1.ModelDriftDetected and
// publishes it to fp.models.drift.detected.
//
// FIELD SOURCING (all server-authoritative, read off the scored report + monitor):
//   - model_name / model_version: the report's model + the EXACT served version it
//     scored (vital under canary split — a report is tied to one version).
//   - drift_type / severity: the headline (the dominant breach type + max severity)
//     duplicated from the report so notification can route/template an alert WITHOUT
//     deserializing the breakdown (cheap routing).
//   - report_id: the report's stable id — deep-link handle + the orchestrator's
//     retrain-saga idempotency key + experiment-tracker's record key.
//   - metrics: the per-feature/output/metric breakdown (the actionable detail) so a
//     reactor records WHICH thing moved without a callback into the monitor.
//   - window_* / sample_count: statistical context (PSI over 12 vs 12,000 samples)
//     and timeline placement.
//   - auto_retrain / retrain_pipeline_id: the closed-loop control fields, carried so
//     the orchestrator can run the retrain without a lookup. From the MONITOR (not
//     the report) — they are config, not a window result. retrain_pipeline_id is
//     OPAQUE and operator-configured (NOT derived from event data → no SSRF).
//   - retrain_context: a small, PII-free Struct of drift context (the top drifted
//     metric) passed as input to the retrain DAG. Struct (not a typed message)
//     because each retrain pipeline's expected inputs differ — a typed field would
//     couple this event to every pipeline's input schema.
//   - detected_at: the report's CreatedAt — the producer-clock instant the drift was
//     detected.
func (p *DriftEventPublisher) PublishDrift(ctx context.Context, e domain.DriftEvent) error {
	r := e.Report

	payload := &eventsv1.ModelDriftDetected{
		ModelName:    r.ModelName,
		ModelVersion: r.ModelVersion,
		DriftType:    driftTypeToProto(r.DriftType),
		Severity:     driftSeverityToProto(r.Severity),
		ReportId:     r.ID,
		Metrics:      driftMetricsToProto(r.Metrics),
		WindowStart:  timestampOrNil(r.WindowStart),
		WindowEnd:    timestampOrNil(r.WindowEnd),
		SampleCount:  int32(r.SampleCount),

		AutoRetrain:       e.AutoRetrain,
		RetrainPipelineId: e.RetrainPipelineID,
		RetrainContext:    retrainContext(r),

		DetectedAt: timestampOrNil(r.CreatedAt),
	}

	// Hand the canonical proto-JSON bytes to natsutil as a json.RawMessage so the
	// Publisher embeds them UNCHANGED in EventEnvelope.Data (json.Marshal of a
	// json.RawMessage is the identity), then wraps the envelope, stamps
	// source/trace/correlation, and publishes with the envelope id as the dedup key.
	data, err := marshalCanonical(payload)
	if err != nil {
		return fmt.Errorf("events: marshal ModelDriftDetected (report_id=%s): %w", r.ID, err)
	}
	if err := p.pub.Publish(ctx, SubjectModelDriftDetected, data); err != nil {
		return fmt.Errorf("events: publish ModelDriftDetected (report_id=%s): %w", r.ID, err)
	}
	return nil
}

// ----------------------------------------------------------------------------
// DOMAIN → EVENT ENUM CONVERSIONS (the decoupling tax, paid at this boundary)
// ----------------------------------------------------------------------------
//
// The events package re-declares these enums (decoupled from the monitor's domain
// enums — see events.proto's SHARED ENUMS block). This adapter is the ONE place
// that knows both the domain meaning and the wire meaning, so the domain stays
// proto-free. We use explicit switches, NOT numeric casts: even though the iota
// orders currently coincide, relying on that is fragile — a future reorder of
// either set would SILENTLY mis-map a drift verdict (alerting/retraining on the
// wrong type/severity). A switch makes the mapping a compile-visible contract and
// the default guards an unmapped value.

func driftTypeToProto(t domain.DriftType) eventsv1.DriftType {
	switch t {
	case domain.DriftTypeData:
		return eventsv1.DriftType_DRIFT_TYPE_DATA
	case domain.DriftTypePrediction:
		return eventsv1.DriftType_DRIFT_TYPE_PREDICTION
	case domain.DriftTypePerformance:
		return eventsv1.DriftType_DRIFT_TYPE_PERFORMANCE
	default:
		return eventsv1.DriftType_DRIFT_TYPE_UNSPECIFIED
	}
}

func driftMethodToProto(m domain.DriftMethod) eventsv1.DriftMethod {
	switch m {
	case domain.DriftMethodPSI:
		return eventsv1.DriftMethod_DRIFT_METHOD_PSI
	case domain.DriftMethodKL:
		return eventsv1.DriftMethod_DRIFT_METHOD_KL
	case domain.DriftMethodKS:
		return eventsv1.DriftMethod_DRIFT_METHOD_KS
	default:
		return eventsv1.DriftMethod_DRIFT_METHOD_UNSPECIFIED
	}
}

func driftSeverityToProto(s domain.DriftSeverity) eventsv1.DriftSeverity {
	switch s {
	case domain.DriftSeverityOK:
		return eventsv1.DriftSeverity_DRIFT_SEVERITY_OK
	case domain.DriftSeverityWarning:
		return eventsv1.DriftSeverity_DRIFT_SEVERITY_WARNING
	case domain.DriftSeverityCritical:
		return eventsv1.DriftSeverity_DRIFT_SEVERITY_CRITICAL
	default:
		return eventsv1.DriftSeverity_DRIFT_SEVERITY_UNSPECIFIED
	}
}

// driftMetricsToProto flattens the per-thing breakdown. Each DriftMetric becomes a
// self-describing wire metric (its method travels WITH its score because thresholds
// are method-specific — PSI 0.3 != KS 0.3).
func driftMetricsToProto(metrics []domain.DriftMetric) []*eventsv1.DriftMetric {
	if len(metrics) == 0 {
		return nil
	}
	out := make([]*eventsv1.DriftMetric, 0, len(metrics))
	for _, m := range metrics {
		out = append(out, &eventsv1.DriftMetric{
			Name:          m.Name,
			Method:        driftMethodToProto(m.Method),
			Score:         m.Score,
			BaselineValue: m.BaselineValue,
			CurrentValue:  m.CurrentValue,
			Severity:      driftSeverityToProto(m.Severity),
		})
	}
	return out
}

// retrainContext builds the small, PII-free Struct fed to the retrain pipeline. We
// surface the single most-drifted metric (name + score) plus the headline counts —
// enough for a retrain DAG to log/branch on, never raw features or labels. WHY a
// Struct and not a typed message: each pipeline's expected inputs differ; a typed
// field would couple this event to every pipeline's schema. structpb.NewStruct
// only fails on unsupported value kinds (we pass only string/float64/int), so on
// the impossible error we return nil rather than fail the publish — the event is
// still actionable without the context hint.
func retrainContext(r domain.DriftReport) *structpb.Struct {
	topName, topScore := topMetric(r.Metrics)
	s, err := structpb.NewStruct(map[string]any{
		"drift_type":    driftTypeLabel(r.DriftType),
		"severity":      driftSeverityLabel(r.Severity),
		"top_metric":    topName,
		"top_score":     topScore,
		"sample_count":  float64(r.SampleCount), // Struct numbers are float64
		"model_version": r.ModelVersion,
		"report_id":     r.ID,
	})
	if err != nil {
		return nil
	}
	return s
}

// topMetric returns the name + score of the worst-drifted metric (the one a
// retrain should focus on). Empty/0 when there are no metrics.
func topMetric(metrics []domain.DriftMetric) (string, float64) {
	var name string
	var score float64
	for _, m := range metrics {
		if m.Score > score {
			score, name = m.Score, m.Name
		}
	}
	return name, score
}

// driftTypeLabel / driftSeverityLabel give human-readable strings for the
// retrain_context Struct (a retrain DAG reads these for logging/branching). Kept
// local to the adapter — the wire ENUMS travel as their proto values on the typed
// fields; these labels are only for the free-form context.
func driftTypeLabel(t domain.DriftType) string {
	switch t {
	case domain.DriftTypeData:
		return "data"
	case domain.DriftTypePrediction:
		return "prediction"
	case domain.DriftTypePerformance:
		return "performance"
	default:
		return "unspecified"
	}
}

func driftSeverityLabel(s domain.DriftSeverity) string {
	switch s {
	case domain.DriftSeverityOK:
		return "ok"
	case domain.DriftSeverityWarning:
		return "warning"
	case domain.DriftSeverityCritical:
		return "critical"
	default:
		return "unspecified"
	}
}

// timestampOrNil maps a domain time.Time to a *timestamppb.Timestamp, returning
// nil for the zero time so the event OMITS the field (rather than emitting the
// proto epoch, a misleading "1970-01-01" on the wire). Applied identically
// wherever a domain timestamp becomes a proto one.
func timestampOrNil(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}
