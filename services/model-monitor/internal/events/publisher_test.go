package events

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/domain"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
)

// ============================================================================
// PUBLISHER TESTS — REAL NATS JETSTREAM round-trip
// ============================================================================
//
// These verify the producer half of the adapter against a real NATS JetStream
// (testcontainers): a domain.DriftEvent published through the adapter lands on
// fp.models.drift.detected with the right ENVELOPE (source="model-monitor") and a
// PAYLOAD a real consumer (the orchestrator decodes with protojson) round-trips —
// including the enum conversions and the retrain_context Struct. We use a real
// broker, not a mock, because the behavior under test includes the canonical-JSON
// wire form a downstream protojson.Unmarshal must accept.

// sampleDriftReport builds a CRITICAL drift report with one drifted metric.
func sampleDriftReport() domain.DriftReport {
	now := time.Date(2026, 6, 18, 3, 14, 0, 0, time.UTC)
	return domain.DriftReport{
		ID:           "report-uuid-1",
		MonitorID:    "monitor-uuid-1",
		OwnerTeam:    "team-fraud",
		ModelName:    "fraud-detector",
		ModelVersion: "v7",
		DriftType:    domain.DriftTypeData,
		Severity:     domain.DriftSeverityCritical,
		Metrics: []domain.DriftMetric{
			{
				Name:          "income",
				Method:        domain.DriftMethodPSI,
				Score:         0.41,
				BaselineValue: 5.1,
				CurrentValue:  7.8,
				Severity:      domain.DriftSeverityCritical,
			},
			{
				Name:          "age",
				Method:        domain.DriftMethodPSI,
				Score:         0.03,
				BaselineValue: 40,
				CurrentValue:  41,
				Severity:      domain.DriftSeverityOK,
			},
		},
		WindowID:    "window-uuid-1",
		SampleCount: 1200,
		WindowStart: now.Add(-10 * time.Minute),
		WindowEnd:   now,
		CreatedAt:   now,
	}
}

// consumeOne subscribes to a subject and returns the first envelope received (or
// fails on timeout). It uses an ephemeral consumer (no durable) so each test is
// isolated.
func consumeOne(t *testing.T, js jetstream.JetStream, stream, subject string) natsutil.EventEnvelope {
	t.Helper()
	sub := natsutil.NewSubscriber(js)
	t.Cleanup(sub.Close)

	got := make(chan natsutil.EventEnvelope, 1)
	if err := sub.Subscribe(context.Background(), stream, subject, func(_ context.Context, env natsutil.EventEnvelope) error {
		select {
		case got <- env:
		default:
		}
		return nil
	}); err != nil {
		t.Fatalf("subscribe %s: %v", subject, err)
	}

	select {
	case env := <-got:
		return env
	case <-time.After(15 * time.Second):
		t.Fatalf("timed out waiting for an event on %s", subject)
		return natsutil.EventEnvelope{}
	}
}

func TestPublishDrift_LandsOnSubjectWithEnvelopeAndPayload(t *testing.T) {
	testutil.SkipIfNoDocker(t)

	url := testutil.StartNATS(t)
	conn, js, err := natsutil.Connect(url)
	if err != nil {
		t.Fatalf("connect NATS: %v", err)
	}
	t.Cleanup(conn.Close)

	ctx := context.Background()
	if err := EnsureStreams(ctx, js); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}

	// Build the adapter over a natsutil.Publisher bound to our source name (so the
	// envelope is stamped "model-monitor").
	pub := NewPublisher(natsutil.NewPublisher(js, SourceName))

	report := sampleDriftReport()
	ev := domain.DriftEvent{
		Report:            report,
		AutoRetrain:       true,
		RetrainPipelineID: "pipe-retrain-fraud",
		OwnerTeam:         "team-fraud",
	}
	if err := pub.PublishDrift(ctx, ev); err != nil {
		t.Fatalf("PublishDrift: %v", err)
	}

	env := consumeOne(t, js, StreamModels, SubjectModelDriftDetected)

	// ---- ENVELOPE assertions -------------------------------------------------
	if env.Source != SourceName {
		t.Errorf("envelope.Source = %q, want %q", env.Source, SourceName)
	}
	// deriveEventType strips "fp.models." → "drift.detected".
	if env.Type != "drift.detected" {
		t.Errorf("envelope.Type = %q, want %q", env.Type, "drift.detected")
	}
	if env.ID == "" {
		t.Error("envelope.ID should be a generated UUID")
	}
	if env.CorrelationID == "" {
		t.Error("envelope.CorrelationID should be populated (trace id or fresh uuid)")
	}

	// ---- PAYLOAD assertions (decode the canonical proto-JSON the way the real
	//      orchestrator consumer does: protojson) ------------------------------
	var p eventsv1.ModelDriftDetected
	if err := protojson.Unmarshal(env.Data, &p); err != nil {
		t.Fatalf("protojson.Unmarshal payload (the orchestrator's decode path): %v", err)
	}

	if p.GetModelName() != "fraud-detector" || p.GetModelVersion() != "v7" {
		t.Errorf("model = %q/%q, want fraud-detector/v7", p.GetModelName(), p.GetModelVersion())
	}
	if p.GetDriftType() != eventsv1.DriftType_DRIFT_TYPE_DATA {
		t.Errorf("drift_type = %v, want DATA", p.GetDriftType())
	}
	if p.GetSeverity() != eventsv1.DriftSeverity_DRIFT_SEVERITY_CRITICAL {
		t.Errorf("severity = %v, want CRITICAL", p.GetSeverity())
	}
	if p.GetReportId() != "report-uuid-1" {
		t.Errorf("report_id = %q, want report-uuid-1", p.GetReportId())
	}
	if !p.GetAutoRetrain() {
		t.Error("auto_retrain should be true")
	}
	if p.GetRetrainPipelineId() != "pipe-retrain-fraud" {
		t.Errorf("retrain_pipeline_id = %q, want pipe-retrain-fraud", p.GetRetrainPipelineId())
	}
	if p.GetSampleCount() != 1200 {
		t.Errorf("sample_count = %d, want 1200", p.GetSampleCount())
	}

	// Metrics breakdown (the actionable detail) + per-metric enum conversion.
	if len(p.GetMetrics()) != 2 {
		t.Fatalf("metrics len = %d, want 2", len(p.GetMetrics()))
	}
	m0 := p.GetMetrics()[0]
	if m0.GetName() != "income" || m0.GetMethod() != eventsv1.DriftMethod_DRIFT_METHOD_PSI {
		t.Errorf("metric[0] = %q/%v, want income/PSI", m0.GetName(), m0.GetMethod())
	}
	if m0.GetScore() != 0.41 || m0.GetBaselineValue() != 5.1 || m0.GetCurrentValue() != 7.8 {
		t.Errorf("metric[0] scores = %v/%v/%v, want 0.41/5.1/7.8", m0.GetScore(), m0.GetBaselineValue(), m0.GetCurrentValue())
	}
	if m0.GetSeverity() != eventsv1.DriftSeverity_DRIFT_SEVERITY_CRITICAL {
		t.Errorf("metric[0] severity = %v, want CRITICAL", m0.GetSeverity())
	}

	// Timestamps survived as canonical RFC-3339 strings (protojson form).
	if p.GetDetectedAt() == nil || !p.GetDetectedAt().AsTime().Equal(report.CreatedAt) {
		t.Errorf("detected_at = %v, want %v", p.GetDetectedAt().AsTime(), report.CreatedAt)
	}
	if p.GetWindowEnd() == nil || !p.GetWindowEnd().AsTime().Equal(report.WindowEnd) {
		t.Errorf("window_end = %v, want %v", p.GetWindowEnd().AsTime(), report.WindowEnd)
	}

	// retrain_context Struct carries the top drifted metric (PII-free hint).
	rc := p.GetRetrainContext()
	if rc == nil {
		t.Fatal("retrain_context should be populated")
	}
	if got := rc.GetFields()["top_metric"].GetStringValue(); got != "income" {
		t.Errorf("retrain_context.top_metric = %q, want income", got)
	}
	if got := rc.GetFields()["top_score"].GetNumberValue(); got != 0.41 {
		t.Errorf("retrain_context.top_score = %v, want 0.41", got)
	}
	if got := rc.GetFields()["severity"].GetStringValue(); got != "critical" {
		t.Errorf("retrain_context.severity = %q, want critical", got)
	}
}

// TestPublishDrift_DualFormatDecoderRoundTrips proves the adapter's OWN dual-format
// decoder reads back what its publisher writes — a sanity check that producing
// canonical proto-JSON and decoding via decodeCanonical (protojson-first) is a
// faithful round-trip for the WKT-heavy ModelDriftDetected (Struct + Timestamps),
// which a plain encoding/json could NOT round-trip.
func TestPublishDrift_DualFormatDecoderRoundTrips(t *testing.T) {
	report := sampleDriftReport()
	p := &eventsv1.ModelDriftDetected{
		ModelName:      report.ModelName,
		ModelVersion:   report.ModelVersion,
		DriftType:      eventsv1.DriftType_DRIFT_TYPE_DATA,
		Severity:       eventsv1.DriftSeverity_DRIFT_SEVERITY_CRITICAL,
		ReportId:       report.ID,
		Metrics:        driftMetricsToProto(report.Metrics),
		WindowEnd:      timestampOrNil(report.WindowEnd),
		DetectedAt:     timestampOrNil(report.CreatedAt),
		RetrainContext: retrainContext(report),
	}
	raw, err := marshalCanonical(p)
	if err != nil {
		t.Fatalf("marshalCanonical: %v", err)
	}

	var got eventsv1.ModelDriftDetected
	if err := decodeCanonical(json.RawMessage(raw), &got); err != nil {
		t.Fatalf("decodeCanonical round-trip: %v", err)
	}
	if got.GetReportId() != report.ID || got.GetModelName() != report.ModelName {
		t.Errorf("round-trip lost identity: %+v", &got)
	}
	if got.GetDetectedAt() == nil || !got.GetDetectedAt().AsTime().Equal(report.CreatedAt) {
		t.Errorf("round-trip lost detected_at")
	}
	if got.GetRetrainContext().GetFields()["top_metric"].GetStringValue() != "income" {
		t.Errorf("round-trip lost retrain_context")
	}
}
