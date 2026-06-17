// events_test.go — REAL pub/sub behavior tests for the Experiment Tracker's NATS
// adapters, run against a real NATS JetStream server (testcontainers).
//
// ============================================================================
// WHY REAL NATS (not a mock bus)
// ============================================================================
//
// The whole point of these adapters is the wire behavior: a produced event lands
// on the RIGHT subject with the RIGHT envelope+payload; a consumer DECODES and
// dispatches correctly; a DUPLICATE redelivery is idempotent; a POISON message
// goes to the DLQ. A mocked NATS would let all of that pass while the real
// JetStream semantics (dedup window, AckWait redelivery, MaxDeliver→DLQ) stay
// untested. So we spin a real server (testutil.StartNATS → nats:2.11 with -js)
// and assert against it. testutil.SkipIfNoDocker keeps the suite green where
// Docker is unavailable.
//
// These tests assert REAL effects (a lineage row recorded with the right fields,
// a message observed on the DLQ subject), NOT mock call counts.
package events_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/experiment-tracker/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/experiment-tracker/internal/events"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ----------------------------------------------------------------------------
// Test doubles + helpers
// ----------------------------------------------------------------------------

// fakeRecorder is an in-memory LineageRecorder. It records every LineageEvent it
// receives AND enforces the idempotency contract itself (dedupe on EventID) so a
// test can prove "same envelope twice → one effect" even at the recorder level.
// recordErr lets a test force a failure (to drive the NAK/DLQ path).
type fakeRecorder struct {
	mu        sync.Mutex
	events    []events.LineageEvent
	seen      map[string]int // EventID → times Record called (proves dedupe)
	recordErr error          // when non-nil, Record always fails (poison path)
}

func newFakeRecorder() *fakeRecorder {
	return &fakeRecorder{seen: make(map[string]int)}
}

func (f *fakeRecorder) Record(_ context.Context, ev events.LineageEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seen[ev.EventID]++
	if f.recordErr != nil {
		return f.recordErr
	}
	// Idempotent append: a second Record of the same EventID is a no-op, mirroring
	// the Postgres adapter's INSERT ... ON CONFLICT (event_id) DO NOTHING.
	if f.seen[ev.EventID] == 1 {
		f.events = append(f.events, ev)
	}
	return nil
}

func (f *fakeRecorder) snapshot() []events.LineageEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]events.LineageEvent, len(f.events))
	copy(out, f.events)
	return out
}

func (f *fakeRecorder) timesSeen(eventID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.seen[eventID]
}

// connectJS dials the test NATS server and returns a JetStream context. Each test
// gets a fresh server (testcontainer), so no cross-test stream/consumer bleed.
func connectJS(t *testing.T, url string) jetstream.JetStream {
	t.Helper()
	conn, js, err := natsutil.Connect(url)
	if err != nil {
		t.Fatalf("connect NATS: %v", err)
	}
	t.Cleanup(conn.Close)
	return js
}

// createStream creates (or updates) a JetStream stream over the given subjects.
// The platform's infra layer owns stream config in prod; in tests we create the
// minimal stream the adapter binds its durable consumer to.
func createStream(t *testing.T, js jetstream.JetStream, name string, subjects ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     name,
		Subjects: subjects,
	})
	if err != nil {
		t.Fatalf("create stream %s: %v", name, err)
	}
}

// publishProto marshals a proto event with protojson and publishes it via
// natsutil, exactly as a real producer (and our own publisher) would, so the
// subscriber under test decodes the identical wire form.
func publishProto(t *testing.T, pub *natsutil.Publisher, subject string, msg proto.Message) {
	t.Helper()
	data, err := protojson.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal %s: %v", subject, err)
	}
	if err := pub.Publish(context.Background(), subject, json.RawMessage(data)); err != nil {
		t.Fatalf("publish %s: %v", subject, err)
	}
}

// waitFor polls cond until true or a 15s deadline, failing the test on timeout.
// Real JetStream delivery is async; polling a real effect (a recorded row) is the
// honest way to synchronize without sleeping a fixed (flaky) duration.
func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

// ----------------------------------------------------------------------------
// PUBLISHER TESTS — produced events land on the right subject with a correct
// envelope + payload.
// ----------------------------------------------------------------------------

func TestPublisher_RunCreated_LandsOnSubjectWithEnvelopeAndPayload(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	url := testutil.StartNATS(t)
	js := connectJS(t, url)
	createStream(t, js, "EXPERIMENTS", "fp.experiments.>")

	// Subscribe RAW (a plain natsutil subscriber) so we observe exactly what the
	// publisher put on the wire — subject, envelope.Source, and the protojson body.
	sub := natsutil.NewSubscriber(js, natsutil.WithConsumerGroup("raw-runcreated"))
	t.Cleanup(sub.Close)

	got := make(chan natsutil.EventEnvelope, 1)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := sub.Subscribe(ctx, "EXPERIMENTS", events.SubjectRunCreated, func(_ context.Context, env natsutil.EventEnvelope) error {
		got <- env
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Produce via the ADAPTER under test (the natsutil.Publisher source is what
	// stamps EventEnvelope.Source = "experiment-tracker").
	pub := events.NewPublisher(natsutil.NewPublisher(js, events.ServiceName))
	started := time.Date(2026, 6, 18, 9, 30, 0, 0, time.UTC)
	err := pub.PublishRunCreated(ctx, domain.RunCreatedEvent{
		RunID:          "run-1",
		ExperimentID:   "exp-1",
		ModelVersionID: "mv-1",
		DisplayName:    "lr=0.01",
		StartedAt:      started,
	})
	if err != nil {
		t.Fatalf("PublishRunCreated: %v", err)
	}

	select {
	case env := <-got:
		// Envelope: derived type (subject minus "fp.experiments.") and source.
		if env.Source != events.ServiceName {
			t.Errorf("envelope source = %q, want %q", env.Source, events.ServiceName)
		}
		if env.Type != "run.created" {
			t.Errorf("envelope type = %q, want %q", env.Type, "run.created")
		}
		if env.ID == "" {
			t.Error("envelope ID (dedup key) is empty")
		}
		// Payload: decode the protojson body back into the canonical proto and
		// assert the fields round-tripped (incl. the RFC3339 timestamp).
		var payload eventsv1.RunCreated
		if err := protojson.Unmarshal(env.Data, &payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		if payload.GetRunId() != "run-1" || payload.GetExperimentId() != "exp-1" ||
			payload.GetModelVersionId() != "mv-1" || payload.GetDisplayName() != "lr=0.01" {
			t.Errorf("payload fields mismatch: %+v", &payload)
		}
		if !payload.GetStartedAt().AsTime().Equal(started) {
			t.Errorf("started_at = %v, want %v", payload.GetStartedAt().AsTime(), started)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for published RunCreated")
	}
}

func TestPublisher_RunFinished_CarriesFinalMetricsAndStatusEnum(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	url := testutil.StartNATS(t)
	js := connectJS(t, url)
	createStream(t, js, "EXPERIMENTS", "fp.experiments.>")

	sub := natsutil.NewSubscriber(js, natsutil.WithConsumerGroup("raw-runfinished"))
	t.Cleanup(sub.Close)
	got := make(chan natsutil.EventEnvelope, 1)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := sub.Subscribe(ctx, "EXPERIMENTS", events.SubjectRunFinished, func(_ context.Context, env natsutil.EventEnvelope) error {
		got <- env
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	pub := events.NewPublisher(natsutil.NewPublisher(js, events.ServiceName))
	ended := time.Date(2026, 6, 18, 10, 0, 0, 0, time.UTC)
	metricTS := time.Date(2026, 6, 18, 9, 59, 0, 0, time.UTC)
	err := pub.PublishRunFinished(ctx, domain.RunFinishedEvent{
		RunID:        "run-9",
		ExperimentID: "exp-9",
		Status:       domain.RunStatusFinished,
		FinalMetrics: []domain.MetricPoint{
			{Key: "accuracy", Value: 0.94, Step: 100, Timestamp: metricTS},
		},
		EndedAt: ended,
	})
	if err != nil {
		t.Fatalf("PublishRunFinished: %v", err)
	}

	select {
	case env := <-got:
		var payload eventsv1.RunFinished
		if err := protojson.Unmarshal(env.Data, &payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		// The domain RunStatus string mapped to the events enum at the boundary.
		if payload.GetStatus() != eventsv1.RunStatus_RUN_STATUS_FINISHED {
			t.Errorf("status = %v, want RUN_STATUS_FINISHED", payload.GetStatus())
		}
		if len(payload.GetFinalMetrics()) != 1 {
			t.Fatalf("final_metrics len = %d, want 1", len(payload.GetFinalMetrics()))
		}
		m := payload.GetFinalMetrics()[0]
		if m.GetKey() != "accuracy" || m.GetValue() != 0.94 || m.GetStep() != 100 {
			t.Errorf("metric mismatch: %+v", m)
		}
		if !m.GetTimestamp().AsTime().Equal(metricTS) {
			t.Errorf("metric ts = %v, want %v", m.GetTimestamp().AsTime(), metricTS)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for published RunFinished")
	}
}

// ----------------------------------------------------------------------------
// SUBSCRIBER TESTS — consume + dispatch correctly (the wide sink).
// ----------------------------------------------------------------------------

func TestSubscriber_ConsumesAndRecordsLineage(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	url := testutil.StartNATS(t)
	js := connectJS(t, url)
	createStream(t, js, "MODELS", "fp.models.>")

	rec := newFakeRecorder()
	store := natsutil.NewMemoryProcessedStore()
	// Bind only the one subject we exercise here via a raw consumer on the MODELS
	// stream (SubscribeAll would try to bind streams we didn't create in this
	// focused test). The adapter is built just to expose HandlerFor — the exact
	// production decode→record handler.
	natsSub := natsutil.NewSubscriber(js,
		natsutil.WithConsumerGroup("exp-tracker-models"),
		natsutil.WithIdempotencyStore(store),
		natsutil.WithMaxRetries(2),
		natsutil.WithDLQSubject("fp.dlq.exp-tracker"),
	)
	t.Cleanup(natsSub.Close)
	sub := events.NewSubscriber(events.SubscriberConfig{JS: js}, rec)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	handler := sub.HandlerFor(events.SubjectModelRegistered)
	if handler == nil {
		t.Fatal("no handler registered for ModelRegistered")
	}
	if err := natsSub.Subscribe(ctx, "MODELS", events.SubjectModelRegistered, handler); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	pub := natsutil.NewPublisher(js, "registry")
	registeredAt := time.Date(2026, 6, 18, 8, 0, 0, 0, time.UTC)
	publishProto(t, pub, events.SubjectModelRegistered, &eventsv1.ModelRegistered{
		ModelId:      "model-7",
		ModelName:    "fraud-detector",
		Team:         "ml-team",
		RegisteredAt: timestamppb.New(registeredAt),
	})

	waitFor(t, func() bool { return len(rec.snapshot()) == 1 }, "lineage recorded")

	le := rec.snapshot()[0]
	if le.Kind != events.LineageModelRegistered {
		t.Errorf("kind = %q, want %q", le.Kind, events.LineageModelRegistered)
	}
	if le.ModelID != "model-7" {
		t.Errorf("model id = %q, want model-7", le.ModelID)
	}
	if le.Source != "registry" {
		t.Errorf("source = %q, want registry (the producing service)", le.Source)
	}
	if le.EventID == "" {
		t.Error("EventID (idempotency key) not propagated from envelope")
	}
	if !le.OccurredAt.Equal(registeredAt) {
		t.Errorf("occurred_at = %v, want %v (producer clock)", le.OccurredAt, registeredAt)
	}
}

// TestSubscriber_DispatchesEveryConsumedSubject proves the dispatch TABLE is
// complete: every consumed subject decodes its own payload type and records the
// right Kind. One real consumer per subject (SubscribeAll), one published event each.
func TestSubscriber_DispatchesEveryConsumedSubject(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	url := testutil.StartNATS(t)
	js := connectJS(t, url)
	// Create every stream the consumed subjects live in.
	createStream(t, js, "MODELS", "fp.models.>")
	createStream(t, js, "PIPELINES", "fp.pipelines.>")
	createStream(t, js, "INFERENCE", "fp.inference.>")
	createStream(t, js, "FEATURES", "fp.features.>")
	createStream(t, js, "BILLING", "fp.billing.>")
	createStream(t, js, "NOTIFICATIONS", "fp.notifications.>")

	rec := newFakeRecorder()
	sub := events.NewSubscriber(events.SubscriberConfig{
		JS:           js,
		ConsumerBase: "exp-tracker-all",
		Store:        natsutil.NewMemoryProcessedStore(),
		MaxRetries:   2,
		DLQSubject:   "fp.dlq.exp-tracker",
	}, rec)
	t.Cleanup(sub.Close)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := sub.SubscribeAll(ctx); err != nil {
		t.Fatalf("SubscribeAll: %v", err)
	}

	pub := natsutil.NewPublisher(js, "producer")
	ts := timestamppb.New(time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC))

	// Publish one of every consumed event.
	publishProto(t, pub, events.SubjectModelRegistered, &eventsv1.ModelRegistered{ModelId: "m1", ModelName: "a", RegisteredAt: ts})
	publishProto(t, pub, events.SubjectModelVersionCreated, &eventsv1.ModelVersionCreated{ModelId: "m1", Version: "1.0", CreatedAt: ts})
	publishProto(t, pub, events.SubjectModelPromoted, &eventsv1.ModelPromoted{ModelId: "m1", Version: "1.0", FromStage: eventsv1.ModelStage_MODEL_STAGE_STAGING, ToStage: eventsv1.ModelStage_MODEL_STAGE_PRODUCTION, PromotedAt: ts})
	publishProto(t, pub, events.SubjectModelDeployed, &eventsv1.ModelDeployed{ModelId: "m1", Version: "1.0", ExecutionId: "ex1", WeightBps: 1000, DeployedAt: ts})
	publishProto(t, pub, events.SubjectStepCompleted, &eventsv1.StepCompleted{ExecutionId: "ex1", StepId: "s1", StepType: eventsv1.StepType_STEP_TYPE_TRAIN, CompletedAt: ts})
	publishProto(t, pub, events.SubjectInferenceCompleted, &eventsv1.InferenceCompleted{RequestId: "r1", ModelId: "m1", Version: "1.0", LatencyMs: 12, CompletedAt: ts})
	publishProto(t, pub, events.SubjectFeaturesWritten, &eventsv1.FeaturesWritten{FeatureViewName: "fv", WrittenCount: 5, WrittenThroughVersion: 3, WrittenAt: ts})
	publishProto(t, pub, events.SubjectFeatureViewDefined, &eventsv1.FeatureViewDefined{FeatureViewName: "fv", SchemaVersion: 1, DefinedAt: ts})
	publishProto(t, pub, events.SubjectUsageRecorded, &eventsv1.UsageRecorded{RecordId: "rec1", Team: "t", MeterType: eventsv1.MeterType_METER_TYPE_INFERENCE_REQUEST, Quantity: 1, SourceRequestId: "r1", OccurredAt: ts})
	publishProto(t, pub, events.SubjectNotificationDelivered, &eventsv1.NotificationDelivered{NotificationId: "n1", Channel: eventsv1.NotificationChannel_NOTIFICATION_CHANNEL_EMAIL, EventType: "fp.pipelines.failed", DeliveredAt: ts})
	publishProto(t, pub, events.SubjectNotificationFailed, &eventsv1.NotificationFailed{NotificationId: "n2", Channel: eventsv1.NotificationChannel_NOTIFICATION_CHANNEL_SLACK, Attempts: 3, ErrorMessage: "boom", FailedAt: ts})
	// Contract-mandated provenance facts: drift on a model + pipeline run boundaries.
	publishProto(t, pub, events.SubjectModelDriftDetected, &eventsv1.ModelDriftDetected{ModelName: "a", ModelVersion: "1.0", DriftType: eventsv1.DriftType_DRIFT_TYPE_DATA, Severity: eventsv1.DriftSeverity_DRIFT_SEVERITY_WARNING, ReportId: "rep1", DetectedAt: ts})
	publishProto(t, pub, events.SubjectPipelineStarted, &eventsv1.PipelineStarted{ExecutionId: "ex1", PipelineId: "p1", PipelineType: eventsv1.PipelineType_PIPELINE_TYPE_TRAINING_DAG, TriggeredBy: "model-monitor", StartedAt: ts})
	publishProto(t, pub, events.SubjectPipelineCompleted, &eventsv1.PipelineCompleted{ExecutionId: "ex1", PipelineId: "p1", PipelineType: eventsv1.PipelineType_PIPELINE_TYPE_TRAINING_DAG, Duration: durationpb.New(90 * time.Second), CompletedAt: ts})

	wantKinds := map[events.LineageEventKind]bool{
		events.LineageModelRegistered:       true,
		events.LineageModelVersionCreated:   true,
		events.LineageModelPromoted:         true,
		events.LineageModelDeployed:         true,
		events.LineagePipelineStepDone:      true,
		events.LineageInferenceCompleted:    true,
		events.LineageFeaturesWritten:       true,
		events.LineageFeatureViewDefined:    true,
		events.LineageUsageRecorded:         true,
		events.LineageNotificationDelivered: true,
		events.LineageNotificationFailed:    true,
		events.LineageModelDriftDetected:    true,
		events.LineagePipelineStarted:       true,
		events.LineagePipelineCompleted:     true,
	}

	waitFor(t, func() bool { return len(rec.snapshot()) == len(wantKinds) }, "all consumed events recorded")

	gotKinds := make(map[events.LineageEventKind]bool)
	for _, le := range rec.snapshot() {
		gotKinds[le.Kind] = true
	}
	for k := range wantKinds {
		if !gotKinds[k] {
			t.Errorf("missing lineage kind %q (dispatch table incomplete)", k)
		}
	}
}

// TestSubscriber_ModelDriftDetected_ProjectsHeadlineScalars proves the drift
// handler — a contract-mandated consumer (event-contract.md subject registry) —
// decodes ModelDriftDetected and projects exactly the headline scalars onto a
// lineage row: the exact serving version that drifted (vital under canary
// splitting), the producer clock as OccurredAt, and the report_id deep-link in
// the summary. It does NOT need a live stream — HandlerFor exposes the same
// decode→record handler SubscribeAll binds, so we drive one envelope through it.
func TestSubscriber_ModelDriftDetected_ProjectsHeadlineScalars(t *testing.T) {
	rec := newFakeRecorder()
	sub := events.NewSubscriber(events.SubscriberConfig{}, rec)
	handler := sub.HandlerFor(events.SubjectModelDriftDetected)
	if handler == nil {
		t.Fatal("no handler registered for ModelDriftDetected (subscription missing)")
	}

	detectedAt := time.Date(2026, 6, 18, 14, 0, 0, 0, time.UTC)
	body, err := protojson.Marshal(&eventsv1.ModelDriftDetected{
		ModelName:    "fraud-detector",
		ModelVersion: "3.2",
		DriftType:    eventsv1.DriftType_DRIFT_TYPE_PREDICTION,
		Severity:     eventsv1.DriftSeverity_DRIFT_SEVERITY_CRITICAL,
		ReportId:     "drift-report-77",
		DetectedAt:   timestamppb.New(detectedAt),
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Source is "model-monitor" — the drift event lives under fp.models.* but is
	// produced by the monitor; lineage records the real producer (audit).
	env := natsutil.NewEnvelope("drift.detected", "model-monitor", json.RawMessage(body))
	if err := handler(context.Background(), env); err != nil {
		t.Fatalf("handle drift: %v", err)
	}

	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("recorded %d rows, want 1", len(got))
	}
	le := got[0]
	if le.Kind != events.LineageModelDriftDetected {
		t.Errorf("kind = %q, want %q", le.Kind, events.LineageModelDriftDetected)
	}
	if le.ModelVersion != "3.2" {
		t.Errorf("model_version = %q, want 3.2 (the exact serving version that drifted)", le.ModelVersion)
	}
	if le.Source != "model-monitor" {
		t.Errorf("source = %q, want model-monitor (the producer, not the subject domain)", le.Source)
	}
	if le.EventID != env.ID {
		t.Errorf("event_id = %q, want %q (idempotency key from envelope)", le.EventID, env.ID)
	}
	if !le.OccurredAt.Equal(detectedAt) {
		t.Errorf("occurred_at = %v, want %v (producer clock)", le.OccurredAt, detectedAt)
	}
	// The deep-link report id must be in the PII-free summary so the UI can link out.
	if !strings.Contains(le.Summary, "drift-report-77") {
		t.Errorf("summary %q missing report id deep-link", le.Summary)
	}
}

// TestSubscriber_PipelineBoundaries_ProjectExecutionID proves the two pipeline
// boundary handlers (started/completed) — also contract-mandated consumers —
// decode their payloads and carry the execution_id (the run-correlation join key)
// onto the lineage row, so a "run Y context" timeline query can bracket the run.
func TestSubscriber_PipelineBoundaries_ProjectExecutionID(t *testing.T) {
	rec := newFakeRecorder()
	sub := events.NewSubscriber(events.SubscriberConfig{}, rec)

	startedAt := time.Date(2026, 6, 18, 15, 0, 0, 0, time.UTC)
	startBody, err := protojson.Marshal(&eventsv1.PipelineStarted{
		ExecutionId:  "exec-42",
		PipelineId:   "retrain-dag",
		PipelineType: eventsv1.PipelineType_PIPELINE_TYPE_TRAINING_DAG,
		TriggeredBy:  "model-monitor",
		StartedAt:    timestamppb.New(startedAt),
	})
	if err != nil {
		t.Fatalf("marshal started: %v", err)
	}
	startHandler := sub.HandlerFor(events.SubjectPipelineStarted)
	if startHandler == nil {
		t.Fatal("no handler for PipelineStarted (subscription missing)")
	}
	if err := startHandler(context.Background(), natsutil.NewEnvelope("started", "pipeline-orchestrator", json.RawMessage(startBody))); err != nil {
		t.Fatalf("handle started: %v", err)
	}

	completedAt := time.Date(2026, 6, 18, 15, 30, 0, 0, time.UTC)
	doneBody, err := protojson.Marshal(&eventsv1.PipelineCompleted{
		ExecutionId:  "exec-42",
		PipelineId:   "retrain-dag",
		PipelineType: eventsv1.PipelineType_PIPELINE_TYPE_TRAINING_DAG,
		Duration:     durationpb.New(30 * time.Minute),
		CompletedAt:  timestamppb.New(completedAt),
	})
	if err != nil {
		t.Fatalf("marshal completed: %v", err)
	}
	doneHandler := sub.HandlerFor(events.SubjectPipelineCompleted)
	if doneHandler == nil {
		t.Fatal("no handler for PipelineCompleted (subscription missing)")
	}
	if err := doneHandler(context.Background(), natsutil.NewEnvelope("completed", "pipeline-orchestrator", json.RawMessage(doneBody))); err != nil {
		t.Fatalf("handle completed: %v", err)
	}

	got := rec.snapshot()
	if len(got) != 2 {
		t.Fatalf("recorded %d rows, want 2 (start + complete)", len(got))
	}
	byKind := make(map[events.LineageEventKind]events.LineageEvent)
	for _, le := range got {
		byKind[le.Kind] = le
	}
	start, ok := byKind[events.LineagePipelineStarted]
	if !ok {
		t.Fatal("missing PIPELINE_STARTED lineage row")
	}
	if start.ExecutionID != "exec-42" {
		t.Errorf("started execution_id = %q, want exec-42", start.ExecutionID)
	}
	if !start.OccurredAt.Equal(startedAt) {
		t.Errorf("started occurred_at = %v, want %v", start.OccurredAt, startedAt)
	}
	done, ok := byKind[events.LineagePipelineCompleted]
	if !ok {
		t.Fatal("missing PIPELINE_COMPLETED lineage row")
	}
	if done.ExecutionID != "exec-42" {
		t.Errorf("completed execution_id = %q, want exec-42 (same run as start)", done.ExecutionID)
	}
	if !done.OccurredAt.Equal(completedAt) {
		t.Errorf("completed occurred_at = %v, want %v", done.OccurredAt, completedAt)
	}
}

// ----------------------------------------------------------------------------
// IDEMPOTENCY — the SAME envelope id delivered twice yields a SINGLE effect.
// ----------------------------------------------------------------------------
//
// We assert idempotency at TWO layers, the way production gets exactly-once-in-
// effect: (1) drive the same envelope through the handler twice directly and
// prove the recorder dedupes on EventID; (2) prove the natsutil ProcessedStore
// short-circuits a real redelivery so the handler isn't even re-invoked.

func TestSubscriber_DuplicateEnvelope_SingleEffect(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	url := testutil.StartNATS(t)
	js := connectJS(t, url)

	rec := newFakeRecorder()
	sub := events.NewSubscriber(events.SubscriberConfig{JS: js}, rec)
	handler := sub.HandlerFor(events.SubjectModelRegistered)
	if handler == nil {
		t.Fatal("no handler for ModelRegistered")
	}

	// Build ONE envelope (one ID) carrying a ModelRegistered payload, then run the
	// handler twice with the identical envelope — simulating a redelivery.
	body, err := protojson.Marshal(&eventsv1.ModelRegistered{ModelId: "dup-model", ModelName: "x", RegisteredAt: timestamppb.Now()})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	env := natsutil.NewEnvelope("registered", "registry", json.RawMessage(body))

	if err := handler(context.Background(), env); err != nil {
		t.Fatalf("first handle: %v", err)
	}
	if err := handler(context.Background(), env); err != nil {
		t.Fatalf("second handle: %v", err)
	}

	// The recorder saw the SAME EventID twice (the handler ran twice in this
	// direct-drive test), but recorded exactly ONE lineage row — idempotent.
	if got := rec.timesSeen(env.ID); got != 2 {
		t.Fatalf("recorder Record called %d times, want 2 (direct double-drive)", got)
	}
	if n := len(rec.snapshot()); n != 1 {
		t.Fatalf("recorded %d lineage rows, want 1 (idempotent on EventID)", n)
	}
}

// TestSubscriber_ProcessedStoreSkipsDuplicate proves the natsutil ProcessedStore
// prevents the handler from running at all for an already-processed envelope id:
// we pre-mark the id, publish a message carrying it, and assert the recorder
// stays empty (the subscriber ACKs the duplicate without invoking the handler).
func TestSubscriber_ProcessedStoreSkipsDuplicate(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	url := testutil.StartNATS(t)
	js := connectJS(t, url)
	createStream(t, js, "MODELS", "fp.models.>")

	rec := newFakeRecorder()
	store := natsutil.NewMemoryProcessedStore()
	natsSub := natsutil.NewSubscriber(js,
		natsutil.WithConsumerGroup("dedupe-grp"),
		natsutil.WithIdempotencyStore(store),
	)
	t.Cleanup(natsSub.Close)
	sub := events.NewSubscriber(events.SubscriberConfig{JS: js}, rec)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := natsSub.Subscribe(ctx, "MODELS", events.SubjectModelRegistered, sub.HandlerFor(events.SubjectModelRegistered)); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Pre-mark an event ID as processed; publish a message with that exact
	// envelope ID. The subscriber must SKIP the handler (already processed) + ACK.
	body, _ := protojson.Marshal(&eventsv1.ModelRegistered{ModelId: "skip-me", ModelName: "x", RegisteredAt: timestamppb.Now()})
	env := natsutil.NewEnvelope("registered", "registry", json.RawMessage(body))
	if err := store.MarkProcessed(ctx, env.ID); err != nil {
		t.Fatalf("pre-mark: %v", err)
	}
	envBytes, _ := json.Marshal(env)
	if _, err := js.Publish(ctx, events.SubjectModelRegistered, envBytes, jetstream.WithMsgID(env.ID)); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Give the consumer time to deliver+skip. The recorder must remain empty.
	time.Sleep(2 * time.Second)
	if n := len(rec.snapshot()); n != 0 {
		t.Fatalf("recorded %d rows, want 0 (ProcessedStore should have skipped)", n)
	}
}

// ----------------------------------------------------------------------------
// DLQ — a poison message (always fails) is routed to the dead-letter subject.
// ----------------------------------------------------------------------------

func TestSubscriber_PoisonMessage_RoutedToDLQ(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	url := testutil.StartNATS(t)
	js := connectJS(t, url)
	// One stream holds BOTH the source subject and the DLQ subject so we can
	// observe the dead-lettered copy.
	createStream(t, js, "MODELS", "fp.models.>", "fp.dlq.>")

	rec := newFakeRecorder()
	// Force every Record to fail → the handler always errors → after MaxRetries
	// the message is poison and must be dead-lettered.
	rec.recordErr = errors.New("permanent record failure")
	const dlqSubject = "fp.dlq.exp-tracker"
	natsSub := natsutil.NewSubscriber(js,
		natsutil.WithConsumerGroup("poison-grp"),
		natsutil.WithMaxRetries(1), // 1 retry → 2 deliveries → then DLQ
		natsutil.WithDLQSubject(dlqSubject),
		natsutil.WithAckWait(1*time.Second), // fast redelivery so the test is quick
	)
	t.Cleanup(natsSub.Close)
	sub := events.NewSubscriber(events.SubscriberConfig{JS: js}, rec)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := natsSub.Subscribe(ctx, "MODELS", events.SubjectModelRegistered, sub.HandlerFor(events.SubjectModelRegistered)); err != nil {
		t.Fatalf("subscribe source: %v", err)
	}

	// Observe the DLQ subject with a separate raw consumer.
	dlqGot := make(chan natsutil.EventEnvelope, 1)
	dlqSub := natsutil.NewSubscriber(js, natsutil.WithConsumerGroup("dlq-observer"))
	t.Cleanup(dlqSub.Close)
	if err := dlqSub.Subscribe(ctx, "MODELS", dlqSubject, func(_ context.Context, env natsutil.EventEnvelope) error {
		dlqGot <- env
		return nil
	}); err != nil {
		t.Fatalf("subscribe dlq: %v", err)
	}

	pub := natsutil.NewPublisher(js, "registry")
	publishProto(t, pub, events.SubjectModelRegistered, &eventsv1.ModelRegistered{ModelId: "poison", ModelName: "x", RegisteredAt: timestamppb.Now()})

	select {
	case env := <-dlqGot:
		// The dead-lettered copy carries the original payload — decode-verify it.
		var payload eventsv1.ModelRegistered
		if err := protojson.Unmarshal(env.Data, &payload); err != nil {
			t.Fatalf("decode DLQ payload: %v", err)
		}
		if payload.GetModelId() != "poison" {
			t.Errorf("DLQ payload model id = %q, want poison", payload.GetModelId())
		}
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for poison message on DLQ")
	}
}
