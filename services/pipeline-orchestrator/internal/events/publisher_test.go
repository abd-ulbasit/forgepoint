package events_test

import (
	"context"
	"testing"
	"time"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/events"
	"github.com/nats-io/nats.go/jetstream"
)

// ============================================================================
// PUBLISHER INTEGRATION TESTS (real NATS JetStream via testcontainers)
// ============================================================================
//
// These prove the OUTBOUND adapter end-to-end: a domain.StepEvent published
// through the adapter lands on the canonical subject, wrapped in an EventEnvelope
// with source = "pipeline-orchestrator", carrying a CANONICAL proto-JSON payload
// that protojson.Unmarshal decodes back into the generated events.v1 type. We use
// a real broker (not a mock) because the contract we care about is the WIRE
// behavior — subject routing + envelope + canonical encoding — which a mock can't
// faithfully reproduce.
// ============================================================================

// newJS spins up a real NATS, creates the PIPELINES and MODELS streams that cover
// every subject these tests touch, and returns a JetStream handle. The streams
// are torn down with the container by t.Cleanup inside StartNATS.
func newJS(t *testing.T) jetstream.JetStream {
	t.Helper()
	testutil.SkipIfNoDocker(t)
	url := testutil.StartNATS(t)

	conn, js, err := natsutil.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(conn.Close)

	ctx := context.Background()
	// PIPELINES stream covers every event THIS service produces. We include the DLQ
	// subject used by the subscriber tests in the MODELS stream below.
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "PIPELINES",
		Subjects: []string{"fp.pipelines.>"},
	}); err != nil {
		t.Fatalf("create PIPELINES stream: %v", err)
	}
	// MODELS stream covers the consumed drift subject AND the DLQ subject the
	// retrain consumer routes poison messages to (the DLQ frequently shares the
	// source stream — see natsutil's DLQ Msg-Id note).
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "MODELS",
		Subjects: []string{"fp.models.>", "fp.dlq.>"},
	}); err != nil {
		t.Fatalf("create MODELS stream: %v", err)
	}
	return js
}

// drainOne subscribes to a subject and returns the first envelope, or fails.
func drainOne(t *testing.T, js jetstream.JetStream, stream, subject string) natsutil.EventEnvelope {
	t.Helper()
	got := make(chan natsutil.EventEnvelope, 1)
	sub := natsutil.NewSubscriber(js)
	t.Cleanup(sub.Close)
	if err := sub.Subscribe(context.Background(), stream, subject, func(_ context.Context, env natsutil.EventEnvelope) error {
		got <- env
		return nil
	}); err != nil {
		t.Fatalf("subscribe %s: %v", subject, err)
	}
	select {
	case env := <-got:
		return env
	case <-time.After(15 * time.Second):
		t.Fatalf("timeout waiting for event on %s", subject)
		return natsutil.EventEnvelope{}
	}
}

func TestPublisher_PipelineStarted_LandsOnSubjectWithCanonicalPayload(t *testing.T) {
	js := newJS(t)
	pub := events.NewPublisher(natsutil.NewPublisher(js, events.Source), nil)

	now := time.Now().UTC().Truncate(time.Second)
	err := pub.Publish(context.Background(), domain.StepEvent{
		Type:         domain.EventPipelineStarted,
		ExecutionID:  "exec-1",
		PipelineID:   "pipe-1",
		PipelineType: domain.PipelineTypeDeploymentSaga,
		TriggeredBy:  "user-42",
		OccurredAt:   now,
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	env := drainOne(t, js, "PIPELINES", events.SubjectPipelineStarted)

	if env.Source != events.Source {
		t.Errorf("envelope.Source = %q, want %q", env.Source, events.Source)
	}
	// Subject-derived type: "fp.pipelines.started" → "started".
	if env.Type != "started" {
		t.Errorf("envelope.Type = %q, want %q", env.Type, "started")
	}

	var payload eventsv1.PipelineStarted
	if err := protojsonUnmarshal(t, env.Data, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload.GetExecutionId() != "exec-1" || payload.GetPipelineId() != "pipe-1" {
		t.Errorf("ids = (%q,%q), want (exec-1,pipe-1)", payload.GetExecutionId(), payload.GetPipelineId())
	}
	if payload.GetPipelineType() != eventsv1.PipelineType_PIPELINE_TYPE_DEPLOYMENT_SAGA {
		t.Errorf("pipeline_type = %v, want DEPLOYMENT_SAGA", payload.GetPipelineType())
	}
	if payload.GetTriggeredBy() != "user-42" {
		t.Errorf("triggered_by = %q, want user-42", payload.GetTriggeredBy())
	}
	if got := payload.GetStartedAt().AsTime(); !got.Equal(now) {
		t.Errorf("started_at = %v, want %v", got, now)
	}
}

func TestPublisher_StepCompleted_CarriesOutputStruct(t *testing.T) {
	js := newJS(t)
	pub := events.NewPublisher(natsutil.NewPublisher(js, events.Source), nil)

	err := pub.Publish(context.Background(), domain.StepEvent{
		Type:        domain.EventStepCompleted,
		ExecutionID: "exec-2",
		PipelineID:  "pipe-2",
		StepID:      "train",
		StepType:    domain.StepTypeTrain,
		Output:      map[string]any{"model_uri": "s3://bucket/m.onnx", "accuracy": 0.97},
		OccurredAt:  time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	env := drainOne(t, js, "PIPELINES", events.SubjectStepCompleted)
	var payload eventsv1.StepCompleted
	if err := protojsonUnmarshal(t, env.Data, &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if payload.GetStepType() != eventsv1.StepType_STEP_TYPE_TRAIN {
		t.Errorf("step_type = %v, want TRAIN", payload.GetStepType())
	}
	out := payload.GetOutput().AsMap()
	if out["model_uri"] != "s3://bucket/m.onnx" {
		t.Errorf("output.model_uri = %v, want s3://bucket/m.onnx", out["model_uri"])
	}
}

func TestPublisher_ModelDeployed_CarriesEndpointAndIdentityFromOutput(t *testing.T) {
	js := newJS(t)
	pub := events.NewPublisher(natsutil.NewPublisher(js, events.Source), nil)

	err := pub.Publish(context.Background(), domain.StepEvent{
		Type:        domain.EventModelDeployed,
		ExecutionID: "exec-3",
		PipelineID:  "pipe-3",
		StepID:      "deploy",
		StepType:    domain.StepTypeDeploy,
		Endpoint:    "iris-v3.fp-models.svc:9090",
		Output: map[string]any{
			"model_id":   "m-1",
			"model_name": "iris",
			"version_id": "v-3",
			"version":    "3.0.0",
			"weight_bps": 1000,
		},
		OccurredAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	env := drainOne(t, js, "PIPELINES", events.SubjectModelDeployed)
	var payload eventsv1.ModelDeployed
	if err := protojsonUnmarshal(t, env.Data, &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if payload.GetEndpoint() != "iris-v3.fp-models.svc:9090" {
		t.Errorf("endpoint = %q", payload.GetEndpoint())
	}
	if payload.GetModelName() != "iris" || payload.GetVersion() != "3.0.0" {
		t.Errorf("identity = (%q,%q), want (iris,3.0.0)", payload.GetModelName(), payload.GetVersion())
	}
	if payload.GetWeightBps() != 1000 {
		t.Errorf("weight_bps = %d, want 1000", payload.GetWeightBps())
	}
	if payload.GetExecutionId() != "exec-3" {
		t.Errorf("execution_id = %q, want exec-3", payload.GetExecutionId())
	}
}

// TestPublisher_AllEventTypes_RouteToExpectedSubjects proves the EventType →
// subject mapping is complete and correct for every produced event. A regression
// that routed StepFailed to the wrong subject would silently break Notification;
// this catches it.
func TestPublisher_AllEventTypes_RouteToExpectedSubjects(t *testing.T) {
	js := newJS(t)
	pub := events.NewPublisher(natsutil.NewPublisher(js, events.Source), nil)

	cases := []struct {
		name    string
		evType  domain.EventType
		subject string
	}{
		{"started", domain.EventPipelineStarted, events.SubjectPipelineStarted},
		{"step.completed", domain.EventStepCompleted, events.SubjectStepCompleted},
		{"step.failed", domain.EventStepFailed, events.SubjectStepFailed},
		{"completed", domain.EventPipelineCompleted, events.SubjectPipelineCompleted},
		{"failed", domain.EventPipelineFailed, events.SubjectPipelineFailed},
		{"compensation", domain.EventCompensationTriggered, events.SubjectCompensationTriggered},
		{"undeployed", domain.EventModelUndeployed, events.SubjectModelUndeployed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Subscribe BEFORE publishing so the durable consumer captures it.
			got := make(chan natsutil.EventEnvelope, 1)
			sub := natsutil.NewSubscriber(js)
			t.Cleanup(sub.Close)
			if err := sub.Subscribe(context.Background(), "PIPELINES", tc.subject,
				func(_ context.Context, env natsutil.EventEnvelope) error { got <- env; return nil }); err != nil {
				t.Fatalf("subscribe: %v", err)
			}

			if err := pub.Publish(context.Background(), domain.StepEvent{
				Type: tc.evType, ExecutionID: "e-" + tc.name, PipelineID: "p", OccurredAt: time.Now().UTC(),
			}); err != nil {
				t.Fatalf("publish: %v", err)
			}

			select {
			case env := <-got:
				if env.Source != events.Source {
					t.Errorf("source = %q, want %q", env.Source, events.Source)
				}
			case <-time.After(15 * time.Second):
				t.Fatalf("event %s did not arrive on %s", tc.name, tc.subject)
			}
		})
	}
}
