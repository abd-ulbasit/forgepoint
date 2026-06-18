package events_test

import (
	"context"
	"testing"
	"time"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/inference-gateway/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/inference-gateway/internal/events"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
)

// ============================================================================
// PUBLISHER TESTS — verify produced events land on the right subject with a
// correct envelope + payload, over a REAL NATS JetStream (testcontainers).
// ============================================================================
//
// We test against real NATS (not a mock) because the whole value of the adapter
// is the wire behavior: the envelope wrapping, the subject targeting, and that a
// consumer can faithfully decode what we publish. A mock of jetstream.JetStream
// would test our code against our assumptions, not against NATS.

// TestPublisher_PublishCompleted_LandsOnSubjectWithEnvelopeAndPayload publishes a
// success event through the adapter and asserts the raw message on
// fp.inference.completed carries the right envelope (type, source) and a payload
// that round-trips back to the canonical events.v1.InferenceCompleted.
func TestPublisher_PublishCompleted_LandsOnSubjectWithEnvelopeAndPayload(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	js, _ := newJetStream(t)
	mustCreateStream(t, js, events.StreamInference, "fp.inference.>")

	// The adapter wraps a natsutil.Publisher whose source MUST be the service name
	// so EventEnvelope.source == "inference-gateway".
	pub := events.NewPublisher(natsutil.NewPublisher(js, events.Source))

	// Subscribe to the produced subject with a raw consumer so we observe exactly
	// what hit the wire (envelope + payload), independent of our subscriber code.
	got := make(chan natsutil.EventEnvelope, 1)
	raw := natsutil.NewSubscriber(js)
	t.Cleanup(raw.Close)
	if err := raw.Subscribe(context.Background(), events.StreamInference, events.SubjectInferenceCompleted,
		func(_ context.Context, env natsutil.EventEnvelope) error {
			got <- env
			return nil
		}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	completedAt := time.Now().UTC().Truncate(time.Millisecond)
	in := domain.InferenceCompleted{
		RequestID:   "req-123",
		ModelName:   "fraud-detector",
		Version:     "v3",
		APIKeyID:    "key-abc",
		IsCanary:    true,
		Latency:     150 * time.Millisecond,
		CompletedAt: completedAt,
	}
	if err := pub.PublishCompleted(context.Background(), in); err != nil {
		t.Fatalf("PublishCompleted: %v", err)
	}

	env := waitEnvelope(t, got)

	// Envelope assertions: derived type (subject minus "fp.inference.") + source.
	if env.Type != "completed" {
		t.Errorf("envelope Type = %q, want %q", env.Type, "completed")
	}
	if env.Source != events.Source {
		t.Errorf("envelope Source = %q, want %q", env.Source, events.Source)
	}
	if env.ID == "" {
		t.Error("envelope ID is empty (needed for transport dedupe)")
	}

	// Payload assertions: it must decode back to the canonical wire message with
	// the server-authoritative fields intact.
	var p eventsv1.InferenceCompleted
	if err := protojson.Unmarshal(env.Data, &p); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if p.GetRequestId() != "req-123" {
		t.Errorf("RequestId = %q, want %q", p.GetRequestId(), "req-123")
	}
	if p.GetModelName() != "fraud-detector" {
		t.Errorf("ModelName = %q, want %q", p.GetModelName(), "fraud-detector")
	}
	if p.GetVersion() != "v3" {
		t.Errorf("Version = %q, want %q", p.GetVersion(), "v3")
	}
	if p.GetApiKeyId() != "key-abc" {
		t.Errorf("ApiKeyId = %q, want %q", p.GetApiKeyId(), "key-abc")
	}
	if !p.GetIsCanary() {
		t.Error("IsCanary = false, want true")
	}
	if p.GetLatencyMs() != 150 {
		t.Errorf("LatencyMs = %d, want 150", p.GetLatencyMs())
	}
	if p.GetLatency().AsDuration() != 150*time.Millisecond {
		t.Errorf("Latency = %v, want 150ms", p.GetLatency().AsDuration())
	}
	if !p.GetCompletedAt().AsTime().Equal(completedAt) {
		t.Errorf("CompletedAt = %v, want %v", p.GetCompletedAt().AsTime(), completedAt)
	}
}

// TestPublisher_PublishFailed_MapsReasonAndSubject publishes a failure event and
// asserts it lands on fp.inference.failed with the domain FailureReason mapped to
// the canonical events.v1 enum.
func TestPublisher_PublishFailed_MapsReasonAndSubject(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	js, _ := newJetStream(t)
	mustCreateStream(t, js, events.StreamInference, "fp.inference.>")

	pub := events.NewPublisher(natsutil.NewPublisher(js, events.Source))

	got := make(chan natsutil.EventEnvelope, 1)
	raw := natsutil.NewSubscriber(js)
	t.Cleanup(raw.Close)
	if err := raw.Subscribe(context.Background(), events.StreamInference, events.SubjectInferenceFailed,
		func(_ context.Context, env natsutil.EventEnvelope) error {
			got <- env
			return nil
		}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	failedAt := time.Now().UTC().Truncate(time.Millisecond)
	in := domain.InferenceFailed{
		RequestID: "req-err-9",
		ModelName: "fraud-detector",
		Version:   "", // failed before version selection (NO_ROUTE)
		APIKeyID:  "key-abc",
		Reason:    domain.FailureReasonCircuitOpen,
		Message:   "breaker open",
		FailedAt:  failedAt,
	}
	if err := pub.PublishFailed(context.Background(), in); err != nil {
		t.Fatalf("PublishFailed: %v", err)
	}

	env := waitEnvelope(t, got)
	if env.Type != "failed" {
		t.Errorf("envelope Type = %q, want %q", env.Type, "failed")
	}
	if env.Source != events.Source {
		t.Errorf("envelope Source = %q, want %q", env.Source, events.Source)
	}

	var p eventsv1.InferenceFailed
	if err := protojson.Unmarshal(env.Data, &p); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if p.GetRequestId() != "req-err-9" {
		t.Errorf("RequestId = %q, want %q", p.GetRequestId(), "req-err-9")
	}
	if p.GetReason() != eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_CIRCUIT_OPEN {
		t.Errorf("Reason = %v, want CIRCUIT_OPEN", p.GetReason())
	}
	if p.GetVersion() != "" {
		t.Errorf("Version = %q, want empty (failed before selection)", p.GetVersion())
	}
	if !p.GetFailedAt().AsTime().Equal(failedAt) {
		t.Errorf("FailedAt = %v, want %v", p.GetFailedAt().AsTime(), failedAt)
	}
}

// ----------------------------------------------------------------------------
// shared test helpers (used by publisher_test.go and subscribers_test.go).
// ----------------------------------------------------------------------------

// newJetStream starts a real NATS+JetStream container and returns a JetStream
// context plus the raw connection (closed on cleanup).
func newJetStream(t *testing.T) (jetstream.JetStream, func()) {
	t.Helper()
	url := testutil.StartNATS(t)
	conn, js, err := natsutil.Connect(url)
	if err != nil {
		t.Fatalf("connect NATS: %v", err)
	}
	t.Cleanup(conn.Close)
	return js, conn.Close
}

// mustCreateStream creates a JetStream stream binding the given subject filter.
// Subscribers need the stream to pre-exist (a consumer attaches to a stream).
func mustCreateStream(t *testing.T, js jetstream.JetStream, name string, subjects ...string) {
	t.Helper()
	_, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:     name,
		Subjects: subjects,
	})
	if err != nil {
		t.Fatalf("create stream %s: %v", name, err)
	}
}

// waitEnvelope reads one envelope or fails the test on timeout.
func waitEnvelope(t *testing.T, ch <-chan natsutil.EventEnvelope) natsutil.EventEnvelope {
	t.Helper()
	select {
	case env := <-ch:
		return env
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for event")
		return natsutil.EventEnvelope{}
	}
}
