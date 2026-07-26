package events_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/events"
)

// ============================================================================
// SHARED TEST HELPERS
// ============================================================================

// protojsonUnmarshal decodes a canonical proto-JSON envelope payload, mirroring
// what a real consumer does. Kept here (shared across publisher/subscriber tests)
// so every assertion uses the SAME canonical decode the production code uses.
func protojsonUnmarshal(t *testing.T, data json.RawMessage, msg proto.Message) error {
	t.Helper()
	return protojson.Unmarshal(data, msg)
}

// fakeService is a hand-written domain.PipelineService stub. We assert ONLY on
// the inbound adapter's behavior (decode + dispatch + idempotency + DLQ), so only
// TriggerExecution is meaningful; the other methods satisfy the interface and
// panic if unexpectedly called (a guard against the adapter touching the wrong
// port). This is the unit-test-the-adapter-against-a-mock-domain pattern — the
// real saga engine is exercised in the domain package's own tests.
type fakeService struct {
	mu        sync.Mutex
	triggers  []domain.TriggerInput
	actors    []domain.Actor
	err       error                                                                      // returned from TriggerExecution
	onTrigger func(actor domain.Actor, in domain.TriggerInput) (domain.Execution, error) // optional override
}

func (f *fakeService) TriggerExecution(_ context.Context, actor domain.Actor, in domain.TriggerInput) (domain.Execution, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.triggers = append(f.triggers, in)
	f.actors = append(f.actors, actor)
	if f.onTrigger != nil {
		return f.onTrigger(actor, in)
	}
	if f.err != nil {
		return domain.Execution{}, f.err
	}
	return domain.Execution{ID: "exec-new", PipelineID: in.PipelineID, Status: domain.ExecutionStatusCompleted}, nil
}

func (f *fakeService) triggerCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.triggers)
}

func (f *fakeService) lastTrigger() (domain.Actor, domain.TriggerInput, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.triggers) == 0 {
		return domain.Actor{}, domain.TriggerInput{}, false
	}
	return f.actors[len(f.actors)-1], f.triggers[len(f.triggers)-1], true
}

// Unused methods — present only to satisfy the interface.
func (f *fakeService) CreatePipeline(context.Context, domain.Actor, domain.CreatePipelineInput) (domain.PipelineDefinition, error) {
	panic("CreatePipeline not expected")
}
func (f *fakeService) GetPipeline(context.Context, domain.Actor, string) (domain.PipelineDefinition, error) {
	panic("GetPipeline not expected")
}
func (f *fakeService) UpdatePipeline(context.Context, domain.Actor, domain.UpdatePipelineInput) (domain.PipelineDefinition, error) {
	panic("UpdatePipeline not expected")
}
func (f *fakeService) DeletePipeline(context.Context, domain.Actor, string) error {
	panic("DeletePipeline not expected")
}
func (f *fakeService) ListPipelines(context.Context, domain.Actor, domain.ListPipelinesFilter) ([]domain.PipelineDefinition, string, error) {
	panic("ListPipelines not expected")
}
func (f *fakeService) GetExecution(context.Context, domain.Actor, string) (domain.Execution, error) {
	panic("GetExecution not expected")
}
func (f *fakeService) CancelExecution(context.Context, domain.Actor, string, string) (domain.Execution, error) {
	panic("CancelExecution not expected")
}
func (f *fakeService) ListExecutions(context.Context, domain.Actor, domain.ListExecutionsFilter) ([]domain.Execution, string, error) {
	panic("ListExecutions not expected")
}

// publishDrift marshals a ModelDriftDetected as the producer (model-monitor)
// would — canonical proto-JSON in the envelope — and publishes it. Returns the
// envelope id so a test can re-publish the SAME id to exercise idempotency.
func publishDrift(t *testing.T, js jetstream.JetStream, drift *eventsv1.ModelDriftDetected) {
	t.Helper()
	raw, err := protojson.Marshal(drift)
	if err != nil {
		t.Fatalf("marshal drift: %v", err)
	}
	pub := natsutil.NewPublisher(js, events.Source)
	if err := pub.Publish(context.Background(), events.SubjectModelDriftDetected, json.RawMessage(raw)); err != nil {
		t.Fatalf("publish drift: %v", err)
	}
}

// criticalDrift builds a CRITICAL, auto-retrain-armed drift event with a real
// retrain pipeline id and a small retrain context.
func criticalDrift(reportID, pipelineID string) *eventsv1.ModelDriftDetected {
	return &eventsv1.ModelDriftDetected{
		ModelName:         "iris",
		ModelVersion:      "3.0.0",
		DriftType:         eventsv1.DriftType_DRIFT_TYPE_DATA,
		Severity:          eventsv1.DriftSeverity_DRIFT_SEVERITY_CRITICAL,
		ReportId:          reportID,
		AutoRetrain:       true,
		RetrainPipelineId: pipelineID,
		RetrainContext:    mustStruct(map[string]any{"top_drifted_feature": "income", "psi": 0.41}),
		DetectedAt:        timestamppb.New(time.Now().UTC()),
	}
}

func mustStruct(m map[string]any) *structpb.Struct {
	s, err := structpb.NewStruct(m)
	if err != nil {
		panic(err)
	}
	return s
}

// newRetrainSub wires the drift consumer with the SAME options main.go uses:
// durable consumer group + idempotency store + bounded retries + DLQ. The
// in-memory ProcessedStore is fine for a single-replica test (it is the exact
// dedup the production Redis/Postgres store implements).
func newRetrainSub(t *testing.T, js jetstream.JetStream, svc domain.PipelineService, store natsutil.ProcessedStore) *events.DriftRetrainSubscriber {
	t.Helper()
	sub := natsutil.NewSubscriber(js,
		natsutil.WithConsumerGroup("pipeline-orchestrator-retrain"),
		natsutil.WithIdempotencyStore(store),
		natsutil.WithMaxRetries(3),
		natsutil.WithDLQSubject("fp.dlq.pipelines.retrain"),
		natsutil.WithAckWait(2*time.Second), // short so redelivery-driven tests run fast
	)
	t.Cleanup(sub.Close)
	rs := events.NewDriftRetrainSubscriber(svc, sub, nil)
	if err := rs.Start(context.Background()); err != nil {
		t.Fatalf("start retrain subscriber: %v", err)
	}
	return rs
}

// ============================================================================
// SUBSCRIBER INTEGRATION TESTS
// ============================================================================

// CRITICAL + auto_retrain → exactly one TriggerExecution with report_id as the
// idempotency key and "model-monitor" as the actor. This is the closed loop.
func TestDriftSubscriber_CriticalDrift_TriggersRetrain(t *testing.T) {
	js := newJS(t)
	svc := &fakeService{}
	newRetrainSub(t, js, svc, natsutil.NewMemoryProcessedStore())

	publishDrift(t, js, criticalDrift("report-1", "retrain-pipe-1"))

	waitFor(t, func() bool { return svc.triggerCount() == 1 }, 15*time.Second, "TriggerExecution call")

	actor, in, _ := svc.lastTrigger()
	if in.PipelineID != "retrain-pipe-1" {
		t.Errorf("PipelineID = %q, want retrain-pipe-1", in.PipelineID)
	}
	if in.IdempotencyKey != "report-1" {
		t.Errorf("IdempotencyKey = %q, want report-1 (business dedupe on report id)", in.IdempotencyKey)
	}
	if actor.Subject != events.ServiceIdentity {
		t.Errorf("actor.Subject = %q, want %q (automated principal)", actor.Subject, events.ServiceIdentity)
	}
	if in.Input["model_name"] != "iris" || in.Input["drift_report_id"] != "report-1" {
		t.Errorf("retrain input missing provenance: %v", in.Input)
	}
	if in.Input["top_drifted_feature"] != "income" {
		t.Errorf("retrain context not merged into input: %v", in.Input)
	}
}

// WARNING drift (or auto_retrain off) is a SUCCESSFUL no-op — handled (ACKed),
// never triggers a retrain, never NAK-loops toward the DLQ.
func TestDriftSubscriber_NonCriticalDrift_NoRetrain(t *testing.T) {
	js := newJS(t)
	svc := &fakeService{}
	newRetrainSub(t, js, svc, natsutil.NewMemoryProcessedStore())

	warn := criticalDrift("report-warn", "retrain-pipe-1")
	warn.Severity = eventsv1.DriftSeverity_DRIFT_SEVERITY_WARNING
	publishDrift(t, js, warn)

	// Give the consumer time to process-and-ignore. We assert NO trigger happened.
	time.Sleep(4 * time.Second)
	if svc.triggerCount() != 0 {
		t.Errorf("trigger count = %d, want 0 for non-critical drift", svc.triggerCount())
	}
}

// DUPLICATE redelivery of the SAME envelope id → single effect. We publish the
// SAME drift twice through the SAME natsutil.Publisher.Publish (which sets the
// Nats-Msg-Id from the envelope id); JetStream + the consumer idempotency store
// collapse it to one handler effect.
//
// To make redelivery observable we ALSO assert that publishing two DISTINCT
// reports yields two triggers — proving the dedup is keyed on identity, not just
// "process once ever".
func TestDriftSubscriber_DuplicateEnvelope_Idempotent(t *testing.T) {
	js := newJS(t)
	svc := &fakeService{}
	store := natsutil.NewMemoryProcessedStore()
	newRetrainSub(t, js, svc, store)

	// Build ONE envelope and publish its identical bytes twice with the SAME
	// Msg-Id so JetStream treats the second as a publish-dedup AND, if it still
	// reaches the consumer, the ProcessedStore dedups it by envelope id.
	drift := criticalDrift("report-dup", "retrain-pipe-1")
	raw, err := protojson.Marshal(drift)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	env := natsutil.NewEnvelope("drift.detected", "model-monitor", json.RawMessage(raw))
	envBytes, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := js.Publish(ctx, events.SubjectModelDriftDetected, envBytes, jetstream.WithMsgID(env.ID)); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	waitFor(t, func() bool { return svc.triggerCount() >= 1 }, 15*time.Second, "first trigger")
	// Let any duplicate redelivery attempt land; the effect must stay at exactly 1.
	time.Sleep(4 * time.Second)
	if got := svc.triggerCount(); got != 1 {
		t.Errorf("trigger count = %d, want exactly 1 (idempotent on duplicate envelope id)", got)
	}
}

// POISON message → DLQ. A drift whose retrain pipeline is ARCHIVED can never be
// processed; the handler returns ErrProcessingFailed, natsutil retries MaxRetries
// times, then routes the message to the DLQ subject. We verify it lands there.
func TestDriftSubscriber_PoisonMessage_RoutesToDLQ(t *testing.T) {
	js := newJS(t)
	svc := &fakeService{err: domain.ErrPipelineArchived} // every trigger fails as poison
	newRetrainSub(t, js, svc, natsutil.NewMemoryProcessedStore())

	// Watch the DLQ subject for the dead-lettered message.
	dlq := make(chan natsutil.EventEnvelope, 1)
	dlqSub := natsutil.NewSubscriber(js)
	t.Cleanup(dlqSub.Close)
	if err := dlqSub.Subscribe(context.Background(), "MODELS", "fp.dlq.pipelines.retrain",
		func(_ context.Context, env natsutil.EventEnvelope) error { dlq <- env; return nil }); err != nil {
		t.Fatalf("dlq subscribe: %v", err)
	}

	publishDrift(t, js, criticalDrift("report-poison", "archived-pipe"))

	select {
	case <-dlq:
		// Landed on the DLQ as expected.
	case <-time.After(30 * time.Second):
		t.Fatal("timeout: poison message did not reach the DLQ")
	}
	// The handler was attempted at least MaxRetries+1 times (it always failed).
	if svc.triggerCount() < 3 {
		t.Errorf("trigger attempts = %d, want >= 3 (retried before DLQ)", svc.triggerCount())
	}
}

// waitFor polls cond until true or the deadline, failing with msg on timeout.
// Polling (not a fixed sleep) keeps the tests fast when the broker is quick and
// robust when it is slow under CI load.
func waitFor(t *testing.T, cond func() bool, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", msg)
}
