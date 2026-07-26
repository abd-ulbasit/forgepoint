package events

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/domain"
)

// errAlwaysFail forces ObserveInference to fail on every delivery (drives the
// NAK → retry → DLQ path for a decodable-but-failing event).
var errAlwaysFail = errors.New("observe failed (test)")

// ============================================================================
// TEST STRATEGY — REAL NATS JETSTREAM, FAKE DOMAIN + FAKE RESOLVER
// ============================================================================
//
// These exercise the consumer half of the adapter against a real NATS JetStream
// (testcontainers) with a FAKE MonitorService (records calls) and a FAKE
// MonitorResolver (model → binding). We verify the four things a consumer adapter
// must get right:
//
//   1. DISPATCH: an event on a canonical subject reaches the right domain method
//      with the right translated input (wire → domain), with the monitor binding
//      resolved by the resolver (the authorized-binding boundary).
//   2. DUAL-FORMAT DECODE: events published the inference-gateway way (raw proto →
//      encoding/json) AND the feature-store way (protojson) both decode correctly.
//   3. IDEMPOTENCY: the SAME envelope id delivered twice → ONE domain effect.
//   4. DLQ: a poison message (undecodable payload) lands on the DLQ after the
//      retry budget instead of looping forever.

// ----------------------------------------------------------------------------
// fakeMonitorService — records ObserveInference / ResetBaselineFromPromotion.
// ----------------------------------------------------------------------------
type fakeMonitorService struct {
	mu sync.Mutex

	observed []domain.InferenceObservation
	rebased  []rebaseCall

	observeCount atomic.Int64
	rebaseCount  atomic.Int64

	// observeErr, if set, makes ObserveInference fail (drives the DLQ path).
	observeErr error
}

type rebaseCall struct {
	ownerTeam  string
	modelName  string
	newVersion string
}

func newFakeService() *fakeMonitorService { return &fakeMonitorService{} }

func (f *fakeMonitorService) ObserveInference(_ context.Context, obs domain.InferenceObservation) (*domain.DriftReport, error) {
	f.observeCount.Add(1)
	f.mu.Lock()
	f.observed = append(f.observed, obs)
	err := f.observeErr
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (f *fakeMonitorService) ResetBaselineFromPromotion(_ context.Context, ownerTeam, modelName, newProdVersion string) error {
	f.rebaseCount.Add(1)
	f.mu.Lock()
	f.rebased = append(f.rebased, rebaseCall{ownerTeam: ownerTeam, modelName: modelName, newVersion: newProdVersion})
	f.mu.Unlock()
	return nil
}

func (f *fakeMonitorService) observedSnapshot() []domain.InferenceObservation {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]domain.InferenceObservation(nil), f.observed...)
}

func (f *fakeMonitorService) rebasedSnapshot() []rebaseCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]rebaseCall(nil), f.rebased...)
}

// ---- unused control-plane methods (zero-value stubs) ----
func (f *fakeMonitorService) ConfigureMonitor(context.Context, string, domain.ConfigureMonitorInput) (domain.Monitor, bool, error) {
	return domain.Monitor{}, false, nil
}
func (f *fakeMonitorService) DeleteMonitor(context.Context, string, string, bool) (int, error) {
	return 0, nil
}
func (f *fakeMonitorService) ResetBaseline(context.Context, string, string, string) (domain.Monitor, error) {
	return domain.Monitor{}, nil
}
func (f *fakeMonitorService) GetModelHealth(context.Context, string, string) (domain.ModelHealth, error) {
	return domain.ModelHealth{}, nil
}
func (f *fakeMonitorService) GetMonitorStatus(context.Context, string, string) (domain.MonitorStatus, error) {
	return domain.MonitorStatus{}, nil
}
func (f *fakeMonitorService) ListMonitors(context.Context, string, domain.DriftSeverity, domain.MonitorState, domain.ListOptions) ([]domain.FleetEntry, string, error) {
	return nil, "", nil
}
func (f *fakeMonitorService) GetDriftReport(context.Context, string, string) (domain.DriftReport, error) {
	return domain.DriftReport{}, nil
}
func (f *fakeMonitorService) ListDriftReports(context.Context, string, domain.ReportFilter, domain.ListOptions) ([]domain.DriftReport, string, error) {
	return nil, "", nil
}
func (f *fakeMonitorService) ListEvalScores(context.Context, string, domain.EvalFilter, domain.ListOptions) ([]domain.Eval, string, error) {
	return nil, "", nil
}
func (f *fakeMonitorService) SubmitGroundTruth(context.Context, string, domain.SubmitGroundTruthInput) (domain.SubmitGroundTruthResult, error) {
	return domain.SubmitGroundTruthResult{}, nil
}

// compile-time proof the fake satisfies the driving port.
var _ domain.MonitorService = (*fakeMonitorService)(nil)

// ----------------------------------------------------------------------------
// fakeResolver — model name → binding, configurable per model.
// ----------------------------------------------------------------------------
type fakeResolver struct {
	mu       sync.Mutex
	bindings map[string]MonitorBinding
	err      error
}

func newFakeResolver() *fakeResolver {
	return &fakeResolver{bindings: make(map[string]MonitorBinding)}
}

func (r *fakeResolver) set(modelName string, b MonitorBinding) {
	r.mu.Lock()
	r.bindings[modelName] = b
	r.mu.Unlock()
}

func (r *fakeResolver) ResolveByModel(_ context.Context, modelName string) (MonitorBinding, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return MonitorBinding{}, false, r.err
	}
	b, ok := r.bindings[modelName]
	return b, ok, nil
}

var _ MonitorResolver = (*fakeResolver)(nil)

// ----------------------------------------------------------------------------
// harness
// ----------------------------------------------------------------------------
type subHarness struct {
	js       jetstream.JetStream
	pub      *natsutil.Publisher
	svc      *fakeMonitorService
	resolver *fakeResolver
	subr     *Subscriber
}

// newSubHarness starts a real NATS JetStream, ensures the streams, and starts the
// adapter with an idempotency store. dlqSubject empty disables DLQ.
func newSubHarness(t *testing.T, dlqSubject string, maxRetries int) *subHarness {
	t.Helper()
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
	if dlqSubject != "" {
		if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     "MONITOR_DLQ",
			Subjects: []string{dlqSubject},
		}); err != nil {
			t.Fatalf("create DLQ stream: %v", err)
		}
	}

	svc := newFakeService()
	resolver := newFakeResolver()

	opts := []natsutil.SubOption{
		natsutil.WithIdempotencyStore(natsutil.NewMemoryProcessedStore()),
		natsutil.WithMessageTimeout(5 * time.Second),
		natsutil.WithAckWait(2 * time.Second), // short so the DLQ test redelivers fast
	}
	if dlqSubject != "" {
		opts = append(opts, natsutil.WithDLQSubject(dlqSubject), natsutil.WithMaxRetries(maxRetries))
	}

	subr := NewSubscriber(svc, resolver, js, nil, opts...)
	if err := subr.Start(ctx); err != nil {
		t.Fatalf("start subscriber: %v", err)
	}
	t.Cleanup(subr.Close)

	return &subHarness{
		js:       js,
		pub:      natsutil.NewPublisher(js, "inference-gateway"),
		svc:      svc,
		resolver: resolver,
		subr:     subr,
	}
}

func waitForObserve(t *testing.T, f *fakeMonitorService, n int) []domain.InferenceObservation {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if calls := f.observedSnapshot(); len(calls) >= n {
			return calls
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d ObserveInference calls (got %d)", n, len(f.observedSnapshot()))
	return nil
}

func waitForRebase(t *testing.T, f *fakeMonitorService, n int) []rebaseCall {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if calls := f.rebasedSnapshot(); len(calls) >= n {
			return calls
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d ResetBaselineFromPromotion calls (got %d)", n, len(f.rebasedSnapshot()))
	return nil
}

// publishRawProto publishes a payload the inference-gateway way: the RAW generated
// proto handed to natsutil.Publisher, which json.Marshal's it (encoding/json wire
// form). This is what the gateway/registry actually emit.
func publishRawProto(t *testing.T, pub *natsutil.Publisher, subject string, msg proto.Message) {
	t.Helper()
	if err := pub.Publish(context.Background(), subject, msg); err != nil {
		t.Fatalf("publish raw-proto to %s: %v", subject, err)
	}
}

// publishProtojson publishes a payload the feature-store way: protojson bytes spliced
// as a json.RawMessage. This is the canonical proto-JSON wire form.
func publishProtojson(t *testing.T, pub *natsutil.Publisher, subject string, msg proto.Message) {
	t.Helper()
	raw, err := protojson.Marshal(msg)
	if err != nil {
		t.Fatalf("protojson marshal: %v", err)
	}
	if err := pub.Publish(context.Background(), subject, json.RawMessage(raw)); err != nil {
		t.Fatalf("publish protojson to %s: %v", subject, err)
	}
}

// publishEnvelope publishes a raw EventEnvelope with an explicit envelope ID and an
// explicit Nats-Msg-Id — controlling both INDEPENDENTLY is what lets the idempotency
// test deliver the SAME envelope id TWICE (two Msg-Ids so JetStream stores both,
// identical envelope.ID so the ProcessedStore dedupes the second).
func publishEnvelope(t *testing.T, js jetstream.JetStream, subject, eventType, envelopeID, natsMsgID string, payloadJSON json.RawMessage) {
	t.Helper()
	env := natsutil.EventEnvelope{
		ID:        envelopeID,
		Type:      eventType,
		Source:    "inference-gateway",
		Timestamp: time.Now().UTC(),
		Data:      payloadJSON,
	}
	envBytes, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	if _, err := js.Publish(context.Background(), subject, envBytes, jetstream.WithMsgID(natsMsgID)); err != nil {
		t.Fatalf("publish envelope to %s: %v", subject, err)
	}
}

// ============================================================================
// TESTS
// ============================================================================

// TestInferenceCompleted_EncodingJSON_DispatchesObserve verifies the inference-
// gateway wire form (raw proto → encoding/json) decodes and dispatches to
// ObserveInference with the resolved binding + translated summaries.
func TestInferenceCompleted_EncodingJSON_DispatchesObserve(t *testing.T) {
	h := newSubHarness(t, "", 0)
	h.resolver.set("fraud-detector", MonitorBinding{MonitorID: "mon-1", OwnerTeam: "team-fraud"})

	payload := &eventsv1.InferenceCompleted{
		RequestId: "req-123",
		ModelName: "fraud-detector",
		Version:   "v7",
		ApiKeyId:  "key-abc",
		Latency:   durationpb.New(12 * time.Millisecond),
		FeatureSummary: &eventsv1.FeatureSummary{
			FeatureValues: map[string]float64{"income": 7.8, "age": 41},
			FeatureCount:  2,
		},
		PredictionSummary: &eventsv1.PredictionSummary{TopLabel: "fraud", TopScore: 0.91},
		CompletedAt:       timestamppb.New(time.Date(2026, 6, 18, 3, 0, 0, 0, time.UTC)),
	}
	publishRawProto(t, h.pub, SubjectInferenceCompleted, payload)

	calls := waitForObserve(t, h.svc, 1)
	obs := calls[0]
	if obs.MonitorID != "mon-1" || obs.OwnerTeam != "team-fraud" {
		t.Errorf("binding = %q/%q, want mon-1/team-fraud", obs.MonitorID, obs.OwnerTeam)
	}
	if obs.RequestID != "req-123" || obs.ModelName != "fraud-detector" || obs.ModelVersion != "v7" {
		t.Errorf("obs identity = %+v", obs)
	}
	if obs.PredictedTop != "fraud" {
		t.Errorf("PredictedTop = %q, want fraud", obs.PredictedTop)
	}
	if obs.Features["income"] != 7.8 || obs.Features["age"] != 41 {
		t.Errorf("Features = %v, want income=7.8 age=41", obs.Features)
	}
	if obs.ObservedAt.IsZero() {
		t.Error("ObservedAt should be the event's completed_at, not zero")
	}
}

// TestInferenceCompleted_Protojson_DispatchesObserve verifies the SAME event in the
// canonical protojson wire form (the feature-store/orchestrator style) also decodes
// — proving the dual-format decoder handles both producer encodings.
func TestInferenceCompleted_Protojson_DispatchesObserve(t *testing.T) {
	h := newSubHarness(t, "", 0)
	h.resolver.set("churn", MonitorBinding{MonitorID: "mon-2", OwnerTeam: "team-growth"})

	payload := &eventsv1.InferenceCompleted{
		RequestId:      "req-xyz",
		ModelName:      "churn",
		Version:        "v3",
		FeatureSummary: &eventsv1.FeatureSummary{FeatureValues: map[string]float64{"tenure": 4.2}},
		CompletedAt:    timestamppb.New(time.Date(2026, 6, 18, 4, 0, 0, 0, time.UTC)),
	}
	publishProtojson(t, h.pub, SubjectInferenceCompleted, payload)

	calls := waitForObserve(t, h.svc, 1)
	obs := calls[0]
	if obs.MonitorID != "mon-2" || obs.OwnerTeam != "team-growth" {
		t.Errorf("binding = %q/%q, want mon-2/team-growth", obs.MonitorID, obs.OwnerTeam)
	}
	if obs.ModelName != "churn" || obs.ModelVersion != "v3" || obs.Features["tenure"] != 4.2 {
		t.Errorf("protojson decode lost fields: %+v", obs)
	}
}

// TestInferenceCompleted_UnmonitoredModel_Skips verifies an event for a model with
// no monitor binding is ACKed and dropped (never folded — the safe default that
// avoids fabricating a tenant / leaking memory on unmonitored traffic).
func TestInferenceCompleted_UnmonitoredModel_Skips(t *testing.T) {
	h := newSubHarness(t, "", 0)
	// resolver has NO binding for "ghost"; it DOES for "real".
	h.resolver.set("real", MonitorBinding{MonitorID: "mon-real", OwnerTeam: "team-real"})

	publishRawProto(t, h.pub, SubjectInferenceCompleted, &eventsv1.InferenceCompleted{
		RequestId: "g1", ModelName: "ghost", Version: "v1",
	})
	// A positive signal to wait on, proving the ghost event was processed-and-skipped.
	publishRawProto(t, h.pub, SubjectInferenceCompleted, &eventsv1.InferenceCompleted{
		RequestId: "r1", ModelName: "real", Version: "v1",
	})

	calls := waitForObserve(t, h.svc, 1)
	for _, c := range calls {
		if c.ModelName == "ghost" {
			t.Errorf("ghost (unmonitored) should have been skipped, got %+v", c)
		}
	}
}

// TestInferenceFailed_DispatchesObserve verifies a failure event folds a sample
// (no feature/prediction signal) with the version attributed.
func TestInferenceFailed_DispatchesObserve(t *testing.T) {
	h := newSubHarness(t, "", 0)
	h.resolver.set("fraud-detector", MonitorBinding{MonitorID: "mon-1", OwnerTeam: "team-fraud"})

	publishRawProto(t, h.pub, SubjectInferenceFailed, &eventsv1.InferenceFailed{
		RequestId: "req-fail-1",
		ModelName: "fraud-detector",
		Version:   "v7",
		Reason:    eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_TIMEOUT,
		FailedAt:  timestamppb.New(time.Date(2026, 6, 18, 5, 0, 0, 0, time.UTC)),
	})

	calls := waitForObserve(t, h.svc, 1)
	obs := calls[0]
	if obs.MonitorID != "mon-1" || obs.ModelVersion != "v7" || obs.RequestID != "req-fail-1" {
		t.Errorf("failed-call obs = %+v", obs)
	}
	if len(obs.Features) != 0 || obs.PredictedTop != "" {
		t.Errorf("a failure should carry no feature/prediction signal: %+v", obs)
	}
}

// TestModelPromoted_ToProduction_RebasesBaseline verifies a promotion to PRODUCTION
// re-pins the baseline via the resolved owner team.
func TestModelPromoted_ToProduction_RebasesBaseline(t *testing.T) {
	h := newSubHarness(t, "", 0)
	h.resolver.set("fraud-detector", MonitorBinding{MonitorID: "mon-1", OwnerTeam: "team-fraud"})

	// registry emits ModelPromoted the raw-proto (encoding/json) way.
	publishRawProto(t, h.pub, SubjectModelPromoted, &eventsv1.ModelPromoted{
		ModelName: "fraud-detector",
		Version:   "v8",
		FromStage: eventsv1.ModelStage_MODEL_STAGE_STAGING,
		ToStage:   eventsv1.ModelStage_MODEL_STAGE_PRODUCTION,
	})

	calls := waitForRebase(t, h.svc, 1)
	c := calls[0]
	if c.ownerTeam != "team-fraud" || c.modelName != "fraud-detector" || c.newVersion != "v8" {
		t.Errorf("rebase call = %+v, want team-fraud/fraud-detector/v8", c)
	}
}

// TestModelPromoted_NonProduction_Ignored verifies a non-PRODUCTION promotion does
// NOT re-baseline (the served baseline only moves on the production transition).
func TestModelPromoted_NonProduction_Ignored(t *testing.T) {
	h := newSubHarness(t, "", 0)
	h.resolver.set("fraud-detector", MonitorBinding{MonitorID: "mon-1", OwnerTeam: "team-fraud"})

	// A DEV→STAGING promotion (not to PRODUCTION) — must be ignored for baseline.
	publishRawProto(t, h.pub, SubjectModelPromoted, &eventsv1.ModelPromoted{
		ModelName: "fraud-detector", Version: "v9",
		FromStage: eventsv1.ModelStage_MODEL_STAGE_DEV,
		ToStage:   eventsv1.ModelStage_MODEL_STAGE_STAGING,
	})
	// A production promotion for a DIFFERENT version as a positive signal to wait on.
	publishRawProto(t, h.pub, SubjectModelPromoted, &eventsv1.ModelPromoted{
		ModelName: "fraud-detector", Version: "v10",
		ToStage: eventsv1.ModelStage_MODEL_STAGE_PRODUCTION,
	})

	calls := waitForRebase(t, h.svc, 1)
	for _, c := range calls {
		if c.newVersion == "v9" {
			t.Errorf("non-production promotion v9 should have been ignored, got %+v", c)
		}
	}
}

// TestFeaturesWritten_Protojson_Acked verifies the feature-store thin event
// (protojson wire form) is decoded and ACKed without error (no domain state change
// — it is a scoping/observability signal). We assert no crash + that a subsequent
// inference event is still processed (proving the consume loop stayed healthy).
func TestFeaturesWritten_Protojson_Acked(t *testing.T) {
	h := newSubHarness(t, "", 0)
	h.resolver.set("fraud-detector", MonitorBinding{MonitorID: "mon-1", OwnerTeam: "team-fraud"})

	// feature-store emits FeaturesWritten the protojson way.
	publishProtojson(t, h.pub, SubjectFeaturesWritten, &eventsv1.FeaturesWritten{
		FeatureViewId:         "fv-1",
		FeatureViewName:       "fraud_features",
		EntityIds:             []string{"e1", "e2"},
		WrittenCount:          2,
		WrittenThroughVersion: 42,
		WrittenAt:             timestamppb.Now(),
	})
	// Positive signal: an inference event after it should still be observed.
	publishRawProto(t, h.pub, SubjectInferenceCompleted, &eventsv1.InferenceCompleted{
		RequestId: "after-features", ModelName: "fraud-detector", Version: "v7",
	})

	calls := waitForObserve(t, h.svc, 1)
	if calls[0].RequestID != "after-features" {
		t.Errorf("expected the post-FeaturesWritten inference to be observed, got %+v", calls[0])
	}
}

// TestIdempotentRedelivery verifies the SAME envelope id delivered TWICE produces
// exactly ONE ObserveInference. Two messages, same envelope.ID, distinct
// Nats-Msg-Ids (so JetStream stores+delivers both) — the consumer-side
// ProcessedStore dedupes the second.
func TestIdempotentRedelivery(t *testing.T) {
	h := newSubHarness(t, "", 0)
	h.resolver.set("dedupe-model", MonitorBinding{MonitorID: "mon-d", OwnerTeam: "team-d"})

	// Build the encoding/json payload bytes the gateway way (raw proto → json.Marshal).
	payload := &eventsv1.InferenceCompleted{
		RequestId: "dup-req", ModelName: "dedupe-model", Version: "v1",
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	envID := uuid.NewString()
	publishEnvelope(t, h.js, SubjectInferenceCompleted, "completed", envID, "msg-A", raw)
	publishEnvelope(t, h.js, SubjectInferenceCompleted, "completed", envID, "msg-B", raw)

	// Wait for the first to be processed, then give the second ample time to (not) run.
	waitForObserve(t, h.svc, 1)
	time.Sleep(1500 * time.Millisecond)

	if got := h.svc.observeCount.Load(); got != 1 {
		t.Errorf("ObserveInference called %d times for a duplicate envelope id; want exactly 1", got)
	}
}

// TestPoisonMessageRoutedToDLQ verifies an UNDECODABLE payload (garbage bytes that
// neither protojson nor encoding/json can parse into the message) is routed to the
// DLQ after the retry budget instead of looping forever.
func TestPoisonMessageRoutedToDLQ(t *testing.T) {
	const dlqSubject = "fp.monitor.dlq"
	h := newSubHarness(t, dlqSubject, 2)
	ctx := context.Background()

	// Consume the DLQ so we can assert the dead letter arrives.
	dlqSub := natsutil.NewSubscriber(h.js)
	t.Cleanup(dlqSub.Close)
	dead := make(chan natsutil.EventEnvelope, 1)
	if err := dlqSub.Subscribe(ctx, "MONITOR_DLQ", dlqSubject, func(_ context.Context, env natsutil.EventEnvelope) error {
		select {
		case dead <- env:
		default:
		}
		return nil
	}); err != nil {
		t.Fatalf("subscribe DLQ: %v", err)
	}

	// A payload that is valid JSON but NOT a decodable InferenceCompleted: a JSON
	// array where the decoders expect an object. protojson fails AND encoding/json
	// into a struct fails → poison.
	poison := json.RawMessage(`[1,2,3]`)
	publishEnvelope(t, h.js, SubjectInferenceCompleted, "completed", uuid.NewString(), "poison-msg", poison)

	select {
	case env := <-dead:
		if env.Type != "completed" {
			t.Errorf("DLQ envelope type = %q, want completed", env.Type)
		}
	case <-time.After(25 * time.Second):
		t.Fatalf("poison message was not routed to DLQ within timeout (observe called %d)", h.svc.observeCount.Load())
	}
}

// TestObserveError_RetriesThenDLQ verifies a TRANSIENT domain error (ObserveInference
// keeps failing) flows NAK → retry → DLQ, proving the consumer's error
// classification routes a persistently-failing-but-decodable event to the DLQ too.
func TestObserveError_RetriesThenDLQ(t *testing.T) {
	const dlqSubject = "fp.monitor.dlq2"
	h := newSubHarness(t, dlqSubject, 2)
	ctx := context.Background()

	// Make every ObserveInference fail → the handler NAKs each delivery.
	h.svc.mu.Lock()
	h.svc.observeErr = errAlwaysFail
	h.svc.mu.Unlock()
	h.resolver.set("flaky", MonitorBinding{MonitorID: "mon-f", OwnerTeam: "team-f"})

	dlqSub := natsutil.NewSubscriber(h.js)
	t.Cleanup(dlqSub.Close)
	dead := make(chan natsutil.EventEnvelope, 1)
	if err := dlqSub.Subscribe(ctx, "MONITOR_DLQ", dlqSubject, func(_ context.Context, env natsutil.EventEnvelope) error {
		select {
		case dead <- env:
		default:
		}
		return nil
	}); err != nil {
		t.Fatalf("subscribe DLQ: %v", err)
	}

	publishRawProto(t, h.pub, SubjectInferenceCompleted, &eventsv1.InferenceCompleted{
		RequestId: "flaky-1", ModelName: "flaky", Version: "v1",
	})

	select {
	case <-dead:
		// reached the DLQ after the retry budget — success.
	case <-time.After(25 * time.Second):
		t.Fatalf("failing observe was not routed to DLQ (observe called %d)", h.svc.observeCount.Load())
	}
}
