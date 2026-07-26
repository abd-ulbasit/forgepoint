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

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/model-serving/internal/domain"
)

// errTransient is a non-domain error used by the DLQ test to force the handler to
// NAK (it is NOT one of the permanent load errors, so ensureLoaded classifies it
// as transient and returns ErrProcessingFailed → NAK → retry → DLQ).
var errTransient = errors.New("transient engine failure (test)")

// ============================================================================
// TEST STRATEGY — REAL NATS JETSTREAM, FAKE DOMAIN
// ============================================================================
//
// These tests exercise the ADAPTER's real pub/sub behavior against a real NATS
// JetStream (testcontainers), with a FAKE ServingService that records calls. We
// verify the three things a consumer adapter must get right:
//
//   1. DISPATCH: an event published on the canonical subject reaches the right
//      domain reaction with the right translated input (wire → domain).
//   2. IDEMPOTENCY: the SAME envelope id delivered twice produces ONE effect
//      (consumer-side dedupe via ProcessedStore, the at-least-once safety net).
//   3. DLQ: a poison message (a payload the handler keeps NAKing) lands on the
//      DLQ subject after the retry budget, instead of looping forever.
//
// We use a real NATS (not a mock) because the behavior under test IS the broker
// interaction: durable consumers, redelivery, dedupe windows, DLQ routing. A
// mock would test our mock, not the contract.

// ----------------------------------------------------------------------------
// fakeService — a ServingService test double that records EnsureLoaded / Unload.
// ----------------------------------------------------------------------------
//
// It implements the full domain.ServingService interface (the unused control/data
// plane methods return zero values) but only the event-reaction methods carry
// behavior. EnsureLoaded/Unload record their inputs and bump a counter so a test
// can assert "called exactly once" for idempotency.
type fakeService struct {
	mu sync.Mutex

	ensureCalls []domain.LoadModelInput
	unloadCalls []unloadCall

	// loaded tracks refs currently "resident" so ListLoadedModels (used by the
	// archived handler) returns them, and Unload removes them.
	loaded map[domain.ModelRef]struct{}

	// hooks let a test inject behavior (e.g. force an error on the first call to
	// drive the DLQ path). nil hook ⇒ default success.
	ensureHook func(domain.LoadModelInput) (domain.ModelStatus, error)

	ensureCount atomic.Int64
	unloadCount atomic.Int64
}

type unloadCall struct {
	ref    domain.ModelRef
	reason string
}

func newFakeService() *fakeService {
	return &fakeService{loaded: make(map[domain.ModelRef]struct{})}
}

func (f *fakeService) EnsureLoaded(_ context.Context, input domain.LoadModelInput) (domain.ModelStatus, error) {
	f.ensureCount.Add(1)
	f.mu.Lock()
	f.ensureCalls = append(f.ensureCalls, input)
	hook := f.ensureHook
	if hook == nil {
		f.loaded[input.Ref] = struct{}{}
	}
	f.mu.Unlock()
	if hook != nil {
		return hook(input)
	}
	return domain.ModelStatus{Ref: input.Ref, State: domain.StateReady}, nil
}

func (f *fakeService) Unload(_ context.Context, ref domain.ModelRef, reason string) (domain.ModelStatus, error) {
	f.unloadCount.Add(1)
	f.mu.Lock()
	f.unloadCalls = append(f.unloadCalls, unloadCall{ref: ref, reason: reason})
	delete(f.loaded, ref)
	f.mu.Unlock()
	return domain.ModelStatus{Ref: ref, State: domain.StateUnloaded}, nil
}

func (f *fakeService) ListLoadedModels(_ context.Context, _ domain.ListOptions) ([]domain.ModelStatus, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]domain.ModelStatus, 0, len(f.loaded))
	for ref := range f.loaded {
		out = append(out, domain.ModelStatus{Ref: ref, State: domain.StateReady})
	}
	return out, "", nil
}

// ---- unused interface methods (zero-value stubs) ----
func (f *fakeService) LoadModel(context.Context, domain.LoadModelInput) (domain.ModelStatus, error) {
	return domain.ModelStatus{}, nil
}
func (f *fakeService) UnloadModel(context.Context, domain.ModelRef, string) (domain.ModelStatus, error) {
	return domain.ModelStatus{}, nil
}
func (f *fakeService) GetModelStatus(context.Context, domain.ModelRef) (domain.ModelStatus, error) {
	return domain.ModelStatus{}, nil
}
func (f *fakeService) GetModelInfo(context.Context, domain.ModelRef) (domain.LoadedModel, error) {
	return domain.LoadedModel{}, nil
}
func (f *fakeService) Predict(context.Context, domain.PredictInput) (domain.PredictResult, error) {
	return domain.PredictResult{}, nil
}
func (f *fakeService) GetServingMetrics(context.Context) domain.ServingMetrics {
	return domain.ServingMetrics{}
}
func (f *fakeService) HealthCheck(context.Context, string) (domain.HealthVerdict, domain.ModelState, time.Duration) {
	return domain.HealthUnspecified, domain.StateUnspecified, 0
}

func (f *fakeService) ensureSnapshot() []domain.LoadModelInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]domain.LoadModelInput(nil), f.ensureCalls...)
}

func (f *fakeService) unloadSnapshot() []unloadCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]unloadCall(nil), f.unloadCalls...)
}

// ----------------------------------------------------------------------------
// test harness
// ----------------------------------------------------------------------------

// harness bundles a connected JetStream, the publisher, the fake service, and the
// started adapter for a test.
type harness struct {
	js    jetstream.JetStream
	pub   *natsutil.Publisher
	fake  *fakeService
	subr  *Subscriber
	store *natsutil.MemoryProcessedStore
}

// newHarness starts a real NATS JetStream, ensures the streams, and starts the
// adapter with an idempotency store + DLQ. dlqSubject empty disables DLQ.
func newHarness(t *testing.T, dlqSubject string, maxRetries int) *harness {
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
	// A stream to capture the DLQ subject so the test can read dead letters.
	if dlqSubject != "" {
		if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     "SERVING_DLQ",
			Subjects: []string{dlqSubject},
		}); err != nil {
			t.Fatalf("create DLQ stream: %v", err)
		}
	}

	fake := newFakeService()
	store := natsutil.NewMemoryProcessedStore()

	opts := []natsutil.SubOption{
		natsutil.WithIdempotencyStore(store),
		natsutil.WithMessageTimeout(5 * time.Second),
		natsutil.WithAckWait(2 * time.Second), // short so the DLQ test redelivers fast
	}
	if dlqSubject != "" {
		opts = append(opts, natsutil.WithDLQSubject(dlqSubject), natsutil.WithMaxRetries(maxRetries))
	}

	subr := NewSubscriber(fake, js, nil, opts...)
	if err := subr.Start(ctx); err != nil {
		t.Fatalf("start subscriber: %v", err)
	}
	t.Cleanup(subr.Close)

	return &harness{
		js:    js,
		pub:   natsutil.NewPublisher(js, "test-producer"),
		fake:  fake,
		subr:  subr,
		store: store,
	}
}

// waitForEnsure polls until at least n EnsureLoaded calls have been recorded or
// the deadline elapses. Returns the snapshot. Async consumption needs a poll, not
// a fixed sleep — a sleep is either flaky (too short) or slow (too long).
func waitForEnsure(t *testing.T, f *fakeService, n int) []domain.LoadModelInput {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if calls := f.ensureSnapshot(); len(calls) >= n {
			return calls
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d EnsureLoaded calls (got %d)", n, len(f.ensureSnapshot()))
	return nil
}

func waitForUnload(t *testing.T, f *fakeService, n int) []unloadCall {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if calls := f.unloadSnapshot(); len(calls) >= n {
			return calls
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d Unload calls (got %d)", n, len(f.unloadSnapshot()))
	return nil
}

// publishEnvelope publishes a raw EventEnvelope with an explicit envelope ID and
// an explicit Nats-Msg-Id. Controlling both INDEPENDENTLY is what lets the
// idempotency test deliver the SAME envelope id TWICE (two distinct Msg-Ids so
// JetStream stores both, identical envelope.ID so the ProcessedStore dedupes).
func publishEnvelope(t *testing.T, js jetstream.JetStream, subject, eventType, envelopeID, natsMsgID string, payload any) {
	t.Helper()
	// Marshal the payload with the SAME dialect natsutil.Publisher now uses:
	// protojson for proto messages (so a hand-built envelope matches the consumer's
	// protojson decode), encoding/json otherwise. Without this, the simulated wire
	// bytes would be Go-JSON and the protojson handler would DLQ a valid event.
	var data []byte
	var err error
	if pm, ok := payload.(proto.Message); ok {
		data, err = protojson.Marshal(pm)
	} else {
		data, err = json.Marshal(payload)
	}
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	env := natsutil.EventEnvelope{
		ID:        envelopeID,
		Type:      eventType,
		Source:    "test-producer",
		Timestamp: time.Now().UTC(),
		Data:      data,
	}
	envBytes, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	if _, err := js.Publish(context.Background(), subject, envBytes, jetstream.WithMsgID(natsMsgID)); err != nil {
		t.Fatalf("publish to %s: %v", subject, err)
	}
}

// ============================================================================
// TESTS
// ============================================================================

// TestModelVersionReady_DispatchesEnsureLoaded verifies the version.ready event
// (the only load-class event carrying the artifact URI) is decoded and dispatched
// to EnsureLoaded with the URI + digest translated correctly.
func TestModelVersionReady_DispatchesEnsureLoaded(t *testing.T) {
	h := newHarness(t, "", 0)

	const (
		name   = "fraud-detector"
		ver    = "v3"
		uri    = "s3://fp-models/fraud/v3.onnx"
		digest = "sha256:deadbeef"
	)
	err := h.pub.Publish(context.Background(), subjectModelVersionReady, &eventsv1.ModelVersionReady{
		ModelName: name, Version: ver, ArtifactPath: uri, ArtifactDigest: digest, SizeBytes: 2048,
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	calls := waitForEnsure(t, h.fake, 1)
	got := calls[0]
	if got.Ref.Name != name || got.Ref.Version != ver {
		t.Errorf("ref = %+v, want {%s %s}", got.Ref, name, ver)
	}
	if got.ArtifactURI != uri {
		t.Errorf("ArtifactURI = %q, want %q", got.ArtifactURI, uri)
	}
	if got.ExpectedDigest != digest {
		t.Errorf("ExpectedDigest = %q, want %q", got.ExpectedDigest, digest)
	}
	if got.IdempotencyKey == "" {
		t.Error("IdempotencyKey should be set to the envelope id")
	}
}

// TestModelDeployed_ReconcilesFromRememberedArtifact verifies the deployed event
// (no URI in its payload) reconciles using the artifact remembered from a prior
// version.ready for the same ref.
func TestModelDeployed_ReconcilesFromRememberedArtifact(t *testing.T) {
	h := newHarness(t, "", 0)
	ctx := context.Background()

	const (
		name   = "iris"
		ver    = "v2"
		uri    = "s3://fp-models/iris/v2.onnx"
		digest = "sha256:cafe"
	)
	// 1) version.ready tells the pod the artifact location.
	if err := h.pub.Publish(ctx, subjectModelVersionReady, &eventsv1.ModelVersionReady{
		ModelName: name, Version: ver, ArtifactPath: uri, ArtifactDigest: digest,
	}); err != nil {
		t.Fatalf("publish version.ready: %v", err)
	}
	waitForEnsure(t, h.fake, 1)

	// 2) deployed (no URI) should reconcile from the remembered artifact.
	if err := h.pub.Publish(ctx, subjectModelDeployed, &eventsv1.ModelDeployed{
		ModelName: name, Version: ver, Endpoint: "iris-v2.fp-models.svc:9090", WeightBps: 1000,
	}); err != nil {
		t.Fatalf("publish deployed: %v", err)
	}

	calls := waitForEnsure(t, h.fake, 2)
	last := calls[len(calls)-1]
	if last.Ref.Name != name || last.Ref.Version != ver {
		t.Errorf("deployed ref = %+v, want {%s %s}", last.Ref, name, ver)
	}
	if last.ArtifactURI != uri {
		t.Errorf("deployed reconciled URI = %q, want remembered %q", last.ArtifactURI, uri)
	}
	if last.ExpectedDigest != digest {
		t.Errorf("deployed reconciled digest = %q, want remembered %q", last.ExpectedDigest, digest)
	}
}

// TestModelDeployed_NoRememberedArtifact_Skips verifies a deployed event for a
// model the pod never saw a version.ready for is ACKed and skipped (no load),
// because the pod cannot fabricate an artifact URI (SSRF/supply-chain guard).
func TestModelDeployed_NoRememberedArtifact_Skips(t *testing.T) {
	h := newHarness(t, "", 0)
	ctx := context.Background()

	if err := h.pub.Publish(ctx, subjectModelDeployed, &eventsv1.ModelDeployed{
		ModelName: "unknown-model", Version: "v1", Endpoint: "x:9090",
	}); err != nil {
		t.Fatalf("publish deployed: %v", err)
	}
	// Also publish a version.ready for a DIFFERENT model so we have a positive
	// signal to wait on (proving the deployed above was processed-and-skipped,
	// not merely slow).
	if err := h.pub.Publish(ctx, subjectModelVersionReady, &eventsv1.ModelVersionReady{
		ModelName: "real", Version: "v1", ArtifactPath: "s3://fp-models/real/v1.onnx",
	}); err != nil {
		t.Fatalf("publish version.ready: %v", err)
	}
	calls := waitForEnsure(t, h.fake, 1)
	for _, c := range calls {
		if c.Ref.Name == "unknown-model" {
			t.Errorf("unknown-model should have been skipped, but EnsureLoaded was called: %+v", c)
		}
	}
}

// TestModelPromoted_ReconcilesFromRememberedArtifact mirrors the deployed test
// for fp.models.promoted.
func TestModelPromoted_ReconcilesFromRememberedArtifact(t *testing.T) {
	h := newHarness(t, "", 0)
	ctx := context.Background()

	const (
		name = "churn"
		ver  = "v5"
		uri  = "s3://fp-models/churn/v5.onnx"
	)
	if err := h.pub.Publish(ctx, subjectModelVersionReady, &eventsv1.ModelVersionReady{
		ModelName: name, Version: ver, ArtifactPath: uri,
	}); err != nil {
		t.Fatalf("publish version.ready: %v", err)
	}
	waitForEnsure(t, h.fake, 1)

	if err := h.pub.Publish(ctx, subjectModelPromoted, &eventsv1.ModelPromoted{
		ModelName: name, Version: ver,
		FromStage: eventsv1.ModelStage_MODEL_STAGE_STAGING,
		ToStage:   eventsv1.ModelStage_MODEL_STAGE_PRODUCTION,
	}); err != nil {
		t.Fatalf("publish promoted: %v", err)
	}

	calls := waitForEnsure(t, h.fake, 2)
	last := calls[len(calls)-1]
	if last.Ref.Name != name || last.Ref.Version != ver || last.ArtifactURI != uri {
		t.Errorf("promoted reconcile = %+v, want ref {%s %s} uri %q", last, name, ver, uri)
	}
}

// TestModelUndeployed_DispatchesUnload verifies the undeploy event dispatches
// Unload with the reason carried on the payload.
func TestModelUndeployed_DispatchesUnload(t *testing.T) {
	h := newHarness(t, "", 0)
	ctx := context.Background()

	if err := h.pub.Publish(ctx, subjectModelUndeployed, &eventsv1.ModelUndeployed{
		ModelName: "fraud-detector", Version: "v3", Reason: "rollback",
	}); err != nil {
		t.Fatalf("publish undeployed: %v", err)
	}

	calls := waitForUnload(t, h.fake, 1)
	if calls[0].ref.Name != "fraud-detector" || calls[0].ref.Version != "v3" {
		t.Errorf("unload ref = %+v", calls[0].ref)
	}
	if calls[0].reason != "rollback" {
		t.Errorf("unload reason = %q, want %q", calls[0].reason, "rollback")
	}
}

// TestModelArchived_UnloadsResidentVersions verifies the archived event (which
// names only the MODEL) tears down whatever version of that model is resident.
func TestModelArchived_UnloadsResidentVersions(t *testing.T) {
	h := newHarness(t, "", 0)
	ctx := context.Background()

	// Load a version of the model so it is "resident" in the fake.
	if err := h.pub.Publish(ctx, subjectModelVersionReady, &eventsv1.ModelVersionReady{
		ModelName: "legacy", Version: "v1", ArtifactPath: "s3://fp-models/legacy/v1.onnx",
	}); err != nil {
		t.Fatalf("publish version.ready: %v", err)
	}
	waitForEnsure(t, h.fake, 1)

	if err := h.pub.Publish(ctx, subjectModelArchived, &eventsv1.ModelArchived{
		ModelName: "legacy", ArchivedBy: "admin",
	}); err != nil {
		t.Fatalf("publish archived: %v", err)
	}

	calls := waitForUnload(t, h.fake, 1)
	if calls[0].ref.Name != "legacy" || calls[0].ref.Version != "v1" {
		t.Errorf("archived unload ref = %+v, want {legacy v1}", calls[0].ref)
	}
	if calls[0].reason != "archived" {
		t.Errorf("archived unload reason = %q, want %q", calls[0].reason, "archived")
	}
}

// TestIdempotentRedelivery verifies the SAME envelope id delivered TWICE produces
// exactly ONE domain effect. We publish two messages with DISTINCT Nats-Msg-Ids
// (so JetStream stores and delivers both) but the SAME envelope.ID (so the
// consumer-side ProcessedStore dedupes the second). This is the at-least-once
// safety net the contract requires.
func TestIdempotentRedelivery(t *testing.T) {
	h := newHarness(t, "", 0)

	envID := uuid.NewString()
	payload := &eventsv1.ModelVersionReady{
		ModelName: "dedupe-me", Version: "v1", ArtifactPath: "s3://fp-models/dedupe/v1.onnx",
	}
	// Two deliveries, same envelope id, different transport Msg-Ids.
	publishEnvelope(t, h.js, subjectModelVersionReady, "version.ready", envID, "msg-A", payload)
	publishEnvelope(t, h.js, subjectModelVersionReady, "version.ready", envID, "msg-B", payload)

	// Wait for the first to be processed.
	waitForEnsure(t, h.fake, 1)

	// Give the second delivery ample time to (not) re-run the handler.
	time.Sleep(1500 * time.Millisecond)

	if got := h.fake.ensureCount.Load(); got != 1 {
		t.Errorf("EnsureLoaded called %d times for a duplicate envelope id; want exactly 1", got)
	}
}

// TestPoisonMessageRoutedToDLQ verifies a message whose handler keeps failing is
// routed to the DLQ after the retry budget, rather than looping forever.
//
// We force EnsureLoaded to return a TRANSIENT error (so the handler NAKs every
// time). With MaxRetries=2 (3 total deliveries) and a short AckWait, the message
// reaches the DLQ subject, which we consume to confirm.
func TestPoisonMessageRoutedToDLQ(t *testing.T) {
	const dlqSubject = "fp.serving.dlq"
	h := newHarness(t, dlqSubject, 2)
	ctx := context.Background()

	// Make every EnsureLoaded fail transiently → the handler NAKs each delivery.
	h.fake.ensureHook = func(domain.LoadModelInput) (domain.ModelStatus, error) {
		return domain.ModelStatus{}, errTransient
	}

	// Consume the DLQ so we can assert the dead letter arrives.
	dlqSub := natsutil.NewSubscriber(h.js)
	t.Cleanup(dlqSub.Close)
	dead := make(chan natsutil.EventEnvelope, 1)
	if err := dlqSub.Subscribe(ctx, "SERVING_DLQ", dlqSubject, func(_ context.Context, env natsutil.EventEnvelope) error {
		select {
		case dead <- env:
		default:
		}
		return nil
	}); err != nil {
		t.Fatalf("subscribe DLQ: %v", err)
	}

	if err := h.pub.Publish(ctx, subjectModelVersionReady, &eventsv1.ModelVersionReady{
		ModelName: "poison", Version: "v1", ArtifactPath: "s3://fp-models/poison/v1.onnx",
	}); err != nil {
		t.Fatalf("publish poison: %v", err)
	}

	select {
	case env := <-dead:
		// Confirm the DLQ'd envelope is the one we sent (decode its payload).
		var p eventsv1.ModelVersionReady
		// protojson: the DLQ copy preserves the original protojson-encoded payload.
		if err := protojson.Unmarshal(env.Data, &p); err != nil {
			t.Fatalf("decode DLQ payload: %v", err)
		}
		if p.GetModelName() != "poison" {
			t.Errorf("DLQ payload model = %q, want poison", p.GetModelName())
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("poison message was not routed to DLQ within timeout (EnsureLoaded called %d times)", h.fake.ensureCount.Load())
	}
}
