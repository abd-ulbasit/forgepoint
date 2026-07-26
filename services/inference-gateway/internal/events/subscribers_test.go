package events_test

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/inference-gateway/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/inference-gateway/internal/events"
)

// ============================================================================
// SUBSCRIBER TESTS — verify each consumed event is decoded and DISPATCHED to the
// right domain reaction, that DUPLICATE redelivery is idempotent (single effect),
// and that a poison message is routed to the DLQ. All over REAL NATS JetStream.
// ============================================================================

// TestSubscribers_DispatchModelLifecycle publishes one of each consumed
// lifecycle/quota event and asserts the corresponding domain method (or the quota
// writer) was called with the mapped payload.
func TestSubscribers_DispatchModelLifecycle(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	js, _ := newJetStream(t)
	// The gateway consumes across three streams.
	mustCreateStream(t, js, events.StreamPipelines, "fp.pipelines.>")
	mustCreateStream(t, js, events.StreamModels, "fp.models.>")
	mustCreateStream(t, js, events.StreamBilling, "fp.billing.>")
	// The DLQ subject must live in a stream too, or DLQ publishes are lost.
	mustCreateStream(t, js, "DLQ", "fp.dlq.>")

	svc := newFakeService()
	quota := newFakeQuota()
	subs := events.NewSubscribers(js,
		events.SubscriberDeps{Service: svc, QuotaWriter: quota},
		natsutil.NewMemoryProcessedStore(),
		events.SubConfig{QuotaBlockTTL: 30 * time.Minute},
	)
	t.Cleanup(subs.Close)
	if err := subs.Register(context.Background()); err != nil {
		t.Fatalf("Register: %v", err)
	}

	pub := natsutil.NewPublisher(js, "pipeline-orchestrator")

	// --- ModelDeployed → ApplyModelDeployed ---------------------------------
	if err := pub.Publish(context.Background(), events.SubjectModelDeployed, &eventsv1.ModelDeployed{
		ModelName: "iris", Version: "v2", Endpoint: "iris-v2.fp-models.svc:9090", WeightBps: 1000,
	}); err != nil {
		t.Fatalf("publish deployed: %v", err)
	}
	dep := waitFor(t, svc.deployed)
	if dep.ModelName != "iris" || dep.Version != "v2" || dep.Endpoint != "iris-v2.fp-models.svc:9090" || dep.WeightBps != 1000 {
		t.Errorf("ApplyModelDeployed got %+v", dep)
	}

	// --- ModelUndeployed → ApplyModelUndeployed -----------------------------
	if err := pub.Publish(context.Background(), events.SubjectModelUndeployed, &eventsv1.ModelUndeployed{
		ModelName: "iris", Version: "v1", Reason: "superseded",
	}); err != nil {
		t.Fatalf("publish undeployed: %v", err)
	}
	und := waitFor(t, svc.undeployed)
	if und.ModelName != "iris" || und.Version != "v1" || und.Reason != "superseded" {
		t.Errorf("ApplyModelUndeployed got %+v", und)
	}

	// --- ModelPromoted → ApplyModelPromoted ---------------------------------
	if err := pub.Publish(context.Background(), events.SubjectModelPromoted, &eventsv1.ModelPromoted{
		ModelName: "iris", Version: "v2", DemotedVersion: "v1",
	}); err != nil {
		t.Fatalf("publish promoted: %v", err)
	}
	prom := waitFor(t, svc.promoted)
	if prom.ModelName != "iris" || prom.Version != "v2" || prom.DemotedVersion != "v1" {
		t.Errorf("ApplyModelPromoted got %+v", prom)
	}

	// --- ModelArchived → ApplyModelArchived ---------------------------------
	if err := pub.Publish(context.Background(), events.SubjectModelArchived, &eventsv1.ModelArchived{
		ModelName: "iris",
	}); err != nil {
		t.Fatalf("publish archived: %v", err)
	}
	arch := waitFor(t, svc.archived)
	if arch.ModelName != "iris" {
		t.Errorf("ApplyModelArchived got %+v", arch)
	}

	// --- QuotaExceeded → QuotaWriter.Block ----------------------------------
	if err := pub.Publish(context.Background(), events.SubjectQuotaExceeded, &eventsv1.QuotaExceeded{
		Team: "acme", QuotaLimit: 1000, CurrentUsage: 1012,
	}); err != nil {
		t.Fatalf("publish quota: %v", err)
	}
	blk := waitFor(t, quota.blocked)
	if blk.team != "acme" {
		t.Errorf("Block team = %q, want acme", blk.team)
	}
	if blk.ttl != 30*time.Minute {
		t.Errorf("Block ttl = %v, want 30m (from SubConfig)", blk.ttl)
	}
}

// TestSubscribers_IdempotentRedelivery proves a DUPLICATE delivery of the SAME
// envelope id produces a SINGLE domain effect. We publish two physically distinct
// JetStream messages that carry the SAME EventEnvelope.id; the consumer-side
// ProcessedStore must recognize the second as already-handled and skip the
// handler. Without idempotency this would dispatch ApplyModelArchived twice.
//
// WHY this construction: natsutil.Publisher mints a fresh envelope id per call,
// so to exercise duplicate-by-id we hand-build one envelope and raw-publish it
// twice with DIFFERENT Nats-Msg-Ids (so the BROKER stores both and delivers both
// — defeating JetStream's publish-window dedup) — isolating the CONSUMER-side
// dedupe as the thing under test.
func TestSubscribers_IdempotentRedelivery(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	js, _ := newJetStream(t)
	mustCreateStream(t, js, events.StreamModels, "fp.models.>")
	mustCreateStream(t, js, events.StreamPipelines, "fp.pipelines.>")
	mustCreateStream(t, js, events.StreamBilling, "fp.billing.>")
	mustCreateStream(t, js, "DLQ", "fp.dlq.>")

	svc := newFakeService()
	quota := newFakeQuota()
	subs := events.NewSubscribers(js,
		events.SubscriberDeps{Service: svc, QuotaWriter: quota},
		natsutil.NewMemoryProcessedStore(),
		events.SubConfig{},
	)
	t.Cleanup(subs.Close)
	if err := subs.Register(context.Background()); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Build ONE envelope (one id) carrying a ModelArchived payload. protojson, not
	// encoding/json: the consumer decodes via decodeData (protojson), so the
	// simulated wire bytes must be canonical proto-JSON to match.
	dupID := uuid.NewString()
	data, err := protojson.Marshal(&eventsv1.ModelArchived{ModelName: "dup-model"})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	env := natsutil.EventEnvelope{
		ID:        dupID,
		Type:      "archived",
		Source:    "registry",
		Timestamp: time.Now().UTC(),
		Data:      data,
	}
	envBytes, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	// Raw-publish the SAME envelope bytes twice, each with a distinct broker
	// Msg-Id so JetStream does NOT collapse them — two real deliveries.
	for i := range 2 {
		if _, err := js.Publish(context.Background(), events.SubjectModelArchived, envBytes,
			jetstream.WithMsgID(dupID+"-delivery-"+string(rune('a'+i)))); err != nil {
			t.Fatalf("raw publish %d: %v", i, err)
		}
	}

	// First delivery must dispatch once.
	first := waitFor(t, svc.archived)
	if first.ModelName != "dup-model" {
		t.Errorf("first dispatch got %+v", first)
	}

	// The second delivery must be deduped (no second dispatch). Give the consumer
	// ample time to have processed+ACKed the duplicate, then assert exactly one
	// effect total.
	select {
	case extra := <-svc.archived:
		t.Fatalf("duplicate redelivery produced a SECOND dispatch: %+v (idempotency broken)", extra)
	case <-time.After(3 * time.Second):
		// no second dispatch — correct.
	}
	if n := svc.archivedCount.Load(); n != 1 {
		t.Errorf("ApplyModelArchived called %d times, want exactly 1", n)
	}
}

// TestSubscribers_PoisonMessageGoesToDLQ proves a payload that can never be
// processed is routed to the DLQ after the retry budget, instead of looping
// forever. We send a QuotaExceeded with an EMPTY team — the handler treats that
// as poison (wraps ErrProcessingFailed), so after MaxRetries+1 attempts the
// message lands on the configured DLQ subject.
func TestSubscribers_PoisonMessageGoesToDLQ(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	js, _ := newJetStream(t)
	mustCreateStream(t, js, events.StreamPipelines, "fp.pipelines.>")
	mustCreateStream(t, js, events.StreamModels, "fp.models.>")
	mustCreateStream(t, js, events.StreamBilling, "fp.billing.>")
	mustCreateStream(t, js, "DLQ", "fp.dlq.>")

	svc := newFakeService()
	quota := newFakeQuota()
	dlqSubject := "fp.dlq.inference-gateway"
	subs := events.NewSubscribers(js,
		events.SubscriberDeps{Service: svc, QuotaWriter: quota},
		natsutil.NewMemoryProcessedStore(),
		events.SubConfig{MaxRetries: 2, DLQSubject: dlqSubject, MessageTimeout: 2 * time.Second},
	)
	t.Cleanup(subs.Close)
	if err := subs.Register(context.Background()); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Watch the DLQ subject.
	dlq := make(chan natsutil.EventEnvelope, 1)
	dlqSub := natsutil.NewSubscriber(js)
	t.Cleanup(dlqSub.Close)
	if err := dlqSub.Subscribe(context.Background(), "DLQ", dlqSubject,
		func(_ context.Context, env natsutil.EventEnvelope) error {
			dlq <- env
			return nil
		}); err != nil {
		t.Fatalf("DLQ subscribe: %v", err)
	}

	// Poison: empty team. The handler rejects it as unprocessable on every attempt.
	pub := natsutil.NewPublisher(js, "billing")
	if err := pub.Publish(context.Background(), events.SubjectQuotaExceeded, &eventsv1.QuotaExceeded{
		Team: "", QuotaLimit: 1, CurrentUsage: 2,
	}); err != nil {
		t.Fatalf("publish poison: %v", err)
	}

	select {
	case env := <-dlq:
		// The DLQ message preserves the original envelope; the payload still decodes.
		// protojson, matching the canonical proto-JSON the publisher now emits.
		var p eventsv1.QuotaExceeded
		if err := protojson.Unmarshal(env.Data, &p); err != nil {
			t.Fatalf("decode DLQ payload: %v", err)
		}
		if p.GetTeam() != "" {
			t.Errorf("DLQ payload team = %q, want empty (the poison we sent)", p.GetTeam())
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timeout: poison message never reached the DLQ")
	}

	// The quota writer must NEVER have been called for the poison event.
	if n := quota.blockCount.Load(); n != 0 {
		t.Errorf("QuotaWriter.Block called %d times for a poison event, want 0", n)
	}
}

// TestPublishConsumeRoundTrip is an end-to-end loop: the gateway's OWN publisher
// emits InferenceCompleted and a raw consumer reads it back — proving producer and
// the canonical wire contract agree (a mini producer/consumer contract test).
func TestPublishConsumeRoundTrip(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	js, _ := newJetStream(t)
	mustCreateStream(t, js, events.StreamInference, "fp.inference.>")

	pub := events.NewPublisher(natsutil.NewPublisher(js, events.Source))

	got := make(chan *eventsv1.InferenceCompleted, 1)
	raw := natsutil.NewSubscriber(js, natsutil.WithIdempotencyStore(natsutil.NewMemoryProcessedStore()))
	t.Cleanup(raw.Close)
	if err := raw.Subscribe(context.Background(), events.StreamInference, events.SubjectInferenceCompleted,
		func(_ context.Context, env natsutil.EventEnvelope) error {
			var p eventsv1.InferenceCompleted
			if err := protojson.Unmarshal(env.Data, &p); err != nil {
				return err
			}
			got <- &p
			return nil
		}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err := pub.PublishCompleted(context.Background(), domain.InferenceCompleted{
		RequestID: "rt-1", ModelName: "m", Version: "v1", APIKeyID: "k",
		Latency: 5 * time.Millisecond, CompletedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("PublishCompleted: %v", err)
	}

	select {
	case p := <-got:
		if p.GetRequestId() != "rt-1" {
			t.Errorf("RequestId = %q, want rt-1", p.GetRequestId())
		}
		if p.GetLatency().AsDuration() != 5*time.Millisecond {
			t.Errorf("Latency = %v, want 5ms", p.GetLatency().AsDuration())
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for round-trip event")
	}
}

// ----------------------------------------------------------------------------
// FAKES — record dispatches so tests assert the consumer wired to the right
// domain method with the mapped payload.
// ----------------------------------------------------------------------------

// fakeService implements domain.InferenceService. Only the EVENT-REACTION
// methods record (those are what the subscribers call); the rest satisfy the
// interface and panic if unexpectedly invoked (they are not exercised here).
type fakeService struct {
	deployed   chan domain.ModelDeployed
	undeployed chan domain.ModelUndeployed
	promoted   chan domain.ModelPromoted
	archived   chan domain.ModelArchived

	archivedCount atomic.Int32
}

func newFakeService() *fakeService {
	return &fakeService{
		deployed:   make(chan domain.ModelDeployed, 4),
		undeployed: make(chan domain.ModelUndeployed, 4),
		promoted:   make(chan domain.ModelPromoted, 4),
		archived:   make(chan domain.ModelArchived, 4),
	}
}

func (f *fakeService) ApplyModelDeployed(_ context.Context, ev domain.ModelDeployed) error {
	f.deployed <- ev
	return nil
}

func (f *fakeService) ApplyModelUndeployed(_ context.Context, ev domain.ModelUndeployed) error {
	f.undeployed <- ev
	return nil
}

func (f *fakeService) ApplyModelPromoted(_ context.Context, ev domain.ModelPromoted) error {
	f.promoted <- ev
	return nil
}

func (f *fakeService) ApplyModelArchived(_ context.Context, ev domain.ModelArchived) error {
	f.archivedCount.Add(1)
	f.archived <- ev
	return nil
}

// --- unused-by-these-tests methods (data/control plane) ---------------------

func (f *fakeService) Predict(context.Context, domain.Principal, domain.PredictInput) (domain.PredictOutput, error) {
	panic("Predict not exercised by events tests")
}
func (f *fakeService) GetRoute(context.Context, string, string) (domain.Route, error) {
	panic("GetRoute not exercised by events tests")
}
func (f *fakeService) ListRoutes(context.Context, string, domain.ListOptions) ([]domain.Route, string, error) {
	panic("ListRoutes not exercised by events tests")
}
func (f *fakeService) UpsertRoute(context.Context, string, string, []domain.ProposedTarget) (domain.Route, error) {
	panic("UpsertRoute not exercised by events tests")
}
func (f *fakeService) SetTrafficSplit(context.Context, string, string, []domain.TrafficWeight) (domain.Route, error) {
	panic("SetTrafficSplit not exercised by events tests")
}
func (f *fakeService) DeleteRoute(context.Context, string, string) error {
	panic("DeleteRoute not exercised by events tests")
}
func (f *fakeService) CircuitStates(string) []domain.CircuitSnapshot {
	panic("CircuitStates not exercised by events tests")
}

// compile-time proof the fake satisfies the full interface.
var _ domain.InferenceService = (*fakeService)(nil)

// fakeQuota implements events.QuotaCacheWriter, recording each Block call.
type blockCall struct {
	team string
	ttl  time.Duration
}

type fakeQuota struct {
	mu         sync.Mutex
	blocked    chan blockCall
	blockCount atomic.Int32
}

func newFakeQuota() *fakeQuota {
	return &fakeQuota{blocked: make(chan blockCall, 4)}
}

func (q *fakeQuota) Block(_ context.Context, team string, ttl time.Duration) error {
	q.blockCount.Add(1)
	q.mu.Lock()
	defer q.mu.Unlock()
	q.blocked <- blockCall{team: team, ttl: ttl}
	return nil
}

var _ events.QuotaCacheWriter = (*fakeQuota)(nil)

// waitFor reads one value from a channel or fails on timeout.
func waitFor[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for dispatch")
		var zero T
		return zero
	}
}
