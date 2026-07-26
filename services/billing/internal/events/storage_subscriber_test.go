package events_test

// storage_subscriber_test.go — the ModelVersionReady STORAGE consumer over REAL
// NATS JetStream. This is the second money axis the event contract requires
// billing to consume (docs/design/event-contract.md): fp.models.version.ready →
// RecordUsage(STORAGE_BYTES). Before this consumer existed, STORAGE_BYTES was never
// metered from any event — a silent revenue gap. These tests prove the new path.
//
// They prove:
//   1. a ModelVersionReady event is decoded and DISPATCHED to RecordUsage with
//      MeterType STORAGE_BYTES, quantity == size_bytes, the resolved owning team,
//      and idempotency key version_id+":storage";
//   2. a zero/negative size_bytes is a NO-OP ACK (nothing to bill, NOT poison);
//   3. a DUPLICATE redelivery of the SAME envelope id meters ONCE (consumer-side
//      ProcessedStore dedupe);
//   4. a POISON event (empty model_id → unattributable) routes to the DLQ after the
//      retry budget, and RecordUsage is NEVER called;
//   5. an unknown model (resolver → ErrTeamNotFound) is permanent → DLQ;
//   6. a TRANSIENT resolver error NAKs + retries (the fail-closed production posture)
//      rather than DLQ'ing immediately — and eventually DLQs after the budget.
//
// The BillingService and VersionTeamResolver are fakes; the NATS transport,
// consumer, idempotency, and DLQ are all REAL (testcontainers JetStream). The
// fakeBilling fake and the wait/stream helpers live in subscriber_test.go /
// harness_test.go (same package).

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/billing/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/billing/internal/events"
)

// TestStorageConsumer_MetersStorageBytes publishes one ModelVersionReady and
// asserts the consumer called RecordUsage ONCE with STORAGE_BYTES, quantity ==
// size_bytes, the resolved team, and the version_id+":storage" idempotency key.
func TestStorageConsumer_MetersStorageBytes(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	js := newJetStream(t)
	mustCreateStream(t, js, events.StreamModels, "fp.models.>")
	mustCreateStream(t, js, "DLQ", "fp.dlq.>")

	svc := newFakeBilling()
	resolver := newFakeVersionResolver(map[string]string{"m-1": "acme"})

	consumer := events.NewStorageConsumer(js, svc, resolver,
		natsutil.NewMemoryProcessedStore(), events.SubConfig{})
	t.Cleanup(consumer.Close)
	if err := consumer.Register(context.Background()); err != nil {
		t.Fatalf("Register: %v", err)
	}

	pub := natsutil.NewPublisher(js, "registry")
	readyAt := time.Now().UTC().Truncate(time.Second)
	if err := pub.Publish(context.Background(), events.SubjectModelVersionReady, &eventsv1.ModelVersionReady{
		ModelId: "m-1", ModelName: "iris", VersionId: "ver-1", Version: "v3",
		ArtifactPath: "s3://fp-models/m-1/v3.onnx", SizeBytes: 4096,
		ReadyAt: timestamppb.New(readyAt),
	}); err != nil {
		t.Fatalf("publish ModelVersionReady: %v", err)
	}

	got := waitRecord(t, svc.calls)
	if got.input.MeterType != domain.MeterTypeStorageBytes {
		t.Errorf("meter = %v, want STORAGE_BYTES", got.input.MeterType)
	}
	if got.team != "acme" {
		t.Errorf("team = %q, want acme", got.team)
	}
	if got.input.Quantity != 4096 {
		t.Errorf("quantity = %d, want 4096 (size_bytes)", got.input.Quantity)
	}
	if got.input.IdempotencyKey != "ver-1:storage" {
		t.Errorf("idempotency key = %q, want ver-1:storage", got.input.IdempotencyKey)
	}
	if got.input.SourceRequestID != "ver-1" {
		t.Errorf("source_request_id = %q, want ver-1", got.input.SourceRequestID)
	}
	if got.input.ModelID != "m-1" {
		t.Errorf("model_id = %q, want m-1", got.input.ModelID)
	}
	if !got.input.OccurredAt.Equal(readyAt) {
		t.Errorf("occurred_at = %v, want %v (ready_at)", got.input.OccurredAt, readyAt)
	}

	// No second call: a single ready-event meters exactly one storage record.
	select {
	case extra := <-svc.calls:
		t.Fatalf("unexpected second RecordUsage for one ready event: %+v", extra.input)
	case <-time.After(2 * time.Second):
	}
}

// TestStorageConsumer_ZeroSizeIsNoOp proves a ModelVersionReady whose size_bytes is
// 0 (or negative) is ACKed WITHOUT a RecordUsage call — an empty artifact has no
// storage to bill and is NOT an error (must not poison/DLQ, must not loop).
func TestStorageConsumer_ZeroSizeIsNoOp(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	js := newJetStream(t)
	mustCreateStream(t, js, events.StreamModels, "fp.models.>")
	mustCreateStream(t, js, "DLQ", "fp.dlq.>")

	svc := newFakeBilling()
	resolver := newFakeVersionResolver(map[string]string{"m-1": "acme"})
	consumer := events.NewStorageConsumer(js, svc, resolver,
		natsutil.NewMemoryProcessedStore(), events.SubConfig{})
	t.Cleanup(consumer.Close)
	if err := consumer.Register(context.Background()); err != nil {
		t.Fatalf("Register: %v", err)
	}

	pub := natsutil.NewPublisher(js, "registry")
	if err := pub.Publish(context.Background(), events.SubjectModelVersionReady, &eventsv1.ModelVersionReady{
		ModelId: "m-1", VersionId: "ver-zero", SizeBytes: 0,
		ReadyAt: timestamppb.New(time.Now().UTC()),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// No RecordUsage at all within a window.
	select {
	case extra := <-svc.calls:
		t.Fatalf("zero-size ready event triggered RecordUsage: %+v (want no-op ACK)", extra.input)
	case <-time.After(3 * time.Second):
	}
	if n := svc.recordCount.Load(); n != 0 {
		t.Errorf("RecordUsage called %d times for a zero-size event, want 0", n)
	}
}

// TestStorageConsumer_IdempotentRedelivery proves a DUPLICATE delivery of the SAME
// envelope id meters ONCE. We hand-build one envelope and raw-publish it twice with
// DIFFERENT broker Msg-Ids (so JetStream delivers both), isolating the consumer-side
// ProcessedStore.
func TestStorageConsumer_IdempotentRedelivery(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	js := newJetStream(t)
	mustCreateStream(t, js, events.StreamModels, "fp.models.>")
	mustCreateStream(t, js, "DLQ", "fp.dlq.>")

	svc := newFakeBilling()
	resolver := newFakeVersionResolver(map[string]string{"m-1": "acme"})
	consumer := events.NewStorageConsumer(js, svc, resolver,
		natsutil.NewMemoryProcessedStore(), events.SubConfig{})
	t.Cleanup(consumer.Close)
	if err := consumer.Register(context.Background()); err != nil {
		t.Fatalf("Register: %v", err)
	}

	dupID := uuid.NewString()
	// protojson (NOT encoding/json): ModelVersionReady carries a
	// google.protobuf.Timestamp (ready_at). The consumer decodes with protojson, so
	// the simulated wire bytes must be canonical proto-JSON, matching the publisher.
	data, err := protojson.Marshal(&eventsv1.ModelVersionReady{
		ModelId: "m-1", VersionId: "ver-dup", SizeBytes: 2048,
		ReadyAt: timestamppb.New(time.Now().UTC()),
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	env := natsutil.EventEnvelope{
		ID: dupID, Type: "ready", Source: "registry",
		Timestamp: time.Now().UTC(), Data: data,
	}
	envBytes, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	for i := range 2 {
		if _, err := js.Publish(context.Background(), events.SubjectModelVersionReady, envBytes,
			jetstream.WithMsgID(dupID+"-delivery-"+string(rune('a'+i)))); err != nil {
			t.Fatalf("raw publish %d: %v", i, err)
		}
	}

	first := waitRecord(t, svc.calls)
	if first.input.IdempotencyKey != "ver-dup:storage" {
		t.Errorf("first call idempotency key = %q, want ver-dup:storage", first.input.IdempotencyKey)
	}

	// The duplicate must NOT produce a second RecordUsage.
	select {
	case extra := <-svc.calls:
		t.Fatalf("duplicate redelivery produced a SECOND RecordUsage: %+v (idempotency broken)", extra.input)
	case <-time.After(3 * time.Second):
	}
	if n := svc.recordCount.Load(); n != 1 {
		t.Errorf("RecordUsage called %d times, want exactly 1", n)
	}
}

// TestStorageConsumer_PoisonGoesToDLQ proves an event that can never be metered
// (empty model_id → unattributable) is routed to the DLQ after the retry budget,
// and RecordUsage is NEVER called.
func TestStorageConsumer_PoisonGoesToDLQ(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	js := newJetStream(t)
	mustCreateStream(t, js, events.StreamModels, "fp.models.>")
	mustCreateStream(t, js, "DLQ", "fp.dlq.>")

	svc := newFakeBilling()
	resolver := newFakeVersionResolver(map[string]string{"m-1": "acme"})
	dlqSubject := "fp.dlq.billing"
	consumer := events.NewStorageConsumer(js, svc, resolver,
		natsutil.NewMemoryProcessedStore(),
		events.SubConfig{MaxRetries: 2, DLQSubject: dlqSubject, MessageTimeout: 2 * time.Second})
	t.Cleanup(consumer.Close)
	if err := consumer.Register(context.Background()); err != nil {
		t.Fatalf("Register: %v", err)
	}

	dlq := make(chan natsutil.EventEnvelope, 1)
	dlqSub := natsutil.NewSubscriber(js)
	t.Cleanup(dlqSub.Close)
	if err := dlqSub.Subscribe(context.Background(), "DLQ", dlqSubject,
		func(_ context.Context, env natsutil.EventEnvelope) error { dlq <- env; return nil }); err != nil {
		t.Fatalf("DLQ subscribe: %v", err)
	}

	// Poison: empty model_id but non-zero size — the handler can't attribute the charge.
	pub := natsutil.NewPublisher(js, "registry")
	if err := pub.Publish(context.Background(), events.SubjectModelVersionReady, &eventsv1.ModelVersionReady{
		ModelId: "", VersionId: "ver-poison", SizeBytes: 1024,
		ReadyAt: timestamppb.New(time.Now().UTC()),
	}); err != nil {
		t.Fatalf("publish poison: %v", err)
	}

	select {
	case env := <-dlq:
		var p eventsv1.ModelVersionReady
		// protojson: the DLQ copy preserves the original canonical proto-JSON payload.
		if err := protojson.Unmarshal(env.Data, &p); err != nil {
			t.Fatalf("decode DLQ payload: %v", err)
		}
		if p.GetVersionId() != "ver-poison" {
			t.Errorf("DLQ payload version_id = %q, want ver-poison", p.GetVersionId())
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timeout: poison message never reached the DLQ")
	}

	if n := svc.recordCount.Load(); n != 0 {
		t.Errorf("RecordUsage called %d times for a poison event, want 0", n)
	}
}

// TestStorageConsumer_UnknownModelIsPoison proves a model the resolver can't map
// (ErrTeamNotFound) is permanent → DLQ, not an infinite retry.
func TestStorageConsumer_UnknownModelIsPoison(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	js := newJetStream(t)
	mustCreateStream(t, js, events.StreamModels, "fp.models.>")
	mustCreateStream(t, js, "DLQ", "fp.dlq.>")

	svc := newFakeBilling()
	resolver := newFakeVersionResolver(map[string]string{}) // empty → every model is unknown
	dlqSubject := "fp.dlq.billing"
	consumer := events.NewStorageConsumer(js, svc, resolver,
		natsutil.NewMemoryProcessedStore(),
		events.SubConfig{MaxRetries: 2, DLQSubject: dlqSubject, MessageTimeout: 2 * time.Second})
	t.Cleanup(consumer.Close)
	if err := consumer.Register(context.Background()); err != nil {
		t.Fatalf("Register: %v", err)
	}

	dlq := make(chan natsutil.EventEnvelope, 1)
	dlqSub := natsutil.NewSubscriber(js)
	t.Cleanup(dlqSub.Close)
	if err := dlqSub.Subscribe(context.Background(), "DLQ", dlqSubject,
		func(_ context.Context, env natsutil.EventEnvelope) error { dlq <- env; return nil }); err != nil {
		t.Fatalf("DLQ subscribe: %v", err)
	}

	pub := natsutil.NewPublisher(js, "registry")
	if err := pub.Publish(context.Background(), events.SubjectModelVersionReady, &eventsv1.ModelVersionReady{
		ModelId: "m-ghost", VersionId: "ver-unknown", SizeBytes: 512,
		ReadyAt: timestamppb.New(time.Now().UTC()),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case <-dlq:
		// Correct: unknown model → poison → DLQ.
	case <-time.After(30 * time.Second):
		t.Fatal("timeout: unknown-model event never reached the DLQ")
	}
	if n := svc.recordCount.Load(); n != 0 {
		t.Errorf("RecordUsage called %d times for an unknown-model event, want 0", n)
	}
}

// TestStorageConsumer_TransientResolverRetriesThenDLQs proves the FAIL-CLOSED
// production posture: a resolver returning a PLAIN (non-ErrTeamNotFound) error is
// TRANSIENT — the event NAKs and is REDELIVERED (the resolver is called more than
// MaxRetries+1 ... actually exactly the delivery budget) — and is only dead-lettered
// after the budget, NOT mis-attributed to a guessed team. This is the exact behavior
// of main.go's unwiredVersionTeamResolver (the all-DLQ safety posture).
func TestStorageConsumer_TransientResolverRetriesThenDLQs(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	js := newJetStream(t)
	mustCreateStream(t, js, events.StreamModels, "fp.models.>")
	mustCreateStream(t, js, "DLQ", "fp.dlq.>")

	svc := newFakeBilling()
	resolver := newAlwaysTransientVersionResolver() // mirrors unwiredVersionTeamResolver
	dlqSubject := "fp.dlq.billing"
	const maxRetries = 2
	consumer := events.NewStorageConsumer(js, svc, resolver,
		natsutil.NewMemoryProcessedStore(),
		events.SubConfig{MaxRetries: maxRetries, DLQSubject: dlqSubject, MessageTimeout: 2 * time.Second})
	t.Cleanup(consumer.Close)
	if err := consumer.Register(context.Background()); err != nil {
		t.Fatalf("Register: %v", err)
	}

	dlq := make(chan natsutil.EventEnvelope, 1)
	dlqSub := natsutil.NewSubscriber(js)
	t.Cleanup(dlqSub.Close)
	if err := dlqSub.Subscribe(context.Background(), "DLQ", dlqSubject,
		func(_ context.Context, env natsutil.EventEnvelope) error { dlq <- env; return nil }); err != nil {
		t.Fatalf("DLQ subscribe: %v", err)
	}

	pub := natsutil.NewPublisher(js, "registry")
	if err := pub.Publish(context.Background(), events.SubjectModelVersionReady, &eventsv1.ModelVersionReady{
		ModelId: "m-1", VersionId: "ver-transient", SizeBytes: 8192,
		ReadyAt: timestamppb.New(time.Now().UTC()),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// It must be RETRIED (transient → NAK), not DLQ'd on the first failure, then
	// eventually dead-lettered after the budget is exhausted.
	select {
	case <-dlq:
		// Correct end-state: after MaxRetries the transient failure is dead-lettered.
	case <-time.After(30 * time.Second):
		t.Fatal("timeout: transient-resolver event never reached the DLQ")
	}

	// The resolver was called MORE THAN ONCE (it retried) — proving the transient
	// classification (NAK + redeliver), not an immediate DLQ. With MaxRetries=2 the
	// delivery budget is 3, so the resolver sees at least 2 calls.
	if n := resolver.callCount(); n < 2 {
		t.Errorf("resolver called %d times, want >= 2 (transient error must RETRY, not DLQ immediately)", n)
	}
	// RecordUsage is never reached — the team never resolved.
	if n := svc.recordCount.Load(); n != 0 {
		t.Errorf("RecordUsage called %d times for an unresolvable event, want 0", n)
	}
}

// ----------------------------------------------------------------------------
// FAKES (storage-specific; the BillingService fake lives in subscriber_test.go)
// ----------------------------------------------------------------------------

// fakeVersionResolver implements events.VersionTeamResolver from a static
// model_id → team map. An unknown model returns ErrTeamNotFound (permanent → poison),
// mirroring fakeResolver on the inference side.
type fakeVersionResolver struct {
	mu    sync.Mutex
	teams map[string]string
}

func newFakeVersionResolver(teams map[string]string) *fakeVersionResolver {
	return &fakeVersionResolver{teams: teams}
}

func (r *fakeVersionResolver) ResolveVersionTeam(_ context.Context, modelID, _ string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if team, ok := r.teams[modelID]; ok {
		return team, nil
	}
	return "", events.ErrTeamNotFound
}

var _ events.VersionTeamResolver = (*fakeVersionResolver)(nil)

// alwaysTransientVersionResolver always returns a PLAIN error (NOT ErrTeamNotFound),
// the exact behavior of main.go's unwiredVersionTeamResolver: the consumer must
// classify this as TRANSIENT (NAK + retry), never poison-on-first-failure. It counts
// calls so a test can assert the event was actually retried.
type alwaysTransientVersionResolver struct {
	mu    sync.Mutex
	calls int
}

func newAlwaysTransientVersionResolver() *alwaysTransientVersionResolver {
	return &alwaysTransientVersionResolver{}
}

func (r *alwaysTransientVersionResolver) ResolveVersionTeam(_ context.Context, _, _ string) (string, error) {
	r.mu.Lock()
	r.calls++
	r.mu.Unlock()
	return "", errors.New("version team resolver not wired (transient) — event will retry")
}

func (r *alwaysTransientVersionResolver) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

var _ events.VersionTeamResolver = (*alwaysTransientVersionResolver)(nil)
