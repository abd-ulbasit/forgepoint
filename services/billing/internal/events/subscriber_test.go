package events_test

// subscriber_test.go — the InferenceCompleted CONSUMER over REAL NATS JetStream.
//
// These tests prove billing's meter-on-inference path:
//   1. an InferenceCompleted event is decoded and DISPATCHED to domain.RecordUsage
//      with the server-resolved team and the right meter axes (request always;
//      tokens when token_count > 0);
//   2. a DUPLICATE redelivery of the SAME envelope id produces a SINGLE effect
//      (the consumer-side ProcessedStore dedupes);
//   3. a POISON event (empty api_key_id → unbillable) is routed to the DLQ after
//      the retry budget instead of looping forever.
//
// The BillingService and TeamResolver are fakes (we observe the RecordUsage calls);
// the NATS transport, consumer, idempotency, and DLQ are all REAL.

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/billing/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/billing/internal/events"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestConsumer_MetersRequestAndTokens publishes one InferenceCompleted with a token
// count and asserts the consumer called RecordUsage TWICE — once for the per-call
// request fee (qty 1) and once for tokens (qty == token_count) — both for the team
// the resolver returned, each with the right idempotency key.
func TestConsumer_MetersRequestAndTokens(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	js := newJetStream(t)
	mustCreateStream(t, js, events.StreamInference, "fp.inference.>")
	mustCreateStream(t, js, "DLQ", "fp.dlq.>")

	svc := newFakeBilling()
	resolver := newFakeResolver(map[string]string{"key-acme": "acme"})

	consumer := events.NewInferenceConsumer(js, svc, resolver,
		natsutil.NewMemoryProcessedStore(), events.SubConfig{})
	t.Cleanup(consumer.Close)
	if err := consumer.Register(context.Background()); err != nil {
		t.Fatalf("Register: %v", err)
	}

	pub := natsutil.NewPublisher(js, "inference-gateway")
	completedAt := time.Now().UTC().Truncate(time.Second)
	if err := pub.Publish(context.Background(), events.SubjectInferenceCompleted, &eventsv1.InferenceCompleted{
		RequestId: "req-1", ModelId: "m-1", ModelName: "iris", Version: "v3",
		ApiKeyId: "key-acme", TokenCount: 1500, CompletedAt: timestamppb.New(completedAt),
	}); err != nil {
		t.Fatalf("publish InferenceCompleted: %v", err)
	}

	// Expect two RecordUsage calls. Collect them (order between the two axes is
	// deterministic in the handler: request first, then tokens).
	first := waitRecord(t, svc.calls)
	second := waitRecord(t, svc.calls)

	calls := map[domain.MeterType]recordCall{first.input.MeterType: first, second.input.MeterType: second}

	req, ok := calls[domain.MeterTypeInferenceRequest]
	if !ok {
		t.Fatal("no INFERENCE_REQUEST RecordUsage call")
	}
	if req.team != "acme" {
		t.Errorf("request-fee team = %q, want acme", req.team)
	}
	if req.input.Quantity != 1 {
		t.Errorf("request-fee quantity = %d, want 1", req.input.Quantity)
	}
	if req.input.IdempotencyKey != "req-1" {
		t.Errorf("request-fee idempotency key = %q, want req-1", req.input.IdempotencyKey)
	}
	if req.input.SourceRequestID != "req-1" {
		t.Errorf("request-fee source_request_id = %q, want req-1", req.input.SourceRequestID)
	}
	if !req.input.OccurredAt.Equal(completedAt) {
		t.Errorf("request-fee occurred_at = %v, want %v", req.input.OccurredAt, completedAt)
	}

	tok, ok := calls[domain.MeterTypeInferenceTokens]
	if !ok {
		t.Fatal("no INFERENCE_TOKENS RecordUsage call")
	}
	if tok.input.Quantity != 1500 {
		t.Errorf("token quantity = %d, want 1500", tok.input.Quantity)
	}
	if tok.input.IdempotencyKey != "req-1:tokens" {
		t.Errorf("token idempotency key = %q, want req-1:tokens", tok.input.IdempotencyKey)
	}
}

// TestConsumer_NoTokensSkipsTokenMeter proves a non-token model (token_count == 0)
// produces ONLY the request-fee record — no token record.
func TestConsumer_NoTokensSkipsTokenMeter(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	js := newJetStream(t)
	mustCreateStream(t, js, events.StreamInference, "fp.inference.>")
	mustCreateStream(t, js, "DLQ", "fp.dlq.>")

	svc := newFakeBilling()
	resolver := newFakeResolver(map[string]string{"key-acme": "acme"})
	consumer := events.NewInferenceConsumer(js, svc, resolver,
		natsutil.NewMemoryProcessedStore(), events.SubConfig{})
	t.Cleanup(consumer.Close)
	if err := consumer.Register(context.Background()); err != nil {
		t.Fatalf("Register: %v", err)
	}

	pub := natsutil.NewPublisher(js, "inference-gateway")
	if err := pub.Publish(context.Background(), events.SubjectInferenceCompleted, &eventsv1.InferenceCompleted{
		RequestId: "req-2", ApiKeyId: "key-acme", TokenCount: 0,
		CompletedAt: timestamppb.New(time.Now().UTC()),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	got := waitRecord(t, svc.calls)
	if got.input.MeterType != domain.MeterTypeInferenceRequest {
		t.Errorf("meter = %v, want INFERENCE_REQUEST", got.input.MeterType)
	}
	// Assert there is NO second call within a window.
	select {
	case extra := <-svc.calls:
		t.Fatalf("unexpected second RecordUsage for a 0-token event: %+v", extra.input)
	case <-time.After(2 * time.Second):
	}
}

// TestConsumer_IdempotentRedelivery proves a DUPLICATE delivery of the SAME
// envelope id meters ONCE. We hand-build one envelope and raw-publish it twice with
// DIFFERENT broker Msg-Ids (so JetStream delivers both — defeating publish-window
// dedup), isolating the CONSUMER-side ProcessedStore as the thing under test.
func TestConsumer_IdempotentRedelivery(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	js := newJetStream(t)
	mustCreateStream(t, js, events.StreamInference, "fp.inference.>")
	mustCreateStream(t, js, "DLQ", "fp.dlq.>")

	svc := newFakeBilling()
	resolver := newFakeResolver(map[string]string{"key-acme": "acme"})
	consumer := events.NewInferenceConsumer(js, svc, resolver,
		natsutil.NewMemoryProcessedStore(), events.SubConfig{})
	t.Cleanup(consumer.Close)
	if err := consumer.Register(context.Background()); err != nil {
		t.Fatalf("Register: %v", err)
	}

	dupID := uuid.NewString()
	data, err := json.Marshal(&eventsv1.InferenceCompleted{
		RequestId: "req-dup", ApiKeyId: "key-acme", TokenCount: 0,
		CompletedAt: timestamppb.New(time.Now().UTC()),
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	env := natsutil.EventEnvelope{
		ID: dupID, Type: "completed", Source: "inference-gateway",
		Timestamp: time.Now().UTC(), Data: data,
	}
	envBytes, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	// Two physical deliveries, same envelope id, distinct broker Msg-Ids.
	for i := range 2 {
		if _, err := js.Publish(context.Background(), events.SubjectInferenceCompleted, envBytes,
			jetstream.WithMsgID(dupID+"-delivery-"+string(rune('a'+i)))); err != nil {
			t.Fatalf("raw publish %d: %v", i, err)
		}
	}

	// First delivery meters once (the request fee).
	first := waitRecord(t, svc.calls)
	if first.input.IdempotencyKey != "req-dup" {
		t.Errorf("first call idempotency key = %q, want req-dup", first.input.IdempotencyKey)
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

// TestConsumer_PoisonGoesToDLQ proves an event that can never be metered (empty
// api_key_id → no team → unbillable) is routed to the DLQ after the retry budget,
// instead of looping forever, and RecordUsage is NEVER called for it.
func TestConsumer_PoisonGoesToDLQ(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	js := newJetStream(t)
	mustCreateStream(t, js, events.StreamInference, "fp.inference.>")
	mustCreateStream(t, js, "DLQ", "fp.dlq.>")

	svc := newFakeBilling()
	resolver := newFakeResolver(map[string]string{"key-acme": "acme"})
	dlqSubject := "fp.dlq.billing"
	consumer := events.NewInferenceConsumer(js, svc, resolver,
		natsutil.NewMemoryProcessedStore(),
		events.SubConfig{MaxRetries: 2, DLQSubject: dlqSubject, MessageTimeout: 2 * time.Second})
	t.Cleanup(consumer.Close)
	if err := consumer.Register(context.Background()); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Watch the DLQ.
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

	// Poison: empty api_key_id — the handler can't attribute the charge.
	pub := natsutil.NewPublisher(js, "inference-gateway")
	if err := pub.Publish(context.Background(), events.SubjectInferenceCompleted, &eventsv1.InferenceCompleted{
		RequestId: "req-poison", ApiKeyId: "", TokenCount: 0,
		CompletedAt: timestamppb.New(time.Now().UTC()),
	}); err != nil {
		t.Fatalf("publish poison: %v", err)
	}

	select {
	case env := <-dlq:
		var p eventsv1.InferenceCompleted
		if err := json.Unmarshal(env.Data, &p); err != nil {
			t.Fatalf("decode DLQ payload: %v", err)
		}
		if p.GetRequestId() != "req-poison" {
			t.Errorf("DLQ payload request_id = %q, want req-poison", p.GetRequestId())
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timeout: poison message never reached the DLQ")
	}

	if n := svc.recordCount.Load(); n != 0 {
		t.Errorf("RecordUsage called %d times for a poison event, want 0", n)
	}
}

// TestConsumer_UnknownTeamIsPoison proves an api_key the resolver can't map
// (ErrTeamNotFound) is permanent → DLQ, not an infinite retry.
func TestConsumer_UnknownTeamIsPoison(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	js := newJetStream(t)
	mustCreateStream(t, js, events.StreamInference, "fp.inference.>")
	mustCreateStream(t, js, "DLQ", "fp.dlq.>")

	svc := newFakeBilling()
	resolver := newFakeResolver(map[string]string{}) // empty → every key is unknown
	dlqSubject := "fp.dlq.billing"
	consumer := events.NewInferenceConsumer(js, svc, resolver,
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

	pub := natsutil.NewPublisher(js, "inference-gateway")
	if err := pub.Publish(context.Background(), events.SubjectInferenceCompleted, &eventsv1.InferenceCompleted{
		RequestId: "req-unknown", ApiKeyId: "key-ghost", TokenCount: 0,
		CompletedAt: timestamppb.New(time.Now().UTC()),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case <-dlq:
		// Correct: unknown team → poison → DLQ.
	case <-time.After(30 * time.Second):
		t.Fatal("timeout: unknown-team event never reached the DLQ")
	}
	if n := svc.recordCount.Load(); n != 0 {
		t.Errorf("RecordUsage called %d times for an unknown-team event, want 0", n)
	}
}

// ----------------------------------------------------------------------------
// FAKES
// ----------------------------------------------------------------------------

// recordCall captures one RecordUsage invocation.
type recordCall struct {
	team  string
	input domain.RecordUsageInput
}

// fakeBilling implements domain.BillingService. Only RecordUsage is exercised by
// these tests; it records each call. The other methods satisfy the interface and
// panic if unexpectedly invoked.
type fakeBilling struct {
	calls       chan recordCall
	recordCount atomic.Int32
}

func newFakeBilling() *fakeBilling {
	return &fakeBilling{calls: make(chan recordCall, 8)}
}

func (f *fakeBilling) RecordUsage(_ context.Context, team string, input domain.RecordUsageInput) (domain.UsageRecord, bool, error) {
	f.recordCount.Add(1)
	f.calls <- recordCall{team: team, input: input}
	return domain.UsageRecord{ID: "rec-" + input.IdempotencyKey, Team: team, MeterType: input.MeterType, Quantity: input.Quantity}, false, nil
}

func (f *fakeBilling) CreateRatePlan(context.Context, domain.CreateRatePlanInput) (domain.RatePlan, bool, error) {
	panic("CreateRatePlan not exercised by events tests")
}
func (f *fakeBilling) GetRatePlan(context.Context, string) (domain.RatePlan, error) {
	panic("GetRatePlan not exercised by events tests")
}
func (f *fakeBilling) CheckQuota(context.Context, string, domain.MeterType) (domain.QuotaStatus, error) {
	panic("CheckQuota not exercised by events tests")
}
func (f *fakeBilling) GetUsage(context.Context, domain.GetUsageInput) ([]domain.UsageSummary, domain.Money, string, error) {
	panic("GetUsage not exercised by events tests")
}
func (f *fakeBilling) GetInvoice(context.Context, string) (domain.Invoice, error) {
	panic("GetInvoice not exercised by events tests")
}
func (f *fakeBilling) ListInvoices(context.Context, domain.ListInvoicesOptions) ([]domain.Invoice, string, error) {
	panic("ListInvoices not exercised by events tests")
}
func (f *fakeBilling) GenerateInvoice(context.Context, string, time.Time, time.Time) (domain.Invoice, error) {
	panic("GenerateInvoice not exercised by events tests")
}

var _ domain.BillingService = (*fakeBilling)(nil)

// fakeResolver implements events.TeamResolver from a static api_key → team map.
type fakeResolver struct {
	mu    sync.Mutex
	teams map[string]string
}

func newFakeResolver(teams map[string]string) *fakeResolver {
	return &fakeResolver{teams: teams}
}

func (r *fakeResolver) ResolveTeam(_ context.Context, apiKeyID string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if team, ok := r.teams[apiKeyID]; ok {
		return team, nil
	}
	return "", events.ErrTeamNotFound
}

var _ events.TeamResolver = (*fakeResolver)(nil)

// waitRecord reads one RecordUsage call or fails on timeout.
func waitRecord(t *testing.T, ch <-chan recordCall) recordCall {
	t.Helper()
	select {
	case c := <-ch:
		return c
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for RecordUsage call")
		return recordCall{}
	}
}
