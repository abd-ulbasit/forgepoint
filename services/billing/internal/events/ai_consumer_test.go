package events_test

// ai_consumer_test.go — the AI-token CONSUMER (fp.ai.completion.served) over REAL
// NATS JetStream. This is the THIRD money axis (M7/L2): billing meters the AI
// Gateway's served-completion events through the existing INFERENCE_TOKENS meter.
//
// These prove billing's meter-on-AI path end-to-end on a real broker:
//   1. a completion event with total_tokens > 0 is decoded and DISPATCHED to
//      domain.RecordUsage(INFERENCE_TOKENS, qty = total_tokens) for the gateway-
//      resolved team, with the ":ai-tokens" idempotency key (distinct from the
//      inference consumer's keys);
//   2. a PURE CACHE HIT (total_tokens == 0) is ACKed as a NO-OP — nothing to meter;
//   3. a POISON event (empty team → unattributable) is routed to the DLQ after the
//      retry budget instead of looping forever, and RecordUsage is NEVER called.
//
// PLAIN JSON, NOT protojson: the AI Gateway publishes this as a plain Go struct (no
// events.v1 proto message, no google.protobuf.* fields), so we publish a matching
// plain struct via natsutil.Publisher — which marshals non-proto payloads with
// encoding/json. This MIRRORS the producer (ai-gateway publisher.go) field-for-field.

import (
	"context"
	"testing"
	"time"

	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/billing/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/billing/internal/events"
)

// aiServedPayload mirrors the AI Gateway's wire struct (ai-gateway publisher.go
// completionServedPayload) and billing's read-model (ai_consumer.go aiCompletionServed):
// lowerCamelCase json tags, no team field on the request body — the team is stamped by
// the trusted gateway and IS server-authoritative on this internal-bus event.
type aiServedPayload struct {
	RequestID    string `json:"requestId"`
	Team         string `json:"team"`
	Model        string `json:"model"`
	Provider     string `json:"provider"`
	PromptTokens int32  `json:"promptTokens"`
	OutputTokens int32  `json:"outputTokens"`
	TotalTokens  int32  `json:"totalTokens"`
	CostMicroUSD int64  `json:"costMicroUsd"`
	CacheHit     bool   `json:"cacheHit"`
	LatencyMs    int64  `json:"latencyMs"`
}

// TestAIConsumer_MetersTokens publishes one served completion with total_tokens > 0
// and asserts the consumer metered it through INFERENCE_TOKENS for the event's team,
// with quantity == total_tokens and the ":ai-tokens" idempotency key.
func TestAIConsumer_MetersTokens(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	js := newJetStream(t)
	mustCreateStream(t, js, events.StreamAI, "fp.ai.>")
	mustCreateStream(t, js, "DLQ", "fp.dlq.>")

	svc := newFakeBilling()
	// NOTE: NO TeamResolver — the AI consumer needs none (the team is on the event).
	consumer := events.NewAIConsumer(js, svc, natsutil.NewMemoryProcessedStore(), events.SubConfig{})
	t.Cleanup(consumer.Close)
	if err := consumer.Register(context.Background()); err != nil {
		t.Fatalf("Register: %v", err)
	}

	pub := natsutil.NewPublisher(js, "ai-gateway")
	if err := pub.Publish(context.Background(), events.SubjectAICompletionServed, aiServedPayload{
		RequestID: "ai-req-1", Team: "acme", Model: "smollm2", Provider: "ollama",
		PromptTokens: 40, OutputTokens: 20, TotalTokens: 60, CostMicroUSD: 17, LatencyMs: 12,
	}); err != nil {
		t.Fatalf("publish Ai completion: %v", err)
	}

	got := waitRecord(t, svc.calls)
	if got.team != "acme" {
		t.Errorf("metered team = %q, want acme (gateway-resolved, on the event)", got.team)
	}
	if got.input.MeterType != domain.MeterTypeInferenceTokens {
		t.Errorf("meter = %v, want INFERENCE_TOKENS", got.input.MeterType)
	}
	if got.input.Quantity != 60 {
		t.Errorf("quantity = %d, want 60 (total_tokens)", got.input.Quantity)
	}
	if got.input.IdempotencyKey != "ai-req-1:ai-tokens" {
		t.Errorf("idempotency key = %q, want ai-req-1:ai-tokens", got.input.IdempotencyKey)
	}
	if got.input.SourceRequestID != "ai-req-1" {
		t.Errorf("source_request_id = %q, want ai-req-1", got.input.SourceRequestID)
	}
}

// TestAIConsumer_CacheHitZeroTokensNoOps proves a pure cache hit (total_tokens == 0)
// is ACKed as a no-op — there is nothing to meter on this axis, so RecordUsage is
// never called and the event does NOT loop or DLQ.
func TestAIConsumer_CacheHitZeroTokensNoOps(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	js := newJetStream(t)
	mustCreateStream(t, js, events.StreamAI, "fp.ai.>")
	mustCreateStream(t, js, "DLQ", "fp.dlq.>")

	svc := newFakeBilling()
	consumer := events.NewAIConsumer(js, svc, natsutil.NewMemoryProcessedStore(), events.SubConfig{})
	t.Cleanup(consumer.Close)
	if err := consumer.Register(context.Background()); err != nil {
		t.Fatalf("Register: %v", err)
	}

	pub := natsutil.NewPublisher(js, "ai-gateway")
	if err := pub.Publish(context.Background(), events.SubjectAICompletionServed, aiServedPayload{
		RequestID: "ai-cache-hit", Team: "acme", TotalTokens: 0, CacheHit: true,
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// No RecordUsage for a zero-token cache hit.
	select {
	case extra := <-svc.calls:
		t.Fatalf("unexpected RecordUsage for a 0-token cache hit: %+v", extra.input)
	case <-time.After(2 * time.Second):
	}
	if n := svc.recordCount.Load(); n != 0 {
		t.Errorf("RecordUsage called %d times for a cache hit, want 0", n)
	}
}

// TestAIConsumer_EmptyTeamIsPoison proves an event with no team (a producer bug /
// corruption — the charge can't be attributed) is permanent → DLQ after the retry
// budget, not an infinite retry, and RecordUsage is NEVER called.
func TestAIConsumer_EmptyTeamIsPoison(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	js := newJetStream(t)
	mustCreateStream(t, js, events.StreamAI, "fp.ai.>")
	mustCreateStream(t, js, "DLQ", "fp.dlq.>")

	svc := newFakeBilling()
	dlqSubject := "fp.dlq.billing"
	consumer := events.NewAIConsumer(js, svc, natsutil.NewMemoryProcessedStore(),
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

	pub := natsutil.NewPublisher(js, "ai-gateway")
	if err := pub.Publish(context.Background(), events.SubjectAICompletionServed, aiServedPayload{
		RequestID: "ai-no-team", Team: "", TotalTokens: 30,
	}); err != nil {
		t.Fatalf("publish poison: %v", err)
	}

	select {
	case <-dlq:
		// Correct: empty team → poison → DLQ.
	case <-time.After(30 * time.Second):
		t.Fatal("timeout: empty-team event never reached the DLQ")
	}
	if n := svc.recordCount.Load(); n != 0 {
		t.Errorf("RecordUsage called %d times for an empty-team event, want 0", n)
	}
}
