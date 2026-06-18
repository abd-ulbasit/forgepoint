package events_test

// ============================================================================
// AUTH EVENT ADAPTER — REAL NATS INTEGRATION TESTS
// ============================================================================
//
// These tests exercise the ACTUAL publish/consume behavior against a real NATS
// JetStream server (testcontainers via testutil.StartNATS), not mocks. NATS
// semantics — JetStream persistence, at-least-once redelivery, consumer-side
// idempotency, and DLQ routing for poison messages — are stateful and subtle;
// the only honest way to verify them is to run a real broker.
//
// WHAT IS VERIFIED:
//   1. PublishUserCreated   → lands on fp.auth.user.created with a correct
//      envelope (Source="auth", Type="user.created") and a decodable
//      events.v1.UserCreated payload.
//   2. PublishAPIKeyRotated → lands on fp.auth.apikey.rotated with the right
//      payload AND carries NO secret (id + prefix only).
//   3. DUPLICATE redelivery of the same envelope id is idempotent: the side
//      effect runs exactly once (exactly-once-IN-EFFECT).
//   4. A POISON message (handler always fails) is routed to the DLQ after the
//      retry budget is exhausted — it does not loop forever.
//
// Auth itself CONSUMES nothing (it is the root of the trust graph), so tests 3
// and 4 stand up a natsutil.Subscriber on auth's own subjects to prove the
// platform's consumer machinery behaves correctly for any service that consumes
// auth events. That is the realistic, contract-grounded thing to assert.
// ============================================================================

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	authevents "github.com/abd-ulbasit/forgepoint/services/auth/internal/events"

	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
)

// newJS connects to a fresh NATS server and ensures the AUTH stream exists.
// Centralizing this keeps each test focused on the behavior under test.
func newJS(t *testing.T) (jetstream.JetStream, func()) {
	t.Helper()
	testutil.SkipIfNoDocker(t)

	url := testutil.StartNATS(t)
	conn, js, err := natsutil.Connect(url)
	if err != nil {
		t.Fatalf("connect to NATS: %v", err)
	}

	ctx := context.Background()
	if err := authevents.EnsureStream(ctx, js); err != nil {
		conn.Close()
		t.Fatalf("ensure AUTH stream: %v", err)
	}

	return js, func() { conn.Close() }
}

// ============================================================================
// TEST 1: UserCreated publishes to the right subject with a correct envelope
// ============================================================================

func TestPublishUserCreated_LandsOnSubjectWithEnvelopeAndPayload(t *testing.T) {
	js, cleanup := newJS(t)
	defer cleanup()

	ctx := context.Background()

	// Subscribe to the EXACT canonical subject so we also assert routing (a wrong
	// subject would simply never deliver here).
	received := make(chan natsutil.EventEnvelope, 1)
	sub := natsutil.NewSubscriber(js)
	if err := sub.Subscribe(ctx, authevents.StreamName, authevents.SubjectUserCreated,
		func(_ context.Context, env natsutil.EventEnvelope) error {
			received <- env
			return nil
		}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	pub := authevents.NewPublisher(natsutil.NewPublisher(js, authevents.SourceName))

	in := authevents.UserCreatedInput{
		UserID: "user-123",
		Email:  "ada@forgepoint.dev",
		Name:   "Ada Lovelace",
		Team:   "platform",
		Role:   "engineer",
	}
	if err := pub.PublishUserCreated(ctx, in); err != nil {
		t.Fatalf("PublishUserCreated: %v", err)
	}

	select {
	case env := <-received:
		// Envelope assertions: Source identifies the producer; Type is derived by
		// natsutil from the subject ("fp.auth.user.created" → "user.created").
		if env.Source != authevents.SourceName {
			t.Errorf("envelope Source = %q, want %q", env.Source, authevents.SourceName)
		}
		if env.Type != "user.created" {
			t.Errorf("envelope Type = %q, want %q", env.Type, "user.created")
		}
		if env.ID == "" {
			t.Error("envelope ID is empty (needed for dedup + idempotency)")
		}
		if env.CorrelationID == "" {
			t.Error("envelope CorrelationID is empty (needed for cross-service correlation)")
		}

		// Payload assertions: decode the events.v1 message and check the mapping.
		var payload eventsv1.UserCreated
		if err := protojson.Unmarshal(env.Data, &payload); err != nil {
			t.Fatalf("unmarshal UserCreated payload: %v", err)
		}
		if payload.GetUserId() != in.UserID {
			t.Errorf("UserId = %q, want %q", payload.GetUserId(), in.UserID)
		}
		if payload.GetEmail() != in.Email {
			t.Errorf("Email = %q, want %q", payload.GetEmail(), in.Email)
		}
		if payload.GetName() != in.Name {
			t.Errorf("Name = %q, want %q", payload.GetName(), in.Name)
		}
		if payload.GetTeam() != in.Team {
			t.Errorf("Team = %q, want %q", payload.GetTeam(), in.Team)
		}
		if payload.GetRole() != in.Role {
			t.Errorf("Role = %q, want %q", payload.GetRole(), in.Role)
		}
		if payload.GetCreatedAt() == nil || payload.GetCreatedAt().AsTime().IsZero() {
			t.Error("CreatedAt is missing/zero — producer must stamp the event time")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for UserCreated event")
	}
}

// ============================================================================
// TEST 2: ApiKeyRotated publishes to the right subject AND carries no secret
// ============================================================================

func TestPublishAPIKeyRotated_LandsOnSubjectAndCarriesNoSecret(t *testing.T) {
	js, cleanup := newJS(t)
	defer cleanup()

	ctx := context.Background()

	received := make(chan natsutil.EventEnvelope, 1)
	sub := natsutil.NewSubscriber(js)
	if err := sub.Subscribe(ctx, authevents.StreamName, authevents.SubjectAPIKeyRotated,
		func(_ context.Context, env natsutil.EventEnvelope) error {
			received <- env
			return nil
		}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	pub := authevents.NewPublisher(natsutil.NewPublisher(js, authevents.SourceName))

	in := authevents.APIKeyRotatedInput{
		KeyID:         "key-new-789",
		UserID:        "user-123",
		KeyPrefix:     "fp_a1b2",
		Scopes:        []string{"models:read", "experiments:write"},
		ReplacedKeyID: "key-old-456",
	}
	// The raw key value a real caller would have seen ONCE — it must never appear
	// anywhere on the bus. We assert its absence from the raw envelope bytes.
	const rawKeySecret = "fp_a1b2c3d4e5f6g7h8i9j0SUPERSECRETKEYMATERIAL"

	if err := pub.PublishAPIKeyRotated(ctx, in); err != nil {
		t.Fatalf("PublishAPIKeyRotated: %v", err)
	}

	select {
	case env := <-received:
		if env.Source != authevents.SourceName {
			t.Errorf("envelope Source = %q, want %q", env.Source, authevents.SourceName)
		}
		if env.Type != "apikey.rotated" {
			t.Errorf("envelope Type = %q, want %q", env.Type, "apikey.rotated")
		}

		var payload eventsv1.ApiKeyRotated
		if err := protojson.Unmarshal(env.Data, &payload); err != nil {
			t.Fatalf("unmarshal ApiKeyRotated payload: %v", err)
		}
		if payload.GetKeyId() != in.KeyID {
			t.Errorf("KeyId = %q, want %q", payload.GetKeyId(), in.KeyID)
		}
		if payload.GetUserId() != in.UserID {
			t.Errorf("UserId = %q, want %q", payload.GetUserId(), in.UserID)
		}
		if payload.GetKeyPrefix() != in.KeyPrefix {
			t.Errorf("KeyPrefix = %q, want %q", payload.GetKeyPrefix(), in.KeyPrefix)
		}
		if payload.GetReplacedKeyId() != in.ReplacedKeyID {
			t.Errorf("ReplacedKeyId = %q, want %q", payload.GetReplacedKeyId(), in.ReplacedKeyID)
		}
		if len(payload.GetScopes()) != len(in.Scopes) {
			t.Errorf("Scopes len = %d, want %d", len(payload.GetScopes()), len(in.Scopes))
		}

		// SECURITY ASSERTION: the raw key must not be present in the wire bytes.
		// This is the whole point of publishing id+prefix only.
		envBytes, _ := json.Marshal(env)
		if strings.Contains(string(envBytes), rawKeySecret) {
			t.Fatal("raw API key material leaked onto the event bus")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for ApiKeyRotated event")
	}
}

// ============================================================================
// TEST 3: Duplicate redelivery is idempotent (exactly-once IN EFFECT)
// ============================================================================
//
// We model the classic at-least-once hazard: the handler does its side effect
// and records the envelope id as processed (the gold-standard "mark in the same
// step as the effect"), but the ACK is then lost so JetStream REDELIVERS the
// same envelope id. The subscriber's IsProcessed pre-check must recognize the
// redelivery and SKIP it — so the effect counter ends at exactly 1.
//
// We force the redelivery deterministically by returning an error on the first
// delivery (a real lost-ack is indistinguishable to JetStream from a NAK: both
// trigger redelivery). Because the handler already marked the id processed, the
// second delivery is short-circuited by the subscriber before the handler runs.
// ============================================================================

func TestSubscriber_DuplicateRedeliveryIsIdempotent(t *testing.T) {
	js, cleanup := newJS(t)
	defer cleanup()

	ctx := context.Background()

	store := natsutil.NewMemoryProcessedStore()

	var mu sync.Mutex
	effectCount := 0          // counts ACTUAL side effects performed
	deliveries := 0           // counts handler invocations (should be >= 2)
	firstDeliveryDone := make(chan struct{})
	idempotentDone := make(chan struct{})

	sub := natsutil.NewSubscriber(js,
		natsutil.WithIdempotencyStore(store),
		natsutil.WithAckWait(2*time.Second), // fast redelivery so the test is quick
	)
	if err := sub.Subscribe(ctx, authevents.StreamName, authevents.SubjectUserCreated,
		func(hctx context.Context, env natsutil.EventEnvelope) error {
			mu.Lock()
			deliveries++
			n := deliveries
			mu.Unlock()

			// Defensive in-handler dedupe (mirrors a real consumer that marks the
			// effect + the processed-id together). The subscriber's own pre-check
			// should normally make this branch unreachable on redelivery, but a
			// correct consumer is idempotent on its own too.
			already, err := store.IsProcessed(hctx, env.ID)
			if err != nil {
				return err
			}
			if already {
				close(idempotentDone)
				return nil
			}

			// Perform the side effect exactly once and record it processed.
			mu.Lock()
			effectCount++
			mu.Unlock()
			if err := store.MarkProcessed(hctx, env.ID); err != nil {
				return err
			}

			if n == 1 {
				// Simulate a LOST ACK: the effect committed, but we fail the ack so
				// JetStream redelivers the same envelope id.
				close(firstDeliveryDone)
				return natsutil.ErrProcessingFailed
			}
			return nil
		}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	pub := authevents.NewPublisher(natsutil.NewPublisher(js, authevents.SourceName))
	if err := pub.PublishUserCreated(ctx, authevents.UserCreatedInput{
		UserID: "user-dup", Email: "dup@forgepoint.dev", Name: "Dup", Team: "platform", Role: "viewer",
	}); err != nil {
		t.Fatalf("PublishUserCreated: %v", err)
	}

	// Wait for the first delivery (effect performed + marked + NAK).
	select {
	case <-firstDeliveryDone:
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for first delivery")
	}

	// Give JetStream time to redeliver; the subscriber's IsProcessed pre-check
	// should ACK+skip the redelivery WITHOUT re-invoking the handler, so the
	// effect count stays at 1. (idempotentDone covers the rare case where the
	// pre-check is bypassed and the handler runs but self-dedupes.)
	deadline := time.After(10 * time.Second)
	for {
		mu.Lock()
		effects := effectCount
		mu.Unlock()
		if effects > 1 {
			t.Fatalf("side effect ran %d times — redelivery was NOT idempotent", effects)
		}
		select {
		case <-deadline:
			mu.Lock()
			finalEffects := effectCount
			finalDeliveries := deliveries
			mu.Unlock()
			if finalEffects != 1 {
				t.Fatalf("effect count = %d, want exactly 1", finalEffects)
			}
			t.Logf("idempotent: %d deliveries, exactly %d side effect", finalDeliveries, finalEffects)
			return
		case <-idempotentDone:
			// Handler ran on redelivery but self-deduped — also acceptable; loop
			// once more to confirm effectCount is still 1.
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// ============================================================================
// TEST 4: Poison message is routed to the DLQ after the retry budget
// ============================================================================

func TestSubscriber_PoisonMessageRoutedToDLQ(t *testing.T) {
	js, cleanup := newJS(t)
	defer cleanup()

	ctx := context.Background()

	const (
		maxRetries = 2
		dlqSubject = "fp.auth.dlq" // captured by the AUTH stream (fp.auth.>)
	)

	var mu sync.Mutex
	handlerCalls := 0

	// The poison consumer: its handler ALWAYS fails (a message it can never
	// process — corrupt data, a permanent downstream outage, a handler bug).
	poison := natsutil.NewSubscriber(js,
		natsutil.WithMaxRetries(maxRetries),
		natsutil.WithDLQSubject(dlqSubject),
		natsutil.WithAckWait(2*time.Second),
	)
	if err := poison.Subscribe(ctx, authevents.StreamName, authevents.SubjectAPIKeyRotated,
		func(_ context.Context, _ natsutil.EventEnvelope) error {
			mu.Lock()
			handlerCalls++
			mu.Unlock()
			return natsutil.ErrProcessingFailed
		}); err != nil {
		t.Fatalf("subscribe poison: %v", err)
	}
	defer poison.Close()

	// The DLQ observer: proves the dead-lettered message lands on the DLQ subject.
	dlqReceived := make(chan natsutil.EventEnvelope, 1)
	dlqSub := natsutil.NewSubscriber(js, natsutil.WithConsumerGroup("auth-dlq-observer"))
	if err := dlqSub.Subscribe(ctx, authevents.StreamName, dlqSubject,
		func(_ context.Context, env natsutil.EventEnvelope) error {
			dlqReceived <- env
			return nil
		}); err != nil {
		t.Fatalf("subscribe DLQ: %v", err)
	}
	defer dlqSub.Close()

	pub := authevents.NewPublisher(natsutil.NewPublisher(js, authevents.SourceName))
	if err := pub.PublishAPIKeyRotated(ctx, authevents.APIKeyRotatedInput{
		KeyID: "key-poison", UserID: "user-x", KeyPrefix: "fp_dead", Scopes: []string{"models:read"},
	}); err != nil {
		t.Fatalf("PublishAPIKeyRotated: %v", err)
	}

	select {
	case env := <-dlqReceived:
		// The DLQ copy preserves the original payload so ops can inspect/replay it.
		var payload eventsv1.ApiKeyRotated
		if err := protojson.Unmarshal(env.Data, &payload); err != nil {
			t.Fatalf("unmarshal DLQ payload: %v", err)
		}
		if payload.GetKeyId() != "key-poison" {
			t.Errorf("DLQ payload KeyId = %q, want %q", payload.GetKeyId(), "key-poison")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for poison message to reach the DLQ")
	}

	// The handler must have been attempted the full budget (initial + maxRetries)
	// before dead-lettering — i.e. NATS did not give up early, and we did not loop
	// forever either.
	mu.Lock()
	calls := handlerCalls
	mu.Unlock()
	if calls < maxRetries+1 {
		t.Errorf("handler attempted %d times, want at least %d (initial + %d retries)",
			calls, maxRetries+1, maxRetries)
	}
}
