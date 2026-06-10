package natsutil_test

import (
	"context"
	"encoding/json"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/testcontainers/testcontainers-go"
	natscontainer "github.com/testcontainers/testcontainers-go/modules/nats"
)

// skipIfNoDocker skips the test if Docker daemon is not available.
// Integration tests require Docker for testcontainers.
func skipIfNoDocker(t *testing.T) {
	t.Helper()
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("Docker not available, skipping integration test")
	}
}

// ============================================================================
// NATS INTEGRATION TESTS
// ============================================================================
//
// WHY real NATS instead of mocks:
//   NATS behavior (JetStream acknowledgment, consumer groups, message
//   redelivery, DLQ) is complex and stateful. Mocking it accurately is
//   harder than running a real NATS server. Testcontainers spins up a
//   real NATS server in ~2 seconds — fast enough for CI.
//
// WHAT WE TEST:
//   1. Event envelope serialization/deserialization
//   2. Publish → subscribe round-trip
//   3. Consumer groups (queue subscription: 2 subscribers, only 1 gets each msg)
//   4. DLQ: after max retries, message goes to dead letter subject
//
// HOW TESTCONTAINERS WORKS:
//   1. Pulls the NATS Docker image (cached after first run)
//   2. Starts a container with JetStream enabled
//   3. Returns the connection URL (dynamic port to avoid conflicts)
//   4. Container is automatically cleaned up when the test finishes
//
// ============================================================================

// startNATS spins up a real NATS server with JetStream enabled via testcontainers.
// Returns the connection URL and a cleanup function.
func startNATS(t *testing.T) string {
	t.Helper()

	ctx := context.Background()

	// JetStream is enabled by default in the testcontainers NATS module
	// (the module passes -DV -js flags automatically).
	container, err := natscontainer.Run(ctx, "nats:2.11")
	if err != nil {
		t.Fatalf("failed to start NATS container: %v", err)
	}

	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("failed to terminate NATS container: %v", err)
		}
	})

	url, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("failed to get NATS connection string: %v", err)
	}

	return url
}

// ============================================================================
// ENVELOPE TESTS
// ============================================================================

func TestNewEnvelope_GeneratesUniqueIDs(t *testing.T) {
	env1 := natsutil.NewEnvelope("test.event", "test-service", []byte(`{"key":"val1"}`))
	env2 := natsutil.NewEnvelope("test.event", "test-service", []byte(`{"key":"val2"}`))

	if env1.ID == "" {
		t.Error("envelope ID is empty")
	}
	if env1.ID == env2.ID {
		t.Error("two envelopes have the same ID — UUIDs should be unique")
	}
}

func TestNewEnvelope_SetsFields(t *testing.T) {
	data := []byte(`{"model_id":"abc"}`)
	env := natsutil.NewEnvelope("model.registered", "registry", data)

	if env.Type != "model.registered" {
		t.Errorf("Type = %q, want %q", env.Type, "model.registered")
	}
	if env.Source != "registry" {
		t.Errorf("Source = %q, want %q", env.Source, "registry")
	}
	if env.Timestamp.IsZero() {
		t.Error("Timestamp is zero")
	}
	if string(env.Data) != string(data) {
		t.Errorf("Data = %q, want %q", string(env.Data), string(data))
	}
}

func TestEnvelope_JSONRoundTrip(t *testing.T) {
	original := natsutil.NewEnvelope("test.event", "test", []byte(`{"foo":"bar"}`))

	// Marshal
	bytes, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	// Unmarshal
	var decoded natsutil.EventEnvelope
	if err := json.Unmarshal(bytes, &decoded); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if decoded.ID != original.ID {
		t.Errorf("ID = %q, want %q", decoded.ID, original.ID)
	}
	if decoded.Type != original.Type {
		t.Errorf("Type = %q, want %q", decoded.Type, original.Type)
	}
}

// ============================================================================
// PUBLISH + SUBSCRIBE ROUND-TRIP
// ============================================================================

func TestPublishAndSubscribe(t *testing.T) {
	skipIfNoDocker(t)
	url := startNATS(t)

	// Connect to NATS
	conn, js, err := natsutil.Connect(url)
	if err != nil {
		t.Fatalf("connect error: %v", err)
	}
	defer conn.Close()

	// Create stream for the subject
	ctx := context.Background()
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"fp.test.>"},
	})
	if err != nil {
		t.Fatalf("create stream error: %v", err)
	}

	// Create publisher and subscriber
	pub := natsutil.NewPublisher(js, "test-service")

	received := make(chan natsutil.EventEnvelope, 1)
	sub := natsutil.NewSubscriber(js)
	err = sub.Subscribe(ctx, "TEST", "fp.test.>", func(ctx context.Context, env natsutil.EventEnvelope) error {
		received <- env
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe error: %v", err)
	}

	// Publish an event
	type TestPayload struct {
		ModelID string `json:"model_id"`
	}

	err = pub.Publish(ctx, "fp.test.model.registered", TestPayload{ModelID: "model-123"})
	if err != nil {
		t.Fatalf("publish error: %v", err)
	}

	// Wait for the event
	select {
	case env := <-received:
		if env.Type != "model.registered" {
			t.Errorf("Type = %q, want %q", env.Type, "model.registered")
		}
		if env.Source != "test-service" {
			t.Errorf("Source = %q, want %q", env.Source, "test-service")
		}
		// Verify payload
		var payload TestPayload
		if err := json.Unmarshal(env.Data, &payload); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}
		if payload.ModelID != "model-123" {
			t.Errorf("ModelID = %q, want %q", payload.ModelID, "model-123")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for event")
	}
}

// ============================================================================
// CONSUMER GROUP (QUEUE SUBSCRIPTION)
// ============================================================================
//
// WHY: In a multi-replica deployment (3 replicas of billing service), each
// event should be processed by exactly ONE replica, not all three. NATS
// consumer groups (queue subscriptions) distribute messages across consumers
// in the same group — each message goes to ONE consumer.
//
// Without consumer groups:
//   Event → Billing-1 processes it (deducts quota)
//   Event → Billing-2 also processes it (deducts quota AGAIN)
//   Event → Billing-3 also processes it (deducts quota TRIPLE)
//
// With consumer groups (name = "billing-consumers"):
//   Event → NATS delivers to exactly ONE of Billing-1/2/3
// ============================================================================

func TestConsumerGroup_OnlyOneReceives(t *testing.T) {
	skipIfNoDocker(t)
	url := startNATS(t)

	conn, js, err := natsutil.Connect(url)
	if err != nil {
		t.Fatalf("connect error: %v", err)
	}
	defer conn.Close()

	ctx := context.Background()
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "CG_TEST",
		Subjects: []string{"fp.cg.>"},
	})
	if err != nil {
		t.Fatalf("create stream error: %v", err)
	}

	// Track which subscriber receives the message
	var mu sync.Mutex
	receivedBy := make([]int, 0)

	// Create two subscribers in the same consumer group
	for i := range 2 {
		subscriberID := i
		sub := natsutil.NewSubscriber(js, natsutil.WithConsumerGroup("test-group"))
		err := sub.Subscribe(ctx, "CG_TEST", "fp.cg.>", func(ctx context.Context, env natsutil.EventEnvelope) error {
			mu.Lock()
			receivedBy = append(receivedBy, subscriberID)
			mu.Unlock()
			return nil
		})
		if err != nil {
			t.Fatalf("subscribe %d error: %v", i, err)
		}
	}

	// Publish one event
	pub := natsutil.NewPublisher(js, "test")
	if err := pub.Publish(ctx, "fp.cg.event", map[string]string{"key": "value"}); err != nil {
		t.Fatalf("publish error: %v", err)
	}

	// Wait and check
	time.Sleep(3 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if len(receivedBy) != 1 {
		t.Errorf("event received by %d subscribers, want exactly 1 (got: %v)", len(receivedBy), receivedBy)
	}
}

// ============================================================================
// DLQ (DEAD LETTER QUEUE)
// ============================================================================
//
// WHY DLQ: Some messages are "poison pills" — they always fail processing
// (corrupt data, missing dependency, bug in handler). Without DLQ, NATS
// redelivers them forever, blocking other messages in the queue.
//
// DLQ FLOW:
//   1. Message arrives → handler returns error → NAK (negative ack)
//   2. NATS redelivers after backoff (1s, 2s, 4s...)
//   3. After MaxRetries failures → publish to DLQ subject
//   4. DLQ subject is monitored by alerting (Grafana alert rule)
//   5. Ops team investigates, fixes the issue, replays from DLQ
//
// HOW UBER/AWS DO IT:
//   - Uber: Kafka DLQ topics with automatic replay tooling
//   - AWS SQS: Built-in DLQ with maxReceiveCount
//   - We implement the same pattern on NATS JetStream
// ============================================================================

func TestDLQ_AfterMaxRetries(t *testing.T) {
	skipIfNoDocker(t)
	url := startNATS(t)

	conn, js, err := natsutil.Connect(url)
	if err != nil {
		t.Fatalf("connect error: %v", err)
	}
	defer conn.Close()

	ctx := context.Background()

	// Create stream for both the main subject and the DLQ subject
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "DLQ_TEST",
		Subjects: []string{"fp.dlq.>"},
	})
	if err != nil {
		t.Fatalf("create stream error: %v", err)
	}

	// Track handler invocations
	var mu sync.Mutex
	handlerCalls := 0
	maxRetries := 3

	// Subscribe with DLQ configured
	dlqReceived := make(chan natsutil.EventEnvelope, 1)

	sub := natsutil.NewSubscriber(js,
		natsutil.WithMaxRetries(maxRetries),
		natsutil.WithDLQSubject("fp.dlq.dead"),
	)

	// Handler always fails — simulates a poison pill message
	err = sub.Subscribe(ctx, "DLQ_TEST", "fp.dlq.events.>", func(ctx context.Context, env natsutil.EventEnvelope) error {
		mu.Lock()
		handlerCalls++
		mu.Unlock()
		return natsutil.ErrProcessingFailed
	})
	if err != nil {
		t.Fatalf("subscribe error: %v", err)
	}

	// Subscribe to DLQ to verify the message lands there
	dlqSub := natsutil.NewSubscriber(js)
	err = dlqSub.Subscribe(ctx, "DLQ_TEST", "fp.dlq.dead", func(ctx context.Context, env natsutil.EventEnvelope) error {
		dlqReceived <- env
		return nil
	})
	if err != nil {
		t.Fatalf("DLQ subscribe error: %v", err)
	}

	// Publish a message that will always fail
	pub := natsutil.NewPublisher(js, "test")
	if err := pub.Publish(ctx, "fp.dlq.events.fail", map[string]string{"poison": "true"}); err != nil {
		t.Fatalf("publish error: %v", err)
	}

	// Wait for DLQ message
	select {
	case env := <-dlqReceived:
		t.Logf("message landed in DLQ after retries: type=%s", env.Type)
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for DLQ message")
	}

	// Verify handler was called at least maxRetries times
	mu.Lock()
	defer mu.Unlock()
	if handlerCalls < maxRetries {
		t.Errorf("handler called %d times, want at least %d", handlerCalls, maxRetries)
	}
}
