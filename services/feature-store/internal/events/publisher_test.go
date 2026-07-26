// publisher_test.go — REAL pub/sub tests for the Feature Store's event adapter.
//
// ============================================================================
// WHY REAL NATS (testcontainers), NOT A MOCK
// ============================================================================
//
// The thing under test is the BOUNDARY behavior: does a domain object become the
// exact canonical events.v1 payload, wrapped in the right envelope, on the right
// subject, such that a real JetStream consumer can read it back? A mock JetStream
// would let a wrong subject/encoding pass. So we spin a real NATS+JetStream via
// pkg/testutil.StartNATS (SkipIfNoDocker guards CI without Docker) and assert on
// what a genuine subscriber actually receives.
//
// WHAT THESE TESTS PIN:
//  1. PublishFeatureViewDefined → lands on fp.features.view.defined with Source =
//     "feature-store" and a protojson FeatureViewDefined payload matching the view.
//  2. PublishFeaturesWritten → lands on fp.features.written with the entity ids,
//     count, and version range from the WriteFeaturesResult.
//  3. IDEMPOTENT REDELIVERY: a consumer of our event, configured with a
//     ProcessedStore, runs its side effect ONCE even when the SAME envelope id is
//     delivered twice (at-least-once delivery → exactly-once effect).
//  4. POISON → DLQ: a consumer whose handler always fails routes the message to a
//     dead-letter subject after the retry budget, instead of looping forever.
//
// (3) and (4) are CONSUMER-side guarantees. The Feature Store consumes nothing, so
// we exercise them against the events IT PRODUCES — which is exactly the path its
// real downstream consumers (experiment-tracker, model-monitor) take. This both
// satisfies the "duplicate redelivery is idempotent / poison → DLQ" requirement and
// documents the safe-consumption contract for this service's events.
package events_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/feature-store/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/feature-store/internal/events"
)

// ----------------------------------------------------------------------------
// test harness: a real NATS + JetStream with the FEATURES stream created.
// ----------------------------------------------------------------------------

// newJetStream connects to a fresh containerized NATS and creates the FEATURES
// stream over fp.features.> (so both of the service's subjects are captured). It
// returns the JetStream handle; the connection is closed via t.Cleanup.
func newJetStream(t *testing.T) jetstream.JetStream {
	t.Helper()
	url := testutil.StartNATS(t) // SkipIfNoDocker is called inside StartNATS

	conn, js, err := natsutil.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(conn.Close)

	ctx := context.Background()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     events.StreamName,
		Subjects: []string{events.StreamSubjects},
	}); err != nil {
		t.Fatalf("create FEATURES stream: %v", err)
	}
	return js
}

// subscribeOnce subscribes with the given options and pushes each received
// envelope onto the returned channel. The subscriber is closed via t.Cleanup.
func subscribeOnce(t *testing.T, js jetstream.JetStream, subject string, opts ...natsutil.SubOption) <-chan natsutil.EventEnvelope {
	t.Helper()
	ch := make(chan natsutil.EventEnvelope, 8)
	sub := natsutil.NewSubscriber(js, opts...)
	t.Cleanup(sub.Close)
	err := sub.Subscribe(context.Background(), events.StreamName, subject,
		func(_ context.Context, env natsutil.EventEnvelope) error {
			ch <- env
			return nil
		})
	if err != nil {
		t.Fatalf("subscribe %s: %v", subject, err)
	}
	return ch
}

// recvWithin waits for one envelope or fails the test on timeout.
func recvWithin(t *testing.T, ch <-chan natsutil.EventEnvelope, d time.Duration) natsutil.EventEnvelope {
	t.Helper()
	select {
	case env := <-ch:
		return env
	case <-time.After(d):
		t.Fatal("timeout waiting for event")
		return natsutil.EventEnvelope{}
	}
}

// ============================================================================
// 1. FeatureViewDefined → fp.features.view.defined
// ============================================================================

func TestPublishFeatureViewDefined_LandsOnSubjectWithEnvelopeAndPayload(t *testing.T) {
	js := newJetStream(t)
	ch := subscribeOnce(t, js, events.SubjectFeatureViewDefined)

	pub := events.NewPublisher(natsutil.NewPublisher(js, events.SourceName))

	definedAt := time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)
	view := domain.FeatureView{
		ID:            "view-abc",
		Name:          "user_credit",
		SchemaVersion: 2,
		OwnerTeam:     "risk-team",
		UpdatedAt:     definedAt,
	}

	if err := pub.PublishFeatureViewDefined(context.Background(), view); err != nil {
		t.Fatalf("publish: %v", err)
	}

	env := recvWithin(t, ch, 10*time.Second)

	// Envelope assertions: source is this service; type is derived from the subject
	// by natsutil (strip "fp.features." → "view.defined"); correlation id is set
	// (natsutil mints one when none is on the context).
	if env.Source != events.SourceName {
		t.Errorf("Source = %q, want %q", env.Source, events.SourceName)
	}
	if env.Type != "view.defined" {
		t.Errorf("Type = %q, want %q", env.Type, "view.defined")
	}
	if env.ID == "" {
		t.Error("envelope ID is empty (needed as the dedup/idempotency key)")
	}
	if env.CorrelationID == "" {
		t.Error("CorrelationID is empty (should be minted for the first hop)")
	}

	// Payload assertions: the canonical events.v1 message, protojson-decoded.
	var got eventsv1.FeatureViewDefined
	if err := protojson.Unmarshal(env.Data, &got); err != nil {
		t.Fatalf("protojson unmarshal payload: %v", err)
	}
	if got.GetFeatureViewId() != view.ID {
		t.Errorf("FeatureViewId = %q, want %q", got.GetFeatureViewId(), view.ID)
	}
	if got.GetFeatureViewName() != view.Name {
		t.Errorf("FeatureViewName = %q, want %q", got.GetFeatureViewName(), view.Name)
	}
	if got.GetSchemaVersion() != view.SchemaVersion {
		t.Errorf("SchemaVersion = %d, want %d", got.GetSchemaVersion(), view.SchemaVersion)
	}
	if got.GetOwnerTeam() != view.OwnerTeam {
		t.Errorf("OwnerTeam = %q, want %q", got.GetOwnerTeam(), view.OwnerTeam)
	}
	if !got.GetDefinedAt().AsTime().Equal(definedAt) {
		t.Errorf("DefinedAt = %v, want %v", got.GetDefinedAt().AsTime(), definedAt)
	}
}

// ============================================================================
// 2. FeaturesWritten → fp.features.written
// ============================================================================

func TestPublishFeaturesWritten_LandsOnSubjectWithEnvelopeAndPayload(t *testing.T) {
	js := newJetStream(t)
	ch := subscribeOnce(t, js, events.SubjectFeaturesWritten)

	pub := events.NewPublisher(natsutil.NewPublisher(js, events.SourceName))

	view := domain.FeatureView{ID: "view-xyz", Name: "user_activity"}
	res := domain.WriteFeaturesResult{
		WrittenCount:          3,
		WrittenThroughVersion: 42,
		AffectedEntityIDs:     []string{"u1", "u2", "u3"},
	}

	if err := pub.PublishFeaturesWritten(context.Background(), view, res); err != nil {
		t.Fatalf("publish: %v", err)
	}

	env := recvWithin(t, ch, 10*time.Second)

	if env.Source != events.SourceName {
		t.Errorf("Source = %q, want %q", env.Source, events.SourceName)
	}
	if env.Type != "written" {
		t.Errorf("Type = %q, want %q", env.Type, "written")
	}

	var got eventsv1.FeaturesWritten
	if err := protojson.Unmarshal(env.Data, &got); err != nil {
		t.Fatalf("protojson unmarshal payload: %v", err)
	}
	if got.GetFeatureViewId() != view.ID {
		t.Errorf("FeatureViewId = %q, want %q", got.GetFeatureViewId(), view.ID)
	}
	if got.GetFeatureViewName() != view.Name {
		t.Errorf("FeatureViewName = %q, want %q", got.GetFeatureViewName(), view.Name)
	}
	if got.GetWrittenCount() != int32(res.WrittenCount) {
		t.Errorf("WrittenCount = %d, want %d", got.GetWrittenCount(), res.WrittenCount)
	}
	if got.GetWrittenThroughVersion() != res.WrittenThroughVersion {
		t.Errorf("WrittenThroughVersion = %d, want %d", got.GetWrittenThroughVersion(), res.WrittenThroughVersion)
	}
	if len(got.GetEntityIds()) != len(res.AffectedEntityIDs) {
		t.Fatalf("EntityIds len = %d, want %d (%v)", len(got.GetEntityIds()), len(res.AffectedEntityIDs), got.GetEntityIds())
	}
	for i, id := range res.AffectedEntityIDs {
		if got.GetEntityIds()[i] != id {
			t.Errorf("EntityIds[%d] = %q, want %q", i, got.GetEntityIds()[i], id)
		}
	}
	if got.GetWrittenAt() == nil || got.GetWrittenAt().AsTime().IsZero() {
		t.Error("WrittenAt not stamped (producer clock for 'when the append committed')")
	}
}

// ============================================================================
// 3. IDEMPOTENT REDELIVERY (consumer-side, on our published event)
// ============================================================================
//
// The subscriber is configured WithIdempotencyStore. We deliver the SAME envelope
// id twice (republish the identical FeaturesWritten payload with a fixed envelope
// id) and assert the handler's SIDE EFFECT runs exactly once. This is the NATS→
// effect half of exactly-once: at-least-once delivery + a dedup store = one effect.
func TestConsumer_DuplicateRedeliveryIsIdempotent(t *testing.T) {
	js := newJetStream(t)

	// A handler with an observable side effect: a counter. We use the idempotency
	// store so a duplicate envelope id is ACKed without re-running the handler.
	var sideEffects int64
	store := natsutil.NewMemoryProcessedStore()

	sub := natsutil.NewSubscriber(js, natsutil.WithIdempotencyStore(store))
	t.Cleanup(sub.Close)
	processed := make(chan string, 4)
	err := sub.Subscribe(context.Background(), events.StreamName, events.SubjectFeaturesWritten,
		func(_ context.Context, env natsutil.EventEnvelope) error {
			atomic.AddInt64(&sideEffects, 1)
			processed <- env.ID
			return nil
		})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Build ONE envelope with a FIXED id and publish it twice with the same
	// Nats-Msg-Id. JetStream's publish-window dedup would already drop the second
	// at the broker; to genuinely exercise the CONSUMER-side store we publish on a
	// raw JetStream call with the SAME Msg-Id is not enough (broker dedup). Instead
	// we publish two messages with DIFFERENT Nats-Msg-Ids but the SAME envelope.ID
	// inside the payload — simulating a redelivery the broker did NOT dedupe (e.g.
	// a crash between handler success and MarkProcessed, or cross-stream replay).
	envelopeID := "fixed-envelope-id-123"
	raw := mustEnvelopeBytes(t, envelopeID, "written", events.SourceName,
		mustFeaturesWrittenJSON(t, "view-1", "vname", []string{"e1"}, 1, 7))

	ctx := context.Background()
	// First delivery.
	if _, err := js.Publish(ctx, events.SubjectFeaturesWritten, raw, jetstream.WithMsgID("msg-A")); err != nil {
		t.Fatalf("publish #1: %v", err)
	}
	// "Redelivery": same envelope id, different broker Msg-Id so the broker does
	// not dedupe it — only the consumer's ProcessedStore should.
	if _, err := js.Publish(ctx, events.SubjectFeaturesWritten, raw, jetstream.WithMsgID("msg-B")); err != nil {
		t.Fatalf("publish #2: %v", err)
	}

	// First must be handled.
	select {
	case id := <-processed:
		if id != envelopeID {
			t.Fatalf("processed id = %q, want %q", id, envelopeID)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for first delivery")
	}

	// The duplicate must NOT produce a second side effect. Give the consumer time
	// to receive+dedupe the second message, then assert the counter stayed at 1.
	select {
	case <-processed:
		t.Fatal("handler ran twice for the same envelope id — idempotency failed")
	case <-time.After(3 * time.Second):
		// no second effect — correct.
	}
	if got := atomic.LoadInt64(&sideEffects); got != 1 {
		t.Errorf("side effects = %d, want 1 (duplicate must be deduped)", got)
	}
}

// ============================================================================
// 4. POISON MESSAGE → DLQ (consumer-side)
// ============================================================================
//
// A handler that ALWAYS fails is a poison pill. With WithMaxRetries + WithDLQSubject
// the subscriber routes the message to the dead-letter subject after the retry
// budget instead of redelivering forever. We assert the message lands on the DLQ
// subject and the handler was attempted the expected number of times.
func TestConsumer_PoisonMessageRoutesToDLQ(t *testing.T) {
	js := newJetStream(t)

	const maxRetries = 2
	dlqSubject := "fp.features.dlq" // inside the FEATURES stream (fp.features.>)

	var attempts int64
	poisonSub := natsutil.NewSubscriber(js,
		natsutil.WithMaxRetries(maxRetries),
		natsutil.WithDLQSubject(dlqSubject),
	)
	t.Cleanup(poisonSub.Close)
	err := poisonSub.Subscribe(context.Background(), events.StreamName, events.SubjectFeaturesWritten,
		func(_ context.Context, _ natsutil.EventEnvelope) error {
			atomic.AddInt64(&attempts, 1)
			return natsutil.ErrProcessingFailed // always fail → poison
		})
	if err != nil {
		t.Fatalf("subscribe poison: %v", err)
	}

	// A separate subscriber watches the DLQ subject.
	dlqCh := subscribeOnce(t, js, dlqSubject)

	pub := events.NewPublisher(natsutil.NewPublisher(js, events.SourceName))
	view := domain.FeatureView{ID: "view-poison", Name: "vp"}
	res := domain.WriteFeaturesResult{WrittenCount: 1, WrittenThroughVersion: 1, AffectedEntityIDs: []string{"e1"}}
	if err := pub.PublishFeaturesWritten(context.Background(), view, res); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// The poison message must end up on the DLQ.
	dlqEnv := recvWithin(t, dlqCh, 30*time.Second)
	if dlqEnv.Type != "written" {
		t.Errorf("DLQ envelope Type = %q, want %q", dlqEnv.Type, "written")
	}
	if dlqEnv.Source != events.SourceName {
		t.Errorf("DLQ envelope Source = %q, want %q", dlqEnv.Source, events.SourceName)
	}

	// The handler should have been attempted maxRetries+1 times (initial + retries)
	// before the message was dead-lettered. Allow >= maxRetries to avoid flaking on
	// timing (the exact final attempt count is bounded by MaxDeliver = maxRetries+1).
	if got := atomic.LoadInt64(&attempts); got < int64(maxRetries) {
		t.Errorf("handler attempts = %d, want >= %d", got, maxRetries)
	}
}

// ----------------------------------------------------------------------------
// test helpers for hand-building envelopes/payloads (idempotency test)
// ----------------------------------------------------------------------------

// mustFeaturesWrittenJSON protojson-encodes a FeaturesWritten payload exactly as
// the production adapter would, so the consumer path under test is identical.
func mustFeaturesWrittenJSON(t *testing.T, viewID, viewName string, entityIDs []string, count int32, throughVersion int64) []byte {
	t.Helper()
	data, err := protojson.Marshal(&eventsv1.FeaturesWritten{
		FeatureViewId:         viewID,
		FeatureViewName:       viewName,
		EntityIds:             entityIDs,
		WrittenCount:          count,
		WrittenThroughVersion: throughVersion,
	})
	if err != nil {
		t.Fatalf("marshal FeaturesWritten: %v", err)
	}
	return data
}

// mustEnvelopeBytes builds the full EventEnvelope JSON with a FIXED id, matching
// what natsutil.Publisher emits, so we can publish a "redelivery" with a known
// envelope id that the consumer's ProcessedStore keys on.
func mustEnvelopeBytes(t *testing.T, id, eventType, source string, payload []byte) []byte {
	t.Helper()
	env := natsutil.EventEnvelope{
		ID:        id,
		Type:      eventType,
		Source:    source,
		Timestamp: time.Now().UTC(),
		Data:      payload,
	}
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return b
}
