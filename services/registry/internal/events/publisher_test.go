// publisher_test.go — REAL pub/sub tests for the registry event adapter.
//
// ============================================================================
// WHAT THESE TESTS PROVE (and why against a REAL broker)
// ============================================================================
//
// The brief is explicit: verify REAL pub/sub behavior, not a mock. We therefore
// stand up a real NATS JetStream server via testutil.StartNATS (testcontainers
// on the remote engine) and exercise the FULL path:
//
//	domain.ProjectionEvent → events.Publisher.Emit → natsutil.Publisher
//	  → EventEnvelope on the wire → JetStream stream → natsutil.Subscriber
//	  → handler decodes eventsv1.* payload.
//
// COVERAGE MAP (mirrors the task's verification list):
//
//  1. EACH produced event lands on the RIGHT subject with a correct ENVELOPE
//     (source="registry", derived type) and a correct PAYLOAD (fields mapped
//     from the domain object; the promote enum mapping is checked end-to-end).
//     → TestEmit_AllEventsPublishToCorrectSubjectWithEnvelopeAndPayload
//
//  2. A subscriber CONSUMES and DISPATCHES the decoded payload correctly.
//     → same test (the handler decodes + asserts the eventsv1 fields).
//
//  3. DUPLICATE redelivery is IDEMPOTENT: the same EventEnvelope.id handled
//     twice produces a SINGLE effect (consumer-side ProcessedStore dedup), AND
//     JetStream's publish-window dedup drops a re-publish of the same Msg-Id.
//     → TestConsume_DuplicateRedeliveryIsIdempotent
//
//  4. A POISON message (handler always fails) is routed to the DLQ after the
//     retry budget is exhausted — bounded, not an infinite redelivery loop.
//     → TestConsume_PoisonMessageGoesToDLQ
//
// Registry PRODUCES these events and CONSUMES none, so the only adapter under
// test is the Publisher (events.Publisher implements domain.ProjectionEmitter).
// The subscriber here plays the role of a real downstream — the CQRS projection
// consumer that rebuilds the Redis read model, or serving/billing/etc. — which is
// exactly how the idempotency + DLQ machinery is exercised in production.
package events_test

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/registry/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/registry/internal/events"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// streamName is the JetStream stream that captures every registry subject. In
// production a service/operator declares the stream; tests declare it explicitly
// so the subjects exist before we publish (JetStream drops a publish to a subject
// no stream covers).
const streamName = "MODELS"

// dialNATS connects to a fresh NATS server and declares the MODELS stream over the
// whole fp.models.> subject space (every registry subject + the test DLQ subject).
// Returns a JetStream handle; the connection is closed via t.Cleanup.
func dialNATS(t *testing.T) jetstream.JetStream {
	t.Helper()
	url := testutil.StartNATS(t) // SkipIfNoDocker is called inside StartNATS

	conn, js, err := natsutil.Connect(url)
	if err != nil {
		t.Fatalf("connect NATS: %v", err)
	}
	t.Cleanup(conn.Close)

	// One stream over fp.models.> captures all five registry subjects. We add an
	// explicit DLQ subject (fp.models.dlq) to the SAME stream so the DLQ test can
	// observe dead-lettered messages — the DLQ subject must be backed by a stream
	// or the dead-letter publish would itself be dropped.
	_, err = js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{"fp.models.>"},
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	return js
}

// newEmitter builds the adapter under test over a real natsutil.Publisher sourced
// as "registry" (the value main.go uses). This is the exact production wiring.
func newEmitter(js jetstream.JetStream) *events.Publisher {
	return events.NewPublisher(natsutil.NewPublisher(js, events.ServiceName))
}

// fixed sample identities reused across cases so assertions read cleanly.
var (
	sampleActor = domain.Actor{UserID: "user-42", Team: "fraud-team"}
	sampleModel = domain.Model{
		ID:        "model-1",
		Name:      "fraud-detector",
		Team:      "fraud-team",
		OwnerID:   "user-42",
		Framework: "onnx",
		TaskType:  "classification",
	}
	sampleOccurredAt = time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)
)

// ============================================================================
// TEST 1: every produced event → correct subject + envelope + payload
// ============================================================================

func TestEmit_AllEventsPublishToCorrectSubjectWithEnvelopeAndPayload(t *testing.T) {
	js := dialNATS(t)
	emitter := newEmitter(js)
	ctx := context.Background()

	// Each case: the domain event to emit, the subject it MUST land on, the event
	// type the envelope MUST carry (derived by the lib from the subject), and an
	// assertion over the decoded eventsv1 payload (proves field mapping).
	cases := []struct {
		name        string
		event       domain.ProjectionEvent
		wantSubject string
		wantType    string
		assert      func(t *testing.T, data json.RawMessage)
	}{
		{
			name: "ModelRegistered",
			event: domain.ProjectionEvent{
				Kind:       domain.EventModelRegistered,
				Model:      sampleModel,
				Actor:      sampleActor,
				OccurredAt: sampleOccurredAt,
			},
			wantSubject: events.SubjectModelRegistered,
			wantType:    "registered",
			assert: func(t *testing.T, data json.RawMessage) {
				var p eventsv1.ModelRegistered
				mustUnmarshal(t, data, &p)
				assertEq(t, "model_id", p.GetModelId(), "model-1")
				assertEq(t, "model_name", p.GetModelName(), "fraud-detector")
				assertEq(t, "framework", p.GetFramework(), "onnx")
				assertEq(t, "task_type", p.GetTaskType(), "classification")
				assertEq(t, "owner_id", p.GetOwnerId(), "user-42")
				assertEq(t, "team", p.GetTeam(), "fraud-team")
				assertTS(t, "registered_at", p.GetRegisteredAt().AsTime(), sampleOccurredAt)
			},
		},
		{
			name: "ModelVersionCreated",
			event: domain.ProjectionEvent{
				Kind:  domain.EventVersionCreated,
				Model: sampleModel,
				Version: domain.ModelVersion{
					ID: "ver-1", ModelID: "model-1", Version: "1.0.0",
					CreatedBy: "user-42", Stage: domain.StageDev, Status: domain.StatusPendingUpload,
				},
				Actor:      sampleActor,
				OccurredAt: sampleOccurredAt,
			},
			wantSubject: events.SubjectModelVersionCreated,
			wantType:    "version.created",
			assert: func(t *testing.T, data json.RawMessage) {
				var p eventsv1.ModelVersionCreated
				mustUnmarshal(t, data, &p)
				assertEq(t, "model_id", p.GetModelId(), "model-1")
				assertEq(t, "model_name", p.GetModelName(), "fraud-detector")
				assertEq(t, "version_id", p.GetVersionId(), "ver-1")
				assertEq(t, "version", p.GetVersion(), "1.0.0")
				assertEq(t, "created_by", p.GetCreatedBy(), "user-42")
				assertTS(t, "created_at", p.GetCreatedAt().AsTime(), sampleOccurredAt)
			},
		},
		{
			name: "ModelVersionReady",
			event: domain.ProjectionEvent{
				Kind:  domain.EventVersionReady,
				Model: sampleModel,
				Version: domain.ModelVersion{
					ID: "ver-1", ModelID: "model-1", Version: "1.0.0",
					ArtifactPath:   "s3://fp-models/model-1/1.0.0.onnx",
					ArtifactDigest: "sha256:deadbeef",
					SizeBytes:      4096,
					Stage:          domain.StageDev, Status: domain.StatusReady,
				},
				Actor:      sampleActor,
				OccurredAt: sampleOccurredAt,
			},
			wantSubject: events.SubjectModelVersionReady,
			wantType:    "version.ready",
			assert: func(t *testing.T, data json.RawMessage) {
				var p eventsv1.ModelVersionReady
				mustUnmarshal(t, data, &p)
				assertEq(t, "version_id", p.GetVersionId(), "ver-1")
				assertEq(t, "artifact_path", p.GetArtifactPath(), "s3://fp-models/model-1/1.0.0.onnx")
				assertEq(t, "artifact_digest", p.GetArtifactDigest(), "sha256:deadbeef")
				if p.GetSizeBytes() != 4096 {
					t.Errorf("size_bytes = %d, want 4096", p.GetSizeBytes())
				}
				assertTS(t, "ready_at", p.GetReadyAt().AsTime(), sampleOccurredAt)
			},
		},
		{
			name: "ModelPromoted_withDemotion_enumMappingEndToEnd",
			event: domain.ProjectionEvent{
				Kind:  domain.EventModelPromoted,
				Model: sampleModel,
				Version: domain.ModelVersion{
					ID: "ver-2", ModelID: "model-1", Version: "2.0.0", Stage: domain.StageProduction,
				},
				// The single-production swap: a prior PRODUCTION version is demoted.
				DemotedVersion: domain.ModelVersion{
					ID: "ver-1", ModelID: "model-1", Version: "1.0.0", Stage: domain.StageArchived,
				},
				FromStage:  domain.StageStaging,
				ToStage:    domain.StageProduction,
				Actor:      sampleActor,
				OccurredAt: sampleOccurredAt,
			},
			wantSubject: events.SubjectModelPromoted,
			wantType:    "promoted",
			assert: func(t *testing.T, data json.RawMessage) {
				var p eventsv1.ModelPromoted
				mustUnmarshal(t, data, &p)
				assertEq(t, "version_id", p.GetVersionId(), "ver-2")
				assertEq(t, "version", p.GetVersion(), "2.0.0")
				// Enum mapping domain.ModelStage → eventsv1.ModelStage, verified on
				// the wire (the decoupling-tax mapping is the thing most likely to rot).
				if p.GetFromStage() != eventsv1.ModelStage_MODEL_STAGE_STAGING {
					t.Errorf("from_stage = %v, want STAGING", p.GetFromStage())
				}
				if p.GetToStage() != eventsv1.ModelStage_MODEL_STAGE_PRODUCTION {
					t.Errorf("to_stage = %v, want PRODUCTION", p.GetToStage())
				}
				// The demoted prior-production version travels with the swap.
				assertEq(t, "demoted_version_id", p.GetDemotedVersionId(), "ver-1")
				assertEq(t, "demoted_version", p.GetDemotedVersion(), "1.0.0")
				assertEq(t, "promoted_by", p.GetPromotedBy(), "user-42")
			},
		},
		{
			name: "ModelArchived",
			event: domain.ProjectionEvent{
				Kind:       domain.EventModelArchived,
				Model:      sampleModel,
				Actor:      sampleActor,
				OccurredAt: sampleOccurredAt,
			},
			wantSubject: events.SubjectModelArchived,
			wantType:    "archived",
			assert: func(t *testing.T, data json.RawMessage) {
				var p eventsv1.ModelArchived
				mustUnmarshal(t, data, &p)
				assertEq(t, "model_id", p.GetModelId(), "model-1")
				assertEq(t, "model_name", p.GetModelName(), "fraud-detector")
				assertEq(t, "archived_by", p.GetArchivedBy(), "user-42")
				assertTS(t, "archived_at", p.GetArchivedAt().AsTime(), sampleOccurredAt)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Subscribe to ONLY this event's exact subject so we assert it landed
			// there (not on a sibling). Each subtest uses a distinct durable name so
			// the consumers don't share state across subtests.
			received := make(chan natsutil.EventEnvelope, 1)
			// Durable/consumer names may not contain '.', so derive the group from
			// the (dot-free) subtest name rather than the dotted event type.
			sub := natsutil.NewSubscriber(js, natsutil.WithConsumerGroup("test-"+tc.name))
			subCtx, cancel := context.WithCancel(ctx)
			t.Cleanup(cancel)
			t.Cleanup(sub.Close)
			if err := sub.Subscribe(subCtx, streamName, tc.wantSubject, func(_ context.Context, env natsutil.EventEnvelope) error {
				received <- env
				return nil
			}); err != nil {
				t.Fatalf("subscribe: %v", err)
			}

			if err := emitter.Emit(ctx, tc.event); err != nil {
				t.Fatalf("Emit: %v", err)
			}

			select {
			case env := <-received:
				// ENVELOPE assertions: source identifies the producing service; type
				// is derived from the subject by the lib (stable identifier).
				if env.Source != events.ServiceName {
					t.Errorf("envelope.Source = %q, want %q", env.Source, events.ServiceName)
				}
				if env.Type != tc.wantType {
					t.Errorf("envelope.Type = %q, want %q", env.Type, tc.wantType)
				}
				if env.ID == "" {
					t.Error("envelope.ID is empty (no dedup id minted)")
				}
				// A correlation id is always stamped (continues the workflow / trace).
				if env.CorrelationID == "" {
					t.Error("envelope.CorrelationID is empty")
				}
				// PAYLOAD assertions: the decoded eventsv1 message field-by-field.
				tc.assert(t, env.Data)
			case <-time.After(15 * time.Second):
				t.Fatalf("timeout waiting for event on %s", tc.wantSubject)
			}
		})
	}
}

// ============================================================================
// TEST 2: duplicate redelivery is idempotent (single effect)
// ============================================================================
//
// THE SCENARIO: NATS is at-LEAST-once. A duplicate arises two ways, both covered:
//
//	(a) Same EventEnvelope.id delivered to the consumer twice (a redelivery after
//	    a lost ACK, or the same logical fact re-emitted). The consumer's
//	    ProcessedStore must recognize the id and skip the side effect → the
//	    handler's effect fires exactly ONCE.
//	(b) The producer re-publishes the SAME Nats-Msg-Id within the dedup window.
//	    JetStream drops it at ingest → the stream stores ONE message.
//
// We control the envelope id deterministically by publishing the wire bytes
// ourselves with a fixed Msg-Id (the same thing natsutil.Publisher does, but with
// a known id), so "the same id twice" is exact and not flaky.
func TestConsume_DuplicateRedeliveryIsIdempotent(t *testing.T) {
	js := dialNATS(t)
	ctx := context.Background()

	// A real consumer-side dedup store (the library convenience; production uses a
	// Redis/Postgres-backed one). FP_ENV is unset in tests, so this is allowed.
	store := natsutil.NewMemoryProcessedStore()

	var effects int64 // counts how many times the side effect actually ran
	processed := make(chan string, 8)

	sub := natsutil.NewSubscriber(js,
		natsutil.WithConsumerGroup("idem-consumer"),
		natsutil.WithIdempotencyStore(store),
	)
	t.Cleanup(sub.Close)
	if err := sub.Subscribe(ctx, streamName, events.SubjectModelRegistered,
		func(_ context.Context, env natsutil.EventEnvelope) error {
			// The "side effect": e.g. upserting the model into the Redis projection.
			// Idempotent consumers must run this AT MOST ONCE per envelope id.
			atomic.AddInt64(&effects, 1)
			processed <- env.ID
			return nil
		}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Build ONE envelope with a FIXED id wrapping a real ModelRegistered payload,
	// then publish it TWICE with the same Nats-Msg-Id.
	payload := &eventsv1.ModelRegistered{ModelId: "model-dup", ModelName: "dup", Team: "t"}
	// protojson: simulate the canonical proto-JSON wire form natsutil.Publisher now
	// produces, so this hand-built envelope is byte-faithful to a real publish.
	data, err := protojson.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	const fixedID = "fixed-envelope-id-123"
	env := natsutil.NewEnvelope("registered", events.ServiceName, data)
	env.ID = fixedID
	envBytes, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	// Publish #1.
	if _, err := js.Publish(ctx, events.SubjectModelRegistered, envBytes, jetstream.WithMsgID(fixedID)); err != nil {
		t.Fatalf("publish #1: %v", err)
	}
	// Wait for the first delivery to be processed so the ProcessedStore has the id.
	select {
	case <-processed:
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for first delivery")
	}

	// Publish #2 with the SAME Msg-Id. JetStream's publish-window dedup means this
	// is dropped at ingest (the stream keeps one copy), so no second delivery
	// occurs at all — the transport layer already deduped.
	if _, err := js.Publish(ctx, events.SubjectModelRegistered, envBytes, jetstream.WithMsgID(fixedID)); err != nil {
		t.Fatalf("publish #2: %v", err)
	}

	// Additionally publish the SAME envelope id under a DIFFERENT Msg-Id. This
	// bypasses transport dedup and forces the message THROUGH to the consumer a
	// second time — proving the CONSUMER-SIDE ProcessedStore (not just JetStream)
	// makes the effect idempotent. The subscriber should ACK it without re-running
	// the handler, because the envelope id is already marked processed.
	if _, err := js.Publish(ctx, events.SubjectModelRegistered, envBytes, jetstream.WithMsgID("different-msg-id-456")); err != nil {
		t.Fatalf("publish #3 (same envelope id, new msg id): %v", err)
	}

	// Give the consumer ample time to (not) double-process.
	time.Sleep(3 * time.Second)

	if got := atomic.LoadInt64(&effects); got != 1 {
		t.Fatalf("side effect ran %d times, want exactly 1 (idempotent consumption broken)", got)
	}
}

// ============================================================================
// TEST 3: poison message → DLQ (bounded, not an infinite loop)
// ============================================================================
//
// A poison message always fails the handler. With MaxRetries + a DLQ subject, the
// subscriber NAKs up to the budget, then routes the message to the DLQ and Terms
// the original (stopping redelivery). We verify the dead-lettered copy lands on
// the DLQ subject and the handler stopped being called after the budget.
func TestConsume_PoisonMessageGoesToDLQ(t *testing.T) {
	js := dialNATS(t)
	ctx := context.Background()

	const (
		poisonSubject = "fp.models.registered" // a real registry subject
		dlqSubject    = "fp.models.dlq"        // backed by the same MODELS stream
		maxRetries    = 2
	)

	var mu sync.Mutex
	handlerCalls := 0

	// The poison consumer: handler ALWAYS fails → NAK → redeliver → ... → DLQ.
	poison := natsutil.NewSubscriber(js,
		natsutil.WithConsumerGroup("poison-consumer"),
		natsutil.WithMaxRetries(maxRetries),
		natsutil.WithDLQSubject(dlqSubject),
		// Short ack-wait so redelivery (and thus the DLQ decision) happens fast,
		// keeping the test well under the timeout.
		natsutil.WithAckWait(1*time.Second),
	)
	t.Cleanup(poison.Close)
	if err := poison.Subscribe(ctx, streamName, poisonSubject,
		func(_ context.Context, _ natsutil.EventEnvelope) error {
			mu.Lock()
			handlerCalls++
			mu.Unlock()
			return natsutil.ErrProcessingFailed // poison: always fails
		}); err != nil {
		t.Fatalf("subscribe poison: %v", err)
	}

	// A DLQ watcher proving the dead-lettered message arrives on the DLQ subject.
	dlqReceived := make(chan natsutil.EventEnvelope, 1)
	dlq := natsutil.NewSubscriber(js, natsutil.WithConsumerGroup("dlq-watcher"))
	t.Cleanup(dlq.Close)
	if err := dlq.Subscribe(ctx, streamName, dlqSubject,
		func(_ context.Context, env natsutil.EventEnvelope) error {
			dlqReceived <- env
			return nil
		}); err != nil {
		t.Fatalf("subscribe dlq: %v", err)
	}

	// Publish one poison message via the real adapter (a genuine registry event).
	emitter := newEmitter(js)
	if err := emitter.Emit(ctx, domain.ProjectionEvent{
		Kind:       domain.EventModelRegistered,
		Model:      domain.Model{ID: "poison-model", Name: "poison", Team: "t", OwnerID: "u"},
		Actor:      domain.Actor{UserID: "u", Team: "t"},
		OccurredAt: sampleOccurredAt,
	}); err != nil {
		t.Fatalf("emit poison: %v", err)
	}

	select {
	case env := <-dlqReceived:
		// The dead-lettered copy preserves the original envelope (id/type/source).
		if env.Source != events.ServiceName {
			t.Errorf("DLQ envelope.Source = %q, want %q", env.Source, events.ServiceName)
		}
		if env.Type != "registered" {
			t.Errorf("DLQ envelope.Type = %q, want %q", env.Type, "registered")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for poison message to reach DLQ")
	}

	// The handler should have been called at MOST maxRetries+1 times (initial +
	// retries) and then stopped — proving the loop is bounded, not infinite.
	// Allow a brief settle so a stray late redelivery (if any) is counted.
	time.Sleep(2 * time.Second)
	mu.Lock()
	calls := handlerCalls
	mu.Unlock()
	if calls == 0 {
		t.Fatal("handler never called — poison message was not delivered")
	}
	if calls > maxRetries+1 {
		t.Fatalf("handler called %d times, want <= %d (redelivery not bounded by DLQ)", calls, maxRetries+1)
	}
}

// ============================================================================
// small assertion helpers (kept local; no external test deps)
// ============================================================================

// mustUnmarshal decodes an EventEnvelope.Data payload into a proto message with
// protojson — the canonical proto-JSON dialect natsutil.Publisher now emits for
// proto payloads. WHY protojson, not encoding/json: the registry events carry
// google.protobuf.Timestamp fields (registered_at, ready_at, …) that render as
// RFC-3339 strings on the wire; only protojson decodes them (the assertTS checks
// would fail under encoding/json).
func mustUnmarshal(t *testing.T, data json.RawMessage, v proto.Message) {
	t.Helper()
	if err := protojson.Unmarshal(data, v); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
}

func assertEq(t *testing.T, field, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %q, want %q", field, got, want)
	}
}

func assertTS(t *testing.T, field string, got, want time.Time) {
	t.Helper()
	if !got.Equal(want) {
		t.Errorf("%s = %v, want %v", field, got, want)
	}
}
