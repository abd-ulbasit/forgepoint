package natsutil_test

// dialect_roundtrip_test.go — the REGRESSION GUARD for the serialization-dialect
// fix (BUG 1).
//
// ============================================================================
// WHAT THIS PROVES
// ============================================================================
//
// The platform's canonical domain events are forgepoint/events/v1 PROTO messages.
// Before the fix, natsutil.Publisher marshalled EVERY payload with encoding/json —
// including proto messages. encoding/json and protojson are INCOMPATIBLE
// proto-JSON dialects: a google.protobuf.Timestamp renders as the Go struct
// {"seconds":..,"nanos":..} under encoding/json but as an RFC-3339 STRING under
// protojson. So a proto event published with encoding/json was DELIVERED but
// FAILED to decode in any protojson consumer (and any non-Go protojson runtime —
// there is now a Python SDK), getting DLQ'd.
//
// THE GUARD: publish a real events/v1 proto that CONTAINS a well-known Timestamp
// (ModelRegistered.registered_at) through the REAL natsutil.Publisher over REAL
// NATS JetStream, consume the envelope, decode envelope.Data with protojson, and
// assert the decoded message EQUALS the original via proto.Equal — i.e. the
// timestamp (the field that broke) survives the round-trip. If the Publisher ever
// regresses to encoding/json for proto payloads, this protojson.Unmarshal fails
// (or the timestamp is lost) and proto.Equal is false — the test goes red.
//
// Run with -race (the publish/consume hop crosses goroutines).

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
)

// TestPublisher_ProtoPayloadRoundTripsViaProtojson is the BUG 1 proof: a proto
// event with a Timestamp published via natsutil.Publisher must decode back
// IDENTICALLY with protojson.Unmarshal on the consumer side.
func TestPublisher_ProtoPayloadRoundTripsViaProtojson(t *testing.T) {
	skipIfNoDocker(t)
	url := startNATS(t)

	conn, js, err := natsutil.Connect(url)
	if err != nil {
		t.Fatalf("connect error: %v", err)
	}
	defer conn.Close()

	ctx := context.Background()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "MODELS",
		Subjects: []string{"fp.models.>"},
	}); err != nil {
		t.Fatalf("create stream error: %v", err)
	}

	// The original proto event. registered_at is a google.protobuf.Timestamp — the
	// well-known type that encoding/json and protojson DISAGREE on. We truncate to
	// microseconds so the RFC-3339 protojson encoding round-trips exactly (protojson
	// preserves nanosecond precision, but using a clean value keeps the intent and
	// the proto.Equal assertion unambiguous).
	original := &eventsv1.ModelRegistered{
		ModelId:      "model-roundtrip-1",
		ModelName:    "fraud-detector",
		Framework:    "onnx",
		TaskType:     "classification",
		OwnerId:      "user-42",
		Team:         "fraud-team",
		RegisteredAt: timestamppb.New(time.Now().UTC().Truncate(time.Microsecond)),
	}

	received := make(chan natsutil.EventEnvelope, 1)
	sub := natsutil.NewSubscriber(js)
	t.Cleanup(sub.Close)
	if err := sub.Subscribe(ctx, "MODELS", "fp.models.>",
		func(_ context.Context, env natsutil.EventEnvelope) error {
			received <- env
			return nil
		}); err != nil {
		t.Fatalf("subscribe error: %v", err)
	}

	// Publish the RAW proto. natsutil.Publisher must now marshal it with protojson
	// (the canonical proto-JSON dialect), NOT encoding/json.
	pub := natsutil.NewPublisher(js, "registry")
	if err := pub.Publish(ctx, "fp.models.registered", original); err != nil {
		t.Fatalf("publish error: %v", err)
	}

	select {
	case env := <-received:
		// Decode envelope.Data with protojson — the symmetric inverse of the
		// publisher's protojson marshal. This is exactly what a protojson consumer
		// (experiment-tracker, billing, model-serving, …) does in production.
		var decoded eventsv1.ModelRegistered
		if err := protojson.Unmarshal(env.Data, &decoded); err != nil {
			t.Fatalf("protojson decode failed — proto event was NOT published as canonical proto-JSON: %v", err)
		}

		// proto.Equal is the regression assertion: the decoded message — INCLUDING
		// the Timestamp — must equal the original. Under the OLD encoding/json
		// publisher this protojson.Unmarshal would error on the {seconds,nanos}
		// shape, or the timestamp would be zero/wrong, failing here.
		if !proto.Equal(original, &decoded) {
			t.Fatalf("round-trip mismatch:\n  original = %v\n  decoded  = %v", original, &decoded)
		}

		// Explicitly assert the well-known Timestamp survived (the field the bug
		// destroyed), so a failure message points straight at the dialect.
		if !decoded.GetRegisteredAt().AsTime().Equal(original.GetRegisteredAt().AsTime()) {
			t.Errorf("registered_at did not survive: got %v, want %v",
				decoded.GetRegisteredAt().AsTime(), original.GetRegisteredAt().AsTime())
		}
	case <-time.After(20 * time.Second):
		t.Fatal("timeout waiting for the proto event")
	}
}
