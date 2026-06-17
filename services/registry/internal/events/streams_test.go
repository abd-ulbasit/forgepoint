// streams_test.go — proves the PRODUCER-OWNED stream provisioning fix.
//
// ============================================================================
// WHAT THIS PROVES (the regression that was shipped)
// ============================================================================
//
// publisher_test.go's dialNATS calls js.CreateStream itself before publishing —
// so those tests passed even though main.go never declared the MODELS stream. That
// masked a production bug: on a fresh cluster with no stream, JetStream REJECTS
// every fp.models.* publish ("no stream matches subject", 10073), and the registry's
// log-and-continue Emit posture drops the event silently while the write commits.
//
// These tests exercise the REAL production path — EnsureStream provisions the
// stream, then the real adapter publishes into it — against a real testcontainers
// JetStream. They would FAIL against the pre-fix code (no EnsureStream existed; a
// raw Emit with no stream returns the 10073 error), so they pin the fix.
//
//	1. EnsureStream creates MODELS over fp.models.> — verified via stream info.
//	2. EnsureStream is idempotent + convergent — a second call (and a concurrent
//	   model-monitor declaring the same stream) is a no-op, not an error.
//	3. After ONLY EnsureStream (no hand-rolled CreateStream), the real
//	   events.Publisher can Emit a lifecycle event successfully and a subscriber
//	   receives it — the end-to-end production wiring main.go now performs.
package events_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/registry/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/registry/internal/events"
)

// dialNATSRaw connects to a fresh testcontainers NATS WITHOUT declaring any stream.
// Unlike publisher_test.go's dialNATS (which CreateStream's MODELS itself), this
// hands back a JetStream context with NO streams — the exact state of a fresh
// production cluster, so EnsureStream is the thing under test, not the test harness.
func dialNATSRaw(t *testing.T) jetstream.JetStream {
	t.Helper()
	url := testutil.StartNATS(t) // SkipIfNoDocker is called inside StartNATS

	conn, js, err := natsutil.Connect(url)
	if err != nil {
		t.Fatalf("connect NATS: %v", err)
	}
	t.Cleanup(conn.Close)
	return js
}

// TestEnsureStream_CreatesModelsStreamOverFullTree proves EnsureStream provisions
// the MODELS stream bound to the whole fp.models.> tree on a cluster that had none.
func TestEnsureStream_CreatesModelsStreamOverFullTree(t *testing.T) {
	js := dialNATSRaw(t)
	ctx := context.Background()

	// Precondition: the stream does NOT exist yet (fresh cluster). Stream lookup
	// returns ErrStreamNotFound — confirming we're testing the create path, not an
	// accidentally-pre-provisioned stream.
	if _, err := js.Stream(ctx, events.StreamName); err == nil {
		t.Fatalf("precondition failed: MODELS stream already exists before EnsureStream")
	}

	if err := events.EnsureStream(ctx, js); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}

	// The stream now exists and binds the full fp.models.> tree.
	stream, err := js.Stream(ctx, events.StreamName)
	if err != nil {
		t.Fatalf("Stream lookup after EnsureStream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Stream.Info: %v", err)
	}
	if info.Config.Name != events.StreamName {
		t.Errorf("stream name = %q, want %q", info.Config.Name, events.StreamName)
	}
	if len(info.Config.Subjects) != 1 || info.Config.Subjects[0] != events.StreamSubjects {
		t.Errorf("stream subjects = %v, want [%q]", info.Config.Subjects, events.StreamSubjects)
	}
}

// TestEnsureStream_IsIdempotentAndConvergent proves the operation is safe to call
// repeatedly (every boot) and concurrently with another service declaring the same
// stream (e.g. model-monitor adding fp.models.drift.detected, or a peer registry pod
// in a rolling deploy). CreateOrUpdateStream reconciles instead of erroring on
// "already exists" — so all of these are no-ops, not failures.
func TestEnsureStream_IsIdempotentAndConvergent(t *testing.T) {
	js := dialNATSRaw(t)
	ctx := context.Background()

	// First call creates it.
	if err := events.EnsureStream(ctx, js); err != nil {
		t.Fatalf("EnsureStream #1: %v", err)
	}
	// Second call must be a no-op (NOT a "stream name already in use" error, which is
	// what CreateStream would return — the bug CreateOrUpdateStream avoids).
	if err := events.EnsureStream(ctx, js); err != nil {
		t.Fatalf("EnsureStream #2 (idempotent re-ensure): %v", err)
	}

	// Convergent with a CONCURRENT peer declaring the SAME stream config directly:
	// both converge on one config, no conflict. This models model-monitor / a GitOps
	// job / another registry pod doing CreateOrUpdate on MODELS at the same time.
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     events.StreamName,
		Subjects: []string{events.StreamSubjects},
	}); err != nil {
		t.Fatalf("concurrent peer CreateOrUpdateStream on same config: %v", err)
	}

	// And EnsureStream once more after the peer — still a no-op.
	if err := events.EnsureStream(ctx, js); err != nil {
		t.Fatalf("EnsureStream #3 (after concurrent peer): %v", err)
	}
}

// TestEnsureStream_EnablesRealPublishEndToEnd is the regression's heart: with ONLY
// EnsureStream run (exactly what main.go now does — no hand-rolled CreateStream),
// the real events.Publisher.Emit must SUCCEED and a subscriber must receive the
// event. Against the pre-fix code (no stream provisioned), the very same Emit
// returns "no stream matches subject" (10073) and nothing is delivered.
func TestEnsureStream_EnablesRealPublishEndToEnd(t *testing.T) {
	js := dialNATSRaw(t)
	ctx := context.Background()

	// PROVISION VIA THE PRODUCTION SEAM ONLY. No CreateStream in this test.
	if err := events.EnsureStream(ctx, js); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}

	// Real adapter, sourced "registry" — identical to main.go's wiring.
	emitter := events.NewPublisher(natsutil.NewPublisher(js, events.ServiceName))

	received := make(chan natsutil.EventEnvelope, 1)
	sub := natsutil.NewSubscriber(js, natsutil.WithConsumerGroup("ensure-stream-e2e"))
	subCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	t.Cleanup(sub.Close)
	if err := sub.Subscribe(subCtx, events.StreamName, events.SubjectModelVersionReady,
		func(_ context.Context, env natsutil.EventEnvelope) error {
			received <- env
			return nil
		}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Emit a real version.ready fact. This is the publish that the pre-fix cluster
	// silently dropped (and that main.go's EnsureStream now makes durable).
	err := emitter.Emit(ctx, domain.ProjectionEvent{
		Kind:  domain.EventVersionReady,
		Model: domain.Model{ID: "model-9", Name: "ready-detector", Team: "t", OwnerID: "u"},
		Version: domain.ModelVersion{
			ID: "ver-9", ModelID: "model-9", Version: "1.0.0",
			ArtifactPath: "s3://fp-models/model-9/1.0.0.onnx", ArtifactDigest: "sha256:cafe",
			SizeBytes: 2048, Stage: domain.StageDev, Status: domain.StatusReady,
		},
		Actor:      domain.Actor{UserID: "u", Team: "t"},
		OccurredAt: time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		// This is the exact failure the bug produced: a PubAck error because no stream
		// captured fp.models.version.ready. Asserting nil here pins the fix.
		t.Fatalf("Emit after EnsureStream must succeed (pre-fix this was 'no stream matches subject'): %v", err)
	}

	select {
	case env := <-received:
		if env.Source != events.ServiceName {
			t.Errorf("envelope.Source = %q, want %q", env.Source, events.ServiceName)
		}
		if env.Type != "version.ready" {
			t.Errorf("envelope.Type = %q, want %q", env.Type, "version.ready")
		}
		var p eventsv1.ModelVersionReady
		if err := json.Unmarshal(env.Data, &p); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		if p.GetVersionId() != "ver-9" {
			t.Errorf("version_id = %q, want %q", p.GetVersionId(), "ver-9")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timeout: event was not delivered after EnsureStream (stream not provisioned?)")
	}
}
