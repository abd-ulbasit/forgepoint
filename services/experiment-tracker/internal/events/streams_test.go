// streams_test.go — proves the PRODUCER-OWNED stream provisioning fix for
// experiment-tracker's EXPERIMENTS stream.
//
// ============================================================================
// WHAT THIS PROVES (the regression that was shipped)
// ============================================================================
//
// The publisher tests (events_test.go) call createStream("EXPERIMENTS", …) in their
// harness before publishing — so they passed even though NO service ever declared
// the EXPERIMENTS stream in production. That masked a real bug: on a real cluster
// with no stream, JetStream REJECTS every fp.experiments.* publish ("no stream
// matches subject", 10073), so run.created/run.finished were dropped and
// notification's reactor degrade-skipped those two consumed subjects.
//
// These tests exercise the REAL production seam — events.EnsureStream provisions the
// stream on a fresh testcontainers JetStream that has NONE, then the real adapter
// publishes into it. They would FAIL against the pre-fix code (no EnsureStream
// existed), so they pin the fix.
package events_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/experiment-tracker/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/experiment-tracker/internal/events"
)

// TestEnsureStream_CreatesExperimentsStreamOverFullTree proves EnsureStream
// provisions the EXPERIMENTS stream bound to the whole fp.experiments.> tree on a
// cluster that had none — exactly what main.go now does at boot.
func TestEnsureStream_CreatesExperimentsStreamOverFullTree(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	url := testutil.StartNATS(t)
	js := connectJS(t, url)
	ctx := context.Background()

	// Precondition: the stream does NOT exist yet (fresh cluster) — confirming we
	// exercise the create path, not an accidentally pre-provisioned stream.
	if _, err := js.Stream(ctx, events.StreamName); err == nil {
		t.Fatalf("precondition failed: %s stream already exists before EnsureStream", events.StreamName)
	}

	if err := events.EnsureStream(ctx, js); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}

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

// TestEnsureStream_IsIdempotent proves the operation is safe to call repeatedly
// (every boot / a peer pod during a rolling deploy). CreateOrUpdateStream reconciles
// instead of erroring on "already exists" — so a second call is a no-op, not the
// "stream name already in use" error CreateStream would return.
func TestEnsureStream_IsIdempotent(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	url := testutil.StartNATS(t)
	js := connectJS(t, url)
	ctx := context.Background()

	if err := events.EnsureStream(ctx, js); err != nil {
		t.Fatalf("EnsureStream #1: %v", err)
	}
	if err := events.EnsureStream(ctx, js); err != nil {
		t.Fatalf("EnsureStream #2 (idempotent re-ensure): %v", err)
	}
}

// TestEnsureStream_EnablesRealPublishEndToEnd is the regression's heart: with ONLY
// EnsureStream run (exactly what main.go now does — no hand-rolled createStream), the
// real Publisher must publish fp.experiments.run.created successfully and a
// subscriber must receive it. Against the pre-fix code (no stream provisioned) the
// same publish returns "no stream matches subject" (10073) and nothing is delivered.
func TestEnsureStream_EnablesRealPublishEndToEnd(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	url := testutil.StartNATS(t)
	js := connectJS(t, url)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// PROVISION VIA THE PRODUCTION SEAM ONLY. No createStream in this test — this is
	// the whole point: the same wiring main.go performs at boot, nothing more.
	if err := events.EnsureStream(ctx, js); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}

	// Subscribe RAW to the EXPERIMENTS stream so we observe exactly what reaches the
	// wire. Against the pre-fix code this subscribe+publish never delivers (no stream).
	sub := natsutil.NewSubscriber(js, natsutil.WithConsumerGroup("ensure-stream-e2e"))
	t.Cleanup(sub.Close)
	got := make(chan natsutil.EventEnvelope, 1)
	if err := sub.Subscribe(ctx, events.StreamName, events.SubjectRunCreated,
		func(_ context.Context, env natsutil.EventEnvelope) error {
			got <- env
			return nil
		}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Real adapter, sourced "experiment-tracker" — identical to main.go's wiring.
	pub := events.NewPublisher(natsutil.NewPublisher(js, events.ServiceName))
	started := time.Date(2026, 6, 18, 9, 30, 0, 0, time.UTC)
	if err := pub.PublishRunCreated(ctx, domain.RunCreatedEvent{
		RunID:        "run-e2e",
		ExperimentID: "exp-e2e",
		DisplayName:  "ensure-stream",
		StartedAt:    started,
	}); err != nil {
		// The exact failure the bug produced: a PubAck error because no stream
		// captured fp.experiments.run.created. Asserting nil here pins the fix.
		t.Fatalf("PublishRunCreated after EnsureStream must succeed (pre-fix: 'no stream matches subject'): %v", err)
	}

	select {
	case env := <-got:
		if env.Source != events.ServiceName {
			t.Errorf("envelope source = %q, want %q", env.Source, events.ServiceName)
		}
		var payload eventsv1.RunCreated
		if err := protojson.Unmarshal(env.Data, &payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		if payload.GetRunId() != "run-e2e" {
			t.Errorf("run_id = %q, want %q", payload.GetRunId(), "run-e2e")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timeout: event was not delivered after EnsureStream (stream not provisioned?)")
	}
}
