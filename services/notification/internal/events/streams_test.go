// streams_test.go — proves the PRODUCER-OWNED stream provisioning fix for
// notification's NOTIFICATIONS domain stream.
//
// ============================================================================
// WHAT THIS PROVES (the regression that was shipped)
// ============================================================================
//
// The publisher tests (publisher_test.go via newJS) CreateStream the NOTIFICATIONS
// stream in their harness before publishing — so they passed even though NO service
// ever declared the NOTIFICATIONS stream in production. That masked a real bug: on a
// real cluster with no stream, JetStream REJECTS every fp.notifications.* publish
// ("no stream matches subject", 10073), so delivered/failed delivery-health events
// were dropped AND experiment-tracker's wide lineage sink (which binds a durable to
// the NOTIFICATIONS stream) had no stream to bind.
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

	"github.com/nats-io/nats.go/jetstream"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/notification/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/notification/internal/events"
)

// rawJS connects to a fresh testcontainers NATS WITHOUT declaring any stream —
// unlike newJS (which pre-creates NOTIFICATIONS), this hands back a JetStream context
// with NO streams, the exact state of a fresh production cluster. So EnsureStream is
// the thing under test, not the test harness.
func rawJS(t *testing.T) jetstream.JetStream {
	t.Helper()
	testutil.SkipIfNoDocker(t)
	url := testutil.StartNATS(t)
	conn, js, err := natsutil.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(conn.Close)
	return js
}

// TestEnsureStream_CreatesNotificationsStreamOverFullTree proves EnsureStream
// provisions the NOTIFICATIONS stream bound to the whole fp.notifications.> tree on a
// cluster that had none — exactly what main.go now does at boot.
func TestEnsureStream_CreatesNotificationsStreamOverFullTree(t *testing.T) {
	js := rawJS(t)
	ctx := context.Background()

	// Precondition: the stream does NOT exist yet (fresh cluster).
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
// instead of erroring on "already exists".
func TestEnsureStream_IsIdempotent(t *testing.T) {
	js := rawJS(t)
	ctx := context.Background()

	if err := events.EnsureStream(ctx, js); err != nil {
		t.Fatalf("EnsureStream #1: %v", err)
	}
	if err := events.EnsureStream(ctx, js); err != nil {
		t.Fatalf("EnsureStream #2 (idempotent re-ensure): %v", err)
	}
}

// TestEnsureStream_EnablesRealPublishEndToEnd is the regression's heart: with ONLY
// EnsureStream run (exactly what main.go now does — no hand-rolled CreateStream), the
// real Publisher must publish fp.notifications.delivered successfully and a subscriber
// must receive it. Against the pre-fix code (no stream provisioned) the same publish
// returns "no stream matches subject" (10073) and nothing is delivered.
func TestEnsureStream_EnablesRealPublishEndToEnd(t *testing.T) {
	js := rawJS(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// PROVISION VIA THE PRODUCTION SEAM ONLY. No CreateStream in this test.
	if err := events.EnsureStream(ctx, js); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}

	// Subscribe RAW to the NOTIFICATIONS stream so we observe exactly what reaches the
	// wire — this is the same path experiment-tracker's lineage sink uses.
	sub := natsutil.NewSubscriber(js, natsutil.WithConsumerGroup("ensure-stream-e2e"))
	t.Cleanup(sub.Close)
	got := make(chan natsutil.EventEnvelope, 1)
	if err := sub.Subscribe(ctx, events.StreamName, events.SubjectNotificationDelivered,
		func(_ context.Context, env natsutil.EventEnvelope) error {
			select {
			case got <- env:
			default:
			}
			return nil
		}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Real adapter, sourced "notification" — identical to main.go's wiring.
	pub := events.NewPublisher(natsutil.NewPublisher(js, events.Source))
	when := time.Date(2026, 6, 18, 9, 30, 0, 0, time.UTC)
	if err := pub.PublishDelivered(ctx, events.DeliveredInput{
		NotificationID:  "notif-e2e",
		RecipientUserID: "user-e2e",
		Channel:         domain.ChannelSlack,
		EventType:       "fp.pipelines.failed",
		DeliveredAt:     when,
	}); err != nil {
		// The exact failure the bug produced: a PubAck error because no stream
		// captured fp.notifications.delivered. Asserting nil here pins the fix.
		t.Fatalf("PublishDelivered after EnsureStream must succeed (pre-fix: 'no stream matches subject'): %v", err)
	}

	select {
	case env := <-got:
		if env.Source != events.Source {
			t.Errorf("envelope source = %q, want %q", env.Source, events.Source)
		}
		var p eventsv1.NotificationDelivered
		protojsonUnmarshal(t, env.Data, &p)
		if p.GetNotificationId() != "notif-e2e" {
			t.Errorf("notification_id = %q, want %q", p.GetNotificationId(), "notif-e2e")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timeout: event was not delivered after EnsureStream (stream not provisioned?)")
	}
}
