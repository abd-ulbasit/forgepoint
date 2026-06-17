package events_test

import (
	"context"
	"testing"
	"time"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/services/notification/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/notification/internal/events"
)

// ============================================================================
// PUBLISHER INTEGRATION TESTS (real NATS JetStream via testcontainers)
// ============================================================================
//
// These prove the OUTBOUND adapter end-to-end: a domain delivery outcome published
// through the adapter lands on the canonical fp.notifications.{delivered,failed}
// subject, wrapped in an EventEnvelope with source = "notification", carrying a
// CANONICAL proto-JSON payload that protojson.Unmarshal decodes back into the
// generated events.v1 type with the correct channel enum mapping.

// PublishDelivered → fp.notifications.delivered with the right envelope + payload.
func TestPublisher_Delivered_LandsOnSubjectWithCanonicalPayload(t *testing.T) {
	js := newJS(t)
	pub := events.NewPublisher(natsutil.NewPublisher(js, events.Source))

	when := time.Date(2026, 6, 18, 9, 30, 0, 0, time.UTC)
	in := events.DeliveredInput{
		NotificationID:  "notif-1",
		RecipientUserID: "user-7",
		Channel:         domain.ChannelSlack,
		EventType:       "fp.pipelines.failed",
		DeliveredAt:     when,
	}
	if err := pub.PublishDelivered(context.Background(), in); err != nil {
		t.Fatalf("PublishDelivered: %v", err)
	}

	env := drainOne(t, js, events.SubjectNotificationDelivered)

	// Envelope-level assertions: the source is THIS service; the type is the
	// prefix-stripped canonical type natsutil derives ("delivered").
	if env.Source != events.Source {
		t.Errorf("envelope source = %q, want %q", env.Source, events.Source)
	}
	if env.Type != "delivered" {
		t.Errorf("envelope type = %q, want %q", env.Type, "delivered")
	}
	if env.ID == "" {
		t.Error("envelope id must be set (dedup/idempotency anchor)")
	}

	// Payload-level: canonical proto-JSON round-trips into the generated type with
	// the correct channel enum mapping (SLACK) and RFC-3339 timestamp.
	var p eventsv1.NotificationDelivered
	protojsonUnmarshal(t, env.Data, &p)
	if p.GetNotificationId() != "notif-1" {
		t.Errorf("notification_id = %q, want notif-1", p.GetNotificationId())
	}
	if p.GetRecipientUserId() != "user-7" {
		t.Errorf("recipient_user_id = %q, want user-7", p.GetRecipientUserId())
	}
	if p.GetChannel() != eventsv1.NotificationChannel_NOTIFICATION_CHANNEL_SLACK {
		t.Errorf("channel = %v, want SLACK", p.GetChannel())
	}
	if p.GetEventType() != "fp.pipelines.failed" {
		t.Errorf("event_type = %q, want fp.pipelines.failed", p.GetEventType())
	}
	if got := p.GetDeliveredAt().AsTime(); !got.Equal(when) {
		t.Errorf("delivered_at = %v, want %v", got, when)
	}
}

// PublishFailed → fp.notifications.failed carrying the failure detail (attempts +
// error message) and NO secret/target.
func TestPublisher_Failed_CarriesFailureDetail(t *testing.T) {
	js := newJS(t)
	pub := events.NewPublisher(natsutil.NewPublisher(js, events.Source))

	in := events.FailedInput{
		NotificationID:  "notif-2",
		RecipientUserID: "user-8",
		Channel:         domain.ChannelWebhook,
		EventType:       "fp.models.drift.detected",
		Attempts:        5,
		ErrorMessage:    "503 from webhook",
	}
	if err := pub.PublishFailed(context.Background(), in); err != nil {
		t.Fatalf("PublishFailed: %v", err)
	}

	env := drainOne(t, js, events.SubjectNotificationFailed)
	if env.Source != events.Source {
		t.Errorf("envelope source = %q, want %q", env.Source, events.Source)
	}

	var p eventsv1.NotificationFailed
	protojsonUnmarshal(t, env.Data, &p)
	if p.GetChannel() != eventsv1.NotificationChannel_NOTIFICATION_CHANNEL_WEBHOOK {
		t.Errorf("channel = %v, want WEBHOOK", p.GetChannel())
	}
	if p.GetAttempts() != 5 {
		t.Errorf("attempts = %d, want 5", p.GetAttempts())
	}
	if p.GetErrorMessage() != "503 from webhook" {
		t.Errorf("error_message = %q, want '503 from webhook'", p.GetErrorMessage())
	}
	// FailedAt was left zero on input → the publisher stamped a non-zero producer clock.
	if p.GetFailedAt() == nil || p.GetFailedAt().AsTime().IsZero() {
		t.Error("failed_at must be stamped by the publisher when not supplied")
	}
}
