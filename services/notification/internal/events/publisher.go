package events

import (
	"context"
	"time"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/services/notification/internal/domain"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ============================================================================
// PUBLISHER — the outbound adapter for DELIVERY-HEALTH events
// ============================================================================
//
// The reactor (subscriber.go) executes a RoutingDecision: it writes the inbox
// row and fires the Notifier for each delivering channel. AFTER each external
// delivery resolves, it tells this Publisher the OUTCOME, which maps it to the
// canonical events.v1 payload and publishes it:
//
//	delivery succeeded  → fp.notifications.delivered (NotificationDelivered)
//	delivery exhausted  → fp.notifications.failed    (NotificationFailed)
//
// WHY publish a delivery-FAILURE event at all (a subtle, interview-worthy point):
// "we tried to alert a human about a CRITICAL drift and the webhook was down" is
// ITSELF an alert-worthy fact. Making delivery health observable platform-wide lets
// experiment-tracker build success-rate dashboards and lets an on-call escalation
// flow react to a notification that never reached anyone. The body/secret never
// rides these events — only IDENTIFIERS + the outcome (see the events.proto
// NotificationFailed doc: "Kept small: identifiers + outcome, not the body").
//
// THIN TRANSLATION LAYER (same shape as the auth publisher): natsutil.Publisher
// already does the cross-cutting work this adapter must NOT re-implement — it wraps
// the payload in the EventEnvelope, stamps Source = "notification" (set once at
// construction), sets Nats-Msg-Id = envelope.ID for JetStream publish-side dedup,
// and continues the correlation id + injects the W3C trace context so the async hop
// stays on the same Tempo trace. This adapter's ONLY job is the domain→events.v1
// field copy, the canonical subject, and protojson encoding.
//
// BEST-EFFORT (the reactor's contract): a NATS hiccup publishing a delivery-health
// event MUST NOT fail the reaction — the inbox row and the actual delivery already
// happened and are the durable record. Publish returns the error for the caller's
// bookkeeping/logging, but the reactor does not NAK the inbound event on a
// delivery-health publish failure (that would re-run the whole reaction and double-
// deliver). The delivery-health feed is observability, not the source of truth.

// DeliveredInput is the framework-free outcome the reactor hands the Publisher
// after a channel DELIVERED successfully. WHY a dedicated input struct (not the
// events.v1 type) at the call boundary: the reactor lives one step from the domain
// and must not construct proto messages; adding a field later stays backward-
// compatible and callers construct by name so they can't transpose args.
type DeliveredInput struct {
	// NotificationID is the inbox-entry id this delivery belongs to (NOT the event
	// id) — the stable handle a dashboard joins delivery health back to.
	NotificationID string
	// RecipientUserID is who the notification reached.
	RecipientUserID string
	// Channel is the transport that succeeded (WEBHOOK/SLACK/EMAIL — IN_APP is a DB
	// write, not an external delivery, so it does not emit a delivery-health event).
	Channel domain.NotificationChannel
	// EventType is provenance: the originating event's type (e.g. "fp.pipelines.failed"),
	// so a consumer can attribute delivery health to the SOURCE event class.
	EventType string
	// DeliveredAt is when delivery succeeded (producer clock). If zero, the
	// Publisher stamps now() so the event always carries a time.
	DeliveredAt time.Time
}

// FailedInput is the framework-free outcome the reactor hands the Publisher after a
// channel EXHAUSTED its retry budget (or its circuit was open). It carries the
// failure DETAIL for triage — never the target URL or an auth header.
type FailedInput struct {
	NotificationID  string
	RecipientUserID string
	Channel         domain.NotificationChannel
	EventType       string
	// Attempts is how many tries were made before giving up (the exhausted budget).
	Attempts int32
	// ErrorMessage is the terminal failure detail ("503 from webhook", "circuit
	// breaker open"). Health detail only — never a secret/target.
	ErrorMessage string
	// FailedAt is when we gave up (producer clock). Zero → Publisher stamps now().
	FailedAt time.Time
}

// Publisher publishes the notification service's delivery-health events onto NATS
// via natsutil. It is the concrete outbound adapter the reactor depends on through
// the DeliveryHealthPublisher port (defined in subscriber.go) — programming to the
// interface keeps the reactor testable with a fake and free of gen/go.
type Publisher struct {
	pub *natsutil.Publisher

	// now is the producer clock for payload timestamps when the caller did not
	// supply one. Defaults to time.Now (UTC); a test may set it for determinism.
	now func() time.Time
}

// NewPublisher constructs the delivery-health Publisher over an existing natsutil
// Publisher.
//
// WHY take a *natsutil.Publisher rather than a jetstream.JetStream: main.go
// constructs ONE natsutil.Publisher per service (it owns the Source name and the
// dedup/trace policy) and shares it; this adapter layers the notification-specific
// mapping on top. Passing the lower-level JS would duplicate that wiring and risk
// two publishers disagreeing on Source.
func NewPublisher(pub *natsutil.Publisher) *Publisher {
	return &Publisher{pub: pub, now: func() time.Time { return time.Now().UTC() }}
}

// compile-time proof the adapter satisfies the reactor's outbound port. A signature
// drift breaks the build HERE, not at a far-away wiring site.
var _ DeliveryHealthPublisher = (*Publisher)(nil)

// PublishDelivered maps a successful delivery to events.v1.NotificationDelivered
// and publishes it on fp.notifications.delivered.
func (p *Publisher) PublishDelivered(ctx context.Context, in DeliveredInput) error {
	ts := in.DeliveredAt
	if ts.IsZero() {
		ts = p.now()
	}
	payload := &eventsv1.NotificationDelivered{
		NotificationId:  in.NotificationID,
		RecipientUserId: in.RecipientUserID,
		Channel:         mapChannel(in.Channel),
		EventType:       in.EventType,
		DeliveredAt:     timestamppb.New(ts.UTC()),
	}
	return p.publish(ctx, SubjectNotificationDelivered, payload)
}

// PublishFailed maps an exhausted delivery to events.v1.NotificationFailed and
// publishes it on fp.notifications.failed.
func (p *Publisher) PublishFailed(ctx context.Context, in FailedInput) error {
	ts := in.FailedAt
	if ts.IsZero() {
		ts = p.now()
	}
	payload := &eventsv1.NotificationFailed{
		NotificationId:  in.NotificationID,
		RecipientUserId: in.RecipientUserID,
		Channel:         mapChannel(in.Channel),
		EventType:       in.EventType,
		Attempts:        in.Attempts,
		ErrorMessage:    in.ErrorMessage,
		FailedAt:        timestamppb.New(ts.UTC()),
	}
	return p.publish(ctx, SubjectNotificationFailed, payload)
}

// publish encodes the payload as canonical proto-JSON and hands the raw bytes to
// natsutil.Publisher (which json.Marshals the RawMessage as a verbatim passthrough,
// preserving the canonical encoding). Centralized so both produced events share the
// exact same encode+publish path.
func (p *Publisher) publish(ctx context.Context, subject string, payload proto.Message) error {
	raw, err := marshalCanonical(payload)
	if err != nil {
		return err
	}
	return p.pub.Publish(ctx, subject, raw)
}

// ============================================================================
// ENUM MAPPING — domain.NotificationChannel → events.v1.NotificationChannel
// ============================================================================
//
// The events contract DELIBERATELY re-declares its own NotificationChannel enum
// (it must not import any service's API). The integer values are kept aligned with
// the domain's (UNSPECIFIED=0, IN_APP=1, WEBHOOK=2, SLACK=3, EMAIL=4), so this is a
// trivial, auditable switch. We use an explicit switch rather than a raw int cast
// so the conversion is intentional and visible: a value added to one enum but not
// the other maps to the safe UNSPECIFIED zero rather than smuggling a bogus integer
// onto the wire.
func mapChannel(c domain.NotificationChannel) eventsv1.NotificationChannel {
	switch c {
	case domain.ChannelInApp:
		return eventsv1.NotificationChannel_NOTIFICATION_CHANNEL_IN_APP
	case domain.ChannelWebhook:
		return eventsv1.NotificationChannel_NOTIFICATION_CHANNEL_WEBHOOK
	case domain.ChannelSlack:
		return eventsv1.NotificationChannel_NOTIFICATION_CHANNEL_SLACK
	case domain.ChannelEmail:
		return eventsv1.NotificationChannel_NOTIFICATION_CHANNEL_EMAIL
	default:
		return eventsv1.NotificationChannel_NOTIFICATION_CHANNEL_UNSPECIFIED
	}
}
