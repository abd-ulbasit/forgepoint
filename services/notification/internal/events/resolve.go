package events

import (
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/services/notification/internal/domain"
)

// ============================================================================
// OPAQUE ENVELOPE → domain.InboundEvent (the choreography boundary)
// ============================================================================
//
// resolveEvent is the SINGLE point where a wire EventEnvelope becomes the domain's
// InboundEvent. The defining constraint (choreography): we read ONLY envelope
// METADATA — id, type, source, timestamp, correlation — plus the recipient and the
// rendered title/body the platform put on the envelope. We DELIBERATELY do not
// decode env.Data into any producer-specific events.v1 message. The payload bytes
// ride through verbatim as InboundEvent.Payload, forwarded to webhook consumers
// that want the raw event (the domain never interprets them).
//
// WHY this is the whole point: the moment we'd `switch env.Type { case ...:
// decode ModelDriftDetected }` we'd recouple the notification service to every
// producer's schema and DESTROY the "a new event type is a zero-code change"
// property. So the only field the reactor interprets is env.Type (which the domain
// pattern-matches against the user's mute/preference patterns), and the only thing
// it needs from outside is "who should this notify, and what should the inbox row
// say" — which is exactly what RecipientRouting supplies.
//
// ============================================================================
// RECIPIENT RESOLUTION — a PORT, because it is the platform's policy, not ours
// ============================================================================
//
// "Given a fp.pipelines.failed about execution X, WHO gets notified (the pipeline
// owner? the on-call rotation?) and what does the row say?" is a PLATFORM mapping,
// not the reactor's business. The events.proto NOTIFICATION block says the service
// "treats inbound events OPAQUELY"; the InboundEvent doc says recipient resolution
// is "the PLATFORM's concern carried in the envelope/payload metadata". So we model
// it as a RecipientRouting port the main.go wiring provides (e.g. a resolver that
// reads a recipient hint the producer stamped, or maps event-type → team → users).
// Keeping it OUT of the reactor keeps the reactor a pure decode→react→execute shell
// and keeps the routing policy swappable without touching this adapter.
//
// The reactor holds a RecipientRouting (defaulting to a no-recipient router that
// drops every event — a safe default that delivers nothing until the platform wires
// a real resolver). resolveEvent calls it and assembles the InboundEvent.

// RoutedRecipient is one resolved (recipient, rendered-content) tuple the router
// returns for an event. The SAME event may fan out to several recipients (one
// inbox row each) — but THIS adapter reacts per (event, recipient): the reactor
// processes the FIRST recipient the router yields per envelope, because the dedup
// unit and the InboundEvent are single-recipient by design (see EventDedupKey).
// A multi-recipient fan-out is the router's job to expand into multiple envelopes
// or a future per-recipient loop; modeling the tuple keeps that evolution open.
type RoutedRecipient struct {
	// RecipientUserID is who to notify. Empty means "no recipient" → the event is
	// skipped (ACKed) by the reactor (a normal case for events with no per-user target).
	RecipientUserID string
	// FullType is the CANONICAL full subject of the event (e.g. "fp.pipelines.failed")
	// that the domain pattern-matches mute/preference patterns and derives severity
	// against. WHY the router supplies it: natsutil.Publisher STRIPS the "fp.<svc>."
	// prefix when it derives EventEnvelope.type (so env.Type is "failed", not
	// "fp.pipelines.failed"), and the JetStream handler callback does not receive the
	// raw NATS subject — so the full subject is NOT recoverable from the envelope
	// alone (env.Source is the PRODUCER, which may differ from the subject domain, e.g.
	// drift is produced by model-monitor but lives under fp.models.*). The router is
	// the platform-policy component that knows the canonical subject, so it returns it
	// here. Empty → resolveEvent falls back to env.Type (best effort).
	FullType string
	// Title/Body are the server-rendered, human-facing strings for the inbox row.
	// Rendered by the router (it knows the platform's templating); the domain just
	// carries them onto the Notification.
	Title string
	Body  string
}

// RecipientRouting maps an opaque platform event to the recipient who should be
// notified and the rendered inbox content. The reactor depends on this port; the
// platform provides the concrete policy at wire time.
type RecipientRouting interface {
	// Route returns the recipient + rendered content for this envelope, or ok=false
	// if the event has no per-user recipient (the reactor then ACKs and skips it).
	// It receives the envelope METADATA and the opaque payload bytes — it MAY peek at
	// the payload to extract a recipient hint, but the REACTOR never does, preserving
	// the reactor's opacity regardless of how routing is implemented.
	Route(env natsutil.EventEnvelope) (RoutedRecipient, bool)
}

// resolveEvent turns an envelope into a domain.InboundEvent using the reactor's
// configured RecipientRouting. Returns ok=false when there is no recipient to
// notify (the reactor ACKs and moves on — not an error).
func resolveEvent(env natsutil.EventEnvelope, router RecipientRouting) (domain.InboundEvent, bool) {
	if router == nil {
		return domain.InboundEvent{}, false
	}
	routed, ok := router.Route(env)
	if !ok || routed.RecipientUserID == "" {
		return domain.InboundEvent{}, false
	}
	// Prefer the router's canonical full subject for domain pattern-matching; fall
	// back to the (prefix-stripped) envelope type when the router did not supply one.
	eventType := routed.FullType
	if eventType == "" {
		eventType = env.Type
	}
	return domain.InboundEvent{
		EventID:         env.ID,
		Type:            eventType,
		Source:          env.Source,
		RecipientUserID: routed.RecipientUserID,
		Title:           routed.Title,
		Body:            routed.Body,
		Payload:         []byte(env.Data), // opaque passthrough — never decoded
		OccurredAt:      env.Timestamp,
	}, true
}
