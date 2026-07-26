package events

import (
	"context"
	"time"

	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// ============================================================================
// PUBLISHER ADAPTER
// ============================================================================
//
// Publisher is the auth service's concrete EventPublisher. It is a thin
// translation layer over pkg/natsutil.Publisher:
//
//	domain object ──► events.v1 message ──► natsutil.Publisher.Publish ──► NATS
//	                  (this adapter maps)    (envelope + dedup + trace)
//
// natsutil.Publisher already handles the cross-cutting concerns the contract
// requires on EVERY event, so this adapter does NOT re-implement them:
//   - wraps the payload in the standard EventEnvelope,
//   - stamps Source = SourceName ("auth") (set once at construction),
//   - sets the Nats-Msg-Id = envelope.ID for JetStream publish-side dedup,
//   - continues the correlation id from ctx and injects the W3C trace context
//     so the async hop stays on the same distributed trace in Tempo.
//
// This adapter's ONLY job is the mapping (domain → events/v1) and choosing the
// canonical subject. That separation is deliberate: the platform-wide envelope/
// dedup/trace policy lives in ONE place (natsutil), and each service's adapter
// stays a small, obviously-correct field copy.
// ============================================================================

// timestampFn produces the payload's producer-clock timestamp. Injected so tests
// can pin a deterministic time and assert exact round-trip equality; production
// uses nowProto (time.Now in UTC).
type timestampFn func() *timestamppb.Timestamp

// nowProto returns the current UTC time as a protobuf Timestamp — the default
// producer clock. UTC because event times cross service/timezone boundaries and
// must be comparable; the bus is not the place for local time.
func nowProto() *timestamppb.Timestamp {
	return timestamppb.New(time.Now().UTC())
}

// Publisher publishes auth domain events onto NATS via natsutil.
type Publisher struct {
	pub *natsutil.Publisher

	// now is the producer clock for payload timestamps. Defaults to nowProto;
	// overridden in tests for deterministic assertions.
	now timestampFn
}

// NewPublisher constructs the auth event Publisher over an existing natsutil
// Publisher.
//
// WHY take a *natsutil.Publisher rather than a jetstream.JetStream directly:
// main.go constructs ONE natsutil.Publisher per service (it owns the Source name
// and the dedup/trace policy) and shares it; this adapter layers the auth-specific
// mapping on top. Passing the lower-level JS would duplicate that wiring and risk
// two publishers disagreeing on Source.
func NewPublisher(pub *natsutil.Publisher) *Publisher {
	return &Publisher{pub: pub, now: nowProto}
}

// PublishUserCreated maps the domain input to events.v1.UserCreated and publishes
// it on fp.auth.user.created.
//
// PUBLISH-AFTER-COMMIT ORDERING (the caller's contract): the domain calls this
// only AFTER the user row commits. If we published first and the commit then
// failed, consumers would act on a user that does not exist (a "phantom event").
// The accepted tradeoff is the inverse, far-less-harmful failure mode: the commit
// succeeds but this publish fails → the event is LOST (at-most-once for that hop).
// For a portfolio control-plane this is acceptable; the gold-standard fix is the
// TRANSACTIONAL OUTBOX (write the event to an outbox table IN the same tx, a relay
// publishes it) — which is exactly the pattern the BILLING service implements.
// Auth deliberately uses the simpler publish-after-commit here; the outbox is
// the named upgrade path.
func (p *Publisher) PublishUserCreated(ctx context.Context, in UserCreatedInput) error {
	payload := asEventsV1UserCreated(in, p.now)
	return p.pub.Publish(ctx, SubjectUserCreated, payload)
}

// PublishAPIKeyRotated maps the domain input to events.v1.ApiKeyRotated and
// publishes it on fp.auth.apikey.rotated.
//
// SECURITY INVARIANT: the input carries NO raw key (only id + display prefix), so
// nothing secret can reach the bus through this path even by mistake — the type
// system makes the safe thing the only thing.
func (p *Publisher) PublishAPIKeyRotated(ctx context.Context, in APIKeyRotatedInput) error {
	payload := asEventsV1APIKeyRotated(in, p.now)
	return p.pub.Publish(ctx, SubjectAPIKeyRotated, payload)
}
