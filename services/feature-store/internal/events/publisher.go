// Package events is the Feature Store's ASYNC adapter ring — the outbound edge of
// the Clean Architecture onion that turns domain facts into NATS events on the
// platform's canonical event bus.
//
// ============================================================================
// THIS SERVICE'S EVENT ROLE (grounded in docs/design/event-contract.md)
// ============================================================================
//
//	PRODUCES:  FeatureViewDefined  → fp.features.view.defined
//	           FeaturesWritten     → fp.features.written
//	CONSUMES:  (none)
//
// The Feature Store is a PURE PRODUCER on the bus. Per the canonical event
// contract's subject registry, it owns the fp.features.* domain and emits exactly
// two events; no other service's events drive its behavior, so there are NO
// subscriber adapters here (contrast the gateway/serving/monitor, which consume
// deploy/promote/drift events). This file therefore delivers only the publisher.
//
// ============================================================================
// WHERE THIS SITS — PORT & ADAPTER (hexagonal)
// ============================================================================
//
// The domain (featurestore_service_impl.go) deliberately does NOT import NATS: it
// returns the values an event needs (the defined FeatureView, the
// WriteFeaturesResult with AffectedEntityIDs) and leaves PUBLISHING to this ring.
// We model the seam as a PORT the domain/wiring depends on — the EventPublisher
// interface below — and a concrete ADAPTER (natsPublisher) that fulfills it over
// pkg/natsutil. The next stage wires natsPublisher into main.go and calls it right
// after the service's DefineFeatureView / WriteFeatures return. Keeping the port in
// the adapter package (not the domain) is fine here because the domain does not yet
// CALL it — the orchestration that fires these events lives at the wiring edge, so
// the consumer of the port is main.go, and "the consumer owns the port" places it
// here. (If a later refactor moves publishing INTO the service impl, this interface
// moves into domain/ports.go unchanged — the signatures are domain-typed precisely
// so that move is a no-op for callers.)
//
// ============================================================================
// WHY protojson, NOT encoding/json, FOR THE PAYLOAD
// ============================================================================
//
// The payloads are the generated forgepoint.events.v1 messages — the SINGLE
// SOURCE OF TRUTH every producer and consumer shares (see events.proto's design
// block). They MUST go on the wire as canonical protobuf-JSON:
//
//   - encoding/json on a generated message emits the wrong shape: it would try to
//     serialize protoimpl internal state fields and would NOT honor the proto
//     json_name camelCase mapping or the well-known-type encodings (a
//     google.protobuf.Timestamp must serialize as an RFC-3339 STRING, an Any as
//     {"@type":...}). A consumer using protojson.Unmarshal would then fail.
//   - protojson is the contract-faithful encoder: it produces exactly the JSON a
//     protojson.Unmarshal on the consumer side round-trips, across languages.
//
// natsutil.Publisher.Publish takes `any` and json.Marshal's it. json.Marshal of a
// json.RawMessage is the IDENTITY (it copies the bytes verbatim). So we protojson-
// marshal the payload ourselves and hand the result to the Publisher as a
// json.RawMessage — the Publisher then wraps those exact bytes in the
// EventEnvelope.Data field, stamps source/trace/correlation, and publishes with the
// envelope ID as the Nats-Msg-Id dedup key. We get canonical payload encoding AND
// all of natsutil's envelope/observability/dedup machinery.
//
// ============================================================================
// DOMAIN ENUM → EVENT ENUM AT THE BOUNDARY (decoupling tax, paid here)
// ============================================================================
//
// The Feature Store's two events carry only scalars/strings/ids — no enum needs
// mapping (unlike, say, ModelPromoted's ModelStage). So the conversion below is a
// straight field copy. The discipline still holds: the adapter is the ONE place
// that knows both the domain shape and the wire shape, so the domain stays
// proto-free and the wire contract stays domain-free.
package events

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/services/feature-store/internal/domain"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ============================================================================
// SUBJECTS & SOURCE (the canonical strings — single source of truth in code)
// ============================================================================

const (
	// SourceName is stamped into EventEnvelope.Source so consumers and operators
	// know which service produced the event. Per the contract, the subject names
	// the RESOURCE domain (fp.features.*) while Source names the PRODUCER — here
	// they happen to coincide, but the distinction is the platform rule.
	SourceName = "feature-store"

	// SubjectFeatureViewDefined / SubjectFeaturesWritten are the two canonical
	// subjects from the event-contract subject registry. They are constants (not
	// derived) so a typo is a compile-time-stable, grep-able fact and the tests pin
	// the exact strings a consumer subscribes to.
	SubjectFeatureViewDefined = "fp.features.view.defined"
	SubjectFeaturesWritten    = "fp.features.written"

	// StreamName is the JetStream stream that captures the fp.features.> subject
	// tree. The stream is created at infra/wiring time; named here so the publisher
	// and its tests reference one canonical value.
	StreamName = "FEATURES"

	// StreamSubjects is the wildcard the FEATURES stream binds, capturing both of
	// this service's subjects (and any future fp.features.* event) in one stream.
	StreamSubjects = "fp.features.>"
)

// ============================================================================
// EventPublisher — the OUTBOUND PORT (domain-typed; NATS-free signature)
// ============================================================================
//
// The wiring layer depends on THIS interface, not the concrete natsPublisher.
// WHY an interface and not just the struct:
//   - main.go can inject a no-op/fake in a wiring test without a NATS server.
//   - the signatures speak DOMAIN types (FeatureView, WriteFeaturesResult), so a
//     caller never sees proto or NATS — the same anti-corruption boundary the
//     handler gives the inbound side.
//
// PublishFailures are returned, not swallowed: the caller decides the policy. For
// the Feature Store the event is a NOTIFICATION (thin event — see FeaturesWritten
// in the contract), so a publish failure after a committed Append is a
// best-effort/at-least-once concern: the caller logs and moves on (the write is
// durable in the log; a missed notification can be backfilled by a RebuildViews-
// style replay or an outbox in a later hardening pass). We surface the error so
// that policy is the CALLER's, not silently hidden in the adapter.
type EventPublisher interface {
	// PublishFeatureViewDefined emits fp.features.view.defined for a view that was
	// just created or evolved. Takes the FULL projected FeatureView (the fold
	// result the service returns) so the adapter reads id/name/schema/team/clock off
	// the authoritative domain object — never off client input.
	PublishFeatureViewDefined(ctx context.Context, view domain.FeatureView) error

	// PublishFeaturesWritten emits fp.features.written after a WriteFeatures append
	// commits. Takes the target view (for id+name) and the WriteFeaturesResult (the
	// count, the assigned version range, and the affected entity ids the domain
	// produced precisely for this event). THIN BY DESIGN: ids + counts + version,
	// never the feature values — a consumer that wants values calls back via
	// GetOnlineFeatures (the contract's thin-event tradeoff).
	PublishFeaturesWritten(ctx context.Context, view domain.FeatureView, res domain.WriteFeaturesResult) error
}

// natsPublisher is the concrete adapter: it builds the canonical events.v1 payload
// from the domain object, protojson-encodes it, and hands it to pkg/natsutil's
// Publisher (which wraps the envelope, stamps source/trace/correlation, and
// publishes with the envelope ID as the JetStream dedup key).
type natsPublisher struct {
	pub *natsutil.Publisher
}

// compile-time proof the adapter satisfies the port.
var _ EventPublisher = (*natsPublisher)(nil)

// NewPublisher constructs the NATS-backed EventPublisher. The caller passes a
// *natsutil.Publisher already bound to this service's source name (via
// natsutil.NewPublisher(js, events.SourceName)); we wrap it so all the
// envelope/dedup/trace behavior is inherited rather than reimplemented.
//
// WHY take *natsutil.Publisher rather than jetstream.JetStream: the Publisher is
// the seam pkg/natsutil exposes and unit-tests, and it is where source is bound.
// Threading it in keeps this adapter a thin payload-builder on top of the shared
// library — the exact "compose, don't reinvent" the pkg/ split exists to enable.
func NewPublisher(pub *natsutil.Publisher) EventPublisher {
	return &natsPublisher{pub: pub}
}

// PublishFeatureViewDefined builds + publishes the FeatureViewDefined event.
//
// FIELD SOURCING (all server-authoritative, read off the folded FeatureView):
//   - feature_view_id / feature_view_name: the view's identity.
//   - schema_version: 1 on create, +1 per evolve — exactly what the consumer
//     (experiment-tracker) correlates a training run's feature schema against.
//   - owner_team: the team-scoping/audit boundary, set by the domain from the
//     Principal, never from client input.
//   - defined_at: the view's UpdatedAt — the producer-clock instant this
//     definition/evolution committed (CreatedAt on first define == UpdatedAt then;
//     on an evolve UpdatedAt is the newer edge, which is the "defined_at" of THIS
//     schema version). Using the domain's clock-stamped field (not time.Now here)
//     keeps the event's timestamp consistent with the persisted view and
//     deterministic under the injected test clock.
func (p *natsPublisher) PublishFeatureViewDefined(ctx context.Context, view domain.FeatureView) error {
	payload := &eventsv1.FeatureViewDefined{
		FeatureViewId:   view.ID,
		FeatureViewName: view.Name,
		SchemaVersion:   view.SchemaVersion,
		OwnerTeam:       view.OwnerTeam,
		DefinedAt:       timestampOrNil(view.UpdatedAt),
	}
	return p.publish(ctx, SubjectFeatureViewDefined, payload)
}

// PublishFeaturesWritten builds + publishes the FeaturesWritten event.
//
// FIELD SOURCING:
//   - feature_view_id / feature_view_name: from the target view.
//   - entity_ids: res.AffectedEntityIDs — the DISTINCT entities touched by the
//     batch, produced by the domain expressly so Model Monitor can scope drift
//     checks / cache invalidation to exactly the changed entities. MAY be
//     truncated for huge batches; written_count stays authoritative.
//   - written_count: res.WrittenCount — the authoritative total rows appended.
//   - written_through_version: res.WrittenThroughVersion — the highest log version
//     this append assigned, so a consumer can request as_of_version >= it to be
//     sure it observes the new data (read-your-writes across the bus).
//   - written_at: the view's UpdatedAt is NOT the right clock here (the view didn't
//     change); a write has no domain timestamp on the result, so we stamp the
//     producer clock at publish via timestamppb.Now(). This is the one field with
//     no upstream domain value to carry, and "now at publish" is the correct
//     producer-clock semantics for "when the append committed" from the bus's view.
func (p *natsPublisher) PublishFeaturesWritten(ctx context.Context, view domain.FeatureView, res domain.WriteFeaturesResult) error {
	payload := &eventsv1.FeaturesWritten{
		FeatureViewId:         view.ID,
		FeatureViewName:       view.Name,
		EntityIds:             res.AffectedEntityIDs,
		WrittenCount:          int32(res.WrittenCount),
		WrittenThroughVersion: res.WrittenThroughVersion,
		WrittenAt:             timestamppb.Now(),
	}
	return p.publish(ctx, SubjectFeaturesWritten, payload)
}

// publish is the shared marshal-and-send path. It protojson-encodes the payload
// (canonical wire form — see the package doc) and forwards the raw bytes to
// natsutil.Publisher. Passing a json.RawMessage means natsutil's internal
// json.Marshal copies the bytes verbatim into EventEnvelope.Data, so the consumer
// reads exactly the protojson we produced.
func (p *natsPublisher) publish(ctx context.Context, subject string, payload proto.Message) error {
	// protojson options: we accept defaults (omit zero-valued scalars, RFC-3339
	// timestamps, camelCase field names). EmitUnpopulated is intentionally OFF —
	// a thin event should not bloat the wire with explicit zeros; a consumer
	// treats an absent field as its zero value, which is the proto3 contract.
	data, err := protojson.Marshal(payload)
	if err != nil {
		return fmt.Errorf("events: marshal %s payload: %w", subject, err)
	}

	// json.RawMessage so natsutil's json.Marshal is the identity on these bytes —
	// the protojson encoding lands unchanged in EventEnvelope.Data.
	if err := p.pub.Publish(ctx, subject, json.RawMessage(data)); err != nil {
		return fmt.Errorf("events: publish %s: %w", subject, err)
	}
	return nil
}

// timestampOrNil maps a domain time.Time to a *timestamppb.Timestamp, returning
// nil for the zero time so the event omits the field (rather than emitting the
// proto epoch, which would be a misleading "1970" on the wire). Pulled out so the
// zero-time rule is applied identically wherever a domain timestamp becomes a
// proto one.
func timestampOrNil(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}
