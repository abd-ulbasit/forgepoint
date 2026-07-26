// publisher.go — the NATS-backed EVENT ADAPTER for the Model Registry.
//
// ============================================================================
// WHERE THIS SITS (Clean Architecture / Hexagonal)
// ============================================================================
//
// The domain defines a CONSUMER-OWNED PORT, domain.ProjectionEmitter, with one
// method: Emit(ctx, ProjectionEvent). The command service calls it AFTER a
// WriteStore commit to announce "this state change happened". The domain speaks
// only its OWN vocabulary (domain.ProjectionEvent) and never imports gen/go,
// NATS, or any wire type — that is the "domain has zero framework imports" rule.
//
// THIS FILE is the ADAPTER that implements that port for real:
//
//	domain.ProjectionEvent  ──(this adapter)──►  eventsv1.* payload
//	                                              + canonical fp.models.* subject
//	                                              + EventEnvelope (id/source/trace)
//	                                              ──►  NATS JetStream
//
// So the wire schema (eventsv1) and the transport (NATS) live ONLY here. Swap
// NATS for Kafka, or eventsv1 for a new contract version, and the domain/service
// do not change a line. This is the whole point of the port/adapter seam the
// ports.go header describes ("the NATS-backed adapter lands in the events phase").
//
// ============================================================================
// WHAT THE REGISTRY PRODUCES (grounded in docs/design/event-contract.md)
// ============================================================================
//
// The Registry is a PRODUCER of five model-lifecycle facts and a CONSUMER of
// none. Each ProjectionEventKind maps 1:1 to a canonical event + subject:
//
//	┌──────────────────────┬───────────────────────────┬───────────────────────────────┐
//	│ ProjectionEventKind  │ subject                   │ eventsv1 payload              │
//	├──────────────────────┼───────────────────────────┼───────────────────────────────┤
//	│ EventModelRegistered │ fp.models.registered      │ eventsv1.ModelRegistered      │
//	│ EventVersionCreated  │ fp.models.version.created │ eventsv1.ModelVersionCreated  │
//	│ EventVersionReady    │ fp.models.version.ready  │ eventsv1.ModelVersionReady    │
//	│ EventModelPromoted   │ fp.models.promoted        │ eventsv1.ModelPromoted        │
//	│ EventModelArchived   │ fp.models.archived        │ eventsv1.ModelArchived        │
//	└──────────────────────┴───────────────────────────┴───────────────────────────────┘
//
// This is the CQRS "ProjectionEmitter" half: the SAME emitted fact feeds BOTH
// the internal Redis read-projection (via a projection consumer, a later phase)
// AND external consumers (serving, billing, monitor, gateway, orchestrator,
// experiment-tracker). One event, many readers — the command never dual-writes.
//
// ============================================================================
// WHY THE PAYLOAD IS A FLAT eventsv1.* MESSAGE, NOT domain.Model
// ============================================================================
//
// The event contract (proto header + event-contract.md) is emphatic: an event
// payload must NOT embed a service's domain/API type, or every consumer would
// compile-depend on the registry's types just to read an event. We therefore map
// the domain object's fields into the FLAT, decoupled eventsv1 message here, at
// the publish boundary. The cost is a few lines of mechanical field copying per
// event (below); the payoff is that the event schema is a published contract that
// evolves independently of the registry's internal model (schema-registry think).
//
// ============================================================================
// ENVELOPE / SOURCE / TRACE — handled by pkg/natsutil.Publisher
// ============================================================================
//
// We do NOT hand-roll the envelope. pkg/natsutil.Publisher.Publish wraps the
// payload in an EventEnvelope: it mints the dedup id (Nats-Msg-Id), sets
// Source = "registry" (the source we construct it with — see NewPublisher),
// derives Type from the subject ("registered", "version.created", ...), and
// propagates the W3C trace context + correlation id from ctx so a consumer's
// spans attach to the registry's trace across the async hop. Keeping all of that
// in the shared lib is exactly why it exists — every producer behaves the same.
package events

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/services/registry/internal/domain"
)

// ServiceName is the value stamped into EventEnvelope.Source for every event the
// registry emits. The SUBJECT records the resource domain (fp.models.*), while
// Source records the actual PRODUCING service — the ownership distinction the
// event contract calls out (a model's drift events live under fp.models.* too but
// are sourced by "model-monitor"; ours are genuinely sourced by "registry").
const ServiceName = "registry"

// ============================================================================
// SUBJECT CONSTANTS — the canonical fp.<domain>.<event...> registry (authoritative).
// ============================================================================
//
// Hard-coded here (not derived) because the subject is a PUBLISHED CONTRACT: a
// typo would silently route an event into the void where no consumer listens.
// These exact strings are the source-of-truth subject map from event-contract.md.
const (
	// SubjectModelRegistered — a model was registered (RegisterModel committed).
	SubjectModelRegistered = "fp.models.registered"
	// SubjectModelVersionCreated — a version row was created (PENDING_UPLOAD).
	SubjectModelVersionCreated = "fp.models.version.created"
	// SubjectModelVersionReady — a version's artifact became READY (servable).
	SubjectModelVersionReady = "fp.models.version.ready"
	// SubjectModelPromoted — the atomic single-production stage swap committed.
	SubjectModelPromoted = "fp.models.promoted"
	// SubjectModelArchived — the model (+ its versions) was soft-deleted.
	SubjectModelArchived = "fp.models.archived"
)

// natsPublisher is the seam the domain's ProjectionEmitter port depends on,
// narrowed to JUST the method this adapter uses. WHY a local interface rather
// than referencing *natsutil.Publisher directly: it makes the adapter trivially
// unit-testable with a fake (assert subject+payload without a broker) AND keeps
// the dependency surface honest (we use exactly one method). *natsutil.Publisher
// satisfies it structurally, so production wiring is unchanged.
type natsPublisher interface {
	Publish(ctx context.Context, subject string, payload any) error
}

// Publisher is the NATS-backed implementation of domain.ProjectionEmitter. It
// translates a domain.ProjectionEvent into the canonical eventsv1.* payload +
// fp.models.* subject and publishes via pkg/natsutil.Publisher.
//
// Compile-time assertion (below) that it satisfies the port — if the port grows
// a method, this fails to build instead of failing mysteriously at wiring time.
type Publisher struct {
	pub natsPublisher
}

// Static interface conformance check. This line is the contract: Publisher MUST
// remain a valid domain.ProjectionEmitter. Cheap, zero-cost, catches drift.
var _ domain.ProjectionEmitter = (*Publisher)(nil)

// NewPublisher wires the adapter over a *natsutil.Publisher. main.go (next stage)
// constructs the natsutil.Publisher with source = ServiceName and passes it here.
//
//	js, _ := jetstream.New(conn)
//	np := natsutil.NewPublisher(js, events.ServiceName)
//	emitter := events.NewPublisher(np)   // ← satisfies domain.ProjectionEmitter
//	svc := domain.NewService(..., emitter)
func NewPublisher(pub natsPublisher) *Publisher {
	return &Publisher{pub: pub}
}

// Emit implements domain.ProjectionEmitter. It switches on the event Kind to pick
// the subject + build the matching eventsv1 payload, then publishes.
//
// FAILURE POSTURE (per the ports.go contract): the service
// calls Emit AFTER the WriteStore commit. The write is the truth and must not be
// undone by a publish failure, so Emit returns the error for the service to LOG —
// the service does NOT roll back the already-committed command. The projection
// (and external consumers) catch up on the next event or a rebuild. The robust
// production form moves the emit INTO the write tx via the Outbox pattern (the
// Billing service); this adapter is the seam an outbox-backed relay slots into.
func (p *Publisher) Emit(ctx context.Context, ev domain.ProjectionEvent) error {
	switch ev.Kind {
	case domain.EventModelRegistered:
		return p.pub.Publish(ctx, SubjectModelRegistered, modelRegistered(ev))
	case domain.EventVersionCreated:
		return p.pub.Publish(ctx, SubjectModelVersionCreated, modelVersionCreated(ev))
	case domain.EventVersionReady:
		return p.pub.Publish(ctx, SubjectModelVersionReady, modelVersionReady(ev))
	case domain.EventModelPromoted:
		return p.pub.Publish(ctx, SubjectModelPromoted, modelPromoted(ev))
	case domain.EventModelArchived:
		return p.pub.Publish(ctx, SubjectModelArchived, modelArchived(ev))
	default:
		// An unknown kind is a PROGRAMMING error (a new ProjectionEventKind added
		// without a mapping here), not a runtime/data error. Surface it loudly so
		// it is caught in dev/tests rather than silently dropping a platform fact.
		return fmt.Errorf("registry/events: no subject mapping for projection kind %d", ev.Kind)
	}
}

// ============================================================================
// DOMAIN → eventsv1 MAPPERS (one per produced event)
// ============================================================================
//
// Each mapper copies the FLAT fields the contract specifies for that event from
// the domain object into the wire message. They are pure functions (no ctx, no
// I/O) so they are dead-simple to unit-test and to read. The
// *_by audit fields come from ev.Actor.UserID — the server-authoritative caller
// identity (never a client value), exactly as the proto field docs require.

// modelRegistered builds the fp.models.registered payload from a RegisterModel
// commit. Carries identity + ownership so pipeline-orchestrator / experiment-
// tracker can associate downstream work without a callback into the registry.
func modelRegistered(ev domain.ProjectionEvent) *eventsv1.ModelRegistered {
	m := ev.Model
	return &eventsv1.ModelRegistered{
		ModelId:      m.ID,
		ModelName:    m.Name,
		Framework:    m.Framework,
		TaskType:     m.TaskType,
		OwnerId:      m.OwnerID, // server-authoritative (from auth claims)
		Team:         m.Team,    // server-authoritative (from auth claims)
		RegisteredAt: timestamppb.New(ev.OccurredAt),
	}
}

// modelVersionCreated builds the fp.models.version.created payload. Fires at ROW
// CREATION (status PENDING_UPLOAD) — NOT a signal the artifact is usable; a
// consumer needing a usable artifact waits for ModelVersionReady instead.
func modelVersionCreated(ev domain.ProjectionEvent) *eventsv1.ModelVersionCreated {
	m, v := ev.Model, ev.Version
	return &eventsv1.ModelVersionCreated{
		ModelId:   m.ID,
		ModelName: m.Name, // denormalized so consumers needn't look the model up
		VersionId: v.ID,
		Version:   v.Version,
		CreatedBy: v.CreatedBy, // lineage/audit (auth claims)
		CreatedAt: timestamppb.New(ev.OccurredAt),
	}
}

// modelVersionReady builds the fp.models.version.ready payload — the
// PENDING_UPLOAD→READY edge that makes a version physically servable. Carries the
// SERVER-MEASURED artifact facts (path/digest/size): serving verifies it loaded
// the exact registered bytes (supply-chain integrity), billing meters storage on
// size. These are taken from the version AFTER MarkVersionReady set them — never
// client-asserted.
func modelVersionReady(ev domain.ProjectionEvent) *eventsv1.ModelVersionReady {
	m, v := ev.Model, ev.Version
	return &eventsv1.ModelVersionReady{
		ModelId:        m.ID,
		ModelName:      m.Name,
		VersionId:      v.ID,
		Version:        v.Version,
		ArtifactPath:   v.ArtifactPath,
		ArtifactDigest: v.ArtifactDigest,
		SizeBytes:      v.SizeBytes,
		ReadyAt:        timestamppb.New(ev.OccurredAt),
	}
}

// modelPromoted builds the fp.models.promoted payload, carrying BOTH ends of the
// atomic single-production swap. THE SINGLE-PRODUCTION INVARIANT: promoting B to
// PRODUCTION atomically demotes the prior production version A to ARCHIVED; the
// event carries A (the demoted version) so a consumer sees the whole swap and can
// tear down the old deployment. from_stage/to_stage make it self-describing so a
// consumer can ignore transitions it doesn't care about (Monitor reacts only when
// to_stage == PRODUCTION).
//
// When there was NO prior production version, ev.DemotedVersion is the zero value
// (ID == ""), so DemotedVersionId/DemotedVersion are emitted empty — exactly the
// "Empty if none" the contract specifies.
func modelPromoted(ev domain.ProjectionEvent) *eventsv1.ModelPromoted {
	m, v, d := ev.Model, ev.Version, ev.DemotedVersion
	return &eventsv1.ModelPromoted{
		ModelId:          m.ID,
		ModelName:        m.Name,
		VersionId:        v.ID,
		Version:          v.Version,
		FromStage:        toEventStage(ev.FromStage), // domain enum → event enum
		ToStage:          toEventStage(ev.ToStage),
		DemotedVersionId: d.ID,      // "" when no prior production existed
		DemotedVersion:   d.Version, // ""
		PromotedBy:       ev.Actor.UserID,
		PromotedAt:       timestamppb.New(ev.OccurredAt),
	}
}

// modelArchived builds the fp.models.archived payload. Carries model_name too
// because gateway/serving key routes/loads BY NAME — carrying it saves every
// consumer a lookup at exactly the moment the model is going away.
func modelArchived(ev domain.ProjectionEvent) *eventsv1.ModelArchived {
	m := ev.Model
	return &eventsv1.ModelArchived{
		ModelId:    m.ID,
		ModelName:  m.Name,
		ArchivedBy: ev.Actor.UserID,
		ArchivedAt: timestamppb.New(ev.OccurredAt),
	}
}

// ============================================================================
// ENUM MAPPING — domain.ModelStage → eventsv1.ModelStage (the decoupling tax)
// ============================================================================
//
// The event contract RE-DECLARES ModelStage in the events package precisely so the
// event schema doesn't import the registry's API/domain enum. The price is this
// mechanical mapping at the publish boundary. We map explicitly (not by numeric
// coincidence) so that if either enum is ever reordered, this stays correct and
// the intent is auditable. The numeric values DO currently align byte-for-byte
// (UNSPECIFIED=0..ARCHIVED=4), but relying on that implicitly would be a latent
// bug: if someone inserts a stage in the middle, this switch breaks visibly
// where the implicit cast would not.
func toEventStage(s domain.ModelStage) eventsv1.ModelStage {
	switch s {
	case domain.StageDev:
		return eventsv1.ModelStage_MODEL_STAGE_DEV
	case domain.StageStaging:
		return eventsv1.ModelStage_MODEL_STAGE_STAGING
	case domain.StageProduction:
		return eventsv1.ModelStage_MODEL_STAGE_PRODUCTION
	case domain.StageArchived:
		return eventsv1.ModelStage_MODEL_STAGE_ARCHIVED
	default:
		return eventsv1.ModelStage_MODEL_STAGE_UNSPECIFIED
	}
}
