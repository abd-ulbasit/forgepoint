// Package events is the auth service's ASYNC adapter ring — the NATS-facing edge
// of the Clean Architecture onion. It implements the domain's outbound
// event-publisher PORT against pkg/natsutil (the platform's JetStream wrapper),
// translating auth DOMAIN objects into the canonical forgepoint.events.v1 wire
// payloads and publishing them on the platform's subject hierarchy.
//
// ============================================================================
// WHERE THIS SITS (and why it is its own package)
// ============================================================================
//
//	handler (gRPC)  ─┐
//	                 ├─► domain.AuthService ──(EventPublisher PORT)──► events.Publisher ──► NATS
//	repository ──────┘                                                 (THIS PACKAGE)
//
// The domain defines WHAT facts it emits (an outbound port: "tell the world a
// user was created"); this package defines HOW (marshal the events/v1 message,
// wrap it in the standard EventEnvelope, publish to the canonical subject with
// dedup + trace propagation). The domain never imports NATS or proto — exactly
// the dependency rule the platform's Clean Architecture mandates. The concrete
// Publisher here is injected into the request path in main.go via the
// EventingAuthService DECORATOR (service_decorator.go), which wraps the domain
// service and publishes after each mutating write commits — so the domain itself
// stays free of any event/NATS knowledge.
//
// ============================================================================
// AUTH'S EVENT ROLE (grounded in docs/design/event-contract.md)
// ============================================================================
//
//	PRODUCES:
//	  fp.auth.user.created    (UserCreated)   — after CreateUser commits.
//	  fp.auth.apikey.rotated  (ApiKeyRotated) — after an API key is minted
//	                                            (and, on rotation, the prior key
//	                                            revoked in the same operation).
//	CONSUMES: nothing. Identity is the ROOT of the platform's trust graph, not a
//	  downstream of it — the auth service reacts to no other service's events.
//	  There are therefore deliberately NO subscriber adapters in this package
//	  (unlike gateway/serving/monitor/billing/notification, which consume). This
//	  is the correct, contract-grounded shape for auth, not an omission.
//
// ============================================================================
// SERIALIZATION CHOICE — encoding/json (NOT protobuf binary / protojson)
// ============================================================================
//
// pkg/natsutil.Publisher marshals the payload with encoding/json and stores it
// in EventEnvelope.Data (a json.RawMessage); the Subscriber hands the handler
// that same json.RawMessage to decode. To round-trip cleanly, BOTH ends must use
// the same codec. We therefore marshal/decode the events/v1 messages with
// encoding/json too. The generated messages carry proto3 json tags
// (json:"user_id,omitempty", etc.), and google.protobuf.Timestamp round-trips
// through encoding/json as {"seconds":..,"nanos":..} — symmetric on both sides
// because producer and consumer share this one codec. (A platform-wide move to
// protojson would be a natsutil-level change, not an auth-level one.)
//
// INTERVIEW FRAMING: "Why JSON on the bus and not protobuf?" → the envelope/codec
// is a transport concern owned by the shared natsutil layer; the event SCHEMA is
// still the versioned events/v1 contract (buf-breaking-guarded). JSON keeps
// messages human-readable in the NATS CLI / DLQ for ops, at a modest size cost —
// acceptable for an ML control-plane's event rates (~1K/s), not a data plane.
package events

import (
	"context"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"

	"github.com/nats-io/nats.go/jetstream"
)

// ============================================================================
// PLATFORM CONSTANTS — subjects, stream, source
// ============================================================================

const (
	// SourceName is stamped into EventEnvelope.Source on every event this service
	// publishes. Per the contract, the subject records the resource DOMAIN while
	// Source records the actual PRODUCING service — here they happen to match
	// ("auth"), but keeping Source explicit is what lets a consumer audit
	// provenance uniformly across all events on the bus.
	SourceName = "auth"

	// StreamName is the JetStream stream that persists all auth-domain events.
	// One stream per producing domain (subjects fp.auth.>) keeps retention,
	// replicas, and limits configurable per domain and keeps the DLQ co-located.
	StreamName = "AUTH"

	// StreamSubjects is the wildcard this stream captures: every auth event.
	StreamSubjects = "fp.auth.>"

	// SubjectUserCreated / SubjectAPIKeyRotated are the CANONICAL subjects from
	// the event contract. They are the single source of truth shared by this
	// producer and every consumer; never hand-build these strings elsewhere.
	SubjectUserCreated   = "fp.auth.user.created"
	SubjectAPIKeyRotated = "fp.auth.apikey.rotated"
)

// EnsureStream creates (or updates) the AUTH JetStream stream so published events
// are persisted and replayable. It is idempotent — safe to call on every startup.
//
// WHY the PRODUCER ensures the stream: JetStream drops a published message if no
// stream captures its subject (core-NATS fire-and-forget semantics leak through
// when there is no stream). The owning service therefore guarantees its stream
// exists before it publishes. Consumers (other services) create CONSUMERS on this
// stream, not the stream itself — the stream is owned by the producing domain.
//
// In production this also belongs in infra-as-code, but ensuring it in-process
// makes local dev and tests self-contained and makes the ownership explicit.
func EnsureStream(ctx context.Context, js jetstream.JetStream) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     StreamName,
		Subjects: []string{StreamSubjects},
	})
	return err
}

// ============================================================================
// OUTBOUND PORT (consumer-owned interface, lives with its caller's intent)
// ============================================================================
//
// EventPublisher is the auth domain's OUTBOUND port for emitting identity
// lifecycle facts. This package's *Publisher implements it; the
// EventingAuthService decorator (service_decorator.go) DEPENDS on it and calls it
// after each mutating write commits; main.go wires the two together.
//
// WHY the interface is declared HERE (not in domain): the domain service is kept
// free of any event/NATS knowledge, so the publish concern is layered on via the
// decorator at the AuthService seam rather than injected into the domain. The port
// therefore lives with the decorator + implementation that use it. Expressing the
// port over small flat input structs (below) keeps this adapter from importing the
// domain's model types and keeps the mapping a trivial, auditable field copy.
//
// The arguments are DOMAIN types (auth.User-shaped fields), never proto: the
// domain hands its own objects to the port and stays ignorant of the wire format.
// To avoid importing the domain package from this adapter (which would risk an
// import cycle once the port moves into the domain), the port is expressed over
// small, flat input structs defined in this package; the domain populates them.
type EventPublisher interface {
	// PublishUserCreated announces a newly provisioned identity. Called by
	// CreateUser AFTER the account commits to Postgres (publish-after-commit: we
	// never announce a user that a failed transaction rolled back).
	PublishUserCreated(ctx context.Context, in UserCreatedInput) error

	// PublishAPIKeyRotated announces that the set of valid API keys for a user
	// changed — a fresh key was minted (and, on a true rotation, a prior key
	// revoked). Called AFTER the key write commits. NEVER carries the raw key.
	PublishAPIKeyRotated(ctx context.Context, in APIKeyRotatedInput) error
}

// UserCreatedInput is the flat, secret-free projection of a created user that the
// domain passes to the port. It mirrors events.v1.UserCreated's fields so the
// adapter's mapping is a trivial, auditable field copy. NOTE: no PasswordHash —
// credentials never leave the auth service over the bus.
type UserCreatedInput struct {
	UserID string
	Email  string
	Name   string
	Team   string
	Role   string // role NAME (flat RBAC), e.g. "viewer"
}

// APIKeyRotatedInput is the flat, secret-free projection of an API key lifecycle
// change. It carries the key id + the 8-char display prefix (safe to show), the
// owner, the scopes, and the id of the key this one replaces (empty on a plain
// create). It NEVER carries the raw key — that exists for one moment in
// CreateAPIKey's return and is shown to the caller once (Stripe/GitHub-PAT model).
type APIKeyRotatedInput struct {
	KeyID         string
	UserID        string
	KeyPrefix     string
	Scopes        []string
	ReplacedKeyID string // empty on first/independent create; set on a true rotation
}

// Compile-time assertion: *Publisher satisfies the port. If a port method is
// added/changed, the build breaks here (in the adapter) rather than silently at
// the wiring site in main.go.
var _ EventPublisher = (*Publisher)(nil)

// asEventsV1UserCreated maps the domain input → the canonical wire message.
// Kept as a free function (not a method) so it is trivially unit-testable and so
// the mapping for each event reads top-to-bottom in one place. createdAt is
// stamped by the adapter from the platform clock at publish time; the consumer
// reads it off the payload (the envelope timestamp is the bus's own clock).
func asEventsV1UserCreated(in UserCreatedInput, ts timestampFn) *eventsv1.UserCreated {
	return &eventsv1.UserCreated{
		UserId:    in.UserID,
		Email:     in.Email,
		Name:      in.Name,
		Team:      in.Team,
		Role:      in.Role,
		CreatedAt: ts(),
	}
}

// asEventsV1APIKeyRotated maps the domain input → the canonical wire message.
func asEventsV1APIKeyRotated(in APIKeyRotatedInput, ts timestampFn) *eventsv1.ApiKeyRotated {
	return &eventsv1.ApiKeyRotated{
		KeyId:         in.KeyID,
		UserId:        in.UserID,
		KeyPrefix:     in.KeyPrefix,
		Scopes:        in.Scopes,
		ReplacedKeyId: in.ReplacedKeyID,
		RotatedAt:     ts(),
	}
}
