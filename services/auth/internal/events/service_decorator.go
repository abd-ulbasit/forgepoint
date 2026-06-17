package events

// ============================================================================
// EVENTING DECORATOR — the publish-after-commit seam for auth (Phase 1.6)
// ============================================================================
//
// THE PROBLEM THIS SOLVES
//
// The auth domain service (domain.NewAuthService) intentionally imports NO NATS
// and NO proto — that is the Clean Architecture dependency rule and it is correct.
// But the platform's event contract (proto/forgepoint/events/v1/events.proto)
// declares auth as the PRODUCER of two facts with real downstream consumers:
//
//	fp.auth.user.created    → notification (welcome), billing (open team account),
//	                          experiment-tracker / audit
//	fp.auth.apikey.rotated  → notification, inference-gateway (API-key cache
//	                          eviction — so a rotated/revoked key stops
//	                          authenticating immediately instead of at TTL)
//
// Without a publish, the producer half of those flows is dead: the gateway never
// evicts a rotated key, billing never opens the tenant account, no welcome fires.
//
// THE PATTERN — DECORATOR over the domain.AuthService PORT
//
//	┌──────────┐   domain.AuthService   ┌──────────────────────┐   domain.AuthService   ┌───────────────┐
//	│ handler  │ ─────────────────────► │ EventingAuthService  │ ─────────────────────► │ authService   │
//	│ (gRPC)   │                        │   (THIS TYPE)        │                        │ (domain impl) │
//	└──────────┘                        │  call inner, then    │                        └───────────────┘
//	                                    │  publish on success  │
//	                                    └──────────┬───────────┘
//	                                               │ PublishUserCreated / PublishAPIKeyRotated
//	                                               ▼
//	                                          events.Publisher ──► NATS
//
// The decorator IS a domain.AuthService (it embeds one and overrides exactly the
// two mutating methods that emit events). main.go wraps the real service with it
// before handing it to the handler, so the handler — and every other layer — is
// unchanged and unaware. This is the textbook Decorator: same interface, layered
// behavior, composed at the wiring root. It is also how you add cross-cutting
// concerns (caching, metrics, events) WITHOUT editing the core that owns the
// business rule — the open/closed principle applied at a port.
//
// WHY a decorator instead of injecting the publisher INTO authService:
//   - It keeps the domain free of any event/NATS knowledge (the dependency rule).
//   - It composes at the SAME seam the handler already uses (the AuthService
//     interface), so no handler or domain signature changes.
//   - The two are equivalent on the wire; the decorator is the less invasive of
//     the two and isolates "when do we publish" in one auditable place.
//
// Tradeoff vs. the gold standard (the BILLING service's transactional outbox):
// this is publish-AFTER-commit, which is at-most-once for the event hop — if the
// process dies between the committed write and the publish, the event is LOST.
// That is the accepted, documented posture for auth's control-plane events (see
// publisher.go). The outbox (write the event in the SAME tx, a relay publishes)
// is the upgrade path and is exactly what billing implements; auth names it as
// the interview answer rather than implementing it here.
//
// ----------------------------------------------------------------------------
// PUBLISH-AFTER-COMMIT, ENFORCED BY ORDERING
//
// The inner domain method returns ONLY after its repository write has committed
// (CreateUser → userRepo.Create; CreateAPIKey → apiKeyRepo.Create). So calling
// the inner method FIRST and publishing only on a nil error is, by construction,
// publish-after-commit: we never announce a user/key that a failed write rolled
// back (no "phantom event").
//
// A publish failure must NOT fail the request: the user/key is already durably
// committed and was already returned to the caller's intent. Failing the RPC here
// would be a lie ("creation failed") about a creation that SUCCEEDED, and would
// invite a retry that hits a duplicate-email / duplicate-write error. So we LOG
// the publish failure (ops/alerting surface) and still return the successful
// result. The lost event is the documented at-most-once tradeoff above.
// ============================================================================

import (
	"context"
	"log/slog"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/auth/internal/domain"
)

// EventingAuthService decorates a domain.AuthService and publishes the auth
// lifecycle events the platform contract promises, AFTER the wrapped call's write
// commits. Every method not overridden here is served by the embedded inner
// service unchanged (Go embedding promotes its methods), so the decorator stays a
// thin two-method overlay rather than a re-implementation of the whole interface.
type EventingAuthService struct {
	// Embedding the interface (not a concrete type) promotes ALL of
	// domain.AuthService's methods onto EventingAuthService for free. We then
	// override only CreateUser and CreateAPIKey below. If a new RPC is added to
	// AuthService, the decorator keeps compiling and transparently forwards it —
	// the decorator only ever intercepts the methods that emit events.
	domain.AuthService

	pub    EventPublisher
	logger *slog.Logger
}

// Compile-time assertion: the decorator IS a domain.AuthService, so it is a
// drop-in for the handler. If the interface drifts, this breaks here at the
// wiring adapter rather than mysteriously at the handler call site.
var _ domain.AuthService = (*EventingAuthService)(nil)

// NewEventingAuthService wraps inner with event publishing on its mutating paths.
//
// inner  — the real domain service (NewAuthService's result).
// pub    — the auth EventPublisher (this package's *Publisher in production).
// logger — where publish failures are recorded; a nil logger falls back to the
//
//	slog default so a forgotten wire never nil-panics on the publish path.
func NewEventingAuthService(inner domain.AuthService, pub EventPublisher, logger *slog.Logger) *EventingAuthService {
	if logger == nil {
		logger = slog.Default()
	}
	return &EventingAuthService{AuthService: inner, pub: pub, logger: logger}
}

// CreateUser provisions the user via the inner service, then — only on success —
// publishes UserCreated. The inner call returns after the user row commits, so
// this is publish-after-commit by ordering (see file header).
func (e *EventingAuthService) CreateUser(ctx context.Context, input domain.CreateUserInput) (domain.User, error) {
	user, err := e.AuthService.CreateUser(ctx, input)
	if err != nil {
		// The write failed/rolled back — emit NOTHING. Returning the error
		// unchanged preserves the inner service's precise error contract
		// (ErrEmailAlreadyExists, ErrValidation, wrapped DB errors).
		return user, err
	}

	// Map the committed domain.User → the secret-free event input. PasswordHash is
	// deliberately NOT carried (credentials never leave auth over the bus); the
	// UserCreatedInput type has no field for it, so the safe thing is the only
	// thing the type system permits.
	in := UserCreatedInput{
		UserID: user.ID,
		Email:  user.Email,
		Name:   user.Name,
		Team:   user.Team,
		Role:   user.Role.Name, // role NAME (flat RBAC), e.g. "viewer"
	}
	if pubErr := e.pub.PublishUserCreated(ctx, in); pubErr != nil {
		// Publish-failure policy: the user is already committed and is being
		// returned successfully. Do NOT fail the RPC; record the lost event so
		// ops/alerting can see the at-most-once gap. (Upgrade path: outbox.)
		e.logger.ErrorContext(ctx, "auth: failed to publish UserCreated event (user is committed; event lost)",
			slog.String("subject", SubjectUserCreated),
			slog.String("user_id", user.ID),
			slog.String("error", pubErr.Error()),
		)
	}
	return user, nil
}

// CreateAPIKey mints the key via the inner service, then — only on success —
// publishes ApiKeyRotated. Publish-after-commit by ordering (the inner call
// returns after the key row commits).
//
// ReplacedKeyID NOTE (honest scope): the domain's CreateAPIKey only MINTS a key;
// it does not revoke a prior one (revocation is the separate RevokeAPIKey RPC). A
// "true rotation" (mint-new-then-revoke-old in one operation) is not a single
// domain call today, so at THIS seam there is no prior-key id to carry — this is
// an INDEPENDENT create, and ReplacedKeyID is therefore correctly empty. The
// field exists on the wire (and on APIKeyRotatedInput) precisely so that a future
// rotation path — which would have both ids — populates it without a contract
// change. Emitting on every create is also correct for the headline consumer:
// inference-gateway treats apikey.rotated as "the set of valid keys for this user
// changed — refresh your cache," which a brand-new key also warrants.
func (e *EventingAuthService) CreateAPIKey(ctx context.Context, userID string, scopes []string, expiresAt *time.Time) (domain.APIKey, string, error) {
	apiKey, rawKey, err := e.AuthService.CreateAPIKey(ctx, userID, scopes, expiresAt)
	if err != nil {
		return apiKey, rawKey, err
	}

	// SECURITY: map ONLY the non-secret projection. We carry the key id + 8-char
	// display prefix + scopes — never rawKey and never KeyHash. rawKey stays a
	// local return value (shown to the caller once) and never reaches this input.
	in := APIKeyRotatedInput{
		KeyID:         apiKey.ID,
		UserID:        apiKey.UserID,
		KeyPrefix:     apiKey.KeyPrefix,
		Scopes:        apiKey.Scopes,
		ReplacedKeyID: "", // independent create; see method doc for the rotation seam
	}
	if pubErr := e.pub.PublishAPIKeyRotated(ctx, in); pubErr != nil {
		e.logger.ErrorContext(ctx, "auth: failed to publish ApiKeyRotated event (key is committed; event lost)",
			slog.String("subject", SubjectAPIKeyRotated),
			slog.String("key_id", apiKey.ID),
			slog.String("user_id", apiKey.UserID),
			slog.String("error", pubErr.Error()),
		)
	}
	// Return the inner result UNCHANGED — including rawKey, so the handler still
	// shows the caller their key exactly once.
	return apiKey, rawKey, nil
}
