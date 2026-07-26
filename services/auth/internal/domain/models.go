// Package domain is the innermost ring of the Clean Architecture onion for the
// Auth service. It holds the canonical business types: User, APIKey, Role,
// Permission, and TokenClaims.
//
// ============================================================================
// CLEAN ARCHITECTURE — DOMAIN LAYER
// ============================================================================
//
// WHY the domain layer exists as a separate package:
//
//	Clean Architecture (Robert Martin, 2017) states that business rules must not
//	depend on delivery mechanisms or infrastructure. The domain layer enforces
//	this by having ZERO imports of gRPC, NATS, database drivers, or generated
//	proto code. It depends only on the Go standard library.
//
//	Dependency rule (arrows point inward — nothing inside points out):
//
//	  ┌─────────────────────────────────────────────────────────────┐
//	  │  cmd/server/main.go           (wires everything together)  │
//	  │    ↓ imports                                               │
//	  │  internal/handler             (proto ↔ domain conversion)  │
//	  │    ↓ imports                                               │
//	  │  internal/domain  ◄──── internal/repository/interfaces     │
//	  │  (THIS PACKAGE)              (defines DB contract in terms │
//	  │  stdlib only                  of domain types)             │
//	  └─────────────────────────────────────────────────────────────┘
//
//	Handlers convert proto ↔ domain types. Repositories convert domain types
//	↔ database rows. The domain itself never knows either exists.
//
// WHY pure Go types instead of using proto-generated types as business objects:
//
//	Generated proto types (.pb.go) carry wire-format metadata (protobuf field
//	numbers, json tags shaped by proto3 naming). Using them as business objects:
//	- Couples business logic to the protobuf schema: renaming a proto field
//	  breaks business logic, not just the wire format.
//	- Makes unit tests import grpc-go (heavyweight dep just to test a hash check).
//	- Prevents running domain logic in contexts where proto isn't available.
//
//	The handler layer is the ONLY place where proto ↔ domain conversion happens
//	(the "anti-corruption layer" in Domain-Driven Design terminology).
//
// DESIGN NOTE:
//
//	Q: Why not just use the proto types directly in the service layer?
//	A: The domain must be independently testable and decoupled from transport.
//	   If I rename a proto field, only the handler changes — not the business
//	   logic or repository. Clean Architecture makes the blast radius of
//	   infrastructure changes predictable and minimal.
//
// ============================================================================
package domain

import "time"

// ============================================================================
// ROLE AND PERMISSION
// ============================================================================

// Role represents a named set of permissions assignable to a User.
// Forgepoint uses a flat RBAC model: each user has exactly one role, and each
// role grants an explicit set of permissions.
//
// WHY flat RBAC over hierarchical / attribute-based (ABAC):
//   - Flat RBAC is easy to reason about, audit, and render in a UI
//     ("you have the 'engineer' role"). Hierarchy adds complexity without
//     proportional benefit for a 10-service ML platform at this stage.
//   - ABAC (AWS IAM style: resource conditions, tag-based) is more expressive
//     but requires a policy engine (OPA, Casbin). Overkill here.
//   - ALTERNATIVE AT SCALE: Google Zanzibar / ReBAC (relation-based access
//     control) is what Google, Airbnb, and Carta use at scale — the natural
//     evolution path when the permission model outgrows flat RBAC.
type Role struct {
	ID          string
	Name        string       // e.g., "admin", "engineer", "viewer"
	Permissions []Permission // explicit capabilities this role grants
	CreatedAt   time.Time
}

// Permission is a single capability: the right to perform Action on Resource.
//
// Examples:
//
//	Permission{Resource: "models", Action: "write"}   → register/update models
//	Permission{Resource: "experiments", Action: "read"} → view experiments
//	Permission{Resource: "billing", Action: "admin"}  → manage billing
//
// WHY structured fields over a flat "models:write" string:
//
//	Strings are stringly-typed — a typo "modles:write" silently grant-or-denies.
//	Structured fields make it possible to query "who can write to models?" and
//	match exactly how CheckPermission receives resource+action as separate args.
type Permission struct {
	Resource string // e.g., "models", "experiments", "pipelines", "billing"
	Action   string // e.g., "read", "write", "delete", "admin"
}

// ============================================================================
// USER
// ============================================================================

// User is the canonical domain representation of an authenticated identity.
// This struct flows through service logic and repository interfaces — NOT the
// proto-generated User message from forgepoint/auth/v1.
//
// SENSITIVE FIELDS: PasswordHash is write-only from the business logic
// perspective. Handlers MUST NOT map it to any proto response. The invariant
// is enforced by never including PasswordHash in the proto-to-domain conversion
// in auth_handler.go.
//
// WHY Team (not Organization):
//
//	Forgepoint serves ML engineering teams inside a company. Team-level
//	multi-tenancy (isolate experiments, models, and billing per team) is the
//	natural boundary for this portfolio stage. A full org hierarchy with nested
//	sub-teams is scope for a later multi-tenant expansion.
type User struct {
	ID           string
	Email        string // unique; used as the login identifier
	Name         string // display name (not required to be unique)
	Team         string // team membership for multi-tenancy scoping
	Role         Role   // single role (flat RBAC; see Role comment above)
	PasswordHash string // bcrypt hash — handlers NEVER include this in responses
	Active       bool   // false = soft-deleted / suspended user
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// ============================================================================
// API KEY
// ============================================================================

// APIKey is a long-lived credential for machine-to-machine auth: CI/CD
// pipelines, training scripts, and SDK clients.
//
// SECURITY DESIGN — store the hash, never the raw key:
//
//	┌──────────────────────────────────────────────────────────────────┐
//	│ Key lifecycle                                                    │
//	│                                                                  │
//	│ Create:   raw_key = "fp_" + 32 random bytes (base64url)         │
//	│           key_hash = SHA-256(raw_key)  → stored in DB           │
//	│           key_prefix = raw_key[:8]     → stored for UI display  │
//	│           raw_key returned ONCE in CreateAPIKeyResponse          │
//	│                                                                  │
//	│ Validate: incoming key → SHA-256 → lookup by hash in DB         │
//	│           raw key never touches the DB or logs                  │
//	│                                                                  │
//	│ Revoke:   set RevokedAt = now → next hash lookup sees non-nil   │
//	│           RevokedAt → rejects immediately                        │
//	└──────────────────────────────────────────────────────────────────┘
//
//	This is how Stripe, GitHub, and HashiCorp Vault handle API keys.
//	A database breach exposes only hashes — useless without the raw key.
//	One-time display of raw_key forces users to save it (like GitHub PATs).
//
// ROTATING AN API KEY WITHOUT DOWNTIME:
//
//	Create the new key first (system accepts both old and new), update clients
//	to use the new key, then revoke the old key. Blue/green applied to creds.
type APIKey struct {
	ID        string
	UserID    string     // owner
	KeyHash   string     // SHA-256(raw_key) — the only key material in the DB
	KeyPrefix string     // first 8 chars of raw key for UI display ("fp_a1b2")
	Scopes    []string   // e.g., ["models:read", "experiments:write"]
	ExpiresAt *time.Time // nil = no expiry (long-lived service keys)
	RevokedAt *time.Time // nil = active; non-nil = immediately invalid
	CreatedAt time.Time
}

// IsValid reports whether the key is currently usable: not revoked and not
// expired relative to now. This is a pure business rule — no DB, no network —
// so it belongs in the domain.
func (k APIKey) IsValid(now time.Time) bool {
	if k.RevokedAt != nil {
		return false
	}
	if k.ExpiresAt != nil && now.After(*k.ExpiresAt) {
		return false
	}
	return true
}

// ============================================================================
// TOKEN CLAIMS
// ============================================================================

// TokenClaims is the decoded, validated payload of a JWT or API-key token.
// This is what auth interceptors in every other service inject into the gRPC
// context after calling AuthService.ValidateToken().
//
// WHY not use the JWT library's Claims struct directly here:
//   - jwt.RegisteredClaims couples the domain to a specific JWT library version.
//   - TokenClaims is a plain struct readable by any package that imports domain —
//     no JWT library needed to consume what's in the context.
//   - The auth service implementation converts jwt.MapClaims → TokenClaims once
//     at the boundary; all other services only ever see TokenClaims.
//
// WHAT OTHER SERVICES DO WITH THIS:
//
//	The auth interceptor stores TokenClaims in the context via ClaimsContextKey.
//	Handlers call ctx.Value(domain.ClaimsContextKey).(domain.TokenClaims) to
//	read the caller's identity without making an extra RPC.
//
// WHAT IS IN THE JWT:
//
//	Standard claims (sub, exp, iat) plus Forgepoint-specific: user_id, email,
//	team, role. We keep the JWT payload small — the permissions list is NOT
//	embedded (it's dynamic from the role, checked at CheckPermission time).
//	This avoids stale permission data in long-lived tokens.
type TokenClaims struct {
	UserID    string // maps to JWT "sub" (subject)
	Email     string
	Name      string
	Team      string
	Role      string   // role name, e.g., "admin", "engineer", "viewer"
	Scopes    []string // populated for API keys; empty for user JWTs
	IssuedAt  time.Time
	ExpiresAt time.Time

	// TokenKind distinguishes human-user JWTs from API key tokens.
	// Validation paths diverge: JWTs require a jti-blacklist check in Redis;
	// API keys require a DB lookup by hash and a non-nil RevokedAt check.
	TokenKind TokenKind
}

// TokenKind distinguishes the type of credential that produced these claims.
type TokenKind string

const (
	// TokenKindJWT is a short-lived JWT produced by the Login RPC.
	// Revocation requires adding the token's jti to a Redis blacklist.
	TokenKindJWT TokenKind = "jwt"

	// TokenKindAPIKey is a long-lived opaque key produced by CreateAPIKey.
	// Revocation is immediate: the DB row's RevokedAt is set and the hash
	// lookup on the next request will find it revoked.
	TokenKindAPIKey TokenKind = "api_key"
)

// claimsKey is an unexported context key type for storing TokenClaims.
//
// WHY an unexported struct type (not a string or int):
//
//	Context key collisions happen when two packages both use ctx.WithValue with
//	the same key value. Using an unexported struct type as the key type means
//	no other package can construct this key — it's uniquely owned by this package.
//	This is the idiomatic Go pattern (see net/http's requestKey, context.cancelCtx).
type claimsKey struct{}

// ClaimsContextKey is the key used to store and retrieve TokenClaims from a
// context.Context within THIS service.
//
// SCOPE — who actually uses this key (corrected):
//
//	pkg/grpcutil does NOT use this key. The shared interceptor library defines
//	its OWN independent, unexported context key (claimsContextKey) and its own
//	Claims type, and it (correctly) does NOT import this service's domain
//	package — a shared library must not depend on one service's internals.
//
//	ClaimsContextKey is for the AUTH SERVICE'S OWN code: its interceptors that
//	inject domain.TokenClaims after ValidateToken, and any handler in this
//	service (or other code that already imports this domain) that needs to read
//	the caller's claims back out. It is exported only so those collaborators in
//	the same trust boundary can share one agreed-upon key value.
//
// Usage where claims are injected (this service's interceptor):
//
//	ctx = context.WithValue(ctx, domain.ClaimsContextKey, claims)
//
// Usage in a handler:
//
//	claims, ok := ctx.Value(domain.ClaimsContextKey).(domain.TokenClaims)
//	if !ok { return nil, status.Error(codes.Unauthenticated, "missing claims") }
var ClaimsContextKey = claimsKey{}
