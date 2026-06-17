// auth_service.go defines the AuthService interface — the primary port through
// which the handler layer reaches the business logic.
//
// ============================================================================
// CLEAN ARCHITECTURE: THE SERVICE INTERFACE AS A "PORT"
// ============================================================================
//
// In Clean Architecture (and its Port/Adapter variant, Hexagonal Architecture),
// a "port" is an interface defined IN the domain that the outer layers implement.
// The domain layer owns this interface; the handler layer depends on it; the
// concrete implementation lives one layer out.
//
// WHY define the interface here instead of in the handler package:
//   - Dependency inversion: the handler depends on an abstraction, not a
//     concrete type. This lets us swap implementations (real Postgres-backed
//     AuthServiceImpl vs. an in-memory stub for tests) without touching the
//     handler code.
//   - Test isolation: handler unit tests construct a mock AuthService (just a
//     struct that implements this interface) — no real DB, no real JWT library.
//
// WHAT'S NOT HERE (and why):
//   The concrete implementation (AuthServiceImpl) and its constructor
//   NewAuthService(userRepo, apiKeyRepo, roleRepo, jwtSecret) arrive in Task 1.3
//   once the repository interfaces (Task 1.4) are in place for it to depend on.
//   Keeping the interface here and the impl separate means Task 1.4 (handler
//   scaffold) can compile and run today while the business logic is still coming.
//
// ============================================================================
package domain

import (
	"context"
	"time"
)

// CreateUserInput carries the validated input fields for creating a new User.
//
// WHY a dedicated input struct instead of passing individual parameters:
//   - If we need to add a field (e.g., ExternalIDPReference for SSO), we add it
//     here without changing the function signature (backward-compatible).
//   - Callers construct the struct explicitly with field names — no risk of
//     accidentally swapping Email and Name positional arguments.
//   - The handler converts proto CreateUserRequest → CreateUserInput before
//     calling the service; the service never sees proto types.
type CreateUserInput struct {
	Email    string // required; must be unique in the system
	Name     string // display name
	Password string // plaintext — the service hashes it with bcrypt before storage
	Team     string // team membership; required for multi-tenancy scoping
}

// AuthService is the primary domain interface for authentication and identity
// management. The handler layer (AuthHandler) holds a value of this interface
// and calls its methods; the repository interfaces are injected into the
// concrete impl (NewAuthService in Task 1.3).
//
// INTERVIEW: "How do you handle the case where you need to mock the database
// in tests for the gRPC handler?"
//   The handler depends on AuthService (an interface), not on AuthServiceImpl
//   (the concrete struct). In handler tests we pass a mock that implements
//   AuthService — zero database, zero network. This is dependency inversion
//   at work: the handler never knows (or cares) what's behind the interface.
type AuthService interface {
	// -----------------------------------------------------------------------
	// IDENTITY MANAGEMENT
	// -----------------------------------------------------------------------

	// CreateUser provisions a new user account.
	//
	// WHAT THE IMPL DOES (Task 1.3):
	//   - Validates email format and uniqueness (UserRepository.GetByEmail).
	//   - Hashes the password with bcrypt (cost 12 — enough to be slow for
	//     brute-force but fast enough for login: ~250ms on modest hardware).
	//   - Writes the user row via UserRepository.Create.
	//   - Assigns the default role ("viewer") unless explicitly set.
	//
	// RETURNS the created User (with ID, CreatedAt populated by the DB).
	// Errors: ErrEmailAlreadyExists if the email is taken, validation errors,
	// or a wrapped repository error for DB failures.
	CreateUser(ctx context.Context, input CreateUserInput) (User, error)

	// -----------------------------------------------------------------------
	// CREDENTIALS
	// -----------------------------------------------------------------------

	// Login authenticates a user by email + password and returns a signed JWT.
	//
	// WHAT THE IMPL DOES:
	//   - Fetches the user by email (UserRepository.GetByEmail).
	//   - Verifies the password against the stored bcrypt hash
	//     (bcrypt.CompareHashAndPassword — intentionally slow).
	//   - Issues a JWT signed with the service's JWTSecret, embedding UserID,
	//     Email, Team, Role, issued-at, and expiry (e.g., 24h).
	//
	// RETURNS the raw JWT string. The caller (the Login gRPC handler) puts it
	// in LoginResponse.token. The JWT is NOT stored server-side — revocation
	// of JWTs is handled by a Redis jti-blacklist in ValidateToken.
	//
	// WHY return the raw string and not a struct:
	//   The JWT is opaque to the caller — they forward it to clients. No need
	//   to parse it back out here. Keeping it as a string avoids coupling the
	//   handler to the JWT library's token struct type.
	Login(ctx context.Context, email, password string) (jwtToken string, err error)

	// CreateAPIKey generates a new API key for the given user.
	//
	// WHAT THE IMPL DOES:
	//   - Generates 32 cryptographically random bytes, base64url-encodes them,
	//     prepends "fp_" → rawKey.
	//   - SHA-256(rawKey) → keyHash (stored in DB via APIKeyRepository.Create).
	//   - Stores rawKey[:8] as keyPrefix for display (never the full key).
	//   - Stores scopes and optional expiresAt.
	//
	// RETURNS both the domain APIKey (stored metadata) AND the rawKey (shown
	// once to the caller; never retrievable again — Stripe's model).
	//
	// SCOPES: Callers specify which capabilities the key should have.
	// The interceptors enforce scopes when the key is used via ValidateToken.
	CreateAPIKey(ctx context.Context, userID string, scopes []string, expiresAt *time.Time) (apiKey APIKey, rawKey string, err error)

	// -----------------------------------------------------------------------
	// TOKEN VALIDATION (called by every other service's auth interceptor)
	// -----------------------------------------------------------------------

	// ValidateToken validates a raw token string (JWT or API key) and returns
	// the decoded TokenClaims on success.
	//
	// WHAT THE IMPL DOES:
	//   For JWTs:
	//     1. Verify HMAC-SHA256 signature with JWTSecret.
	//     2. Check exp claim (not expired).
	//     3. Check jti against Redis blacklist (not revoked).
	//     4. Return TokenClaims{TokenKind: TokenKindJWT, ...}.
	//
	//   For API keys (prefix "fp_"):
	//     1. SHA-256 the incoming key → look up by hash in APIKeyRepository.
	//     2. Call APIKey.IsValid(time.Now()) (pure domain check).
	//     3. Return TokenClaims{TokenKind: TokenKindAPIKey, Scopes: key.Scopes, ...}.
	//
	// CACHING (Task 1.3 impl detail):
	//   Validation results are cached in Redis with a 30-second TTL.
	//   This keeps per-RPC auth overhead below 1ms p99. The TTL is short enough
	//   that a revoked token is rejected within ~30s even under cache.
	//
	// INTERVIEW: "What happens if the auth service is down?"
	//   Other services' interceptors use fail-closed policy (deny on error) and
	//   a circuit breaker on the auth gRPC client to stop cascading failures.
	//   We chose fail-closed over fail-open because auth is a security boundary.
	ValidateToken(ctx context.Context, token string) (TokenClaims, error)

	// -----------------------------------------------------------------------
	// ACCESS CONTROL (RBAC)
	// -----------------------------------------------------------------------

	// CheckPermission evaluates whether a user is allowed to perform action on
	// resource. Called by auth interceptors after ValidateToken to enforce RBAC.
	//
	// WHAT THE IMPL DOES:
	//   1. Load the user's Role via RoleRepository.GetUserRoles(userID).
	//   2. Scan Role.Permissions for a Permission{Resource: resource, Action: action}.
	//   3. Return (true, nil) if found; (false, nil) if not.
	//
	// WHY return (bool, error) instead of just error:
	//   - error = infrastructure failure (DB down, timeout) — the interceptor
	//     can distinguish "permission denied" from "couldn't check" and apply
	//     appropriate behavior (fail-closed vs. return PermissionDenied status).
	//   - bool = the authorization decision itself.
	//
	// CACHING: Permission results can be cached by (userID, resource, action)
	// with a short TTL. Role changes take effect within the cache window (acceptable
	// for an ML platform where role changes are admin-level, infrequent events).
	CheckPermission(ctx context.Context, userID, resource, action string) (allowed bool, err error)

	// AssignRole changes a user's role. Requires the caller to have admin permission
	// (enforced in the handler via CheckPermission before calling this).
	//
	// NOTE: The role change takes effect on the NEXT token issuance. Existing
	// JWTs embed the old role and remain valid until they expire or are revoked.
	// For immediate enforcement, also revoke the user's active tokens (add their
	// jti values to the Redis blacklist). This is a documented tradeoff of
	// stateless JWTs: they carry state that can become stale.
	AssignRole(ctx context.Context, userID, roleName string) error
}
