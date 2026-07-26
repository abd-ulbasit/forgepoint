// auth_service_impl.go — the concrete AuthService implementation.
//
// This is the platform's most security-critical code: every other service
// trusts the claims this file produces. The comments below name the
// patterns and the attacks each measure defends against.
//
// ============================================================================
// CLEAN ARCHITECTURE PLACEMENT
// ============================================================================
//
// authService lives in the domain package and depends only on:
//   - the repository INTERFACES (not their Postgres implementations), and
//   - crypto/stdlib + bcrypt + the JWT helpers in this same package.
//
// It imports NO gRPC, NO NATS, NO database driver. The handler injects the
// real Postgres-backed repositories at wire time (main.go); tests inject mocks.
// This is dependency inversion: the business logic dictates the repository
// contract; the infrastructure conforms to it.
package domain

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// apiKeyPrefix is the human-readable tag on every Forgepoint API key, à la
// Stripe's "sk_live_" / GitHub's "ghp_". WHY a prefix at all:
//   - Secret scanners (GitHub, gitleaks, TruffleHog) can pattern-match "fp_…"
//     and auto-revoke leaked keys. A prefix-less random blob is invisible to them.
//   - ValidateToken uses it as a cheap discriminator: a credential starting with
//     "fp_" is an API key; anything else is treated as a JWT.
const apiKeyPrefix = "fp_"

// keyPrefixLen is how many leading characters of the raw key we persist as
// KeyPrefix for UI display and (in Postgres) an indexed narrowing column.
// 8 chars of base64url ≈ 48 bits — enough to show a recognizable fragment
// without revealing enough of the secret to matter.
const keyPrefixLen = 8

// apiKeySecretBytes is the entropy of the random portion of an API key.
// 32 bytes = 256 bits. WHY 256 bits: it makes brute force / guessing
// computationally infeasible, which is precisely WHY API keys can use a fast
// hash (SHA-256) instead of a slow one (bcrypt) — see design D3. bcrypt is for
// LOW-entropy human passwords; a 256-bit random key needs no work factor.
const apiKeySecretBytes = 32

// authService is the production implementation of AuthService.
//
// It is unexported: callers receive it only through the AuthService interface
// returned by NewAuthService. This enforces programming-to-the-interface and
// keeps the concrete type's fields private.
type authService struct {
	userRepo   UserRepository
	apiKeyRepo APIKeyRepository
	roleRepo   RoleRepository

	jwtSecret []byte        // HMAC signing key for issued JWTs
	jwtTTL    time.Duration // lifetime of issued JWTs (kept short per design D2)
}

// NewAuthService wires the dependencies and returns the AuthService interface.
//
// WHY return the interface, not *authService:
//
//	Callers depend on the abstraction. They cannot reach into private fields or
//	call unexported methods, and they can be tested against a different
//	implementation. This is the constructor half of dependency inversion.
//
// A compile-time assertion (var _ AuthService = ...) below guarantees
// *authService actually satisfies the interface; if a method signature drifts,
// the package fails to build rather than failing mysteriously at a call site.
func NewAuthService(
	userRepo UserRepository,
	apiKeyRepo APIKeyRepository,
	roleRepo RoleRepository,
	jwtSecret []byte,
	jwtTTL time.Duration,
) AuthService {
	return &authService{
		userRepo:   userRepo,
		apiKeyRepo: apiKeyRepo,
		roleRepo:   roleRepo,
		jwtSecret:  jwtSecret,
		jwtTTL:     jwtTTL,
	}
}

var _ AuthService = (*authService)(nil)

// ============================================================================
// CreateUser
// ============================================================================

// CreateUser validates input, hashes the password with bcrypt, and persists the
// user. The plaintext password exists only as a local parameter and is never
// logged, returned, or stored — only its bcrypt hash reaches the repository.
func (s *authService) CreateUser(ctx context.Context, input CreateUserInput) (User, error) {
	// Normalize the email BEFORE validation/storage. WHY:
	//   Emails are case-insensitive in practice; "Ada@x.dev" and "ada@x.dev" are
	//   the same account. Lowercasing here makes the unique constraint and the
	//   Login lookup agree, preventing duplicate accounts that differ only by case.
	email := strings.ToLower(strings.TrimSpace(input.Email))

	// Validate up front so we never spend a (deliberately expensive) bcrypt hash
	// or a DB round-trip on obviously bad input. %w wraps ErrValidation so the
	// handler can errors.Is(err, ErrValidation) → codes.InvalidArgument while
	// still getting a specific human message.
	if email == "" {
		return User{}, fmt.Errorf("%w: email is required", ErrValidation)
	}
	if input.Password == "" {
		return User{}, fmt.Errorf("%w: password is required", ErrValidation)
	}

	// bcrypt.GenerateFromPassword salts internally (the salt is embedded in the
	// output), so identical passwords yield different hashes — defeating rainbow
	// tables. DefaultCost (currently 10) is the tuned work factor: slow enough to
	// blunt offline brute force, fast enough for interactive login (~tens of ms).
	//
	// SECURITY NOTE: bcrypt silently truncates inputs beyond 72 bytes. We reject
	// over-long passwords rather than let two different long passwords collide on
	// their first 72 bytes (a real, if obscure, auth-bypass class).
	if len(input.Password) > 72 {
		return User{}, fmt.Errorf("%w: password must be at most 72 bytes", ErrValidation)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(input.Password), bcrypt.DefaultCost)
	if err != nil {
		// Do NOT include the password in the error. A generic message keeps the
		// secret out of logs/traces if this ever fires.
		return User{}, fmt.Errorf("hashing password: %w", err)
	}

	user := User{
		Email:        email,
		Name:         strings.TrimSpace(input.Name),
		Team:         input.Team,
		PasswordHash: string(hash),
		Active:       true, // new accounts are active; suspension sets this false
	}

	created, err := s.userRepo.Create(ctx, user)
	if err != nil {
		// Translate the STORAGE error into a BUSINESS error so the handler need
		// not import the repository package. errors.Is unwraps any wrapping the
		// repo applied around the sentinel.
		if errors.Is(err, ErrRepoEmailExists) {
			return User{}, ErrEmailAlreadyExists
		}
		return User{}, fmt.Errorf("creating user: %w", err)
	}
	return created, nil
}

// ============================================================================
// Login
// ============================================================================

// Login authenticates by email + password and returns a signed JWT.
//
// ANTI-ENUMERATION (the headline security property): every failure path returns
// the SAME ErrInvalidCredentials — unknown email, wrong password, inactive user.
// If these differed (distinct errors, or even distinct timing), an attacker
// could probe which emails are registered. One opaque error closes that channel.
func (s *authService) Login(ctx context.Context, email, password string) (string, error) {
	email = strings.ToLower(strings.TrimSpace(email))

	user, err := s.userRepo.GetByEmail(ctx, email)
	if err != nil {
		if errors.Is(err, ErrRepoNotFound) {
			// SECURITY: still run a bcrypt comparison against a dummy hash so the
			// "unknown email" path takes roughly the same time as the "wrong
			// password" path. Without this, an attacker measures response time:
			// fast = no such user, slow = user exists. This is a timing-based
			// enumeration defense. We ignore the result; the outcome is fixed.
			_ = bcrypt.CompareHashAndPassword(dummyBcryptHash, []byte(password))
			return "", ErrInvalidCredentials
		}
		// A genuine infrastructure error (DB down) is distinct from a credential
		// failure — surface it so the handler returns Internal, not Unauthenticated.
		return "", fmt.Errorf("looking up user: %w", err)
	}

	// CompareHashAndPassword is constant-time with respect to the hash content
	// and intentionally slow (it re-runs bcrypt at the stored cost). A mismatch
	// (including a tampered hash) returns a non-nil error.
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		return "", ErrInvalidCredentials
	}

	// A suspended / soft-deleted account must not receive a token even with the
	// correct password. Same opaque error — we don't tell the caller the account
	// merely exists but is disabled.
	if !user.Active {
		return "", ErrInvalidCredentials
	}

	// Mint the JWT. The role NAME (not its permission list) goes in the token:
	// permissions are resolved fresh at CheckPermission time, so a role's
	// permission set can change without invalidating outstanding tokens, and the
	// token can't carry stale grants (design note in TokenClaims).
	claims := TokenClaims{
		UserID:    user.ID,
		Email:     user.Email,
		Name:      user.Name,
		Team:      user.Team,
		Role:      user.Role.Name,
		TokenKind: TokenKindJWT,
	}
	token, err := GenerateToken(claims, s.jwtSecret, s.jwtTTL)
	if err != nil {
		return "", fmt.Errorf("issuing token: %w", err)
	}
	return token, nil
}

// dummyBcryptHash is a precomputed bcrypt hash of an arbitrary value, used only
// to equalize timing on the unknown-email path (see Login). It is NOT a secret
// and never matches any real password input — its sole purpose is to make the
// CPU do the same work whether or not the email exists.
var dummyBcryptHash = []byte("$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy")

// ============================================================================
// CreateAPIKey
// ============================================================================

// CreateAPIKey generates a new API key, stores ONLY its SHA-256 hash and an
// 8-char prefix, and returns the raw key to the caller exactly once.
//
// KEY ANATOMY:   fp_<43 base64url chars of 32 random bytes>
//
//	             └┬┘└──────────────┬──────────────────────┘
//	          prefix tag        256-bit secret (crypto/rand)
//
//	stored:  KeyHash   = hex(SHA-256(rawKey))   ← only key material in the DB
//	         KeyPrefix = rawKey[:8]             ← for display + indexed lookup
//	returned once: rawKey
//
// This is the Stripe/GitHub model (design D3): a DB breach leaks only hashes,
// which are useless without the raw key; one-time display forces the user to
// save it.
func (s *authService) CreateAPIKey(ctx context.Context, userID string, scopes []string, expiresAt *time.Time) (APIKey, string, error) {
	if strings.TrimSpace(userID) == "" {
		return APIKey{}, "", fmt.Errorf("%w: userID is required", ErrValidation)
	}

	// crypto/rand (NOT math/rand) is a cryptographically secure RNG. math/rand is
	// deterministic given its seed — an attacker who learns the seed (or observes
	// enough output) can predict future keys. For anything a security decision
	// rests on, crypto/rand is mandatory. rand.Read fills the buffer or errors;
	// it never returns short without an error.
	secretBytes := make([]byte, apiKeySecretBytes)
	if _, err := rand.Read(secretBytes); err != nil {
		return APIKey{}, "", fmt.Errorf("generating api key entropy: %w", err)
	}

	// base64url (no padding) is URL/header-safe — the key travels in HTTP
	// Authorization headers and CLI configs where '+', '/', '=' are awkward.
	rawKey := apiKeyPrefix + base64URLNoPad(secretBytes)

	// SHA-256 the FULL raw key (prefix included) → the value we store and later
	// look up by. A fast hash is correct here precisely because the input has
	// 256 bits of entropy (see apiKeySecretBytes comment / design D3).
	keyHash := sha256Hex(rawKey)

	apiKey := APIKey{
		UserID:    userID,
		KeyHash:   keyHash,
		KeyPrefix: rawKey[:keyPrefixLen],
		Scopes:    scopes,
		ExpiresAt: expiresAt,
	}

	stored, err := s.apiKeyRepo.Create(ctx, apiKey)
	if err != nil {
		return APIKey{}, "", fmt.Errorf("storing api key: %w", err)
	}
	// Return the stored metadata AND the raw key. After this returns, rawKey
	// goes out of scope and is unrecoverable — exactly the intended lifecycle.
	return stored, rawKey, nil
}

// ============================================================================
// ValidateToken
// ============================================================================

// ValidateToken resolves a raw credential (JWT or API key) to verified claims.
// It is the single entry point every other service's auth interceptor calls,
// regardless of credential type (design D6).
//
// ORDER MATTERS: we try the JWT path first (pure, local crypto — no DB) and
// only fall back to the API-key path (a DB lookup) on JWT failure. This keeps
// the common case (short-lived user JWTs on the hot path) off the database.
//
//	credential ──► looks like "fp_…"? ──► API-key path (SHA-256 + DB lookup)
//	           └─► otherwise          ──► JWT path (HMAC verify, no DB)
func (s *authService) ValidateToken(ctx context.Context, token string) (TokenClaims, error) {
	if token == "" {
		return TokenClaims{}, ErrInvalidToken
	}

	// API keys are unambiguously identifiable by their prefix, so we route them
	// directly to the key path rather than wastefully attempting a JWT parse
	// (which would always fail) first. JWTs never start with "fp_".
	if strings.HasPrefix(token, apiKeyPrefix) {
		return s.validateAPIKey(ctx, token)
	}

	// JWT path: GenerateToken's counterpart. A non-nil error here means the token
	// is not a valid JWT (bad signature, expired, malformed, wrong alg). We
	// collapse all of those into the opaque ErrInvalidToken — the caller (and any
	// attacker) learns only "rejected", never WHY.
	claims, err := ValidateToken(token, s.jwtSecret)
	if err != nil {
		return TokenClaims{}, ErrInvalidToken
	}
	return *claims, nil
}

// validateAPIKey verifies an API key credential against the stored hash.
func (s *authService) validateAPIKey(ctx context.Context, rawKey string) (TokenClaims, error) {
	keyHash := sha256Hex(rawKey)

	stored, err := s.apiKeyRepo.GetByKeyHash(ctx, keyHash)
	if err != nil {
		// ErrNotFound (no such hash) and any other lookup failure both become the
		// opaque ErrInvalidToken: an attacker must not be able to distinguish
		// "this key never existed" from any other rejection.
		return TokenClaims{}, ErrInvalidToken
	}

	// SECURITY — constant-time compare: even though we already matched by hash in
	// the DB lookup, we re-verify the stored hash against the recomputed hash
	// using subtle.ConstantTimeCompare. WHY this matters:
	//   - It makes the verification independent of any string-comparison timing.
	//   - It is the correct habit to demonstrate and reuse: whenever you compare
	//     secret-derived bytes, use a constant-time compare so you never leak how
	//     many leading bytes matched via timing. ConstantTimeCompare returns 1
	//     only if the byte slices are equal AND the same length.
	if subtle.ConstantTimeCompare([]byte(stored.KeyHash), []byte(keyHash)) != 1 {
		return TokenClaims{}, ErrInvalidToken
	}

	// IsValid is the pure domain rule: not revoked AND not past expiry. Evaluated
	// against the current time. Revocation (RevokedAt set) takes effect on the
	// very next call — no cache to wait out, unlike stateless JWTs.
	if !stored.IsValid(time.Now()) {
		return TokenClaims{}, ErrInvalidToken
	}

	return TokenClaims{
		UserID:    stored.UserID,
		Scopes:    stored.Scopes,
		TokenKind: TokenKindAPIKey,
		IssuedAt:  stored.CreatedAt,
		// ExpiresAt left zero when the key has no expiry; the IsValid check above
		// already enforced any expiry that does exist.
	}, nil
}

// ============================================================================
// CheckPermission (RBAC)
// ============================================================================

// CheckPermission returns whether the user may perform action on resource,
// based on the user's ROLE alone (no per-credential scope context — see the
// interface doc and CheckPermissionForClaims).
//
// It returns (bool, error) deliberately:
//   - error  = we could NOT make a decision (DB down) → interceptor fails closed.
//   - bool   = the decision itself (allowed / denied).
//
// Conflating "denied" with "couldn't check" would let a transient DB outage
// silently deny everyone OR (worse, if mishandled) allow everyone. The split
// keeps the security posture explicit.
func (s *authService) CheckPermission(ctx context.Context, userID, resource, action string) (bool, error) {
	// GATE 0 — INPUT SANITY: reject empty resource or action up front.
	//
	// WHY this is a security gate, not just input validation:
	//
	//	permissionMatches uses string equality: perm.Resource == resource.  If
	//	the caller passes resource="" and a stored permission has Resource="",
	//	the comparison returns true — an accidental grant from a corrupted or
	//	zero-value permission row.  Beyond the stored-permission case, accepting
	//	an empty resource/action as a valid authorization request is semantically
	//	meaningless ("may user X do <nothing> on <nothing>?") and represents a
	//	programming error in the caller (e.g. a handler that forgot to extract a
	//	field from the proto request). The safest posture is an immediate deny:
	//	there is no legitimate grant that should be issued for an empty coordinate.
	//
	//	We return (false, nil) — a DENY decision, NOT an error — because we COULD
	//	make the decision (the answer is unambiguously "no"); we just don't need
	//	to consult the database to know it. This preserves the (bool, error)
	//	contract: error means "I couldn't decide", false means "I decided: denied".
	//
	// WHY false AND NOT AN ERROR:
	//
	//	Because returning an error would conflate a client mistake with an
	//	infrastructure failure. The interceptor would surface codes.Internal
	//	instead of codes.PermissionDenied, confusing operators. A clean (false,
	//	nil) signals "authoritatively denied, no investigation needed".
	if resource == "" || action == "" {
		return false, nil
	}

	// GATE 1 — ACCOUNT STATUS (the fix for the suspended-user bypass).
	//
	// Login rejects inactive users, but a JWT minted before suspension stays
	// cryptographically valid until exp, and an API key with no expiry is valid
	// FOREVER. If authorization only consulted roles, a deactivated employee's
	// outstanding credential would keep full access until it independently
	// expired (which, for API keys, may be never). So every authorization
	// decision must re-confirm the account is live RIGHT NOW.
	//
	// FAIL-CLOSED on a genuine lookup error (DB down) — return the error so the
	// interceptor denies and can surface Internal vs PermissionDenied. A missing
	// user (ErrRepoNotFound) is a clean DENY, not an error: there is simply no
	// active subject to grant anything to.
	user, err := s.userRepo.GetByID(ctx, userID)
	if err != nil {
		if errors.Is(err, ErrRepoNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("loading user: %w", err)
	}
	if !user.Active {
		// Suspended / soft-deleted (models.go: Active==false). Deny cleanly —
		// this is a decision, not an infrastructure failure.
		return false, nil
	}

	// GATE 2 — ROLE PERMISSIONS. Delegated to a shared helper so the
	// claims-aware path (CheckPermissionForClaims) reuses the exact same role
	// evaluation without duplicating the scan or the error contract.
	return s.roleAllows(ctx, userID, resource, action)
}

// roleAllows performs ONLY the role-permission half of an authorization
// decision: load the user's roles and scan their permissions with wildcard
// semantics. It assumes the account-status gate has already run (or is run by
// the caller). Extracted so CheckPermission and CheckPermissionForClaims share
// one definition of "the role permits this".
//
// Same (bool, error) contract: error = couldn't decide (fail closed); bool =
// the decision.
func (s *authService) roleAllows(ctx context.Context, userID, resource, action string) (bool, error) {
	roles, err := s.roleRepo.GetUserRoles(ctx, userID)
	if err != nil {
		// Propagate as an error (NOT a false deny) so the caller can distinguish
		// infrastructure failure and apply fail-closed at the boundary.
		return false, fmt.Errorf("loading user roles: %w", err)
	}

	// Scan every permission of every role. The first match wins (grants are
	// additive — having ANY matching permission allows). This is the core of
	// flat RBAC: roles → permissions, evaluated as a membership test.
	for _, role := range roles {
		for _, perm := range role.Permissions {
			if permissionMatches(perm, resource, action) {
				return true, nil
			}
		}
	}
	return false, nil
}

// CheckPermissionForClaims is the credential-aware authorization decision.
//
// EFFECTIVE GRANT = role permissions  ∩  (API-key scopes, if any)
//
//	┌────────────┐ ValidateToken ┌──────────────┐ CheckPermissionForClaims
//	│ credential │ ────────────► │ TokenClaims  │ ───────────────────────►
//	└────────────┘               │ Kind, Scopes │   role gate AND scope gate
//	                             └──────────────┘
//
// WHY this exists separately from CheckPermission: the CheckPermission RPC's
// request carries only (user_id, resource, action) — it has no idea WHICH
// credential is asking, so it cannot enforce per-key scopes. The interceptors,
// however, have just produced TokenClaims from ValidateToken, so they can (and
// must) call THIS method to honor the scope a key was minted with.
//
// SECURITY — intersection, never union:
//   - JWT (TokenKindJWT): a human session carrying the user's full role
//     authority. No scope narrowing applies; the role gate alone decides.
//   - API key (TokenKindAPIKey): in addition to the role gate, the requested
//     (resource, action) must be covered by one of the key's scopes. A
//     ["models:read"] key is therefore DENIED models:write even if the owner's
//     role allows it — least privilege per credential.
//
// A non-error caller MUST pass claims it obtained from ValidateToken; passing
// attacker-controlled scopes would defeat the gate, but scopes only ever come
// from the verified DB row (API keys) or the signed token (JWTs).
func (s *authService) CheckPermissionForClaims(ctx context.Context, claims TokenClaims, resource, action string) (bool, error) {
	// GATE 1 + GATE 2 (account status + role) are exactly CheckPermission. Reuse
	// it so there is a SINGLE place that decides "active user + role permits".
	allowed, err := s.CheckPermission(ctx, claims.UserID, resource, action)
	if err != nil || !allowed {
		// Either we couldn't decide (propagate the error, fail closed) or the
		// role itself denies — no scope check can widen a role denial.
		return allowed, err
	}

	// GATE 3 — SCOPE (API keys only). The role already allows; now require the
	// key's scopes to ALSO cover the request. JWTs skip this: they are not scope
	// narrowed.
	if claims.TokenKind != TokenKindAPIKey {
		return true, nil
	}
	for _, scope := range claims.Scopes {
		if scopeMatches(scope, resource, action) {
			return true, nil
		}
	}
	// Role allowed but no scope covers it → the narrowing bites. DENY.
	return false, nil
}

// scopeMatches reports whether an API-key scope string covers (resource, action).
//
// Scope grammar (string form of a Permission): "resource:action", where either
// side may be the "*" wildcard — e.g. "models:read", "models:*", "*:read",
// "*:*". We parse the string into a Permission and reuse permissionMatches so
// the wildcard semantics are IDENTICAL to role evaluation (one source of truth
// for "does this grant cover that request"). A malformed scope (missing exactly
// one ':' separator) matches nothing — fail closed on garbage input rather than
// guessing intent.
func scopeMatches(scope, resource, action string) bool {
	parts := strings.SplitN(scope, ":", 2)
	if len(parts) != 2 {
		return false
	}
	return permissionMatches(Permission{Resource: parts[0], Action: parts[1]}, resource, action)
}

// permissionMatches implements the wildcard semantics from design D5:
//
//	a permission grants (resource, action) iff
//	  (perm.Resource == "*" OR perm.Resource == resource) AND
//	  (perm.Action   == "*" OR perm.Action   == action)
//
// So:  {models, write} matches only models/write.
//
//	{models, *}     matches any action on models.
//	{*, read}       matches read on any resource (but NOT write).
//	{*, *}          matches everything — the admin grant.
//
// WHY wildcards live here and not in the DB query: matching is pure business
// logic. Keeping it in the domain means the rule is unit-testable without a
// database and identical regardless of which repository backs GetUserRoles.
//
// SECURITY INVARIANT — empty stored fields are NEVER treated as wildcards:
//
//	A wildcard is an explicit capability grant written as the literal string "*".
//	An empty string ("") in a stored permission is a misconfiguration or a
//	zero-value row that was never properly populated. Treating "" as a wildcard
//	would mean a corrupted/absent DB value silently grants access — a violation
//	of the principle of least privilege and a potential authorization bypass if
//	an attacker can inject zero-value rows.
//
//	The guard `perm.Resource != ""` (and the same for perm.Action) means:
//	  - Permission{Resource:"*", Action:"*"} still matches everything (intended).
//	  - Permission{Resource:"", Action:""}   matches nothing (corrupted data).
//	  - Permission{Resource:"", Action:"*"}  matches nothing (partial corruption).
//
//	Note: CheckPermission already rejects empty REQUESTED resource/action at
//	GATE 0, so by the time we reach here both resource and action are non-empty.
//	The guard here defends against empty STORED fields.
//
// EMPTY STRING vs WILDCARD:
//
//	A wildcard is intent; an empty string is the absence of intent. We never
//	grant permissions by accident. Any stored field that isn't a legitimate
//	resource name or the literal "*" is treated as non-matching (deny-by-default).
func permissionMatches(perm Permission, resource, action string) bool {
	// Reject zero-value or corrupted stored permission fields immediately.
	// An empty stored field is NOT a wildcard; only the explicit "*" qualifies.
	if perm.Resource == "" || perm.Action == "" {
		return false
	}
	resourceOK := perm.Resource == wildcard || perm.Resource == resource
	actionOK := perm.Action == wildcard || perm.Action == action
	return resourceOK && actionOK
}

// wildcard is the "any" token in a Permission's Resource or Action.
const wildcard = "*"

// ============================================================================
// AssignRole
// ============================================================================

// AssignRole sets the user's role by NAME. It resolves the name to a role ID and
// upserts the assignment.
//
// NOTE (documented tradeoff): the change takes effect on the user's NEXT token
// issuance. Existing JWTs embed the old role name and remain valid until they
// expire — the stateless-JWT staleness tradeoff (design D2). For immediate
// effect, also revoke the user's outstanding tokens.
func (s *authService) AssignRole(ctx context.Context, userID, roleName string) error {
	// Confirm the user exists first so we return a precise ErrUserNotFound rather
	// than letting a dangling assignment row be created for a ghost user.
	if _, err := s.userRepo.GetByID(ctx, userID); err != nil {
		if errors.Is(err, ErrRepoNotFound) {
			return ErrUserNotFound
		}
		return fmt.Errorf("looking up user: %w", err)
	}

	// Resolve the role NAME → its ID. The join table stores IDs, not names, so a
	// rename of a role doesn't orphan assignments.
	role, err := s.roleRepo.GetByName(ctx, roleName)
	if err != nil {
		if errors.Is(err, ErrRepoNotFound) {
			return ErrRoleNotFound
		}
		return fmt.Errorf("looking up role: %w", err)
	}

	// AssignToUser is an UPSERT (see RoleRepository doc): idempotent and safe
	// under concurrent calls — the end state is always "user has exactly this role".
	if err := s.roleRepo.AssignToUser(ctx, userID, role.ID); err != nil {
		return fmt.Errorf("assigning role: %w", err)
	}
	return nil
}

// ============================================================================
// crypto helpers (small, pure, shared by the methods above)
// ============================================================================

// sha256Hex returns the lowercase hex encoding of SHA-256(s). Hex (not base64)
// because the DB column is a fixed 64-char string that's easy to index and eyeball.
func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// base64URLEncoding is the URL-safe base64 alphabet with padding stripped.
// Declared once as a package var so we don't re-derive RawURLEncoding per call.
var base64URLEncoding = base64.RawURLEncoding

// base64URLNoPad encodes bytes with the URL-safe alphabet and no '=' padding —
// safe to drop into HTTP headers, URLs, and CLI config without escaping.
func base64URLNoPad(b []byte) string {
	return base64URLEncoding.EncodeToString(b)
}
