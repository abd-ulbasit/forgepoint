// Package auth provides a shared JWT validator that every Forgepoint service
// wires into its gRPC interceptor chain.
//
// ============================================================================
// DESIGN D2 — LOCAL JWT VERIFICATION (WHY NO RPC ON THE HOT PATH)
// ============================================================================
//
// Every authenticated gRPC call on every one of the 9 non-auth services must
// verify a token. There are two architecturally valid strategies:
//
//   D1 — REMOTE: call auth.ValidateToken over gRPC for every request.
//        PRO: handles API-key revocation and JWT revocation instantly.
//        CON: every RPC now has a synchronous network hop to auth service.
//             auth service becomes a reliability single-point-of-failure.
//             A 1ms auth call on a 5ms handler is 20% overhead.
//
//   D2 — LOCAL: verify the JWT signature in-process with the shared secret.
//        PRO: zero network, zero latency overhead — a pure SHA-256 HMAC op.
//             auth service unavailability does NOT block in-flight RPCs.
//        CON: a revoked JWT is still accepted until it expires (stateless JWTs).
//             API keys (prefix "fp_") have their validity in auth's database and
//             cannot be resolved locally — those still require the D1 path.
//
// DECISION: D2 for JWTs, with short TTLs (15m default) to bound the revocation
// window. Every non-auth service uses THIS validator for the JWT hot path. API keys
// will be handled by a separate RPCValidator in a future pkg/auth/rpc package.
//
// REAL-WORLD COMPARISON:
//   - Stripe: D2 for their internal services; short-lived JWTs.
//   - GitHub: D1 for PATs (database lookup); D2 for OAuth short-lived JWTs.
//   - Kubernetes: D2 via ServiceAccount JWTs (in-cluster validation).
//
// ============================================================================
// SECURITY MODEL — ALGORITHM CONFUSION DEFENSES (MIRRORING domain/jwt.go)
// ============================================================================
//
// This validator implements EXACTLY the same two-gate defense as the auth
// service's domain.ValidateToken to ensure every service in the platform
// rejects the same malformed tokens in the same way.
//
// GATE 1 — keyfunc type assertion:
//   The keyfunc receives the *jwt.Token after header decode but before signature
//   verification. It asserts token.Method is *jwt.SigningMethodHMAC AND is the
//   exact signingMethod instance (HS256). This rejects:
//     • alg=none  (whose method is *jwt.signingMethodNone, a different type)
//     • HS384, HS512 (different *jwt.SigningMethodHMAC instances)
//     • RS256, ES256 (different types entirely)
//
// GATE 2 — WithValidMethods(["HS256"]):
//   The parser rejects any token whose alg header is not in this list before the
//   keyfunc even runs. Belt-and-suspenders: two independent, ordered checks.
//
// GATE 3 — WithExpirationRequired():
//   A token with no exp claim is rejected outright. Without this, a buggy or
//   malicious minter could issue a never-expiring token.
//
// GATE 4 — >=32-byte secret enforcement:
//   Mirrors domain.validateHMACSecret — weak secrets are rejected at
//   construction time, not silently accepted.
//
// ALGORITHM CONFUSION:
//   The attacker changes the alg header in a JWT to trick the verifier into
//   using a different algorithm than the signer intended. Prevention: pin the
//   algorithm on the VERIFIER side (never trust the token's alg header); assert
//   the concrete signing method type inside the keyfunc AND use WithValidMethods
//   as a second, earlier gate. Reject alg=none unconditionally.
package auth

import (
	"context"
	"fmt"

	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// signingMethod is the ONE algorithm this validator accepts — identical to the
// constant in services/auth/internal/domain/jwt.go. Pinning the same instance
// means the keyfunc comparison (token.Method == signingMethod) is an identity
// check that rejects all other HMAC variants, not just a name check.
var signingMethod = jwt.SigningMethodHS256

// minHMACSecretBytes mirrors domain/jwt.go's constant: RFC 7518 §3.2 requires
// the HMAC key to be at least as long as the hash output (32 bytes for SHA-256).
//
// WHY enforce this in the shared validator too (not just in auth's domain):
// If a service is misconfigured with a short JWT_SECRET it would accept tokens
// that are trivially forgeable by an offline brute-force attack — the same
// attack path domain/jwt.go guards against. The rule must be symmetric across
// every verifier, not just the minter.
const minHMACSecretBytes = 32

// jwtClaims is the typed claims struct that mirrors the EXACT wire format auth
// issues (see services/auth/internal/domain/jwt.go:jwtClaims). Using a typed
// struct instead of jwt.MapClaims means:
//   - Every field access is type-safe (no runtime type assertions that silently
//     yield zero values on a typo).
//   - JSON unmarshalling is handled by the library's standard decoder — no
//     manual map key traversal.
//
// The json tags MUST match what auth's domain writes. If auth ever renames a
// claim field, this struct must be updated in the same change — and because this
// package is shared, the build will fail at every consumer simultaneously, making
// the drift impossible to miss.
type jwtClaims struct {
	jwt.RegisteredClaims        // sub, exp, iat, nbf, iss, aud, jti
	Email             string   `json:"email,omitempty"`
	Name              string   `json:"name,omitempty"`
	Team              string   `json:"team,omitempty"`
	Role              string   `json:"role,omitempty"`
	Scopes            []string `json:"scopes,omitempty"`
}

// jwtValidator is the concrete implementation of grpcutil.TokenValidator that
// performs local HS256 JWT verification.
//
// It is unexported because consumers construct it through NewJWTValidator and
// interact with it only through the grpcutil.TokenValidator interface. This lets
// us evolve the struct fields without breaking callers.
type jwtValidator struct {
	secret []byte
}

// Compile-time proof that *jwtValidator satisfies grpcutil.TokenValidator.
// If the interface signature ever changes, this line fails the build here (at
// the implementation) rather than silently at the wiring site in main.go.
var _ grpcutil.TokenValidator = (*jwtValidator)(nil)

// NewJWTValidator constructs a local JWT validator that verifies HS256 tokens
// signed with secret.
//
// The secret MUST be at least 32 bytes (256 bits). Shorter secrets violate
// RFC 7518 §3.2 and shrink the HMAC key space to a brute-forceable size.
// NewJWTValidator rejects short secrets immediately rather than waiting for
// the first Validate call — fail fast at startup, not under load.
//
// USAGE in a service's main.go:
//
//	validator := auth.NewJWTValidator([]byte(cfg.JWTSecret))
//	srv := grpcutil.NewServer(
//	    grpcutil.WithAuthValidator(validator,
//	        append(servicePublicMethods, grpcutil.HealthAndReflectionMethods()...)...,
//	    ),
//	    grpcutil.WithReflection(),
//	)
func NewJWTValidator(secret []byte) (grpcutil.TokenValidator, error) {
	if len(secret) < minHMACSecretBytes {
		return nil, fmt.Errorf(
			"auth: JWT secret too short: got %d bytes, minimum is %d (RFC 7518 §3.2 requires key >= hash output size)",
			len(secret), minHMACSecretBytes,
		)
	}
	// Copy the secret slice so the caller can't mutate it after construction.
	// A shared slice would allow a caller to change the verifier's key after
	// startup — a subtle bug that's hard to trace. Defensive copy is cheap
	// (32–64 bytes) and closes the mutability window entirely.
	s := make([]byte, len(secret))
	copy(s, secret)
	return &jwtValidator{secret: s}, nil
}

// Validate implements grpcutil.TokenValidator.
//
// It parses and verifies the JWT string, then maps the wire claims to
// *grpcutil.Claims (sub → UserID, email, team, role, scopes).
//
// ERROR CONTRACT (must match services/auth/internal/authn/validator.go):
//
//	On any verification failure → status.Error(codes.Unauthenticated, "invalid token").
//	  The message is deliberately opaque: we do not say whether the token is
//	  expired, forged, or simply malformed. Why? An "expired" vs "bad signature"
//	  distinction is a timing/oracle side-channel that gives an attacker
//	  information about token validity ranges. The client's correct response
//	  to ANY failure is "go get a new token" — the reason does not help them.
//
//	  The grpcutil.authenticate() function passes Unauthenticated messages through
//	  to the client unchanged (it only sanitizes non-client-facing codes like
//	  Internal). So "invalid token" is exactly what clients receive — matching
//	  the auth-side validator's behavior for a uniform surface.
//
// CONTEXT: ctx is passed through to satisfy the interface. The local JWT
// verifier does not need network or DB access, so ctx is unused here. If this
// validator is ever upgraded to the D1 (RPC) path for API-key support, ctx
// will carry the deadline and cancellation signal for the outbound call.
func (v *jwtValidator) Validate(ctx context.Context, tokenString string) (*grpcutil.Claims, error) {
	claims := &jwtClaims{}

	// keyFunc is called by the JWT parser after decoding the header but BEFORE
	// verifying the signature. Our job here is to:
	//   1. Inspect the declared algorithm (token.Method) and REJECT anything
	//      that is not exactly our pinned HS256 instance.
	//   2. Return the HMAC secret so the parser can compute the expected signature.
	//
	// WHY assert token.Method == signingMethod (pointer equality) rather than
	// just checking the name string "HS256":
	//   Comparing the concrete *jwt.SigningMethodHMAC pointer instance rejects
	//   HS384/HS512 (different instances of the same type) AND rejects alg=none
	//   (whose type is jwt.signingMethodNone — a completely different type).
	//   A name-string check ("HS256") would fail if the library ever introduced
	//   a second struct type that produces the string "HS256". Pointer equality
	//   is the strictest possible gate.
	keyFunc := func(token *jwt.Token) (interface{}, error) {
		if token.Method != signingMethod {
			// Return an error here; the parser wraps it in ErrTokenSignatureInvalid.
			// We don't expose the alg value in our own error message because the
			// interceptor will map this to the opaque "invalid token" message anyway,
			// but we do include it internally for log-level debugging (the logging
			// interceptor logs the auth error before it is sanitized at the boundary).
			return nil, fmt.Errorf("auth: unexpected signing method %q — expected HS256", token.Header["alg"])
		}
		return v.secret, nil
	}

	// ParseWithClaims decodes the three JWT segments, calls keyFunc with the
	// decoded token (so we can gate on the alg), verifies the HMAC signature,
	// and validates the registered claims (exp, nbf, iat) according to the options.
	//
	// Parser options (belt-and-suspenders against keyFunc's type assertion):
	//
	//   WithValidMethods(["HS256"]): the parser rejects any token whose alg
	//     header is not in this allowlist before keyFunc even runs. This is
	//     GATE 2 — an earlier, cheaper check that stops alg=none and RS256
	//     before we do any crypto work.
	//
	//   WithExpirationRequired(): rejects tokens that have no exp claim at all.
	//     Without this, a minter that "forgot" to set exp would produce a
	//     never-expiring token — a security hole we close unconditionally.
	parsed, err := jwt.ParseWithClaims(
		tokenString,
		claims,
		keyFunc,
		jwt.WithValidMethods([]string{signingMethod.Alg()}),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		// Map ALL JWT parse/verify errors to the same opaque Unauthenticated
		// status. The library returns typed sentinels (ErrTokenExpired,
		// ErrTokenSignatureInvalid, ErrTokenMalformed, ErrTokenNotValidYet, ...)
		// but we deliberately do NOT expose which one fired — that is the opaque
		// posture documented above. The interceptor forwards Unauthenticated
		// messages through unchanged, so clients see exactly "invalid token".
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}

	// Defensive: ParseWithClaims only returns err==nil when the token is
	// valid, but we assert parsed.Valid explicitly. A future library version
	// that relaxes this invariant would silently let invalid tokens through
	// without this guard — belt-and-suspenders again.
	if !parsed.Valid {
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}

	// ── Claims mapping ────────────────────────────────────────────────────────
	// Translate wire claims → grpcutil.Claims at this single boundary.
	//
	// sub (Subject) → UserID: the canonical identity reference. The "sub" is
	// set to UserID at mint time (see domain/jwt.go:GenerateToken) and is the
	// field handlers key security decisions off (requireAdmin checks UserID).
	//
	// Name is intentionally NOT mapped to grpcutil.Claims: the Claims struct
	// carries the fields the interceptor and handlers need for authorization
	// decisions (UserID, Email, Team, Role, Scopes). Display name is presentation
	// data that handlers retrieve from the user service if needed — it does not
	// belong in the auth context. Adding it would widen the Claims API without
	// a security or routing use-case.
	return &grpcutil.Claims{
		UserID: claims.Subject, // RegisteredClaims.Subject == "sub" claim
		Email:  claims.Email,
		Team:   claims.Team,
		Role:   claims.Role,
		Scopes: claims.Scopes,
	}, nil
}
