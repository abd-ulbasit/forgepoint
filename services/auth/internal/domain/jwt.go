// jwt.go — JWT minting and verification for the Auth service.
//
// ============================================================================
// WHY JWT LOGIC LIVES IN THE DOMAIN LAYER
// ============================================================================
//
// A JWT is the wire representation of a TokenClaims value: GenerateToken turns
// claims → signed string, ValidateToken turns string → verified claims. That is
// a pure business operation (no DB, no network), so it belongs in the domain.
// The only third-party import here is the JWT library itself — which is a
// crypto/encoding utility, not an infrastructure dependency. The Clean
// Architecture rule we care about (no gRPC/NATS/database imports) holds.
//
// ============================================================================
// THE SECURITY MODEL — WHY THE KEYFUNC IS THE WHOLE GAME
// ============================================================================
//
// A JWT is: base64url(header) "." base64url(payload) "." base64url(signature).
// The header and payload are NOT encrypted — anyone can read them. The ONLY
// thing that makes a JWT trustworthy is the signature: proof that whoever
// minted it holds the secret. So verification correctness is everything.
//
// The single most dangerous JWT mistake is letting the *token* dictate *how it
// is verified*. The header carries an "alg" field. A naive verifier reads alg
// from the header and picks the verification routine accordingly. Two attacks
// exploit that:
//
//  1. alg=none — the attacker sets alg to "none" and sends NO signature. A
//     verifier that honors the header skips signature checking entirely and
//     accepts any payload the attacker writes (e.g. role=admin).
//
//  2. HS/RS confusion — a service verifies RS256 (asymmetric: public key
//     verifies, private key signs) and PUBLISHES its public key. The attacker
//     crafts a token with alg=HS256 and signs it using the *public key bytes*
//     as the HMAC secret. A verifier that picks HMAC because the header says
//     HS256, then feeds it the RSA public key, will validate the forgery.
//
// DEFENSE (implemented below): the keyfunc inspects token.Method and accepts
// the key ONLY if the method is the exact HMAC instance we expect. Anything
// else returns an error before any signature math runs. We ALSO pass
// jwt.WithValidMethods(["HS256"]) as belt-and-suspenders: the parser rejects
// unexpected algs before even calling the keyfunc. Two independent gates.
//
// PREVENTING JWT ALGORITHM CONFUSION:
//
//	Pin the algorithm on the verifier side; never trust the token's alg header.
//	Concretely: assert token.Method is *jwt.SigningMethodHMAC inside the keyfunc
//	AND constrain the parser with WithValidMethods. Reject everything else,
//	including alg=none.
package domain

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// signingMethod is the ONE algorithm this service issues and accepts.
//
// WHY HS256 (HMAC-SHA256, symmetric) for this platform:
//   - One shared secret signs and verifies. Simple to distribute to the 10
//     services via a Kubernetes Secret. No keypair/JWKS rotation machinery.
//   - HMAC is fast: signature verification is a single SHA-256, keeping
//     per-request auth overhead negligible on the hot path.
//
// TRADEOFF vs RS256/ES256 (asymmetric):
//   - HS256 means every verifier also holds the *signing* secret — any service
//     that can verify a token can also MINT one. That is acceptable here
//     because all services are first-party and run in the same trust boundary
//     (one cluster, one team). If we ever let third parties verify tokens
//     without being able to forge them, we'd switch to RS256/ES256 + JWKS so
//     only Auth holds the private key. Documented as the migration path.
var signingMethod = jwt.SigningMethodHS256

// jwtClaims is the concrete claims type we sign and parse. It embeds the
// library's RegisteredClaims (sub, exp, iat, ...) and adds Forgepoint-specific
// private claims. Embedding RegisteredClaims makes jwtClaims satisfy the
// jwt.Claims interface for free (the Get* methods are promoted), so the
// library's validator checks exp/iat/nbf automatically.
//
// WHY a typed struct instead of jwt.MapClaims:
//
//	MapClaims is a map[string]any — every read is a type assertion that can
//	silently yield the zero value on a typo. A struct gives compile-time field
//	names and typed JSON unmarshalling, eliminating that whole class of bugs.
//	The domain.TokenClaims <-> jwtClaims conversion is the single boundary
//	where wire fields meet business fields.
type jwtClaims struct {
	jwt.RegisteredClaims          // sub, exp, iat, nbf, iss, aud, jti
	Email                string   `json:"email,omitempty"`
	Name                 string   `json:"name,omitempty"`
	Team                 string   `json:"team,omitempty"`
	Role                 string   `json:"role,omitempty"`
	Scopes               []string `json:"scopes,omitempty"`
}

// minHMACSecretBytes is the minimum number of bytes an HS256 signing secret
// must contain.
//
// WHY 32 bytes (256 bits):
//
//	RFC 7518 §3.2 states: "A key of the same size as the hash output (for
//	instance, 256 bits for 'HS256') or larger MUST be used". The hash output
//	for SHA-256 is 32 bytes. A shorter secret shrinks the effective HMAC key
//	space below 2^256, making offline brute-force against any captured token
//	progressively easier. At exactly 32 bytes the brute-force cost is 2^256
//	— computationally infeasible for the foreseeable future.
//
//	Empty / nil secrets are also covered: len(nil) == 0 < 32.
//
// WHY THE MINIMUM KEY LENGTH IS ENFORCED IN CODE, NOT JUST DOCS:
//
//	Docs can be ignored or never read; a run-time check fires on every call.
//	An operator who accidentally sets JWT_SECRET=abc gets an immediate, named
//	error (ErrWeakSecret) instead of silently running with a breakable HMAC.
const minHMACSecretBytes = 32

// validateHMACSecret is the single internal gate that enforces the HMAC key-
// strength invariant. It is called by BOTH GenerateToken and ValidateToken so
// the rule is symmetric: a verifier configured with a weak secret cannot
// accidentally accept tokens that would never have been minted with one.
//
// WHY symmetric enforcement matters:
//
//	If only GenerateToken rejects weak secrets, a misconfigured deployment might
//	run ValidateToken with a short secret and still accept tokens (possibly ones
//	the attacker forged with an offline brute-force of that short key). Checking
//	in both functions closes the gap: any call site that touches HMAC key
//	material must satisfy the length requirement.
//
// Returns ErrWeakSecret (a named sentinel, not a generic error) so callers can
// errors.Is(err, ErrWeakSecret) and distinguish misconfiguration from an invalid
// credential.
func validateHMACSecret(secret []byte) error {
	if len(secret) < minHMACSecretBytes {
		return ErrWeakSecret
	}
	return nil
}

// GenerateToken mints a signed JWT carrying the given claims, valid for ttl.
//
// It sets the registered claims the verifier relies on:
//   - sub  (Subject)    = claims.UserID — the canonical identity reference.
//   - iat  (IssuedAt)   = now           — when the token was minted.
//   - exp  (ExpiresAt)  = now + ttl      — when it stops being valid.
//
// The caller (Login) chooses ttl. Per design D2 we issue SHORT-lived JWTs so
// that revocation latency (a stateless JWT is valid until exp) stays small.
func GenerateToken(claims TokenClaims, secret []byte, ttl time.Duration) (string, error) {
	// Reject weak secrets before doing any JWT work. An empty or short HMAC key
	// produces a signature that is trivially forgeable by an offline attacker who
	// brute-forces the reduced key space. validateHMACSecret returns ErrWeakSecret
	// (a named sentinel) so operators can distinguish misconfiguration from a
	// legitimately invalid credential. See the minHMACSecretBytes comment for the
	// cryptographic rationale.
	if err := validateHMACSecret(secret); err != nil {
		return "", err
	}

	now := time.Now()
	registered := jwt.RegisteredClaims{
		Subject:   claims.UserID,
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
	}

	tok := jwt.NewWithClaims(signingMethod, &jwtClaims{
		RegisteredClaims: registered,
		Email:            claims.Email,
		Name:             claims.Name,
		Team:             claims.Team,
		Role:             claims.Role,
		Scopes:           claims.Scopes,
	})

	// SignedString runs the HMAC over header.payload with our secret and appends
	// the base64url signature. The output is the complete three-segment JWT.
	signed, err := tok.SignedString(secret)
	if err != nil {
		return "", fmt.Errorf("jwt: signing token: %w", err)
	}
	return signed, nil
}

// ValidateToken parses, verifies, and decodes a JWT string into TokenClaims.
//
// On success the returned claims are trustworthy: the signature checked out
// against our secret, the algorithm was the pinned HMAC method, and exp/iat
// passed the library's time-based validation. On ANY failure it returns an
// error and zero claims — callers must treat a non-nil error as "deny".
func ValidateToken(tokenString string, secret []byte) (*TokenClaims, error) {
	// Enforce the same key-strength requirement as GenerateToken. WHY here too:
	// symmetric enforcement is the security property. If ValidateToken accepted a
	// weak secret, a misconfigured deployment could verify tokens with a
	// brute-forceable key even though no legitimate token was ever minted with
	// one. An attacker who discovered the short secret (via offline brute-force
	// against any captured token) could mint forgeries that this misconfigured
	// verifier would accept. Checking in both functions closes that gap. The
	// caller receives the same named ErrWeakSecret sentinel either way, so
	// startup health checks can surface the misconfiguration clearly.
	if err := validateHMACSecret(secret); err != nil {
		return nil, err
	}

	claims := &jwtClaims{}

	// keyFunc is called by the parser AFTER it decodes the header but BEFORE it
	// verifies the signature. Its job is to return the key to verify with — and,
	// critically, to REFUSE if the token's declared method is not the one we
	// pinned. This is the alg-confusion / alg=none defense (see file header).
	keyFunc := func(token *jwt.Token) (interface{}, error) {
		// token.Method is the SigningMethod the parser instantiated from the
		// header's alg field. We require it to be the EXACT instance we issue
		// with. Comparing the concrete *jwt.SigningMethodHMAC pointer rejects
		// HS384/HS512, every asymmetric method, and "none" (whose method is a
		// different type entirely). We do NOT trust the header — we assert it.
		if token.Method != signingMethod {
			return nil, fmt.Errorf("jwt: unexpected signing method %q", token.Header["alg"])
		}
		return secret, nil
	}

	// Parser options harden validation independently of the keyfunc:
	//   - WithValidMethods: the parser rejects any alg not in this allowlist
	//     before the keyfunc even runs — a second, earlier gate against alg
	//     confusion (defense in depth).
	//   - WithExpirationRequired: a token with NO exp claim is rejected. Without
	//     this, an attacker (or a buggy minter) could issue a never-expiring
	//     token. We require every token to carry an expiry.
	parsed, err := jwt.ParseWithClaims(
		tokenString,
		claims,
		keyFunc,
		jwt.WithValidMethods([]string{signingMethod.Alg()}),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		// The library returns sentinel errors (ErrTokenExpired,
		// ErrTokenSignatureInvalid, ErrTokenMalformed, ...) wrapped in the
		// returned error. We surface a stable message while preserving the
		// wrapped sentinel via %w so callers can errors.Is() if they need to
		// distinguish (e.g. ValidateToken in the service falls through to the
		// API-key path on ANY JWT parse failure).
		return nil, fmt.Errorf("jwt: token invalid: %w", err)
	}
	// Defensive: ParseWithClaims only returns err==nil when the token is valid,
	// but we assert Valid explicitly so a future library change can't silently
	// let an invalid token through.
	if !parsed.Valid {
		return nil, errors.New("jwt: token reported invalid")
	}

	// Convert wire claims → domain claims at this single boundary. iat/exp are
	// guaranteed non-nil here: WithExpirationRequired enforces exp, and we always
	// set iat at mint time (a missing iat just yields the zero time, harmless).
	out := &TokenClaims{
		UserID:    claims.Subject,
		Email:     claims.Email,
		Name:      claims.Name,
		Team:      claims.Team,
		Role:      claims.Role,
		Scopes:    claims.Scopes,
		TokenKind: TokenKindJWT,
	}
	if claims.IssuedAt != nil {
		out.IssuedAt = claims.IssuedAt.Time
	}
	if claims.ExpiresAt != nil {
		out.ExpiresAt = claims.ExpiresAt.Time
	}
	return out, nil
}
