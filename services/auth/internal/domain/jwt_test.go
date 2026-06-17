package domain

// jwt_test.go — TDD specification for the JWT generate/validate pair.
//
// These tests are written BEFORE jwt.go exists (red phase). They encode the
// security-critical contract the implementation must satisfy:
//
//   1. ROUNDTRIP: claims that go into GenerateToken come back out of
//      ValidateToken unchanged (sub, email, team, role, scopes, iat, exp).
//   2. EXPIRY: a token whose exp is in the past is rejected.
//   3. TAMPER / WRONG SECRET: a token validated with the wrong secret (or whose
//      signature bytes were altered) is rejected — the signature is the only
//      thing standing between a forged token and full account takeover.
//   4. ALG CONFUSION: a token signed with anything other than the pinned HMAC
//      method is rejected. This is the famous "alg=none" / "HS/RS confusion"
//      class of JWT vulnerabilities; the keyfunc must defend against it.
//
// The tests verify REAL behavior (decode actual bytes, flip actual signature
// chars), not just that a function was called.

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestGenerateAndValidateToken_Roundtrip(t *testing.T) {
	secret := []byte("test-secret-do-not-use-in-prod!!")
	in := TokenClaims{
		UserID:    "user-123",
		Email:     "ada@forgepoint.dev",
		Name:      "Ada Lovelace",
		Team:      "platform",
		Role:      "engineer",
		Scopes:    []string{"models:read", "experiments:write"},
		TokenKind: TokenKindJWT,
	}

	tok, err := GenerateToken(in, secret, time.Hour)
	if err != nil {
		t.Fatalf("GenerateToken: unexpected error: %v", err)
	}
	if tok == "" {
		t.Fatal("GenerateToken returned empty token")
	}

	out, err := ValidateToken(tok, secret)
	if err != nil {
		t.Fatalf("ValidateToken: unexpected error: %v", err)
	}

	// Identity fields must survive the roundtrip exactly.
	if out.UserID != in.UserID {
		t.Errorf("UserID = %q, want %q", out.UserID, in.UserID)
	}
	if out.Email != in.Email {
		t.Errorf("Email = %q, want %q", out.Email, in.Email)
	}
	if out.Name != in.Name {
		t.Errorf("Name = %q, want %q", out.Name, in.Name)
	}
	if out.Team != in.Team {
		t.Errorf("Team = %q, want %q", out.Team, in.Team)
	}
	if out.Role != in.Role {
		t.Errorf("Role = %q, want %q", out.Role, in.Role)
	}
	if out.TokenKind != TokenKindJWT {
		t.Errorf("TokenKind = %q, want %q", out.TokenKind, TokenKindJWT)
	}
	if len(out.Scopes) != len(in.Scopes) {
		t.Fatalf("Scopes len = %d, want %d (%v)", len(out.Scopes), len(in.Scopes), out.Scopes)
	}
	// range-over-slice with both index and value: idiomatic and avoids the
	// "loop variable reuse" footgun that the older `for i := range slice`
	// pattern introduced in pre-Go 1.22 code.
	for i, want := range in.Scopes {
		if out.Scopes[i] != want {
			t.Errorf("Scopes[%d] = %q, want %q", i, out.Scopes[i], want)
		}
	}

	// exp/iat must be populated and ordered correctly.
	if out.IssuedAt.IsZero() {
		t.Error("IssuedAt is zero; expected iat to be set")
	}
	if out.ExpiresAt.IsZero() {
		t.Error("ExpiresAt is zero; expected exp to be set")
	}
	if !out.ExpiresAt.After(out.IssuedAt) {
		t.Errorf("ExpiresAt (%v) must be after IssuedAt (%v)", out.ExpiresAt, out.IssuedAt)
	}
	// TTL was 1h: exp - iat should be ~1h.
	if d := out.ExpiresAt.Sub(out.IssuedAt); d < 59*time.Minute || d > 61*time.Minute {
		t.Errorf("exp-iat = %v, want ~1h", d)
	}
}

func TestValidateToken_ExpiredRejected(t *testing.T) {
	secret := []byte("test-secret-do-not-use-in-prod!!")
	in := TokenClaims{UserID: "user-123", TokenKind: TokenKindJWT}

	// Negative TTL → exp is in the past the moment the token is minted.
	tok, err := GenerateToken(in, secret, -time.Minute)
	if err != nil {
		t.Fatalf("GenerateToken: unexpected error: %v", err)
	}

	_, err = ValidateToken(tok, secret)
	if err == nil {
		t.Fatal("ValidateToken accepted an expired token; want rejection")
	}
	// The error should specifically be an expiry error, not a generic one,
	// so the implementation must validate exp (not just the signature).
	if !strings.Contains(err.Error(), "expired") {
		t.Errorf("error = %q, want it to mention expiry", err.Error())
	}
}

func TestValidateToken_WrongSecretRejected(t *testing.T) {
	in := TokenClaims{UserID: "user-123", TokenKind: TokenKindJWT}
	// Secrets must be >= 32 bytes; pad both to meet the requirement while
	// keeping them clearly distinct so the HMAC cannot spuriously match.
	tok, err := GenerateToken(in, []byte("the-real-secret-padded-to-32byt!"), time.Hour)
	if err != nil {
		t.Fatalf("GenerateToken: unexpected error: %v", err)
	}

	// A different secret means the HMAC won't match — forged/foreign token.
	_, err = ValidateToken(tok, []byte("an-attacker-secret-padded-32byt!"))
	if err == nil {
		t.Fatal("ValidateToken accepted a token signed with a different secret")
	}
}

func TestValidateToken_TamperedSignatureRejected(t *testing.T) {
	secret := []byte("test-secret-do-not-use-in-prod!!")
	in := TokenClaims{UserID: "user-123", Role: "viewer", TokenKind: TokenKindJWT}
	tok, err := GenerateToken(in, secret, time.Hour)
	if err != nil {
		t.Fatalf("GenerateToken: unexpected error: %v", err)
	}

	// A JWT is header.payload.signature. Flip a character in the signature
	// segment; the HMAC verification must fail.
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		t.Fatalf("token does not have 3 segments: %q", tok)
	}
	sig := []byte(parts[2])
	// Flip the first signature byte to a different valid base64url char.
	if sig[0] == 'A' {
		sig[0] = 'B'
	} else {
		sig[0] = 'A'
	}
	tampered := parts[0] + "." + parts[1] + "." + string(sig)

	if _, err := ValidateToken(tampered, secret); err == nil {
		t.Fatal("ValidateToken accepted a token with a tampered signature")
	}
}

func TestValidateToken_TamperedPayloadRejected(t *testing.T) {
	secret := []byte("test-secret-do-not-use-in-prod!!")

	// Re-sign a privilege-escalated payload with a DIFFERENT key (the attacker
	// does not have our secret) and present it. Verification must fail because
	// the signature was produced with the wrong key.
	forged := jwt.NewWithClaims(jwt.SigningMethodHS256, &jwtClaims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: "user-123"},
		Role:             "admin", // escalated
	})
	// The attacker's signing key can be any length (it's their own key); the
	// test is that OUR ValidateToken rejects the token regardless.  We still
	// use a >=32-byte key here so the JWT library itself doesn't complain about
	// key strength before we even get to our own validation logic.
	forgedStr, err := forged.SignedString([]byte("attacker-key-padded-to-32-bytes!"))
	if err != nil {
		t.Fatalf("signing forged token: %v", err)
	}
	if _, err := ValidateToken(forgedStr, secret); err == nil {
		t.Fatal("ValidateToken accepted a forged token signed with the wrong key")
	}
}

// TestValidateToken_AlgNoneRejected is THE security test. An attacker takes a
// valid-looking payload and signs it with the "none" algorithm (no signature).
// A naive verifier that trusts the token's own alg header would accept it,
// granting the attacker any identity they encode. Our keyfunc pins the method
// to HMAC, so this must be rejected.
func TestValidateToken_AlgNoneRejected(t *testing.T) {
	secret := []byte("test-secret-do-not-use-in-prod!!")

	// Sanity: a properly-minted token IS accepted with this secret, so a
	// rejection below is attributable to alg=none, not a bad secret.
	good, err := GenerateToken(TokenClaims{UserID: "user-123", TokenKind: TokenKindJWT}, secret, time.Hour)
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	if _, err := ValidateToken(good, secret); err != nil {
		t.Fatalf("sanity: well-formed token rejected: %v", err)
	}

	// jwt.UnsafeAllowNoneSignatureType is the library's explicit opt-in escape
	// hatch for producing an unsigned token — exactly what an attacker would do.
	unsigned := jwt.NewWithClaims(jwt.SigningMethodNone, &jwtClaims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: "attacker"},
		Role:             "admin",
	})
	noneTok, err := unsigned.SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatalf("creating alg=none token: %v", err)
	}

	if _, err := ValidateToken(noneTok, secret); err == nil {
		t.Fatal("ValidateToken accepted an alg=none token; alg confusion vulnerability")
	}
}

func TestValidateToken_MalformedRejected(t *testing.T) {
	secret := []byte("test-secret-do-not-use-in-prod!!")
	for _, garbage := range []string{"", "not-a-jwt", "a.b", "a.b.c.d"} {
		if _, err := ValidateToken(garbage, secret); err == nil {
			t.Errorf("ValidateToken accepted malformed input %q", garbage)
		}
	}
}

// ============================================================================
// SECURITY REGRESSION: HMAC secret strength enforcement (Finding 1)
// ============================================================================
//
// WHY these tests exist:
//
//	For HS256, the secret IS the entire security boundary. A short secret
//	(< 32 bytes / 256 bits) shrinks the key space from 2^256 to something an
//	attacker can brute-force offline against any captured token. An empty
//	secret is even worse — it is a constant, publicly-known "key".
//
//	The original code rejected empty secrets in GenerateToken but NOT in
//	ValidateToken, and neither function enforced a minimum length. The
//	asymmetry means a misconfigured verifier could accept tokens it never
//	should have validated.
//
//	The fix introduces a single internal helper (validateHMACSecret) called
//	in BOTH paths so the invariant is enforced symmetrically — the verifier
//	is at least as strict as the minter. These tests pin that contract.
//
// INTERVIEW: "What happens if your JWT secret is short?"
//
//	Brute-force becomes feasible. Any attacker who captures a signed token
//	can try every possible key offline, without rate limits, until one
//	produces a matching signature — then forge tokens freely. Enforcing >=32
//	bytes closes that window: 2^256 operations is computationally infeasible.

// TestGenerateToken_WeakSecretRejected ensures GenerateToken refuses to mint
// tokens under a secret shorter than 32 bytes.
func TestGenerateToken_WeakSecretRejected(t *testing.T) {
	// The claim payload is irrelevant here — we're testing secret validation,
	// which runs BEFORE any HMAC operation.
	in := TokenClaims{UserID: "user-1", TokenKind: TokenKindJWT}

	cases := []struct {
		name   string
		secret []byte
	}{
		{"empty secret", []byte{}},
		{"nil secret", nil},
		{"1-byte secret", []byte("x")},
		{"31-byte secret (one short)", []byte("this-is-31-bytes-exactly-padded")}, // 31 bytes
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := GenerateToken(in, tc.secret, time.Hour)
			if err == nil {
				t.Fatalf("GenerateToken with %s: expected ErrWeakSecret, got nil", tc.name)
			}
			if !errors.Is(err, ErrWeakSecret) {
				t.Errorf("GenerateToken with %s: error = %v, want errors.Is ErrWeakSecret", tc.name, err)
			}
		})
	}
}

// TestGenerateToken_StrongSecretAccepted ensures exactly-32-byte and longer
// secrets still work — confirming the boundary is >= 32, not > 32.
func TestGenerateToken_StrongSecretAccepted(t *testing.T) {
	in := TokenClaims{UserID: "user-1", TokenKind: TokenKindJWT}

	cases := []struct {
		name   string
		secret []byte
	}{
		{"exactly 32 bytes", []byte("12345678901234567890123456789012")},                                          // 32 bytes
		{"64 bytes (common in prod)", []byte("this-is-a-long-jwt-secret-for-hs256-at-exactly-sixty-four-bytes!")}, // 64 bytes
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tok, err := GenerateToken(in, tc.secret, time.Hour)
			if err != nil {
				t.Fatalf("GenerateToken with %s: unexpected error: %v", tc.name, err)
			}
			if tok == "" {
				t.Fatalf("GenerateToken with %s: returned empty token", tc.name)
			}
			// The minted token must also validate with the SAME secret.
			if _, err := ValidateToken(tok, tc.secret); err != nil {
				t.Fatalf("ValidateToken after GenerateToken with %s: %v", tc.name, err)
			}
		})
	}
}

// TestValidateToken_WeakSecretRejected is the ASYMMETRY fix: ValidateToken
// must also reject short/empty secrets. Without this, a misconfigured verifier
// could accept arbitrary tokens (the HMAC over an empty key still "matches"
// a token that was signed with the same empty key — or worse, a token where
// the signature was crafted knowing the key is empty/guessable).
func TestValidateToken_WeakSecretRejected(t *testing.T) {
	// Mint a legitimate token with a strong secret first — we need a syntactically
	// valid token string to hand to ValidateToken.
	strongSecret := []byte("32-bytes-strong-secret-for-test!")
	tok, err := GenerateToken(TokenClaims{UserID: "u1", TokenKind: TokenKindJWT}, strongSecret, time.Hour)
	if err != nil {
		t.Fatalf("setup: GenerateToken: %v", err)
	}

	cases := []struct {
		name   string
		secret []byte
	}{
		{"empty secret", []byte{}},
		{"nil secret", nil},
		{"1-byte secret", []byte("x")},
		{"31-byte secret (one short)", []byte("this-is-31-bytes-exactly-padded")}, // 31 bytes
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ValidateToken(tok, tc.secret)
			if err == nil {
				t.Fatalf("ValidateToken with %s: expected error, got nil", tc.name)
			}
			if !errors.Is(err, ErrWeakSecret) {
				t.Errorf("ValidateToken with %s: error = %v, want errors.Is ErrWeakSecret", tc.name, err)
			}
		})
	}
}
