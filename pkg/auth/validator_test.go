package auth_test

// ============================================================================
// TDD: pkg/auth JWT VALIDATOR TESTS
// ============================================================================
//
// These tests are the specification for NewJWTValidator and its Validate method.
// They were written first (failing) then the implementation was written to make
// them pass — that is the TDD flow described in docs/design/service-architecture.md.
//
// WHAT IS COVERED:
//   ✓ Happy path: valid HS256 token with all claims maps correctly
//   ✓ Sub (UserID), email, team, role, scopes all transfer correctly
//   ✓ alg=none token is rejected (algorithm confusion defense)
//   ✓ Wrong secret signature is rejected
//   ✓ Expired token is rejected
//   ✓ Token with no exp claim is rejected (WithExpirationRequired)
//   ✓ Garbage string is rejected
//   ✓ Empty token string is rejected
//   ✓ <32-byte secret is rejected at construction time
//   ✓ Empty secret is rejected at construction time
//   ✓ Validator returns Unauthenticated (not Internal) on bad tokens
//   ✓ Error message is opaque "invalid token" (no reason leakage)
//   ✓ Race detector clean (-race flag)
//
// HOW TOKENS ARE MINTED IN TESTS:
//   We use golang-jwt/jwt directly to sign test tokens — the same library the
//   auth service uses. This is a white-box test of the token format: it proves
//   the validator accepts tokens that auth actually issues, not just tokens that
//   happen to parse. The signing logic mirrors domain/jwt.go exactly (same
//   signingMethod, same claim struct shape, same field names).
// ============================================================================

import (
	"context"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/abd-ulbasit/forgepoint/pkg/auth"
)

// ── Test helpers ─────────────────────────────────────────────────────────────

// validSecret32 is a 32-byte secret (exactly the minimum). Use a fixed value
// so tests are deterministic. In production, secrets are random and >= 32 bytes.
var validSecret32 = []byte("00000000000000000000000000000032") // exactly 32 bytes

// testClaims is a mirror of the auth domain's jwtClaims struct.
//
// WHY duplicate it here: the auth package's jwtClaims is unexported (it is an
// internal wire type). Tests must be able to mint tokens in exactly the same
// format to prove round-trip fidelity. We replicate the struct with the same
// json tags, which is the behaviorally correct way to test the wire contract.
type testClaims struct {
	jwt.RegisteredClaims
	Email  string   `json:"email,omitempty"`
	Name   string   `json:"name,omitempty"`
	Team   string   `json:"team,omitempty"`
	Role   string   `json:"role,omitempty"`
	Scopes []string `json:"scopes,omitempty"`
}

// mintToken creates a signed JWT using the auth domain's format (HS256,
// typed testClaims struct). ttl <= 0 means "do not set exp" (for
// WithExpirationRequired tests). If ttl < 0, the exp is set in the past.
func mintToken(t *testing.T, secret []byte, userID, email, team, role string, scopes []string, ttl time.Duration, setExp bool) string {
	t.Helper()

	now := time.Now()
	registered := jwt.RegisteredClaims{
		Subject:  userID,
		IssuedAt: jwt.NewNumericDate(now),
	}
	if setExp {
		registered.ExpiresAt = jwt.NewNumericDate(now.Add(ttl))
	}

	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, &testClaims{
		RegisteredClaims: registered,
		Email:            email,
		Name:             "Test User",
		Team:             team,
		Role:             role,
		Scopes:           scopes,
	})

	signed, err := tok.SignedString(secret)
	if err != nil {
		t.Fatalf("mintToken: failed to sign: %v", err)
	}
	return signed
}

// assertUnauthenticated checks the error is a gRPC Unauthenticated status.
func assertUnauthenticated(t *testing.T, err error, context string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected Unauthenticated error, got nil", context)
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("%s: error is not a gRPC status: %v", context, err)
	}
	if st.Code() != codes.Unauthenticated {
		t.Errorf("%s: code = %s, want Unauthenticated", context, st.Code())
	}
}

// assertOpaqueMessage checks the error message is the opaque sentinel and does
// not leak any JWT internals (expired / signature / algorithm names).
func assertOpaqueMessage(t *testing.T, err error, context string) {
	t.Helper()
	st, _ := status.FromError(err)
	msg := st.Message()
	if msg != "invalid token" {
		t.Errorf("%s: message = %q, want \"invalid token\" (must be opaque)", context, msg)
	}
}

// ── Construction tests ────────────────────────────────────────────────────────

func TestNewJWTValidator_RejectsEmptySecret(t *testing.T) {
	_, err := auth.NewJWTValidator(nil)
	if err == nil {
		t.Fatal("expected error for nil secret, got nil")
	}
}

func TestNewJWTValidator_RejectsShortSecret(t *testing.T) {
	// Table of secrets that are below the 32-byte minimum.
	cases := []struct {
		name   string
		secret []byte
	}{
		{"empty string", []byte("")},
		{"1 byte", []byte("x")},
		{"31 bytes", []byte("0000000000000000000000000000031")}, // 31 chars
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := auth.NewJWTValidator(tc.secret)
			if err == nil {
				t.Errorf("secret len=%d: expected error for short secret, got nil", len(tc.secret))
			}
		})
	}
}

func TestNewJWTValidator_AcceptsExactly32Bytes(t *testing.T) {
	secret := make([]byte, 32)
	v, err := auth.NewJWTValidator(secret)
	if err != nil {
		t.Fatalf("32-byte secret: unexpected error: %v", err)
	}
	if v == nil {
		t.Fatal("expected non-nil validator")
	}
}

func TestNewJWTValidator_AcceptsLongerSecret(t *testing.T) {
	secret := make([]byte, 64)
	v, err := auth.NewJWTValidator(secret)
	if err != nil {
		t.Fatalf("64-byte secret: unexpected error: %v", err)
	}
	if v == nil {
		t.Fatal("expected non-nil validator")
	}
}

// ── Happy-path tests ──────────────────────────────────────────────────────────

// TestValidate_ValidToken is the primary happy path: a well-formed HS256 token
// with all claims present is accepted and all fields map correctly.
func TestValidate_ValidToken(t *testing.T) {
	v, err := auth.NewJWTValidator(validSecret32)
	if err != nil {
		t.Fatalf("NewJWTValidator: %v", err)
	}

	tokenStr := mintToken(t, validSecret32,
		"user-123", "alice@fp.io", "platform", "admin",
		[]string{"models:read", "models:write"},
		15*time.Minute, true,
	)

	claims, err := v.Validate(context.Background(), tokenStr)
	if err != nil {
		t.Fatalf("Validate: unexpected error: %v", err)
	}

	// Verify every field maps correctly from the JWT to grpcutil.Claims.
	if claims.UserID != "user-123" {
		t.Errorf("UserID = %q, want %q", claims.UserID, "user-123")
	}
	if claims.Email != "alice@fp.io" {
		t.Errorf("Email = %q, want %q", claims.Email, "alice@fp.io")
	}
	if claims.Team != "platform" {
		t.Errorf("Team = %q, want %q", claims.Team, "platform")
	}
	if claims.Role != "admin" {
		t.Errorf("Role = %q, want %q", claims.Role, "admin")
	}
	if len(claims.Scopes) != 2 || claims.Scopes[0] != "models:read" || claims.Scopes[1] != "models:write" {
		t.Errorf("Scopes = %v, want [models:read models:write]", claims.Scopes)
	}
}

// TestValidate_EmptyScopesOmitted verifies tokens without scopes (nil Scopes in
// the JWT) do not cause a panic or mapping error — the field is optional.
func TestValidate_EmptyScopesOmitted(t *testing.T) {
	v, _ := auth.NewJWTValidator(validSecret32)

	tokenStr := mintToken(t, validSecret32,
		"user-456", "bob@fp.io", "ml", "viewer", nil,
		time.Hour, true,
	)

	claims, err := v.Validate(context.Background(), tokenStr)
	if err != nil {
		t.Fatalf("Validate: unexpected error: %v", err)
	}
	if claims.UserID != "user-456" {
		t.Errorf("UserID = %q, want %q", claims.UserID, "user-456")
	}
	// Scopes may be nil or empty — both are acceptable.
	if len(claims.Scopes) != 0 {
		t.Errorf("Scopes = %v, want empty", claims.Scopes)
	}
}

// ── Rejection tests ───────────────────────────────────────────────────────────

// TestValidate_RejectsAlgNone is the most critical security test.
//
// ATTACK BACKGROUND: JWT's alg=none says "I claim this token needs no signature
// verification." A verifier that honors the header would accept ANY payload with
// alg=none — including role=admin forged by an attacker. Our keyfunc asserts
// token.Method == signingMethod (HS256 instance), so alg=none (a different Go
// type) is rejected before any signature math.
//
// We use jwt.UnsafeAllowNoneSignatureType to construct the token. If Validate
// accepts it, the test fails — proving the defense is absent.
func TestValidate_RejectsAlgNone(t *testing.T) {
	v, _ := auth.NewJWTValidator(validSecret32)

	now := time.Now()
	claims := &testClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "attacker",
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)),
		},
		Role: "admin",
	}

	// jwt.UnsafeAllowNoneSignatureType is the library's escape hatch for
	// signing with alg=none. Using it here proves we reject the produced token.
	tok := jwt.NewWithClaims(jwt.SigningMethodNone, claims)
	tokenStr, err := tok.SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatalf("failed to mint alg=none token: %v", err)
	}

	_, validateErr := v.Validate(context.Background(), tokenStr)
	assertUnauthenticated(t, validateErr, "alg=none token")
	assertOpaqueMessage(t, validateErr, "alg=none token")
}

// TestValidate_RejectsWrongSecret verifies a token signed with a different
// secret (simulating a forged or cross-environment token) is rejected.
func TestValidate_RejectsWrongSecret(t *testing.T) {
	v, _ := auth.NewJWTValidator(validSecret32)

	// Sign with a DIFFERENT secret — same length, different bytes.
	wrongSecret := []byte("99999999999999999999999999999932")
	tokenStr := mintToken(t, wrongSecret,
		"user-789", "eve@attacker.io", "external", "admin", nil,
		time.Hour, true,
	)

	_, err := v.Validate(context.Background(), tokenStr)
	assertUnauthenticated(t, err, "wrong-secret token")
	assertOpaqueMessage(t, err, "wrong-secret token")
}

// TestValidate_RejectsExpiredToken checks that a token whose exp is in the past
// is rejected by the library's time validation (WithExpirationRequired + the
// standard exp claim check).
func TestValidate_RejectsExpiredToken(t *testing.T) {
	v, _ := auth.NewJWTValidator(validSecret32)

	// Negative TTL → exp is set to now - 1 minute (already expired).
	tokenStr := mintToken(t, validSecret32,
		"user-expired", "expired@fp.io", "team", "viewer", nil,
		-time.Minute, true, // setExp=true but in the past
	)

	_, err := v.Validate(context.Background(), tokenStr)
	assertUnauthenticated(t, err, "expired token")
	assertOpaqueMessage(t, err, "expired token")
}

// TestValidate_RejectsMissingExp verifies that a token with NO exp claim is
// rejected by WithExpirationRequired. This closes the "immortal token" attack
// where a minter accidentally omits exp and the token stays valid forever.
func TestValidate_RejectsMissingExp(t *testing.T) {
	v, _ := auth.NewJWTValidator(validSecret32)

	// setExp=false → no ExpiresAt claim in the token.
	tokenStr := mintToken(t, validSecret32,
		"user-noexp", "noexp@fp.io", "team", "viewer", nil,
		0, false, // ttl irrelevant; exp will not be set
	)

	_, err := v.Validate(context.Background(), tokenStr)
	assertUnauthenticated(t, err, "no-exp token")
	assertOpaqueMessage(t, err, "no-exp token")
}

// TestValidate_RejectsGarbage verifies that completely malformed strings
// (not even a JWT shape) are rejected cleanly — no panic, Unauthenticated.
func TestValidate_RejectsGarbage(t *testing.T) {
	v, _ := auth.NewJWTValidator(validSecret32)

	cases := []struct {
		name  string
		token string
	}{
		{"empty string", ""},
		{"random string", "notavalidjwt"},
		{"two segments only", "header.payload"},
		{"random bytes", "aGVsbG8=.d29ybGQ=."},
		{"sql injection", "' OR 1=1 --"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := v.Validate(context.Background(), tc.token)
			assertUnauthenticated(t, err, tc.name)
			// For garbage inputs the message is still opaque — we don't want
			// "malformed token: could not base64 decode" leaking parse internals.
			assertOpaqueMessage(t, err, tc.name)
		})
	}
}

// ── Concurrency / race detector tests ────────────────────────────────────────

// TestValidate_ConcurrentSafe runs concurrent validations against a single
// validator instance to confirm there are no data races. Run with -race.
//
// WHY test concurrency for a validator: validators are singletons created once
// at service startup and called from every goroutine handling an RPC. Any mutable
// state in the validator would race. The secret []byte is read-only after
// construction; this test proves the library path is safe too.
func TestValidate_ConcurrentSafe(t *testing.T) {
	v, _ := auth.NewJWTValidator(validSecret32)

	tokenStr := mintToken(t, validSecret32,
		"user-concurrent", "c@fp.io", "platform", "engineer",
		[]string{"models:read"},
		15*time.Minute, true,
	)

	const goroutines = 20
	errs := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			_, err := v.Validate(context.Background(), tokenStr)
			errs <- err
		}()
	}

	for i := 0; i < goroutines; i++ {
		if err := <-errs; err != nil {
			t.Errorf("concurrent Validate: unexpected error: %v", err)
		}
	}
}
