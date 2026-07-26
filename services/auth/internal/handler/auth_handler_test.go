// auth_handler_test.go — COMPONENT tests for the Auth gRPC handler.
//
// ============================================================================
// WHAT "COMPONENT TEST" MEANS HERE (and why this is the right level)
// ============================================================================
//
// These tests exercise the handler END-TO-END through a REAL gRPC stack
// (bufconn: an in-process HTTP/2 transport — see pkg/testutil/grpc.go) but with
// the DOMAIN SERVICE replaced by a hand-written mock. That isolation is
// deliberate:
//
//   - We mock the AuthService INTERFACE, not the repositories. The unit under
//     test is the handler: proto<->domain conversion, validation, error mapping,
//     and the never-leak-secrets invariant. Mocking the service (one interface)
//     instead of three repos keeps the test about the handler, not the domain's
//     internal wiring. The domain's own logic has its own -race unit tests.
//
//   - Using bufconn instead of calling handler methods directly means the proto
//     actually serializes over the wire: a field we forget to map shows up as a
//     zero value on the client side, and a status error round-trips through
//     gRPC's status machinery exactly as a real client would see it. Calling the
//     method in-process would skip that and could hide marshalling bugs.
//
// NO testcontainers, NO database, NO network sockets — this whole file runs in
// milliseconds and is safe to run on every change.
//
// ============================================================================
// THE FOUR THINGS EVERY RPC TEST ASSERTS (the task's contract)
// ============================================================================
//
//	(a) HAPPY PATH: proto<->domain conversion is correct (the mock records what
//	    domain input it received; we assert the response fields).
//	(b) VALIDATION: bad requests are rejected with codes.InvalidArgument BEFORE
//	    the domain is ever called (we assert the mock was NOT invoked).
//	(c) ERROR MAPPING: each domain sentinel maps to the right gRPC status code.
//	(d) NO LEAK: error messages never contain internal/SQL/PII text; secret
//	    fields (PasswordHash, KeyHash) never appear in any response.
package handler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	authv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/auth/v1"
	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/auth/internal/domain"
)

// ----------------------------------------------------------------------------
// Tiny test helpers (kept local so the test file is self-contained).
// ----------------------------------------------------------------------------

// errorsNew is a thin alias so tests read cleanly when fabricating an
// UNRECOGNIZED domain error (one toStatusError must sanitize to Internal).
func errorsNew(msg string) error { return errors.New(msg) }

// wrap simulates the domain wrapping a sentinel with a specific message
// (fmt.Errorf("...: %w", sentinel)) so we can verify errors.Is-based mapping
// survives wrapping.
func wrap(sentinel error, msg string) error { return fmt.Errorf("%s: %w", msg, sentinel) }

// timestamppbNew is a local shorthand for constructing a proto timestamp.
func timestamppbNew(t time.Time) *timestamppb.Timestamp { return timestamppb.New(t) }

// stubValidator implements grpcutil.TokenValidator by returning fixed claims.
// We drive the REAL grpcutil.AuthUnaryInterceptor with it so the test exercises
// the genuine claims-injection path (the handler reads claims via grpcutil's
// own private context key, which only that interceptor can populate).
type stubValidator struct {
	claims *grpcutil.Claims
	err    error
}

func (s stubValidator) Validate(ctx context.Context, token string) (*grpcutil.Claims, error) {
	return s.claims, s.err
}

// ============================================================================
// MOCK domain.AuthService — a hand-written test double
// ============================================================================
//
// WHY hand-written and not gomock/mockery: a hand-written mock with function
// fields is dependency-free, reads top-to-bottom, and makes each test set ONLY
// the behavior it needs (the rest stay nil and would panic if unexpectedly
// called — a useful "this RPC should not touch the domain" assertion). For an
// 8-method interface this is less code than a generated mock plus its tooling.
//
// Each field is a func matching the interface method. A nil field means "this
// test does not expect this method to be called"; calling it fails the test via
// the t captured in the closure (we instead record calls and let the absence of
// a set function surface as a nil-call panic recovered by the recovery
// interceptor -> Internal, which the test then flags). To make "not called"
// assertions precise we also track call counts.
type mockAuthService struct {
	loginFn         func(ctx context.Context, email, password string) (string, error)
	createUserFn    func(ctx context.Context, in domain.CreateUserInput) (domain.User, error)
	createAPIKeyFn  func(ctx context.Context, userID string, scopes []string, expiresAt *time.Time) (domain.APIKey, string, error)
	validateTokenFn func(ctx context.Context, token string) (domain.TokenClaims, error)
	checkPermFn     func(ctx context.Context, userID, resource, action string) (bool, error)
	checkPermClaims func(ctx context.Context, claims domain.TokenClaims, resource, action string) (bool, error)
	assignRoleFn    func(ctx context.Context, userID, roleName string) error

	// Call recorders — let validation tests assert the domain was NOT reached.
	loginCalls        int
	createUserCalls   int
	createAPIKeyCalls int
	validateCalls     int
	checkPermCalls    int
	assignRoleCalls   int

	// Captured inputs for happy-path conversion assertions.
	lastCreateUserInput   domain.CreateUserInput
	lastAPIKeyOwnerID     string
	lastAPIKeyScopes      []string
	lastAPIKeyExpiresAt   *time.Time
	lastCheckPermResource string
	lastCheckPermAction   string
	lastCheckPermUserID   string
}

// The mock must satisfy domain.AuthService. Each method delegates to its func
// field (panicking via a clear message if a test forgot to set one), records
// the call, and captures inputs.

func (m *mockAuthService) CreateUser(ctx context.Context, in domain.CreateUserInput) (domain.User, error) {
	m.createUserCalls++
	m.lastCreateUserInput = in
	return m.createUserFn(ctx, in)
}

func (m *mockAuthService) Login(ctx context.Context, email, password string) (string, error) {
	m.loginCalls++
	return m.loginFn(ctx, email, password)
}

func (m *mockAuthService) CreateAPIKey(ctx context.Context, userID string, scopes []string, expiresAt *time.Time) (domain.APIKey, string, error) {
	m.createAPIKeyCalls++
	m.lastAPIKeyOwnerID = userID
	m.lastAPIKeyScopes = scopes
	m.lastAPIKeyExpiresAt = expiresAt
	return m.createAPIKeyFn(ctx, userID, scopes, expiresAt)
}

func (m *mockAuthService) ValidateToken(ctx context.Context, token string) (domain.TokenClaims, error) {
	m.validateCalls++
	return m.validateTokenFn(ctx, token)
}

func (m *mockAuthService) CheckPermission(ctx context.Context, userID, resource, action string) (bool, error) {
	m.checkPermCalls++
	m.lastCheckPermUserID = userID
	m.lastCheckPermResource = resource
	m.lastCheckPermAction = action
	return m.checkPermFn(ctx, userID, resource, action)
}

func (m *mockAuthService) CheckPermissionForClaims(ctx context.Context, claims domain.TokenClaims, resource, action string) (bool, error) {
	return m.checkPermClaims(ctx, claims, resource, action)
}

func (m *mockAuthService) AssignRole(ctx context.Context, userID, roleName string) error {
	m.assignRoleCalls++
	return m.assignRoleFn(ctx, userID, roleName)
}

// ============================================================================
// TEST HARNESS
// ============================================================================

// newTestClient wires the handler (backed by the given mock) onto a bufconn
// gRPC server and returns a ready AuthServiceClient. We OPTIONALLY install an
// interceptor that injects claims into the context — CreateAPIKey reads claims,
// so its tests need them present.
func newTestClient(t *testing.T, svc domain.AuthService, opts ...grpc.ServerOption) authv1.AuthServiceClient {
	t.Helper()
	h := NewAuthHandler(svc)
	conn := testutil.NewTestGRPCServer(t, func(s *grpc.Server) {
		authv1.RegisterAuthServiceServer(s, h)
	}, opts...)
	return authv1.NewAuthServiceClient(conn)
}

// claimsInjector is a server option that installs the REAL
// grpcutil.AuthUnaryInterceptor backed by a stub validator returning the given
// claims. This is the SAME public code path the platform uses to put claims in
// the context, so the handler's grpcutil.ClaimsFromContext sees an authenticated
// caller exactly as in production — we are not faking the context plumbing.
func claimsInjector(claims *grpcutil.Claims) grpc.ServerOption {
	return grpc.ChainUnaryInterceptor(grpcutil.AuthUnaryInterceptor(stubValidator{claims: claims}))
}

// adminClaims is the standard authenticated caller used by the admin-gated RPC
// tests (CreateUser / ListUsers / AssignRole). The handler does NOT trust the
// claim's role string for authorization — it asks the domain via CheckPermission
// — so tests pair this with allowAdmin (below) to grant the decision.
var adminClaims = &grpcutil.Claims{UserID: "admin-uuid", Role: "admin"}

// allowAdmin returns a checkPermFn that grants the "admin" action on the given
// resource for the configured caller and denies everything else. It is the
// in-handler authorization gate's positive answer: it lets admin-gated RPC tests
// pass the requireAdmin / cross-user check and proceed to the real assertions.
//
// It records nothing extra (the mock already records the last triple); it simply
// returns the decision the test wants.
// allowAll is a checkPermFn that grants every authorization decision. Used by
// admin-RPC tests whose focus is conversion/validation/error-mapping, not the
// authz gate itself (which has its own dedicated tests below).
func allowAll(ctx context.Context, userID, resource, action string) (bool, error) {
	return true, nil
}

// ctxWithBearer attaches an "authorization: Bearer <token>" header to the
// OUTGOING context so the server-side AuthUnaryInterceptor (when installed)
// extracts it and runs the validator. The token value is irrelevant — the stub
// validator ignores it and returns fixed claims — but it must be present, since
// extractBearerToken rejects a missing header before the validator runs.
func ctxWithBearer(t *testing.T) context.Context {
	t.Helper()
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer test-token")
}

// requireCode asserts that err carries the expected gRPC status code.
func requireCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with code %s, got nil", want)
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected a gRPC status error, got %T: %v", err, err)
	}
	if st.Code() != want {
		t.Fatalf("expected code %s, got %s (message: %q)", want, st.Code(), st.Message())
	}
}

// assertNoLeak fails if the message contains any substring that would indicate
// an internal/SQL/secret/PII leak. This is the (d) guarantee, checked uniformly.
//
// NOTE on the word "password": a user-facing login error legitimately reads
// "invalid email or password" — that is NOT a leak (it names no secret VALUE).
// What must never appear is HASH material or driver/storage internals. So we ban
// the bcrypt hash marker ("$2a$") and the literal word "bcrypt", not the generic
// word "password".
func assertNoLeak(t *testing.T, msg string) {
	t.Helper()
	lower := strings.ToLower(msg)
	for _, banned := range []string{
		"repository:", // storage-layer sentinel text
		"sql",         // SQL fragments
		"pgx", "pq:",  // driver internals
		"$2a$",      // bcrypt hash prefix
		"bcrypt",    // never reference the hashing scheme
		"sha256",    // never reference key-hash material
		"goroutine", // stack-trace leak
		"connection string",
		"panic",
	} {
		if strings.Contains(lower, banned) {
			t.Fatalf("error message leaks internal detail (%q): %q", banned, msg)
		}
	}
}

// ============================================================================
// Login
// ============================================================================

func TestLogin_HappyPath(t *testing.T) {
	mock := &mockAuthService{
		loginFn: func(ctx context.Context, email, password string) (string, error) {
			// Assert the handler forwarded the exact request fields to the domain.
			if email != "ada@forgepoint.dev" || password != "s3cret-pass" {
				t.Errorf("domain received wrong creds: email=%q password=%q", email, password)
			}
			return "signed.jwt.token", nil
		},
	}
	client := newTestClient(t, mock)

	resp, err := client.Login(context.Background(), &authv1.LoginRequest{
		Email:    "ada@forgepoint.dev",
		Password: "s3cret-pass",
	})
	if err != nil {
		t.Fatalf("Login returned error: %v", err)
	}
	if resp.GetAccessToken() != "signed.jwt.token" {
		t.Fatalf("access_token = %q, want %q", resp.GetAccessToken(), "signed.jwt.token")
	}
	if mock.loginCalls != 1 {
		t.Fatalf("expected exactly 1 domain Login call, got %d", mock.loginCalls)
	}
}

func TestLogin_Validation(t *testing.T) {
	// Both fields missing -> InvalidArgument, and the domain is NEVER called.
	cases := []struct {
		name string
		req  *authv1.LoginRequest
	}{
		{"empty email", &authv1.LoginRequest{Password: "x"}},
		{"empty password", &authv1.LoginRequest{Email: "a@b.dev"}},
		{"both empty", &authv1.LoginRequest{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockAuthService{
				loginFn: func(ctx context.Context, email, password string) (string, error) {
					t.Fatal("domain Login must NOT be called for invalid input")
					return "", nil
				},
			}
			client := newTestClient(t, mock)
			_, err := client.Login(context.Background(), tc.req)
			requireCode(t, err, codes.InvalidArgument)
			if mock.loginCalls != 0 {
				t.Fatalf("domain Login was called %d times; want 0", mock.loginCalls)
			}
		})
	}
}

func TestLogin_InvalidCredentials_MapsUnauthenticated_NoLeak(t *testing.T) {
	mock := &mockAuthService{
		loginFn: func(ctx context.Context, email, password string) (string, error) {
			return "", domain.ErrInvalidCredentials
		},
	}
	client := newTestClient(t, mock)

	_, err := client.Login(context.Background(), &authv1.LoginRequest{
		Email: "ghost@nowhere.dev", Password: "wrong",
	})
	requireCode(t, err, codes.Unauthenticated)

	st, _ := status.FromError(err)
	// Anti-enumeration: the message must be generic and must NOT reveal whether
	// the email existed or the password was wrong.
	if strings.Contains(strings.ToLower(st.Message()), "email") && strings.Contains(strings.ToLower(st.Message()), "not found") {
		t.Fatalf("login error reveals account existence: %q", st.Message())
	}
	assertNoLeak(t, st.Message())
}

func TestLogin_InternalError_Sanitized(t *testing.T) {
	// An UNRECOGNIZED error (simulating a DB failure wrapping SQL text) must map
	// to Internal with a sanitized message — the raw text must NOT reach the client.
	mock := &mockAuthService{
		loginFn: func(ctx context.Context, email, password string) (string, error) {
			return "", errorsNew("pq: connection refused to host db-primary:5432 password=topsecret")
		},
	}
	client := newTestClient(t, mock)

	_, err := client.Login(context.Background(), &authv1.LoginRequest{Email: "a@b.dev", Password: "x"})
	requireCode(t, err, codes.Internal)
	st, _ := status.FromError(err)
	assertNoLeak(t, st.Message())
	if strings.Contains(st.Message(), "db-primary") || strings.Contains(st.Message(), "topsecret") {
		t.Fatalf("internal error leaked infra detail: %q", st.Message())
	}
}

// ============================================================================
// CreateUser
// ============================================================================

func TestCreateUser_HappyPath_ConvertsAndNeverLeaksHash(t *testing.T) {
	created := domain.User{
		ID:           "user-uuid-1",
		Email:        "grace@forgepoint.dev",
		Name:         "Grace",
		Team:         "ml-platform",
		Role:         domain.Role{Name: "viewer"},
		PasswordHash: "$2a$12$SUPERSECRETBCRYPTHASH", // MUST NOT appear in the response
		Active:       true,
		CreatedAt:    time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
	}
	mock := &mockAuthService{
		createUserFn: func(ctx context.Context, in domain.CreateUserInput) (domain.User, error) {
			return created, nil
		},
		// CreateUser is admin-gated; grant the gate so the conversion path runs.
		checkPermFn: allowAll,
	}
	// Admin caller: claims present + CheckPermission allows (see allowAll).
	client := newTestClient(t, mock, claimsInjector(adminClaims))

	resp, err := client.CreateUser(ctxWithBearer(t), &authv1.CreateUserRequest{
		Email:    "grace@forgepoint.dev",
		Name:     "Grace",
		Password: "plaintext-pw",
		Team:     "ml-platform",
	})
	if err != nil {
		t.Fatalf("CreateUser returned error: %v", err)
	}

	// (a) conversion: the handler forwarded request fields into the domain input.
	in := mock.lastCreateUserInput
	if in.Email != "grace@forgepoint.dev" || in.Name != "Grace" || in.Password != "plaintext-pw" || in.Team != "ml-platform" {
		t.Fatalf("domain input mismatch: %+v", in)
	}

	// (a) conversion: domain.User -> proto User, role reduced to its name.
	u := resp.GetUser()
	if u.GetId() != "user-uuid-1" || u.GetEmail() != "grace@forgepoint.dev" || u.GetRole() != "viewer" {
		t.Fatalf("proto user mismatch: %+v", u)
	}
	if !u.GetCreatedAt().AsTime().Equal(created.CreatedAt) {
		t.Fatalf("created_at not converted: got %v want %v", u.GetCreatedAt().AsTime(), created.CreatedAt)
	}

	// (d) NO LEAK: the bcrypt hash must appear nowhere in the serialized response.
	if strings.Contains(u.String(), "BCRYPT") || strings.Contains(u.String(), "$2a$") {
		t.Fatalf("PasswordHash leaked into proto response: %q", u.String())
	}
}

func TestCreateUser_Validation(t *testing.T) {
	cases := []struct {
		name string
		req  *authv1.CreateUserRequest
	}{
		{"missing email", &authv1.CreateUserRequest{Password: "p", Team: "t"}},
		{"missing password", &authv1.CreateUserRequest{Email: "a@b.dev", Team: "t"}},
		{"missing team", &authv1.CreateUserRequest{Email: "a@b.dev", Password: "p"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockAuthService{
				createUserFn: func(ctx context.Context, in domain.CreateUserInput) (domain.User, error) {
					t.Fatal("domain CreateUser must NOT be called for invalid input")
					return domain.User{}, nil
				},
				// Caller is an admin: the request reaches validation (and is rejected
				// there). This isolates the validation behavior from the authz gate.
				checkPermFn: allowAll,
			}
			client := newTestClient(t, mock, claimsInjector(adminClaims))
			_, err := client.CreateUser(ctxWithBearer(t), tc.req)
			requireCode(t, err, codes.InvalidArgument)
			if mock.createUserCalls != 0 {
				t.Fatalf("domain CreateUser called %d times; want 0", mock.createUserCalls)
			}
		})
	}
}

func TestCreateUser_ErrorMapping(t *testing.T) {
	cases := []struct {
		name     string
		domErr   error
		wantCode codes.Code
	}{
		{"email exists", domain.ErrEmailAlreadyExists, codes.AlreadyExists},
		{"validation", wrap(domain.ErrValidation, "email must contain @"), codes.InvalidArgument},
		{"unknown -> internal", errorsNew("repository: deadlock detected in users table"), codes.Internal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockAuthService{
				createUserFn: func(ctx context.Context, in domain.CreateUserInput) (domain.User, error) {
					return domain.User{}, tc.domErr
				},
				// Admin caller so the request reaches the domain and we test the
				// domain-error -> gRPC-code mapping, not the authz gate.
				checkPermFn: allowAll,
			}
			client := newTestClient(t, mock, claimsInjector(adminClaims))
			_, err := client.CreateUser(ctxWithBearer(t), &authv1.CreateUserRequest{
				Email: "a@b.dev", Password: "p", Team: "t",
			})
			requireCode(t, err, tc.wantCode)
			st, _ := status.FromError(err)
			assertNoLeak(t, st.Message())
		})
	}
}

// ============================================================================
// CreateAPIKey — server-authoritative ownership from claims
// ============================================================================

func TestCreateAPIKey_HappyPath_OwnerFromClaims(t *testing.T) {
	expiry := time.Date(2027, 6, 1, 0, 0, 0, 0, time.UTC)
	mock := &mockAuthService{
		createAPIKeyFn: func(ctx context.Context, userID string, scopes []string, expiresAt *time.Time) (domain.APIKey, string, error) {
			return domain.APIKey{
				ID:        "key-uuid-1",
				UserID:    userID,
				KeyHash:   "SHA256-SECRET-HASH", // MUST NOT leak into response
				KeyPrefix: "fp_a1b2c",
				Scopes:    scopes,
				ExpiresAt: expiresAt,
				CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			}, "fp_a1b2c_FULLRAWKEYVALUE", nil
		},
	}
	// Install the real auth interceptor so claims are present (caller = self).
	client := newTestClient(t, mock, claimsInjector(&grpcutil.Claims{UserID: "caller-uuid"}))

	resp, err := client.CreateAPIKey(ctxWithBearer(t), &authv1.CreateAPIKeyRequest{
		// user_id omitted on purpose: owner must default to the caller's claims.
		Scopes:    []string{"models:read"},
		ExpiresAt: timestamppbNew(expiry),
	})
	if err != nil {
		t.Fatalf("CreateAPIKey returned error: %v", err)
	}

	// SERVER-AUTHORITATIVE: the domain must have been called with the CALLER's id
	// (from claims), not a client-supplied one (none was sent).
	if mock.lastAPIKeyOwnerID != "caller-uuid" {
		t.Fatalf("owner id = %q; want caller-uuid (from claims)", mock.lastAPIKeyOwnerID)
	}
	if len(mock.lastAPIKeyScopes) != 1 || mock.lastAPIKeyScopes[0] != "models:read" {
		t.Fatalf("scopes not forwarded: %v", mock.lastAPIKeyScopes)
	}
	if mock.lastAPIKeyExpiresAt == nil || !mock.lastAPIKeyExpiresAt.Equal(expiry) {
		t.Fatalf("expires_at not converted: %v", mock.lastAPIKeyExpiresAt)
	}

	// Response: raw key returned once; metadata carries prefix, not hash.
	if resp.GetRawKey() != "fp_a1b2c_FULLRAWKEYVALUE" {
		t.Fatalf("raw_key = %q", resp.GetRawKey())
	}
	if resp.GetApiKey().GetKeyPrefix() != "fp_a1b2c" {
		t.Fatalf("key_prefix = %q", resp.GetApiKey().GetKeyPrefix())
	}
	// (d) NO LEAK: the SHA-256 hash must not be anywhere in the api_key message.
	if strings.Contains(resp.GetApiKey().String(), "SHA256") || strings.Contains(resp.GetApiKey().String(), "HASH") {
		t.Fatalf("KeyHash leaked into proto response: %q", resp.GetApiKey().String())
	}
}

func TestCreateAPIKey_NoClaims_Unauthenticated(t *testing.T) {
	// No auth interceptor installed -> no claims in context -> the handler must
	// fail closed with Unauthenticated and never reach the domain.
	mock := &mockAuthService{
		createAPIKeyFn: func(ctx context.Context, userID string, scopes []string, expiresAt *time.Time) (domain.APIKey, string, error) {
			t.Fatal("domain CreateAPIKey must NOT be called without claims")
			return domain.APIKey{}, "", nil
		},
	}
	client := newTestClient(t, mock) // NOTE: no claimsInjector

	_, err := client.CreateAPIKey(context.Background(), &authv1.CreateAPIKeyRequest{Scopes: []string{"models:read"}})
	requireCode(t, err, codes.Unauthenticated)
	if mock.createAPIKeyCalls != 0 {
		t.Fatalf("domain CreateAPIKey called %d times without claims; want 0", mock.createAPIKeyCalls)
	}
}

func TestCreateAPIKey_ErrorMapping(t *testing.T) {
	mock := &mockAuthService{
		createAPIKeyFn: func(ctx context.Context, userID string, scopes []string, expiresAt *time.Time) (domain.APIKey, string, error) {
			return domain.APIKey{}, "", errorsNew("repository: insert into api_keys failed: unique violation")
		},
	}
	client := newTestClient(t, mock, claimsInjector(&grpcutil.Claims{UserID: "caller-uuid"}))

	_, err := client.CreateAPIKey(ctxWithBearer(t), &authv1.CreateAPIKeyRequest{Scopes: []string{"models:read"}})
	requireCode(t, err, codes.Internal)
	st, _ := status.FromError(err)
	assertNoLeak(t, st.Message())
}

// ============================================================================
// ValidateToken
// ============================================================================

func TestValidateToken_HappyPath_ConvertsClaims(t *testing.T) {
	iat := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	exp := iat.Add(24 * time.Hour)
	mock := &mockAuthService{
		validateTokenFn: func(ctx context.Context, token string) (domain.TokenClaims, error) {
			if token != "the-token" {
				t.Errorf("domain received token %q, want the-token", token)
			}
			return domain.TokenClaims{
				UserID:    "u1",
				Email:     "a@b.dev",
				Team:      "ml",
				Role:      "engineer",
				Scopes:    []string{"models:read", "models:write"},
				IssuedAt:  iat,
				ExpiresAt: exp,
				TokenKind: domain.TokenKindJWT,
			}, nil
		},
	}
	client := newTestClient(t, mock)

	resp, err := client.ValidateToken(context.Background(), &authv1.ValidateTokenRequest{Token: "the-token"})
	if err != nil {
		t.Fatalf("ValidateToken error: %v", err)
	}
	c := resp.GetClaims()
	if c.GetUserId() != "u1" || c.GetEmail() != "a@b.dev" || c.GetTeam() != "ml" || c.GetRole() != "engineer" {
		t.Fatalf("claims mismatch: %+v", c)
	}
	if len(c.GetScopes()) != 2 {
		t.Fatalf("scopes not converted: %v", c.GetScopes())
	}
	if !c.GetExp().AsTime().Equal(exp) || !c.GetIat().AsTime().Equal(iat) {
		t.Fatalf("timestamps not converted: exp=%v iat=%v", c.GetExp().AsTime(), c.GetIat().AsTime())
	}
}

func TestValidateToken_Validation(t *testing.T) {
	mock := &mockAuthService{
		validateTokenFn: func(ctx context.Context, token string) (domain.TokenClaims, error) {
			t.Fatal("domain ValidateToken must NOT be called for empty token")
			return domain.TokenClaims{}, nil
		},
	}
	client := newTestClient(t, mock)
	_, err := client.ValidateToken(context.Background(), &authv1.ValidateTokenRequest{Token: ""})
	requireCode(t, err, codes.InvalidArgument)
	if mock.validateCalls != 0 {
		t.Fatalf("domain ValidateToken called %d times; want 0", mock.validateCalls)
	}
}

func TestValidateToken_InvalidToken_Unauthenticated_Opaque(t *testing.T) {
	mock := &mockAuthService{
		validateTokenFn: func(ctx context.Context, token string) (domain.TokenClaims, error) {
			return domain.TokenClaims{}, domain.ErrInvalidToken
		},
	}
	client := newTestClient(t, mock)
	_, err := client.ValidateToken(context.Background(), &authv1.ValidateTokenRequest{Token: "expired-or-forged"})
	requireCode(t, err, codes.Unauthenticated)
	st, _ := status.FromError(err)
	// Opaque: the message must not say WHY (expired vs forged vs revoked).
	low := strings.ToLower(st.Message())
	for _, banned := range []string{"expired", "revoked", "signature", "forged"} {
		if strings.Contains(low, banned) {
			t.Fatalf("token error reveals failure reason %q: %q", banned, st.Message())
		}
	}
	assertNoLeak(t, st.Message())
}

// ============================================================================
// CheckPermission — (bool, error) contract
// ============================================================================

func TestCheckPermission_Allowed(t *testing.T) {
	mock := &mockAuthService{
		checkPermFn: func(ctx context.Context, userID, resource, action string) (bool, error) {
			return true, nil
		},
	}
	client := newTestClient(t, mock)
	resp, err := client.CheckPermission(context.Background(), &authv1.CheckPermissionRequest{
		UserId: "u1", Resource: "models", Action: "write",
	})
	if err != nil {
		t.Fatalf("CheckPermission error: %v", err)
	}
	if !resp.GetAllowed() {
		t.Fatalf("allowed = false; want true")
	}
	// reason must be populated (proto contract) and reference the coordinate.
	if resp.GetReason() == "" || !strings.Contains(resp.GetReason(), "models") {
		t.Fatalf("reason not populated correctly: %q", resp.GetReason())
	}
	// Conversion: handler forwarded the exact triple to the domain.
	if mock.lastCheckPermUserID != "u1" || mock.lastCheckPermResource != "models" || mock.lastCheckPermAction != "write" {
		t.Fatalf("domain received wrong triple: user=%q res=%q act=%q",
			mock.lastCheckPermUserID, mock.lastCheckPermResource, mock.lastCheckPermAction)
	}
}

func TestCheckPermission_Denied_IsNotAnError(t *testing.T) {
	// A DENY must come back as allowed=false with NO gRPC error — the question
	// was answered successfully ("no").
	mock := &mockAuthService{
		checkPermFn: func(ctx context.Context, userID, resource, action string) (bool, error) {
			return false, nil
		},
	}
	client := newTestClient(t, mock)
	resp, err := client.CheckPermission(context.Background(), &authv1.CheckPermissionRequest{
		UserId: "u1", Resource: "billing", Action: "admin",
	})
	if err != nil {
		t.Fatalf("a deny must not be a gRPC error, got: %v", err)
	}
	if resp.GetAllowed() {
		t.Fatalf("allowed = true; want false")
	}
	if !strings.HasPrefix(resp.GetReason(), "denied:") {
		t.Fatalf("deny reason malformed: %q", resp.GetReason())
	}
}

func TestCheckPermission_InfraError_IsInternal(t *testing.T) {
	// A non-nil domain error means "couldn't decide" -> Internal (so the calling
	// interceptor fails closed), NOT a deny.
	mock := &mockAuthService{
		checkPermFn: func(ctx context.Context, userID, resource, action string) (bool, error) {
			return false, errorsNew("repository: timeout querying user_roles")
		},
	}
	client := newTestClient(t, mock)
	_, err := client.CheckPermission(context.Background(), &authv1.CheckPermissionRequest{
		UserId: "u1", Resource: "models", Action: "read",
	})
	requireCode(t, err, codes.Internal)
	st, _ := status.FromError(err)
	assertNoLeak(t, st.Message())
}

func TestCheckPermission_Validation(t *testing.T) {
	cases := []struct {
		name string
		req  *authv1.CheckPermissionRequest
	}{
		{"missing user", &authv1.CheckPermissionRequest{Resource: "models", Action: "read"}},
		{"missing resource", &authv1.CheckPermissionRequest{UserId: "u1", Action: "read"}},
		{"missing action", &authv1.CheckPermissionRequest{UserId: "u1", Resource: "models"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockAuthService{
				checkPermFn: func(ctx context.Context, userID, resource, action string) (bool, error) {
					t.Fatal("domain CheckPermission must NOT be called for invalid input")
					return false, nil
				},
			}
			client := newTestClient(t, mock)
			_, err := client.CheckPermission(context.Background(), tc.req)
			requireCode(t, err, codes.InvalidArgument)
			if mock.checkPermCalls != 0 {
				t.Fatalf("domain CheckPermission called %d times; want 0", mock.checkPermCalls)
			}
		})
	}
}

// ============================================================================
// AssignRole
// ============================================================================

func TestAssignRole_HappyPath(t *testing.T) {
	mock := &mockAuthService{
		assignRoleFn: func(ctx context.Context, userID, roleName string) error {
			if userID != "u1" || roleName != "admin" {
				t.Errorf("domain received user=%q role=%q", userID, roleName)
			}
			return nil
		},
		// AssignRole is admin-gated; grant the gate so the happy path runs.
		checkPermFn: allowAll,
	}
	client := newTestClient(t, mock, claimsInjector(adminClaims))
	_, err := client.AssignRole(ctxWithBearer(t), &authv1.AssignRoleRequest{UserId: "u1", RoleName: "admin"})
	if err != nil {
		t.Fatalf("AssignRole error: %v", err)
	}
	if mock.assignRoleCalls != 1 {
		t.Fatalf("expected 1 domain AssignRole call, got %d", mock.assignRoleCalls)
	}
}

func TestAssignRole_ErrorMapping(t *testing.T) {
	cases := []struct {
		name     string
		domErr   error
		wantCode codes.Code
	}{
		{"user not found", domain.ErrUserNotFound, codes.NotFound},
		{"role not found", domain.ErrRoleNotFound, codes.NotFound},
		{"unknown -> internal", errorsNew("repository: tx rollback failed"), codes.Internal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockAuthService{
				assignRoleFn: func(ctx context.Context, userID, roleName string) error {
					return tc.domErr
				},
				// Admin caller so we reach the domain and test error mapping.
				checkPermFn: allowAll,
			}
			client := newTestClient(t, mock, claimsInjector(adminClaims))
			_, err := client.AssignRole(ctxWithBearer(t), &authv1.AssignRoleRequest{UserId: "u1", RoleName: "admin"})
			requireCode(t, err, tc.wantCode)
			st, _ := status.FromError(err)
			assertNoLeak(t, st.Message())
		})
	}
}

func TestAssignRole_Validation(t *testing.T) {
	cases := []struct {
		name string
		req  *authv1.AssignRoleRequest
	}{
		{"missing user", &authv1.AssignRoleRequest{RoleName: "admin"}},
		{"missing role", &authv1.AssignRoleRequest{UserId: "u1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockAuthService{
				assignRoleFn: func(ctx context.Context, userID, roleName string) error {
					t.Fatal("domain AssignRole must NOT be called for invalid input")
					return nil
				},
				// Admin caller: the request passes the authz gate and is rejected at
				// validation, isolating validation behavior from authorization.
				checkPermFn: allowAll,
			}
			client := newTestClient(t, mock, claimsInjector(adminClaims))
			_, err := client.AssignRole(ctxWithBearer(t), tc.req)
			requireCode(t, err, codes.InvalidArgument)
			if mock.assignRoleCalls != 0 {
				t.Fatalf("domain AssignRole called %d times; want 0", mock.assignRoleCalls)
			}
		})
	}
}

// ============================================================================
// ListUsers / RevokeAPIKey — wired but service method not present yet
// ============================================================================

func TestListUsers_NegativePageSize_InvalidArgument(t *testing.T) {
	// Validation runs even though the RPC returns Unimplemented afterwards: a
	// negative page size is a client error and must be reported as such. The
	// caller is an admin so the request passes the authz gate and reaches the
	// pagination validation (which is what this test exercises).
	mock := &mockAuthService{checkPermFn: allowAll}
	client := newTestClient(t, mock, claimsInjector(adminClaims))
	_, err := client.ListUsers(ctxWithBearer(t), &authv1.ListUsersRequest{
		Pagination: &commonv1.PaginationRequest{PageSize: -5},
	})
	requireCode(t, err, codes.InvalidArgument)
}

func TestListUsers_Unimplemented(t *testing.T) {
	// Admin caller -> passes the authz gate -> reaches the (honest) Unimplemented.
	mock := &mockAuthService{checkPermFn: allowAll}
	client := newTestClient(t, mock, claimsInjector(adminClaims))
	_, err := client.ListUsers(ctxWithBearer(t), &authv1.ListUsersRequest{})
	requireCode(t, err, codes.Unimplemented)
}

func TestRevokeAPIKey_Validation(t *testing.T) {
	client := newTestClient(t, &mockAuthService{})
	_, err := client.RevokeAPIKey(context.Background(), &authv1.RevokeAPIKeyRequest{KeyId: ""})
	requireCode(t, err, codes.InvalidArgument)
}

func TestRevokeAPIKey_Unimplemented(t *testing.T) {
	client := newTestClient(t, &mockAuthService{})
	_, err := client.RevokeAPIKey(context.Background(), &authv1.RevokeAPIKeyRequest{KeyId: "key-uuid"})
	requireCode(t, err, codes.Unimplemented)
}

// ============================================================================
// NIL-SVC GUARD — a binary wired before the domain service lands
// ============================================================================

func TestNilService_ReturnsUnimplemented(t *testing.T) {
	// Handler with svc==nil: every unary RPC must return Unimplemented instead of
	// panicking on a nil-interface method call. We spot-check a representative RPC.
	client := newTestClient(t, nil)
	_, err := client.Login(context.Background(), &authv1.LoginRequest{Email: "a@b.dev", Password: "x"})
	requireCode(t, err, codes.Unimplemented)
}

// ============================================================================
// AUTHORIZATION — admin gate on CreateUser / ListUsers / AssignRole
// ============================================================================
//
// These tests PROVE the fix for the "missing per-RPC authorization" finding:
// the admin-gated RPCs now enforce admin in-handler (via CheckPermission)
// because no authz interceptor exists in the platform. Each RPC must:
//
//	(1) reject an UNAUTHENTICATED caller (no claims) with Unauthenticated and
//	    NEVER touch the domain's write method;
//	(2) reject a non-admin caller (CheckPermission -> false) with
//	    PermissionDenied and NEVER touch the domain's write method;
//	(3) fail CLOSED to Internal when the authz check itself errors (DB down),
//	    NEVER proceeding to the write.
//
// We assert the negative space (the write fn is never called) by leaving it nil:
// if the handler reached it, the nil call would panic -> recovery interceptor ->
// Internal, which would fail the Unauthenticated/PermissionDenied expectation.

// adminGatedRPC describes one admin-only RPC under test: how to invoke it and
// how to read its domain-write call count from the mock. Table-driving these
// keeps the three identical authz contracts in one place (CreateUser, ListUsers,
// AssignRole all share the requireAdmin gate).
type adminGatedRPC struct {
	name string
	// call invokes the RPC against the client with a valid (non-empty) request,
	// so the ONLY thing that can reject it is the authz gate (not validation).
	call func(ctx context.Context, c authv1.AuthServiceClient) error
	// writeCalls returns how many times the domain's write/work method was hit.
	writeCalls func(m *mockAuthService) int
}

func adminGatedRPCs() []adminGatedRPC {
	return []adminGatedRPC{
		{
			name: "CreateUser",
			call: func(ctx context.Context, c authv1.AuthServiceClient) error {
				_, err := c.CreateUser(ctx, &authv1.CreateUserRequest{
					Email: "a@b.dev", Password: "p", Team: "t",
				})
				return err
			},
			writeCalls: func(m *mockAuthService) int { return m.createUserCalls },
		},
		{
			name: "AssignRole",
			call: func(ctx context.Context, c authv1.AuthServiceClient) error {
				_, err := c.AssignRole(ctx, &authv1.AssignRoleRequest{UserId: "u1", RoleName: "admin"})
				return err
			},
			writeCalls: func(m *mockAuthService) int { return m.assignRoleCalls },
		},
		{
			name: "ListUsers",
			call: func(ctx context.Context, c authv1.AuthServiceClient) error {
				_, err := c.ListUsers(ctx, &authv1.ListUsersRequest{})
				return err
			},
			// ListUsers has no domain write today (returns Unimplemented after the
			// gate); there is no write method to count, so report 0 always. The
			// authz behavior is still fully asserted via the returned status code.
			writeCalls: func(m *mockAuthService) int { return 0 },
		},
	}
}

func TestAdminRPCs_NoClaims_Unauthenticated(t *testing.T) {
	for _, rpc := range adminGatedRPCs() {
		t.Run(rpc.name, func(t *testing.T) {
			// No claimsInjector -> no claims in context. The write fn is nil: if the
			// handler skipped the gate and called it, the panic would surface as
			// Internal, not Unauthenticated, failing this test.
			mock := &mockAuthService{
				checkPermFn: func(ctx context.Context, userID, resource, action string) (bool, error) {
					t.Fatal("CheckPermission must NOT be called for an unauthenticated caller")
					return false, nil
				},
			}
			client := newTestClient(t, mock) // no claims
			err := rpc.call(context.Background(), client)
			requireCode(t, err, codes.Unauthenticated)
			if got := rpc.writeCalls(mock); got != 0 {
				t.Fatalf("domain write reached %d times without auth; want 0", got)
			}
			if mock.checkPermCalls != 0 {
				t.Fatalf("CheckPermission called %d times for anon caller; want 0", mock.checkPermCalls)
			}
		})
	}
}

func TestAdminRPCs_NonAdmin_PermissionDenied(t *testing.T) {
	for _, rpc := range adminGatedRPCs() {
		t.Run(rpc.name, func(t *testing.T) {
			// Authenticated but NOT admin: CheckPermission decisively returns false.
			// The handler must return PermissionDenied and never reach the write.
			mock := &mockAuthService{
				checkPermFn: func(ctx context.Context, userID, resource, action string) (bool, error) {
					// Sanity: the gate asks the right question on behalf of the caller.
					if userID != "viewer-uuid" || resource != "users" || action != "admin" {
						t.Errorf("authz asked wrong question: user=%q res=%q act=%q", userID, resource, action)
					}
					return false, nil
				},
			}
			client := newTestClient(t, mock, claimsInjector(&grpcutil.Claims{UserID: "viewer-uuid", Role: "viewer"}))
			err := rpc.call(ctxWithBearer(t), client)
			requireCode(t, err, codes.PermissionDenied)
			if got := rpc.writeCalls(mock); got != 0 {
				t.Fatalf("domain write reached %d times for non-admin; want 0", got)
			}
			st, _ := status.FromError(err)
			assertNoLeak(t, st.Message())
		})
	}
}

func TestAdminRPCs_AuthzInfraError_FailsClosedInternal(t *testing.T) {
	for _, rpc := range adminGatedRPCs() {
		t.Run(rpc.name, func(t *testing.T) {
			// The authz check itself errors ("couldn't decide", e.g. DB down). The
			// handler must FAIL CLOSED -> Internal (sanitized), never proceeding.
			mock := &mockAuthService{
				checkPermFn: func(ctx context.Context, userID, resource, action string) (bool, error) {
					return false, errorsNew("repository: timeout querying user_roles")
				},
			}
			client := newTestClient(t, mock, claimsInjector(adminClaims))
			err := rpc.call(ctxWithBearer(t), client)
			requireCode(t, err, codes.Internal)
			if got := rpc.writeCalls(mock); got != 0 {
				t.Fatalf("domain write reached %d times despite authz error; want 0", got)
			}
			st, _ := status.FromError(err)
			assertNoLeak(t, st.Message())
		})
	}
}

// ============================================================================
// CreateAPIKey — cross-user privilege-escalation gate (the High finding)
// ============================================================================
//
// These tests PROVE the fix for the impersonation/mass-assignment finding:
// a client-supplied user_id naming ANOTHER user is no longer honored on trust.
// It is permitted ONLY when the caller passes an explicit admin check; otherwise
// it is denied and the domain is never asked to mint the key.

func TestCreateAPIKey_CrossUser_AdminAllowed_OwnerIsTarget(t *testing.T) {
	// An admin caller asks to mint a key for ANOTHER user. The handler must run
	// the admin check, pass, and set the owner to the REQUESTED user_id.
	mock := &mockAuthService{
		createAPIKeyFn: func(ctx context.Context, userID string, scopes []string, expiresAt *time.Time) (domain.APIKey, string, error) {
			return domain.APIKey{ID: "key-uuid", UserID: userID, KeyPrefix: "fp_x"}, "fp_x_RAW", nil
		},
		checkPermFn: func(ctx context.Context, userID, resource, action string) (bool, error) {
			// The gate must ask whether the CALLER (not the target) has api-keys:admin.
			if userID != "admin-uuid" || resource != "api-keys" || action != "admin" {
				t.Errorf("authz asked wrong question: user=%q res=%q act=%q", userID, resource, action)
			}
			return true, nil
		},
	}
	client := newTestClient(t, mock, claimsInjector(adminClaims))

	resp, err := client.CreateAPIKey(ctxWithBearer(t), &authv1.CreateAPIKeyRequest{
		UserId: "victim-uuid", // a DIFFERENT user than the caller
		Scopes: []string{"models:read"},
	})
	if err != nil {
		t.Fatalf("admin cross-user CreateAPIKey returned error: %v", err)
	}
	// SERVER-AUTHORITATIVE, but admin-authorized: owner is the requested target.
	if mock.lastAPIKeyOwnerID != "victim-uuid" {
		t.Fatalf("owner id = %q; want victim-uuid (admin-authorized cross-user)", mock.lastAPIKeyOwnerID)
	}
	if mock.checkPermCalls != 1 {
		t.Fatalf("expected exactly 1 admin check for cross-user create, got %d", mock.checkPermCalls)
	}
	if resp.GetRawKey() != "fp_x_RAW" {
		t.Fatalf("raw_key = %q", resp.GetRawKey())
	}
}

func TestCreateAPIKey_CrossUser_NonAdmin_PermissionDenied_NoMint(t *testing.T) {
	// THE BUG, now closed: a non-admin caller supplies a foreign user_id. The
	// handler MUST deny and MUST NOT mint a key owned by the other user.
	mock := &mockAuthService{
		createAPIKeyFn: func(ctx context.Context, userID string, scopes []string, expiresAt *time.Time) (domain.APIKey, string, error) {
			t.Fatal("domain CreateAPIKey must NOT be called when cross-user authz is denied")
			return domain.APIKey{}, "", nil
		},
		checkPermFn: func(ctx context.Context, userID, resource, action string) (bool, error) {
			return false, nil // decisively: caller is not an api-keys admin
		},
	}
	client := newTestClient(t, mock, claimsInjector(&grpcutil.Claims{UserID: "attacker-uuid"}))

	_, err := client.CreateAPIKey(ctxWithBearer(t), &authv1.CreateAPIKeyRequest{
		UserId: "victim-uuid", // impersonation attempt
		Scopes: []string{"models:*"},
	})
	requireCode(t, err, codes.PermissionDenied)
	if mock.createAPIKeyCalls != 0 {
		t.Fatalf("key was minted for another user despite denial (%d calls)", mock.createAPIKeyCalls)
	}
	st, _ := status.FromError(err)
	assertNoLeak(t, st.Message())
}

func TestCreateAPIKey_CrossUser_AuthzInfraError_FailsClosedInternal_NoMint(t *testing.T) {
	// The cross-user admin check errors out -> fail closed to Internal, no mint.
	mock := &mockAuthService{
		createAPIKeyFn: func(ctx context.Context, userID string, scopes []string, expiresAt *time.Time) (domain.APIKey, string, error) {
			t.Fatal("domain CreateAPIKey must NOT be called when authz couldn't be decided")
			return domain.APIKey{}, "", nil
		},
		checkPermFn: func(ctx context.Context, userID, resource, action string) (bool, error) {
			return false, errorsNew("repository: connection reset by peer")
		},
	}
	client := newTestClient(t, mock, claimsInjector(&grpcutil.Claims{UserID: "caller-uuid"}))

	_, err := client.CreateAPIKey(ctxWithBearer(t), &authv1.CreateAPIKeyRequest{
		UserId: "other-uuid",
		Scopes: []string{"models:read"},
	})
	requireCode(t, err, codes.Internal)
	if mock.createAPIKeyCalls != 0 {
		t.Fatalf("key minted despite undecidable authz (%d calls)", mock.createAPIKeyCalls)
	}
	st, _ := status.FromError(err)
	assertNoLeak(t, st.Message())
}

func TestCreateAPIKey_SelfService_OwnUserId_NoAdminCheck(t *testing.T) {
	// Self-service must NOT require admin even when the client explicitly names
	// its OWN user_id (user_id == caller). The handler must skip CheckPermission
	// entirely and mint a key owned by the caller.
	mock := &mockAuthService{
		createAPIKeyFn: func(ctx context.Context, userID string, scopes []string, expiresAt *time.Time) (domain.APIKey, string, error) {
			return domain.APIKey{ID: "key-uuid", UserID: userID, KeyPrefix: "fp_s"}, "fp_s_RAW", nil
		},
		checkPermFn: func(ctx context.Context, userID, resource, action string) (bool, error) {
			t.Fatal("CheckPermission must NOT be called for self-service key creation")
			return false, nil
		},
	}
	client := newTestClient(t, mock, claimsInjector(&grpcutil.Claims{UserID: "self-uuid"}))

	resp, err := client.CreateAPIKey(ctxWithBearer(t), &authv1.CreateAPIKeyRequest{
		UserId: "self-uuid", // explicitly self -> still self-service, no admin needed
		Scopes: []string{"models:read"},
	})
	if err != nil {
		t.Fatalf("self-service CreateAPIKey returned error: %v", err)
	}
	if mock.lastAPIKeyOwnerID != "self-uuid" {
		t.Fatalf("owner id = %q; want self-uuid", mock.lastAPIKeyOwnerID)
	}
	if mock.checkPermCalls != 0 {
		t.Fatalf("admin check ran for self-service create (%d calls); want 0", mock.checkPermCalls)
	}
	if resp.GetRawKey() != "fp_s_RAW" {
		t.Fatalf("raw_key = %q", resp.GetRawKey())
	}
}
