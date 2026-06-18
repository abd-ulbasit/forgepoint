// auth_handler_interceptor_test.go — COMPONENT tests that exercise the FULL
// production authentication path: real bufconn server + the REAL
// grpcutil.AuthUnaryInterceptor + the REAL authn.Validator (which verifies a
// real, signed JWT with the shared secret) + the handler's requireAdmin authZ
// gate. NO injected claims.
//
// ============================================================================
// WHY THIS FILE EXISTS (the bug it proves fixed)
// ============================================================================
//
// The pre-existing handler tests (auth_handler_test.go) install the auth
// interceptor with a STUB validator that returns fixed claims (claimsInjector).
// That proves the handler reads claims correctly, but it MASKS the production
// bug: main.go built the server with NO validator at all, so the interceptor was
// never added and claims were NEVER populated — every admin RPC was permanently
// Unauthenticated even with a valid admin JWT.
//
// These tests close that gap. They wire the server EXACTLY as main.go now does:
//
//	srv := grpcutil.NewServer(
//	    grpcutil.WithLogger(...),
//	    grpcutil.WithAuthValidator(authn.NewValidator(svc), authn.SkipMethods()...),
//	)
//
// and then drive a protected RPC (CreateUser) with REAL Bearer tokens:
//
//	(1) NO token            → Unauthenticated   (interceptor rejects pre-handler)
//	(2) GARBAGE token       → Unauthenticated   (validator: not a valid JWT)
//	(3) WRONG-SECRET token  → Unauthenticated   (signature verify fails)
//	(4) EXPIRED token       → Unauthenticated   (exp in the past)
//	(5) VALID ADMIN token   → SUCCESS            (claims set → CheckPermission admin)
//	(6) VALID NON-ADMIN tok → PermissionDenied   (claims set → CheckPermission deny)
//
// (1)-(4) prove AUTHENTICATION now runs in production. (5)-(6) prove the claims
// the interceptor injected reach requireAdmin and drive the authZ decision.
package handler

import (
	"context"
	"testing"
	"time"

	authv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/auth/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/auth/internal/authn"
	"github.com/abd-ulbasit/forgepoint/services/auth/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// testJWTSecret is a 32+ byte secret so domain.GenerateToken/ValidateToken do not
// reject it with ErrWeakSecret (the HS256 256-bit floor). It mirrors the shape of
// the FP_JWT_SECRET injected in the cluster.
var testJWTSecret = []byte("forgepoint-test-jwt-signing-secret-32b+")

// jwtBackedService is a domain-token stub whose ValidateToken does REAL JWT
// verification via domain.ValidateToken(token, secret). This exercises the
// genuine local-JWT crypto path (design D2) the validator wraps, without needing
// a Postgres-backed service: a token signed with the matching secret validates,
// anything else returns ErrInvalidToken — exactly like the real domain service's
// JWT branch. The remaining domain.AuthService methods are unused by the
// authentication path, so they panic if (unexpectedly) called.
//
// CheckPermission is the ONE other method the test needs: requireAdmin calls it
// to make the admin decision AFTER the interceptor has authenticated. We back it
// with a settable function so each test grants or denies admin.
type jwtBackedService struct {
	domain.AuthService // embedded: unimplemented methods panic if called
	checkPerm          func(ctx context.Context, userID, resource, action string) (bool, error)
	createUser         func(ctx context.Context, in domain.CreateUserInput) (domain.User, error)
}

func (s *jwtBackedService) ValidateToken(_ context.Context, token string) (domain.TokenClaims, error) {
	// REAL crypto: verify signature + expiry with the shared secret. A wrong
	// secret, a garbage string, or an expired token all fail here and collapse to
	// the opaque ErrInvalidToken — exactly the production domain behavior.
	claims, err := domain.ValidateToken(token, testJWTSecret)
	if err != nil {
		return domain.TokenClaims{}, domain.ErrInvalidToken
	}
	return *claims, nil
}

func (s *jwtBackedService) CheckPermission(ctx context.Context, userID, resource, action string) (bool, error) {
	return s.checkPerm(ctx, userID, resource, action)
}

func (s *jwtBackedService) CreateUser(ctx context.Context, in domain.CreateUserInput) (domain.User, error) {
	return s.createUser(ctx, in)
}

// newInterceptedClient wires the handler onto a bufconn server with the REAL auth
// interceptor + REAL validator + the production skip-list — i.e. the same chain
// main.go builds. This is the difference from newTestClient: there is no injected
// claims shortcut; a request authenticates by presenting a real Bearer token.
func newInterceptedClient(t *testing.T, svc domain.AuthService) authv1.AuthServiceClient {
	t.Helper()
	h := NewAuthHandler(svc)
	validator := authn.NewValidator(svc)
	conn := testutil.NewTestGRPCServer(t, func(s *grpc.Server) {
		authv1.RegisterAuthServiceServer(s, h)
	},
		// EXACTLY the production wiring: the auth interceptor backed by the local
		// token validator, skipping the public methods (Login/ValidateToken/
		// CheckPermission) and the health/reflection infra RPCs.
		grpc.ChainUnaryInterceptor(
			grpcutil.AuthUnaryInterceptor(validator, grpcutil.WithSkipMethods(authn.SkipMethods()...)),
		),
	)
	return authv1.NewAuthServiceClient(conn)
}

// mintToken signs a JWT for the given user/role with the given secret and ttl,
// the way domain Login does. A negative ttl yields an already-expired token.
func mintToken(t *testing.T, userID, role string, secret []byte, ttl time.Duration) string {
	t.Helper()
	tok, err := domain.GenerateToken(domain.TokenClaims{
		UserID:    userID,
		Email:     userID + "@forgepoint.local",
		Team:      "platform",
		Role:      role,
		TokenKind: domain.TokenKindJWT,
	}, secret, ttl)
	if err != nil {
		t.Fatalf("failed to mint test token: %v", err)
	}
	return tok
}

// bearer attaches "authorization: Bearer <token>" to the outgoing context so the
// server-side interceptor extracts and validates it — the real client behavior.
func bearer(token string) context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+token)
}

// createUserReq is a valid request so the ONLY thing that can reject it is the
// auth interceptor or the requireAdmin gate — never field validation.
func createUserReq() *authv1.CreateUserRequest {
	return &authv1.CreateUserRequest{Email: "new@forgepoint.local", Password: "p4ssword", Team: "platform"}
}

// denyAllPerms is a CheckPermission that must NOT be reached in the rejection
// tests; if the handler ever calls it the test learns the interceptor let an
// unauthenticated/garbage caller through.
func mustNotCheckPerm(t *testing.T) func(ctx context.Context, userID, resource, action string) (bool, error) {
	return func(_ context.Context, _, _, _ string) (bool, error) {
		t.Fatal("CheckPermission reached for a caller that should have failed authentication")
		return false, nil
	}
}

// ----------------------------------------------------------------------------
// AUTHENTICATION rejections (interceptor + validator), no claims injected.
// ----------------------------------------------------------------------------

func TestInterceptor_CreateUser_NoToken_Unauthenticated(t *testing.T) {
	svc := &jwtBackedService{checkPerm: mustNotCheckPerm(t)}
	client := newInterceptedClient(t, svc)

	// No authorization header at all → extractBearerToken rejects pre-handler.
	_, err := client.CreateUser(context.Background(), createUserReq())
	requireCode(t, err, codes.Unauthenticated)
}

func TestInterceptor_CreateUser_GarbageToken_Unauthenticated(t *testing.T) {
	svc := &jwtBackedService{checkPerm: mustNotCheckPerm(t)}
	client := newInterceptedClient(t, svc)

	// A syntactically-bogus token is not a valid JWT → validator returns
	// Unauthenticated → handler never runs.
	_, err := client.CreateUser(bearer("not-a-real-jwt"), createUserReq())
	requireCode(t, err, codes.Unauthenticated)
}

func TestInterceptor_CreateUser_WrongSecretToken_Unauthenticated(t *testing.T) {
	svc := &jwtBackedService{checkPerm: mustNotCheckPerm(t)}
	client := newInterceptedClient(t, svc)

	// A token signed with a DIFFERENT secret: structurally a JWT, but the HMAC
	// signature verify fails against testJWTSecret. This is the core local-verify
	// property (D2) — a forged/foreign-signed token is rejected.
	forged := mintToken(t, "attacker", "admin", []byte("a-totally-different-secret-of-32-bytes!"), 15*time.Minute)
	_, err := client.CreateUser(bearer(forged), createUserReq())
	requireCode(t, err, codes.Unauthenticated)
}

func TestInterceptor_CreateUser_ExpiredToken_Unauthenticated(t *testing.T) {
	svc := &jwtBackedService{checkPerm: mustNotCheckPerm(t)}
	client := newInterceptedClient(t, svc)

	// Correctly signed but expired (negative ttl → exp in the past).
	expired := mintToken(t, "admin", "admin", testJWTSecret, -1*time.Minute)
	_, err := client.CreateUser(bearer(expired), createUserReq())
	requireCode(t, err, codes.Unauthenticated)
}

// ----------------------------------------------------------------------------
// AUTHENTICATION success → claims reach requireAdmin → authZ decision.
// ----------------------------------------------------------------------------

func TestInterceptor_CreateUser_ValidAdminToken_Succeeds(t *testing.T) {
	var sawUserID string
	svc := &jwtBackedService{
		// requireAdmin asks CheckPermission(caller, "users", "admin"). The caller id
		// MUST be the one the interceptor extracted from the validated JWT — proving
		// the claims plumbing works end-to-end. We grant admin.
		checkPerm: func(_ context.Context, userID, resource, action string) (bool, error) {
			sawUserID = userID
			return resource == "users" && action == "admin", nil
		},
		createUser: func(_ context.Context, in domain.CreateUserInput) (domain.User, error) {
			return domain.User{ID: "created-uuid", Email: in.Email, Team: in.Team, Role: domain.Role{Name: "viewer"}}, nil
		},
	}
	client := newInterceptedClient(t, svc)

	token := mintToken(t, "admin-uuid", "admin", testJWTSecret, 15*time.Minute)
	resp, err := client.CreateUser(bearer(token), createUserReq())
	if err != nil {
		t.Fatalf("valid admin CreateUser must SUCCEED, got: %v", err)
	}
	if resp.GetUser().GetId() != "created-uuid" {
		t.Fatalf("unexpected created user: %+v", resp.GetUser())
	}
	// PROOF the interceptor injected the JWT's identity (not a stub): requireAdmin
	// asked the authz question on behalf of the token's subject.
	if sawUserID != "admin-uuid" {
		t.Fatalf("CheckPermission saw caller %q; want admin-uuid (from the validated JWT)", sawUserID)
	}
}

func TestInterceptor_CreateUser_ValidNonAdminToken_PermissionDenied(t *testing.T) {
	svc := &jwtBackedService{
		// Authenticated (valid JWT) but the authz gate decisively denies admin.
		checkPerm: func(_ context.Context, _, _, _ string) (bool, error) { return false, nil },
		createUser: func(_ context.Context, _ domain.CreateUserInput) (domain.User, error) {
			t.Fatal("CreateUser domain write must NOT run for a non-admin caller")
			return domain.User{}, nil
		},
	}
	client := newInterceptedClient(t, svc)

	// A perfectly valid token for a viewer: authentication PASSES (claims set),
	// authorization FAILS → PermissionDenied (not Unauthenticated).
	token := mintToken(t, "viewer-uuid", "viewer", testJWTSecret, 15*time.Minute)
	_, err := client.CreateUser(bearer(token), createUserReq())
	requireCode(t, err, codes.PermissionDenied)
}

// ----------------------------------------------------------------------------
// SKIP-LIST: public methods must remain callable with NO token.
// ----------------------------------------------------------------------------

func TestInterceptor_Login_IsPublic_NoTokenRequired(t *testing.T) {
	// Login is in the skip-list: it must run with no authorization header. We use
	// the existing mock (not the jwt-backed stub) since only Login is exercised.
	mock := &mockAuthService{
		loginFn: func(_ context.Context, _, _ string) (string, error) { return "signed.jwt", nil },
	}
	client := newInterceptedClient(t, mock)

	resp, err := client.Login(context.Background(), &authv1.LoginRequest{Email: "a@b.dev", Password: "x"})
	if err != nil {
		t.Fatalf("Login is public; it must not require a token, got: %v", err)
	}
	if resp.GetAccessToken() != "signed.jwt" {
		t.Fatalf("unexpected Login response: %+v", resp)
	}
}

func TestInterceptor_ValidateToken_IsPublic_NoTokenRequired(t *testing.T) {
	// ValidateToken is public: the token to check is in the request body, not the
	// header. It must run with no authorization metadata.
	mock := &mockAuthService{
		validateTokenFn: func(_ context.Context, _ string) (domain.TokenClaims, error) {
			return domain.TokenClaims{UserID: "u1", Role: "engineer", IssuedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}, nil
		},
	}
	client := newInterceptedClient(t, mock)

	resp, err := client.ValidateToken(context.Background(), &authv1.ValidateTokenRequest{Token: "some-token"})
	if err != nil {
		t.Fatalf("ValidateToken is public; it must not require a header token, got: %v", err)
	}
	if resp.GetClaims().GetUserId() != "u1" {
		t.Fatalf("unexpected ValidateToken response: %+v", resp.GetClaims())
	}
}
