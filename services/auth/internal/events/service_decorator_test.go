package events_test

// ============================================================================
// EVENTING DECORATOR — UNIT TESTS (no broker; fakes for inner svc + publisher)
// ============================================================================
//
// These tests prove the Phase-1.6 wiring fix at the seam where it matters: that
// the EventingAuthService decorator PUBLISHES the right event AFTER a successful
// write, publishes NOTHING when the inner write fails (publish-after-commit), and
// NEVER fails the RPC just because a publish failed. They use fakes (a stub
// domain.AuthService and a recording EventPublisher) so they need no NATS — the
// real-broker round-trip is already covered by publisher_test.go. Here we isolate
// the decorator's decision logic, which is the actual bug the finding flags
// (the producer half was non-functional because nothing called Publish*).
// ============================================================================

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	authevents "github.com/abd-ulbasit/forgepoint/services/auth/internal/events"
	"github.com/abd-ulbasit/forgepoint/services/auth/internal/domain"
)

// ----------------------------------------------------------------------------
// FAKE EventPublisher — records every call so a test can assert exactly what was
// (or was NOT) emitted, and can be told to fail to exercise the log-not-fail path.
// ----------------------------------------------------------------------------

type recordingPublisher struct {
	userCreated []authevents.UserCreatedInput
	apiKeyRot   []authevents.APIKeyRotatedInput
	failWith    error // when non-nil, both Publish* return this error
}

func (r *recordingPublisher) PublishUserCreated(_ context.Context, in authevents.UserCreatedInput) error {
	r.userCreated = append(r.userCreated, in)
	return r.failWith
}

func (r *recordingPublisher) PublishAPIKeyRotated(_ context.Context, in authevents.APIKeyRotatedInput) error {
	r.apiKeyRot = append(r.apiKeyRot, in)
	return r.failWith
}

// ----------------------------------------------------------------------------
// FAKE inner domain.AuthService — only CreateUser/CreateAPIKey carry behavior we
// assert; the rest exist to satisfy the interface and to prove pass-through. A
// caller flag records whether the inner method was actually invoked.
// ----------------------------------------------------------------------------

type stubAuthService struct {
	// CreateUser
	createUserOut domain.User
	createUserErr error

	// CreateAPIKey
	createKeyOut    domain.APIKey
	createKeyRawKey string
	createKeyErr    error

	// pass-through probe
	loginCalled bool
}

func (s *stubAuthService) CreateUser(_ context.Context, _ domain.CreateUserInput) (domain.User, error) {
	return s.createUserOut, s.createUserErr
}

func (s *stubAuthService) CreateAPIKey(_ context.Context, _ string, _ []string, _ *time.Time) (domain.APIKey, string, error) {
	return s.createKeyOut, s.createKeyRawKey, s.createKeyErr
}

func (s *stubAuthService) Login(_ context.Context, _, _ string) (string, error) {
	s.loginCalled = true
	return "stub-jwt", nil
}

func (s *stubAuthService) ValidateToken(_ context.Context, _ string) (domain.TokenClaims, error) {
	return domain.TokenClaims{}, nil
}

func (s *stubAuthService) CheckPermission(_ context.Context, _, _, _ string) (bool, error) {
	return false, nil
}

func (s *stubAuthService) CheckPermissionForClaims(_ context.Context, _ domain.TokenClaims, _, _ string) (bool, error) {
	return false, nil
}

func (s *stubAuthService) AssignRole(_ context.Context, _, _ string) error { return nil }

// compile-time guard: the stub really is a domain.AuthService.
var _ domain.AuthService = (*stubAuthService)(nil)

// ============================================================================
// CreateUser
// ============================================================================

// On a successful create, the decorator must publish UserCreated exactly once with
// the committed user's fields mapped through (and the role NAME, not the struct).
func TestDecorator_CreateUser_PublishesAfterCommit(t *testing.T) {
	inner := &stubAuthService{
		createUserOut: domain.User{
			ID:    "user-123",
			Email: "ada@forgepoint.dev",
			Name:  "Ada Lovelace",
			Team:  "platform",
			Role:  domain.Role{Name: "engineer"},
		},
	}
	pub := &recordingPublisher{}
	svc := authevents.NewEventingAuthService(inner, pub, nil)

	got, err := svc.CreateUser(context.Background(), domain.CreateUserInput{Email: "ada@forgepoint.dev"})
	if err != nil {
		t.Fatalf("CreateUser returned error: %v", err)
	}
	if got.ID != "user-123" {
		t.Errorf("returned user ID = %q, want %q", got.ID, "user-123")
	}

	if len(pub.userCreated) != 1 {
		t.Fatalf("PublishUserCreated called %d times, want exactly 1", len(pub.userCreated))
	}
	ev := pub.userCreated[0]
	if ev.UserID != "user-123" || ev.Email != "ada@forgepoint.dev" || ev.Name != "Ada Lovelace" || ev.Team != "platform" {
		t.Errorf("UserCreatedInput mapping wrong: %+v", ev)
	}
	if ev.Role != "engineer" {
		t.Errorf("Role = %q, want the role NAME %q", ev.Role, "engineer")
	}
	if len(pub.apiKeyRot) != 0 {
		t.Errorf("ApiKeyRotated published on a user-create path: %d", len(pub.apiKeyRot))
	}
}

// When the inner write FAILS, the decorator must publish NOTHING (publish-after-
// commit: never announce a user a failed/rolled-back write didn't persist) and
// must propagate the inner error unchanged so the handler's error mapping holds.
func TestDecorator_CreateUser_NoPublishOnError(t *testing.T) {
	wantErr := domain.ErrEmailAlreadyExists
	inner := &stubAuthService{createUserErr: wantErr}
	pub := &recordingPublisher{}
	svc := authevents.NewEventingAuthService(inner, pub, nil)

	_, err := svc.CreateUser(context.Background(), domain.CreateUserInput{Email: "dup@forgepoint.dev"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v (unchanged inner contract)", err, wantErr)
	}
	if len(pub.userCreated) != 0 {
		t.Errorf("published %d UserCreated events on a failed create, want 0", len(pub.userCreated))
	}
}

// A publish FAILURE must not fail the RPC: the user is already committed and was
// returned. The decorator logs and still returns the success.
func TestDecorator_CreateUser_PublishErrorDoesNotFailRPC(t *testing.T) {
	inner := &stubAuthService{createUserOut: domain.User{ID: "user-x", Role: domain.Role{Name: "viewer"}}}
	pub := &recordingPublisher{failWith: errors.New("nats down")}
	svc := authevents.NewEventingAuthService(inner, pub, nil)

	got, err := svc.CreateUser(context.Background(), domain.CreateUserInput{})
	if err != nil {
		t.Fatalf("publish failure must NOT fail the RPC; got error: %v", err)
	}
	if got.ID != "user-x" {
		t.Errorf("returned user ID = %q, want %q (success still returned)", got.ID, "user-x")
	}
	if len(pub.userCreated) != 1 {
		t.Errorf("publish was attempted %d times, want exactly 1", len(pub.userCreated))
	}
}

// ============================================================================
// CreateAPIKey
// ============================================================================

// On a successful mint, the decorator publishes ApiKeyRotated with the id/prefix/
// scopes mapped through, an EMPTY ReplacedKeyID (independent create), the rawKey
// preserved in the return, and NO secret material in the event input.
func TestDecorator_CreateAPIKey_PublishesAfterCommitNoSecret(t *testing.T) {
	const rawKey = "fp_a1b2c3d4e5f6SUPERSECRETKEYMATERIAL"
	inner := &stubAuthService{
		createKeyOut: domain.APIKey{
			ID:        "key-789",
			UserID:    "user-123",
			KeyHash:   "deadbeefhash",
			KeyPrefix: "fp_a1b2",
			Scopes:    []string{"models:read", "experiments:write"},
		},
		createKeyRawKey: rawKey,
	}
	pub := &recordingPublisher{}
	svc := authevents.NewEventingAuthService(inner, pub, nil)

	gotKey, gotRaw, err := svc.CreateAPIKey(context.Background(), "user-123", []string{"models:read"}, nil)
	if err != nil {
		t.Fatalf("CreateAPIKey returned error: %v", err)
	}
	if gotRaw != rawKey {
		t.Errorf("rawKey = %q, want it preserved as %q (shown to caller once)", gotRaw, rawKey)
	}
	if gotKey.ID != "key-789" {
		t.Errorf("returned key ID = %q, want %q", gotKey.ID, "key-789")
	}

	if len(pub.apiKeyRot) != 1 {
		t.Fatalf("PublishAPIKeyRotated called %d times, want exactly 1", len(pub.apiKeyRot))
	}
	ev := pub.apiKeyRot[0]
	if ev.KeyID != "key-789" || ev.UserID != "user-123" || ev.KeyPrefix != "fp_a1b2" {
		t.Errorf("APIKeyRotatedInput mapping wrong: %+v", ev)
	}
	if ev.ReplacedKeyID != "" {
		t.Errorf("ReplacedKeyID = %q, want empty on an independent create", ev.ReplacedKeyID)
	}
	if len(ev.Scopes) != 2 {
		t.Errorf("Scopes len = %d, want 2", len(ev.Scopes))
	}

	// SECURITY: neither the raw key nor the stored hash may appear in the event
	// input the decorator built — the type carries only id + prefix + scopes.
	for _, s := range ev.Scopes {
		if strings.Contains(s, rawKey) {
			t.Fatal("raw key material leaked into a scope")
		}
	}
	if strings.Contains(ev.KeyID+ev.UserID+ev.KeyPrefix+ev.ReplacedKeyID, rawKey) {
		t.Fatal("raw key material leaked into the ApiKeyRotated input")
	}
	if strings.Contains(ev.KeyID+ev.UserID+ev.KeyPrefix+ev.ReplacedKeyID, "deadbeefhash") {
		t.Fatal("stored key hash leaked into the ApiKeyRotated input")
	}
}

// A failed mint publishes nothing and propagates the error.
func TestDecorator_CreateAPIKey_NoPublishOnError(t *testing.T) {
	wantErr := errors.New("storing api key: boom")
	inner := &stubAuthService{createKeyErr: wantErr}
	pub := &recordingPublisher{}
	svc := authevents.NewEventingAuthService(inner, pub, nil)

	_, _, err := svc.CreateAPIKey(context.Background(), "user-123", nil, nil)
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if len(pub.apiKeyRot) != 0 {
		t.Errorf("published %d ApiKeyRotated events on a failed mint, want 0", len(pub.apiKeyRot))
	}
}

// A publish failure on the key path must not fail the RPC; rawKey is still returned.
func TestDecorator_CreateAPIKey_PublishErrorDoesNotFailRPC(t *testing.T) {
	inner := &stubAuthService{
		createKeyOut:    domain.APIKey{ID: "key-1", UserID: "u1", KeyPrefix: "fp_xxxx"},
		createKeyRawKey: "fp_raw",
	}
	pub := &recordingPublisher{failWith: errors.New("nats down")}
	svc := authevents.NewEventingAuthService(inner, pub, nil)

	_, gotRaw, err := svc.CreateAPIKey(context.Background(), "u1", nil, nil)
	if err != nil {
		t.Fatalf("publish failure must NOT fail the RPC; got error: %v", err)
	}
	if gotRaw != "fp_raw" {
		t.Errorf("rawKey = %q, want it still returned despite publish failure", gotRaw)
	}
}

// ============================================================================
// PASS-THROUGH — non-mutating RPCs reach the inner service unchanged and emit
// nothing (the decorator only intercepts the two event-bearing write paths).
// ============================================================================

func TestDecorator_NonMutatingRPCs_PassThroughNoPublish(t *testing.T) {
	inner := &stubAuthService{}
	pub := &recordingPublisher{}
	svc := authevents.NewEventingAuthService(inner, pub, nil)

	tok, err := svc.Login(context.Background(), "ada@forgepoint.dev", "pw")
	if err != nil {
		t.Fatalf("Login passthrough error: %v", err)
	}
	if tok != "stub-jwt" {
		t.Errorf("Login token = %q, want the inner service's %q", tok, "stub-jwt")
	}
	if !inner.loginCalled {
		t.Error("Login did not reach the inner service (embedding not forwarding)")
	}
	if len(pub.userCreated) != 0 || len(pub.apiKeyRot) != 0 {
		t.Errorf("a non-mutating RPC published events: user=%d apikey=%d",
			len(pub.userCreated), len(pub.apiKeyRot))
	}
}
