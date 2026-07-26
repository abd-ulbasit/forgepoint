// validator_test.go — unit tests for the local TokenValidator adapter.
//
// These tests exercise the ADAPTER in isolation: given a stub domain.AuthService
// (an interface, no DB), does the validator translate ValidateToken's results
// into the grpcutil.Claims + gRPC status codes the interceptor expects?
//
// The validator is the AUTHENTICATION half of the fix: main.go wires it into
// grpcutil.AuthUnaryInterceptor so a Bearer token on a protected RPC becomes
// grpcutil.Claims in the context — which requireAdmin then reads. Without this
// adapter the production server never populated claims and every admin RPC was
// permanently Unauthenticated.
package authn

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/abd-ulbasit/forgepoint/services/auth/internal/domain"
)

// stubTokenService is a minimal domain.TokenValidator stub. We only need the one
// method the adapter calls (ValidateToken); the adapter depends on a NARROW
// interface (interface segregation), so the stub is tiny.
type stubTokenService struct {
	claims domain.TokenClaims
	err    error
}

func (s stubTokenService) ValidateToken(_ context.Context, _ string) (domain.TokenClaims, error) {
	return s.claims, s.err
}

func TestValidator_ValidJWT_ProducesClaims(t *testing.T) {
	svc := stubTokenService{
		claims: domain.TokenClaims{
			UserID:    "admin-uuid",
			Email:     "admin@forgepoint.local",
			Team:      "platform",
			Role:      "admin",
			Scopes:    []string{"models:read"},
			IssuedAt:  time.Now(),
			ExpiresAt: time.Now().Add(15 * time.Minute),
			TokenKind: domain.TokenKindJWT,
		},
	}
	v := NewValidator(svc)

	got, err := v.Validate(context.Background(), "any-token")
	if err != nil {
		t.Fatalf("Validate returned error for a valid token: %v", err)
	}
	if got == nil {
		t.Fatal("Validate returned nil claims for a valid token")
	}
	// The adapter must map EVERY identity field the handler relies on: requireAdmin
	// keys off UserID, and downstream services read Email/Team/Role/Scopes.
	if got.UserID != "admin-uuid" || got.Email != "admin@forgepoint.local" ||
		got.Team != "platform" || got.Role != "admin" {
		t.Fatalf("claims mismatch: %+v", got)
	}
	if len(got.Scopes) != 1 || got.Scopes[0] != "models:read" {
		t.Fatalf("scopes not mapped: %v", got.Scopes)
	}
}

func TestValidator_InvalidToken_Unauthenticated(t *testing.T) {
	// The domain collapses expired/forged/wrong-secret/revoked into the single
	// opaque ErrInvalidToken. The adapter must surface it as codes.Unauthenticated
	// — the interceptor passes that code+message through to the client unchanged.
	v := NewValidator(stubTokenService{err: domain.ErrInvalidToken})

	got, err := v.Validate(context.Background(), "bad-token")
	if got != nil {
		t.Fatalf("expected nil claims on invalid token, got %+v", got)
	}
	if code := status.Code(err); code != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %s (err=%v)", code, err)
	}
}

func TestValidator_WrappedInvalidToken_Unauthenticated(t *testing.T) {
	// errors.Is semantics: a wrapped sentinel must still map to Unauthenticated.
	wrapped := stubTokenService{err: fmt.Errorf("validate api key: %w", domain.ErrInvalidToken)}
	got, err := NewValidator(wrapped).Validate(context.Background(), "bad")
	if got != nil {
		t.Fatalf("expected nil claims, got %+v", got)
	}
	if code := status.Code(err); code != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated for wrapped sentinel, got %s", code)
	}
}

func TestValidator_InfraError_Internal(t *testing.T) {
	// A NON-ErrInvalidToken error means the auth backend itself failed to decide
	// (DB down on the API-key path). That must NOT look like a bad token: it maps
	// to codes.Internal so the interceptor sanitizes the message and SLO alerting
	// fires on the correct (server-fault) signal, not a client-fault Unauthenticated.
	v := NewValidator(stubTokenService{err: errors.New("repository: connection reset by peer")})

	got, err := v.Validate(context.Background(), "token")
	if got != nil {
		t.Fatalf("expected nil claims on infra error, got %+v", got)
	}
	if code := status.Code(err); code != codes.Internal {
		t.Fatalf("expected Internal on infra error, got %s (err=%v)", code, err)
	}
	// NO LEAK: the raw repository/driver text must not ride out in the status message.
	if msg := status.Convert(err).Message(); strings.Contains(msg, "repository") || strings.Contains(msg, "connection reset") {
		t.Fatalf("infra error leaked internal detail: %q", msg)
	}
}
