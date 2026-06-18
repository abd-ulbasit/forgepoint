// Package authn provides the AUTHENTICATION adapter that bridges the auth
// service's domain token logic to the shared grpcutil auth interceptor.
//
// ============================================================================
// THE BUG THIS PACKAGE CLOSES
// ============================================================================
//
// pkg/grpcutil ships an AuthUnaryInterceptor that extracts the Bearer token,
// calls a grpcutil.TokenValidator, and injects the resulting *grpcutil.Claims
// into the request context — that is the ONLY code path that can populate the
// private context key ClaimsFromContext reads. The auth handler's requireAdmin
// reads those claims and fails closed ("missing authentication") when absent.
//
// But the auth service's main.go originally built its gRPC server WITHOUT an
// auth validator (NewServer(WithLogger, WithReflection) only). With no validator
// the interceptor is never added, claims are NEVER in the context, and so every
// admin-gated RPC (CreateUser/AssignRole/RevokeAPIKey/ListUsers) returned
// Unauthenticated even when the caller presented a perfectly valid admin JWT.
// Login worked (it mints the token) but the token could never be USED.
//
// This package is the missing piece: a grpcutil.TokenValidator that validates
// the token LOCALLY (design D2 — verify the JWT signature with the shared
// FP_JWT_SECRET in-process, no remote call) by reusing the domain's already-built
// ValidateToken (which handles BOTH JWTs and "fp_" API keys), and converts the
// domain.TokenClaims into the grpcutil.Claims the interceptor injects.
//
// ============================================================================
// REUSABLE PATTERN (this is the template for the OTHER 9 services)
// ============================================================================
//
//   - The AUTH service is special: it OWNS the token logic, so its validator
//     reuses the in-process domain.ValidateToken directly (no network hop, no
//     circular dependency). It is the source of truth.
//
//   - The OTHER 9 services do NOT own the token logic. They will implement the
//     SAME grpcutil.TokenValidator interface but back it with EITHER:
//       (a) local JWT verification using the shared FP_JWT_SECRET (design D2 —
//           pure crypto, no DB, the hot path), OR
//       (b) a gRPC call to auth.ValidateToken (needed for "fp_" API keys, whose
//           validity lives in auth's database).
//     In every case the wiring in main.go is identical: build a TokenValidator,
//     pass it to grpcutil.WithAuthValidator(validator, skipMethods...), and list
//     the public methods + health/reflection in the skip set. See main.go.
package authn

import (
	"context"
	"errors"

	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/services/auth/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// tokenValidator is the NARROW dependency this adapter needs from the domain.
//
// WHY a one-method interface instead of taking the whole domain.AuthService:
// interface segregation (the I in SOLID). The adapter only ever calls
// ValidateToken; depending on the full 8-method service would couple it to
// methods it never uses and force test stubs to implement all of them. The
// concrete *authService (and the EventingAuthService decorator) both satisfy
// this implicitly, so main.go can pass either.
type tokenValidator interface {
	ValidateToken(ctx context.Context, token string) (domain.TokenClaims, error)
}

// Validator adapts the domain token logic to grpcutil.TokenValidator.
//
// It is a thin anti-corruption layer: domain.TokenClaims (a business type) is
// translated into grpcutil.Claims (the transport-layer identity the interceptor
// injects). The handler then reads grpcutil.Claims and never learns whether a
// JWT or an API key produced them.
type Validator struct {
	svc tokenValidator
}

// Compile-time proof that *Validator satisfies the interface the interceptor
// expects. If grpcutil.TokenValidator ever changes shape, this line breaks the
// build here (at the adapter) instead of silently at the wiring site.
var _ grpcutil.TokenValidator = (*Validator)(nil)

// NewValidator builds the local token validator from the auth domain service.
// The auth service IS the token authority, so we reuse its in-process
// ValidateToken (JWT verify with FP_JWT_SECRET + API-key DB lookup) rather than
// calling itself over the network — that would be circular and add a needless hop.
func NewValidator(svc tokenValidator) *Validator {
	return &Validator{svc: svc}
}

// Validate implements grpcutil.TokenValidator.
//
// ERROR MAPPING (this is the load-bearing translation):
//
//	The domain returns exactly one of:
//	  - nil               → a verified token; map claims and accept.
//	  - ErrInvalidToken   → opaque "bad token" (expired/forged/wrong-secret/
//	                        revoked/unknown API key). Map to Unauthenticated.
//	                        The interceptor passes Unauthenticated's code AND
//	                        message through to the client unchanged (client-facing
//	                        by design), so we use a generic "invalid token" string
//	                        that reveals no reason — matching the domain's opaque
//	                        posture (no expired-vs-forged side channel).
//	  - any other error   → the auth backend could not DECIDE (e.g. the API-key
//	                        path's DB is down). This is a SERVER fault, not a bad
//	                        token, so it maps to codes.Internal. The interceptor
//	                        then SANITIZES the message to "authentication service
//	                        error" (it only passes Unauthenticated/PermissionDenied
//	                        messages through), so no DB host/SQL leaks — while the
//	                        Internal code keeps SLO alerting on the right signal.
//
// WHY this distinction matters (interview): conflating "backend down" with "bad
// token" would (a) tell an attacker nothing useful but (b) silently mis-route
// your alerting — a Postgres outage would look like a spike of clients sending
// bad tokens (a 4xx-class Unauthenticated) instead of a 5xx-class server fault.
// Preserve the code; sanitize the message.
func (v *Validator) Validate(ctx context.Context, token string) (*grpcutil.Claims, error) {
	claims, err := v.svc.ValidateToken(ctx, token)
	if err != nil {
		if errors.Is(err, domain.ErrInvalidToken) {
			// Opaque, client-facing. The interceptor forwards this message verbatim,
			// so it must not say WHY the token failed.
			return nil, status.Error(codes.Unauthenticated, "invalid token")
		}
		// "Couldn't decide" → server fault. The raw err may wrap SQL/driver/host
		// detail; we attach a generic message and Internal code. The interceptor
		// replaces the message anyway for non-client-facing codes, and the real
		// error is already logged by the logging interceptor (which runs first).
		return nil, status.Error(codes.Internal, "authentication backend error")
	}

	// SUCCESS: translate domain.TokenClaims → grpcutil.Claims. This is the single
	// boundary where the business identity becomes the transport identity the
	// handler's ClaimsFromContext returns. UserID is what requireAdmin keys off;
	// Email/Team/Role/Scopes are carried for any handler/downstream that needs them.
	return &grpcutil.Claims{
		UserID: claims.UserID,
		Email:  claims.Email,
		Team:   claims.Team,
		Role:   claims.Role,
		Scopes: claims.Scopes,
	}, nil
}
