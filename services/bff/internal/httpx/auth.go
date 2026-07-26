// Package httpx holds the BFF's HTTP plumbing: the auth/token-forwarding seam,
// gRPC-status -> HTTP-status mapping, JSON helpers, and middleware (CORS,
// logging, recovery). None of it contains business logic (ADR guardrail) — it
// is pure protocol/session translation between the browser and gRPC.
package httpx

import (
	"context"
	"net/http"
	"strings"

	"google.golang.org/grpc/metadata"
)

// ============================================================================
// TOKEN PROPAGATION — THE BFF FORWARDS THE CALLER'S IDENTITY, IT DOES NOT FORGE
// ============================================================================
//
// THE SECURITY MODEL: the BFF is NOT an identity authority.
// It does NOT validate the JWT (the domain services do, via grpcutil's auth
// interceptor + FP_JWT_SECRET) and it does NOT mint or rewrite claims. Its only
// job with the token is to carry it VERBATIM from the browser's HTTP request to
// the downstream gRPC call, so each service authenticates the REAL end user —
// not the BFF's own identity. This is "identity propagation" / "on-behalf-of",
// the same pattern an API gateway uses (Kong/Envoy ext_authz, Istio's
// request.auth.principal). If the BFF instead used a single service account, the
// services would lose per-user authZ and audit — every action would look like
// "the BFF did it". Forwarding the caller's token keeps authZ and audit honest.
//
// THE MECHANISM:
//   1. requestToken extracts the bearer token from the inbound HTTP request
//      (Authorization header first; falls back to an httpOnly cookie for the
//      cookie-based SPA flow the ADR prefers for XSS safety).
//   2. The auth middleware stashes the raw token in the request context.
//   3. Before every downstream gRPC call, handlers call ContextWithToken to copy
//      that token into OUTGOING gRPC metadata as "authorization: Bearer <token>"
//      via metadata.AppendToOutgoingContext — the exact key/format grpcutil's
//      server-side extractBearerToken expects. The service's interceptor then
//      validates it and sets the user's claims. No round-trip, no re-signing.
//
// WE NEVER LOG THE TOKEN. It is a bearer credential — logging it is equivalent
// to logging a password. It lives in the context and outgoing metadata only.
// ============================================================================

// tokenCtxKey is an unexported context key type so no other package can collide
// with (or read) our token slot — the standard Go context-key hygiene.
type tokenCtxKey struct{}

// cookieName is the httpOnly cookie the SPA MAY use instead of the Authorization
// header. Using a cookie keeps the token out of JS-readable storage (XSS-safe),
// which the ADR notes is the more secure option; the dev SPA flow returns the
// token in the login body, but the middleware accepts either transport.
const cookieName = "fp_token"

// requestToken pulls the bearer token out of an inbound HTTP request, trying the
// Authorization header first, then the httpOnly cookie. Returns "" if absent.
func requestToken(r *http.Request) string {
	// Authorization: Bearer <token> — RFC 6750. Scheme is case-insensitive.
	if h := r.Header.Get("Authorization"); h != "" {
		const scheme = "bearer "
		if len(h) > len(scheme) && strings.EqualFold(h[:len(scheme)], scheme) {
			return strings.TrimSpace(h[len(scheme):])
		}
	}
	// Cookie fallback (the XSS-safe SPA transport).
	if ck, err := r.Cookie(cookieName); err == nil {
		return ck.Value
	}
	return ""
}

// contextWithToken stashes the raw token in the request context. Stored as the
// bare token (no "Bearer " prefix); ContextWithToken adds the scheme when it
// builds the outgoing metadata, keeping a single source of truth for the format.
func contextWithToken(ctx context.Context, token string) context.Context {
	return context.WithValue(ctx, tokenCtxKey{}, token)
}

// TokenFromContext returns the caller's raw token, or "" if the request was
// unauthenticated (a public route). Handlers rarely need this directly; they use
// ContextWithToken, which reads it internally.
func TokenFromContext(ctx context.Context) string {
	tok, _ := ctx.Value(tokenCtxKey{}).(string)
	return tok
}

// ContextWithToken derives a gRPC-call context that carries the caller's token
// as outgoing metadata "authorization: Bearer <token>". Handlers wrap EVERY
// downstream call's context with this so the service authenticates the real
// user. If there is no token (a public path that still happens to fan out), the
// context is returned unchanged — the downstream then decides whether the RPC is
// allowed unauthenticated, which is the service's call to make, not the BFF's.
//
// metadata.AppendToOutgoingContext is the canonical gRPC way to attach
// per-call metadata; it merges with any existing outgoing metadata rather than
// replacing it, so trace headers injected by the otel interceptor survive.
func ContextWithToken(ctx context.Context) context.Context {
	tok := TokenFromContext(ctx)
	if tok == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tok)
}

// RequireAuth is the middleware guarding PROTECTED routes. It extracts the token
// and:
//   - if absent -> 401 immediately (default-deny; we never reach a service with
//     no identity, which would either fail there or, worse, run as nobody).
//   - if present -> stash it in the context and continue. NOTE: we do NOT verify
//     the token here. Verification is the service's job (single source of auth
//     truth = the auth service's secret). The BFF only guarantees a token is
//     PRESENT to forward; whether it is VALID is decided downstream and surfaces
//     as a gRPC Unauthenticated -> HTTP 401 via the status mapping.
//
// Public routes (login, health) are simply not wrapped with this middleware.
func RequireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok := requestToken(r)
		if tok == "" {
			WriteError(w, http.StatusUnauthorized, "missing or malformed authorization token")
			return
		}
		next.ServeHTTP(w, r.WithContext(contextWithToken(r.Context(), tok)))
	})
}

// AttachToken is a lighter middleware for routes that are PUBLIC but may still
// forward a token if one happens to be present (none today). Kept for symmetry;
// protected routes use RequireAuth.
func AttachToken(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if tok := requestToken(r); tok != "" {
			r = r.WithContext(contextWithToken(r.Context(), tok))
		}
		next.ServeHTTP(w, r)
	})
}
