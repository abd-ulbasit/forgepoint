package httpx

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TestCORS_LockedToAllowlist proves the CORS middleware reflects ONLY allowlisted
// origins (never "*") and answers preflight — the browser-session security rule.
func TestCORS_LockedToAllowlist(t *testing.T) {
	t.Parallel()
	mw := CORS([]string{"https://ui.forgepoint.dev"})
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))

	cases := []struct {
		name           string
		method         string
		origin         string
		wantStatus     int
		wantAllowOrig  string
	}{
		{"allowed origin echoed", http.MethodGet, "https://ui.forgepoint.dev", http.StatusOK, "https://ui.forgepoint.dev"},
		{"disallowed origin not echoed", http.MethodGet, "https://evil.example", http.StatusOK, ""},
		{"preflight short-circuits", http.MethodOptions, "https://ui.forgepoint.dev", http.StatusNoContent, "https://ui.forgepoint.dev"},
		{"no origin header", http.MethodGet, "", http.StatusOK, ""},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := httptest.NewRequest(tc.method, "/api/v1/models", nil)
			if tc.origin != "" {
				r.Header.Set("Origin", tc.origin)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d", w.Code, tc.wantStatus)
			}
			if got := w.Header().Get("Access-Control-Allow-Origin"); got != tc.wantAllowOrig {
				t.Errorf("Allow-Origin = %q, want %q", got, tc.wantAllowOrig)
			}
			// A wildcard must NEVER appear.
			if w.Header().Get("Access-Control-Allow-Origin") == "*" {
				t.Error("CORS must never emit a wildcard origin")
			}
		})
	}
}

// TestHTTPStatusForGRPC covers the code-mapping table directly.
func TestHTTPStatusForGRPC(t *testing.T) {
	t.Parallel()
	cases := []struct {
		code codes.Code
		want int
	}{
		{codes.OK, http.StatusOK},
		{codes.Unauthenticated, http.StatusUnauthorized},
		{codes.PermissionDenied, http.StatusForbidden},
		{codes.NotFound, http.StatusNotFound},
		{codes.InvalidArgument, http.StatusBadRequest},
		{codes.FailedPrecondition, http.StatusBadRequest},
		{codes.AlreadyExists, http.StatusConflict},
		{codes.Unavailable, http.StatusServiceUnavailable},
		{codes.Internal, http.StatusInternalServerError},
		{codes.Unknown, http.StatusInternalServerError},
	}
	for _, tc := range cases {
		if got := HTTPStatusForGRPC(status.Error(tc.code, "x")); got != tc.want {
			t.Errorf("code %s -> %d, want %d", tc.code, got, tc.want)
		}
	}
	if HTTPStatusForGRPC(nil) != http.StatusOK {
		t.Error("nil error should map to 200")
	}
}

// TestContextWithToken_AddsOutgoingMetadata proves the forwarding primitive
// attaches "authorization: Bearer <token>" to OUTGOING gRPC metadata, and is a
// no-op when there is no token.
func TestContextWithToken_AddsOutgoingMetadata(t *testing.T) {
	t.Parallel()

	// With a token in the request context, the outgoing metadata carries it.
	ctx := contextWithToken(context.Background(), "abc123")
	out := ContextWithToken(ctx)
	md, ok := metadata.FromOutgoingContext(out)
	if !ok {
		t.Fatal("expected outgoing metadata")
	}
	if v := md.Get("authorization"); len(v) != 1 || v[0] != "Bearer abc123" {
		t.Errorf("authorization metadata = %v, want [Bearer abc123]", v)
	}

	// With no token, the context is returned unchanged (no metadata added).
	out2 := ContextWithToken(context.Background())
	if _, ok := metadata.FromOutgoingContext(out2); ok {
		t.Error("expected no outgoing metadata when token is absent")
	}
}

// ============================================================================
// BODY LIMIT: unauthenticated clients cannot stream an arbitrarily large body.
// ============================================================================

// TestBodyLimit_OversizedBody_Returns400 verifies that a request body that
// exceeds the configured cap causes the handler to receive an error from
// json.Decoder (or io.ReadAll) and respond 400. The HTTP server also closes the
// connection, but because we write the status ourselves (from the Decode error
// path) we assert 400 here. A real network client would also see the connection
// reset by the server.
//
// WHY 400 not 413: http.MaxBytesReader makes the underlying Read return
// *http.MaxBytesError. json.Decoder.Decode surfaces this as a decode error.
// The handler's json.Decode branch writes 400 ("invalid JSON body"). If we
// wanted 413 we would need to type-assert on *http.MaxBytesError BEFORE the
// generic decode error write. The 400 is intentional: from the client's
// perspective "your body was malformed" is accurate and doesn't leak the cap
// value. An explicit 413 would require an additional error-type check in every
// handler; keeping it 400 maintains the single-place policy.
func TestBodyLimit_OversizedBody_Returns400(t *testing.T) {
	t.Parallel()

	const cap = 16 // tiny cap so the test is fast

	// A handler that tries to decode JSON from the (capped) body.
	// Mirrors what auth.Login and models.Register do in production.
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var v map[string]any
		if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
			// Exactly what the real handlers do on a bad body.
			WriteError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	h := BodyLimit(cap)(inner)

	// Body is valid JSON but larger than the cap.
	bigBody := strings.NewReader(`{"email":"user@example.com","password":"hunter2"}`)
	r := httptest.NewRequest(http.MethodPost, "/api/v1/login", bigBody)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("oversized body: status = %d, want 400; body=%s", w.Code, w.Body.String())
	}
}

// TestBodyLimit_ExactlyAtCap_Passes verifies that a body whose size equals the
// cap is NOT rejected. The limit is strictly greater-than, not greater-or-equal.
func TestBodyLimit_ExactlyAtCap_Passes(t *testing.T) {
	t.Parallel()

	// 48 bytes of valid JSON: {"email":"a@b.c","password":"hunter2"}
	body := `{"email":"a@b.c","password":"hunter2"}`
	cap := int64(len(body)) // exact fit

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var v map[string]any
		if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
			WriteError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	h := BodyLimit(cap)(inner)
	r := httptest.NewRequest(http.MethodPost, "/api/v1/login", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("body at exact cap: status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
}

// TestBodyLimit_OneMiBCap_CoverageCheck verifies the production cap (1 MiB)
// blocks a 2 MiB body. This is the actual cap applied in router.New so we
// confirm the value is sensible.
func TestBodyLimit_OneMiBCap_CoverageCheck(t *testing.T) {
	t.Parallel()

	const oneMiB = 1 << 20
	// Build a 2 MiB body of repeated bytes (not valid JSON, but that's fine —
	// MaxBytesReader errors before the JSON parser even runs).
	twoBig := strings.NewReader(strings.Repeat("x", 2*oneMiB))

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := r.Body.Read(make([]byte, 1)); err != nil {
			WriteError(w, http.StatusBadRequest, "body too large")
			return
		}
		// Keep reading to force the limit to trigger.
		buf := make([]byte, 2*oneMiB)
		if _, err := r.Body.Read(buf); err != nil {
			WriteError(w, http.StatusBadRequest, "body too large")
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	h := BodyLimit(oneMiB)(inner)
	r := httptest.NewRequest(http.MethodPost, "/api/v1/models", twoBig)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("2 MiB body with 1 MiB cap: status = %d, want 400; body=%s", w.Code, w.Body.String())
	}
}

// TestRequireAuth_CookieFallback proves the middleware accepts the httpOnly
// cookie transport (the XSS-safe SPA flow), not just the Authorization header.
func TestRequireAuth_CookieFallback(t *testing.T) {
	t.Parallel()
	var seenToken string
	next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seenToken = TokenFromContext(r.Context())
	})
	h := RequireAuth(next)

	r := httptest.NewRequest(http.MethodGet, "/api/v1/models", nil)
	r.AddCookie(&http.Cookie{Name: "fp_token", Value: "cookie-token"})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (cookie should authenticate)", w.Code)
	}
	if seenToken != "cookie-token" {
		t.Errorf("token from cookie = %q, want cookie-token", seenToken)
	}
}
