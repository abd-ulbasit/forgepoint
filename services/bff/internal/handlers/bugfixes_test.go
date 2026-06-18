package handlers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	authv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/auth/v1"
	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	pipelinev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/pipeline/v1"
	registryv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/registry/v1"
	"github.com/abd-ulbasit/forgepoint/services/bff/internal/httpx"
)

// testSSELimits returns generous caps so the existing SSE relay tests (which open
// a single stream) are never rejected. The dedicated cap tests below build their
// own tight limiter so they exercise the 429 path deterministically.
func testSSELimits() SSELimits {
	return SSELimits{MaxGlobal: 100, MaxPerUser: 100, MaxLifetime: 0}
}

// makeJWT builds a syntactically-valid, UNSIGNED-for-test JWT string whose
// payload carries the given claims. decodeJWTClaims only reads the middle
// (payload) segment and never verifies the signature, so a fixed dummy header and
// signature are fine. This mirrors exactly what the auth service emits: a
// base64url-RawURLEncoding payload with sub/email/name/team/role/exp.
func makeJWT(t *testing.T, claims map[string]any) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payloadJSON, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	payload := base64.RawURLEncoding.EncodeToString(payloadJSON)
	return header + "." + payload + ".sig-not-verified-by-bff"
}

// ============================================================================
// BUG 1: login populates user + expiresAt from the JWT claims when the auth
// LoginResponse omits them (the M1 auth service returns only access_token).
// ============================================================================

func TestLogin_PopulatesUserFromJWTClaims_WhenAuthOmitsThem(t *testing.T) {
	t.Parallel()

	exp := time.Date(2026, 6, 18, 15, 0, 0, 0, time.UTC)
	token := makeJWT(t, map[string]any{
		"sub":   "u-42",
		"email": "ada@forgepoint.dev",
		"name":  "Ada Lovelace",
		"team":  "platform",
		"role":  "engineer",
		"exp":   exp.Unix(),
		"iat":   exp.Add(-time.Hour).Unix(),
	})

	auth := &mockAuth{
		loginFn: func(_ context.Context, _ *authv1.LoginRequest) (*authv1.LoginResponse, error) {
			// Exactly what the real M1 auth service returns: ONLY the token. No
			// User, no ExpiresAt — this is the condition that produced user:null.
			return &authv1.LoginResponse{AccessToken: token}, nil
		},
	}
	h := NewAuthHandler(auth, testLogger())

	body := strings.NewReader(`{"email":"ada@forgepoint.dev","password":"pw"}`)
	r := httptest.NewRequest(http.MethodPost, "/api/v1/login", body)
	w := httptest.NewRecorder()
	h.Login(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var resp loginResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v; body=%s", err, w.Body.String())
	}

	// The user must be reconstructed from the JWT — NOT null.
	if resp.User == nil {
		t.Fatalf("user is nil; want it decoded from JWT claims. body=%s", w.Body.String())
	}
	if resp.User.ID != "u-42" {
		t.Errorf("user.id = %q, want u-42", resp.User.ID)
	}
	if resp.User.Email != "ada@forgepoint.dev" {
		t.Errorf("user.email = %q, want ada@forgepoint.dev", resp.User.Email)
	}
	if resp.User.Name != "Ada Lovelace" {
		t.Errorf("user.name = %q, want 'Ada Lovelace'", resp.User.Name)
	}
	if resp.User.Team != "platform" {
		t.Errorf("user.team = %q, want platform", resp.User.Team)
	}
	if resp.User.Role != "engineer" {
		t.Errorf("user.role = %q, want engineer", resp.User.Role)
	}

	// expiresAt must be the RFC3339 form of the JWT's exp claim.
	if got, want := resp.ExpiresAt, exp.Format(time.RFC3339); got != want {
		t.Errorf("expiresAt = %q, want %q", got, want)
	}

	// SECURITY: the response must NOT have logged or leaked anything beyond the
	// shaped fields; the token only appears in accessToken (the SPA needs it).
	if resp.AccessToken != token {
		t.Errorf("accessToken not echoed verbatim")
	}
}

// When the auth LoginResponse DOES carry a structured user/expiry, those win over
// the JWT fallback (the fallback only fills blanks). Guards the precedence.
func TestLogin_PrefersAuthResponseUserOverJWT(t *testing.T) {
	t.Parallel()

	// JWT says "jwt-name"; the structured response says "proto-name". Proto wins.
	token := makeJWT(t, map[string]any{"sub": "u-1", "name": "jwt-name", "exp": time.Now().Add(time.Hour).Unix()})
	authExp := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	auth := &mockAuth{
		loginFn: func(_ context.Context, _ *authv1.LoginRequest) (*authv1.LoginResponse, error) {
			return &authv1.LoginResponse{
				AccessToken: token,
				ExpiresAt:   timestamppb.New(authExp),
				User:        &authv1.User{Id: "u-1", Name: "proto-name", Email: "p@x.io"},
			}, nil
		},
	}
	h := NewAuthHandler(auth, testLogger())
	r := httptest.NewRequest(http.MethodPost, "/api/v1/login", strings.NewReader(`{"email":"p@x.io","password":"pw"}`))
	w := httptest.NewRecorder()
	h.Login(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var resp loginResponse
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.User == nil || resp.User.Name != "proto-name" {
		t.Errorf("structured user should win; got %+v", resp.User)
	}
	if resp.ExpiresAt != authExp.Format(time.RFC3339) {
		t.Errorf("expiresAt = %q, want %q (structured)", resp.ExpiresAt, authExp.Format(time.RFC3339))
	}
}

// An opaque / non-JWT token must not break login: user stays nil, no panic, 200.
func TestLogin_OpaqueToken_NoPanic(t *testing.T) {
	t.Parallel()
	auth := &mockAuth{
		loginFn: func(_ context.Context, _ *authv1.LoginRequest) (*authv1.LoginResponse, error) {
			return &authv1.LoginResponse{AccessToken: "not-a-jwt"}, nil
		},
	}
	h := NewAuthHandler(auth, testLogger())
	r := httptest.NewRequest(http.MethodPost, "/api/v1/login", strings.NewReader(`{"email":"a@b.c","password":"pw"}`))
	w := httptest.NewRecorder()
	h.Login(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var resp loginResponse
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.AccessToken != "not-a-jwt" {
		t.Errorf("token should still be returned")
	}
}

// ============================================================================
// BUG 2: dashboard model count falls back to len(rows) when the registry's
// total_count is not populated (it currently returns 0).
// ============================================================================

// makeListModelsResponse builds a ListModelsResponse with nRows model rows and
// the given total_count, so modelCount's precedence can be exercised directly.
func makeListModelsResponse(nRows int, totalCount int32) *registryv1.ListModelsResponse {
	models := make([]*registryv1.Model, nRows)
	for i := range models {
		models[i] = &registryv1.Model{Id: "m-" + string(rune('a'+i))}
	}
	return &registryv1.ListModelsResponse{
		Models:     models,
		Pagination: &commonv1.PaginationResponse{TotalCount: totalCount},
	}
}

func TestModelCount_FallsBackToRowCount_WhenTotalCountZero(t *testing.T) {
	t.Parallel()
	// total_count == 0 (the registry's current behavior) but 3 rows returned.
	resp := makeListModelsResponse(3, 0)
	if got := modelCount(resp); got != 3 {
		t.Errorf("modelCount = %d, want 3 (fallback to len(models))", got)
	}
}

func TestModelCount_PrefersTotalCount_WhenPositive(t *testing.T) {
	t.Parallel()
	// Registry-fixed case: total_count=42 exceeds the 1 returned row; trust it.
	resp := makeListModelsResponse(1, 42)
	if got := modelCount(resp); got != 42 {
		t.Errorf("modelCount = %d, want 42 (trust total_count)", got)
	}
}

func TestModelCount_NegativeTotalCount_FallsBack(t *testing.T) {
	t.Parallel()
	// Proto reserves -1 for "too expensive to count" — treat as no count.
	resp := makeListModelsResponse(2, -1)
	if got := modelCount(resp); got != 2 {
		t.Errorf("modelCount = %d, want 2 (fallback on -1)", got)
	}
}

// ============================================================================
// BUG 3: SSE concurrency cap returns 429 when exceeded.
// ============================================================================

// TestSSELimiter_Acquire enforces the global + per-user caps directly (fast,
// deterministic — no streams needed).
func TestSSELimiter_Acquire(t *testing.T) {
	t.Parallel()

	t.Run("per-user cap", func(t *testing.T) {
		t.Parallel()
		l := newSSELimiter(SSELimits{MaxGlobal: 100, MaxPerUser: 2})
		r1, ok1 := l.acquire("alice")
		_, ok2 := l.acquire("alice")
		_, ok3 := l.acquire("alice") // 3rd for alice -> rejected
		if !ok1 || !ok2 {
			t.Fatalf("first two acquires should succeed: %v %v", ok1, ok2)
		}
		if ok3 {
			t.Fatalf("third acquire for same user should be rejected (per-user cap=2)")
		}
		// A DIFFERENT user is unaffected by alice saturating her own bucket.
		if _, ok := l.acquire("bob"); !ok {
			t.Fatalf("bob should not be blocked by alice's per-user cap")
		}
		// Releasing one of alice's frees a slot.
		r1()
		if _, ok := l.acquire("alice"); !ok {
			t.Fatalf("after release, alice should acquire again")
		}
	})

	t.Run("global cap", func(t *testing.T) {
		t.Parallel()
		l := newSSELimiter(SSELimits{MaxGlobal: 2, MaxPerUser: 100})
		_, ok1 := l.acquire("a")
		r2, ok2 := l.acquire("b")
		_, ok3 := l.acquire("c") // global budget (2) exhausted
		if !ok1 || !ok2 {
			t.Fatalf("first two global acquires should succeed")
		}
		if ok3 {
			t.Fatalf("third acquire should be rejected (global cap=2)")
		}
		// Returning a global token lets the next user in.
		r2()
		if _, ok := l.acquire("c"); !ok {
			t.Fatalf("after a global release, c should acquire")
		}
	})

	t.Run("global release does not leak per-user count", func(t *testing.T) {
		t.Parallel()
		// per-user has room (cap 5) but global is full (cap 1): the per-user slot
		// taken first MUST be handed back when the global send fails, else the
		// rejected request leaks a per-user count and starves the user.
		l := newSSELimiter(SSELimits{MaxGlobal: 1, MaxPerUser: 5})
		if _, ok := l.acquire("zoe"); !ok {
			t.Fatalf("first acquire should succeed")
		}
		if _, ok := l.acquire("zoe"); ok {
			t.Fatalf("second acquire should be rejected (global full)")
		}
		// zoe's per-user count must be exactly 1 (not 2): the rejected attempt was
		// rolled back. We verify by filling global via a release then re-acquiring
		// up to the per-user cap without surprise rejections.
		l.mu.Lock()
		got := l.perUser["zoe"]
		l.mu.Unlock()
		if got != 1 {
			t.Fatalf("zoe per-user count = %d, want 1 (rejected attempt must roll back)", got)
		}
	})
}

// TestWatch_Returns429_WhenCapExceeded drives the cap through the real HTTP
// handler: with a global cap of 1, the FIRST watch holds the slot (its stream
// blocks in Recv until the request context is cancelled) and the SECOND watch is
// rejected with 429 BEFORE any upstream stream is opened.
func TestWatch_Returns429_WhenCapExceeded(t *testing.T) {
	t.Parallel()

	// firstOpened signals the first stream has acquired its slot and is blocked in
	// Recv (so the slot is genuinely held when we fire the second request).
	firstOpened := make(chan struct{})
	var secondAttempted bool

	pipe := &mockPipeline{
		watchFn: func(ctx context.Context, _ *pipelinev1.WatchExecutionRequest) (grpc.ServerStreamingClient[pipelinev1.WatchExecutionResponse], error) {
			close(firstOpened)
			return &blockingWatchStream{streamCtx: ctx}, nil
		},
	}
	// Global cap of exactly 1 — the second concurrent watch must be 429'd.
	h := NewPipelinesHandler(pipe, testLogger(), SSELimits{MaxGlobal: 1, MaxPerUser: 100})

	// --- First request: holds the single global slot. -------------------------
	req1Ctx, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	r1 := httptest.NewRequest(http.MethodGet, "/api/v1/executions/e-1/watch", nil)
	r1 = r1.WithContext(req1Ctx)
	r1.Header.Set("Authorization", "Bearer "+makeJWT(t, map[string]any{"sub": "u-1"}))
	r1.SetPathValue("id", "e-1")
	w1 := httptest.NewRecorder()

	done1 := make(chan struct{})
	go func() {
		defer close(done1)
		serveWatchThroughAuth(h, w1, r1)
	}()

	// Wait until the first stream actually holds the slot.
	select {
	case <-firstOpened:
	case <-time.After(2 * time.Second):
		t.Fatal("first watch never opened its stream")
	}

	// --- Second request (different user, so per-user is NOT the limiter): the
	// global cap is full, so it must be rejected with 429 without opening a stream.
	r2 := httptest.NewRequest(http.MethodGet, "/api/v1/executions/e-2/watch", nil)
	r2.Header.Set("Authorization", "Bearer "+makeJWT(t, map[string]any{"sub": "u-2"}))
	r2.SetPathValue("id", "e-2")
	w2 := httptest.NewRecorder()
	secondAttempted = true
	serveWatchThroughAuth(h, w2, r2)

	if w2.Code != http.StatusTooManyRequests {
		t.Fatalf("second watch status = %d, want 429; body=%s", w2.Code, w2.Body.String())
	}
	if !strings.Contains(w2.Body.String(), "too many") {
		t.Errorf("429 body should explain the cap; got %s", w2.Body.String())
	}
	if ra := w2.Header().Get("Retry-After"); ra == "" {
		t.Errorf("429 should set a Retry-After header")
	}

	// Release the first slot (browser closes the tab) and confirm a third request
	// now succeeds in acquiring — i.e. the slot was returned, not leaked.
	cancel1()
	select {
	case <-done1:
	case <-time.After(2 * time.Second):
		t.Fatal("first watch did not return after cancel (slot leak)")
	}

	if !secondAttempted { // keep the variable meaningful / silence linters
		t.Fatal("test did not attempt the second request")
	}

	// Third request after the slot is freed should NOT be 429 (the global token
	// came back). We let it open and immediately cancel so it returns fast.
	firstOpened2 := make(chan struct{})
	pipe.watchFn = func(ctx context.Context, _ *pipelinev1.WatchExecutionRequest) (grpc.ServerStreamingClient[pipelinev1.WatchExecutionResponse], error) {
		close(firstOpened2)
		return &blockingWatchStream{streamCtx: ctx}, nil
	}
	r3Ctx, cancel3 := context.WithCancel(context.Background())
	r3 := httptest.NewRequest(http.MethodGet, "/api/v1/executions/e-3/watch", nil).WithContext(r3Ctx)
	r3.Header.Set("Authorization", "Bearer "+makeJWT(t, map[string]any{"sub": "u-3"}))
	r3.SetPathValue("id", "e-3")
	w3 := httptest.NewRecorder()
	done3 := make(chan struct{})
	go func() {
		defer close(done3)
		serveWatchThroughAuth(h, w3, r3)
	}()
	select {
	case <-firstOpened2:
		// good — slot was free, stream opened (no 429)
	case <-time.After(2 * time.Second):
		t.Fatal("third watch did not open after slot freed — slot leaked")
	}
	cancel3()
	<-done3
	if w3.Code == http.StatusTooManyRequests {
		t.Errorf("third watch should NOT be 429 after the slot was freed")
	}
}

// serveWatchThroughAuth runs Watch behind RequireAuth so the token lands in the
// request context (sseSubject reads it) exactly as in production. We use the same
// http.ResponseWriter the caller passed (which may be a goroutine-local recorder)
// rather than allocating a new one, so the caller can inspect the result.
func serveWatchThroughAuth(h *PipelinesHandler, w http.ResponseWriter, r *http.Request) {
	httpx.RequireAuth(http.HandlerFunc(h.Watch)).ServeHTTP(w, r)
}
