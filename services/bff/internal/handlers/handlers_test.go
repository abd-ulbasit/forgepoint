package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	authv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/auth/v1"
	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	registryv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/registry/v1"
	"github.com/abd-ulbasit/forgepoint/services/bff/internal/httpx"
)

// testLogger discards output so tests stay quiet but the code path (which logs)
// still runs.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// ctxWithToken simulates what the RequireAuth middleware does: stash a token in
// the request context so the handler's httpx.ContextWithToken forwards it. We go
// through the real middleware in the integration-style tests; for direct handler
// tests we build a request whose context already carries the token by routing it
// through RequireAuth with a stub next.
func requestWithToken(method, target, token string, body io.Reader) *http.Request {
	r := httptest.NewRequest(method, target, body)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	return r
}

// serveThroughAuth runs the handler behind the real RequireAuth middleware so the
// token in the Authorization header lands in the context exactly as in prod.
func serveThroughAuth(h http.HandlerFunc, r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	httpx.RequireAuth(h).ServeHTTP(w, r)
	return w
}

// ============================================================================
// LOGIN: proxies to auth and returns the token in the JSON body.
// ============================================================================

func TestLogin_ProxiesAndReturnsToken(t *testing.T) {
	t.Parallel()
	auth := &mockAuth{
		loginFn: func(_ context.Context, in *authv1.LoginRequest) (*authv1.LoginResponse, error) {
			if in.GetEmail() != "user@example.com" || in.GetPassword() != "hunter2" {
				t.Fatalf("login received wrong credentials: %+v", in)
			}
			return &authv1.LoginResponse{
				AccessToken: "jwt-abc-123",
				ExpiresAt:   timestamppb.New(timeMustParse()),
				User: &authv1.User{
					Id: "u-1", Email: "user@example.com", Name: "U", Team: "ml", Role: "engineer",
				},
			}, nil
		},
	}
	h := NewAuthHandler(auth, testLogger())

	body := strings.NewReader(`{"email":"user@example.com","password":"hunter2"}`)
	r := httptest.NewRequest(http.MethodPost, "/api/v1/login", body)
	w := httptest.NewRecorder()
	h.Login(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var resp loginResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v; body=%s", err, w.Body.String())
	}
	if resp.AccessToken != "jwt-abc-123" {
		t.Errorf("accessToken = %q, want jwt-abc-123", resp.AccessToken)
	}
	if resp.User == nil || resp.User.Email != "user@example.com" {
		t.Errorf("user not echoed: %+v", resp.User)
	}
	if resp.ExpiresAt == "" {
		t.Error("expiresAt should be populated")
	}
}

func TestLogin_BadJSON_400(t *testing.T) {
	t.Parallel()
	h := NewAuthHandler(&mockAuth{}, testLogger())
	r := httptest.NewRequest(http.MethodPost, "/api/v1/login", strings.NewReader("{not json"))
	w := httptest.NewRecorder()
	h.Login(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestLogin_InvalidCredentials_Maps401(t *testing.T) {
	t.Parallel()
	auth := &mockAuth{
		loginFn: func(_ context.Context, _ *authv1.LoginRequest) (*authv1.LoginResponse, error) {
			return nil, status.Error(codes.Unauthenticated, "invalid credentials")
		},
	}
	h := NewAuthHandler(auth, testLogger())
	r := httptest.NewRequest(http.MethodPost, "/api/v1/login",
		strings.NewReader(`{"email":"a@b.c","password":"x"}`))
	w := httptest.NewRecorder()
	h.Login(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401; body=%s", w.Code, w.Body.String())
	}
	// The body must be sanitized JSON, not the raw gRPC error.
	if strings.Contains(w.Body.String(), "rpc error") {
		t.Errorf("error body leaked gRPC detail: %s", w.Body.String())
	}
}

// ============================================================================
// PROTECTED ROUTE WITHOUT TOKEN -> 401 (via the RequireAuth middleware).
// ============================================================================

func TestProtectedRoute_NoToken_401(t *testing.T) {
	t.Parallel()
	// The handler must NEVER be reached; if it is, the mock's nil func panics.
	reg := &mockRegistry{}
	mh := NewModelsHandler(reg, testLogger())

	r := httptest.NewRequest(http.MethodGet, "/api/v1/models", nil) // no Authorization header
	w := serveThroughAuth(mh.List, r)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
	if reg.capturedAuthz != "" {
		t.Errorf("registry should not have been called; saw authz=%q", reg.capturedAuthz)
	}
}

// ============================================================================
// TOKEN FORWARDING: a handler forwards the bearer token as outgoing gRPC
// metadata "authorization: Bearer <token>".
// ============================================================================

func TestModelsList_ForwardsTokenAsGRPCMetadata(t *testing.T) {
	t.Parallel()
	reg := &mockRegistry{
		listModelsFn: func(_ context.Context, _ *registryv1.ListModelsRequest) (*registryv1.ListModelsResponse, error) {
			return &registryv1.ListModelsResponse{
				Models:     []*registryv1.Model{{Id: "m-1", Name: "fraud"}},
				Pagination: &commonv1.PaginationResponse{TotalCount: 1},
			}, nil
		},
	}
	mh := NewModelsHandler(reg, testLogger())

	r := requestWithToken(http.MethodGet, "/api/v1/models", "tok-xyz", nil)
	w := serveThroughAuth(mh.List, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	// THE CORE ASSERTION: the downstream saw the forwarded, verbatim token.
	if reg.capturedAuthz != "Bearer tok-xyz" {
		t.Errorf("forwarded authz = %q, want %q", reg.capturedAuthz, "Bearer tok-xyz")
	}
	// JSON shape: models[] present with lowerCamelCase fields.
	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("response not JSON: %v", err)
	}
	if _, ok := got["models"]; !ok {
		t.Errorf("expected 'models' key, got keys: %v", keys(got))
	}
}

// ============================================================================
// STATUS-CODE MAPPING: gRPC codes -> HTTP statuses (table-driven).
// ============================================================================

func TestStatusCodeMapping_ModelsGet(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		grpcCode codes.Code
		wantHTTP int
	}{
		{"not found -> 404", codes.NotFound, http.StatusNotFound},
		{"permission denied -> 403", codes.PermissionDenied, http.StatusForbidden},
		{"unauthenticated -> 401", codes.Unauthenticated, http.StatusUnauthorized},
		{"invalid argument -> 400", codes.InvalidArgument, http.StatusBadRequest},
		{"already exists -> 409", codes.AlreadyExists, http.StatusConflict},
		{"internal -> 500", codes.Internal, http.StatusInternalServerError},
		{"unavailable -> 503", codes.Unavailable, http.StatusServiceUnavailable},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reg := &mockRegistry{
				getModelFn: func(_ context.Context, _ *registryv1.GetModelRequest) (*registryv1.GetModelResponse, error) {
					return nil, status.Error(tc.grpcCode, "downstream says no")
				},
			}
			mh := NewModelsHandler(reg, testLogger())
			r := requestWithToken(http.MethodGet, "/api/v1/models/m-1", "tok", nil)
			r.SetPathValue("id", "m-1")
			w := httptest.NewRecorder()
			mh.Get(w, r)
			if w.Code != tc.wantHTTP {
				t.Fatalf("grpc %s -> http %d, want %d", tc.grpcCode, w.Code, tc.wantHTTP)
			}
			// 5xx must never leak the downstream message.
			if tc.wantHTTP >= 500 && strings.Contains(w.Body.String(), "downstream says no") {
				t.Errorf("5xx leaked internal detail: %s", w.Body.String())
			}
		})
	}
}

// ============================================================================
// REGISTER: POST body -> proto, 201 on success.
// ============================================================================

func TestRegisterModel_Returns201(t *testing.T) {
	t.Parallel()
	var got *registryv1.RegisterModelRequest
	reg := &mockRegistry{
		registerModelFn: func(_ context.Context, in *registryv1.RegisterModelRequest) (*registryv1.RegisterModelResponse, error) {
			got = in
			return &registryv1.RegisterModelResponse{Model: &registryv1.Model{Id: "m-9", Name: in.GetName()}}, nil
		},
	}
	mh := NewModelsHandler(reg, testLogger())
	body := bytes.NewBufferString(`{"name":"fraud-v2","framework":"sklearn","taskType":"classification"}`)
	r := requestWithToken(http.MethodPost, "/api/v1/models", "tok", body)
	w := serveThroughAuth(mh.Register, r)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body.String())
	}
	if got.GetName() != "fraud-v2" || got.GetFramework() != "sklearn" {
		t.Errorf("body not mapped to proto: %+v", got)
	}
	if reg.capturedAuthz != "Bearer tok" {
		t.Errorf("token not forwarded on register: %q", reg.capturedAuthz)
	}
}

func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
