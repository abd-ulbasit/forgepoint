package grpcutil_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ============================================================================
// INTERCEPTOR TESTS
// ============================================================================
//
// WHY test interceptors in isolation:
//   Interceptors are cross-cutting concerns that run on EVERY RPC. A bug in
//   an interceptor affects all handlers in all services. Testing them in
//   isolation (without a real gRPC server) is faster and more precise.
//
// HOW gRPC interceptors work:
//   An interceptor wraps the handler function. It receives:
//   - ctx: request context (with metadata, deadlines, etc.)
//   - req: the proto request message
//   - info: method name, server
//   - handler: the next interceptor or the actual handler
//
//   The interceptor can:
//   - Modify context (inject auth claims, add logging fields)
//   - Short-circuit (return error without calling handler — e.g., auth failure)
//   - Wrap the handler call (measure duration, catch panics)
//
//   Chain order matters: recovery → logging → tracing → auth
//   Recovery is outermost so it catches panics from ANY inner interceptor.
//   Logging is before auth so auth failures are logged.
//   Auth is innermost so only authenticated requests reach the handler.
// ============================================================================

// fakeUnaryHandler is a mock gRPC handler for testing interceptors.
// It records whether it was called and can return a configurable response.
func fakeUnaryHandler(resp any, err error) grpc.UnaryHandler {
	return func(ctx context.Context, req any) (any, error) {
		return resp, err
	}
}

// panicHandler simulates a handler that panics — used to test recovery interceptor.
func panicHandler() grpc.UnaryHandler {
	return func(ctx context.Context, req any) (any, error) {
		panic("something went terribly wrong")
	}
}

// ============================================================================
// RECOVERY INTERCEPTOR
// ============================================================================
//
// WHY: A panic in a gRPC handler kills the goroutine serving that request.
// Without recovery, the client hangs (no response sent, connection may break).
// The recovery interceptor catches panics, logs the stack trace, and returns
// a proper gRPC Internal error — the server stays healthy.
//
// HOW UBER DOES IT: go-grpc-middleware/recovery — same pattern, we inline it
// to avoid the dependency.
// ============================================================================

func TestRecoveryInterceptor_NoPanic(t *testing.T) {
	interceptor := grpcutil.RecoveryUnaryInterceptor()

	resp, err := interceptor(
		context.Background(),
		"request",
		&grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"},
		fakeUnaryHandler("response", nil),
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != "response" {
		t.Errorf("resp = %v, want %q", resp, "response")
	}
}

func TestRecoveryInterceptor_CatchesPanic(t *testing.T) {
	interceptor := grpcutil.RecoveryUnaryInterceptor()

	resp, err := interceptor(
		context.Background(),
		"request",
		&grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"},
		panicHandler(),
	)

	// Should return an error, not re-panic
	if err == nil {
		t.Fatal("expected error from panic, got nil")
	}
	if resp != nil {
		t.Errorf("resp = %v, want nil", resp)
	}

	// Error should be gRPC Internal status
	st, ok := status.FromError(err)
	if !ok {
		t.Fatal("error is not a gRPC status")
	}
	if st.Code() != codes.Internal {
		t.Errorf("code = %v, want Internal", st.Code())
	}
}

// ============================================================================
// LOGGING INTERCEPTOR
// ============================================================================
//
// WHY: Every RPC call should be logged with: method, duration, status code.
// This is the gRPC equivalent of HTTP access logs. Without it, debugging
// production issues requires adding ad-hoc logging to every handler.
//
// The interceptor logs AFTER the handler completes, so duration and status
// are accurate. It uses slog for structured JSON output.
// ============================================================================

func TestLoggingInterceptor_LogsMethodAndStatus(t *testing.T) {
	// Capture log output
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	interceptor := grpcutil.LoggingUnaryInterceptor(logger)

	_, _ = interceptor(
		context.Background(),
		"request",
		&grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"},
		fakeUnaryHandler("response", nil),
	)

	// Parse the logged JSON
	var logEntry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("failed to parse log: %v (raw: %s)", err, buf.String())
	}

	// Verify method is logged
	if method, ok := logEntry["grpc.method"]; !ok || method != "/test.Service/Method" {
		t.Errorf("grpc.method = %v, want /test.Service/Method", method)
	}

	// Verify status code is logged
	if code, ok := logEntry["grpc.code"]; !ok || code != "OK" {
		t.Errorf("grpc.code = %v, want OK", code)
	}
}

func TestLoggingInterceptor_LogsErrors(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	interceptor := grpcutil.LoggingUnaryInterceptor(logger)

	_, _ = interceptor(
		context.Background(),
		"request",
		&grpc.UnaryServerInfo{FullMethod: "/test.Service/Fail"},
		fakeUnaryHandler(nil, status.Error(codes.NotFound, "not found")),
	)

	var logEntry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("failed to parse log: %v", err)
	}

	if code, ok := logEntry["grpc.code"]; !ok || code != "NotFound" {
		t.Errorf("grpc.code = %v, want NotFound", code)
	}
}

// ============================================================================
// AUTH INTERCEPTOR
// ============================================================================
//
// WHY: Most RPCs require authentication. Instead of each handler extracting
// and validating tokens, the auth interceptor does it once, injects claims
// into the context, and short-circuits with Unauthenticated if invalid.
//
// HOW IT WORKS:
//   1. Extract "authorization" from gRPC metadata (equivalent to HTTP header)
//   2. Strip "Bearer " prefix
//   3. Call TokenValidator.Validate(token) — returns claims or error
//   4. If valid: inject claims into context, call handler
//   5. If invalid: return codes.Unauthenticated immediately
//
// SKIPPABLE METHODS:
//   Some RPCs don't require auth (health checks, reflection, Login).
//   The interceptor accepts a skip list to bypass auth for these methods.
// ============================================================================

// mockValidator implements grpcutil.TokenValidator for testing.
type mockValidator struct {
	claims *grpcutil.Claims
	err    error
}

func (m *mockValidator) Validate(ctx context.Context, token string) (*grpcutil.Claims, error) {
	return m.claims, m.err
}

func TestAuthInterceptor_ValidToken(t *testing.T) {
	validator := &mockValidator{
		claims: &grpcutil.Claims{
			UserID: "user-123",
			Email:  "test@fp.io",
			Role:   "engineer",
		},
	}

	interceptor := grpcutil.AuthUnaryInterceptor(validator)

	// Set authorization metadata
	md := metadata.Pairs("authorization", "Bearer valid-token")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	handlerCalled := false
	handler := func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		// Verify claims are in context
		claims, ok := grpcutil.ClaimsFromContext(ctx)
		if !ok {
			t.Error("claims not found in context")
		}
		if claims.UserID != "user-123" {
			t.Errorf("UserID = %q, want %q", claims.UserID, "user-123")
		}
		return "ok", nil
	}

	resp, err := interceptor(ctx, "request",
		&grpc.UnaryServerInfo{FullMethod: "/test.Service/SecureMethod"},
		handler,
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handlerCalled {
		t.Error("handler was not called")
	}
	if resp != "ok" {
		t.Errorf("resp = %v, want ok", resp)
	}
}

func TestAuthInterceptor_MissingToken(t *testing.T) {
	validator := &mockValidator{}

	interceptor := grpcutil.AuthUnaryInterceptor(validator)

	// No authorization metadata
	_, err := interceptor(
		context.Background(),
		"request",
		&grpc.UnaryServerInfo{FullMethod: "/test.Service/SecureMethod"},
		fakeUnaryHandler("should not reach", nil),
	)

	if err == nil {
		t.Fatal("expected error for missing token")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unauthenticated {
		t.Errorf("code = %v, want Unauthenticated", st.Code())
	}
}

func TestAuthInterceptor_InvalidToken(t *testing.T) {
	validator := &mockValidator{
		err: status.Error(codes.Unauthenticated, "invalid token"),
	}

	interceptor := grpcutil.AuthUnaryInterceptor(validator)

	md := metadata.Pairs("authorization", "Bearer bad-token")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	_, err := interceptor(ctx, "request",
		&grpc.UnaryServerInfo{FullMethod: "/test.Service/SecureMethod"},
		fakeUnaryHandler("should not reach", nil),
	)

	if err == nil {
		t.Fatal("expected error for invalid token")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unauthenticated {
		t.Errorf("code = %v, want Unauthenticated", st.Code())
	}
}

func TestAuthInterceptor_SkippedMethod(t *testing.T) {
	validator := &mockValidator{
		err: status.Error(codes.Unauthenticated, "should not be called"),
	}

	// Health check should be skipped
	interceptor := grpcutil.AuthUnaryInterceptor(validator,
		grpcutil.WithSkipMethods("/grpc.health.v1.Health/Check"),
	)

	handlerCalled := false
	handler := func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return "healthy", nil
	}

	resp, err := interceptor(
		context.Background(),
		"request",
		&grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"},
		handler,
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handlerCalled {
		t.Error("handler was not called for skipped method")
	}
	if resp != "healthy" {
		t.Errorf("resp = %v, want healthy", resp)
	}
}
