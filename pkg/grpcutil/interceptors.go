// Package grpcutil provides a gRPC server factory with a standard interceptor
// chain and functional options for configuration.
//
// ============================================================================
// gRPC INTERCEPTOR CHAIN
// ============================================================================
//
// WHY interceptors:
//
//	gRPC interceptors are the equivalent of HTTP middleware. They provide
//	cross-cutting concerns (logging, auth, tracing, recovery) that apply to
//	every RPC without modifying handler code. Without interceptors, every
//	handler would need to: check auth, log request, recover from panics,
//	start trace spans — that's 20+ lines of boilerplate per handler.
//
// CHAIN ORDER: recovery → logging → tracing → auth → handler
//
//	┌──────────────────────────────────────────────────────┐
//	│  Incoming RPC Request                                 │
//	│  ┌─────────────┐                                      │
//	│  │  Recovery    │ ← Outermost: catches panics from    │
//	│  │             │    ALL inner interceptors + handler   │
//	│  │  ┌─────────┐│                                      │
//	│  │  │ Logging  ││ ← Logs method, duration, status     │
//	│  │  │         ││    (even for auth failures)          │
//	│  │  │ ┌──────┐││                                      │
//	│  │  │ │Trace │││ ← Creates span, propagates trace_id │
//	│  │  │ │      │││                                      │
//	│  │  │ │┌────┐│││                                      │
//	│  │  │ ││Auth│││ ← Validates token, injects claims    │
//	│  │  │ ││    │││    (short-circuits if invalid)        │
//	│  │  │ │└────┘│││                                      │
//	│  │  │ └──────┘││                                      │
//	│  │  └─────────┘│                                      │
//	│  └─────────────┘                                      │
//	│  → Handler (your business logic)                      │
//	└──────────────────────────────────────────────────────┘
//
// WHY this order:
//   - Recovery MUST be outermost: if auth panics, we still want to catch it
//   - Logging before auth: we want to log auth failures (for security audit)
//   - Tracing before auth: auth spans show up in traces for debugging
//   - Auth innermost: only authenticated requests reach the handler
//
// HOW UBER DOES IT: go-grpc-middleware v2 provides the same chain pattern.
// We inline it to keep the dependency footprint minimal.
//
// ============================================================================
package grpcutil

import (
	"context"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ============================================================================
// CLAIMS + CONTEXT
// ============================================================================
//
// Claims are extracted from the JWT or API key by the auth interceptor and
// injected into the request context. Handlers retrieve them via
// ClaimsFromContext(ctx) without knowing HOW authentication happened.
//
// This is the CLEAN ARCHITECTURE boundary: the handler layer knows about
// Claims (a domain concept), not about JWTs or Bearer tokens (transport).
// ============================================================================

// Claims represents the authenticated user's identity and permissions.
// Extracted from JWT tokens or API keys by the auth interceptor.
type Claims struct {
	UserID string
	Email  string
	Team   string
	Role   string
	Scopes []string
}

// claimsKey is the context key for storing Claims.
// Using a private type prevents collisions with other packages.
type claimsContextKey struct{}

// ClaimsFromContext extracts Claims from the request context.
// Returns the claims and true if found, or nil and false if the request
// is unauthenticated (e.g., skipped method like health check).
func ClaimsFromContext(ctx context.Context) (*Claims, bool) {
	claims, ok := ctx.Value(claimsContextKey{}).(*Claims)
	return claims, ok
}

// contextWithClaims injects Claims into the context.
func contextWithClaims(ctx context.Context, claims *Claims) context.Context {
	return context.WithValue(ctx, claimsContextKey{}, claims)
}

// ============================================================================
// TOKEN VALIDATOR INTERFACE
// ============================================================================
//
// WHY an interface instead of a concrete JWT validator:
//   The grpcutil package is shared by ALL services. The actual token
//   validation logic lives in the auth service. Other services call the
//   auth service's ValidateToken RPC (or validate JWTs locally with a
//   shared secret). By using an interface, each service provides its own
//   implementation without grpcutil depending on auth internals.
//
//   This is Dependency Inversion (the D in SOLID): high-level module
//   (grpcutil) depends on an abstraction (interface), not a concrete
//   implementation.
// ============================================================================

// TokenValidator validates authentication tokens and returns claims.
// Implementations include:
//   - Local JWT validation (for services that have the JWT secret)
//   - Remote validation via auth service gRPC call
//   - API key validation via auth service gRPC call
type TokenValidator interface {
	Validate(ctx context.Context, token string) (*Claims, error)
}

// ============================================================================
// RECOVERY INTERCEPTOR
// ============================================================================

// RecoveryUnaryInterceptor catches panics in handlers and returns a gRPC
// Internal error instead of crashing the server.
//
// WHY: Go panics are unrecoverable by default — they kill the goroutine.
// In a gRPC server, each RPC runs in its own goroutine. A panic crashes
// that goroutine, leaving the client hanging (no response, eventual timeout).
// Worse: if the panic happens in a shared resource (e.g., map write without
// mutex), it can crash the entire process.
//
// HOW: Go's recover() function catches panics when called from a deferred
// function. We defer the recovery, catch any panic value, log the stack
// trace, and return a proper gRPC error. The server continues serving other
// requests.
//
// FAILURE MODE: If the panic corrupted shared state (e.g., a sync.Map),
// subsequent requests may also fail. The health check should detect this
// (readiness fails → K8s stops routing traffic → pod restarts).
func RecoveryUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				// Log the panic with stack trace for debugging.
				// In production, this goes to Loki via stdout JSON.
				slog.ErrorContext(ctx, "panic recovered in gRPC handler",
					slog.String("method", info.FullMethod),
					slog.Any("panic", r),
					slog.String("stack", string(debug.Stack())),
				)
				// Return gRPC Internal error — client gets a proper error response
				// instead of a hanging connection.
				err = status.Errorf(codes.Internal, "internal server error")
			}
		}()

		return handler(ctx, req)
	}
}

// ============================================================================
// LOGGING INTERCEPTOR
// ============================================================================

// LoggingUnaryInterceptor logs every RPC call with method, duration, and
// status code using structured JSON logging.
//
// WHY: This is the gRPC equivalent of an HTTP access log. Every request/
// response is logged with:
//   - Method: which RPC was called
//   - Duration: how long it took (for latency analysis)
//   - Code: gRPC status code (for error rate calculation)
//
// These logs feed into Loki, where you can query:
//
//	{service="auth"} | json | grpc_code="NotFound"
//
// PERFORMANCE: slog is allocation-efficient. The JSON handler pre-allocates
// buffers. Logging adds ~1-2 microseconds per RPC — negligible vs network.
func LoggingUnaryInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		start := time.Now()

		resp, err := handler(ctx, req)

		duration := time.Since(start)
		code := status.Code(err)
		level := logLevelForCode(code)

		// Include the error message itself when the RPC failed — a "code=Internal"
		// access log with no message gives an on-call engineer nothing to act on.
		attrs := []slog.Attr{
			slog.String("grpc.method", info.FullMethod),
			slog.String("grpc.code", code.String()),
			slog.Duration("grpc.duration", duration),
		}
		if err != nil {
			attrs = append(attrs, slog.String("grpc.error", err.Error()))
		}

		logger.LogAttrs(ctx, level, "gRPC request", attrs...)

		return resp, err
	}
}

// ============================================================================
// AUTH INTERCEPTOR
// ============================================================================

// AuthOption configures the auth interceptor.
type AuthOption func(*authConfig)

type authConfig struct {
	skipMethods map[string]bool
}

// WithSkipMethods specifies gRPC methods that bypass authentication.
// Common skip targets: health checks, gRPC reflection, Login RPC.
//
// WHY skip instead of "require" list:
//
//	Default-deny is more secure. New RPCs are automatically protected.
//	A "require" list risks forgetting to add a new sensitive RPC.
//	Skip list is explicit: you consciously exempt specific methods.
func WithSkipMethods(methods ...string) AuthOption {
	return func(cfg *authConfig) {
		for _, m := range methods {
			cfg.skipMethods[m] = true
		}
	}
}

// AuthUnaryInterceptor extracts bearer tokens from gRPC metadata, validates
// them via the TokenValidator, and injects Claims into the context.
//
// HOW gRPC METADATA WORKS:
//
//	gRPC metadata is the equivalent of HTTP headers. Clients set metadata
//	via grpc.WithPerRPCCredentials or metadata.AppendToOutgoingContext.
//	Servers read it via metadata.FromIncomingContext(ctx).
//
//	The "authorization" key follows HTTP convention:
//	  "authorization": "Bearer <token>"
//
// TOKEN FLOW:
//
//	Client → sets "authorization" metadata → Server interceptor
//	  → extracts token → calls TokenValidator.Validate()
//	  → if valid: injects Claims into ctx → handler receives Claims
//	  → if invalid: returns Unauthenticated immediately (handler never called)
func AuthUnaryInterceptor(validator TokenValidator, opts ...AuthOption) grpc.UnaryServerInterceptor {
	cfg := &authConfig{
		skipMethods: make(map[string]bool),
	}
	for _, opt := range opts {
		opt(cfg)
	}

	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		// Skip auth for exempted methods (health checks, Login, etc.)
		if cfg.skipMethods[info.FullMethod] {
			return handler(ctx, req)
		}

		authedCtx, err := authenticate(ctx, validator)
		if err != nil {
			return nil, err
		}
		return handler(authedCtx, req)
	}
}

// authenticate extracts the bearer token, validates it, and returns a context
// carrying the resulting Claims. Shared by the unary and stream interceptors.
//
// ERROR NORMALIZATION (this is the subtle, interview-worthy part):
//
//	A validator can fail for two very different reasons:
//	  1. The token is genuinely bad → Unauthenticated (client's fault).
//	  2. The auth backend is DOWN → the remote ValidateToken RPC returns
//	     codes.Internal/Unavailable (our fault, not the caller's).
//
//	The old code flattened BOTH to Unauthenticated, which would make a total
//	auth-service outage look like every client suddenly had invalid tokens —
//	masking the real incident and breaking error-rate/SLO alerting. So we
//	PRESERVE an error that is already a gRPC status (the validator chose its
//	code deliberately) and only wrap genuinely-unstructured errors as
//	Unauthenticated.
func authenticate(ctx context.Context, validator TokenValidator) (context.Context, error) {
	token, err := extractBearerToken(ctx)
	if err != nil {
		return ctx, err
	}

	claims, err := validator.Validate(ctx, token)
	if err != nil {
		// status.FromError reports ok==true only when err already carries a
		// gRPC status code; in that case pass it through untouched.
		if _, ok := status.FromError(err); ok {
			return ctx, err
		}
		return ctx, status.Error(codes.Unauthenticated, "invalid token")
	}

	return contextWithClaims(ctx, claims), nil
}

// extractBearerToken extracts the token from the "authorization" metadata.
// Returns Unauthenticated error if missing or malformed.
func extractBearerToken(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing metadata")
	}

	values := md.Get("authorization")
	if len(values) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing authorization header")
	}

	// Strip "Bearer " prefix (case-insensitive, per RFC 6750).
	auth := values[0]
	const prefix = "Bearer "
	if !strings.HasPrefix(auth, prefix) {
		return "", status.Error(codes.Unauthenticated,
			"invalid authorization format, expected 'Bearer <token>'")
	}

	token := strings.TrimPrefix(auth, prefix)
	if token == "" {
		return "", status.Error(codes.Unauthenticated, "empty token")
	}

	return token, nil
}

// ============================================================================
// STREAM INTERCEPTORS
// ============================================================================
//
// WHY a parallel set of interceptors:
//   gRPC has TWO interceptor types — unary (request/response) and stream
//   (long-lived bidirectional/server/client streams). They are wired
//   SEPARATELY: grpc.ChainUnaryInterceptor vs grpc.ChainStreamInterceptor.
//   Registering only unary interceptors leaves every streaming RPC with NO
//   recovery, NO logging, and — most dangerously — NO authentication.
//
//   This is not academic: Pipeline Orchestrator's WatchExecution is a
//   server-streaming RPC. Without these, it would ship unauthenticated and
//   crash its goroutine on any panic. Stream and unary must be kept at parity.
//
// THE ServerStream WRAPPER TRICK:
//   A unary interceptor can swap the context before calling the handler. A
//   stream interceptor CANNOT — the handler reads its context from
//   grpc.ServerStream.Context(), which we don't control. The idiom is to wrap
//   the ServerStream and override Context() to return our enriched context
//   (the one carrying Claims). The handler then transparently sees the
//   authenticated context. This is exactly how go-grpc-middleware does it.
// ============================================================================

// wrappedServerStream overrides Context() so an interceptor can inject values
// (e.g., Claims) that the streaming handler will see via ss.Context().
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStream) Context() context.Context { return w.ctx }

// RecoveryStreamInterceptor is the streaming counterpart of
// RecoveryUnaryInterceptor: it catches panics in streaming handlers and
// converts them to a gRPC Internal error instead of crashing the goroutine.
func RecoveryStreamInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) (err error) {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(ss.Context(), "panic recovered in gRPC stream handler",
					slog.String("method", info.FullMethod),
					slog.Any("panic", r),
					slog.String("stack", string(debug.Stack())),
				)
				err = status.Errorf(codes.Internal, "internal server error")
			}
		}()

		return handler(srv, ss)
	}
}

// LoggingStreamInterceptor logs each streaming RPC with method, total duration
// (open → close), and final status code. Duration here is the lifetime of the
// whole stream, not a single message.
func LoggingStreamInterceptor(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		start := time.Now()

		err := handler(srv, ss)

		code := status.Code(err)
		level := logLevelForCode(code)

		logger.LogAttrs(ss.Context(), level, "gRPC stream",
			slog.String("grpc.method", info.FullMethod),
			slog.String("grpc.code", code.String()),
			slog.Duration("grpc.duration", time.Since(start)),
		)

		return err
	}
}

// AuthStreamInterceptor authenticates streaming RPCs, injecting Claims into the
// stream's context via a wrappedServerStream. Mirrors AuthUnaryInterceptor.
func AuthStreamInterceptor(validator TokenValidator, opts ...AuthOption) grpc.StreamServerInterceptor {
	cfg := &authConfig{skipMethods: make(map[string]bool)}
	for _, opt := range opts {
		opt(cfg)
	}

	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		if cfg.skipMethods[info.FullMethod] {
			return handler(srv, ss)
		}

		authedCtx, err := authenticate(ss.Context(), validator)
		if err != nil {
			return err
		}

		// Hand the handler a stream whose Context() returns the authenticated
		// context, so ClaimsFromContext works inside streaming handlers.
		return handler(srv, &wrappedServerStream{ServerStream: ss, ctx: authedCtx})
	}
}

// logLevelForCode maps a gRPC status code to a slog level. Shared by the unary
// and stream logging interceptors so their log levels stay consistent.
//
//	Internal/Unknown            → Error (bugs / infrastructure)
//	Unauthenticated/PermissionDenied → Warn  (potential security events)
//	everything else             → Info  (normal operation, incl. NotFound)
func logLevelForCode(code codes.Code) slog.Level {
	switch code {
	case codes.Internal, codes.Unknown:
		return slog.LevelError
	case codes.Unauthenticated, codes.PermissionDenied:
		return slog.LevelWarn
	default:
		return slog.LevelInfo
	}
}
