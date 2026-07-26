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

// maxStackLen is the maximum number of bytes we log from a panic stack trace.
// debug.Stack() can return megabytes for deep call stacks (e.g., deeply nested
// goroutines). Logging that verbatim floods Loki and can hit slog's line-length
// limits. 4096 bytes is enough to see the top ~20 frames — the ones that matter
// for diagnosing the panic origin — without drowning the log pipeline.
const maxStackLen = 4096

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
				// Cap stack trace to avoid flooding Loki. debug.Stack() can return
				// megabytes for deep stacks; we only need the top frames.
				stack := debug.Stack()
				if len(stack) > maxStackLen {
					stack = stack[:maxStackLen]
				}
				slog.ErrorContext(ctx, "panic recovered in gRPC handler",
					slog.String("method", info.FullMethod),
					slog.Any("panic", r),
					slog.String("stack", string(stack)),
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

		// Include the error when the RPC failed — a "code=Internal" access log
		// with no error detail gives an on-call engineer nothing to act on.
		//
		// SANITIZATION:
		//   We log st.Message() for gRPC status errors, not err.Error(). The raw
		//   err.Error() string is "rpc error: code = X desc = Y" — verbose and
		//   already contains the code we logged separately. More importantly, if
		//   the message contains internal detail (host names, stack traces), it is
		//   sanitized at the authenticate() level (Fix 4a) before it reaches here.
		//
		//   For plain Go errors (not gRPC statuses): log a constant string rather
		//   than err.Error() because raw error strings can embed payload content or
		//   PII (e.g., "failed processing user email=alice@example.com"). The error
		//   TYPE is still useful for debugging patterns in Loki.
		attrs := []slog.Attr{
			slog.String("grpc.method", info.FullMethod),
			slog.String("grpc.code", code.String()),
			slog.Duration("grpc.duration", duration),
		}
		if err != nil {
			attrs = append(attrs, slog.String("grpc.error", sanitizeGRPCError(err)))
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
// ERROR NORMALIZATION:
//
//	A validator can fail for two very different reasons:
//	  1. The token is genuinely bad → Unauthenticated (client's fault).
//	  2. The auth backend is DOWN → codes.Internal/Unavailable (our fault).
//
//	We PRESERVE the gRPC code (so SLO alerting fires on the correct signal)
//	but SANITIZE the message for non-client-facing codes:
//	  - Unauthenticated / PermissionDenied: pass the message through — these
//	    are client-facing by design ("invalid token", "not authorized for X").
//	  - Any other code (Internal, Unavailable, etc.): replace the message with
//	    "authentication service error". The raw message can contain internal
//	    topology (DB hosts, IP addresses, stack traces) that must not reach
//	    clients. The real message is still logged internally by the logging
//	    interceptor (which runs before auth in the chain).
//
// TRADEOFF: We lose the specific internal message at the client boundary
// (security win). Internal diagnosis uses service logs, not client errors.
func authenticate(ctx context.Context, validator TokenValidator) (context.Context, error) {
	token, err := extractBearerToken(ctx)
	if err != nil {
		return ctx, err
	}

	claims, err := validator.Validate(ctx, token)
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			// Plain Go error (not a gRPC status) → treat as bad token.
			return ctx, status.Error(codes.Unauthenticated, "invalid token")
		}

		// gRPC status: preserve the code, but sanitize the message for
		// codes that expose internal infrastructure detail.
		switch st.Code() {
		case codes.Unauthenticated, codes.PermissionDenied:
			// Client-facing codes: pass the message through unchanged.
			// "token signature mismatch" is useful feedback for the client.
			return ctx, err
		default:
			// Internal, Unavailable, etc.: preserve the code for SLO alerting
			// but replace the message with a generic string so internal details
			// (DB hosts, stack traces, service URLs) do not reach the client.
			return ctx, status.Error(st.Code(), "authentication service error")
		}
	}

	return contextWithClaims(ctx, claims), nil
}

// extractBearerToken extracts the token from the "authorization" metadata.
// Returns Unauthenticated error if missing or malformed.
//
// RFC 6750 §2.1 specifies "Authorization: Bearer <token>". The scheme name
// ("Bearer") is case-insensitive per RFC 7235 §3.1 which defines the
// Authorization header grammar. Many clients send "bearer" (lowercase) or
// "BEARER" (uppercase) — a case-sensitive check incorrectly rejects them.
//
// MULTIPLE HEADERS REJECTION:
//
//	gRPC metadata allows multiple values per key. Two "authorization" headers
//	is ambiguous: different layers of the stack may pick the first or the last
//	header, enabling header-confusion attacks (e.g., an attacker appends a
//	second valid token after a WAF has already validated the first). We reject
//	any request with len(values) > 1 to eliminate this ambiguity entirely.
func extractBearerToken(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing metadata")
	}

	values := md.Get("authorization")
	if len(values) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing authorization header")
	}

	// Reject ambiguous requests with more than one authorization header.
	// This eliminates header-confusion attacks where different stack layers
	// parse different header values. The legitimate case has exactly one header.
	if len(values) > 1 {
		return "", status.Error(codes.Unauthenticated,
			"multiple authorization headers not allowed")
	}

	// Case-insensitive Bearer scheme check per RFC 6750 / RFC 7235.
	// strings.ToLower on just the first 7 characters avoids allocating a
	// lowercase copy of the entire (potentially large) auth string.
	auth := values[0]
	const schemeLen = 7 // len("bearer ")
	if len(auth) < schemeLen || strings.ToLower(auth[:schemeLen]) != "bearer " {
		return "", status.Error(codes.Unauthenticated,
			"invalid authorization format, expected 'Bearer <token>'")
	}

	token := auth[schemeLen:]
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
// Stack trace is capped to maxStackLen bytes (same as the unary version).
func RecoveryStreamInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) (err error) {
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()
				if len(stack) > maxStackLen {
					stack = stack[:maxStackLen]
				}
				slog.ErrorContext(ss.Context(), "panic recovered in gRPC stream handler",
					slog.String("method", info.FullMethod),
					slog.Any("panic", r),
					slog.String("stack", string(stack)),
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

// ============================================================================
// HEALTH + REFLECTION SKIP LIST (CANONICAL, SHARED ACROSS ALL SERVICES)
// ============================================================================

// HealthAndReflectionMethods returns the gRPC full-method strings that every
// Forgepoint service MUST exempt from authentication.
//
// WHY these four methods (and nothing else):
//
//   - /grpc.health.v1.Health/Check: Kubernetes liveness/readiness probes call
//     this from the kubelet — no credential. If auth intercepts it the pod never
//     becomes Ready (fails its own probe → restarts → fails again, forever).
//
//   - /grpc.health.v1.Health/Watch: The streaming variant of Check. A K8s probe
//     may use either form; both must be open.
//
//   - /grpc.reflection.v1.ServerReflection/ServerReflectionInfo: grpcurl and
//     grpcui call this to discover services and methods in dev/staging. When
//     reflection is enabled (WithReflection()), it must be exempt or discovery
//     breaks entirely (you'd need a token just to list available RPCs).
//
//   - /grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo: The
//     older v1alpha reflection endpoint. Some clients (older grpcurl, Postman)
//     still use this. Both must be exempt for full compatibility.
//
// WHERE THIS FUNCTION LIVES (and why here, not in services/auth/internal/authn):
//
//	services/auth/internal/authn/skip.go defines an identical HealthAndReflection-
//	Methods() locally. That was the right place BEFORE this shared package
//	existed — but it means every service would need to either import auth's
//	internal package (a Clean Architecture violation) or duplicate the list.
//
//	By moving the canonical copy here, into the package that already owns
//	the skip-list mechanism (WithSkipMethods, AuthOption), we give every
//	service a single import-free source of truth:
//
//	  grpcutil.WithAuthValidator(validator,
//	      append(servicePublicMethods, grpcutil.HealthAndReflectionMethods()...)...,
//	  )
//
//	The auth service will adopt this in a follow-up (the local copy in authn/
//	can delegate to or be replaced by this function without any call-site change).
//
// WHY THE HEALTH ENDPOINT IS UNAUTHENTICATED:
//
//	K8s probes originate from the kubelet process on the node — there is no
//	mechanism to pass a credential. The health endpoint's threat model is
//	availability (anyone can see SERVING/NOT_SERVING), not confidentiality.
//	Requiring auth there would make the service undeployable in Kubernetes.
func HealthAndReflectionMethods() []string {
	return []string{
		"/grpc.health.v1.Health/Check",
		"/grpc.health.v1.Health/Watch",
		"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
		"/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo",
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

// sanitizeGRPCError returns a safe string to log for an RPC error.
//
// WHY: err.Error() for a gRPC status is "rpc error: code = X desc = Y".
// That format is verbose and duplicates the code we already log separately.
// More critically, Y (the description) may contain PII or internal details
// if it came from a handler that didn't sanitize its errors.
//
// STRATEGY:
//   - gRPC status error: log st.Message() only (the desc part, already
//     sanitized at the authenticate() boundary for auth errors).
//   - Plain Go error: log a constant. The raw error string can embed payload
//     content (e.g. "invalid user email=alice@corp.com") and must not be
//     emitted to a shared log store like Loki.
//
// TRADEOFF: We lose raw error strings in logs. Engineers needing full error
// detail should add structured attributes in the handler itself, where they
// control what is logged and can redact PII explicitly.
func sanitizeGRPCError(err error) string {
	if st, ok := status.FromError(err); ok {
		msg := st.Message()
		if msg == "" {
			return st.Code().String()
		}
		return msg
	}
	// Non-status error: log a constant rather than the raw string.
	// The error type/code is already captured in grpc.code above.
	return "non-status error"
}
