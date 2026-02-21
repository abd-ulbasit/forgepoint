package grpcutil

import (
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// ============================================================================
// gRPC SERVER FACTORY
// ============================================================================
//
// WHY a factory instead of raw grpc.NewServer():
//   Every service needs the same interceptor chain, health checks, and
//   reflection. Without a factory, each service's main.go would have 30+
//   lines of gRPC setup with subtle differences (e.g., one service forgets
//   recovery interceptor → panic crashes that service in production).
//
//   The factory provides:
//   1. Standard interceptor chain (recovery → logging → tracing → auth)
//   2. gRPC health service (K8s readiness/liveness probes)
//   3. gRPC reflection (grpcurl/grpcui can discover services without .proto)
//   4. Functional options for customization
//
// FUNCTIONAL OPTIONS PATTERN:
//   Go doesn't have default parameter values or overloading. The functional
//   options pattern (popularized by Dave Cheney) uses variadic functions:
//     NewServer(WithAuth(validator), WithReflection())
//
//   Benefits over config struct:
//   - Zero-value is sensible (no auth = no auth interceptor)
//   - Order-independent
//   - Backward compatible (add new options without changing existing callers)
//   - Self-documenting (option names describe what they do)
//
//   This pattern is used by: gRPC itself, OTel, most Go libraries.
//
// ============================================================================

// ServerOption configures a gRPC server.
type ServerOption func(*serverConfig)

type serverConfig struct {
	logger         *slog.Logger
	validator      TokenValidator
	skipMethods    []string
	enableReflect  bool
	extraUnaryInt  []grpc.UnaryServerInterceptor
	extraStreamInt []grpc.StreamServerInterceptor
}

// WithLogger sets the logger for the logging interceptor.
// If not set, a default slog logger is used.
func WithLogger(logger *slog.Logger) ServerOption {
	return func(cfg *serverConfig) {
		cfg.logger = logger
	}
}

// WithAuthValidator enables the auth interceptor with the given validator.
// If not set, no auth interceptor is added (useful for tests).
//
// WHY optional auth: Some services may have public RPCs (e.g., the auth
// service's Login RPC). The auth interceptor is still added but with
// skip methods for those public RPCs.
func WithAuthValidator(validator TokenValidator, skipMethods ...string) ServerOption {
	return func(cfg *serverConfig) {
		cfg.validator = validator
		cfg.skipMethods = skipMethods
	}
}

// WithReflection enables gRPC server reflection.
//
// WHY: Reflection lets clients (grpcurl, grpcui, Postman) discover
// available services and methods without having .proto files locally.
// Essential for debugging in dev. In production, you may disable it
// to reduce attack surface (service discovery is an info leak).
//
// HOW: The server registers a special reflection service that responds
// to introspection queries. grpcurl uses this to list services and methods:
//   grpcurl -plaintext localhost:9090 list
//   grpcurl -plaintext localhost:9090 describe goml.auth.v1.AuthService
func WithReflection() ServerOption {
	return func(cfg *serverConfig) {
		cfg.enableReflect = true
	}
}

// WithUnaryInterceptors adds custom unary interceptors that run AFTER the
// standard chain (recovery → logging → auth). Use for service-specific
// interceptors like rate limiting or request validation.
func WithUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) ServerOption {
	return func(cfg *serverConfig) {
		cfg.extraUnaryInt = append(cfg.extraUnaryInt, interceptors...)
	}
}

// WithStreamInterceptors adds custom stream interceptors.
func WithStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) ServerOption {
	return func(cfg *serverConfig) {
		cfg.extraStreamInt = append(cfg.extraStreamInt, interceptors...)
	}
}

// NewServer creates a gRPC server with the standard interceptor chain
// and the given options.
//
// The interceptor chain is built in this order (outermost to innermost):
//   1. Recovery — catches panics from everything below
//   2. Logging — logs method, duration, status for every RPC
//   3. Auth (if validator provided) — validates tokens, injects claims
//   4. Custom interceptors (if any)
//
// Additionally:
//   - gRPC health service is always registered (K8s probes need it)
//   - gRPC reflection is registered if WithReflection() is used
//
// RETURNS: A *grpc.Server ready to register service handlers and serve.
//
// USAGE:
//
//	srv := grpcutil.NewServer(
//	    grpcutil.WithLogger(logger),
//	    grpcutil.WithAuthValidator(validator, "/goml.auth.v1.AuthService/Login"),
//	    grpcutil.WithReflection(),
//	)
//	authpb.RegisterAuthServiceServer(srv, handler)
//	srv.Serve(lis)
func NewServer(opts ...ServerOption) *grpc.Server {
	cfg := &serverConfig{
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(cfg)
	}

	// Build the unary interceptor chain.
	// Order: recovery → logging → auth → custom
	var unaryInterceptors []grpc.UnaryServerInterceptor

	unaryInterceptors = append(unaryInterceptors, RecoveryUnaryInterceptor())
	unaryInterceptors = append(unaryInterceptors, LoggingUnaryInterceptor(cfg.logger))

	if cfg.validator != nil {
		unaryInterceptors = append(unaryInterceptors,
			AuthUnaryInterceptor(cfg.validator, WithSkipMethods(cfg.skipMethods...)),
		)
	}

	unaryInterceptors = append(unaryInterceptors, cfg.extraUnaryInt...)

	// Create the gRPC server with the chained interceptors.
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(unaryInterceptors...),
		grpc.ChainStreamInterceptor(cfg.extraStreamInt...),
	)

	// Register health service — used by K8s liveness/readiness probes.
	// K8s can probe gRPC health directly (since K8s 1.24) using:
	//   livenessProbe:
	//     grpc:
	//       port: 9090
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(srv, healthServer)

	// Register reflection — enables grpcurl/grpcui introspection.
	if cfg.enableReflect {
		reflection.Register(srv)
	}

	return srv
}
