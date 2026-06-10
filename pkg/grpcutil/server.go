package grpcutil

import (
	"context"
	"log/slog"
	"net"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
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
//   1. Standard interceptor chains (unary AND stream — kept at parity)
//   2. Distributed tracing via OpenTelemetry (otelgrpc stats handler)
//   3. gRPC health service (K8s readiness/liveness probes)
//   4. gRPC reflection (grpcurl/grpcui can discover services without .proto)
//   5. Graceful shutdown + readiness status control
//
// INTERCEPTOR ORDER (outermost → innermost): recovery → logging → auth → custom
//   - Recovery outermost: catches panics from everything below.
//   - Logging next: logs every RPC including auth failures (security audit).
//   - Auth innermost (of the standard chain): only authenticated RPCs reach
//     the handler / custom interceptors.
//
// WHERE TRACING LIVES (and why it's NOT an interceptor):
//   otelgrpc moved from interceptors to a grpc.StatsHandler. The stats handler
//   sees the RPC lifecycle earlier and more completely than an interceptor can
//   (including transport-level events), so it produces more accurate spans and
//   correctly extracts the W3C traceparent the client propagated. It applies to
//   unary AND streaming automatically. That's why you won't find a
//   "tracing interceptor" here — tracing is wired via grpc.StatsHandler below.
//
// FUNCTIONAL OPTIONS PATTERN (Dave Cheney): variadic option funcs give us
// sensible zero-values, order-independence, and backward-compatible extension.
// Used by gRPC itself, OTel, and most Go libraries.
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

// WithAuthValidator enables the auth interceptors with the given validator.
// If not set, no auth interceptors are added (useful for tests).
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
//	grpcurl -plaintext localhost:9090 list
//	grpcurl -plaintext localhost:9090 describe forgepoint.auth.v1.AuthService
func WithReflection() ServerOption {
	return func(cfg *serverConfig) {
		cfg.enableReflect = true
	}
}

// WithUnaryInterceptors adds custom unary interceptors that run AFTER the
// standard chain (recovery → logging → auth). Use for service-specific
// interceptors like rate limiting or request validation — they run
// post-authentication, so they can read Claims via ClaimsFromContext.
func WithUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) ServerOption {
	return func(cfg *serverConfig) {
		cfg.extraUnaryInt = append(cfg.extraUnaryInt, interceptors...)
	}
}

// WithStreamInterceptors adds custom stream interceptors that run AFTER the
// standard stream chain (recovery → logging → auth).
func WithStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) ServerOption {
	return func(cfg *serverConfig) {
		cfg.extraStreamInt = append(cfg.extraStreamInt, interceptors...)
	}
}

// Server bundles the gRPC server with its health server and provides graceful
// shutdown. Register your service handlers on the embedded GRPC field.
//
// WHY a wrapper instead of returning *grpc.Server directly:
//
//	Two things every service needs can't be done through a bare *grpc.Server:
//	(1) flipping the gRPC health status to SERVING only once dependencies are
//	ready (a bare server reports SERVING immediately, so probes pass before the
//	service can actually serve), and (2) a single Serve(ctx) that drains
//	in-flight RPCs on SIGTERM. The wrapper owns both.
type Server struct {
	// GRPC is the underlying server. Register handlers on it:
	//   authpb.RegisterAuthServiceServer(srv.GRPC, handler)
	GRPC *grpc.Server

	health *health.Server
	logger *slog.Logger
}

// NewServer creates a Server with the standard interceptor chains, tracing,
// health service, and the given options.
//
// The gRPC health status starts as NOT_SERVING. The service must call
// SetServing(true) once its dependencies (DB, NATS, etc.) are ready — this is
// what makes a gRPC readiness probe meaningful instead of always-green.
//
// USAGE:
//
//	srv := grpcutil.NewServer(
//	    grpcutil.WithLogger(logger),
//	    grpcutil.WithAuthValidator(validator, "/forgepoint.auth.v1.AuthService/Login"),
//	    grpcutil.WithReflection(),
//	)
//	authpb.RegisterAuthServiceServer(srv.GRPC, handler)
//	srv.SetServing(true)           // dependencies are ready
//	srv.Serve(ctx, lis)            // blocks; drains on ctx cancel (SIGTERM)
func NewServer(opts ...ServerOption) *Server {
	cfg := &serverConfig{
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(cfg)
	}

	// Unary chain: recovery → logging → auth → custom.
	unary := []grpc.UnaryServerInterceptor{
		RecoveryUnaryInterceptor(),
		LoggingUnaryInterceptor(cfg.logger),
	}
	if cfg.validator != nil {
		unary = append(unary, AuthUnaryInterceptor(cfg.validator, WithSkipMethods(cfg.skipMethods...)))
	}
	unary = append(unary, cfg.extraUnaryInt...)

	// Stream chain: the same shape as unary, so streaming RPCs are equally
	// protected (recovery + logging + auth). This parity is the fix for the
	// previously-unauthenticated, panic-unsafe streaming path.
	stream := []grpc.StreamServerInterceptor{
		RecoveryStreamInterceptor(),
		LoggingStreamInterceptor(cfg.logger),
	}
	if cfg.validator != nil {
		stream = append(stream, AuthStreamInterceptor(cfg.validator, WithSkipMethods(cfg.skipMethods...)))
	}
	stream = append(stream, cfg.extraStreamInt...)

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(unary...),
		grpc.ChainStreamInterceptor(stream...),
		// Distributed tracing for unary + streaming. Creates a span per RPC and
		// continues the trace the caller propagated via the traceparent metadata.
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)

	// gRPC health service — K8s can probe it directly (since K8s 1.24):
	//   readinessProbe: { grpc: { port: 9090 } }
	// Start NOT_SERVING; the service flips it via SetServing(true) when ready.
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	if cfg.enableReflect {
		reflection.Register(grpcServer)
	}

	return &Server{
		GRPC:   grpcServer,
		health: healthServer,
		logger: cfg.logger,
	}
}

// SetServing flips the gRPC health status. Call SetServing(true) once
// dependencies are ready, and SetServing(false) to drain before shutdown.
func (s *Server) SetServing(serving bool) {
	status := healthpb.HealthCheckResponse_NOT_SERVING
	if serving {
		status = healthpb.HealthCheckResponse_SERVING
	}
	// Empty service name "" is the conventional overall-server status that
	// K8s gRPC probes check.
	s.health.SetServingStatus("", status)
}

// Serve starts the server on lis and blocks until the server stops or ctx is
// cancelled. On cancellation (typically SIGTERM in K8s) it marks the server
// NOT_SERVING — so in-flight readiness probes fail fast and the pod is pulled
// from the Service endpoints — then GracefulStop drains in-flight RPCs before
// returning.
//
// WHY GracefulStop over Stop: Stop hard-kills in-flight RPCs (clients see
// broken connections mid-call). GracefulStop stops accepting new RPCs and waits
// for active ones to finish — the correct behavior for a K8s rolling update,
// which gives a 30s termination grace period for exactly this.
func (s *Server) Serve(ctx context.Context, lis net.Listener) error {
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- s.GRPC.Serve(lis)
	}()

	select {
	case err := <-serveErr:
		// Server stopped on its own (listener error, etc.).
		return err
	case <-ctx.Done():
		s.logger.Info("shutting down gRPC server", slog.String("reason", ctx.Err().Error()))
		s.SetServing(false)
		s.GRPC.GracefulStop()
		// GracefulStop unblocks Serve, which returns nil after a clean drain.
		return <-serveErr
	}
}
