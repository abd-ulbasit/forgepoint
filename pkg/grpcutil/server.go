package grpcutil

import (
	"context"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

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
// INTERCEPTOR ORDER (outermost → innermost):
//   recovery → logging → pre-auth → auth → custom
//   - Recovery outermost: catches panics from everything below.
//   - Logging next: logs every RPC including auth failures (security audit).
//   - Pre-auth (WithPreAuthUnaryInterceptors): WRAPS auth so it can observe auth's
//     own short-circuit denials — the slot the audit DENY-capture interceptor uses
//     (anything inner of auth never runs when auth rejects a request).
//   - Auth (of the standard chain): only authenticated RPCs reach the handler /
//     custom (post-auth) interceptors.
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

// defaultDrainTimeout is the maximum time GracefulStop is allowed to run before
// we hard-stop the server. 25s is chosen to be comfortably within a K8s
// terminationGracePeriodSeconds of 30s, leaving 5s for the container runtime
// to clean up after us.
const defaultDrainTimeout = 25 * time.Second

// ServerOption configures a gRPC server.
type ServerOption func(*serverConfig)

type serverConfig struct {
	logger           *slog.Logger
	validator        TokenValidator
	skipMethods      []string
	enableReflect    bool
	drainTimeout     time.Duration
	preAuthUnaryInt  []grpc.UnaryServerInterceptor
	preAuthStreamInt []grpc.StreamServerInterceptor
	extraUnaryInt    []grpc.UnaryServerInterceptor
	extraStreamInt   []grpc.StreamServerInterceptor
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

// WithPreAuthUnaryInterceptors adds custom unary interceptors that run BEFORE the
// auth interceptor — i.e. they WRAP auth (chain position:
// recovery → logging → THESE → auth → WithUnaryInterceptors → handler).
//
// WHY a distinct slot OUTSIDE auth: most cross-cutting interceptors want to run
// AFTER auth (so Claims are available) — that is WithUnaryInterceptors. But the
// audit DENY-capture interceptor must OBSERVE auth's own rejection: when auth
// short-circuits an unauthenticated/forbidden request, it returns WITHOUT calling
// its inner handler, so anything wired inner of auth never runs for that denial.
// Placing an interceptor here lets it see auth's Unauthenticated/PermissionDenied
// return and record it (the anonymous-probe audit signal). This is the ONLY case
// in the platform that needs to wrap auth; the option is deliberately separate
// from WithUnaryInterceptors so the common (post-auth) case stays the default.
func WithPreAuthUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) ServerOption {
	return func(cfg *serverConfig) {
		cfg.preAuthUnaryInt = append(cfg.preAuthUnaryInt, interceptors...)
	}
}

// WithPreAuthStreamInterceptors is the streaming counterpart of
// WithPreAuthUnaryInterceptors: stream interceptors that wrap the auth stream
// interceptor so they can capture auth's short-circuit denials on streaming RPCs.
func WithPreAuthStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) ServerOption {
	return func(cfg *serverConfig) {
		cfg.preAuthStreamInt = append(cfg.preAuthStreamInt, interceptors...)
	}
}

// WithDrainTimeout sets the maximum duration for GracefulStop to drain active
// RPCs before a hard Stop() is issued. The zero value uses defaultDrainTimeout
// (25 seconds).
//
// WHY THIS MATTERS FOR K8S ROLLING DEPLOYS:
//   When K8s sends SIGTERM, it gives the pod terminationGracePeriodSeconds (30s
//   by default) to finish up. Our Serve() catches ctx.Done(), marks the server
//   NOT_SERVING (so probes fail → pod leaves Service endpoints), then calls
//   GracefulStop. GracefulStop blocks until ALL in-flight RPCs finish — but a
//   single slow streaming RPC (e.g., a long-running WatchExecution with no
//   client deadline) can hold GracefulStop open indefinitely, causing the pod to
//   be SIGKILL'd mid-stream anyway (ugly) AND blocking the rolling update.
//
//   The bounded drain: GracefulStop races against the timeout. If the timeout
//   fires first, we hard-Stop (clients get UNAVAILABLE — reconnect-able) and
//   return. This ensures pods finish within terminationGracePeriodSeconds even
//   under pathological streaming clients.
//
//   TRADEOFF: A client that holds a stream open longer than the drain timeout
//   sees a hard disconnect. The alternative — no timeout — risks blocking the
//   entire rolling update. Setting the client's RPC deadline < drainTimeout
//   eliminates this entirely (well-behaved clients), so the timeout is only
//   a safety net for misbehaving or stuck clients.
func WithDrainTimeout(d time.Duration) ServerOption {
	return func(cfg *serverConfig) {
		cfg.drainTimeout = d
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

	health       *health.Server
	logger       *slog.Logger
	drainTimeout time.Duration

	// serving is an atomic flag preventing a second Serve() call from running
	// concurrently with an already-serving instance. 0 = idle, 1 = serving.
	// WHY atomic: we need a lockless check at the top of Serve() without
	// introducing a mutex that would complicate the select loop below.
	serving atomic.Int32
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

	// Unary chain: recovery → logging → pre-auth (wraps auth) → auth → custom.
	// Pre-auth interceptors go OUTSIDE auth on purpose (see WithPreAuthUnary-
	// Interceptors) so the audit DENY-capture interceptor can observe auth's own
	// short-circuit rejection of an unauthenticated/forbidden request.
	unary := []grpc.UnaryServerInterceptor{
		RecoveryUnaryInterceptor(),
		LoggingUnaryInterceptor(cfg.logger),
	}
	unary = append(unary, cfg.preAuthUnaryInt...)
	if cfg.validator != nil {
		unary = append(unary, AuthUnaryInterceptor(cfg.validator, WithSkipMethods(cfg.skipMethods...)))
	}
	unary = append(unary, cfg.extraUnaryInt...)

	// Stream chain: the same shape as unary, so streaming RPCs are equally
	// protected (recovery + logging + pre-auth + auth). This parity is the fix for
	// the previously-unauthenticated, panic-unsafe streaming path.
	stream := []grpc.StreamServerInterceptor{
		RecoveryStreamInterceptor(),
		LoggingStreamInterceptor(cfg.logger),
	}
	stream = append(stream, cfg.preAuthStreamInt...)
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

	drainTimeout := cfg.drainTimeout
	if drainTimeout == 0 {
		drainTimeout = defaultDrainTimeout
	}

	return &Server{
		GRPC:         grpcServer,
		health:       healthServer,
		logger:       cfg.logger,
		drainTimeout: drainTimeout,
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
// from the Service endpoints — then attempts GracefulStop with a bounded drain
// timeout (default 25s), falling back to hard Stop() if streams don't finish.
//
// WHY GracefulStop over Stop: Stop hard-kills in-flight RPCs (clients see
// broken connections mid-call). GracefulStop stops accepting new RPCs and waits
// for active ones to finish — the correct behavior for a K8s rolling update,
// which gives a 30s termination grace period for exactly this.
//
// WHY a drain TIMEOUT on GracefulStop: a streaming RPC held open by a client
// (with no deadline) would block GracefulStop forever, causing the pod to be
// SIGKILL'd mid-stream anyway AND blocking the rolling update. The bounded drain
// ensures we always return within the K8s grace period. See WithDrainTimeout.
//
// DOUBLE-SERVE GUARD: Calling Serve() twice on the same Server instance would
// start two goroutines listening on the same listener (likely a panic or accept
// error inside grpc-go). The atomic guard returns a clear error instead.
func (s *Server) Serve(ctx context.Context, lis net.Listener) error {
	// Guard against calling Serve() twice on the same server instance.
	// CompareAndSwap from 0→1 atomically; if it returns false, another
	// goroutine is already in Serve().
	if !s.serving.CompareAndSwap(0, 1) {
		return grpc.ErrServerStopped // reuse the sentinel; caller gets a clear error
	}
	defer s.serving.Store(0)

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- s.GRPC.Serve(lis)
	}()

	select {
	case err := <-serveErr:
		// Server stopped on its own (listener error, Stop(), GracefulStop(), etc.).
		// grpc.ErrServerStopped is returned when Stop/GracefulStop was called
		// externally; that is a clean shutdown, not an error. Map it to nil so
		// main() doesn't log a spurious error on normal shutdown.
		if err == grpc.ErrServerStopped {
			return nil
		}
		return err

	case <-ctx.Done():
		s.logger.Info("shutting down gRPC server",
			slog.String("reason", ctx.Err().Error()),
			slog.Duration("drain_timeout", s.drainTimeout),
		)
		// Mark NOT_SERVING immediately: K8s readiness probes start failing now,
		// which causes the load balancer to stop routing new traffic to this pod
		// before we've fully stopped. This is the "connection draining" equivalent
		// for gRPC — the pod leaves the Service endpoints gracefully.
		s.SetServing(false)

		// Bounded graceful drain:
		//   1. Run GracefulStop in a goroutine (it can block indefinitely).
		//   2. Race it against a timer.
		//   3. If the timer wins, hard-Stop and return.
		gracefulDone := make(chan struct{})
		go func() {
			s.GRPC.GracefulStop()
			close(gracefulDone)
		}()

		select {
		case <-gracefulDone:
			// Clean drain — all in-flight RPCs finished within the timeout.
			// GracefulStop already called Serve() to return; drain the channel.
			err := <-serveErr
			if err == grpc.ErrServerStopped {
				return nil
			}
			return err

		case <-time.After(s.drainTimeout):
			// Timeout: some RPCs are still open. Hard-stop now so the pod
			// exits within K8s terminationGracePeriodSeconds. Clients with
			// open streams see UNAVAILABLE and should reconnect.
			s.logger.Warn("graceful drain timed out — forcing hard stop",
				slog.Duration("drain_timeout", s.drainTimeout),
			)
			s.GRPC.Stop()
			// Wait for both GracefulStop and Serve goroutines to finish.
			<-gracefulDone
			err := <-serveErr
			if err == grpc.ErrServerStopped {
				return nil
			}
			return err
		}
	}
}
