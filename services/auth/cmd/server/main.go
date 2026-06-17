// Package main is the entrypoint for the Auth service.
//
// ============================================================================
// WIRING — THE COMPOSITION ROOT
// ============================================================================
//
// In Clean Architecture, main.go is the "composition root" — the only place
// where all layers are imported together and wired into a running program.
// It knows about every layer (config, observability, domain, repository,
// handler) but none of those layers know about each other except through the
// interfaces they define.
//
// STARTUP SEQUENCE:
//
//   ┌──────────────────────────────────────────────────────────────────┐
//   │  1. Load config (env vars → AuthConfig struct)                  │
//   │  2. Setup OpenTelemetry (traces + metrics)                      │
//   │  3. Build gRPC server via grpcutil.NewServer (interceptor chain)│
//   │  4. Register AuthHandler on the gRPC server                     │
//   │  5. Mark server SERVING (health probe goes green)               │
//   │  6. Start HTTP health server (liveness + readiness probes)      │
//   │  7. Start gRPC server (blocks until SIGTERM/SIGINT)             │
//   │  8. Graceful shutdown (OTel flush → gRPC drain)                 │
//   └──────────────────────────────────────────────────────────────────┘
//
// WHAT'S WIRED NOW (scaffold phase, Tasks 1.2–1.4):
//   - AuthHandler with nil domain service (embedded UnimplementedAuthServiceServer
//     handles all RPCs, returning codes.Unimplemented — correct behavior).
//   - No Postgres pool, no NATS connection (those arrive in Tasks 1.3/1.4).
//
// WHAT ARRIVES LATER:
//   Task 1.3 (domain impl): NewAuthService(userRepo, apiKeyRepo, roleRepo, secret)
//   Task 1.4 (repository impl): postgres.NewUserRepo(pool), etc.
//   Task 1.5 (RPC impl): handler methods call svc.Login, svc.CreateUser, etc.
//
// Once Tasks 1.3/1.4 land, the wiring in this file gains:
//   pool := pgxpool.New(ctx, cfg.DatabaseURL)
//   userRepo := postgres.NewUserRepo(pool)
//   svc := authdomain.NewAuthService(userRepo, ...)
//   h := handler.NewAuthHandler(svc)
//   healthHandler.AddCheck("db", func(ctx) error { return pool.Ping(ctx) })
//
// ============================================================================
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"

	authv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/auth/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/config"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/health"
	"github.com/abd-ulbasit/forgepoint/pkg/observability"
	"github.com/abd-ulbasit/forgepoint/services/auth/internal/handler"
)

// AuthConfig extends BaseConfig with auth-specific configuration.
//
// WHY embed BaseConfig:
//   Every Forgepoint service needs Port, GRPCPort, LogLevel, OTelEndpoint,
//   NATSUrl, DatabaseURL. Embedding them avoids repeating those field definitions
//   in every service's config struct. config.Load[AuthConfig]("FP") reads
//   FP_PORT, FP_GRPC_PORT, FP_JWT_SECRET, etc. via reflection (see pkg/config).
//
// SECURITY NOTE: JWTSecret is required (required:"true"). The service refuses to
// start if it's unset — fail-fast is the correct behavior for security config.
// In K8s, this is injected via a Secret mounted as an env var.
type AuthConfig struct {
	config.BaseConfig

	// JWTSecret is the HMAC-SHA256 signing key for JWT tokens.
	// Must be at least 32 bytes of high-entropy random data in production.
	// K8s: mounted from a Secret (not a ConfigMap — secrets are base64-encoded
	// and access-controlled, unlike ConfigMaps which are plaintext in etcd).
	JWTSecret string `env:"JWT_SECRET" required:"true"`
}

func main() {
	// ================================================================
	// 1. STRUCTURED LOGGER
	// ================================================================
	// slog is the stdlib structured logger (Go 1.21+). We use JSON output for
	// container environments where logs are shipped to Loki via the container
	// runtime's stdout capture. JSON is parse-friendly for log aggregators.
	//
	// WHY not zap or zerolog: stdlib slog is fast enough for this use case,
	// has zero external dependencies, and is the direction the Go standard
	// library is heading. We'd adopt zap if benchmarks showed slog as a
	// bottleneck (unlikely for network-IO-bound services).
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// ================================================================
	// 2. LOAD CONFIG
	// ================================================================
	// config.Load reads FP_* env vars into AuthConfig using reflection.
	// It fails fast if any required field is missing. In K8s, required config
	// comes from Secrets/ConfigMaps mapped to env vars in the Deployment spec.
	cfg, err := config.Load[AuthConfig]("FP")
	if err != nil {
		logger.Error("failed to load config", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("config loaded",
		slog.Int("grpc_port", cfg.GRPCPort),
		slog.Int("health_port", cfg.Port),
		slog.String("log_level", cfg.LogLevel),
	)

	// ================================================================
	// 3. OPENTELEMETRY
	// ================================================================
	// Setup initializes the trace provider (→ Tempo) and meter provider
	// (→ Prometheus via OTel Collector). All spans and metrics from this
	// process flow through these providers.
	//
	// WHY NotifyContext here (not a plain background context):
	//   We pass the signal-aware context to observability.Setup so that the
	//   OTLP connection setup respects the process signal (SIGTERM during
	//   startup = abort cleanly). The same ctx drives the shutdown below.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	otelShutdown, err := observability.Setup(ctx, observability.Config{
		ServiceName:    "auth",
		ServiceVersion: "dev",
		Environment:    "local",
		OTLPEndpoint:   cfg.OTelEndpoint,
		OTLPInsecure:   true, // plaintext in local docker-compose (no TLS cert on the collector)
	})
	if err != nil {
		logger.Error("failed to setup observability", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer func() {
		// Flush all pending spans and metrics on shutdown. Without this, the
		// last ~5 seconds of telemetry are lost (OTel batches internally).
		// K8s gives us 30s grace on SIGTERM — plenty of time to flush.
		if shutdownErr := otelShutdown(context.Background()); shutdownErr != nil {
			logger.Error("otel shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// ================================================================
	// 4. BUILD gRPC SERVER
	// ================================================================
	// grpcutil.NewServer applies the standard interceptor chain:
	//   recovery → logging → (auth, added in Task 1.3 once we have a validator)
	//
	// WHY no WithAuthValidator here:
	//   The AuthService itself IS the token validator — it would be circular
	//   for the auth service to require an auth interceptor on its own RPCs.
	//   Instead, the auth service's RPCs that require privilege (e.g., CreateUser
	//   requires admin) validate the caller's token in the handler, not via an
	//   interceptor, because the service is the source of truth.
	//   (In Task 1.5 we'll add an internal-only validator for admin RPCs.)
	//
	// WithReflection: enabled so grpcurl/grpcui can introspect the service
	// during development without needing the .proto files locally.
	srv := grpcutil.NewServer(
		grpcutil.WithLogger(logger),
		grpcutil.WithReflection(),
	)

	// ================================================================
	// 5. REGISTER AUTH HANDLER
	// ================================================================
	// The handler embeds UnimplementedAuthServiceServer — it satisfies
	// authv1.AuthServiceServer fully right now. All RPCs return Unimplemented
	// until Task 1.5 overrides them with real logic.
	//
	// We pass nil as the domain service for the scaffold phase. The embedded
	// Unimplemented methods never dereference the svc field, so this is safe.
	// Once Task 1.3 delivers AuthServiceImpl and Task 1.4 delivers Postgres
	// repositories, main.go will construct and pass the real service:
	//   svc := authdomain.NewAuthService(userRepo, apiKeyRepo, roleRepo, cfg.JWTSecret)
	//   authv1.RegisterAuthServiceServer(srv.GRPC, handler.NewAuthHandler(svc))
	authv1.RegisterAuthServiceServer(srv.GRPC, handler.NewAuthHandler(nil))
	logger.Info("auth handler registered")

	// ================================================================
	// 6. HEALTH SERVER (HTTP)
	// ================================================================
	// The HTTP health server runs on Port (default 8080) and serves:
	//   GET /healthz → liveness probe (always 200 while process is alive)
	//   GET /readyz  → readiness probe (checks dependencies are reachable)
	//
	// WHY a separate HTTP port for health:
	//   gRPC uses HTTP/2; kubelet health probes use HTTP/1.1. They can't share
	//   a port on a plain net.Listener. Separate ports keeps them independent:
	//   the gRPC server can be shut down while the health port returns 503,
	//   signalling to K8s that the pod is no longer ready for traffic.
	//
	// No readiness checks are registered yet (no DB, no NATS). Task 1.3/1.4
	// will add:
	//   healthHandler.AddCheck("db", func(ctx) error { return pool.Ping(ctx) })
	healthHandler := health.New()
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", healthHandler.LivenessHandler())
	healthMux.HandleFunc("/readyz", healthHandler.ReadinessHandler())

	healthAddr := fmt.Sprintf(":%d", cfg.Port)
	healthServer := &http.Server{
		Addr:    healthAddr,
		Handler: healthMux,
	}

	// Run the health server in a goroutine so it doesn't block the gRPC server.
	go func() {
		logger.Info("health server listening", slog.String("addr", healthAddr))
		if serveErr := healthServer.ListenAndServe(); serveErr != nil && serveErr != http.ErrServerClosed {
			logger.Error("health server error", slog.String("error", serveErr.Error()))
		}
	}()

	// Shut down the health server when the process exits.
	defer func() {
		if shutdownErr := healthServer.Shutdown(context.Background()); shutdownErr != nil {
			logger.Error("health server shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// ================================================================
	// 7. START gRPC SERVER
	// ================================================================
	// Mark the gRPC health service as SERVING so K8s readiness probes pass.
	// (We flip this back to NOT_SERVING in grpcutil.Server.Serve on SIGTERM,
	// before draining in-flight RPCs — this is the graceful shutdown dance.)
	srv.SetServing(true)

	grpcAddr := fmt.Sprintf(":%d", cfg.GRPCPort)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("failed to listen on gRPC port", slog.String("addr", grpcAddr), slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("gRPC server listening", slog.String("addr", grpcAddr))

	// Serve blocks until ctx is cancelled (SIGINT/SIGTERM via NotifyContext above).
	// On cancellation:
	//   1. srv.SetServing(false) — readiness probes fail → pod leaves LB endpoints
	//   2. GracefulStop (up to 25s drain) — in-flight RPCs finish
	//   3. Hard Stop if drain times out
	//   4. Returns here, deferred shutdown of OTel + health server run
	if serveErr := srv.Serve(ctx, lis); serveErr != nil {
		logger.Error("gRPC server error", slog.String("error", serveErr.Error()))
		os.Exit(1)
	}

	logger.Info("auth service stopped cleanly")
}
