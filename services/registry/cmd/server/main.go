// Package main is the entrypoint for the Model Registry service.
//
// ============================================================================
// WIRING — THE COMPOSITION ROOT
// ============================================================================
//
// In Clean Architecture, main.go is the "composition root": the only place that
// imports every layer (config, observability, domain, handler) and wires them into
// a running program. The layers themselves know each other only through the
// interfaces the domain defines.
//
// STARTUP SEQUENCE:
//
//	┌──────────────────────────────────────────────────────────────────┐
//	│ 1. Structured logger (slog JSON → stdout → Loki)                   │
//	│ 2. Load config (FP_* env vars → RegistryConfig)                    │
//	│ 3. Setup OpenTelemetry (traces → Tempo, metrics → Prometheus)      │
//	│ 4. Build gRPC server via grpcutil (interceptor chain)              │
//	│ 5. Register RegistryHandler                                        │
//	│ 6. Start HTTP health server (/healthz + /readyz)                   │
//	│ 7. Mark SERVING + start gRPC server (blocks until SIGTERM/SIGINT)  │
//	│ 8. Graceful shutdown (OTel flush → gRPC drain → health close)      │
//	└──────────────────────────────────────────────────────────────────┘
//
// WHAT'S WIRED NOW (scaffold phase):
//   - RegistryHandler with a NIL domain service. The embedded
//     UnimplementedRegistryServiceServer answers every RPC with codes.Unimplemented
//     and never dereferences the service — so a nil is correct and safe here.
//   - No Postgres pool, no Redis client, no NATS connection yet.
//
// WHAT ARRIVES LATER (and the exact wiring it adds here):
//
//   - Repository phase: a pgx pool (WriteStore) + a Redis client (ReadStore).
//
//   - Events phase:      a NATS JetStream connection behind the ProjectionEmitter.
//     Then this file constructs the real service and a real handler:
//
//     pool   := pgxpool.New(ctx, cfg.DatabaseURL)
//     rdb    := redis.NewClient(&redis.Options{Addr: cfg.RedisURL})
//     write  := postgres.NewWriteStore(pool)
//     read   := redisstore.NewReadStore(rdb)
//     pub    := events.NewPublisher(natsConn)            // satisfies ProjectionEmitter
//     svc    := domain.NewRegistryService(write, read, pub,
//     domain.NewRealClock(), domain.NewUUIDGenerator())
//     registryv1.RegisterRegistryServiceServer(srv.GRPC, handler.NewRegistryHandler(svc))
//     healthHandler.AddCheck("postgres", func(ctx) error { return pool.Ping(ctx) })
//     healthHandler.AddCheck("redis",    func(ctx) error { return rdb.Ping(ctx).Err() })
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

	registryv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/registry/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/config"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/health"
	"github.com/abd-ulbasit/forgepoint/pkg/observability"
	"github.com/abd-ulbasit/forgepoint/services/registry/internal/handler"
)

// RegistryConfig extends BaseConfig with registry-specific configuration.
//
// WHY embed BaseConfig: every Forgepoint service needs Port, GRPCPort, LogLevel,
// OTelEndpoint, NATSUrl, DatabaseURL. Embedding avoids repeating those, and
// config.Load[RegistryConfig]("FP") reads FP_PORT, FP_GRPC_PORT, FP_REDIS_URL, etc.
// via reflection (see pkg/config). RegistryConfig is the WRITE store (DatabaseURL,
// Postgres) PLUS the READ store (RedisURL) — the two halves of CQRS made explicit
// in configuration: one connection string per side.
type RegistryConfig struct {
	config.BaseConfig

	// RedisURL is the connection string for the CQRS READ projection (Redis). It is
	// distinct from DatabaseURL (the Postgres WRITE store) precisely because the two
	// stores are separate in CQRS. Not required at scaffold time (no Redis client is
	// built yet); the read adapter phase will mark it required and add a /readyz check.
	RedisURL string `env:"REDIS_URL" default:"redis://localhost:6379"`

	// OTelInsecure controls whether the OTLP exporter uses plaintext (no TLS). It is
	// true in local docker-compose (the collector has no cert) and false in
	// production (mTLS to the collector). Wiring it FROM CONFIG — rather than
	// hard-coding true as the early auth scaffold did — is the correct, env-driven
	// 12-factor approach: the same binary is secure in prod and convenient locally.
	OTelInsecure bool `env:"OTEL_INSECURE" default:"true"`
}

func main() {
	// ================================================================
	// 1. STRUCTURED LOGGER (slog JSON → stdout, shipped to Loki by the runtime)
	// ================================================================
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// ================================================================
	// 2. LOAD CONFIG (FP_* env vars → RegistryConfig; fail fast if required missing)
	// ================================================================
	cfg, err := config.Load[RegistryConfig]("FP")
	if err != nil {
		logger.Error("failed to load config", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("config loaded",
		slog.Int("grpc_port", cfg.GRPCPort),
		slog.Int("health_port", cfg.Port),
		slog.String("log_level", cfg.LogLevel),
		slog.Bool("otel_insecure", cfg.OTelInsecure),
	)

	// ================================================================
	// 3. OPENTELEMETRY (traces → Tempo, metrics → Prometheus via the collector)
	// ================================================================
	// NotifyContext gives us a signal-aware context: SIGTERM/SIGINT cancels it,
	// which both aborts a slow startup cleanly AND drives the graceful shutdown
	// of the gRPC server below. The same ctx is threaded through everything.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	otelShutdown, err := observability.Setup(ctx, observability.Config{
		ServiceName:    "registry",
		ServiceVersion: "dev",
		Environment:    "local",
		OTLPEndpoint:   cfg.OTelEndpoint,
		OTLPInsecure:   cfg.OTelInsecure, // ← from config, not hard-coded (see RegistryConfig)
	})
	if err != nil {
		logger.Error("failed to setup observability", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer func() {
		// Flush pending spans/metrics on shutdown; without this the last batch of
		// telemetry is lost. K8s gives 30s grace on SIGTERM — ample to flush.
		if shutdownErr := otelShutdown(context.Background()); shutdownErr != nil {
			logger.Error("otel shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// ================================================================
	// 4. BUILD gRPC SERVER (recovery → logging interceptor chain)
	// ================================================================
	// WHY no WithAuthValidator yet: the registry's RPCs DO require auth in
	// production (owner/team come from validated claims), but the auth interceptor
	// needs an auth-service TokenValidator client, which is wired in a later phase.
	// Adding it now with a nil validator would reject every call. The scaffold runs
	// without it; the handler phase adds:
	//   grpcutil.WithAuthValidator(authClient, /* no public methods to skip */)
	// WithReflection lets grpcurl/grpcui introspect the service during development.
	srv := grpcutil.NewServer(
		grpcutil.WithLogger(logger),
		grpcutil.WithReflection(),
	)

	// ================================================================
	// 5. REGISTER REGISTRY HANDLER (nil service — see the handler's doc comment)
	// ================================================================
	// The handler embeds UnimplementedRegistryServiceServer, so it fully satisfies
	// registryv1.RegistryServiceServer right now: every RPC returns
	// codes.Unimplemented until the handler phase overrides it. nil svc is safe (the
	// embedded base never dereferences it).
	registryv1.RegisterRegistryServiceServer(srv.GRPC, handler.NewRegistryHandler(nil))
	logger.Info("registry handler registered")

	// ================================================================
	// 6. HEALTH SERVER (HTTP /healthz liveness + /readyz readiness)
	// ================================================================
	// WHY a separate HTTP port from gRPC: gRPC is HTTP/2, kubelet probes are
	// HTTP/1.1 — they can't share a net.Listener. Separate ports also let us flip
	// /readyz to 503 (drain) while gRPC finishes in-flight calls on shutdown.
	// No dependency checks are registered yet (no Postgres/Redis client). The repo
	// phase adds AddCheck("postgres", pool.Ping) and AddCheck("redis", rdb.Ping).
	healthHandler := health.New()
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", healthHandler.LivenessHandler())
	healthMux.HandleFunc("/readyz", healthHandler.ReadinessHandler())

	healthAddr := fmt.Sprintf(":%d", cfg.Port)
	healthServer := &http.Server{Addr: healthAddr, Handler: healthMux}

	go func() {
		logger.Info("health server listening", slog.String("addr", healthAddr))
		if serveErr := healthServer.ListenAndServe(); serveErr != nil && serveErr != http.ErrServerClosed {
			logger.Error("health server error", slog.String("error", serveErr.Error()))
		}
	}()
	defer func() {
		if shutdownErr := healthServer.Shutdown(context.Background()); shutdownErr != nil {
			logger.Error("health server shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// ================================================================
	// 7. START gRPC SERVER (mark SERVING, listen, block until signal)
	// ================================================================
	// SetServing(true) flips the gRPC health service to SERVING so K8s readiness
	// passes. Serve flips it back to NOT_SERVING on SIGTERM before draining.
	srv.SetServing(true)

	grpcAddr := fmt.Sprintf(":%d", cfg.GRPCPort)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("failed to listen on gRPC port", slog.String("addr", grpcAddr), slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("gRPC server listening", slog.String("addr", grpcAddr))

	// Serve blocks until ctx is cancelled (SIGINT/SIGTERM). On cancellation it
	// drains in-flight RPCs (bounded), then returns here so the deferred shutdowns
	// (OTel flush, health close) run in order.
	if serveErr := srv.Serve(ctx, lis); serveErr != nil {
		logger.Error("gRPC server error", slog.String("error", serveErr.Error()))
		os.Exit(1)
	}

	logger.Info("registry service stopped cleanly")
}
