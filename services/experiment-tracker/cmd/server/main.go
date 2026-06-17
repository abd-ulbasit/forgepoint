// Package main is the entrypoint for the Experiment Tracker service.
//
// ============================================================================
// WIRING — THE COMPOSITION ROOT
// ============================================================================
//
// In Clean Architecture, main.go is the "composition root" — the ONLY place
// where all layers are imported together and wired into a running program. It
// knows about every layer (config, observability, domain, handler) but those
// layers never know about each other except through the interfaces they define.
//
// STARTUP SEQUENCE:
//
//	┌──────────────────────────────────────────────────────────────────┐
//	│ 1. Structured logger (slog JSON → stdout → Loki)                 │
//	│ 2. Load config (FP_* env vars → ExperimentTrackerConfig)         │
//	│ 3. Setup OpenTelemetry (traces → Tempo, metrics → Prometheus)    │
//	│ 4. Build gRPC server via grpcutil.NewServer (interceptor chain)  │
//	│ 5. Register ExperimentHandler (nil svc — see below)              │
//	│ 6. HTTP health server (/healthz liveness, /readyz readiness)     │
//	│ 7. Mark SERVING, start gRPC (blocks until SIGTERM/SIGINT)        │
//	│ 8. Graceful shutdown (OTel flush → gRPC drain → health close)    │
//	└──────────────────────────────────────────────────────────────────┘
//
// WHAT'S WIRED NOW (scaffold phase):
//   - ExperimentHandler with a nil domain service. The embedded
//     UnimplementedExperimentTrackerServiceServer answers every RPC with
//     codes.Unimplemented — a correct, running server.
//   - No Postgres pool, no NATS connection, no Redis idempotency store yet.
//
// WHAT ARRIVES LATER (and the wiring it adds here):
//   - Repository phase: pgxpool + postgres.NewExperimentRepo / NewRunRepo, a
//     Redis/Postgres IdempotencyStore, and a readiness check:
//     healthHandler.AddCheck("db", func(ctx) error { return pool.Ping(ctx) })
//   - Events phase: a NATS connection → an EventPublisher adapter AND the async
//     batch consumer (subscribe → buffer → flush via svc.LogMetrics, NAK on
//     overload = back-pressure). The consumer is started here as a goroutine.
//   - Then: svc := domain.NewExperimentService(expRepo, runRepo, idem, pub,
//     domain.NewRealClock()) and the handler is registered with the real svc.
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

	experimentv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/experiment/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/config"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/health"
	"github.com/abd-ulbasit/forgepoint/pkg/observability"
	"github.com/abd-ulbasit/forgepoint/services/experiment-tracker/internal/handler"
)

// ExperimentTrackerConfig extends BaseConfig with service-specific configuration.
//
// WHY embed BaseConfig: every Forgepoint service needs Port, GRPCPort, LogLevel,
// OTelEndpoint, NATSUrl, DatabaseURL. Embedding avoids re-declaring them.
// config.Load[ExperimentTrackerConfig]("FP") reads FP_PORT, FP_GRPC_PORT,
// FP_OTEL_INSECURE, etc. via reflection (see pkg/config).
type ExperimentTrackerConfig struct {
	config.BaseConfig

	// OTLPInsecure drives whether the OTLP exporter uses plaintext (no TLS).
	//
	// WHY this is config (not hardcoded): in local docker-compose the OTel
	// collector has no TLS cert, so the exporter must use plaintext — but in a
	// real cluster the collector terminates TLS and we must NOT downgrade to
	// plaintext (that would send traces/metrics in the clear). Defaulting to
	// true keeps local dev frictionless; production sets FP_OTEL_INSECURE=false.
	// Making it config means the SAME binary is secure-by-deployment rather than
	// secure-by-recompile.
	OTLPInsecure bool `env:"OTEL_INSECURE" default:"true"`
}

func main() {
	// ================================================================
	// 1. STRUCTURED LOGGER
	// ================================================================
	// slog (stdlib structured logging) with JSON output: container runtimes
	// capture stdout and ship it to Loki, which parses JSON natively. Zero
	// external deps — adequate for network-IO-bound services.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// ================================================================
	// 2. LOAD CONFIG
	// ================================================================
	// Fail-fast: a missing required field aborts startup with a clear error
	// rather than a half-configured process. In K8s, config comes from
	// ConfigMaps/Secrets mapped to env vars in the Deployment spec.
	cfg, err := config.Load[ExperimentTrackerConfig]("FP")
	if err != nil {
		logger.Error("failed to load config", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("config loaded",
		slog.Int("grpc_port", cfg.GRPCPort),
		slog.Int("health_port", cfg.Port),
		slog.String("log_level", cfg.LogLevel),
		slog.Bool("otel_insecure", cfg.OTLPInsecure),
	)

	// ================================================================
	// 3. OPENTELEMETRY
	// ================================================================
	// Setup initializes the trace provider (→ Tempo) and meter provider
	// (→ Prometheus via the OTel Collector). The signal-aware ctx also drives
	// graceful shutdown below: a SIGTERM during startup aborts cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	otelShutdown, err := observability.Setup(ctx, observability.Config{
		ServiceName:    "experiment-tracker",
		ServiceVersion: "dev",
		Environment:    "local",
		OTLPEndpoint:   cfg.OTelEndpoint,
		OTLPInsecure:   cfg.OTLPInsecure, // driven by config, not hardcoded (see field comment)
	})
	if err != nil {
		logger.Error("failed to setup observability", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer func() {
		// Flush pending spans/metrics on shutdown — without this the last ~5s of
		// telemetry (OTel batches internally) is lost. K8s grants 30s grace on
		// SIGTERM, ample time to flush.
		if shutdownErr := otelShutdown(context.Background()); shutdownErr != nil {
			logger.Error("otel shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// ================================================================
	// 4. BUILD gRPC SERVER
	// ================================================================
	// grpcutil.NewServer applies the standard interceptor chain
	// (recovery → logging → …). WithReflection lets grpcurl/grpcui introspect
	// the service in dev without the .proto files locally.
	//
	// NOTE: the auth interceptor (validating callers' JWTs and injecting
	// TokenClaims) is added in the handler phase, once this service is wired to
	// call the Auth service's ValidateToken — the handler then lifts the
	// server-authoritative Actor{UserID, Team} from those claims.
	srv := grpcutil.NewServer(
		grpcutil.WithLogger(logger),
		grpcutil.WithReflection(),
	)

	// ================================================================
	// 5. REGISTER HANDLER
	// ================================================================
	// The handler embeds UnimplementedExperimentTrackerServiceServer, so it fully
	// satisfies the server interface now; every RPC returns codes.Unimplemented
	// until later phases override them. We pass nil as the domain service: the
	// embedded Unimplemented methods never dereference it, so this is safe. Once
	// the repositories/events exist, main.go constructs the real service and
	// passes it here (see file header).
	experimentv1.RegisterExperimentTrackerServiceServer(srv.GRPC, handler.NewExperimentHandler(nil))
	logger.Info("experiment-tracker handler registered")

	// ================================================================
	// 6. HEALTH SERVER (HTTP)
	// ================================================================
	// Separate HTTP port for health because gRPC is HTTP/2 and kubelet probes are
	// HTTP/1.1 — they can't share a plain listener. Separation also lets us flip
	// /readyz to 503 (pod leaves the LB) while the gRPC server drains in-flight
	// RPCs. No readiness checks yet (no DB/NATS); the repository/events phases add
	// healthHandler.AddCheck("db", …) / ("nats", …).
	healthHandler := health.New()
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", healthHandler.LivenessHandler())
	healthMux.HandleFunc("/readyz", healthHandler.ReadinessHandler())

	healthAddr := fmt.Sprintf(":%d", cfg.Port)
	healthServer := &http.Server{
		Addr:    healthAddr,
		Handler: healthMux,
	}
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
	// 7. START gRPC SERVER
	// ================================================================
	// Mark the gRPC health service SERVING so readiness probes pass. grpcutil
	// flips this to NOT_SERVING on SIGTERM before draining (the graceful dance).
	srv.SetServing(true)

	grpcAddr := fmt.Sprintf(":%d", cfg.GRPCPort)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("failed to listen on gRPC port", slog.String("addr", grpcAddr), slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("gRPC server listening", slog.String("addr", grpcAddr))

	// Serve blocks until ctx is cancelled (SIGINT/SIGTERM). On cancellation:
	//   1. SetServing(false) → readiness fails → pod leaves LB endpoints
	//   2. GracefulStop (bounded drain) → in-flight RPCs finish
	//   3. Hard Stop if the drain times out
	//   4. Returns here; deferred OTel + health shutdown run.
	if serveErr := srv.Serve(ctx, lis); serveErr != nil {
		logger.Error("gRPC server error", slog.String("error", serveErr.Error()))
		os.Exit(1)
	}

	logger.Info("experiment-tracker service stopped cleanly")
}
