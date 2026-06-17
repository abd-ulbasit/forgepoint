// Package main is the entrypoint for the Feature Store service.
//
// ============================================================================
// WIRING — THE COMPOSITION ROOT
// ============================================================================
//
// In Clean Architecture, main.go is the "composition root": the ONLY place where
// all layers are imported together and wired into a running program. It knows
// about every layer (config, observability, domain, handler) but none of those
// layers know about each other except through the interfaces they define.
//
// STARTUP SEQUENCE:
//
//	┌────────────────────────────────────────────────────────────────────┐
//	│  1. Structured logger (slog JSON → stdout → Loki)                  │
//	│  2. Load config (FP_* env vars → FeatureStoreConfig)               │
//	│  3. Setup OpenTelemetry (traces → Tempo, metrics → Prometheus)     │
//	│  4. Build gRPC server via grpcutil.NewServer (interceptor chain)   │
//	│  5. Register FeatureStoreHandler on the gRPC server                │
//	│  6. HTTP health server (/healthz liveness, /readyz readiness)      │
//	│  7. Mark SERVING + start gRPC (blocks until SIGTERM/SIGINT)        │
//	│  8. Graceful shutdown (OTel flush → gRPC drain → health stop)      │
//	└────────────────────────────────────────────────────────────────────┘
//
// WHAT'S WIRED NOW (scaffold phase):
//   - FeatureStoreHandler with a nil domain service. The embedded
//     UnimplementedFeatureStoreServiceServer answers every RPC with
//     codes.Unimplemented (correct behavior), never dereferencing the nil svc.
//   - No Postgres event log, no Redis online store, no NATS connection yet.
//
// WHAT ARRIVES LATER (the adapter + handler phases):
//   - postgres.NewEventLog(pool)            implements domain.EventLog (append-only)
//   - redis.NewOnlineViewStore(client)      implements domain.OnlineViewStore
//   - postgres.NewOfflineViewStore(pool)    implements domain.OfflineViewStore
//   - a clock (time.Now) + an IDGenerator (uuid.NewString)
//   - svc := domain.NewFeatureStoreService(eventLog, online, offline, clock, idgen)
//   - featurestorev1.RegisterFeatureStoreServiceServer(srv.GRPC, handler.NewFeatureStoreHandler(svc))
//   - healthHandler.AddCheck("postgres", pool.Ping) / ("redis", client.Ping) / ("nats", nc.Status)
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

	featurestorev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/featurestore/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/config"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/health"
	"github.com/abd-ulbasit/forgepoint/pkg/observability"
	"github.com/abd-ulbasit/forgepoint/services/feature-store/internal/handler"
)

// FeatureStoreConfig extends BaseConfig with feature-store-specific configuration.
//
// WHY embed BaseConfig: every Forgepoint service needs Port, GRPCPort, LogLevel,
// OTelEndpoint, NATSUrl, DatabaseURL. config.Load[FeatureStoreConfig]("FP") reads
// FP_PORT, FP_GRPC_PORT, FP_REDIS_URL, etc. via reflection (see pkg/config).
//
// RedisURL is the online "latest" projection store (the low-latency hot-path read
// model). It is NOT required at scaffold time — until the adapters land the
// service runs without it — so it has no `required:"true"`. When the Redis online
// store adapter is wired, an empty RedisURL should fail readiness (the online read
// path is unavailable), enforced via a /readyz check rather than a hard startup
// requirement, so the service can still start to serve health + the catalog reads
// that only need Postgres.
type FeatureStoreConfig struct {
	config.BaseConfig

	// RedisURL points at the Redis backing the online (latest-value) projection.
	// Format: redis://host:port/db. Injected via env (K8s ConfigMap, or a Secret if
	// it carries credentials). Empty in the scaffold phase.
	RedisURL string `env:"REDIS_URL"`
}

func main() {
	// ================================================================
	// 1. STRUCTURED LOGGER
	// ================================================================
	// slog JSON to stdout: the container runtime captures stdout and ships it to
	// Loki. JSON is parse-friendly for the log aggregator. Zero external deps.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// ================================================================
	// 2. LOAD CONFIG
	// ================================================================
	// Reads FP_* env vars into FeatureStoreConfig via reflection, failing fast on a
	// missing required field. In K8s, config comes from ConfigMaps/Secrets mapped
	// to env vars in the Deployment spec.
	cfg, err := config.Load[FeatureStoreConfig]("FP")
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
	// Setup initializes the trace provider (→ Tempo) and meter provider (→
	// Prometheus via the OTel Collector). We pass a SIGNAL-AWARE context so OTLP
	// setup aborts cleanly if a SIGTERM arrives during startup; the same ctx drives
	// the graceful shutdown below.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	otelShutdown, err := observability.Setup(ctx, observability.Config{
		ServiceName:    "feature-store",
		ServiceVersion: "dev",
		Environment:    "local",
		OTLPEndpoint:   cfg.OTelEndpoint,
		// OTLPInsecure from config intent: plaintext OTLP in local docker-compose
		// (no TLS cert on the collector). In a real cluster this would be derived
		// from config rather than hardcoded; kept true here to match the local stack.
		OTLPInsecure: true,
	})
	if err != nil {
		logger.Error("failed to setup observability", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer func() {
		// Flush pending spans/metrics on shutdown. OTel batches internally; without
		// this the last few seconds of telemetry are lost. K8s' 30s SIGTERM grace is
		// ample. Use a fresh Background ctx — the signal ctx is already cancelled here.
		if shutdownErr := otelShutdown(context.Background()); shutdownErr != nil {
			logger.Error("otel shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// ================================================================
	// 4. BUILD gRPC SERVER
	// ================================================================
	// grpcutil.NewServer applies the standard interceptor chain (recovery →
	// logging → tracing → auth). The auth validator is added when this service is
	// wired to call the Auth service's ValidateToken (the handler phase); at
	// scaffold time we run without it so the server starts standalone.
	//
	// WithReflection: grpcurl/grpcui can introspect the service in development
	// without the .proto files locally.
	srv := grpcutil.NewServer(
		grpcutil.WithLogger(logger),
		grpcutil.WithReflection(),
	)

	// ================================================================
	// 5. REGISTER FEATURE STORE HANDLER
	// ================================================================
	// The handler embeds UnimplementedFeatureStoreServiceServer, so it satisfies
	// featurestorev1.FeatureStoreServiceServer fully right now; every RPC returns
	// Unimplemented until the handler phase. nil svc is safe — the embedded base
	// never dereferences it. When the adapters land, main.go builds the real
	// service (see the file header) and passes it to NewFeatureStoreHandler.
	featurestorev1.RegisterFeatureStoreServiceServer(srv.GRPC, handler.NewFeatureStoreHandler(nil))
	logger.Info("feature-store handler registered")

	// ================================================================
	// 6. HEALTH SERVER (HTTP)
	// ================================================================
	// gRPC uses HTTP/2; kubelet probes use HTTP/1.1 — they can't share a port, so
	// health runs on its own HTTP port. This lets us flip /readyz to 503 (draining)
	// while the gRPC server finishes in-flight RPCs. No dependency checks yet (no
	// Postgres/Redis/NATS); the adapter phase adds AddCheck("postgres"/"redis"/"nats", …).
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
	// 7. START gRPC SERVER
	// ================================================================
	// Mark health SERVING so K8s readiness passes. grpcutil.Server.Serve flips it
	// to NOT_SERVING on SIGTERM before draining (the graceful-shutdown dance).
	srv.SetServing(true)

	grpcAddr := fmt.Sprintf(":%d", cfg.GRPCPort)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("failed to listen on gRPC port", slog.String("addr", grpcAddr), slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("gRPC server listening", slog.String("addr", grpcAddr))

	// Serve blocks until ctx is cancelled (SIGINT/SIGTERM). On cancellation:
	//   1. SetServing(false) → readiness fails → pod leaves the LB endpoints
	//   2. GracefulStop (bounded drain) → in-flight RPCs finish
	//   3. hard Stop if drain times out
	//   4. returns here; deferred OTel + health shutdowns run
	if serveErr := srv.Serve(ctx, lis); serveErr != nil {
		logger.Error("gRPC server error", slog.String("error", serveErr.Error()))
		os.Exit(1)
	}

	logger.Info("feature-store service stopped cleanly")
}
