// Package main is the entrypoint for the Model Monitor service — the service that
// CLOSES the platform's ML lifecycle loop (serve → monitor → retrain).
//
// ============================================================================
// WIRING — THE COMPOSITION ROOT
// ============================================================================
//
// main.go is the only place where all layers are imported together and wired into
// a running program (Clean Architecture's "composition root"). It knows about
// every layer; the layers know only the interfaces they define.
//
// STARTUP SEQUENCE:
//
//	┌──────────────────────────────────────────────────────────────────┐
//	│  1. Structured logger (slog JSON → stdout → Loki)                │
//	│  2. Load config (FP_* env → MonitorConfig)                       │
//	│  3. Setup OpenTelemetry (traces → Tempo, metrics → Prometheus)   │
//	│  4. Build gRPC server via grpcutil.NewServer (interceptor chain) │
//	│  5. Register MonitorHandler on the gRPC server                   │
//	│  6. Start HTTP health server (/healthz liveness, /readyz ready)  │
//	│  7. Mark SERVING + start gRPC server (blocks until SIGTERM)      │
//	│  8. Graceful shutdown (OTel flush → gRPC drain → health stop)    │
//	└──────────────────────────────────────────────────────────────────┘
//
// WHAT'S WIRED NOW (scaffold phase, Task 16.1):
//   - MonitorHandler with a NIL domain service. The embedded
//     UnimplementedMonitorServiceServer answers every RPC with codes.Unimplemented
//     and never dereferences the service, so nil is safe and the server runs.
//   - No Postgres pool, no Redis client, no NATS consumer, no orchestrator client
//     (those arrive with the repository/events adapter phases).
//
// WHAT ARRIVES LATER (and what this file will gain):
//
//	pool   := pgxpool.New(ctx, cfg.DatabaseURL)
//	rdb    := redis.NewClient(cfg.RedisURL)
//	nc     := natsutil.Connect(cfg.NATSUrl)
//	orch   := orchestratorclient.New(cfg.OrchestratorEndpoint)   // FIXED endpoint (no SSRF)
//	svc    := domain.NewMonitorService(reports, windows, truth, baselines,
//	                                   publisher, orch, gate, cfg.RetrainCooldown, time.Now)
//	monitorv1.RegisterMonitorServiceServer(srv.GRPC, handler.NewMonitorHandler(svc))
//	// + a NATS consumer goroutine that maps InferenceCompleted → domain.InferenceObservation
//	//   and calls svc.ObserveInference — the streaming-aggregation heartbeat.
//	healthHandler.AddCheck("db",    func(ctx) error { return pool.Ping(ctx) })
//	healthHandler.AddCheck("redis", func(ctx) error { return rdb.Ping(ctx).Err() })
//	healthHandler.AddCheck("nats",  func(ctx) error { return natsReady(nc) })
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

	monitorv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/monitor/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/config"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/health"
	"github.com/abd-ulbasit/forgepoint/pkg/observability"
	monitorcfg "github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/config"
	"github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/handler"
)

func main() {
	// ================================================================
	// 1. STRUCTURED LOGGER
	// ================================================================
	// slog JSON to stdout: container runtimes capture stdout → Loki. JSON is
	// parse-friendly for log aggregators. stdlib slog avoids an external logging dep.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// ================================================================
	// 2. LOAD CONFIG
	// ================================================================
	// config.Load reads FP_* env vars into MonitorConfig via reflection, failing
	// fast on a missing required field. In K8s, env comes from ConfigMaps/Secrets.
	cfg, err := config.Load[monitorcfg.MonitorConfig]("FP")
	if err != nil {
		logger.Error("failed to load config", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("config loaded",
		slog.Int("grpc_port", cfg.GRPCPort),
		slog.Int("health_port", cfg.Port),
		slog.String("log_level", cfg.LogLevel),
		slog.String("retrain_cooldown", cfg.RetrainCooldown.String()),
	)

	// ================================================================
	// 3. OPENTELEMETRY
	// ================================================================
	// Setup initializes the trace provider (→ Tempo) and meter provider
	// (→ Prometheus via the OTel Collector). OTLPInsecure comes from intent: in
	// local docker-compose the collector has no TLS cert, so we send plaintext.
	// The signal-aware ctx also drives graceful shutdown below.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	otelShutdown, err := observability.Setup(ctx, observability.Config{
		ServiceName:    "model-monitor",
		ServiceVersion: "dev",
		Environment:    "local",
		OTLPEndpoint:   cfg.OTelEndpoint,
		OTLPInsecure:   true, // plaintext to the local collector (no TLS cert there)
	})
	if err != nil {
		logger.Error("failed to setup observability", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer func() {
		// Flush pending spans/metrics on shutdown — OTel batches internally, so
		// without this the last few seconds of telemetry are lost. K8s gives 30s
		// grace on SIGTERM, ample time to flush.
		if shutdownErr := otelShutdown(context.Background()); shutdownErr != nil {
			logger.Error("otel shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// ================================================================
	// 4. BUILD gRPC SERVER
	// ================================================================
	// grpcutil.NewServer applies the standard interceptor chain
	// (recovery → logging → … ). The auth interceptor (WithAuthValidator) is added
	// once the auth client is wired in a later phase — every Model Monitor RPC is
	// team-scoped by the caller's claims, so it WILL run here in production.
	// WithReflection lets grpcurl/grpcui introspect the service in development.
	srv := grpcutil.NewServer(
		grpcutil.WithLogger(logger),
		grpcutil.WithReflection(),
	)

	// ================================================================
	// 5. REGISTER MONITOR HANDLER
	// ================================================================
	// The handler embeds UnimplementedMonitorServiceServer, so it fully satisfies
	// monitorv1.MonitorServiceServer right now; every RPC returns Unimplemented
	// until a later phase overrides it. We pass nil for the domain service during
	// the scaffold phase — the embedded Unimplemented methods never dereference it.
	// Once the repository/Redis/NATS/orchestrator adapters land, main.go constructs
	// the real domain.MonitorService and passes it to NewMonitorHandler.
	monitorv1.RegisterMonitorServiceServer(srv.GRPC, handler.NewMonitorHandler(nil))
	logger.Info("monitor handler registered")

	// ================================================================
	// 6. HEALTH SERVER (HTTP)
	// ================================================================
	// Separate HTTP port for kubelet probes: gRPC is HTTP/2, probes are HTTP/1.1 —
	// they can't share a plain listener. Keeping them independent lets the gRPC
	// server drain while /readyz returns 503, pulling the pod from the LB cleanly.
	// No readiness checks are registered yet (no DB/Redis/NATS). The adapter phases
	// add Ping-based checks (see the wiring note in the package doc).
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
	// Mark the gRPC health service SERVING so K8s readiness passes. grpcutil flips
	// it to NOT_SERVING on SIGTERM before draining in-flight RPCs (graceful shutdown).
	srv.SetServing(true)

	grpcAddr := fmt.Sprintf(":%d", cfg.GRPCPort)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("failed to listen on gRPC port", slog.String("addr", grpcAddr), slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("gRPC server listening", slog.String("addr", grpcAddr))

	// Serve blocks until ctx is cancelled (SIGINT/SIGTERM). On cancellation it
	// flips readiness off, drains in-flight RPCs (bounded), then returns here so the
	// deferred OTel + health-server shutdowns run.
	if serveErr := srv.Serve(ctx, lis); serveErr != nil {
		logger.Error("gRPC server error", slog.String("error", serveErr.Error()))
		os.Exit(1)
	}

	logger.Info("model-monitor service stopped cleanly")
}
