// Package main is the entrypoint for the Pipeline Orchestrator service.
//
// ============================================================================
// THE COMPOSITION ROOT
// ============================================================================
//
// In Clean Architecture, main.go is the ONLY place that imports every layer
// (config, observability, domain, handler) and wires them into a running
// program. It also owns the concrete implementations of the domain's small
// utility ports (Clock, IDGenerator) — see systemClock / uuidGenerator below —
// so the domain package itself stays free of even the uuid import (matching the
// Auth service's stdlib-only domain).
//
// STARTUP SEQUENCE:
//
//	1. Structured logger (slog JSON → stdout → Loki)
//	2. Load config (FP_* env vars → PipelineConfig)
//	3. Setup OpenTelemetry (traces → Tempo, metrics → Prometheus)
//	4. Build gRPC server (grpcutil interceptor chain: recovery → logging → auth)
//	5. Register the PipelineHandler
//	6. HTTP health server (/healthz liveness, /readyz readiness)
//	7. Start gRPC (blocks until SIGTERM/SIGINT)
//	8. Graceful shutdown (OTel flush → gRPC drain → health server stop)
//
// WHAT'S WIRED NOW (scaffold phase): PipelineHandler with a nil domain service.
// The embedded UnimplementedPipelineOrchestratorServiceServer answers every RPC
// with codes.Unimplemented — a correct, running server.
//
// WHAT ARRIVES LATER (repository/executor/events phases):
//
//	pool := pgxpool.New(ctx, cfg.DatabaseURL)
//	pipelineRepo := postgres.NewPipelineRepo(pool)
//	executionRepo := postgres.NewExecutionRepo(pool)
//	registry := executors.NewRegistry(registryClient, gatewayClient, k8sClient) // DEPLOY/CANARY/...
//	publisher := events.NewPublisher(natsConn)
//	svc := domain.NewPipelineService(pipelineRepo, executionRepo, registry, publisher, systemClock{}, uuidGenerator{})
//	h := handler.NewPipelineHandler(svc)
//	healthHandler.AddCheck("db", func(ctx) error { return pool.Ping(ctx) })
//	healthHandler.AddCheck("nats", func(ctx) error { return natsConn.Ping() })
//
// LEADER ELECTION NOTE (deployment, surfaced for interviews): the gRPC API can be
// served by many replicas, but only the ELECTED LEADER drives sagas (runs steps,
// writes checkpoints) so two pods never double-apply a deployment. Read RPCs
// (Get/List/Watch) are served by any replica from the shared DB. The leader-
// election wiring lands with the repository phase (it needs the durable store).
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
	"time"

	pipelinev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/pipeline/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/config"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/health"
	"github.com/abd-ulbasit/forgepoint/pkg/observability"
	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/handler"
	"github.com/google/uuid"
)

// PipelineConfig extends BaseConfig with this service's own configuration.
//
// WHY embed BaseConfig: every Forgepoint service needs Port, GRPCPort, LogLevel,
// OTelEndpoint, NATSUrl, DatabaseURL. config.Load[PipelineConfig]("FP") reads
// FP_PORT, FP_GRPC_PORT, FP_DATABASE_URL, etc. via reflection (see pkg/config).
//
// The Orchestrator-specific fields below are commented as future config the
// repository/executor phases will consume. They're declared now so the config
// surface is visible and documented, but only the embedded BaseConfig fields are
// REQUIRED to start the scaffold. (No `required:"true"` here yet — the scaffold
// runs with sane defaults; DATABASE_URL/NATS_URL become required once those
// adapters are wired and the readiness checks depend on them.)
type PipelineConfig struct {
	config.BaseConfig

	// CompensationTimeout bounds how long a single step's compensation may run
	// during a rollback before the engine declares it COMPENSATION_FAILED (a stuck
	// saga). Defaults to 60s — matching the engine's internal compensation budget.
	// Surfaced as config so an operator can tune it per environment.
	CompensationTimeout time.Duration `env:"COMPENSATION_TIMEOUT" default:"60s"`
}

// systemClock is the production domain.Clock — real wall-clock time in UTC. It
// lives in the composition root (not the domain) so the domain package needs no
// time-source dependency beyond what its pure logic uses. Tests inject a fake.
type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

// uuidGenerator is the production domain.IDGenerator — crypto-random UUIDv4 for
// execution/step/pipeline ids. It lives HERE (the composition root) so the uuid
// import stays out of the domain package, keeping the domain's dependency
// footprint to the standard library. uuid.NewString uses crypto/rand, so ids are
// unguessable (no IDOR via predictable ids).
type uuidGenerator struct{}

func (uuidGenerator) NewID() string { return uuid.NewString() }

func main() {
	// ================================================================
	// 1. STRUCTURED LOGGER
	// ================================================================
	// JSON to stdout for container log capture → Loki. stdlib slog: fast enough
	// for network-IO-bound services, zero external deps.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// ================================================================
	// 2. LOAD CONFIG
	// ================================================================
	cfg, err := config.Load[PipelineConfig]("FP")
	if err != nil {
		logger.Error("failed to load config", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("config loaded",
		slog.Int("grpc_port", cfg.GRPCPort),
		slog.Int("health_port", cfg.Port),
		slog.String("log_level", cfg.LogLevel),
		slog.Duration("compensation_timeout", cfg.CompensationTimeout),
	)

	// ================================================================
	// 3. OPENTELEMETRY
	// ================================================================
	// signal-aware context: a SIGTERM during startup aborts OTLP setup cleanly,
	// and the same ctx drives graceful shutdown below.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	otelShutdown, err := observability.Setup(ctx, observability.Config{
		ServiceName:    "pipeline-orchestrator",
		ServiceVersion: "dev",
		Environment:    "local",
		OTLPEndpoint:   cfg.OTelEndpoint,
		OTLPInsecure:   true, // plaintext to the local collector (no TLS in docker-compose)
	})
	if err != nil {
		logger.Error("failed to setup observability", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer func() {
		// Flush pending spans/metrics on shutdown (OTel batches internally; without
		// this the last few seconds of telemetry are lost). K8s gives 30s grace.
		if shutdownErr := otelShutdown(context.Background()); shutdownErr != nil {
			logger.Error("otel shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// ================================================================
	// 4. BUILD gRPC SERVER
	// ================================================================
	// grpcutil.NewServer applies the standard chain (recovery → logging → auth).
	// WithReflection lets grpcurl/grpcui introspect the service in development.
	//
	// The auth interceptor (added when the shared validator is wired) supplies the
	// TokenClaims the handler turns into a domain.Actor — that is how CreatedBy /
	// TriggeredBy / Team become server-authoritative (never client request fields).
	srv := grpcutil.NewServer(
		grpcutil.WithLogger(logger),
		grpcutil.WithReflection(),
	)

	// ================================================================
	// 5. REGISTER PIPELINE HANDLER
	// ================================================================
	// The handler embeds UnimplementedPipelineOrchestratorServiceServer — it fully
	// satisfies the service interface right now. All RPCs return Unimplemented
	// until the handler phase fills them in. nil svc is safe: the embedded base
	// never dereferences it. Real wiring (see the comment block at the top of this
	// file) lands once the repositories, executor registry, and event publisher
	// exist:
	//   svc := domain.NewPipelineService(pipelineRepo, executionRepo, registry, publisher, systemClock{}, uuidGenerator{})
	//   pipelinev1.RegisterPipelineOrchestratorServiceServer(srv.GRPC, handler.NewPipelineHandler(svc))
	pipelinev1.RegisterPipelineOrchestratorServiceServer(srv.GRPC, handler.NewPipelineHandler(nil))
	logger.Info("pipeline handler registered")

	// ================================================================
	// 6. HEALTH SERVER (HTTP)
	// ================================================================
	// Separate HTTP port for kubelet probes: gRPC is HTTP/2, health is HTTP/1.1,
	// so they can't share a plain listener. Keeping them apart lets us fail
	// readiness (503) while draining gRPC during shutdown.
	//
	// No readiness checks registered yet (no DB/NATS). The repository/events phases
	// add: healthHandler.AddCheck("db", pool.Ping) and ("nats", natsConn.Ping).
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
	// Flip the gRPC health service to SERVING so K8s readiness passes. On SIGTERM,
	// grpcutil.Server.Serve flips it back to NOT_SERVING, then drains in-flight
	// RPCs before stopping (the graceful-shutdown dance).
	srv.SetServing(true)

	grpcAddr := fmt.Sprintf(":%d", cfg.GRPCPort)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("failed to listen on gRPC port", slog.String("addr", grpcAddr), slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("gRPC server listening", slog.String("addr", grpcAddr))

	// Serve blocks until ctx is cancelled (SIGINT/SIGTERM). On cancellation it
	// fails readiness, drains in-flight RPCs (bounded), then returns so the
	// deferred OTel + health-server shutdowns run.
	if serveErr := srv.Serve(ctx, lis); serveErr != nil {
		logger.Error("gRPC server error", slog.String("error", serveErr.Error()))
		os.Exit(1)
	}

	logger.Info("pipeline-orchestrator service stopped cleanly")
}
