// Package main is the entrypoint for the Inference Gateway service.
//
// ============================================================================
// WIRING — THE COMPOSITION ROOT
// ============================================================================
//
// main.go is the "composition root": the ONLY place that imports every layer
// (config, observability, domain, handler) and wires them into a running
// program. Each layer knows nothing of the others except through interfaces.
//
// STARTUP SEQUENCE:
//
//   ┌──────────────────────────────────────────────────────────────────┐
//   │ 1. Structured logger (slog JSON → stdout → Loki)                 │
//   │ 2. Load config (FP_* env vars → InferenceConfig)                 │
//   │ 3. Setup OpenTelemetry (traces → Tempo, metrics → Prometheus)    │
//   │ 4. Build gRPC server via grpcutil.NewServer (interceptor chain)  │
//   │ 5. Register InferenceHandler                                     │
//   │ 6. HTTP health server (/healthz liveness, /readyz readiness)     │
//   │ 7. Start gRPC (blocks until SIGTERM/SIGINT)                      │
//   │ 8. Graceful shutdown (OTel flush → gRPC drain → health close)    │
//   └──────────────────────────────────────────────────────────────────┘
//
// WHAT'S WIRED NOW (scaffold phase):
//   - InferenceHandler with a nil domain service. The embedded
//     UnimplementedInferenceGatewayServiceServer answers every RPC with
//     codes.Unimplemented — a correct, running server.
//   - No Redis (route store + rate limiter + quota cache), no model-serving gRPC
//     client, no NATS publisher/subscriber yet.
//
// WHAT ARRIVES LATER (and the exact wiring it adds here):
//   redis  := infra dial → routeStore := redisadapter.NewRouteStore(redis)
//                          limiter   := redisadapter.NewTokenBucket(redis, rate, burst)
//                          quota     := redisadapter.NewQuotaCache(redis)
//   serving := grpc.Dial backends → backend := servingclient.New(...)
//   nats    := natsutil.Connect    → publisher := events.NewPublisher(nats)
//   svc := domain.NewInferenceService(domain.ServiceDeps{
//             Routes: routeStore, Limiter: limiter, Backend: backend,
//             Publisher: publisher, Quota: quota,
//             Breakers: domain.NewBreakerRegistry(tuning, time.Now),
//             RandSource: func(n int) int { return rand.Intn(n) },
//             Now: time.Now,
//          })
//   // a subscriber routes ModelDeployed/Undeployed/Promoted/Archived → svc.Apply*
//   inferencev1.RegisterInferenceGatewayServiceServer(srv.GRPC, handler.NewInferenceHandler(svc))
//   healthHandler.AddCheck("redis", func(ctx) error { return redis.Ping(ctx).Err() })
//   healthHandler.AddCheck("nats",  func(ctx) error { return nats.Drain-readiness })
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

	inferencev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/inference/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/config"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/health"
	"github.com/abd-ulbasit/forgepoint/pkg/observability"
	"github.com/abd-ulbasit/forgepoint/services/inference-gateway/internal/handler"
)

// InferenceConfig extends BaseConfig with gateway-specific configuration.
//
// WHY embed BaseConfig: every Forgepoint service needs Port, GRPCPort, LogLevel,
// OTelEndpoint, NATSUrl, DatabaseURL. Embedding avoids repeating them.
// config.Load[InferenceConfig]("FP") reads FP_PORT, FP_REDIS_URL, etc. via
// reflection (see pkg/config).
//
// The gateway is Redis-backed (rate-limit token buckets, the quota cache, and
// the warm copy of the routing table) and forwards to model-serving over gRPC —
// so its extra config is the Redis URL, the rate-limit tuning, and the breaker
// tuning. Defaults make it runnable against local docker-compose with no env set.
type InferenceConfig struct {
	config.BaseConfig

	// RedisURL is the connection string for the rate-limit buckets, the quota
	// cache, and the routing-table mirror. In K8s this points at the fp-infra
	// Redis Service. Defaulted for local docker-compose.
	RedisURL string `env:"REDIS_URL" default:"redis://localhost:6379"`

	// RateLimitPerSec / RateLimitBurst tune the per-API-key token bucket: refill
	// rate (tokens/sec) and bucket depth (max burst). Defaults are conservative;
	// production tunes per rate plan. These are the RATE LIMITING knobs.
	RateLimitPerSec int `env:"RATE_LIMIT_PER_SEC" default:"100"`
	RateLimitBurst  int `env:"RATE_LIMIT_BURST" default:"200"`

	// CircuitFailureThreshold / CircuitResetSeconds tune the per-backend breaker:
	// consecutive failures to trip OPEN, and the OPEN cooldown before a probe.
	// These are the CIRCUIT BREAKER knobs (consumed by NewBreakerRegistry later).
	CircuitFailureThreshold int `env:"CIRCUIT_FAILURE_THRESHOLD" default:"5"`
	CircuitResetSeconds     int `env:"CIRCUIT_RESET_SECONDS" default:"30"`
}

func main() {
	// ================================================================
	// 1. STRUCTURED LOGGER (slog JSON → stdout → Loki)
	// ================================================================
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// ================================================================
	// 2. LOAD CONFIG (fails fast on a missing required field)
	// ================================================================
	cfg, err := config.Load[InferenceConfig]("FP")
	if err != nil {
		logger.Error("failed to load config", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("config loaded",
		slog.Int("grpc_port", cfg.GRPCPort),
		slog.Int("health_port", cfg.Port),
		slog.String("log_level", cfg.LogLevel),
		slog.Int("rate_limit_per_sec", cfg.RateLimitPerSec),
		slog.Int("circuit_failure_threshold", cfg.CircuitFailureThreshold),
	)

	// ================================================================
	// 3. OPENTELEMETRY (traces → Tempo, metrics → Prometheus)
	// ================================================================
	// NotifyContext gives a signal-aware context that drives both the OTLP setup
	// and the graceful shutdown below: SIGTERM cancels ctx → Serve returns →
	// deferred flush/drain run.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	otelShutdown, err := observability.Setup(ctx, observability.Config{
		ServiceName:    "inference-gateway",
		ServiceVersion: "dev",
		Environment:    "local",
		OTLPEndpoint:   cfg.OTelEndpoint,
		OTLPInsecure:   true, // plaintext to the local collector (no TLS cert in docker-compose)
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
	// 4. BUILD gRPC SERVER (recovery → logging → [auth] interceptor chain)
	// ================================================================
	// NO WithAuthValidator yet: the auth interceptor needs a TokenValidator that
	// calls the Auth service's ValidateToken over gRPC — wired once the auth gRPC
	// client adapter exists. When added, the data-plane RPCs require an inference
	// scope and the control-plane RPCs an elevated deploy/admin scope; the
	// principal (api_key_id, team) is read from the verified claims, never a
	// request field. Reflection is on for grpcurl/grpcui during development.
	srv := grpcutil.NewServer(
		grpcutil.WithLogger(logger),
		grpcutil.WithReflection(),
	)

	// ================================================================
	// 5. REGISTER INFERENCE HANDLER
	// ================================================================
	// The handler embeds UnimplementedInferenceGatewayServiceServer, fully
	// satisfying the server interface now; every RPC returns codes.Unimplemented
	// until implemented. We pass nil as the domain service for the scaffold phase
	// — the embedded Unimplemented methods never dereference it. Once the Redis /
	// serving-client / NATS adapters land, main.go constructs the real
	// domain.NewInferenceService(...) and passes it here (see the header block).
	inferencev1.RegisterInferenceGatewayServiceServer(srv.GRPC, handler.NewInferenceHandler(nil))
	logger.Info("inference-gateway handler registered")

	// ================================================================
	// 6. HEALTH SERVER (HTTP) — separate port from gRPC
	// ================================================================
	// gRPC is HTTP/2; kubelet probes are HTTP/1.1 — they can't share a port. A
	// separate health port also lets us flip /readyz to 503 (draining) while the
	// gRPC server finishes in-flight RPCs. No real dependency checks registered
	// yet (no Redis/NATS); later: healthHandler.AddCheck("redis", redis.Ping).
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
	// Mark gRPC health SERVING so K8s readiness passes. In the scaffold phase the
	// service has no external deps to wait on; once Redis/NATS are wired, flip
	// this to true only AFTER their readiness checks pass. Serve flips it back to
	// NOT_SERVING on SIGTERM before draining in-flight RPCs.
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
	//   4. returns here → deferred OTel flush + health-server close run
	if serveErr := srv.Serve(ctx, lis); serveErr != nil {
		logger.Error("gRPC server error", slog.String("error", serveErr.Error()))
		os.Exit(1)
	}

	logger.Info("inference-gateway stopped cleanly")
}
