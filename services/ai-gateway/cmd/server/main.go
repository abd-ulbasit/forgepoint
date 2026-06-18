// Package main is the entrypoint for the AI Gateway service (M7 — LLMOps).
//
// ============================================================================
// WIRING — THE COMPOSITION ROOT
// ============================================================================
//
// main.go is the ONLY place that imports every layer (config, observability,
// domain, providers, budget, events, handler) and wires them into a running
// program. Each layer knows the others only through the PORTS the domain declares.
//
// STARTUP SEQUENCE (strict ordering, top to bottom):
//
//  1. Structured logger (slog JSON → stdout → Loki)
//  2. Load config (FP_* env vars → AIConfig)
//  3. Setup OpenTelemetry (traces → Tempo, metrics → Prometheus)
//  4. Connect Redis (per-team token budgets) + NATS (events + warm signal),
//     each with a readiness ping and a deferred drain/close.
//  5. Ensure the AI + AI_REQUESTS JetStream streams (idempotent).
//  6. Build the providers (Ollama from OLLAMA_URL + the deterministic Stub),
//     the breaker registry, the budget store, the event publisher → the DOMAIN
//     SERVICE → the handler.
//  7. Build the gRPC server (auth interceptor: ChatCompletion REQUIRES auth) and
//     register the handler.
//  8. HTTP health server (/healthz, /readyz with real dep checks).
//  9. Serve gRPC (blocks until SIGTERM/SIGINT).
//  10. Graceful shutdown — deferred LIFO: drain gRPC → drain NATS → close Redis →
//     close health → flush OTel.
//
// DATA STORES — WHY REDIS + NATS, NO POSTGRES (like the inference gateway): the
// gateway is on the hot path of every completion; its state is ephemeral, high-
// churn, latency-critical, and NOT a system of record (token-budget buckets,
// breaker counters). That is Redis's profile. The durable cost ledger lives in
// Billing (fed by our fp.ai.completion.served events); the warm-signal lag lives in
// NATS. The prompt registry (L3) WILL add Postgres; this core does not.
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

	aiv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/ai/v1"
	fpauth "github.com/abd-ulbasit/forgepoint/pkg/auth"
	"github.com/abd-ulbasit/forgepoint/pkg/config"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/health"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/observability"
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/budget"
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/events"
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/handler"
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/providers"
	goredis "github.com/redis/go-redis/v9"
)

// serviceName is the canonical identity for telemetry, the NATS event source, and
// consumer-group naming. It MUST match events.Source.
const serviceName = "ai-gateway"

// AIConfig extends BaseConfig with AI-gateway-specific config. config.Load[AIConfig]("FP")
// reads FP_PORT, FP_REDIS_URL, FP_OLLAMA_URL, etc. via reflection. Defaults make it
// runnable against local docker-compose with no env set (and the stub serves when
// Ollama is absent).
type AIConfig struct {
	config.BaseConfig

	// JWTSecret is the shared HMAC-SHA256 key for LOCAL JWT verification (design D2:
	// no per-request round-trip to Auth). required:"true" → fail fast if unset; a
	// gateway that can't authenticate is useless, not degraded. Injected from a K8s
	// Secret as FP_JWT_SECRET.
	JWTSecret string `env:"JWT_SECRET" required:"true"`

	// RedisURL backs the per-team token budgets. Defaulted for local docker-compose.
	RedisURL string `env:"REDIS_URL" default:"redis://localhost:6379"`

	// OllamaURL is the in-cluster Ollama serving endpoint. Defaulted to the fp-ml
	// Service; local dev can point it at a laptop Ollama or leave it (the stub serves
	// when Ollama is unreachable — failover).
	OllamaURL string `env:"OLLAMA_URL" default:"http://ollama.fp-ml.svc.cluster.local:11434"`

	// Environment tags telemetry.
	Environment string `env:"ENV" default:"local"`

	// TeamTokenBudget is the per-team token allowance per window (0 = unlimited). The
	// BUDGET (token-denominated rate limiter) knob. Conservative default; production
	// tunes per plan.
	TeamTokenBudget int64 `env:"TEAM_TOKEN_BUDGET" default:"1000000"`
	// BudgetWindowSeconds is the rolling window the budget refills over. Default 1h.
	BudgetWindowSeconds int64 `env:"BUDGET_WINDOW_SECONDS" default:"3600"`

	// CircuitFailureThreshold / CircuitResetSeconds tune the per-PROVIDER breaker that
	// drives failover. These are the CIRCUIT BREAKER knobs.
	CircuitFailureThreshold int `env:"CIRCUIT_FAILURE_THRESHOLD" default:"3"`
	CircuitResetSeconds     int `env:"CIRCUIT_RESET_SECONDS" default:"30"`
}

func main() {
	// 1. LOGGER
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// 2. CONFIG (fails fast on a missing required field)
	cfg, err := config.Load[AIConfig]("FP")
	if err != nil {
		logger.Error("failed to load config", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("config loaded",
		slog.Int("grpc_port", cfg.GRPCPort),
		slog.Int("health_port", cfg.Port),
		slog.String("environment", cfg.Environment),
		slog.String("ollama_url", cfg.OllamaURL),
		slog.Int64("team_token_budget", cfg.TeamTokenBudget),
		slog.Int("circuit_failure_threshold", cfg.CircuitFailureThreshold),
	)

	// 3. OPENTELEMETRY (signal-aware ctx drives OTLP setup + graceful shutdown)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	otelShutdown, err := observability.Setup(ctx, observability.Config{
		ServiceName:    serviceName,
		ServiceVersion: "dev",
		Environment:    cfg.Environment,
		OTLPEndpoint:   cfg.OTelEndpoint,
		OTLPInsecure:   true,
	})
	if err != nil {
		logger.Error("failed to setup observability", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer func() {
		if shutdownErr := otelShutdown(context.Background()); shutdownErr != nil {
			logger.Error("otel shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// 4a. REDIS — the per-team token-budget store.
	redisOpts, err := goredis.ParseURL(cfg.RedisURL)
	if err != nil {
		logger.Error("failed to parse redis url", slog.String("error", err.Error()))
		os.Exit(1)
	}
	rdb := goredis.NewClient(redisOpts)
	defer func() {
		if closeErr := rdb.Close(); closeErr != nil {
			logger.Error("redis close error", slog.String("error", closeErr.Error()))
		}
	}()
	pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
	if pingErr := rdb.Ping(pingCtx).Err(); pingErr != nil {
		pingCancel()
		logger.Error("redis ping failed at startup", slog.String("error", pingErr.Error()))
		os.Exit(1)
	}
	pingCancel()
	logger.Info("redis connected", slog.String("url", cfg.RedisURL))

	// 4b. NATS JetStream — events + warm signal.
	natsConn, js, err := natsutil.Connect(cfg.NATSUrl)
	if err != nil {
		logger.Error("failed to connect to NATS", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer func() {
		if drainErr := natsConn.Drain(); drainErr != nil {
			logger.Error("nats drain error", slog.String("error", drainErr.Error()))
		}
	}()
	logger.Info("NATS connected", slog.String("url", cfg.NATSUrl))

	// 5. ENSURE STREAMS (idempotent; fail fast — a gateway that can't guarantee its
	// streams would silently drop cost events and never wake Ollama).
	streamCtx, streamCancel := context.WithTimeout(ctx, 10*time.Second)
	if streamErr := events.EnsureStreams(streamCtx, js); streamErr != nil {
		streamCancel()
		logger.Error("failed to ensure AI streams", slog.String("error", streamErr.Error()))
		os.Exit(1)
	}
	streamCancel()
	logger.Info("AI JetStream streams ensured", slog.String("streams", "AI, AI_REQUESTS"))

	// 6. ADAPTERS → DOMAIN SERVICE → HANDLER.
	//
	// PROVIDERS in FAILOVER ORDER: Ollama FIRST (the real model, preferred), the
	// deterministic Stub LAST (the always-available fallback). A cold/down Ollama
	// trips its breaker and the loop fails over to the stub — so the gateway always
	// answers. This is the failover demo made real on a RAM-tight node.
	ollama := providers.NewOllamaProvider(cfg.OllamaURL)
	stub := providers.NewStubProvider()
	registry := domain.NewProviderRegistry(ollama, stub)

	breakers := domain.NewBreakerRegistry(domain.BreakerTuning{
		FailureThreshold: cfg.CircuitFailureThreshold,
		ResetTimeout:     time.Duration(cfg.CircuitResetSeconds) * time.Second,
	}, time.Now)

	budgetStore := budget.NewRedisBudget(rdb, budget.Config{
		Budget:        cfg.TeamTokenBudget,
		WindowSeconds: cfg.BudgetWindowSeconds,
	})

	publisher := events.NewPublisher(natsutil.NewPublisher(js, events.Source))

	svc := domain.NewGatewayService(domain.ServiceDeps{
		Providers: registry,
		Breakers:  breakers,
		Budget:    budgetStore,
		Publisher: publisher,
		Now:       time.Now,
	})
	logger.Info("ai-gateway domain service constructed (ollama + stub, failover order)")

	// 7. gRPC SERVER + AUTH.
	//
	// authN ("WHO are you?") is the shared JWT interceptor (local HMAC verification,
	// design D2 — no per-request Auth round-trip on the hot path). It runs on every
	// RPC, injects *grpcutil.Claims (incl. Team) into the context, and ChatCompletion
	// reads the TEAM from there — NEVER from the request body (budget-theft defense).
	//
	// SKIP LIST = health + reflection ONLY. publicMethods is EMPTY: every business RPC
	// (ChatCompletion, ListProviders, GetUsage, and the prompt RPCs) meters/authorizes
	// against the caller's team, so all REQUIRE a valid token (default-deny). Adding a
	// new RPC is authenticated automatically.
	validator, err := fpauth.NewJWTValidator([]byte(cfg.JWTSecret))
	if err != nil {
		logger.Error("failed to construct JWT validator", slog.String("error", err.Error()))
		os.Exit(1)
	}
	var publicMethods []string

	srv := grpcutil.NewServer(
		grpcutil.WithLogger(logger),
		grpcutil.WithAuthValidator(validator, append(publicMethods, grpcutil.HealthAndReflectionMethods()...)...),
		grpcutil.WithReflection(),
	)
	aiv1.RegisterAIGatewayServiceServer(srv.GRPC, handler.NewHandler(svc))
	logger.Info("ai-gateway handler registered (real service wired)", slog.Any("public_methods", publicMethods))

	// 8. HEALTH SERVER (separate port; real dep checks for readiness).
	healthHandler := health.New()
	healthHandler.AddCheck("redis", func(ctx context.Context) error {
		return rdb.Ping(ctx).Err()
	})
	healthHandler.AddCheck("nats", func(_ context.Context) error {
		if !natsConn.IsConnected() {
			return fmt.Errorf("nats not connected (status=%v)", natsConn.Status())
		}
		return nil
	})
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

	// 9. SERVE gRPC (blocks until SIGTERM/SIGINT). Flip health to SERVING only after
	// every dep is connected/checked so the readiness probe is meaningful.
	srv.SetServing(true)
	grpcAddr := fmt.Sprintf(":%d", cfg.GRPCPort)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("failed to listen on gRPC port", slog.String("addr", grpcAddr), slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("gRPC server listening", slog.String("addr", grpcAddr))

	if serveErr := srv.Serve(ctx, lis); serveErr != nil {
		logger.Error("gRPC server error", slog.String("error", serveErr.Error()))
		os.Exit(1)
	}
	logger.Info("ai-gateway stopped cleanly")
}
