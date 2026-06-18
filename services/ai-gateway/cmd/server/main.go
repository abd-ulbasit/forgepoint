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
// DATA STORES — WHY REDIS + NATS for the CORE, and OPTIONAL POSTGRES for L3: the
// gateway is on the hot path of every completion; its hot-path state is ephemeral,
// high-churn, latency-critical, and NOT a system of record (token-budget buckets,
// breaker counters). That is Redis's profile. The durable cost ledger lives in
// Billing (fed by our fp.ai.completion.served events); the warm-signal lag lives in
// NATS. The PROMPT REGISTRY (L3) adds an OPTIONAL Postgres database — its only
// relational, system-of-record concern (versioned, team-scoped prompt templates).
// It SELF-GATES on FP_DATABASE_URL: absent → the gateway runs chat-only and the
// prompt RPCs return Unimplemented; present → the prompt registry is wired live. A
// failed prompt-DB connect DEGRADES the prompt surface, it never fails the gateway
// (Redis/NATS are hot-path-critical and DO fail fast; the prompt DB does not). See
// step 6b below.
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
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/cache"
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/embed"
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/events"
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/handler"
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/prompt"
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/providers"
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/repository/postgres"
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/usage"
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

	// --- SEMANTIC CACHE (L2) knobs -------------------------------------------
	// CacheEnabled is the kill-switch for the L2 semantic cache. DEFAULT false: the
	// cache is an optimization that depends on an embedding model being available, so
	// it is opt-in (a deployment without all-minilm served by Ollama must not silently
	// fail-open on every request). When false we leave Embedder/Cache nil → the domain
	// skips the cache stage entirely (the nil-Embedder/Cache = cache OFF path).
	CacheEnabled bool `env:"AI_CACHE_ENABLED" default:"false"`
	// CacheEmbeddingModel is the Ollama model used to vectorize prompts (default
	// all-minilm — a tiny CPU model). Must be pulled/available on the Ollama server.
	CacheEmbeddingModel string `env:"AI_CACHE_EMBEDDING_MODEL" default:"all-minilm"`
	// CacheSimilarityThreshold is the minimum cosine similarity for a HIT (0 → the
	// domain's 0.95 default). Higher = stricter (fewer, more-exact hits).
	CacheSimilarityThreshold float64 `env:"AI_CACHE_SIMILARITY_THRESHOLD" default:"0.95"`
	// CacheMaxEntriesPerTeam caps a team's stored completions (the LTRIM bound that
	// keeps the per-team cache from growing unbounded). 0 → adapter default (256).
	CacheMaxEntriesPerTeam int `env:"AI_CACHE_MAX_ENTRIES_PER_TEAM" default:"256"`
	// CacheTTLSeconds is the per-team cache key TTL (idle eviction + answer-staleness
	// bound). 0 → adapter default (24h).
	CacheTTLSeconds int64 `env:"AI_CACHE_TTL_SECONDS" default:"86400"`

	// --- L4 QUALITY EVAL (M7) -----------------------------------------------
	// EvalIncludeText opts the gateway into attaching the raw prompt+response TEXT to
	// the fp.ai.completion.served event so the model-monitor's LLM-as-judge can score
	// it. DEFAULT false (PII discipline): the cost/audit event carries NO message
	// content unless an operator deliberately enables the homelab quality-eval
	// pipeline. With it off, model-monitor's judge degrades gracefully (records the
	// completion UNSCORED, notes the limitation) rather than judging blind. Off keeps
	// the served event byte-identical to its pre-L4 shape (omitempty on the wire).
	EvalIncludeText bool `env:"AI_EVAL_INCLUDE_TEXT" default:"false"`
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

	// USAGE ACCUMULATOR (the GetUsage breakdown fix): a Redis-backed per-team
	// prompt/completion counter, ALWAYS wired (over the same Redis client as the
	// budget). The budget bucket only ever saw the TOTAL via Deduct, which zeroed the
	// breakdown; this accumulator records the SPLIT so GetUsage reports a real
	// prompt/completion breakdown.
	usageStore := usage.NewRedisUsage(rdb, usage.Config{})

	// SEMANTIC CACHE (L2), behind AI_CACHE_ENABLED. When disabled we leave both ports
	// nil → the domain skips the cache stage entirely (nil Embedder/Cache = cache OFF).
	// When enabled we wire BOTH (the domain treats either being nil as off):
	//   - the Ollama embeddings adapter (text → vector), and
	//   - the Redis per-team bounded+TTL'd cache (LTRIM cap + EXPIRE so it can't grow
	//     unbounded, namespaced ai:cache:<team> so Team A never reads Team B's answer).
	var embedder domain.Embedder
	var semanticCache domain.SemanticCache
	if cfg.CacheEnabled {
		embedder = embed.NewOllamaEmbedder(cfg.OllamaURL, cfg.CacheEmbeddingModel)
		semanticCache = cache.NewRedisCache(rdb, cache.Config{
			MaxEntries: cfg.CacheMaxEntriesPerTeam,
			TTL:        time.Duration(cfg.CacheTTLSeconds) * time.Second,
		})
		logger.Info("semantic cache ENABLED (L2)",
			slog.String("embedding_model", cfg.CacheEmbeddingModel),
			slog.Float64("similarity_threshold", cfg.CacheSimilarityThreshold),
			slog.Int("max_entries_per_team", cfg.CacheMaxEntriesPerTeam),
			slog.Int64("ttl_seconds", cfg.CacheTTLSeconds))
	} else {
		logger.Info("semantic cache DISABLED (AI_CACHE_ENABLED=false); serving every request via providers")
	}

	svc := domain.NewGatewayService(domain.ServiceDeps{
		Providers:       registry,
		Breakers:        breakers,
		Budget:          budgetStore,
		Usage:           usageStore,
		Embedder:        embedder,
		Cache:           semanticCache,
		CacheThreshold:  cfg.CacheSimilarityThreshold,
		Publisher:       publisher,
		EvalIncludeText: cfg.EvalIncludeText,
		Now:             time.Now,
	})
	logger.Info("ai-gateway domain service constructed (ollama + stub, failover order)")

	// 6b. PROMPT REGISTRY (L3) — Postgres-backed, SELF-GATING on FP_DATABASE_URL.
	//
	// THE SELF-GATING CONTRACT: the gateway's CORE is Redis-only and must run
	// chat-only WITHOUT a database. The prompt registry is an OPTIONAL, Postgres-
	// backed add-on. So:
	//   - No FP_DATABASE_URL set  → we skip the DB entirely, leave promptSvc nil, and
	//     the four prompt RPCs return codes.Unimplemented (the embedded base). The
	//     gateway still serves ChatCompletion/ListProviders/GetUsage normally.
	//   - FP_DATABASE_URL set     → we connect a pgx pool (fail-fast ping in the
	//     constructor) and wire the PromptService. The prompt RPCs become live.
	//
	// WHY a failed connect does NOT os.Exit: a transient DB outage must not take down
	// the whole gateway (which can serve completions without a prompt DB). On a connect
	// error we LOG it and proceed with promptSvc nil — the prompt RPCs degrade to
	// Unimplemented, everything else keeps working. This is graceful degradation, not a
	// hard dependency. (Contrast Redis/NATS above, which ARE hot-path-critical and DO
	// fail fast — the gateway is useless without them.)
	var promptSvc prompt.PromptService
	var promptStore *postgres.PromptStore
	if cfg.DatabaseURL == "" {
		logger.Info("prompt registry DISABLED: no FP_DATABASE_URL set; prompt RPCs return Unimplemented (chat-only mode)")
	} else {
		dbCtx, dbCancel := context.WithTimeout(ctx, 10*time.Second)
		store, dbErr := postgres.NewPromptStore(dbCtx, cfg.DatabaseURL)
		dbCancel()
		if dbErr != nil {
			// DEGRADE, don't die: the gateway runs chat-only; prompt RPCs stay Unimplemented.
			logger.Error("prompt registry DB connect failed; continuing WITHOUT prompt registry (prompt RPCs return Unimplemented)",
				slog.String("error", dbErr.Error()))
		} else {
			promptStore = store
			promptSvc = prompt.NewPromptService(store, prompt.UUIDGenerator{}, prompt.RealClock{})
			logger.Info("prompt registry ENABLED (Postgres-backed, versioned, team-scoped)")
		}
	}
	// Close the pool at shutdown (only when we actually opened one).
	defer func() {
		if promptStore != nil {
			promptStore.Close()
		}
	}()

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
	// Wire BOTH services into the handler. promptSvc may be nil (no database) — then
	// NewHandlerWithPrompts is equivalent to NewHandler and the prompt RPCs stay
	// Unimplemented. One construction path regardless of whether the DB is present.
	aiv1.RegisterAIGatewayServiceServer(srv.GRPC, handler.NewHandlerWithPrompts(svc, promptSvc))
	logger.Info("ai-gateway handler registered (real service wired)",
		slog.Any("public_methods", publicMethods),
		slog.Bool("prompt_registry_enabled", promptSvc != nil))

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
