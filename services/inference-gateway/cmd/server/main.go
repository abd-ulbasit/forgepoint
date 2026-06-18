// Package main is the entrypoint for the Inference Gateway service.
//
// ============================================================================
// WIRING — THE COMPOSITION ROOT (fully wired)
// ============================================================================
//
// main.go is the "composition root": the ONLY place that imports every layer
// (config, observability, domain, repository adapters, event adapters, handler)
// and wires them into a running program. Each layer knows nothing of the others
// except through the interfaces (PORTS) the domain declares.
//
// STARTUP SEQUENCE (and the strict ordering, top to bottom):
//
//	┌──────────────────────────────────────────────────────────────────────┐
//	│ 1. Structured logger (slog JSON → stdout → Loki)                      │
//	│ 2. Load config (FP_* env vars → InferenceConfig)                      │
//	│ 3. Setup OpenTelemetry (traces → Tempo, metrics → Prometheus)         │
//	│ 4. Connect datastores: Redis (hot-path state) + NATS (event bus),     │
//	│    each with a readiness ping + cleanup registered for shutdown.      │
//	│ 5. Construct adapters → publisher → DOMAIN SERVICE → handler.         │
//	│ 6. Build gRPC server (interceptor chain) + register the REAL handler. │
//	│ 7. Register/start the event SUBSCRIBERS (route table + quota reactor).│
//	│ 8. HTTP health server (/healthz liveness, /readyz readiness checks).  │
//	│ 9. Serve gRPC (blocks until SIGTERM/SIGINT).                          │
//	│ 10. Graceful shutdown — deferred in REVERSE construction order:       │
//	│     drain gRPC → stop subscribers → drain NATS → close Redis →        │
//	│     close health server → flush OTel.                                 │
//	└──────────────────────────────────────────────────────────────────────┘
//
// WHY THE SHUTDOWN ORDER MATTERS (LIFO of construction):
//   - gRPC drains FIRST (inside srv.Serve on ctx cancel) so in-flight predicts
//     finish before we tear down the things they depend on.
//   - Subscribers stop NEXT so no new event mutates the route table after we've
//     decided to exit (and so consume loops don't outlive the process).
//   - NATS drains (flushes pending publishes — the best-effort inference events)
//     BEFORE the connection closes, so a just-completed predict's event isn't lost.
//   - Redis closes after everything that uses it (limiter/route/quota) is done.
//   - OTel flushes LAST so the shutdown's own spans/logs are exported.
//
// Deferred funcs run LIFO, so registering them in construction order yields this
// exact reverse teardown for free.
//
// DATA STORES — WHY REDIS + NATS, AND NO POSTGRES:
//
//	The gateway is on the HOT PATH of every prediction; its state is ephemeral,
//	high-churn, latency-critical, and NOT a system of record (rate-limit buckets,
//	breaker counters, the route-table mirror, the quota flag). That is Redis's
//	profile, not Postgres's. The authoritative copies live elsewhere (Billing's
//	ledger, the Registry's deploy state, the durable NATS event log). So there is
//	deliberately NO pgxpool here — see internal/repository/redis/doc.go.
//
// ============================================================================
package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	inferencev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/inference/v1"
	fpauth "github.com/abd-ulbasit/forgepoint/pkg/auth"
	"github.com/abd-ulbasit/forgepoint/pkg/config"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/health"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/observability"
	"github.com/abd-ulbasit/forgepoint/services/inference-gateway/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/inference-gateway/internal/events"
	"github.com/abd-ulbasit/forgepoint/services/inference-gateway/internal/handler"
	redisrepo "github.com/abd-ulbasit/forgepoint/services/inference-gateway/internal/repository/redis"
	goredis "github.com/redis/go-redis/v9"
)

// serviceName is the canonical identity used for telemetry resource attributes,
// the NATS event source, and the consumer-group/DLQ naming. It MUST match
// events.Source (the value stamped into every published EventEnvelope.source) so
// telemetry and the event contract agree on who this process is.
const serviceName = "inference-gateway"

// InferenceConfig extends BaseConfig with gateway-specific configuration.
//
// WHY embed BaseConfig: every Forgepoint service needs Port, GRPCPort, LogLevel,
// OTelEndpoint, NATSUrl. Embedding avoids repeating them. config.Load[InferenceConfig]("FP")
// reads FP_PORT, FP_REDIS_URL, etc. via reflection (see pkg/config).
//
// The gateway is Redis-backed (rate-limit token buckets, the quota cache, and the
// warm copy of the routing table) and forwards to model-serving over gRPC — so its
// extra config is the Redis URL, the rate-limit tuning, and the breaker tuning.
// Defaults make it runnable against local docker-compose with no env set.
type InferenceConfig struct {
	config.BaseConfig

	// JWTSecret is the HMAC-SHA256 key the gateway uses to LOCALLY verify the
	// bearer JWT on every RPC (design D2: stateless local verification, no per-
	// request round-trip to the Auth service). It is the SAME shared secret Auth
	// signs tokens with — the gateway only verifies, it never mints. required:"true"
	// so the service fails fast at startup if it is unset rather than discovering it
	// on the first request (a gateway that can't authenticate is useless, not
	// degraded). pkg/auth.NewJWTValidator enforces the >= 32-byte (256-bit) floor
	// per RFC 7518 §3.2 and rejects a short key at construction.
	//
	// K8s: injected from a Secret (NOT a ConfigMap — a signing key is access-
	// controlled, not plaintext config) as FP_JWT_SECRET via envFrom. See
	// deploy/helm/fp-inference-gateway/templates/secret.yaml.
	JWTSecret string `env:"JWT_SECRET" required:"true"`

	// RedisURL is the connection string for the rate-limit buckets, the quota
	// cache, and the routing-table mirror. In K8s this points at the fp-infra
	// Redis Service. Defaulted for local docker-compose.
	RedisURL string `env:"REDIS_URL" default:"redis://localhost:6379"`

	// Environment ("local"/"staging"/"production") tags telemetry and gates the
	// in-memory idempotency-store fallback (production REFUSES it — see below).
	// Mirrors FP_ENV, which natsutil.NewMemoryProcessedStore also reads.
	Environment string `env:"ENV" default:"local"`

	// RateLimitPerSec / RateLimitBurst tune the per-API-key token bucket: refill
	// rate (tokens/sec) and bucket depth (max burst). Defaults are conservative;
	// production tunes per rate plan. These are the RATE LIMITING knobs.
	RateLimitPerSec int `env:"RATE_LIMIT_PER_SEC" default:"100"`
	RateLimitBurst  int `env:"RATE_LIMIT_BURST" default:"200"`

	// CircuitFailureThreshold / CircuitResetSeconds tune the per-backend breaker:
	// consecutive failures to trip OPEN, and the OPEN cooldown before a probe.
	// These are the CIRCUIT BREAKER knobs (consumed by NewBreakerRegistry below).
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
		slog.String("environment", cfg.Environment),
		slog.Int("rate_limit_per_sec", cfg.RateLimitPerSec),
		slog.Int("circuit_failure_threshold", cfg.CircuitFailureThreshold),
	)

	// ================================================================
	// 3. OPENTELEMETRY (traces → Tempo, metrics → Prometheus)
	// ================================================================
	// NotifyContext gives a signal-aware context that drives both the OTLP setup
	// and the graceful shutdown below: SIGTERM cancels ctx → Serve returns →
	// deferred flush/drain/close run.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	otelShutdown, err := observability.Setup(ctx, observability.Config{
		ServiceName:    serviceName,
		ServiceVersion: "dev",
		Environment:    cfg.Environment,
		OTLPEndpoint:   cfg.OTelEndpoint,
		OTLPInsecure:   true, // plaintext to the local collector (no TLS cert in docker-compose)
	})
	if err != nil {
		logger.Error("failed to setup observability", slog.String("error", err.Error()))
		os.Exit(1)
	}
	// Deferred FIRST so it runs LAST (LIFO): flush pending spans/metrics on
	// shutdown after every other component has emitted its teardown telemetry.
	defer func() {
		if shutdownErr := otelShutdown(context.Background()); shutdownErr != nil {
			logger.Error("otel shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// ================================================================
	// 4a. REDIS — the hot-path state store (buckets, quota, route mirror)
	// ================================================================
	// ParseURL turns "redis://host:port/db" into a client options struct (auth,
	// db index, TLS all encoded in the URL) — the 12-factor way to configure a
	// datastore from one env var. A parse failure is a config bug: fail fast.
	redisOpts, err := goredis.ParseURL(cfg.RedisURL)
	if err != nil {
		logger.Error("failed to parse redis url", slog.String("error", err.Error()))
		os.Exit(1)
	}
	rdb := goredis.NewClient(redisOpts)
	// Deferred close (runs after subscribers stop and NATS drains): no adapter
	// touches Redis once the gRPC server and consumers are down.
	defer func() {
		if closeErr := rdb.Close(); closeErr != nil {
			logger.Error("redis close error", slog.String("error", closeErr.Error()))
		}
	}()

	// Startup readiness gate: a PING proves the connection is live BEFORE we flip
	// gRPC to SERVING. go-redis dials lazily, so without this the first predict
	// would be the one to discover Redis is unreachable. Bounded so a wedged Redis
	// can't hang boot forever.
	pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
	if pingErr := rdb.Ping(pingCtx).Err(); pingErr != nil {
		pingCancel()
		logger.Error("redis ping failed at startup", slog.String("error", pingErr.Error()))
		os.Exit(1)
	}
	pingCancel()
	logger.Info("redis connected", slog.String("url", cfg.RedisURL))

	// ================================================================
	// 4b. NATS JetStream — the async event bus (publish + subscribe)
	// ================================================================
	// natsutil.Connect returns the raw connection (for lifecycle: Drain/Close,
	// IsConnected health) AND the JetStream context (for publish/consume). We keep
	// both: the conn for shutdown + health, the js for the publisher/subscribers.
	natsConn, js, err := natsutil.Connect(cfg.NATSUrl)
	if err != nil {
		logger.Error("failed to connect to NATS", slog.String("error", err.Error()))
		os.Exit(1)
	}
	// Deferred Drain (runs after subscribers stop, before Redis close): Drain
	// flushes any in-flight best-effort inference-event publishes and lets the
	// server ACK them before the socket closes — Close alone could drop the last
	// few events. Drain also closes the connection when it completes.
	defer func() {
		if drainErr := natsConn.Drain(); drainErr != nil {
			logger.Error("nats drain error", slog.String("error", drainErr.Error()))
		}
	}()
	logger.Info("NATS connected", slog.String("url", cfg.NATSUrl))

	// ================================================================
	// 5. CONSTRUCT ADAPTERS → PUBLISHER → DOMAIN SERVICE → HANDLER
	// ================================================================
	// The adapters are the OUTER ring: each implements a domain PORT over Redis or
	// NATS. The domain depends only on the interfaces, never these concrete types.

	// --- RouteStore: in-memory authoritative table + Redis warm-start mirror. ---
	routeStore := redisrepo.NewRouteStore(rdb)
	// Warm the table from the Redis mirror BEFORE serving and BEFORE consuming
	// events, so a freshly-scheduled replica serves the last-known routes instead
	// of NO_ROUTE-for-everything until events replay. A warm failure is non-fatal:
	// we start cold and let the consumed ModelDeployed events refill the table.
	warmCtx, warmCancel := context.WithTimeout(ctx, 5*time.Second)
	if warmErr := routeStore.Warm(warmCtx); warmErr != nil {
		logger.Warn("route table warm-start failed; starting cold (events will refill)",
			slog.String("error", warmErr.Error()))
	}
	warmCancel()

	// --- RateLimiter: atomic token bucket via a single Lua script (per api-key). ---
	rateLimiter := redisrepo.NewRateLimiter(rdb, redisrepo.RateLimiterConfig{
		RatePerSec: float64(cfg.RateLimitPerSec),
		Burst:      float64(cfg.RateLimitBurst),
	})

	// --- QuotaChecker: eventually-consistent per-team "blocked" flag (read side
	//     on the hot path; the write side is driven by the QuotaExceeded consumer). ---
	quotaChecker := redisrepo.NewQuotaChecker(rdb)

	// --- EventPublisher: domain payload → events.v1 wire message → NATS envelope.
	//     natsutil.Publisher stamps source=serviceName so EventEnvelope.source is
	//     "inference-gateway"; events.NewPublisher maps the domain → proto at the
	//     boundary. This is the domain.EventPublisher port impl. ---
	publisher := events.NewPublisher(natsutil.NewPublisher(js, events.Source))

	// --- BreakerRegistry: in-memory, one breaker per (model,version). This is the
	//     domain's default registry (pure state machine over an injected clock); a
	//     Redis-backed cross-replica composite (breaker_store.go) is the later
	//     multi-replica upgrade behind the SAME port. ---
	breakers := domain.NewBreakerRegistry(domain.BreakerTuning{
		FailureThreshold: cfg.CircuitFailureThreshold,
		ResetTimeout:     time.Duration(cfg.CircuitResetSeconds) * time.Second,
	}, time.Now)

	// --- Backend: the model-serving client port. No real gRPC serving client
	//     adapter exists in the tree yet, so we wire an explicit "unwired backend"
	//     that fails every forward with the domain's ErrUpstream sentinel. WHY a
	//     real object and not nil: the domain's Predict dereferences Backend on the
	//     hot path, so nil would panic the first request. This object keeps the
	//     resilience stack fully exercisable (the breaker records the failures, the
	//     handler maps ErrUpstream → UNAVAILABLE, the publisher emits InferenceFailed)
	//     and is the single, obvious seam the real model-serving gRPC client drops
	//     into later. It is NOT a silent stub: it is named, logged once, and returns
	//     a typed, contract-correct error rather than fabricating a fake prediction. ---
	backend := unwiredBackend{logger: logger}

	// --- The DOMAIN SERVICE: the Predict use-case that COMPOSES the resilience
	//     stack (validate → quota → rate-limit → route → split → breaker → forward
	//     → emit). Everything above is injected through ServiceDeps; the service
	//     depends on ports, not implementations. ---
	svc := domain.NewInferenceService(domain.ServiceDeps{
		Routes:    routeStore,
		Limiter:   rateLimiter,
		Backend:   backend,
		Publisher: publisher,
		Quota:     quotaChecker,
		Breakers:  breakers,
		// RandSource feeds the weighted traffic splitter. math/rand is correct here
		// (load distribution, not security); the domain injects it so tests can make
		// the split deterministic. rand.Intn panics on n<=0, but the splitter only
		// calls it with n>0 (the eligible-weight total), per its contract.
		RandSource: func(n int) int { return rand.Intn(n) }, //nolint:gosec // non-crypto traffic split
		Now:        time.Now,
	})
	logger.Info("inference domain service constructed")

	// ================================================================
	// 6. BUILD gRPC SERVER + REGISTER THE REAL HANDLER
	// ================================================================
	// AUTHENTICATION (this interceptor) vs AUTHORIZATION (the handlers):
	//
	//   - authN — "WHO are you?" — is what WithAuthValidator wires here. The shared
	//     auth UNARY/STREAM interceptor runs on EVERY RPC, reads the bearer JWT from
	//     the authorization metadata, verifies it LOCALLY with the shared HMAC secret
	//     (design D2 — no round-trip to the Auth service on the hot path; the gateway
	//     sees ALL inference traffic, so a per-request auth RPC would be a latency and
	//     availability tax), and on success injects the *grpcutil.Claims (sub, team,
	//     scopes) into the request context. On failure it short-circuits with
	//     Unauthenticated and the handler never runs.
	//
	//   - authZ — "MAY you do this?" — stays in the handlers / domain. The interceptor
	//     does NOT decide permissions; it only proves identity and populates claims.
	//     Each RPC's own check (e.g. data-plane needs an inference scope; control-plane
	//     UpsertRoute/SetTrafficSplit/DeleteRoute and version_override need an elevated
	//     deploy/admin scope) reads those claims via grpcutil.ClaimsFromContext and
	//     fails closed when the scope is absent. This interceptor is the precondition
	//     that makes those claims trustworthy and present.
	//
	// WHY fpauth.NewJWTValidator and not a gRPC call to Auth.ValidateToken: the proto's
	// design block (and design D2) call for LOCAL verification with the shared secret —
	// the validator is a pure function over the JWT signature, no network. The Auth
	// service is the only MINTER; every other service is a stateless VERIFIER holding
	// the same key. Construction fails fast if the key is < 32 bytes (RFC 7518 §3.2).
	//
	// SKIP LIST = publicMethods + health + reflection. publicMethods is EMPTY for this
	// service: there are NO genuinely-public business RPCs. Every InferenceGatewayService
	// RPC operates on a platform resource and meters/authorizes against the caller's
	// token (the proto's AUTH & TENANCY block is explicit: the principal — api_key_id,
	// team, scopes — is taken FROM THE VERIFIED TOKEN, never a request field; a client-
	// supplied principal would be account-takeover-for-billing). So Predict/BatchPredict/
	// StreamPredict, GetModelInfo, and the whole routing/breaker control plane ALL require
	// auth — default-deny. We skip ONLY the infrastructure RPCs the kubelet/grpcurl call
	// with no credential: grpc.health.v1.Health/* and the reflection services
	// (grpcutil.HealthAndReflectionMethods). Adding a new RPC to the proto is therefore
	// authenticated automatically unless someone consciously lists it public.
	//
	// Reflection is on for grpcurl/grpcui during development.
	validator, err := fpauth.NewJWTValidator([]byte(cfg.JWTSecret))
	if err != nil {
		// A weak/missing secret is a security misconfiguration, not a runtime
		// condition to limp along with: refuse to start so the pod never serves
		// traffic it cannot authenticate. (required:"true" already guarantees the
		// var is set; this additionally enforces the >= 32-byte key-strength floor.)
		logger.Error("failed to construct JWT validator", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// publicMethods: the gateway's genuinely-PUBLIC RPC full-method strings
	// (/forgepoint.inference.v1.InferenceGatewayService/<Rpc>). Empty by design —
	// every business RPC requires a valid token (see the rationale above). Declared
	// as an explicit slice so the wiring reads the same as every other service and a
	// future genuinely-public RPC has an obvious, reviewed place to be added.
	var publicMethods []string

	srv := grpcutil.NewServer(
		grpcutil.WithLogger(logger),
		grpcutil.WithAuthValidator(validator, append(publicMethods, grpcutil.HealthAndReflectionMethods()...)...),
		grpcutil.WithReflection(),
	)

	// The handler now holds the REAL domain service (not nil): every per-RPC method
	// delegates to svc, and the nil-guards in inference_handler_impl.go are inert.
	inferencev1.RegisterInferenceGatewayServiceServer(srv.GRPC, handler.NewInferenceHandler(svc))
	logger.Info("inference-gateway handler registered (real service wired)",
		// public_methods is empty: every business RPC requires a valid JWT; only
		// health + reflection bypass auth (default-deny, see the wiring comment).
		slog.Any("public_methods", publicMethods),
	)

	// ================================================================
	// 7. EVENT SUBSCRIBERS — the route table + quota cache as event reactors
	// ================================================================
	// The control plane is EVENT-DRIVEN: the gateway REACTS to ModelDeployed/
	// Undeployed/Promoted/Archived (route table) and QuotaExceeded (quota flag)
	// rather than polling. Register starts five durable consumers across three
	// streams (PIPELINES, MODELS, BILLING). Each consumer is idempotent two ways:
	// the ProcessedStore dedupes on envelope id, and the Apply* methods converge on
	// replay.
	//
	// IDEMPOTENCY STORE: production needs a SHARED, durable store (so a redelivery
	// to a different replica is recognized) — a Redis SET with TTL fits exactly.
	// We build a tiny Redis-backed ProcessedStore here. The in-memory store would
	// be wrong in production (per-replica, unbounded, lost on restart) and natsutil
	// panics if you try it with FP_ENV=production — so we never reach for it.
	processedStore := newRedisProcessedStore(rdb)

	subscribers := events.NewSubscribers(js, events.SubscriberDeps{
		Service:     svc,          // route-table reactions dispatch into the domain
		QuotaWriter: quotaChecker, // the QuotaExceeded consumer flips the team flag
	}, processedStore, events.SubConfig{
		ConsumerGroup: serviceName, // replicas of this service share the group → each event handled once
	})

	// Register is NON-FATAL on failure: the five streams are provisioned once at
	// platform bootstrap (the infra Helm chart / a stream-bootstrap job), not by
	// this service. If they don't exist yet (fresh local cluster), the DATA plane
	// (Predict) is fully independent and must still serve — so we log loudly and
	// continue rather than refusing to start. A control-plane that can't subscribe
	// surfaces as a stale route table (NO_ROUTE), which is observable and self-heals
	// once the streams exist and the consumer reconnects on the next deploy.
	if subErr := subscribers.Register(ctx); subErr != nil {
		logger.Warn("event subscribers failed to register; data plane will still serve, control plane is degraded",
			slog.String("error", subErr.Error()))
	} else {
		logger.Info("event subscribers registered (route table + quota reactors live)")
	}
	// Deferred Close (runs after gRPC drains, before NATS drains): stop the consume
	// loops so no event mutates the route table during shutdown and no goroutine
	// outlives the process. Idempotent and safe after a partial/failed Register.
	defer subscribers.Close()

	// ================================================================
	// 8. HEALTH SERVER (HTTP) — separate port from gRPC, REAL dep checks
	// ================================================================
	// gRPC is HTTP/2; kubelet probes are HTTP/1.1 — they can't share a port. A
	// separate health port also lets us flip /readyz to 503 (draining) while the
	// gRPC server finishes in-flight RPCs. Readiness now checks the REAL deps so a
	// pod with a dead Redis/NATS is pulled from the Service endpoints (liveness is
	// kept dependency-free so a dep outage never triggers a restart storm).
	healthHandler := health.New()
	healthHandler.AddCheck("redis", func(ctx context.Context) error {
		return rdb.Ping(ctx).Err()
	})
	healthHandler.AddCheck("nats", func(_ context.Context) error {
		// IsConnected is the cheap, correct readiness signal for NATS: the client
		// auto-reconnects (MaxReconnects=-1), so a transient blip flips this false
		// then true without a restart — exactly what readiness (not liveness) wants.
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
	// Deferred shutdown (runs after gRPC/subscribers/NATS/Redis): close the probe
	// endpoint last among the HTTP/grpc surfaces so K8s can still scrape /readyz
	// (now 503) while the gRPC server drains.
	defer func() {
		if shutdownErr := healthServer.Shutdown(context.Background()); shutdownErr != nil {
			logger.Error("health server shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// ================================================================
	// 9. START gRPC SERVER (blocks until SIGTERM/SIGINT)
	// ================================================================
	// Flip gRPC health to SERVING only AFTER every dependency is connected and
	// checked (Redis pinged, NATS connected, subscribers registered) — that is what
	// makes a gRPC readiness probe meaningful instead of always-green. Serve flips
	// it back to NOT_SERVING on SIGTERM before draining in-flight RPCs.
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
	//   4. returns here → deferred teardown runs LIFO:
	//        subscribers.Close → NATS Drain → Redis Close → health Shutdown → OTel flush
	if serveErr := srv.Serve(ctx, lis); serveErr != nil {
		logger.Error("gRPC server error", slog.String("error", serveErr.Error()))
		os.Exit(1)
	}

	logger.Info("inference-gateway stopped cleanly")
}

// ============================================================================
// unwiredBackend — the model-serving client port, pending the real gRPC adapter.
// ============================================================================
//
// It satisfies domain.ModelServerClient so the composition root is COMPLETE and
// the resilience stack is fully wired and exercisable end to end. It deliberately
// returns the domain's ErrUpstream sentinel rather than fabricating a prediction:
// every Predict that reaches the forward step records a breaker failure (correct —
// there is genuinely no backend), the handler maps ErrUpstream → UNAVAILABLE, and
// the publisher emits InferenceFailed{reason=UPSTREAM_ERROR}. When the real
// model-serving gRPC client adapter lands (internal/repository/grpc, dialing the
// SERVER-RESOLVED endpoint from the route target — never a client value, anti-SSRF),
// it replaces this one line in main with no other change: the seam is the port.
type unwiredBackend struct {
	logger *slog.Logger
}

// Compile-time proof we satisfy the port. If domain.ModelServerClient changes,
// this breaks here (at the wiring) rather than at a distant call site.
var _ domain.ModelServerClient = unwiredBackend{}

// Predict returns ErrUpstream: there is no model-serving backend dialed yet. The
// endpoint is the server-resolved target the splitter chose; we log it once at
// debug so an operator can see the gateway WOULD forward there, then return the
// typed sentinel the breaker and handler understand.
func (b unwiredBackend) Predict(_ context.Context, endpoint string, _ domain.PredictInput) (domain.PredictResult, error) {
	b.logger.Debug("model-serving backend not wired; failing predict with upstream error",
		slog.String("endpoint", endpoint))
	return domain.PredictResult{}, domain.ErrUpstream
}

// ============================================================================
// redisProcessedStore — a shared, durable natsutil.ProcessedStore over Redis.
// ============================================================================
//
// WHY here and not natsutil: natsutil ships only MemoryProcessedStore (tests/dev;
// it panics under FP_ENV=production because it is per-replica, unbounded, and lost
// on restart). The gateway runs multiple replicas in a consumer group, so a
// redelivery can land on a DIFFERENT replica than first processed it — only a
// SHARED store dedupes that. A Redis SET with TTL is the canonical short-window
// dedup: SETNX-style "have I seen this id?" with self-eviction so the keyspace
// stays bounded. This is the consumer-side idempotency layer (a) from the
// subscribers' two-layer guarantee; the Apply* methods' own idempotency is layer (b).
//
// The TTL bounds memory while comfortably outlasting JetStream's redelivery window:
// a duplicate arrives within seconds-to-minutes of the original, far inside 24h.
type redisProcessedStore struct {
	rdb    *goredis.Client
	prefix string
	ttl    time.Duration
}

var _ natsutil.ProcessedStore = (*redisProcessedStore)(nil)

func newRedisProcessedStore(rdb *goredis.Client) *redisProcessedStore {
	return &redisProcessedStore{
		rdb:    rdb,
		prefix: "fp:ig:processed:",
		ttl:    24 * time.Hour,
	}
}

// IsProcessed reports whether the event id is already recorded. A Redis error is
// surfaced (not swallowed): the subscriber NAKs on an idempotency-check error so we
// retry rather than risk double-processing a side-effecting event — at-least-once
// is the safe failure mode for a dedup check.
func (s *redisProcessedStore) IsProcessed(ctx context.Context, eventID string) (bool, error) {
	n, err := s.rdb.Exists(ctx, s.prefix+eventID).Result()
	if err != nil {
		return false, fmt.Errorf("redisProcessedStore: exists %s: %w", eventID, err)
	}
	return n > 0, nil
}

// MarkProcessed records the event id with a TTL. Called by the subscriber AFTER a
// successful handle and BEFORE the ACK, so a redelivery is recognized. A failure
// here makes the subscriber NAK (retry) — see the caveat in natsutil/idempotency.go:
// for TRUE exactly-once the mark must be in the same transaction as the side effect,
// which a route-table/quota cache write can't offer; this shared store is the
// library-level convenience that closes the cross-replica duplicate window.
func (s *redisProcessedStore) MarkProcessed(ctx context.Context, eventID string) error {
	if err := s.rdb.Set(ctx, s.prefix+eventID, "1", s.ttl).Err(); err != nil {
		return fmt.Errorf("redisProcessedStore: set %s: %w", eventID, err)
	}
	return nil
}
