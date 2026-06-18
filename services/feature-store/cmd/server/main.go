// Package main is the entrypoint for the Feature Store service.
//
// ============================================================================
// WIRING — THE COMPOSITION ROOT (fully wired, end-to-end)
// ============================================================================
//
// In Clean Architecture, main.go is the "composition root": the ONLY place where
// all layers are imported together and wired into a running program. It knows
// about every layer (config, observability, domain, repository, events, handler)
// but none of those layers know about each other except through the interfaces
// the domain defines (Hexagonal: the domain owns the ports; this file injects the
// adapters that fulfill them).
//
// STARTUP SEQUENCE (each step is fail-fast: a dependency that can't come up exits
// the process so K8s restarts the pod rather than serving a half-wired service):
//
//	┌────────────────────────────────────────────────────────────────────┐
//	│  1. Structured logger (slog JSON → stdout → Loki)                  │
//	│  2. Load config (FP_* env vars → FeatureStoreConfig)               │
//	│  3. Setup OpenTelemetry (traces → Tempo, metrics → Prometheus)     │
//	│  4. Connect datastores:                                            │
//	│       - Postgres pgxpool  → event log (write model) + offline view │
//	│       - Redis go-redis    → online "latest" projection            │
//	│       - NATS JetStream    → event bus (this service is a producer) │
//	│     + ensure the FEATURES stream exists (the producer owns it)     │
//	│  5. Build adapters → domain service → publishing decorator         │
//	│  6. Build gRPC server, register the REAL handler                   │
//	│  7. HTTP health server (/readyz checks Postgres+Redis+NATS)        │
//	│  8. Mark SERVING + start gRPC (blocks until SIGTERM/SIGINT)        │
//	│  9. Graceful shutdown — REVERSE of startup (drain gRPC → drain NATS │
//	│     → close Redis → close pgxpool → flush OTel → stop health)      │
//	└────────────────────────────────────────────────────────────────────┘
//
// SHUTDOWN ORDER — WHY REVERSE OF STARTUP:
//
//	On SIGTERM the signal ctx cancels and srv.Serve returns. We then tear down in
//	the OPPOSITE order we built up, so nothing is closed out from under something
//	still using it:
//	  gRPC drain (Serve)  — stop accepting RPCs, finish in-flight ones FIRST, so
//	                        no handler touches Postgres/Redis/NATS after we close them.
//	  NATS drain + close  — flush any in-flight publishes, then close the conn.
//	  Redis close         — release the online-store pool.
//	  Postgres close      — release the event-log/offline pool (closed AFTER the
//	                        gRPC drain so a slow in-flight read isn't cut off).
//	  OTel flush          — export the last spans/metrics describing the shutdown.
//	  health server close — last, so /readyz can report 503 (draining) until the
//	                        very end.
//	Go runs deferred funcs LIFO, so we register the defers in startup order and
//	they fire in reverse automatically — the language gives us the ordering for free.
//
// EVENTS — WHY A PUBLISHING DECORATOR (and not a domain change):
//
//	The Feature Store is a PURE PRODUCER on the bus (events package doc): it emits
//	fp.features.view.defined and fp.features.written and consumes nothing — so there
//	are NO subscribers to start here. The domain service deliberately does NOT import
//	NATS; the events package doc places publishing "at the wiring edge". The handler,
//	however, calls the domain service directly, so there is no per-RPC seam in main.go
//	to hang a publish on. The composition-root-clean way to make events actually fire
//	WITHOUT editing the handler or the domain is a DECORATOR: publishingService wraps
//	domain.FeatureStoreService, delegates every method to the real impl, and on a
//	successful DefineFeatureView / WriteFeatures additionally calls the EventPublisher.
//	The domain stays NATS-free; the handler still sees only the interface; the wiring
//	layer owns the "publish after the write commits" policy. A publish failure is
//	logged, NOT surfaced to the caller — the write is already durable in the log, and
//	these are thin notifications (a missed one can be backfilled by a RebuildViews-
//	style replay or an outbox in a later hardening pass), so best-effort is correct.
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
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"
	goredis "github.com/redis/go-redis/v9"

	featurestorev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/featurestore/v1"
	fpauth "github.com/abd-ulbasit/forgepoint/pkg/auth"
	"github.com/abd-ulbasit/forgepoint/pkg/config"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/health"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/observability"
	"github.com/abd-ulbasit/forgepoint/services/feature-store/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/feature-store/internal/events"
	"github.com/abd-ulbasit/forgepoint/services/feature-store/internal/handler"
	pgrepo "github.com/abd-ulbasit/forgepoint/services/feature-store/internal/repository/postgres"
	redisrepo "github.com/abd-ulbasit/forgepoint/services/feature-store/internal/repository/redis"
)

// FeatureStoreConfig extends BaseConfig with feature-store-specific configuration.
//
// WHY embed BaseConfig: every Forgepoint service needs Port, GRPCPort, LogLevel,
// OTelEndpoint, NATSUrl, DatabaseURL. config.Load[FeatureStoreConfig]("FP") reads
// FP_PORT, FP_GRPC_PORT, FP_REDIS_URL, etc. via reflection (see pkg/config).
//
// RedisURL is the online "latest" projection store (the low-latency hot-path read
// model). DatabaseURL (from BaseConfig) backs the event log (write model) + the
// offline point-in-time read model. NATSUrl (from BaseConfig) is the event bus this
// service publishes to. None carry required:"true" tags here so the binary can build
// and unit-test without env; a MISSING value still fails fast at connect time below
// (an empty DSN/URL makes pgxpool/redis/nats error), and an unreachable dependency
// fails readiness via the /readyz checks — the right place to surface "dependency
// down" (drain from the LB) versus a hard startup crash.
type FeatureStoreConfig struct {
	config.BaseConfig

	// JWTSecret is the HMAC-SHA256 verification key for inbound JWTs. The shared
	// AUTHENTICATION interceptor (wired in main below) uses it to verify the
	// signature of the bearer token on every non-public RPC — feature-store does NOT
	// mint tokens (auth does), it only VERIFIES them locally (design D2: stateless
	// local verify, no per-request round-trip to auth). The two services share the
	// SAME secret precisely so a token auth signs verifies here without a callout.
	//
	// required:"true" → the process refuses to start without it. That is the correct
	// fail-fast posture for security config: a feature-store running with auth
	// silently disabled (no validator) would serve every team's features to any
	// unauthenticated caller. pkg/auth.NewJWTValidator additionally rejects a secret
	// shorter than 32 bytes (RFC 7518 §3.2 — the HMAC key must be >= the hash output
	// width), so a weak key fails startup too, not just an absent one.
	//
	// K8s: mounted from a SECRET (never a ConfigMap — a ConfigMap is plaintext in
	// etcd and readable by anyone with get on ConfigMaps). Same FP_JWT_SECRET key the
	// auth chart uses, sourced from the SAME managed secret in prod so the signing and
	// verifying halves of the platform can never drift apart.
	JWTSecret string `env:"JWT_SECRET" required:"true"`

	// RedisURL points at the Redis backing the online (latest-value) projection.
	// Format: redis://host:port/db. Injected via env (K8s ConfigMap, or a Secret if
	// it carries credentials).
	RedisURL string `env:"REDIS_URL" default:"redis://localhost:6379"`

	// OTelInsecure controls whether the OTLP exporter uses plaintext (no TLS). It is
	// true in local docker-compose (the collector has no cert) and false in
	// production (mTLS to the collector). Wiring it FROM CONFIG — rather than
	// hard-coding true — is the correct env-driven 12-factor approach: the same
	// binary is secure in prod and convenient locally.
	OTelInsecure bool `env:"OTEL_INSECURE" default:"true"`
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
		slog.Bool("otel_insecure", cfg.OTelInsecure),
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
		OTLPInsecure:   cfg.OTelInsecure, // ← from config, not hard-coded
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
	// 4. CONNECT DATASTORES (Postgres, Redis, NATS) — fail fast on each
	// ================================================================
	//
	// 4a. POSTGRES (pgxpool) — backs BOTH the append-only event log (the write
	// model / source of truth) AND the offline point-in-time read model. pgxpool is
	// a connection POOL: the adapters borrow a connection per call and return it, so
	// concurrent RPCs share a bounded set of connections (the right model for a gRPC
	// service under load). pgxpool.New only PARSES the DSN — it does not dial — so we
	// Ping to verify the database is actually reachable before declaring readiness.
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("failed to create postgres pool", slog.String("error", err.Error()))
		os.Exit(1)
	}
	// Registered now so it fires LAST among the store closes (LIFO) — after the gRPC
	// drain has finished any in-flight read, never cutting one off mid-query.
	defer func() {
		logger.Info("closing postgres pool")
		pool.Close()
	}()
	// Bounded Ping so an unreachable DB fails startup quickly rather than hanging.
	pingCtx, cancelPing := context.WithTimeout(ctx, 5*time.Second)
	if pingErr := pool.Ping(pingCtx); pingErr != nil {
		cancelPing()
		logger.Error("postgres not reachable", slog.String("error", pingErr.Error()))
		os.Exit(1)
	}
	cancelPing()
	logger.Info("postgres connected")

	// 4b. REDIS (go-redis) — backs the online "latest" projection (the inference
	// hot path). We parse the redis:// URL into Options (host/port/db/credentials)
	// rather than hand-building Options, so the same connection-string format the
	// rest of the platform uses works here. NewClient is lazy (no dial until first
	// command), so we Ping to confirm reachability before readiness.
	redisOpts, err := goredis.ParseURL(cfg.RedisURL)
	if err != nil {
		logger.Error("failed to parse redis url", slog.String("error", err.Error()))
		os.Exit(1)
	}
	rdb := goredis.NewClient(redisOpts)
	defer func() {
		logger.Info("closing redis client")
		if closeErr := rdb.Close(); closeErr != nil {
			logger.Error("redis close error", slog.String("error", closeErr.Error()))
		}
	}()
	pingRedisCtx, cancelRedisPing := context.WithTimeout(ctx, 5*time.Second)
	if pingErr := rdb.Ping(pingRedisCtx).Err(); pingErr != nil {
		cancelRedisPing()
		logger.Error("redis not reachable", slog.String("error", pingErr.Error()))
		os.Exit(1)
	}
	cancelRedisPing()
	logger.Info("redis connected")

	// 4c. NATS JetStream — the event bus. Connect returns the raw *nats.Conn (for
	// lifecycle: Drain/Close, status checks) AND a jetstream.JetStream context (for
	// publishing). natsutil.Connect sets MaxReconnects(-1) so the client survives
	// transient blips (K8s restarts the pod only if readiness stays down).
	nc, js, err := natsutil.Connect(cfg.NATSUrl)
	if err != nil {
		logger.Error("failed to connect to nats", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer func() {
		// Drain flushes in-flight publishes and unsubscribes cleanly, THEN closes.
		// Preferred over a bare Close (which would drop anything still buffered). We
		// publish best-effort, so a drain error is logged, not fatal.
		logger.Info("draining nats connection")
		if drainErr := nc.Drain(); drainErr != nil {
			logger.Error("nats drain error", slog.String("error", drainErr.Error()))
		}
	}()

	// ENSURE THE FEATURES STREAM EXISTS. JetStream silently DROPS a published
	// message whose subject no stream captures (core-NATS fire-and-forget leaks
	// through when there is no stream). The PRODUCER therefore guarantees its own
	// stream before publishing. CreateOrUpdateStream is idempotent + convergent: if
	// the stream already exists (e.g. infra-as-code created it), this reconciles the
	// config rather than failing. The FEATURES stream binds fp.features.> — both of
	// this service's subjects (events.SubjectFeatureViewDefined / FeaturesWritten).
	streamCtx, cancelStream := context.WithTimeout(ctx, 10*time.Second)
	if _, streamErr := js.CreateOrUpdateStream(streamCtx, jetstream.StreamConfig{
		Name:     events.StreamName,
		Subjects: []string{events.StreamSubjects},
	}); streamErr != nil {
		cancelStream()
		logger.Error("failed to ensure FEATURES stream", slog.String("error", streamErr.Error()))
		os.Exit(1)
	}
	cancelStream()
	logger.Info("nats connected; FEATURES stream ensured",
		slog.String("stream", events.StreamName),
		slog.String("subjects", events.StreamSubjects),
	)

	// ================================================================
	// 5. BUILD ADAPTERS → DOMAIN SERVICE → PUBLISHING DECORATOR
	// ================================================================
	//
	// This is dependency inversion made concrete: the domain declares the ports
	// (EventLog, OnlineViewStore, OfflineViewStore, Clock, IDGenerator); here we
	// construct the infrastructure adapters that satisfy them and INJECT them. The
	// domain never imported pgx, go-redis, or nats — it only ever sees its own
	// interfaces, which these adapters implement.
	eventLog := pgrepo.NewEventLog(pool)             // domain.EventLog (append-only write model)
	offlineStore := pgrepo.NewOfflineViewStore(pool) // domain.OfflineViewStore (durable as-of read model)
	onlineStore := redisrepo.NewOnlineViewStore(rdb) // domain.OnlineViewStore (Redis latest read model)

	// Production Clock + IDGenerator — the impure edges injected so the domain core
	// stays deterministic in tests (tests pass a fixed clock + counting idgen). The
	// domain ships no production impls (they belong at the wiring edge, not the pure
	// core), so we provide these tiny adapters here. uuid.NewString is crypto/rand-
	// backed → ids are unguessable (they appear in URLs/logs; a guessable id is an
	// enumeration handle), which is the security property ports.go calls out.
	svc := domain.NewFeatureStoreService(
		eventLog,
		onlineStore,
		offlineStore,
		systemClock{},
		uuidGenerator{},
	)

	// The NATS-backed publisher (events.EventPublisher). NewPublisher wraps a
	// natsutil.Publisher bound to this service's source name, inheriting the
	// envelope/dedup/trace machinery; the adapter only builds the canonical
	// events.v1 payload and protojson-encodes it.
	publisher := events.NewPublisher(natsutil.NewPublisher(js, events.SourceName))

	// PRODUCER-EDGE publish deduper: one process-wide instance shared by the
	// decorator so a replayed (retried) command does not re-emit its event. The
	// domain already dedupes the APPEND on the idempotency key; this dedupes the
	// PUBLISH on the same key. In-memory is acceptable for THIN notification events
	// whose consumers are idempotent; a durable outbox replaces it in a later
	// hardening pass (the events.PublishDeduper port is the seam for that swap).
	dedup := events.NewInMemoryDeduper()

	// DECORATE: wrap the domain service so a successful Define/Write also publishes
	// the corresponding fp.features.* event. See the package doc ("EVENTS — WHY A
	// PUBLISHING DECORATOR"). The handler receives this decorated service through the
	// SAME domain.FeatureStoreService interface — it cannot tell the difference, which
	// is exactly the point: publishing is a wiring-edge concern, transparent to the
	// inbound ring.
	wiredSvc := newPublishingService(svc, publisher, dedup, logger)

	// ================================================================
	// 6. BUILD gRPC SERVER + REGISTER THE REAL HANDLER
	// ================================================================
	// grpcutil.NewServer applies the standard interceptor chain (recovery →
	// logging → auth → tracing). WithReflection lets grpcurl/grpcui introspect the
	// service in development.
	//
	// AUTHENTICATION (this interceptor) vs AUTHORIZATION (the handlers) — the
	// division of labor is the interview-critical point:
	//
	//   - AUTHN here: fpauth.NewJWTValidator verifies the bearer token's HS256
	//     signature LOCALLY with the shared FP_JWT_SECRET (design D2: stateless
	//     local verify, no per-RPC round-trip to the auth service) and, on success,
	//     puts the caller's *grpcutil.Claims (user_id, team, role, scopes) into the
	//     request context. It answers ONLY "who are you, and is your token genuine
	//     and unexpired" — it does NOT decide what you may do.
	//
	//   - AUTHZ in the handlers: each RPC reads those claims via
	//     grpcutil.ClaimsFromContext and makes the PER-RPC permission decision —
	//     the proto's "Requires features:read / features:write / features:admin"
	//     scopes, plus the team-scoping that stops one team reading another team's
	//     views (the mass-assignment / cross-team guards documented throughout
	//     featurestore.proto). Without the interceptor those claims would never be
	//     in the context and every authz check would fail closed (Unauthenticated),
	//     making the whole service unreachable — so this wiring is what makes the
	//     handlers' authz checks actually work, not a bypass of them.
	//
	// PUBLIC METHODS — DEFAULT-DENY, and feature-store has NO public business RPCs:
	//   Every FeatureStoreService RPC operates on TEAM-SCOPED platform resources
	//   (define/read/write/serve/delete/rebuild feature views) and the proto marks
	//   each one as requiring a scope derived from an authenticated principal —
	//   there is no "log in" or "validate token" style RPC here that must be
	//   reachable WITHOUT a credential (that lives in the auth service). So the
	//   service-specific public set is EMPTY: we skip ONLY the gRPC health + server
	//   reflection methods (grpcutil.HealthAndReflectionMethods), which the kubelet
	//   probe and grpcurl call with no token. Everything else requires a valid JWT.
	//   Adding a future RPC therefore requires auth automatically unless someone
	//   consciously adds it to publicMethods — the secure default (default-deny).
	//
	// REUSABLE PATTERN (mirrors services/auth/cmd/server/main.go): build the
	// validator from the shared secret, then
	//   WithAuthValidator(validator, append(publicMethods, HealthAndReflectionMethods()...)...).
	validator, err := fpauth.NewJWTValidator([]byte(cfg.JWTSecret))
	if err != nil {
		// Fail fast: a missing/short secret means we cannot authenticate callers.
		// Starting anyway would mean either no auth (open service) or every RPC
		// rejected — both worse than refusing to boot so K8s surfaces the misconfig.
		logger.Error("failed to build JWT validator", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// publicMethods: the feature-store RPCs that bypass authentication. EMPTY by
	// design (see the comment above) — no FeatureStoreService RPC is intended for
	// unauthenticated access. We keep it as a named, explicitly-empty slice rather
	// than inlining nothing so the default-deny stance is visible and a future
	// genuinely-public RPC has an obvious, single place to be added (consciously).
	publicMethods := []string{}

	srv := grpcutil.NewServer(
		grpcutil.WithLogger(logger),
		// Append the constant health+reflection exemptions to this service's public
		// set; the variadic spread passes the full skip list to the interceptor.
		grpcutil.WithAuthValidator(validator, append(publicMethods, grpcutil.HealthAndReflectionMethods()...)...),
		grpcutil.WithReflection(),
	)

	// Register the FULLY-WIRED handler (real service, NOT nil). Every RPC now hits
	// the event-sourcing engine backed by Postgres + Redis, and Define/Write fire
	// NATS events via the decorator.
	featurestorev1.RegisterFeatureStoreServiceServer(srv.GRPC, handler.NewFeatureStoreHandler(wiredSvc))
	logger.Info("feature-store handler registered (wired: postgres event log + redis online + nats publisher; JWT auth enabled)",
		slog.Any("public_methods", publicMethods), // empty: every business RPC requires a valid JWT
	)

	// ================================================================
	// 7. HEALTH SERVER (HTTP) — readiness checks the REAL dependencies
	// ================================================================
	// gRPC uses HTTP/2; kubelet probes use HTTP/1.1 — they can't share a port, so
	// health runs on its own HTTP port. This lets us flip /readyz to 503 (draining)
	// while the gRPC server finishes in-flight RPCs. Liveness (/healthz) NEVER checks
	// dependencies (a DB blip must not restart-storm the fleet); readiness (/readyz)
	// checks Postgres + Redis + NATS so a pod with a dead dependency is pulled from
	// the Service endpoints until it recovers.
	healthHandler := health.New()
	healthHandler.AddCheck("postgres", func(ctx context.Context) error {
		return pool.Ping(ctx)
	})
	healthHandler.AddCheck("redis", func(ctx context.Context) error {
		return rdb.Ping(ctx).Err()
	})
	healthHandler.AddCheck("nats", func(_ context.Context) error {
		// IsConnected is the cheap, non-blocking liveness signal for the NATS conn.
		// During an automatic reconnect it is briefly false — readiness correctly
		// reflects "can't publish right now", and recovers when the client reconnects.
		if !nc.IsConnected() {
			return fmt.Errorf("nats not connected (status: %s)", nc.Status())
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
		// Closed LAST (registered first among the post-OTel defers would be wrong; it
		// is registered here so it fires before the OTel/store defers? No — Go is LIFO,
		// so this, registered last, fires FIRST). We WANT health to stop early enough
		// that /readyz reports unavailable, but the gRPC drain (below, runs before any
		// defer) already pulled us from traffic. Closing it here is clean teardown.
		if shutdownErr := healthServer.Shutdown(context.Background()); shutdownErr != nil {
			logger.Error("health server shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// ================================================================
	// 8. START gRPC SERVER (mark SERVING only now that all deps are up)
	// ================================================================
	// We reach SetServing(true) ONLY after Postgres/Redis/NATS connected and the
	// stream exists — so the gRPC readiness signal is honest: green means the service
	// can actually serve. grpcutil.Server.Serve flips it to NOT_SERVING on SIGTERM
	// before draining (the graceful-shutdown dance).
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
	//   2. GracefulStop (bounded drain) → in-flight RPCs finish (still touching
	//      Postgres/Redis/NATS, which are STILL OPEN — the drain runs before any
	//      store-closing defer fires)
	//   3. hard Stop if drain times out
	//   4. returns here; the deferred teardowns run LIFO: health → NATS drain →
	//      Redis close → Postgres close → OTel flush (reverse of startup).
	if serveErr := srv.Serve(ctx, lis); serveErr != nil {
		logger.Error("gRPC server error", slog.String("error", serveErr.Error()))
		os.Exit(1)
	}

	logger.Info("feature-store service stopped cleanly")
}

// ============================================================================
// systemClock / uuidGenerator — the production impure-edge adapters
// ============================================================================
//
// The domain (ports.go) injects a Clock and an IDGenerator so the pure service
// core is deterministic under test (a fixed clock + a counting id generator let
// tests assert exact timestamps/ids). The domain ships NO production impls on
// purpose — those are wiring-edge concerns — so the composition root provides
// them. They are deliberately trivial: the WHOLE value of the seam is that these
// two real implementations are the only place wall-clock time and crypto-random
// ids enter the system.
type systemClock struct{}

// Now returns the real wall-clock time. The domain stamps created_at / appended_at
// / event_time from this in production.
func (systemClock) Now() time.Time { return time.Now() }

type uuidGenerator struct{}

// NewID mints a UUID v4. google/uuid is crypto/rand-backed, so ids are unguessable
// — important because ids appear in URLs and logs and a guessable id is an
// enumeration handle (the security note ports.go makes).
func (uuidGenerator) NewID() string { return uuid.NewString() }

// ============================================================================
// publishingService — the WIRING-EDGE PUBLISHING DECORATOR
// ============================================================================
//
// Wraps a domain.FeatureStoreService and adds "publish the matching event after a
// successful mutating call". This is the Decorator pattern applied at the
// composition root: it implements the SAME interface it wraps, so the handler is
// oblivious to it (it sees only domain.FeatureStoreService). Every method delegates
// to the inner service; the two MUTATING methods that have a corresponding event
// (DefineFeatureView → fp.features.view.defined, WriteFeatures → fp.features.written)
// additionally call the publisher AFTER the inner call succeeds.
//
// WHY a decorator and not publishing inside the domain: the domain must not import
// NATS (Clean Architecture). The events package doc states publishing belongs "at
// the wiring edge". The handler calls the service directly, so main.go has no
// per-RPC hook EXCEPT by decorating the service it injects. This is that hook, kept
// in the composition root where infrastructure concerns belong.
//
// PUBLISH FAILURE POLICY — best-effort, logged not propagated: the write already
// committed to the append-only log (the source of truth) before we publish. These
// are THIN notification events; a missed one is recoverable (a RebuildViews replay,
// or an outbox in a later hardening pass). Returning a publish error to the caller
// would wrongly imply the write failed and could trigger a retry that re-appends
// (the idempotency key guards that, but the caller-visible error would still be a
// lie). So we log and return the successful domain result.
//
// WriteFeatures needs the FeatureView (for id+name) to build the event, but the
// service returns only a WriteFeaturesResult. We fetch the view via the inner
// service's GetFeatureViewByID (team-scoped, same Principal) to hand the publisher
// what it needs — one extra cheap read on the write path, off the latency-critical
// read path.
type publishingService struct {
	domain.FeatureStoreService // embedded: inherit all methods, override the two that publish
	publisher                  events.EventPublisher
	dedup                      events.PublishDeduper // skip re-publish on a replayed (retried) command
	logger                     *slog.Logger
}

// newPublishingService builds the decorator. We embed the inner service so the
// non-publishing methods (GetFeatureView*, ListFeatureViews, GetOnlineFeatures,
// GetHistoricalFeatures, DeleteFeatureView, RebuildViews) pass through unchanged —
// only DefineFeatureView and WriteFeatures are overridden below.
//
// The dedup port is the PRODUCER-EDGE idempotency guard: the domain's Append is
// idempotent on the command's IdempotencyKey (a retried command appends nothing
// and returns replayed=true), but a naive decorator would still re-emit the event
// on that retry — a fresh envelope id (natsutil mints a new uuid per publish)
// slips past JetStream's Nats-Msg-Id dedup, so consumers see a duplicate. We gate
// the publish on the SAME key the domain dedupes the append on, so a replayed
// command does not re-publish. See events/dedup.go for the full rationale.
func newPublishingService(inner domain.FeatureStoreService, publisher events.EventPublisher, dedup events.PublishDeduper, logger *slog.Logger) domain.FeatureStoreService {
	return &publishingService{
		FeatureStoreService: inner,
		publisher:           publisher,
		dedup:               dedup,
		logger:              logger,
	}
}

// DefineFeatureView delegates, then publishes fp.features.view.defined on success.
//
// IDEMPOTENT PUBLISH: if a command with this IdempotencyKey already had its event
// published (a replayed/retried define — the domain appended nothing new and
// returned the original view), we SKIP the publish. Re-emitting would be pure
// duplication: a fresh envelope id would slip past JetStream dedup and downstream
// consumers would record the same schema-version twice. See events/dedup.go.
func (s *publishingService) DefineFeatureView(ctx context.Context, p domain.Principal, in domain.DefineFeatureViewInput) (domain.FeatureView, error) {
	view, err := s.FeatureStoreService.DefineFeatureView(ctx, p, in)
	if err != nil {
		return view, err // domain error: do NOT publish (nothing committed)
	}
	if s.dedup.AlreadyPublished(in.IdempotencyKey) {
		// Replayed command: the event was already emitted on the original attempt.
		s.logger.Debug("skip publish FeatureViewDefined: replayed command (already published)",
			slog.String("feature_view_id", view.ID),
			slog.String("idempotency_key", in.IdempotencyKey),
		)
		return view, nil
	}
	if pubErr := s.publisher.PublishFeatureViewDefined(ctx, view); pubErr != nil {
		// Best-effort: the definition is durable in the log; log and move on. We do
		// NOT mark the key published, so a later retry of this command will attempt
		// the publish again (at-least-once-to-publish) rather than silently dropping
		// the event — the key gate only records a SUCCESSFUL publish.
		s.logger.Warn("publish FeatureViewDefined failed (definition is durable)",
			slog.String("feature_view_id", view.ID),
			slog.String("error", pubErr.Error()),
		)
		return view, nil
	}
	// Publish succeeded: record the key so a replayed command does not re-emit.
	s.dedup.MarkPublished(in.IdempotencyKey)
	return view, nil
}

// WriteFeatures delegates, then publishes fp.features.written on success.
//
// IDEMPOTENT PUBLISH: the domain's Append is idempotent on in.IdempotencyKey — a
// retried write appends nothing and returns the ORIGINAL version range. We gate
// the publish on that same key so a replayed write does not re-emit
// fp.features.written. Without this gate the retry mints a fresh envelope id that
// slips past JetStream's Nats-Msg-Id dedup, and downstream consumers (model-
// monitor, experiment-tracker) would apply the same feature-write twice. See
// events/dedup.go for why keying on the idempotency key is safer than reading a
// replayed bool (it also recovers a crash-after-append-before-publish).
func (s *publishingService) WriteFeatures(ctx context.Context, p domain.Principal, in domain.WriteFeaturesInput) (domain.WriteFeaturesResult, error) {
	res, err := s.FeatureStoreService.WriteFeatures(ctx, p, in)
	if err != nil {
		return res, err // domain error: nothing appended, do NOT publish
	}
	if s.dedup.AlreadyPublished(in.IdempotencyKey) {
		// Replayed write: the event was already emitted on the original attempt.
		s.logger.Debug("skip publish FeaturesWritten: replayed command (already published)",
			slog.String("feature_view_id", in.FeatureViewID),
			slog.String("idempotency_key", in.IdempotencyKey),
		)
		return res, nil
	}
	// The event needs the view's id+name; the result doesn't carry them. Fetch the
	// view (team-scoped via the same Principal). If the read fails we still return
	// the successful write — the publish is best-effort, never a write-path failure.
	view, viewErr := s.FeatureStoreService.GetFeatureViewByID(ctx, p, in.FeatureViewID)
	if viewErr != nil {
		s.logger.Warn("skip publish FeaturesWritten: could not load view (write is durable)",
			slog.String("feature_view_id", in.FeatureViewID),
			slog.String("error", viewErr.Error()),
		)
		return res, nil
	}
	if pubErr := s.publisher.PublishFeaturesWritten(ctx, view, res); pubErr != nil {
		// Best-effort: do NOT mark the key published, so a retry re-attempts the
		// publish rather than dropping the event (the gate records only SUCCESS).
		s.logger.Warn("publish FeaturesWritten failed (write is durable)",
			slog.String("feature_view_id", view.ID),
			slog.Int("written_count", res.WrittenCount),
			slog.String("error", pubErr.Error()),
		)
		return res, nil
	}
	// Publish succeeded: record the key so a replayed write does not re-emit.
	s.dedup.MarkPublished(in.IdempotencyKey)
	return res, nil
}
