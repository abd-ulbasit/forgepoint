// Package main is the entrypoint for the Model Registry service.
//
// ============================================================================
// WIRING — THE COMPOSITION ROOT (fully wired: Postgres + Redis + NATS)
// ============================================================================
//
// In Clean Architecture, main.go is the "composition root": the ONLY place that
// imports every layer (config, observability, repository adapters, the domain
// service, the event adapter, the handler) and wires them into a running program.
// The layers themselves know each other only through the interfaces the domain
// defines (WriteStore / ReadStore / ProjectionEmitter / Clock / IDGenerator). This
// file is where those ports get their concrete adapters — "Postgres for writes,
// Redis for reads, NATS for events" becomes literal here and nowhere else.
//
// THE DEPENDENCY-INJECTION GRAPH WE ASSEMBLE (inner depends on nothing outer):
//
//	pgxpool ─► postgres.WriteStore ─┐
//	go-redis ─► redis.ReadStore ────┤
//	                                 ├─► domain.NewRegistryService ─► handler.RegistryHandler ─► gRPC
//	jetstream ─► natsutil.Publisher ─► events.Publisher (ProjectionEmitter) ─┘
//	                                 │
//	                  domain.NewRealClock() + domain.NewUUIDGenerator() (pure ports)
//
// STARTUP SEQUENCE (each step fail-fast — a bad dep aborts boot, never serves):
//
//	┌──────────────────────────────────────────────────────────────────────────┐
//	│ 1. Structured logger (slog JSON → stdout → Loki)                           │
//	│ 2. Load config (FP_* env vars → RegistryConfig); require DatabaseURL       │
//	│ 3. Setup OpenTelemetry (traces → Tempo, metrics → Prometheus)              │
//	│ 4. Connect datastores (Postgres pool, Redis client, NATS+JetStream)        │
//	│    — each constructor PINGS so an unreachable dep fails boot, not RPC #1    │
//	│ 5. Build adapters → domain service → real handler (NOT nil)                │
//	│ 6. Build gRPC server, register the WIRED handler                           │
//	│ 7. Start HTTP health server (/readyz checks Postgres, Redis, NATS live)    │
//	│ 8. Mark SERVING + start gRPC server (blocks until SIGTERM/SIGINT)          │
//	│ 9. Graceful shutdown in LIFO order (see the shutdown note at the bottom)   │
//	└──────────────────────────────────────────────────────────────────────────┘
//
// EVENT SUBSCRIBERS — WHY THERE ARE NONE TO START HERE (deliberate, not missing):
//
//	The Registry is the sole event PRODUCER of five model-lifecycle facts
//	(fp.models.registered / version.created / version.ready / promoted / archived).
//	It is ALSO its own internal CONSUMER: the CQRS PROJECTION that rebuilds the
//	Redis read model from those same events (events.Projection). Commands write
//	Postgres (the truth) and emit the events; the projection consumes them and
//	UPSERTS Redis; queries read Redis. Without the projection wired here, the read
//	side stays EMPTY — POST /models would write Postgres but GET /models would
//	return [] (the bug this wiring fixes). So step 5 DOES start a subscriber for
//	this service: the projection consumer, bound to the registry's own MODELS
//	stream, started under the serve context and Closed on shutdown.
//
//	The projection is an idempotent, durable consumer (one durable per subject,
//	load-merge-upsert handlers, a ProcessedStore fast-path, a DLQ for poison
//	events) — see internal/events/projection.go. It is intentionally IN-PROCESS
//	here for simplicity; the CQRS split still holds (read store is independently
//	rebuildable by replaying the stream), and extracting it to a separate
//	deployable later is a wiring change, not an architecture change.
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

	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"

	registryv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/registry/v1"
	fpauth "github.com/abd-ulbasit/forgepoint/pkg/auth"
	"github.com/abd-ulbasit/forgepoint/pkg/config"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/health"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/observability"
	"github.com/abd-ulbasit/forgepoint/services/registry/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/registry/internal/events"
	"github.com/abd-ulbasit/forgepoint/services/registry/internal/handler"
	"github.com/abd-ulbasit/forgepoint/services/registry/internal/repository/postgres"
	redisstore "github.com/abd-ulbasit/forgepoint/services/registry/internal/repository/redis"
)

// depConnectTimeout bounds how long we wait, AT BOOT, for each datastore's
// fail-fast ping. WHY a bound: a constructor that blocks forever on an
// unreachable dependency would hang the pod in a not-ready state with no signal;
// a bounded ctx turns "DB is down" into a clear boot error K8s can act on (the
// pod CrashLoops and surfaces the cause in logs/events). Kept short — if a core
// dependency isn't reachable in 10s at startup, failing fast is the right call.
const depConnectTimeout = 10 * time.Second

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
	// stores are separate in CQRS. NewReadStore takes a host:port "addr" (not a URL),
	// so the default is bare host:port and we pass it through directly. A full
	// "redis://" URL with auth/db would need redis.ParseURL — a later concern when
	// Redis grows credentials; bare addr is correct for the local/in-cluster stack.
	RedisURL string `env:"REDIS_URL" default:"localhost:6379"`

	// OTelInsecure controls whether the OTLP exporter uses plaintext (no TLS). It is
	// true in local docker-compose (the collector has no cert) and false in
	// production (mTLS to the collector). Wiring it FROM CONFIG — rather than
	// hard-coding true as the early auth scaffold did — is the correct, env-driven
	// 12-factor approach: the same binary is secure in prod and convenient locally.
	OTelInsecure bool `env:"OTEL_INSECURE" default:"true"`

	// JWTSecret is the SHARED HMAC-SHA256 verification key for inbound JWTs.
	//
	// WHY the registry needs a JWT secret even though it never MINTS tokens (only
	// auth does): under design D2 (LOCAL verification — see pkg/auth/validator.go),
	// every non-auth service verifies the JWT signature IN-PROCESS rather than
	// calling auth.ValidateToken on the hot path. That local HMAC verify needs the
	// SAME secret auth signed with — so this key is the verify-side counterpart of
	// auth's sign-side FP_JWT_SECRET. It MUST be byte-identical to auth's secret
	// (same K8s Secret value), or every token auth issues fails verification here.
	//
	// required:"true" — the auth interceptor cannot be built without it, and a
	// registry that can't authenticate any caller is useless; fail-fast at startup
	// (config.Load returns an error) is the correct posture for security config,
	// exactly as auth does. pkg/auth.NewJWTValidator additionally rejects a secret
	// shorter than 32 bytes (RFC 7518 §3.2 — the HMAC key must be >= the SHA-256
	// output width), so a weak key is caught at construction, not under load.
	//
	// K8s: injected from a Secret (never a ConfigMap — secrets are access-controlled,
	// ConfigMaps are plaintext), mirroring auth's FP_JWT_SECRET wiring.
	JWTSecret string `env:"JWT_SECRET" required:"true"`
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

	// DatabaseURL has no compile-time `required` tag because it lives on the SHARED
	// BaseConfig (some services — e.g. a pure gateway — have no DB). For the registry
	// it is mandatory: the WriteStore is the source of truth. We enforce it HERE, at
	// the one place that knows this service needs a DB, rather than mutating the
	// shared struct. Failing now (not on the first CreateModel) is the fail-fast rule.
	if cfg.DatabaseURL == "" {
		logger.Error("FP_DATABASE_URL is required for the registry service (the Postgres write store)")
		os.Exit(1)
	}

	logger.Info("config loaded",
		slog.Int("grpc_port", cfg.GRPCPort),
		slog.Int("health_port", cfg.Port),
		slog.String("log_level", cfg.LogLevel),
		slog.String("nats_url", cfg.NATSUrl),
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
	// NOTE ON SHUTDOWN ORDERING: we DON'T `defer otelShutdown` here. Telemetry must
	// flush LAST (after the gRPC server has drained and the stores are closed) so the
	// spans/metrics emitted DURING shutdown are captured. All cleanup is therefore
	// hung off an explicit, ordered shutdown() closure invoked once at the very end,
	// rather than a stack of defers whose LIFO order is hard to read. See the bottom.

	// ================================================================
	// 4. CONNECT DATASTORES (Postgres WRITE, Redis READ, NATS events) — fail-fast
	// ================================================================
	// We own the RAW pgxpool.Pool and go-redis.Client here (not just the adapters)
	// for two reasons the adapters' own constructors can't give us from main:
	//   - /readyz needs a live ping handle on each dep, and
	//   - ordered shutdown needs to Close the pool/client at the right moment.
	// We PING each at boot (fail-fast: an unreachable dep aborts startup, never
	// serves) and then wrap the pool/client with the adapters' *FromPool / *FromClient
	// constructors — the exact seam the adapters expose for "I already hold the handle"
	// (the same one the integration tests use to share one pool). A BOUNDED ctx (not
	// the signal ctx) guards against a hung dial wedging startup indefinitely.
	bootCtx, bootCancel := context.WithTimeout(context.Background(), depConnectTimeout)
	defer bootCancel()

	// --- Postgres: the CQRS WRITE store (source of truth) ---
	// pgxpool.New is LAZY (no TCP until first use); Ping forces a real connection so a
	// bad DSN / unreachable DB fails boot here, not on the first RegisterModel RPC.
	pgPool, err := pgxpool.New(bootCtx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("failed to create Postgres pool", slog.String("error", err.Error()))
		os.Exit(1)
	}
	if err := pgPool.Ping(bootCtx); err != nil {
		pgPool.Close()
		logger.Error("failed to ping Postgres", slog.String("error", err.Error()))
		os.Exit(1)
	}
	writeStore := postgres.NewWriteStoreFromPool(pgPool) // domain.WriteStore
	logger.Info("postgres write store connected")

	// --- Redis: the CQRS READ projection (eventually-consistent query side) ---
	// go-redis's Client is itself a pool; Ping fails fast on an unreachable server.
	rdb := goredis.NewClient(&goredis.Options{Addr: cfg.RedisURL})
	if err := rdb.Ping(bootCtx).Err(); err != nil {
		_ = rdb.Close()
		pgPool.Close()
		logger.Error("failed to ping Redis", slog.String("error", err.Error()))
		os.Exit(1)
	}
	readStore := redisstore.NewReadStoreFromClient(rdb) // domain.ReadStore
	logger.Info("redis read store connected")

	// --- NATS JetStream: the event transport behind the ProjectionEmitter ---
	// Connect returns the raw conn (for lifecycle: Drain on shutdown, IsConnected for
	// the readiness check) AND the JetStream context (for the Publisher). We keep the
	// conn so /readyz can report NATS health and so shutdown can DRAIN it (flush
	// buffered publishes) rather than hard-Close mid-flight.
	natsConn, js, err := natsutil.Connect(cfg.NATSUrl)
	if err != nil {
		_ = rdb.Close()
		pgPool.Close()
		logger.Error("failed to connect NATS", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("nats connected", slog.String("url", natsConn.ConnectedUrl()))

	// --- ENSURE THE MODELS STREAM EXISTS (producer owns its stream) ---
	// The registry is the OWNER and sole producer of the fp.models.* lifecycle tree.
	// JetStream REJECTS a publish whose subject no stream captures ("no stream matches
	// subject", 10073), and our documented Emit posture is to LOG that error WITHOUT
	// failing the already-committed write — so on a fresh cluster with no MODELS stream,
	// every lifecycle event would be silently dropped while writes commit, and the whole
	// downstream serve/deploy/meter/route/re-baseline flow would die. We therefore
	// GUARANTEE our own stream here, before wiring the emitter, exactly as auth and
	// feature-store do for their trees. EnsureStream uses CreateOrUpdateStream
	// (idempotent + convergent — safe on every boot and under rolling deploys / a
	// concurrent model-monitor declaring fp.models.drift.detected on the same stream).
	// We FAIL FAST: an unprovisionable stream aborts boot (K8s CrashLoops with the cause
	// in logs) rather than letting the pod serve and drop every event at runtime. The
	// context is bounded (10s, like the dep pings) so a hung NATS server can't wedge boot.
	streamCtx, streamCancel := context.WithTimeout(context.Background(), depConnectTimeout)
	if err := events.EnsureStream(streamCtx, js); err != nil {
		streamCancel()
		_ = natsConn.Drain()
		_ = rdb.Close()
		pgPool.Close()
		logger.Error("failed to ensure MODELS stream", slog.String("error", err.Error()))
		os.Exit(1)
	}
	streamCancel()
	logger.Info("MODELS stream ensured",
		slog.String("stream", events.StreamName),
		slog.String("subjects", events.StreamSubjects),
	)

	// ================================================================
	// 5. BUILD ADAPTERS → DOMAIN SERVICE → REAL HANDLER
	// ================================================================
	// The event adapter chain: a pkg/natsutil.Publisher (envelope/dedup/trace) wrapped
	// by events.Publisher, which maps a domain.ProjectionEvent → the canonical
	// eventsv1.* payload + fp.models.* subject. events.Publisher satisfies the
	// domain.ProjectionEmitter port — the domain stays wire-agnostic, this is the seam.
	natsPub := natsutil.NewPublisher(js, events.ServiceName) // source = "registry"
	emitter := events.NewPublisher(natsPub)                  // domain.ProjectionEmitter

	// The CQRS service: WRITE store (Postgres) + READ store (Redis) + the emitter,
	// plus the two pure ports — the production wall clock and the UUIDv4 id generator.
	// These last two are injected (not called inline) so tests stay deterministic;
	// production wires the real implementations here.
	svc := domain.NewRegistryService(
		writeStore,                // domain.WriteStore  (commands → source of truth)
		readStore,                 // domain.ReadStore   (queries → projection)
		emitter,                   // domain.ProjectionEmitter (CQRS sync seam → NATS)
		domain.NewRealClock(),     // domain.Clock       (time.Now)
		domain.NewUUIDGenerator(), // domain.IDGenerator (UUIDv4)
	)

	// --- CQRS PROJECTION CONSUMER (the read-side rebuild) ---
	// This is the OTHER half of CQRS, and the bug this wires fixes: commands above
	// write Postgres and emit fp.models.* events, but nothing was UPDATING the Redis
	// read model — so GET /models returned [] no matter how many models were
	// registered. The projection is an idempotent, durable NATS consumer that
	// subscribes to the registry's OWN events on the MODELS stream and upserts the
	// Redis read store (readStore satisfies events.ProjectionWriter — it exposes the
	// UpsertModel/UpsertVersion/unscoped-load methods the projection needs). Decodes
	// with protojson (the canonical dialect the publisher emits). We start it under
	// the serve context so it drains on SIGTERM, and Close it in shutdown.
	//
	//   write model emits events ─► projection consumes ─► upserts read model ─►
	//   queries read Redis.  Eventual consistency; the command RESPONSE still returns
	//   the authoritative write-side state, so a client never sees its own write miss.
	// writeStore is passed as the ModelReader read-back source so the projection
	// hydrates description/tags (which the thin fp.models.registered event does not
	// carry) from the Postgres truth — making the Redis read model FIELD-COMPLETE so
	// GET /models/{id} returns the real description. See events.ModelReader.
	projection := events.NewProjection(js, readStore, writeStore, events.ProjectionConfig{})
	if err := projection.Start(ctx); err != nil {
		_ = natsConn.Drain()
		_ = rdb.Close()
		pgPool.Close()
		logger.Error("failed to start CQRS projection consumer", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("CQRS projection consumer started (Redis read model now rebuilt from fp.models.* events)",
		slog.String("stream", events.StreamName),
	)

	// ================================================================
	// 6. BUILD gRPC SERVER + REGISTER THE WIRED HANDLER (real svc, NOT nil)
	// ================================================================
	// AUTHENTICATION (authN) vs AUTHORIZATION (authZ) — the division of labor:
	//
	//   - authN (THIS interceptor): "WHO are you?" Every RPC must carry a valid JWT.
	//     The grpcutil auth interceptor pulls the bearer token from the
	//     `authorization` metadata, hands it to the validator below, and on success
	//     injects the resolved *grpcutil.Claims (UserID, Team, Role, Scopes) into the
	//     request context. On failure it returns Unauthenticated and the handler is
	//     never reached. This is the LOCAL-VERIFY path of design D2 (pkg/auth):
	//     in-process HS256 signature check with the shared secret — zero network hop,
	//     and auth-service downtime does NOT block registry RPCs (the tradeoff is that
	//     a revoked JWT stays valid until it expires; short TTLs bound that window).
	//
	//   - authZ (the HANDLERS, per-RPC): "MAY you do THIS?" The registry's handlers
	//     read those injected claims to make per-RPC permission/tenancy decisions —
	//     owner_id/team are taken from claims.Team/claims.UserID (the mass-assignment
	//     guard in the proto header), and a write may be gated on role. The
	//     interceptor does NOT authorize; it only authenticates and populates claims
	//     so the handlers HAVE something authoritative to authorize against. Before
	//     this wiring, claims were never in the context, so any claims-based handler
	//     check would fail closed — every authenticated RPC was effectively unreachable.
	//
	// The validator is the shared pkg/auth JWT validator (NOT an auth-service RPC
	// client): registry verifies the signature itself with cfg.JWTSecret. We fail
	// startup if the secret is missing/weak — NewJWTValidator rejects a <32-byte key
	// (RFC 7518 §3.2), so a misconfigured secret can never silently accept forgeable
	// tokens. This is the exact pattern auth uses (services/auth/cmd/server/main.go),
	// generalized: build a TokenValidator, pass it to WithAuthValidator with the skip
	// list = this service's PUBLIC RPCs + the health/reflection infra RPCs.
	validator, err := fpauth.NewJWTValidator([]byte(cfg.JWTSecret))
	if err != nil {
		logger.Error("failed to build JWT validator", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// publicMethods — the registry's GENUINELY-public, unauthenticated RPCs.
	//
	// It is EMPTY, and that is the correct, default-deny answer: every RegistryService
	// RPC operates on platform resources (models/versions owned by a team) and is
	// either a mutating command (RegisterModel, UpdateModel, CreateVersion,
	// ConfirmVersionUpload, PromoteVersion, DeleteModel), a tenancy-scoped query
	// (GetModel, ListModels, SearchByTag, GetVersion, ListVersions — all scoped to the
	// caller's team FROM CLAIMS, never the request body), or a credential-minting
	// storage op (GetUploadURL/GetDownloadURL hand out presigned object-store URLs).
	// NONE of these has any business being callable without an identity — an
	// unauthenticated lister could enumerate another team's models, and an
	// unauthenticated upload-URL grant is a write credential for anyone. So unlike
	// auth (whose Login/ValidateToken/CheckPermission are inherently pre-auth or S2S),
	// the registry has NO public business RPC. We therefore skip ONLY the gRPC health
	// and reflection services (which the kubelet probe and grpcurl call with no
	// credential — gating them would make the pod fail its own readiness probe).
	//
	// SECURE-BY-DEFAULT: a NEW RPC added to the proto is automatically authenticated
	// unless someone consciously adds it here — exactly the posture we want.
	var publicMethods []string

	srv := grpcutil.NewServer(
		grpcutil.WithLogger(logger),
		grpcutil.WithAuthValidator(validator, append(publicMethods, grpcutil.HealthAndReflectionMethods()...)...),
		grpcutil.WithReflection(),
	)

	// The composition is complete: hand the fully-wired domain service to the handler.
	// The handler holds the RegistryService INTERFACE — it has no idea the impl is
	// Postgres-write/Redis-read/NATS-emit. This single line is the difference between
	// the scaffold (NewRegistryHandler(nil) → every RPC Unimplemented) and a working
	// service.
	registryv1.RegisterRegistryServiceServer(srv.GRPC, handler.NewRegistryHandler(svc))
	logger.Info("registry handler registered with wired service (JWT auth enforced)",
		// Empty by design: every business RPC requires a token. Logged explicitly so
		// the absence of public RPCs is visible/auditable, not a silent assumption.
		slog.Any("public_methods", publicMethods),
	)

	// ================================================================
	// 7. HEALTH SERVER (HTTP /healthz liveness + /readyz readiness over real deps)
	// ================================================================
	// WHY a separate HTTP port from gRPC: gRPC is HTTP/2, kubelet probes are
	// HTTP/1.1 — they can't share a net.Listener. Separate ports also let us flip
	// /readyz to 503 (drain) while gRPC finishes in-flight calls on shutdown.
	//
	// READINESS over the REAL dependencies: each check is what makes /readyz
	// meaningful. If Postgres/Redis/NATS goes away, /readyz → 503, K8s pulls the pod
	// from the Service endpoints (no traffic) WITHOUT restarting it (liveness stays
	// green) — the correct posture for a transient dependency outage (see pkg/health).
	healthHandler := health.New()
	healthHandler.AddCheck("postgres", func(ctx context.Context) error {
		// Borrow a conn and round-trip — proves the pool can actually reach Postgres.
		return pgPool.Ping(ctx)
	})
	healthHandler.AddCheck("redis", func(ctx context.Context) error {
		return rdb.Ping(ctx).Err()
	})
	healthHandler.AddCheck("nats", func(_ context.Context) error {
		// IsConnected reflects the live transport state (the client auto-reconnects in
		// the background; this reports whether it currently HAS a connection). Cheaper
		// and more accurate than a round-trip publish for a liveness-of-transport check.
		if !natsConn.IsConnected() {
			return fmt.Errorf("nats: not connected (status %v)", natsConn.Status())
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

	// shutdown runs cleanup in the CORRECT order, ONCE, after Serve returns. Defining
	// it as a closure (not a pile of defers) makes the ordering explicit and readable —
	// the order below is load-bearing, see the per-step rationale.
	shutdown := func() {
		// Use a fresh, bounded context — the signal ctx is already cancelled by now,
		// so anything that honors it would return ctx.Canceled immediately. K8s gives
		// terminationGracePeriodSeconds (30s default); this stays well within it.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		// (a) Stop accepting health probes. By now gRPC has already drained (Serve
		//     returned) and SetServing(false) flipped readiness, so the LB has long
		//     since stopped routing here; closing the HTTP server just releases the port.
		if err := healthServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("health server shutdown error", slog.String("error", err.Error()))
		}

		// (b) STOP the projection consume loops BEFORE draining NATS, so no handler is
		//     mid-upsert against Redis when we close it below. Close is idempotent and
		//     the consume loops also stop on ctx cancellation — this is belt-and-braces
		//     for a clean drain.
		projection.Close()

		// (c) DRAIN NATS (not Close): Drain flushes any buffered publishes and lets
		//     in-flight messages complete before tearing down the connection — so the
		//     last events a draining RPC emitted are not lost. Close would discard them.
		if err := natsConn.Drain(); err != nil {
			logger.Error("nats drain error", slog.String("error", err.Error()))
		}

		// (d) Close the datastore pools. Safe now because gRPC has drained AND the
		//     projection has stopped: no handler is still mid-query/mid-upsert holding a
		//     borrowed connection. Closing earlier could fail an in-flight RPC's query.
		//     We close the RAW handles we own (the adapters are thin wrappers over these).
		if err := rdb.Close(); err != nil {
			logger.Error("redis close error", slog.String("error", err.Error()))
		}
		pgPool.Close() // pgxpool.Close has no error to return

		// (e) FLUSH OpenTelemetry LAST so the spans/metrics produced during (a)-(d)
		//     (and during the gRPC drain) are exported, not dropped. This is exactly
		//     why otelShutdown was NOT deferred up top.
		if err := otelShutdown(shutdownCtx); err != nil {
			logger.Error("otel shutdown error", slog.String("error", err.Error()))
		}
	}

	// ================================================================
	// 8. START gRPC SERVER (mark SERVING, listen, block until signal)
	// ================================================================
	// SetServing(true) flips the gRPC health service to SERVING only NOW — after every
	// dependency connected and the handler is wired — so a gRPC readiness probe is
	// honest (green means actually-serviceable). Serve flips it back to NOT_SERVING on
	// SIGTERM before draining.
	srv.SetServing(true)

	grpcAddr := fmt.Sprintf(":%d", cfg.GRPCPort)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("failed to listen on gRPC port", slog.String("addr", grpcAddr), slog.String("error", err.Error()))
		shutdown() // release the deps we already opened before exiting
		os.Exit(1)
	}
	logger.Info("gRPC server listening", slog.String("addr", grpcAddr))

	// Serve blocks until ctx is cancelled (SIGINT/SIGTERM). On cancellation it marks
	// the server NOT_SERVING (probes fail → pod leaves the LB), drains in-flight RPCs
	// (bounded), then returns here so our ordered shutdown() runs.
	if serveErr := srv.Serve(ctx, lis); serveErr != nil {
		logger.Error("gRPC server error", slog.String("error", serveErr.Error()))
		shutdown()
		os.Exit(1)
	}

	// ================================================================
	// 9. GRACEFUL SHUTDOWN (ordered: gRPC already drained → health → NATS → pools → otel)
	// ================================================================
	shutdown()
	logger.Info("registry service stopped cleanly")
}
