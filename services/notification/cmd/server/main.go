// Package main is the entrypoint for the Notification service.
//
// ============================================================================
// WIRING — THE COMPOSITION ROOT
// ============================================================================
//
// In Clean Architecture, main.go is the "composition root" — the only place
// where every layer is imported together and wired into a running program. It
// knows about config, observability, the domain, the handler, the Postgres/Redis
// adapters, and the NATS event adapters; none of THOSE layers know about each
// other except through the interfaces (ports) the domain defines.
//
// STARTUP SEQUENCE (each step fails fast — a half-up service must never report ready):
//
//	┌──────────────────────────────────────────────────────────────────────────┐
//	│  1. Structured logger (slog JSON → stdout → Loki)                          │
//	│  2. Load config (FP_* env vars → NotificationConfig)                       │
//	│  3. Setup OpenTelemetry (traces → Tempo, metrics → Prometheus)             │
//	│  4. Connect datastores (Postgres pool, Redis client, NATS JetStream)       │
//	│     + ensure the EVENTS firehose stream exists, each Ping'd for reachability│
//	│  5. Build adapters → domain service → event publisher                      │
//	│  6. Register the REAL handler (not nil) on the gRPC server                 │
//	│  7. Start the NATS reactor (the choreography consumer on fp.>)             │
//	│  8. HTTP health server (/readyz checks Postgres + Redis + NATS)            │
//	│  9. Start gRPC server (SERVING only now); block until SIGTERM/SIGINT       │
//	│ 10. Graceful shutdown LIFO: gRPC drain → reactor stop → NATS drain →       │
//	│     Redis close → Postgres close → OTel flush → health close              │
//	└──────────────────────────────────────────────────────────────────────────┘
//
// ============================================================================
// THE TWO SURFACES OF THIS SERVICE (and why the wiring has two halves)
// ============================================================================
//
// Notification is unusual: its real work is ASYNC (the choreography reactor that
// consumes fp.> and reacts), and its gRPC surface is a tiny CONTROL PLANE (the
// inbox/preferences/delivery-log RPCs). So the composition root wires BOTH:
//
//	SYNC  : handler → domain.NotificationService → {Postgres, Redis, Notifier}
//	ASYNC : NATS fp.> → events.Reactor → domain.ReactToEvent → DecisionExecutor
//
// ============================================================================
// THE ONE PIECE THAT IS NOT YET IMPLEMENTED — THE SSRF-GUARDED DELIVERY ADAPTER
// ============================================================================
//
// The domain depends on a domain.Notifier port (ports.go) — the external-delivery
// adapter that performs the actual webhook/Slack/SMTP send WITH the mandated SSRF
// guard (https-only, hooks.slack.com allowlist, private/loopback/link-local IP
// denylist, pinned-IP connect), retries, and a per-URL circuit breaker. That
// adapter (the `delivery` package the ports.go doc describes) has NOT been written
// yet. Two further async-path collaborators are likewise unwritten: the
// events.DecisionExecutor (writes the inbox row + fires the Notifier + records the
// suppressed log) and the events.RecipientRouting (maps an opaque event → the
// recipient + rendered content).
//
// Rather than register a nil service (which would make the WHOLE gRPC surface
// return Unimplemented), the composition root injects SAFE-DEFAULT edge adapters
// for the three missing collaborators:
//
//   - denyAllNotifier  — a Notifier that rejects every external delivery
//     (fail-closed: a missing delivery adapter must NEVER silently "succeed", and
//     must never bypass the SSRF guard). The control-plane RPCs that do not deliver
//     externally (ListNotifications / GetNotification / MarkRead /
//     ListDeliveryAttempts / GetPreferences / UpdatePreferences) are FULLY live
//     against Postgres + Redis; only TestChannel on an external channel returns the
//     fail-closed outcome (which the settings UI shows inline as "✗ …").
//
//   - noRecipientRouter — the exact safe default resolve.go documents: a router
//     that drops every event (ok=false), so the reactor ACKs-and-skips until the
//     platform's real recipient-resolution policy is wired. The reactor still runs
//     (subscription, dedup, DLQ machinery all live); it simply has nothing to route.
//
//   - inertExecutor     — a DecisionExecutor that performs no side effect (it is
//     only ever reached if a recipient is resolved, which noRecipientRouter never
//     does, so it is unreachable today; present so the reactor type-checks and so
//     swapping in the real executor is a one-line wiring change).
//
// These three are the ONLY non-final pieces. They are clearly fail-closed, mirror
// patterns already sanctioned elsewhere in this codebase (resolve.go's documented
// no-recipient default; feature-store/inference-gateway's edge adapters), and let
// this service compile, boot, pass readiness, and serve its control plane today.
// Replacing them with the real `delivery` package + executor + router is a pure
// wiring change here — no layer below main.go changes.
// ============================================================================
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"
	goredis "github.com/redis/go-redis/v9"

	notificationv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/notification/v1"
	fpauth "github.com/abd-ulbasit/forgepoint/pkg/auth"
	"github.com/abd-ulbasit/forgepoint/pkg/config"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/health"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/observability"
	"github.com/abd-ulbasit/forgepoint/services/notification/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/notification/internal/events"
	"github.com/abd-ulbasit/forgepoint/services/notification/internal/handler"
	postgresrepo "github.com/abd-ulbasit/forgepoint/services/notification/internal/repository/postgres"
	redisrepo "github.com/abd-ulbasit/forgepoint/services/notification/internal/repository/redis"
)

// NotificationConfig extends BaseConfig with notification-specific configuration.
//
// WHY embed BaseConfig: every Forgepoint service needs Port, GRPCPort, LogLevel,
// OTelEndpoint, NATSUrl, DatabaseURL. config.Load[NotificationConfig]("FP") reads
// FP_PORT, FP_GRPC_PORT, FP_NATS_URL, etc. via reflection (see pkg/config).
type NotificationConfig struct {
	config.BaseConfig

	// JWTSecret is the HMAC-SHA256 key the gRPC auth interceptor uses to VERIFY
	// (never to mint) caller tokens. Notification does not issue JWTs — the Auth
	// service does — but every authenticated RPC here must validate the bearer
	// token locally (design D2: in-process signature verify, no per-request hop to
	// auth), so this verifier MUST share the SAME secret the auth service signs
	// with. fpauth.NewJWTValidator enforces a >=32-byte floor (RFC 7518 §3.2), so a
	// weak/short secret fails startup rather than silently accepting forgeable tokens.
	//
	// required:"true" → fail-fast: a notification pod with no JWT secret cannot
	// authenticate anyone, so it must refuse to start rather than boot into a state
	// where every authenticated RPC is permanently Unauthenticated. K8s injects this
	// from a Secret mounted as FP_JWT_SECRET (never a ConfigMap — secrets are
	// access-controlled; see deploy/helm/fp-notification/templates/secret.yaml).
	JWTSecret string `env:"JWT_SECRET" required:"true"`

	// RedisURL backs the IdempotencyStore (sync dedup: prefs/test idempotency keys)
	// AND the reactor's domain-level per-(event,recipient) dedup store. A redis://
	// URL (host/port/db/credentials), parsed by go-redis ParseURL.
	// K8s: a plain ConfigMap value pointing at the in-cluster Redis.
	RedisURL string `env:"REDIS_URL" default:"redis://localhost:6379"`

	// SMTPAddr is the SMTP server for the EMAIL channel (host:port). Optional —
	// consumed by the (not-yet-written) delivery adapter; unset is valid today.
	SMTPAddr string `env:"SMTP_ADDR"`

	// OTLPInsecure controls whether the OTLP exporter dials the collector over
	// plaintext (local/docker-compose, no TLS) vs TLS (in-cluster behind the mesh).
	// Driven by config rather than hard-coded so the same binary works in both.
	OTLPInsecure bool `env:"OTLP_INSECURE" default:"true"`

	// Environment tags telemetry (resource attribute) so traces/metrics are
	// filterable by deployment. Mirrors FP_ENV (which natsutil's MemoryProcessedStore
	// guard also reads) so "production" is consistent across the process.
	Environment string `env:"ENV" default:"local"`
}

func main() {
	// ================================================================
	// 1. STRUCTURED LOGGER
	// ================================================================
	// slog (stdlib) with JSON output: container runtimes capture stdout and ship it
	// to Loki, and JSON is parse-friendly for the aggregator. Zero external deps.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// ================================================================
	// 2. LOAD CONFIG
	// ================================================================
	// config.Load reads FP_* env vars into NotificationConfig via reflection and
	// fails fast on a missing required field. In K8s, config comes from
	// ConfigMaps/Secrets mapped to env vars in the Deployment spec.
	cfg, err := config.Load[NotificationConfig]("FP")
	if err != nil {
		logger.Error("failed to load config", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("config loaded",
		slog.Int("grpc_port", cfg.GRPCPort),
		slog.Int("health_port", cfg.Port),
		slog.String("log_level", cfg.LogLevel),
		slog.String("nats_url", cfg.NATSUrl),
		slog.String("environment", cfg.Environment),
	)

	// SIGTERM/SIGINT-aware root context. Every blocking startup step and the gRPC
	// Serve loop honor it, so a signal during startup aborts cleanly and the SAME
	// ctx drives the graceful shutdown below. We include SIGTERM explicitly (K8s
	// sends SIGTERM, not SIGINT, to a terminating pod).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ================================================================
	// 3. OPENTELEMETRY
	// ================================================================
	// Setup initializes the trace provider (→ Tempo) and meter provider
	// (→ Prometheus via the OTel Collector). All spans/metrics from this process
	// flow through these providers — including the NATS consumer spans that complete
	// the serve→…→notify trace across the async hop (natsutil re-attaches the
	// producer's trace context per message).
	otelShutdown, err := observability.Setup(ctx, observability.Config{
		ServiceName:    "notification",
		ServiceVersion: "dev",
		Environment:    cfg.Environment,
		OTLPEndpoint:   cfg.OTelEndpoint,
		OTLPInsecure:   cfg.OTLPInsecure,
	})
	if err != nil {
		logger.Error("failed to setup observability", slog.String("error", err.Error()))
		os.Exit(1)
	}
	// Flush pending spans/metrics on shutdown. OTel batches internally; without this
	// the last few seconds of telemetry are lost. Registered FIRST so it fires LAST
	// (LIFO) — after every store-closing defer, so a teardown error still gets traced.
	// Uses a fresh Background ctx because the signal ctx is already cancelled here.
	defer func() {
		if shutdownErr := otelShutdown(context.Background()); shutdownErr != nil {
			logger.Error("otel shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// ================================================================
	// 4. CONNECT DATASTORES (Postgres, Redis, NATS) — fail fast on each
	// ================================================================
	//
	// 4a. POSTGRES (pgxpool) — backs the inbox read model, the delivery log, and the
	// per-user preferences. pgxpool.New only PARSES the DSN (it does not dial), so we
	// Ping to verify the database is actually reachable before declaring readiness.
	// The pool is created HERE and closed HERE (we own its lifecycle) — postgres.New
	// would create+ping+own one, but we manage the pool ourselves so the readiness
	// check and the close are visible in the composition root.
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("failed to create postgres pool", slog.String("error", err.Error()))
		os.Exit(1)
	}
	// Registered now so it fires near-LAST among the store closes (LIFO) — after the
	// gRPC drain finished any in-flight read, never cutting one off mid-query.
	defer func() {
		logger.Info("closing postgres pool")
		pool.Close()
	}()
	pingCtx, cancelPing := context.WithTimeout(ctx, 5*time.Second)
	if pingErr := pool.Ping(pingCtx); pingErr != nil {
		cancelPing()
		logger.Error("postgres not reachable", slog.String("error", pingErr.Error()))
		os.Exit(1)
	}
	cancelPing()
	logger.Info("postgres connected")

	// 4b. REDIS (go-redis) — backs the sync IdempotencyStore (prefs/test idempotency
	// keys) AND the reactor's domain per-(event,recipient) dedup store. ParseURL turns
	// "redis://host:port/db" into Options (auth, db, TLS), matching the platform's
	// connection-string format. NewClient is lazy, so we Ping to confirm reachability.
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
	// lifecycle: Drain/Close, IsConnected) AND a jetstream.JetStream context (for
	// publishing + consuming). natsutil.Connect sets MaxReconnects(-1) so the client
	// survives transient blips (K8s restarts the pod only if readiness stays down).
	nc, js, err := natsutil.Connect(cfg.NATSUrl)
	if err != nil {
		logger.Error("failed to connect to nats", slog.String("error", err.Error()))
		os.Exit(1)
	}
	// Drain (not bare Close): flush in-flight publishes + unsubscribe cleanly, THEN
	// close. We publish delivery-health best-effort, so a drain error is logged.
	defer func() {
		logger.Info("draining nats connection")
		if drainErr := nc.Drain(); drainErr != nil {
			logger.Error("nats drain error", slog.String("error", drainErr.Error()))
		}
	}()

	// NOTIFICATION CONSUMES from the EXISTING per-domain streams it does NOT own. It
	// binds ONE durable consumer per subject (events.ConsumedSubjects) to the stream
	// that owns each subject — MODELS (fp.models.>), PIPELINES (fp.pipelines.>),
	// INFERENCE (fp.inference.>), BILLING (fp.billing.>), EXPERIMENTS (fp.experiments.>)
	// — each provisioned by its OWN producing service at boot. The reactor resolves the
	// owning stream for each subject via js.StreamNameBySubject (events.Reactor.Start).
	// It does NOT create those consumed streams.
	//
	// WHY NOT a single EVENTS stream over fp.>: a stream's subjects must not OVERLAP
	// another stream's, and fp.> overlaps every per-domain stream → JetStream err 10065
	// ("subjects overlap with an existing stream") → CrashLoop. That is the exact bug
	// this deploy surfaced. We removed the fp.> stream creation entirely.
	//
	// ENSURE THE NOTIFICATIONS DOMAIN STREAM EXISTS (producer owns its stream).
	// Notification is the OWNER and sole producer of the fp.notifications.* delivery-
	// health feed (fp.notifications.delivered / .failed). JetStream REJECTS a publish
	// whose subject no stream captures (10073); without this stream every delivery-health
	// event was dropped AND experiment-tracker's wide lineage sink (which binds a durable
	// to the NOTIFICATIONS stream) had no stream to bind. We ensure it here, before the
	// publisher is wired, exactly as registry/experiment-tracker/billing do for their
	// trees. CreateOrUpdateStream is idempotent + convergent; we FAIL FAST on error so an
	// unprovisionable stream CrashLoops with the cause in logs rather than silently
	// dropping events. fp.notifications.> overlaps no other stream.
	domainStreamCtx, cancelDomainStream := context.WithTimeout(ctx, 10*time.Second)
	if streamErr := events.EnsureStream(domainStreamCtx, js); streamErr != nil {
		cancelDomainStream()
		logger.Error("failed to ensure NOTIFICATIONS stream", slog.String("error", streamErr.Error()))
		os.Exit(1)
	}
	cancelDomainStream()
	logger.Info("NOTIFICATIONS domain stream ensured",
		slog.String("stream", events.StreamName),
		slog.String("subjects", events.StreamSubjects),
	)

	// ENSURE THE DEDICATED DLQ STREAM EXISTS. A JetStream subject is bound to exactly
	// ONE stream, so the DLQ subject (events.SubjectDLQ = "fp_dlq.notification", a token
	// OUTSIDE every fp.<domain>.> tree, so it overlaps nothing) needs its own stream to
	// land in. The reactor NEVER consumes this stream — it is a SINK that ops drain/replay
	// from out-of-band (mirrors Kafka's separate dead-letter topic / SQS's dead-letter
	// queue). Provisioning it here keeps the local stack self-contained; in production
	// the platform bootstrap owns it, and CreateOrUpdate reconciles either way.
	dlqStreamCtx, cancelDLQStream := context.WithTimeout(ctx, 10*time.Second)
	if _, streamErr := js.CreateOrUpdateStream(dlqStreamCtx, jetstream.StreamConfig{
		Name:     events.StreamDLQ,
		Subjects: []string{events.SubjectDLQ},
	}); streamErr != nil {
		cancelDLQStream()
		logger.Error("failed to ensure DLQ stream", slog.String("error", streamErr.Error()))
		os.Exit(1)
	}
	cancelDLQStream()

	logger.Info("nats connected; DLQ stream ensured (no domain stream created — binds to existing per-domain streams)",
		slog.String("dlq_stream", events.StreamDLQ),
		slog.String("dlq_subject", events.SubjectDLQ),
	)

	// ================================================================
	// 5. BUILD ADAPTERS → DOMAIN SERVICE → EVENT PUBLISHER
	// ================================================================
	//
	// Dependency inversion made concrete: the domain declares the ports
	// (NotificationRepository, PreferenceRepository, IdempotencyStore, Notifier,
	// Clock, IDGenerator); here we construct the infrastructure adapters that satisfy
	// them and INJECT them. The domain never imported pgx, go-redis, or nats.
	pgStore := postgresrepo.NewWithPool(pool) // wraps the pool WE own (NewWithPool, not New)
	notifRepo := pgStore.Notifications()      // domain.NotificationRepository (inbox + delivery log)
	prefRepo := pgStore.Preferences()         // domain.PreferenceRepository (per-user routing rules)
	idemStore := redisrepo.NewIdempotencyStore(rdb)

	// The external-delivery port. The real SSRF-guarded delivery adapter (the
	// `delivery` package) is not written yet, so we inject a fail-closed default that
	// rejects every external send (see denyAllNotifier). The control-plane RPCs that
	// never deliver externally are unaffected; only TestChannel on an external channel
	// returns the fail-closed outcome, which the settings UI shows inline.
	notifier := denyAllNotifier{}

	// The domain brain. NewNotificationService returns the NotificationService
	// interface (programming-to-the-interface); the handler and the reactor both
	// receive it. SystemClock/UUIDGenerator are the domain's own production
	// impure-edge adapters (defaults.go) — wall-clock time and crypto-random ids
	// enter the system only here.
	svc := domain.NewNotificationService(
		notifRepo,
		prefRepo,
		idemStore,
		notifier,
		domain.SystemClock{},
		domain.UUIDGenerator{},
	)

	// The delivery-health publisher (events.DeliveryHealthPublisher). NewPublisher
	// wraps a natsutil.Publisher bound to this service's source name (events.Source),
	// inheriting the envelope/dedup/trace machinery; the adapter only builds the
	// canonical events.v1 payload and protojson-encodes it.
	publisher := events.NewPublisher(natsutil.NewPublisher(js, events.Source))

	// ================================================================
	// 6. BUILD gRPC SERVER + REGISTER THE REAL HANDLER
	// ================================================================
	// grpcutil.NewServer applies the standard interceptor chain (recovery → logging →
	// auth → tracing). We now wire the AUTHENTICATION interceptor via
	// WithAuthValidator — until this was added, the auth interceptor never ran, so
	// caller claims were NEVER injected into the request context and every handler
	// that scopes to grpcutil.ClaimsFromContext (the inbox/prefs RPCs, which read the
	// caller's UserID — the anti-IDOR rule) had no identity to scope to.
	//
	// authN vs authZ — THE DIVISION OF LABOR:
	//   - AUTHENTICATION (this interceptor): "who are you?" It validates the bearer
	//     JWT's signature/expiry LOCALLY with the shared secret (design D2 — no
	//     per-request hop to the auth service) and, on success, puts the typed
	//     *grpcutil.Claims (UserID, Email, Team, Role, Scopes) into the context. On
	//     failure it rejects with Unauthenticated before the handler ever runs. It
	//     makes NO permission decision.
	//   - AUTHORIZATION ("may you do this?") stays in the HANDLERS: each RPC reads the
	//     claims the interceptor injected and enforces its own per-RPC rule (e.g.
	//     scope every inbox/prefs query to claims.UserID so one user can never read or
	//     mutate another's notifications/preferences). The interceptor authenticates;
	//     it does not authorize. That handler-side authZ is unchanged by this wiring.
	//
	// fpauth.NewJWTValidator returns the shared grpcutil.TokenValidator (the SAME
	// local-verify implementation every non-auth service uses) and ERRORS on a weak
	// (<32-byte) secret — we fail startup on that rather than boot a pod that would
	// accept forgeable tokens.
	//
	// PUBLIC METHODS = EMPTY (default-deny). EVERY NotificationService RPC operates on
	// the AUTHENTICATED CALLER'S OWN platform resources — the inbox (List/Get/MarkRead),
	// the delivery-log audit (ListDeliveryAttempts), and preferences/self-test
	// (Get/UpdatePreferences/TestChannel). The proto deliberately omits a user_id on
	// every request precisely BECAUSE identity comes from the token, so NONE of these
	// can function without authenticated claims and NONE is intended for unauthenticated
	// access. There is no Login-style token-minting RPC here and no service-to-service
	// RPC that takes its credential in the request body (contrast auth's ValidateToken/
	// CheckPermission). So the only methods we skip are the gRPC health + reflection
	// infrastructure RPCs (kubelet probes / grpcurl carry no token) via
	// grpcutil.HealthAndReflectionMethods(). Adding a new RPC to the proto is therefore
	// authenticated-by-default unless someone consciously adds it to publicMethods.
	//
	// WithReflection lets grpcurl/grpcui introspect the service in development.
	validator, err := fpauth.NewJWTValidator([]byte(cfg.JWTSecret))
	if err != nil {
		logger.Error("failed to build JWT validator", slog.String("error", err.Error()))
		os.Exit(1)
	}
	publicMethods := []string{} // no genuinely-public business RPCs — all require auth
	srv := grpcutil.NewServer(
		grpcutil.WithLogger(logger),
		grpcutil.WithAuthValidator(validator, append(publicMethods, grpcutil.HealthAndReflectionMethods()...)...),
		grpcutil.WithReflection(),
	)

	// Register the FULLY-WIRED handler (real service, NOT nil). Every control-plane
	// RPC now hits the domain service backed by Postgres + Redis.
	notificationv1.RegisterNotificationServiceServer(srv.GRPC, handler.NewNotificationHandler(svc))
	logger.Info("notification handler registered (wired: postgres inbox/prefs + redis idempotency; all RPCs require a valid JWT — only health/reflection are public)",
		slog.Int("public_methods", len(publicMethods)),
	)

	// ================================================================
	// 7. START THE NATS REACTOR (the choreography consumer on fp.>)
	// ================================================================
	//
	// The reactor is the imperative shell around the pure domain.ReactToEvent brain:
	// it consumes the fp.> firehose, resolves a recipient (Router), loads prefs
	// (Prefs), runs ReactToEvent, executes the decision (Executor), and publishes
	// delivery-health events (Health). Its ports:
	//
	//   - Service  : the SAME domain service the handler uses (calls ReactToEvent).
	//   - Router   : recipient-resolution policy. The real platform policy is not
	//                written yet, so we inject the safe no-recipient default that
	//                resolve.go documents — it drops every event, so the reactor
	//                ACKs-and-skips. The subscription/dedup/DLQ machinery is fully
	//                live; it simply has nothing to route until the policy is wired.
	//   - Prefs    : LoadPreferences with default fall-back. domain.GetPreferences
	//                already implements EXACTLY that contract (never errors on a
	//                never-configured user → returns defaults), so we adapt the
	//                service to the one-method port (prefsLoader) rather than add a
	//                second prefs path.
	//   - Executor : the decision's side effects (inbox write + Notifier + suppressed
	//                log). The real executor is not written yet; inertExecutor is a
	//                no-op placeholder, unreachable today because noRecipientRouter
	//                never yields a recipient (so handle() returns before Execute).
	//   - Health   : the delivery-health publisher (real — wired above).
	//
	// DOMAIN DEDUP STORE (layer (b)): a SHARED, durable Redis-backed
	// natsutil.ProcessedStore keyed on the per-(event,recipient) business key. A
	// memory store would not dedup across replicas in the consumer group, so we use
	// Redis with a TTL (bounded keyspace, comfortably outlasting the redelivery
	// window). natsutil ships only MemoryProcessedStore (panics in production), hence
	// this small edge adapter — same pattern as the inference-gateway.
	processedStore := newRedisProcessedStore(rdb)

	// The reactor builds ONE durable consumer per subject in events.ConsumedSubjects,
	// each bound to the EXISTING per-domain stream that owns the subject (resolved at
	// Start via js.StreamNameBySubject). It takes the JetStream context directly and
	// owns the per-subject natsutil.Subscribers internally (the per-subject durable
	// requirement lives in the events package, not here).
	reactor := events.NewReactor(
		js,
		events.ReactorDeps{
			Service:  svc,
			Router:   noRecipientRouter{},
			Prefs:    prefsLoader{svc: svc},
			Executor: inertExecutor{},
			Health:   publisher,
		},
		processedStore,
		events.ReactorConfig{},
		logger,
	)
	if startErr := reactor.Start(ctx); startErr != nil {
		logger.Error("failed to start notification reactor", slog.String("error", startErr.Error()))
		os.Exit(1)
	}
	// Stop the consume loops on shutdown. Registered AFTER the NATS-drain defer so it
	// runs BEFORE it (LIFO): stop consuming new events, THEN drain the connection.
	defer func() {
		logger.Info("stopping notification reactor")
		reactor.Close()
	}()
	logger.Info("notification reactor started (per-subject consumers bound to existing per-domain streams)",
		slog.Int("subjects", len(events.ConsumedSubjects)),
		slog.String("dlq_subject", events.SubjectDLQ),
	)

	// ================================================================
	// 8. HEALTH SERVER (HTTP) — readiness checks the REAL dependencies
	// ================================================================
	// gRPC is HTTP/2; kubelet probes are HTTP/1.1 — they can't share a port, so health
	// runs on its own HTTP port. Liveness (/healthz) NEVER checks dependencies (a DB
	// blip must not restart-storm the fleet). Readiness (/readyz) checks Postgres +
	// Redis + NATS so a pod with a dead dependency is pulled from the Service endpoints
	// until it recovers.
	healthHandler := health.New()
	healthHandler.AddCheck("postgres", func(ctx context.Context) error {
		return pool.Ping(ctx)
	})
	healthHandler.AddCheck("redis", func(ctx context.Context) error {
		return rdb.Ping(ctx).Err()
	})
	healthHandler.AddCheck("nats", func(_ context.Context) error {
		// IsConnected is the cheap, non-blocking signal. During an automatic reconnect
		// it is briefly false — readiness correctly reflects "can't publish/consume
		// right now" and recovers when the client reconnects.
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
		if serveErr := healthServer.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			logger.Error("health server error", slog.String("error", serveErr.Error()))
		}
	}()
	defer func() {
		if shutdownErr := healthServer.Shutdown(context.Background()); shutdownErr != nil {
			logger.Error("health server shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// ================================================================
	// 9. START gRPC SERVER (mark SERVING only now that all deps are up)
	// ================================================================
	// We reach SetServing(true) ONLY after Postgres/Redis/NATS connected, the stream
	// exists, and the reactor is consuming — so the gRPC readiness signal is honest:
	// green means the service can actually serve. grpcutil.Server.Serve flips it to
	// NOT_SERVING on SIGTERM before draining (the graceful-shutdown dance).
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
	//   2. GracefulStop (bounded drain) → in-flight control-plane RPCs finish (still
	//      touching Postgres/Redis/NATS, which are STILL OPEN — the drain runs before
	//      any store-closing defer fires)
	//   3. hard Stop if drain times out
	//   4. returns here; the deferred teardowns run LIFO:
	//        health close → reactor stop → NATS drain → Redis close → Postgres close
	//        → OTel flush.
	//      That order is correct: stop consuming/serving FIRST, then close the
	//      datastores those handlers used, then flush telemetry LAST.
	if serveErr := srv.Serve(ctx, lis); serveErr != nil {
		logger.Error("gRPC server error", slog.String("error", serveErr.Error()))
		os.Exit(1)
	}

	logger.Info("notification service stopped cleanly")
}

// ============================================================================
// SAFE-DEFAULT EDGE ADAPTERS (the three not-yet-implemented collaborators)
// ============================================================================
//
// These live in the composition root (not in any layer below) because they are
// WIRING-EDGE stand-ins for adapters that belong in their own packages once
// written (the SSRF-guarded `delivery` Notifier, the platform RecipientRouting
// policy, the DecisionExecutor). Keeping them here keeps every inner layer pure and
// makes the eventual swap a one-line change at each injection site above.

// denyAllNotifier is the fail-closed default for the domain.Notifier port until the
// real SSRF-guarded delivery adapter is written.
//
// WHY FAIL CLOSED (reject), not fail open (pretend success): a Notifier is the SSRF
// boundary — the one place an attacker-influenced URL would reach the network. A
// stub that returned StatusDelivered would (a) lie to the delivery log / health
// feed and (b) risk masking the absence of the SSRF guard. Rejecting every external
// delivery is the only safe placeholder: the inbox (IN_APP) still works, and an
// external TestChannel surfaces a clear FAILED outcome instead of a false positive.
type denyAllNotifier struct{}

// Deliver rejects every external delivery. The domain's TestChannel maps this error
// to a FAILED TestChannelOutput (shown inline in the settings UI), and the reactor's
// executor (once written) would record a FAILED delivery_log row — neither path
// bypasses the SSRF guard, because no socket is ever opened.
func (denyAllNotifier) Deliver(_ context.Context, _ domain.DeliveryTarget) (domain.DeliveryResult, error) {
	return domain.DeliveryResult{
		Status:       domain.StatusFailed,
		Attempts:     0,
		ErrorMessage: "external delivery adapter not configured",
	}, errors.New("notification: delivery adapter not wired")
}

var _ domain.Notifier = denyAllNotifier{}

// prefsLoader adapts the domain.NotificationService to the reactor's narrow
// events.PreferenceLoader port. WHY adapt rather than wire domain.PreferenceRepository
// directly: the reactor needs ONLY "load for this user, defaulting on miss", and
// domain.GetPreferences ALREADY encodes exactly that (it returns sane defaults for a
// never-configured user instead of erroring). Reusing it means the reactor and the
// GetPreferences RPC share one prefs-resolution path — no second, drifting copy.
type prefsLoader struct {
	svc domain.NotificationService
}

// LoadPreferences delegates to the service's default-on-miss GetPreferences.
func (p prefsLoader) LoadPreferences(ctx context.Context, userID string) (domain.NotificationPreferences, error) {
	return p.svc.GetPreferences(ctx, userID)
}

var _ events.PreferenceLoader = prefsLoader{}

// noRecipientRouter is the safe default events.RecipientRouting documented in
// resolve.go: it resolves NO recipient for any event, so the reactor ACKs-and-skips
// every message. This is the correct fail-safe until the platform's real
// recipient-resolution policy (event-type → team → users, or a producer-stamped
// recipient hint) is wired: a service that doesn't yet know WHO to notify must
// deliver to NO ONE rather than guess.
type noRecipientRouter struct{}

// Route always returns ok=false (no recipient). resolveEvent then yields ok=false and
// the reactor logs "no recipient; skipping" and ACKs — a normal, non-error path.
func (noRecipientRouter) Route(_ natsutil.EventEnvelope) (events.RoutedRecipient, bool) {
	return events.RoutedRecipient{}, false
}

var _ events.RecipientRouting = noRecipientRouter{}

// inertExecutor is the placeholder events.DecisionExecutor until the real one (inbox
// upsert + Notifier dispatch + suppressed-log) is written. It is UNREACHABLE today:
// the reactor only calls Execute AFTER a recipient is resolved, and noRecipientRouter
// never resolves one — so handle() returns before reaching Execute. It exists so the
// reactor type-checks and so swapping in the real executor is a one-line change. If
// it WERE reached, it performs no side effect and reports no deliveries (fail-closed:
// it never claims a delivery that did not happen).
type inertExecutor struct{}

// Execute performs no side effect and reports an empty result.
func (inertExecutor) Execute(_ context.Context, _ domain.RoutingDecision, _ domain.InboundEvent) (events.ExecutionResult, error) {
	return events.ExecutionResult{}, nil
}

var _ events.DecisionExecutor = inertExecutor{}

// ============================================================================
// redisProcessedStore — a shared, durable natsutil.ProcessedStore over Redis.
// ============================================================================
//
// WHY here and not natsutil: natsutil ships only MemoryProcessedStore (tests/dev; it
// panics under FP_ENV=production because it is per-replica, unbounded, and lost on
// restart). The reactor runs in a consumer group across replicas, so a redelivery can
// land on a DIFFERENT replica than first processed it — only a SHARED store dedupes
// that. A Redis key with TTL is the canonical short-window dedup: "have I seen this
// key?" with self-eviction so the keyspace stays bounded. This backs BOTH the
// transport-layer dedup (natsutil keys it on EventEnvelope.id) and the reactor's
// domain-layer per-(event,recipient) dedup (it keys it on domain.EventDedupKey).
//
// The 24h TTL bounds memory while comfortably outlasting JetStream's redelivery
// window (a duplicate arrives within seconds-to-minutes of the original). Mirrors the
// inference-gateway's identical edge adapter.
type redisProcessedStore struct {
	rdb    *goredis.Client
	prefix string
	ttl    time.Duration
}

var _ natsutil.ProcessedStore = (*redisProcessedStore)(nil)

func newRedisProcessedStore(rdb *goredis.Client) *redisProcessedStore {
	return &redisProcessedStore{
		rdb:    rdb,
		prefix: "fp:notif:processed:",
		ttl:    24 * time.Hour,
	}
}

// IsProcessed reports whether the key is already recorded. A Redis error is surfaced
// (not swallowed): the reactor NAKs on an idempotency-check error so we retry rather
// than risk double-processing a side-effecting event — at-least-once is the safe
// failure mode for a dedup check.
func (s *redisProcessedStore) IsProcessed(ctx context.Context, eventID string) (bool, error) {
	n, err := s.rdb.Exists(ctx, s.prefix+eventID).Result()
	if err != nil {
		return false, fmt.Errorf("redisProcessedStore: exists %s: %w", eventID, err)
	}
	return n > 0, nil
}

// MarkProcessed records the key with a TTL. Called AFTER a successful handle and
// BEFORE the ACK, so a redelivery is recognized. For TRUE exactly-once the mark must
// be in the same transaction as the side effect (see natsutil/idempotency.go); the
// durable inbox upsert on (event_id, recipient) is that transactional backstop, so
// this shared store is the fast cross-replica dedup layer on top of it.
func (s *redisProcessedStore) MarkProcessed(ctx context.Context, eventID string) error {
	if err := s.rdb.Set(ctx, s.prefix+eventID, "1", s.ttl).Err(); err != nil {
		return fmt.Errorf("redisProcessedStore: set %s: %w", eventID, err)
	}
	return nil
}
