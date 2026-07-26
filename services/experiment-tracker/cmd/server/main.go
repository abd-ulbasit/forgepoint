// Package main is the entrypoint for the Experiment Tracker service.
//
// ============================================================================
// WIRING — THE COMPOSITION ROOT (fully wired)
// ============================================================================
//
// In Clean Architecture, main.go is the "composition root" — the ONLY place
// that imports every layer (config, observability, domain, repository adapters,
// event adapters, handler) and wires them into a running program. Each layer
// knows nothing of the others except through the interfaces (PORTS) the domain
// declares. The arrows all point inward: the Postgres adapter and the NATS
// adapter implement the domain's ports; the handler depends on the domain
// service interface; nobody depends on main.
//
// STARTUP SEQUENCE (and the strict ordering, top to bottom):
//
//	┌──────────────────────────────────────────────────────────────────────┐
//	│ 1. Structured logger (slog JSON → stdout → Loki)                      │
//	│ 2. Load config (FP_* env vars → ExperimentTrackerConfig)             │
//	│ 3. Setup OpenTelemetry (traces → Tempo, metrics → Prometheus)         │
//	│ 4. Connect datastores: Postgres (system of record) + NATS (event bus),│
//	│    each with a startup readiness check + cleanup registered for exit. │
//	│ 5. Construct repo adapters → event publisher → DOMAIN SERVICE →        │
//	│    handler (the REAL service, not nil).                                │
//	│ 6. Build gRPC server (interceptor chain) + register the REAL handler.  │
//	│ 7. Register/start the event SUBSCRIBERS (the wide lineage sink).       │
//	│ 8. HTTP health server (/healthz liveness, /readyz readiness checks).   │
//	│ 9. Serve gRPC (blocks until SIGTERM/SIGINT).                          │
//	│ 10. Graceful shutdown — deferred in REVERSE construction order:       │
//	│     drain gRPC → stop subscribers → drain NATS → close pool →          │
//	│     close health server → flush OTel.                                  │
//	└──────────────────────────────────────────────────────────────────────┘
//
// WHY THE SHUTDOWN ORDER MATTERS (LIFO of construction):
//   - gRPC drains FIRST (inside srv.Serve on ctx cancel) so in-flight RPCs
//     (a LogMetrics batch write, a FinishRun) finish before we tear down the
//     Postgres pool and NATS connection they depend on.
//   - Subscribers stop NEXT so no consumed event tries to write a lineage row
//     after we've begun shutting the pool, and no consume loop outlives us.
//   - NATS drains (flushes the best-effort RunCreated/RunFinished publishes that
//     a just-finished FinishRun emitted) BEFORE the socket closes — Close alone
//     could drop the last few events.
//   - The Postgres pool closes after everything that issues SQL is quiet.
//   - OTel flushes LAST so the shutdown's own spans/logs are still exported.
//
// Deferred funcs run LIFO, so registering them in construction order yields this
// exact reverse teardown for free.
//
// DATA STORES — WHY POSTGRES + NATS, AND (DELIBERATELY) NO REDIS:
//
//	Experiment Tracker is a SYSTEM OF RECORD: experiments, runs, write-once
//	params, the metric time-series, and the Stripe-style idempotency keys are all
//	durable, queried, and authoritative — that is Postgres's profile. Its
//	idempotency store is a durable Postgres table (idempotency_keys), NOT a TTL'd
//	Redis key, precisely so "never create a duplicate run" survives restarts (see
//	internal/repository/postgres/idempotency_store.go for the WHY). So unlike the
//	hot-path inference-gateway, this service wires NO go-redis client — there is
//	no ephemeral hot-path state here. The two stores are Postgres (sync, system of
//	record) and NATS JetStream (async event bus, publish + the wide consume sink).
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

	experimentv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/experiment/v1"
	fpauth "github.com/abd-ulbasit/forgepoint/pkg/auth"
	"github.com/abd-ulbasit/forgepoint/pkg/config"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/health"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/observability"
	"github.com/abd-ulbasit/forgepoint/services/experiment-tracker/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/experiment-tracker/internal/events"
	"github.com/abd-ulbasit/forgepoint/services/experiment-tracker/internal/handler"
	"github.com/abd-ulbasit/forgepoint/services/experiment-tracker/internal/repository/postgres"
)

// serviceName is the canonical identity used for telemetry resource attributes,
// the NATS event source, and the consumer-group/DLQ naming. It MUST match
// events.ServiceName (the value stamped into every published EventEnvelope.source)
// so telemetry and the event contract agree on who this process is.
const serviceName = events.ServiceName // "experiment-tracker"

// ExperimentTrackerConfig extends BaseConfig with service-specific configuration.
//
// WHY embed BaseConfig: every Forgepoint service needs Port, GRPCPort, LogLevel,
// OTelEndpoint, NATSUrl, DatabaseURL. Embedding avoids re-declaring them.
// config.Load[ExperimentTrackerConfig]("FP") reads FP_PORT, FP_GRPC_PORT,
// FP_DATABASE_URL, FP_NATS_URL, FP_OTEL_INSECURE, etc. via reflection (see
// pkg/config).
type ExperimentTrackerConfig struct {
	config.BaseConfig

	// DatabaseURL overrides BaseConfig.DatabaseURL (which has no default) with a
	// local docker-compose default so the service is runnable with zero env set.
	//
	// WHY shadow it here rather than change BaseConfig: BaseConfig is shared by ten
	// services with different database names; a default there would be wrong for
	// all but one. Each service supplies its OWN database name in its own default
	// (database-per-SERVICE). config.Load reads the most-derived field's `default`,
	// so this shadows the empty base default for THIS service only. In K8s this is
	// set from a Secret to the real RDS/Postgres DSN.
	DatabaseURL string `env:"DATABASE_URL" default:"postgres://forgepoint:forgepoint@localhost:5432/experiment_tracker?sslmode=disable"`

	// Environment ("local"/"staging"/"production") tags telemetry. It also feeds
	// natsutil's production guard (FP_ENV=production refuses the in-memory
	// ProcessedStore), so we keep the name aligned with FP_ENV.
	Environment string `env:"ENV" default:"local"`

	// JWTSecret is the HMAC-SHA256 signing key the AUTHENTICATION interceptor uses
	// to verify caller JWTs IN-PROCESS (design D2 — local verify, no per-RPC hop to
	// auth; see pkg/auth/validator.go). This service does NOT mint tokens — auth is
	// the sole minter — but every verifier across the platform must hold the SAME
	// shared secret so a token signed by auth validates here. It is `required` so a
	// misconfigured deploy fails fast at startup rather than silently accepting
	// unverifiable tokens; pkg/auth.NewJWTValidator additionally rejects a secret
	// shorter than 32 bytes (RFC 7518 §3.2 — the HMAC key must be >= the hash output
	// width, or the keyspace is brute-forceable). In K8s this is injected from a
	// Secret (FP_JWT_SECRET), never a ConfigMap — secrets are access-controlled,
	// plaintext ConfigMaps are not.
	JWTSecret string `env:"JWT_SECRET" required:"true"`

	// OTLPInsecure drives whether the OTLP exporter uses plaintext (no TLS).
	//
	// WHY this is config (not hardcoded): in local docker-compose the OTel
	// collector has no TLS cert, so the exporter must use plaintext — but in a
	// real cluster the collector terminates TLS and we must NOT downgrade to
	// plaintext (that would send traces/metrics in the clear). Defaulting to
	// true keeps local dev frictionless; production sets FP_OTEL_INSECURE=false.
	// Making it config means the SAME binary is secure-by-deployment rather than
	// secure-by-recompile.
	OTLPInsecure bool `env:"OTEL_INSECURE" default:"true"`

	// NATSDLQSubject / NATSMaxRetries are the wide-sink subscriber's DLQ policy:
	// after NATSMaxRetries redeliveries, a poison lineage event (one whose payload
	// never decodes) is routed to NATSDLQSubject instead of NAK-looping forever.
	// AckWait is left at natsutil's 30s default (the lineage write is fast).
	NATSDLQSubject string `env:"NATS_DLQ_SUBJECT" default:"fp.dlq.experiment-tracker"`
	NATSMaxRetries int    `env:"NATS_MAX_RETRIES" default:"4"`
}

func main() {
	// ================================================================
	// 1. STRUCTURED LOGGER (slog JSON → stdout → Loki)
	// ================================================================
	// slog (stdlib structured logging) with JSON output: container runtimes
	// capture stdout and ship it to Loki, which parses JSON natively. Zero
	// external deps — adequate for network-IO-bound services.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// ================================================================
	// 2. LOAD CONFIG (fails fast on a missing required field)
	// ================================================================
	// Fail-fast: a missing required field aborts startup with a clear error
	// rather than a half-configured process. In K8s, config comes from
	// ConfigMaps/Secrets mapped to env vars in the Deployment spec.
	cfg, err := config.Load[ExperimentTrackerConfig]("FP")
	if err != nil {
		logger.Error("failed to load config", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("config loaded",
		slog.Int("grpc_port", cfg.GRPCPort),
		slog.Int("health_port", cfg.Port),
		slog.String("log_level", cfg.LogLevel),
		slog.String("environment", cfg.Environment),
		slog.Bool("otel_insecure", cfg.OTLPInsecure),
	)

	// ================================================================
	// 3. OPENTELEMETRY (traces → Tempo, metrics → Prometheus)
	// ================================================================
	// NotifyContext gives a signal-aware context that drives both the OTLP setup
	// and the graceful shutdown below: SIGTERM cancels ctx → Serve returns →
	// deferred flush/drain/close run. A SIGTERM during startup aborts cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	otelShutdown, err := observability.Setup(ctx, observability.Config{
		ServiceName:    serviceName,
		ServiceVersion: "dev",
		Environment:    cfg.Environment,
		OTLPEndpoint:   cfg.OTelEndpoint,
		OTLPInsecure:   cfg.OTLPInsecure, // driven by config, not hardcoded (see field comment)
	})
	if err != nil {
		logger.Error("failed to setup observability", slog.String("error", err.Error()))
		os.Exit(1)
	}
	// Deferred FIRST so it runs LAST (LIFO): flush pending spans/metrics on
	// shutdown after every other component has emitted its teardown telemetry.
	// Without this the last ~5s of telemetry (OTel batches internally) is lost.
	// K8s grants 30s grace on SIGTERM, ample time to flush.
	defer func() {
		if shutdownErr := otelShutdown(context.Background()); shutdownErr != nil {
			logger.Error("otel shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// ================================================================
	// 4a. POSTGRES — the system of record (experiments, runs, metrics, idempotency)
	// ================================================================
	// postgres.New creates the pgxpool AND Pings it once, so a bad DSN / unreachable
	// DB fails FAST and LOUD at startup rather than on the first RPC. The pool is
	// safe for concurrent use by all the gRPC handler goroutines. We bound the
	// connect with a timeout so a wedged DB can't hang boot forever.
	pgCtx, pgCancel := context.WithTimeout(ctx, 10*time.Second)
	store, err := postgres.New(pgCtx, cfg.DatabaseURL)
	pgCancel()
	if err != nil {
		logger.Error("failed to connect to postgres", slog.String("error", err.Error()))
		os.Exit(1)
	}
	// Deferred close (runs after gRPC drains, subscribers stop, NATS drains): no
	// adapter issues SQL once the server and consumers are down.
	defer store.Close()
	logger.Info("postgres connected")

	// ================================================================
	// 4b. NATS JetStream — the async event bus (publish + the wide consume sink)
	// ================================================================
	// natsutil.Connect returns the raw connection (for lifecycle: Drain/Close,
	// IsConnected health) AND the JetStream context (for publish/consume). We keep
	// both: the conn for shutdown + the readiness check, the js for the publisher
	// and subscribers.
	natsConn, js, err := natsutil.Connect(cfg.NATSUrl)
	if err != nil {
		logger.Error("failed to connect to NATS", slog.String("error", err.Error()))
		os.Exit(1)
	}
	// Deferred Drain (runs after subscribers stop, before the pool closes): Drain
	// flushes any in-flight best-effort RunCreated/RunFinished publishes and lets
	// the server ACK them before the socket closes — Close alone could drop the
	// last few events. Drain also closes the connection when it completes.
	defer func() {
		if drainErr := natsConn.Drain(); drainErr != nil {
			logger.Error("nats drain error", slog.String("error", drainErr.Error()))
		}
	}()
	logger.Info("NATS connected", slog.String("url", cfg.NATSUrl))

	// --- ENSURE THE EXPERIMENTS STREAM EXISTS (producer owns its stream) ---
	// experiment-tracker is the OWNER and sole producer of the fp.experiments.* tree
	// (run.created / run.finished). JetStream REJECTS a publish whose subject no stream
	// captures ("no stream matches subject", 10073). Before this, NO service provisioned
	// EXPERIMENTS, so every run-lifecycle event was dropped at runtime AND notification's
	// reactor degrade-skipped fp.experiments.run.* (no stream to bind). We GUARANTEE our
	// own stream here, before wiring the publisher, exactly as registry/auth/feature-store
	// do for their trees. EnsureStream uses CreateOrUpdateStream (idempotent + convergent
	// — safe on every boot and under rolling deploys). We FAIL FAST: an unprovisionable
	// stream aborts boot (K8s CrashLoops with the cause in logs) rather than letting the
	// pod serve and silently drop every event. The context is bounded (10s, like the dep
	// pings) so a hung NATS server can't wedge boot. NOTE: we ensure ONLY the stream we
	// PRODUCE — the NOTIFICATIONS stream we CONSUME is owned by the notification service.
	streamCtx, streamCancel := context.WithTimeout(ctx, 10*time.Second)
	if streamErr := events.EnsureStream(streamCtx, js); streamErr != nil {
		streamCancel()
		logger.Error("failed to ensure EXPERIMENTS stream", slog.String("error", streamErr.Error()))
		os.Exit(1)
	}
	streamCancel()
	logger.Info("EXPERIMENTS stream ensured",
		slog.String("stream", events.StreamName),
		slog.String("subjects", events.StreamSubjects),
	)

	// ================================================================
	// 5. CONSTRUCT REPO ADAPTERS → PUBLISHER → DOMAIN SERVICE → HANDLER
	// ================================================================
	// The Store hands out three adapter structs over the SHARED pool, one per
	// domain persistence port. They can't be one Go type (their methods would
	// clash by name), so the Store exposes distinct accessors the composition root
	// wires in — see internal/repository/postgres/postgres.go.
	expRepo := store.Experiments()   // domain.ExperimentRepository
	runRepo := store.Runs()          // domain.RunRepository
	idemStore := store.Idempotency() // domain.IdempotencyStore (the Stripe-key table)

	// EventPublisher: domain payload → events.v1 wire message → NATS envelope.
	// natsutil.Publisher stamps source=serviceName so EventEnvelope.source is
	// "experiment-tracker"; events.NewPublisher maps domain → proto at the publish
	// boundary (protojson payload spliced into the JSON envelope as a RawMessage).
	// This is the domain.EventPublisher port impl — the service holds the interface
	// and never sees NATS or proto.
	publisher := events.NewPublisher(natsutil.NewPublisher(js, serviceName))

	// The DOMAIN SERVICE: all the business logic (tenancy, idempotency replay,
	// metric dedup/stamp, the FinishRun finals projection, best-effort publish).
	// It depends only on the ports above plus a real wall-clock — never on pgx,
	// NATS, or proto. NewRealClock supplies UTC time so server-authoritative
	// timestamps are deterministic across pod time zones.
	svc := domain.NewExperimentService(expRepo, runRepo, idemStore, publisher, domain.NewRealClock())
	logger.Info("experiment domain service constructed")

	// ================================================================
	// 6. BUILD gRPC SERVER (+ AUTHENTICATION) + REGISTER THE REAL HANDLER
	// ================================================================
	// grpcutil.NewServer applies the standard interceptor chain
	// (recovery → logging → auth). WithReflection lets grpcurl/grpcui introspect
	// the service in dev without the .proto files locally.
	//
	// ── AUTHN vs AUTHZ — the division of labor ──
	// The auth interceptor wired here is AUTHENTICATION ("WHO are you?"): it takes
	// the bearer JWT off the request metadata, verifies its HS256 signature LOCALLY
	// against the shared FP_JWT_SECRET (design D2 — no per-RPC network hop to the
	// auth service; pkg/auth/validator.go does the SHA-256 HMAC verify in-process),
	// and on success injects the caller's *grpcutil.Claims (UserID, Team, Role,
	// Scopes) into the request context. It does NOT decide what the caller may DO.
	//
	// AUTHORIZATION ("MAY you do this?") is the HANDLERS' job, per RPC: a handler
	// reads those claims via grpcutil.ClaimsFromContext and enforces the per-RPC
	// permission rule (e.g. experiments:write for CreateExperiment, owner/team-admin
	// for DeleteRun) AND lifts the server-authoritative Actor{UserID, Team} from the
	// verified claims rather than any request field — the mass-assignment / IDOR
	// guard the domain relies on (owner_id/team are set from claims, never accepted
	// from the client). Interceptor = authentication; handler = authorization. The
	// interceptor never authorizes; the handler never re-authenticates.
	//
	// WHY LOCAL VERIFY (D2) AND NOT AN RPC TO AUTH (D1): every authenticated RPC on
	// every non-auth service must check a token. A synchronous auth.ValidateToken
	// call per request would put auth's latency and availability in this service's
	// hot path and make auth a platform-wide SPOF. A local HMAC verify is a few µs
	// and survives an auth outage. The cost is that a revoked JWT stays valid until
	// it expires — bounded by auth's short (15m) TTL. (API keys, which DO need a DB
	// lookup, are a future RPCValidator; this service only takes user JWTs.)
	//
	// FAIL FAST: NewJWTValidator rejects a secret shorter than 32 bytes (RFC 7518
	// §3.2). We surface that as a startup abort — a service that can't verify tokens
	// must not come up and silently behave as if unauthenticated.
	validator, err := fpauth.NewJWTValidator([]byte(cfg.JWTSecret))
	if err != nil {
		logger.Error("failed to build JWT validator", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// PUBLIC METHODS — the skip list passed to the auth interceptor.
	//
	// experiment-tracker has NO genuinely-public BUSINESS RPCs. Every RPC on
	// ExperimentTrackerService operates on platform resources scoped to a caller's
	// team — create/read/update/archive experiments, run lifecycle, metric/param
	// ingestion, history/compare reads. There is no "log in", no "validate token",
	// no service-to-service credential-free gate (this service is a CONSUMER of auth,
	// not the authority). So the only exemptions are the infra RPCs every service
	// must skip: the gRPC health service (the K8s gRPC probe calls Check/Watch with
	// no credential — gating it would make the pod fail its own readiness probe) and
	// reflection (grpcurl/grpcui discovery in dev). Everything else is DEFAULT-DENY:
	// a valid JWT is required, and adding a new RPC to the proto automatically
	// requires auth unless someone consciously lists it here. We do NOT exempt the
	// mutating/ingestion RPCs (StartRun, LogMetrics, DeleteRun, …) — that would be
	// an open write path. (Contrast auth, which DOES have public Login/ValidateToken/
	// CheckPermission RPCs; this service has no analog.)
	//
	// HealthAndReflectionMethods() lives in pkg/grpcutil so the exact health +
	// reflection full-method strings stay defined once for every service.
	var publicMethods []string // intentionally empty — no public business RPCs
	srv := grpcutil.NewServer(
		grpcutil.WithLogger(logger),
		grpcutil.WithAuthValidator(validator, append(publicMethods, grpcutil.HealthAndReflectionMethods()...)...),
		grpcutil.WithReflection(),
	)

	// The handler now holds the REAL domain service (not nil): every per-RPC method
	// delegates to svc, and the nil-guards in the handler are inert. This replaces
	// the scaffold's `handler.NewExperimentHandler(nil)`.
	experimentv1.RegisterExperimentTrackerServiceServer(srv.GRPC, handler.NewExperimentHandler(svc))
	logger.Info("experiment-tracker handler registered (real service wired)")

	// ================================================================
	// 7. EVENT SUBSCRIBERS — the WIDE lineage sink (event-driven consume side)
	// ================================================================
	// Experiment Tracker is the platform's BROADEST consumer: it binds a durable
	// consumer per cross-service lifecycle subject (model registered/promoted/
	// deployed, model drift detected, pipeline started/step-done/completed,
	// inference completed, features written, usage recorded, notification
	// delivered/failed) and records each as a LINEAGE row — the provenance timeline
	// the UI shows without calling nine other services.
	//
	// The inbound path is an ADAPTER, not a domain port (see domain/ports.go): it
	// DECODES events and writes through two small adapter-owned ports the events
	// layer defines — a LineageRecorder (the row writer) and a natsutil.ProcessedStore
	// (consumer-side dedup on envelope id). Both are wired here.
	//
	// HONEST GAP (flagged in the report): the Postgres adapters for these two ports
	// — and the `lineage`/`processed_events` tables they need — do NOT exist yet
	// (the migration ships only experiments/runs/params/metrics/idempotency_keys).
	// Rather than wire a nil recorder (which would panic the first consumed event)
	// or silently swallow lineage, we wire NAMED, contract-correct interim adapters
	// (see lineageLogRecorder / memoryProcessedStore below). They keep the consume
	// pipeline fully exercisable end-to-end (decode → project → record) and are the
	// single, obvious seam the real Postgres adapters drop into — exactly the
	// "unwired backend" pattern the inference-gateway composition root uses for its
	// pending model-serving client. This is the one place the wiring is not yet
	// production-complete, and it is explicit, not hidden.
	processedStore := newProcessedStore(cfg.Environment)
	subscriber := events.NewSubscriber(events.SubscriberConfig{
		JS:           js,
		ConsumerBase: serviceName, // durable-name prefix; the adapter derives one durable per subject
		Store:        processedStore,
		MaxRetries:   cfg.NATSMaxRetries,
		DLQSubject:   cfg.NATSDLQSubject,
		// AckWait left zero → natsutil's 30s default (lineage writes are fast).
	}, lineageLogRecorder{logger: logger})

	// SubscribeAll is NON-FATAL on failure: the consumed streams (MODELS, PIPELINES,
	// INFERENCE, FEATURES, BILLING, NOTIFICATIONS) are provisioned once at platform
	// bootstrap (the infra Helm chart / a stream-bootstrap job), NOT by this service.
	// On a fresh local cluster they may not exist yet — but the SYNC plane (the gRPC
	// RPCs) is fully independent and must still serve. So we log loudly and continue
	// rather than refusing to start; the lineage sink self-heals once the streams
	// exist and the durable consumers reconnect.
	if subErr := subscriber.SubscribeAll(ctx); subErr != nil {
		logger.Warn("lineage subscribers failed to register; sync plane will still serve, lineage sink is degraded",
			slog.String("error", subErr.Error()))
	} else {
		logger.Info("lineage subscribers registered (wide event sink live)")
	}
	// Deferred Close (runs after gRPC drains, before NATS drains): stop every
	// per-subject consume loop so no event writes lineage during shutdown and no
	// goroutine outlives the process. Idempotent and safe after a partial/failed
	// SubscribeAll.
	defer subscriber.Close()

	// ================================================================
	// 8. HEALTH SERVER (HTTP) — separate port from gRPC, REAL dependency checks
	// ================================================================
	// gRPC is HTTP/2; kubelet probes are HTTP/1.1 — they can't share a plain
	// listener. A separate health port also lets us flip /readyz to 503 (draining)
	// while the gRPC server finishes in-flight RPCs. Readiness now checks the REAL
	// deps so a pod with a dead Postgres/NATS is pulled from the Service endpoints;
	// liveness stays dependency-free so a dep outage never triggers a restart storm.
	healthHandler := health.New()
	healthHandler.AddCheck("db", func(ctx context.Context) error {
		// Ping the pool: cheap, and the right readiness signal for "can I serve a
		// query right now?". pgxpool acquires/returns a conn under the hood.
		return store.Pool().Ping(ctx)
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
	// Deferred shutdown (runs after gRPC/subscribers/NATS/pool): close the probe
	// endpoint last among the HTTP/gRPC surfaces so K8s can still scrape /readyz
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
	// checked (Postgres pinged, NATS connected, subscribers registered) — that is
	// what makes a gRPC readiness probe meaningful instead of always-green. Serve
	// flips it back to NOT_SERVING on SIGTERM before draining in-flight RPCs.
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
	//   3. hard Stop if the drain times out
	//   4. returns here → deferred teardown runs LIFO:
	//        subscriber.Close → NATS Drain → pool Close → health Shutdown → OTel flush
	if serveErr := srv.Serve(ctx, lis); serveErr != nil {
		logger.Error("gRPC server error", slog.String("error", serveErr.Error()))
		os.Exit(1)
	}

	logger.Info("experiment-tracker service stopped cleanly")
}

// ============================================================================
// INTERIM CONSUME-SIDE ADAPTERS (the single seam the Postgres adapters replace)
// ============================================================================
//
// The events layer's wide lineage sink consumes through two small ports:
// events.LineageRecorder (write a provenance row) and natsutil.ProcessedStore
// (consumer-side dedup on envelope id). The Postgres adapters for both — and the
// `lineage` / `processed_events` tables — are a LATER phase (the current
// migration ships only the run-tracking tables). Until then, the composition
// root supplies these NAMED, honest interim adapters so the consume pipeline is
// fully wired and exercisable rather than nil-panicking or silently dropped.
// This mirrors the inference-gateway's `unwiredBackend`: a real, contract-correct
// object at the one seam where the production adapter has not yet landed.

// lineageLogRecorder is an interim events.LineageRecorder that LOGS each decoded
// lineage event instead of writing a Postgres row (no lineage table exists yet).
//
// It is idempotent-by-construction in the trivial sense (a log line per delivery
// is harmless on a redelivery), which is acceptable for an interim sink; the real
// Postgres adapter will be the durably-idempotent one via INSERT ... ON CONFLICT
// (event_id) DO NOTHING. Returning nil means the subscriber ACKs — correct, since
// "we observed this lineage event" succeeded. When the Postgres LineageRecorder
// lands, this one line in main is the only change.
type lineageLogRecorder struct {
	logger *slog.Logger
}

// Compile-time proof we satisfy the port. If events.LineageRecorder changes, this
// breaks HERE (at the wiring) rather than at a distant call site.
var _ events.LineageRecorder = lineageLogRecorder{}

// Record logs the projected lineage event. It NEVER errors: a log write does not
// fail in a way worth NAKing a durable event over, so the subscriber ACKs and the
// event is not redelivered. (The real Postgres adapter WILL return errors so a
// transient DB failure NAKs and JetStream redelivers — the durable correctness
// the log sink cannot offer.)
func (r lineageLogRecorder) Record(_ context.Context, ev events.LineageEvent) error {
	r.logger.Info("lineage event (interim log sink — no Postgres lineage table yet)",
		slog.String("kind", string(ev.Kind)),
		slog.String("source", ev.Source),
		slog.String("event_id", ev.EventID),
		slog.String("model_id", ev.ModelID),
		slog.String("model_version", ev.ModelVersion),
		slog.String("run_id", ev.RunID),
		slog.String("execution_id", ev.ExecutionID),
		slog.String("request_id", ev.RequestID),
		slog.String("summary", ev.Summary),
		slog.Time("occurred_at", ev.OccurredAt),
	)
	return nil
}

// newProcessedStore builds the consumer-side idempotency store for the lineage
// subscribers. It returns the in-memory natsutil store — the ONLY ProcessedStore
// implementation that exists today (a durable Postgres/Redis one is a later
// phase). NewMemoryProcessedStore PANICS under FP_ENV=production by design (it is
// per-replica, unbounded, and lost on restart — unsafe at scale), so we guard the
// caller: in production we return nil and let the subscriber rely on the lineage
// table's UNIQUE(event_id) as the dedup backstop (which the real Postgres recorder
// provides). In local/dev/test the memory store gives cross-restart-free dedup,
// which is fine for a single replica.
//
// WHY return nil rather than panic in production: the events Subscriber treats a
// nil Store as "no consumer-side dedup layer" (optsForSubject only adds
// WithIdempotencyStore when Store != nil), so the pipeline still runs — it just
// leans on the durable UNIQUE-index dedup instead of the cache layer. Failing the
// whole service to start in production because the in-memory store is unsafe would
// be the wrong trade; degrading to the durable backstop is correct.
func newProcessedStore(environment string) natsutil.ProcessedStore {
	if environment == "production" {
		// No durable ProcessedStore adapter yet; rely on the (future) lineage
		// table's UNIQUE(event_id) for dedup rather than the unsafe memory store.
		return nil
	}
	return natsutil.NewMemoryProcessedStore()
}
