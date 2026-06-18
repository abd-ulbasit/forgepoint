// Package main is the entrypoint for the Auth service.
//
// ============================================================================
// WIRING — THE COMPOSITION ROOT
// ============================================================================
//
// In Clean Architecture, main.go is the "composition root" — the only place
// where all layers are imported together and wired into a running program.
// It knows about every layer (config, observability, repository adapters, the
// domain service, the gRPC handler, the event adapter) but none of those layers
// know about each other except through the interfaces they define. Dependencies
// are constructed OUTSIDE-IN here and injected INWARD: infrastructure (pools,
// NATS) → adapters (repos, publisher) → domain service → handler.
//
// STARTUP SEQUENCE (and the strict ORDER, which is the load-bearing part):
//
//	┌──────────────────────────────────────────────────────────────────────┐
//	│  1. Structured logger (slog JSON → stdout → Loki)                     │
//	│  2. Load config (FP_* env vars → AuthConfig)                          │
//	│  3. Setup OpenTelemetry (traces → Tempo, metrics → Prom)             │
//	│  4. Connect datastores:                                               │
//	│       a. Postgres pool (pgxpool) — Ping at construct = fail-fast      │
//	│       b. NATS JetStream — connect + EnsureStream(AUTH)               │
//	│  5. Construct ADAPTERS: repos (user/apikey/role), event Publisher    │
//	│  6. Construct DOMAIN service: NewAuthService(repos…, secret, ttl)     │
//	│  7. Construct HANDLER with the REAL service (no longer nil)           │
//	│  8. Build gRPC server, register handler                              │
//	│  9. (Subscribers) auth consumes NOTHING — see note in step 9         │
//	│ 10. Health server: readiness checks the REAL deps (Postgres, NATS)   │
//	│ 11. SetServing(true) → Serve (blocks until SIGTERM/SIGINT)           │
//	│ 12. Graceful shutdown in REVERSE dependency order (see defers)        │
//	└──────────────────────────────────────────────────────────────────────┘
//
// SHUTDOWN ORDER (why reverse): things that USE a resource must stop before the
// resource they use is closed. Go runs deferred funcs LIFO, so registering them
// in construction order yields the correct teardown order automatically:
//
//	gRPC drain (Serve returns) → health server stop → NATS drain → Postgres
//	close → OTel flush.
//
// gRPC drains FIRST (Serve owns that on ctx-cancel) so no in-flight RPC touches
// a closed pool. OTel flushes LAST so shutdown spans/metrics from every earlier
// step are still recorded. (Deferred funcs are written in this file in the
// order they must RUN, exploiting LIFO so the code reads top-to-bottom.)
//
// ----------------------------------------------------------------------------
// STATE OF THE ASYNC EDGE (read this before an interview):
//
//   - PUBLISHER (Phase 1.6, DONE): we connect NATS, EnsureStream the AUTH stream,
//     construct the events.Publisher adapter, and INJECT it into the request path
//     via the EventingAuthService DECORATOR. Rather than add a publisher parameter
//     to domain.NewAuthService (which would drag NATS/proto into the domain and
//     break the dependency rule), the decorator wraps the domain.AuthService at the
//     SAME interface seam the handler uses and publishes AFTER each mutating write
//     commits (publish-after-commit). So auth genuinely emits on
//     fp.auth.user.created (CreateUser) and fp.auth.apikey.rotated (CreateAPIKey).
//     A publish failure is LOGGED, not surfaced as an RPC error — the write is
//     already committed; the lost event is the documented at-most-once tradeoff
//     (the transactional outbox, as in billing, is the upgrade path).
//
//   - SUBSCRIBERS: auth CONSUMES nothing by design — identity is the ROOT of the
//     platform's trust graph, not a downstream of it (see events/events.go). So
//     "start the subscribers" is intentionally a NO-OP for this service. Other
//     services (gateway/serving/monitor/billing/notification) wire subscribers
//     here; auth deliberately does not. This is contract-correct, not an omission.
//
//   - REDIS: the design doc lists Redis (validation cache) as a FUTURE optimization
//     for ValidateToken. No auth code consumes Redis today, so wiring a go-redis
//     client now would be dead infrastructure with a health check guarding nothing
//     real. We deliberately do NOT connect Redis until the cache lands.
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
	"syscall"
	"time"

	authv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/auth/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/config"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/health"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/observability"
	authdomain "github.com/abd-ulbasit/forgepoint/services/auth/internal/domain"
	authevents "github.com/abd-ulbasit/forgepoint/services/auth/internal/events"
	"github.com/abd-ulbasit/forgepoint/services/auth/internal/handler"
	"github.com/abd-ulbasit/forgepoint/services/auth/internal/repository/postgres"
)

// AuthConfig extends BaseConfig with auth-specific configuration.
//
// WHY embed BaseConfig:
//
//	Every Forgepoint service needs Port, GRPCPort, LogLevel, OTelEndpoint,
//	NATSUrl, DatabaseURL. Embedding them avoids repeating those field definitions
//	in every service's config struct. config.Load[AuthConfig]("FP") reads
//	FP_PORT, FP_GRPC_PORT, FP_JWT_SECRET, etc. via reflection (see pkg/config).
//
// SECURITY NOTE: JWTSecret is required (required:"true"). The service refuses to
// start if it's unset — fail-fast is the correct behavior for security config.
// In K8s, this is injected via a Secret mounted as an env var.
type AuthConfig struct {
	config.BaseConfig

	// JWTSecret is the HMAC-SHA256 signing key for JWT tokens.
	// Must be at least 32 bytes of high-entropy random data in production
	// (the domain rejects shorter secrets with ErrWeakSecret — 256-bit floor,
	// matching SHA-256's output width). K8s: mounted from a Secret (not a
	// ConfigMap — secrets are access-controlled, unlike plaintext ConfigMaps).
	JWTSecret string `env:"JWT_SECRET" required:"true"`

	// JWTTTL is the lifetime of issued JWTs. Kept short by default (15m) per the
	// stateless-JWT staleness tradeoff (design D2): a JWT embeds role/claims that
	// can go stale (e.g. after AssignRole or suspension), and a JWT cannot be
	// revoked without a blacklist, so a SHORT ttl bounds how long a stale or
	// compromised token stays valid. We expose it as config (not a hardcoded
	// const) so ops can tune the security/usability tradeoff per environment.
	// time.Duration parses Go duration strings ("15m", "1h") via pkg/config.
	JWTTTL time.Duration `env:"JWT_TTL" default:"15m"`

	// BootstrapAdminEmail / BootstrapAdminPassword drive the OPTIONAL first-admin
	// bootstrap (the chicken-and-egg fix: the migration seeds roles but no users,
	// and CreateUser is admin-gated, so without this nobody could ever
	// authenticate). When BOTH are set, the service ensures an admin user with
	// this email exists at boot (see domain.AdminBootstrapper.BootstrapAdmin) —
	// idempotent, so it's safe on every restart/replica.
	//
	// NEITHER is required:"true" — bootstrap is opt-in. We gate on BOTH being
	// present (an XOR is a misconfig we warn about) rather than making either
	// mandatory, so a cluster that provisions its first admin some other way
	// (e.g. a one-off Job) can leave these unset.
	//
	// SECURITY: the EMAIL is non-secret config (ConfigMap). The PASSWORD is a
	// SECRET (mounted from a K8s Secret, never a ConfigMap, never committed). The
	// password is read once at boot, used to bcrypt, and never logged.
	BootstrapAdminEmail    string `env:"BOOTSTRAP_ADMIN_EMAIL"`
	BootstrapAdminPassword string `env:"BOOTSTRAP_ADMIN_PASSWORD"`
}

func main() {
	// ================================================================
	// 1. STRUCTURED LOGGER
	// ================================================================
	// slog is the stdlib structured logger (Go 1.21+). JSON output for container
	// environments where logs are shipped to Loki via the runtime's stdout
	// capture. JSON is parse-friendly for log aggregators. (We construct it before
	// config so even a config-load failure logs in the structured format.)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// ================================================================
	// 2. LOAD CONFIG
	// ================================================================
	// config.Load reads FP_* env vars into AuthConfig using reflection. It fails
	// fast if any required field (JWT_SECRET) is missing.
	cfg, err := config.Load[AuthConfig]("FP")
	if err != nil {
		logger.Error("failed to load config", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("config loaded",
		slog.Int("grpc_port", cfg.GRPCPort),
		slog.Int("health_port", cfg.Port),
		slog.String("log_level", cfg.LogLevel),
		slog.Duration("jwt_ttl", cfg.JWTTTL),
	)

	// ================================================================
	// 3. SIGNAL-AWARE ROOT CONTEXT + OPENTELEMETRY
	// ================================================================
	// signal.NotifyContext cancels ctx on SIGINT/SIGTERM. SIGTERM is what K8s
	// sends on pod termination (rolling update, scale-down); SIGINT is Ctrl-C in
	// local dev. This single ctx drives BOTH the OTLP setup and the gRPC Serve
	// loop's graceful-shutdown trigger below — one cancellation source, one clean
	// path. We add SIGTERM explicitly (os.Interrupt alone misses it on Linux).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Setup initializes the trace provider (→ Tempo) and meter provider
	// (→ Prometheus via the OTel Collector). All spans/metrics from this process
	// flow through these providers. OTLPInsecure=true because the local
	// docker-compose collector has no TLS cert; in production this is driven by
	// config and TLS is on.
	otelShutdown, err := observability.Setup(ctx, observability.Config{
		ServiceName:    "auth",
		ServiceVersion: "dev",
		Environment:    "local",
		OTLPEndpoint:   cfg.OTelEndpoint,
		OTLPInsecure:   true,
	})
	if err != nil {
		logger.Error("failed to setup observability", slog.String("error", err.Error()))
		os.Exit(1)
	}
	// DEFER #LAST-TO-RUN: flush telemetry. Registered first → runs last (LIFO), so
	// shutdown spans/metrics from every later teardown step are still exported.
	// We use a FRESH context (not the cancelled ctx) bounded by a timeout: the
	// root ctx is already Done by the time we get here, and a Done context would
	// abort the flush immediately, losing the final batch.
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if shutdownErr := otelShutdown(shutdownCtx); shutdownErr != nil {
			logger.Error("otel shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// ================================================================
	// 4a. CONNECT POSTGRES (pgxpool)
	// ================================================================
	// postgres.Connect opens a pgxpool and PINGS it — turning a bad DSN or an
	// unreachable database into a loud, fast startup failure (this exit) instead
	// of a confusing error on the first user request. The pool is the SHARED
	// process-wide handle all three repo adapters borrow connections from.
	//
	// We bound the connect with a timeout so a hung/unreachable Postgres can't
	// wedge startup forever — the pod would never report ready and K8s would never
	// learn why. A bounded dial fails fast and the pod restarts.
	dbCtx, dbCancel := context.WithTimeout(ctx, 10*time.Second)
	db, err := postgres.Connect(dbCtx, cfg.DatabaseURL)
	dbCancel()
	if err != nil {
		logger.Error("failed to connect to postgres", slog.String("error", err.Error()))
		os.Exit(1)
	}
	// DEFER: close the pool near-last (after gRPC has drained, so no in-flight
	// query borrows a connection from a closing pool). Registered here so it runs
	// before the OTel flush (LIFO) but after the gRPC/NATS teardown registered later.
	defer db.Close()
	logger.Info("postgres connected")

	// ================================================================
	// 4b. CONNECT NATS JETSTREAM + ENSURE STREAM
	// ================================================================
	// natsutil.Connect returns the raw *nats.Conn (for lifecycle: IsConnected,
	// Drain) and a JetStream context (for publish/stream ops). We then EnsureStream
	// the AUTH stream so published events are PERSISTED — JetStream silently drops
	// a publish whose subject no stream captures (core-NATS fire-and-forget leaks
	// through without a stream). The PRODUCER owns its stream; consumers create
	// consumers ON it, never the stream itself (see events/events.go).
	//
	// EnsureStream is idempotent (CreateOrUpdateStream), so it is safe on every
	// boot and converges the stream config under rolling deploys.
	natsConn, js, err := natsutil.Connect(cfg.NATSUrl)
	if err != nil {
		logger.Error("failed to connect to nats", slog.String("error", err.Error()))
		os.Exit(1)
	}
	// DEFER: drain NATS before closing Postgres/flushing OTel. Drain (not Close)
	// flushes any buffered publishes and lets in-flight handlers finish — the
	// graceful analogue of GracefulStop for the message bus. It is registered
	// AFTER db.Close above, so by LIFO it runs BEFORE db.Close — correct: anything
	// that might publish during shutdown stops before the DB it reads from closes.
	defer func() {
		if drainErr := natsConn.Drain(); drainErr != nil {
			logger.Error("nats drain error", slog.String("error", drainErr.Error()))
		}
	}()

	streamCtx, streamCancel := context.WithTimeout(ctx, 10*time.Second)
	err = authevents.EnsureStream(streamCtx, js)
	streamCancel()
	if err != nil {
		logger.Error("failed to ensure AUTH jetstream stream", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("nats connected and AUTH stream ensured",
		slog.String("stream", authevents.StreamName),
		slog.String("subjects", authevents.StreamSubjects),
	)

	// ================================================================
	// 5. CONSTRUCT ADAPTERS (repositories + event publisher)
	// ================================================================
	// Repository adapters: the Postgres-backed implementations of the domain's
	// repository PORTS (domain.UserRepository, APIKeyRepository, RoleRepository).
	// They share the ONE pool (db) — three pools would triple the connection
	// budget against the same database for no benefit.
	userRepo := postgres.NewUserRepo(db)
	apiKeyRepo := postgres.NewAPIKeyRepo(db)
	roleRepo := postgres.NewRoleRepo(db)

	// Event publisher adapter: layers auth's domain→events/v1 mapping on top of the
	// shared natsutil.Publisher (which owns the envelope + dedup + trace policy).
	// SourceName ("auth") is stamped into every event's envelope for provenance.
	//
	// Phase 1.6 (DONE): this Publisher is now INJECTED into the request path via the
	// EventingAuthService decorator constructed below — auth genuinely emits on
	// fp.auth.user.created and fp.auth.apikey.rotated. The publisher satisfies the
	// authevents.EventPublisher PORT the decorator depends on.
	natsPublisher := natsutil.NewPublisher(js, authevents.SourceName)
	eventPublisher := authevents.NewPublisher(natsPublisher)
	logger.Info("auth event publisher constructed",
		slog.String("source", authevents.SourceName),
		slog.String("subject_user_created", authevents.SubjectUserCreated),
		slog.String("subject_apikey_rotated", authevents.SubjectAPIKeyRotated),
	)

	// ================================================================
	// 6. CONSTRUCT DOMAIN SERVICE
	// ================================================================
	// NewAuthService injects the repository ports + the signing secret + the JWT
	// ttl, and returns the domain.AuthService INTERFACE (not the concrete struct):
	// the handler depends on the abstraction, the constructor is the only place
	// that knows the concrete impl. The secret is passed as []byte (the domain
	// validates >= 32 bytes at mint/verify time → ErrWeakSecret on a weak key).
	svc := authdomain.NewAuthService(
		userRepo,
		apiKeyRepo,
		roleRepo,
		[]byte(cfg.JWTSecret),
		cfg.JWTTTL,
	)

	// ================================================================
	// 6a. BOOTSTRAP THE FIRST ADMIN (trusted in-process path) — OPTIONAL
	// ================================================================
	// THE CHICKEN-AND-EGG: the migration seeds the admin/engineer/viewer ROLES but
	// NO users, and CreateUser is admin-gated at the RPC layer. So on a fresh
	// database there is no identity that can authenticate and no way to mint the
	// first admin over the API. We close that here with a TRUSTED, IN-PROCESS
	// bootstrap that bypasses the RPC admin-gate precisely because it does NOT go
	// through gRPC — the operator who set FP_BOOTSTRAP_ADMIN_* IS the trust anchor
	// (the same shape as Grafana's GF_SECURITY_ADMIN_*, Keycloak's KEYCLOAK_ADMIN_*).
	//
	// We run it against the UN-decorated svc (the raw domain.AuthService). Bootstrap
	// is not a platform RPC and must not emit fp.auth.user.created — it is control-
	// plane provisioning, not an application event; using svc (not eventingSvc, built
	// below) keeps it off the event bus by construction.
	//
	// ORDERING: this runs AFTER Postgres is connected (4a) and the migrate
	// initContainer has applied the schema + seeded roles (deploy/helm: the
	// initContainer completes before the app container starts), so the 'admin' role
	// AssignRole resolves by name is guaranteed present. It is IDEMPOTENT, so it is
	// safe on every boot of every replica.
	//
	// GATING: only when BOTH env vars are set. An XOR is a misconfiguration we warn
	// about (and skip) rather than fail on, so a half-set config can't wedge startup.
	switch {
	case cfg.BootstrapAdminEmail != "" && cfg.BootstrapAdminPassword != "":
		// The concrete domain service exposes the boot-only AdminBootstrapper
		// capability; the type assertion is the single place that reaches past the
		// RPC-facing AuthService interface to it (see domain/bootstrap.go for WHY it
		// is a segregated interface and not an AuthService method).
		bootstrapper, ok := svc.(authdomain.AdminBootstrapper)
		if !ok {
			// Defensive: NewAuthService always returns *authService, which satisfies
			// AdminBootstrapper (compile-time asserted in bootstrap.go). If this ever
			// fails, the wiring changed and we must fail loudly, not silently skip
			// provisioning the only admin.
			logger.Error("auth service does not implement AdminBootstrapper; cannot bootstrap admin")
			os.Exit(1)
		}
		bootCtx, bootCancel := context.WithTimeout(ctx, 10*time.Second)
		adminUser, createdNow, bootErr := bootstrapper.BootstrapAdmin(bootCtx, cfg.BootstrapAdminEmail, cfg.BootstrapAdminPassword)
		bootCancel()
		if bootErr != nil {
			// A bootstrap failure (DB error, role missing, validation) is fatal: the
			// platform is unusable without a first admin, so refusing to start makes
			// the problem loud instead of leaving a silently login-less deployment.
			logger.Error("failed to bootstrap admin user", slog.String("error", bootErr.Error()))
			os.Exit(1)
		}
		// NOTE: we log the EMAIL and id (non-secret) but NEVER the password.
		if createdNow {
			logger.Info("bootstrap admin created",
				slog.String("email", adminUser.Email),
				slog.String("user_id", adminUser.ID),
				slog.String("role", "admin"),
			)
		} else {
			logger.Info("bootstrap admin already exists; skipping (idempotent)",
				slog.String("email", adminUser.Email),
				slog.String("user_id", adminUser.ID),
			)
		}
	case cfg.BootstrapAdminEmail != "" || cfg.BootstrapAdminPassword != "":
		// Exactly one of the pair is set — an operator likely intended to bootstrap
		// but mis-wired the secret/config. Warn and proceed (don't fail): the
		// service can still run for an already-provisioned database.
		logger.Warn("admin bootstrap is half-configured; set BOTH FP_BOOTSTRAP_ADMIN_EMAIL and FP_BOOTSTRAP_ADMIN_PASSWORD to enable it (skipping bootstrap)",
			slog.Bool("email_set", cfg.BootstrapAdminEmail != ""),
			slog.Bool("password_set", cfg.BootstrapAdminPassword != ""),
		)
	default:
		logger.Info("admin bootstrap disabled (FP_BOOTSTRAP_ADMIN_EMAIL/PASSWORD not set)")
	}

	// ================================================================
	// 6b. DECORATE THE DOMAIN SERVICE WITH EVENT PUBLISHING (Phase 1.6)
	// ================================================================
	// The DECORATOR injects the publisher at the SAME seam the handler already uses
	// (the domain.AuthService interface), so the domain stays free of any NATS/proto
	// knowledge (the Clean Architecture dependency rule) and the handler is unchanged.
	//
	// EventingAuthService forwards every RPC to svc and, on the two MUTATING paths,
	// publishes AFTER the inner write commits (publish-after-commit, by ordering):
	//   CreateUser   → fp.auth.user.created    (notification welcome, billing open-account, audit)
	//   CreateAPIKey → fp.auth.apikey.rotated  (inference-gateway API-key cache eviction, notification)
	// A publish failure is LOGGED, not surfaced as an RPC error — the user/key is
	// already committed, so failing the request would be a lie about a success and
	// invite a duplicate-write retry. The lost event is the documented at-most-once
	// tradeoff (the outbox, as in billing, is the upgrade path).
	//
	// From here on the handler is given `eventingSvc` (still a domain.AuthService),
	// so non-mutating RPCs (Login/ValidateToken/CheckPermission/...) pass straight
	// through to svc untouched and only the event-bearing writes are intercepted.
	eventingSvc := authevents.NewEventingAuthService(svc, eventPublisher, logger)

	// ================================================================
	// 7. CONSTRUCT HANDLER WITH THE REAL SERVICE (no longer nil)
	// ================================================================
	// This is the line the scaffold promised: the handler now holds the fully
	// wired domain service — wrapped by the event-publishing decorator. Its RPC
	// methods (auth_handler_rpcs.go) call svc.Login, svc.CreateUser, etc.; the
	// decorator emits the platform events on the write paths. The nil-svc guards in
	// those methods are now dead-but-defensive (a real svc is always present here).
	authHandler := handler.NewAuthHandler(eventingSvc)

	// ================================================================
	// 8. BUILD gRPC SERVER + REGISTER HANDLER
	// ================================================================
	// grpcutil.NewServer applies the standard interceptor chain
	// (recovery → logging → tracing). We do NOT add WithAuthValidator here:
	//
	//	The AuthService itself IS the token validator — it would be circular for
	//	the auth service to require an auth interceptor on its own RPCs (and Login/
	//	ValidateToken are unauthenticated by design). Privileged admin RPCs
	//	(CreateUser/AssignRole/...) enforce authorization IN the handler via
	//	requireAdmin → CheckPermission until a platform-wide authz interceptor
	//	lands (see auth_handler_rpcs.go). So the auth service is the source of
	//	truth, not a consumer of the interceptor.
	//
	// WithReflection: lets grpcurl/grpcui introspect the service in dev without
	// the .proto files locally.
	srv := grpcutil.NewServer(
		grpcutil.WithLogger(logger),
		grpcutil.WithReflection(),
	)
	authv1.RegisterAuthServiceServer(srv.GRPC, authHandler)
	logger.Info("auth handler registered with wired domain service")

	// ================================================================
	// 9. EVENT SUBSCRIBERS — INTENTIONALLY NONE FOR AUTH
	// ================================================================
	// Auth CONSUMES no events: identity is the ROOT of the platform's trust graph,
	// not a downstream of it (events/events.go). Every other consuming service
	// would here construct natsutil.NewSubscriber(...).Subscribe(...) for each
	// subject it reacts to, and register their lifecycle in shutdown. Auth has
	// nothing to subscribe to, so this step is a deliberate, contract-correct
	// NO-OP — recorded in the log so the absence is explicit, not an oversight.
	logger.Info("no event subscribers for auth (identity is the trust-graph root; consumes nothing)")

	// ================================================================
	// 10. HEALTH SERVER (HTTP) — readiness checks the REAL dependencies
	// ================================================================
	// The HTTP health server runs on Port (default 8080):
	//   GET /healthz → liveness  (200 while the process is alive)
	//   GET /readyz  → readiness (200 iff ALL registered checks pass, else 503)
	//
	// WHY a separate HTTP port from gRPC: gRPC is HTTP/2, kubelet HTTP probes are
	// HTTP/1.1; they can't share a plain listener. Separate ports let the gRPC
	// server drain (readiness flips to 503) independently of process liveness.
	//
	// Readiness now reflects REALITY — it checks the actual dependencies:
	//   - "postgres": pool.Ping — is the database reachable RIGHT NOW.
	//   - "nats":     conn.IsConnected — is the bus reachable (auto-reconnect means
	//                 a transient blip self-heals; a hard-down NATS fails readiness
	//                 so K8s pulls the pod from the Service until it recovers).
	// This is what makes /readyz meaningful: the pod reports ready only when it can
	// actually do its job, not merely "process is up".
	healthHandler := health.New()
	healthHandler.AddCheck("postgres", func(ctx context.Context) error {
		return db.Pool().Ping(ctx)
	})
	healthHandler.AddCheck("nats", func(_ context.Context) error {
		if !natsConn.IsConnected() {
			return fmt.Errorf("nats not connected (status: %s)", natsConn.Status())
		}
		return nil
	})

	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", healthHandler.LivenessHandler())
	healthMux.HandleFunc("/readyz", healthHandler.ReadinessHandler())

	healthAddr := fmt.Sprintf(":%d", cfg.Port)
	healthServer := &http.Server{
		Addr:    healthAddr,
		Handler: healthMux,
		// A read header timeout bounds slowloris-style stalls on the health port.
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Run the health server in a goroutine so it doesn't block the gRPC Serve below.
	go func() {
		logger.Info("health server listening", slog.String("addr", healthAddr))
		if serveErr := healthServer.ListenAndServe(); serveErr != nil && serveErr != http.ErrServerClosed {
			logger.Error("health server error", slog.String("error", serveErr.Error()))
		}
	}()

	// DEFER: stop the health server during shutdown. Registered AFTER db/nats
	// defers, so by LIFO it runs BEFORE them — readiness stops answering 200
	// before the deps it checks are torn down (no false "ready" mid-shutdown).
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if shutdownErr := healthServer.Shutdown(shutdownCtx); shutdownErr != nil {
			logger.Error("health server shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// ================================================================
	// 11. START gRPC SERVER (blocks until SIGTERM/SIGINT)
	// ================================================================
	// Flip the gRPC health status to SERVING now that EVERY dependency is wired
	// and verified (pool pinged, NATS connected, stream ensured). Doing this only
	// after construction is what makes a gRPC readiness probe meaningful instead
	// of always-green from the first instant.
	srv.SetServing(true)

	grpcAddr := fmt.Sprintf(":%d", cfg.GRPCPort)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("failed to listen on gRPC port", slog.String("addr", grpcAddr), slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("gRPC server listening", slog.String("addr", grpcAddr))

	// Serve blocks until ctx is cancelled (SIGINT/SIGTERM). On cancellation it:
	//   1. SetServing(false) — readiness fails → pod leaves the Service endpoints
	//   2. GracefulStop (bounded drain) — in-flight RPCs finish, no new ones admitted
	//   3. hard Stop if drain times out
	//   4. returns here → the deferred teardown runs in LIFO order:
	//        health server stop → NATS drain → Postgres close → OTel flush.
	if serveErr := srv.Serve(ctx, lis); serveErr != nil {
		logger.Error("gRPC server error", slog.String("error", serveErr.Error()))
		os.Exit(1)
	}

	logger.Info("auth service stopped cleanly")
}
