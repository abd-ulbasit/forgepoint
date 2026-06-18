// Package main is the entrypoint for the Billing service.
//
// ============================================================================
// WIRING — THE COMPOSITION ROOT (fully wired: Postgres + Redis + NATS)
// ============================================================================
//
// In Clean Architecture, main.go is the "composition root" — the ONLY place all
// layers are imported together and wired into a running program. It knows about
// every layer (config, observability, repository adapters, the domain service,
// the event adapters, the handler) but none of those layers know about each
// other except through the interfaces the domain defines (UsageStore /
// RatePlanStore / InvoiceStore / IDProvider / Clock, and — on the events side —
// OutboxReader / TeamResolver / ProcessedStore). Here those ports get their
// concrete adapters: "Postgres for the ledger, the same pool for the outbox
// drain, Redis for consumer dedup, NATS for the bus" becomes literal in this
// file and nowhere else.
//
// THE DEPENDENCY-INJECTION GRAPH WE ASSEMBLE (inner depends on nothing outer):
//
//	pgxpool ─► postgres.Store ─► Usage/RatePlans/Invoices ─┐
//	                                                        ├─► domain.NewBillingService ─► handler.BillingHandler ─► gRPC
//	postgres.UUIDProvider + postgres.SystemClock ──────────┘
//
//	                  ┌─ OUTBOX RELAY (DB→NATS producer half) ─────────────────┐
//	pgxpool ─► events.PgxOutboxReader ─┐                                        │
//	jetstream ─► events.RelayPublisher ─┴─► events.OutboxRelay ─► go relay.Run  │
//	                                                                            │
//	                  ┌─ INFERENCE CONSUMER (NATS→meter half) ─────────────────┤
//	jetstream + svc + TeamResolver + ProcessedStore ─► events.InferenceConsumer ┘
//	                                                    └─► consumer.Register(ctx)
//
// STARTUP SEQUENCE (each step fail-fast — a bad dep aborts boot, never serves):
//
//	┌──────────────────────────────────────────────────────────────────────────┐
//	│ 1. Structured logger (slog JSON → stdout → Loki)                           │
//	│ 2. Load config (FP_* env vars → BillingConfig); require DatabaseURL        │
//	│ 3. Setup OpenTelemetry (traces → Tempo, metrics → Prometheus)              │
//	│ 4. Connect datastores (Postgres pool, Redis client, NATS+JetStream)        │
//	│    — each constructor PINGS so an unreachable dep fails boot, not RPC #1    │
//	│    — ensure the BILLING / INFERENCE / DLQ streams exist (idempotent)        │
//	│ 5. Build adapters → domain service → real handler (NOT nil)                │
//	│ 6. Build gRPC server, register the WIRED handler                           │
//	│ 7. Start the OUTBOX RELAY (producer) + INFERENCE CONSUMER (subscriber)     │
//	│ 8. Start HTTP health server (/readyz checks Postgres, Redis, NATS live)    │
//	│ 9. Mark SERVING + start gRPC server (blocks until SIGTERM/SIGINT)          │
//	│ 10. Graceful shutdown in order (gRPC drain → health → subscriber → relay   │
//	│     stop via ctx → NATS drain → Redis/pool close → otel flush LAST)         │
//	└──────────────────────────────────────────────────────────────────────────┘
//
// BILLING'S DUAL EVENT ROLE (both halves wired here — see internal/events):
//   - PRODUCER (outbox relay): RecordUsage / GenerateInvoice commit an `outbox`
//     row in the SAME Postgres tx as the business write; the relay drains those
//     rows to NATS (fp.billing.usage.recorded / quota.exceeded / invoice.generated).
//     That is the DB→NATS half of exactly-once-in-effect.
//   - CONSUMER (inference consumer): billing meters usage by REACTING to
//     fp.inference.completed — each event becomes one (or two) RecordUsage calls,
//     which themselves write the outbox, closing the loop.
//
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
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"

	billingv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/billing/v1"
	fpauth "github.com/abd-ulbasit/forgepoint/pkg/auth"
	"github.com/abd-ulbasit/forgepoint/pkg/config"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/health"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/observability"
	"github.com/abd-ulbasit/forgepoint/services/billing/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/billing/internal/events"
	"github.com/abd-ulbasit/forgepoint/services/billing/internal/handler"
	"github.com/abd-ulbasit/forgepoint/services/billing/internal/repository/postgres"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// depConnectTimeout bounds how long we wait, AT BOOT, for each datastore's
// fail-fast ping / stream-ensure. WHY a bound: a constructor that blocks forever
// on an unreachable dependency would hang the pod in a not-ready state with no
// signal; a bounded ctx turns "DB is down" into a clear boot error K8s can act on
// (the pod CrashLoops and surfaces the cause in logs/events). Kept short — if a
// core dependency isn't reachable in 10s at startup, failing fast is correct.
const depConnectTimeout = 10 * time.Second

// idempotencyTTL bounds how long the Redis-backed consumer dedup key lives. The
// key only needs to outlive the NATS redelivery window (a redelivered event must
// be recognized as already-processed). 48h is comfortably beyond any realistic
// redelivery, and the DURABLE backstop is the domain's own (team, idempotency_key)
// uniqueness anyway — so this fast-path key is allowed to expire. See the
// ProcessedStore note below and natsutil/idempotency.go's exactly-once caveat.
const idempotencyTTL = 48 * time.Hour

// BillingConfig extends BaseConfig with billing-specific configuration.
//
// WHY embed BaseConfig: every Forgepoint service needs Port, GRPCPort, LogLevel,
// OTelEndpoint, NATSUrl, DatabaseURL. Embedding them avoids repeating those
// fields. config.Load[BillingConfig]("FP") reads FP_PORT, FP_GRPC_PORT,
// FP_REDIS_URL, FP_DEFAULT_CURRENCY, etc. via reflection (see pkg/config).
type BillingConfig struct {
	config.BaseConfig

	// JWTSecret is the HMAC-SHA256 verification key for the auth interceptor.
	//
	// WHY billing needs it even though it never MINTS tokens: every BillingService
	// RPC is team-scoped (the handler reads the caller's team/role from claims for
	// tenant isolation and the CreateRatePlan admin gate), so every RPC must be
	// AUTHENTICATED. Authentication here is design D2 — LOCAL JWT verification
	// (pkg/auth.NewJWTValidator): the interceptor verifies the HS256 signature
	// in-process with this shared secret, so there is NO synchronous hop to the auth
	// service on the hot path and an auth-service outage cannot block in-flight
	// metering. The auth service SIGNS with the same FP_JWT_SECRET; billing only
	// VERIFIES with it — symmetric key, asymmetric role.
	//
	// required:"true" (fail-fast): the interceptor cannot be wired without it, and a
	// billing service that accepted unauthenticated RPCs would mis-attribute money.
	// pkg/auth enforces a >=32-byte floor (RFC 7518 §3.2) at construction, rejecting
	// a weak/forgeable secret at startup rather than under load. K8s: mounted from a
	// Secret (access-controlled), NEVER a ConfigMap (plaintext in etcd).
	JWTSecret string `env:"JWT_SECRET" required:"true"`

	// DefaultCurrency is the deployment's settlement currency (ISO-4217). Used as
	// the fallback currency for server-built artifacts when no plan currency
	// applies. Validated to a 3-letter uppercase code by the domain on use.
	DefaultCurrency string `env:"DEFAULT_CURRENCY" default:"USD"`

	// RedisURL is the connection string (bare host:port) for the consumer-side
	// idempotency ProcessedStore — the fast dedup layer over NATS at-least-once
	// redelivery. NewClient takes an "addr", so the default is bare host:port; a
	// full "redis://" URL with auth/db would need redis.ParseURL (a later concern
	// when Redis grows credentials). Distinct from DatabaseURL: Redis holds only
	// the short-lived dedup keys, Postgres holds the money-truth ledger + outbox.
	RedisURL string `env:"REDIS_URL" default:"localhost:6379"`

	// OTelInsecure controls whether the OTLP exporter uses plaintext (no TLS). It
	// is true in local docker-compose (the collector has no cert) and false in
	// production (mTLS to the collector). Wiring it FROM CONFIG — rather than
	// hard-coding true — is the correct, env-driven 12-factor approach: the same
	// binary is secure in prod and convenient locally. Zero value is false (secure).
	OTelInsecure bool `env:"OTEL_INSECURE" default:"true"`

	// MeteringEnabled is the kill-switch for the event consumers (inference +
	// storage). DEFAULT true (preserves prior behaviour). Set FP_BILLING_METERING_
	// ENABLED=false to NOT register the consumers — useful while the team resolver is
	// unwired, so billing can serve its gRPC/read APIs and run the outbox relay
	// WITHOUT dead-lettering 100% of inference/storage traffic to fp.dlq.billing. The
	// DLQ'd events (if any accrued before disabling) remain replayable once the
	// resolver lands. WHY a flag and not just "always register": with an unwired
	// resolver, registering guarantees an all-DLQ outage; an operator needs an
	// explicit, observable way to opt out of that until metering can actually work.
	MeteringEnabled bool `env:"BILLING_METERING_ENABLED" default:"true"`

	// UnwiredResolverFailClosed records, for the observability signal, that the team
	// resolvers are the fail-closed placeholders rather than the real Auth/registry
	// clients. It defaults to true because this binary currently ships WITHOUT the
	// real resolvers wired; when they land, set FP_BILLING_UNWIRED_RESOLVER_FAIL_
	// CLOSED=false (or remove this field as the placeholder is deleted) so the
	// startup WARN + billing_unwired_team_resolver_active counter stop firing. This
	// makes the "billing is a no-op" condition a CONFIG-VISIBLE, alertable fact
	// instead of a silent code default.
	UnwiredResolverFailClosed bool `env:"BILLING_UNWIRED_RESOLVER_FAIL_CLOSED" default:"true"`
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
	// 2. LOAD CONFIG (FP_* env vars → BillingConfig, fail-fast)
	// ================================================================
	cfg, err := config.Load[BillingConfig]("FP")
	if err != nil {
		logger.Error("failed to load config", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// DatabaseURL has no compile-time `required` tag because it lives on the SHARED
	// BaseConfig (some services have no DB). For billing it is mandatory: the
	// Postgres ledger is the money source of truth and the outbox transactional
	// boundary. Enforce it HERE, at the one place that knows this service needs a
	// DB, rather than mutating the shared struct. Failing now (not on the first
	// RecordUsage) is the fail-fast rule.
	if cfg.DatabaseURL == "" {
		logger.Error("FP_DATABASE_URL is required for the billing service (the Postgres ledger + outbox)")
		os.Exit(1)
	}

	logger.Info("config loaded",
		slog.Int("grpc_port", cfg.GRPCPort),
		slog.Int("health_port", cfg.Port),
		slog.String("log_level", cfg.LogLevel),
		slog.String("nats_url", cfg.NATSUrl),
		slog.String("default_currency", cfg.DefaultCurrency),
		slog.Bool("otel_insecure", cfg.OTelInsecure),
	)

	// ================================================================
	// 3. OPENTELEMETRY (traces → Tempo, metrics → Prometheus via the collector)
	// ================================================================
	// NotifyContext gives a signal-aware context: SIGTERM/SIGINT cancels it, which
	// both aborts a slow startup cleanly AND drives the graceful shutdown of the
	// gRPC server, the outbox relay, and the consumer below. The same ctx is
	// threaded through everything that must stop on shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	otelShutdown, err := observability.Setup(ctx, observability.Config{
		ServiceName:    "billing",
		ServiceVersion: "dev",
		Environment:    "local",
		OTLPEndpoint:   cfg.OTelEndpoint,
		OTLPInsecure:   cfg.OTelInsecure, // ← from config, not hard-coded
	})
	if err != nil {
		logger.Error("failed to setup observability", slog.String("error", err.Error()))
		os.Exit(1)
	}
	// NOTE ON SHUTDOWN ORDERING: we DON'T `defer otelShutdown` here. Telemetry must
	// flush LAST (after gRPC drained, the relay/consumer stopped, and the stores
	// closed) so the spans/metrics emitted DURING shutdown are captured. All cleanup
	// is hung off an explicit, ordered shutdown() closure invoked once at the end.

	// ================================================================
	// 4. CONNECT DATASTORES (Postgres ledger+outbox, Redis dedup, NATS bus) — fail-fast
	// ================================================================
	// We own the RAW pgxpool.Pool, go-redis.Client, and nats.Conn here (not just the
	// adapters) because: (a) /readyz needs a live ping handle on each dep, (b) the
	// outbox relay reads the SAME pool the repository wrote (database-per-service),
	// and (c) ordered shutdown must close the pool/client/conn at the right moment.
	// A BOUNDED ctx (not the signal ctx) guards against a hung dial wedging startup.
	bootCtx, bootCancel := context.WithTimeout(context.Background(), depConnectTimeout)
	defer bootCancel()

	// --- Postgres: the ledger (money truth) + the outbox transactional boundary ---
	// pgxpool.New is LAZY (no TCP until first use); Ping forces a real connection so
	// a bad DSN / unreachable DB fails boot here, not on the first RecordUsage RPC.
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
	logger.Info("postgres connected (ledger + outbox)")

	// --- Redis: the consumer-side idempotency ProcessedStore (fast dedup) ---
	// go-redis's Client is itself a pool; Ping fails fast on an unreachable server.
	rdb := goredis.NewClient(&goredis.Options{Addr: cfg.RedisURL})
	if err := rdb.Ping(bootCtx).Err(); err != nil {
		_ = rdb.Close()
		pgPool.Close()
		logger.Error("failed to ping Redis", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("redis connected (consumer idempotency store)")

	// --- NATS JetStream: the event bus (relay publishes, consumer reads) ---
	// Connect returns the raw conn (for lifecycle: Drain on shutdown, IsConnected
	// for readiness) AND the JetStream context (for the relay publisher + consumer).
	natsConn, js, err := natsutil.Connect(cfg.NATSUrl)
	if err != nil {
		_ = rdb.Close()
		pgPool.Close()
		logger.Error("failed to connect NATS", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("nats connected", slog.String("url", natsConn.ConnectedUrl()))

	// ENSURE THE STREAMS BILLING USES EXIST. JetStream silently DROPS a published
	// message whose subject no stream captures, and a consumer cannot attach to a
	// non-existent stream. In production these are provisioned once at platform
	// bootstrap (the infra Helm chart) since multiple services share BILLING /
	// INFERENCE / the DLQ stream; we reconcile them here too with the idempotent +
	// convergent CreateOrUpdateStream so a fresh local stack (docker-compose) comes
	// up working without a separate bootstrap step. If infra-as-code already created
	// them, this no-ops to the same config rather than failing.
	//   - BILLING  binds fp.billing.>   (the relay publishes here)
	//   - INFERENCE binds fp.inference.> (the consumer reads fp.inference.completed)
	//   - the DLQ stream binds fp.dlq.>  (poison events from the consumer land here;
	//     without it the subscriber's DLQ publishes would vanish — see subscriber.go)
	if err := ensureStreams(bootCtx, js); err != nil {
		_ = rdb.Close()
		pgPool.Close()
		_ = natsConn.Drain()
		logger.Error("failed to ensure JetStream streams", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("jetstream streams ensured",
		slog.String("streams", "BILLING, INFERENCE, MODELS, AI, DLQ"))

	// ================================================================
	// 5. BUILD ADAPTERS → DOMAIN SERVICE → REAL HANDLER (real svc, NOT nil)
	// ================================================================
	// The Store wraps the shared pool and hands out the three persistence adapters
	// that satisfy the domain's ports. NewWithPool (not New) because main.go OWNS the
	// pool's lifecycle — the relay's PgxOutboxReader reads the SAME pool, and ordered
	// shutdown closes it once at the end. A Store built with NewWithPool does NOT own
	// the pool, so it has no Close() to call — exactly right when the pool is shared.
	store := postgres.NewWithPool(pgPool)

	// The pure purity-seam adapters: production UUIDv4 ids + the real UTC wall clock.
	// Injected (not called inline in the domain) so the domain's tests stay
	// deterministic; production wires the real implementations here.
	svc := domain.NewBillingService(
		store.Usage(),           // domain.UsageStore    (ledger + outbox commit boundary)
		store.RatePlans(),       // domain.RatePlanStore (pricing + team→plan resolution)
		store.Invoices(),        // domain.InvoiceStore  (invoices + aggregation + outbox)
		postgres.UUIDProvider{}, // domain.IDProvider
		postgres.SystemClock{},  // domain.Clock
	)

	// ================================================================
	// 6. BUILD gRPC SERVER + REGISTER THE WIRED HANDLER
	// ================================================================
	// AUTHENTICATION (the interceptor we wire here) vs AUTHORIZATION (the handler):
	//
	//   - authN — WHO ARE YOU. The auth UnaryInterceptor (added by WithAuthValidator)
	//     runs BEFORE every RPC: it reads the bearer JWT, has the validator verify the
	//     HS256 signature LOCALLY (design D2 — no hop to the auth service), and injects
	//     the verified *grpcutil.Claims into the request context. A missing/invalid
	//     token is rejected with Unauthenticated before the handler is ever entered.
	//   - authZ — MAY YOU DO THIS. The handler then makes the per-RPC permission
	//     decisions off those claims: scoping reads to the caller's own team (tenant
	//     isolation on GetUsage/GetInvoice/ListInvoices/GetRatePlan/CheckQuota) and the
	//     admin gate on CreateRatePlan. The interceptor does not authorize; the handler
	//     does not authenticate. This is the same division of labor as the auth service.
	//
	// THE VALIDATOR (fpauth.NewJWTValidator): local HS256 verification with the shared
	// FP_JWT_SECRET — the auth service SIGNS with this key, every other service VERIFIES
	// with it. NewJWTValidator fails fast if the secret is < 32 bytes (RFC 7518 §3.2), so
	// a misconfigured-weak secret aborts boot here, not on the first forged token.
	validator, err := fpauth.NewJWTValidator([]byte(cfg.JWTSecret))
	if err != nil {
		logger.Error("failed to build JWT validator", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// publicMethods — the BillingService RPCs that BYPASS authentication. It is
	// EMPTY by deliberate design: there is no genuinely-public business RPC on
	// billing. EVERY RPC operates on a team's money/usage and reads the caller's
	// team/role from claims for tenant isolation (the proto's "non-admins are scoped
	// to their own team server-side" contract) — so every one requires a valid token.
	// This is DEFAULT-DENY: a new RPC added to the proto is authenticated unless it is
	// consciously listed here, the secure default. Only the gRPC health + reflection
	// infrastructure RPCs are exempt (appended below) — the kubelet probe and grpcurl
	// call those with no credential, so gating them would make the pod fail its own
	// readiness probe / break dev introspection.
	var publicMethods []string

	// WithReflection lets grpcurl/grpcui introspect the service during development.
	srv := grpcutil.NewServer(
		grpcutil.WithLogger(logger),
		grpcutil.WithAuthValidator(validator, append(publicMethods, grpcutil.HealthAndReflectionMethods()...)...),
		grpcutil.WithReflection(),
	)
	logger.Info("auth interceptor wired (local JWT verify, design D2)",
		slog.Int("public_business_methods", len(publicMethods)),
		slog.Int("infra_skip_methods", len(grpcutil.HealthAndReflectionMethods())),
	)

	// The composition is complete: hand the fully-wired domain service to the
	// handler. The handler holds the BillingService INTERFACE — it has no idea the
	// impl is Postgres-backed with a NATS outbox. This single line is the difference
	// between the scaffold (NewBillingHandler(nil) → every RPC Unimplemented) and a
	// working service.
	billingv1.RegisterBillingServiceServer(srv.GRPC, handler.NewBillingHandler(svc))
	logger.Info("billing handler registered with wired service")

	// ================================================================
	// 7. START THE EVENT MACHINERY — OUTBOX RELAY (producer) + CONSUMER (subscriber)
	// ================================================================

	// --- PRODUCER: the outbox relay (DB→NATS half of the transactional outbox) ---
	// RelayPublisher publishes with a CALLER-SUPPLIED envelope id (the outbox row id)
	// so a republish (crash before mark) carries the SAME id and the consumer dedupes
	// it. PgxOutboxReader claims/marks rows over the SAME pool the repository wrote.
	// NewOutboxRelay with a zero RelayConfig{} uses the platform defaults (1s poll,
	// batch 100 — see relay.go). go relay.Run(ctx) is the long-lived drain loop; it
	// returns ctx.Err() when the signal ctx is cancelled (clean shutdown), self-heals
	// on transient errors, and is never fatal — a billing relay must not die.
	relayPub := events.NewRelayPublisher(js, events.Source) // source = "billing"
	outboxReader := events.NewPgxOutboxReader(pgPool)
	relay := events.NewOutboxRelay(outboxReader, relayPub, events.RelayConfig{})
	go func() {
		if runErr := relay.Run(ctx); runErr != nil && !errors.Is(runErr, context.Canceled) {
			// Run only returns a non-nil error on ctx cancellation (ctx.Err()); a
			// non-Canceled error here would be unexpected. Log it; the process is
			// already shutting down (ctx is cancelled) so we do not exit.
			logger.Error("outbox relay stopped", slog.String("error", runErr.Error()))
		}
	}()
	logger.Info("outbox relay started (DB→NATS publisher)")

	// --- CONSUMER: meter usage by reacting to fp.inference.completed ---
	// The consumer needs three injected ports:
	//   - the domain BillingService (whose RecordUsage meters + writes the outbox),
	//   - a TeamResolver (api_key_id → SERVER-AUTHORITATIVE billed team), and
	//   - a ProcessedStore (consumer-side dedup over at-least-once redelivery).
	//
	// ProcessedStore: a Redis SET-with-TTL store (durable + shared across replicas,
	// unlike natsutil.MemoryProcessedStore which is dev-only and OOMs). The DURABLE
	// backstop for double-metering is the domain's own (team, idempotency_key)
	// uniqueness; this Redis layer just makes the common duplicate cheap (no DB
	// round-trip). See natsutil/idempotency.go's exactly-once caveat.
	processed := newRedisProcessedStore(rdb)

	// Consumer handles declared in the outer scope so the ordered shutdown() closure
	// can Close() them. They stay nil when metering is disabled (consumers never
	// registered) and the shutdown nil-guards on that.
	var inferenceConsumer *events.InferenceConsumer
	var storageConsumer *events.StorageConsumer
	var aiConsumer *events.AIConsumer

	// TeamResolver: resolving api_key_id → owning team is an AUTH/identity concern
	// billing does not own; the production resolver is an Auth gRPC client (a later
	// wiring phase). Until that client is wired, we use a FAIL-CLOSED resolver: it
	// returns a TRANSIENT error so an inference event NAKs and is retried (NOT
	// poisoned/DLQ'd, and NEVER mis-attributed to a wrong team). Billing a wrong
	// account is the one outcome a money service must never produce, so the safe
	// default while the resolver is pending is "retry, don't guess". The consumer is
	// otherwise fully wired; flipping in the real resolver is a one-line change here.
	//
	// ⚠️  OBSERVABILITY OF THE NO-OP STATE (this finding's fix). "Fail-closed +
	// retry" is the correct SAFETY posture, but as shipped it makes billing a SILENT
	// no-op: every inference (and every storage) event NAKs MaxRetries+1 times and is
	// dead-lettered to fp.dlq.billing, so the service meters ZERO usage while the DLQ
	// fills with 100% of traffic — an OUTAGE that is invisible without a signal. We
	// make it LOUD and MEASURABLE here:
	//   - a startup WARN names the exact condition and its blast radius, and
	//   - a Prometheus counter (billing_unwired_team_resolver_active, exported via the
	//     OTel meter → OTLP → Prometheus) is incremented per affected axis so an alert
	//     can fire on "resolver unwired in a running billing pod" and a dashboard shows
	//     the degraded state. When the real resolver lands, this metric goes to 0 and
	//     the DLQ'd events are REPLAYED (see the runbook in unwiredTeamResolver's doc).
	teams := unwiredTeamResolver{}
	versionTeams := unwiredVersionTeamResolver{}
	markUnwiredResolverActive(ctx, logger, cfg.UnwiredResolverFailClosed)

	// FEATURE FLAG / KILL-SWITCH for the all-DLQ state. With the resolver unwired,
	// registering the consumers guarantees every event dead-letters. FP_BILLING_
	// METERING_ENABLED lets an operator DISABLE the consumers entirely (default: true,
	// preserving the prior behaviour) so a deployment can run billing's gRPC/read APIs
	// and the outbox relay WITHOUT flooding the DLQ with un-meterable events while the
	// resolver is pending. When disabled we emit a clear WARN instead of registering.
	if cfg.MeteringEnabled {
		consumer := events.NewInferenceConsumer(js, svc, teams, processed, events.SubConfig{})
		if err := consumer.Register(ctx); err != nil {
			_ = rdb.Close()
			pgPool.Close()
			_ = natsConn.Drain()
			logger.Error("failed to register inference consumer", slog.String("error", err.Error()))
			os.Exit(1)
		}
		inferenceConsumer = consumer
		logger.Info("inference consumer registered (meters fp.inference.completed → INFERENCE_REQUEST/TOKENS)")

		// STORAGE CONSUMER: meter STORAGE_BYTES by reacting to fp.models.version.ready.
		// This is the second money axis the event contract requires billing to consume
		// (alongside inference). Same fail-closed resolver posture: until the registry/
		// auth resolver is wired, storage events NAK + retry, then DLQ — counted by the
		// same unwired-resolver metric above. Wiring the real VersionTeamResolver is the
		// one-line change here, mirroring the inference path.
		storage := events.NewStorageConsumer(js, svc, versionTeams, processed, events.SubConfig{})
		if err := storage.Register(ctx); err != nil {
			_ = rdb.Close()
			pgPool.Close()
			_ = natsConn.Drain()
			logger.Error("failed to register storage consumer", slog.String("error", err.Error()))
			os.Exit(1)
		}
		storageConsumer = storage
		logger.Info("storage consumer registered (meters fp.models.version.ready → STORAGE_BYTES)")

		// AI CONSUMER: meter AI tokens by reacting to fp.ai.completion.served — the THIRD
		// money axis (M7/L2). UNLIKE the inference/storage consumers, it needs NO
		// TeamResolver: the AI Gateway is a trusted platform service that ALREADY resolved
		// the team server-side (from the caller's api key) and stamps it on the event, so
		// the consumer meters that team directly (see ai_consumer.go's "TEAM ATTRIBUTION").
		// That is why, even while the inference/storage resolvers are the fail-closed
		// placeholders above (every inference/storage event DLQs), the AI axis meters
		// NORMALLY — its team is on the wire, not behind an unwired resolver. We still gate
		// it on FP_BILLING_METERING_ENABLED so the one kill-switch disables ALL consumers.
		ai := events.NewAIConsumer(js, svc, processed, events.SubConfig{})
		if err := ai.Register(ctx); err != nil {
			_ = rdb.Close()
			pgPool.Close()
			_ = natsConn.Drain()
			logger.Error("failed to register AI consumer", slog.String("error", err.Error()))
			os.Exit(1)
		}
		aiConsumer = ai
		logger.Info("AI consumer registered (meters fp.ai.completion.served → INFERENCE_TOKENS)")
	} else {
		logger.Warn("BILLING METERING DISABLED (FP_BILLING_METERING_ENABLED=false): "+
			"the inference + storage consumers are NOT registered; billing meters ZERO usage. "+
			"Use this only while the team resolver is unwired, to avoid flooding fp.dlq.billing. "+
			"The outbox relay and gRPC/read APIs remain active.",
			slog.Bool("metering_enabled", false))
	}

	// ================================================================
	// 8. HEALTH SERVER (HTTP /healthz liveness + /readyz readiness over real deps)
	// ================================================================
	// WHY a separate HTTP port from gRPC: gRPC is HTTP/2, kubelet probes are
	// HTTP/1.1 — they can't share a net.Listener. Separate ports also let /readyz
	// flip to 503 (drain) while gRPC finishes in-flight calls on shutdown.
	//
	// READINESS over the REAL dependencies: if Postgres/Redis/NATS goes away,
	// /readyz → 503, K8s pulls the pod from the Service endpoints (no traffic)
	// WITHOUT restarting it (liveness stays green) — the correct posture for a
	// transient dependency outage (see pkg/health).
	healthHandler := health.New()
	healthHandler.AddCheck("postgres", func(ctx context.Context) error {
		return pgPool.Ping(ctx)
	})
	healthHandler.AddCheck("redis", func(ctx context.Context) error {
		return rdb.Ping(ctx).Err()
	})
	healthHandler.AddCheck("nats", func(_ context.Context) error {
		// IsConnected reflects the live transport state (the client auto-reconnects
		// in the background; this reports whether it currently HAS a connection).
		// Cheaper and more accurate than a round-trip for a transport liveness check.
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

	// shutdown runs cleanup in the CORRECT order, ONCE, after Serve returns. The
	// order below is load-bearing — see the per-step rationale.
	shutdown := func() {
		// Fresh, bounded context — the signal ctx is already cancelled by now, so
		// anything that honors it would return ctx.Canceled immediately. K8s gives
		// terminationGracePeriodSeconds (30s default); this stays well within it.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		// (a) Stop accepting health probes. By now gRPC has already drained (Serve
		//     returned) and SetServing(false) flipped readiness, so the LB has long
		//     since stopped routing here; closing the HTTP server releases the port.
		if err := healthServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("health server shutdown error", slog.String("error", err.Error()))
		}

		// (b) STOP THE CONSUMERS first (before draining NATS): Close() stops each
		//     consume loop so no new inference/storage event starts metering while we
		//     tear down. Both handles are nil when metering is disabled (the consumers
		//     were never registered) — Close() is a no-op on a nil-sub consumer, but we
		//     nil-guard anyway since the variables themselves may be nil. The outbox
		//     RELAY is stopped by the cancelled signal ctx (its Run loop returns on
		//     ctx.Done) — no separate handle; by the time we reach here ctx is cancelled
		//     and the relay's last batch has finished or will not start. Any outbox rows
		//     it didn't publish stay durable in Postgres and are drained on the next
		//     process start (at-least-once → no lost billing event).
		if inferenceConsumer != nil {
			inferenceConsumer.Close()
		}
		if storageConsumer != nil {
			storageConsumer.Close()
		}
		if aiConsumer != nil {
			aiConsumer.Close()
		}

		// (c) DRAIN NATS (not Close): Drain flushes any buffered publishes (the
		//     relay's in-flight UsageRecorded events) and lets in-flight consume
		//     messages complete before tearing down the connection — so the last
		//     events a draining batch emitted are not lost. Close would discard them.
		if err := natsConn.Drain(); err != nil {
			logger.Error("nats drain error", slog.String("error", err.Error()))
		}

		// (d) Close the datastore handles. Safe now because gRPC has drained and the
		//     consumer/relay are stopped: nothing is still mid-query holding a
		//     borrowed connection. Closing earlier could fail an in-flight RecordUsage.
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
	// 9. START gRPC SERVER (mark SERVING, listen, block until signal)
	// ================================================================
	// SetServing(true) flips the gRPC health service to SERVING only NOW — after
	// every dependency connected, the handler is wired, and the relay/consumer are
	// running — so a gRPC readiness probe is honest (green means actually-serviceable).
	// Serve flips it back to NOT_SERVING on SIGTERM before draining.
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
	// 10. GRACEFUL SHUTDOWN (ordered: gRPC drained → health → consumer → relay(ctx) → NATS → stores → otel)
	// ================================================================
	shutdown()
	logger.Info("billing service stopped cleanly")
}

// ensureStreams reconciles the three JetStream streams billing interacts with.
// CreateOrUpdateStream is idempotent + convergent: a stream that infra-as-code
// already created is reconciled to this config rather than erroring. We name the
// subject bindings exactly as the events package constants document them.
func ensureStreams(ctx context.Context, js jetstream.JetStream) error {
	specs := []jetstream.StreamConfig{
		// The relay PUBLISHES fp.billing.> here (usage.recorded / quota.exceeded /
		// invoice.generated).
		{Name: events.StreamBilling, Subjects: []string{"fp.billing.>"}},
		// The inference consumer READS fp.inference.completed from here. In production
		// this stream is owned by the inference-gateway's bootstrap; we reconcile it so
		// a local stack works standalone.
		{Name: events.StreamInference, Subjects: []string{"fp.inference.>"}},
		// The storage consumer READS fp.models.version.ready from here. In production
		// the MODELS stream is owned by the registry's bootstrap (and shared by serving,
		// experiment-tracker, …); we reconcile it idempotently so a local stack works
		// standalone. Without this stream, billing's storage-metering consumer could not
		// attach and STORAGE_BYTES would never be metered.
		{Name: events.StreamModels, Subjects: []string{"fp.models.>"}},
		// The AI consumer READS fp.ai.completion.served from here. In production the AI
		// stream is OWNED by the ai-gateway's bootstrap (it produces into it); we
		// reconcile it idempotently so a billing-FIRST boot can attach the consumer even
		// if the gateway hasn't booted yet (graceful degrade).
		//
		// CRITICAL: the subject list MUST match the gateway's StreamAI definition
		// EXACTLY — the gateway binds StreamAI to the single completion subject (NOT
		// fp.ai.>, which it deliberately avoids so the cost log and the KEDA warm-signal
		// stream AI_REQUESTS=fp.ai.warm.requested don't collide). If billing declares
		// fp.ai.> here it OVERLAPS the gateway's existing AI + AI_REQUESTS streams and
		// JetStream rejects it (err_code=10065 subjects overlap) — a fatal ensureStreams
		// crash. Declaring the IDENTICAL subject makes the two ensure calls idempotent:
		// whoever boots first creates it, the other sees the same config.
		{Name: events.StreamAI, Subjects: []string{events.SubjectAICompletionServed}},
		// The DLQ stream captures poison events the consumers dead-letter
		// (fp.dlq.billing, default in SubConfig). Without a stream binding fp.dlq.>,
		// the subscriber's DLQ publish would vanish (core-NATS drop) and a poison
		// event would be lost instead of parked for inspection.
		{Name: "DLQ", Subjects: []string{"fp.dlq.>"}},
	}
	for _, spec := range specs {
		if _, err := js.CreateOrUpdateStream(ctx, spec); err != nil {
			return fmt.Errorf("ensure stream %s: %w", spec.Name, err)
		}
	}
	return nil
}

// ============================================================================
// redisProcessedStore — a durable, shared natsutil.ProcessedStore over Redis.
// ============================================================================
//
// WHY here (in main, not a billing repository package): billing has no Redis
// adapter package of its own (its persistence is Postgres-only), and the only
// thing it needs Redis for is this consumer-dedup ProcessedStore. It is a tiny
// SET-NX-with-TTL over the shared client, so wiring it inline at the composition
// root keeps the dependency where it is used without inventing a package.
//
// SEMANTICS (matching natsutil.ProcessedStore's contract):
//   - IsProcessed: GET the key; present ⇒ already handled (skip the handler).
//   - MarkProcessed: SET the key with a TTL (recorded AFTER the handler succeeds).
//
// The key is namespaced (fp:billing:processed:) so it cannot collide with any
// other Forgepoint Redis user sharing the instance. The TTL bounds the dedup
// window (the durable floor is the domain's idempotency-key uniqueness — see the
// idempotencyTTL note above).
type redisProcessedStore struct {
	rdb *goredis.Client
}

const processedKeyPrefix = "fp:billing:processed:"

func newRedisProcessedStore(rdb *goredis.Client) *redisProcessedStore {
	return &redisProcessedStore{rdb: rdb}
}

// Compile-time proof the store satisfies the port the subscriber expects.
var _ natsutil.ProcessedStore = (*redisProcessedStore)(nil)

// IsProcessed reports whether eventID was already handled. A miss (redis.Nil) is
// the NORMAL first-delivery path and is NOT an error — return (false, nil).
func (s *redisProcessedStore) IsProcessed(ctx context.Context, eventID string) (bool, error) {
	err := s.rdb.Get(ctx, processedKeyPrefix+eventID).Err()
	if err == nil {
		return true, nil
	}
	if errors.Is(err, goredis.Nil) {
		return false, nil
	}
	return false, fmt.Errorf("billing: idempotency IsProcessed: %w", err)
}

// MarkProcessed records eventID as handled with a bounded TTL. Called AFTER the
// handler succeeds (the subscriber records-then-acks), so last-write-wins is fine.
func (s *redisProcessedStore) MarkProcessed(ctx context.Context, eventID string) error {
	if err := s.rdb.Set(ctx, processedKeyPrefix+eventID, "1", idempotencyTTL).Err(); err != nil {
		return fmt.Errorf("billing: idempotency MarkProcessed: %w", err)
	}
	return nil
}

// ============================================================================
// unwiredTeamResolver — the FAIL-CLOSED placeholder for api_key_id → team.
// ============================================================================
//
// The production resolver is an Auth-service gRPC client (GetAPIKey → owning
// team), wired in a later phase. Until it lands, this resolver returns a
// TRANSIENT error for every key so the consumer NAKs and retries the inference
// event rather than EITHER (a) poisoning it to the DLQ or (b) — far worse —
// guessing a team and billing the wrong account. For a money service "retry,
// don't guess" is the only safe default while the real resolver is pending.
//
// Note this is deliberately NOT events.ErrTeamNotFound: that sentinel means
// "permanent, unknown key → poison/DLQ", which we do NOT want here. A plain error
// is classified by the consumer as transient (NAK + retry). When the Auth client
// is wired, replace this single value with the real resolver — the consumer wiring
// above is unchanged.
//
// ───────────────────────── DLQ DRAIN / REPLAY RUNBOOK ─────────────────────────
// While this placeholder is active EVERY inference (and, via its storage twin,
// every model-version-ready) event NAKs MaxRetries+1 times and is dead-lettered to
// fp.dlq.billing. Those events are NOT lost — they are PARKED on the DLQ stream and
// must be REPLAYED once the real resolver lands, or the usage they represent is
// never billed. Procedure:
//   1. WATCH the signal: alert on billing_unwired_team_resolver_active > 0 (the
//      counter incremented at startup below) AND on the depth/rate of fp.dlq.billing.
//      Both being non-zero in a running pod = "billing is meter-dead, DLQ filling".
//   2. WIRE the resolvers: replace unwiredTeamResolver / unwiredVersionTeamResolver
//      with the real Auth/registry gRPC clients, set FP_BILLING_UNWIRED_RESOLVER_
//      FAIL_CLOSED=false, deploy. The counter goes to 0 and new events meter
//      normally (the live path, no replay needed for traffic after this point).
//   3. REPLAY the parked DLQ: the DLQ stream retains the ORIGINAL EventEnvelopes
//      (same envelope ids). Re-publish each fp.dlq.billing message back onto its
//      source subject (fp.inference.completed / fp.models.version.ready). Idempotency
//      makes replay SAFE: the consumer-side ProcessedStore dedupes on envelope id
//      and domain.RecordUsage dedupes on (team, idempotency_key = request_id /
//      version_id+":storage"), so a message that somehow already metered is a no-op.
//   4. VERIFY: fp.dlq.billing drains to ~0 and the usage ledger now contains the
//      previously-DLQ'd records. (Tooling for step 3 — a `fp dlq replay` command — is
//      an M4 CLI concern; until then it is a manual `nats` CLI republish.)
// As a stop-gap BEFORE the resolver lands, set FP_BILLING_METERING_ENABLED=false so
// the consumers are not registered at all and the DLQ does not fill in the first
// place (the relay + read APIs still run).
type unwiredTeamResolver struct{}

var _ events.TeamResolver = unwiredTeamResolver{}

func (unwiredTeamResolver) ResolveTeam(_ context.Context, _ string) (string, error) {
	return "", errors.New("billing: team resolver not wired (Auth gRPC client pending) — event will retry")
}

// unwiredVersionTeamResolver is the FAIL-CLOSED placeholder for the STORAGE path's
// model → owning-team lookup, the exact twin of unwiredTeamResolver. The production
// resolver is a registry/auth gRPC client (model_id → owning team); until it lands
// this returns a TRANSIENT error so every fp.models.version.ready event NAKs +
// retries (then DLQs) rather than guessing a team and mis-billing storage. Same
// "retry, don't guess" money-safety rule; same one-line swap when the real resolver
// is wired; same DLQ-replay runbook above.
type unwiredVersionTeamResolver struct{}

var _ events.VersionTeamResolver = unwiredVersionTeamResolver{}

func (unwiredVersionTeamResolver) ResolveVersionTeam(_ context.Context, _, _ string) (string, error) {
	return "", errors.New("billing: version team resolver not wired (registry gRPC client pending) — event will retry")
}

// ============================================================================
// OBSERVABILITY OF THE UNWIRED-RESOLVER (all-DLQ) STATE
// ============================================================================
//
// The whole point of this finding's fix: the fail-closed resolvers make billing a
// SILENT no-op (every event DLQs, zero usage metered) unless we emit a signal. We
// emit TWO:
//   - a startup WARN (human-visible in logs the moment the pod boots), and
//   - a Prometheus counter, billing_unwired_team_resolver_active, exported via the
//     OTel meter the platform already wires (observability.Setup → OTLP → collector
//     → Prometheus scrape). A counter that increments at startup gives Prometheus a
//     non-zero series an alert can match: "any billing pod running with the resolver
//     unwired" → page. (We use a counter, not a gauge, because the existing
//     observability stack is OTLP-push of OTel instruments; a one-shot Add(1) at
//     boot yields a series whose presence/increase the alert keys on. A gauge would
//     be marginally cleaner but is an unnecessary refactor of the metric kind here.)
//
// markUnwiredResolverActive is the single call site that fires both signals. It is a
// no-op when failClosed is false (the real resolvers are wired) so a correctly-wired
// production deployment is quiet.
const unwiredResolverMetricName = "billing_unwired_team_resolver_active"

func markUnwiredResolverActive(ctx context.Context, logger *slog.Logger, failClosed bool) {
	if !failClosed {
		// Real resolvers wired — nothing to warn about, no metric to emit.
		return
	}

	logger.Warn("BILLING TEAM RESOLVERS UNWIRED (fail-closed placeholders active): "+
		"api_key→team and model→team both return a transient error, so EVERY "+
		"fp.inference.completed and fp.models.version.ready event will NAK, retry, then "+
		"dead-letter to fp.dlq.billing. Billing meters ZERO usage in this state and the "+
		"DLQ fills with 100% of traffic. Wire the Auth/registry gRPC resolvers (set "+
		"FP_BILLING_UNWIRED_RESOLVER_FAIL_CLOSED=false) and then DRAIN/REPLAY the DLQ — "+
		"see the runbook in unwiredTeamResolver's doc. To avoid filling the DLQ in the "+
		"meantime, set FP_BILLING_METERING_ENABLED=false.",
		slog.String("metric", unwiredResolverMetricName))

	// OTel counter → OTLP → Prometheus. We swallow instrument-construction errors:
	// telemetry must never abort billing's boot. The WARN above is the guaranteed
	// signal; the metric is the alertable one when the collector is up.
	meter := otel.Meter("billing")
	counter, err := meter.Int64Counter(
		unwiredResolverMetricName,
		metric.WithDescription("Set when billing is running with fail-closed (unwired) team resolvers; "+
			"every inference/storage event dead-letters and zero usage is metered."),
		metric.WithUnit("{pod}"),
	)
	if err != nil {
		logger.Warn("failed to create unwired-resolver metric (continuing; WARN log is the fallback signal)",
			slog.String("error", err.Error()))
		return
	}
	// One increment per active axis (inference + storage) so the series reflects how
	// many money axes are degraded, and is unambiguously > 0 for the alert.
	counter.Add(ctx, 2)
}
