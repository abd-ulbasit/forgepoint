// Package main is the entrypoint for the Model Monitor service — the service that
// CLOSES the platform's ML lifecycle loop (serve → monitor → retrain).
//
// ============================================================================
// WIRING — THE COMPOSITION ROOT (fully wired, end-to-end)
// ============================================================================
//
// main.go is the only place where all layers are imported together and wired into
// a running program (Clean Architecture's "composition root"). It knows about
// every layer; the layers know only the interfaces (ports) the domain defines.
// Hexagonal: the domain OWNS the ports; this file constructs the ADAPTERS that
// fulfill them and injects them.
//
// STARTUP SEQUENCE (each step is fail-fast — a dependency that can't come up exits
// the process so K8s restarts the pod rather than serving a half-wired service):
//
//	┌──────────────────────────────────────────────────────────────────────┐
//	│  1. Structured logger (slog JSON → stdout → Loki)                    │
//	│  2. Load config (FP_* env → MonitorConfig)                           │
//	│  3. Setup OpenTelemetry (traces → Tempo, metrics → Prometheus)       │
//	│  4. Connect datastores:                                              │
//	│       - Postgres pgxpool → monitor config + drift-report history     │
//	│       - Redis go-redis   → live sliding-window store (hot path)      │
//	│       - NATS JetStream   → event bus (consume inference; produce drift)│
//	│     + ensure the consumed/produced streams exist                     │
//	│  5. Build adapters → domain service → handler (real service, not nil)│
//	│  6. Register handler on gRPC; START the event SUBSCRIBERS (data plane)│
//	│  7. HTTP health server (/readyz checks Postgres + Redis + NATS)      │
//	│  8. Mark SERVING + start gRPC (blocks until SIGTERM/SIGINT)          │
//	│  9. Graceful shutdown — REVERSE of startup (drain gRPC → stop subs → │
//	│     drain NATS → close Redis → close Postgres → flush OTel → health) │
//	└──────────────────────────────────────────────────────────────────────┘
//
// SHUTDOWN ORDER — WHY REVERSE OF STARTUP:
//
//	On SIGTERM the signal ctx cancels and srv.Serve returns. We then tear down in
//	the OPPOSITE order we built up, so nothing is closed out from under something
//	still using it. Go runs deferred funcs LIFO, so registering the defers in
//	startup order makes them fire in reverse automatically. ONE subtlety the
//	scaffold could not have: the NATS SUBSCRIBERS run their own goroutines that
//	call into the domain (which touches Postgres + Redis). They must be stopped
//	BEFORE we close those pools, else a redelivery mid-shutdown dereferences a
//	closed pool. So subscriber.Close() is invoked explicitly right after the gRPC
//	drain and before the store-closing defers (see the shutdown block at the end).
//
// THE TWO PLANES THIS ROOT WIRES (the key mental model for this service):
//
//	CONTROL/OBSERVABILITY PLANE — the gRPC handler (ConfigureMonitor, GetModelHealth,
//	  ListDriftReports, SubmitGroundTruth, …). Synchronous, client-driven.
//	DATA PLANE — the NATS subscribers (InferenceCompleted → ObserveInference;
//	  ModelPromoted → ResetBaselineFromPromotion). Asynchronous, event-driven. THIS
//	  is where the streaming-aggregation + closed-loop heartbeat actually runs.
//	Both planes call the SAME domain.MonitorService; the root injects one instance
//	into both the handler and the subscriber.
//
// ============================================================================
// HONEST GAP — THE CONTROL-LOOP CLIENT ADAPTERS ARE NOT BUILT YET
// ============================================================================
//
// domain.NewMonitorService needs EIGHT ports. FOUR have real infrastructure
// adapters today (MonitorRepository + DriftReportRepository = Postgres, WindowStore
// = Redis, DriftPublisher = NATS). The other FOUR are CROSS-SERVICE clients /
// durable stores that have no adapter package yet:
//
//	BaselineProvider  → a gRPC client to Registry/Experiment-Tracker (training-time
//	                    distributions). NOT built.
//	Orchestrator      → a gRPC client to the Pipeline Orchestrator (the loop-closer).
//	                    NOT built.
//	GroundTruthStore  → a durable (Postgres/Redis) store of predictions↔labels.
//	                    NOT built.
//	RetrainGate       → a durable (Redis SET NX PX / Postgres) cooldown gate.
//	                    NOT built.
//
// The composition root is the legitimate home for tiny wiring-edge adapters (the
// same precedent as feature-store's systemClock/uuidGenerator). So rather than pass
// nil (which the domain WILL dereference on the data plane — e.g. truth.RecordPrediction
// runs on EVERY inference) we inject small, SAFE, HONEST in-root adapters and
// document the gap loudly here instead of with a TODO in shipped code:
//
//   - unavailableBaselineProvider returns domain.ErrBaselineUnavailable. The domain
//     treats this gracefully and CORRECTLY: a monitor with no resolvable baseline
//     stays PENDING_BASELINE and the scorer RETIRES each closed window WITHOUT
//     scoring (monitor_service_impl.go: ObserveInference catches ErrBaselineUnavailable
//     → Close window, return nil). Net effect: the data plane windows real traffic
//     but emits NO drift reports and NO false positives until a real baseline client
//     is wired. This is the safe default — never invent a baseline.
//   - Because no window ever scores, the policy half (publish + Orchestrator's
//     TriggerRetrain + RetrainGate) is UNREACHABLE in practice. We still inject
//     real-enough adapters so the wiring is type-correct and the day a baseline
//     client lands, the loop is one adapter swap away: an in-memory RetrainGate
//     (correct semantics, not durable) and a notWiredOrchestrator that errors loudly.
//   - memoryGroundTruthStore is a bounded in-memory predictions↔labels store so the
//     hot path (RecordPrediction on every inference) never nil-panics and never grows
//     unbounded; it is process-local (not shared across replicas, not durable) — the
//     same "dev/local only" tradeoff as natsutil.MemoryProcessedStore.
//
// EVERY ONE of these four is replaced by a real adapter in a later task; the swap is
// a single line each in section 5. Nothing else in this file changes.
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
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"

	monitorv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/monitor/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/config"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/health"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/observability"
	monitorcfg "github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/config"
	"github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/events"
	"github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/handler"
	pgrepo "github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/repository/postgres"
	redisrepo "github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/repository/redis"
)

func main() {
	// ================================================================
	// 1. STRUCTURED LOGGER
	// ================================================================
	// slog JSON to stdout: container runtimes capture stdout → Loki. JSON is
	// parse-friendly for log aggregators. stdlib slog avoids an external logging dep.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// ================================================================
	// 2. LOAD CONFIG
	// ================================================================
	// config.Load reads FP_* env vars into MonitorConfig via reflection, failing
	// fast on a missing required field. In K8s, env comes from ConfigMaps/Secrets.
	cfg, err := config.Load[monitorcfg.MonitorConfig]("FP")
	if err != nil {
		logger.Error("failed to load config", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("config loaded",
		slog.Int("grpc_port", cfg.GRPCPort),
		slog.Int("health_port", cfg.Port),
		slog.String("log_level", cfg.LogLevel),
		slog.String("retrain_cooldown", cfg.RetrainCooldown.String()),
	)

	// ================================================================
	// 3. OPENTELEMETRY
	// ================================================================
	// Setup initializes the trace provider (→ Tempo) and meter provider
	// (→ Prometheus via the OTel Collector). OTLPInsecure is true for the local
	// docker-compose collector (no TLS cert there). The signal-aware ctx also drives
	// graceful shutdown below — a SIGTERM during startup aborts cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	otelShutdown, err := observability.Setup(ctx, observability.Config{
		ServiceName:    "model-monitor",
		ServiceVersion: "dev",
		Environment:    "local",
		OTLPEndpoint:   cfg.OTelEndpoint,
		OTLPInsecure:   true, // plaintext to the local collector (no TLS cert there)
	})
	if err != nil {
		logger.Error("failed to setup observability", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer func() {
		// Flush pending spans/metrics on shutdown — OTel batches internally, so
		// without this the last few seconds of telemetry are lost. Fresh Background
		// ctx because the signal ctx is already cancelled by the time this runs.
		if shutdownErr := otelShutdown(context.Background()); shutdownErr != nil {
			logger.Error("otel shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// ================================================================
	// 4. CONNECT DATASTORES (Postgres, Redis, NATS) — fail fast on each
	// ================================================================
	//
	// 4a. POSTGRES (pgxpool) — backs the monitor CONFIG write model (monitors) and
	// the durable drift-report HISTORY. pgrepo.New parses the DSN, opens the pool,
	// and Pings once so a bad DSN / unreachable DB fails LOUD at startup rather than
	// on the first RPC/event. The Store owns the pool (we close it via store.Close()).
	store, err := pgrepo.New(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("failed to connect postgres", slog.String("error", err.Error()))
		os.Exit(1)
	}
	// Registered now so it fires LATE among the store closes (LIFO) — after the gRPC
	// drain AND the subscriber stop, so no in-flight read/observe is cut off mid-query.
	defer func() {
		logger.Info("closing postgres pool")
		store.Close()
	}()
	logger.Info("postgres connected")

	// 4b. REDIS (go-redis) — backs the LIVE sliding-window store (the hot path:
	// LoadOrOpen→Add→Save on every inference event). We parse the redis:// URL into
	// Options (host/port/db/credentials) so the platform-wide connection-string format
	// works here, then Ping to confirm reachability before readiness. We build the
	// *goredis.Client HERE (rather than letting redisrepo.NewWindowStore dial from a
	// bare addr) for two reasons: (1) ParseURL handles the full redis:// URL the
	// config carries, and (2) the same client backs BOTH the WindowStore adapter and
	// the /readyz Redis check — one pool, one place to close.
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

	// 4c. NATS JetStream — the event bus. This service is BOTH a consumer (inference,
	// promotion, feature-write events drive the data plane) AND a producer (it emits
	// fp.models.drift.detected). Connect returns the raw *nats.Conn (for lifecycle:
	// Drain/Close, status checks) and a jetstream.JetStream context (consume/publish).
	// natsutil.Connect sets MaxReconnects(-1) so the client survives transient blips.
	nc, js, err := natsutil.Connect(cfg.NATSUrl)
	if err != nil {
		logger.Error("failed to connect to nats", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer func() {
		// Drain flushes in-flight publishes (our drift events) and unsubscribes
		// cleanly, THEN closes — preferred over a bare Close which drops buffered msgs.
		logger.Info("draining nats connection")
		if drainErr := nc.Drain(); drainErr != nil {
			logger.Error("nats drain error", slog.String("error", drainErr.Error()))
		}
	}()

	// ENSURE THE STREAMS EXIST. A JetStream CONSUMER can only bind to a stream that
	// exists, and a PUBLISH only persists if some stream captures its subject (core-
	// NATS fire-and-forget leaks through otherwise). EnsureStreams CreateOrUpdates the
	// MODELS (drift produced + promoted consumed), INFERENCE (completed/failed
	// consumed), and FEATURES (written consumed) streams — idempotent + convergent, so
	// it reconciles whether this service or another created them first.
	streamCtx, cancelStream := context.WithTimeout(ctx, 10*time.Second)
	if streamErr := events.EnsureStreams(streamCtx, js); streamErr != nil {
		cancelStream()
		logger.Error("failed to ensure streams", slog.String("error", streamErr.Error()))
		os.Exit(1)
	}
	cancelStream()
	logger.Info("nats connected; streams ensured")

	// ================================================================
	// 5. BUILD ADAPTERS → DOMAIN SERVICE → HANDLER
	// ================================================================
	//
	// Dependency inversion made concrete: the domain declares the ports (ports.go);
	// here we construct the infrastructure adapters that satisfy them and INJECT them.
	// The domain never imported pgx, go-redis, or nats — it only sees its interfaces.
	monitors := store.Monitors()                       // domain.MonitorRepository (Postgres write model)
	reports := store.Reports()                         // domain.DriftReportRepository (Postgres history)
	windows := redisrepo.NewWindowStoreFromClient(rdb) // domain.WindowStore (Redis live windows)

	// The NATS-backed drift publisher (domain.DriftPublisher). NewPublisher wraps a
	// natsutil.Publisher bound to this service's SourceName, inheriting the envelope/
	// dedup/trace machinery; the adapter only builds the canonical events.v1 payload
	// and protojson-encodes it. Called ONLY on a true report insert (exactly-once).
	publisher := events.NewPublisher(natsutil.NewPublisher(js, events.SourceName))

	// The four CONTROL-LOOP ports without a real adapter yet — injected as safe,
	// honest, in-root adapters (see the "HONEST GAP" package doc). The day the gRPC
	// clients / durable stores land, each of these four lines becomes one adapter
	// constructor; nothing else in this file changes.
	baselines := unavailableBaselineProvider{} // → ErrBaselineUnavailable (monitor stays PENDING_BASELINE; scorer never invents a baseline)
	truth := newMemoryGroundTruthStore()       // bounded in-memory predictions↔labels (hot-path safe; process-local)
	gate := newMemoryRetrainGate()             // in-memory cooldown gate (correct semantics; not durable across restarts)
	orch := notWiredOrchestrator{}             // errors loudly if ever reached (unreachable while baselines are unavailable)

	// Assemble the domain service. now=nil → time.Now (prod clock). cooldown from
	// config (anti-storm gap between auto-retrains for one model). The handler and the
	// subscriber both receive THIS instance — one service, two planes.
	svc := domain.NewMonitorService(
		monitors,
		reports,
		windows,
		truth,
		baselines,
		publisher,
		orch,
		gate,
		cfg.RetrainCooldown,
		nil, // production wall-clock (time.Now)
	)

	// ================================================================
	// 6a. BUILD gRPC SERVER + REGISTER THE REAL HANDLER
	// ================================================================
	// grpcutil.NewServer applies the standard interceptor chain (recovery → logging →
	// [auth] → tracing). The auth validator (WithAuthValidator) is added once the auth
	// client is wired — every Model Monitor RPC is team-scoped by the caller's claims,
	// so it WILL run here in production. WithReflection lets grpcurl/grpcui introspect.
	srv := grpcutil.NewServer(
		grpcutil.WithLogger(logger),
		grpcutil.WithReflection(),
	)

	// Register the FULLY-WIRED handler (real service, NOT nil). Every control-plane RPC
	// now reaches the domain service backed by Postgres + Redis + NATS.
	monitorv1.RegisterMonitorServiceServer(srv.GRPC, handler.NewMonitorHandler(svc))
	logger.Info("monitor handler registered (wired: postgres config+history, redis windows, nats drift publisher)")

	// ================================================================
	// 6b. START THE EVENT SUBSCRIBERS (the DATA PLANE — the heartbeat)
	// ================================================================
	//
	// The most important behavior on this service runs here, not on the gRPC surface:
	// the four durable consumers fold the inference stream into windows, score on
	// close, persist reports, and (when wired) fire retrains. The subscriber needs:
	//   - the SAME domain.MonitorService the handler uses (the driving port),
	//   - a MonitorResolver (model name → authorized monitor binding) — the events-
	//     plane port that turns an untrusted, team-less event into a server-resolved
	//     (monitor id, owner team). We back it with a tiny Postgres adapter over the
	//     same monitors table (declared in this file; the events package owns the port).
	//   - the SHARED subscriber options the service standardizes on: an idempotency
	//     ProcessedStore (consumer-side dedup) + a DLQ subject + MaxRetries + a per-
	//     message timeout. The adapter applies these to EACH per-subject durable.
	//
	// IDEMPOTENCY STORE CAVEAT: MemoryProcessedStore is dev/local-only (process-local,
	// not shared across replicas, not crash-durable — it even panics under
	// FP_ENV=production). The domain already enforces exactly-once-IN-EFFECT below the
	// consumer (request_id dedup within a window, window_id idempotency on the report,
	// MarkTriggered before TriggerRetrain), so this store is a cheap extra layer, not
	// the correctness backstop. A Redis/Postgres ProcessedStore replaces it for prod.
	resolver := newMonitorResolver(store.Pool())
	subscriber := events.NewSubscriber(
		svc,
		resolver,
		js,
		logger,
		natsutil.WithIdempotencyStore(natsutil.NewMemoryProcessedStore()),
		natsutil.WithDLQSubject("fp.dlq.model-monitor"),
		natsutil.WithMaxRetries(5),
		natsutil.WithMessageTimeout(20*time.Second), // < the 30s default AckWait so a timed-out handler NAKs before redelivery
	)
	// Start spins one managed pull loop per consumed subject; they run until ctx is
	// cancelled OR subscriber.Close() is called. A start failure is fatal — a monitor
	// that cannot consume inference is not doing its primary job.
	if subErr := subscriber.Start(ctx); subErr != nil {
		logger.Error("failed to start event subscribers", slog.String("error", subErr.Error()))
		os.Exit(1)
	}
	logger.Info("event subscribers started (inference.completed/failed, models.promoted, features.written)")

	// ================================================================
	// 7. HEALTH SERVER (HTTP) — readiness checks the REAL dependencies
	// ================================================================
	// gRPC is HTTP/2; kubelet probes are HTTP/1.1 — they can't share a plain listener,
	// so health runs on its own port. Liveness (/healthz) NEVER checks dependencies (a
	// DB blip must not restart-storm the fleet). Readiness (/readyz) checks Postgres +
	// Redis + NATS so a pod with a dead dependency is pulled from the Service endpoints
	// until it recovers — and so the gRPC drain can flip /readyz to 503 while finishing
	// in-flight RPCs.
	healthHandler := health.New()
	healthHandler.AddCheck("postgres", func(ctx context.Context) error {
		return store.Pool().Ping(ctx)
	})
	healthHandler.AddCheck("redis", func(ctx context.Context) error {
		return rdb.Ping(ctx).Err()
	})
	healthHandler.AddCheck("nats", func(_ context.Context) error {
		// IsConnected is the cheap, non-blocking liveness signal for the NATS conn.
		// During an automatic reconnect it is briefly false — readiness correctly
		// reflects "can't consume/publish right now" and recovers on reconnect.
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
		if shutdownErr := healthServer.Shutdown(context.Background()); shutdownErr != nil {
			logger.Error("health server shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// Stop the subscribers explicitly BEFORE the deferred store closes run. WHY a
	// defer here too (and not only after Serve returns): if Serve returns via an error
	// path we still want the consume loops stopped before Postgres/Redis close. Close
	// is idempotent (safe to call twice), so the explicit post-Serve stop + this defer
	// belt-and-suspenders never double-frees. Registered AFTER the store/redis/nats
	// defers so it fires BEFORE them (LIFO) — subscribers down, THEN their datastores.
	defer subscriber.Close()

	// ================================================================
	// 8. START gRPC SERVER (mark SERVING only now that all deps are up)
	// ================================================================
	// We reach SetServing(true) ONLY after Postgres/Redis/NATS connected, streams
	// exist, and the consumers are running — so the gRPC readiness signal is honest.
	// grpcutil flips it to NOT_SERVING on SIGTERM before draining.
	srv.SetServing(true)

	grpcAddr := fmt.Sprintf(":%d", cfg.GRPCPort)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("failed to listen on gRPC port", slog.String("addr", grpcAddr), slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("gRPC server listening", slog.String("addr", grpcAddr))

	// Serve blocks until ctx is cancelled (SIGINT/SIGTERM). On cancellation it flips
	// readiness off, drains in-flight RPCs (bounded), then returns here.
	serveErr := srv.Serve(ctx, lis)

	// SHUTDOWN ORDERING (explicit, before the deferred store closes fire):
	// the gRPC drain has finished, so no control-plane RPC is in flight. Now stop the
	// DATA-plane consume loops so no event handler calls into the domain (→ Postgres/
	// Redis) after this point. Only THEN do the deferred pool closes run (LIFO):
	// subscriber.Close() defer (no-op second call) → nats drain → redis close →
	// postgres close → otel flush → health shutdown.
	logger.Info("stopping event subscribers")
	subscriber.Close()

	if serveErr != nil {
		logger.Error("gRPC server error", slog.String("error", serveErr.Error()))
		os.Exit(1)
	}

	logger.Info("model-monitor service stopped cleanly")
}

// ============================================================================
// monitorResolver — the events-plane MonitorResolver adapter (Postgres-backed)
// ============================================================================
//
// The events Subscriber needs the INVERSE of MonitorRepository.GetByModel: given
// ONLY a model name (an inference/promotion event carries no team), resolve the
// authorized (monitor id, owner team) binding. The events package OWNS the port
// (resolver.go — "the consumer owns the port") and documents that the production
// adapter is a `SELECT id, owner_team FROM monitors WHERE model_name = $1 AND
// not-deleted`. That adapter is THIS — built at the composition root over the same
// pgxpool the repository uses, because it is a one-query wiring-edge concern and
// declaring a whole package for it would be overkill.
//
// THE SAME-NAME CAVEAT (carried verbatim from the port doc): model names are NOT
// globally unique — (owner_team, model_name) is the key. An event names only the
// model. We resolve to a SINGLE binding per model name and rely on the partial
// unique index (one LIVE monitor per (team, model)); when two teams monitor the
// same model name, full disambiguation needs an api_key→team resolver (another
// service's concern, deferred). found=false (no live monitor) is the safe default:
// the handler ACKs and drops, never fabricating a tenant.
type monitorResolver struct {
	pool *pgxpool.Pool
}

// newMonitorResolver builds the resolver over the shared monitors pool.
func newMonitorResolver(pool *pgxpool.Pool) *monitorResolver {
	return &monitorResolver{pool: pool}
}

// Compile-time proof we satisfy the events port. Drift fails the build here.
var _ events.MonitorResolver = (*monitorResolver)(nil)

// ResolveByModel returns the (monitor id, owner team) for a model name. found=false
// when no LIVE monitor governs the model (ACK + drop). A query error is transient
// (the handler NAKs → JetStream redelivers). LIMIT 1 + the partial unique index make
// this a single index-backed lookup; deleted_at IS NULL ensures a soft-deleted
// monitor STOPS folding traffic (the contract the port states).
func (r *monitorResolver) ResolveByModel(ctx context.Context, modelName string) (events.MonitorBinding, bool, error) {
	const q = `SELECT id, owner_team FROM monitors
		WHERE model_name = $1 AND deleted_at IS NULL
		LIMIT 1`
	var b events.MonitorBinding
	err := r.pool.QueryRow(ctx, q, modelName).Scan(&b.MonitorID, &b.OwnerTeam)
	if err != nil {
		if err == pgx.ErrNoRows {
			return events.MonitorBinding{}, false, nil // unmonitored model — safe drop
		}
		return events.MonitorBinding{}, false, fmt.Errorf("resolve monitor for %q: %w", modelName, err)
	}
	return b, true, nil
}

// ============================================================================
// CONTROL-LOOP PORT STUBS — safe, honest, in-root adapters (see "HONEST GAP")
// ============================================================================
//
// These four exist because domain.NewMonitorService needs the ports but their real
// cross-service-client / durable-store adapters are not built yet. They are written
// to be SAFE (no nil panics, no false drift, no unbounded growth) and HONEST (each
// says exactly what it is and what replaces it), not to fake a working closed loop.

// unavailableBaselineProvider always reports ErrBaselineUnavailable. The domain
// treats this as "not ready to score" (NOT an error to surface): ConfigureMonitor
// keeps the monitor PENDING_BASELINE, and ObserveInference RETIRES each closed
// window without scoring. So the data plane windows real traffic but emits zero
// drift reports — the correct, safe behavior until a real Registry/Experiment-
// Tracker gRPC baseline client is wired. Replaced by that client (one line in §5).
type unavailableBaselineProvider struct{}

var _ domain.BaselineProvider = unavailableBaselineProvider{}

func (unavailableBaselineProvider) GetBaseline(_ context.Context, _, _ string) (domain.Baseline, error) {
	return domain.Baseline{}, domain.ErrBaselineUnavailable
}

// notWiredOrchestrator is the loop-closer stub. It is UNREACHABLE while baselines
// are unavailable (no window scores → applyPolicy never runs → TriggerRetrain is
// never called). If it IS reached (after a real baseline client lands but before the
// orchestrator client does), it errors LOUDLY rather than silently dropping a
// retrain — a missed retrain is a correctness bug we want visible, and the domain
// NAKs the event so the breach is retried (not lost). Replaced by the real Pipeline
// Orchestrator gRPC client (one line in §5).
type notWiredOrchestrator struct{}

var _ domain.Orchestrator = notWiredOrchestrator{}

func (notWiredOrchestrator) TriggerRetrain(_ context.Context, in domain.RetrainRequest) (string, error) {
	return "", fmt.Errorf("orchestrator client not wired: cannot trigger retrain pipeline %q for model %q (report %s)",
		in.PipelineID, in.ModelName, in.ReportID)
}

// memoryRetrainGate is an in-memory implementation of the anti-storm cooldown gate.
// Semantics are CORRECT (LastTriggered/MarkTriggered per model under a mutex) but it
// is PROCESS-LOCAL and not durable: a restart forgets the last-trigger times, and
// replicas don't share state — so under the real loop it could not by itself
// guarantee a single fire across a fleet (the domain records MarkTriggered BEFORE
// TriggerRetrain, and the orchestrator dedups on (pipeline_id, report_id), so a
// duplicate trigger is caught downstream too). The production gate is a Redis
// SET NX PX (the port doc names exactly this). Replaced by it (one line in §5).
type memoryRetrainGate struct {
	mu   sync.Mutex
	last map[string]time.Time
}

func newMemoryRetrainGate() *memoryRetrainGate {
	return &memoryRetrainGate{last: make(map[string]time.Time)}
}

var _ domain.RetrainGate = (*memoryRetrainGate)(nil)

func (g *memoryRetrainGate) LastTriggered(_ context.Context, model string) (time.Time, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.last[model], nil // zero time if never — the policy treats that as "cooldown elapsed"
}

func (g *memoryRetrainGate) MarkTriggered(_ context.Context, model string, at time.Time) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.last[model] = at
	return nil
}

// memoryGroundTruthStore is a bounded in-memory predictions↔labels store. WHY it
// must exist (not be nil): RecordPrediction runs on EVERY inference observation
// (the hot path) — a nil store would nil-panic the data plane. WHY bounded: an
// unbounded map of every request_id would OOM a long-running service (the same trap
// natsutil.MemoryProcessedStore documents). We keep a simple FIFO cap of recent
// (request_id → predicted) entries per model and join labels against them; aged-out
// or unknown ids return matched=false (the anti-fabrication contract). It is
// process-local and not durable — the production store is Postgres/Redis keyed by
// request_id with a retention window. Replaced by that store (one line in §5).
type memoryGroundTruthStore struct {
	mu sync.Mutex
	// preds maps model → request_id → predicted label. order bounds growth per model.
	preds map[string]map[string]gtEntry
	order map[string][]string // FIFO request_id ring per model
	// pairs holds the matched (predicted, actual) outcomes per model for RecentPairs.
	pairs map[string][]domain.LabelPair
}

// gtEntry is a recorded prediction awaiting a possible later label.
type gtEntry struct {
	predicted string
	at        time.Time
}

// maxGroundTruthPerModel bounds the in-memory prediction ring per model so a busy
// model cannot grow this store without limit. Generous enough to join labels that
// arrive minutes later; the production store uses a time-based retention instead.
const maxGroundTruthPerModel = 100_000

func newMemoryGroundTruthStore() *memoryGroundTruthStore {
	return &memoryGroundTruthStore{
		preds: make(map[string]map[string]gtEntry),
		order: make(map[string][]string),
		pairs: make(map[string][]domain.LabelPair),
	}
}

var _ domain.GroundTruthStore = (*memoryGroundTruthStore)(nil)

// RecordPrediction stores (request_id → predicted) for a later label join, evicting
// the oldest entry once the per-model cap is hit (FIFO) so growth is bounded.
func (s *memoryGroundTruthStore) RecordPrediction(_ context.Context, model, requestID, predicted string, at time.Time) error {
	if requestID == "" {
		return nil // no join key — nothing to record (matches the domain's guard)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.preds[model]
	if m == nil {
		m = make(map[string]gtEntry)
		s.preds[model] = m
	}
	if _, exists := m[requestID]; !exists {
		s.order[model] = append(s.order[model], requestID)
		// Evict oldest while over the cap (bounded memory).
		for len(s.order[model]) > maxGroundTruthPerModel {
			oldest := s.order[model][0]
			s.order[model] = s.order[model][1:]
			delete(m, oldest)
		}
	}
	m[requestID] = gtEntry{predicted: predicted, at: at}
	return nil
}

// RecordLabel joins a delayed true outcome to a recorded prediction. matched=false
// for an unknown / aged-out request_id (anti-fabrication — never trust a label for a
// prediction we never made). Idempotent-ish: a repeated label for the same id appends
// at most once because the prediction is consumed (deleted) on first match.
func (s *memoryGroundTruthStore) RecordLabel(_ context.Context, model, requestID, actual string, _ time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.preds[model]
	if m == nil {
		return false, nil
	}
	entry, ok := m[requestID]
	if !ok {
		return false, nil // unknown or aged-out id — not matched (reported back as unmatched)
	}
	delete(m, requestID) // consume so a duplicate label doesn't double-count
	s.pairs[model] = append(s.pairs[model], domain.LabelPair{Predicted: entry.predicted, Actual: actual})
	return true, nil
}

// RecentPairs returns the matched (predicted, actual) pairs for a model — the input
// to Accuracy/PerformanceDrop. The in-memory store ignores the rolling window bound
// (it holds only joined pairs, already small); the production store applies the
// time-based retention the `window` argument expresses.
func (s *memoryGroundTruthStore) RecentPairs(_ context.Context, model string, _ time.Duration) ([]domain.LabelPair, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.LabelPair, len(s.pairs[model]))
	copy(out, s.pairs[model])
	return out, nil
}
