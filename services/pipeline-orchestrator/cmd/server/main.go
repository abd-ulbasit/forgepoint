// Package main is the entrypoint for the Pipeline Orchestrator service.
//
// ============================================================================
// THE COMPOSITION ROOT (fully wired: Postgres + NATS publisher + drift consumer)
// ============================================================================
//
// In Clean Architecture, main.go is the ONLY place that imports every layer
// (config, observability, repository adapters, the domain saga engine, the event
// adapters, the handler) and wires them into a running program. The layers know
// each other only through the interfaces the DOMAIN defines (PipelineRepository,
// ExecutionRepository, ExecutorRegistry, EventPublisher, Clock, IDGenerator) —
// this file is where each of those ports gets its concrete adapter, and nowhere
// else. It also owns the concrete implementations of the domain's small utility
// ports (Clock, IDGenerator) — see systemClock / uuidGenerator below — so the
// domain package itself stays free of even the uuid import (matching the Auth
// service's stdlib-only domain).
//
// THE DEPENDENCY-INJECTION GRAPH WE ASSEMBLE (inner depends on nothing outer):
//
//	pgxpool ─► postgres.Store ─► .Pipelines() (PipelineRepository) ─┐
//	                          └─ .Executions() (ExecutionRepository) ┤
//	                                                                  ├─► domain.NewPipelineService ─► handler.PipelineHandler ─► gRPC
//	jetstream ─► natsutil.Publisher ─► events.Publisher (EventPublisher) ─┤
//	            stepExecutorRegistry (ExecutorRegistry) ──────────────────┤
//	            systemClock{} (Clock) + uuidGenerator{} (IDGenerator) ─────┘
//	                                                                  │
//	jetstream ─► natsutil.Subscriber ─► events.DriftRetrainSubscriber ─┘ (drives the SAME svc inward → auto-retrain)
//
// STARTUP SEQUENCE (each step fail-fast — a bad dependency aborts boot, never serves):
//
//	┌──────────────────────────────────────────────────────────────────────────┐
//	│ 1. Structured logger (slog JSON → stdout → Loki)                           │
//	│ 2. Load config (FP_* env vars → PipelineConfig); require DatabaseURL       │
//	│ 3. Setup OpenTelemetry (traces → Tempo, metrics → Prometheus)              │
//	│ 4. Connect datastores (Postgres pool, NATS + JetStream) — each PINGS so an  │
//	│    unreachable dep fails boot, not RPC #1; provision the JetStream streams  │
//	│ 5. Build adapters → domain saga engine → REAL handler (NOT nil)            │
//	│ 6. Build gRPC server, register the WIRED handler                          │
//	│ 7. Start the drift-retrain SUBSCRIBER (closes serve→monitor→retrain)       │
//	│ 8. Start HTTP health server (/readyz checks Postgres + NATS live)          │
//	│ 9. Mark SERVING + start gRPC server (blocks until SIGTERM/SIGINT)          │
//	│ 10. Graceful shutdown in order (gRPC drained → health → subscriber → NATS  │
//	│     drain → pool close → OTel flush)                                       │
//	└──────────────────────────────────────────────────────────────────────────┘
//
// WHY THIS SERVICE WIRES NO REDIS (deliberate, not missing): the Pipeline
// Orchestrator's durability backbone is Postgres alone (the saga write-ahead log
// + idempotency tables). Its only adapters are the two Postgres repositories;
// there is no Redis port in the domain and no Redis adapter in internal/. The
// platform-wide wiring checklist mentions Redis generically, but adding a Redis
// client here would be both architecturally wrong (no port consumes it) and a new
// dependency this module does not carry. So Redis is intentionally absent.
//
// EVENT FLOW THIS SERVICE OWNS:
//   - PRODUCES fp.pipelines.* (PipelineStarted/StepCompleted/.../ModelDeployed) via
//     the events.Publisher adapter — best-effort lifecycle feed (a NATS hiccup
//     never fails a saga; durability is the Postgres write-ahead log).
//   - CONSUMES fp.models.drift.detected via the DriftRetrainSubscriber — the LAST
//     edge of the closed loop: a CRITICAL drift with auto_retrain armed triggers
//     the model's retrain pipeline with no human in the path.
//
// LEADER ELECTION NOTE (deployment, surfaced for interviews): the gRPC API can be
// served by many replicas, but only the ELECTED LEADER should DRIVE sagas (run
// steps, write checkpoints) so two pods never double-apply a deployment. Read RPCs
// (Get/List/Watch) are served by any replica from the shared DB. The drift
// consumer already forms a consumer GROUP (retrainGroup), so a drift event triggers
// a retrain on exactly ONE replica; full leader election for the synchronous gRPC
// TriggerExecution path is a later (M3) hardening step layered on the same durable
// store — it does not change this composition root.
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

	"github.com/nats-io/nats.go/jetstream"

	pipelinev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/pipeline/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/config"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/health"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/observability"
	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/events"
	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/handler"
	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/repository/postgres"
	"github.com/google/uuid"
)

// depConnectTimeout bounds how long we wait, AT BOOT, for each datastore's
// fail-fast ping / stream provisioning. WHY a bound: a constructor that blocks
// forever on an unreachable dependency would hang the pod in a not-ready state
// with no signal; a bounded ctx turns "DB is down" into a clear boot error K8s can
// act on (the pod CrashLoops and surfaces the cause). Kept short — if a core
// dependency isn't reachable in 10s at startup, failing fast is the right call.
const depConnectTimeout = 10 * time.Second

// shutdownTimeout bounds the post-Serve cleanup. K8s gives
// terminationGracePeriodSeconds (30s default) on SIGTERM; 15s leaves ample margin
// for the drain + flush while guaranteeing we never hang the pod's teardown.
const shutdownTimeout = 15 * time.Second

// PipelineConfig extends BaseConfig with this service's own configuration.
//
// WHY embed BaseConfig: every Forgepoint service needs Port, GRPCPort, LogLevel,
// OTelEndpoint, NATSUrl, DatabaseURL. config.Load[PipelineConfig]("FP") reads
// FP_PORT, FP_GRPC_PORT, FP_DATABASE_URL, etc. via reflection (see pkg/config).
type PipelineConfig struct {
	config.BaseConfig

	// CompensationTimeout bounds how long a single step's compensation may run
	// during a rollback before the engine declares it COMPENSATION_FAILED (a stuck
	// saga). Defaults to 60s — matching the engine's internal compensation budget.
	// Surfaced as config so an operator can tune it per environment.
	CompensationTimeout time.Duration `env:"COMPENSATION_TIMEOUT" default:"60s"`

	// OTelInsecure controls whether the OTLP exporter uses plaintext (no TLS). True
	// in local docker-compose (the collector has no cert) and false in production
	// (mTLS to the collector). Wiring it FROM CONFIG — rather than hard-coding true —
	// is the correct, env-driven 12-factor approach: the same binary is secure in
	// prod and convenient locally.
	OTelInsecure bool `env:"OTEL_INSECURE" default:"true"`
}

// systemClock is the production domain.Clock — real wall-clock time in UTC. It
// lives in the composition root (not the domain) so the domain package needs no
// time-source dependency beyond what its pure logic uses. Tests inject a fake.
type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

// uuidGenerator is the production domain.IDGenerator — crypto-random UUIDv4 for
// execution/step/pipeline ids. It lives HERE (the composition root) so the uuid
// import stays out of the domain package, keeping the domain's dependency
// footprint to the standard library. uuid.NewString uses crypto/rand, so ids are
// unguessable (no IDOR via predictable ids).
type uuidGenerator struct{}

func (uuidGenerator) NewID() string { return uuid.NewString() }

// stepExecutorRegistry is the production domain.ExecutorRegistry — the StepType →
// StepExecutor map the saga engine consults for each step. It lives in the
// composition root because WHICH executors exist is a wiring concern: the engine
// only knows the ExecutorRegistry PORT (Executor(StepType) (StepExecutor, bool));
// it never imports a concrete K8s/Registry/Gateway-backed executor. Tests inject a
// map of mock executors and production injects the real ones HERE — the engine code
// is identical in both.
//
// IMPORTANT (honest state of the build): no concrete StepExecutor adapters exist
// yet (the DEPLOY/CANARY/TRAIN/... executors that talk to K8s/Registry/Gateway are
// a later phase). So this registry is constructed EMPTY. The engine handles that
// correctly: an unregistered step type yields (nil, false) → ErrNoExecutor, so a
// TriggerExecution against a real step FAILS LOUDLY with "no executor registered"
// rather than silently doing nothing. The CRUD/list/Get RPCs and the saga state
// machine are fully wired and exercisable; only the side-effecting step work waits
// on the executor adapters. When they land, they register here via Register().
type stepExecutorRegistry struct {
	byType map[domain.StepType]domain.StepExecutor
}

// newStepExecutorRegistry builds an empty registry. Executors are added with
// Register as their adapters are implemented (a single, central wiring point).
func newStepExecutorRegistry() *stepExecutorRegistry {
	return &stepExecutorRegistry{byType: make(map[domain.StepType]domain.StepExecutor)}
}

// Register maps a StepType to the executor that runs it. (Unused until the first
// real executor adapter lands — kept so that wiring is a one-line change here, not
// a refactor of main.)
func (r *stepExecutorRegistry) Register(t domain.StepType, e domain.StepExecutor) {
	r.byType[t] = e
}

// Executor satisfies domain.ExecutorRegistry: returns the executor for a type, or
// (nil, false) which the engine turns into ErrNoExecutor.
func (r *stepExecutorRegistry) Executor(t domain.StepType) (domain.StepExecutor, bool) {
	e, ok := r.byType[t]
	return e, ok
}

// Compile-time proof the registry satisfies the domain port — if the port's
// signature ever drifts, the build breaks HERE, at the wiring site, with a precise
// message rather than a confusing failure deep in the engine.
var _ domain.ExecutorRegistry = (*stepExecutorRegistry)(nil)

func main() {
	// ================================================================
	// 1. STRUCTURED LOGGER
	// ================================================================
	// JSON to stdout for container log capture → Loki. stdlib slog: fast enough
	// for network-IO-bound services, zero external deps.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// ================================================================
	// 2. LOAD CONFIG
	// ================================================================
	cfg, err := config.Load[PipelineConfig]("FP")
	if err != nil {
		logger.Error("failed to load config", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// DatabaseURL has no compile-time `required` tag because it lives on the SHARED
	// BaseConfig (some services have no DB). For the orchestrator it is MANDATORY:
	// the saga's write-ahead log (executions + step checkpoints) IS the durability
	// contract. Enforce it HERE, the one place that knows this service needs a DB,
	// rather than mutating the shared struct. Failing now (not on the first
	// TriggerExecution) is the fail-fast rule.
	if cfg.DatabaseURL == "" {
		logger.Error("FP_DATABASE_URL is required for the pipeline-orchestrator service (the saga write-ahead store)")
		os.Exit(1)
	}

	logger.Info("config loaded",
		slog.Int("grpc_port", cfg.GRPCPort),
		slog.Int("health_port", cfg.Port),
		slog.String("log_level", cfg.LogLevel),
		slog.String("nats_url", cfg.NATSUrl),
		slog.Bool("otel_insecure", cfg.OTelInsecure),
		slog.Duration("compensation_timeout", cfg.CompensationTimeout),
	)

	// ================================================================
	// 3. OPENTELEMETRY
	// ================================================================
	// signal-aware context: a SIGTERM during startup aborts OTLP setup cleanly,
	// and the same ctx drives graceful shutdown below (it is cancelled on signal,
	// which makes srv.Serve return so our ordered shutdown runs).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	otelShutdown, err := observability.Setup(ctx, observability.Config{
		ServiceName:    "pipeline-orchestrator",
		ServiceVersion: "dev",
		Environment:    "local",
		OTLPEndpoint:   cfg.OTelEndpoint,
		OTLPInsecure:   cfg.OTelInsecure, // from config, not hard-coded
	})
	if err != nil {
		logger.Error("failed to setup observability", slog.String("error", err.Error()))
		os.Exit(1)
	}
	// NOTE ON SHUTDOWN ORDERING: we DON'T `defer otelShutdown` here. Telemetry must
	// flush LAST (after gRPC drains, the subscriber stops, and the pool closes) so
	// the spans/metrics emitted DURING shutdown are captured. All cleanup is hung off
	// the explicit, ordered shutdown() closure invoked once at the very end.

	// ================================================================
	// 4. CONNECT DATASTORES (Postgres write-ahead store, NATS event bus) — fail-fast
	// ================================================================
	// A BOUNDED ctx (not the signal ctx) guards against a hung dial wedging startup
	// indefinitely — the pod would never report ready and K8s would never learn why.
	bootCtx, bootCancel := context.WithTimeout(context.Background(), depConnectTimeout)
	defer bootCancel()

	// --- Postgres: the saga's durability backbone (write-ahead checkpoints) ---
	// postgres.New creates the pgxpool AND pings it, so a bad DSN / unreachable DB
	// fails boot HERE, not on the first TriggerExecution. The Store hands out the two
	// repository adapters (Pipelines, Executions) that back the same pool — one pool,
	// because a trigger writes an execution + its step checkpoints in one tx.
	store, err := postgres.New(bootCtx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("failed to connect to postgres", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("postgres connected (saga write-ahead store)")

	// --- NATS JetStream: the lifecycle event bus (produce fp.pipelines.*, consume drift) ---
	// Connect returns the raw conn (for lifecycle: IsConnected for readiness, Drain on
	// shutdown) AND the JetStream context (for the Publisher + Subscriber). We keep the
	// conn so /readyz can report NATS health and shutdown can DRAIN (flush buffered
	// publishes) rather than hard-Close mid-flight.
	natsConn, js, err := natsutil.Connect(cfg.NATSUrl)
	if err != nil {
		store.Close() // release the pool we just opened before exiting
		logger.Error("failed to connect to nats", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("nats connected", slog.String("url", natsConn.ConnectedUrl()))

	// Provision the JetStream streams this service depends on, idempotently.
	// WHY here (and why CreateOrUpdateStream): a PUBLISH only persists if some stream
	// captures its subject (JetStream RETURNS AN ERROR on a publish no stream
	// captures), and a CONSUMER can only attach to a stream that already exists. So:
	//   - PIPELINES captures fp.pipelines.> — the lifecycle events WE produce.
	//   - MODELS captures fp.models.>       — where the drift event we CONSUME lives
	//     (model-monitor produces it; stream creation is a cluster-bootstrap concern,
	//     not a per-producer one, so we converge it defensively so a fresh cluster /
	//     integration test has somewhere for our consumer to bind).
	//   - DLQ captures fp.dlq.>            — THE DEAD-LETTER STREAM (Finding 1). The
	//     retrain consumer routes a poison drift (after MaxRetries) to
	//     fp.dlq.pipelines.retrain (see WithDLQSubject below). If NO stream captures
	//     that subject, natsutil.routeToDLQ's Publish FAILS, it logs
	//     dlq_publish_failed and Term()s the ORIGINAL message — so a poison event is
	//     SILENTLY DROPPED instead of being parked for an operator, breaking the DLQ
	//     guarantee the subscriber's package doc promises. A dedicated DLQ stream
	//     (rather than folding fp.dlq.> into MODELS) keeps dead letters on their own
	//     retention/lifecycle so operators can drain/replay/age them out independently
	//     of live model traffic.
	// CreateOrUpdateStream is convergent + idempotent (like `kubectl apply`): a no-op
	// if the stream already matches, a reconcile if another service created it first —
	// no "already exists" race to special-case.
	streamCtx, streamCancel := context.WithTimeout(bootCtx, depConnectTimeout)
	streams := []jetstream.StreamConfig{
		{Name: "PIPELINES", Subjects: []string{"fp.pipelines.>"}}, // produced by this service
		{Name: "MODELS", Subjects: []string{"fp.models.>"}},       // consumed (drift) — see events.driftStream
		{Name: "DLQ", Subjects: []string{"fp.dlq.>"}},             // dead letters (poison drift parked for operators)
	}
	for _, sc := range streams {
		if _, streamErr := js.CreateOrUpdateStream(streamCtx, sc); streamErr != nil {
			streamCancel()
			_ = natsConn.Drain()
			store.Close()
			logger.Error("failed to ensure jetstream stream",
				slog.String("stream", sc.Name), slog.String("error", streamErr.Error()))
			os.Exit(1)
		}
	}
	streamCancel()
	logger.Info("jetstream streams ensured", slog.String("streams", "PIPELINES, MODELS, DLQ"))

	// ================================================================
	// 5. BUILD ADAPTERS → DOMAIN SAGA ENGINE → REAL HANDLER
	// ================================================================
	// Event publisher adapter chain: a pkg/natsutil.Publisher (envelope/dedup/trace)
	// wrapped by events.Publisher, which maps a domain.StepEvent → the canonical
	// eventsv1.* payload + fp.pipelines.* subject. events.Publisher satisfies the
	// domain.EventPublisher port — the engine stays wire-agnostic; this is the seam.
	natsPub := natsutil.NewPublisher(js, events.Source) // source = "pipeline-orchestrator"
	publisher := events.NewPublisher(natsPub, logger)   // domain.EventPublisher

	// The executor registry (StepType → StepExecutor). EMPTY for now — no concrete
	// executor adapters exist yet (see stepExecutorRegistry's doc). The engine returns
	// ErrNoExecutor for any step until they are registered here, which is the honest,
	// loud behavior. CRUD + the saga state machine are fully exercisable regardless.
	registry := newStepExecutorRegistry()

	// The saga engine: the two Postgres repositories (template store + run/checkpoint
	// store) + the executor registry + the event publisher, plus the two pure utility
	// ports (real wall clock, crypto-random UUIDv4). These last two are injected (not
	// called inline) so the domain stays uuid/time-free and tests stay deterministic;
	// production wires the real implementations HERE. NewPipelineService PANICS if
	// clock/ids are nil — a fail-fast guard against a mis-wired composition root.
	svc := domain.NewPipelineService(
		store.Pipelines(),  // domain.PipelineRepository  (templates)
		store.Executions(), // domain.ExecutionRepository (runs + write-ahead checkpoints)
		registry,           // domain.ExecutorRegistry    (StepType → StepExecutor)
		publisher,          // domain.EventPublisher      (best-effort lifecycle feed → NATS)
		systemClock{},      // domain.Clock               (time.Now, UTC)
		uuidGenerator{},    // domain.IDGenerator         (crypto-random UUIDv4)
	)

	// ================================================================
	// 6. BUILD gRPC SERVER + REGISTER THE WIRED HANDLER (real svc, NOT nil)
	// ================================================================
	// grpcutil.NewServer applies the standard chain (recovery → logging → … ). The
	// auth interceptor (WithAuthValidator) is added once the auth-service
	// TokenValidator client is wired in a later phase — adding it now with a nil
	// validator would reject every call. The auth interceptor is what supplies the
	// TokenClaims the handler turns into a domain.Actor (CreatedBy/TriggeredBy/Team are
	// then server-authoritative, never client request fields). WithReflection lets
	// grpcurl/grpcui introspect the service in development.
	srv := grpcutil.NewServer(
		grpcutil.WithLogger(logger),
		grpcutil.WithReflection(),
	)

	// The composition is complete: hand the fully-wired saga engine to the handler.
	// The handler holds the PipelineService INTERFACE — it has no idea the impl is
	// Postgres-backed with a NATS event feed. This single line is the difference
	// between the scaffold (NewPipelineHandler(nil) → every RPC Unimplemented) and a
	// working service.
	pipelinev1.RegisterPipelineOrchestratorServiceServer(srv.GRPC, handler.NewPipelineHandler(svc))
	logger.Info("pipeline handler registered with wired service")

	// ================================================================
	// 7. START THE DRIFT-RETRAIN SUBSCRIBER (closes serve→monitor→retrain)
	// ================================================================
	// The inbound adapter that makes the platform self-healing: on a CRITICAL drift
	// with auto_retrain armed, it triggers the model's retrain pipeline. It drives the
	// SAME domain svc inward (PipelineService.TriggerExecution) — it knows nothing of
	// Postgres or the engine internals (the dependency direction Clean Architecture
	// prescribes).
	//
	// The natsutil.Subscriber options encode the three delivery guarantees the
	// subscriber's package doc promises:
	//   - WithConsumerGroup(retrainGroup): durable name → survives restarts AND forms
	//     a consumer group so a drift event triggers a retrain on exactly ONE replica.
	//   - WithMaxRetries(5) + WithDLQSubject: a poison drift (names a missing/archived
	//     pipeline) is retried a bounded number of times then parked on the DLQ for an
	//     operator instead of NAK-looping forever.
	//   - WithIdempotencyStore: transport-level dedup on the envelope id so a
	//     redelivered drift event does not start a second retrain (the business-level
	//     half is drift.report_id used as the saga IdempotencyKey, inside the handler).
	//   - WithMessageTimeout(< AckWait): bounds one handler run so a wedged retrain
	//     NAKs before JetStream redelivers underneath it (avoids duplicate work).
	//
	// TRANSPORT DEDUP STORE — env-gated so the PRODUCTION binary boots (Finding 2).
	// MemoryProcessedStore is per-replica and non-durable, and it PANICS under
	// FP_ENV=production by design (unbounded map → OOM; no cross-replica sharing; no
	// crash-survival). Unconditionally constructing it here would hard-crash the
	// process during this step in its TARGET environment. events.NewProcessedStore
	// selects by FP_ENV: under production it builds a durable, SHARED Postgres-backed
	// store (the same pool the saga write-ahead log uses — no new dependency) that
	// dedups across the whole consumer group and survives restarts; for dev/test it
	// returns the in-memory store (correct for a single replica). Across the consumer
	// GROUP the durable consumer-group name already routes a given delivery to one
	// replica, and the saga's report_id IdempotencyKey makes the EFFECT exactly-once
	// even across replicas; this store collapses transport-level redeliveries (now
	// across replicas, in production, because it is shared).
	processedStore, err := events.NewProcessedStore(bootCtx, store.Pool())
	if err != nil {
		_ = natsConn.Drain()
		store.Close()
		logger.Error("failed to build processed-events store", slog.String("error", err.Error()))
		os.Exit(1)
	}
	driftSub := natsutil.NewSubscriber(js,
		natsutil.WithConsumerGroup("pipeline-orchestrator-retrain"),
		natsutil.WithMaxRetries(5),
		natsutil.WithDLQSubject("fp.dlq.pipelines.retrain"),
		natsutil.WithIdempotencyStore(processedStore),
		natsutil.WithMessageTimeout(20*time.Second),
	)
	retrainSub := events.NewDriftRetrainSubscriber(svc, driftSub, logger)
	// Start the consume loop. It returns once the loop is running (the loop itself is
	// managed by natsutil and stops on ctx cancel OR driftSub.Close() in shutdown). We
	// pass the signal-aware ctx so a SIGTERM stops consuming new drift events promptly.
	if subErr := retrainSub.Start(ctx); subErr != nil {
		_ = natsConn.Drain()
		store.Close()
		logger.Error("failed to start drift-retrain subscriber", slog.String("error", subErr.Error()))
		os.Exit(1)
	}
	logger.Info("drift-retrain subscriber started (serve→monitor→retrain loop armed)")

	// ================================================================
	// 8. HEALTH SERVER (HTTP /healthz liveness + /readyz readiness over real deps)
	// ================================================================
	// Separate HTTP port from gRPC: gRPC is HTTP/2, kubelet probes are HTTP/1.1 — they
	// can't share a net.Listener. Separate ports also let us flip /readyz to 503
	// (drain) while gRPC finishes in-flight calls on shutdown.
	//
	// READINESS over the REAL dependencies makes /readyz meaningful: if Postgres/NATS
	// goes away, /readyz → 503, K8s pulls the pod from the Service endpoints (no
	// traffic) WITHOUT restarting it (liveness stays green) — the correct posture for a
	// transient dependency outage.
	healthHandler := health.New()
	healthHandler.AddCheck("postgres", func(ctx context.Context) error {
		// Borrow a conn and round-trip — proves the pool can actually reach Postgres.
		return store.Pool().Ping(ctx)
	})
	healthHandler.AddCheck("nats", func(_ context.Context) error {
		// IsConnected reflects the live transport state (the client auto-reconnects in
		// the background; this reports whether it currently HAS a connection). Cheaper
		// and more accurate than a round-trip publish for a transport liveness check.
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
	// it as a closure (not a pile of defers) makes the load-bearing ordering explicit.
	shutdown := func() {
		// Fresh, bounded context — the signal ctx is already cancelled by now, so
		// anything honoring it would return ctx.Canceled immediately. Stays well within
		// K8s's terminationGracePeriodSeconds.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		// (a) Stop accepting health probes. By now gRPC has drained (Serve returned)
		//     and SetServing(false) flipped readiness, so the LB long since stopped
		//     routing here; closing the HTTP server just releases the port.
		if err := healthServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("health server shutdown error", slog.String("error", err.Error()))
		}

		// (b) Stop the drift-retrain subscriber BEFORE draining NATS / closing the pool:
		//     a retrain handler in flight drives the saga engine, which writes to
		//     Postgres and publishes lifecycle events. Closing it first means no handler
		//     starts a new saga while we tear the deps down underneath it. Close stops the
		//     consume loop and its watcher goroutine (no leak).
		driftSub.Close()

		// (c) DRAIN NATS (not Close): Drain flushes any buffered publishes and lets
		//     in-flight messages complete before tearing down the connection — so the
		//     last lifecycle events a draining saga emitted are not lost. Close would
		//     discard them. Runs after (b) so the subscriber is already quiesced.
		if err := natsConn.Drain(); err != nil {
			logger.Error("nats drain error", slog.String("error", err.Error()))
		}

		// (d) Close the Postgres pool. Safe now: gRPC has drained and the subscriber is
		//     stopped, so no handler is still mid-query holding a borrowed connection.
		//     Closing earlier could fail an in-flight saga's checkpoint write.
		store.Close()

		// (e) FLUSH OpenTelemetry LAST so the spans/metrics produced during (a)-(d)
		//     (and during the gRPC drain) are exported, not dropped. This is exactly why
		//     otelShutdown was NOT deferred up top.
		if err := otelShutdown(shutdownCtx); err != nil {
			logger.Error("otel shutdown error", slog.String("error", err.Error()))
		}
	}

	// ================================================================
	// 9. START gRPC SERVER (mark SERVING, listen, block until signal)
	// ================================================================
	// SetServing(true) flips the gRPC health service to SERVING only NOW — after every
	// dependency connected, the handler is wired, and the subscriber is armed — so a
	// gRPC readiness probe is honest (green means actually-serviceable). On SIGTERM,
	// grpcutil.Server.Serve flips it back to NOT_SERVING, then drains in-flight RPCs
	// before returning here.
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
	// 10. GRACEFUL SHUTDOWN (ordered: gRPC drained → health → subscriber → NATS → pool → otel)
	// ================================================================
	shutdown()
	logger.Info("pipeline-orchestrator service stopped cleanly")
}
