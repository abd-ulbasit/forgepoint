// Package main is the entrypoint for the Model Serving service.
//
// ============================================================================
// WIRING — THE COMPOSITION ROOT (fully wired)
// ============================================================================
//
// In Clean Architecture, main.go is the "composition root" — the ONLY place all
// layers are imported together and wired into a running program. It knows about
// every layer (config, observability, domain, handler, events, artifactstore)
// but those layers do not know about each other except through the interfaces
// they define. This file constructs the concrete adapters, injects them into the
// domain, hands the real domain service to the gRPC handler AND the event
// subscriber, then runs the server with a correct startup + shutdown ordering.
//
// STARTUP SEQUENCE (acquire order — shutdown runs in REVERSE via stacked defers):
//
//	┌────────────────────────────────────────────────────────────────────┐
//	│  1. Structured logger (slog JSON → stdout → Loki)                   │
//	│  2. Load config (FP_* env vars → ServingConfig)                     │
//	│  3. Setup OpenTelemetry (traces → Tempo, metrics → Prometheus)      │
//	│  4. Connect NATS JetStream (the only datastore this pod talks to)   │
//	│  5. Build adapters: ModelFetcher (file/http) + InferenceEngine seam │
//	│  6. Construct the domain ServingService (inject ports + clock)      │
//	│  7. SELF-LOAD this pod's model (sidecar: "the pod IS the model")    │
//	│  8. Build + register the gRPC handler (REAL service, not nil)       │
//	│  9. Build + Start the event SUBSCRIBERS (reconcile-controller)      │
//	│ 10. HTTP health server (/readyz checks the REAL deps: model + NATS) │
//	│ 11. Flip gRPC SERVING and Serve (blocks until SIGTERM/SIGINT)       │
//	│ 12. Graceful shutdown (gRPC drain → subs stop → NATS drain → OTel)  │
//	└────────────────────────────────────────────────────────────────────┘
//
// WHY NO POSTGRES / REDIS HERE (deliberate, not an omission):
//
//	The platform's data-store table lists model-serving as "In-memory (ONNX)".
//	A serving pod's source of truth is the loaded model in process memory — the
//	domain's runtime registry IS the state. It owns no Postgres database and no
//	Redis cache: it publishes nothing (the gateway owns InferenceCompleted) and
//	caches predict results in-process (the domain's idempotency cache), so there
//	is no cross-pod store to wire. We therefore connect ONLY NATS (to consume the
//	five lifecycle events) and do NOT open a pgxpool or a go-redis client. Adding
//	them would be dead weight and would pull deps this module doesn't carry.
//
// THE INFERENCE-ENGINE SEAM (read this — it is the one honest gap):
//
//	The domain depends on a domain.InferenceEngine PORT (the ONNX abstraction).
//	The REAL adapter — a cgo binding over onnxruntime (e.g. yalue/onnxruntime_go)
//	— is not in this module yet (it needs native libs + a dependency bump that a
//	pure-wiring change must not do). To keep this a production-correct RUNNING
//	server rather than one that nil-panics on the first Predict, we inject a
//	small, explicit placeholder engine (placeholderEngine below) that satisfies
//	the port. It is clearly labelled and is the SINGLE line to swap when the ONNX
//	adapter lands: replace newPlaceholderEngine() with runtime.NewONNXEngine(...).
//	Everything else — fetcher, self-load, handler, subscribers — is real.
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
	"path/filepath"
	"sync"
	"syscall"
	"time"

	servingv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/serving/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/config"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/health"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/observability"
	"github.com/abd-ulbasit/forgepoint/services/model-serving/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/model-serving/internal/events"
	"github.com/abd-ulbasit/forgepoint/services/model-serving/internal/handler"
	"github.com/abd-ulbasit/forgepoint/services/model-serving/internal/repository/artifactstore"
)

// ServingConfig extends BaseConfig with model-serving-specific configuration.
//
// WHY embed BaseConfig: every Forgepoint service needs Port, GRPCPort, LogLevel,
// OTelEndpoint, NATSUrl, DatabaseURL. Embedding avoids repeating those in every
// service. config.Load[ServingConfig]("FP") reads FP_PORT, FP_GRPC_PORT,
// FP_MODEL_NAME, FP_ARTIFACT_URI, etc. via reflection (see pkg/config).
//
// SIDECAR NOTE: a serving pod is "the model" — its identity (ModelName,
// ModelVersion) and artifact pointer (ArtifactURI, ArtifactDigest) come from the
// Deployment's env vars (one Deployment per model version). The pod self-loads
// that exact artifact at startup. These are SERVER-side config injected by the
// operator/controller, never client-set — the anti-spoofing posture.
type ServingConfig struct {
	config.BaseConfig

	// ModelName / ModelVersion identify the single model this pod serves. Set by
	// the per-version Deployment. Required: a serving pod with no model identity
	// is misconfigured and should fail fast rather than start empty.
	ModelName    string `env:"MODEL_NAME" required:"true"`
	ModelVersion string `env:"MODEL_VERSION" required:"true"`

	// ArtifactURI is the object-storage location of the ONNX artifact this pod
	// loads at startup (e.g. "s3://fp-models/iris/v1.onnx"). Validated against the
	// allow-list (SSRF guard) by the domain before any fetch.
	ArtifactURI string `env:"ARTIFACT_URI" required:"true"`

	// ArtifactDigest is the optional expected sha256 of the artifact. When set,
	// the pod refuses to serve weights whose digest does not match (supply-chain
	// integrity). Empty = no integrity check (acceptable in local dev).
	ArtifactDigest string `env:"ARTIFACT_DIGEST"`

	// ArtifactBucketURI is the allow-listed bucket/prefix the pod may load from
	// (the SSRF allow-list root, e.g. "s3://fp-models/"). The domain rejects any
	// ArtifactURI outside it. Defaults to the fp-models bucket.
	ArtifactBucketURI string `env:"ARTIFACT_BUCKET_URI" default:"s3://fp-models/"`

	// LivenessLatencyMs is the p99/last-inference latency threshold (ms) above
	// which the liveness signal degrades and the pod is pulled from rotation
	// (and eventually restarted). Inference latency, not CPU, is the health
	// signal for an inference pod — see the HPA rationale in the proto.
	LivenessLatencyMs int `env:"LIVENESS_LATENCY_MS" default:"1000"`

	// ArtifactScratchDir is the pod-owned directory the ModelFetcher copies the
	// downloaded artifact into (so the engine loads a path THIS process owns,
	// decoupled from the source mount's lifetime). Defaults to the OS temp dir.
	ArtifactScratchDir string `env:"ARTIFACT_SCRATCH_DIR"`

	// MaxArtifactBytes caps the bytes the HTTP fetcher will pull from a presigned
	// URL (DoS guard: a hostile/misconfigured endpoint can't fill the pod disk).
	// 0 = unlimited (set a real cap in production). Default 512 MiB is generous
	// for ONNX models while still bounding a runaway download.
	MaxArtifactBytes int64 `env:"MAX_ARTIFACT_BYTES" default:"536870912"`

	// EventMaxRetries / EventDLQSubject configure the consumer's retry+DLQ policy
	// (the events adapter applies these to every per-subject Subscriber). After
	// MaxRetries NAKs a poison message is routed to the DLQ subject instead of
	// being redelivered forever.
	EventMaxRetries       int    `env:"EVENT_MAX_RETRIES" default:"5"`
	EventDLQSubject       string `env:"EVENT_DLQ_SUBJECT" default:"fp.dlq.model-serving"`
	EventHandlerTimeoutMs int    `env:"EVENT_HANDLER_TIMEOUT_MS" default:"30000"`
}

func main() {
	// ================================================================
	// 1. STRUCTURED LOGGER (JSON → stdout → Loki)
	// ================================================================
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// ================================================================
	// 2. LOAD CONFIG
	// ================================================================
	// config.Load reads FP_* env vars into ServingConfig via reflection and fails
	// fast on a missing required field (MODEL_NAME, MODEL_VERSION, ARTIFACT_URI).
	// In K8s those come from the per-version Deployment's env/ConfigMap.
	cfg, err := config.Load[ServingConfig]("FP")
	if err != nil {
		logger.Error("failed to load config", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("config loaded",
		slog.Int("grpc_port", cfg.GRPCPort),
		slog.Int("health_port", cfg.Port),
		slog.String("model_name", cfg.ModelName),
		slog.String("model_version", cfg.ModelVersion),
		slog.String("artifact_bucket", cfg.ArtifactBucketURI),
	)

	// Signal-aware root context: SIGTERM (K8s rolling update) / SIGINT (Ctrl-C)
	// cancel it, which drives the graceful shutdown of every component below. We
	// build it BEFORE any blocking setup so a signal during startup is honored.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ================================================================
	// 3. OPENTELEMETRY (deferred flush is the LAST shutdown step)
	// ================================================================
	otelShutdown, err := observability.Setup(ctx, observability.Config{
		ServiceName:    "model-serving",
		ServiceVersion: "dev",
		Environment:    "local",
		OTLPEndpoint:   cfg.OTelEndpoint,
		OTLPInsecure:   true, // plaintext to the local docker-compose collector (no TLS cert)
	})
	if err != nil {
		logger.Error("failed to setup observability", slog.String("error", err.Error()))
		os.Exit(1)
	}
	// Deferred FIRST so it runs LAST (defers are LIFO): we want spans/metrics from
	// every other component's shutdown to be captured before we flush+stop OTel.
	defer func() {
		// A fresh context: the signal ctx is already cancelled by shutdown time, so
		// reusing it would abort the flush. K8s grace (30s) is ample.
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if shutdownErr := otelShutdown(flushCtx); shutdownErr != nil {
			logger.Error("otel shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// ================================================================
	// 4. CONNECT NATS JETSTREAM (the only datastore this pod uses)
	// ================================================================
	// natsutil.Connect returns the raw *nats.Conn (for lifecycle: Drain on
	// shutdown) and a jetstream.JetStream context (for stream/consumer ops). We
	// keep the raw conn so we can DRAIN it gracefully: Drain flushes pending acks
	// and lets in-flight consume callbacks finish before the socket closes — the
	// async-bus equivalent of the gRPC graceful drain.
	natsConn, js, err := natsutil.Connect(cfg.NATSUrl)
	if err != nil {
		logger.Error("failed to connect to NATS", slog.String("url", cfg.NATSUrl), slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("connected to NATS JetStream", slog.String("url", cfg.NATSUrl))
	// Deferred SECOND-from-last (runs after subscribers are stopped below, before
	// OTel flush). Drain is graceful; if it errors we still proceed to exit.
	defer func() {
		if drainErr := natsConn.Drain(); drainErr != nil {
			logger.Error("nats drain error", slog.String("error", drainErr.Error()))
		}
		logger.Info("nats connection drained")
	}()

	// Provision the JetStream streams this pod's consumers bind to (idempotent,
	// convergent like `kubectl apply`). A fresh cluster needs the MODELS and
	// PIPELINES streams to exist before a durable consumer can attach.
	if err := events.EnsureStreams(ctx, js); err != nil {
		logger.Error("failed to ensure JetStream streams", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// ================================================================
	// 5. BUILD THE ADAPTERS (driven ports the domain depends on)
	// ================================================================
	// ModelFetcher: a scheme dispatcher routing file:// to a FileFetcher and
	// http(s):// to an HTTPFetcher. The domain sees ONE domain.ModelFetcher and is
	// oblivious to the scheme split. The bucket allow-list root doubles as the
	// FileFetcher's allowed source root (defense in depth under the domain's SSRF
	// guard). s3:// is not yet handled (the native MinIO client is a follow-up —
	// see artifactstore's package doc); presigned-URL HTTP or a file mount cover
	// production today.
	scratchDir := cfg.ArtifactScratchDir
	if scratchDir == "" {
		scratchDir = filepath.Join(os.TempDir(), "fp-model-serving")
	}
	fileFetcher := artifactstore.NewFileFetcher(scratchDir)
	httpFetcher := artifactstore.NewHTTPFetcher(scratchDir,
		artifactstore.WithMaxBytes(cfg.MaxArtifactBytes),
		artifactstore.WithRequestTimeout(60*time.Second),
	)
	fetcher := artifactstore.NewFetcher(fileFetcher, httpFetcher)

	// InferenceEngine: the SEAM. Real ONNX adapter is a follow-up dependency bump
	// (see the file header). The placeholder satisfies the port so the server runs
	// and the wiring is exercised end-to-end. Swap this ONE line for the ONNX
	// adapter when it lands; nothing else in this file changes.
	engine := newPlaceholderEngine(logger)

	// ================================================================
	// 6. CONSTRUCT THE DOMAIN SERVICE (inject ports + clock + tunables)
	// ================================================================
	// This is the dependency-injection moment: the business logic dictates the
	// engine/fetcher/clock contracts; main wires the concrete implementations.
	// The allow-list (SSRF/supply-chain guard) is the configured bucket root.
	svc := domain.NewServingService(domain.ServiceConfig{
		Engine:              engine,
		Fetcher:             fetcher,
		Clock:               realClock{},
		AllowedArtifactURIs: []string{cfg.ArtifactBucketURI},
		LivenessLatencyMax:  time.Duration(cfg.LivenessLatencyMs) * time.Millisecond,
		// Other caps (MaxInputTensors, MaxTensorBytes, page sizes, cache size) fall
		// back to the domain's sane defaults — there is no operational reason to
		// override them per-pod, and leaving them zero keeps the contract numbers in
		// one place (the domain).
	})

	// ================================================================
	// 7. SELF-LOAD THIS POD'S MODEL (the Sidecar contract: "the pod IS the model")
	// ================================================================
	// A serving pod exists to serve ONE model version. We load it eagerly at boot
	// so that /readyz only goes green once the artifact is fetched, verified
	// (digest), and the engine session is initialized — readiness == model in
	// memory, exactly the Sidecar probe contract. We do NOT os.Exit on a load
	// failure: the domain models a failed load as StateFailed (not an error), and
	// the readiness check below stays red until a redelivered lifecycle event (or
	// an operator LoadModel) succeeds. Crash-looping on a transient fetch hiccup
	// would be worse than starting NotReady and letting the probe + controller
	// reconcile.
	loadCtx, loadCancel := context.WithTimeout(ctx, 2*time.Minute)
	loadStatus, loadErr := svc.LoadModel(loadCtx, domain.LoadModelInput{
		Ref:            domain.ModelRef{Name: cfg.ModelName, Version: cfg.ModelVersion},
		ArtifactURI:    cfg.ArtifactURI,
		ExpectedDigest: cfg.ArtifactDigest,
	})
	loadCancel()
	switch {
	case loadErr != nil:
		// A synchronous error here is a CALLER error (bad input or SSRF reject) —
		// the artifact URI is outside the allow-list, or required fields are empty.
		// That is a misconfiguration the operator must fix; log loudly but keep the
		// process up so the probe reports NotReady rather than crash-looping (which
		// would obscure the cause behind restart noise).
		logger.Error("self-load rejected (misconfiguration) — starting NotReady",
			slog.String("artifact_uri", cfg.ArtifactURI),
			slog.String("error", loadErr.Error()))
	case loadStatus.State != domain.StateReady:
		logger.Warn("self-load did not reach Ready — starting NotReady, controller will reconcile",
			slog.String("state", loadStatus.State.String()),
			slog.String("message", loadStatus.Message))
	default:
		logger.Info("model self-loaded and Ready",
			slog.String("model", cfg.ModelName),
			slog.String("version", cfg.ModelVersion),
			slog.String("state", loadStatus.State.String()))
	}

	// ================================================================
	// 8. BUILD gRPC SERVER + REGISTER THE REAL HANDLER (not nil)
	// ================================================================
	// grpcutil.NewServer applies the standard interceptor chain (recovery →
	// logging → …). The serving pod is auth-agnostic on the data plane by design
	// (the gateway owns identity); control-plane auth (operator-only LoadModel)
	// is a later concern, added here as an auth validator when implemented.
	srv := grpcutil.NewServer(
		grpcutil.WithLogger(logger),
		grpcutil.WithReflection(),
	)
	// THE REPLACEMENT: the handler now holds the fully-wired domain service, not
	// nil. Every RPC executes real business logic against the runtime registry.
	servingv1.RegisterModelServingServiceServer(srv.GRPC, handler.NewServingHandler(svc))
	logger.Info("serving handler registered with live domain service")

	// ================================================================
	// 9. EVENT SUBSCRIBERS — the reconcile controller over the bus
	// ================================================================
	// The pod is a PURE CONSUMER: it reacts to five lifecycle events and
	// reconciles its resident model (EnsureLoaded / Unload). The subscriber owns
	// the delivery mechanics; the BEHAVIOR lives in the domain (and is the SAME
	// svc the handler uses — one source of truth for "load this model").
	//
	// Shared SubOptions applied to every per-subject consumer:
	//   - WithIdempotencyStore: an envelope-id dedupe so a redelivered event is
	//     recognized before the handler runs (belt-and-braces with the domain's
	//     idempotent EnsureLoaded/Unload). In-memory store: a sidecar consuming its
	//     own model's events doesn't need cross-restart dedupe — the domain's
	//     idempotency already makes a post-restart redelivery a no-op.
	//   - WithMaxRetries + WithDLQSubject: after N NAKs a poison message is routed
	//     to the DLQ instead of redelivered forever.
	//   - WithMessageTimeout: bounds a single reconcile (a slow fetch/engine init)
	//     so one stuck event can't wedge the consume loop.
	subOpts := []natsutil.SubOption{
		natsutil.WithIdempotencyStore(natsutil.NewMemoryProcessedStore()),
		natsutil.WithMaxRetries(cfg.EventMaxRetries),
		natsutil.WithDLQSubject(cfg.EventDLQSubject),
		natsutil.WithMessageTimeout(time.Duration(cfg.EventHandlerTimeoutMs) * time.Millisecond),
	}
	subscriber := events.NewSubscriber(svc, js, logger, subOpts...)
	if err := subscriber.Start(ctx); err != nil {
		logger.Error("failed to start event subscribers", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("event subscribers started (reconcile controller live on 5 subjects)")
	// Deferred to run AFTER the gRPC drain (Serve returns) and BEFORE the NATS
	// drain: stop the consume loops so no new event is dispatched while we tear
	// down, then drain the conn to flush pending acks.
	defer func() {
		subscriber.Close()
		logger.Info("event subscribers stopped")
	}()

	// ================================================================
	// 10. HEALTH SERVER (HTTP) — readiness checks the REAL dependencies
	// ================================================================
	// Separate HTTP port for K8s probes: gRPC is HTTP/2, kubelet probes are
	// HTTP/1.1 — they can't share a plain listener, and separating them lets the
	// gRPC server drain while /readyz returns 503 (pod leaves the LB).
	healthHandler := health.New()

	// Readiness check #1 — THE MODEL: /readyz is green only once the domain reports
	// HealthServing for this pod's model (StateReady AND latency within threshold).
	// This is the Sidecar pattern's probe contract: ready == model resident in
	// memory and healthy. Before the self-load succeeds (or after a degrade) this
	// returns an error → 503 → the pod leaves the Service endpoints.
	healthHandler.AddCheck("model", func(ctx context.Context) error {
		verdict, state, _ := svc.HealthCheck(ctx, cfg.ModelName)
		if verdict != domain.HealthServing {
			return fmt.Errorf("model %s/%s not serving (state=%s)", cfg.ModelName, cfg.ModelVersion, state)
		}
		return nil
	})

	// Readiness check #2 — NATS: the pod's reconcile controller depends on the bus.
	// If NATS is down the pod can still serve cached/loaded predictions, but it can
	// no longer react to lifecycle events — surface that as not-ready so traffic
	// drains to a replica with a live control path.
	healthHandler.AddCheck("nats", func(ctx context.Context) error {
		if !natsConn.IsConnected() {
			return errors.New("nats connection is not established")
		}
		return nil
	})

	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", healthHandler.LivenessHandler())
	healthMux.HandleFunc("/readyz", healthHandler.ReadinessHandler())

	healthAddr := fmt.Sprintf(":%d", cfg.Port)
	healthServer := &http.Server{
		Addr:              healthAddr,
		Handler:           healthMux,
		ReadHeaderTimeout: 5 * time.Second, // slowloris guard on the health listener
	}
	go func() {
		logger.Info("health server listening", slog.String("addr", healthAddr))
		if serveErr := healthServer.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			logger.Error("health server error", slog.String("error", serveErr.Error()))
		}
	}()
	// Deferred to run near-last (after gRPC + subs + NATS): keep /readyz answering
	// 503 as long as possible so the LB sees the pod leave before the listener dies.
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if shutdownErr := healthServer.Shutdown(shutCtx); shutdownErr != nil {
			logger.Error("health server shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// ================================================================
	// 11. START gRPC SERVER (blocks until SIGTERM/SIGINT)
	// ================================================================
	// SetServing(true) flips the gRPC health service to SERVING. The model may or
	// may not be loaded yet (self-load above is best-effort); the HTTP /readyz
	// check is the authoritative model-readiness gate K8s keys on. We mark the gRPC
	// health SERVING so the transport accepts calls; the domain's readiness gate
	// (StateReady) still rejects a Predict against an unloaded model with
	// FailedPrecondition, so there is no risk of serving from empty memory.
	srv.SetServing(true)

	grpcAddr := fmt.Sprintf(":%d", cfg.GRPCPort)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("failed to listen on gRPC port", slog.String("addr", grpcAddr), slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("gRPC server listening", slog.String("addr", grpcAddr))

	// Serve blocks until ctx is cancelled (SIGINT/SIGTERM). On cancellation it
	// flips NOT_SERVING (readiness fails → pod leaves LB), runs a bounded
	// GracefulStop (in-flight predicts finish), then returns. The stacked defers
	// THEN run in LIFO order:
	//   health server Shutdown → subscriber.Close → nats Drain → otel flush.
	// That ordering is deliberate: stop accepting/serving first, then stop
	// reacting to events, then drain the bus, then flush telemetry last so every
	// shutdown step's spans are captured.
	if serveErr := srv.Serve(ctx, lis); serveErr != nil {
		logger.Error("gRPC server error", slog.String("error", serveErr.Error()))
		os.Exit(1)
	}

	logger.Info("model-serving service stopped cleanly")
}

// ============================================================================
// realClock — the production domain.Clock (time.Now / time.Since)
// ============================================================================
//
// The domain abstracts time behind a Clock port so tests can inject a
// deterministic fake and assert exact timestamps/latencies. Production wires this
// trivial real implementation. (See domain/ports.go's Clock rationale.)
type realClock struct{}

func (realClock) Now() time.Time                  { return time.Now() }
func (realClock) Since(t time.Time) time.Duration { return time.Since(t) }

var _ domain.Clock = realClock{}

// ============================================================================
// placeholderEngine — a stand-in domain.InferenceEngine (THE ONE HONEST GAP)
// ============================================================================
//
// WHY THIS EXISTS: the real ONNX adapter (a cgo binding over onnxruntime) is not
// in this module yet — adding it requires native libraries and a dependency bump
// that a pure-wiring change must not perform. The domain REQUIRES a non-nil
// InferenceEngine (LoadModel calls Load; Predict calls Predict); injecting nil
// would nil-panic on the first call. This placeholder lets the composition root
// be fully exercised — fetch + digest verify + state machine + handler +
// subscribers all run for real — with ONLY the final native-math step stubbed.
//
// IT IS NOT A SILENT FAKE: Load succeeds (reporting the digest the fetcher
// computed, so the supply-chain check is real), but Predict returns an error so a
// real inference attempt fails LOUDLY rather than returning fabricated tensors.
// When the ONNX adapter lands, replace newPlaceholderEngine(logger) in main with
// runtime.NewONNXEngine(...) — no other line in this file changes (that
// substitutability is exactly what the InferenceEngine PORT buys us).
type placeholderEngine struct {
	log *slog.Logger

	mu     sync.Mutex
	loaded map[domain.ModelRef]struct{}
}

func newPlaceholderEngine(log *slog.Logger) *placeholderEngine {
	return &placeholderEngine{log: log, loaded: make(map[domain.ModelRef]struct{})}
}

// Load records the session as "loaded" and echoes back the digest the fetcher
// already computed over the bytes on disk. We compute no schema (a real engine
// reads it from the ONNX graph) and report a zero memory estimate. Returning the
// fetcher's digest path is intentional: the domain's supply-chain digest check
// runs against a REAL digest, not a fabricated one.
func (e *placeholderEngine) Load(_ context.Context, ref domain.ModelRef, localPath string) (domain.LoadedSession, error) {
	e.mu.Lock()
	e.loaded[ref] = struct{}{}
	e.mu.Unlock()
	e.log.Warn("placeholder engine: Load is a no-op session (real ONNX adapter pending)",
		slog.String("model", ref.Name), slog.String("version", ref.Version),
		slog.String("local_path", localPath))
	// Digest empty here means "engine has no opinion" — the domain then trusts the
	// fetcher's digest (see LoadModel step 7). A real adapter would return the
	// digest of the bytes it mmap'd and the schema it read from the graph.
	return domain.LoadedSession{}, nil
}

// Predict fails loudly: there is no native runtime to run inference. Returning
// an error (rather than fake outputs) means a client/test hitting Predict gets a
// clear server fault instead of silently-wrong predictions — the honest behavior
// for a not-yet-implemented engine. The domain wraps this as ErrInferenceFailed
// → the handler maps it to codes.Internal (sanitized).
func (e *placeholderEngine) Predict(_ context.Context, ref domain.ModelRef, _ map[string]domain.Tensor) (map[string]domain.Tensor, error) {
	return nil, fmt.Errorf("placeholder engine cannot run inference for %s/%s: real ONNX runtime adapter not yet wired", ref.Name, ref.Version)
}

// Unload forgets the session (idempotent — unloading an unknown ref is fine).
func (e *placeholderEngine) Unload(_ context.Context, ref domain.ModelRef) error {
	e.mu.Lock()
	delete(e.loaded, ref)
	e.mu.Unlock()
	return nil
}

var _ domain.InferenceEngine = (*placeholderEngine)(nil)
