// Package main is the entrypoint for the Model Serving service.
//
// ============================================================================
// WIRING — THE COMPOSITION ROOT
// ============================================================================
//
// In Clean Architecture, main.go is the "composition root" — the ONLY place all
// layers are imported together and wired into a running program. It knows about
// every layer (config, observability, domain, handler) but those layers do not
// know about each other except through the interfaces they define.
//
// STARTUP SEQUENCE:
//
//	┌────────────────────────────────────────────────────────────────────┐
//	│  1. Structured logger (slog JSON → stdout → Loki)                  │
//	│  2. Load config (FP_* env vars → ServingConfig)                    │
//	│  3. Setup OpenTelemetry (traces → Tempo, metrics → Prometheus)     │
//	│  4. Build gRPC server via grpcutil.NewServer (interceptor chain)   │
//	│  5. Register ServingHandler on the gRPC server                     │
//	│  6. HTTP health server (/healthz liveness, /readyz readiness)      │
//	│  7. Mark SERVING and start gRPC (blocks until SIGTERM/SIGINT)      │
//	│  8. Graceful shutdown (OTel flush → gRPC drain → health server)    │
//	└────────────────────────────────────────────────────────────────────┘
//
// WHAT'S WIRED NOW (scaffold phase):
//   - ServingHandler with a nil domain service. The embedded
//     UnimplementedModelServingServiceServer answers every RPC with
//     codes.Unimplemented, so the nil service is never dereferenced. This is a
//     production-correct running server, not a stub.
//
// WHAT ARRIVES LATER (handler + adapter phases):
//
//   - internal/runtime: the ONNX InferenceEngine adapter (the real native binding).
//
//   - internal/storage: the MinIO/S3 ModelFetcher adapter.
//
//   - main.go then constructs the domain service and self-loads the pod's model:
//
//     eng   := runtime.NewONNXEngine(...)
//     fetch := storage.NewMinIOFetcher(cfg.ArtifactBucketURI, ...)
//     svc   := domain.NewServingService(domain.ServiceConfig{
//     Engine: eng, Fetcher: fetch, Clock: realClock{},
//     AllowedArtifactURIs: []string{cfg.ArtifactBucketURI},
//     LivenessLatencyMax:  cfg.LivenessLatencyMax,
//     ... })
//     // self-load the model this pod was deployed for (sidecar: "the pod IS the model"):
//     _, _ = svc.LoadModel(ctx, domain.LoadModelInput{
//     Ref: domain.ModelRef{Name: cfg.ModelName, Version: cfg.ModelVersion},
//     ArtifactURI: cfg.ArtifactURI, ExpectedDigest: cfg.ArtifactDigest})
//     servingv1.RegisterModelServingServiceServer(srv.GRPC, handler.NewServingHandler(svc))
//     // readiness probe goes green only once the model is StateReady:
//     healthHandler.AddCheck("model", func(ctx) error {
//     v, _, _ := svc.HealthCheck(ctx, cfg.ModelName)
//     if v != domain.HealthServing { return errors.New("model not ready") }
//     return nil })
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

	servingv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/serving/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/config"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/health"
	"github.com/abd-ulbasit/forgepoint/pkg/observability"
	"github.com/abd-ulbasit/forgepoint/services/model-serving/internal/handler"
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
	)

	// ================================================================
	// 3. OPENTELEMETRY
	// ================================================================
	// Pass the signal-aware ctx so OTLP setup respects a SIGTERM during startup,
	// and reuse the same ctx to drive graceful shutdown below.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

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
	defer func() {
		// Flush pending spans/metrics on shutdown; without this the last batch is
		// lost (OTel batches internally). K8s gives 30s grace — ample to flush.
		if shutdownErr := otelShutdown(context.Background()); shutdownErr != nil {
			logger.Error("otel shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// ================================================================
	// 4. BUILD gRPC SERVER
	// ================================================================
	// grpcutil.NewServer applies the standard interceptor chain (recovery →
	// logging → …). WHY no auth validator here yet: the serving pod is
	// auth-agnostic on the data plane by design (the gateway owns identity — see
	// the proto's Sidecar rationale); when control-plane RPCs (LoadModel/Unload)
	// are implemented they'll be gated by an operator-identity validator added
	// here. WithReflection lets grpcurl/grpcui introspect during development.
	srv := grpcutil.NewServer(
		grpcutil.WithLogger(logger),
		grpcutil.WithReflection(),
	)

	// ================================================================
	// 5. REGISTER SERVING HANDLER
	// ================================================================
	// The handler embeds UnimplementedModelServingServiceServer, so it satisfies
	// servingv1.ModelServingServiceServer fully right now; all RPCs return
	// Unimplemented until the handler phase. We pass nil as the domain service
	// for the scaffold phase — the embedded Unimplemented methods never
	// dereference it. Real wiring (engine + fetcher + self-load) lands when the
	// runtime/storage adapters exist (see the file header).
	servingv1.RegisterModelServingServiceServer(srv.GRPC, handler.NewServingHandler(nil))
	logger.Info("serving handler registered (scaffold: RPCs return Unimplemented until handler phase)")

	// ================================================================
	// 6. HEALTH SERVER (HTTP)
	// ================================================================
	// Separate HTTP port for K8s probes: gRPC is HTTP/2, kubelet probes are
	// HTTP/1.1 — they can't share a plain listener, and separating them lets the
	// gRPC server drain while /readyz returns 503 (pod leaves the LB).
	//
	// No readiness checks registered yet. In the handler phase the readiness
	// probe gains the model-loaded check (see the file header): /readyz goes
	// green only once the domain reports HealthServing for this pod's model —
	// readiness == model in memory, exactly the Sidecar pattern's probe contract.
	healthHandler := health.New()
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
		if serveErr := healthServer.ListenAndServe(); serveErr != nil && serveErr != http.ErrServerClosed {
			logger.Error("health server error", slog.String("error", serveErr.Error()))
		}
	}()
	defer func() {
		if shutdownErr := healthServer.Shutdown(context.Background()); shutdownErr != nil {
			logger.Error("health server shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// ================================================================
	// 7. START gRPC SERVER
	// ================================================================
	// SetServing(true) flips the gRPC health service to SERVING so readiness
	// passes. (grpcutil flips it back to NOT_SERVING on SIGTERM before draining —
	// the graceful-shutdown dance.) NOTE: in the handler phase we keep this gated
	// on the MODEL being loaded (the pod isn't truly ready until StateReady), so
	// SetServing(true) will move to after a successful self-load.
	srv.SetServing(true)

	grpcAddr := fmt.Sprintf(":%d", cfg.GRPCPort)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("failed to listen on gRPC port", slog.String("addr", grpcAddr), slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("gRPC server listening", slog.String("addr", grpcAddr))

	// Serve blocks until ctx is cancelled (SIGINT/SIGTERM). On cancellation:
	//   1. SetServing(false) — readiness fails → pod leaves LB endpoints
	//   2. GracefulStop (bounded drain) — in-flight predicts finish
	//   3. hard Stop if drain times out
	//   4. returns here; deferred OTel + health-server shutdown run
	if serveErr := srv.Serve(ctx, lis); serveErr != nil {
		logger.Error("gRPC server error", slog.String("error", serveErr.Error()))
		os.Exit(1)
	}

	logger.Info("model-serving service stopped cleanly")
}
