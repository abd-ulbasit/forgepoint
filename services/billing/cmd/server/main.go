// Package main is the entrypoint for the Billing service.
//
// ============================================================================
// WIRING — THE COMPOSITION ROOT
// ============================================================================
//
// In Clean Architecture, main.go is the "composition root" — the ONLY place all
// layers are imported together and wired into a running program. It knows about
// every layer (config, observability, domain, handler) but none of those layers
// know about each other except through the interfaces they define.
//
// STARTUP SEQUENCE (identical shape to the Auth service — uniform platform):
//
//	┌──────────────────────────────────────────────────────────────────┐
//	│  1. Structured logger (slog JSON → stdout → Loki)               │
//	│  2. Load config (FP_* env vars → BillingConfig)                 │
//	│  3. Setup OpenTelemetry (traces → Tempo, metrics → Prometheus)  │
//	│  4. Build gRPC server via grpcutil.NewServer (interceptor chain)│
//	│  5. Register BillingHandler on the gRPC server                  │
//	│  6. HTTP health server (/healthz liveness, /readyz readiness)   │
//	│  7. Mark SERVING + start gRPC (blocks until SIGTERM/SIGINT)     │
//	│  8. Graceful shutdown (OTel flush → gRPC drain → health stop)   │
//	└──────────────────────────────────────────────────────────────────┘
//
// WHAT'S WIRED NOW (scaffold phase): BillingHandler with a nil domain service
// (the embedded UnimplementedBillingServiceServer answers every RPC with
// codes.Unimplemented — correct behavior). No Postgres pool, no NATS connection,
// no outbox relay yet.
//
// WHAT ARRIVES LATER (and where it slots in):
//
//	Repo phase:   pool := pgxpool.New(ctx, cfg.DatabaseURL)
//	              usageStore := postgres.NewUsageStore(pool); etc.
//	              svc := domain.NewBillingService(usageStore, planStore,
//	                       invoiceStore, ids, clock)
//	              healthHandler.AddCheck("db", func(ctx) error { return pool.Ping(ctx) })
//	Events phase: nc := natsutil.Connect(cfg.NATSUrl)
//	              relay := events.NewOutboxRelay(pool, nc)  // tails outbox → NATS
//	              go relay.Run(ctx)                          // the DB→NATS half
//	              consumer := events.NewInferenceConsumer(nc, svc) // NATS→DB metering
//	Handler phase: each RPC method on *BillingHandler calls svc.* and maps
//	              proto ↔ domain + domain errors → gRPC status codes.
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

	billingv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/billing/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/config"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/health"
	"github.com/abd-ulbasit/forgepoint/pkg/observability"
	"github.com/abd-ulbasit/forgepoint/services/billing/internal/handler"
)

// BillingConfig extends BaseConfig with billing-specific configuration.
//
// WHY embed BaseConfig: every Forgepoint service needs Port, GRPCPort, LogLevel,
// OTelEndpoint, NATSUrl, DatabaseURL. Embedding them avoids repeating those
// fields. config.Load[BillingConfig]("FP") reads FP_PORT, FP_GRPC_PORT,
// FP_DEFAULT_CURRENCY, etc. via reflection (see pkg/config).
//
// WHY DefaultCurrency is here (a billing-specific knob): a deployment runs in a
// single settlement currency by default; a rate plan may still set its own, but
// any server-built artifact that needs a currency before a plan is resolved
// (validation messages, an empty-usage rollup's zero total) uses this. ISO-4217.
type BillingConfig struct {
	config.BaseConfig

	// DefaultCurrency is the deployment's settlement currency (ISO-4217). Used as
	// the fallback currency for server-built artifacts when no plan currency
	// applies. Validated to a 3-letter uppercase code by the domain on use.
	DefaultCurrency string `env:"DEFAULT_CURRENCY" default:"USD"`
}

func main() {
	// ================================================================
	// 1. STRUCTURED LOGGER (slog JSON → stdout → Loki)
	// ================================================================
	// JSON output for container environments where logs are shipped to Loki via
	// the runtime's stdout capture. stdlib slog: fast enough, zero deps, the
	// direction the std library is heading.
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
	logger.Info("config loaded",
		slog.Int("grpc_port", cfg.GRPCPort),
		slog.Int("health_port", cfg.Port),
		slog.String("log_level", cfg.LogLevel),
		slog.String("default_currency", cfg.DefaultCurrency),
	)

	// ================================================================
	// 3. OPENTELEMETRY (traces → Tempo, metrics → Prometheus via collector)
	// ================================================================
	// NotifyContext gives a signal-aware context: SIGTERM during startup aborts
	// cleanly, and the same ctx drives the graceful shutdown below.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	otelShutdown, err := observability.Setup(ctx, observability.Config{
		ServiceName:    "billing",
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
		// Flush pending spans/metrics on shutdown (OTel batches internally; without
		// this the last few seconds of telemetry are lost). K8s grants 30s grace.
		if shutdownErr := otelShutdown(context.Background()); shutdownErr != nil {
			logger.Error("otel shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// ================================================================
	// 4. BUILD gRPC SERVER (recovery → logging interceptor chain)
	// ================================================================
	// WithReflection lets grpcurl/grpcui introspect the service in dev without the
	// .proto files locally.
	//
	// NOTE on auth: unlike most services, Billing's RPCs WILL need an auth
	// interceptor (so the handler can read the caller's team/role from claims for
	// the server-authoritative `team` and the admin check on CreateRatePlan). That
	// validator is wired in the handler phase once the auth client exists; the
	// scaffold runs without it (every RPC returns Unimplemented anyway).
	srv := grpcutil.NewServer(
		grpcutil.WithLogger(logger),
		grpcutil.WithReflection(),
	)

	// ================================================================
	// 5. REGISTER BILLING HANDLER (nil svc — Unimplemented base answers)
	// ================================================================
	// The handler embeds UnimplementedBillingServiceServer, so it fully satisfies
	// billingv1.BillingServiceServer right now; every RPC returns Unimplemented
	// until the handler phase overrides them. nil svc is safe: the embedded base
	// never dereferences it.
	billingv1.RegisterBillingServiceServer(srv.GRPC, handler.NewBillingHandler(nil))
	logger.Info("billing handler registered")

	// ================================================================
	// 6. HEALTH SERVER (HTTP — separate port from gRPC)
	// ================================================================
	// WHY a separate HTTP port: gRPC is HTTP/2; kubelet probes are HTTP/1.1 — they
	// can't share a plain net.Listener. Separate ports let the gRPC server drain
	// while /readyz returns 503, signalling K8s to stop routing traffic.
	//
	// No readiness checks registered yet (no DB/NATS). The repo phase adds:
	//   healthHandler.AddCheck("db", func(ctx) error { return pool.Ping(ctx) })
	healthHandler := health.New()
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

	// ================================================================
	// 7. START gRPC SERVER (mark SERVING, then block until signal)
	// ================================================================
	srv.SetServing(true)

	grpcAddr := fmt.Sprintf(":%d", cfg.GRPCPort)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("failed to listen on gRPC port", slog.String("addr", grpcAddr), slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("gRPC server listening", slog.String("addr", grpcAddr))

	// Serve blocks until ctx is cancelled (SIGINT/SIGTERM). On cancellation:
	//   1. SetServing(false) → readiness fails → pod leaves LB endpoints
	//   2. GracefulStop (bounded drain) → in-flight RPCs finish
	//   3. hard Stop if drain times out
	//   4. returns here; deferred OTel flush + health shutdown run
	if serveErr := srv.Serve(ctx, lis); serveErr != nil {
		logger.Error("gRPC server error", slog.String("error", serveErr.Error()))
		os.Exit(1)
	}

	logger.Info("billing service stopped cleanly")
}
