// Package main is the entrypoint for the Notification service.
//
// ============================================================================
// WIRING — THE COMPOSITION ROOT
// ============================================================================
//
// In Clean Architecture, main.go is the "composition root" — the only place
// where every layer is imported together and wired into a running program. It
// knows about config, observability, domain, handler (and, in later phases, the
// Postgres/HTTP/NATS adapters); none of those layers know about each other except
// through the interfaces (ports) the domain defines.
//
// STARTUP SEQUENCE:
//
//	┌──────────────────────────────────────────────────────────────────┐
//	│  1. Structured logger (slog JSON → stdout → Loki)                  │
//	│  2. Load config (FP_* env vars → NotificationConfig)               │
//	│  3. Setup OpenTelemetry (traces → Tempo, metrics → Prometheus)     │
//	│  4. Build gRPC server via grpcutil.NewServer (interceptor chain)   │
//	│  5. Register NotificationHandler                                   │
//	│  6. HTTP health server (/healthz liveness, /readyz readiness)      │
//	│  7. Start gRPC server (blocks until SIGTERM/SIGINT)                │
//	│  8. Graceful shutdown (OTel flush → gRPC drain → health 503)       │
//	└──────────────────────────────────────────────────────────────────┘
//
// WHAT'S WIRED NOW (scaffold phase):
//   - NotificationHandler with a nil domain service (the embedded
//     UnimplementedNotificationServiceServer handles all RPCs, returning
//     codes.Unimplemented — correct behavior).
//   - No Postgres pool, no Redis, no NATS subscription yet.
//
// WHAT ARRIVES LATER (the rest of the service):
//
//   - Postgres adapter implementing domain.NotificationRepository +
//     domain.PreferenceRepository (the inbox, delivery_log, preferences tables).
//
//   - Redis adapter implementing domain.IdempotencyStore (event + sync dedup).
//
//   - HTTP/Slack/SMTP delivery adapter implementing domain.Notifier — THIS is
//     where the mandated SSRF guard (https-only, hooks.slack.com allowlist,
//     private/loopback/link-local IP denylist, pinned-IP connect) + retries +
//     per-URL circuit breaker live.
//
//   - NATS JetStream consumer on `fp.>` (the choreography reactor's imperative
//     shell): for each event it loads the recipient's prefs, calls
//     svc.ReactToEvent, then executes the RoutingDecision (write inbox row, fire
//     the Notifier per delivering channel, record SUPPRESSED attempts) and
//     publishes fp.notifications.delivered / fp.notifications.failed
//     (forgepoint.events.v1) for delivery-health consumers.
//
//     Once those land, the wiring below grows:
//     pool := pgxpool.New(ctx, cfg.DatabaseURL)
//     notifRepo := postgres.NewNotificationRepo(pool)
//     prefRepo  := postgres.NewPreferenceRepo(pool)
//     idem      := redisadapter.NewIdempotencyStore(redisClient)
//     notifier  := delivery.NewNotifier(...)   // SSRF guard lives here
//     svc := domain.NewNotificationService(notifRepo, prefRepo, idem, notifier,
//     systemClock{}, uuidGen{})
//     notificationv1.RegisterNotificationServiceServer(srv.GRPC,
//     handler.NewNotificationHandler(svc))
//     healthHandler.AddCheck("db", func(ctx) error { return pool.Ping(ctx) })
//     healthHandler.AddCheck("nats", natsConn.Ping)
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

	notificationv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/notification/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/config"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/health"
	"github.com/abd-ulbasit/forgepoint/pkg/observability"
	"github.com/abd-ulbasit/forgepoint/services/notification/internal/handler"
)

// NotificationConfig extends BaseConfig with notification-specific configuration.
//
// WHY embed BaseConfig: every Forgepoint service needs Port, GRPCPort, LogLevel,
// OTelEndpoint, NATSUrl, DatabaseURL. config.Load[NotificationConfig]("FP") reads
// FP_PORT, FP_GRPC_PORT, FP_NATS_URL, etc. via reflection (see pkg/config).
//
// The notification-specific fields below are NOT required during this scaffold
// phase (the adapters that consume them arrive later), so they are optional with
// safe defaults — the service must still boot with only the base config set, so
// the scaffold is runnable today.
type NotificationConfig struct {
	config.BaseConfig

	// RedisURL backs the IdempotencyStore (event + sync dedup) once the Redis
	// adapter lands. Optional here; the dedup adapter validates it at wire time.
	// K8s: a plain ConfigMap value (no secret) pointing at the in-cluster Redis.
	RedisURL string `env:"REDIS_URL" default:"redis://localhost:6379"`

	// SMTPAddr is the SMTP server for the EMAIL channel (host:port). Optional —
	// the plan allows mocking email in non-prod, so an unset value is valid until
	// the EMAIL delivery adapter is enabled.
	SMTPAddr string `env:"SMTP_ADDR"`
}

func main() {
	// ================================================================
	// 1. STRUCTURED LOGGER
	// ================================================================
	// slog (stdlib, Go 1.21+) with JSON output: container runtimes capture stdout
	// and ship it to Loki, and JSON is parse-friendly for the aggregator. Zero
	// external deps; fast enough for a network-IO-bound service.
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
	)

	// ================================================================
	// 3. OPENTELEMETRY
	// ================================================================
	// Setup initializes the trace provider (→ Tempo) and meter provider
	// (→ Prometheus via the OTel Collector). All spans/metrics from this process
	// flow through these providers — including, later, the NATS consumer spans
	// that complete the serve→monitor→notify trace across the async hop.
	//
	// WHY NotifyContext here: we pass the signal-aware context to Setup so OTLP
	// connection setup respects a SIGTERM during startup (abort cleanly). The same
	// ctx drives the graceful shutdown below.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	otelShutdown, err := observability.Setup(ctx, observability.Config{
		ServiceName:    "notification",
		ServiceVersion: "dev",
		Environment:    "local",
		OTLPEndpoint:   cfg.OTelEndpoint,
		// OTLPInsecure is sourced from the (local) deployment posture: plaintext to
		// the collector in docker-compose where there is no TLS cert. In a real
		// cluster this would be driven by config/mTLS via the mesh.
		OTLPInsecure: true,
	})
	if err != nil {
		logger.Error("failed to setup observability", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer func() {
		// Flush pending spans/metrics on shutdown (OTel batches internally, so
		// without this the last few seconds of telemetry are lost). K8s grants 30s
		// on SIGTERM — ample time to flush.
		if shutdownErr := otelShutdown(context.Background()); shutdownErr != nil {
			logger.Error("otel shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// ================================================================
	// 4. BUILD gRPC SERVER
	// ================================================================
	// grpcutil.NewServer applies the standard interceptor chain
	// (recovery → logging → tracing → auth). WHY no WithAuthValidator wired yet:
	// the auth interceptor needs a token validator (the Auth service's public key
	// / introspection), which is injected when the platform's shared auth client
	// is available. The control-plane RPCs here are all caller-scoped — once the
	// validator is wired, the interceptor populates grpcutil.Claims and each
	// handler reads RecipientUserID/UserID from it (never from the request body).
	//
	// WithReflection: enabled so grpcurl/grpcui can introspect the service during
	// development without the .proto files locally.
	srv := grpcutil.NewServer(
		grpcutil.WithLogger(logger),
		grpcutil.WithReflection(),
	)

	// ================================================================
	// 5. REGISTER NOTIFICATION HANDLER
	// ================================================================
	// The handler embeds UnimplementedNotificationServiceServer — it fully
	// satisfies notificationv1.NotificationServiceServer right now; all RPCs return
	// codes.Unimplemented until the handler phase overrides them. We pass nil as
	// the domain service for the scaffold phase; the embedded Unimplemented methods
	// never dereference svc, so this is safe and the server is runnable today.
	notificationv1.RegisterNotificationServiceServer(srv.GRPC, handler.NewNotificationHandler(nil))
	logger.Info("notification handler registered")

	// ================================================================
	// 6. HEALTH SERVER (HTTP)
	// ================================================================
	// The HTTP health server runs on Port (default 8080):
	//   GET /healthz → liveness (200 while the process is alive)
	//   GET /readyz  → readiness (checks dependencies once they're wired)
	//
	// WHY a separate HTTP port: gRPC is HTTP/2; kubelet probes are HTTP/1.1 — they
	// can't share a net.Listener. Separate ports let the gRPC server drain while
	// /readyz returns 503, telling K8s to pull the pod from the LB endpoints.
	//
	// No readiness checks registered yet (no DB/NATS). Later phases add:
	//   healthHandler.AddCheck("db", func(ctx) error { return pool.Ping(ctx) })
	//   healthHandler.AddCheck("nats", natsConn.Ping)
	healthHandler := health.New()
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", healthHandler.LivenessHandler())
	healthMux.HandleFunc("/readyz", healthHandler.ReadinessHandler())

	healthAddr := fmt.Sprintf(":%d", cfg.Port)
	healthServer := &http.Server{
		Addr:    healthAddr,
		Handler: healthMux,
	}

	// Run the health server in a goroutine so it doesn't block the gRPC server.
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
	// Mark the gRPC health service SERVING so K8s readiness passes. (grpcutil flips
	// it to NOT_SERVING on SIGTERM before draining in-flight RPCs.)
	srv.SetServing(true)

	grpcAddr := fmt.Sprintf(":%d", cfg.GRPCPort)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("failed to listen on gRPC port", slog.String("addr", grpcAddr), slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("gRPC server listening", slog.String("addr", grpcAddr))

	// Serve blocks until ctx is cancelled (SIGINT/SIGTERM via NotifyContext). On
	// cancellation: readiness fails → pod leaves LB endpoints → GracefulStop drains
	// in-flight RPCs → hard Stop if drain times out → returns here → deferred OTel +
	// health shutdown run.
	if serveErr := srv.Serve(ctx, lis); serveErr != nil {
		logger.Error("gRPC server error", slog.String("error", serveErr.Error()))
		os.Exit(1)
	}

	logger.Info("notification service stopped cleanly")
}
