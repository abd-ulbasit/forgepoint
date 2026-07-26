// Package main is the composition root for the BFF (Backend-for-Frontend).
//
// ============================================================================
// WIRING — mirrors services/auth/cmd/server/main.go's patterns
// ============================================================================
//
// The BFF is the browser's single entry point to the platform: it speaks
// JSON/HTTP + SSE to the SPA and gRPC to the domain services it fronts (ADR
// 0001). It
// holds ZERO business logic — only composition, protocol/stream translation, and
// browser-session concerns (CORS, token forwarding).
//
// STARTUP SEQUENCE:
//
//	1. Structured logger (slog JSON → stdout → Loki)
//	2. Load config (FP_* env vars → BFFConfig: HTTP port + per-service gRPC
//	   addresses + CORS origins)
//	3. Setup OpenTelemetry (traces → Tempo, metrics → Prom)
//	4. Dial the downstream gRPC services (lazy, connection-reusing) — does NOT
//	   block on reachability, so a down service can't stop BFF startup
//	5. Construct handlers (each given the minimal typed stub it needs)
//	6. Build the HTTP mux (Go 1.22 ServeMux pattern routing) + middleware chain
//	   (recover → log → CORS; auth on the protected sub-tree)
//	7. Health server endpoints (/healthz liveness; /readyz checks it can reach
//	   auth + registry)
//	8. Serve, blocking until SIGTERM/SIGINT
//	9. Graceful shutdown: stop accepting HTTP, drain, then close gRPC conns
//
// WHY NO JWT SECRET HERE: unlike auth, the BFF does NOT validate tokens — it
// FORWARDS the caller's token to the services, which validate it with the shared
// secret. So the BFF needs no FP_JWT_SECRET; it needs only the CORS allowlist
// (browser-session config) and the service addresses (composition config).
// ============================================================================
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/abd-ulbasit/forgepoint/pkg/config"
	"github.com/abd-ulbasit/forgepoint/pkg/health"
	"github.com/abd-ulbasit/forgepoint/pkg/observability"
	"github.com/abd-ulbasit/forgepoint/services/bff/internal/clients"
	"github.com/abd-ulbasit/forgepoint/services/bff/internal/handlers"
	"github.com/abd-ulbasit/forgepoint/services/bff/internal/router"
)

// BFFConfig is the BFF's env-driven configuration. It embeds BaseConfig (Port,
// GRPCPort, LogLevel, OTelEndpoint, ...) and adds the per-service addresses and
// CORS origins. Defaults target the in-cluster DNS so a vanilla deploy works;
// every field is overridable for local dev (point at localhost:port-forwards)
// or other namespaces.
type BFFConfig struct {
	config.BaseConfig

	// AllowedOrigins is the CORS allowlist (comma-separated env → []string via
	// pkg/config). NEVER "*": a wildcard with credentials is forbidden and unsafe.
	// Default is the SPA's local dev origin; production sets the real UI origin.
	AllowedOrigins []string `env:"CORS_ALLOWED_ORIGINS" default:"http://localhost:5173"`

	// Per-service gRPC dial targets. Defaults follow
	// fp-<svc>.fp-system.svc.cluster.local:9090 (model-serving would be
	// fp-models, but the BFF doesn't call it directly — see clients.go).
	AuthAddr         string `env:"AUTH_ADDR" default:"fp-auth.fp-system.svc.cluster.local:9090"`
	RegistryAddr     string `env:"REGISTRY_ADDR" default:"fp-registry.fp-system.svc.cluster.local:9090"`
	PipelineAddr     string `env:"PIPELINE_ADDR" default:"fp-pipeline-orchestrator.fp-system.svc.cluster.local:9090"`
	ExperimentAddr   string `env:"EXPERIMENT_ADDR" default:"fp-experiment-tracker.fp-system.svc.cluster.local:9090"`
	MonitorAddr      string `env:"MONITOR_ADDR" default:"fp-model-monitor.fp-system.svc.cluster.local:9090"`
	BillingAddr      string `env:"BILLING_ADDR" default:"fp-billing.fp-system.svc.cluster.local:9090"`
	NotificationAddr string `env:"NOTIFICATION_ADDR" default:"fp-notification.fp-system.svc.cluster.local:9090"`
	// AIGatewayAddr is the M7 LLM gateway. The BFF relays its ChatCompletion
	// server-stream to the browser as SSE (the chat playground) and proxies its
	// ListProviders/GetUsage RPCs. Same fp-system/:9090 grain as the others.
	AIGatewayAddr string `env:"AI_GATEWAY_ADDR" default:"fp-ai-gateway.fp-system.svc.cluster.local:9090"`

	// HTTPPort is the SPA-facing HTTP port. We don't reuse BaseConfig.GRPCPort
	// (the BFF serves no gRPC) and keep Port for the health server, matching the
	// other services' split. Default 8081 to avoid clashing with a co-located
	// service's 8080 health port in local dev.
	HTTPPort int `env:"HTTP_PORT" default:"8081"`

	// ---- SSE (server-stream relay) resource caps ----------------------------
	// Each /executions/{id}/watch opens one upstream gRPC stream that lives as
	// long as the browser keeps the EventSource open. Without caps an
	// AUTHENTICATED client can open thousands of these, each amplified into a
	// long-lived gRPC stream against the orchestrator (gRPC amplification DoS).
	// These bounds make that finite. See handlers.SSELimits.
	//
	// SSEMaxGlobal caps total concurrent SSE streams across ALL users (process
	// fuse). SSEMaxPerUser caps per-token-subject so one user cannot consume the
	// whole global budget. SSEMaxLifetime force-closes any single stream after a
	// hard ceiling so a stuck client that keeps the heartbeat alive is still
	// reaped (defense against a slow-loris that answers pings forever).
	SSEMaxGlobal   int           `env:"SSE_MAX_GLOBAL" default:"256"`
	SSEMaxPerUser  int           `env:"SSE_MAX_PER_USER" default:"8"`
	SSEMaxLifetime time.Duration `env:"SSE_MAX_LIFETIME" default:"30m"`
}

func main() {
	// 1. LOGGER ----------------------------------------------------------------
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// 2. CONFIG ----------------------------------------------------------------
	cfg, err := config.Load[BFFConfig]("FP")
	if err != nil {
		logger.Error("failed to load config", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("config loaded",
		slog.Int("http_port", cfg.HTTPPort),
		slog.Int("health_port", cfg.Port),
		slog.Any("cors_allowed_origins", cfg.AllowedOrigins),
	)

	// 3. SIGNAL CONTEXT + OTEL -------------------------------------------------
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	otelShutdown, err := observability.Setup(ctx, observability.Config{
		ServiceName:    "bff",
		ServiceVersion: "dev",
		Environment:    "local",
		OTLPEndpoint:   cfg.OTelEndpoint,
		OTLPInsecure:   true,
	})
	if err != nil {
		logger.Error("failed to setup observability", slog.String("error", err.Error()))
		os.Exit(1)
	}
	// LAST to run (LIFO): flush telemetry with a fresh, bounded context.
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if shutdownErr := otelShutdown(shutdownCtx); shutdownErr != nil {
			logger.Error("otel shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// 4. DIAL DOWNSTREAM gRPC SERVICES -----------------------------------------
	// Lazy dials: this returns immediately even if some services are unreachable,
	// so the BFF starts and degrades gracefully (the dashboard tiles handle it).
	cl, err := clients.Dial(logger, clients.Addresses{
		Auth:         cfg.AuthAddr,
		Registry:     cfg.RegistryAddr,
		Pipeline:     cfg.PipelineAddr,
		Experiment:   cfg.ExperimentAddr,
		Monitor:      cfg.MonitorAddr,
		Billing:      cfg.BillingAddr,
		Notification: cfg.NotificationAddr,
		AIGateway:    cfg.AIGatewayAddr,
	})
	if err != nil {
		logger.Error("failed to dial downstream services", slog.String("error", err.Error()))
		os.Exit(1)
	}
	// Close gRPC conns near-last (after HTTP drains, so no in-flight handler uses
	// a closing conn). Registered before health/HTTP defers → runs after them.
	defer func() {
		if closeErr := cl.Close(); closeErr != nil {
			logger.Error("error closing gRPC clients", slog.String("error", closeErr.Error()))
		}
	}()

	// 5+6. BUILD THE ROUTER (handlers + middleware) ----------------------------
	// router.New constructs every handler from the typed stubs and assembles the
	// Go 1.22 ServeMux with method+path patterns, public vs protected sub-trees,
	// and the middleware chain.
	httpHandler := router.New(router.Config{
		Clients:        cl,
		Logger:         logger,
		AllowedOrigins: cfg.AllowedOrigins,
		SSELimits: handlers.SSELimits{
			MaxGlobal:   cfg.SSEMaxGlobal,
			MaxPerUser:  cfg.SSEMaxPerUser,
			MaxLifetime: cfg.SSEMaxLifetime,
		},
	})

	// HTTP server with TIMEOUTS (anti-slowloris). Read/Write/Idle bound how long a
	// slow or malicious client can hold a connection. NOTE: WriteTimeout would
	// kill the long-lived SSE stream, so we DO NOT set a global WriteTimeout;
	// instead we bound headers via ReadHeaderTimeout (slowloris protection that
	// doesn't fight streaming) and rely on the SSE handler honoring ctx for
	// teardown. Read/Idle timeouts still apply.
	httpServer := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler:           httpHandler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		IdleTimeout:       60 * time.Second,
		// No WriteTimeout (SSE needs an open write side); ReadHeaderTimeout is the
		// slowloris guard.
	}

	// 7. HEALTH SERVER ---------------------------------------------------------
	// /readyz checks the BFF can REACH its two most critical upstreams (auth =
	// login, registry = the dashboard's primary tile). We don't health-check all
	// seven: the dashboard already degrades per-tile, so a down monitor/billing
	// shouldn't pull the whole BFF out of the Service. Auth + registry are the
	// "can the app do its core job" floor. The check uses gRPC connectivity state
	// (conn is reachable) bounded by the health package's per-check timeout.
	healthHandler := health.New()
	healthHandler.AddCheck("auth", cl.AuthReachable)
	healthHandler.AddCheck("registry", cl.RegistryReachable)

	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", healthHandler.LivenessHandler())
	healthMux.HandleFunc("/readyz", healthHandler.ReadinessHandler())
	healthServer := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           healthMux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		logger.Info("health server listening", slog.String("addr", healthServer.Addr))
		if serveErr := healthServer.ListenAndServe(); serveErr != nil && serveErr != http.ErrServerClosed {
			logger.Error("health server error", slog.String("error", serveErr.Error()))
		}
	}()
	// Stop health server before gRPC conns close (LIFO): readiness stops answering
	// 200 before the deps it checks are torn down.
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if shutdownErr := healthServer.Shutdown(shutdownCtx); shutdownErr != nil {
			logger.Error("health server shutdown error", slog.String("error", shutdownErr.Error()))
		}
	}()

	// 8. SERVE HTTP (run in goroutine; block on ctx) ---------------------------
	go func() {
		logger.Info("bff http server listening", slog.String("addr", httpServer.Addr))
		if serveErr := httpServer.ListenAndServe(); serveErr != nil && serveErr != http.ErrServerClosed {
			logger.Error("http server error", slog.String("error", serveErr.Error()))
			stop() // unblock main so the process exits and K8s restarts it
		}
	}()

	// Block until SIGINT/SIGTERM (K8s pod termination).
	<-ctx.Done()
	logger.Info("shutdown signal received; draining http server")

	// 9. GRACEFUL HTTP SHUTDOWN ------------------------------------------------
	// Stop accepting new connections and let in-flight requests finish (bounded).
	// This runs BEFORE the deferred gRPC-conn close (which runs as the stack
	// unwinds), so no in-flight HTTP handler touches a closed gRPC conn.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if shutdownErr := httpServer.Shutdown(shutdownCtx); shutdownErr != nil {
		logger.Error("http server shutdown error", slog.String("error", shutdownErr.Error()))
		// Force-close as a last resort so we don't hang the pod past its grace period.
		_ = httpServer.Close()
	}

	logger.Info("bff stopped cleanly")
	// Deferred teardown now runs LIFO: health server stop → gRPC conns close →
	// OTel flush. (httpServer.Shutdown ran above, explicitly, before this point.)
}
