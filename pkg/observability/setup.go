// Package observability provides a single initialization point for the
// "three pillars" of observability: traces, metrics, and structured logs.
//
// ============================================================================
// OPENTELEMETRY OBSERVABILITY SETUP
// ============================================================================
//
// WHY OpenTelemetry (OTel):
//   OTel is the CNCF standard for instrumenting distributed systems. Before
//   OTel, you'd use separate libraries for each pillar:
//   - Traces: OpenTracing or OpenCensus (now merged into OTel)
//   - Metrics: Prometheus client_golang
//   - Logs: slog/zap/zerolog (still separate, OTel log bridge is newer)
//
//   OTel unifies all three with a single SDK, single context propagation,
//   and vendor-neutral exporters. You instrument once, then switch backends
//   (Jaeger→Tempo, Prometheus→Datadog) by changing config, not code.
//
// THREE PILLARS EXPLAINED:
//
//   1. TRACES (distributed tracing):
//      A trace follows a single request across all services it touches.
//      Each service creates a "span" (a timed operation) linked by trace_id.
//
//      Example trace for a prediction request:
//      ┌─────────────────────────────────────────────────────────────────┐
//      │ trace_id: abc-123                                               │
//      │                                                                 │
//      │ gateway.predict ─────────────────────────────── 45ms           │
//      │   ├─ auth.validate ──── 3ms                                    │
//      │   ├─ serving.predict ──────────── 35ms                         │
//      │   └─ billing.record ── 2ms (async via NATS)                    │
//      └─────────────────────────────────────────────────────────────────┘
//
//   2. METRICS (Prometheus):
//      Numeric aggregates: counters, histograms, gauges.
//      RED method per service:
//      - Rate: requests/sec
//      - Errors: error rate (%)
//      - Duration: latency histogram (p50, p95, p99)
//
//   3. LOGS (structured JSON via slog):
//      Event-level detail linked to traces via trace_id.
//      Enables: "show me all logs for trace abc-123" in Grafana.
//
// HOW UBER/NETFLIX DO IT:
//   - Uber: Jaeger (they created it) → now migrating to OTel + Tempo
//   - Netflix: Custom tracing → migrating to OTel
//   - Google: Cloud Trace (proprietary but OTel-compatible)
//   - AWS: X-Ray (proprietary, OTel bridge available)
//   - The industry is converging on OTel as THE standard
//
// ARCHITECTURE:
//
//   Service → OTel SDK → OTLP Exporter → OTel Collector → Backends
//                                              │
//                                    ┌─────────┼─────────┐
//                                    ▼         ▼         ▼
//                                  Tempo   Prometheus   Loki
//                                 (traces) (metrics)   (logs)
//                                    └─────────┼─────────┘
//                                              ▼
//                                           Grafana
//
// ============================================================================
package observability

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.39.0"
)

// Config holds the configuration for observability setup.
type Config struct {
	// ServiceName identifies this service in traces, metrics, and logs.
	// Must be unique per service: "auth", "registry", "inference-gateway".
	ServiceName string

	// ServiceVersion is the deployed version (e.g., "v1.2.3", git SHA).
	// Shows up in trace metadata — helps identify which version produced a span.
	ServiceVersion string

	// Environment: "dev", "staging", "production".
	// Used as a resource attribute for filtering in Grafana.
	Environment string

	// OTLPEndpoint is the OpenTelemetry Collector gRPC endpoint.
	// If empty, falls back to stdout exporters (useful for testing/dev).
	// Production: "otel-collector.goml-infra.svc.cluster.local:4317"
	OTLPEndpoint string
}

// Setup initializes OpenTelemetry trace and metric providers.
//
// WHY a single Setup() function:
//   Every service needs the exact same initialization sequence. Without this,
//   each service's main.go would have 50+ lines of boilerplate OTel setup.
//   One function, one import, consistent behavior across all 9 services.
//
// RETURNS:
//   A shutdown function that flushes all pending telemetry data and releases
//   resources. Call this in main() with defer:
//     shutdown, err := observability.Setup(cfg)
//     if err != nil { log.Fatal(err) }
//     defer shutdown(context.Background())
//
//   WHY shutdown matters: OTel batches traces and metrics for efficiency.
//   Without flushing on shutdown, the last few seconds of telemetry are lost.
//   In K8s, SIGTERM gives 30s grace period — plenty of time to flush.
//
// FAILURE MODES:
//   - OTLP endpoint unreachable: Setup succeeds, spans are buffered then
//     dropped when buffer fills. Service continues running — observability
//     is never a hard dependency (you don't want monitoring to cause outages).
//   - Invalid config: Returns error immediately (fail fast).
func Setup(cfg Config) (shutdown func(ctx context.Context) error, err error) {
	var shutdownFuncs []func(context.Context) error

	// shutdown combines all cleanup functions into one.
	// Collects errors from all providers (doesn't stop on first error).
	shutdown = func(ctx context.Context) error {
		var errs []error
		for _, fn := range shutdownFuncs {
			if fnErr := fn(ctx); fnErr != nil {
				errs = append(errs, fnErr)
			}
		}
		return errors.Join(errs...)
	}

	// On error, call shutdown to clean up any providers that were initialized
	// before the failure point.
	handleErr := func(inErr error) {
		err = errors.Join(inErr, shutdown(context.Background()))
	}

	// ================================================================
	// RESOURCE
	// ================================================================
	// A Resource describes the entity producing telemetry. It's attached
	// to every trace span and metric data point. This is how Grafana
	// filters by service: `{service.name="auth"}`.
	//
	// Semantic conventions (semconv) are OTel's standardized attribute
	// names. Using semconv ensures all services label their telemetry
	// consistently, so Grafana queries work across services.
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion(cfg.ServiceVersion),
			semconv.DeploymentEnvironmentName(cfg.Environment),
		),
	)
	if err != nil {
		return nil, err
	}

	// ================================================================
	// PROPAGATION
	// ================================================================
	// Propagators inject/extract trace context across process boundaries.
	//
	// W3C TraceContext: The standard propagation format.
	//   HTTP header: `traceparent: 00-<trace_id>-<span_id>-<flags>`
	//   gRPC metadata: same header, automatically propagated by otelgrpc
	//
	// W3C Baggage: Key-value pairs propagated alongside trace context.
	//   Use case: propagate user_id, team, API key across services without
	//   passing them as explicit RPC arguments.
	//
	// Both are needed: TraceContext for distributed tracing continuity,
	// Baggage for business context propagation.
	prop := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
	otel.SetTextMapPropagator(prop)

	// ================================================================
	// TRACE PROVIDER
	// ================================================================
	tracerProvider, err := newTracerProvider(cfg, res)
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, tracerProvider.Shutdown)
	otel.SetTracerProvider(tracerProvider)

	// ================================================================
	// METER PROVIDER
	// ================================================================
	meterProvider, err := newMeterProvider(cfg, res)
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, meterProvider.Shutdown)
	otel.SetMeterProvider(meterProvider)

	return shutdown, nil
}

// newTracerProvider creates a trace provider with the appropriate exporter.
//
// WHY BatchSpanProcessor over SimpleSpanProcessor:
//   - Simple: exports each span immediately (synchronous, blocks the handler)
//   - Batch: buffers spans, exports in bulk every 5s or when buffer fills
//   - Batch is 10-100x less overhead in production (fewer network calls)
//   - We use Simple only for tests (immediate export for assertion)
//
// EXPORTER STRATEGY:
//   - OTLPEndpoint set → OTLP gRPC exporter (production: sends to OTel Collector)
//   - OTLPEndpoint empty → stdout exporter (dev/test: prints spans to console)
//   Future: add OTLP exporter when we set up the OTel Collector in docker-compose
func newTracerProvider(cfg Config, res *resource.Resource) (*trace.TracerProvider, error) {
	// For now, use stdout exporter. When OTel Collector is set up (Task 0.9),
	// we'll add OTLP gRPC exporter conditioned on cfg.OTLPEndpoint.
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		return nil, err
	}

	tp := trace.NewTracerProvider(
		// BatchSpanProcessor batches spans and exports them periodically.
		// Default: export every 5s or when 2048 spans are buffered.
		trace.WithBatcher(exporter),
		trace.WithResource(res),
		// Sampler controls what percentage of traces are recorded.
		// AlwaysSample for dev/test. In production, switch to
		// TraceIDRatioBased(0.1) for 10% sampling to reduce cost.
		trace.WithSampler(trace.AlwaysSample()),
	)

	return tp, nil
}

// newMeterProvider creates a metric provider with the appropriate exporter.
//
// METRICS PIPELINE:
//   Service → OTel Meter SDK → Prometheus Exporter → /metrics endpoint
//                                                      ↓
//                                               Prometheus scrapes
//                                                      ↓
//                                               Grafana dashboards
//
// WHY Prometheus-style (pull) over OTLP push:
//   - K8s ecosystem is Prometheus-native (ServiceMonitor, PodMonitor)
//   - Prometheus has mature alerting (Alertmanager) and Grafana integration
//   - Pull model: Prometheus decides scrape interval, no back-pressure on services
//   - OTLP push: better for serverless/short-lived processes (not our case)
func newMeterProvider(cfg Config, res *resource.Resource) (*metric.MeterProvider, error) {
	// Use stdout exporter for now. When Prometheus is set up (Task 0.9),
	// we'll add the Prometheus exporter that serves /metrics.
	exporter, err := stdoutmetric.New()
	if err != nil {
		return nil, err
	}

	mp := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(metric.NewPeriodicReader(exporter)),
	)

	return mp, nil
}
