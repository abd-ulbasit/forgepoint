// Package observability provides a single initialization point for the
// "three pillars" of observability: traces, metrics, and structured logs.
//
// ============================================================================
// OPENTELEMETRY OBSERVABILITY SETUP
// ============================================================================
//
// WHY OpenTelemetry (OTel):
//
//	OTel is the CNCF standard for instrumenting distributed systems. Before
//	OTel, you'd use separate libraries for each pillar:
//	- Traces: OpenTracing or OpenCensus (now merged into OTel)
//	- Metrics: Prometheus client_golang
//	- Logs: slog/zap/zerolog (still separate, OTel log bridge is newer)
//
//	OTel unifies all three with a single SDK, single context propagation,
//	and vendor-neutral exporters. You instrument once, then switch backends
//	(Jaeger→Tempo, Prometheus→Datadog) by changing config, not code.
//
// THREE PILLARS EXPLAINED:
//
//  1. TRACES (distributed tracing):
//     A trace follows a single request across all services it touches.
//     Each service creates a "span" (a timed operation) linked by trace_id.
//
//     Example trace for a prediction request:
//     ┌─────────────────────────────────────────────────────────────────┐
//     │ trace_id: abc-123                                               │
//     │                                                                 │
//     │ gateway.predict ─────────────────────────────── 45ms           │
//     │   ├─ auth.validate ──── 3ms                                    │
//     │   ├─ serving.predict ──────────── 35ms                         │
//     │   └─ billing.record ── 2ms (async via NATS)                    │
//     └─────────────────────────────────────────────────────────────────┘
//
//  2. METRICS (Prometheus):
//     Numeric aggregates: counters, histograms, gauges.
//     RED method per service:
//     - Rate: requests/sec
//     - Errors: error rate (%)
//     - Duration: latency histogram (p50, p95, p99)
//
//  3. LOGS (structured JSON via slog):
//     Event-level detail linked to traces via trace_id.
//     Enables: "show me all logs for trace abc-123" in Grafana.
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
//	Service → OTel SDK → OTLP Exporter → OTel Collector → Backends
//	                                           │
//	                                 ┌─────────┼─────────┐
//	                                 ▼         ▼         ▼
//	                               Tempo   Prometheus   Loki
//	                              (traces) (metrics)   (logs)
//	                                 └─────────┼─────────┘
//	                                           ▼
//	                                        Grafana
//
// ============================================================================
package observability

import (
	"context"
	"errors"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"

	// The semconv version MUST track the one baked into the SDK's
	// resource.Default() for this otel release. resource.Merge() rejects two
	// resources whose Schema URLs differ, so a mismatch makes Setup() return
	// "conflicting Schema URL" — a RUNTIME failure in every service's
	// composition root, invisible to the compiler. otel 1.44's
	// resource.Default() carries schema 1.41.0.
	//
	// Bump this together with the otel modules. TestSetup_* in this package is
	// what catches it if you don't: it has now caught it on two consecutive
	// otel upgrades (1.40 -> 1.42 -> 1.44), which is the argument for keeping
	// the version pinned here rather than reaching for resource.NewSchemaless
	// to make the mismatch impossible-and-silent.
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
)

// setupCalled (guarded by setupMu) ensures Setup() runs at most once per process.
// A second call returns errSetupDone without replacing the existing providers.
//
// WHY: OTel's global providers (otel.SetTracerProvider, otel.SetMeterProvider)
// are package-level singletons. Calling Setup() a second time would:
//  1. Replace the existing providers, orphaning their exporters/batchers.
//     Those goroutines keep running (leaked) and may continue flushing
//     to a now-redundant connection.
//  2. Lose any spans/metrics created after the first Setup but before the
//     second (the old provider's buffer is abandoned).
//
// TESTS: call ResetForTest() before each test that calls Setup(), so tests
// are independent of each other regardless of execution order.
var (
	errSetupDone = errors.New("observability: Setup already called; call ResetForTest() in tests")
	setupCalled  bool
	setupMu      sync.Mutex // protects setupCalled for ResetForTest
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
	// Production: "otel-collector.fp-infra.svc.cluster.local:4317"
	OTLPEndpoint string

	// OTLPInsecure disables TLS on the OTLP gRPC exporter connection.
	//
	// When false (zero value / production default): the exporter uses TLS.
	//   - Inside an Istio service mesh: TLS is terminated at the sidecar proxy,
	//     so the local connection (pod → sidecar → mesh) may be plaintext, but
	//     the mesh enforces mTLS between pods. Set to true for local-to-sidecar.
	//   - Outside a mesh with a real collector cert: leave false, the exporter
	//     negotiates TLS automatically via transport credentials.
	//
	// When true (local dev): the OTel Collector in docker-compose has no TLS
	// certificate, so we must skip TLS. Set OTLPInsecure: true in your local
	// config or via an env variable that maps to this field.
	//
	// ZERO VALUE IS SECURE: Go's zero value is false, so omitting this field
	// in a config struct defaults to TLS — fail-safe.
	OTLPInsecure bool
}

// Setup initializes OpenTelemetry trace and metric providers.
//
// WHY a single Setup() function:
//
//	Every service needs the exact same initialization sequence. Without this,
//	each service's main.go would have 50+ lines of boilerplate OTel setup.
//	One function, one import, consistent behavior across every service.
//
// RETURNS:
//
//	A shutdown function that flushes all pending telemetry data and releases
//	resources. Call this in main() with defer:
//	  shutdown, err := observability.Setup(ctx, cfg)
//	  if err != nil { log.Fatal(err) }
//	  defer shutdown(context.Background())
//
//	WHY shutdown matters: OTel batches traces and metrics for efficiency.
//	Without flushing on shutdown, the last few seconds of telemetry are lost.
//	In K8s, SIGTERM gives 30s grace period — plenty of time to flush.
//
//	ctx is used while establishing the OTLP exporter connections.
//
// FAILURE MODES:
//   - OTLP endpoint unreachable: Setup succeeds, spans are buffered then
//     dropped when buffer fills. Service continues running — observability
//     is never a hard dependency (you don't want monitoring to cause outages).
//   - Invalid config: Returns error immediately (fail fast).
//
// ResetForTest resets the single-init guard so Setup() can be called again.
// MUST only be called from test code — it is a test-only escape hatch.
// Normal application code should never call this.
func ResetForTest() {
	setupMu.Lock()
	defer setupMu.Unlock()
	setupCalled = false
}

func Setup(ctx context.Context, cfg Config) (shutdown func(ctx context.Context) error, err error) {
	// Single-init guard: return an error if Setup() has already been called.
	// This prevents accidental double-init (e.g., two packages both calling Setup)
	// from silently orphaning the first set of OTel providers and their goroutines.
	setupMu.Lock()
	if setupCalled {
		setupMu.Unlock()
		return nil, errSetupDone
	}
	setupCalled = true
	setupMu.Unlock()

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
			// semconv 1.41 reclassified deployment.environment.name as an Enum
			// and dropped the DeploymentEnvironmentName(string) helper. The
			// attribute key is unchanged, so the emitted telemetry is identical;
			// only the constructor moved.
			semconv.DeploymentEnvironmentNameKey.String(cfg.Environment),
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
	tracerProvider, err := newTracerProvider(ctx, cfg, res)
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, tracerProvider.Shutdown)
	otel.SetTracerProvider(tracerProvider)

	// ================================================================
	// METER PROVIDER
	// ================================================================
	meterProvider, err := newMeterProvider(ctx, cfg, res)
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
func newTracerProvider(ctx context.Context, cfg Config, res *resource.Resource) (*trace.TracerProvider, error) {
	exporter, err := newSpanExporter(ctx, cfg)
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
// METRICS PIPELINE (OTLP → Collector → Prometheus):
//
//	Service → OTel Meter SDK → OTLP push → OTel Collector → Prometheus scrapes
//	                                                                ↓
//	                                                        Grafana dashboards
//
// WHY push OTLP to the collector instead of exposing /metrics per service:
//   - ONE instrumentation path for all three pillars (traces+metrics+logs all
//     speak OTLP to the same collector) — less code, one config to change.
//   - The collector re-exports to Prometheus, so we KEEP the Prometheus pull
//     model where it matters (Prometheus still scrapes the collector with its
//     ServiceMonitor/Alertmanager ecosystem intact) without every service
//     having to run and secure its own scrape endpoint.
//   - Backends become swappable by reconfiguring the collector, not the code.
func newMeterProvider(ctx context.Context, cfg Config, res *resource.Resource) (*metric.MeterProvider, error) {
	exporter, err := newMetricExporter(ctx, cfg)
	if err != nil {
		return nil, err
	}

	mp := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(metric.NewPeriodicReader(exporter)),
	)

	return mp, nil
}

// newSpanExporter builds the trace exporter based on config: OTLP gRPC to the
// collector when an endpoint is set, otherwise stdout for local dev.
//
// TLS BEHAVIOR:
//   - OTLPInsecure=false (default): TLS is enabled on the OTLP connection.
//     The exporter uses the system certificate pool by default. In production,
//     either the collector has a signed cert or Istio mTLS handles it.
//   - OTLPInsecure=true: plaintext. Use for local docker-compose where the
//     collector has no TLS certificate.
//
// WHY no WithPrettyPrint on stdout: the container's stdout is scraped line-by-
// line into Loki, which expects ONE JSON object per line. Pretty-printed,
// multi-line spans would break that parsing. Compact output is the correct
// choice for a log-shipped pipeline.
func newSpanExporter(ctx context.Context, cfg Config) (trace.SpanExporter, error) {
	if cfg.OTLPEndpoint != "" {
		opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint)}
		if cfg.OTLPInsecure {
			// Opt-in plaintext for local dev; production should NOT set this.
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		return otlptracegrpc.New(ctx, opts...)
	}
	return stdouttrace.New()
}

// newMetricExporter builds the metric exporter based on config: OTLP gRPC to the
// collector when an endpoint is set, otherwise stdout for local dev.
func newMetricExporter(ctx context.Context, cfg Config) (metric.Exporter, error) {
	if cfg.OTLPEndpoint != "" {
		opts := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(cfg.OTLPEndpoint)}
		if cfg.OTLPInsecure {
			opts = append(opts, otlpmetricgrpc.WithInsecure())
		}
		return otlpmetricgrpc.New(ctx, opts...)
	}
	return stdoutmetric.New()
}
