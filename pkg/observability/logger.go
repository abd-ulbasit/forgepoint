package observability

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

// ============================================================================
// STRUCTURED LOGGING
// ============================================================================
//
// WHY slog over zerolog/zap:
//   Go 1.21 added slog to the standard library. Before that, the community
//   was split between zerolog (zero-allocation, fastest), zap (Uber, popular),
//   and logrus (oldest, slowest). slog unifies the ecosystem:
//   - Standard library = no external dependency, guaranteed maintenance
//   - Structured by design: key-value pairs, not format strings
//   - Pluggable handlers: JSON (production), text (dev), custom
//   - 3rd-party backends (Datadog, Loki) provide slog handlers
//
// ALTERNATIVES:
//   ┌──────────────┬─────────────────────────────┬──────────────────────────┐
//   │ Library       │ Pros                        │ Cons                     │
//   ├──────────────┼─────────────────────────────┼──────────────────────────┤
//   │ ✅ slog      │ Stdlib, structured, fast    │ Newer (Go 1.21+), fewer │
//   │ (stdlib)      │ enough, pluggable handlers  │ features than zap        │
//   ├──────────────┼─────────────────────────────┼──────────────────────────┤
//   │ zap (Uber)   │ Battle-tested, fast, rich   │ External dep, verbose    │
//   │               │ field types                 │ API (zap.String("k","v"))│
//   ├──────────────┼─────────────────────────────┼──────────────────────────┤
//   │ zerolog      │ Zero-alloc, fastest         │ External dep, method     │
//   │               │                             │ chaining can be awkward  │
//   └──────────────┴─────────────────────────────┴──────────────────────────┘
//
// WHY JSON format for production:
//   Log aggregators (Loki, Elasticsearch, CloudWatch) parse JSON natively.
//   Text logs require regex parsing (fragile, slow). JSON logs also support
//   structured queries like: {service="auth"} |= `"level":"ERROR"` in Loki.
//
// HOW TRACE CONTEXT GETS INTO LOGS:
//   Every log call that passes a context carrying an OTel span (slog's
//   LogAttrs/InfoContext/etc.) automatically gets trace_id and span_id added,
//   via the traceContextHandler below. This links logs to distributed traces:
//     {"level":"INFO","msg":"request handled","trace_id":"abc123","service":"auth"}
//   → Click trace_id in Grafana → jump from Loki log to Tempo trace.
//   The interceptor/handler must use the *Context logging methods for this to
//   fire (a bare logger.Info with no ctx has no span to read).
//
// ============================================================================

// NewLogger creates a structured JSON logger with service metadata and
// automatic trace correlation.
//
// The logger writes JSON to stdout (12-Factor App: logs are event streams,
// not files). The container runtime (Docker/K8s) captures stdout and routes
// to the log aggregator (Loki, CloudWatch, etc.).
//
// The service name is included as a default attribute so every log line
// from this service is identifiable in a multi-service log aggregator.
func NewLogger(serviceName, level string) *slog.Logger {
	logLevel := parseLogLevel(level)

	base := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
		// AddSource includes the file:line in every log entry.
		// Useful for debugging but adds ~10% overhead. Enabled only at debug level.
		AddSource: logLevel == slog.LevelDebug,
	})

	// Wrap the JSON handler so trace_id/span_id are injected from context.
	handler := traceContextHandler{Handler: base}

	return slog.New(handler).With(
		slog.String("service", serviceName),
	)
}

// traceContextHandler is a slog.Handler decorator that adds trace_id and span_id
// from the OTel span in the record's context. This is what makes Loki↔Tempo
// correlation work without a full OTel LoggerProvider — we keep the simple
// stdout-JSON-to-Loki path and just enrich each line.
//
// WHY override WithAttrs/WithGroup: slog calls these to derive child handlers
// (e.g., logger.With(...)). If we embedded slog.Handler and didn't override
// them, the child would be the UNWRAPPED inner handler and we'd silently lose
// trace correlation after any .With() call. So we re-wrap on each.
type traceContextHandler struct {
	slog.Handler
}

func (h traceContextHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.Handler.Handle(ctx, r)
}

func (h traceContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return traceContextHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h traceContextHandler) WithGroup(name string) slog.Handler {
	return traceContextHandler{Handler: h.Handler.WithGroup(name)}
}

// parseLogLevel converts a string log level to slog.Level.
// Case-insensitive. Defaults to Info for unrecognized values.
func parseLogLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
