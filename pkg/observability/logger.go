package observability

import (
	"log/slog"
	"os"
	"strings"
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
//   When a handler processes a request, the context carries an OTel span.
//   Middleware extracts trace_id and span_id from the span context and adds
//   them as slog attributes. This links logs to distributed traces in Tempo:
//     {"level":"INFO","msg":"request handled","trace_id":"abc123","service":"auth"}
//   → Click trace_id in Grafana → jump from Loki log to Tempo trace.
//
// ============================================================================

// NewLogger creates a structured JSON logger with service metadata.
//
// The logger writes JSON to stdout (12-Factor App: logs are event streams,
// not files). The container runtime (Docker/K8s) captures stdout and routes
// to the log aggregator (Loki, CloudWatch, etc.).
//
// The service name is included as a default attribute so every log line
// from this service is identifiable in a multi-service log aggregator.
func NewLogger(serviceName, level string) *slog.Logger {
	logLevel := parseLogLevel(level)

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
		// AddSource includes the file:line in every log entry.
		// Useful for debugging but adds ~10% overhead. Enabled only at debug level.
		AddSource: logLevel == slog.LevelDebug,
	})

	return slog.New(handler).With(
		slog.String("service", serviceName),
	)
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
