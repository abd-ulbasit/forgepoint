package httpx

import (
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"
)

// ============================================================================
// CROSS-CUTTING HTTP MIDDLEWARE: CORS, request logging, panic recovery.
// ============================================================================
//
// Ordering note (see cmd/server): recovery is OUTERMOST so it catches a panic in
// any inner layer (including logging); then logging; then CORS; then auth on the
// protected sub-tree. The mux is innermost.
// ============================================================================

// CORS returns middleware that answers browser preflight and stamps the
// Access-Control-* headers — LOCKED to an explicit allowlist of origins, never
// "*". A wildcard origin combined with credentials is forbidden by the Fetch
// spec AND is a security hole (any site could call the API with the user's
// cookie). We echo back the request's Origin only if it is in the configured
// allowlist, and set Allow-Credentials so the cookie-based SPA flow works.
//
// WHY echo-the-origin instead of emitting the whole list: the CORS response
// header takes a SINGLE origin; to support N allowed origins you must reflect
// the matching one. Reflecting only allowlisted origins keeps it safe.
func CORS(allowedOrigins []string) func(http.Handler) http.Handler {
	allowed := make(map[string]struct{}, len(allowedOrigins))
	for _, o := range allowedOrigins {
		allowed[o] = struct{}{}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin != "" {
				if _, ok := allowed[origin]; ok {
					w.Header().Set("Access-Control-Allow-Origin", origin)
					// Vary: Origin so caches don't serve one origin's CORS headers
					// to another origin.
					w.Header().Add("Vary", "Origin")
					w.Header().Set("Access-Control-Allow-Credentials", "true")
					w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
					w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
					w.Header().Set("Access-Control-Max-Age", "600")
				}
			}
			// Preflight: answer OPTIONS here, do not pass to handlers.
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// statusRecorder captures the status code an inner handler wrote so the logging
// middleware can record it. http.ResponseWriter does not expose the code after
// the fact, so we wrap WriteHeader.
type statusRecorder struct {
	http.ResponseWriter
	status int
	// flushSupported lets us pass Flush through for SSE (text/event-stream) so
	// wrapping the writer doesn't break streaming.
	wrote bool
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.wrote = true
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if !s.wrote {
		s.status = http.StatusOK
		s.wrote = true
	}
	return s.ResponseWriter.Write(b)
}

// Flush forwards to the underlying writer's Flusher so SSE streaming works
// through the recorder. If the underlying writer is not a Flusher this is a
// no-op (the http.Server's default writer always supports Flush for HTTP/1.1).
func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Logging records one structured line per request: method, path, status,
// duration. It NEVER logs the Authorization header, cookies, or the request body
// — those carry the bearer token and PII (the no-token-logging rule).
func Logging(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)
			logger.Info("http request",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path), // path only; query may carry filters but no secrets
				slog.Int("status", rec.status),
				slog.Duration("duration", time.Since(start)),
			)
		})
	}
}

// Recover is the OUTERMOST middleware: it converts a panic in any handler into a
// sanitized 500 instead of crashing the process / leaking a stack to the client.
// The stack goes to the server log only. This mirrors grpcutil's recovery
// interceptor on the gRPC side — one safety net per protocol edge.
func Recover(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					logger.Error("panic recovered in http handler",
						slog.Any("panic", rec),
						slog.String("path", r.URL.Path),
						slog.String("stack", string(debug.Stack())),
					)
					// The handler may have already written a header; guard with a
					// best-effort write. If headers were sent, this is a no-op write
					// that at least doesn't crash.
					WriteError(w, http.StatusInternalServerError, "internal error")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// Chain composes middlewares so the FIRST argument is the OUTERMOST layer.
// Chain(a, b, c)(h) == a(b(c(h))) — reads in execution order.
func Chain(mws ...func(http.Handler) http.Handler) func(http.Handler) http.Handler {
	return func(final http.Handler) http.Handler {
		for i := len(mws) - 1; i >= 0; i-- {
			final = mws[i](final)
		}
		return final
	}
}

// BodyLimit wraps r.Body with http.MaxBytesReader so that a request body larger
// than maxBytes causes json.Decoder (and io.ReadAll) to return an error before
// reading beyond the cap. This prevents an unauthenticated client from streaming
// an arbitrarily large body into the server process.
//
// WHY apply it as middleware rather than per-handler: applying it ONCE at the
// router level means every new route that accepts a body is protected by
// construction. A per-handler approach relies on every author remembering — an
// easy miss that creates a silent DoS hole on a public endpoint.
//
// HOW it works (interview point): http.MaxBytesReader wraps the body with a
// reader that returns *http.MaxBytesError after maxBytes have been consumed. The
// HTTP server uses this to close the connection and send a 413 automatically
// ONLY if the handler has not started writing yet — in practice, our handlers
// call json.NewDecoder(r.Body).Decode and return early on the error, so we
// write the 413/400 ourselves. The important semantic is that Decode returns a
// non-nil error as soon as the cap is hit, rather than continuing to allocate.
//
// TRADEOFF — this cap applies to EVERY route including multipart file uploads.
// If the BFF ever adds a file-upload route it will need its own higher cap or a
// dedicated mux branch that bypasses this middleware. For now all BFF routes are
// thin JSON relays for which 1 MiB is generous.
func BodyLimit(maxBytes int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// MaxBytesReader replaces r.Body; once the limit is hit the reader
			// returns *http.MaxBytesError and the server closes the connection.
			// We must assign back to r.Body so downstream handlers (json.Decoder,
			// io.ReadAll) all read the capped version.
			r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
			next.ServeHTTP(w, r)
		})
	}
}
