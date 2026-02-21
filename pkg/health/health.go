package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
)

// ============================================================================
// HEALTH CHECK HANDLER
// ============================================================================
//
// WHY separate liveness and readiness:
//   Kubernetes uses two distinct probes:
//
//   LIVENESS (/healthz):
//     "Is the process alive and not deadlocked?"
//     If this fails → K8s RESTARTS the pod (kill + recreate).
//     Should NEVER check external dependencies (DB, NATS, Redis).
//     If liveness checked DB and DB was down, K8s would restart all pods —
//     which won't fix the DB and creates a restart storm.
//
//   READINESS (/readyz):
//     "Can this pod serve traffic right now?"
//     If this fails → K8s REMOVES pod from Service endpoints (no traffic).
//     Should check external dependencies (DB, NATS, Redis).
//     When dependency recovers → readiness passes → traffic restored.
//
//   ┌──────────────┬──────────────────────┬──────────────────────┐
//   │ Probe        │ Failure Action       │ What to Check        │
//   ├──────────────┼──────────────────────┼──────────────────────┤
//   │ Liveness     │ Pod restart          │ Process alive only   │
//   │ Readiness    │ Remove from LB       │ DB, NATS, Redis, etc │
//   └──────────────┴──────────────────────┴──────────────────────┘
//
// HOW KUBERNETES PROBES WORK:
//   kubelet (on each node) periodically calls the probe endpoints:
//     livenessProbe:
//       httpGet:
//         path: /healthz
//         port: 8080
//       initialDelaySeconds: 5    # wait before first check
//       periodSeconds: 10         # check every 10s
//       failureThreshold: 3       # restart after 3 consecutive failures
//     readinessProbe:
//       httpGet:
//         path: /readyz
//         port: 8080
//       periodSeconds: 5
//       failureThreshold: 2       # remove from endpoints after 2 failures
//
// HOW AWS/GCP DO IT:
//   - AWS ALB: Target group health checks (similar concept, HTTP GET)
//   - GCP: Separate liveness/readiness/startup probes
//   - Envoy (Istio): Active health checking + outlier detection
// ============================================================================

// CheckFunc is a health check function that returns nil if healthy,
// or an error describing what's wrong.
type CheckFunc func(ctx context.Context) error

// CheckResult represents the result of a single health check.
type CheckResult struct {
	Status string `json:"status"`          // "ok" or "fail"
	Error  string `json:"error,omitempty"` // error message if failed
}

// ReadinessResponse is the JSON body returned by the readiness endpoint.
type ReadinessResponse struct {
	Status string                 `json:"status"` // "ok" or "unavailable"
	Checks map[string]CheckResult `json:"checks"` // per-check results
}

// Handler manages health check functions and serves HTTP endpoints.
type Handler struct {
	mu     sync.RWMutex
	checks map[string]CheckFunc
}

// New creates a new health check Handler.
func New() *Handler {
	return &Handler{
		checks: make(map[string]CheckFunc),
	}
}

// AddCheck registers a named health check function.
// Checks are run on every readiness probe request.
//
// Example checks:
//   - "db":   func(ctx) error { return db.PingContext(ctx) }
//   - "nats": func(ctx) error { if !nc.IsConnected() { return err }; return nil }
//   - "redis": func(ctx) error { return rdb.Ping(ctx).Err() }
func (h *Handler) AddCheck(name string, check CheckFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.checks[name] = check
}

// LivenessHandler returns an HTTP handler for the liveness probe (/healthz).
//
// Always returns 200 OK. The process is alive if it can respond to HTTP.
// Never checks external dependencies — that would cause unnecessary restarts.
func (h *Handler) LivenessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
	}
}

// ReadinessHandler returns an HTTP handler for the readiness probe (/readyz).
//
// Runs all registered checks. Returns 200 if ALL pass, 503 if ANY fail.
// Response body includes per-check status for debugging:
//
//	{
//	  "status": "unavailable",
//	  "checks": {
//	    "db":   {"status": "ok"},
//	    "nats": {"status": "fail", "error": "connection refused"}
//	  }
//	}
func (h *Handler) ReadinessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.mu.RLock()
		checks := make(map[string]CheckFunc, len(h.checks))
		for k, v := range h.checks {
			checks[k] = v
		}
		h.mu.RUnlock()

		results := make(map[string]CheckResult, len(checks))
		allOK := true

		for name, check := range checks {
			if err := check(r.Context()); err != nil {
				results[name] = CheckResult{Status: "fail", Error: err.Error()}
				allOK = false
			} else {
				results[name] = CheckResult{Status: "ok"}
			}
		}

		resp := ReadinessResponse{
			Status: "ok",
			Checks: results,
		}

		w.Header().Set("Content-Type", "application/json")
		if !allOK {
			resp.Status = "unavailable"
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}

		json.NewEncoder(w).Encode(resp) //nolint:errcheck
	}
}
