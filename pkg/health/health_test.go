package health_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/abd-ulbasit/goml/pkg/health"
)

// ============================================================================
// HEALTH CHECK TESTS
// ============================================================================
//
// WHY health checks matter in Kubernetes:
//   Kubernetes uses two probes to manage pod lifecycle:
//   - Liveness: "Is the process alive?" → Restart if not (kubelet restarts pod)
//   - Readiness: "Can it serve traffic?" → Remove from Service endpoints if not
//
//   Example scenario:
//     Pod starts → liveness=200 (alive) → readiness=503 (DB not ready yet)
//     → K8s keeps pod alive but doesn't route traffic to it
//     → DB connects → readiness=200 → K8s adds pod to Service endpoints
//     → DB goes down → readiness=503 → K8s removes from endpoints (no traffic)
//     → Process deadlocks → liveness fails → K8s restarts the pod
//
// HOW NETFLIX/UBER DO IT:
//   - Netflix Eureka: heartbeat-based health with self-preservation mode
//   - Uber: deep health checks including downstream dependencies
//   - AWS ALB: configurable health check paths with grace period
//   - We implement K8s-native /healthz (liveness) and /readyz (readiness)
// ============================================================================

func TestLivenessHandler_Returns200(t *testing.T) {
	h := health.New()

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	h.LivenessHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("liveness status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestLivenessHandler_AlwaysReturns200_EvenWhenChecksAdded(t *testing.T) {
	// Liveness should ALWAYS return 200 (process is alive).
	// Even if readiness checks fail, liveness must pass — otherwise K8s
	// restarts the pod when the real problem is a dependency (DB, NATS).
	h := health.New()
	h.AddCheck("broken", func(ctx context.Context) error {
		return errors.New("connection refused")
	})

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	h.LivenessHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("liveness status = %d, want %d (liveness should always be 200)", rec.Code, http.StatusOK)
	}
}

func TestReadinessHandler_Returns200_WhenAllChecksPass(t *testing.T) {
	h := health.New()
	h.AddCheck("db", func(ctx context.Context) error {
		return nil // DB is reachable
	})
	h.AddCheck("nats", func(ctx context.Context) error {
		return nil // NATS is reachable
	})

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	h.ReadinessHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("readiness status = %d, want %d", rec.Code, http.StatusOK)
	}

	// Verify JSON response body
	var result health.ReadinessResponse
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if result.Status != "ok" {
		t.Errorf("status = %q, want %q", result.Status, "ok")
	}
	if len(result.Checks) != 2 {
		t.Errorf("checks count = %d, want 2", len(result.Checks))
	}
}

func TestReadinessHandler_Returns503_WhenAnyCheckFails(t *testing.T) {
	h := health.New()
	h.AddCheck("db", func(ctx context.Context) error {
		return nil
	})
	h.AddCheck("nats", func(ctx context.Context) error {
		return errors.New("connection refused")
	})

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	h.ReadinessHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("readiness status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}

	var result health.ReadinessResponse
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if result.Status != "unavailable" {
		t.Errorf("status = %q, want %q", result.Status, "unavailable")
	}

	// Check that NATS is reported as failed
	natsCheck, ok := result.Checks["nats"]
	if !ok {
		t.Fatal("missing nats check in response")
	}
	if natsCheck.Status != "fail" {
		t.Errorf("nats check status = %q, want %q", natsCheck.Status, "fail")
	}
	if natsCheck.Error == "" {
		t.Error("nats check error should be non-empty")
	}
}

func TestReadinessHandler_Returns200_WhenNoChecksRegistered(t *testing.T) {
	// No checks = ready by default (useful during startup before deps are registered)
	h := health.New()

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	h.ReadinessHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("readiness status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestReadinessHandler_CheckReceivesContext(t *testing.T) {
	// Verify that the HTTP request context is passed to checks.
	// This allows checks to respect deadlines/cancellation.
	h := health.New()
	var receivedCtx context.Context
	h.AddCheck("ctx-test", func(ctx context.Context) error {
		receivedCtx = ctx
		return nil
	})

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	h.ReadinessHandler().ServeHTTP(rec, req)

	if receivedCtx == nil {
		t.Error("check did not receive context")
	}
}
