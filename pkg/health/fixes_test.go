package health_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/abd-ulbasit/forgepoint/pkg/health"
)

// serveReadiness runs the readiness handler and returns the response recorder.
func serveReadiness(h *health.Handler) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	h.ReadinessHandler()(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	return rr
}

// A slow check must be bounded by the per-check timeout, not run to completion,
// so the probe answers the kubelet in time instead of hanging.
func TestReadiness_SlowCheckTimesOut(t *testing.T) {
	h := health.New(health.WithCheckTimeout(100 * time.Millisecond))
	h.AddCheck("fast", func(context.Context) error { return nil })
	h.AddCheck("slow", func(ctx context.Context) error {
		select {
		case <-time.After(5 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err() // respects the per-check timeout
		}
	})

	start := time.Now()
	rr := serveReadiness(h)
	elapsed := time.Since(start)

	if elapsed > time.Second {
		t.Fatalf("readiness took %v; per-check timeout did not bound the slow check", elapsed)
	}
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}

	var body health.ReadinessResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Checks["fast"].Status != "ok" {
		t.Errorf("fast check = %+v, want ok", body.Checks["fast"])
	}
	if body.Checks["slow"].Status != "fail" {
		t.Errorf("slow check = %+v, want fail", body.Checks["slow"])
	}
}

// Checks must run concurrently: N checks each sleeping D should finish in ~D,
// not N*D. Five 150ms checks finishing under 500ms proves they are not serial.
func TestReadiness_RunsChecksConcurrently(t *testing.T) {
	h := health.New(health.WithCheckTimeout(2 * time.Second))
	const n = 5
	const each = 150 * time.Millisecond
	for i := 0; i < n; i++ {
		h.AddCheck(fmt.Sprintf("c%d", i), func(context.Context) error {
			time.Sleep(each)
			return nil
		})
	}

	start := time.Now()
	rr := serveReadiness(h)
	elapsed := time.Since(start)

	if elapsed > each*time.Duration(n)/2 {
		t.Fatalf("readiness took %v for %d checks of %v; looks serial, not concurrent", elapsed, n, each)
	}
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
}

// A panicking check must fail that check, not crash the process.
func TestReadiness_PanickingCheckIsContained(t *testing.T) {
	h := health.New()
	h.AddCheck("boom", func(context.Context) error {
		panic("kaboom")
	})

	rr := serveReadiness(h) // must not panic the test process
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}
}
