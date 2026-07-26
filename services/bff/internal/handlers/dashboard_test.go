package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	billingv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/billing/v1"
	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	monitorv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/monitor/v1"
	pipelinev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/pipeline/v1"
	registryv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/registry/v1"
)

// timeMustParse is a fixed timestamp for deterministic tests.
func timeMustParse() time.Time {
	return time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)
}

// ============================================================================
// DASHBOARD PARTIAL FAILURE: one service down degrades ONE tile, not the whole
// response. The HTTP status stays 200 and the healthy tiles still render.
// ============================================================================

func TestDashboard_PartialFailure_DegradesOneTile(t *testing.T) {
	t.Parallel()

	reg := &mockRegistry{
		listModelsFn: func(_ context.Context, _ *registryv1.ListModelsRequest) (*registryv1.ListModelsResponse, error) {
			return &registryv1.ListModelsResponse{
				Pagination: &commonv1.PaginationResponse{TotalCount: 42},
			}, nil
		},
	}
	pipe := &mockPipeline{
		listExecFn: func(_ context.Context, _ *pipelinev1.ListExecutionsRequest) (*pipelinev1.ListExecutionsResponse, error) {
			return &pipelinev1.ListExecutionsResponse{
				Executions: []*pipelinev1.Execution{{Id: "e-1"}},
				Pagination: &commonv1.PaginationResponse{TotalCount: 1},
			}, nil
		},
	}
	// MONITOR is DOWN — this tile must degrade, the others must survive.
	mon := &mockMonitor{
		listDriftFn: func(_ context.Context, _ *monitorv1.ListDriftReportsRequest) (*monitorv1.ListDriftReportsResponse, error) {
			return nil, status.Error(codes.Unavailable, "monitor connection refused")
		},
	}
	bill := &mockBilling{
		getUsageFn: func(_ context.Context, _ *billingv1.GetUsageRequest) (*billingv1.GetUsageResponse, error) {
			return &billingv1.GetUsageResponse{
				GrandTotal: &billingv1.Money{CurrencyCode: "USD", AmountMicros: 100_000_000},
			}, nil
		},
	}

	dh := NewDashboardHandler(reg, pipe, mon, bill, testLogger())
	r := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard", nil)
	w := httptest.NewRecorder()
	dh.Dashboard(w, r)

	// Always 200 — partial failures are per-tile, never a top-level error.
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}

	var resp dashboardResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal dashboard: %v; body=%s", err, w.Body.String())
	}

	// Healthy tiles are ok=true with data.
	if !resp.Models.Ok || len(resp.Models.Data) == 0 {
		t.Errorf("models tile should be ok with data, got %+v", resp.Models)
	}
	if !resp.ActiveExecutions.Ok {
		t.Errorf("activeExecutions tile should be ok, got %+v", resp.ActiveExecutions)
	}
	if !resp.Usage.Ok {
		t.Errorf("usage tile should be ok, got %+v", resp.Usage)
	}

	// The DOWN tile is degraded: ok=false, generic error, NO leaked detail.
	if resp.RecentDrift.Ok {
		t.Errorf("recentDrift tile should be degraded, got ok=true")
	}
	if resp.RecentDrift.Error == "" {
		t.Errorf("recentDrift tile should carry an error message")
	}
	if got := w.Body.String(); contains(got, "connection refused") {
		t.Errorf("dashboard leaked downstream detail: %s", got)
	}

	// The models tile total count survived the fan-out.
	var modelsData map[string]int
	if err := json.Unmarshal(resp.Models.Data, &modelsData); err != nil {
		t.Fatalf("models tile data not the expected shape: %v", err)
	}
	if modelsData["total"] != 42 {
		t.Errorf("models total = %d, want 42", modelsData["total"])
	}
}

func TestDashboard_AllHealthy_AllTilesOk(t *testing.T) {
	t.Parallel()
	ok := func() *mockRegistry {
		return &mockRegistry{listModelsFn: func(_ context.Context, _ *registryv1.ListModelsRequest) (*registryv1.ListModelsResponse, error) {
			return &registryv1.ListModelsResponse{Pagination: &commonv1.PaginationResponse{TotalCount: 1}}, nil
		}}
	}
	dh := NewDashboardHandler(
		ok(),
		&mockPipeline{listExecFn: func(_ context.Context, _ *pipelinev1.ListExecutionsRequest) (*pipelinev1.ListExecutionsResponse, error) {
			return &pipelinev1.ListExecutionsResponse{Pagination: &commonv1.PaginationResponse{}}, nil
		}},
		&mockMonitor{listDriftFn: func(_ context.Context, _ *monitorv1.ListDriftReportsRequest) (*monitorv1.ListDriftReportsResponse, error) {
			return &monitorv1.ListDriftReportsResponse{Pagination: &commonv1.PaginationResponse{}}, nil
		}},
		&mockBilling{getUsageFn: func(_ context.Context, _ *billingv1.GetUsageRequest) (*billingv1.GetUsageResponse, error) {
			return &billingv1.GetUsageResponse{}, nil
		}},
		testLogger(),
	)
	r := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard", nil)
	w := httptest.NewRecorder()
	dh.Dashboard(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp dashboardResponse
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if !resp.Models.Ok || !resp.ActiveExecutions.Ok || !resp.RecentDrift.Ok || !resp.Usage.Ok {
		t.Errorf("all tiles should be ok; got %+v", resp)
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (indexOf(haystack, needle) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
