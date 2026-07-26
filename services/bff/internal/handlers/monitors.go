package handlers

import (
	"log/slog"
	"net/http"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	monitorv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/monitor/v1"
	"github.com/abd-ulbasit/forgepoint/services/bff/internal/httpx"
)

// MonitorsHandler maps /api/v1/monitors and /api/v1/drift-reports onto the
// model-monitor service.
type MonitorsHandler struct {
	monitor MonitorClient
	logger  *slog.Logger
}

func NewMonitorsHandler(monitor MonitorClient, logger *slog.Logger) *MonitorsHandler {
	return &MonitorsHandler{monitor: monitor, logger: logger}
}

// List handles GET /api/v1/monitors.
func (h *MonitorsHandler) List(w http.ResponseWriter, r *http.Request) {
	ctx := httpx.ContextWithToken(r.Context())
	resp, err := h.monitor.ListMonitors(ctx, &monitorv1.ListMonitorsRequest{
		Pagination: httpx.Pagination(r),
	})
	if err != nil {
		httpx.WriteGRPCError(w, h.logger, err)
		return
	}
	httpx.WriteProto(w, http.StatusOK, resp)
}

// DriftReports handles GET /api/v1/drift-reports. Optional model_name filter.
func (h *MonitorsHandler) DriftReports(w http.ResponseWriter, r *http.Request) {
	ctx := httpx.ContextWithToken(r.Context())
	resp, err := h.monitor.ListDriftReports(ctx, &monitorv1.ListDriftReportsRequest{
		ModelName:  r.URL.Query().Get("model_name"),
		Pagination: httpx.Pagination(r),
	})
	if err != nil {
		httpx.WriteGRPCError(w, h.logger, err)
		return
	}
	httpx.WriteProto(w, http.StatusOK, resp)
}

// Evals handles GET /api/v1/evals — the LLM-eval read path (L4). It lists the
// caller TEAM's recent quality evals (relevance/coherence/safety/overall per
// judged LLM response), newest first. The team is derived DOWNSTREAM from the
// forwarded JWT (never a query param), so the BFF only relays the optional
// filters and the page cursor.
//
// FILTERS (both optional, both logic-free pass-throughs):
//   - ?model_name=… narrows to one model's evals.
//   - ?since=<RFC3339> bounds the window to recent scores. We parse it into a
//     protobuf Timestamp here ONLY because the wire type is a Timestamp, not a
//     string — a malformed value is silently ignored (treated as "no lower
//     bound") rather than a 400, mirroring how Pagination clamps page_size: a bad
//     soft filter shouldn't fail the whole request. The monitor service still
//     enforces its own default window, so dropping a bad `since` is safe.
func (h *MonitorsHandler) Evals(w http.ResponseWriter, r *http.Request) {
	req := &monitorv1.ListEvalScoresRequest{
		ModelName:  r.URL.Query().Get("model_name"),
		Pagination: httpx.Pagination(r),
	}
	if raw := r.URL.Query().Get("since"); raw != "" {
		if t, err := time.Parse(time.RFC3339, raw); err == nil {
			req.Since = timestamppb.New(t)
		}
	}
	ctx := httpx.ContextWithToken(r.Context())
	resp, err := h.monitor.ListEvalScores(ctx, req)
	if err != nil {
		httpx.WriteGRPCError(w, h.logger, err)
		return
	}
	httpx.WriteProto(w, http.StatusOK, resp)
}
