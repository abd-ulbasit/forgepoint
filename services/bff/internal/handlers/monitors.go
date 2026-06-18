package handlers

import (
	"log/slog"
	"net/http"

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
