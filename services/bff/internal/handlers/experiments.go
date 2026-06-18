package handlers

import (
	"log/slog"
	"net/http"

	experimentv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/experiment/v1"
	"github.com/abd-ulbasit/forgepoint/services/bff/internal/httpx"
)

// ExperimentsHandler maps /api/v1/runs/* onto the experiment-tracker service.
type ExperimentsHandler struct {
	experiment ExperimentClient
	logger     *slog.Logger
}

func NewExperimentsHandler(experiment ExperimentClient, logger *slog.Logger) *ExperimentsHandler {
	return &ExperimentsHandler{experiment: experiment, logger: logger}
}

// ListRuns handles GET /api/v1/runs. Optional experiment_id filter from query.
func (h *ExperimentsHandler) ListRuns(w http.ResponseWriter, r *http.Request) {
	ctx := httpx.ContextWithToken(r.Context())
	resp, err := h.experiment.ListRuns(ctx, &experimentv1.ListRunsRequest{
		ExperimentId: r.URL.Query().Get("experiment_id"),
		Pagination:   httpx.Pagination(r),
	})
	if err != nil {
		httpx.WriteGRPCError(w, h.logger, err)
		return
	}
	httpx.WriteProto(w, http.StatusOK, resp)
}

// GetRun handles GET /api/v1/runs/{id}.
func (h *ExperimentsHandler) GetRun(w http.ResponseWriter, r *http.Request) {
	ctx := httpx.ContextWithToken(r.Context())
	resp, err := h.experiment.GetRun(ctx, &experimentv1.GetRunRequest{Id: r.PathValue("id")})
	if err != nil {
		httpx.WriteGRPCError(w, h.logger, err)
		return
	}
	httpx.WriteProto(w, http.StatusOK, resp)
}
