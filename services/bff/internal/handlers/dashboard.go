package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	billingv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/billing/v1"
	monitorv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/monitor/v1"
	pipelinev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/pipeline/v1"
	registryv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/registry/v1"
	"github.com/abd-ulbasit/forgepoint/services/bff/internal/httpx"
)

// ============================================================================
// DASHBOARD AGGREGATION — THE BFF'S REASON TO EXIST (fan-out + partial failure)
// ============================================================================
//
// GET /api/v1/dashboard composes ONE screen from FOUR services so the browser
// makes ONE call instead of four (the ADR's "one call per screen"). This is the
// canonical BFF win: server-side composition over the fast in-cluster network
// instead of four browser round-trips over the public internet.
//
// CONCURRENCY: the four downstream calls are INDEPENDENT, so we fan them out
// CONCURRENTLY with a sync.WaitGroup (stdlib — the task forbids adding the
// errgroup dependency, and we don't need errgroup's "cancel-all-on-first-error"
// semantics here; in fact we want the OPPOSITE — see partial failure). Total
// latency becomes max(t1..t4) instead of t1+t2+t3+t4. Each goroutine writes to
// its OWN tile field, so there is NO shared mutable state between goroutines and
// thus no data race (each tile is written by exactly one goroutine; the parent
// reads only after wg.Wait()). This is the memory-model guarantee that makes the
// lock-free fan-out correct: Wait() happens-after every Done().
//
// PARTIAL-FAILURE TOLERANCE (the interview point): a BFF dashboard must DEGRADE,
// not collapse. If model-monitor is down, the "drift" tile shows an error and
// the OTHER three tiles still render. So each tile carries its own {data,error,
// ok} and a per-call failure is captured into that tile — it NEVER fails the
// whole request. The HTTP status is always 200; the SPA inspects per-tile `ok`.
// (errgroup would have cancelled siblings on the first error — exactly the wrong
// behavior for a dashboard, which is the deeper reason plain WaitGroup fits.)
//
// Each fan-out call forwards the caller's token and is bounded by a short
// per-call timeout so one slow service can't hang the whole dashboard.
// ============================================================================

// DashboardHandler fans out to registry, pipeline, monitor, and billing.
type DashboardHandler struct {
	registry RegistryClient
	pipeline PipelineClient
	monitor  MonitorClient
	billing  BillingClient
	logger   *slog.Logger
	// perCallTimeout bounds each downstream call independently.
	perCallTimeout time.Duration
}

func NewDashboardHandler(
	registry RegistryClient,
	pipeline PipelineClient,
	monitor MonitorClient,
	billing BillingClient,
	logger *slog.Logger,
) *DashboardHandler {
	return &DashboardHandler{
		registry:       registry,
		pipeline:       pipeline,
		monitor:        monitor,
		billing:        billing,
		logger:         logger,
		perCallTimeout: 3 * time.Second,
	}
}

// tile is a single dashboard panel. `Ok` lets the SPA render a degraded state
// for just this panel when a service is down. `Error` is a SANITIZED, generic
// message (never the raw downstream detail).
type tile struct {
	Ok    bool            `json:"ok"`
	Error string          `json:"error,omitempty"`
	Data  json.RawMessage `json:"data,omitempty"`
}

// dashboardResponse is the composed payload. Each field is one tile.
type dashboardResponse struct {
	Models           tile `json:"models"`           // total model count
	ActiveExecutions tile `json:"activeExecutions"` // running pipeline executions
	RecentDrift      tile `json:"recentDrift"`      // latest drift reports
	Usage            tile `json:"usage"`            // billing usage summary
}

// Dashboard handles GET /api/v1/dashboard.
func (h *DashboardHandler) Dashboard(w http.ResponseWriter, r *http.Request) {
	// Forward the token once; every fan-out goroutine derives its bounded ctx
	// from this so all four calls authenticate as the real user.
	baseCtx := httpx.ContextWithToken(r.Context())

	var out dashboardResponse
	var wg sync.WaitGroup
	wg.Add(4)

	// --- Tile 1: model count ---------------------------------------------------
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(baseCtx, h.perCallTimeout)
		defer cancel()
		// page_size 1 — we only need pagination.total_count, not the rows. This
		// keeps the dashboard cheap (we don't pull every model to count them).
		resp, err := h.registry.ListModels(ctx, &registryv1.ListModelsRequest{
			Pagination: &commonPageSize1,
		})
		out.Models = h.tileFromCount("models", err, func() any {
			return map[string]int32{"total": resp.GetPagination().GetTotalCount()}
		})
	}()

	// --- Tile 2: active (running) executions -----------------------------------
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(baseCtx, h.perCallTimeout)
		defer cancel()
		resp, err := h.pipeline.ListExecutions(ctx, &pipelinev1.ListExecutionsRequest{
			StatusFilter: pipelinev1.ExecutionStatus_EXECUTION_STATUS_RUNNING,
			Pagination:   &commonPageSize20,
		})
		out.ActiveExecutions = h.tileFromProto("activeExecutions", err, resp)
	}()

	// --- Tile 3: recent drift reports ------------------------------------------
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(baseCtx, h.perCallTimeout)
		defer cancel()
		resp, err := h.monitor.ListDriftReports(ctx, &monitorv1.ListDriftReportsRequest{
			Pagination: &commonPageSize5,
		})
		out.RecentDrift = h.tileFromProto("recentDrift", err, resp)
	}()

	// --- Tile 4: usage summary -------------------------------------------------
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(baseCtx, h.perCallTimeout)
		defer cancel()
		resp, err := h.billing.GetUsage(ctx, &billingv1.GetUsageRequest{
			Team:       r.URL.Query().Get("team"),
			Pagination: &commonPageSize20,
		})
		out.Usage = h.tileFromProto("usage", err, resp)
	}()

	// Wait() happens-after every Done(): safe to read all tiles now (no race).
	wg.Wait()

	// Always 200 — partial failures are encoded per tile, not as an HTTP error.
	httpx.WriteJSON(w, http.StatusOK, out)
}

// tileFromProto builds a tile from a proto response or a gRPC error. On error it
// logs the real reason server-side and returns a degraded tile with a generic
// message — the dashboard renders the other tiles regardless.
func (h *DashboardHandler) tileFromProto(name string, err error, msg proto.Message) tile {
	if err != nil {
		h.logger.Warn("dashboard tile degraded",
			slog.String("tile", name),
			slog.Int("http_status", httpx.HTTPStatusForGRPC(err)),
			slog.String("error", err.Error()),
		)
		return tile{Ok: false, Error: "tile unavailable"}
	}
	b, mErr := dashboardMarshaler.Marshal(msg)
	if mErr != nil {
		return tile{Ok: false, Error: "tile encode failed"}
	}
	return tile{Ok: true, Data: b}
}

// tileFromCount builds a tile from a plain Go value (used where we summarize to a
// scalar like a count rather than echo a whole proto).
func (h *DashboardHandler) tileFromCount(name string, err error, dataFn func() any) tile {
	if err != nil {
		h.logger.Warn("dashboard tile degraded",
			slog.String("tile", name),
			slog.String("error", err.Error()),
		)
		return tile{Ok: false, Error: "tile unavailable"}
	}
	b, mErr := json.Marshal(dataFn())
	if mErr != nil {
		return tile{Ok: false, Error: "tile encode failed"}
	}
	return tile{Ok: true, Data: b}
}

// dashboardMarshaler mirrors httpx's response marshaler (lowerCamelCase, emit
// unpopulated) so tile JSON matches the rest of the API's shape.
var dashboardMarshaler = protojson.MarshalOptions{EmitUnpopulated: true, UseProtoNames: false}

// Pre-built pagination requests for the fan-out. The dashboard wants small,
// fixed pages (a summary, not full lists), so these are constants rather than
// parsed from the request. They are read-only and shared safely across the
// concurrent goroutines (no goroutine mutates them).
var (
	commonPageSize1  = commonv1.PaginationRequest{PageSize: 1}
	commonPageSize5  = commonv1.PaginationRequest{PageSize: 5}
	commonPageSize20 = commonv1.PaginationRequest{PageSize: 20}
)
