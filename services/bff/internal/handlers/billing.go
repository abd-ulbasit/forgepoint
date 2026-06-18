package handlers

import (
	"log/slog"
	"net/http"

	billingv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/billing/v1"
	"github.com/abd-ulbasit/forgepoint/services/bff/internal/httpx"
)

// BillingHandler maps /api/v1/usage onto the billing service.
type BillingHandler struct {
	billing BillingClient
	logger  *slog.Logger
}

func NewBillingHandler(billing BillingClient, logger *slog.Logger) *BillingHandler {
	return &BillingHandler{billing: billing, logger: logger}
}

// Usage handles GET /api/v1/usage. Optional team filter from query; the period
// is left to the service's defaults when unset (the BFF adds no business rules).
func (h *BillingHandler) Usage(w http.ResponseWriter, r *http.Request) {
	ctx := httpx.ContextWithToken(r.Context())
	resp, err := h.billing.GetUsage(ctx, &billingv1.GetUsageRequest{
		Team:       r.URL.Query().Get("team"),
		Pagination: httpx.Pagination(r),
	})
	if err != nil {
		httpx.WriteGRPCError(w, h.logger, err)
		return
	}
	httpx.WriteProto(w, http.StatusOK, resp)
}
