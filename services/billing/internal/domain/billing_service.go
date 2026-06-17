// billing_service.go defines the BillingService interface — the primary port the
// handler layer reaches the business logic through.
//
// ============================================================================
// THE SERVICE INTERFACE AS A "PORT" (Hexagonal)
// ============================================================================
//
// The handler depends on this interface, never on the concrete impl. That gives
// dependency inversion (swap the real impl for a stub) and test isolation
// (handler tests pass a mock BillingService — no DB, no NATS). The concrete
// billingService and NewBillingService live in billing_service_impl.go.
//
// WHY the inputs are dedicated structs (RecordUsageInput, etc.) and NOT proto
// types: the domain must not import generated proto. The handler converts proto
// → these inputs (and domain results → proto) — the anti-corruption layer.
//
// SECURITY FRAMING (this is a billing service): notice what the input structs do
// NOT contain. The client never supplies Team, RatePlanID, Cost, or any price —
// those are SERVER-AUTHORITATIVE and attached inside the impl. The input carries
// only the verifiable raw metering FACTS. Accepting a client-supplied price or
// billed-team would be a mass-assignment vulnerability with a direct financial
// blast radius.
package domain

import (
	"context"
	"time"
)

// ============================================================================
// INPUT TYPES (the strict, minimal client-supplied surface)
// ============================================================================

// RecordUsageInput is the metering-input contract: ONLY the verifiable raw facts.
//
// THE TRUST BOUNDARY, made explicit by absence:
//   - Team is NOT here — it is server-derived (passed separately from auth
//     claims / the inference api key, see BillingService.RecordUsage's `team`
//     parameter, which the handler fills from claims, never from the request).
//   - RatePlanID / Cost / Money are NOT here — resolved/computed by the server.
//   - Status / invoice fields are NOT here — never settable.
type RecordUsageInput struct {
	MeterType MeterType // must be a known, non-unspecified meter
	Quantity  int64     // non-negative (clients never submit credits), <= MaxQuantityPerRecord

	ModelID      string // attribution only (reporting), never pricing
	ModelVersion string

	SourceRequestID string    // inference request id / upstream event id (lineage + dedupe anchor)
	OccurredAt      time.Time // event time; server CLAMPS it (no future, no backdating into closed period). Zero = now()
	IdempotencyKey  string    // stable key; a repeat returns the original record
}

// CreateRatePlanInput is the admin pricing-definition contract. The client
// supplies only the pricing DEFINITION; id/created_at are server-set
// (mass-assignment guard, enforced by their absence here).
type CreateRatePlanInput struct {
	Name               string
	UnitPrices         map[MeterType]Money // ALL must share one currency (server-validated)
	IncludedQuantities map[MeterType]int64
	QuotaLimits        map[MeterType]int64
	IdempotencyKey     string
}

// GetUsageInput is the (server-validated) reporting query. Team is supplied by
// the handler from auth claims for non-admins (tenant isolation), so the impl
// trusts it as already-authorized.
type GetUsageInput struct {
	Team        string
	PeriodStart time.Time // zero → current period start
	PeriodEnd   time.Time // zero → now
	MeterFilter MeterType // "" → all meters
	PageSize    int
	PageToken   string
}

// BillingService is the primary domain interface for metering, pricing, quota,
// and reporting. The handler holds a value of this interface.
type BillingService interface {
	// -----------------------------------------------------------------------
	// METERING (the outbox write path — the teaching centerpiece)
	// -----------------------------------------------------------------------

	// RecordUsage meters one billable action: it resolves the team's plan, prices
	// the quantity (overflow-safe, after free allowance), and persists the priced
	// UsageRecord TOGETHER WITH an outbox event in ONE transaction.
	//
	// `team` is SERVER-AUTHORITATIVE — the handler passes the caller's team from
	// auth claims (or, for the InferenceCompleted consumer, the team resolved from
	// the api_key_id). It is NOT a field on RecordUsageInput precisely so a client
	// can never name the billed team.
	//
	// IDEMPOTENCY: a repeat with the same (team, IdempotencyKey) returns the
	// ORIGINAL record with deduplicated=true and inserts nothing — the NATS→DB
	// half of exactly-once-in-effect (the outbox is the DB→NATS half).
	//
	// OUTBOX: the impl enqueues a UsageRecordedPayload outbox event with every
	// write, and ADDITIONALLY a QuotaExceededPayload event when this write pushes
	// the team over the plan's quota — both committed in the same tx as the row.
	//
	// Errors: ErrNegativeQuantity / ErrQuantityTooLarge / ErrUnknownMeter
	// (InvalidArgument), ErrRatePlanNotFound (FailedPrecondition), ErrAmountOverflow
	// (Internal — should be unreachable given the bound, asserted anyway).
	RecordUsage(ctx context.Context, team string, input RecordUsageInput) (record UsageRecord, deduplicated bool, err error)

	// -----------------------------------------------------------------------
	// PRICING (admin write + read)
	// -----------------------------------------------------------------------

	// CreateRatePlan defines a new priced plan (ADMIN-ONLY at the handler; the
	// impl enforces the single-currency + non-negative-price invariants). id and
	// created_at are server-assigned. Idempotent via IdempotencyKey.
	CreateRatePlan(ctx context.Context, input CreateRatePlanInput) (plan RatePlan, deduplicated bool, err error)

	// GetRatePlan returns a plan's pricing by id. ErrRatePlanNotFound → NotFound.
	GetRatePlan(ctx context.Context, id string) (RatePlan, error)

	// -----------------------------------------------------------------------
	// QUOTA (gateway pre-flight)
	// -----------------------------------------------------------------------

	// CheckQuota returns the team's standing on a meter (limit, current usage,
	// remaining, exceeded) for the current period. All fields server-computed.
	// `team` is server-authoritative (handler fills it from claims for non-admins).
	CheckQuota(ctx context.Context, team string, meter MeterType) (QuotaStatus, error)

	// -----------------------------------------------------------------------
	// REPORTING (read)
	// -----------------------------------------------------------------------

	// GetUsage returns aggregated, paginated per-meter rollups plus a grand total
	// for a team over a window. The window is server-validated (ErrInvalidPeriod
	// on an inverted or too-wide span).
	GetUsage(ctx context.Context, input GetUsageInput) (summaries []UsageSummary, grandTotal Money, nextToken string, err error)

	// GetInvoice fetches a single invoice. ErrInvoiceNotFound → NotFound.
	GetInvoice(ctx context.Context, id string) (Invoice, error)

	// ListInvoices returns a team's invoices newest-first, paginated, optionally
	// filtered by status.
	ListInvoices(ctx context.Context, opts ListInvoicesOptions) (invoices []Invoice, nextToken string, err error)

	// -----------------------------------------------------------------------
	// INVOICING (period close — the InvoiceGenerated outbox path)
	// -----------------------------------------------------------------------

	// GenerateInvoice aggregates a team's usage over [periodStart, periodEnd) into
	// a FINALIZED invoice and persists it with an InvoiceGenerated outbox event in
	// one tx. Returns ErrNoUsage (the period-close job SKIPS a $0 invoice) when the
	// team had no usage. This is the period-close half of the outbox pattern; it is
	// driven by a scheduled job, not a gRPC RPC (no RPC in billing.proto creates an
	// invoice — invoices are server-derived, never client-authored).
	GenerateInvoice(ctx context.Context, team string, periodStart, periodEnd time.Time) (Invoice, error)
}

// QuotaStatus is the server-computed answer CheckQuota returns. A pure value
// type (no proto) the handler maps to CheckQuotaResponse.
type QuotaStatus struct {
	Team         string
	RatePlanID   string
	MeterType    MeterType
	QuotaLimit   int64 // plan cap (0 = no cap)
	CurrentUsage int64 // usage so far this period
	Remaining    int64 // max(0, limit-usage); 0 when no cap or over
	Exceeded     bool  // usage >= limit (false when no cap)
}
