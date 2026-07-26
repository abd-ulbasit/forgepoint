// billing_service_impl.go — the concrete BillingService implementation.
//
// This is the platform's "money truth": the code here decides what a customer is
// charged. The comments name each pattern (Outbox, idempotent consumer,
// server-authoritative pricing) and the financial bug each guard prevents.
//
// ============================================================================
// CLEAN ARCHITECTURE PLACEMENT
// ============================================================================
//
// billingService lives in the domain package and depends ONLY on:
//   - the persistence/external PORTS (UsageStore, RatePlanStore, InvoiceStore,
//     IDProvider, Clock) — never their adapters, and
//   - stdlib + the pure money math in models.go.
//
// It imports NO gRPC, NO NATS, NO database driver, NO generated proto. The
// composition root (main.go) injects the real Postgres-backed stores + a uuid
// IDProvider + a real Clock; tests inject mocks + a fixed clock. This is
// dependency inversion: the business logic dictates the port contracts; the
// infrastructure conforms.
package domain

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// MaxPageSize caps any list/usage page so a caller cannot request an unbounded
// scan (same discipline as the registry/auth list RPCs). The handler also clamps
// at the proto boundary; the service re-clamps as defense in depth.
const MaxPageSize = 100

// defaultPageSize is used when a caller passes 0.
const defaultPageSize = 20

// MaxUsageWindow caps the span of a GetUsage / GenerateInvoice query. WHY:
//
//	GetUsage scans the ledger over [start,end). An unbounded window is a
//	cost-blowup / DoS vector. 366 days covers any single billing year; a wider
//	request is REJECTED (ErrInvalidPeriod), never silently truncated.
const MaxUsageWindow = 366 * 24 * time.Hour

// billingService is the production implementation. Unexported: callers receive
// it only through the BillingService interface from NewBillingService, enforcing
// program-to-the-interface and keeping fields private.
type billingService struct {
	usage    UsageStore
	plans    RatePlanStore
	invoices InvoiceStore
	ids      IDProvider
	clock    Clock
}

// NewBillingService wires the ports and returns the BillingService interface.
// Returning the interface (not *billingService) is the constructor half of
// dependency inversion; the compile-time assertion below guarantees the concrete
// type satisfies the interface so signature drift fails the build, not a call site.
func NewBillingService(
	usage UsageStore,
	plans RatePlanStore,
	invoices InvoiceStore,
	ids IDProvider,
	clock Clock,
) BillingService {
	return &billingService{usage: usage, plans: plans, invoices: invoices, ids: ids, clock: clock}
}

var _ BillingService = (*billingService)(nil)

// ============================================================================
// RecordUsage — THE OUTBOX WRITE PATH (the centerpiece)
// ============================================================================
//
// ASCII — the exactly-once-in-effect pipeline this method anchors:
//
//	NATS (at-least-once)                         DB→NATS (outbox, at-least-once)
//	   InferenceCompleted ─┐                          ┌─► fp.billing.usage.recorded
//	                       ▼                          │
//	  ┌── RecordUsage ─────────────────────────────┐ │
//	  │ 1. validate facts (overflow-safe bounds)   │ │
//	  │ 2. DEDUPE on (team, idempotency_key) ◄──────┼─┘  NATS→DB half (this guard)
//	  │ 3. resolve plan (SERVER price source)      │
//	  │ 4. price = checked(quantity × unit price)  │
//	  │ 5. build UsageRecord + OutboxEvent(s)      │
//	  │ 6. RecordUsageTx: row + outbox in ONE tx ──┼──► (commit = durable intent)
//	  └────────────────────────────────────────────┘
//
// Steps 2 (idempotency) and 6 (transactional outbox) are the two ends of the
// pipe; BOTH must be idempotent for the whole thing to be exactly-once in effect.
func (s *billingService) RecordUsage(ctx context.Context, team string, input RecordUsageInput) (UsageRecord, bool, error) {
	// ----- SECURITY GATE: team is server-authoritative -----
	// `team` arrives from the handler (auth claims / inference api key), NEVER
	// from the request body. An empty team here is a wiring bug, not client input;
	// fail closed rather than bill an empty tenant.
	if team == "" {
		return UsageRecord{}, false, fmt.Errorf("%w: server-authoritative team is empty", ErrValidation)
	}

	// ----- STEP 1: validate the raw facts (overflow-safe bounds) -----
	// Order matters: reject obviously bad input BEFORE any port round-trip.
	if !input.MeterType.IsBillable() {
		// Unspecified/unknown meter — we never guess a price (a guessed $0 is a
		// silent revenue leak). InvalidArgument-class.
		return UsageRecord{}, false, fmt.Errorf("%w: %q", ErrUnknownMeter, input.MeterType)
	}
	if input.Quantity < 0 {
		// SECURITY: only the SERVER issues negative quantities (credits). A negative
		// from the client write path is an attempt to manufacture a credit.
		return UsageRecord{}, false, ErrNegativeQuantity
	}
	if input.Quantity > MaxQuantityPerRecord {
		// OVERFLOW GUARD (input half): bound quantity so quantity×price provably
		// fits int64. Out-of-range is REJECTED, never truncated. The checked
		// multiply below is the second half (belt-and-suspenders).
		return UsageRecord{}, false, fmt.Errorf("%w: %d > %d", ErrQuantityTooLarge, input.Quantity, MaxQuantityPerRecord)
	}

	// ----- STEP 2: IDEMPOTENCY (NATS→DB half of exactly-once) -----
	// A redelivered InferenceCompleted or a client retry carries the same
	// idempotency key. If we already recorded it, return the ORIGINAL record and
	// insert NOTHING. We check by (team, key) because client keys are only unique
	// within a tenant. This is the "idempotent consumer" pattern: the consumer,
	// not the broker, guarantees no double-effect under at-least-once delivery.
	if input.IdempotencyKey != "" {
		if existing, err := s.usage.FindByIdempotencyKey(ctx, team, input.IdempotencyKey); err == nil {
			return existing, true, nil
		} else if !errors.Is(err, ErrRepoNotFound) {
			// A genuine lookup failure (DB down) is distinct from "not seen before";
			// fail the operation rather than risk a double insert on a flaky read.
			return UsageRecord{}, false, fmt.Errorf("idempotency lookup: %w", err)
		}
	}

	// ----- STEP 3: resolve the plan (SERVER-AUTHORITATIVE price source) -----
	// The client never names a plan or a price. We resolve the team's CURRENT plan
	// and pin its id on the record so the price is reproducible even if the team's
	// plan later changes. No plan ⇒ we cannot price ⇒ FailedPrecondition.
	plan, err := s.plans.ResolvePlanForTeam(ctx, team)
	if err != nil {
		if errors.Is(err, ErrRepoNotFound) {
			return UsageRecord{}, false, ErrRatePlanNotFound
		}
		return UsageRecord{}, false, fmt.Errorf("resolving rate plan: %w", err)
	}

	// ----- STEP 4: price the usage (overflow-safe, after free allowance) -----
	// Read prior period usage so the free allowance is applied across the team's
	// CUMULATIVE usage (the first N units of the period are free, not the first N
	// of every record). This is read BEFORE the write so it reflects state without
	// this record. The store also uses it to decide the quota crossing below.
	periodStart := s.currentPeriodStart()
	priorUsage, err := s.usage.CurrentPeriodUsage(ctx, team, input.MeterType, periodStart)
	if err != nil {
		return UsageRecord{}, false, fmt.Errorf("reading period usage: %w", err)
	}

	billableQty := billableQuantity(priorUsage, input.Quantity, plan.IncludedQuantities[input.MeterType])

	cost, err := computeCost(plan, input.MeterType, billableQty)
	if err != nil {
		// ErrUnknownMeter (unpriced meter) or ErrAmountOverflow. The latter should
		// be unreachable given MaxQuantityPerRecord, but a money service asserts it.
		return UsageRecord{}, false, err
	}

	// ----- STEP 5: build the record + outbox event(s) as ONE unit of work -----
	occurredAt := s.clampOccurredAt(input.OccurredAt)
	now := s.clock.Now()
	record := UsageRecord{
		ID:              s.ids.NewID(),
		Team:            team,    // server-authoritative
		RatePlanID:      plan.ID, // server-resolved + pinned
		MeterType:       input.MeterType,
		Quantity:        billableQty, // the BILLED quantity (post-allowance)
		Cost:            cost,        // server-computed
		ModelID:         input.ModelID,
		ModelVersion:    input.ModelVersion,
		SourceRequestID: input.SourceRequestID,
		IdempotencyKey:  input.IdempotencyKey,
		OccurredAt:      occurredAt,
		CreatedAt:       now,
	}

	// OUTBOX: every write carries a UsageRecorded event. Its ID is the dedupe
	// handle consumers key on (it becomes the EventEnvelope.id). The payload
	// FLATTENS Money to micros+currency so a consumer needn't depend on this API.
	events := []OutboxEvent{
		s.buildOutboxEvent(record.ID, UsageRecordedPayload{
			RecordID:        record.ID,
			Team:            record.Team,
			RatePlanID:      record.RatePlanID,
			MeterType:       record.MeterType,
			Quantity:        record.Quantity,
			CostMicros:      record.Cost.AmountMicros,
			CurrencyCode:    record.Cost.CurrencyCode,
			ModelID:         record.ModelID,
			ModelVersion:    record.ModelVersion,
			SourceRequestID: record.SourceRequestID,
			OccurredAt:      record.OccurredAt,
		}, now),
	}

	// QUOTA CROSSING: if THIS write pushes the team over the plan's hard cap, emit
	// a QuotaExceeded event IN THE SAME outbox batch — so the gateway's
	// cache-invalidation signal is as durable as the usage that triggered it. We
	// compute the new cumulative usage from the prior read + the RAW input
	// quantity (quota counts gross usage, not post-allowance billed units).
	if limit, capped := plan.QuotaLimits[input.MeterType]; capped && limit > 0 {
		newUsage := priorUsage + input.Quantity
		if newUsage >= limit && priorUsage < limit {
			// Only on the CROSSING (prior was under, now at/over) — not on every
			// subsequent over-cap call — so we don't spam duplicate alerts.
			events = append(events, s.buildOutboxEvent(record.ID, QuotaExceededPayload{
				Team:         team,
				RatePlanID:   plan.ID,
				MeterType:    input.MeterType,
				QuotaLimit:   limit,
				CurrentUsage: newUsage,
				OccurredAt:   occurredAt,
			}, now))
		}
	}

	// ----- STEP 6: commit row + outbox atomically (the outbox boundary) -----
	stored, err := s.usage.RecordUsageTx(ctx, record, events)
	if err != nil {
		// RACE: a concurrent writer won the unique (team, idempotency_key) insert.
		// Re-read the winner's record so BOTH callers observe the same single row —
		// idempotency holds even under concurrency. This is why the store reports
		// ErrRepoDuplicate distinctly rather than a generic error.
		if errors.Is(err, ErrRepoDuplicate) && input.IdempotencyKey != "" {
			winner, rerr := s.usage.FindByIdempotencyKey(ctx, team, input.IdempotencyKey)
			if rerr != nil {
				return UsageRecord{}, false, fmt.Errorf("resolving idempotency race: %w", rerr)
			}
			return winner, true, nil
		}
		return UsageRecord{}, false, fmt.Errorf("recording usage: %w", err)
	}
	return stored, false, nil
}

// buildOutboxEvent constructs an unpublished OutboxEvent for a payload. The
// EventType comes from the payload itself (one source of truth), AggregateID
// ties it to the business row, and the fresh id becomes the envelope id the
// relay publishes and consumers dedupe on. PublishedAt is nil — the relay stamps
// it after a successful NATS publish; the domain only ever creates UNPUBLISHED
// intents.
func (s *billingService) buildOutboxEvent(aggregateID string, payload OutboxPayload, now time.Time) OutboxEvent {
	return OutboxEvent{
		ID:          s.ids.NewID(),
		AggregateID: aggregateID,
		EventType:   payload.EventType(),
		Payload:     payload,
		CreatedAt:   now,
		PublishedAt: nil,
	}
}

// billableQuantity applies the free allowance across CUMULATIVE period usage.
//
//	Given priorUsage already recorded, a free allowance of `included` units per
//	period, and `quantity` new units: the units in [priorUsage, priorUsage+quantity)
//	that fall AT OR ABOVE `included` are billable; those below are free.
//
// Example: included=100, prior=80, quantity=50 → units 80..130; 80..100 (20) are
// free, 100..130 (30) bill. So billable=30. If prior already ≥ included, the
// whole quantity bills. This is how SaaS "first N free per month" actually works
// (the allowance is consumed once per period, not reset per record).
func billableQuantity(priorUsage, quantity, included int64) int64 {
	if included <= 0 {
		return quantity // no free tier
	}
	// Free units still remaining at the start of this record.
	remainingFree := included - priorUsage
	if remainingFree < 0 {
		remainingFree = 0
	}
	if remainingFree >= quantity {
		return 0 // entire record covered by the allowance
	}
	return quantity - remainingFree
}

// computeCost prices a billable quantity against the plan, OVERFLOW-SAFE.
//
//	cost_micros = billableQty × unit_price_micros, via mulInt64Checked.
//
// An unpriced meter (no entry in the plan) is an ERROR (ErrUnknownMeter), never
// a silent $0 — billing a metered action as free is a revenue leak. A zero
// billable quantity yields a zero cost in the plan's currency (a legitimate $0
// line, e.g. fully within the free allowance) — note we still stamp the currency
// so the zero is well-typed for later summation.
func computeCost(plan RatePlan, meter MeterType, billableQty int64) (Money, error) {
	price, ok := plan.PriceFor(meter)
	if !ok {
		return Money{}, fmt.Errorf("%w: meter %q not priced by plan %s", ErrUnknownMeter, meter, plan.ID)
	}
	amount, err := mulInt64Checked(billableQty, price.AmountMicros)
	if err != nil {
		// Provably unreachable given MaxQuantityPerRecord + sane prices, but a
		// money service refuses to emit a number it cannot vouch for.
		return Money{}, err
	}
	return Money{AmountMicros: amount, CurrencyCode: price.CurrencyCode}, nil
}

// clampOccurredAt enforces the "no backdating, no future-dating" rule on the
// client-supplied event time. WHY: occurred_at decides which billing period the
// usage lands in. A future timestamp could shove usage into a not-yet-open
// period; a far-past one could try to land in a closed/paid invoice. We accept
// SLIGHT lateness (queue lag is real) but clamp anything in the future to now and
// floor at the current period start so usage can't slip into a settled period.
func (s *billingService) clampOccurredAt(t time.Time) time.Time {
	now := s.clock.Now()
	if t.IsZero() || t.After(now) {
		return now // empty or future → use server now
	}
	periodStart := s.currentPeriodStart()
	if t.Before(periodStart) {
		return periodStart // floor at the open period start (no backdating into closed periods)
	}
	return t
}

// currentPeriodStart returns the start of the current monthly billing period
// (first of the month, UTC). WHY monthly + UTC: invoices are monthly; UTC avoids
// DST/timezone ambiguity at period boundaries (a billing bug magnet). Pure
// derivation from the injected clock so it is deterministic in tests.
func (s *billingService) currentPeriodStart() time.Time {
	now := s.clock.Now().UTC()
	return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
}

// ============================================================================
// CreateRatePlan — admin pricing write (mass-assignment + invariant guards)
// ============================================================================

// CreateRatePlan validates the pricing definition, assigns server-authoritative
// id/created_at, and persists it idempotently. The handler enforces the ADMIN
// role before calling; the impl enforces the single-currency + non-negative-price
// invariants so a malformed plan can never reach the metering math.
func (s *billingService) CreateRatePlan(ctx context.Context, input CreateRatePlanInput) (RatePlan, bool, error) {
	if len(input.UnitPrices) == 0 {
		return RatePlan{}, false, fmt.Errorf("%w: a plan must price at least one meter", ErrValidation)
	}

	// INVARIANT: single currency + non-negative prices + valid meter + valid
	// currency code. A mixed-currency plan would make a usage rollup attempt to
	// sum incommensurable amounts (caught later by Money.Add, but we fail FAST
	// here at definition time where the error is actionable for the admin).
	var planCurrency string
	for meter, price := range input.UnitPrices {
		if !meter.IsBillable() {
			return RatePlan{}, false, fmt.Errorf("%w: %q is not a valid meter type", ErrValidation, meter)
		}
		if !IsValidCurrencyCode(price.CurrencyCode) {
			return RatePlan{}, false, fmt.Errorf("%w: %q", ErrInvalidCurrency, price.CurrencyCode)
		}
		if price.AmountMicros < 0 {
			return RatePlan{}, false, fmt.Errorf("%w: price for %q is negative", ErrValidation, meter)
		}
		if planCurrency == "" {
			planCurrency = price.CurrencyCode
		} else if price.CurrencyCode != planCurrency {
			return RatePlan{}, false, fmt.Errorf("%w: plan mixes %s and %s", ErrCurrencyMismatch, planCurrency, price.CurrencyCode)
		}
	}

	// IDEMPOTENCY: admin tooling retries too. A repeat with the same key returns
	// the ORIGINAL plan rather than creating a duplicate priced plan.
	if input.IdempotencyKey != "" {
		if existing, err := s.plans.FindPlanByIdempotencyKey(ctx, input.IdempotencyKey); err == nil {
			return existing, true, nil
		} else if !errors.Is(err, ErrRepoNotFound) {
			return RatePlan{}, false, fmt.Errorf("rate-plan idempotency lookup: %w", err)
		}
	}

	plan := RatePlan{
		ID:                 s.ids.NewID(), // server-assigned (mass-assignment guard)
		Name:               input.Name,
		UnitPrices:         input.UnitPrices,
		IncludedQuantities: input.IncludedQuantities,
		QuotaLimits:        input.QuotaLimits,
		CreatedAt:          s.clock.Now(), // server-stamped
	}

	stored, err := s.plans.CreatePlan(ctx, plan, input.IdempotencyKey)
	if err != nil {
		if errors.Is(err, ErrRepoDuplicate) && input.IdempotencyKey != "" {
			// Lost an idempotency race — return the winner's plan.
			winner, rerr := s.plans.FindPlanByIdempotencyKey(ctx, input.IdempotencyKey)
			if rerr != nil {
				return RatePlan{}, false, fmt.Errorf("resolving rate-plan race: %w", rerr)
			}
			return winner, true, nil
		}
		return RatePlan{}, false, fmt.Errorf("creating rate plan: %w", err)
	}
	return stored, false, nil
}

// GetRatePlan returns a plan's pricing by id, translating the storage miss to the
// business ErrRatePlanNotFound (→ NotFound).
func (s *billingService) GetRatePlan(ctx context.Context, id string) (RatePlan, error) {
	plan, err := s.plans.GetPlan(ctx, id)
	if err != nil {
		if errors.Is(err, ErrRepoNotFound) {
			return RatePlan{}, ErrRatePlanNotFound
		}
		return RatePlan{}, fmt.Errorf("getting rate plan: %w", err)
	}
	return plan, nil
}

// ============================================================================
// CheckQuota — the gateway pre-flight (server-computed standing)
// ============================================================================

// CheckQuota computes a team's standing on a meter for the current period. All
// fields are server-derived; the gateway caches the answer and the QuotaExceeded
// event invalidates that cache. `team` is server-authoritative (handler-filled).
func (s *billingService) CheckQuota(ctx context.Context, team string, meter MeterType) (QuotaStatus, error) {
	if team == "" {
		return QuotaStatus{}, fmt.Errorf("%w: team is required", ErrValidation)
	}
	if !meter.IsBillable() {
		return QuotaStatus{}, fmt.Errorf("%w: %q", ErrUnknownMeter, meter)
	}
	plan, err := s.plans.ResolvePlanForTeam(ctx, team)
	if err != nil {
		if errors.Is(err, ErrRepoNotFound) {
			return QuotaStatus{}, ErrRatePlanNotFound
		}
		return QuotaStatus{}, fmt.Errorf("resolving rate plan: %w", err)
	}
	usage, err := s.usage.CurrentPeriodUsage(ctx, team, meter, s.currentPeriodStart())
	if err != nil {
		return QuotaStatus{}, fmt.Errorf("reading period usage: %w", err)
	}

	limit := plan.QuotaLimits[meter] // 0 == no cap
	status := QuotaStatus{
		Team:         team,
		RatePlanID:   plan.ID,
		MeterType:    meter,
		QuotaLimit:   limit,
		CurrentUsage: usage,
	}
	if limit > 0 {
		// remaining = max(0, limit-usage); exceeded when at/over the cap.
		remaining := limit - usage
		if remaining < 0 {
			remaining = 0
		}
		status.Remaining = remaining
		status.Exceeded = usage >= limit
	}
	// No cap (limit == 0): remaining stays 0 and exceeded stays false — there is
	// nothing to exceed. (Remaining 0 with no cap means "N/A", not "blocked"; the
	// gateway keys off Exceeded, which is false.)
	return status, nil
}

// ============================================================================
// GetUsage — aggregated, paginated reporting (read)
// ============================================================================

// GetUsage validates the window, caps the page, and returns per-meter rollups
// plus a grand total. For the scaffold the heavy aggregation is delegated to the
// store; the domain owns the window/period rules + the overflow-safe grand-total
// sum. (The store's paged bucket query lands in the repo phase; this method is
// the domain contract and the validation/aggregation it owns.)
func (s *billingService) GetUsage(ctx context.Context, input GetUsageInput) ([]UsageSummary, Money, string, error) {
	if input.Team == "" {
		return nil, Money{}, "", fmt.Errorf("%w: team is required", ErrValidation)
	}

	// Default the window to the current period when unset, then validate it.
	start := input.PeriodStart
	end := input.PeriodEnd
	if start.IsZero() {
		start = s.currentPeriodStart()
	}
	if end.IsZero() {
		end = s.clock.Now()
	}
	if !end.After(start) {
		return nil, Money{}, "", fmt.Errorf("%w: end %v must be after start %v", ErrInvalidPeriod, end, start)
	}
	if end.Sub(start) > MaxUsageWindow {
		return nil, Money{}, "", fmt.Errorf("%w: window %v exceeds max %v", ErrInvalidPeriod, end.Sub(start), MaxUsageWindow)
	}

	// The per-meter buckets come from the invoice store's aggregation (a GROUP BY
	// in Postgres in the repo phase). We reuse AggregateUsageForPeriod here so the
	// domain's grand-total math is exercised against the same buckets invoicing
	// uses — one source of truth for "team usage over a period".
	buckets, err := s.invoices.AggregateUsageForPeriod(ctx, input.Team, start, end)
	if err != nil {
		return nil, Money{}, "", fmt.Errorf("aggregating usage: %w", err)
	}

	byMeter := make(map[MeterType]MeterUsage, len(buckets))
	var grandTotal Money
	for _, b := range buckets {
		if input.MeterFilter != MeterTypeUnspecified && b.MeterType != input.MeterFilter {
			continue
		}
		byMeter[b.MeterType] = b
		// Overflow-safe accumulation: a hostile/huge ledger cannot wrap the total.
		grandTotal, err = grandTotal.Add(b.TotalCost)
		if err != nil {
			return nil, Money{}, "", fmt.Errorf("summing usage cost: %w", err)
		}
	}

	summary := UsageSummary{
		Team:        input.Team,
		PeriodStart: start,
		PeriodEnd:   end,
		ByMeter:     byMeter,
		TotalCost:   grandTotal,
	}
	// Single summary bucket for the whole window (sub-period daily bucketing is a
	// repo-phase refinement; the contract returns a slice so adding buckets later
	// is backward compatible). No further pages in the scaffold.
	return []UsageSummary{summary}, grandTotal, "", nil
}

// ============================================================================
// GetInvoice / ListInvoices — read
// ============================================================================

func (s *billingService) GetInvoice(ctx context.Context, id string) (Invoice, error) {
	inv, err := s.invoices.GetInvoice(ctx, id)
	if err != nil {
		if errors.Is(err, ErrRepoNotFound) {
			return Invoice{}, ErrInvoiceNotFound
		}
		return Invoice{}, fmt.Errorf("getting invoice: %w", err)
	}
	return inv, nil
}

func (s *billingService) ListInvoices(ctx context.Context, opts ListInvoicesOptions) ([]Invoice, string, error) {
	if opts.Team == "" {
		return nil, "", fmt.Errorf("%w: team is required", ErrValidation)
	}
	// Clamp the page size as defense in depth (the handler also clamps).
	if opts.PageSize <= 0 {
		opts.PageSize = defaultPageSize
	} else if opts.PageSize > MaxPageSize {
		opts.PageSize = MaxPageSize
	}
	invoices, nextToken, err := s.invoices.ListInvoices(ctx, opts)
	if err != nil {
		return nil, "", fmt.Errorf("listing invoices: %w", err)
	}
	return invoices, nextToken, nil
}

// ============================================================================
// GenerateInvoice — period close (the InvoiceGenerated outbox path)
// ============================================================================

// GenerateInvoice rolls a team's period usage into a FINALIZED invoice and
// persists it with an InvoiceGenerated outbox event in ONE tx. Driven by a
// scheduled period-close job (no RPC creates invoices — they are server-derived).
//
// THE MONEY MATH LIVES HERE (overflow-safe), the SET MATH lives in the store:
//
//	the store returns per-meter buckets (GROUP BY); the domain turns each into a
//	line item, sums them with the checked Money.Add, and rounds the total to
//	cents ONCE at finalization (sum-then-round, never round-then-sum).
func (s *billingService) GenerateInvoice(ctx context.Context, team string, periodStart, periodEnd time.Time) (Invoice, error) {
	if team == "" {
		return Invoice{}, fmt.Errorf("%w: team is required", ErrValidation)
	}
	if !periodEnd.After(periodStart) {
		return Invoice{}, fmt.Errorf("%w: end %v must be after start %v", ErrInvalidPeriod, periodEnd, periodStart)
	}
	if periodEnd.Sub(periodStart) > MaxUsageWindow {
		return Invoice{}, fmt.Errorf("%w: period exceeds max %v", ErrInvalidPeriod, MaxUsageWindow)
	}

	buckets, err := s.invoices.AggregateUsageForPeriod(ctx, team, periodStart, periodEnd)
	if err != nil {
		return Invoice{}, fmt.Errorf("aggregating period usage: %w", err)
	}
	if len(buckets) == 0 {
		// No usage ⇒ skip a $0 invoice (and its event). The caller errors.Is this
		// to branch, not to log a failure.
		return Invoice{}, ErrNoUsage
	}

	// Resolve the plan to pin on the invoice (reporting/transparency).
	plan, err := s.plans.ResolvePlanForTeam(ctx, team)
	if err != nil {
		if errors.Is(err, ErrRepoNotFound) {
			return Invoice{}, ErrRatePlanNotFound
		}
		return Invoice{}, fmt.Errorf("resolving rate plan: %w", err)
	}

	lineItems := make([]InvoiceLineItem, 0, len(buckets))
	var total Money
	for _, b := range buckets {
		unitPrice, _ := plan.PriceFor(b.MeterType) // may be zero-value if plan changed; cost is authoritative
		lineItems = append(lineItems, InvoiceLineItem{
			MeterType:   b.MeterType,
			Description: lineDescription(b.MeterType),
			Quantity:    b.TotalQuantity,
			UnitPrice:   unitPrice,
			Amount:      b.TotalCost,
		})
		// Overflow-safe + currency-checked accumulation. If two buckets somehow had
		// different currencies (a plan-currency bug), Add returns ErrCurrencyMismatch
		// and we refuse to emit a nonsense total rather than silently combine them.
		total, err = total.Add(b.TotalCost)
		if err != nil {
			return Invoice{}, fmt.Errorf("summing invoice total: %w", err)
		}
	}

	// Round to the currency's minor unit ONCE, at finalization. Internally we summed
	// sub-cent micros exactly; the customer is billed whole cents.
	total = total.RoundToCents()

	now := s.clock.Now()
	dueAt := now.AddDate(0, 0, 30) // net-30 terms (a sane default; configurable later)
	invoice := Invoice{
		ID:            s.ids.NewID(),
		InvoiceNumber: s.invoiceNumber(now),
		Team:          team,
		RatePlanID:    plan.ID,
		Status:        InvoiceStatusFinalized, // period close locks the totals
		PeriodStart:   periodStart,
		PeriodEnd:     periodEnd,
		LineItems:     lineItems,
		Total:         total,
		CreatedAt:     now,
		FinalizedAt:   &now,
		DueAt:         &dueAt,
	}

	// OUTBOX: the InvoiceGenerated event commits with the invoice (same tx). Its
	// payload flattens the total to micros+currency (no embedded Invoice) so
	// Notification can render/send without depending on this API proto.
	event := s.buildOutboxEvent(invoice.ID, InvoiceGeneratedPayload{
		InvoiceID:     invoice.ID,
		InvoiceNumber: invoice.InvoiceNumber,
		Team:          team,
		RatePlanID:    plan.ID,
		PeriodStart:   periodStart,
		PeriodEnd:     periodEnd,
		TotalMicros:   total.AmountMicros,
		CurrencyCode:  total.CurrencyCode,
		FinalizedAt:   now,
	}, now)

	stored, err := s.invoices.SaveInvoiceTx(ctx, invoice, event)
	if err != nil {
		return Invoice{}, fmt.Errorf("saving invoice: %w", err)
	}
	return stored, nil
}

// invoiceNumber builds a human-facing, printable invoice number. Distinct from
// the UUID (which is the internal key) so it is safe to share. WHY include the
// id suffix: it makes the number unique without a separate sequence table in the
// scaffold; the repo phase replaces this with a monotonic DB sequence
// ("INV-2026-000042"). Kept deterministic-from-clock+id so tests are stable.
func (s *billingService) invoiceNumber(now time.Time) string {
	return fmt.Sprintf("INV-%d-%s", now.Year(), s.ids.NewID())
}

// lineDescription renders a customer-facing label for a meter line.
func lineDescription(mt MeterType) string {
	switch mt {
	case MeterTypeInferenceRequest:
		return "Inference requests"
	case MeterTypeInferenceTokens:
		return "Inference tokens"
	case MeterTypeComputeSeconds:
		return "Compute seconds"
	case MeterTypeStorageBytes:
		return "Storage bytes"
	default:
		return string(mt)
	}
}
