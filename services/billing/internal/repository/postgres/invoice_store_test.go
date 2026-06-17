// invoice_store_test.go — integration tests for the InvoiceStore adapter against
// REAL Postgres. Verifies: the GROUP BY aggregation (the SET MATH the domain
// delegates), SaveInvoiceTx's outbox atomicity (invoice + InvoiceGenerated event
// in one tx), the not-found sentinel, the line-item + nullable-timestamp JSONB
// round trip, the unique invoice_number constraint, and keyset pagination
// (newest-first, status filter, stable cursor across pages).
package postgres

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/billing/internal/domain"
)

// seedUsage records n token usages of `qty` each for a team within the period, so
// the aggregation has real ledger rows to roll up. Returns the team.
func seedUsage(t *testing.T, ctx context.Context, store *Store, team, planID string, meter domain.MeterType, qty, count int64, occurredAt time.Time) {
	t.Helper()
	for i := int64(0); i < count; i++ {
		rec := usageRecord(team, planID, meter, qty, usd(qty*400), "")
		rec.OccurredAt = occurredAt // pin into the target period
		if _, err := store.Usage().RecordUsageTx(ctx, rec, []domain.OutboxEvent{usageRecordedEvent(rec)}); err != nil {
			t.Fatalf("seedUsage RecordUsageTx: %v", err)
		}
	}
}

// TestInvoiceStore_AggregateUsageForPeriod proves the GROUP BY: per-meter buckets
// summing quantity and cost, scoped to the half-open [start, end) window, with an
// EMPTY slice for a team with no usage (the service maps empty → ErrNoUsage).
func TestInvoiceStore_AggregateUsageForPeriod(t *testing.T) {
	store, ctx := newTestStore(t)
	inv := store.Invoices()
	team := "team-" + newID()
	plan := mustCreatePlanAndAssign(t, ctx, store, team, samplePlan())

	periodStart := nowMicro().Add(-24 * time.Hour)
	periodEnd := nowMicro().Add(24 * time.Hour)
	inPeriod := nowMicro()

	// Empty period → empty slice, no error.
	buckets, err := inv.AggregateUsageForPeriod(ctx, team, periodStart, periodEnd)
	if err != nil {
		t.Fatalf("AggregateUsageForPeriod(empty): %v", err)
	}
	if len(buckets) != 0 {
		t.Fatalf("empty-period buckets = %d, want 0", len(buckets))
	}

	// 3 token records of 10 (qty 30, cost 30*400=12000) + 2 request records of 1.
	seedUsage(t, ctx, store, team, plan.ID, domain.MeterTypeInferenceTokens, 10, 3, inPeriod)
	seedUsage(t, ctx, store, team, plan.ID, domain.MeterTypeInferenceRequest, 1, 2, inPeriod)

	// A record OUTSIDE the window must NOT be counted (proves the time predicate).
	out := usageRecord(team, plan.ID, domain.MeterTypeInferenceTokens, 999, usd(999*400), "")
	out.OccurredAt = periodStart.Add(-time.Hour) // before the window
	if _, err := store.Usage().RecordUsageTx(ctx, out, []domain.OutboxEvent{usageRecordedEvent(out)}); err != nil {
		t.Fatalf("seed out-of-window: %v", err)
	}

	buckets, err = inv.AggregateUsageForPeriod(ctx, team, periodStart, periodEnd)
	if err != nil {
		t.Fatalf("AggregateUsageForPeriod: %v", err)
	}
	byMeter := map[domain.MeterType]domain.MeterUsage{}
	for _, b := range buckets {
		byMeter[b.MeterType] = b
	}
	tokens := byMeter[domain.MeterTypeInferenceTokens]
	if tokens.TotalQuantity != 30 {
		t.Errorf("tokens qty = %d, want 30 (out-of-window 999 excluded)", tokens.TotalQuantity)
	}
	if tokens.TotalCost != usd(12000) {
		t.Errorf("tokens cost = %+v, want %+v", tokens.TotalCost, usd(12000))
	}
	reqs := byMeter[domain.MeterTypeInferenceRequest]
	if reqs.TotalQuantity != 2 {
		t.Errorf("requests qty = %d, want 2", reqs.TotalQuantity)
	}
	// seedUsage stores cost = qty*400 per record; 2 request records of qty 1 →
	// 2 * (1*400) = 800 micros. The aggregation sums exactly the stored costs, so
	// the expectation must mirror what was seeded (this asserts the DB SUM, not the
	// plan price — pricing is the domain's job, summing is the store's).
	if reqs.TotalCost != usd(800) {
		t.Errorf("requests cost = %+v, want %+v", reqs.TotalCost, usd(800))
	}
}

// TestInvoiceStore_SaveInvoiceTx_WritesInvoiceAndOutboxAtomically proves the
// SECOND outbox boundary: the finalized invoice AND its InvoiceGenerated event
// commit together, the event is unpublished, and the invoice (with line items and
// nullable timestamps) round-trips exactly.
func TestInvoiceStore_SaveInvoiceTx_WritesInvoiceAndOutboxAtomically(t *testing.T) {
	store, ctx := newTestStore(t)
	inv := store.Invoices()
	team := "team-" + newID()
	plan := mustCreatePlanAndAssign(t, ctx, store, team, samplePlan())

	invoice, event := buildFinalizedInvoice(team, plan.ID)

	stored, err := inv.SaveInvoiceTx(ctx, invoice, event)
	if err != nil {
		t.Fatalf("SaveInvoiceTx: %v", err)
	}
	if stored.ID != invoice.ID {
		t.Errorf("stored.ID = %s, want %s", stored.ID, invoice.ID)
	}

	// Outbox row landed in the same tx, unpublished.
	if n := countOutbox(t, ctx, store, invoice.ID); n != 1 {
		t.Fatalf("invoice outbox count = %d, want 1", n)
	}
	if n := countUnpublishedOutbox(t, ctx, store, invoice.ID); n != 1 {
		t.Fatalf("unpublished invoice outbox = %d, want 1", n)
	}
	if got := outboxEventTypes(t, ctx, store, invoice.ID); len(got) != 1 || got[0] != domain.EventTypeInvoiceGenerated {
		t.Fatalf("outbox types = %v, want [%s]", got, domain.EventTypeInvoiceGenerated)
	}

	// Full round trip: line items + nullable finalized_at/due_at survive.
	got, err := inv.GetInvoice(ctx, invoice.ID)
	if err != nil {
		t.Fatalf("GetInvoice: %v", err)
	}
	assertInvoiceEqual(t, got, invoice)
}

func TestInvoiceStore_GetInvoice_NotFound(t *testing.T) {
	store, ctx := newTestStore(t)
	_, err := store.Invoices().GetInvoice(ctx, newID())
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("GetInvoice(missing) = %v, want ErrRepoNotFound", err)
	}
}

// TestInvoiceStore_DuplicateInvoiceNumber proves the UNIQUE(invoice_number)
// constraint surfaces as ErrRepoDuplicate (a generation bug / retried finalize).
func TestInvoiceStore_DuplicateInvoiceNumber(t *testing.T) {
	store, ctx := newTestStore(t)
	inv := store.Invoices()
	team := "team-" + newID()
	plan := mustCreatePlanAndAssign(t, ctx, store, team, samplePlan())

	first, ev1 := buildFinalizedInvoice(team, plan.ID)
	if _, err := inv.SaveInvoiceTx(ctx, first, ev1); err != nil {
		t.Fatalf("first SaveInvoiceTx: %v", err)
	}

	// Second invoice reuses the SAME invoice_number (different id) → unique violation.
	second, ev2 := buildFinalizedInvoice(team, plan.ID)
	second.InvoiceNumber = first.InvoiceNumber
	_, err := inv.SaveInvoiceTx(ctx, second, ev2)
	if !errors.Is(err, domain.ErrRepoDuplicate) {
		t.Fatalf("duplicate invoice_number error = %v, want ErrRepoDuplicate", err)
	}

	// Ground truth: the second invoice's outbox row rolled back (no phantom event).
	if n := countOutbox(t, ctx, store, second.ID); n != 0 {
		t.Fatalf("rolled-back invoice outbox = %d, want 0", n)
	}
}

// TestInvoiceStore_ListInvoices_PaginationAndFilter proves keyset pagination:
// newest-first ordering, a stable cursor that walks every row exactly once across
// pages with no dup/skip, and the status filter.
func TestInvoiceStore_ListInvoices_PaginationAndFilter(t *testing.T) {
	store, ctx := newTestStore(t)
	inv := store.Invoices()
	team := "team-" + newID()
	plan := mustCreatePlanAndAssign(t, ctx, store, team, samplePlan())

	// Insert 5 invoices with strictly increasing created_at so ordering is
	// deterministic. 3 FINALIZED, 2 VOID — to exercise the status filter.
	base := nowMicro().Add(-time.Hour)
	var allIDs []string
	for i := 0; i < 5; i++ {
		invoice, event := buildFinalizedInvoice(team, plan.ID)
		invoice.CreatedAt = base.Add(time.Duration(i) * time.Minute)
		if i >= 3 {
			invoice.Status = domain.InvoiceStatusVoid
		}
		if _, err := inv.SaveInvoiceTx(ctx, invoice, event); err != nil {
			t.Fatalf("seed invoice #%d: %v", i, err)
		}
		allIDs = append(allIDs, invoice.ID)
	}

	// Walk all invoices in pages of 2; collect ids; assert newest-first + complete.
	var got []string
	token := ""
	for {
		page, next, err := inv.ListInvoices(ctx, domain.ListInvoicesOptions{
			Team: team, PageSize: 2, PageToken: token,
		})
		if err != nil {
			t.Fatalf("ListInvoices: %v", err)
		}
		for _, p := range page {
			got = append(got, p.ID)
		}
		if next == "" {
			break
		}
		token = next
		if len(got) > 5 {
			t.Fatalf("pagination did not terminate (got %d ids)", len(got))
		}
	}
	if len(got) != 5 {
		t.Fatalf("paginated total = %d, want 5", len(got))
	}
	// Newest-first: reverse insertion order (index 4 first ... index 0 last).
	for i := 0; i < 5; i++ {
		want := allIDs[4-i]
		if got[i] != want {
			t.Errorf("page order[%d] = %s, want %s (newest-first)", i, got[i], want)
		}
	}

	// Status filter: only the 2 VOID invoices (the last two inserted, i=3,4).
	voids, _, err := inv.ListInvoices(ctx, domain.ListInvoicesOptions{
		Team: team, StatusFilter: domain.InvoiceStatusVoid, PageSize: 10,
	})
	if err != nil {
		t.Fatalf("ListInvoices(VOID): %v", err)
	}
	if len(voids) != 2 {
		t.Fatalf("VOID invoices = %d, want 2", len(voids))
	}
	for _, v := range voids {
		if v.Status != domain.InvoiceStatusVoid {
			t.Errorf("filtered invoice has status %s, want VOID", v.Status)
		}
	}
}

// TestInvoiceStore_ListInvoices_TeamScoped proves a list only ever returns the
// caller's team — tenancy isolation at the persistence layer.
func TestInvoiceStore_ListInvoices_TeamScoped(t *testing.T) {
	store, ctx := newTestStore(t)
	inv := store.Invoices()
	teamA := "team-a-" + newID()
	teamB := "team-b-" + newID()
	planA := mustCreatePlanAndAssign(t, ctx, store, teamA, samplePlan())
	planB := mustCreatePlanAndAssign(t, ctx, store, teamB, samplePlan())

	iA, eA := buildFinalizedInvoice(teamA, planA.ID)
	iB, eB := buildFinalizedInvoice(teamB, planB.ID)
	if _, err := inv.SaveInvoiceTx(ctx, iA, eA); err != nil {
		t.Fatalf("save A: %v", err)
	}
	if _, err := inv.SaveInvoiceTx(ctx, iB, eB); err != nil {
		t.Fatalf("save B: %v", err)
	}

	page, _, err := inv.ListInvoices(ctx, domain.ListInvoicesOptions{Team: teamA, PageSize: 10})
	if err != nil {
		t.Fatalf("ListInvoices(A): %v", err)
	}
	if len(page) != 1 || page[0].ID != iA.ID {
		t.Fatalf("team A list = %+v, want only invoice %s", page, iA.ID)
	}
}

// buildFinalizedInvoice constructs a finalized Invoice with two line items and its
// InvoiceGenerated outbox event — the shape GenerateInvoice produces. Returns both
// so SaveInvoiceTx tests pass the pair.
func buildFinalizedInvoice(team, planID string) (domain.Invoice, domain.OutboxEvent) {
	now := nowMicro()
	due := now.AddDate(0, 0, 30)
	periodStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	periodEnd := periodStart.AddDate(0, 1, 0)

	lineItems := []domain.InvoiceLineItem{
		{
			MeterType:   domain.MeterTypeInferenceTokens,
			Description: "Inference tokens",
			Quantity:    30,
			UnitPrice:   usd(400),
			Amount:      usd(12000),
		},
		{
			MeterType:   domain.MeterTypeInferenceRequest,
			Description: "Inference requests",
			Quantity:    2,
			UnitPrice:   usd(1000000),
			Amount:      usd(2000000),
		},
	}
	total := usd(2012000)

	id := newID()
	invoice := domain.Invoice{
		ID:            id,
		InvoiceNumber: "INV-2026-" + id[:8],
		Team:          team,
		RatePlanID:    planID,
		Status:        domain.InvoiceStatusFinalized,
		PeriodStart:   periodStart,
		PeriodEnd:     periodEnd,
		LineItems:     lineItems,
		Total:         total,
		CreatedAt:     now,
		FinalizedAt:   ptr(now),
		DueAt:         ptr(due),
	}
	event := domain.OutboxEvent{
		ID:          newID(),
		AggregateID: invoice.ID,
		EventType:   domain.EventTypeInvoiceGenerated,
		Payload: domain.InvoiceGeneratedPayload{
			InvoiceID:     invoice.ID,
			InvoiceNumber: invoice.InvoiceNumber,
			Team:          team,
			RatePlanID:    planID,
			PeriodStart:   periodStart,
			PeriodEnd:     periodEnd,
			TotalMicros:   total.AmountMicros,
			CurrencyCode:  total.CurrencyCode,
			FinalizedAt:   now,
		},
		CreatedAt: now,
	}
	return invoice, event
}

// assertInvoiceEqual compares a read-back invoice to what was written, including
// the line items (JSONB array) and the nullable finalized_at/due_at pointers.
func assertInvoiceEqual(t *testing.T, got, want domain.Invoice) {
	t.Helper()
	if got.ID != want.ID || got.InvoiceNumber != want.InvoiceNumber || got.Team != want.Team {
		t.Errorf("identity mismatch: got (%s,%s,%s) want (%s,%s,%s)",
			got.ID, got.InvoiceNumber, got.Team, want.ID, want.InvoiceNumber, want.Team)
	}
	if got.Status != want.Status {
		t.Errorf("status = %s, want %s", got.Status, want.Status)
	}
	if got.Total != want.Total {
		t.Errorf("total = %+v, want %+v", got.Total, want.Total)
	}
	if !reflect.DeepEqual(got.LineItems, want.LineItems) {
		t.Errorf("line items mismatch:\n got  %#v\n want %#v", got.LineItems, want.LineItems)
	}
	if !got.PeriodStart.Equal(want.PeriodStart) || !got.PeriodEnd.Equal(want.PeriodEnd) {
		t.Errorf("period mismatch: got [%v,%v) want [%v,%v)",
			got.PeriodStart, got.PeriodEnd, want.PeriodStart, want.PeriodEnd)
	}
	if !got.CreatedAt.Equal(want.CreatedAt) {
		t.Errorf("created_at = %v, want %v", got.CreatedAt, want.CreatedAt)
	}
	// Nullable timestamps: both nil or both equal.
	assertTimePtrEqual(t, "finalized_at", got.FinalizedAt, want.FinalizedAt)
	assertTimePtrEqual(t, "due_at", got.DueAt, want.DueAt)
}

func assertTimePtrEqual(t *testing.T, field string, got, want *time.Time) {
	t.Helper()
	switch {
	case got == nil && want == nil:
		return
	case got == nil || want == nil:
		t.Errorf("%s nil mismatch: got %v want %v", field, got, want)
	case !got.Equal(*want):
		t.Errorf("%s = %v, want %v", field, *got, *want)
	}
}
