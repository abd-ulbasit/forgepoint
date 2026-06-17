// billing_service_test.go — TDD specification for the billing domain.
//
// ============================================================================
// WHY THIS IS AN EXTERNAL TEST PACKAGE (domain_test)
// ============================================================================
//
// Same reasoning as Auth: writing tests in the EXTERNAL test package forces them
// to exercise only the EXPORTED surface — the exact contract the handler/repo
// depend on. (Billing's ports live in `domain` so there is no import cycle to
// dodge here, unlike Auth; external-test is still the better discipline.)
//
// THESE TESTS ASSERT REAL BEHAVIOR, NOT MOCK CALLS:
//   - money math: actual products/sums, actual overflow rejection, actual
//     round-half-up — computed and compared by value.
//   - metering: the stored record's Cost equals quantity×price after the free
//     allowance, derived independently in the test.
//   - idempotency: a second call returns the SAME record the mock holds, and the
//     store's insert is NOT called a second time (state, not just a flag).
//   - outbox: the event the service handed the store actually carries the
//     matching team/cost (we inspect the captured OutboxEvent payload).
//
// ============================================================================
package domain_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/billing/internal/domain"
)

// ----------------------------------------------------------------------------
// Hand-written mocks (no codegen — every line interview-explainable).
// Unset function fields panic, surfacing accidental coupling. The usage store
// keeps in-memory state so idempotency can be asserted as REAL behavior (a
// second call sees the first call's row), not just a returned bool.
// ----------------------------------------------------------------------------

type mockUsageStore struct {
	byKey       map[string]domain.UsageRecord // (team|key) → record, for idempotency
	periodUsage map[string]int64              // (team|meter) → prior usage
	recordCalls int                           // how many times RecordUsageTx inserted
	lastEvents  []domain.OutboxEvent          // the outbox events from the last write (for assertions)
	// dupOnInsert, when set, makes the NEXT RecordUsageTx return ErrRepoDuplicate
	// then serve the stored record — simulating a concurrent-writer race.
	dupOnInsert bool
}

func newMockUsageStore() *mockUsageStore {
	return &mockUsageStore{
		byKey:       make(map[string]domain.UsageRecord),
		periodUsage: make(map[string]int64),
	}
}

func keyOf(team, k string) string                     { return team + "|" + k }
func meterKey(team string, m domain.MeterType) string { return team + "|" + string(m) }

func (m *mockUsageStore) FindByIdempotencyKey(_ context.Context, team, key string) (domain.UsageRecord, error) {
	if rec, ok := m.byKey[keyOf(team, key)]; ok {
		return rec, nil
	}
	return domain.UsageRecord{}, domain.ErrRepoNotFound
}

func (m *mockUsageStore) CurrentPeriodUsage(_ context.Context, team string, meter domain.MeterType, _ time.Time) (int64, error) {
	return m.periodUsage[meterKey(team, meter)], nil
}

func (m *mockUsageStore) RecordUsageTx(_ context.Context, record domain.UsageRecord, events []domain.OutboxEvent) (domain.UsageRecord, error) {
	if m.dupOnInsert {
		// Simulate the unique (team, idempotency_key) constraint firing because a
		// concurrent writer already inserted. The service must then re-read.
		m.dupOnInsert = false
		return domain.UsageRecord{}, domain.ErrRepoDuplicate
	}
	m.recordCalls++
	m.lastEvents = events
	m.byKey[keyOf(record.Team, record.IdempotencyKey)] = record
	// Reflect the write into period usage so a later CheckQuota/allowance sees it.
	m.periodUsage[meterKey(record.Team, record.MeterType)] += record.Quantity
	return record, nil
}

type mockRatePlanStore struct {
	planForTeam  map[string]domain.RatePlan
	planByID     map[string]domain.RatePlan
	createdByKey map[string]domain.RatePlan
}

func newMockRatePlanStore() *mockRatePlanStore {
	return &mockRatePlanStore{
		planForTeam:  make(map[string]domain.RatePlan),
		planByID:     make(map[string]domain.RatePlan),
		createdByKey: make(map[string]domain.RatePlan),
	}
}

func (m *mockRatePlanStore) CreatePlan(_ context.Context, plan domain.RatePlan, key string) (domain.RatePlan, error) {
	if _, ok := m.createdByKey[key]; ok && key != "" {
		return domain.RatePlan{}, domain.ErrRepoDuplicate
	}
	m.planByID[plan.ID] = plan
	if key != "" {
		m.createdByKey[key] = plan
	}
	return plan, nil
}
func (m *mockRatePlanStore) FindPlanByIdempotencyKey(_ context.Context, key string) (domain.RatePlan, error) {
	if p, ok := m.createdByKey[key]; ok {
		return p, nil
	}
	return domain.RatePlan{}, domain.ErrRepoNotFound
}
func (m *mockRatePlanStore) GetPlan(_ context.Context, id string) (domain.RatePlan, error) {
	if p, ok := m.planByID[id]; ok {
		return p, nil
	}
	return domain.RatePlan{}, domain.ErrRepoNotFound
}
func (m *mockRatePlanStore) ResolvePlanForTeam(_ context.Context, team string) (domain.RatePlan, error) {
	if p, ok := m.planForTeam[team]; ok {
		return p, nil
	}
	return domain.RatePlan{}, domain.ErrRepoNotFound
}

type mockInvoiceStore struct {
	aggregate  []domain.MeterUsage
	savedTx    *domain.Invoice
	savedEvent *domain.OutboxEvent
	byID       map[string]domain.Invoice
}

func newMockInvoiceStore() *mockInvoiceStore {
	return &mockInvoiceStore{byID: make(map[string]domain.Invoice)}
}

func (m *mockInvoiceStore) AggregateUsageForPeriod(_ context.Context, _ string, _, _ time.Time) ([]domain.MeterUsage, error) {
	return m.aggregate, nil
}
func (m *mockInvoiceStore) SaveInvoiceTx(_ context.Context, inv domain.Invoice, ev domain.OutboxEvent) (domain.Invoice, error) {
	m.savedTx = &inv
	m.savedEvent = &ev
	m.byID[inv.ID] = inv
	return inv, nil
}
func (m *mockInvoiceStore) GetInvoice(_ context.Context, id string) (domain.Invoice, error) {
	if inv, ok := m.byID[id]; ok {
		return inv, nil
	}
	return domain.Invoice{}, domain.ErrRepoNotFound
}
func (m *mockInvoiceStore) ListInvoices(_ context.Context, _ domain.ListInvoicesOptions) ([]domain.Invoice, string, error) {
	return nil, "", nil
}

// Deterministic id/clock seams so we can assert exact equality.
type seqIDProvider struct{ n int }

func (s *seqIDProvider) NewID() string {
	s.n++
	return "id-" + itoa(s.n)
}

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

// itoa avoids importing strconv just for the mock id.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// Compile-time proof the mocks satisfy the ports. Drift fails the build.
var (
	_ domain.UsageStore    = (*mockUsageStore)(nil)
	_ domain.RatePlanStore = (*mockRatePlanStore)(nil)
	_ domain.InvoiceStore  = (*mockInvoiceStore)(nil)
	_ domain.IDProvider    = (*seqIDProvider)(nil)
	_ domain.Clock         = fixedClock{}
)

// usdPlan builds a standard single-currency plan for tests.
func usdPlan(id string) domain.RatePlan {
	return domain.RatePlan{
		ID:   id,
		Name: "standard",
		UnitPrices: map[domain.MeterType]domain.Money{
			// 0.000400 USD per token == 400 micro-USD.
			domain.MeterTypeInferenceTokens: {AmountMicros: 400, CurrencyCode: "USD"},
			// 1000 micro-USD per request == 0.001 USD.
			domain.MeterTypeInferenceRequest: {AmountMicros: 1000, CurrencyCode: "USD"},
		},
		IncludedQuantities: map[domain.MeterType]int64{
			domain.MeterTypeInferenceTokens: 100, // first 100 tokens free
		},
		QuotaLimits: map[domain.MeterType]int64{
			domain.MeterTypeInferenceTokens: 1000, // hard cap 1000 tokens/period
		},
		CreatedAt: time.Unix(0, 0),
	}
}

func newSvc(t *testing.T) (domain.BillingService, *mockUsageStore, *mockRatePlanStore, *mockInvoiceStore) {
	t.Helper()
	us := newMockUsageStore()
	rs := newMockRatePlanStore()
	is := newMockInvoiceStore()
	clk := fixedClock{t: time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)}
	svc := domain.NewBillingService(us, rs, is, &seqIDProvider{}, clk)
	return svc, us, rs, is
}

// ============================================================================
// MONEY MATH — the overflow guards and rounding (pure, no service).
// ============================================================================

func TestMoneyAdd_SameCurrency(t *testing.T) {
	a := domain.Money{AmountMicros: 400, CurrencyCode: "USD"}
	b := domain.Money{AmountMicros: 600, CurrencyCode: "USD"}
	got, err := a.Add(b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.AmountMicros != 1000 || got.CurrencyCode != "USD" {
		t.Fatalf("got %+v, want {1000 USD}", got)
	}
}

func TestMoneyAdd_CurrencyMismatch(t *testing.T) {
	a := domain.Money{AmountMicros: 1, CurrencyCode: "USD"}
	b := domain.Money{AmountMicros: 1, CurrencyCode: "EUR"}
	if _, err := a.Add(b); !errors.Is(err, domain.ErrCurrencyMismatch) {
		t.Fatalf("want ErrCurrencyMismatch, got %v", err)
	}
}

func TestMoneyAdd_ZeroIsIdentity(t *testing.T) {
	// A zero-value accumulator (no currency) must absorb the first real addend so
	// fold-style summation works without seeding the currency.
	zero := domain.Money{}
	x := domain.Money{AmountMicros: 42, CurrencyCode: "EUR"}
	got, err := zero.Add(x)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != x {
		t.Fatalf("zero identity broken: got %+v want %+v", got, x)
	}
}

func TestMoneyAdd_OverflowRejected(t *testing.T) {
	// MaxInt64 + 1 micro must NOT wrap to a negative — it must error.
	a := domain.Money{AmountMicros: math.MaxInt64, CurrencyCode: "USD"}
	b := domain.Money{AmountMicros: 1, CurrencyCode: "USD"}
	if _, err := a.Add(b); !errors.Is(err, domain.ErrAmountOverflow) {
		t.Fatalf("want ErrAmountOverflow on add wrap, got %v", err)
	}
}

func TestMoneyRoundToCents(t *testing.T) {
	cases := []struct {
		name   string
		micros int64
		want   int64
	}{
		// 1.234567 USD → 1.23 USD (1_230_000 micros). 4567 sub-cent rounds down.
		{"round down", 1_234_567, 1_230_000},
		// 1.235000 USD → exactly half a cent over 1.23 → round half UP to 1.24.
		{"round half up", 1_235_000, 1_240_000},
		// already whole cents → unchanged.
		{"exact cents", 1_240_000, 1_240_000},
		// negative credit rounds away from zero (-0.005 → -0.01).
		{"negative half up", -1_235_000, -1_240_000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := domain.Money{AmountMicros: tc.micros, CurrencyCode: "USD"}.RoundToCents()
			if got.AmountMicros != tc.want {
				t.Fatalf("RoundToCents(%d) = %d, want %d", tc.micros, got.AmountMicros, tc.want)
			}
		})
	}
}

func TestCurrencyCodeValidation(t *testing.T) {
	valid := []string{"USD", "EUR", "JPY", "GBP"}
	invalid := []string{"", "us", "usd", "US$", "USDD", "Usd", "12A"}
	for _, c := range valid {
		if !domain.IsValidCurrencyCode(c) {
			t.Errorf("IsValidCurrencyCode(%q) = false, want true", c)
		}
	}
	for _, c := range invalid {
		if domain.IsValidCurrencyCode(c) {
			t.Errorf("IsValidCurrencyCode(%q) = true, want false", c)
		}
	}
}

// ============================================================================
// RECORD USAGE — pricing, free allowance, overflow, idempotency, outbox.
// ============================================================================

func TestRecordUsage_PricesAfterFreeAllowance(t *testing.T) {
	svc, us, rs, _ := newSvc(t)
	rs.planForTeam["acme"] = usdPlan("plan-1")

	// 150 tokens, 100 free → 50 billable × 400 micros = 20_000 micros (0.02 USD).
	rec, dedup, err := svc.RecordUsage(context.Background(), "acme", domain.RecordUsageInput{
		MeterType:       domain.MeterTypeInferenceTokens,
		Quantity:        150,
		SourceRequestID: "req-1",
		IdempotencyKey:  "idem-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dedup {
		t.Fatalf("first call must not be deduplicated")
	}
	if rec.Quantity != 50 {
		t.Fatalf("billable quantity = %d, want 50 (after 100 free)", rec.Quantity)
	}
	if rec.Cost.AmountMicros != 20_000 || rec.Cost.CurrencyCode != "USD" {
		t.Fatalf("cost = %+v, want {20000 USD}", rec.Cost)
	}
	// Server-authoritative fields must be set by the server, not the client.
	if rec.Team != "acme" {
		t.Fatalf("team = %q, want acme (server-derived)", rec.Team)
	}
	if rec.RatePlanID != "plan-1" {
		t.Fatalf("rate plan = %q, want plan-1 (server-resolved+pinned)", rec.RatePlanID)
	}
	if us.recordCalls != 1 {
		t.Fatalf("RecordUsageTx calls = %d, want 1", us.recordCalls)
	}
}

func TestRecordUsage_RejectsNegativeQuantity(t *testing.T) {
	svc, _, rs, _ := newSvc(t)
	rs.planForTeam["acme"] = usdPlan("plan-1")
	_, _, err := svc.RecordUsage(context.Background(), "acme", domain.RecordUsageInput{
		MeterType: domain.MeterTypeInferenceTokens, Quantity: -5, IdempotencyKey: "n",
	})
	if !errors.Is(err, domain.ErrNegativeQuantity) {
		t.Fatalf("want ErrNegativeQuantity, got %v", err)
	}
}

func TestRecordUsage_RejectsQuantityOverCap(t *testing.T) {
	svc, _, rs, _ := newSvc(t)
	rs.planForTeam["acme"] = usdPlan("plan-1")
	_, _, err := svc.RecordUsage(context.Background(), "acme", domain.RecordUsageInput{
		MeterType:      domain.MeterTypeInferenceTokens,
		Quantity:       domain.MaxQuantityPerRecord + 1,
		IdempotencyKey: "big",
	})
	if !errors.Is(err, domain.ErrQuantityTooLarge) {
		t.Fatalf("want ErrQuantityTooLarge, got %v", err)
	}
}

func TestRecordUsage_RejectsUnknownMeter(t *testing.T) {
	svc, _, rs, _ := newSvc(t)
	rs.planForTeam["acme"] = usdPlan("plan-1")
	_, _, err := svc.RecordUsage(context.Background(), "acme", domain.RecordUsageInput{
		MeterType: domain.MeterTypeUnspecified, Quantity: 1, IdempotencyKey: "u",
	})
	if !errors.Is(err, domain.ErrValidation) && !errors.Is(err, domain.ErrUnknownMeter) {
		t.Fatalf("want ErrValidation/ErrUnknownMeter, got %v", err)
	}
}

func TestRecordUsage_UnpricedMeterRejected(t *testing.T) {
	svc, _, rs, _ := newSvc(t)
	// Plan prices tokens+requests but NOT compute-seconds. Metering compute must
	// NOT silently bill $0 — it must error.
	rs.planForTeam["acme"] = usdPlan("plan-1")
	_, _, err := svc.RecordUsage(context.Background(), "acme", domain.RecordUsageInput{
		MeterType: domain.MeterTypeComputeSeconds, Quantity: 10, IdempotencyKey: "c",
	})
	if !errors.Is(err, domain.ErrUnknownMeter) {
		t.Fatalf("want ErrUnknownMeter for unpriced meter, got %v", err)
	}
}

func TestRecordUsage_NoPlanForTeam(t *testing.T) {
	svc, _, _, _ := newSvc(t)
	// No plan assigned → cannot price → FailedPrecondition (ErrRatePlanNotFound).
	_, _, err := svc.RecordUsage(context.Background(), "ghost", domain.RecordUsageInput{
		MeterType: domain.MeterTypeInferenceTokens, Quantity: 1, IdempotencyKey: "x",
	})
	if !errors.Is(err, domain.ErrRatePlanNotFound) {
		t.Fatalf("want ErrRatePlanNotFound, got %v", err)
	}
}

func TestRecordUsage_Idempotent(t *testing.T) {
	svc, us, rs, _ := newSvc(t)
	rs.planForTeam["acme"] = usdPlan("plan-1")
	in := domain.RecordUsageInput{
		MeterType: domain.MeterTypeInferenceRequest, Quantity: 1, IdempotencyKey: "same",
	}
	first, dedup1, err := svc.RecordUsage(context.Background(), "acme", in)
	if err != nil || dedup1 {
		t.Fatalf("first call: err=%v dedup=%v", err, dedup1)
	}
	second, dedup2, err := svc.RecordUsage(context.Background(), "acme", in)
	if err != nil {
		t.Fatalf("second call err: %v", err)
	}
	if !dedup2 {
		t.Fatalf("second call must be deduplicated")
	}
	if second.ID != first.ID {
		t.Fatalf("dedup must return the ORIGINAL record: got id %q want %q", second.ID, first.ID)
	}
	// REAL behavior: the store inserted exactly ONCE despite two calls.
	if us.recordCalls != 1 {
		t.Fatalf("RecordUsageTx inserted %d times, want 1 (idempotent)", us.recordCalls)
	}
}

func TestRecordUsage_RaceFallsBackToWinnersRecord(t *testing.T) {
	svc, us, rs, _ := newSvc(t)
	rs.planForTeam["acme"] = usdPlan("plan-1")
	// Pre-seed the "winner" record the concurrent writer committed.
	winner := domain.UsageRecord{
		ID: "winner", Team: "acme", IdempotencyKey: "race",
		MeterType: domain.MeterTypeInferenceRequest, Quantity: 1,
	}
	us.byKey[keyOf("acme", "race")] = winner
	us.dupOnInsert = true // our insert will lose the unique-constraint race

	rec, dedup, err := svc.RecordUsage(context.Background(), "acme", domain.RecordUsageInput{
		MeterType: domain.MeterTypeInferenceRequest, Quantity: 1, IdempotencyKey: "race",
	})
	if err != nil {
		t.Fatalf("race must resolve to the winner's record, got err %v", err)
	}
	if !dedup || rec.ID != "winner" {
		t.Fatalf("race fallback: dedup=%v id=%q, want dedup=true id=winner", dedup, rec.ID)
	}
}

func TestRecordUsage_EnqueuesUsageRecordedOutboxEvent(t *testing.T) {
	svc, us, rs, _ := newSvc(t)
	rs.planForTeam["acme"] = usdPlan("plan-1")
	rec, _, err := svc.RecordUsage(context.Background(), "acme", domain.RecordUsageInput{
		MeterType: domain.MeterTypeInferenceRequest, Quantity: 1, IdempotencyKey: "ev",
		SourceRequestID: "req-9",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// OUTBOX INVARIANT: exactly one UsageRecorded event accompanies the write,
	// and its payload carries the SAME server-computed facts as the record.
	if len(us.lastEvents) != 1 {
		t.Fatalf("outbox events = %d, want 1 (UsageRecorded)", len(us.lastEvents))
	}
	ev := us.lastEvents[0]
	if ev.EventType != domain.EventTypeUsageRecorded {
		t.Fatalf("event type = %q, want %q", ev.EventType, domain.EventTypeUsageRecorded)
	}
	if ev.AggregateID != rec.ID {
		t.Fatalf("event aggregate id = %q, want record id %q", ev.AggregateID, rec.ID)
	}
	payload, ok := ev.Payload.(domain.UsageRecordedPayload)
	if !ok {
		t.Fatalf("payload type = %T, want UsageRecordedPayload", ev.Payload)
	}
	if payload.Team != "acme" || payload.CostMicros != rec.Cost.AmountMicros || payload.CurrencyCode != "USD" {
		t.Fatalf("payload mismatch: %+v vs record cost %+v", payload, rec.Cost)
	}
	if payload.SourceRequestID != "req-9" {
		t.Fatalf("payload source_request_id = %q, want req-9", payload.SourceRequestID)
	}
	// The outbox id is the dedupe handle consumers use — must be non-empty.
	if ev.ID == "" {
		t.Fatalf("outbox event id must be set (it becomes the envelope id)")
	}
}

func TestRecordUsage_EnqueuesQuotaExceededWhenOverCap(t *testing.T) {
	svc, us, rs, _ := newSvc(t)
	rs.planForTeam["acme"] = usdPlan("plan-1") // token quota = 1000
	// Pre-seed 950 tokens already used this period.
	us.periodUsage[meterKey("acme", domain.MeterTypeInferenceTokens)] = 950

	// Record 100 more tokens → 1050 total, crosses the 1000 cap. (100 tokens, but
	// allowance already consumed by the prior 950 — service applies allowance on
	// the CUMULATIVE usage, so all 100 are billable here.)
	_, _, err := svc.RecordUsage(context.Background(), "acme", domain.RecordUsageInput{
		MeterType: domain.MeterTypeInferenceTokens, Quantity: 100, IdempotencyKey: "over",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Expect TWO outbox events: UsageRecorded + QuotaExceeded, both in the same tx.
	var sawUsage, sawQuota bool
	for _, ev := range us.lastEvents {
		switch p := ev.Payload.(type) {
		case domain.UsageRecordedPayload:
			sawUsage = true
		case domain.QuotaExceededPayload:
			sawQuota = true
			if p.QuotaLimit != 1000 || p.CurrentUsage < 1000 {
				t.Fatalf("quota payload = %+v, want limit 1000 and usage >= 1000", p)
			}
		}
	}
	if !sawUsage || !sawQuota {
		t.Fatalf("expected both UsageRecorded and QuotaExceeded events; got usage=%v quota=%v", sawUsage, sawQuota)
	}
}

func TestRecordUsage_NoQuotaEventWhenUnderCap(t *testing.T) {
	svc, us, rs, _ := newSvc(t)
	rs.planForTeam["acme"] = usdPlan("plan-1")
	_, _, err := svc.RecordUsage(context.Background(), "acme", domain.RecordUsageInput{
		MeterType: domain.MeterTypeInferenceTokens, Quantity: 200, IdempotencyKey: "under",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, ev := range us.lastEvents {
		if _, isQuota := ev.Payload.(domain.QuotaExceededPayload); isQuota {
			t.Fatalf("must NOT emit QuotaExceeded under cap")
		}
	}
}

func TestRecordUsage_ClampsFutureOccurredAt(t *testing.T) {
	svc, us, rs, _ := newSvc(t)
	rs.planForTeam["acme"] = usdPlan("plan-1")
	future := time.Date(2999, 1, 1, 0, 0, 0, 0, time.UTC)
	rec, _, err := svc.RecordUsage(context.Background(), "acme", domain.RecordUsageInput{
		MeterType: domain.MeterTypeInferenceRequest, Quantity: 1, IdempotencyKey: "future",
		OccurredAt: future,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// A client must not backdate INTO THE FUTURE — server clamps to now().
	now := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	if rec.OccurredAt.After(now) {
		t.Fatalf("occurred_at = %v not clamped to now %v", rec.OccurredAt, now)
	}
	_ = us
}

// ============================================================================
// RATE PLAN — validation + idempotency.
// ============================================================================

func TestCreateRatePlan_RejectsMixedCurrency(t *testing.T) {
	svc, _, _, _ := newSvc(t)
	_, _, err := svc.CreateRatePlan(context.Background(), domain.CreateRatePlanInput{
		Name: "bad",
		UnitPrices: map[domain.MeterType]domain.Money{
			domain.MeterTypeInferenceTokens:  {AmountMicros: 400, CurrencyCode: "USD"},
			domain.MeterTypeInferenceRequest: {AmountMicros: 1, CurrencyCode: "EUR"},
		},
		IdempotencyKey: "k",
	})
	if !errors.Is(err, domain.ErrCurrencyMismatch) && !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("want currency mismatch/validation, got %v", err)
	}
}

func TestCreateRatePlan_RejectsNegativePrice(t *testing.T) {
	svc, _, _, _ := newSvc(t)
	_, _, err := svc.CreateRatePlan(context.Background(), domain.CreateRatePlanInput{
		Name: "bad",
		UnitPrices: map[domain.MeterType]domain.Money{
			domain.MeterTypeInferenceTokens: {AmountMicros: -1, CurrencyCode: "USD"},
		},
		IdempotencyKey: "k",
	})
	if !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("want ErrValidation for negative price, got %v", err)
	}
}

func TestCreateRatePlan_AssignsServerFieldsAndIsIdempotent(t *testing.T) {
	svc, _, _, _ := newSvc(t)
	in := domain.CreateRatePlanInput{
		Name: "standard",
		UnitPrices: map[domain.MeterType]domain.Money{
			domain.MeterTypeInferenceTokens: {AmountMicros: 400, CurrencyCode: "USD"},
		},
		IdempotencyKey: "plan-key",
	}
	p1, dedup1, err := svc.CreateRatePlan(context.Background(), in)
	if err != nil || dedup1 {
		t.Fatalf("first create: err=%v dedup=%v", err, dedup1)
	}
	if p1.ID == "" || p1.CreatedAt.IsZero() {
		t.Fatalf("server must assign id (%q) and created_at (%v)", p1.ID, p1.CreatedAt)
	}
	p2, dedup2, err := svc.CreateRatePlan(context.Background(), in)
	if err != nil {
		t.Fatalf("second create err: %v", err)
	}
	if !dedup2 || p2.ID != p1.ID {
		t.Fatalf("idempotent create must return original: dedup=%v id=%q want %q", dedup2, p2.ID, p1.ID)
	}
}

// ============================================================================
// INVOICE AGGREGATION — the overflow-safe rollup + finalize + outbox event.
// ============================================================================

func TestGenerateInvoice_AggregatesAndFinalizes(t *testing.T) {
	svc, _, rs, is := newSvc(t)
	rs.planForTeam["acme"] = usdPlan("plan-1")
	// Two meters' rollups for the period.
	is.aggregate = []domain.MeterUsage{
		{MeterType: domain.MeterTypeInferenceTokens, TotalQuantity: 50,
			TotalCost: domain.Money{AmountMicros: 20_000, CurrencyCode: "USD"}},
		{MeterType: domain.MeterTypeInferenceRequest, TotalQuantity: 3,
			TotalCost: domain.Money{AmountMicros: 3_000, CurrencyCode: "USD"}},
	}
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	inv, err := svc.GenerateInvoice(context.Background(), "acme", start, end)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if inv.Status != domain.InvoiceStatusFinalized {
		t.Fatalf("status = %q, want FINALIZED", inv.Status)
	}
	// Total = 20_000 + 3_000 = 23_000 micros, rounded to cents (23_000 is already
	// 2.3 cents → rounds to 2 cents? No: 23_000 micros = 0.023 USD = 2.3 cents →
	// round half up to 2 cents = 20_000? Let's assert the rounded value the impl
	// produces and the relationship, not a guessed constant.)
	if len(inv.LineItems) != 2 {
		t.Fatalf("line items = %d, want 2", len(inv.LineItems))
	}
	// The (pre-round) sum is 23_000 micros; finalized total rounds to cents.
	wantRounded := domain.Money{AmountMicros: 23_000, CurrencyCode: "USD"}.RoundToCents()
	if inv.Total != wantRounded {
		t.Fatalf("total = %+v, want %+v (sum 23000 rounded to cents)", inv.Total, wantRounded)
	}
	if inv.FinalizedAt == nil {
		t.Fatalf("finalized invoice must have FinalizedAt set")
	}
	// OUTBOX: an InvoiceGenerated event must accompany the finalize, same tx.
	if is.savedEvent == nil || is.savedEvent.EventType != domain.EventTypeInvoiceGenerated {
		t.Fatalf("expected InvoiceGenerated outbox event, got %+v", is.savedEvent)
	}
	p, ok := is.savedEvent.Payload.(domain.InvoiceGeneratedPayload)
	if !ok || p.TotalMicros != inv.Total.AmountMicros || p.Team != "acme" {
		t.Fatalf("invoice event payload mismatch: %+v vs total %+v", is.savedEvent.Payload, inv.Total)
	}
}

func TestGenerateInvoice_NoUsageSkips(t *testing.T) {
	svc, _, rs, is := newSvc(t)
	rs.planForTeam["acme"] = usdPlan("plan-1")
	is.aggregate = nil // no usage
	_, err := svc.GenerateInvoice(context.Background(), "acme",
		time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC))
	if !errors.Is(err, domain.ErrNoUsage) {
		t.Fatalf("want ErrNoUsage for empty period, got %v", err)
	}
	if is.savedTx != nil {
		t.Fatalf("must not save a $0 invoice")
	}
}

func TestGenerateInvoice_RejectsInvertedPeriod(t *testing.T) {
	svc, _, rs, _ := newSvc(t)
	rs.planForTeam["acme"] = usdPlan("plan-1")
	end := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC) // start AFTER end
	_, err := svc.GenerateInvoice(context.Background(), "acme", start, end)
	if !errors.Is(err, domain.ErrInvalidPeriod) {
		t.Fatalf("want ErrInvalidPeriod, got %v", err)
	}
}

// ============================================================================
// QUOTA — server-computed standing.
// ============================================================================

func TestCheckQuota(t *testing.T) {
	svc, us, rs, _ := newSvc(t)
	rs.planForTeam["acme"] = usdPlan("plan-1") // token quota 1000
	us.periodUsage[meterKey("acme", domain.MeterTypeInferenceTokens)] = 750

	st, err := svc.CheckQuota(context.Background(), "acme", domain.MeterTypeInferenceTokens)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.QuotaLimit != 1000 || st.CurrentUsage != 750 || st.Remaining != 250 || st.Exceeded {
		t.Fatalf("quota status = %+v, want limit1000 usage750 remaining250 !exceeded", st)
	}
}

func TestCheckQuota_NoCapMeansNotExceeded(t *testing.T) {
	svc, us, rs, _ := newSvc(t)
	plan := usdPlan("plan-1")
	plan.QuotaLimits = nil // no caps
	rs.planForTeam["acme"] = plan
	us.periodUsage[meterKey("acme", domain.MeterTypeInferenceTokens)] = 1_000_000

	st, err := svc.CheckQuota(context.Background(), "acme", domain.MeterTypeInferenceTokens)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.Exceeded || st.QuotaLimit != 0 || st.Remaining != 0 {
		t.Fatalf("no-cap status = %+v, want !exceeded limit0 remaining0", st)
	}
}
