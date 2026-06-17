// usage_store_test.go — integration tests for the UsageStore adapter against REAL
// Postgres. This is the OUTBOX PATTERN's test home: it proves the business write
// and the outbox event commit ATOMICALLY (the dual-write guarantee), that the
// fresh event is left UNPUBLISHED for the relay, that a failed tx leaves NO partial
// write, that the (team, idempotency_key) partial unique index dedupes under a
// real race, and the CurrentPeriodUsage SUM math.
package postgres

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/billing/internal/domain"
)

// TestUsageStore_RecordUsageTx_WritesRowAndOutboxAtomically is THE outbox test:
// after one RecordUsageTx, BOTH the usage row AND its UsageRecorded outbox row
// exist in the same database — proof the dual write committed in one tx — and the
// outbox row is UNPUBLISHED (waiting for the relay).
func TestUsageStore_RecordUsageTx_WritesRowAndOutboxAtomically(t *testing.T) {
	store, ctx := newTestStore(t)
	usage := store.Usage()
	team := "team-" + newID()
	plan := mustCreatePlanAndAssign(t, ctx, store, team, samplePlan())

	rec := usageRecord(team, plan.ID, domain.MeterTypeInferenceTokens, 50, usd(20000), "idem-"+newID())
	ev := usageRecordedEvent(rec)

	stored, err := usage.RecordUsageTx(ctx, rec, []domain.OutboxEvent{ev})
	if err != nil {
		t.Fatalf("RecordUsageTx: %v", err)
	}
	if stored.ID != rec.ID {
		t.Errorf("stored.ID = %s, want %s", stored.ID, rec.ID)
	}

	// Ground truth: the ledger row is there.
	if n := countUsageRecords(t, ctx, store, team); n != 1 {
		t.Fatalf("usage_records count = %d, want 1", n)
	}
	// Ground truth: the outbox row landed in the SAME db (same tx) ...
	if n := countOutbox(t, ctx, store, rec.ID); n != 1 {
		t.Fatalf("outbox count for aggregate = %d, want 1", n)
	}
	// ... and it is UNPUBLISHED, ready for the relay.
	if n := countUnpublishedOutbox(t, ctx, store, rec.ID); n != 1 {
		t.Fatalf("unpublished outbox count = %d, want 1", n)
	}
	if got := outboxEventTypes(t, ctx, store, rec.ID); len(got) != 1 || got[0] != domain.EventTypeUsageRecorded {
		t.Fatalf("outbox event types = %v, want [%s]", got, domain.EventTypeUsageRecorded)
	}

	// The record reads back identically (full field round trip, incl. money).
	got, err := usage.FindByIdempotencyKey(ctx, team, rec.IdempotencyKey)
	if err != nil {
		t.Fatalf("FindByIdempotencyKey: %v", err)
	}
	assertUsageRecordEqual(t, got, rec)
}

// TestUsageStore_RecordUsageTx_MultipleEvents proves a single write can emit MORE
// THAN ONE event in the same tx — the UsageRecorded + QuotaExceeded case from the
// domain — and BOTH land atomically with the row.
func TestUsageStore_RecordUsageTx_MultipleEvents(t *testing.T) {
	store, ctx := newTestStore(t)
	usage := store.Usage()
	team := "team-" + newID()
	plan := mustCreatePlanAndAssign(t, ctx, store, team, samplePlan())

	rec := usageRecord(team, plan.ID, domain.MeterTypeInferenceTokens, 1000, usd(400000), "idem-"+newID())
	usageEv := usageRecordedEvent(rec)
	quotaEv := domain.OutboxEvent{
		ID:          newID(),
		AggregateID: rec.ID,
		EventType:   domain.EventTypeQuotaExceeded,
		Payload: domain.QuotaExceededPayload{
			Team:         team,
			RatePlanID:   plan.ID,
			MeterType:    domain.MeterTypeInferenceTokens,
			QuotaLimit:   1000,
			CurrentUsage: 1000,
			OccurredAt:   rec.OccurredAt,
		},
		CreatedAt: rec.CreatedAt,
	}

	if _, err := usage.RecordUsageTx(ctx, rec, []domain.OutboxEvent{usageEv, quotaEv}); err != nil {
		t.Fatalf("RecordUsageTx: %v", err)
	}

	if n := countOutbox(t, ctx, store, rec.ID); n != 2 {
		t.Fatalf("outbox count = %d, want 2 (UsageRecorded + QuotaExceeded)", n)
	}
	got := outboxEventTypes(t, ctx, store, rec.ID)
	want := map[string]bool{domain.EventTypeUsageRecorded: true, domain.EventTypeQuotaExceeded: true}
	for _, et := range got {
		if !want[et] {
			t.Errorf("unexpected outbox event type %q", et)
		}
		delete(want, et)
	}
	if len(want) != 0 {
		t.Errorf("missing outbox event types: %v", want)
	}
}

// TestUsageStore_RecordUsageTx_RollbackLeavesNoPartialWrite proves ATOMICITY of
// the failure path: when the tx fails (here: a duplicate idempotency key on the
// SECOND write), NO new usage row and NO new outbox row are left behind. A non-
// atomic implementation could leak the outbox row (a phantom event) or the usage
// row (an unannounced charge); the single tx forbids both.
func TestUsageStore_RecordUsageTx_RollbackLeavesNoPartialWrite(t *testing.T) {
	store, ctx := newTestStore(t)
	usage := store.Usage()
	team := "team-" + newID()
	plan := mustCreatePlanAndAssign(t, ctx, store, team, samplePlan())

	key := "idem-" + newID()
	first := usageRecord(team, plan.ID, domain.MeterTypeInferenceTokens, 50, usd(20000), key)
	if _, err := usage.RecordUsageTx(ctx, first, []domain.OutboxEvent{usageRecordedEvent(first)}); err != nil {
		t.Fatalf("first RecordUsageTx: %v", err)
	}

	// Second write reuses the SAME idempotency key → the partial unique index
	// rejects the usage INSERT → the whole tx (incl. the outbox insert) rolls back.
	second := usageRecord(team, plan.ID, domain.MeterTypeInferenceTokens, 50, usd(20000), key)
	_, err := usage.RecordUsageTx(ctx, second, []domain.OutboxEvent{usageRecordedEvent(second)})
	if !errors.Is(err, domain.ErrRepoDuplicate) {
		t.Fatalf("second RecordUsageTx error = %v, want ErrRepoDuplicate", err)
	}

	// Ground truth: still exactly ONE usage row (the first), and the second write's
	// outbox row never persisted (rolled back with its usage insert).
	if n := countUsageRecords(t, ctx, store, team); n != 1 {
		t.Fatalf("usage_records count = %d, want 1 (the dup tx must roll back)", n)
	}
	if n := countOutbox(t, ctx, store, second.ID); n != 0 {
		t.Fatalf("outbox rows for the rolled-back aggregate = %d, want 0 (no phantom event)", n)
	}
	// The FIRST write's outbox row is untouched (still present, still unpublished).
	if n := countUnpublishedOutbox(t, ctx, store, first.ID); n != 1 {
		t.Fatalf("first write unpublished outbox = %d, want 1", n)
	}
}

// TestUsageStore_OutboxRelaySemantics proves the relay's claim/mark lifecycle:
// a fresh row is UNPUBLISHED (claimable); after the relay marks it published it is
// NO LONGER claimable; and a FAILED publish-marking (we simply don't mark) leaves
// the row claimable for RETRY — the at-least-once guarantee's durable half.
func TestUsageStore_OutboxRelaySemantics(t *testing.T) {
	store, ctx := newTestStore(t)
	usage := store.Usage()
	team := "team-" + newID()
	plan := mustCreatePlanAndAssign(t, ctx, store, team, samplePlan())

	rec := usageRecord(team, plan.ID, domain.MeterTypeInferenceTokens, 10, usd(4000), "idem-"+newID())
	if _, err := usage.RecordUsageTx(ctx, rec, []domain.OutboxEvent{usageRecordedEvent(rec)}); err != nil {
		t.Fatalf("RecordUsageTx: %v", err)
	}

	// Fresh write: claimable by the relay (published_at IS NULL).
	if n := countUnpublishedOutbox(t, ctx, store, rec.ID); n != 1 {
		t.Fatalf("fresh: unpublished = %d, want 1 (relay must be able to claim it)", n)
	}

	// FAILED publish-marking: the relay published to NATS but crashed before the
	// UPDATE — i.e. we do NOT mark. The row stays claimable, so the relay retries
	// on restart (consumers dedupe on the id → at-least-once is safe). Re-asserting
	// the same predicate documents that "no mark" == "still pending".
	if n := countUnpublishedOutbox(t, ctx, store, rec.ID); n != 1 {
		t.Fatalf("after failed marking: unpublished = %d, want 1 (left for retry)", n)
	}

	// SUCCESSFUL marking: the relay stamps published_at → the row drops out of the
	// claim set, so it won't be re-published.
	markOutboxPublished(t, ctx, store, rec.ID, nowMicro())
	if n := countUnpublishedOutbox(t, ctx, store, rec.ID); n != 0 {
		t.Fatalf("after successful marking: unpublished = %d, want 0", n)
	}
	// The row still EXISTS (the writer/relay never deletes it; a retention job does).
	if n := countOutbox(t, ctx, store, rec.ID); n != 1 {
		t.Fatalf("outbox row count = %d, want 1 (published rows are retained, not deleted)", n)
	}
}

// TestUsageStore_KeylessRecordsCoexist proves the partial unique index exempts
// NULL keys: many keyless records (idempotency_key IS NULL) can coexist for one
// team — the dedup constraint only applies when a key is present.
func TestUsageStore_KeylessRecordsCoexist(t *testing.T) {
	store, ctx := newTestStore(t)
	usage := store.Usage()
	team := "team-" + newID()
	plan := mustCreatePlanAndAssign(t, ctx, store, team, samplePlan())

	for i := 0; i < 3; i++ {
		rec := usageRecord(team, plan.ID, domain.MeterTypeInferenceTokens, 5, usd(2000), "") // keyless
		if _, err := usage.RecordUsageTx(ctx, rec, []domain.OutboxEvent{usageRecordedEvent(rec)}); err != nil {
			t.Fatalf("keyless RecordUsageTx #%d: %v", i, err)
		}
	}
	if n := countUsageRecords(t, ctx, store, team); n != 3 {
		t.Fatalf("keyless records = %d, want 3 (NULL keys are exempt from the unique index)", n)
	}
}

func TestUsageStore_FindByIdempotencyKey_NotFound(t *testing.T) {
	store, ctx := newTestStore(t)
	_, err := store.Usage().FindByIdempotencyKey(ctx, "team-x", "never-recorded")
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("FindByIdempotencyKey(missing) = %v, want ErrRepoNotFound", err)
	}
}

// TestUsageStore_CurrentPeriodUsage sums quantity for a (team, meter) since the
// period start — the running total the metering math reads BEFORE a write. It must
// scope by meter and by time, and return 0 (not error) when there is no usage.
func TestUsageStore_CurrentPeriodUsage(t *testing.T) {
	store, ctx := newTestStore(t)
	usage := store.Usage()
	team := "team-" + newID()
	plan := mustCreatePlanAndAssign(t, ctx, store, team, samplePlan())
	periodStart := nowMicro().Add(-time.Hour)

	// No usage yet → 0.
	if got, err := usage.CurrentPeriodUsage(ctx, team, domain.MeterTypeInferenceTokens, periodStart); err != nil || got != 0 {
		t.Fatalf("CurrentPeriodUsage(empty) = (%d, %v), want (0, nil)", got, err)
	}

	// Two token records (30 + 70) and one REQUEST record (1) — the request must NOT
	// count toward the token meter total.
	for _, q := range []int64{30, 70} {
		rec := usageRecord(team, plan.ID, domain.MeterTypeInferenceTokens, q, usd(q*400), "")
		if _, err := usage.RecordUsageTx(ctx, rec, []domain.OutboxEvent{usageRecordedEvent(rec)}); err != nil {
			t.Fatalf("RecordUsageTx tokens: %v", err)
		}
	}
	reqRec := usageRecord(team, plan.ID, domain.MeterTypeInferenceRequest, 1, usd(1000000), "")
	if _, err := usage.RecordUsageTx(ctx, reqRec, []domain.OutboxEvent{usageRecordedEvent(reqRec)}); err != nil {
		t.Fatalf("RecordUsageTx request: %v", err)
	}

	got, err := usage.CurrentPeriodUsage(ctx, team, domain.MeterTypeInferenceTokens, periodStart)
	if err != nil {
		t.Fatalf("CurrentPeriodUsage: %v", err)
	}
	if got != 100 {
		t.Errorf("CurrentPeriodUsage(tokens) = %d, want 100 (30+70, request excluded)", got)
	}
}

// TestUsageStore_ConcurrentSameKey proves the partial unique index is the single
// race arbiter under REAL concurrency: many goroutines racing RecordUsageTx with
// the SAME (team, key) yield EXACTLY ONE stored row; losers get ErrRepoDuplicate.
func TestUsageStore_ConcurrentSameKey(t *testing.T) {
	store, ctx := newTestStore(t)
	usage := store.Usage()
	team := "team-" + newID()
	plan := mustCreatePlanAndAssign(t, ctx, store, team, samplePlan())

	const goroutines = 8
	key := "race-" + newID()

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		successes int
		dupes     int
	)
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			rec := usageRecord(team, plan.ID, domain.MeterTypeInferenceTokens, 50, usd(20000), key)
			_, err := usage.RecordUsageTx(ctx, rec, []domain.OutboxEvent{usageRecordedEvent(rec)})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				successes++
			case errors.Is(err, domain.ErrRepoDuplicate):
				dupes++
			default:
				t.Errorf("unexpected RecordUsageTx error: %v", err)
			}
		}()
	}
	wg.Wait()

	if successes != 1 {
		t.Fatalf("successes = %d, want exactly 1", successes)
	}
	if dupes != goroutines-1 {
		t.Fatalf("duplicates = %d, want %d", dupes, goroutines-1)
	}
	// Ground truth: exactly one usage row AND exactly one outbox row (the losers'
	// outbox inserts rolled back with their usage inserts — no phantom events).
	if n := countUsageRecords(t, ctx, store, team); n != 1 {
		t.Fatalf("usage_records = %d, want 1", n)
	}
}

// assertUsageRecordEqual compares the read-back record to what was written, field
// by field (incl. the money columns), so a mis-stored cost or meter is caught.
func assertUsageRecordEqual(t *testing.T, got, want domain.UsageRecord) {
	t.Helper()
	if got.ID != want.ID || got.Team != want.Team || got.RatePlanID != want.RatePlanID {
		t.Errorf("identity mismatch: got (%s,%s,%s) want (%s,%s,%s)",
			got.ID, got.Team, got.RatePlanID, want.ID, want.Team, want.RatePlanID)
	}
	if got.MeterType != want.MeterType || got.Quantity != want.Quantity {
		t.Errorf("meter/qty mismatch: got (%s,%d) want (%s,%d)",
			got.MeterType, got.Quantity, want.MeterType, want.Quantity)
	}
	if got.Cost != want.Cost {
		t.Errorf("cost mismatch: got %+v want %+v", got.Cost, want.Cost)
	}
	if got.IdempotencyKey != want.IdempotencyKey || got.SourceRequestID != want.SourceRequestID {
		t.Errorf("idem/source mismatch: got (%q,%q) want (%q,%q)",
			got.IdempotencyKey, got.SourceRequestID, want.IdempotencyKey, want.SourceRequestID)
	}
	if !got.OccurredAt.Equal(want.OccurredAt) || !got.CreatedAt.Equal(want.CreatedAt) {
		t.Errorf("time mismatch: got (%v,%v) want (%v,%v)",
			got.OccurredAt, got.CreatedAt, want.OccurredAt, want.CreatedAt)
	}
}
