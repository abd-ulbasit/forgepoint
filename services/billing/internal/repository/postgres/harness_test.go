// harness_test.go — shared integration-test scaffolding for the Postgres adapter.
//
// Every test here runs against a REAL Postgres (postgres:17) started by
// pkg/testutil.StartPostgres on the remote Docker engine — never a mock. Mocking a
// database hides the very behavior we must verify: the partial unique index, the
// transactional-outbox atomicity, JSONB round-trips of money/price-maps, GROUP BY
// aggregation, and keyset pagination under real ordering. testutil.SkipIfNoDocker
// (called inside StartPostgres) skips cleanly when no engine is reachable so the
// unit suite still runs locally.
//
// Each test gets its OWN container (no shared state, no cross-test contamination)
// and a freshly-migrated, production-shaped schema (migrations applied via the
// embedded .up.sql — see migrate_test.go).
package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/billing/internal/domain"
)

// newTestStore starts a real Postgres container, applies the production
// migrations, and returns a Store wired to the resulting pool. The pool is closed
// automatically via t.Cleanup.
func newTestStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	ctx := context.Background()

	dsn := testutil.StartPostgres(t) // SkipIfNoDocker is called inside

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	t.Cleanup(pool.Close)

	applyMigrations(t, ctx, pool)

	return NewWithPool(pool), ctx
}

// newID returns a fresh UUIDv4 string (the same shape the production UUIDProvider
// emits). Tests use it for ids they don't otherwise assert on.
func newID() string { return uuid.NewString() }

// nowMicro returns the current UTC time truncated to microseconds — the precision
// Postgres TIMESTAMPTZ stores. Truncating fixtures here means a read-back equals
// what was written, so exact-equality assertions on time fields hold (Go's
// time.Time has nanoseconds; an un-truncated value would differ after round trip).
func nowMicro() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

// usd builds a USD Money in micro-units. Terse helper so price/cost fixtures read
// as intent ("400 micros = $0.0004 per token").
func usd(micros int64) domain.Money {
	return domain.Money{AmountMicros: micros, CurrencyCode: "USD"}
}

// ----------------------------------------------------------------------------
// Domain builders — terse, valid fixtures so each test states only what matters.
// ----------------------------------------------------------------------------

// samplePlan builds a minimal-but-valid RatePlan: prices tokens and requests in
// USD, a 100-token free allowance, and a 1000-token quota. Enough structure to
// exercise all three JSONB maps round-tripping.
func samplePlan() domain.RatePlan {
	return domain.RatePlan{
		ID:   newID(),
		Name: "standard-" + uuid.NewString()[:8],
		UnitPrices: map[domain.MeterType]domain.Money{
			domain.MeterTypeInferenceTokens:  usd(400),     // $0.0004 / token
			domain.MeterTypeInferenceRequest: usd(1000000), // $1.00 / request
		},
		IncludedQuantities: map[domain.MeterType]int64{
			domain.MeterTypeInferenceTokens: 100,
		},
		QuotaLimits: map[domain.MeterType]int64{
			domain.MeterTypeInferenceTokens: 1000,
		},
		CreatedAt: nowMicro(),
	}
}

// usageRecord builds a valid UsageRecord for a team against a plan. idempotencyKey
// may be "" for a keyless record. occurredAt defaults to now if zero.
func usageRecord(team, planID string, meter domain.MeterType, qty int64, cost domain.Money, idemKey string) domain.UsageRecord {
	now := nowMicro()
	return domain.UsageRecord{
		ID:              newID(),
		Team:            team,
		RatePlanID:      planID,
		MeterType:       meter,
		Quantity:        qty,
		Cost:            cost,
		ModelID:         "model-x",
		ModelVersion:    "3",
		SourceRequestID: "req-" + uuid.NewString()[:8],
		IdempotencyKey:  idemKey,
		OccurredAt:      now,
		CreatedAt:       now,
	}
}

// usageRecordedEvent builds the UsageRecorded outbox event the domain emits
// alongside a usage record (see billing_service_impl.go). Mirrors the real payload
// so the stored JSONB matches production.
func usageRecordedEvent(record domain.UsageRecord) domain.OutboxEvent {
	now := nowMicro()
	return domain.OutboxEvent{
		ID:          newID(),
		AggregateID: record.ID,
		EventType:   domain.EventTypeUsageRecorded,
		Payload: domain.UsageRecordedPayload{
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
		},
		CreatedAt: now,
	}
}

// mustCreatePlanAndAssign persists a plan and binds it to team — the common setup
// for any usage test that needs a resolvable plan. Returns the stored plan.
func mustCreatePlanAndAssign(t *testing.T, ctx context.Context, store *Store, team string, plan domain.RatePlan) domain.RatePlan {
	t.Helper()
	stored, err := store.RatePlans().CreatePlan(ctx, plan, "")
	if err != nil {
		t.Fatalf("create plan: %v", err)
	}
	if err := store.RatePlans().AssignPlanToTeam(ctx, team, stored.ID); err != nil {
		t.Fatalf("assign plan: %v", err)
	}
	return stored
}

// ptr returns a pointer to v — for the nullable *time.Time invoice fields.
func ptr[T any](v T) *T { return &v }
