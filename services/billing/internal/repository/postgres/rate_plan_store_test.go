// rate_plan_store_test.go — integration tests for the RatePlanStore adapter against
// REAL Postgres. Verifies: JSONB price-map round-trip (the security-critical price
// source), idempotent create + the unique-violation→ErrRepoDuplicate race mapping,
// the not-found sentinel, and team→plan resolution.
package postgres

import (
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/abd-ulbasit/forgepoint/services/billing/internal/domain"
)

func TestRatePlanStore_CreateAndGet_RoundTrip(t *testing.T) {
	store, ctx := newTestStore(t)
	repo := store.RatePlans()

	plan := samplePlan()
	created, err := repo.CreatePlan(ctx, plan, "")
	if err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}

	got, err := repo.GetPlan(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetPlan: %v", err)
	}

	// The price maps are the WHOLE point of this store — assert they survive the
	// JSONB round trip EXACTLY (a mispriced read is a security/revenue bug).
	if !reflect.DeepEqual(got.UnitPrices, plan.UnitPrices) {
		t.Errorf("UnitPrices round-trip mismatch:\n got  %#v\n want %#v", got.UnitPrices, plan.UnitPrices)
	}
	if !reflect.DeepEqual(got.IncludedQuantities, plan.IncludedQuantities) {
		t.Errorf("IncludedQuantities mismatch:\n got  %#v\n want %#v", got.IncludedQuantities, plan.IncludedQuantities)
	}
	if !reflect.DeepEqual(got.QuotaLimits, plan.QuotaLimits) {
		t.Errorf("QuotaLimits mismatch:\n got  %#v\n want %#v", got.QuotaLimits, plan.QuotaLimits)
	}
	if got.Name != plan.Name {
		t.Errorf("Name = %q, want %q", got.Name, plan.Name)
	}
	if !got.CreatedAt.Equal(plan.CreatedAt) {
		t.Errorf("CreatedAt = %v, want %v", got.CreatedAt, plan.CreatedAt)
	}
}

func TestRatePlanStore_GetPlan_NotFound(t *testing.T) {
	store, ctx := newTestStore(t)

	_, err := store.RatePlans().GetPlan(ctx, newID())
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("GetPlan(missing) error = %v, want ErrRepoNotFound", err)
	}
}

func TestRatePlanStore_Idempotency_SecondCreateIsDuplicate(t *testing.T) {
	store, ctx := newTestStore(t)
	repo := store.RatePlans()

	key := "idem-" + newID()
	first, err := repo.CreatePlan(ctx, samplePlan(), key)
	if err != nil {
		t.Fatalf("first CreatePlan: %v", err)
	}

	// A second create under the SAME key must be rejected as a duplicate (the
	// unique index on rate_plan_idempotency fired) — the service then re-reads the
	// winner via FindPlanByIdempotencyKey.
	_, err = repo.CreatePlan(ctx, samplePlan(), key)
	if !errors.Is(err, domain.ErrRepoDuplicate) {
		t.Fatalf("second CreatePlan error = %v, want ErrRepoDuplicate", err)
	}

	found, err := repo.FindPlanByIdempotencyKey(ctx, key)
	if err != nil {
		t.Fatalf("FindPlanByIdempotencyKey: %v", err)
	}
	if found.ID != first.ID {
		t.Errorf("FindPlanByIdempotencyKey returned %s, want the original %s", found.ID, first.ID)
	}
}

func TestRatePlanStore_FindByIdempotencyKey_NotFound(t *testing.T) {
	store, ctx := newTestStore(t)

	_, err := store.RatePlans().FindPlanByIdempotencyKey(ctx, "never-used")
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("FindPlanByIdempotencyKey(missing) = %v, want ErrRepoNotFound", err)
	}
}

// TestRatePlanStore_ConcurrentIdempotentCreate proves the DB is the single race
// arbiter: many goroutines racing CreatePlan with the SAME key yield EXACTLY ONE
// stored plan; every loser gets ErrRepoDuplicate and the winner is unique.
func TestRatePlanStore_ConcurrentIdempotentCreate(t *testing.T) {
	store, ctx := newTestStore(t)
	repo := store.RatePlans()

	const goroutines = 8
	key := "race-" + newID()

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		successes int
		dupes     int
		winnerID  string
	)
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			plan, err := repo.CreatePlan(ctx, samplePlan(), key)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				successes++
				winnerID = plan.ID
			case errors.Is(err, domain.ErrRepoDuplicate):
				dupes++
			default:
				t.Errorf("unexpected CreatePlan error: %v", err)
			}
		}()
	}
	wg.Wait()

	if successes != 1 {
		t.Fatalf("successes = %d, want exactly 1 (the DB must arbitrate the race)", successes)
	}
	if dupes != goroutines-1 {
		t.Fatalf("duplicates = %d, want %d", dupes, goroutines-1)
	}

	// Ground truth: exactly ONE plan rows under this key, and it is the winner.
	found, err := repo.FindPlanByIdempotencyKey(ctx, key)
	if err != nil {
		t.Fatalf("FindPlanByIdempotencyKey: %v", err)
	}
	if found.ID != winnerID {
		t.Errorf("stored plan %s != reported winner %s", found.ID, winnerID)
	}
}

func TestRatePlanStore_ResolvePlanForTeam(t *testing.T) {
	store, ctx := newTestStore(t)
	repo := store.RatePlans()
	team := "team-" + newID()

	// No assignment yet → not found (the service maps this to ErrRatePlanNotFound).
	if _, err := repo.ResolvePlanForTeam(ctx, team); !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("ResolvePlanForTeam(unassigned) = %v, want ErrRepoNotFound", err)
	}

	plan := mustCreatePlanAndAssign(t, ctx, store, team, samplePlan())

	got, err := repo.ResolvePlanForTeam(ctx, team)
	if err != nil {
		t.Fatalf("ResolvePlanForTeam: %v", err)
	}
	if got.ID != plan.ID {
		t.Errorf("resolved plan %s, want %s", got.ID, plan.ID)
	}

	// Reassign to a second plan — UPSERT must switch the binding (not duplicate it).
	plan2, err := repo.CreatePlan(ctx, samplePlan(), "")
	if err != nil {
		t.Fatalf("CreatePlan plan2: %v", err)
	}
	if err := repo.AssignPlanToTeam(ctx, team, plan2.ID); err != nil {
		t.Fatalf("reassign: %v", err)
	}
	got, err = repo.ResolvePlanForTeam(ctx, team)
	if err != nil {
		t.Fatalf("ResolvePlanForTeam after reassign: %v", err)
	}
	if got.ID != plan2.ID {
		t.Errorf("after reassign resolved %s, want %s", got.ID, plan2.ID)
	}
}
