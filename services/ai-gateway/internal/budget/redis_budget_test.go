// redis_budget_test.go — UNIT tests for the budget store's PURE branches.
//
// The Redis-backed Lua path needs a real Redis (integration territory — excluded
// here, the thinkpad is RAM-tight). These tests cover the logic that runs WITHOUT
// Redis: the UNLIMITED (budget=0) short-circuits and the refill-rate derivation.
// They prove the unlimited contract (never block, report budget 0) and that the
// constructor's window math is correct, with no external dependency.
package budget

import (
	"context"
	"testing"
)

func TestUnlimitedBudgetNeverBlocks(t *testing.T) {
	t.Parallel()
	// Budget 0 = unlimited: Check/Deduct/Usage all short-circuit BEFORE touching Redis
	// (rdb is nil here, which would panic if any path dialed it — proving they don't).
	b := NewRedisBudget(nil, Config{Budget: 0})

	allowed, remaining, err := b.Check(context.Background(), "team-a")
	if err != nil || !allowed {
		t.Fatalf("unlimited Check = (%v, %d, %v), want (true, _, nil)", allowed, remaining, err)
	}

	rem, err := b.Deduct(context.Background(), "team-a", 12345)
	if err != nil || rem != 0 {
		t.Fatalf("unlimited Deduct = (%d, %v), want (0, nil)", rem, err)
	}

	consumed, budget, err := b.Usage(context.Background(), "team-a")
	if err != nil || consumed != 0 || budget != 0 {
		t.Fatalf("unlimited Usage = (%d, %d, %v), want (0, 0, nil)", consumed, budget, err)
	}
}

func TestRefillRateDerivedFromWindow(t *testing.T) {
	t.Parallel()
	// budget 3600 over a 3600s window → exactly 1 token/sec refill.
	b := NewRedisBudget(nil, Config{Budget: 3600, WindowSeconds: 3600})
	if b.ratePerSec != 1.0 {
		t.Fatalf("ratePerSec = %v, want 1.0", b.ratePerSec)
	}
	// A zero/negative window falls back to the 1h default.
	b2 := NewRedisBudget(nil, Config{Budget: 7200, WindowSeconds: 0})
	if b2.ratePerSec != 2.0 { // 7200 / 3600 default window = 2 tokens/sec.
		t.Fatalf("default-window ratePerSec = %v, want 2.0", b2.ratePerSec)
	}
}

func TestDeductZeroTokensIsNoop(t *testing.T) {
	t.Parallel()
	// Deducting 0 tokens on a LIMITED budget must not touch Redis (rdb nil → no panic).
	b := NewRedisBudget(nil, Config{Budget: 1000, WindowSeconds: 3600})
	rem, err := b.Deduct(context.Background(), "team-a", 0)
	if err != nil || rem != 0 {
		t.Fatalf("Deduct(0) = (%d, %v), want (0, nil)", rem, err)
	}
}
