// breaker_store_test.go — integration tests for the cross-replica BreakerStore
// against a REAL Redis. Verifies the shared failure counter trips to OPEN at the
// threshold, HALF_OPEN→failure re-opens, success resets to CLOSED, an absent
// backend reads as the healthy CLOSED default, per-backend isolation (v1 vs v2),
// the atomic increment under concurrency, and TTL eviction. Run with -race.
package redisrepo

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/inference-gateway/internal/domain"
)

func TestBreakerStore_TripsToOpenAtThreshold(t *testing.T) {
	rdb := newRedis(t)
	ctx := context.Background()
	store := NewBreakerStore(rdb, time.Minute)
	now := time.Now()

	const threshold = 3
	// First two failures stay CLOSED (below threshold).
	for i := 0; i < threshold-1; i++ {
		st, err := store.RecordFailure(ctx, "iris", "v2", threshold, now)
		if err != nil {
			t.Fatalf("RecordFailure #%d: %v", i, err)
		}
		if st != domain.CircuitClosed {
			t.Fatalf("after %d failures state=%v, want CLOSED", i+1, st)
		}
	}
	// The threshold-th consecutive failure trips OPEN.
	st, err := store.RecordFailure(ctx, "iris", "v2", threshold, now)
	if err != nil {
		t.Fatalf("tripping RecordFailure: %v", err)
	}
	if st != domain.CircuitOpen {
		t.Fatalf("at threshold state=%v, want OPEN", st)
	}

	// Get reflects the shared OPEN state + the count + the opened timestamp.
	got, err := store.Get(ctx, "iris", "v2")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != domain.CircuitOpen {
		t.Fatalf("Get state=%v, want OPEN", got.State)
	}
	if got.ConsecutiveFailures != threshold {
		t.Fatalf("Get fails=%d, want %d", got.ConsecutiveFailures, threshold)
	}
	if got.OpenedAt.IsZero() {
		t.Fatal("Get OpenedAt is zero, want the trip timestamp")
	}
}

// TestBreakerStore_SuccessResetsToClosed verifies a success clears the shared
// failure count and returns the backend to CLOSED.
func TestBreakerStore_SuccessResetsToClosed(t *testing.T) {
	rdb := newRedis(t)
	ctx := context.Background()
	store := NewBreakerStore(rdb, time.Minute)
	now := time.Now()

	const threshold = 5
	for i := 0; i < 3; i++ { // rack up some (but not threshold) failures
		if _, err := store.RecordFailure(ctx, "fraud", "v1", threshold, now); err != nil {
			t.Fatalf("RecordFailure: %v", err)
		}
	}
	if err := store.RecordSuccess(ctx, "fraud", "v1"); err != nil {
		t.Fatalf("RecordSuccess: %v", err)
	}
	got, err := store.Get(ctx, "fraud", "v1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != domain.CircuitClosed || got.ConsecutiveFailures != 0 {
		t.Fatalf("after success state=%v fails=%d, want CLOSED/0", got.State, got.ConsecutiveFailures)
	}
}

// TestBreakerStore_AbsentBackendIsClosed confirms a never-seen backend reads as the
// healthy CLOSED zero state (no error on absence) — the safe default.
func TestBreakerStore_AbsentBackendIsClosed(t *testing.T) {
	rdb := newRedis(t)
	ctx := context.Background()
	store := NewBreakerStore(rdb, time.Minute)

	got, err := store.Get(ctx, "unseen", "v9")
	if err != nil {
		t.Fatalf("Get(absent): %v", err)
	}
	if got.State != domain.CircuitClosed || got.ConsecutiveFailures != 0 || !got.OpenedAt.IsZero() {
		t.Fatalf("absent backend = %+v, want CLOSED/0/zero-time", got)
	}
}

// TestBreakerStore_BackendsIsolated proves breakers are PER (model,version): v2
// tripping OPEN must not affect v1 (the canary-isolation property).
func TestBreakerStore_BackendsIsolated(t *testing.T) {
	rdb := newRedis(t)
	ctx := context.Background()
	store := NewBreakerStore(rdb, time.Minute)
	now := time.Now()

	const threshold = 2
	// Trip v2 OPEN.
	for i := 0; i < threshold; i++ {
		if _, err := store.RecordFailure(ctx, "churn", "v2", threshold, now); err != nil {
			t.Fatalf("RecordFailure v2: %v", err)
		}
	}
	v2, _ := store.Get(ctx, "churn", "v2")
	if v2.State != domain.CircuitOpen {
		t.Fatalf("v2 state=%v, want OPEN", v2.State)
	}
	// v1 untouched → CLOSED.
	v1, err := store.Get(ctx, "churn", "v1")
	if err != nil {
		t.Fatalf("Get v1: %v", err)
	}
	if v1.State != domain.CircuitClosed {
		t.Fatalf("v1 state=%v, want CLOSED (isolation broken)", v1.State)
	}
}

// TestBreakerStore_HalfOpenFailureReopens verifies that a failure while the shared
// state is HALF_OPEN re-opens immediately (pessimistic: a flapping backend stays
// out). We force HALF_OPEN directly via the hash then record a failure.
func TestBreakerStore_HalfOpenFailureReopens(t *testing.T) {
	rdb := newRedis(t)
	ctx := context.Background()
	store := NewBreakerStore(rdb, time.Minute)
	now := time.Now()

	key := store.key("flappy", "v1")
	// Seed shared state as HALF_OPEN (state=2) — the probing phase.
	if err := rdb.HSet(ctx, key, "state", int(domain.CircuitHalfOpen), "fails", 0).Err(); err != nil {
		t.Fatalf("seed HALF_OPEN: %v", err)
	}
	st, err := store.RecordFailure(ctx, "flappy", "v1", 5, now)
	if err != nil {
		t.Fatalf("RecordFailure in HALF_OPEN: %v", err)
	}
	if st != domain.CircuitOpen {
		t.Fatalf("HALF_OPEN + failure → state=%v, want OPEN", st)
	}
}

// TestBreakerStore_ConcurrentFailuresAtomic is the atomicity test for the shared
// counter: many goroutines (simulating replicas) recording failures concurrently
// must increment exactly once each — never lose an increment to a read-modify-write
// race — so the trip happens at the true global count. With threshold above the
// goroutine count we never trip, and the final count must equal the number of calls.
func TestBreakerStore_ConcurrentFailuresAtomic(t *testing.T) {
	rdb := newRedis(t)
	ctx := context.Background()
	store := NewBreakerStore(rdb, time.Minute)
	now := time.Now()

	const calls = 100
	const threshold = 1000 // above calls so the breaker never trips during the race
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := store.RecordFailure(ctx, "race", "v1", threshold, now); err != nil {
				t.Errorf("RecordFailure: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	got, err := store.Get(ctx, "race", "v1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ConsecutiveFailures != calls {
		t.Fatalf("after %d concurrent failures count=%d, want %d (no lost increments)", calls, got.ConsecutiveFailures, calls)
	}
	if got.State != domain.CircuitClosed {
		t.Fatalf("state=%v, want CLOSED (threshold not reached)", got.State)
	}
}

// TestBreakerStore_SetsTTL confirms recorded state carries an eviction TTL so
// undeployed-backend state self-cleans.
func TestBreakerStore_SetsTTL(t *testing.T) {
	rdb := newRedis(t)
	ctx := context.Background()
	store := NewBreakerStore(rdb, 30*time.Second)
	now := time.Now()

	if _, err := store.RecordFailure(ctx, "ttl", "v1", 5, now); err != nil {
		t.Fatalf("RecordFailure: %v", err)
	}
	ttl, err := rdb.PTTL(ctx, store.key("ttl", "v1")).Result()
	if err != nil {
		t.Fatalf("PTTL: %v", err)
	}
	if ttl <= 0 {
		t.Fatalf("breaker state TTL = %v, want positive (idle eviction)", ttl)
	}
}
