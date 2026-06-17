package postgres

import (
	"sync"
	"testing"
)

// TestIdempotencyLookupMiss: an unrecorded (operation, key) is a clean miss —
// found=false, no error. This is the first-call path every mutating RPC takes.
func TestIdempotencyLookupMiss(t *testing.T) {
	s, ctx := newTestStore(t)
	_, found, err := s.Idempotency().Lookup(ctx, "start_run", "key-1")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if found {
		t.Fatal("unrecorded key should be a miss")
	}
}

// TestIdempotencyRecordAndReplay: after Record, a Lookup with the SAME
// (operation, key) returns the stored result_id — the replay that lets a retried
// RPC return the original result instead of creating a duplicate.
func TestIdempotencyRecordAndReplay(t *testing.T) {
	s, ctx := newTestStore(t)
	store := s.Idempotency()

	if err := store.Record(ctx, "start_run", "key-1", "run-123"); err != nil {
		t.Fatalf("record: %v", err)
	}
	got, found, err := store.Lookup(ctx, "start_run", "key-1")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if !found || got != "run-123" {
		t.Fatalf("replay mismatch: found=%v id=%q", found, got)
	}
}

// TestIdempotencyScopedByOperation: the SAME key under DIFFERENT operations does
// not collide — keys are scoped per-RPC by the (operation, key) composite PK.
func TestIdempotencyScopedByOperation(t *testing.T) {
	s, ctx := newTestStore(t)
	store := s.Idempotency()

	if err := store.Record(ctx, "start_run", "shared", "run-A"); err != nil {
		t.Fatalf("record start_run: %v", err)
	}
	if err := store.Record(ctx, "log_metrics", "shared", "run-B"); err != nil {
		t.Fatalf("record log_metrics: %v", err)
	}

	a, _, _ := store.Lookup(ctx, "start_run", "shared")
	b, _, _ := store.Lookup(ctx, "log_metrics", "shared")
	if a != "run-A" || b != "run-B" {
		t.Fatalf("operation scoping failed: start_run=%q log_metrics=%q", a, b)
	}
}

// TestIdempotencyRecordIsIdempotent: recording the SAME (operation, key) twice is
// a no-op (ON CONFLICT DO NOTHING), never a unique_violation error — the
// success-path Record must stay quiet under a benign double-write.
func TestIdempotencyRecordIsIdempotent(t *testing.T) {
	s, ctx := newTestStore(t)
	store := s.Idempotency()

	if err := store.Record(ctx, "delete_run", "k", "run-1"); err != nil {
		t.Fatalf("first record: %v", err)
	}
	// Second record of the same key with the same id — must not error.
	if err := store.Record(ctx, "delete_run", "k", "run-1"); err != nil {
		t.Fatalf("duplicate record should be a no-op, got %v", err)
	}
	// The stored value is unchanged.
	got, found, _ := store.Lookup(ctx, "delete_run", "k")
	if !found || got != "run-1" {
		t.Fatalf("value changed after duplicate record: %q", got)
	}
}

// TestIdempotencyConcurrentRecord: many goroutines racing to Record the SAME key
// all succeed (DO NOTHING absorbs the losers) and the final Lookup returns a
// single consistent value. This is the concurrency contract that protects the
// success path under real retries hitting the same key simultaneously.
func TestIdempotencyConcurrentRecord(t *testing.T) {
	s, ctx := newTestStore(t)
	store := s.Idempotency()

	const n = 16
	var wg sync.WaitGroup
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			errs[i] = store.Record(ctx, "log_metrics", "race-key", "run-winner")
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent record %d errored (DO NOTHING should absorb races): %v", i, err)
		}
	}
	got, found, err := store.Lookup(ctx, "log_metrics", "race-key")
	if err != nil || !found || got != "run-winner" {
		t.Fatalf("post-race state wrong: found=%v id=%q err=%v", found, got, err)
	}
}
