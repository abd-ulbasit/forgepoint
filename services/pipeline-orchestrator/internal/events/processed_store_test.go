package events_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/events"
)

// ============================================================================
// FINDING 2 — the production binary must BOOT under FP_ENV=production
// ============================================================================
//
// Before the fix, main.go unconditionally called natsutil.NewMemoryProcessedStore(),
// which PANICS when FP_ENV=production — so the orchestrator could not boot in its
// target environment. The fix routes store selection through
// events.NewProcessedStore, which under FP_ENV=production builds a durable,
// shared Postgres-backed store instead. These tests pin that the production path
// (a) does NOT panic, (b) actually dedups, and (c) is DURABLE across a simulated
// restart — and that the dev/test path returns the in-memory store.

// TestNewProcessedStore_DevReturnsMemoryStore: with FP_ENV unset/dev, the selector
// returns the in-memory store (correct for a single-replica local run). No Postgres
// needed — this is the fast path the integration suite uses.
func TestNewProcessedStore_DevReturnsMemoryStore(t *testing.T) {
	t.Setenv("FP_ENV", "development")

	store, err := events.NewProcessedStore(context.Background(), nil)
	if err != nil {
		t.Fatalf("NewProcessedStore(dev): unexpected error: %v", err)
	}
	if _, ok := store.(*natsutil.MemoryProcessedStore); !ok {
		t.Errorf("dev store type = %T, want *natsutil.MemoryProcessedStore", store)
	}
}

// TestNewProcessedStore_ProductionBootsWithDurableStore is the core Finding-2
// proof: under FP_ENV=production the selector MUST NOT panic (the old failure
// mode) and MUST return a working, durable store. We exercise the real dedup
// semantics against a real Postgres, then build a SECOND store over the SAME
// database (simulating a process restart / a different replica) and confirm the
// mark is still visible — the cross-replica + crash-survival property the
// in-memory store lacks and the whole fix is about.
func TestNewProcessedStore_ProductionBootsWithDurableStore(t *testing.T) {
	dsn := testutil.StartPostgres(t) // SkipIfNoDocker inside
	t.Setenv("FP_ENV", "production")

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	// The exact call main.go makes at boot. Before the fix this code path reached
	// NewMemoryProcessedStore() and PANICKED under FP_ENV=production; now it builds
	// the durable store and returns cleanly — i.e. the service can boot.
	store, err := events.NewProcessedStore(ctx, pool)
	if err != nil {
		t.Fatalf("NewProcessedStore(production): unexpected error: %v", err)
	}
	if _, ok := store.(*natsutil.MemoryProcessedStore); ok {
		t.Fatalf("production store is the in-memory store; want the durable Postgres-backed store")
	}

	const id = "evt-prod-1"
	// Fresh table → not processed yet.
	seen, err := store.IsProcessed(ctx, id)
	if err != nil {
		t.Fatalf("IsProcessed(before): %v", err)
	}
	if seen {
		t.Fatalf("IsProcessed(before) = true, want false on a fresh store")
	}
	// Mark, then it must be seen. MarkProcessed is idempotent (ON CONFLICT) — mark
	// twice to prove a redelivery-driven double mark does not error.
	if err := store.MarkProcessed(ctx, id); err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}
	if err := store.MarkProcessed(ctx, id); err != nil {
		t.Fatalf("MarkProcessed(again, idempotent): %v", err)
	}
	seen, err = store.IsProcessed(ctx, id)
	if err != nil {
		t.Fatalf("IsProcessed(after): %v", err)
	}
	if !seen {
		t.Fatalf("IsProcessed(after) = false, want true after MarkProcessed")
	}

	// DURABILITY / CROSS-REPLICA: a SECOND store over the SAME database (a restarted
	// process or a different replica in the consumer group) must still see the mark.
	// This is precisely what MemoryProcessedStore cannot do and why production needs
	// this store.
	store2, err := events.NewProcessedStore(ctx, pool)
	if err != nil {
		t.Fatalf("NewProcessedStore(production, 2nd): %v", err)
	}
	seen, err = store2.IsProcessed(ctx, id)
	if err != nil {
		t.Fatalf("IsProcessed(store2): %v", err)
	}
	if !seen {
		t.Errorf("second store does not see the mark; durable/shared dedup is broken")
	}
}
