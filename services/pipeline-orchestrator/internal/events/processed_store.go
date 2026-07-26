package events

import (
	"context"
	"fmt"
	"os"

	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ============================================================================
// DURABLE, SHARED ProcessedStore — the PRODUCTION transport-dedup backend
// ============================================================================
//
// WHY THIS EXISTS (Finding 2): the drift-retrain consumer needs a
// natsutil.ProcessedStore to dedup redelivered drift events at the transport
// layer (envelope id). The library ships MemoryProcessedStore for tests/dev, but
// that store PANICS under FP_ENV=production by design — it is unbounded (OOM),
// non-durable (loses dedup state on restart → re-runs a retrain), and per-replica
// (a redelivery to a DIFFERENT replica in the consumer group is processed twice).
// So the production binary MUST NOT call NewMemoryProcessedStore(); it needs a
// durable, SHARED store. This is that store, backed by the SAME Postgres the saga
// write-ahead log already uses (no new infra dependency for this service).
//
// WHY POSTGRES (not Redis) HERE: this service's only datastore is Postgres (the
// saga durability backbone). A Redis SET-with-TTL is the other common choice for a
// bounded dedup window, but it would add a brand-new dependency to a service that
// deliberately wires no Redis (see main.go's "WHY THIS SERVICE WIRES NO REDIS").
// Postgres also unlocks the GOLD-STANDARD exactly-once path: because the dedup
// row and the saga's business write both live in Postgres, a future enhancement
// can INSERT the processed_events row in the SAME transaction as the saga state
// change — closing the "handler committed, then crashed before MarkProcessed"
// gap that any separate store leaves open (see pkg/natsutil/idempotency.go's
// EXACTLY-ONCE CAVEAT). We don't take that final step here (the natsutil
// Subscriber calls IsProcessed/MarkProcessed outside the saga tx), but choosing
// Postgres keeps that door open; Redis would not.
//
// THE TABLE is self-provisioned (CREATE TABLE IF NOT EXISTS) on construction so
// the store carries its own schema — there is no cross-service migration to keep
// in lockstep, and a fresh database / integration test has the table the moment
// the store is built. processed_at lets an operator (or a future TTL/janitor job)
// see and prune old dedup rows so the table does not grow unbounded — the exact
// failure mode that makes MemoryProcessedStore unusable in production.
//
// MAKING AN AT-LEAST-ONCE CONSUMER IDEMPOTENT:
//   - transport layer: this store dedups on the envelope id (handled by natsutil
//     BEFORE our handler runs) — collapses redeliveries even across replicas
//     because the store is SHARED (one Postgres table, not per-pod memory),
//   - business layer: the saga uses drift.report_id as its IdempotencyKey, so even
//     a DIFFERENT envelope carrying the same report returns the SAME Execution.
//   At-least-once delivery + idempotent effect = exactly-once IN EFFECT.

// pgProcessedStore is a durable, shared natsutil.ProcessedStore backed by a
// Postgres table. Safe for concurrent use (pgxpool is concurrency-safe) and
// shared across all replicas of the consumer group (they read/write one table).
type pgProcessedStore struct {
	pool  *pgxpool.Pool
	table string // fully-qualified, validated at construction (never interpolated from input)
}

// processedEventsDDL provisions the dedup table. The PRIMARY KEY on event_id makes
// MarkProcessed naturally idempotent (a duplicate INSERT is an ON CONFLICT no-op)
// and IsProcessed a single indexed lookup.
const processedEventsDDL = `
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     TEXT        PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`

// newPGProcessedStore builds the Postgres-backed store and ensures its table
// exists. It pings-by-DDL at construction so a broken/unreachable DB fails the
// composition root's wiring (fail-fast) rather than the first redelivered event.
func newPGProcessedStore(ctx context.Context, pool *pgxpool.Pool) (*pgProcessedStore, error) {
	if pool == nil {
		return nil, fmt.Errorf("events: newPGProcessedStore requires a non-nil pgx pool")
	}
	if _, err := pool.Exec(ctx, processedEventsDDL); err != nil {
		return nil, fmt.Errorf("events: provision processed_events table: %w", err)
	}
	return &pgProcessedStore{pool: pool, table: "processed_events"}, nil
}

// IsProcessed reports whether the event id is already recorded (a single indexed
// PK lookup). A DB error is RETURNED (not treated as "not processed"): the
// natsutil Subscriber must decide how to handle a dedup-store outage; silently
// returning false here would risk double-processing on a transient DB blip.
func (s *pgProcessedStore) IsProcessed(ctx context.Context, eventID string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM processed_events WHERE event_id = $1)`, eventID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("events: IsProcessed query: %w", err)
	}
	return exists, nil
}

// MarkProcessed records the event id as handled. ON CONFLICT DO NOTHING makes it
// idempotent: a concurrent or redelivered mark for the same id is a harmless
// no-op rather than a primary-key violation, so two replicas racing the same
// delivery never error each other out.
func (s *pgProcessedStore) MarkProcessed(ctx context.Context, eventID string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT (event_id) DO NOTHING`, eventID,
	)
	if err != nil {
		return fmt.Errorf("events: MarkProcessed insert: %w", err)
	}
	return nil
}

// Compile-time proof the Postgres store satisfies the port the subscriber needs.
var _ natsutil.ProcessedStore = (*pgProcessedStore)(nil)

// NewProcessedStore selects the ProcessedStore by ENVIRONMENT — the env-gated
// composition the production binary needs so it never calls
// NewMemoryProcessedStore() (which panics under FP_ENV=production).
//
//   - FP_ENV=production → the durable, shared Postgres store. Required: a panic on
//     a misconfigured production boot is far better than silent at-least-once dedup
//     loss, but here we go further and actually WIRE the correct store.
//   - anything else (dev/test/unset) → the in-memory store, which is the right
//     dedup for a single-replica local run and is exactly what the integration
//     tests use.
//
// WHY a function in this package (not inlined in main.go): the env→store choice is
// a policy this adapter owns end-to-end (it ships both the port consumer and the
// production implementation), and keeping it here makes it unit-testable and gives
// main.go a single, intention-revealing call. main.go passes the saga's existing
// pgx pool so production adds NO new dependency.
func NewProcessedStore(ctx context.Context, pool *pgxpool.Pool) (natsutil.ProcessedStore, error) {
	if os.Getenv("FP_ENV") == "production" {
		// Gate construction so the production path NEVER reaches
		// NewMemoryProcessedStore(): build the durable, shared store instead.
		store, err := newPGProcessedStore(ctx, pool)
		if err != nil {
			return nil, fmt.Errorf("events: production ProcessedStore: %w", err)
		}
		return store, nil
	}
	// Dev/test: in-memory is correct (single replica, no durability needed) and is
	// what the integration tests exercise. NewMemoryProcessedStore does not panic
	// outside FP_ENV=production.
	return natsutil.NewMemoryProcessedStore(), nil
}
