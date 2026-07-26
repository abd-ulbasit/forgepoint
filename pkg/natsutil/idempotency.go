package natsutil

import (
	"context"
	"os"
	"sync"
)

// ============================================================================
// IDEMPOTENT CONSUMPTION
// ============================================================================
//
// WHY: NATS JetStream gives at-LEAST-once delivery — a message can be delivered
// more than once (handler succeeded but the ACK was lost; a redelivery after a
// consumer crash; etc.). If a handler has side effects (charge a customer,
// increment usage, send a webhook), processing the same event twice is a bug.
//
// The standard fix is an idempotency key. Every event carries a unique ID
// (EventEnvelope.ID). A consumer records the IDs it has processed and skips any
// it has seen before. ProcessedStore is that record.
//
// THE EXACTLY-ONCE CAVEAT:
//   A separate store gives at-least-once + dedup, NOT true exactly-once. The
//   gap: if the handler commits its side effect, then the process crashes
//   before MarkProcessed, the redelivery re-runs the side effect. To close that
//   gap, the dedup record must be written in the SAME transaction as the side
//   effect — which only the owning service can do (e.g., INSERT into a
//   processed_events table inside the business transaction). ProcessedStore is
//   the library-level convenience; the transactional version is the gold
//   standard.
// ============================================================================

// ProcessedStore records which event IDs have already been handled, enabling
// idempotent consumption. Implementations should be safe for concurrent use and
// are expected to be backed by durable storage (a Postgres table, a Redis SET
// with TTL, etc.) so dedup survives restarts.
type ProcessedStore interface {
	// IsProcessed reports whether the event ID was already successfully handled.
	IsProcessed(ctx context.Context, eventID string) (bool, error)
	// MarkProcessed records the event ID as successfully handled.
	MarkProcessed(ctx context.Context, eventID string) error
}

// MemoryProcessedStore is an in-memory ProcessedStore for TESTS and local dev
// only. It must NOT be used in production because:
//   - It does not survive process restarts (events are re-processed after restart)
//   - The internal map is unbounded — long-running services accumulate event IDs
//     indefinitely, causing OOM in high-throughput scenarios
//   - It is not shared across replicas — a consumer group of 3 replicas each has
//     a separate store, so a message redelivered to a different replica is processed
//     twice even though another replica already handled it
//
// In production use a durable, shared backend: a Redis SET with TTL (for
// short dedup windows), or a Postgres processed_events table (for full history
// and transactional safety — see the exactly-once caveat comment above).
type MemoryProcessedStore struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

// NewMemoryProcessedStore creates an empty in-memory store.
//
// PRODUCTION GUARD: panics immediately if FP_ENV=production. MemoryProcessedStore
// must not be used in production (unbounded map → OOM; no cross-replica sharing;
// no crash-survival). Use a Redis/Postgres-backed ProcessedStore instead.
//
// Local dev: leave FP_ENV unset or set to "development".
// Tests: leave FP_ENV unset (or use t.Setenv("FP_ENV", "test")).
func NewMemoryProcessedStore() *MemoryProcessedStore {
	// Fail loud and fast: if someone wires this in production (e.g. via a
	// misconfigured dependency injection), we'd rather panic at startup (visible,
	// auditable) than silently cause data-correctness bugs at scale.
	if os.Getenv("FP_ENV") == "production" {
		panic("MemoryProcessedStore is for tests/dev only; use a Redis/Postgres-backed ProcessedStore in production")
	}
	return &MemoryProcessedStore{seen: make(map[string]struct{})}
}

// IsProcessed reports whether eventID has been marked.
func (m *MemoryProcessedStore) IsProcessed(_ context.Context, eventID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.seen[eventID]
	return ok, nil
}

// MarkProcessed records eventID as processed.
func (m *MemoryProcessedStore) MarkProcessed(_ context.Context, eventID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seen[eventID] = struct{}{}
	return nil
}
