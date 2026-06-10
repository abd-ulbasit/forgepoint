package natsutil

import (
	"context"
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
// THE EXACTLY-ONCE CAVEAT (important, and a common interview question):
//   A separate store gives at-least-once + dedup, NOT true exactly-once. The
//   gap: if the handler commits its side effect, then the process crashes
//   before MarkProcessed, the redelivery re-runs the side effect. To close that
//   gap, the dedup record must be written in the SAME transaction as the side
//   effect — which only the owning service can do (e.g., INSERT into a
//   processed_events table inside the business transaction). ProcessedStore is
//   the library-level convenience; the transactional version is the gold
//   standard. Say exactly this in an interview.
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
// only. It does not survive restarts and grows unbounded, so it must not be
// used in production — back the interface with Postgres/Redis there.
type MemoryProcessedStore struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

// NewMemoryProcessedStore creates an empty in-memory store.
func NewMemoryProcessedStore() *MemoryProcessedStore {
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
