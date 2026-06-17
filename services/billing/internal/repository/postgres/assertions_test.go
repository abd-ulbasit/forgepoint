// assertions_test.go — small test-only query/assertion helpers.
//
// These reach into the database directly (a privilege tests have but production
// code does not) to verify GROUND TRUTH — "is there really only one usage row?",
// "did the outbox row land in the SAME tx as the usage row?", "did the failed tx
// leave NO partial write?". Verifying the actual stored state (not just the
// method's return value) is what makes these tests catch a partial write, a leaked
// outbox row, or a missing event that a return-value-only check would miss.
package postgres

import (
	"context"
	"testing"
	"time"
)

// countUsageRecords returns the number of usage_records rows for a team, read
// directly. Used to assert idempotency / rollback prevented a duplicate.
func countUsageRecords(t *testing.T, ctx context.Context, store *Store, team string) int {
	t.Helper()
	var n int
	if err := store.Pool().QueryRow(ctx,
		`SELECT count(*) FROM usage_records WHERE team = $1`, team).Scan(&n); err != nil {
		t.Fatalf("countUsageRecords: %v", err)
	}
	return n
}

// countOutbox returns the number of outbox rows for an aggregate (a usage record
// or invoice id), read directly. The CORE outbox assertion: after RecordUsageTx,
// the event rows exist in the SAME database as the business row — proof the dual
// write committed atomically.
func countOutbox(t *testing.T, ctx context.Context, store *Store, aggregateID string) int {
	t.Helper()
	var n int
	if err := store.Pool().QueryRow(ctx,
		`SELECT count(*) FROM outbox WHERE aggregate_id = $1`, aggregateID).Scan(&n); err != nil {
		t.Fatalf("countOutbox: %v", err)
	}
	return n
}

// countUnpublishedOutbox returns how many outbox rows are still unpublished
// (published_at IS NULL) for an aggregate. Used to prove (a) a fresh write leaves
// the row UNPUBLISHED for the relay, and (b) a failed publish-marking leaves the
// row for retry.
func countUnpublishedOutbox(t *testing.T, ctx context.Context, store *Store, aggregateID string) int {
	t.Helper()
	var n int
	if err := store.Pool().QueryRow(ctx,
		`SELECT count(*) FROM outbox WHERE aggregate_id = $1 AND published_at IS NULL`,
		aggregateID).Scan(&n); err != nil {
		t.Fatalf("countUnpublishedOutbox: %v", err)
	}
	return n
}

// outboxEventTypes returns the event_type strings of an aggregate's outbox rows,
// ordered by created_at. Lets a test assert "exactly [UsageRecorded] " or
// "[UsageRecorded, QuotaExceeded]" landed.
func outboxEventTypes(t *testing.T, ctx context.Context, store *Store, aggregateID string) []string {
	t.Helper()
	rows, err := store.Pool().Query(ctx,
		`SELECT event_type FROM outbox WHERE aggregate_id = $1 ORDER BY created_at, event_type`,
		aggregateID)
	if err != nil {
		t.Fatalf("outboxEventTypes query: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var et string
		if err := rows.Scan(&et); err != nil {
			t.Fatalf("outboxEventTypes scan: %v", err)
		}
		out = append(out, et)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("outboxEventTypes iterate: %v", err)
	}
	return out
}

// markOutboxPublished simulates the relay stamping an aggregate's outbox rows as
// published. Used to prove the relay's claim query (WHERE published_at IS NULL)
// then SKIPS them — and, by NOT calling it, that a failed marking leaves them
// claimable on retry.
func markOutboxPublished(t *testing.T, ctx context.Context, store *Store, aggregateID string, at time.Time) {
	t.Helper()
	if _, err := store.Pool().Exec(ctx,
		`UPDATE outbox SET published_at = $2 WHERE aggregate_id = $1`, aggregateID, at); err != nil {
		t.Fatalf("markOutboxPublished: %v", err)
	}
}
