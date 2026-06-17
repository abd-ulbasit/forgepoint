package events

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ============================================================================
// PgxOutboxReader — the Postgres adapter for the relay's OutboxReader port
// ============================================================================
//
// This is the SQL behind the relay's two operations: claim the oldest unpublished
// rows, and mark a row published. It is the only place the relay touches the
// database, and it lives in the events package (next to the relay it serves)
// rather than in repository/postgres because it is an EVENTS-LAYER concern — the
// repository owns the WRITE side of the outbox (RecordUsageTx inserts the rows in
// the business tx); the relay owns the READ/PUBLISH side. Splitting them this way
// keeps the transactional-write boundary (repository) cleanly separate from the
// async-drain boundary (events), which is exactly the outbox pattern's two halves.
//
// WHY pgxpool (the same pool the repository uses): there is ONE Postgres database
// per service (database-per-service), so the relay reads the very rows the
// repository wrote through the SAME pool. main.go (next stage) builds one pool and
// hands it to both the Store and this reader.
//
// EVERY query is PARAMETERIZED — caller input never touches query text. (The only
// "input" here is the batch limit, an int we still bind as a parameter.)
// ============================================================================

// PgxOutboxReader reads/marks outbox rows over a pgx pool. It implements
// OutboxReader (compile-time proof below).
type PgxOutboxReader struct {
	pool *pgxpool.Pool
}

// NewPgxOutboxReader wraps a pool. The pool is owned by the composition root (it is
// the same pool the postgres.Store uses); this reader does not close it.
func NewPgxOutboxReader(pool *pgxpool.Pool) *PgxOutboxReader {
	return &PgxOutboxReader{pool: pool}
}

var _ OutboxReader = (*PgxOutboxReader)(nil)

// ClaimUnpublished returns up to `limit` unpublished outbox rows, OLDEST FIRST.
//
// THE QUERY rides the partial index outbox_unpublished_idx (created in the
// migration as `ON outbox (created_at) WHERE published_at IS NULL`): the WHERE
// matches the index predicate so the planner scans ONLY unpublished rows in
// created_at order — the scan cost is proportional to the backlog, not the whole
// (ever-growing) outbox table. This is why the relay stays cheap even as the table
// accumulates millions of published rows awaiting a retention prune.
//
// ORDER BY created_at gives a stable FIFO-ish drain so events publish roughly in
// the order they were written (best-effort — NATS ordering is per-stream, and the
// consumer dedupes/orders by business key anyway, so strict total order is neither
// promised nor needed).
//
// SINGLE-RELAY assumption (see relay.go header): a plain SELECT, not
// FOR UPDATE SKIP LOCKED. Two relays would each claim the same batch and both
// publish — safe (consumers dedupe on the row id) but wasteful. The documented
// upgrade for a multi-replica relay is to append FOR UPDATE SKIP LOCKED and run the
// claim+publish+mark inside one tx so each replica grabs a disjoint batch.
func (r *PgxOutboxReader) ClaimUnpublished(ctx context.Context, limit int) ([]OutboxRow, error) {
	const q = `
		SELECT id, event_type, payload, created_at
		FROM outbox
		WHERE published_at IS NULL
		ORDER BY created_at
		LIMIT $1`
	rows, err := r.pool.Query(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("query unpublished outbox: %w", err)
	}
	defer rows.Close()

	var out []OutboxRow
	for rows.Next() {
		var row OutboxRow
		// payload is JSONB; pgx scans it into []byte (the raw document) which the
		// relay's decodeOutboxPayload unmarshals by event_type. We never interpret
		// the bytes here — the reader is a dumb conduit, the relay owns the mapping.
		if err := rows.Scan(&row.ID, &row.EventType, &row.Payload, &row.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan outbox row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		// A row-iteration error (e.g. connection dropped mid-stream) must not look
		// like a clean empty result — surface it so the relay logs + retries.
		return nil, fmt.Errorf("iterate outbox rows: %w", err)
	}
	return out, nil
}

// MarkPublished stamps published_at = now() for the row, removing it from the
// unpublished set (and the partial index). Called only AFTER a successful publish
// (publish-then-mark → at-least-once; see relay.go).
//
// WHY now() (DB clock) and not a Go timestamp: the column is purely operational
// (when did the relay drain this row), and using the database's clock avoids
// clock-skew between relay replicas writing inconsistent times. It is NOT a
// business timestamp (the event's own occurred_at lives in the payload).
//
// IDEMPOTENT: re-marking an already-published row (a republish race) is a harmless
// no-op UPDATE — the WHERE matches by id and re-sets published_at; we don't guard
// on published_at IS NULL because re-stamping is benign and the extra predicate
// would only mask a double-mark we don't care about.
func (r *PgxOutboxReader) MarkPublished(ctx context.Context, id string) error {
	const q = `UPDATE outbox SET published_at = now() WHERE id = $1`
	if _, err := r.pool.Exec(ctx, q, id); err != nil {
		return fmt.Errorf("mark outbox row %s published: %w", id, err)
	}
	return nil
}
