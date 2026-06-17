// idempotency_store.go — the Postgres adapter for domain.IdempotencyStore.
//
// ============================================================================
// THE STRIPE IDEMPOTENCY-KEY PATTERN, PERSISTED
// ============================================================================
//
// A mutating RPC (StartRun, LogMetrics, DeleteRun, SetRunArtifacts) may be
// retried — by a flaky client, a redelivered NATS message, a gateway timeout. The
// idempotency table records (operation, key) → result_id so a RETRY returns/
// references the SAME result instead of creating a duplicate. This is exactly how
// Stripe's Idempotency-Key header works.
//
//   Lookup(op, key)  → the recorded result_id, or (·, false) on a miss.
//   Record(op, key, result_id) → store the mapping AFTER the mutation succeeds.
//
// (operation, key) is the composite PRIMARY KEY, so the same client-generated
// UUID reused across different RPCs never collides — keys are scoped PER-RPC.
//
// WHY a durable table (not Redis with a TTL): exactly-once across restarts. A
// TTL'd cache key could expire and let a late retry create a second run; the table
// keeps the promise indefinitely (a retention/cleanup job can prune ancient keys
// out of band without weakening the guarantee for in-flight retries). The port is
// backend-agnostic, so a Redis adapter remains a drop-in for a different
// deployment — but durable is the right default for "never create a duplicate."
// ============================================================================
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/abd-ulbasit/forgepoint/services/experiment-tracker/internal/domain"
)

// Compile-time proof the adapter satisfies the domain port.
var _ domain.IdempotencyStore = (*IdempotencyStore)(nil)

// Lookup returns the previously-recorded result_id for (operation, key) and
// whether it was found. A miss is (·, false, nil) — NOT an error: "no prior
// request with this key" is the normal first-call path, and the service treats
// found=false as "proceed with the mutation."
func (s *IdempotencyStore) Lookup(ctx context.Context, operation, key string) (string, bool, error) {
	const q = `SELECT result_id FROM idempotency_keys WHERE operation = $1 AND key = $2`
	var resultID string
	if err := s.pool.QueryRow(ctx, q, operation, key).Scan(&resultID); err != nil {
		if isNoRows(err) {
			return "", false, nil // miss → service proceeds with the mutation
		}
		return "", false, fmt.Errorf("idempotency lookup: %w", err)
	}
	return resultID, true, nil
}

// insertIdempotencyKeySQL is the single INSERT used by BOTH Record (its own
// pool.Exec) and RecordTx (a caller-supplied tx). Defining it once keeps the
// ON CONFLICT target and column order in exactly one place, so the standalone and
// the transactional paths can never drift apart.
//
// ON CONFLICT DO NOTHING makes the insert idempotent: if two concurrent retries
// both succeed at their mutation and race to record the SAME key, the second
// insert is a no-op rather than a unique_violation error. The recorded value is
// identical (same key → same result the service produced), so dropping the
// duplicate write loses nothing. WHY swallow the conflict rather than error: this
// runs on the success path after a mutation; failing it on a benign race would
// turn a successful operation into a reported error and possibly trigger a
// (harmful) retry. DO NOTHING keeps the success-path quiet and correct.
const insertIdempotencyKeySQL = `
	INSERT INTO idempotency_keys (operation, key, result_id)
	VALUES ($1, $2, $3)
	ON CONFLICT (operation, key) DO NOTHING`

// Record stores (operation, key) → result_id in its OWN pool.Exec, AFTER the
// mutation succeeded.
//
// ============================================================================
// THE PARTIAL-WRITE WINDOW THIS METHOD CANNOT CLOSE (read before using it)
// ============================================================================
//
// Calling Record as a SEPARATE statement after a separately-committed mutation
// opens a crash gap: if the process dies, the connection drops, or ctx is
// cancelled BETWEEN the committed mutation and this insert, the effect exists but
// no idempotency row does — and the next retry of the same key sees Lookup
// found=false and re-runs the mutation. For StartRun that is a DUPLICATE run,
// which is the very thing the key was meant to prevent (LogMetrics is saved by the
// run_metrics (run_id,key,step) UNIQUE backstop; StartRun has no such backstop).
//
// Record is retained for the no-mutation-coupling cases and for backward
// compatibility, but a state change that MUST be exactly-once across a crash has
// to record the key in the SAME transaction as the effect. Use the *Idem repo
// methods (CreateRunIdem / DeleteRunIdem / AppendMetricsIdem / SetArtifactsIdem),
// which call RecordTx inside the mutation's own pgx.BeginFunc so both the effect
// and the key commit together or not at all.
func (s *IdempotencyStore) Record(ctx context.Context, operation, key, resultID string) error {
	if _, err := s.pool.Exec(ctx, insertIdempotencyKeySQL, operation, key, resultID); err != nil {
		return fmt.Errorf("idempotency record: %w", err)
	}
	return nil
}

// RecordTx writes the (operation, key) → result_id row using a CALLER-SUPPLIED
// transaction so the idempotency key commits ATOMICALLY with the state change it
// guards. This is the fix for the partial-write window described on Record: the
// repository's *Idem methods open ONE pgx.BeginFunc, perform the mutation (insert
// the run / metrics / delete / artifacts) AND call RecordTx on the same tx, so a
// crash either rolls back BOTH or commits BOTH. There is no gap in which the run
// exists but its idempotency row does not — exactly-once survives a crash.
//
// It takes pgx.Tx (the interface the BeginFunc callback receives), not a concrete
// type, so any caller inside a transaction can enlist this write. Same ON CONFLICT
// DO NOTHING semantics as Record: a concurrent racer that already wrote the row is
// absorbed, not surfaced as an error that would abort the enclosing tx.
func (s *IdempotencyStore) RecordTx(ctx context.Context, tx pgx.Tx, operation, key, resultID string) error {
	if _, err := tx.Exec(ctx, insertIdempotencyKeySQL, operation, key, resultID); err != nil {
		return fmt.Errorf("idempotency record (tx): %w", err)
	}
	return nil
}

// recordIdempotencyKeyTx is the package-private helper the RunRepository's *Idem
// methods use to write an idempotency key inside their own transaction WITHOUT
// holding an *IdempotencyStore reference. It shares the exact same INSERT (and
// thus the same ON CONFLICT semantics) as Record/RecordTx. An empty key is a
// no-op: the *Idem methods accept an OPTIONAL key, and "no key" means "no
// idempotency requested" — the mutation still runs, just without a recorded row.
func recordIdempotencyKeyTx(ctx context.Context, tx pgx.Tx, operation, key, resultID string) error {
	if key == "" {
		return nil // no idempotency requested for this call
	}
	if _, err := tx.Exec(ctx, insertIdempotencyKeySQL, operation, key, resultID); err != nil {
		return fmt.Errorf("idempotency record (tx): %w", err)
	}
	return nil
}
