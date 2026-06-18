// Package audit is the auth service's PERSISTENCE adapter for the platform's
// tamper-evident audit log. It is the PERSIST half of the capture/persist split
// (the CAPTURE half lives in pkg/audit, wired as an interceptor in every service).
//
// ============================================================================
// WHAT THIS PACKAGE DOES
// ============================================================================
//
//	NATS fp.audit.recorded ──► Consumer (consumer.go) ──► Repository (this file)
//	                                                       └─ append to the
//	                                                          hash-chained,
//	                                                          append-only table
//
// The Repository appends one audit Record to the audit_log table, computing the
// hash chain (prev_hash / entry_hash / seq) so the log is tamper-EVIDENT, and
// doing so under a SINGLE-WRITER lock so concurrent appends cannot fork the chain.
// It is idempotent on the NATS event id so a duplicate redelivery never
// double-appends.
//
// ============================================================================
// WHY THE HASH IS COMPUTED HERE (server-side), NOT trusted from the event
// ============================================================================
//
// The publishing service could compute a hash, but it does NOT know the chain
// position (the previous entry_hash) — only the single appender does. The chain
// is a property of the ORDER records are persisted in, which is decided here. So
// the producer ships the raw Record; this repository decides its place in the
// chain and seals it. (This is the same reason a blockchain node, not the
// transaction submitter, computes the block hash.)
package audit

import (
	"context"
	"errors"
	"fmt"

	pkgaudit "github.com/abd-ulbasit/forgepoint/pkg/audit"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// auditChainLockKey is the application-chosen key for pg_advisory_xact_lock. Any
// constant works as long as EVERY appender uses the SAME key — it is just a named
// mutex inside Postgres. We pick a fixed, distinctive 64-bit value so it is
// recognizable in pg_locks during debugging and cannot collide with another
// subsystem's advisory lock by accident.
const auditChainLockKey int64 = 0x4655_4450_4155_4454 // "FUDPAUDT"-ish marker

// Repository appends audit records to the append-only, hash-chained audit_log
// table. It holds the shared pool; each Append runs in its own transaction.
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository wires the shared Postgres pool into the audit Repository.
func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// AppendResult reports what Append did, so the caller (the consumer) can log
// meaningfully and tests can assert behavior.
type AppendResult struct {
	// Appended is false when the event was a DUPLICATE (event_id already present)
	// and was therefore skipped — the idempotent no-op path.
	Appended bool
	// Seq is the chain position of the row (the existing row's seq on a duplicate,
	// the new row's seq on an append). Zero only if a duplicate row's seq could
	// not be re-read (non-fatal; the row exists either way).
	Seq int64
	// EntryHash is the chain hash sealed for the row (empty on the rare duplicate
	// path where we don't re-read it).
	EntryHash string
}

// Append persists one audit Record, computing and sealing its hash-chain links.
//
// ============================================================================
// SINGLE-WRITER CHAIN INTEGRITY (the heart of correctness under concurrency)
// ============================================================================
//
// A hash chain is a LINKED LIST sealed by hashes: entry_hash[n] depends on
// entry_hash[n-1]. If two appenders run concurrently, BOTH could read the same
// current head, BOTH compute entry_hash off the same prev_hash, and we'd get TWO
// rows claiming the same predecessor — a FORKED chain that no verifier can walk.
//
// We serialize appends with pg_advisory_xact_lock(key): the FIRST transaction to
// take the lock proceeds; any concurrent appender BLOCKS until the first COMMITs
// (the lock auto-releases at transaction end — that's the "_xact_" variant). So
// reading the head, computing the next hash, and inserting are effectively atomic
// with respect to other appenders. The chain extends strictly linearly.
//
//	WHY an advisory lock and not SELECT ... FOR UPDATE on the head row:
//	  FOR UPDATE locks an EXISTING row; the genesis case (empty table) has no row
//	  to lock, so two first-appenders could still race. An advisory lock is a pure
//	  named mutex independent of row existence — it covers the genesis case
//	  cleanly. (FOR UPDATE on the max-seq row is the alternative once a genesis row
//	  exists; the advisory lock is simpler and uniform across both cases.)
//
//	WHY this scales fine for an audit log:
//	  Audit appends are serialized by design — there is ONE logical tail of the
//	  chain. The single consumer (this service) is the only writer, usually a
//	  single replica draining fp.audit.recorded; the lock is essentially
//	  uncontended. Even with multiple consumer replicas, the lock makes their
//	  appends safe at the cost of serializing them — acceptable for a control-plane
//	  audit rate (~1K/s), not a data plane. (To scale writes you would sequence
//	  per-source sub-chains; out of scope here.)
//
// ============================================================================
// IDEMPOTENCY (a duplicate redelivery must not double-append)
// ============================================================================
//
// NATS is at-least-once: the same audit event can be delivered twice. event_id is
// the envelope id (UNIQUE in the schema). We INSERT ... ON CONFLICT (event_id) DO
// NOTHING: a duplicate inserts zero rows, and we report Appended=false. WITHOUT
// this, a duplicate would either violate the UNIQUE constraint (error → NAK →
// redeliver → error, a poison loop) or — worse, if we keyed on nothing — append a
// second row and BREAK THE CHAIN. The ON CONFLICT skip is what makes the consumer
// safely idempotent.
func (r *Repository) Append(ctx context.Context, eventID string, rec pkgaudit.Record) (AppendResult, error) {
	// Begin a transaction: the advisory lock, the head read, the hash computation,
	// and the insert must all live in ONE transaction so the lock is held across
	// the read-compute-insert and released exactly at commit.
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return AppendResult{}, fmt.Errorf("audit: begin tx: %w", err)
	}
	// Rollback is a no-op after a successful Commit, so this defer is the safe
	// catch-all for every early-return error path below.
	defer func() { _ = tx.Rollback(ctx) }()

	// FAST-PATH IDEMPOTENCY: if this event_id is already persisted, skip entirely —
	// before even taking the chain lock, so duplicate redeliveries don't serialize
	// behind real appends. (The ON CONFLICT below is the authoritative guard; this
	// is the cheap early-out.)
	if seq, hash, found, dErr := lookupByEventID(ctx, tx, eventID); dErr != nil {
		return AppendResult{}, dErr
	} else if found {
		return AppendResult{Appended: false, Seq: seq, EntryHash: hash}, nil
	}

	// SINGLE-WRITER GATE: take the advisory lock for the audit chain. Concurrent
	// appenders block here until our transaction commits/rolls back.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, auditChainLockKey); err != nil {
		return AppendResult{}, fmt.Errorf("audit: acquire chain lock: %w", err)
	}

	// Read the CURRENT head of the chain (highest seq) under the lock. Empty table
	// → genesis: prevHash = GenesisHash (""), nextSeq = 1.
	prevHash, nextSeq, err := readHead(ctx, tx)
	if err != nil {
		return AppendResult{}, err
	}

	// Stamp the chain links into the record's neighbors and SEAL it. ChainHash is
	// the SAME function pkg/audit defines and any verifier uses — the server seals,
	// the verifier re-walks, the bytes must match. The hash commits to the record
	// AS PERSISTED, so the canonical encoding here must equal what a verifier feeds
	// ChainHash; pkg/audit.canonicalJSON is that single source of truth.
	entryHash := pkgaudit.ChainHash(prevHash, rec)

	const insert = `
		INSERT INTO audit_log (
			event_id, seq,
			actor_user_id, actor_email, actor_team, actor_role,
			action, resource, decision, grpc_code, err, correlation_id,
			occurred_at, prev_hash, entry_hash
		) VALUES (
			$1, $2,
			$3, $4, $5, $6,
			$7, $8, $9, $10, $11, $12,
			$13, $14, $15
		)
		ON CONFLICT (event_id) DO NOTHING`

	tag, err := tx.Exec(ctx, insert,
		eventID, nextSeq,
		rec.Actor.UserID, rec.Actor.Email, rec.Actor.Team, rec.Actor.Role,
		rec.Action, rec.Resource, string(rec.Decision), rec.GRPCCode, rec.Err, rec.CorrelationID,
		rec.OccurredAt.UTC(), prevHash, entryHash,
	)
	if err != nil {
		return AppendResult{}, fmt.Errorf("audit: insert record: %w", err)
	}

	if tag.RowsAffected() == 0 {
		// A concurrent appender inserted this same event_id between our fast-path
		// check and here (a genuine race the ON CONFLICT absorbs). Treat as a
		// successful idempotent no-op; commit (releasing the lock) and report skip.
		if err := tx.Commit(ctx); err != nil {
			return AppendResult{}, fmt.Errorf("audit: commit (dup): %w", err)
		}
		return AppendResult{Appended: false}, nil
	}

	if err := tx.Commit(ctx); err != nil {
		return AppendResult{}, fmt.Errorf("audit: commit: %w", err)
	}
	return AppendResult{Appended: true, Seq: nextSeq, EntryHash: entryHash}, nil
}

// readHead returns the prev_hash to chain onto (the current head's entry_hash) and
// the next seq to assign. On an empty table it returns the genesis values:
// prevHash = pkgaudit.GenesisHash (""), nextSeq = 1.
//
// We read the MAX(seq) row's entry_hash. Because this runs while holding the
// advisory lock, no other appender can change the head between this read and our
// insert — that is the whole point of the lock.
func readHead(ctx context.Context, tx pgx.Tx) (prevHash string, nextSeq int64, err error) {
	const q = `
		SELECT entry_hash, seq
		FROM audit_log
		ORDER BY seq DESC
		LIMIT 1`
	var headHash string
	var headSeq int64
	row := tx.QueryRow(ctx, q)
	switch scanErr := row.Scan(&headHash, &headSeq); {
	case scanErr == nil:
		return headHash, headSeq + 1, nil
	case errors.Is(scanErr, pgx.ErrNoRows):
		// Genesis: empty table. Chain onto the empty-string genesis hash, seq 1.
		return pkgaudit.GenesisHash, 1, nil
	default:
		return "", 0, fmt.Errorf("audit: read chain head: %w", scanErr)
	}
}

// lookupByEventID returns the seq + entry_hash of an existing row for eventID, and
// whether it was found. Used for the fast-path idempotency check.
func lookupByEventID(ctx context.Context, tx pgx.Tx, eventID string) (seq int64, hash string, found bool, err error) {
	const q = `SELECT seq, entry_hash FROM audit_log WHERE event_id = $1`
	row := tx.QueryRow(ctx, q, eventID)
	switch scanErr := row.Scan(&seq, &hash); {
	case scanErr == nil:
		return seq, hash, true, nil
	case errors.Is(scanErr, pgx.ErrNoRows):
		return 0, "", false, nil
	default:
		return 0, "", false, fmt.Errorf("audit: lookup by event_id: %w", scanErr)
	}
}
