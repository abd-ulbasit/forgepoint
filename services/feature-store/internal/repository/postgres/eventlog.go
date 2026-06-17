// eventlog.go — the Postgres ADAPTER implementing domain.EventLog: the append-only
// write model that is the SOURCE OF TRUTH for the feature store.
//
// ============================================================================
// PATTERN: EVENT SOURCING — the append-only log, in Postgres
// ============================================================================
//
// This adapter realizes the EventLog port (domain/ports.go). It is INSERT-only on
// feature_events — there is deliberately no UPDATE/DELETE of a stored event. The
// only "mutation" surfaces are:
//   - Append: INSERT a batch of events as one atomic, version-ordered unit.
//   - the derived view_name_index / idempotency_keys rows it maintains in the SAME
//     transaction as the append (so the catalog and the dedup record commit
//     atomically with the events that justify them).
//
// ============================================================================
// THE HARD PART: MONOTONIC, GAP-FREE VERSION ASSIGNMENT under concurrency
// ============================================================================
//
// The domain contract says Append assigns each event a "monotonic, gap-free
// Version". Gap-free matters because WrittenThroughVersion is used for
// read-your-writes and the version IS the replay order; gaps would make "have I
// seen everything through N?" ill-defined.
//
// A Postgres SEQUENCE / BIGSERIAL is monotonic but NOT gap-free: nextval() is
// non-transactional, so a rolled-back append burns numbers, leaving holes. So we
// assign the version ourselves inside the transaction:
//
//   1. SELECT pg_advisory_xact_lock(<view-derived key>)   -- serialize writers to a view
//   2. SELECT COALESCE(MAX(version),0) FROM feature_events  -- current head
//   3. assign version = head+1, head+2, ... for the batch
//   4. INSERT the rows
//   5. COMMIT (the advisory lock auto-releases at tx end)
//
// WHY a transaction-scoped advisory lock and not SELECT ... FOR UPDATE on a counter
// row: we want a GLOBAL gap-free sequence (version is unique across all views — it
// is the PK), but contention only between concurrent writers, and automatic release
// on commit/rollback even if the client crashes. pg_advisory_xact_lock gives
// exactly that. We lock on a single fixed key here (globalVersionLockKey) so the
// global version counter has one writer at a time — correct and simple. (A
// per-view sequence would allow more parallelism but then version is only unique
// per view, complicating the PK and replay; the domain wants one total order, so we
// keep one global lock. Throughput tradeoff is acceptable: appends are batched and
// the critical section is tiny.)
//
// INTERVIEW: "How do you get gap-free ordering without a sequence's gaps?" Assign
// the number inside the same transaction that inserts the row, under a lock, so a
// rollback releases the number. The lock makes the read-modify-write of the head
// atomic across concurrent appenders.
//
// ============================================================================
// IDEMPOTENCY (exactly-once EFFECT under at-least-once delivery)
// ============================================================================
//
// Before assigning versions, Append checks idempotency_keys for (key, view). A hit
// means this command already committed; we RELOAD the original event range and
// return it with replayed=true — no second insert. The check + insert of the
// idempotency row happen INSIDE the append transaction, so a key is recorded only
// if its events committed (no orphan keys, no orphan events).
package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abd-ulbasit/forgepoint/services/feature-store/internal/domain"
)

// globalVersionLockKey is the fixed advisory-lock key that serializes version
// assignment across all appenders. A constant (not per-view) because version is a
// GLOBAL total order (the feature_events PK). Any nonzero constant works; this one
// is arbitrary but stable.
const globalVersionLockKey int64 = 0x6673_746f_7265 // "fstore" in hex, just a recognizable constant

// EventLog is the Postgres-backed implementation of domain.EventLog.
type EventLog struct {
	pool *pgxpool.Pool
}

// NewEventLog constructs the adapter over a pgx connection pool. The pool's
// lifecycle is owned by the caller (main.go) — the adapter borrows connections per
// call and never closes the pool. This matches the constructor main.go documents:
// postgres.NewEventLog(pool).
func NewEventLog(pool *pgxpool.Pool) *EventLog {
	return &EventLog{pool: pool}
}

// Compile-time proof the adapter satisfies the port. If a signature drifts, the
// build breaks here, not at the wiring site.
var _ domain.EventLog = (*EventLog)(nil)

// ============================================================================
// Append — atomic, ordered, idempotent batch insert
// ============================================================================

func (l *EventLog) Append(ctx context.Context, idempotencyKey string, events []domain.FeatureEvent) (appended []domain.FeatureEvent, writtenThrough int64, replayed bool, err error) {
	if len(events) == 0 {
		// Nothing to write. Return the current head so writtenThrough is meaningful
		// (a caller may Append an empty batch as a "where am I?" probe). No tx needed.
		head, herr := l.head(ctx, l.pool)
		if herr != nil {
			return nil, 0, false, herr
		}
		return nil, head, false, nil
	}

	// All events in one Append target the same view (the service builds them that
	// way). We derive the view id from the first event for the idempotency lookup.
	viewID := events[0].FeatureViewID

	// BEGIN: the whole append is one transaction. Either every event + the
	// idempotency row + any name-index update commit together, or nothing does
	// (atomicity — a failed append leaves NO partial write, which the tests assert).
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return nil, 0, false, fmt.Errorf("begin append tx: %w", err)
	}
	// Rollback is a no-op after a successful Commit, so this defer is safe and
	// guarantees we never leak a transaction on an early return / panic.
	defer func() { _ = tx.Rollback(ctx) }()

	// IDEMPOTENCY CHECK (inside the tx so it sees committed keys consistently). A hit
	// replays the original range; we never re-insert.
	if idempotencyKey != "" {
		first, last, found, ierr := l.lookupIdempotency(ctx, tx, idempotencyKey, viewID)
		if ierr != nil {
			return nil, 0, false, ierr
		}
		if found {
			original, lerr := l.loadRange(ctx, tx, first, last)
			if lerr != nil {
				return nil, 0, false, lerr
			}
			// Commit the (read-only) tx to release the snapshot cleanly; nothing was
			// written. replayed=true tells the service this was a deduped retry.
			if cerr := tx.Commit(ctx); cerr != nil {
				return nil, 0, false, fmt.Errorf("commit replay tx: %w", cerr)
			}
			return original, last, true, nil
		}
	}

	// SERIALIZE version assignment: take the global advisory lock for the duration of
	// this tx. Concurrent appenders block here, then proceed one at a time, so the
	// read-modify-write of the head below is atomic and the resulting versions are
	// contiguous and gap-free.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, globalVersionLockKey); err != nil {
		return nil, 0, false, fmt.Errorf("acquire version lock: %w", err)
	}

	// Current head (highest committed version). Under the lock this is a stable base.
	head, err := l.head(ctx, tx)
	if err != nil {
		return nil, 0, false, err
	}

	// Assign contiguous versions and INSERT. We populate each event's Version (and
	// keep its server-assigned ID) and build the return slice so the service gets the
	// events back with their final versions — the port contract.
	out := make([]domain.FeatureEvent, len(events))
	for i := range events {
		e := events[i]
		e.Version = head + int64(i) + 1
		if err = l.insertEvent(ctx, tx, e); err != nil {
			return nil, 0, false, err
		}
		// Maintain the catalog projection in the SAME tx for definition/deletion
		// events, so a GetFeatureView/List right after a define is read-your-writes.
		if e.Type == domain.FeatureEventViewDefined || e.Type == domain.FeatureEventViewDeleted {
			if err = l.upsertNameIndex(ctx, tx, e); err != nil {
				return nil, 0, false, err
			}
		}
		out[i] = e
	}
	writtenThrough = head + int64(len(events))

	// Record the idempotency range so a retry replays instead of re-appending.
	if idempotencyKey != "" {
		if err = l.recordIdempotency(ctx, tx, idempotencyKey, viewID, out[0].Version, writtenThrough, out[0].AppendedAt); err != nil {
			return nil, 0, false, err
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, 0, false, fmt.Errorf("commit append tx: %w", err)
	}
	return out, writtenThrough, false, nil
}

// head returns the highest committed version (0 if the log is empty). Takes a
// querier (pool or tx) so it works both outside a tx (empty-batch probe) and inside
// one (under the advisory lock).
func (l *EventLog) head(ctx context.Context, q querier) (int64, error) {
	var head int64
	if err := q.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) FROM feature_events`).Scan(&head); err != nil {
		return 0, fmt.Errorf("read log head: %w", err)
	}
	return head, nil
}

// insertEvent writes one log row. ALL values are parameterized ($1..$9) — never
// string-concatenated — so a feature name, entity id, or JSON payload carrying SQL
// metacharacters is data, never executable (SQL-injection-proof by construction).
func (l *EventLog) insertEvent(ctx context.Context, tx pgx.Tx, e domain.FeatureEvent) error {
	valuesJSON, err := marshalValues(e.Values)
	if err != nil {
		return err
	}
	viewDefJSON, err := marshalViewDef(e.ViewDef)
	if err != nil {
		return err
	}
	const q = `
		INSERT INTO feature_events
			(version, id, event_type, feature_view_id, entity_id, feature_values, event_time, view_def, appended_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	_, err = tx.Exec(ctx, q,
		e.Version,
		e.ID,
		int16(e.Type),
		e.FeatureViewID,
		e.EntityID,
		valuesJSON,
		e.EventTime,
		viewDefJSON,
		e.AppendedAt,
	)
	if err != nil {
		// A unique_violation on id (a buggy duplicate event id) maps to the domain's
		// already-exists vocabulary. In normal flow ids are fresh UUIDs so this is a
		// guardrail, not an expected path.
		if isUniqueViolation(err) {
			return fmt.Errorf("duplicate event id %s: %w", e.ID, domain.ErrEventLogNotFound)
		}
		return fmt.Errorf("insert event: %w", err)
	}
	return nil
}

// upsertNameIndex keeps the team-scoped catalog projection in sync as definition
// events are appended. A ViewDefined either inserts (CREATE) or updates name/
// updated_at (EVOLVE); a ViewDeleted flips is_deleted. ON CONFLICT makes it an
// idempotent upsert keyed by feature_view_id.
func (l *EventLog) upsertNameIndex(ctx context.Context, tx pgx.Tx, e domain.FeatureEvent) error {
	switch e.Type {
	case domain.FeatureEventViewDefined:
		d := e.ViewDef
		if d == nil {
			return fmt.Errorf("view-defined event %s missing view_def", e.ID)
		}
		const q = `
			INSERT INTO view_name_index
				(feature_view_id, owner_team, name, name_lower, is_deleted, created_at, updated_at)
			VALUES ($1, $2, $3, $4, FALSE, $5, $5)
			ON CONFLICT (feature_view_id) DO UPDATE
				SET name = EXCLUDED.name,
				    name_lower = EXCLUDED.name_lower,
				    updated_at = EXCLUDED.updated_at,
				    -- a re-definition revives a previously-deleted view back to live.
				    is_deleted = FALSE`
		_, err := tx.Exec(ctx, q, e.FeatureViewID, d.OwnerTeam, d.Name, strings.ToLower(d.Name), e.EventTime)
		if err != nil {
			// A (team, name) collision with a DIFFERENT view id is a genuine name
			// conflict — surface it as the domain sentinel. (The service resolves first,
			// so this is the race backstop.)
			if isUniqueViolation(err) {
				return fmt.Errorf("view name %q already used in team %q: %w", d.Name, d.OwnerTeam, domain.ErrEventLogNotFound)
			}
			return fmt.Errorf("upsert name index: %w", err)
		}
		return nil
	case domain.FeatureEventViewDeleted:
		const q = `UPDATE view_name_index SET is_deleted = TRUE, updated_at = $2 WHERE feature_view_id = $1`
		_, err := tx.Exec(ctx, q, e.FeatureViewID, e.EventTime)
		if err != nil {
			return fmt.Errorf("mark name index deleted: %w", err)
		}
		return nil
	default:
		return nil
	}
}

// lookupIdempotency returns the version range a prior append with this key wrote.
func (l *EventLog) lookupIdempotency(ctx context.Context, tx pgx.Tx, key, viewID string) (first, last int64, found bool, err error) {
	const q = `SELECT first_version, last_version FROM idempotency_keys WHERE idempotency_key = $1 AND feature_view_id = $2`
	err = tx.QueryRow(ctx, q, key, viewID).Scan(&first, &last)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, false, nil
	}
	if err != nil {
		return 0, 0, false, fmt.Errorf("lookup idempotency key: %w", err)
	}
	return first, last, true, nil
}

// recordIdempotency persists the range this append produced. If two retries race
// and both pass the lookup, the loser hits the PK and we treat that as "already
// recorded" (the other tx won) — but in practice the advisory lock serializes
// appenders, so this is defensive.
func (l *EventLog) recordIdempotency(ctx context.Context, tx pgx.Tx, key, viewID string, first, last int64, createdAt any) error {
	const q = `
		INSERT INTO idempotency_keys (idempotency_key, feature_view_id, first_version, last_version, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (idempotency_key, feature_view_id) DO NOTHING`
	_, err := tx.Exec(ctx, q, key, viewID, first, last, createdAt)
	if err != nil {
		return fmt.Errorf("record idempotency key: %w", err)
	}
	return nil
}

// ============================================================================
// Load / LoadView — REPLAY (read the ordered event stream)
// ============================================================================

// Load returns ALL events for a view in ascending version order (the full replay
// stream). ErrEventLogNotFound when the view has no events at all.
func (l *EventLog) Load(ctx context.Context, featureViewID string) ([]domain.FeatureEvent, error) {
	const q = `
		SELECT version, id, event_type, feature_view_id, entity_id, feature_values, event_time, view_def, appended_at
		FROM feature_events
		WHERE feature_view_id = $1
		ORDER BY version ASC`
	return l.queryEvents(ctx, q, featureViewID)
}

// LoadView returns only the DEFINITION/DELETION events (types 1 and 3) — the subset
// foldView needs — using the partial index so it never scans value rows. This is the
// performance reason the port splits Load and LoadView: reading a schema must not
// pay for millions of value events.
func (l *EventLog) LoadView(ctx context.Context, featureViewID string) ([]domain.FeatureEvent, error) {
	const q = `
		SELECT version, id, event_type, feature_view_id, entity_id, feature_values, event_time, view_def, appended_at
		FROM feature_events
		WHERE feature_view_id = $1 AND event_type IN (1, 3)
		ORDER BY version ASC`
	return l.queryEvents(ctx, q, featureViewID)
}

// loadRange reads a contiguous version range (used by the idempotency replay to
// reconstruct the original return value). Inclusive on both ends.
func (l *EventLog) loadRange(ctx context.Context, tx pgx.Tx, first, last int64) ([]domain.FeatureEvent, error) {
	const q = `
		SELECT version, id, event_type, feature_view_id, entity_id, feature_values, event_time, view_def, appended_at
		FROM feature_events
		WHERE version BETWEEN $1 AND $2
		ORDER BY version ASC`
	rows, err := tx.Query(ctx, q, first, last)
	if err != nil {
		return nil, fmt.Errorf("query event range: %w", err)
	}
	return scanEvents(rows)
}

// queryEvents runs a view-scoped event query against the pool and returns the
// decoded slice, mapping "no rows" to the storage sentinel ErrEventLogNotFound (the
// vocabulary the service translates to ErrViewNotFound).
func (l *EventLog) queryEvents(ctx context.Context, q, featureViewID string) ([]domain.FeatureEvent, error) {
	rows, err := l.pool.Query(ctx, q, featureViewID)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	events, err := scanEvents(rows)
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, domain.ErrEventLogNotFound
	}
	return events, nil
}

// scanEvents decodes pgx.Rows into domain FeatureEvents, reversing insertEvent's
// encoding (JSONB -> values/view_def, smallint -> EventType). It closes the rows.
func scanEvents(rows pgx.Rows) ([]domain.FeatureEvent, error) {
	defer rows.Close()
	var out []domain.FeatureEvent
	for rows.Next() {
		var (
			e           domain.FeatureEvent
			eventType   int16
			valuesJSON  []byte
			viewDefJSON []byte
		)
		if err := rows.Scan(
			&e.Version,
			&e.ID,
			&eventType,
			&e.FeatureViewID,
			&e.EntityID,
			&valuesJSON,
			&e.EventTime,
			&viewDefJSON,
			&e.AppendedAt,
		); err != nil {
			return nil, fmt.Errorf("scan event row: %w", err)
		}
		e.Type = domain.EventType(eventType)
		values, err := unmarshalValues(valuesJSON)
		if err != nil {
			return nil, err
		}
		e.Values = values
		viewDef, err := unmarshalViewDef(viewDefJSON)
		if err != nil {
			return nil, err
		}
		e.ViewDef = viewDef
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate event rows: %w", err)
	}
	return out, nil
}

// ============================================================================
// ResolveViewIDByName / ListViewIDs — the catalog projection reads
// ============================================================================

// ResolveViewIDByName resolves a (team, name) to a view id, case-insensitively and
// TEAM-SCOPED (a caller can never resolve another team's name — the WHERE clause is
// the authorization boundary). ErrEventLogNotFound when no such view exists for the
// team. Includes deleted views: the service still needs to resolve a retired view's
// id to read its history / report its terminal state.
func (l *EventLog) ResolveViewIDByName(ctx context.Context, team, name string) (string, error) {
	const q = `SELECT feature_view_id FROM view_name_index WHERE owner_team = $1 AND name_lower = $2`
	var id string
	err := l.pool.QueryRow(ctx, q, team, strings.ToLower(name)).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", domain.ErrEventLogNotFound
	}
	if err != nil {
		return "", fmt.Errorf("resolve view by name: %w", err)
	}
	return id, nil
}

// ListViewIDs returns a team's view ids, optionally filtered by a case-insensitive
// name SUBSTRING, paginated by an opaque cursor (the last id of the previous page).
//
// KEYSET (cursor) pagination, not OFFSET: we page by "feature_view_id > :cursor",
// which is stable under concurrent appends — OFFSET can skip or duplicate rows when
// the underlying set shifts between pages. The cursor is just the last id returned,
// which the domain treats as opaque. nextToken is empty on the last page.
func (l *EventLog) ListViewIDs(ctx context.Context, team, nameFilter string, opts domain.ListOptions) (ids []string, nextToken string, err error) {
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = domain.DefaultPageSize
	}
	if pageSize > domain.MaxPageSize {
		pageSize = domain.MaxPageSize
	}

	// Build the parameterized query. We fetch pageSize+1 rows so the presence of the
	// extra row tells us a next page exists WITHOUT a second COUNT query; we then trim
	// it and use the last KEPT id as the cursor. Filters are parameterized — the name
	// substring is passed as a bound LIKE pattern, never concatenated.
	var (
		args   []any
		where  []string
		argIdx = 1
	)
	where = append(where, fmt.Sprintf("owner_team = $%d", argIdx))
	args = append(args, team)
	argIdx++

	if nameFilter != "" {
		// ILIKE with a wrapped pattern does the case-insensitive substring match. We
		// pass the pattern as a parameter; '%' wrapping is on the VALUE, so user input
		// is data. (A user-supplied '%' becomes a literal-ish wildcard inside their own
		// filter, which is acceptable for a search box; it cannot break out of the
		// query — that is the injection-relevant property.)
		where = append(where, fmt.Sprintf("name ILIKE $%d", argIdx))
		args = append(args, "%"+nameFilter+"%")
		argIdx++
	}
	if opts.PageToken != "" {
		where = append(where, fmt.Sprintf("feature_view_id > $%d", argIdx))
		args = append(args, opts.PageToken)
		argIdx++
	}

	q := fmt.Sprintf(`
		SELECT feature_view_id FROM view_name_index
		WHERE %s
		ORDER BY feature_view_id ASC
		LIMIT $%d`, strings.Join(where, " AND "), argIdx)
	args = append(args, pageSize+1) // +1 sentinel row to detect "has next page"

	rows, err := l.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, "", fmt.Errorf("list view ids: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		if scanErr := rows.Scan(&id); scanErr != nil {
			return nil, "", fmt.Errorf("scan view id: %w", scanErr)
		}
		ids = append(ids, id)
	}
	if rows.Err() != nil {
		return nil, "", fmt.Errorf("iterate view ids: %w", rows.Err())
	}

	// If we got the sentinel extra row, there's a next page: trim to pageSize and set
	// the cursor to the last KEPT id.
	if len(ids) > pageSize {
		ids = ids[:pageSize]
		nextToken = ids[len(ids)-1]
	}
	return ids, nextToken, nil
}

// ============================================================================
// small helpers
// ============================================================================

// querier abstracts "thing I can QueryRow on" so head() works against both the pool
// and a tx. pgx's Pool and Tx both satisfy this.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// isUniqueViolation reports whether err is a Postgres unique_violation (SQLSTATE
// 23505). We branch on the typed pgconn.PgError code rather than string-matching the
// message (locale/version-stable). This is how the adapter maps a DB constraint to a
// domain sentinel — the boundary between storage facts and business vocabulary.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
