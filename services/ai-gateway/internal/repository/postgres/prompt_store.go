// Package postgres is the Postgres-backed adapter for the AI Gateway's PROMPT
// REGISTRY: it implements prompt.PromptRepository against PostgreSQL using
// jackc/pgx/v5.
//
// ============================================================================
// THE ADAPTER CONTRACT (the persistence boundary) — mirrors the Model Registry
// ============================================================================
//
// The domain (internal/prompt) defines the PromptRepository PORT and stays pure —
// no SQL, no pgx. THIS package is the ADAPTER that satisfies that port against a
// real database. The dependency points inward: postgres imports prompt (for the
// types + the ErrRecordNotFound/ErrWriteConflict sentinels it must return), never
// the reverse.
//
// THE FOUR RESPONSIBILITIES:
//
//  1. PARAMETERIZED SQL ONLY. Every caller-controlled value goes through a $1/$2
//     bind parameter — NEVER string concatenation. pgx sends query text and args
//     separately, so a value like "'; DROP TABLE prompts; --" is data, never SQL.
//     This is the structural SQL-injection defense.
//
//  2. ERROR TRANSLATION. SQLSTATE 23505 (unique_violation) → prompt.ErrWriteConflict;
//     pgx.ErrNoRows → prompt.ErrRecordNotFound. The service maps those storage
//     sentinels to business errors. We translate at THIS boundary so the domain
//     never sees a driver-specific error.
//
//  3. THE ATOMIC VERSION ASSIGNMENT. CreateNextVersion computes the next per-(team,
//     name) version AND inserts the row inside ONE transaction, so the version number
//     has no read-modify-write race window (the centerpiece — see below).
//
//  4. TEAM-SCOPED READS. Every SELECT carries WHERE team = $1, so no query can return
//     another team's row. Tenancy is enforced in the SQL, not just the service.
//
// WHY pgxpool: a connection POOL is required for a concurrent gRPC server (each RPC
// borrows a conn). pgx's native pool is faster than database/sql for Postgres and
// exposes pgconn.PgError for SQLSTATE inspection (point 2).
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/prompt"
)

// PromptStore is the Postgres-backed implementation of prompt.PromptRepository.
// It holds the pool, not individual connections — every method borrows a conn for
// the duration of its query/transaction and returns it. The pool is safe for
// concurrent use by many goroutines (each RPC handler).
type PromptStore struct {
	pool *pgxpool.Pool
}

// Compile-time proof the adapter satisfies the port. Interface skew fails the BUILD
// here — the cheapest place to catch it.
var _ prompt.PromptRepository = (*PromptStore)(nil)

// NewPromptStore builds a PromptStore from a DSN, creating and pinging a pgx pool.
//
// WHY ping here: pgxpool.New is LAZY (no connection until the first query). Pinging
// in the constructor surfaces a bad DSN / unreachable DB at startup (fail fast)
// rather than on the first RPC. The caller (main.go) treats a constructor error as a
// recoverable DB-unavailable signal and leaves the prompt RPCs Unimplemented (the
// gateway still serves chat-only) — see the self-gating note in main.go.
func NewPromptStore(ctx context.Context, dsn string) (*PromptStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("ai-gateway/postgres: create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ai-gateway/postgres: ping: %w", err)
	}
	return &PromptStore{pool: pool}, nil
}

// NewPromptStoreFromPool wraps an already-constructed pool. Integration tests use this
// to share ONE pool (the one that applied migrations) with the adapter.
func NewPromptStoreFromPool(pool *pgxpool.Pool) *PromptStore {
	return &PromptStore{pool: pool}
}

// Close releases the pool. Called at shutdown.
func (s *PromptStore) Close() { s.pool.Close() }

// ============================================================================
// SQL ERROR TRANSLATION — the storage→domain sentinel boundary
// ============================================================================

// isUniqueViolation reports whether err is a Postgres unique_violation (SQLSTATE
// 23505). We inspect the typed *pgconn.PgError rather than string-matching (locale-
// and version-dependent, brittle). 23505 is the signal that the (team,name,version)
// or (team,idempotency_key) index rejected the row → prompt.ErrWriteConflict.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// promptColumns is the canonical SELECT list, in the exact order scanPrompt reads.
// Defined once so every SELECT and the scanner stay in lockstep.
const promptColumns = `id, name, version, stage, template, variables, description, team, created_at`

// ============================================================================
// CreateNextVersion — THE ATOMIC PER-(team,name) VERSION ASSIGNMENT
// ============================================================================
//
// ASSIGNING A MONOTONIC VERSION WITH NO RACE:
//
//	The next version is COALESCE(MAX(version),0)+1 over the (team,name) rows. If we
//	read MAX in one statement and INSERT in another WITHOUT a transaction, two
//	concurrent creates could both read MAX=3, both try to INSERT version 4, and one
//	would violate the (team,name,version) unique index. We close that window two ways
//	that work TOGETHER:
//
//	  1. Both statements run in ONE transaction (pgx.BeginFunc → commit on nil,
//	     rollback on error). The MAX read and the INSERT are atomic w.r.t. THIS tx.
//	  2. The (team,name,version) UNIQUE INDEX is the backstop: even if two txs
//	     interleave and both compute version 4 (READ COMMITTED lets each see the
//	     other's uncommitted-then-committed state differently), the SECOND INSERT
//	     hits 23505 → we return ErrWriteConflict, and the SERVICE retries with a
//	     recomputed MAX+1. So correctness does NOT depend on a heavier isolation
//	     level; the index + the service's retry loop make it self-correcting.
//
//	WHY not SERIALIZABLE isolation instead: it would also work, but it forces the
//	caller to handle serialization-failure retries anyway AND adds contention cost on
//	every create. The unique-index-plus-retry approach is the standard, lighter
//	pattern (the same one the Model Registry uses for its version labels).
//
// The INSERT records the idempotency key inline ("" → SQL NULL via nullableString so
// the partial unique index ignores no-key rows). The assigned version is read back
// onto the returned Prompt.
func (s *PromptStore) CreateNextVersion(ctx context.Context, p prompt.Prompt, idempotencyKey string) (prompt.Prompt, error) {
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// 1. Next version for this (team, name). COALESCE handles the brand-new name
		//    case (no rows → MAX is NULL → 0 → +1 = version 1).
		const nextQ = `SELECT COALESCE(MAX(version), 0) + 1 FROM prompts WHERE team = $1 AND name = $2`
		var next int
		if err := tx.QueryRow(ctx, nextQ, p.Team, p.Name).Scan(&next); err != nil {
			return fmt.Errorf("compute next version: %w", err)
		}
		p.Version = next

		// 2. Insert the row at the computed version. A (team,name,version) collision
		//    (lost race) or (team,idempotency_key) collision raises 23505.
		const insertQ = `
			INSERT INTO prompts
				(id, name, version, stage, template, variables, description, team, idempotency_key, created_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`
		_, err := tx.Exec(ctx, insertQ,
			p.ID, p.Name, p.Version, int16(p.Stage), p.Template, p.Variables, p.Description,
			p.Team, nullableString(idempotencyKey), p.CreatedAt,
		)
		return err
	})
	if err != nil {
		if isUniqueViolation(err) {
			return prompt.Prompt{}, prompt.ErrWriteConflict
		}
		return prompt.Prompt{}, fmt.Errorf("ai-gateway/postgres: create prompt version: %w", err)
	}
	return p, nil
}

// LookupByIdempotencyKey returns the prompt created with this key in this team, or
// prompt.ErrRecordNotFound. An EMPTY key never matches (caller opted out) — we
// short-circuit without a query so an empty key can't accidentally match.
func (s *PromptStore) LookupByIdempotencyKey(ctx context.Context, team, key string) (prompt.Prompt, error) {
	if key == "" {
		return prompt.Prompt{}, prompt.ErrRecordNotFound
	}
	q := `SELECT ` + promptColumns + ` FROM prompts WHERE team = $1 AND idempotency_key = $2`
	return scanPrompt(s.pool.QueryRow(ctx, q, team, key))
}

// GetVersion returns the exact (team, name, version), or ErrRecordNotFound. Team-scoped.
func (s *PromptStore) GetVersion(ctx context.Context, team, name string, version int) (prompt.Prompt, error) {
	q := `SELECT ` + promptColumns + ` FROM prompts WHERE team = $1 AND name = $2 AND version = $3`
	return scanPrompt(s.pool.QueryRow(ctx, q, team, name, version))
}

// GetLatest returns the highest-version row for (team, name), or ErrRecordNotFound.
// ORDER BY version DESC LIMIT 1 rides the (team, name, version DESC) index as a single
// seek. Team-scoped.
func (s *PromptStore) GetLatest(ctx context.Context, team, name string) (prompt.Prompt, error) {
	q := `SELECT ` + promptColumns + `
	        FROM prompts
	       WHERE team = $1 AND name = $2
	       ORDER BY version DESC
	       LIMIT 1`
	return scanPrompt(s.pool.QueryRow(ctx, q, team, name))
}

// GetLatestProduction returns the highest-version PRODUCTION row for (team, name), or
// ErrRecordNotFound if the name has no production version. Team-scoped; the stage
// equality lets the planner filter atop the same index.
func (s *PromptStore) GetLatestProduction(ctx context.Context, team, name string) (prompt.Prompt, error) {
	q := `SELECT ` + promptColumns + `
	        FROM prompts
	       WHERE team = $1 AND name = $2 AND stage = $3
	       ORDER BY version DESC
	       LIMIT 1`
	return scanPrompt(s.pool.QueryRow(ctx, q, team, name, int16(prompt.StageProduction)))
}

// ============================================================================
// List — team-scoped, NEWEST-FIRST, KEYSET pagination on (created_at, id)
// ============================================================================
//
// KEYSET (not OFFSET) pagination, mirroring the Model Registry: the cursor is the
// (created_at, id) of the last row of the previous page. The next page is "rows
// strictly BEFORE that anchor in (created_at DESC, id DESC) order". WHY keyset over
// OFFSET: OFFSET N rescans and discards N rows (O(N) per page, and a row inserted/
// deleted mid-pagination shifts the window → duplicates/gaps). Keyset is O(log N)
// via the (team, created_at DESC, id DESC) index and is STABLE under concurrent
// writes. The id tiebreaker makes the cursor TOTAL (rows sharing a created_at still
// order deterministically), so no row is ever skipped or repeated across pages.
//
// We fetch pageSize+1 rows: if the extra row comes back, there's a next page and its
// (created_at,id) becomes the next cursor; we then trim to pageSize.
func (s *PromptStore) List(ctx context.Context, team string, opts prompt.ListOptions) ([]prompt.Prompt, string, error) {
	cur, err := decodeCursor(opts.PageToken)
	if err != nil {
		return nil, "", err
	}
	limit := opts.PageSize
	if limit <= 0 {
		limit = prompt.DefaultPageSize
	}

	// The row-keyset predicate. The first page (no cursor) has no anchor, so we omit
	// the (created_at,id) < (anchor) clause. Postgres row-value comparison
	// (created_at, id) < ($anchorTime, $anchorID) expresses "strictly before the
	// anchor in DESC order" in one comparison — exactly the total keyset boundary.
	var (
		rows pgx.Rows
		q    string
	)
	if cur.empty {
		q = `SELECT ` + promptColumns + `
		        FROM prompts
		       WHERE team = $1
		       ORDER BY created_at DESC, id DESC
		       LIMIT $2`
		rows, err = s.pool.Query(ctx, q, team, limit+1)
	} else {
		q = `SELECT ` + promptColumns + `
		        FROM prompts
		       WHERE team = $1 AND (created_at, id) < ($2, $3)
		       ORDER BY created_at DESC, id DESC
		       LIMIT $4`
		rows, err = s.pool.Query(ctx, q, team, cur.createdAt, cur.id, limit+1)
	}
	if err != nil {
		return nil, "", fmt.Errorf("ai-gateway/postgres: list prompts: %w", err)
	}
	defer rows.Close()

	items := make([]prompt.Prompt, 0, limit+1)
	for rows.Next() {
		p, scanErr := scanPrompt(rows)
		if scanErr != nil {
			return nil, "", scanErr
		}
		items = append(items, p)
	}
	if rows.Err() != nil {
		return nil, "", fmt.Errorf("ai-gateway/postgres: list prompts rows: %w", rows.Err())
	}

	// If we got the sentinel extra row, there's a next page. The cursor anchors on the
	// LAST item of THIS page (after trimming the extra).
	var next string
	if len(items) > limit {
		items = items[:limit]
		last := items[limit-1]
		next = encodeCursor(cursor{createdAt: last.CreatedAt, id: last.ID})
	}
	return items, next, nil
}

// ============================================================================
// SCANNER + helpers
// ============================================================================

// rowScanner abstracts pgx.Row and pgx.Rows so scanPrompt works for both QueryRow
// (single) and Query (loop).
type rowScanner interface {
	Scan(dest ...any) error
}

// scanPrompt reads one row in promptColumns order, translating pgx.ErrNoRows to the
// domain's ErrRecordNotFound and decoding the SMALLINT stage into the domain enum.
// pgx decodes a Postgres text[] directly into a Go []string (the Variables slice),
// so no JSON envelope is needed.
func scanPrompt(row rowScanner) (prompt.Prompt, error) {
	var (
		p     prompt.Prompt
		stage int16
	)
	err := row.Scan(
		&p.ID, &p.Name, &p.Version, &stage, &p.Template, &p.Variables, &p.Description,
		&p.Team, &p.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return prompt.Prompt{}, prompt.ErrRecordNotFound
		}
		return prompt.Prompt{}, fmt.Errorf("ai-gateway/postgres: scan prompt: %w", err)
	}
	p.Stage = prompt.Stage(stage)
	return p, nil
}

// nullableString maps "" → SQL NULL (via a *string) so the partial unique idempotency
// index treats no-key rows as absent. pgx encodes a nil *string as NULL.
func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// ============================================================================
// KEYSET CURSOR — opaque (created_at, id) token (mirrors the Registry's scoreCursor)
// ============================================================================

// cursor is a page boundary: the (created_at, id) of the last row of the previous
// page. `empty` marks "no cursor" (the first page) distinctly from a zero-time anchor.
type cursor struct {
	createdAt time.Time
	id        string
	empty     bool
}

// encodeCursor renders a cursor as an opaque base64 token. Opaque so clients treat it
// as a handle, not a parseable structure they can tamper with. Format: RFC3339Nano
// timestamp + "|" + id, then base64. (The id cannot contain "|": it's a UUID/hex.)
func encodeCursor(c cursor) string {
	raw := c.createdAt.UTC().Format(time.RFC3339Nano) + "|" + c.id
	return base64Encode(raw)
}

// decodeCursor parses a token back to a cursor. An EMPTY token means "first page"
// (cursor{empty:true}). A malformed token is a VALIDATION error (the service/handler
// maps it to InvalidArgument), never a silent first-page fallback that would hide a
// client bug.
func decodeCursor(token string) (cursor, error) {
	if token == "" {
		return cursor{empty: true}, nil
	}
	raw, err := base64Decode(token)
	if err != nil {
		return cursor{}, fmt.Errorf("%w: malformed page token", prompt.ErrValidation)
	}
	sep := -1
	for i := len(raw) - 1; i >= 0; i-- { // id is suffix; split on the LAST '|'
		if raw[i] == '|' {
			sep = i
			break
		}
	}
	if sep < 0 {
		return cursor{}, fmt.Errorf("%w: malformed page token", prompt.ErrValidation)
	}
	ts, err := time.Parse(time.RFC3339Nano, raw[:sep])
	if err != nil {
		return cursor{}, fmt.Errorf("%w: malformed page token", prompt.ErrValidation)
	}
	return cursor{createdAt: ts, id: raw[sep+1:]}, nil
}
