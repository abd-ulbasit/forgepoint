// Package postgres is the PERSISTENCE ADAPTER for the Model Monitor's Postgres
// ports. It implements domain.MonitorRepository (monitor CONFIG, the write model)
// and domain.DriftReportRepository (the durable drift-verdict history) against a
// real PostgreSQL database using pgx/v5.
//
// ============================================================================
// HEXAGONAL: THIS IS AN ADAPTER, NOT THE DOMAIN
// ============================================================================
//
// The domain (services/model-monitor/internal/domain) declares the PORTS it needs
// and stays pure — stdlib + uuid only. This package is the outward ADAPTER that
// fulfills those ports with SQL. The arrow points inward: postgres → domain (we
// import the domain for its types and the ErrRepoNotFound storage sentinel; the
// domain never imports us). Swapping Postgres for another store means writing a
// new adapter against the SAME ports — the domain code is untouched. That is the
// entire payoff of the dependency rule.
//
// WHY pgx/v5 (+ pgxpool) and not database/sql: pgx is the de-facto high-perf
// Postgres driver for Go. Its native interface gives us first-class JSONB (we hand
// it []byte / json.RawMessage for thresholds + metrics), structured error codes
// (*pgconn.PgError with SQLSTATE — how we map unique_violation → ErrRepoNotFound /
// handle the idempotent conflict), and a context-honoring connection pool. EVERY
// query in this package is PARAMETERIZED ($1,$2,...) — we never string-concatenate
// caller input, so SQL injection is structurally impossible (values travel in the
// protocol's parameter slots, never in the query text). Even the dynamically
// assembled WHERE clauses (list filters) append only PLACEHOLDER text ($3, $4 …)
// by index; the values are always bound parameters.
// ============================================================================
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ============================================================================
// PostgreSQL SQLSTATE codes we translate to domain outcomes.
// ============================================================================
//
// pgx surfaces the database's error as a *pgconn.PgError whose .Code is the 5-char
// SQLSTATE. We centralize the codes that carry a domain meaning here (not as magic
// strings scattered through queries) so the mapping is auditable in one place.
const (
	// 23505 unique_violation — a duplicate key. On drift_reports it is the
	// window_id uniqueness firing (concurrent double-score of one window); Save
	// treats it as "already inserted" and refetches the existing row, which is the
	// idempotency contract. On monitors it would be the (owner_team, model_name)
	// partial-unique index; Upsert avoids it entirely via ON CONFLICT.
	pgUniqueViolation = "23505"

	// 23503 foreign_key_violation — inserting a drift_report under a monitor_id
	// that doesn't exist. We map it to ErrRepoNotFound for the referenced monitor
	// so the service reports "monitor not found" rather than a raw DB error.
	pgForeignKeyViolation = "23503"
)

// Store owns the connection pool and hands out the repository adapters that
// implement the domain's persistence ports.
//
// WHY ONE pool shared by BOTH adapter types (not a pool each): monitors and
// drift_reports live in the SAME Postgres database (database-per-SERVICE) and a
// report write FK-references a monitor — they share the same connection space. One
// pool is correct and is safe for concurrent use by many goroutines (the gRPC
// handlers + the NATS consumer). They CANNOT be one Go type: MonitorRepository and
// DriftReportRepository both declare a method named List with different signatures,
// and Go forbids two methods of the same name on one type. So Store exposes
// distinct adapter structs, each wrapping the shared pool.
type Store struct {
	pool *pgxpool.Pool
}

// New connects to Postgres at dsn and returns a Store with a ready pool.
//
// POOL LIFECYCLE (12-factor / interview note): the pool is created here and MUST be
// released by the caller via Close() at shutdown — no hidden global. pgxpool.New
// parses the DSN (incl. pool tunables like pool_max_conns) and establishes the pool
// lazily; we Ping once so a bad DSN / unreachable DB fails FAST at startup (loud,
// with a clear error) rather than on the first request.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("model-monitor/postgres: create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close() // don't leak the pool we just created
		return nil, fmt.Errorf("model-monitor/postgres: ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// NewWithPool wraps an already-constructed pool. Used by tests (which build the
// pool against a testcontainer and run migrations on it first) and by a composition
// root that manages the pool itself. The Store does NOT own a pool passed this way —
// whoever created it closes it.
func NewWithPool(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Monitors returns the adapter implementing domain.MonitorRepository.
func (s *Store) Monitors() *MonitorRepository { return &MonitorRepository{pool: s.pool} }

// Reports returns the adapter implementing domain.DriftReportRepository.
func (s *Store) Reports() *DriftReportRepository { return &DriftReportRepository{pool: s.pool} }

// Pool exposes the underlying pool (tests run migrations / assertions through it).
// Not part of any domain port — an adapter-local convenience.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Close releases the pool. Only call this for a Store created by New (which owns its
// pool); a Store built with NewWithPool shares a pool the caller owns.
func (s *Store) Close() { s.pool.Close() }

// MonitorRepository is the Postgres adapter for domain.MonitorRepository. It holds
// the shared pool; its methods are in monitor_repository.go.
type MonitorRepository struct{ pool *pgxpool.Pool }

// DriftReportRepository is the Postgres adapter for domain.DriftReportRepository.
// Methods are in drift_report_repository.go.
type DriftReportRepository struct{ pool *pgxpool.Pool }

// ============================================================================
// ERROR MAPPING — pgx error → domain vocabulary
// ============================================================================

// isUniqueViolation reports whether err is a Postgres unique_violation (23505) and,
// if so, on which constraint. The constraint name lets a call site distinguish (e.g.)
// the window_id idempotency conflict from any other.
func isUniqueViolation(err error) (constraint string, ok bool) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
		return pgErr.ConstraintName, true
	}
	return "", false
}

// isForeignKeyViolation reports whether err is a Postgres foreign_key_violation
// (23503) — used to turn a report insert against a missing monitor into
// ErrRepoNotFound.
func isForeignKeyViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgForeignKeyViolation
}

// isNoRows reports whether err is pgx's "no rows" sentinel — the signal a
// QueryRow/Scan found nothing, which the repositories translate to
// domain.ErrRepoNotFound.
func isNoRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }
