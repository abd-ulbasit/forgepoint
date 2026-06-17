// Package postgres is the PERSISTENCE ADAPTER for the Experiment Tracker's domain
// ports. It implements domain.ExperimentRepository, domain.RunRepository, and
// domain.IdempotencyStore against a real PostgreSQL database using pgx/v5.
//
// ============================================================================
// HEXAGONAL: THIS IS AN ADAPTER, NOT THE DOMAIN
// ============================================================================
//
// The domain (services/.../internal/domain) declares the PORTS it needs and
// stays pure — stdlib + uuid only. This package is the outward ADAPTER that
// fulfills those ports with SQL. The arrow points inward: postgres → domain (we
// import the domain for its types and storage sentinels; the domain never
// imports us). Swapping Postgres for another store means writing a new adapter
// against the SAME ports — the engine code is untouched. That is the entire
// payoff of the dependency rule.
//
// WHY pgx/v5 (+ pgxpool) and not database/sql: pgx is the de-facto high-perf
// Postgres driver for Go. Its native interface gives us first-class JSONB
// (we hand it []byte / json.RawMessage), structured error codes
// (*pgconn.PgError with SQLSTATE — how we map unique_violation → ErrRepoConflict),
// and a context-honoring connection pool. Every query in this package is
// PARAMETERIZED ($1,$2,...) — we never string-concatenate caller input, so SQL
// injection is structurally impossible (values travel in the protocol's
// parameter slots, never in the query text). Even the dynamically-assembled
// WHERE clauses (metric history, list filters) build only the PLACEHOLDER text
// ($3, $4, ...) by index — the values are always bound parameters.
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
// pgx surfaces the database's error as a *pgconn.PgError whose .Code is the
// 5-char SQLSTATE. We map the two that carry a domain meaning; everything else
// is an opaque infrastructure error the caller logs. Centralizing the codes (not
// sprinkling magic strings through queries) makes the mapping auditable in one
// place.
const (
	// 23505 unique_violation — a duplicate key. For experiments it means the
	// (team, name) active-uniqueness was violated → ErrRepoConflict. For metrics
	// it is caught by ON CONFLICT DO NOTHING and never surfaces. For idempotency
	// inserts it signals a concurrent racer with the same key (handled per call
	// site).
	pgUniqueViolation = "23505"

	// 23503 foreign_key_violation — e.g. inserting a run under an experiment id
	// that doesn't exist, or params/metrics under a missing run. We map it to
	// ErrRepoNotFound for the referenced parent so the service reports
	// "experiment/run not found" rather than a raw DB error.
	pgForeignKeyViolation = "23503"
)

// Store owns the connection pool and hands out the repository adapters that
// implement the domain's persistence ports.
//
// WHY ONE pool shared by SEVERAL adapter types (not a pool each): experiments,
// runs, params, metrics, and idempotency keys all live in the SAME Postgres
// database (database-per-SERVICE) and share transaction boundaries (e.g.
// FinishRun's status+finals write, a batch metric INSERT). One pool is correct
// and the pool is safe for concurrent use by many goroutines (the gRPC handlers).
//
// They CANNOT be the same Go type: domain.ExperimentRepository and
// domain.RunRepository both declare methods that would clash by name/signature,
// and Go forbids two methods of the same name on one type. So Store exposes
// distinct adapter structs, each wrapping the shared pool, with accessors the
// composition root wires into NewExperimentService.
type Store struct {
	pool *pgxpool.Pool
}

// New connects to Postgres at dsn and returns a Store with a ready pool.
//
// POOL LIFECYCLE (12-factor / interview note): the pool is created here and MUST
// be released by the caller via Close() at shutdown — we do NOT hide a global.
// pgxpool.New parses the DSN (incl. pool tunables like pool_max_conns) and
// establishes the pool lazily; we Ping once so a bad DSN / unreachable DB fails
// FAST at startup (loud, with a clear error) rather than on the first request.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close() // don't leak the pool we just created
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// NewWithPool wraps an already-constructed pool. Used by tests (which build the
// pool against a testcontainer and run migrations on it first) and by a
// composition root that manages the pool itself. The Store does NOT own a pool
// passed in this way — whoever created it closes it.
func NewWithPool(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Experiments returns the adapter implementing domain.ExperimentRepository.
func (s *Store) Experiments() *ExperimentRepository { return &ExperimentRepository{pool: s.pool} }

// Runs returns the adapter implementing domain.RunRepository.
func (s *Store) Runs() *RunRepository { return &RunRepository{pool: s.pool} }

// Idempotency returns the adapter implementing domain.IdempotencyStore.
func (s *Store) Idempotency() *IdempotencyStore { return &IdempotencyStore{pool: s.pool} }

// Pool exposes the underlying pool (tests run migrations / assertions through
// it). Not part of any domain port — an adapter-local convenience.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Close releases the pool. Only call this for a Store created by New (which owns
// its pool); a Store built with NewWithPool shares a pool the caller owns.
func (s *Store) Close() { s.pool.Close() }

// ExperimentRepository is the Postgres adapter for domain.ExperimentRepository.
// It holds the shared pool; all its methods are in experiment_repository.go.
type ExperimentRepository struct{ pool *pgxpool.Pool }

// RunRepository is the Postgres adapter for domain.RunRepository — runs plus
// their params and metric time-series. Methods are in run_repository.go.
type RunRepository struct{ pool *pgxpool.Pool }

// IdempotencyStore is the Postgres adapter for domain.IdempotencyStore — the
// (operation, key) → result_id dedup table. Methods are in idempotency_store.go.
type IdempotencyStore struct{ pool *pgxpool.Pool }

// ============================================================================
// ERROR MAPPING — pgx error → domain vocabulary
// ============================================================================

// isUniqueViolation reports whether err is a Postgres unique_violation (23505)
// and, if so, on which constraint. The constraint name lets a call site
// distinguish (e.g.) a duplicate experiment name from a duplicate idempotency
// key.
func isUniqueViolation(err error) (constraint string, ok bool) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
		return pgErr.ConstraintName, true
	}
	return "", false
}

// isForeignKeyViolation reports whether err is a Postgres foreign_key_violation
// (23503) — used to turn a dangling parent reference (run under a missing
// experiment, params under a missing run) into ErrRepoNotFound.
func isForeignKeyViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgForeignKeyViolation
}

// isNoRows reports whether err is pgx's "no rows" sentinel — the signal a
// QueryRow/Scan found nothing, which the repositories translate to
// domain.ErrRepoNotFound.
func isNoRows(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}
