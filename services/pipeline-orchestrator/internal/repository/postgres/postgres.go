// Package postgres is the PERSISTENCE ADAPTER for the Pipeline Orchestrator's
// domain ports. It implements domain.PipelineRepository and
// domain.ExecutionRepository against a real PostgreSQL database using pgx/v5.
//
// ============================================================================
// HEXAGONAL: THIS IS AN ADAPTER, NOT THE DOMAIN
// ============================================================================
//
// The domain (services/.../internal/domain) declares the PORTS it needs
// (PipelineRepository, ExecutionRepository) and stays pure — stdlib + uuid only.
// This package is the outward ADAPTER that fulfills those ports with SQL. The
// arrow points inward: postgres → domain (we import the domain for its types and
// sentinels; the domain never imports us). Swapping Postgres for another store
// means writing a new adapter against the SAME ports — the engine code is
// untouched. That is the entire payoff of the dependency rule.
//
// WHY pgx/v5 (+ pgxpool) and not database/sql: pgx is the de-facto high-perf
// Postgres driver for Go. Its native interface (vs the database/sql shim) gives
// us: first-class JSONB via the codec (we hand it []byte / json.RawMessage),
// structured error codes (*pgconn.PgError with SQLSTATE — how we map
// unique_violation → ErrAlreadyExists), and a context-honoring connection pool.
// Every query below is PARAMETERIZED ($1,$2,...) — we never string-concatenate
// caller input, so SQL injection is structurally impossible (the values travel
// in the protocol's parameter slots, never in the query text).
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
// 5-char SQLSTATE. We map the two that have a domain meaning; everything else is
// an opaque infrastructure error the caller logs. Centralizing the codes (not
// sprinkling magic strings) makes the mapping auditable in one place.
const (
	// 23505 unique_violation — a duplicate key. For us this is a races-with-another
	// writer / repeat-without-idempotency-key situation. We translate it per call
	// site (the idempotency tables handle the *intended* dedup; a bare 23505 on the
	// PK means a genuine duplicate id, which is a programmer/UUID-collision bug).
	pgUniqueViolation = "23505"

	// 23503 foreign_key_violation — e.g. creating an execution for a pipeline id
	// that doesn't exist. We map it to ErrRepoNotFound for the referenced parent so
	// the service reports "pipeline not found" rather than a raw DB error.
	pgForeignKeyViolation = "23503"
)

// Store owns the connection pool and hands out the two repository adapters that
// implement the domain's persistence ports.
//
// WHY ONE pool shared by TWO repository types (not a pool each, and not one giant
// type): both repositories (pipelines, executions) live in the SAME Postgres
// database (database-per-SERVICE) and share a transaction boundary when a trigger
// writes an execution + its step checkpoints atomically — so one pool is right.
// But they CANNOT be the same Go type: domain.PipelineRepository and
// domain.ExecutionRepository BOTH declare Create/GetByID/List with DIFFERENT
// signatures, and Go forbids two methods of the same name on one type. So Store
// exposes two distinct adapter structs (PipelineRepository, ExecutionRepository),
// each wrapping the shared pool. Pipelines() / Executions() are the accessors the
// composition root wires into NewPipelineService. The pool is safe for concurrent
// use by many goroutines (the gRPC handlers).
type Store struct {
	pool *pgxpool.Pool
}

// New connects to Postgres at dsn and returns a Store with a ready pool.
//
// POOL LIFECYCLE (12-factor): the pool is created here and MUST
// be released by the caller via Close() at shutdown — we do NOT hide a global.
// pgxpool.New parses the DSN (incl. pool tunables like pool_max_conns) and
// establishes the pool lazily; we Ping once so a bad DSN / unreachable DB fails
// FAST at startup (loud, with a clear error) rather than on the first request.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: create pool: %w", err)
	}
	// Fail fast on an unreachable DB so a mis-wired composition root surfaces at
	// boot, not under first traffic. Ping borrows a conn, round-trips, returns it.
	if err := pool.Ping(ctx); err != nil {
		pool.Close() // don't leak the pool we just created
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// NewWithPool wraps an already-constructed pool. Used by tests (which build the
// pool against a testcontainer and want to run migrations on it first) and by a
// composition root that manages the pool itself. The Store does NOT own a pool
// passed in this way — whoever created it closes it.
func NewWithPool(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Pipelines returns the adapter implementing domain.PipelineRepository.
func (s *Store) Pipelines() *PipelineRepository { return &PipelineRepository{pool: s.pool} }

// Executions returns the adapter implementing domain.ExecutionRepository.
func (s *Store) Executions() *ExecutionRepository { return &ExecutionRepository{pool: s.pool} }

// Pool exposes the underlying pool (tests run migrations / assertions through
// it). Not part of any domain port — an adapter-local convenience.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Close releases the pool. Idempotent-safe to call once at shutdown. Only call
// this for a Store created by New (which owns its pool); a Store built with
// NewWithPool shares a pool the caller owns.
func (s *Store) Close() { s.pool.Close() }

// PipelineRepository is the Postgres adapter for domain.PipelineRepository — the
// TEMPLATE store. It holds the shared pool; all its methods are in
// pipeline_repository.go.
type PipelineRepository struct{ pool *pgxpool.Pool }

// ExecutionRepository is the Postgres adapter for domain.ExecutionRepository —
// the RUN/saga-state store. It holds the shared pool; all its methods are in
// execution_repository.go.
type ExecutionRepository struct{ pool *pgxpool.Pool }

// ============================================================================
// ERROR MAPPING — pgx error → domain vocabulary
// ============================================================================

// isUniqueViolation reports whether err is a Postgres unique_violation (23505)
// and, if so, on which constraint. The constraint name lets a call site
// distinguish (e.g.) a duplicate idempotency key from a duplicate template name.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation
}

// isForeignKeyViolation reports whether err is a Postgres foreign_key_violation
// (23503) — used to turn a dangling parent reference into ErrRepoNotFound.
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
