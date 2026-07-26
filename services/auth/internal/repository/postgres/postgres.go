// Package postgres is the PERSISTENCE ADAPTER for the Auth service.
//
// ============================================================================
// HEXAGONAL ARCHITECTURE — THIS IS AN ADAPTER, NOT THE DOMAIN
// ============================================================================
//
// The domain (services/auth/internal/domain) defines the PORTS it needs:
// UserRepository, APIKeyRepository, RoleRepository (see domain/ports.go). This
// package provides the Postgres-backed ADAPTERS that implement those ports. The
// dependency arrow points INWARD only — this package imports `domain`; `domain`
// never imports this package:
//
//	domain (ports + business rules)  ◄──implements──  postgres (this package)
//
// That direction is the whole point of Clean/Hexagonal Architecture: the
// business logic dictates the contract; infrastructure conforms. We could swap
// Postgres for CockroachDB or an in-memory fake by writing a new adapter, with
// ZERO changes to the domain.
//
// ============================================================================
// WHY pgx/pgxpool (not database/sql + lib/pq)
// ============================================================================
//
//   - pgx is a Postgres-NATIVE driver: it speaks the binary wire protocol, so
//     it avoids the text-encoding round-trips database/sql forces and supports
//     Postgres types (arrays, JSONB, UUID) directly without manual scanning glue.
//   - pgxpool is a concurrency-safe connection pool tuned for pgx. A *pgxpool.Pool
//     is safe for concurrent use by many goroutines (each gRPC request handler
//     borrows a connection for the duration of a query and returns it). We never
//     share a single connection across goroutines — that is the classic data race.
//   - database/sql is the generic abstraction; we don't need driver-swappability
//     here (we own the database, it's Postgres), and the native driver is faster
//     and more expressive. This mirrors what high-throughput Go shops (e.g. teams
//     at companies using Postgres heavily) standardize on.
//
// pgx vs database/sql: database/sql is a portable interface with a
// driver behind it; pgx is a Postgres-specific driver that ALSO offers a
// database/sql-compatible mode, but used natively gives binary protocol, real
// array/JSONB support, and a better pool. We use it natively.
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgreSQL SQLSTATE error codes we translate to domain sentinels. WHY match on
// the code, not the error message: messages are localized and version-dependent;
// the 5-char SQLSTATE is a stable contract (defined by the SQL standard and the
// Postgres docs). Hard-coding the string "23505" once, here, with a name, makes
// every call site read clearly.
const (
	// pgErrUniqueViolation (23505) — an INSERT/UPDATE violated a UNIQUE or
	// PRIMARY KEY constraint. We map the users.email case to ErrRepoEmailExists.
	pgErrUniqueViolation = "23505"
	// pgErrForeignKeyViolation (23503) — an INSERT/UPDATE referenced a row that
	// does not exist (e.g. an api_key for a non-existent user_id). We map it to
	// ErrRepoNotFound for the referenced entity.
	pgErrForeignKeyViolation = "23503"
)

// DB is the Postgres connection handle shared by all three repository adapters in
// this package. It wraps a *pgxpool.Pool so the adapters depend on one owned
// type, and so we control the pool's lifecycle (Connect / Close) in one place
// rather than scattering pool management across repositories.
//
// WHY one pool shared by all repos (not a pool per repo): a connection pool is a
// process-wide resource (it caps total connections to Postgres). Three pools
// would triple the connection budget for no benefit — all three repos talk to
// the SAME database. They share the pool; the pool internally hands each
// concurrent query its own connection.
type DB struct {
	pool *pgxpool.Pool
}

// Connect opens a pgx connection pool to the given DSN and verifies it with a
// Ping. WHY ping at construction: a pool created with ParseConfig+NewWithConfig
// is LAZY — it does not actually dial Postgres until the first query. Pinging
// here turns a misconfigured DSN or an unreachable database into a fast, loud
// startup failure (main.go aborts) instead of a confusing error on the first
// user request.
//
// The caller owns the returned *DB and MUST call Close() at shutdown to release
// the pooled connections back to Postgres (otherwise they linger until the
// server's idle timeout, exhausting the connection budget under churn).
func Connect(ctx context.Context, dsn string) (*DB, error) {
	// ParseConfig validates the DSN and yields a config we could further tune
	// (MaxConns, health-check period, etc.). We keep defaults here and let
	// deployment override via DSN query params — 12-factor: configuration lives
	// in the environment, not the code.
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parsing postgres dsn: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("creating postgres pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		// Close the half-open pool so we don't leak its goroutines/connections
		// when we return an error.
		pool.Close()
		return nil, fmt.Errorf("pinging postgres: %w", err)
	}

	return &DB{pool: pool}, nil
}

// NewDBFromPool wraps an already-constructed pool. This exists primarily for
// INTEGRATION TESTS, which start a testcontainer, apply migrations, and want to
// hand the resulting pool to the repositories without re-dialing. Production code
// uses Connect.
func NewDBFromPool(pool *pgxpool.Pool) *DB {
	return &DB{pool: pool}
}

// Pool exposes the underlying pool so tests (and main.go's migration runner) can
// execute raw SQL. Production repository code goes through the typed methods.
func (db *DB) Pool() *pgxpool.Pool {
	return db.pool
}

// Close releases all pooled connections. Idempotent-safe to call once at shutdown.
func (db *DB) Close() {
	db.pool.Close()
}

// isPgErrCode reports whether err is a Postgres server error with the given
// SQLSTATE code. WHY errors.As into *pgconn.PgError: pgx wraps the server's error
// in a *pgconn.PgError that carries the structured Code; we unwrap to inspect it
// rather than string-matching the human message. This is the idiomatic pgx way to
// branch on "unique violation" vs "foreign key violation" vs anything else.
func isPgErrCode(err error, code string) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == code
	}
	return false
}

// noRows reports whether err signals "query returned no rows". pgx returns
// pgx.ErrNoRows from QueryRow.Scan when the result set is empty; the repositories
// translate that into the domain's ErrRepoNotFound sentinel. We use errors.Is so
// a wrapped ErrNoRows is still recognized.
func noRows(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}
