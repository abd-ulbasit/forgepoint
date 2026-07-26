// Package postgres is the PERSISTENCE ADAPTER for the billing service's domain
// ports. It implements domain.UsageStore, domain.RatePlanStore, and
// domain.InvoiceStore against a real PostgreSQL database using pgx/v5.
//
// ============================================================================
// HEXAGONAL: THIS IS AN ADAPTER, NOT THE DOMAIN
// ============================================================================
//
// The domain (internal/domain) declares the PORTS it needs (UsageStore,
// RatePlanStore, InvoiceStore, IDProvider, Clock — see ports.go) and stays pure
// (stdlib + uuid only). This package is the outward ADAPTER that fulfills those
// ports with SQL. The dependency arrow points INWARD: postgres → domain (we
// import the domain for its types and storage sentinels; the domain never imports
// us). Swapping Postgres for another store means writing a new adapter against
// the SAME ports — the billing engine code is untouched. That is the whole payoff
// of the dependency rule.
//
// WHY pgx/v5 (+ pgxpool) and not database/sql: pgx is the de-facto high-perf
// Postgres driver for Go. Its native interface gives us first-class JSONB (we
// hand it []byte), structured error codes (*pgconn.PgError with SQLSTATE — how we
// map unique_violation → ErrRepoDuplicate), and a context-honoring pool. EVERY
// query below is PARAMETERIZED ($1,$2,...) — caller input never touches the query
// TEXT, it travels in the protocol's parameter slots, so SQL injection is
// structurally impossible even where we assemble an optional WHERE clause.
//
// ============================================================================
// THE OUTBOX BOUNDARY LIVES IN THIS ADAPTER (the centerpiece)
// ============================================================================
//
// The domain's UsageStore.RecordUsageTx / InvoiceStore.SaveInvoiceTx are CONTRACT
// methods whose meaning is "commit the business row AND the outbox event(s) in ONE
// transaction". The domain states the requirement; THIS adapter supplies the
// MECHANISM (pgx.BeginFunc → BEGIN/COMMIT/ROLLBACK). The business row INSERT and
// the outbox INSERT(s) share the tx, so either both durable or neither — no dual-
// write gap between Postgres and NATS. See usage_store.go / invoice_store.go.
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
// 5-char SQLSTATE. We map the ones that carry a domain meaning; everything else
// stays an opaque infrastructure error the caller logs. Centralizing the codes
// (not sprinkling magic strings through queries) makes the mapping auditable.
const (
	// 23505 unique_violation — a duplicate key. For billing this is the
	// idempotency race: two concurrent RecordUsage / CreateRatePlan with the same
	// key; one wins the INSERT, the other gets this and re-reads the winner. We
	// translate it to domain.ErrRepoDuplicate at the call site.
	pgUniqueViolation = "23505"
)

// Store owns the connection pool and hands out the three repository adapters that
// implement the billing domain's persistence ports.
//
// WHY ONE pool shared by THREE adapter types (not a pool each, not one giant
// type): all three repositories live in the SAME Postgres database
// (database-per-SERVICE) and the outbox tx must write a business row and an outbox
// row through the SAME pool/tx — so one shared pool is correct. They CANNOT be the
// same Go type: domain.UsageStore, RatePlanStore and InvoiceStore would collide on
// method names of different signatures, and Go forbids two methods of the same name
// on one type. So Store exposes three distinct adapter structs, each wrapping the
// shared pool. The pool is safe for concurrent use by many goroutines (the handlers).
type Store struct {
	pool *pgxpool.Pool
}

// New connects to Postgres at dsn and returns a Store with a ready pool.
//
// POOL LIFECYCLE (12-factor): the pool is created here and MUST
// be released by the caller via Close() at shutdown — we do NOT hide a global.
// pgxpool.New parses the DSN (incl. pool tunables) and establishes the pool
// lazily; we Ping once so a bad DSN / unreachable DB fails FAST at startup (loud,
// with a clear error) rather than on the first request.
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
// composition root that manages the pool itself. A Store built this way does NOT
// own the pool — whoever created it closes it.
func NewWithPool(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Usage returns the adapter implementing domain.UsageStore (the ledger + outbox
// transactional boundary).
func (s *Store) Usage() *UsageStore { return &UsageStore{pool: s.pool} }

// RatePlans returns the adapter implementing domain.RatePlanStore (pricing +
// team→plan resolution).
func (s *Store) RatePlans() *RatePlanStore { return &RatePlanStore{pool: s.pool} }

// Invoices returns the adapter implementing domain.InvoiceStore (invoices +
// aggregation + outbox boundary).
func (s *Store) Invoices() *InvoiceStore { return &InvoiceStore{pool: s.pool} }

// Pool exposes the underlying pool (tests run migrations / ground-truth
// assertions through it). Not part of any domain port — an adapter-local
// convenience.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Close releases the pool. Call only for a Store created by New (which owns its
// pool); a Store built with NewWithPool shares a pool the caller owns.
func (s *Store) Close() { s.pool.Close() }

// UsageStore is the Postgres adapter for domain.UsageStore — the append-only
// ledger AND the outbox transactional boundary for usage events.
type UsageStore struct{ pool *pgxpool.Pool }

// RatePlanStore is the Postgres adapter for domain.RatePlanStore — the
// server-authoritative pricing catalog + team→plan resolution.
type RatePlanStore struct{ pool *pgxpool.Pool }

// InvoiceStore is the Postgres adapter for domain.InvoiceStore — period invoices,
// the per-meter aggregation (GROUP BY), and the invoice outbox boundary.
type InvoiceStore struct{ pool *pgxpool.Pool }

// ============================================================================
// ERROR MAPPING — pgx error → domain vocabulary
// ============================================================================

// isUniqueViolation reports whether err is a Postgres unique_violation (23505).
// It does NOT return the constraint name: every call site here has exactly one
// unique index in play on the statement it guards, so the code alone is enough.
// A call site that needs to tell two constraints apart should match on
// pgErr.ConstraintName directly rather than widen this helper.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation
}

// isNoRows reports whether err is pgx's "no rows" sentinel — the signal a
// QueryRow/Scan found nothing, which the repositories translate to
// domain.ErrRepoNotFound.
func isNoRows(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}
