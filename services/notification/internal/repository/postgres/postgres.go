// Package postgres is the PERSISTENCE ADAPTER for the Notification service's
// Postgres-backed domain ports. It implements domain.NotificationRepository (the
// inbox read model + delivery log) and domain.PreferenceRepository (per-user
// routing rules) against a real PostgreSQL database using pgx/v5.
//
// ============================================================================
// HEXAGONAL: THIS IS AN ADAPTER, NOT THE DOMAIN
// ============================================================================
//
// The domain (services/notification/internal/domain) declares the PORTS it needs
// (ports.go) and stays pure — stdlib + uuid only. This package is the outward
// ADAPTER that fulfills those ports with SQL. The dependency arrow points inward:
// postgres → domain (we import the domain for its types and the ErrRepoNotFound
// storage sentinel; the domain never imports us). Swapping Postgres for another
// store means writing a new adapter against the SAME ports — the routing brain is
// untouched. That is the entire payoff of the dependency rule.
//
// WHY pgx/v5 (+ pgxpool) and not database/sql: pgx is the de-facto high-perf
// Postgres driver for Go. It gives us native array support (the channels
// SMALLINT[] and muted_event_patterns TEXT[] scan straight into Go slices via
// pgx's array codec), structured error codes (*pgconn.PgError with SQLSTATE — how
// we map unique_violation → ErrRepoAlreadyExists / ErrRepoNotFound), and a
// context-honoring connection pool. Every query in this package is PARAMETERIZED
// ($1,$2,...) — we never string-concatenate caller input, so SQL injection is
// structurally impossible (values travel in the protocol's parameter slots, never
// in the query text). Even the dynamically-assembled WHERE clauses (the list
// filters) build only the PLACEHOLDER text ($3, $4, ...) by index — the values are
// always bound parameters.
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
// STORAGE SENTINELS — the adapter's vocabulary for "already exists".
// ============================================================================
//
// The domain port (ports.go) defines ONE storage sentinel the repositories must
// return: ErrRepoNotFound (absent row). It has no "already exists" sentinel
// because the only port that can collide — NotificationRepository.Create on a
// duplicate (event_id, recipient) — is on the IDEMPOTENT-CONSUMER path, where the
// caller's reaction to "already created" differs from a normal not-found. We need
// a distinct signal there, so the adapter owns ErrRepoAlreadyExists (an
// adapter-local sentinel) and the consumer adapter treats it as "this event was
// already materialized; skip — not an error". Keeping it HERE (not in the domain
// port) is correct: the domain's ports never promised a Create-conflict error, so
// inventing one in the domain would be speculative; the consumer that wires this
// adapter is the one place that interprets it.
var ErrRepoAlreadyExists = errors.New("notification: repository: already exists")

// ============================================================================
// PostgreSQL SQLSTATE codes we translate to domain/adapter outcomes.
// ============================================================================
//
// pgx surfaces the database's error as a *pgconn.PgError whose .Code is the 5-char
// SQLSTATE. We map the ones that carry meaning; everything else is an opaque
// infrastructure error the caller logs. Centralizing the codes (not sprinkling
// magic strings through queries) makes the mapping auditable in one place.
const (
	// 23505 unique_violation. On notifications it means the idempotent-consumer
	// UNIQUE (event_id, recipient_user_id) was tripped by a redelivered event →
	// ErrRepoAlreadyExists. It is the only user-trippable unique index in this
	// schema, so any 23505 from Create is the dedup collision.
	pgUniqueViolation = "23505"

	// 23503 foreign_key_violation. On delivery_log it means an AppendDeliveryAttempt
	// referenced a notification_id that doesn't exist → we surface ErrRepoNotFound
	// for the missing parent (the attempt has nowhere to attach).
	pgForeignKeyViolation = "23503"
)

// Store owns the connection pool and hands out the repository adapters that
// implement the domain's Postgres-backed persistence ports.
//
// WHY ONE pool shared by SEVERAL adapter types (not a pool each): notifications,
// preferences, channel prefs, and the delivery log all live in the SAME Postgres
// database (database-per-SERVICE) and share transaction boundaries (e.g. the
// preferences Upsert replaces the aggregate root + its channel rows in one tx).
// One pool is correct and is safe for concurrent use by many goroutines (the gRPC
// handlers and the NATS consumer share it).
//
// They CANNOT be the same Go type: NotificationRepository and PreferenceRepository
// would clash if their methods lived on one struct. So Store exposes distinct
// adapter structs, each wrapping the shared pool, with accessors the composition
// root wires into the service.
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

// Notifications returns the adapter implementing domain.NotificationRepository.
func (s *Store) Notifications() *NotificationRepository {
	return &NotificationRepository{pool: s.pool}
}

// Preferences returns the adapter implementing domain.PreferenceRepository.
func (s *Store) Preferences() *PreferenceRepository {
	return &PreferenceRepository{pool: s.pool}
}

// Pool exposes the underlying pool (tests run migrations / assertions through it).
// Not part of any domain port — an adapter-local convenience.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Close releases the pool. Only call this for a Store created by New (which owns
// its pool); a Store built with NewWithPool shares a pool the caller owns.
func (s *Store) Close() { s.pool.Close() }

// NotificationRepository is the Postgres adapter for domain.NotificationRepository
// — the inbox read model plus the delivery_log. Methods are in
// notification_repository.go.
type NotificationRepository struct{ pool *pgxpool.Pool }

// PreferenceRepository is the Postgres adapter for domain.PreferenceRepository —
// the per-user routing rules (the aggregate root + its channel rows). Methods are
// in preference_repository.go.
type PreferenceRepository struct{ pool *pgxpool.Pool }

// ============================================================================
// ERROR MAPPING — pgx error → adapter/domain vocabulary
// ============================================================================

// isUniqueViolation reports whether err is a Postgres unique_violation (23505).
// The only user-trippable unique index in this schema is the idempotent-consumer
// constraint on notifications, so callers treat any true here as that collision.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation
}

// isForeignKeyViolation reports whether err is a Postgres foreign_key_violation
// (23503) — used to turn a delivery_log row that points at a missing notification
// into domain.ErrRepoNotFound.
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
