// apikey_repository.go — Postgres adapter for domain.APIKeyRepository.
//
// Implements: Create, GetByKeyHash, Revoke, ListByUser.
//
// ============================================================================
// SECURITY INVARIANT THIS ADAPTER UPHOLDS: never store the raw key
// ============================================================================
// The domain hashes the raw key (SHA-256 → hex) BEFORE calling Create; this
// adapter only ever sees and persists KeyHash + KeyPrefix. The raw key never
// reaches Postgres or any log line here. GetByKeyHash looks the key up by that
// hash on every API-key-authenticated request. A database breach therefore leaks
// only hashes, which are useless without the corresponding raw key (the Stripe /
// GitHub / Vault model, design D3).
package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/auth/internal/domain"
)

// APIKeyRepo is the Postgres-backed implementation of domain.APIKeyRepository.
type APIKeyRepo struct {
	db *DB
}

var _ domain.APIKeyRepository = (*APIKeyRepo)(nil)

// NewAPIKeyRepo wires the shared pool into an APIKeyRepository adapter.
func NewAPIKeyRepo(db *DB) *APIKeyRepo {
	return &APIKeyRepo{db: db}
}

// Create persists a new API key whose KeyHash/KeyPrefix the domain already
// computed. Returns the row with its DB-generated id and created_at.
//
// SCOPES BINDING: domain.APIKey.Scopes is a []string; we bind it straight to the
// TEXT[] column. pgx encodes a Go []string into a Postgres array natively (no
// manual "{a,b}" string building, which would be both ugly and an injection risk
// if done by hand). A nil slice binds as an empty array, matching the column's
// DEFAULT '{}'.
//
// We do NOT bind RevokedAt on insert: a newly created key is active by
// definition, so revoked_at stays NULL (the column default).
func (r *APIKeyRepo) Create(ctx context.Context, key domain.APIKey) (domain.APIKey, error) {
	const q = `
		INSERT INTO api_keys (user_id, key_hash, key_prefix, scopes, expires_at)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, created_at`

	// Normalize a nil scopes slice to a non-nil empty slice so the bound array is
	// '{}' rather than NULL — the column is NOT NULL, and the domain treats "no
	// scopes" as an empty list, not NULL.
	scopes := key.Scopes
	if scopes == nil {
		scopes = []string{}
	}

	row := r.db.pool.QueryRow(ctx, q,
		key.UserID, key.KeyHash, key.KeyPrefix, scopes, key.ExpiresAt)

	if err := row.Scan(&key.ID, &key.CreatedAt); err != nil {
		// A foreign-key violation here means the user_id does not exist — the
		// domain shouldn't reach this (it creates keys for authenticated users),
		// but we translate it to ErrRepoNotFound rather than leak a raw pg error.
		if isPgErrCode(err, pgErrForeignKeyViolation) {
			return domain.APIKey{}, domain.ErrRepoNotFound
		}
		// A unique violation means a duplicate key_hash — astronomically unlikely
		// (SHA-256 collision on 256-bit input), but we surface it rather than
		// silently swallow a genuine integrity error.
		return domain.APIKey{}, fmt.Errorf("inserting api key: %w", err)
	}
	return key, nil
}

// apiKeyColumns is the shared SELECT list so GetByKeyHash and ListByUser scan
// identically via scanAPIKey.
const apiKeyColumns = `
	SELECT id, user_id, key_hash, key_prefix, scopes, expires_at, revoked_at, created_at
	FROM api_keys`

// GetByKeyHash looks up a key by its SHA-256 hash — the validate-token hot path.
// Returns ErrRepoNotFound if absent. Note we return the row REGARDLESS of whether
// it is revoked/expired: the domain's APIKey.IsValid(now) makes that decision, so
// the storage layer stays a dumb, honest mirror of the row. (Filtering revoked
// keys here would hide them from the audit-oriented ListByUser logic and split
// the validity rule across two layers.)
func (r *APIKeyRepo) GetByKeyHash(ctx context.Context, keyHash string) (domain.APIKey, error) {
	row := r.db.pool.QueryRow(ctx, apiKeyColumns+` WHERE key_hash = $1`, keyHash)
	return scanAPIKey(row)
}

// Revoke soft-deletes a key by stamping revoked_at = now. SOFT delete (not DELETE)
// preserves the audit trail: we can still answer "when was this key revoked and
// who owned it?". The very next GetByKeyHash returns the row with a non-nil
// RevokedAt, and the domain's IsValid rejects it — revocation is effective
// immediately, with no cache to wait out (the API-key counterpart to JWT's
// eventual-revocation tradeoff).
//
// IDEMPOTENCY / NOT-FOUND: we check the command tag's RowsAffected. Zero rows
// affected means no key with that id exists → ErrRepoNotFound, so a caller
// revoking a phantom id gets a precise error rather than a silent success.
// Revoking an ALREADY-revoked key simply re-stamps revoked_at (still 1 row
// affected) — harmless and idempotent in effect.
func (r *APIKeyRepo) Revoke(ctx context.Context, keyID string, now time.Time) error {
	const q = `UPDATE api_keys SET revoked_at = $2 WHERE id = $1`

	tag, err := r.db.pool.Exec(ctx, q, keyID, now)
	if err != nil {
		return fmt.Errorf("revoking api key: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrRepoNotFound
	}
	return nil
}

// ListByUser returns ALL of a user's keys (active AND revoked) newest-first for
// the "my API keys" UI. We include revoked keys deliberately: the UI shows a key's
// status (active/revoked/expired), which requires returning the revoked ones so
// the user sees a complete history.
func (r *APIKeyRepo) ListByUser(ctx context.Context, userID string) ([]domain.APIKey, error) {
	const q = apiKeyColumns + ` WHERE user_id = $1 ORDER BY created_at DESC, id DESC`

	rows, err := r.db.pool.Query(ctx, q, userID)
	if err != nil {
		return nil, fmt.Errorf("querying api keys: %w", err)
	}
	defer rows.Close()

	keys := make([]domain.APIKey, 0)
	for rows.Next() {
		k, serr := scanAPIKey(rows)
		if serr != nil {
			return nil, fmt.Errorf("scanning api key row: %w", serr)
		}
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating api keys: %w", err)
	}
	return keys, nil
}

// scanAPIKey maps one api_keys row into a domain.APIKey.
//
// NULLABLE TIMESTAMPS: expires_at and revoked_at are nullable in the schema and
// pointer-typed in the domain (*time.Time, where nil = "no expiry" / "active").
// pgx scans a SQL NULL into a nil *time.Time and a present value into a non-nil
// one — so the domain's IsValid pointer checks (k.RevokedAt != nil, etc.) work
// directly off what we scan, with no nil/zero-time ambiguity.
//
// SCOPES: the TEXT[] column scans straight into a []string. pgx handles the
// array decode; an empty array becomes an empty (non-nil) slice.
func scanAPIKey(s rowScanner) (domain.APIKey, error) {
	var k domain.APIKey
	err := s.Scan(
		&k.ID, &k.UserID, &k.KeyHash, &k.KeyPrefix,
		&k.Scopes, &k.ExpiresAt, &k.RevokedAt, &k.CreatedAt,
	)
	if err != nil {
		if noRows(err) {
			return domain.APIKey{}, domain.ErrRepoNotFound
		}
		return domain.APIKey{}, fmt.Errorf("scanning api key: %w", err)
	}
	return k, nil
}
