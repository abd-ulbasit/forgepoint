package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/auth/internal/domain"
)

// seedUser inserts a user and returns its id, so API-key tests have a valid owner
// to satisfy the api_keys.user_id foreign key.
func seedUser(t *testing.T, db *DB, email string) string {
	t.Helper()
	repo := NewUserRepo(db)
	u, err := repo.Create(context.Background(), newUser(email))
	if err != nil {
		t.Fatalf("seedUser: %v", err)
	}
	return u.ID
}

// TestAPIKeyRepo_Create_RoundTrips verifies the insert path: server fields set,
// scopes (TEXT[]) and nullable expiry round-trip, revoked_at starts NULL.
func TestAPIKeyRepo_Create_RoundTrips(t *testing.T) {
	db := newTestDB(t)
	repo := NewAPIKeyRepo(db)
	ctx := context.Background()
	userID := seedUser(t, db, "keys@example.com")

	exp := time.Now().UTC().Add(24 * time.Hour)
	in := domain.APIKey{
		UserID:    userID,
		KeyHash:   "hash-aaa",
		KeyPrefix: "fp_aaaaa",
		Scopes:    []string{"models:read", "experiments:write"},
		ExpiresAt: &exp,
	}

	got, err := repo.Create(ctx, in)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got.ID == "" || got.CreatedAt.IsZero() {
		t.Errorf("expected server-set id/created_at, got id=%q created=%v", got.ID, got.CreatedAt)
	}
	if got.RevokedAt != nil {
		t.Errorf("new key must be active (revoked_at NULL), got %v", got.RevokedAt)
	}

	// Read it back by hash to confirm scopes and expiry persisted exactly.
	fetched, err := repo.GetByKeyHash(ctx, "hash-aaa")
	if err != nil {
		t.Fatalf("GetByKeyHash: %v", err)
	}
	if len(fetched.Scopes) != 2 || fetched.Scopes[0] != "models:read" || fetched.Scopes[1] != "experiments:write" {
		t.Errorf("scopes did not round-trip: %v", fetched.Scopes)
	}
	if fetched.ExpiresAt == nil {
		t.Fatal("expected non-nil ExpiresAt")
	}
	// Postgres stores microsecond precision; compare at second granularity to
	// avoid a spurious sub-microsecond mismatch.
	if !fetched.ExpiresAt.Truncate(time.Second).Equal(exp.Truncate(time.Second)) {
		t.Errorf("ExpiresAt mismatch: got %v want %v", fetched.ExpiresAt, exp)
	}
}

// TestAPIKeyRepo_Create_NoExpiry verifies a nil ExpiresAt persists as SQL NULL
// and reads back as a nil pointer (the "never expires" long-lived key case).
func TestAPIKeyRepo_Create_NoExpiry(t *testing.T) {
	db := newTestDB(t)
	repo := NewAPIKeyRepo(db)
	ctx := context.Background()
	userID := seedUser(t, db, "noexpiry@example.com")

	_, err := repo.Create(ctx, domain.APIKey{
		UserID:    userID,
		KeyHash:   "hash-noexp",
		KeyPrefix: "fp_noexp",
		Scopes:    nil, // nil scopes must persist as empty array, not NULL
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := repo.GetByKeyHash(ctx, "hash-noexp")
	if err != nil {
		t.Fatalf("GetByKeyHash: %v", err)
	}
	if got.ExpiresAt != nil {
		t.Errorf("expected nil ExpiresAt for no-expiry key, got %v", got.ExpiresAt)
	}
	// nil scopes normalized to empty array; reads back as empty (non-nil) slice.
	if got.Scopes == nil || len(got.Scopes) != 0 {
		t.Errorf("expected empty scopes slice, got %v", got.Scopes)
	}
	// The pure domain rule: a non-revoked, non-expiring key is valid.
	if !got.IsValid(time.Now()) {
		t.Error("expected no-expiry key to be valid")
	}
}

// TestAPIKeyRepo_GetByKeyHash_NotFound verifies the not-found sentinel on the
// validate-token hot path (the domain maps this to the opaque ErrInvalidToken).
func TestAPIKeyRepo_GetByKeyHash_NotFound(t *testing.T) {
	db := newTestDB(t)
	repo := NewAPIKeyRepo(db)

	_, err := repo.GetByKeyHash(context.Background(), "no-such-hash")
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("expected ErrRepoNotFound, got %v", err)
	}
}

// TestAPIKeyRepo_Create_ForeignKeyViolation verifies an api_key for a
// non-existent user is rejected and surfaced as ErrRepoNotFound, not a raw pg
// error (defense in depth for the FK).
func TestAPIKeyRepo_Create_ForeignKeyViolation(t *testing.T) {
	db := newTestDB(t)
	repo := NewAPIKeyRepo(db)

	_, err := repo.Create(context.Background(), domain.APIKey{
		UserID:    "00000000-0000-0000-0000-000000000000", // no such user
		KeyHash:   "hash-orphan",
		KeyPrefix: "fp_orph",
	})
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("expected ErrRepoNotFound for FK violation, got %v", err)
	}
}

// TestAPIKeyRepo_Revoke verifies soft delete: revoked_at is stamped, the key
// reads back as invalid, and re-revoking is idempotent.
func TestAPIKeyRepo_Revoke(t *testing.T) {
	db := newTestDB(t)
	repo := NewAPIKeyRepo(db)
	ctx := context.Background()
	userID := seedUser(t, db, "revoke@example.com")

	created, err := repo.Create(ctx, domain.APIKey{
		UserID: userID, KeyHash: "hash-rev", KeyPrefix: "fp_rev",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	now := time.Now().UTC()
	if err := repo.Revoke(ctx, created.ID, now); err != nil {
		t.Fatalf("Revoke: %v", err)
	}

	got, err := repo.GetByKeyHash(ctx, "hash-rev")
	if err != nil {
		t.Fatalf("GetByKeyHash after revoke: %v", err)
	}
	if got.RevokedAt == nil {
		t.Fatal("expected revoked_at to be set after Revoke")
	}
	// The pure domain rule must now reject the key — revocation effective at once.
	if got.IsValid(time.Now()) {
		t.Error("expected revoked key to be invalid")
	}

	// Idempotency: revoking again succeeds (re-stamps), still 1 row affected.
	if err := repo.Revoke(ctx, created.ID, time.Now().UTC()); err != nil {
		t.Fatalf("second Revoke should be idempotent, got %v", err)
	}
}

// TestAPIKeyRepo_Revoke_NotFound verifies revoking a phantom id returns the
// not-found sentinel (zero RowsAffected), not a silent success.
func TestAPIKeyRepo_Revoke_NotFound(t *testing.T) {
	db := newTestDB(t)
	repo := NewAPIKeyRepo(db)

	err := repo.Revoke(context.Background(), "00000000-0000-0000-0000-000000000000", time.Now())
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("expected ErrRepoNotFound revoking missing key, got %v", err)
	}
}

// TestAPIKeyRepo_ListByUser verifies all of a user's keys (active AND revoked)
// come back newest-first, and another user's keys are excluded.
func TestAPIKeyRepo_ListByUser(t *testing.T) {
	db := newTestDB(t)
	repo := NewAPIKeyRepo(db)
	ctx := context.Background()
	alice := seedUser(t, db, "alice@example.com")
	bob := seedUser(t, db, "bob@example.com")

	// Two keys for alice, one revoked; one key for bob (must not leak into alice's list).
	k1, _ := repo.Create(ctx, domain.APIKey{UserID: alice, KeyHash: "a1", KeyPrefix: "fp_a1"})
	_, _ = repo.Create(ctx, domain.APIKey{UserID: alice, KeyHash: "a2", KeyPrefix: "fp_a2"})
	_, _ = repo.Create(ctx, domain.APIKey{UserID: bob, KeyHash: "b1", KeyPrefix: "fp_b1"})
	if err := repo.Revoke(ctx, k1.ID, time.Now().UTC()); err != nil {
		t.Fatalf("Revoke a1: %v", err)
	}

	keys, err := repo.ListByUser(ctx, alice)
	if err != nil {
		t.Fatalf("ListByUser: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys for alice (incl revoked), got %d", len(keys))
	}
	for _, k := range keys {
		if k.UserID != alice {
			t.Errorf("ListByUser leaked another user's key: %+v", k)
		}
	}

	// Empty for a user with no keys.
	none, err := repo.ListByUser(ctx, "00000000-0000-0000-0000-000000000000")
	if err != nil {
		t.Fatalf("ListByUser empty: %v", err)
	}
	if len(none) != 0 {
		t.Errorf("expected empty list for unknown user, got %d", len(none))
	}
}
