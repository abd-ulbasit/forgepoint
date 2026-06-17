package postgres

import (
	"context"
	"testing"
)

// TestSmoke_MigrationsApply is a fast sanity check that the container starts and
// the migrations create the expected tables + seed roles. If this fails, the
// richer CRUD tests below would fail too — this isolates "is the plumbing wired"
// from "is the SQL logic right".
func TestSmoke_MigrationsApply(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	var roleCount int
	if err := db.pool.QueryRow(ctx, `SELECT count(*) FROM roles`).Scan(&roleCount); err != nil {
		t.Fatalf("counting seeded roles: %v", err)
	}
	if roleCount != 3 {
		t.Fatalf("expected 3 seeded roles, got %d", roleCount)
	}
}
