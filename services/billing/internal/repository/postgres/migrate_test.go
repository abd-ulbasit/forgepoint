// migrate_test.go — TEST-ONLY migration applier.
//
// ============================================================================
// WHY WE APPLY MIGRATIONS BY READING .up.sql DIRECTLY (not golang-migrate's lib)
// ============================================================================
//
// The migrate CLI / an initContainer runs migrations at DEPLOY time (a separate
// operational concern). In TESTS we apply the SAME .up.sql files ourselves via the
// migrations package's embedded files (migrations.ApplyUp). WHY not import
// github.com/golang-migrate/migrate's Go library here: its source/database DRIVERS
// pull a transitive dependency tree that breaks the module under -mod=readonly in
// this workspace. The embedded-file applier needs nothing beyond pgx (already
// required) — zero new deps — and it exercises the EXACT SQL that ships, so the
// schema under test is the production schema, file-for-file.
// ============================================================================
package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abd-ulbasit/forgepoint/services/billing/migrations"
)

// applyMigrations runs every embedded .up.sql (in lexical = dependency order)
// against the pool. Called by newTestStore after starting the container, so each
// test runs against a freshly-migrated, production-shaped schema. A failure fails
// the test loudly — a bad migration must never silently yield a half-built schema
// the assertions then misread.
func applyMigrations(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if err := migrations.ApplyUp(ctx, pool); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
}
