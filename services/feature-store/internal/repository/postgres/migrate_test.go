// migrate_test.go — applies the service's golang-migrate .up.sql files in tests by
// reading them and executing via pgx, WITHOUT importing the golang-migrate Go
// library.
//
// ============================================================================
// WHY READ-AND-EXEC INSTEAD OF importing github.com/golang-migrate/migrate
// ============================================================================
//
// golang-migrate's database drivers pull a dependency tree (lib/pq, file source,
// etc.) that conflicts with this workspace's pinned, -mod=readonly module set and
// would require a go.mod change we are not permitted to make here. The migration
// FILES are the source of truth either way; in production the migrate CLI / an
// initContainer applies them. In tests we only need the SCHEMA to exist, so we read
// each NNN_name.up.sql in lexical order and Exec it. This keeps tests dependency-
// light and still exercises the REAL DDL the deploy pipeline runs (same files, no
// drift between "what tests use" and "what prod uses").
//
// The whole-file Exec works because pgx's simple-protocol Exec accepts multiple
// statements separated by ';' in one call, which is exactly how a .up.sql reads.
package postgres

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// migrationsDir is the path from this test package to the service's migrations,
// resolved relative to this file's directory. internal/repository/postgres ->
// ../../../migrations.
const migrationsDir = "../../../migrations"

// applyMigrations connects to dsn, applies every *.up.sql in lexical (numeric)
// order, and returns a ready pool. It t.Fatals on any failure — a test that cannot
// build its schema cannot meaningfully proceed.
func applyMigrations(t *testing.T, ctx context.Context, dsn string) *pgxpool.Pool {
	t.Helper()

	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		t.Fatalf("read migrations dir: %v", err)
	}
	var upFiles []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".up.sql") {
			upFiles = append(upFiles, e.Name())
		}
	}
	// Lexical sort = numeric order given the NNN_ prefix (001, 002, ...). This is the
	// same ordering golang-migrate applies.
	sort.Strings(upFiles)
	if len(upFiles) == 0 {
		t.Fatalf("no .up.sql migrations found in %s", migrationsDir)
	}

	// Apply DDL on a single connection (not the pool) so all statements run on one
	// session, matching how a migration tool executes a file.
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect for migration: %v", err)
	}
	for _, name := range upFiles {
		sqlBytes, rerr := os.ReadFile(filepath.Join(migrationsDir, name))
		if rerr != nil {
			_ = conn.Close(ctx)
			t.Fatalf("read migration %s: %v", name, rerr)
		}
		if _, eerr := conn.Exec(ctx, string(sqlBytes)); eerr != nil {
			_ = conn.Close(ctx)
			t.Fatalf("apply migration %s: %v", name, eerr)
		}
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("close migration conn: %v", err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}
