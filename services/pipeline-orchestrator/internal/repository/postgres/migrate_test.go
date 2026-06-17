// migrate_test.go — TEST-ONLY migration applier.
//
// ============================================================================
// WHY WE APPLY MIGRATIONS BY READING .up.sql DIRECTLY (not golang-migrate's lib)
// ============================================================================
//
// The migrate CLI / an initContainer runs migrations at DEPLOY time (a separate
// operational concern). In TESTS we apply the SAME .up.sql files ourselves by
// reading them off disk and Exec'ing them through pgx. WHY not import
// github.com/golang-migrate/migrate's Go library here: its source/database
// DRIVERS pull a transitive dependency tree that breaks the module under
// -mod=readonly in this workspace. Reading the file + conn.Exec needs nothing
// beyond pgx (already required) and os (stdlib) — zero new deps, and it exercises
// the EXACT SQL that ships. The schema under test is therefore the production
// schema, file-for-file.
//
// We sort the .up.sql files lexically so 001_, 002_, ... apply in order (the
// golang-migrate file-naming contract), and execute each whole file in one Exec
// (Postgres accepts multiple semicolon-separated statements per simple-query). A
// failure fails the test loudly — a bad migration must never silently yield a
// half-built schema the assertions then misread.
// ============================================================================
package postgres

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// migrationsPath resolves the service's migrations directory RELATIVE TO THIS
// SOURCE FILE via runtime.Caller, not relative to the test's working directory.
// WHY: `go test` sets CWD to the package dir, but `go test ./...` and some IDE
// runners do not — anchoring to the source file makes the path robust regardless
// of how the suite is invoked. This file lives at
// internal/repository/postgres/migrate_test.go, so the migrations are three dirs
// up (../../../migrations).
func migrationsPath(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed to locate the test source file")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "migrations")
}

// applyMigrations reads every NNN_*.up.sql file in the migrations dir (sorted)
// and executes it against the pool. Called by every integration test after
// starting the container, so each test runs against a freshly-migrated,
// production-shaped schema.
func applyMigrations(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	migrationsDir := migrationsPath(t)

	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		t.Fatalf("read migrations dir %q: %v", migrationsDir, err)
	}

	var upFiles []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		// Only the forward (.up.sql) migrations; .down.sql is for rollback.
		if strings.HasSuffix(name, ".up.sql") {
			upFiles = append(upFiles, name)
		}
	}
	// Lexical sort = numeric order given the zero-padded NNN_ prefix (001, 002…).
	sort.Strings(upFiles)
	if len(upFiles) == 0 {
		t.Fatalf("no .up.sql migrations found in %q", migrationsDir)
	}

	for _, name := range upFiles {
		sqlBytes, err := os.ReadFile(filepath.Join(migrationsDir, name))
		if err != nil {
			t.Fatalf("read migration %q: %v", name, err)
		}
		// Exec the whole file. pgx's simple-query path accepts multiple
		// statements separated by ';' in one call — exactly how the file is shaped.
		if _, err := pool.Exec(ctx, string(sqlBytes)); err != nil {
			t.Fatalf("apply migration %q: %v", name, err)
		}
	}
}
