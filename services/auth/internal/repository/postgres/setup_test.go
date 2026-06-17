// setup_test.go — shared bootstrap for the Postgres adapter integration tests.
//
// ============================================================================
// REAL POSTGRES, NOT A MOCK (why these are INTEGRATION tests)
// ============================================================================
// These tests run every repository method against a REAL Postgres 17 container
// (via pkg/testutil → testcontainers). Mocking the database would test our mock,
// not our SQL: it cannot catch a JSONB encode/decode mismatch, a citext
// case-folding surprise, an array-binding bug, a foreign-key/unique constraint we
// misdeclared, or a cursor query that skips rows. The whole VALUE of this layer's
// tests is that they exercise the actual SQL against the actual engine the
// production code will hit. The cost is ~seconds per container; the payoff is
// confidence the adapter is correct, which mocks cannot give.
//
// ============================================================================
// MIGRATIONS IN TESTS — read the .up.sql and Exec it (no golang-migrate lib)
// ============================================================================
// We do NOT import the golang-migrate Go library in tests: its database/source
// drivers pull dependencies that break `-mod=readonly` in this workspace. The
// migrate CLI / an initContainer runs migrations at DEPLOY time (a separate
// concern). For tests we read the SAME NNN_name.up.sql files the deploy uses and
// Exec them through pgx — so the schema under test is byte-for-byte the schema we
// ship. applyMigrations finds the migrations directory relative to THIS source
// file (runtime.Caller), so it works regardless of the test's working directory.
package postgres

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
)

// newTestDB starts a Postgres container, applies all up-migrations, and returns a
// *DB ready for the repositories. The container and pool are torn down via
// t.Cleanup (registered by testutil.StartPostgres and here), so each test gets an
// ISOLATED database with no shared state — tests cannot interfere with each other.
func newTestDB(t *testing.T) *DB {
	t.Helper()
	testutil.SkipIfNoDocker(t) // clean skip when the Docker engine is unreachable

	// StartPostgres spins up postgres:17 and returns a DSN pointing at the
	// container's (overridden) host/port. It registers its own t.Cleanup to
	// terminate the container.
	dsn := testutil.StartPostgres(t)

	// A short-lived context bounds the whole setup (connect + ping + migrate) so a
	// hung container surfaces as a test timeout, not an indefinite block.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connecting test pool: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := applyMigrations(ctx, pool); err != nil {
		t.Fatalf("applying migrations: %v", err)
	}

	return NewDBFromPool(pool)
}

// applyMigrations reads every *.up.sql in the service's migrations directory (in
// version order) and executes it against the pool. This mirrors what
// golang-migrate does, minus the version-tracking table — which we don't need in
// a throwaway per-test database that starts empty and is dropped at cleanup.
func applyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	dir := migrationsDir()

	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	// Collect and SORT the *.up.sql files by name. The NNN_ numeric prefix makes a
	// lexical sort equal a version sort (001 before 002 before 010), so migrations
	// apply in the same order migrate would apply them.
	var upFiles []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), ".up.sql") {
			upFiles = append(upFiles, e.Name())
		}
	}
	sort.Strings(upFiles)

	for _, name := range upFiles {
		sqlBytes, rerr := os.ReadFile(filepath.Join(dir, name))
		if rerr != nil {
			return rerr
		}
		// Exec runs the whole file as one batch. pgx's simple-protocol Exec accepts
		// multiple statements separated by ';', which is exactly what a migration
		// file is. (No parameters here — migration DDL has no user input, so there
		// is no injection surface; this is the one legitimate place we Exec raw SQL.)
		if _, eerr := pool.Exec(ctx, string(sqlBytes)); eerr != nil {
			return eerr
		}
	}
	return nil
}

// migrationsDir resolves the absolute path to services/auth/migrations relative
// to THIS test source file. runtime.Caller(0) gives this file's path; the
// migrations dir is three levels up (postgres → repository → internal → auth)
// then into migrations. Anchoring to the source file (not os.Getwd) makes the
// helper independent of which directory `go test` was invoked from.
func migrationsDir() string {
	_, thisFile, _, _ := runtime.Caller(0)
	// thisFile = .../services/auth/internal/repository/postgres/setup_test.go
	pkgDir := filepath.Dir(thisFile)                            // .../postgres
	authDir := filepath.Join(pkgDir, "..", "..", "..")          // .../services/auth
	return filepath.Clean(filepath.Join(authDir, "migrations")) // .../services/auth/migrations
}
