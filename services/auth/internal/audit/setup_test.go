// setup_test.go — shared testcontainers bootstrap for the audit sink integration
// tests. Real Postgres (for the append-only trigger + hash chain) and, in the
// end-to-end test, real NATS (for publish→consume→persist). Mocking either would
// test the mock, not the SQL trigger / the JetStream delivery semantics that are
// the entire point of these tests.
package audit

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

// newTestPool starts a Postgres container, applies ALL up-migrations (so the
// audit_log table, its indexes, and the append-only trigger exist), and returns a
// ready pool. Each test gets an isolated database via t.Cleanup teardown.
func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	testutil.SkipIfNoDocker(t)

	dsn := testutil.StartPostgres(t)

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
	return pool
}

// applyMigrations reads every *.up.sql in services/auth/migrations (version order)
// and execs it — the SAME files the deploy applies, so the schema under test is
// byte-for-byte the shipped schema (including 002_audit_log's trigger).
func applyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	dir := migrationsDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var upFiles []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), ".up.sql") {
			upFiles = append(upFiles, e.Name())
		}
	}
	sort.Strings(upFiles) // NNN_ prefix → lexical sort == version sort
	for _, name := range upFiles {
		sqlBytes, rerr := os.ReadFile(filepath.Join(dir, name))
		if rerr != nil {
			return rerr
		}
		if _, eerr := pool.Exec(ctx, string(sqlBytes)); eerr != nil {
			return eerr
		}
	}
	return nil
}

// migrationsDir resolves services/auth/migrations relative to THIS file.
// thisFile = .../services/auth/internal/audit/setup_test.go → up two dirs to
// services/auth, then into migrations.
func migrationsDir() string {
	_, thisFile, _, _ := runtime.Caller(0)
	pkgDir := filepath.Dir(thisFile)             // .../internal/audit
	authDir := filepath.Join(pkgDir, "..", "..") // .../services/auth
	return filepath.Clean(filepath.Join(authDir, "migrations"))
}
