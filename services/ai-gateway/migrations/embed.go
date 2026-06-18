// Package migrations embeds the AI Gateway's prompt-registry SQL migrations so
// tests (and, if wanted, the binary) can read them without a filesystem path
// dependency.
//
// ============================================================================
// WHY embed.FS RATHER THAN THE golang-migrate GO LIBRARY (mirrors the Registry)
// ============================================================================
//
// The deploy-time migration runner is the golang-migrate CLI / an initContainer —
// a SEPARATE concern that runs the .up.sql files against the real database before
// the service starts (see deploy/helm/fp-ai-gateway). That is the production path.
//
// For TESTS, importing the golang-migrate *Go library* drags in its source drivers
// whose transitive deps don't all resolve cleanly under this workspace's
// -mod=readonly posture. So integration tests apply migrations the simplest way:
// read the embedded .up.sql text and Exec it over the same pgx pool the adapter
// uses (ApplyUp). No extra driver, no extra dep, identical SQL to production. This
// is a byte-for-byte copy of the Registry's migrations/embed.go pattern.
//
// NOTE: the UNIT tests for the prompt registry use a FAKE in-memory repository (no
// Postgres, no testcontainers), so they never call ApplyUp. This embed exists for
// the SAME reason the Registry's does — a future integration test (real Postgres
// via testcontainers) can apply the schema with zero extra dependencies.
package migrations

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// files embeds every .sql migration in this directory into the binary. The glob is
// evaluated at COMPILE time; a missing/renamed file is a build error, not a runtime
// surprise.
//
//go:embed *.sql
var files embed.FS

// Execer is the tiny slice of pgx we need to run migrations: anything that can Exec.
// Both *pgxpool.Pool and *pgx.Conn satisfy this shape (we match pgx's real Exec
// signature returning pgconn.CommandTag), so a test may pass either.
type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// ApplyUp reads every NNN_*.up.sql file in lexical (== dependency) order and
// executes each one over the provided Execer (a pgx pool or conn). It is the TEST
// migration runner.
//
// We Exec each whole file as a single string: pgx's simple-query path (no args)
// accepts multiple ';'-separated statements in one call, so a migration file with
// several CREATE TABLE/INDEX statements runs as one round-trip — no fragile hand-
// rolled SQL splitter.
func ApplyUp(ctx context.Context, db Execer) error {
	names, err := upFileNames()
	if err != nil {
		return err
	}
	for _, name := range names {
		sqlBytes, err := files.ReadFile(name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		if _, err := db.Exec(ctx, string(sqlBytes)); err != nil {
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
	}
	return nil
}

// upFileNames returns the embedded *.up.sql filenames in lexical (== dependency) order.
func upFileNames() ([]string, error) {
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations dir: %w", err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), ".up.sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}
