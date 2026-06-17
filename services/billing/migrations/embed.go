// Package migrations embeds the billing service's SQL migration files so tests
// (and, if wanted, the binary) can read them without a filesystem path dependency.
//
// ============================================================================
// WHY embed.FS RATHER THAN THE golang-migrate GO LIBRARY (a deliberate choice)
// ============================================================================
//
// The deploy-time migration runner is the golang-migrate CLI / an initContainer —
// a SEPARATE concern that runs the .up.sql files against the real database before
// the service starts. That is the production path and it stays the production path.
//
// For TESTS, though, importing the golang-migrate *Go library* drags in its source
// drivers (file://, github://, ...) whose transitive deps don't all resolve cleanly
// under this workspace's -mod=readonly posture. So the integration tests apply
// migrations the simplest possible way: read the embedded .up.sql text and Exec it
// over the same pgx pool the adapter uses (see ApplyUp). No extra driver, no extra
// dep, identical SQL to production.
//
// Embedding (rather than os.ReadFile with a relative path) means the test does not
// care what the working directory is — `go test ./...` from the repo root and from
// the package dir both work, because the files travel INSIDE the test binary.
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
// surprise — exactly the guarantee we want for schema the adapter assumes exists.
//
//go:embed *.sql
var files embed.FS

// Execer is the tiny slice of pgx we need to run migrations: anything that can Exec.
// Both *pgxpool.Pool and *pgx.Conn satisfy this shape, so a test may pass either. We
// match pgx's real Exec signature (returning pgconn.CommandTag) so the interface is
// satisfied STRUCTURALLY — a method whose return type differs would not match.
type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// ApplyUp reads every NNN_*.up.sql file in lexical order and executes each one over
// the provided Execer (a pgx pool or conn). It is the TEST migration runner.
//
// WHY lexical order is correct: the files are named with a zero-padded numeric prefix
// (001_, 002_, ...) precisely so a plain string sort is also the dependency order.
// We sort defensively rather than trusting embed.FS's iteration order.
//
// WHY we do NOT split on ';': we Exec each whole file as a single string. pgx's
// simple-query path (used when Exec has no args) accepts multiple statements separated
// by ';' in one call, so a migration file with several CREATE TABLE/INDEX statements
// runs as one round-trip. This keeps the runner trivial and avoids a fragile hand-
// rolled SQL splitter that would choke on a ';' inside a string literal or function body.
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
