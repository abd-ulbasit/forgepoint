// harness_test.go — shared integration-test scaffolding for the Postgres adapter.
//
// Every test here runs against a REAL Postgres (postgres:17) started by
// pkg/testutil.StartPostgres on the remote Docker engine — never a mock. Mocking
// a database hides the very behavior we must verify: the partial-unique
// experiment-name constraint, FK cascades, the run_metrics ON CONFLICT DO NOTHING
// dedup, transaction atomicity, JSONB round-trips, and keyset pagination under
// real ordering. testutil.SkipIfNoDocker (called inside StartPostgres) skips
// cleanly when no engine is reachable so the unit suite still runs locally.
package postgres

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/experiment-tracker/internal/domain"
)

// newTestStore starts a real Postgres container, applies the production
// migrations, and returns a Store wired to the resulting pool. The pool is closed
// automatically via t.Cleanup. Each test gets its OWN container — no shared state,
// no cross-test contamination.
func newTestStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	ctx := context.Background()

	dsn := testutil.StartPostgres(t) // SkipIfNoDocker is called inside

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	t.Cleanup(pool.Close)

	applyMigrations(t, ctx, pool)

	return NewWithPool(pool), ctx
}

// ----------------------------------------------------------------------------
// Time helper — Postgres stores TIMESTAMPTZ at microsecond precision, so we
// truncate fixtures to microseconds and use UTC. Comparing a Go time.Time that
// carries nanoseconds against a round-tripped microsecond value would otherwise
// fail spuriously. Every fixture timestamp flows through this.
// ----------------------------------------------------------------------------
func tnow() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

func newID() string { return uuid.NewString() }

// ----------------------------------------------------------------------------
// Domain builders — terse, valid fixtures so each test states only what matters.
// ----------------------------------------------------------------------------

// newExperiment builds a minimal-but-valid active Experiment for a team. The
// caller overrides name where uniqueness matters.
func newExperiment(team, name string) domain.Experiment {
	now := tnow()
	return domain.Experiment{
		ID:          newID(),
		Name:        name,
		Description: "desc",
		Tags:        map[string]string{"env": "test"},
		OwnerID:     "user-" + team,
		Team:        team,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

// newRunningRun builds a RUNNING, API-source run under an experiment, with the
// given params. final_metrics empty, artifacts nil, ended_at nil — exactly what
// StartRun produces.
func newRunningRun(experimentID string, params ...domain.Param) domain.Run {
	return domain.Run{
		ID:             newID(),
		ExperimentID:   experimentID,
		DisplayName:    "run-" + uuid.NewString()[:8],
		Status:         domain.RunStatusRunning,
		Source:         domain.RunSourceAPI,
		ModelVersionID: "",
		OwnerID:        "user-x",
		Params:         params,
		StartedAt:      tnow(),
	}
}

// mp builds a server-stamped MetricPoint (ts truncated to micros for round-trip
// equality). The service stamps ts before AppendMetrics; tests do the same.
func mp(key string, step int64, value float64) domain.MetricPoint {
	return domain.MetricPoint{Key: key, Step: step, Value: value, Timestamp: tnow()}
}

// mustCreateExperiment persists an experiment and fails the test on error.
func mustCreateExperiment(t *testing.T, ctx context.Context, repo *ExperimentRepository, e domain.Experiment) domain.Experiment {
	t.Helper()
	created, err := repo.Create(ctx, e)
	if err != nil {
		t.Fatalf("create experiment: %v", err)
	}
	return created
}

// mustCreateRun persists a run and fails the test on error.
func mustCreateRun(t *testing.T, ctx context.Context, repo *RunRepository, r domain.Run) domain.Run {
	t.Helper()
	created, err := repo.CreateRun(ctx, r)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	return created
}

// seedExperimentAndRun creates an experiment + a RUNNING run and returns both —
// the common precondition for the run/metric/param tests.
func seedExperimentAndRun(t *testing.T, ctx context.Context, s *Store, team string) (domain.Experiment, domain.Run) {
	t.Helper()
	exp := mustCreateExperiment(t, ctx, s.Experiments(), newExperiment(team, "exp-"+uuid.NewString()[:8]))
	run := mustCreateRun(t, ctx, s.Runs(), newRunningRun(exp.ID))
	return exp, run
}

func ptr[T any](v T) *T { return &v }

// contextBG is a tiny alias so helper builders that don't take a ctx can still
// issue queries with a background context. Kept local to the test package.
func contextBG() context.Context { return context.Background() }

// itoa formats an int64 for building map keys in pagination/dedup assertions.
func itoa(n int64) string { return strconv.FormatInt(n, 10) }
