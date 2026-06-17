// harness_test.go — shared integration-test scaffolding for the Postgres adapter.
//
// Every test here runs against a REAL Postgres (postgres:17) started by
// pkg/testutil.StartPostgres on the remote Docker engine — never a mock. Mocking a
// database hides the very behavior we must verify: the (owner_team, model_name)
// partial-unique constraint, the window_id idempotency conflict, the FK from
// drift_reports → monitors, soft-delete semantics, JSONB round-trips of thresholds /
// metrics, and keyset pagination under real ordering. testutil.SkipIfNoDocker (called
// inside StartPostgres) skips cleanly when no engine is reachable so the unit suite
// still runs locally.
package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/domain"
)

// newTestStore starts a real Postgres container, applies the production migrations,
// and returns a Store wired to the resulting pool. The pool is closed automatically
// via t.Cleanup. Each test gets its OWN container — no shared state, no cross-test
// contamination.
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
// Time helper — Postgres stores TIMESTAMPTZ at microsecond precision, so we truncate
// fixtures to microseconds and use UTC. Comparing a Go time.Time that carries
// nanoseconds against a round-tripped microsecond value would otherwise fail
// spuriously. Every fixture timestamp flows through this.
// ----------------------------------------------------------------------------
func tnow() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

func newID() string { return uuid.NewString() }

// ----------------------------------------------------------------------------
// Domain builders — terse, valid fixtures so each test states only what matters.
// ----------------------------------------------------------------------------

// newMonitor builds a minimal-but-valid ACTIVE monitor for a team+model, with one
// data-drift threshold and a window shape. The caller overrides fields where the
// behavior under test demands it (auto-retrain, state, baseline).
func newMonitor(team, model string) domain.Monitor {
	now := tnow()
	return domain.Monitor{
		ID:             newID(),
		ModelName:      model,
		OwnerTeam:      team,
		WindowDuration: 5 * time.Minute,
		WindowSize:     1000,
		MinSamples:     100,
		Thresholds: []domain.ThresholdConfig{
			{
				DriftType:     domain.DriftTypeData,
				Method:        domain.DriftMethodPSI,
				WarnScore:     0.1,
				CriticalScore: 0.3,
			},
			{
				DriftType:     domain.DriftTypePrediction,
				Method:        domain.DriftMethodKL,
				WarnScore:     0.05,
				CriticalScore: 0.2,
			},
		},
		AutoRetrain:        false,
		RetrainPipelineID:  "",
		State:              domain.MonitorStateActive,
		BaselineVersion:    "v1.2.0",
		BaselineCapturedAt: now.Add(-24 * time.Hour),
		CreatedAt:          now,
		UpdatedAt:          now,
	}
}

// newReport builds a valid CRITICAL data-drift report for a monitor. The caller
// overrides WindowID where idempotency is under test and window_end where ordering /
// pagination is under test.
func newReport(m domain.Monitor) domain.DriftReport {
	now := tnow()
	return domain.DriftReport{
		ID:           newID(),
		MonitorID:    m.ID,
		OwnerTeam:    m.OwnerTeam,
		ModelName:    m.ModelName,
		ModelVersion: "v1.2.0",
		DriftType:    domain.DriftTypeData,
		Severity:     domain.DriftSeverityCritical,
		Metrics: []domain.DriftMetric{
			{
				Name:          "income",
				Method:        domain.DriftMethodPSI,
				Score:         0.41,
				BaselineValue: 5.1,
				CurrentValue:  7.8,
				Severity:      domain.DriftSeverityCritical,
			},
			{
				Name:          "age",
				Method:        domain.DriftMethodPSI,
				Score:         0.02,
				BaselineValue: 40.0,
				CurrentValue:  40.3,
				Severity:      domain.DriftSeverityOK,
			},
		},
		WindowID:    newID(),
		SampleCount: 850,
		WindowStart: now.Add(-5 * time.Minute),
		WindowEnd:   now,
		CreatedAt:   now,
	}
}

// mustSaveMonitor persists a monitor via the concrete adapter and fails the test on
// error, returning the stored row and the created flag.
func mustSaveMonitor(t *testing.T, ctx context.Context, repo *MonitorRepository, m domain.Monitor) (domain.Monitor, bool) {
	t.Helper()
	stored, created, err := repo.Upsert(ctx, m)
	if err != nil {
		t.Fatalf("upsert monitor: %v", err)
	}
	return stored, created
}

// mustSaveReport persists a report and fails the test on error.
func mustSaveReport(t *testing.T, ctx context.Context, repo *DriftReportRepository, rep domain.DriftReport) (domain.DriftReport, bool) {
	t.Helper()
	stored, inserted, err := repo.Save(ctx, rep)
	if err != nil {
		t.Fatalf("save report: %v", err)
	}
	return stored, inserted
}
