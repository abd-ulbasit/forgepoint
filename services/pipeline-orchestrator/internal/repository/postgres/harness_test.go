// harness_test.go — shared integration-test scaffolding for the Postgres adapter.
//
// Every test here runs against a REAL Postgres (postgres:17) started by
// pkg/testutil.StartPostgres on the remote Docker engine — never a mock. Mocking
// a database hides the very behavior we must verify: unique constraints, FK
// enforcement, transaction atomicity, JSONB round-trips, and keyset pagination
// under real ordering. testutil.SkipIfNoDocker (called inside StartPostgres)
// skips cleanly when no engine is reachable so the unit suite still runs locally.
package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/domain"
)

// newTestStore starts a real Postgres container, applies the production
// migrations, and returns a Store wired to the resulting pool. The pool is
// closed automatically via t.Cleanup. Each test gets its OWN container — no
// shared state, no cross-test contamination.
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
// Domain builders — terse, valid fixtures so each test states only what matters.
// ----------------------------------------------------------------------------

// newID returns a fresh UUIDv4 string (the same shape the production IDGenerator
// emits). Tests use it for ids they don't otherwise assert on.
func newID() string { return uuid.NewString() }

// sagaPipeline builds a minimal-but-valid DeploymentSaga template for the given
// team: validate → deploy(compensation=teardown) → teardown. Enough structure to
// exercise step JSONB, depends_on edges, and a compensation pointer round-trip.
func sagaPipeline(team string) domain.PipelineDefinition {
	return domain.PipelineDefinition{
		ID:        newID(),
		Name:      "deploy-" + uuid.NewString()[:8],
		Type:      domain.PipelineTypeDeploymentSaga,
		CreatedBy: "user-" + team,
		Team:      team,
		CreatedAt: time.Now().UTC().Truncate(time.Microsecond), // pg stores microsecond precision
		Steps: []domain.StepDefinition{
			{ID: "validate", Name: "Validate", Type: domain.StepTypeValidate},
			{
				ID:                 "deploy",
				Name:               "Deploy",
				Type:               domain.StepTypeDeploy,
				DependsOn:          []string{"validate"},
				CompensationStepID: "teardown",
				Config:             map[string]any{"model_id": "m1", "version": "3", "replicas": float64(2)},
				Timeout:            30 * time.Second,
				MaxRetries:         2,
			},
			{ID: "teardown", Name: "Teardown", Type: domain.StepTypeCustom},
		},
	}
}

// pendingExecution builds a PENDING Execution for a pipeline, with one PENDING
// StepExecution per template step — exactly what the engine's
// newPendingExecution produces and what Create must persist atomically.
func pendingExecution(p domain.PipelineDefinition, triggeredBy string) domain.Execution {
	e := domain.Execution{
		ID:          newID(),
		PipelineID:  p.ID,
		Status:      domain.ExecutionStatusPending,
		TriggeredBy: triggeredBy,
		StartedAt:   time.Now().UTC().Truncate(time.Microsecond),
		Input:       map[string]any{"trigger": "manual"},
	}
	for _, def := range p.Steps {
		e.Steps = append(e.Steps, domain.StepExecution{
			ID:          newID(),
			ExecutionID: e.ID,
			StepID:      def.ID,
			StepType:    def.Type,
			Status:      domain.StepStatusPending,
		})
	}
	return e
}

// mustCreatePipeline persists a pipeline and fails the test on error — a helper
// so the many tests that need a pre-existing pipeline read cleanly.
func mustCreatePipeline(t *testing.T, ctx context.Context, repo *PipelineRepository, p domain.PipelineDefinition) domain.PipelineDefinition {
	t.Helper()
	created, err := repo.Create(ctx, p, "")
	if err != nil {
		t.Fatalf("create pipeline: %v", err)
	}
	return created
}

// ptr returns a pointer to v — for the nullable *time.Time fields in assertions.
func ptr[T any](v T) *T { return &v }
