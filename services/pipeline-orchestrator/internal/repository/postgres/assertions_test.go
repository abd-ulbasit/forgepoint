// assertions_test.go — small test-only query/assertion helpers.
//
// These reach into the database directly (a privilege tests have but production
// code does not) to verify GROUND TRUTH — e.g. "is there really only one row?",
// "did the failed tx really leave no idempotency key?". Verifying the actual
// stored state (not just the method's return value) is what makes these tests
// catch a partial write or a leaked row that a return-value-only check would miss.
package postgres

import (
	"context"
	"testing"

	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/domain"
)

// countPipelines returns the number of LIVE (non-archived) pipelines for a team,
// read directly from the table. Used to assert dedup/rollback prevented a dup.
func countPipelines(t *testing.T, ctx context.Context, store *Store, team string) int {
	t.Helper()
	var n int
	err := store.Pool().QueryRow(ctx,
		`SELECT count(*) FROM pipelines WHERE team = $1 AND NOT archived`, team).Scan(&n)
	if err != nil {
		t.Fatalf("countPipelines: %v", err)
	}
	return n
}

// countSteps returns the number of step_executions rows for an execution, read
// directly. Used to assert Create's atomic multi-step insert (or its rollback).
func countSteps(t *testing.T, ctx context.Context, store *Store, executionID string) int {
	t.Helper()
	var n int
	err := store.Pool().QueryRow(ctx,
		`SELECT count(*) FROM step_executions WHERE execution_id = $1`, executionID).Scan(&n)
	if err != nil {
		t.Fatalf("countSteps: %v", err)
	}
	return n
}

// countExecutions returns the number of executions for a pipeline, read directly.
func countExecutions(t *testing.T, ctx context.Context, store *Store, pipelineID string) int {
	t.Helper()
	var n int
	err := store.Pool().QueryRow(ctx,
		`SELECT count(*) FROM executions WHERE pipeline_id = $1`, pipelineID).Scan(&n)
	if err != nil {
		t.Fatalf("countExecutions: %v", err)
	}
	return n
}

// keyExists reports whether an idempotency key row exists in the given table
// (pipeline_idempotency or execution_idempotency). Used to prove an atomic-tx
// rollback left NO orphan key.
func keyExists(t *testing.T, ctx context.Context, store *Store, table, team, key string) bool {
	t.Helper()
	// table is a fixed test-supplied constant (never user input), so interpolating
	// it here is safe; the team/key VALUES remain bound parameters.
	q := "SELECT EXISTS (SELECT 1 FROM " + table + " WHERE team = $1 AND idempotency_key = $2)"
	var exists bool
	if err := store.Pool().QueryRow(ctx, q, team, key).Scan(&exists); err != nil {
		t.Fatalf("keyExists(%s): %v", table, err)
	}
	return exists
}

// findDef returns a pointer to the StepDefinition with the given id, or nil.
func findDef(steps []domain.StepDefinition, id string) *domain.StepDefinition {
	for i := range steps {
		if steps[i].ID == id {
			return &steps[i]
		}
	}
	return nil
}

// findStepExec returns a pointer to the StepExecution with the given template
// step id within an execution, or nil.
func findStepExec(steps []domain.StepExecution, stepID string) *domain.StepExecution {
	for i := range steps {
		if steps[i].StepID == stepID {
			return &steps[i]
		}
	}
	return nil
}

// assertUnique fails the test if ids contains a duplicate — the core pagination
// guarantee (no row returned twice across pages).
func assertUnique(t *testing.T, ids []string) {
	t.Helper()
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate id across pages: %q", id)
		}
		seen[id] = struct{}{}
	}
}
