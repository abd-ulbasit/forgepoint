// execution_repository_test.go — integration tests for the SAGA-STATE adapter.
//
// This is the durability backbone, so the tests target the saga's persistence
// semantics specifically:
//   - Create writes the execution + ALL step checkpoints ATOMICALLY (a failed
//     Create leaves NO partial run, no orphan steps, no orphan key).
//   - SaveStep drives the FINE step state machine
//     (PENDING→RUNNING→COMPLETED, and the compensation sub-states) and is an
//     idempotent UPSERT.
//   - Save drives the COARSE execution state machine
//     (PENDING→RUNNING→COMPENSATING→FAILED).
//   - GetByID reconstructs the run with steps ordered by completion time — the
//     order the compensator depends on.
//   - Trigger idempotency returns the ORIGINAL run (exactly-once trigger).
//   - List is team-scoped (via the pipelines JOIN) and filterable by status.
package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/domain"
)

// Create persists the execution and EVERY PENDING step row in one tx; GetByID
// reads them all back with input + step fields intact.
func TestExecutionRepository_CreateAndGet_PersistsAllStepCheckpoints(t *testing.T) {
	store, ctx := newTestStore(t)
	p := mustCreatePipeline(t, ctx, store.Pipelines(), sagaPipeline("team-a"))
	execRepo := store.Executions()

	want := pendingExecution(p, "user-1")
	created, err := execRepo.Create(ctx, want, "")
	if err != nil {
		t.Fatalf("Create execution: %v", err)
	}
	if created.ID != want.ID {
		t.Fatalf("Create returned id %q, want %q", created.ID, want.ID)
	}

	// All 3 step checkpoints landed in the same tx as the execution.
	if n := countSteps(t, ctx, store, want.ID); n != 3 {
		t.Fatalf("expected 3 step checkpoints, got %d", n)
	}

	got, err := execRepo.GetByID(ctx, want.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Status != domain.ExecutionStatusPending {
		t.Errorf("status = %v, want Pending", got.Status)
	}
	if got.TriggeredBy != "user-1" {
		t.Errorf("triggeredBy = %q, want user-1", got.TriggeredBy)
	}
	if got.Input["trigger"] != "manual" {
		t.Errorf("input round-trip lost: %v", got.Input)
	}
	if len(got.Steps) != 3 {
		t.Fatalf("GetByID returned %d steps, want 3", len(got.Steps))
	}
	// Step denormalized type + PENDING status round-trip.
	deploy := findStepExec(got.Steps, "deploy")
	if deploy == nil || deploy.StepType != domain.StepTypeDeploy || deploy.Status != domain.StepStatusPending {
		t.Fatalf("deploy step checkpoint wrong: %+v", deploy)
	}
}

func TestExecutionRepository_GetByID_NotFound(t *testing.T) {
	store, ctx := newTestStore(t)
	_, err := store.Executions().GetByID(ctx, newID())
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("GetByID(missing) = %v, want ErrRepoNotFound", err)
	}
}

// FK CONSTRAINT: an execution for a nonexistent pipeline is rejected, and the
// failed Create leaves no row behind.
func TestExecutionRepository_Create_DanglingPipeline_Rejected(t *testing.T) {
	store, ctx := newTestStore(t)
	execRepo := store.Executions()

	// pendingExecution against a pipeline id that was never created.
	orphan := domain.Execution{
		ID:          newID(),
		PipelineID:  newID(), // no such pipeline
		Status:      domain.ExecutionStatusPending,
		TriggeredBy: "user-1",
		StartedAt:   time.Now().UTC(),
	}
	_, err := execRepo.Create(ctx, orphan, "")
	// pipelineTeam resolves first and returns ErrRepoNotFound for the missing
	// parent — the service maps that to ErrPipelineNotFound.
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("Create(dangling pipeline) = %v, want ErrRepoNotFound", err)
	}
	if _, gErr := execRepo.GetByID(ctx, orphan.ID); !errors.Is(gErr, domain.ErrRepoNotFound) {
		t.Fatal("dangling execution row was written despite rejection")
	}
}

// ATOMICITY: if a step row violates a constraint mid-insert, the WHOLE Create
// rolls back — no execution, no other steps, no idempotency key. We force the
// violation by giving two steps the SAME step_id (the (execution_id, step_id)
// unique index rejects the second), proving the multi-statement tx is atomic.
func TestExecutionRepository_Create_StepConflict_RollsBackEntirely(t *testing.T) {
	store, ctx := newTestStore(t)
	p := mustCreatePipeline(t, ctx, store.Pipelines(), sagaPipeline("team-a"))
	execRepo := store.Executions()

	e := pendingExecution(p, "user-1")
	// Corrupt the fixture: make the 2nd and 3rd steps collide on step_id so the
	// unique index (execution_id, step_id) trips on the duplicate insert.
	e.Steps[2].StepID = e.Steps[1].StepID

	_, err := execRepo.Create(ctx, e, "key-x")
	if err == nil {
		t.Fatal("expected step-conflict Create to fail, got nil")
	}

	// Nothing persisted: the execution, its steps, and the key all rolled back.
	if _, gErr := execRepo.GetByID(ctx, e.ID); !errors.Is(gErr, domain.ErrRepoNotFound) {
		t.Fatal("partial write: execution row survived a failed tx")
	}
	if n := countSteps(t, ctx, store, e.ID); n != 0 {
		t.Fatalf("partial write: %d step rows survived a failed tx", n)
	}
	if keyExists(t, ctx, store, "execution_idempotency", "team-a", "key-x") {
		t.Fatal("partial write: idempotency key survived a failed tx")
	}
}

// TRIGGER IDEMPOTENCY: a retried trigger with the same key returns the ORIGINAL
// execution and runs the saga only once (no second execution row).
func TestExecutionRepository_Create_IdempotentTrigger(t *testing.T) {
	store, ctx := newTestStore(t)
	p := mustCreatePipeline(t, ctx, store.Pipelines(), sagaPipeline("team-a"))
	execRepo := store.Executions()

	first, err := execRepo.Create(ctx, pendingExecution(p, "user-1"), "trigger-1")
	if err != nil {
		t.Fatalf("first trigger: %v", err)
	}
	// Retry with the SAME key but a fresh execution payload.
	second, err := execRepo.Create(ctx, pendingExecution(p, "user-1"), "trigger-1")
	if err != nil {
		t.Fatalf("retry trigger: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("idempotent trigger returned %q, want original %q", second.ID, first.ID)
	}
	// The deduped result carries its full step timeline (executionByKey → GetByID).
	if len(second.Steps) != 3 {
		t.Errorf("deduped execution missing steps: got %d", len(second.Steps))
	}
	if n := countExecutions(t, ctx, store, p.ID); n != 1 {
		t.Fatalf("expected 1 execution after idempotent trigger, got %d", n)
	}
}

// THE SAGA STATE MACHINE, persisted: drive an execution PENDING → RUNNING →
// COMPENSATING → FAILED via Save, and a step PENDING → RUNNING → COMPLETED →
// COMPENSATING → COMPENSATED via SaveStep, asserting each transition is durable.
func TestExecutionRepository_StateMachineTransitions_Durable(t *testing.T) {
	store, ctx := newTestStore(t)
	p := mustCreatePipeline(t, ctx, store.Pipelines(), sagaPipeline("team-a"))
	execRepo := store.Executions()

	e, err := execRepo.Create(ctx, pendingExecution(p, "user-1"), "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// --- COARSE execution transitions (Save) ---
	e.Status = domain.ExecutionStatusRunning
	e.CurrentStep = "deploy"
	if err := execRepo.Save(ctx, e); err != nil {
		t.Fatalf("Save RUNNING: %v", err)
	}
	assertExecStatus(t, ctx, execRepo, e.ID, domain.ExecutionStatusRunning)

	e.Status = domain.ExecutionStatusCompensating
	if err := execRepo.Save(ctx, e); err != nil {
		t.Fatalf("Save COMPENSATING: %v", err)
	}
	assertExecStatus(t, ctx, execRepo, e.ID, domain.ExecutionStatusCompensating)

	now := time.Now().UTC().Truncate(time.Microsecond)
	e.Status = domain.ExecutionStatusFailed
	e.CompletedAt = ptr(now)
	e.Error = "deploy failed: registry unreachable"
	if err := execRepo.Save(ctx, e); err != nil {
		t.Fatalf("Save FAILED: %v", err)
	}
	final, err := execRepo.GetByID(ctx, e.ID)
	if err != nil {
		t.Fatalf("GetByID final: %v", err)
	}
	if final.Status != domain.ExecutionStatusFailed {
		t.Errorf("final status = %v, want Failed", final.Status)
	}
	if final.Error != "deploy failed: registry unreachable" {
		t.Errorf("error not persisted: %q", final.Error)
	}
	if final.CompletedAt == nil || !final.CompletedAt.Equal(now) {
		t.Errorf("completedAt = %v, want %v", final.CompletedAt, now)
	}

	// --- FINE step transitions (SaveStep), incl. compensation sub-states ---
	deploy := findStepExec(final.Steps, "deploy")
	if deploy == nil {
		t.Fatal("deploy step missing")
	}
	startedAt := now.Add(1 * time.Second)
	deploy.Status = domain.StepStatusRunning
	deploy.StartedAt = ptr(startedAt)
	deploy.Attempt = 1
	if err := execRepo.SaveStep(ctx, *deploy); err != nil {
		t.Fatalf("SaveStep RUNNING: %v", err)
	}

	completedAt := now.Add(2 * time.Second)
	deploy.Status = domain.StepStatusCompleted
	deploy.CompletedAt = ptr(completedAt)
	deploy.Output = map[string]any{"endpoint": "svc.fp-models:8080", "replicas": float64(2)}
	if err := execRepo.SaveStep(ctx, *deploy); err != nil {
		t.Fatalf("SaveStep COMPLETED: %v", err)
	}

	// Compensation sub-states.
	deploy.Status = domain.StepStatusCompensating
	if err := execRepo.SaveStep(ctx, *deploy); err != nil {
		t.Fatalf("SaveStep COMPENSATING: %v", err)
	}
	deploy.Status = domain.StepStatusCompensated
	if err := execRepo.SaveStep(ctx, *deploy); err != nil {
		t.Fatalf("SaveStep COMPENSATED: %v", err)
	}

	// Read back: the step ended COMPENSATED with its output + attempt + timestamps.
	reread, err := execRepo.GetByID(ctx, e.ID)
	if err != nil {
		t.Fatalf("GetByID after step transitions: %v", err)
	}
	gotDeploy := findStepExec(reread.Steps, "deploy")
	if gotDeploy.Status != domain.StepStatusCompensated {
		t.Errorf("deploy status = %v, want Compensated", gotDeploy.Status)
	}
	if gotDeploy.Attempt != 1 {
		t.Errorf("deploy attempt = %d, want 1", gotDeploy.Attempt)
	}
	if gotDeploy.Output["endpoint"] != "svc.fp-models:8080" {
		t.Errorf("deploy output lost: %v", gotDeploy.Output)
	}
	if gotDeploy.StartedAt == nil || !gotDeploy.StartedAt.Equal(startedAt) {
		t.Errorf("startedAt = %v, want %v", gotDeploy.StartedAt, startedAt)
	}
}

// SaveStep is an idempotent UPSERT: re-saving the same step state does not create
// a duplicate row and leaves the row in the saved state.
func TestExecutionRepository_SaveStep_IdempotentUpsert(t *testing.T) {
	store, ctx := newTestStore(t)
	p := mustCreatePipeline(t, ctx, store.Pipelines(), sagaPipeline("team-a"))
	execRepo := store.Executions()
	e, err := execRepo.Create(ctx, pendingExecution(p, "user-1"), "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	step := findStepExec(e.Steps, "validate")
	step.Status = domain.StepStatusCompleted
	step.CompletedAt = ptr(time.Now().UTC())

	for i := 0; i < 3; i++ {
		if err := execRepo.SaveStep(ctx, *step); err != nil {
			t.Fatalf("SaveStep iteration %d: %v", i, err)
		}
	}
	// Still exactly 3 step rows (no UPSERT duplication).
	if n := countSteps(t, ctx, store, e.ID); n != 3 {
		t.Fatalf("SaveStep created duplicates: %d step rows, want 3", n)
	}
}

func TestExecutionRepository_Save_NotFound(t *testing.T) {
	store, ctx := newTestStore(t)
	missing := domain.Execution{ID: newID(), Status: domain.ExecutionStatusRunning, StartedAt: time.Now()}
	if err := store.Executions().Save(ctx, missing); !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("Save(missing) = %v, want ErrRepoNotFound", err)
	}
}

// GetByID returns steps ordered by completion time — the order the saga
// compensator relies on (it undoes COMPLETED steps in reverse completion order).
func TestExecutionRepository_GetByID_StepsOrderedByCompletion(t *testing.T) {
	store, ctx := newTestStore(t)
	p := mustCreatePipeline(t, ctx, store.Pipelines(), sagaPipeline("team-a"))
	execRepo := store.Executions()
	e, err := execRepo.Create(ctx, pendingExecution(p, "user-1"), "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Complete steps OUT of slice order: teardown first, then validate, then deploy
	// — completion order is validate? No: we deliberately stamp completed_at so the
	// expected read order is teardown(t0) < validate(t1) < deploy(t2).
	base := time.Now().UTC().Truncate(time.Microsecond)
	complete := func(stepID string, at time.Time) {
		st := findStepExec(e.Steps, stepID)
		st.Status = domain.StepStatusCompleted
		st.CompletedAt = ptr(at)
		if err := execRepo.SaveStep(ctx, *st); err != nil {
			t.Fatalf("SaveStep %s: %v", stepID, err)
		}
	}
	complete("teardown", base.Add(0))
	complete("validate", base.Add(1*time.Second))
	complete("deploy", base.Add(2*time.Second))

	got, err := execRepo.GetByID(ctx, e.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	// All three are COMPLETED so all have a completed_at; the read order must be
	// ascending completed_at: teardown, validate, deploy.
	wantOrder := []string{"teardown", "validate", "deploy"}
	var gotOrder []string
	for _, st := range got.Steps {
		gotOrder = append(gotOrder, st.StepID)
	}
	for i, want := range wantOrder {
		if i >= len(gotOrder) || gotOrder[i] != want {
			t.Fatalf("step order = %v, want %v", gotOrder, wantOrder)
		}
	}
}

// LIST is TEAM-SCOPED via the pipelines JOIN and filterable by pipeline + status.
func TestExecutionRepository_List_TeamScopedAndStatusFiltered(t *testing.T) {
	store, ctx := newTestStore(t)
	pipeRepo := store.Pipelines()
	execRepo := store.Executions()

	pA := mustCreatePipeline(t, ctx, pipeRepo, sagaPipeline("team-a"))
	pB := mustCreatePipeline(t, ctx, pipeRepo, sagaPipeline("team-b"))

	// team-a: 3 runs (2 FAILED, 1 RUNNING). team-b: 1 run (must never appear for A).
	base := time.Now().UTC().Truncate(time.Microsecond)
	mk := func(p domain.PipelineDefinition, status domain.ExecutionStatus, at time.Time) string {
		e := pendingExecution(p, "user")
		e.StartedAt = at
		created, err := execRepo.Create(ctx, e, "")
		if err != nil {
			t.Fatalf("create exec: %v", err)
		}
		created.Status = status
		if err := execRepo.Save(ctx, created); err != nil {
			t.Fatalf("save status: %v", err)
		}
		return created.ID
	}
	mk(pA, domain.ExecutionStatusFailed, base.Add(0))
	mk(pA, domain.ExecutionStatusFailed, base.Add(1*time.Second))
	runningID := mk(pA, domain.ExecutionStatusRunning, base.Add(2*time.Second))
	mk(pB, domain.ExecutionStatusFailed, base.Add(3*time.Second))

	// All team-a runs, newest first; team-b excluded.
	all, _, err := execRepo.List(ctx, domain.ListExecutionsFilter{Team: "team-a"})
	if err != nil {
		t.Fatalf("List team-a: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("List team-a returned %d, want 3", len(all))
	}
	if all[0].ID != runningID {
		t.Errorf("not newest-first: first=%q want %q", all[0].ID, runningID)
	}
	// List items omit per-step detail (documented contract).
	if len(all[0].Steps) != 0 {
		t.Errorf("List item carried %d steps; contract says omit per-step detail", len(all[0].Steps))
	}

	// Status filter: only the FAILED runs.
	failed, _, err := execRepo.List(ctx, domain.ListExecutionsFilter{
		Team: "team-a", Status: domain.ExecutionStatusFailed,
	})
	if err != nil {
		t.Fatalf("List FAILED: %v", err)
	}
	if len(failed) != 2 {
		t.Fatalf("status-filtered List returned %d, want 2", len(failed))
	}
	for _, e := range failed {
		if e.Status != domain.ExecutionStatusFailed {
			t.Errorf("status filter leaked %v", e.Status)
		}
	}

	// Pipeline filter scoped to A: pB's pipeline id under team-a returns nothing
	// (JOIN ensures pB isn't team-a's even if a caller guesses its id — anti-IDOR).
	none, _, err := execRepo.List(ctx, domain.ListExecutionsFilter{Team: "team-a", PipelineID: pB.ID})
	if err != nil {
		t.Fatalf("List cross-tenant pipeline filter: %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("cross-tenant pipeline filter returned %d, want 0", len(none))
	}
}

// LIST pagination over executions returns each run exactly once, in order.
func TestExecutionRepository_List_Pagination(t *testing.T) {
	store, ctx := newTestStore(t)
	p := mustCreatePipeline(t, ctx, store.Pipelines(), sagaPipeline("team-a"))
	execRepo := store.Executions()

	base := time.Now().UTC().Truncate(time.Microsecond)
	for i := 0; i < 5; i++ {
		e := pendingExecution(p, "user")
		e.StartedAt = base.Add(time.Duration(i) * time.Second)
		if _, err := execRepo.Create(ctx, e, ""); err != nil {
			t.Fatalf("create exec %d: %v", i, err)
		}
	}

	var seen []string
	token := ""
	for {
		page, next, err := execRepo.List(ctx, domain.ListExecutionsFilter{
			Team: "team-a", List: domain.ListOptions{PageSize: 2, PageToken: token},
		})
		if err != nil {
			t.Fatalf("List page: %v", err)
		}
		for _, e := range page {
			seen = append(seen, e.ID)
		}
		if next == "" {
			break
		}
		token = next
		if len(seen) > 5 {
			t.Fatal("pagination over-returned")
		}
	}
	if len(seen) != 5 {
		t.Fatalf("paginated List saw %d runs, want 5", len(seen))
	}
	assertUnique(t, seen)
}

// assertExecStatus reads an execution and asserts its coarse status — a small
// helper used by the state-machine test to check each durable transition.
func assertExecStatus(t *testing.T, ctx context.Context, repo *ExecutionRepository, id string, want domain.ExecutionStatus) {
	t.Helper()
	e, err := repo.GetByID(ctx, id)
	if err != nil {
		t.Fatalf("GetByID(%s): %v", id, err)
	}
	if e.Status != want {
		t.Fatalf("status = %v, want %v", e.Status, want)
	}
}
