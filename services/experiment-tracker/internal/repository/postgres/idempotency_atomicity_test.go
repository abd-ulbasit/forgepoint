// idempotency_atomicity_test.go — proves the FIX for the partial-write window
// between a committed state change and its idempotency-key record.
//
// ============================================================================
// THE BUG THESE TESTS PIN
// ============================================================================
//
// The old shape committed the mutation (e.g. the run insert) in its own tx and
// THEN wrote the idempotency row in a SEPARATE pool.Exec. A crash / dropped
// connection / cancelled ctx in the GAP left the effect committed but no key
// recorded — so the next retry of the same key saw Lookup found=false and created
// a DUPLICATE (a second run for StartRun, which has no other natural-key backstop).
//
// The fix puts BOTH writes in ONE pgx.BeginFunc (the *Idem repo methods, calling
// RecordTx on the same tx). These tests assert the two invariants that gives:
//
//   1. COMMIT-TOGETHER: after a successful *Idem call, the effect AND its
//      idempotency row are BOTH present (queried straight off the pool).
//   2. ROLLBACK-TOGETHER: when the enclosing tx fails (here, via the StartRun
//      natural-key backstop runs_idempotency_uniq), NEITHER a second effect NOR a
//      second key row lands — there is no partial write a retry could misread.
//
// We can't kill the process mid-tx deterministically, so we exercise the same
// atomic boundary through a forced unique-violation: it rolls back the WHOLE tx
// exactly as a crash-before-commit would, and we assert nothing partial survived.
package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/abd-ulbasit/forgepoint/services/experiment-tracker/internal/domain"
)

// countRows is a tiny helper to assert table cardinality straight off the pool —
// the ground truth the adapter methods must produce, read without going through
// them so a buggy read can't hide a bad write.
func countRows(t *testing.T, ctx context.Context, s *Store, query string, args ...any) int {
	t.Helper()
	var n int
	if err := s.Pool().QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("count query %q: %v", query, err)
	}
	return n
}

// lookupIdem reads the idempotency_keys table directly (not via the store) so the
// test verifies the ROW, not the store's behavior on top of it.
func lookupIdem(t *testing.T, ctx context.Context, s *Store, op, key string) (string, bool) {
	t.Helper()
	var resultID string
	err := s.Pool().QueryRow(ctx,
		`SELECT result_id FROM idempotency_keys WHERE operation = $1 AND key = $2`, op, key).Scan(&resultID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false
	}
	if err != nil {
		t.Fatalf("direct idem lookup: %v", err)
	}
	return resultID, true
}

// ============================================================================
// CreateRunIdem — StartRun's exactly-once create
// ============================================================================

// TestCreateRunIdem_RecordsKeyInSameTx: a successful CreateRunIdem commits the run
// AND its idempotency row together. We read BOTH straight off the pool. This is
// invariant (1): the key is recorded iff the run exists — the gap is closed.
func TestCreateRunIdem_RecordsKeyInSameTx(t *testing.T) {
	s, ctx := newTestStore(t)
	exp := mustCreateExperiment(t, ctx, s.Experiments(), newExperiment("team-a", "e1"))

	run := newRunningRun(exp.ID, domain.Param{Key: "lr", Value: "0.01"})
	created, err := s.Runs().CreateRunIdem(ctx, run, "start_run", "idem-key-1")
	if err != nil {
		t.Fatalf("CreateRunIdem: %v", err)
	}

	// The run committed.
	if n := countRows(t, ctx, s, `SELECT count(*) FROM runs WHERE id = $1`, created.ID); n != 1 {
		t.Fatalf("expected the run to be committed, found %d rows", n)
	}
	// The idempotency row committed in the SAME tx, mapping the key → this run.
	gotID, found := lookupIdem(t, ctx, s, "start_run", "idem-key-1")
	if !found {
		t.Fatal("idempotency row missing — the key was NOT recorded atomically with the run")
	}
	if gotID != created.ID {
		t.Fatalf("idempotency row points at %q, want the created run %q", gotID, created.ID)
	}
	// The natural-key backstop columns are stamped on the run row too.
	if n := countRows(t, ctx, s,
		`SELECT count(*) FROM runs WHERE id = $1 AND idempotency_operation = 'start_run' AND idempotency_key = 'idem-key-1'`,
		created.ID); n != 1 {
		t.Fatalf("backstop columns not stamped on the run row (found %d)", n)
	}
}

// TestCreateRunIdem_NoKeyWritesNoRowAndNullBackstop: with an EMPTY key, the run is
// created but NO idempotency row is written and the backstop columns are NULL — so
// keyless runs never collide on the partial UNIQUE index. This guards the
// "delegate with empty key" path CreateRun takes.
func TestCreateRunIdem_NoKeyWritesNoRowAndNullBackstop(t *testing.T) {
	s, ctx := newTestStore(t)
	exp := mustCreateExperiment(t, ctx, s.Experiments(), newExperiment("team-a", "e1"))

	// Two keyless runs must BOTH succeed (NULL idempotency_key is ignored by the
	// partial unique index — they don't collide on NULL).
	r1, err := s.Runs().CreateRunIdem(ctx, newRunningRun(exp.ID), "", "")
	if err != nil {
		t.Fatalf("first keyless create: %v", err)
	}
	r2, err := s.Runs().CreateRunIdem(ctx, newRunningRun(exp.ID), "", "")
	if err != nil {
		t.Fatalf("second keyless create must not collide on NULL key: %v", err)
	}

	// No idempotency rows for keyless creates.
	if n := countRows(t, ctx, s, `SELECT count(*) FROM idempotency_keys`); n != 0 {
		t.Fatalf("keyless creates must write no idempotency rows, found %d", n)
	}
	// Backstop columns are NULL on both.
	for _, id := range []string{r1.ID, r2.ID} {
		if n := countRows(t, ctx, s,
			`SELECT count(*) FROM runs WHERE id = $1 AND idempotency_key IS NULL AND idempotency_operation IS NULL`,
			id); n != 1 {
			t.Fatalf("keyless run %s should have NULL backstop columns", id)
		}
	}
}

// TestCreateRunIdem_DuplicateKeyRollsBackAtomically: this is invariant (2) and the
// CORE crash-gap proof. A second CreateRunIdem reusing the same (operation, key)
// trips the natural-key backstop runs_idempotency_uniq. We assert:
//   - the call returns ErrRepoConflict (the backstop fired, not a raw error),
//   - the WHOLE second tx rolled back: still exactly ONE run, and the second run's
//     id is absent — no partial write,
//   - the idempotency row STILL maps to the FIRST run (untouched).
//
// Because the second run's INSERT and its key write share one tx, the backstop
// rolling back one rolls back BOTH. That is precisely the atomicity a crash-in-the-
// gap would otherwise have broken in the old separate-Exec shape.
func TestCreateRunIdem_DuplicateKeyRollsBackAtomically(t *testing.T) {
	s, ctx := newTestStore(t)
	exp := mustCreateExperiment(t, ctx, s.Experiments(), newExperiment("team-a", "e1"))

	first, err := s.Runs().CreateRunIdem(ctx, newRunningRun(exp.ID), "start_run", "dup-key")
	if err != nil {
		t.Fatalf("first create: %v", err)
	}

	// Second create with a DIFFERENT run id but the SAME idempotency key.
	second := newRunningRun(exp.ID)
	_, err = s.Runs().CreateRunIdem(ctx, second, "start_run", "dup-key")
	if !errors.Is(err, domain.ErrRepoConflict) {
		t.Fatalf("duplicate key should return ErrRepoConflict, got %v", err)
	}

	// Exactly one run under the experiment — the second tx rolled back wholesale.
	if n := countRows(t, ctx, s, `SELECT count(*) FROM runs WHERE experiment_id = $1`, exp.ID); n != 1 {
		t.Fatalf("expected exactly 1 run after a duplicate-key create, found %d (partial write!)", n)
	}
	// The second run id must NOT exist.
	if n := countRows(t, ctx, s, `SELECT count(*) FROM runs WHERE id = $1`, second.ID); n != 0 {
		t.Fatalf("the rejected second run must not have been written, found %d", n)
	}
	// The key still points at the FIRST run; the service's replay will return it.
	gotID, found := lookupIdem(t, ctx, s, "start_run", "dup-key")
	if !found || gotID != first.ID {
		t.Fatalf("idempotency row should still map to the first run %q, got id=%q found=%v", first.ID, gotID, found)
	}
}

// TestCreateRunIdem_DifferentOperationsSameKeyCoexist: the same key under a
// DIFFERENT operation does not collide — the backstop is scoped per-RPC by
// (operation, key), like the idempotency_keys PK. (A run created under
// 'start_run'/'k' and another under 'other_op'/'k' both stand.)
func TestCreateRunIdem_DifferentOperationsSameKeyCoexist(t *testing.T) {
	s, ctx := newTestStore(t)
	exp := mustCreateExperiment(t, ctx, s.Experiments(), newExperiment("team-a", "e1"))

	if _, err := s.Runs().CreateRunIdem(ctx, newRunningRun(exp.ID), "start_run", "shared"); err != nil {
		t.Fatalf("create under start_run: %v", err)
	}
	if _, err := s.Runs().CreateRunIdem(ctx, newRunningRun(exp.ID), "other_op", "shared"); err != nil {
		t.Fatalf("same key under a different operation must not collide: %v", err)
	}
	if n := countRows(t, ctx, s, `SELECT count(*) FROM runs WHERE experiment_id = $1`, exp.ID); n != 2 {
		t.Fatalf("expected 2 runs (per-operation scoping), found %d", n)
	}
}

// ============================================================================
// DeleteRunIdem — DeleteRun's exactly-once delete
// ============================================================================

// TestDeleteRunIdem_RecordsKeyInSameTx: a successful delete removes the run AND
// records the key together. The replay path the service relies on (Lookup finds the
// key → return nil) is only sound if the key is durably present whenever the run is
// gone — which this proves.
func TestDeleteRunIdem_RecordsKeyInSameTx(t *testing.T) {
	s, ctx := newTestStore(t)
	_, run := seedExperimentAndRun(t, ctx, s, "team-a")

	if err := s.Runs().DeleteRunIdem(ctx, run.ID, "delete_run", "del-key"); err != nil {
		t.Fatalf("DeleteRunIdem: %v", err)
	}
	// Run gone.
	if n := countRows(t, ctx, s, `SELECT count(*) FROM runs WHERE id = $1`, run.ID); n != 0 {
		t.Fatalf("run should be deleted, found %d rows", n)
	}
	// Key recorded in the same tx.
	gotID, found := lookupIdem(t, ctx, s, "delete_run", "del-key")
	if !found || gotID != run.ID {
		t.Fatalf("delete key not recorded atomically: id=%q found=%v", gotID, found)
	}
}

// TestDeleteRunIdem_AlreadyGoneRecordsNothing: deleting a run that is already gone
// returns ErrRepoNotFound and records NO key — there was no successful delete on
// THIS call to make idempotent. (The run never existed here.)
func TestDeleteRunIdem_AlreadyGoneRecordsNothing(t *testing.T) {
	s, ctx := newTestStore(t)

	err := s.Runs().DeleteRunIdem(ctx, newID(), "delete_run", "ghost-key")
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("deleting a missing run should return ErrRepoNotFound, got %v", err)
	}
	if _, found := lookupIdem(t, ctx, s, "delete_run", "ghost-key"); found {
		t.Fatal("no key should be recorded when nothing was deleted")
	}
}

// TestDeleteRun_DelegatesWithoutKey: the port method DeleteRun (empty key) still
// deletes and records nothing — the delegation path is intact.
func TestDeleteRun_DelegatesWithoutKey(t *testing.T) {
	s, ctx := newTestStore(t)
	_, run := seedExperimentAndRun(t, ctx, s, "team-a")

	if err := s.Runs().DeleteRun(ctx, run.ID); err != nil {
		t.Fatalf("DeleteRun: %v", err)
	}
	if n := countRows(t, ctx, s, `SELECT count(*) FROM idempotency_keys`); n != 0 {
		t.Fatalf("keyless delete must record nothing, found %d rows", n)
	}
}

// ============================================================================
// AppendMetricsIdem — LogMetrics's batch + key in one tx
// ============================================================================

// TestAppendMetricsIdem_RecordsKeyInSameTx: a batch write and its key commit
// together. The accepted count is the newly-inserted rows; the key maps to the run.
func TestAppendMetricsIdem_RecordsKeyInSameTx(t *testing.T) {
	s, ctx := newTestStore(t)
	_, run := seedExperimentAndRun(t, ctx, s, "team-a")

	points := []domain.MetricPoint{mp("loss", 1, 0.5), mp("loss", 2, 0.4)}
	written, err := s.Runs().AppendMetricsIdem(ctx, run.ID, points, "log_metrics", "batch-key")
	if err != nil {
		t.Fatalf("AppendMetricsIdem: %v", err)
	}
	if written != 2 {
		t.Fatalf("expected 2 new points, got %d", written)
	}
	if n := countRows(t, ctx, s, `SELECT count(*) FROM run_metrics WHERE run_id = $1`, run.ID); n != 2 {
		t.Fatalf("expected 2 metric rows, found %d", n)
	}
	gotID, found := lookupIdem(t, ctx, s, "log_metrics", "batch-key")
	if !found || gotID != run.ID {
		t.Fatalf("metrics key not recorded atomically: id=%q found=%v", gotID, found)
	}
}

// TestAppendMetricsIdem_KeylessDelegation: AppendMetrics (empty key) writes the
// batch and records no key — the (run_id,key,step) UNIQUE backstop still dedups a
// replay, but no idempotency row is created.
func TestAppendMetricsIdem_KeylessDelegation(t *testing.T) {
	s, ctx := newTestStore(t)
	_, run := seedExperimentAndRun(t, ctx, s, "team-a")

	written, err := s.Runs().AppendMetrics(ctx, run.ID, []domain.MetricPoint{mp("acc", 1, 0.9)})
	if err != nil {
		t.Fatalf("AppendMetrics: %v", err)
	}
	if written != 1 {
		t.Fatalf("expected 1 written, got %d", written)
	}
	if n := countRows(t, ctx, s, `SELECT count(*) FROM idempotency_keys`); n != 0 {
		t.Fatalf("keyless append must record nothing, found %d", n)
	}
}

// ============================================================================
// SetArtifactsIdem — SetRunArtifacts's overwrite + key in one tx
// ============================================================================

// TestSetArtifactsIdem_RecordsKeyInSameTx: the artifacts UPDATE and its key commit
// together, and the returned run carries the new artifacts plus its params.
func TestSetArtifactsIdem_RecordsKeyInSameTx(t *testing.T) {
	s, ctx := newTestStore(t)
	exp := mustCreateExperiment(t, ctx, s.Experiments(), newExperiment("team-a", "e1"))
	run := mustCreateRun(t, ctx, s.Runs(), newRunningRun(exp.ID, domain.Param{Key: "lr", Value: "0.1"}))

	arts := map[string]any{"checkpoint": "s3://bucket/model.onnx"}
	updated, err := s.Runs().SetArtifactsIdem(ctx, run.ID, arts, "set_run_artifacts", "art-key")
	if err != nil {
		t.Fatalf("SetArtifactsIdem: %v", err)
	}
	if updated.Artifacts["checkpoint"] != "s3://bucket/model.onnx" {
		t.Fatalf("artifacts not returned: %+v", updated.Artifacts)
	}
	if len(updated.Params) != 1 || updated.Params[0].Key != "lr" {
		t.Fatalf("params not re-attached: %+v", updated.Params)
	}
	gotID, found := lookupIdem(t, ctx, s, "set_run_artifacts", "art-key")
	if !found || gotID != run.ID {
		t.Fatalf("artifacts key not recorded atomically: id=%q found=%v", gotID, found)
	}
}

// TestSetArtifactsIdem_MissingRunRecordsNothing: setting artifacts on a missing run
// returns ErrRepoNotFound and records no key — the UPDATE matched no row, so the tx
// (including the would-be key write) rolls back.
func TestSetArtifactsIdem_MissingRunRecordsNothing(t *testing.T) {
	s, ctx := newTestStore(t)

	_, err := s.Runs().SetArtifactsIdem(ctx, newID(), map[string]any{"x": 1}, "set_run_artifacts", "miss-key")
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("missing run should return ErrRepoNotFound, got %v", err)
	}
	if _, found := lookupIdem(t, ctx, s, "set_run_artifacts", "miss-key"); found {
		t.Fatal("no key should be recorded when no run was updated")
	}
}

// ============================================================================
// RecordTx — the building block, exercised directly
// ============================================================================

// TestRecordTx_CommitsAndRollsBackWithTx: RecordTx enlists in a caller tx. On
// commit the row is present; on rollback it is not. This is the primitive the
// *Idem methods rely on to bind the key to the effect's fate.
func TestRecordTx_CommitsAndRollsBackWithTx(t *testing.T) {
	s, ctx := newTestStore(t)
	store := s.Idempotency()

	// Commit path.
	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := store.RecordTx(ctx, tx, "op", "committed", "res-1"); err != nil {
		t.Fatalf("RecordTx: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if id, found := lookupIdem(t, ctx, s, "op", "committed"); !found || id != "res-1" {
		t.Fatalf("committed row missing/wrong: id=%q found=%v", id, found)
	}

	// Rollback path: the row written via RecordTx must vanish with the tx.
	tx2, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin 2: %v", err)
	}
	if err := store.RecordTx(ctx, tx2, "op", "rolledback", "res-2"); err != nil {
		t.Fatalf("RecordTx 2: %v", err)
	}
	if err := tx2.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if _, found := lookupIdem(t, ctx, s, "op", "rolledback"); found {
		t.Fatal("rolled-back RecordTx row must NOT be present — it was not bound to the tx")
	}
}
