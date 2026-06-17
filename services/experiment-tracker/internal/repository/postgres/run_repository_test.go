package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/experiment-tracker/internal/domain"
)

// TestCreateRunAtomicWithParams: CreateRun writes the run AND its initial params
// in one transaction. We verify both landed and the run round-trips (status/source
// enums, started_at, empty finals, nil artifacts).
func TestCreateRunAtomicWithParams(t *testing.T) {
	s, ctx := newTestStore(t)
	exp := mustCreateExperiment(t, ctx, s.Experiments(), newExperiment("team-a", "e1"))

	run := newRunningRun(exp.ID,
		domain.Param{Key: "lr", Value: "0.01"},
		domain.Param{Key: "optimizer", Value: "adam"},
	)
	created, err := s.Runs().CreateRun(ctx, run)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if created.Status != domain.RunStatusRunning || created.Source != domain.RunSourceAPI {
		t.Fatalf("enum fields wrong: %+v", created)
	}

	got, err := s.Runs().GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if len(got.Params) != 2 {
		t.Fatalf("expected 2 params persisted in the same tx, got %d", len(got.Params))
	}
	// Params come back ordered by key: lr, optimizer.
	if got.Params[0].Key != "lr" || got.Params[1].Key != "optimizer" {
		t.Fatalf("params order/content wrong: %+v", got.Params)
	}
	if len(got.FinalMetrics) != 0 {
		t.Fatalf("fresh run should have no finals, got %v", got.FinalMetrics)
	}
	if got.Artifacts != nil {
		t.Fatalf("fresh run should have nil artifacts, got %v", got.Artifacts)
	}
	if !got.StartedAt.Equal(run.StartedAt) {
		t.Fatalf("started_at mismatch: %v vs %v", got.StartedAt, run.StartedAt)
	}
	if got.EndedAt != nil {
		t.Fatalf("running run should have nil ended_at")
	}
}

// TestCreateRunDanglingExperimentFK: a run under a non-existent experiment trips
// the FK (23503) → ErrRepoNotFound. This proves the FK is real, not advisory.
func TestCreateRunDanglingExperimentFK(t *testing.T) {
	s, ctx := newTestStore(t)
	run := newRunningRun(newID()) // experiment id that was never created
	_, err := s.Runs().CreateRun(ctx, run)
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("want ErrRepoNotFound for dangling experiment FK, got %v", err)
	}
}

// TestGetRunNotFound: a missing run yields ErrRepoNotFound.
func TestGetRunNotFound(t *testing.T) {
	s, ctx := newTestStore(t)
	_, err := s.Runs().GetRun(ctx, newID())
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("want ErrRepoNotFound, got %v", err)
	}
}

// TestAppendMetricsBatchAndIdempotency is the centerpiece: one batched INSERT,
// accurate accepted-count, and CROSS-batch idempotency via ON CONFLICT
// (run_id,key,step) DO NOTHING. A re-sent batch double-writes nothing.
func TestAppendMetricsBatchAndIdempotency(t *testing.T) {
	s, ctx := newTestStore(t)
	_, run := seedExperimentAndRun(t, ctx, s, "team-a")

	batch := []domain.MetricPoint{
		mp("loss", 1, 0.9),
		mp("loss", 2, 0.7),
		mp("accuracy", 1, 0.4),
	}
	written, err := s.Runs().AppendMetrics(ctx, run.ID, batch)
	if err != nil {
		t.Fatalf("append metrics: %v", err)
	}
	if written != 3 {
		t.Fatalf("first batch should write all 3, got %d", written)
	}

	// Re-send the EXACT same batch (a retry / redelivery). The unique index makes
	// every row a conflict → DO NOTHING → 0 newly written.
	again, err := s.Runs().AppendMetrics(ctx, run.ID, batch)
	if err != nil {
		t.Fatalf("re-append: %v", err)
	}
	if again != 0 {
		t.Fatalf("retried batch must write 0 (idempotent), got %d", again)
	}

	// A partly-overlapping batch: only the genuinely new (key,step) lands.
	mixed := []domain.MetricPoint{
		mp("loss", 2, 0.7),    // duplicate (key,step) → dropped
		mp("loss", 3, 0.5),    // new
		mp("accuracy", 2, 0.6), // new
	}
	n, err := s.Runs().AppendMetrics(ctx, run.ID, mixed)
	if err != nil {
		t.Fatalf("mixed append: %v", err)
	}
	if n != 2 {
		t.Fatalf("mixed batch should write only the 2 new points, got %d", n)
	}

	// Total distinct points = 5 (loss@1,2,3 + accuracy@1,2). Verify via history.
	all := drainHistory(t, ctx, s.Runs(), domain.MetricHistoryQuery{RunID: run.ID})
	if len(all) != 5 {
		t.Fatalf("expected 5 distinct points stored, got %d", len(all))
	}

	// The FIRST value for (loss,2) must win (the DB kept the stored row; the
	// re-send with the same value is moot here, but the dedup contract is "stored
	// row wins"). Confirm the value at (loss,2) is the original 0.7.
	for _, p := range all {
		if p.Key == "loss" && p.Step == 2 && p.Value != 0.7 {
			t.Fatalf("stored (loss,2) value should be the first-written 0.7, got %v", p.Value)
		}
	}
}

// TestAppendMetricsValueRoundTrip: DOUBLE PRECISION values survive exactly,
// including negatives and large magnitudes — the finite-real contract.
func TestAppendMetricsValueRoundTrip(t *testing.T) {
	s, ctx := newTestStore(t)
	_, run := seedExperimentAndRun(t, ctx, s, "team-a")

	want := map[int64]float64{
		1: -1.5,
		2: 123456.789,
		3: 0.000001,
	}
	var batch []domain.MetricPoint
	for step, v := range want {
		batch = append(batch, mp("loss", step, v))
	}
	if _, err := s.Runs().AppendMetrics(ctx, run.ID, batch); err != nil {
		t.Fatalf("append: %v", err)
	}
	got := drainHistory(t, ctx, s.Runs(), domain.MetricHistoryQuery{RunID: run.ID})
	for _, p := range got {
		if want[p.Step] != p.Value {
			t.Fatalf("value round-trip mismatch at step %d: got %v want %v", p.Step, p.Value, want[p.Step])
		}
	}
}

// TestAppendMetricsRunFK: appending to a non-existent run trips the FK →
// ErrRepoNotFound.
func TestAppendMetricsRunFK(t *testing.T) {
	s, ctx := newTestStore(t)
	_, err := s.Runs().AppendMetrics(ctx, newID(), []domain.MetricPoint{mp("loss", 1, 0.1)})
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("want ErrRepoNotFound appending to missing run, got %v", err)
	}
}

// TestAppendParamsWriteOnce: AppendParams writes new keys and returns the count;
// the (run_id,key) PK is the write-once backstop — a racing duplicate key inserts
// nothing extra (DO NOTHING), and GetRunParams reads them ordered.
func TestAppendParamsWriteOnce(t *testing.T) {
	s, ctx := newTestStore(t)
	_, run := seedExperimentAndRun(t, ctx, s, "team-a")

	n, err := s.Runs().AppendParams(ctx, run.ID, []domain.Param{
		{Key: "lr", Value: "0.01"},
		{Key: "batch", Value: "32"},
	})
	if err != nil {
		t.Fatalf("append params: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 new params, got %d", n)
	}

	// Re-appending an existing key (same value) is a DB-level no-op: DO NOTHING,
	// 0 newly written. (The service already filters identical re-logs; this proves
	// the storage backstop.)
	again, err := s.Runs().AppendParams(ctx, run.ID, []domain.Param{{Key: "lr", Value: "0.01"}})
	if err != nil {
		t.Fatalf("re-append params: %v", err)
	}
	if again != 0 {
		t.Fatalf("re-appending existing key must write 0, got %d", again)
	}

	// The stored value for an existing key is NOT overwritten even if a different
	// value sneaks to the adapter (the service rejects this, but the DO NOTHING
	// backstop must also keep the original).
	if _, err := s.Runs().AppendParams(ctx, run.ID, []domain.Param{{Key: "lr", Value: "0.99"}}); err != nil {
		t.Fatalf("conflicting re-append should not error at adapter (DO NOTHING): %v", err)
	}
	params, err := s.Runs().GetRunParams(ctx, run.ID)
	if err != nil {
		t.Fatalf("get params: %v", err)
	}
	for _, p := range params {
		if p.Key == "lr" && p.Value != "0.01" {
			t.Fatalf("write-once violated: lr became %q", p.Value)
		}
	}
	if len(params) != 2 {
		t.Fatalf("expected 2 params total, got %d", len(params))
	}
}

// TestUpdateRunStatusStampsFinals: the terminal transition writes status,
// ended_at, and the denormalized final_metrics (the CQRS projection) in ONE
// write; a reader then sees the complete terminal state. Verifies the finals
// JSONB round-trips.
func TestUpdateRunStatusStampsFinals(t *testing.T) {
	s, ctx := newTestStore(t)
	_, run := seedExperimentAndRun(t, ctx, s, "team-a")

	endedAt := tnow()
	finals := []domain.MetricPoint{
		{Key: "accuracy", Value: 0.94, Step: 100, Timestamp: endedAt},
		{Key: "loss", Value: 0.08, Step: 100, Timestamp: endedAt},
	}
	updated, err := s.Runs().UpdateRunStatus(ctx, run.ID, domain.RunStatusFinished, endedAt, finals)
	if err != nil {
		t.Fatalf("update status: %v", err)
	}
	if updated.Status != domain.RunStatusFinished {
		t.Fatalf("status not updated: %v", updated.Status)
	}
	if updated.EndedAt == nil || !updated.EndedAt.Equal(endedAt) {
		t.Fatalf("ended_at not stamped: %v", updated.EndedAt)
	}
	if len(updated.FinalMetrics) != 2 {
		t.Fatalf("finals not stamped: %v", updated.FinalMetrics)
	}

	// Re-read confirms durability of the terminal state + finals projection.
	got, _ := s.Runs().GetRun(ctx, run.ID)
	if got.Status != domain.RunStatusFinished || got.EndedAt == nil {
		t.Fatalf("terminal state not durable: %+v", got)
	}
	byKey := map[string]domain.MetricPoint{}
	for _, p := range got.FinalMetrics {
		byKey[p.Key] = p
	}
	if byKey["accuracy"].Value != 0.94 || byKey["accuracy"].Step != 100 {
		t.Fatalf("finals projection did not round-trip: %#v", got.FinalMetrics)
	}
}

// TestUpdateRunStatusNotFound: a vanished run → ErrRepoNotFound.
func TestUpdateRunStatusNotFound(t *testing.T) {
	s, ctx := newTestStore(t)
	_, err := s.Runs().UpdateRunStatus(ctx, newID(), domain.RunStatusFailed, tnow(), nil)
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("want ErrRepoNotFound, got %v", err)
	}
}

// TestDeleteRunCascades: hard-deleting a run removes its params and metrics via
// ON DELETE CASCADE. After delete, GetRun is ErrRepoNotFound and no child rows
// remain. A second delete is ErrRepoNotFound (the service treats that as
// idempotent success when a key matches).
func TestDeleteRunCascades(t *testing.T) {
	s, ctx := newTestStore(t)
	_, run := seedExperimentAndRun(t, ctx, s, "team-a")

	if _, err := s.Runs().AppendParams(ctx, run.ID, []domain.Param{{Key: "lr", Value: "0.01"}}); err != nil {
		t.Fatalf("seed params: %v", err)
	}
	if _, err := s.Runs().AppendMetrics(ctx, run.ID, []domain.MetricPoint{mp("loss", 1, 0.5), mp("loss", 2, 0.4)}); err != nil {
		t.Fatalf("seed metrics: %v", err)
	}

	if err := s.Runs().DeleteRun(ctx, run.ID); err != nil {
		t.Fatalf("delete run: %v", err)
	}
	if _, err := s.Runs().GetRun(ctx, run.ID); !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("run should be gone, got %v", err)
	}

	// Children gone (assert directly via the pool — the CASCADE is the contract).
	var nParams, nMetrics int
	if err := s.Pool().QueryRow(ctx, `SELECT count(*) FROM run_params WHERE run_id=$1`, run.ID).Scan(&nParams); err != nil {
		t.Fatalf("count params: %v", err)
	}
	if err := s.Pool().QueryRow(ctx, `SELECT count(*) FROM run_metrics WHERE run_id=$1`, run.ID).Scan(&nMetrics); err != nil {
		t.Fatalf("count metrics: %v", err)
	}
	if nParams != 0 || nMetrics != 0 {
		t.Fatalf("CASCADE failed: params=%d metrics=%d remain", nParams, nMetrics)
	}

	// Second delete → not found.
	if err := s.Runs().DeleteRun(ctx, run.ID); !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("second delete should be ErrRepoNotFound, got %v", err)
	}
}

// TestSetArtifactsRoundTripAndClear: SetArtifacts stores a JSON-shaped map, a
// re-read returns it faithfully (nested values intact), and setting nil clears it
// back to SQL NULL → nil map.
func TestSetArtifactsRoundTrip(t *testing.T) {
	s, ctx := newTestStore(t)
	_, run := seedExperimentAndRun(t, ctx, s, "team-a")

	art := map[string]any{
		"confusion_matrix": []any{float64(10), float64(2), float64(1), float64(7)},
		"notes":            "looks good",
		"nested":           map[string]any{"k": float64(1)},
	}
	updated, err := s.Runs().SetArtifacts(ctx, run.ID, art)
	if err != nil {
		t.Fatalf("set artifacts: %v", err)
	}
	if updated.Artifacts["notes"] != "looks good" {
		t.Fatalf("artifacts not stored: %#v", updated.Artifacts)
	}

	got, _ := s.Runs().GetRun(ctx, run.ID)
	nested, ok := got.Artifacts["nested"].(map[string]any)
	if !ok || nested["k"] != float64(1) {
		t.Fatalf("nested artifact did not round-trip: %#v", got.Artifacts)
	}

	// Clear by setting an empty map → SQL NULL → nil on read.
	cleared, err := s.Runs().SetArtifacts(ctx, run.ID, nil)
	if err != nil {
		t.Fatalf("clear artifacts: %v", err)
	}
	if cleared.Artifacts != nil {
		t.Fatalf("artifacts should be nil after clear, got %#v", cleared.Artifacts)
	}
}

// TestListRunsStatusFilterAndPagination: ListRuns scopes to the experiment,
// applies the optional status filter, and keyset-paginates newest-first.
func TestListRunsStatusFilterAndPagination(t *testing.T) {
	s, ctx := newTestStore(t)
	exp := mustCreateExperiment(t, ctx, s.Experiments(), newExperiment("team-a", "e1"))
	// A second experiment whose runs must NOT appear (scope check).
	other := mustCreateExperiment(t, ctx, s.Experiments(), newExperiment("team-a", "e2"))
	mustCreateRun(t, ctx, s.Runs(), newRunningRun(other.ID))

	base := tnow()
	var runs []domain.Run
	for i := 0; i < 4; i++ {
		r := newRunningRun(exp.ID)
		r.StartedAt = base.Add(time.Duration(i) * time.Second)
		runs = append(runs, mustCreateRun(t, ctx, s.Runs(), r))
	}
	// Finish two of them so the status filter has something to bite on.
	if _, err := s.Runs().UpdateRunStatus(ctx, runs[0].ID, domain.RunStatusFinished, tnow(), nil); err != nil {
		t.Fatalf("finish r0: %v", err)
	}
	if _, err := s.Runs().UpdateRunStatus(ctx, runs[1].ID, domain.RunStatusFinished, tnow(), nil); err != nil {
		t.Fatalf("finish r1: %v", err)
	}

	// No filter → all 4 in exp (other experiment's run excluded).
	all, _, err := s.Runs().ListRuns(ctx, exp.ID, domain.RunStatusUnspecified, domain.ListOptions{PageSize: 100})
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all) != 4 {
		t.Fatalf("expected 4 runs in experiment, got %d", len(all))
	}

	// Status filter → only the 2 FINISHED.
	finished, _, err := s.Runs().ListRuns(ctx, exp.ID, domain.RunStatusFinished, domain.ListOptions{PageSize: 100})
	if err != nil {
		t.Fatalf("list finished: %v", err)
	}
	if len(finished) != 2 {
		t.Fatalf("expected 2 FINISHED runs, got %d", len(finished))
	}
	for _, r := range finished {
		if r.Status != domain.RunStatusFinished {
			t.Fatalf("status filter leaked a %s run", r.Status)
		}
	}

	// Paginate the unfiltered list at size 2 → two pages of 2, no dupes.
	seen := map[string]bool{}
	token := ""
	for {
		page, next, err := s.Runs().ListRuns(ctx, exp.ID, domain.RunStatusUnspecified, domain.ListOptions{PageSize: 2, PageToken: token})
		if err != nil {
			t.Fatalf("page: %v", err)
		}
		for _, r := range page {
			if seen[r.ID] {
				t.Fatalf("duplicate run %q across pages", r.ID)
			}
			seen[r.ID] = true
		}
		if next == "" {
			break
		}
		token = next
	}
	if len(seen) != 4 {
		t.Fatalf("pagination saw %d runs, want 4", len(seen))
	}
}

// drainHistory pages GetMetricHistory to completion and returns every point — the
// same drain FinishRun does, so the test exercises the real multi-page contract.
func drainHistory(t *testing.T, ctx context.Context, repo *RunRepository, q domain.MetricHistoryQuery) []domain.MetricPoint {
	t.Helper()
	var all []domain.MetricPoint
	token := ""
	for {
		q.Pagination.PageToken = token
		pts, next, err := repo.GetMetricHistory(ctx, q)
		if err != nil {
			t.Fatalf("metric history: %v", err)
		}
		all = append(all, pts...)
		if next == "" || next == token {
			break
		}
		token = next
	}
	return all
}
