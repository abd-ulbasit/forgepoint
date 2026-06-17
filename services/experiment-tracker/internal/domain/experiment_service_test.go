// experiment_service_test.go — TDD specification for ExperimentService AND the
// pure pattern helpers (dedup, projection, state machine).
//
// ============================================================================
// WHY package domain_test (EXTERNAL test package)
// ============================================================================
//
// The tests exercise only the EXPORTED surface — exactly what the handler and
// the NATS consumer depend on. Using the external `_test` package keeps us
// honest (no reaching into unexported helpers) and mirrors the Auth service's
// choice. Hand-written mocks satisfy the domain PORTS (no codegen — every line
// is interview-explainable). An unset mock func panics if called, surfacing
// accidental coupling (a test hitting a repo method it didn't intend to).
//
// These tests assert REAL behavior — the dedup actually drops the right point,
// the state machine actually rejects the illegal edge, the final-metric
// projection actually picks the latest step, the publisher actually receives the
// computed finals — NOT mock call counts.
package domain_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/experiment-tracker/internal/domain"
	"github.com/google/uuid"
)

// ----------------------------------------------------------------------------
// Test clock — deterministic time so we can assert exact server-stamped values.
// ----------------------------------------------------------------------------

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

var testNow = time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)

// ----------------------------------------------------------------------------
// Hand-written mocks for the domain ports. Each is a struct of function fields;
// an unset field panics (catches unintended calls). State-holding mocks (the
// run repo) keep an in-memory map so we can assert REAL roundtrips: a run
// "created" in one call is "fetched" in the next, metrics appended actually
// accumulate, etc.
// ----------------------------------------------------------------------------

type mockExperimentRepo struct {
	createFn  func(ctx context.Context, e domain.Experiment) (domain.Experiment, error)
	getByIDFn func(ctx context.Context, id string) (domain.Experiment, error)
	updateFn  func(ctx context.Context, e domain.Experiment) (domain.Experiment, error)
	listFn    func(ctx context.Context, team string, includeArchived bool, opts domain.ListOptions) ([]domain.Experiment, string, error)
}

func (m *mockExperimentRepo) Create(ctx context.Context, e domain.Experiment) (domain.Experiment, error) {
	return m.createFn(ctx, e)
}
func (m *mockExperimentRepo) GetByID(ctx context.Context, id string) (domain.Experiment, error) {
	return m.getByIDFn(ctx, id)
}
func (m *mockExperimentRepo) Update(ctx context.Context, e domain.Experiment) (domain.Experiment, error) {
	return m.updateFn(ctx, e)
}
func (m *mockExperimentRepo) List(ctx context.Context, team string, includeArchived bool, opts domain.ListOptions) ([]domain.Experiment, string, error) {
	return m.listFn(ctx, team, includeArchived, opts)
}

// mockRunRepo holds real in-memory state so the service's behavior (dedup
// persisting, status transitions, final-metric writes) can be asserted against
// what actually landed, not against a captured argument.
type mockRunRepo struct {
	runs    map[string]domain.Run
	metrics map[string][]domain.MetricPoint // runID -> appended points (post-service-dedup)
	params  map[string][]domain.Param       // runID -> params

	// getMetricHistoryFn, when non-nil, OVERRIDES the default in-memory history
	// read. The default returns every stored point in ONE page (no truncation) so
	// most tests stay simple. The override lets a single test model a REAL
	// paginated adapter (page cap + nextToken cursor) to prove FinishRun drains
	// every page before computing finals — the truncation-bug regression test.
	getMetricHistoryFn func(ctx context.Context, q domain.MetricHistoryQuery) ([]domain.MetricPoint, string, error)
}

func newMockRunRepo() *mockRunRepo {
	return &mockRunRepo{
		runs:    make(map[string]domain.Run),
		metrics: make(map[string][]domain.MetricPoint),
		params:  make(map[string][]domain.Param),
	}
}

func (m *mockRunRepo) CreateRun(ctx context.Context, run domain.Run) (domain.Run, error) {
	m.runs[run.ID] = run
	return run, nil
}
func (m *mockRunRepo) GetRun(ctx context.Context, id string) (domain.Run, error) {
	r, ok := m.runs[id]
	if !ok {
		return domain.Run{}, domain.ErrRepoNotFound
	}
	// Attach accumulated params/finals the way a real adapter would on read.
	r.Params = m.params[id]
	return r, nil
}
func (m *mockRunRepo) ListRuns(ctx context.Context, experimentID string, statusFilter domain.RunStatus, opts domain.ListOptions) ([]domain.Run, string, error) {
	var out []domain.Run
	for _, r := range m.runs {
		if r.ExperimentID != experimentID {
			continue
		}
		if statusFilter != domain.RunStatusUnspecified && r.Status != statusFilter {
			continue
		}
		out = append(out, r)
	}
	return out, "", nil
}
func (m *mockRunRepo) UpdateRunStatus(ctx context.Context, runID string, status domain.RunStatus, endedAt time.Time, finalMetrics []domain.MetricPoint) (domain.Run, error) {
	r, ok := m.runs[runID]
	if !ok {
		return domain.Run{}, domain.ErrRepoNotFound
	}
	r.Status = status
	ea := endedAt
	r.EndedAt = &ea
	r.FinalMetrics = finalMetrics
	m.runs[runID] = r
	return r, nil
}
func (m *mockRunRepo) DeleteRun(ctx context.Context, runID string) error {
	if _, ok := m.runs[runID]; !ok {
		return domain.ErrRepoNotFound
	}
	delete(m.runs, runID)
	delete(m.metrics, runID)
	delete(m.params, runID)
	return nil
}
func (m *mockRunRepo) AppendMetrics(ctx context.Context, runID string, points []domain.MetricPoint) (int, error) {
	// Emulate the UNIQUE (run_id,key,step) index with ON CONFLICT DO NOTHING:
	// only points whose (key,step) is not already stored are written. This lets
	// the test prove CROSS-batch idempotency, not just intra-batch dedup.
	existing := make(map[[2]any]struct{})
	for _, p := range m.metrics[runID] {
		existing[[2]any{p.Key, p.Step}] = struct{}{}
	}
	written := 0
	for _, p := range points {
		k := [2]any{p.Key, p.Step}
		if _, dup := existing[k]; dup {
			continue
		}
		existing[k] = struct{}{}
		m.metrics[runID] = append(m.metrics[runID], p)
		written++
	}
	return written, nil
}
func (m *mockRunRepo) AppendParams(ctx context.Context, runID string, params []domain.Param) (int, error) {
	m.params[runID] = append(m.params[runID], params...)
	return len(params), nil
}
func (m *mockRunRepo) GetRunParams(ctx context.Context, runID string) ([]domain.Param, error) {
	return m.params[runID], nil
}
func (m *mockRunRepo) GetMetricHistory(ctx context.Context, q domain.MetricHistoryQuery) ([]domain.MetricPoint, string, error) {
	if m.getMetricHistoryFn != nil {
		return m.getMetricHistoryFn(ctx, q)
	}
	var out []domain.MetricPoint
	keyWanted := func(k string) bool {
		if len(q.Keys) == 0 {
			return true
		}
		for _, want := range q.Keys {
			if want == k {
				return true
			}
		}
		return false
	}
	for _, p := range m.metrics[q.RunID] {
		if !keyWanted(p.Key) {
			continue
		}
		if q.HasMinStep && p.Step < q.MinStep {
			continue
		}
		if q.HasMaxStep && p.Step > q.MaxStep {
			continue
		}
		out = append(out, p)
	}
	return out, "", nil
}
func (m *mockRunRepo) SetArtifacts(ctx context.Context, runID string, artifacts map[string]any) (domain.Run, error) {
	r, ok := m.runs[runID]
	if !ok {
		return domain.Run{}, domain.ErrRepoNotFound
	}
	r.Artifacts = artifacts
	m.runs[runID] = r
	return r, nil
}

// mockIdempotencyStore is a simple in-memory (operation,key)->id map.
type mockIdempotencyStore struct {
	recorded map[[2]string]string
}

func newMockIdempotencyStore() *mockIdempotencyStore {
	return &mockIdempotencyStore{recorded: make(map[[2]string]string)}
}
func (m *mockIdempotencyStore) Lookup(ctx context.Context, operation, key string) (string, bool, error) {
	id, ok := m.recorded[[2]string{operation, key}]
	return id, ok, nil
}
func (m *mockIdempotencyStore) Record(ctx context.Context, operation, key, resultID string) error {
	m.recorded[[2]string{operation, key}] = resultID
	return nil
}

// mockPublisher captures emitted events so tests assert the RIGHT payload
// (e.g. RunFinished carries the computed final metrics), not just "publish was
// called".
type mockPublisher struct {
	created  []domain.RunCreatedEvent
	finished []domain.RunFinishedEvent
	failNext bool // simulate a transient publish failure (must NOT fail the op)
}

func (m *mockPublisher) PublishRunCreated(ctx context.Context, ev domain.RunCreatedEvent) error {
	if m.failNext {
		m.failNext = false
		return errors.New("simulated publish failure")
	}
	m.created = append(m.created, ev)
	return nil
}
func (m *mockPublisher) PublishRunFinished(ctx context.Context, ev domain.RunFinishedEvent) error {
	if m.failNext {
		m.failNext = false
		return errors.New("simulated publish failure")
	}
	m.finished = append(m.finished, ev)
	return nil
}

// Compile-time proof the mocks satisfy the ports. If a signature drifts, the
// package fails to build — catching interface skew at test time.
var (
	_ domain.ExperimentRepository = (*mockExperimentRepo)(nil)
	_ domain.RunRepository        = (*mockRunRepo)(nil)
	_ domain.IdempotencyStore     = (*mockIdempotencyStore)(nil)
	_ domain.EventPublisher       = (*mockPublisher)(nil)
)

// testActor is the server-authoritative caller identity used across tests.
var testActor = domain.Actor{UserID: "user-1", Team: "risk"}

// harness bundles the mocks + the service under test so each test can tweak one
// behavior without re-wiring the whole graph.
type harness struct {
	expRepo *mockExperimentRepo
	runRepo *mockRunRepo
	idem    *mockIdempotencyStore
	pub     *mockPublisher
	svc     domain.ExperimentService
}

func newHarness() *harness {
	expRepo := &mockExperimentRepo{}
	runRepo := newMockRunRepo()
	idem := newMockIdempotencyStore()
	pub := &mockPublisher{}
	svc := domain.NewExperimentService(expRepo, runRepo, idem, pub, fixedClock{t: testNow})
	return &harness{expRepo: expRepo, runRepo: runRepo, idem: idem, pub: pub, svc: svc}
}

// seedExperiment wires the experiment repo to return a team-owned experiment for
// a given id (used by run tests that need a valid parent).
func (h *harness) seedExperiment(id string) {
	h.expRepo.getByIDFn = func(ctx context.Context, gotID string) (domain.Experiment, error) {
		if gotID != id {
			return domain.Experiment{}, domain.ErrRepoNotFound
		}
		return domain.Experiment{ID: id, Name: "exp", Team: testActor.Team, OwnerID: testActor.UserID}, nil
	}
}

// startRun is a helper that opens a run for the seeded experiment.
func (h *harness) startRun(t *testing.T, expID string) domain.Run {
	t.Helper()
	run, err := h.svc.StartRun(context.Background(), testActor, domain.StartRunInput{ExperimentID: expID})
	if err != nil {
		t.Fatalf("StartRun: unexpected error: %v", err)
	}
	return run
}

// ============================================================================
// PURE HELPERS — the pattern's correctness core (no service, no mocks).
// ============================================================================

func TestDedupMetricPoints_FirstWinsPerKeyStep(t *testing.T) {
	in := []domain.MetricPoint{
		{Key: "loss", Step: 1, Value: 0.9},
		{Key: "loss", Step: 1, Value: 0.5}, // duplicate (key,step) — must be dropped, first wins
		{Key: "loss", Step: 2, Value: 0.4},
		{Key: "acc", Step: 1, Value: 0.6}, // different key, same step — kept
	}
	out := domain.DedupMetricPoints(in)
	if len(out) != 3 {
		t.Fatalf("expected 3 surviving points, got %d: %+v", len(out), out)
	}
	// First (loss,1) value (0.9) must win — proves first-wins, not last-wins.
	if out[0].Key != "loss" || out[0].Step != 1 || out[0].Value != 0.9 {
		t.Fatalf("expected first (loss,1)=0.9 to win, got %+v", out[0])
	}
}

func TestComputeFinalMetrics_LatestStepWins(t *testing.T) {
	history := []domain.MetricPoint{
		{Key: "acc", Step: 1, Value: 0.50, Timestamp: testNow},
		{Key: "acc", Step: 3, Value: 0.94, Timestamp: testNow}, // highest step → headline
		{Key: "acc", Step: 2, Value: 0.80, Timestamp: testNow},
		{Key: "loss", Step: 2, Value: 0.10, Timestamp: testNow},
		{Key: "loss", Step: 1, Value: 0.90, Timestamp: testNow},
	}
	finals := domain.ComputeFinalMetrics(history)
	got := make(map[string]float64)
	for _, p := range finals {
		got[p.Key] = p.Value
	}
	if got["acc"] != 0.94 {
		t.Fatalf("acc headline = latest step value 0.94, got %v", got["acc"])
	}
	if got["loss"] != 0.10 {
		t.Fatalf("loss headline = latest step value 0.10, got %v", got["loss"])
	}
}

func TestRunStatus_StateMachine(t *testing.T) {
	cases := []struct {
		from, to domain.RunStatus
		ok       bool
	}{
		{domain.RunStatusRunning, domain.RunStatusFinished, true},
		{domain.RunStatusRunning, domain.RunStatusFailed, true},
		{domain.RunStatusRunning, domain.RunStatusKilled, true},
		{domain.RunStatusRunning, domain.RunStatusRunning, false},     // can't go to RUNNING
		{domain.RunStatusFinished, domain.RunStatusRunning, false},    // terminal is absorbing
		{domain.RunStatusFinished, domain.RunStatusFailed, false},     // can't re-terminate
		{domain.RunStatusRunning, domain.RunStatusUnspecified, false}, // never to unspecified
	}
	for _, c := range cases {
		if got := c.from.CanTransitionTo(c.to); got != c.ok {
			t.Errorf("CanTransitionTo(%q->%q)=%v, want %v", c.from, c.to, got, c.ok)
		}
	}
}

// ============================================================================
// SERVICE — batch ingestion (the pattern centerpiece).
// ============================================================================

func TestLogMetrics_DedupsAndStampsTimestamps(t *testing.T) {
	h := newHarness()
	h.seedExperiment("exp-1")
	run := h.startRun(t, "exp-1")

	res, err := h.svc.LogMetrics(context.Background(), testActor, domain.LogMetricsInput{
		RunID: run.ID,
		Points: []domain.MetricPoint{
			{Key: "loss", Step: 1, Value: 0.9, Timestamp: time.Unix(0, 0)}, // client ts must be overwritten
			{Key: "loss", Step: 1, Value: 0.5},                             // dup (loss,1) -> dropped
			{Key: "loss", Step: 2, Value: 0.4},
		},
	})
	if err != nil {
		t.Fatalf("LogMetrics: %v", err)
	}
	if res.AcceptedCount != 2 {
		t.Fatalf("expected 2 accepted (one dup dropped), got %d", res.AcceptedCount)
	}
	stored := h.runRepo.metrics[run.ID]
	if len(stored) != 2 {
		t.Fatalf("expected 2 stored points, got %d", len(stored))
	}
	for _, p := range stored {
		if !p.Timestamp.Equal(testNow) {
			t.Errorf("point %+v: timestamp not server-stamped to %v", p, testNow)
		}
	}
}

func TestLogMetrics_IdempotentReplaySkipsRewrite(t *testing.T) {
	h := newHarness()
	h.seedExperiment("exp-1")
	run := h.startRun(t, "exp-1")

	in := domain.LogMetricsInput{
		RunID:          run.ID,
		IdempotencyKey: uuid.NewString(),
		Points: []domain.MetricPoint{
			{Key: "loss", Step: 1, Value: 0.9},
			{Key: "loss", Step: 2, Value: 0.4},
		},
	}
	first, err := h.svc.LogMetrics(context.Background(), testActor, in)
	if err != nil {
		t.Fatalf("first LogMetrics: %v", err)
	}
	if first.AcceptedCount != 2 {
		t.Fatalf("first call accepted %d, want 2", first.AcceptedCount)
	}
	// Replay with the SAME idempotency key: the service must NOT re-write the
	// batch (accepted 0) and the store must still hold exactly 2 points.
	second, err := h.svc.LogMetrics(context.Background(), testActor, in)
	if err != nil {
		t.Fatalf("replay LogMetrics: %v", err)
	}
	if second.AcceptedCount != 0 {
		t.Fatalf("idempotent replay accepted %d, want 0", second.AcceptedCount)
	}
	if len(h.runRepo.metrics[run.ID]) != 2 {
		t.Fatalf("after replay store has %d points, want 2 (no double write)", len(h.runRepo.metrics[run.ID]))
	}
}

func TestLogMetrics_RejectsTerminalRun(t *testing.T) {
	h := newHarness()
	h.seedExperiment("exp-1")
	run := h.startRun(t, "exp-1")
	if _, err := h.svc.FinishRun(context.Background(), testActor, domain.FinishRunInput{RunID: run.ID, TargetStatus: domain.RunStatusFinished}); err != nil {
		t.Fatalf("FinishRun: %v", err)
	}
	_, err := h.svc.LogMetrics(context.Background(), testActor, domain.LogMetricsInput{
		RunID:  run.ID,
		Points: []domain.MetricPoint{{Key: "loss", Step: 9, Value: 0.1}},
	})
	if !errors.Is(err, domain.ErrRunNotRunning) {
		t.Fatalf("logging to a FINISHED run: got %v, want ErrRunNotRunning", err)
	}
}

func TestLogMetrics_RejectsOversizeBatch(t *testing.T) {
	h := newHarness()
	h.seedExperiment("exp-1")
	run := h.startRun(t, "exp-1")
	big := make([]domain.MetricPoint, domain.MaxMetricsPerBatch+1)
	for i := range big {
		big[i] = domain.MetricPoint{Key: "loss", Step: int64(i), Value: float64(i)}
	}
	_, err := h.svc.LogMetrics(context.Background(), testActor, domain.LogMetricsInput{RunID: run.ID, Points: big})
	if !errors.Is(err, domain.ErrBatchTooLarge) {
		t.Fatalf("oversize batch: got %v, want ErrBatchTooLarge", err)
	}
}

func TestLogMetrics_RejectsNonFiniteValue(t *testing.T) {
	h := newHarness()
	h.seedExperiment("exp-1")
	run := h.startRun(t, "exp-1")
	inf := 1.0
	for i := 0; i < 400; i++ { // build +Inf without importing math in the test
		inf *= 1e308
	}
	_, err := h.svc.LogMetrics(context.Background(), testActor, domain.LogMetricsInput{
		RunID:  run.ID,
		Points: []domain.MetricPoint{{Key: "loss", Step: 1, Value: inf}},
	})
	if !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("non-finite metric: got %v, want ErrValidation", err)
	}
}

// ============================================================================
// SERVICE — run lifecycle + events.
// ============================================================================

func TestStartRun_SetsServerAuthoritativeFieldsAndPublishes(t *testing.T) {
	h := newHarness()
	h.seedExperiment("exp-1")

	run, err := h.svc.StartRun(context.Background(), testActor, domain.StartRunInput{
		ExperimentID: "exp-1",
		DisplayName:  "lr=0.01",
		Params:       []domain.Param{{Key: "lr", Value: "0.01"}},
	})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if run.ID == "" {
		t.Fatal("run id must be server-generated")
	}
	if run.Status != domain.RunStatusRunning {
		t.Fatalf("new run status = %q, want RUNNING", run.Status)
	}
	if run.Source != domain.RunSourceAPI {
		t.Fatalf("sync StartRun source = %q, want API", run.Source)
	}
	if run.OwnerID != testActor.UserID {
		t.Fatalf("owner = %q, want server-set %q", run.OwnerID, testActor.UserID)
	}
	if !run.StartedAt.Equal(testNow) {
		t.Fatalf("startedAt = %v, want server clock %v", run.StartedAt, testNow)
	}
	if len(h.pub.created) != 1 || h.pub.created[0].RunID != run.ID {
		t.Fatalf("expected one RunCreated event for run %s, got %+v", run.ID, h.pub.created)
	}
}

func TestStartRun_RejectsForeignTeamExperiment(t *testing.T) {
	h := newHarness()
	// Experiment exists but belongs to ANOTHER team — tenancy guard must reject
	// with NotFound (not "permission denied", to avoid leaking existence).
	h.expRepo.getByIDFn = func(ctx context.Context, id string) (domain.Experiment, error) {
		return domain.Experiment{ID: id, Team: "other-team"}, nil
	}
	_, err := h.svc.StartRun(context.Background(), testActor, domain.StartRunInput{ExperimentID: "exp-x"})
	if !errors.Is(err, domain.ErrExperimentNotFound) {
		t.Fatalf("foreign-team experiment: got %v, want ErrExperimentNotFound", err)
	}
}

func TestStartRun_IdempotentReplayReturnsSameRun(t *testing.T) {
	h := newHarness()
	h.seedExperiment("exp-1")
	key := uuid.NewString()
	in := domain.StartRunInput{ExperimentID: "exp-1", IdempotencyKey: key}

	first, err := h.svc.StartRun(context.Background(), testActor, in)
	if err != nil {
		t.Fatalf("first StartRun: %v", err)
	}
	second, err := h.svc.StartRun(context.Background(), testActor, in)
	if err != nil {
		t.Fatalf("replay StartRun: %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("idempotent StartRun returned different ids: %s vs %s", first.ID, second.ID)
	}
	// Exactly one run created, exactly one RunCreated event published.
	if len(h.runRepo.runs) != 1 {
		t.Fatalf("expected 1 run after idempotent replay, got %d", len(h.runRepo.runs))
	}
	if len(h.pub.created) != 1 {
		t.Fatalf("expected 1 RunCreated event after idempotent replay, got %d", len(h.pub.created))
	}
}

func TestFinishRun_ComputesFinalsAndPublishesFatEvent(t *testing.T) {
	h := newHarness()
	h.seedExperiment("exp-1")
	run := h.startRun(t, "exp-1")

	// Log a small curve, then finish: the headline (latest-step) value must flow
	// into FinalMetrics AND into the published RunFinished fat event.
	if _, err := h.svc.LogMetrics(context.Background(), testActor, domain.LogMetricsInput{
		RunID: run.ID,
		Points: []domain.MetricPoint{
			{Key: "acc", Step: 1, Value: 0.5},
			{Key: "acc", Step: 2, Value: 0.94},
		},
	}); err != nil {
		t.Fatalf("LogMetrics: %v", err)
	}

	finished, err := h.svc.FinishRun(context.Background(), testActor, domain.FinishRunInput{
		RunID: run.ID, TargetStatus: domain.RunStatusFinished,
	})
	if err != nil {
		t.Fatalf("FinishRun: %v", err)
	}
	if finished.Status != domain.RunStatusFinished {
		t.Fatalf("status = %q, want FINISHED", finished.Status)
	}
	if finished.EndedAt == nil || !finished.EndedAt.Equal(testNow) {
		t.Fatalf("endedAt = %v, want server clock %v", finished.EndedAt, testNow)
	}
	headline := finalValue(finished.FinalMetrics, "acc")
	if headline != 0.94 {
		t.Fatalf("final acc = %v, want latest-step 0.94", headline)
	}
	if len(h.pub.finished) != 1 {
		t.Fatalf("expected 1 RunFinished event, got %d", len(h.pub.finished))
	}
	if v := finalValue(h.pub.finished[0].FinalMetrics, "acc"); v != 0.94 {
		t.Fatalf("RunFinished event final acc = %v, want 0.94 (fat event must carry finals)", v)
	}
}

// TestFinishRun_DrainsAllMetricPagesForFinals is the regression test for the
// truncation bug: FinishRun must fold the FULL series, not a single capped page.
//
// We model a REAL paginated adapter: history is ordered ascending by step (the
// port's documented (key, step, timestamp) order) and returned ONE point per
// page with a nextToken until exhausted. The LATEST step (the true headline) is
// on the LAST page. If FinishRun read only the first page, it would pick the
// early-step value (0.10) as "final acc" — the silent corruption. Draining all
// pages must yield the latest-step value (0.97) in BOTH the persisted finals and
// the fat RunFinished event.
func TestFinishRun_DrainsAllMetricPagesForFinals(t *testing.T) {
	h := newHarness()
	h.seedExperiment("exp-1")
	run := h.startRun(t, "exp-1")

	// The full curve, ascending by step. The headline is the LAST (highest-step)
	// point: acc@5 = 0.97. Note the values are NOT monotonic so "first page wins"
	// would visibly pick the wrong number.
	full := []domain.MetricPoint{
		{Key: "acc", Step: 1, Value: 0.10, Timestamp: testNow},
		{Key: "acc", Step: 2, Value: 0.40, Timestamp: testNow},
		{Key: "acc", Step: 3, Value: 0.80, Timestamp: testNow},
		{Key: "acc", Step: 4, Value: 0.90, Timestamp: testNow},
		{Key: "acc", Step: 5, Value: 0.97, Timestamp: testNow}, // highest step → headline, on the LAST page
	}

	// Paginating adapter: return exactly ONE point per call, using the page token
	// as an index cursor. This emulates a Postgres adapter that caps the page far
	// below the series length — the exact condition that truncated the old read.
	var pagesServed int
	h.runRepo.getMetricHistoryFn = func(ctx context.Context, q domain.MetricHistoryQuery) ([]domain.MetricPoint, string, error) {
		// Prove the service asks for an explicit (non-zero) page size, not the
		// zero-value Pagination the buggy code used.
		if q.Pagination.PageSize <= 0 {
			t.Fatalf("FinishRun must request an explicit page size when draining finals, got %d", q.Pagination.PageSize)
		}
		idx := 0
		if q.Pagination.PageToken != "" {
			n, err := strconv.Atoi(q.Pagination.PageToken)
			if err != nil {
				t.Fatalf("unexpected page token %q: %v", q.Pagination.PageToken, err)
			}
			idx = n
		}
		if idx >= len(full) {
			return nil, "", nil // exhausted
		}
		pagesServed++
		next := ""
		if idx+1 < len(full) {
			next = strconv.Itoa(idx + 1) // more pages remain
		}
		return []domain.MetricPoint{full[idx]}, next, nil
	}

	finished, err := h.svc.FinishRun(context.Background(), testActor, domain.FinishRunInput{
		RunID: run.ID, TargetStatus: domain.RunStatusFinished,
	})
	if err != nil {
		t.Fatalf("FinishRun: %v", err)
	}
	// All pages must have been visited (one per point here).
	if pagesServed != len(full) {
		t.Fatalf("FinishRun drained %d pages, want %d (must read the WHOLE series)", pagesServed, len(full))
	}
	// The persisted headline must be the LATEST-step value, not the first page's.
	if got := finalValue(finished.FinalMetrics, "acc"); got != 0.97 {
		t.Fatalf("persisted final acc = %v, want latest-step 0.97 (finals truncated to an early page?)", got)
	}
	// And the fat event leaderboards rank on must carry the same correct headline.
	if len(h.pub.finished) != 1 {
		t.Fatalf("expected 1 RunFinished event, got %d", len(h.pub.finished))
	}
	if got := finalValue(h.pub.finished[0].FinalMetrics, "acc"); got != 0.97 {
		t.Fatalf("RunFinished event final acc = %v, want 0.97 (fat event carried a truncated final)", got)
	}
}

func TestFinishRun_RejectsIllegalTransition(t *testing.T) {
	h := newHarness()
	h.seedExperiment("exp-1")
	run := h.startRun(t, "exp-1")
	if _, err := h.svc.FinishRun(context.Background(), testActor, domain.FinishRunInput{RunID: run.ID, TargetStatus: domain.RunStatusFinished}); err != nil {
		t.Fatalf("first FinishRun: %v", err)
	}
	// Second finish (FINISHED -> FAILED) is an illegal edge.
	_, err := h.svc.FinishRun(context.Background(), testActor, domain.FinishRunInput{RunID: run.ID, TargetStatus: domain.RunStatusFailed})
	if !errors.Is(err, domain.ErrInvalidStatusTransition) {
		t.Fatalf("re-terminating a run: got %v, want ErrInvalidStatusTransition", err)
	}
}

func TestFinishRun_RejectsNonTerminalTarget(t *testing.T) {
	h := newHarness()
	h.seedExperiment("exp-1")
	run := h.startRun(t, "exp-1")
	_, err := h.svc.FinishRun(context.Background(), testActor, domain.FinishRunInput{RunID: run.ID, TargetStatus: domain.RunStatusRunning})
	if !errors.Is(err, domain.ErrInvalidStatusTransition) {
		t.Fatalf("finishing to RUNNING: got %v, want ErrInvalidStatusTransition", err)
	}
}

func TestFinishRun_PublishFailureDoesNotFailOperation(t *testing.T) {
	h := newHarness()
	h.seedExperiment("exp-1")
	run := h.startRun(t, "exp-1")
	h.pub.failNext = true // the RunFinished publish will error
	finished, err := h.svc.FinishRun(context.Background(), testActor, domain.FinishRunInput{RunID: run.ID, TargetStatus: domain.RunStatusFinished})
	if err != nil {
		t.Fatalf("FinishRun must succeed despite publish failure, got %v", err)
	}
	if finished.Status != domain.RunStatusFinished {
		t.Fatalf("run should be FINISHED in the store regardless of publish, got %q", finished.Status)
	}
}

// ============================================================================
// SERVICE — params (write-once) + compare.
// ============================================================================

func TestLogParams_WriteOnceConflict(t *testing.T) {
	h := newHarness()
	h.seedExperiment("exp-1")
	run := h.startRun(t, "exp-1")

	if _, err := h.svc.LogParams(context.Background(), testActor, domain.LogParamsInput{
		RunID: run.ID, Params: []domain.Param{{Key: "lr", Value: "0.01"}},
	}); err != nil {
		t.Fatalf("first LogParams: %v", err)
	}
	// Re-log SAME key/value => idempotent no-op (0 accepted, no error).
	res, err := h.svc.LogParams(context.Background(), testActor, domain.LogParamsInput{
		RunID: run.ID, Params: []domain.Param{{Key: "lr", Value: "0.01"}},
	})
	if err != nil {
		t.Fatalf("idempotent re-log: %v", err)
	}
	if res.AcceptedCount != 0 {
		t.Fatalf("identical re-log accepted %d, want 0", res.AcceptedCount)
	}
	// Re-log SAME key, DIFFERENT value => conflict.
	_, err = h.svc.LogParams(context.Background(), testActor, domain.LogParamsInput{
		RunID: run.ID, Params: []domain.Param{{Key: "lr", Value: "0.99"}},
	})
	if !errors.Is(err, domain.ErrParamConflict) {
		t.Fatalf("changed param value: got %v, want ErrParamConflict", err)
	}
}

// TestSetRunArtifacts_RejectsOversizeNestedPayload is the regression test for the
// size-cap bypass. The old heuristic charged a flat 32 bytes for ANY non-string
// value, so a single key holding a multi-million-element array (tens of MB once
// serialized) was counted as ~40 bytes and slipped past the 256 KiB cap. The fix
// measures the ACTUAL serialized (JSON) bytes, so a nested array/map that exceeds
// the cap is rejected with ErrValidation — the domain is the only guard until the
// handler's precise wire check is wired, so this MUST hold here.
func TestSetRunArtifacts_RejectsOversizeNestedPayload(t *testing.T) {
	h := newHarness()
	h.seedExperiment("exp-1")
	run := h.startRun(t, "exp-1")

	// A single key whose value is a large array. Each number serializes to several
	// bytes, so 5,000,000 elements is tens of MB — far over 256 KiB. The old
	// heuristic scored this as ~40 bytes (one non-string value = 32). Build it big
	// enough that real serialized bytes blow the cap but the per-entry heuristic
	// would not.
	huge := make([]any, 5_000_000)
	for i := range huge {
		huge[i] = i // ints serialize to multiple digits each
	}
	_, err := h.svc.SetRunArtifacts(context.Background(), testActor, domain.SetArtifactsInput{
		RunID:     run.ID,
		Artifacts: map[string]any{"blob": huge},
	})
	if !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("oversize nested artifacts: got %v, want ErrValidation (size cap must count nested payloads)", err)
	}
	// Defense in depth: nothing oversized should have reached the store.
	if h.runRepo.runs[run.ID].Artifacts != nil {
		t.Fatalf("oversize artifacts must NOT be persisted, but run has artifacts set")
	}
}

// TestSetRunArtifacts_AcceptsSmallPayload proves the cap doesn't over-reject: a
// modest nested artifacts map (well under 256 KiB) is accepted and persisted.
// Without this, a too-strict size check could pass the rejection test while
// breaking the happy path.
func TestSetRunArtifacts_AcceptsSmallPayload(t *testing.T) {
	h := newHarness()
	h.seedExperiment("exp-1")
	run := h.startRun(t, "exp-1")

	artifacts := map[string]any{
		"confusion_matrix": []any{[]any{10, 2}, []any{1, 20}}, // nested, but tiny
		"notes":            "best run so far",
	}
	updated, err := h.svc.SetRunArtifacts(context.Background(), testActor, domain.SetArtifactsInput{
		RunID:     run.ID,
		Artifacts: artifacts,
	})
	if err != nil {
		t.Fatalf("small artifacts must be accepted, got %v", err)
	}
	if updated.Artifacts == nil || updated.Artifacts["notes"] != "best run so far" {
		t.Fatalf("artifacts not persisted as expected: %+v", updated.Artifacts)
	}
}

func TestCompareRuns_ReturnsPerRunSeries(t *testing.T) {
	h := newHarness()
	h.seedExperiment("exp-1")
	a := h.startRun(t, "exp-1")
	b := h.startRun(t, "exp-1")
	mustLog(t, h, a.ID, []domain.MetricPoint{{Key: "acc", Step: 1, Value: 0.7}, {Key: "acc", Step: 2, Value: 0.8}})
	mustLog(t, h, b.ID, []domain.MetricPoint{{Key: "acc", Step: 1, Value: 0.6}})

	cmps, err := h.svc.CompareRuns(context.Background(), testActor, domain.CompareRunsInput{
		RunIDs: []string{a.ID, b.ID}, MetricKeys: []string{"acc"},
	})
	if err != nil {
		t.Fatalf("CompareRuns: %v", err)
	}
	if len(cmps) != 2 {
		t.Fatalf("expected 2 comparisons, got %d", len(cmps))
	}
	// Order preserved: first comparison is run a with 2 acc points.
	if cmps[0].Run.ID != a.ID {
		t.Fatalf("comparison order not preserved: first is %s, want %s", cmps[0].Run.ID, a.ID)
	}
	if len(cmps[0].Series) != 1 || cmps[0].Series[0].Key != "acc" || len(cmps[0].Series[0].Points) != 2 {
		t.Fatalf("run a acc series wrong: %+v", cmps[0].Series)
	}
}

func TestCompareRuns_RejectsTooManyRuns(t *testing.T) {
	h := newHarness()
	ids := make([]string, domain.MaxCompareRuns+1)
	for i := range ids {
		ids[i] = uuid.NewString()
	}
	_, err := h.svc.CompareRuns(context.Background(), testActor, domain.CompareRunsInput{RunIDs: ids})
	if !errors.Is(err, domain.ErrTooManyRuns) {
		t.Fatalf("oversize compare: got %v, want ErrTooManyRuns", err)
	}
}

func TestCreateExperiment_SetsOwnerAndTeamFromActor(t *testing.T) {
	h := newHarness()
	h.expRepo.createFn = func(ctx context.Context, e domain.Experiment) (domain.Experiment, error) {
		return e, nil // echo back what the service constructed
	}
	exp, err := h.svc.CreateExperiment(context.Background(), testActor, domain.CreateExperimentInput{Name: "sweep"})
	if err != nil {
		t.Fatalf("CreateExperiment: %v", err)
	}
	if exp.OwnerID != testActor.UserID || exp.Team != testActor.Team {
		t.Fatalf("owner/team must come from actor: got owner=%q team=%q", exp.OwnerID, exp.Team)
	}
	if exp.ID == "" || !exp.CreatedAt.Equal(testNow) {
		t.Fatalf("id/createdAt must be server-set: id=%q createdAt=%v", exp.ID, exp.CreatedAt)
	}
}

// ----------------------------------------------------------------------------
// small test helpers
// ----------------------------------------------------------------------------

func finalValue(points []domain.MetricPoint, key string) float64 {
	for _, p := range points {
		if p.Key == key {
			return p.Value
		}
	}
	return -1
}

func mustLog(t *testing.T, h *harness, runID string, points []domain.MetricPoint) {
	t.Helper()
	if _, err := h.svc.LogMetrics(context.Background(), testActor, domain.LogMetricsInput{RunID: runID, Points: points}); err != nil {
		t.Fatalf("mustLog: %v", err)
	}
}
