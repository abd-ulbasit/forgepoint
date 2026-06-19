package domain

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ============================================================================
// FAKES specific to the L4 quality-eval service (the shared monitor/report/gate/
// orch/publisher fakes live in mocks_test.go and are reused here).
// ============================================================================

// fakeJudge returns a programmable score. It also COUNTS calls so the sampling-at-
// the-consumer behavior and the "unscored short-circuits" behavior are assertable.
type fakeJudge struct {
	mu     sync.Mutex
	scores JudgeScores
	calls  int
}

func (j *fakeJudge) Score(_ context.Context, _ Completion) (JudgeScores, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.calls++
	return j.scores, nil
}

func (j *fakeJudge) callCount() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.calls
}

// fakeEvalStore is an in-memory EvalStore with REAL request_id idempotency and REAL
// (team, model) tenant partitioning — exactly the invariants the Postgres adapter
// must hold. Keying by request_id (idempotency) and by (team|model) for the rolling
// read mirrors the production schema (UNIQUE request_id; WHERE team=$1 AND model=$2).
type fakeEvalStore struct {
	mu       sync.Mutex
	byReq    map[string]Eval      // request_id → eval (idempotency)
	byScope  map[string][]float64 // team|model → SCORED overall scores, newest last
	recorded []Eval               // every Record call's eval (for assertions)
}

func newFakeEvalStore() *fakeEvalStore {
	return &fakeEvalStore{byReq: map[string]Eval{}, byScope: map[string][]float64{}}
}

func evalScope(team, model string) string { return team + "|" + model }

func (s *fakeEvalStore) Record(_ context.Context, e Eval) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// IDEMPOTENCY on request_id: a redelivery is a no-op (no double-count).
	if _, ok := s.byReq[e.RequestID]; ok {
		return nil
	}
	s.byReq[e.RequestID] = e
	s.recorded = append(s.recorded, e)
	// Only SCORED evals contribute to the rolling overall (the adapter's WHERE
	// scored = TRUE). Unscored rows are stored but excluded from the average.
	if e.Scores.Scored {
		k := evalScope(e.Team, e.Model)
		s.byScope[k] = append(s.byScope[k], e.Scores.Overall)
	}
	return nil
}

func (s *fakeEvalStore) RecentOverall(_ context.Context, team, model string, limit int) ([]float64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	all := s.byScope[evalScope(team, model)]
	// Newest-first, capped at limit (mirrors ORDER BY created_at DESC LIMIT N).
	out := make([]float64, 0, len(all))
	for i := len(all) - 1; i >= 0 && len(out) < limit; i-- {
		out = append(out, all[i])
	}
	return out, nil
}

func (s *fakeEvalStore) recordedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.recorded)
}

// ListScores is the in-memory twin of the Postgres adapter's keyset read: it filters by
// Team (mandatory), optional Model and Since, sorts NEWEST-FIRST by (created_at,
// request_id) — the exact total order the adapter's index gives — and pages via the
// opaque cursor. It includes BOTH scored and unscored rows (the dashboard surfaces
// un-judgeable traffic), unlike RecentOverall. Keying the filter by Team is what makes
// the team-isolation test real: a row for another team is simply never selected.
func (s *fakeEvalStore) ListScores(_ context.Context, f EvalFilter, opts ListOptions) ([]Eval, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. FILTER on the same predicates the SQL WHERE applies (team mandatory; model/since
	// optional). We never match on model alone — team always scopes first.
	matched := make([]Eval, 0, len(s.recorded))
	for _, e := range s.recorded {
		if e.Team != f.Team {
			continue
		}
		if f.Model != "" && e.Model != f.Model {
			continue
		}
		if !f.Since.IsZero() && e.CreatedAt.Before(f.Since) {
			continue
		}
		matched = append(matched, e)
	}

	// 2. SORT newest-first by (created_at, request_id) — the adapter's ORDER BY
	// created_at DESC, request_id DESC. request_id is the tiebreaker (total order).
	sort.Slice(matched, func(i, j int) bool {
		if !matched[i].CreatedAt.Equal(matched[j].CreatedAt) {
			return matched[i].CreatedAt.After(matched[j].CreatedAt)
		}
		return matched[i].RequestID > matched[j].RequestID
	})

	// 3. APPLY the keyset cursor: drop everything at-or-after the previous page's last
	// (created_at, request_id) — i.e. keep rows strictly BEFORE it in the DESC order.
	if opts.PageToken != "" {
		ts, id, ok := decodeFakeEvalCursor(opts.PageToken)
		if !ok {
			return nil, "", fmt.Errorf("%w: bad eval page token", ErrValidation)
		}
		filtered := matched[:0:0]
		for _, e := range matched {
			if e.CreatedAt.Before(ts) || (e.CreatedAt.Equal(ts) && e.RequestID < id) {
				filtered = append(filtered, e)
			}
		}
		matched = filtered
	}

	// 4. PAGE: take PageSize, mint a cursor only when more rows remain (mirrors the
	// adapter's +1 sentinel). PageSize is already defaulted/capped by the service.
	size := opts.PageSize
	if size <= 0 {
		size = DefaultListPageSize
	}
	var next string
	if len(matched) > size {
		last := matched[size-1]
		next = encodeFakeEvalCursor(last.CreatedAt, last.RequestID)
		matched = matched[:size]
	}
	return matched, next, nil
}

// encodeFakeEvalCursor / decodeFakeEvalCursor are a trivial test-only cursor codec (the
// real adapter uses base64url JSON; the fake just needs a round-trippable token to prove
// the service forwards cursors correctly across pages).
func encodeFakeEvalCursor(ts time.Time, id string) string {
	return strconv.FormatInt(ts.UnixNano(), 10) + "|" + id
}

func decodeFakeEvalCursor(tok string) (time.Time, string, bool) {
	i := strings.IndexByte(tok, '|')
	if i < 0 {
		return time.Time{}, "", false
	}
	ns, err := strconv.ParseInt(tok[:i], 10, 64)
	if err != nil {
		return time.Time{}, "", false
	}
	return time.Unix(0, ns), tok[i+1:], true
}

// qualityHarness bundles a wired QualityEvalService + its fakes.
type qualityHarness struct {
	svc       QualityEvalService
	judge     *fakeJudge
	evals     *fakeEvalStore
	monitors  *fakeMonitorRepo
	reports   *fakeReportRepo
	publisher *fakePublisher
	orch      *fakeOrchestrator
	gate      *fakeGate
	clock     *fakeClock
}

func newQualityHarness(t *testing.T, cfg QualityDriftConfig, scores JudgeScores, monitor Monitor) *qualityHarness {
	t.Helper()
	h := &qualityHarness{
		judge:     &fakeJudge{scores: scores},
		evals:     newFakeEvalStore(),
		monitors:  newFakeMonitorRepo(),
		reports:   newFakeReportRepo(),
		publisher: &fakePublisher{},
		orch:      &fakeOrchestrator{},
		gate:      newFakeGate(),
		clock:     &fakeClock{t: time.Unix(2_000_000, 0)},
	}
	// Seed the monitor (its GetByID is how the service loads owner_team + auto_retrain).
	if _, _, err := h.monitors.Upsert(context.Background(), monitor); err != nil {
		t.Fatalf("seed monitor: %v", err)
	}
	h.svc = NewQualityEvalService(QualityEvalDeps{
		Judge:        h.judge,
		Evals:        h.evals,
		Monitors:     h.monitors,
		Reports:      h.reports,
		Publisher:    h.publisher,
		Orch:         h.orch,
		Gate:         h.gate,
		Config:       cfg,
		WarnDrop:     0.5,
		CriticalDrop: 1.0,
		Cooldown:     time.Hour,
		Now:          h.clock.now,
	})
	return h
}

// healthyMonitor returns a monitor with auto_retrain ON + a pipeline, so a CRITICAL
// quality drift can fire the loop.
func qualityMonitor(team, model string, autoRetrain bool) Monitor {
	return Monitor{
		ID:                "mon-" + model,
		ModelName:         model,
		OwnerTeam:         team,
		State:             MonitorStateActive,
		AutoRetrain:       autoRetrain,
		RetrainPipelineID: "pipe-retrain",
	}
}

func goodScores() JudgeScores {
	return JudgeScores{Relevance: 5, Coherence: 5, Safety: 5}.Finalize() // overall 5
}

func badScores() JudgeScores {
	return JudgeScores{Relevance: 2, Coherence: 2, Safety: 2}.Finalize() // overall 2
}

// ============================================================================
// TESTS
// ============================================================================

// A window of low judged scores drives the rolling average below the floor → a
// CRITICAL quality-drift report fires the EXISTING emit + retrain loop. This is the
// end-to-end "quality drift reuses the alert/retrain loop" proof.
func TestQualityEval_LowScores_FireRetrainLoop(t *testing.T) {
	t.Parallel()
	cfg := QualityDriftConfig{Window: 10, MinSamples: 5, FloorScore: 3.0}
	h := newQualityHarness(t, cfg, badScores(), qualityMonitor("team-a", "chatbot", true))
	ctx := context.Background()

	var lastReport *DriftReport
	for i := 0; i < 10; i++ {
		r, err := h.svc.EvaluateCompletion(ctx, "mon-chatbot", Completion{
			Team: "team-a", Model: "chatbot", RequestID: reqID(i), Response: "an answer",
		})
		if err != nil {
			t.Fatalf("EvaluateCompletion[%d]: %v", i, err)
		}
		if r != nil {
			lastReport = r
		}
	}

	// Once the window reaches MinSamples (5) with avg 2.0 (< floor 3.0), drift fires.
	if lastReport == nil {
		t.Fatalf("expected a quality-drift report once the window filled")
	}
	// It is a PERFORMANCE-type report named "llm_quality" — the reuse of the existing
	// drift type/schema (no new proto/enum).
	if lastReport.DriftType != DriftTypePerformance {
		t.Fatalf("report drift type = %v, want DriftTypePerformance", lastReport.DriftType)
	}
	if len(lastReport.Metrics) != 1 || lastReport.Metrics[0].Name != qualityMetricName {
		t.Fatalf("expected single %q metric, got %+v", qualityMetricName, lastReport.Metrics)
	}
	// avg 2.0 vs floor 3.0 → drop 1.0 ⇒ CRITICAL (criticalDrop 1.0) ⇒ retrain armed.
	if lastReport.Severity != DriftSeverityCritical {
		t.Fatalf("severity = %v, want CRITICAL", lastReport.Severity)
	}
	if h.publisher.count() == 0 {
		t.Fatalf("expected a drift event emitted through the shared publisher")
	}
	if h.orch.count() == 0 {
		t.Fatalf("expected a retrain trigger through the shared orchestrator (closed loop)")
	}
}

// Healthy scores never raise a report and never fire the loop.
func TestQualityEval_HealthyScores_NoReport(t *testing.T) {
	t.Parallel()
	cfg := QualityDriftConfig{Window: 10, MinSamples: 5, FloorScore: 3.0}
	h := newQualityHarness(t, cfg, goodScores(), qualityMonitor("team-a", "chatbot", true))
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		r, err := h.svc.EvaluateCompletion(ctx, "mon-chatbot", Completion{
			Team: "team-a", Model: "chatbot", RequestID: reqID(i), Response: "great answer",
		})
		if err != nil {
			t.Fatalf("EvaluateCompletion[%d]: %v", i, err)
		}
		if r != nil {
			t.Fatalf("healthy scores must not raise a report, got %+v", r)
		}
	}
	if h.publisher.count() != 0 || h.orch.count() != 0 {
		t.Fatalf("healthy scores must not emit/trigger: emit=%d trigger=%d", h.publisher.count(), h.orch.count())
	}
}

// A redelivered completion (same request_id) records the eval ONCE and, even if a
// drift report is raised, emits the drift event at most once (window_id idempotency).
func TestQualityEval_IdempotentConsume(t *testing.T) {
	t.Parallel()
	cfg := QualityDriftConfig{Window: 3, MinSamples: 1, FloorScore: 3.0}
	h := newQualityHarness(t, cfg, badScores(), qualityMonitor("team-a", "chatbot", false))
	ctx := context.Background()

	// First delivery of request "r-1": records + (MinSamples=1, avg 2.0 < floor) raises
	// a report and emits (auto_retrain off, so no trigger).
	r1, err := h.svc.EvaluateCompletion(ctx, "mon-chatbot", Completion{
		Team: "team-a", Model: "chatbot", RequestID: "r-1", Response: "x",
	})
	if err != nil {
		t.Fatalf("first deliver: %v", err)
	}
	if r1 == nil {
		t.Fatalf("expected a report on first delivery")
	}
	emitAfterFirst := h.publisher.count()
	recordedAfterFirst := h.evals.recordedCount()

	// REDELIVERY of the SAME request_id: eval Record is a no-op (idempotent), and the
	// report Save collides on the synthesized window_id (llmq-r-1) → inserted=false →
	// NO second emit.
	if _, err := h.svc.EvaluateCompletion(ctx, "mon-chatbot", Completion{
		Team: "team-a", Model: "chatbot", RequestID: "r-1", Response: "x",
	}); err != nil {
		t.Fatalf("redeliver: %v", err)
	}
	if got := h.evals.recordedCount(); got != recordedAfterFirst {
		t.Fatalf("redelivery double-recorded the eval: %d → %d", recordedAfterFirst, got)
	}
	if got := h.publisher.count(); got != emitAfterFirst {
		t.Fatalf("redelivery re-emitted the drift event: %d → %d (must be exactly-once)", emitAfterFirst, got)
	}
}

// An UNSCORED judgment (judge failed / no text) is recorded but never contributes to
// the average and never raises drift — a parse failure must not read as low quality.
func TestQualityEval_UnscoredRecordedButNoDrift(t *testing.T) {
	t.Parallel()
	cfg := QualityDriftConfig{Window: 10, MinSamples: 3, FloorScore: 3.0}
	h := newQualityHarness(t, cfg, Unscored(), qualityMonitor("team-a", "chatbot", true))
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		r, err := h.svc.EvaluateCompletion(ctx, "mon-chatbot", Completion{
			Team: "team-a", Model: "chatbot", RequestID: reqID(i), Response: "ans",
		})
		if err != nil {
			t.Fatalf("EvaluateCompletion[%d]: %v", i, err)
		}
		if r != nil {
			t.Fatalf("unscored evals must not raise a report, got %+v", r)
		}
	}
	// All 10 were RECORDED (observability of un-judgeable volume) ...
	if got := h.evals.recordedCount(); got != 10 {
		t.Fatalf("expected 10 unscored evals recorded, got %d", got)
	}
	// ... but NONE contributed to the rolling average (and so no drift fired).
	if scored, _ := h.evals.RecentOverall(ctx, "team-a", "chatbot", 10); len(scored) != 0 {
		t.Fatalf("unscored evals must not enter the rolling average, got %d", len(scored))
	}
	if h.publisher.count() != 0 {
		t.Fatalf("unscored evals must never emit drift")
	}
}

// TEAM-SCOPING: two teams share the model name "chatbot". Team-a sends all-bad scores
// (would drift); team-b sends all-good. Team-b's average must NOT see team-a's scores
// (no cross-tenant bleed), so team-b never drifts.
func TestQualityEval_TeamScoping_NoCrossTenantBleed(t *testing.T) {
	t.Parallel()
	cfg := QualityDriftConfig{Window: 10, MinSamples: 5, FloorScore: 3.0}

	// Shared store + monitors so both teams' evals live in the SAME backend (the
	// realistic deployment) — scoping must be by (team, model), not model alone.
	store := newFakeEvalStore()
	monitors := newFakeMonitorRepo()
	publisher := &fakePublisher{}
	reports := newFakeReportRepo()
	gate := newFakeGate()
	orch := &fakeOrchestrator{}
	clock := &fakeClock{t: time.Unix(2_000_000, 0)}

	_, _, _ = monitors.Upsert(context.Background(), qualityMonitor("team-a", "chatbot", false))
	_, _, _ = monitors.Upsert(context.Background(), qualityMonitor("team-b", "chatbot", false))

	mk := func(s JudgeScores) QualityEvalService {
		return NewQualityEvalService(QualityEvalDeps{
			Judge: &fakeJudge{scores: s},
			Evals: store, Monitors: monitors, Reports: reports,
			Publisher: publisher, Orch: orch, Gate: gate,
			Config: cfg, WarnDrop: 0.5, CriticalDrop: 1.0, Cooldown: time.Hour, Now: clock.now,
		})
	}
	teamABad := mk(badScores())
	teamBGood := mk(goodScores())
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		// team-a/chatbot — bad. Its monitor id is mon-chatbot (same id since model name
		// matches), but tenancy is by (team, model) on the EVAL store, which is what we test.
		if _, err := teamABad.EvaluateCompletion(ctx, "mon-chatbot", Completion{
			Team: "team-a", Model: "chatbot", RequestID: "a-" + reqID(i), Response: "bad",
		}); err != nil {
			t.Fatalf("team-a[%d]: %v", i, err)
		}
		// team-b/chatbot — good.
		if _, err := teamBGood.EvaluateCompletion(ctx, "mon-chatbot", Completion{
			Team: "team-b", Model: "chatbot", RequestID: "b-" + reqID(i), Response: "good",
		}); err != nil {
			t.Fatalf("team-b[%d]: %v", i, err)
		}
	}

	// Team-a's rolling average is all 2.0; team-b's is all 5.0 — the store partitioned
	// them by team despite the shared model name.
	aScores, _ := store.RecentOverall(ctx, "team-a", "chatbot", 10)
	bScores, _ := store.RecentOverall(ctx, "team-b", "chatbot", 10)
	if len(aScores) != 10 || len(bScores) != 10 {
		t.Fatalf("each team should have 10 scored evals; a=%d b=%d", len(aScores), len(bScores))
	}
	for _, v := range bScores {
		if v != 5 {
			t.Fatalf("team-b average polluted by team-a (saw %v, want all 5.0) — CROSS-TENANT BLEED", v)
		}
	}
	for _, v := range aScores {
		if v != 2 {
			t.Fatalf("team-a scores polluted (saw %v, want all 2.0)", v)
		}
	}
}

// reqID makes a stable request id per index for the loop tests.
func reqID(i int) string { return "req-" + string(rune('a'+i)) }
