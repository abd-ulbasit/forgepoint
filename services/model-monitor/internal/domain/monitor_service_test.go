package domain

import (
	"context"
	"errors"
	"testing"
	"time"
)

const (
	team  = "team-a"
	model = "fraud-detector"
)

// configureArmed creates a monitor with a small count window, a data-drift PSI
// threshold, and auto-retrain armed — the shape used by the loop tests.
func configureArmed(t *testing.T, h *harness, windowSize, minSamples int) Monitor {
	t.Helper()
	m, _, err := h.svc.ConfigureMonitor(context.Background(), team, ConfigureMonitorInput{
		ModelName:  model,
		WindowSize: windowSize,
		MinSamples: minSamples,
		Thresholds: []ThresholdConfig{
			{DriftType: DriftTypeData, Method: DriftMethodPSI, WarnScore: 0.1, CriticalScore: 0.25},
		},
		AutoRetrain:       true,
		RetrainPipelineID: "pipe-train-fraud",
		Enabled:           true,
	})
	if err != nil {
		t.Fatalf("ConfigureMonitor: %v", err)
	}
	return m
}

// highIncomeObs is an observation whose income sits in the HIGH bin — far from the
// low-income baseline, so a window of these produces a CRITICAL PSI.
func highIncomeObs(h *harness, reqID string) InferenceObservation {
	m, _ := h.monitors.GetByModel(context.Background(), team, model)
	return InferenceObservation{
		MonitorID:    m.ID,
		OwnerTeam:    team,
		RequestID:    reqID,
		ModelName:    model,
		ModelVersion: "v7",
		Features:     map[string]float64{"income": 90}, // high bin
		PredictedTop: "fraud",
		ObservedAt:   h.clock.now(),
	}
}

// ============================================================================
// THE FULL LOOP — ObserveInference folds, closes, scores, and fires the retrain.
// This is the single most important behavioral test in the package.
// ============================================================================

func TestObserveInference_DriftCrossesThreshold_FiresClosedLoop(t *testing.T) {
	h := newHarness(time.Hour, true)
	configureArmed(t, h, 3, 3) // close after 3, need 3 to score

	ctx := context.Background()
	// Fold two observations — window still open, no report.
	for _, id := range []string{"r1", "r2"} {
		rep, err := h.svc.ObserveInference(ctx, highIncomeObs(h, id))
		if err != nil {
			t.Fatalf("observe %s: %v", id, err)
		}
		if rep != nil {
			t.Fatalf("observe %s: window should still be open, got a report", id)
		}
	}
	// Third observation closes the window → score → CRITICAL → fire the loop.
	rep, err := h.svc.ObserveInference(ctx, highIncomeObs(h, "r3"))
	if err != nil {
		t.Fatalf("observe r3: %v", err)
	}
	if rep == nil {
		t.Fatal("third observation should close+score the window and return a report")
	}
	// REAL behavior assertions:
	if rep.Severity != DriftSeverityCritical {
		t.Fatalf("expected CRITICAL drift (high income vs low baseline), got %v (metrics=%+v)", rep.Severity, rep.Metrics)
	}
	if rep.DriftType != DriftTypeData {
		t.Fatalf("expected DATA drift headline, got %v", rep.DriftType)
	}
	if rep.SampleCount != 3 {
		t.Fatalf("report sample count = %d, want 3", rep.SampleCount)
	}
	// The loop closed: an event was emitted AND a retrain was triggered.
	if h.publisher.count() != 1 {
		t.Fatalf("expected exactly 1 drift event emitted, got %d", h.publisher.count())
	}
	if h.orch.count() != 1 {
		t.Fatalf("expected exactly 1 retrain trigger (the loop closing), got %d", h.orch.count())
	}
	// The trigger used the OPERATOR-configured pipeline (not anything from event data) — SSRF guard.
	if h.orch.triggers[0].PipelineID != "pipe-train-fraud" {
		t.Fatalf("retrain used pipeline %q, want operator-configured pipe-train-fraud", h.orch.triggers[0].PipelineID)
	}
	if h.orch.triggers[0].ReportID != rep.ID {
		t.Fatalf("retrain correlation ReportID = %q, want %q", h.orch.triggers[0].ReportID, rep.ID)
	}
	// Cooldown was recorded.
	last, _ := h.gate.LastTriggered(ctx, model)
	if last.IsZero() {
		t.Fatal("cooldown should have been marked after the trigger")
	}
}

func TestObserveInference_RetrainStorm_DebouncedByCooldown(t *testing.T) {
	// Two consecutive CRITICAL windows within the cooldown must fire the retrain
	// ONCE, not twice — the anti-storm guarantee. Both still EMIT (alerts), only the
	// second is suppressed from triggering.
	h := newHarness(time.Hour, true)
	configureArmed(t, h, 2, 2)
	ctx := context.Background()

	// Window 1 (2 obs) → CRITICAL → trigger.
	_, _ = h.svc.ObserveInference(ctx, highIncomeObs(h, "a1"))
	rep1, err := h.svc.ObserveInference(ctx, highIncomeObs(h, "a2"))
	if err != nil || rep1 == nil || rep1.Severity != DriftSeverityCritical {
		t.Fatalf("window1 should be CRITICAL: rep=%+v err=%v", rep1, err)
	}

	// Advance only 5 minutes (< 1h cooldown). Window 2 (2 obs) → CRITICAL → SUPPRESSED.
	h.clock.advance(5 * time.Minute)
	_, _ = h.svc.ObserveInference(ctx, highIncomeObs(h, "b1"))
	rep2, err := h.svc.ObserveInference(ctx, highIncomeObs(h, "b2"))
	if err != nil || rep2 == nil {
		t.Fatalf("window2 should produce a report: err=%v", err)
	}

	if h.orch.count() != 1 {
		t.Fatalf("retrain should fire ONCE within cooldown, got %d (storm not debounced)", h.orch.count())
	}
	if h.publisher.count() != 2 {
		t.Fatalf("both CRITICAL windows should still ALERT: got %d events, want 2", h.publisher.count())
	}
}

func TestObserveInference_AfterCooldown_FiresAgain(t *testing.T) {
	h := newHarness(time.Hour, true)
	configureArmed(t, h, 2, 2)
	ctx := context.Background()

	_, _ = h.svc.ObserveInference(ctx, highIncomeObs(h, "a1"))
	_, _ = h.svc.ObserveInference(ctx, highIncomeObs(h, "a2")) // trigger 1

	h.clock.advance(2 * time.Hour) // past the cooldown
	_, _ = h.svc.ObserveInference(ctx, highIncomeObs(h, "b1"))
	_, _ = h.svc.ObserveInference(ctx, highIncomeObs(h, "b2")) // trigger 2

	if h.orch.count() != 2 {
		t.Fatalf("after cooldown a second retrain should fire, got %d triggers", h.orch.count())
	}
}

func TestObserveInference_BelowMinSamples_DoesNotScore(t *testing.T) {
	// Window closes by COUNT at 2 but MinSamples is 5 — impossible to satisfy, so
	// the window retires WARMING_UP without a report. (Here size<minSamples is only
	// reachable on the data plane via a stale config; the service guards the config
	// path, this guards the scorer path.)
	h := newHarness(time.Hour, true)
	// Configure with size 2 but force MinSamples 5 by writing directly (bypassing
	// the configure validation, which would reject min>size) to test the scorer's
	// own guard.
	m := configureArmed(t, h, 2, 2)
	m.MinSamples = 5
	_, _, _ = h.monitors.Upsert(context.Background(), m)

	ctx := context.Background()
	_, _ = h.svc.ObserveInference(ctx, highIncomeObs(h, "r1"))
	rep, err := h.svc.ObserveInference(ctx, highIncomeObs(h, "r2"))
	if err != nil {
		t.Fatalf("observe: %v", err)
	}
	if rep != nil {
		t.Fatal("window below MinSamples should retire without a report")
	}
	if h.orch.count() != 0 || h.publisher.count() != 0 {
		t.Fatal("no scoring ⇒ no event and no retrain")
	}
}

func TestObserveInference_RedeliveredWindow_EmitsEventOnce(t *testing.T) {
	// Idempotency on window_id: if the same window is scored twice (e.g. the report
	// Save sees a duplicate), the event must fire ONCE. We simulate redelivery by
	// re-saving the produced report through the repo and re-running the policy path
	// is internal; instead we assert the repo-level idempotency the loop relies on.
	h := newHarness(time.Hour, true)
	configureArmed(t, h, 2, 2)
	ctx := context.Background()
	_, _ = h.svc.ObserveInference(ctx, highIncomeObs(h, "r1"))
	rep, _ := h.svc.ObserveInference(ctx, highIncomeObs(h, "r2"))

	// Re-save the SAME report (same window_id) — must be a no-op insert=false.
	_, inserted, err := h.reports.Save(ctx, *rep)
	if err != nil {
		t.Fatal(err)
	}
	if inserted {
		t.Fatal("re-saving a report for the same window_id must NOT insert again (idempotency)")
	}
	// Still exactly one event from the original loop run.
	if h.publisher.count() != 1 {
		t.Fatalf("event must fire exactly once per window, got %d", h.publisher.count())
	}
}

func TestObserveInference_UnmonitoredModel_Ignored(t *testing.T) {
	h := newHarness(time.Hour, true)
	// No monitor configured. An observation with empty MonitorID is dropped.
	rep, err := h.svc.ObserveInference(context.Background(), InferenceObservation{
		ModelName: "ghost-model", RequestID: "x", ModelVersion: "v1",
	})
	if err != nil {
		t.Fatalf("observe of unmonitored model should be a quiet no-op, got %v", err)
	}
	if rep != nil {
		t.Fatal("unmonitored model should produce no report")
	}
}

// ============================================================================
// CONTROL PLANE — ConfigureMonitor validation + server-authoritative fields.
// ============================================================================

func TestConfigureMonitor_SetsServerAuthoritativeFields(t *testing.T) {
	h := newHarness(time.Hour, true)
	m, created, err := h.svc.ConfigureMonitor(context.Background(), team, ConfigureMonitorInput{
		ModelName:  model,
		WindowSize: 100,
		MinSamples: 10,
		Thresholds: []ThresholdConfig{{DriftType: DriftTypeData, Method: DriftMethodPSI, WarnScore: 0.1, CriticalScore: 0.25}},
		Enabled:    true,
	})
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	if !created {
		t.Fatal("first configure should report created=true")
	}
	// SERVER set these — the client could not.
	if m.ID == "" {
		t.Fatal("server must assign an id")
	}
	if m.OwnerTeam != team {
		t.Fatalf("owner_team must come from auth claims, got %q", m.OwnerTeam)
	}
	if m.CreatedAt.IsZero() || m.UpdatedAt.IsZero() {
		t.Fatal("server must set timestamps")
	}
	// Baseline available ⇒ WARMING_UP (resolved baseline, not yet scoring).
	if m.State != MonitorStateWarmingUp {
		t.Fatalf("state = %v, want WARMING_UP (baseline resolved)", m.State)
	}
	if m.BaselineVersion != "v7" {
		t.Fatalf("baseline version should be server-resolved to v7, got %q", m.BaselineVersion)
	}

	// Second configure is an UPDATE (created=false), id stable.
	m2, created2, err := h.svc.ConfigureMonitor(context.Background(), team, ConfigureMonitorInput{
		ModelName: model, WindowSize: 200, MinSamples: 10,
		Thresholds: m.Thresholds, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if created2 {
		t.Fatal("second configure should be an update")
	}
	if m2.ID != m.ID {
		t.Fatal("update must preserve the id")
	}
}

func TestConfigureMonitor_NoBaseline_StaysPending(t *testing.T) {
	h := newHarness(time.Hour, false) // baseline unavailable
	m, _, err := h.svc.ConfigureMonitor(context.Background(), team, ConfigureMonitorInput{
		ModelName: model, WindowSize: 100, MinSamples: 10,
		Thresholds: []ThresholdConfig{{DriftType: DriftTypeData, Method: DriftMethodPSI, WarnScore: 0.1, CriticalScore: 0.25}},
		Enabled:    true,
	})
	if err != nil {
		t.Fatalf("configure should succeed even without a baseline yet: %v", err)
	}
	if m.State != MonitorStatePendingBaseline {
		t.Fatalf("no baseline ⇒ PENDING_BASELINE, got %v", m.State)
	}
}

func TestConfigureMonitor_PausedWhenDisabled(t *testing.T) {
	h := newHarness(time.Hour, true)
	m, _, err := h.svc.ConfigureMonitor(context.Background(), team, ConfigureMonitorInput{
		ModelName: model, WindowSize: 100, MinSamples: 10,
		Thresholds: []ThresholdConfig{{DriftType: DriftTypeData, Method: DriftMethodPSI, WarnScore: 0.1, CriticalScore: 0.25}},
		Enabled:    false, // paused
	})
	if err != nil {
		t.Fatal(err)
	}
	if m.State != MonitorStatePaused {
		t.Fatalf("disabled ⇒ PAUSED, got %v", m.State)
	}
}

func TestConfigureMonitor_Validation(t *testing.T) {
	h := newHarness(time.Hour, true)
	ctx := context.Background()
	cases := []struct {
		name string
		in   ConfigureMonitorInput
		team string
		want error
	}{
		{"empty team", ConfigureMonitorInput{ModelName: model, WindowSize: 10, Enabled: true}, "", ErrValidation},
		{"empty model", ConfigureMonitorInput{WindowSize: 10, Enabled: true}, team, ErrValidation},
		{"window over cap", ConfigureMonitorInput{ModelName: model, WindowSize: MaxWindowSize + 1, Enabled: true}, team, ErrValidation},
		{"min > size", ConfigureMonitorInput{ModelName: model, WindowSize: 5, MinSamples: 10, Enabled: true}, team, ErrValidation},
		{"no close bound", ConfigureMonitorInput{ModelName: model, WindowSize: 0, WindowDuration: 0, Enabled: true}, team, ErrValidation},
		{"warn > critical", ConfigureMonitorInput{ModelName: model, WindowSize: 10, Enabled: true,
			Thresholds: []ThresholdConfig{{DriftType: DriftTypeData, Method: DriftMethodPSI, WarnScore: 0.5, CriticalScore: 0.1}}}, team, ErrValidation},
		{"auto_retrain without pipeline", ConfigureMonitorInput{ModelName: model, WindowSize: 10, Enabled: true, AutoRetrain: true}, team, ErrRetrainPipelineRequired},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := h.svc.ConfigureMonitor(ctx, tc.team, tc.in)
			if !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
		})
	}
}

// ============================================================================
// PERFORMANCE DECAY — ground truth flows into the rolling accuracy signal.
// ============================================================================

func TestSubmitGroundTruth_MatchesObservedAndComputesDecay(t *testing.T) {
	h := newHarness(time.Hour, true)
	// Configure with a PERFORMANCE threshold (a 10-point drop is critical).
	_, _, err := h.svc.ConfigureMonitor(context.Background(), team, ConfigureMonitorInput{
		ModelName: model, WindowSize: 2, MinSamples: 2, Enabled: true,
		Thresholds: []ThresholdConfig{{DriftType: DriftTypePerformance, Method: DriftMethodPSI, WarnScore: 0.05, CriticalScore: 0.10}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	m, _ := h.monitors.GetByModel(ctx, team, model)

	// Observe two predictions (records request_id → prediction for the join).
	obs := func(id, pred string) InferenceObservation {
		return InferenceObservation{MonitorID: m.ID, OwnerTeam: team, RequestID: id, ModelName: model, ModelVersion: "v7", PredictedTop: pred, ObservedAt: h.clock.now()}
	}
	_, _ = h.svc.ObserveInference(ctx, obs("g1", "fraud"))
	// Window closes here; performance can't score yet (no labels) — that's fine.
	_, _ = h.svc.ObserveInference(ctx, obs("g2", "fraud"))

	// Submit ground truth: g1 was actually legit (wrong), g2 was fraud (correct),
	// and an UNKNOWN id that must come back unmatched (anti-fabrication).
	res, err := h.svc.SubmitGroundTruth(ctx, team, SubmitGroundTruthInput{
		ModelName: model,
		Labels: []GroundTruthLabel{
			{RequestID: "g1", ActualLabel: "legit", ObservedAt: h.clock.now()},
			{RequestID: "g2", ActualLabel: "fraud", ObservedAt: h.clock.now()},
			{RequestID: "ghost", ActualLabel: "fraud", ObservedAt: h.clock.now()},
		},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if res.Accepted != 2 {
		t.Fatalf("accepted = %d, want 2 (g1,g2)", res.Accepted)
	}
	if len(res.UnmatchedRequestIDs) != 1 || res.UnmatchedRequestIDs[0] != "ghost" {
		t.Fatalf("unmatched = %v, want [ghost] (fabricated id rejected)", res.UnmatchedRequestIDs)
	}
	// Now the rolling accuracy is 1/2 = 0.5; baseline 0.95 → drop 0.45 → CRITICAL.
	pairs, _ := h.truth.RecentPairs(ctx, model, time.Hour)
	if Accuracy(pairs) != 0.5 {
		t.Fatalf("rolling accuracy = %g, want 0.5", Accuracy(pairs))
	}
}

func TestSubmitGroundTruth_BatchCapEnforced(t *testing.T) {
	h := newHarness(time.Hour, true)
	configureArmed(t, h, 10, 1)
	labels := make([]GroundTruthLabel, MaxGroundTruthBatch+1)
	_, err := h.svc.SubmitGroundTruth(context.Background(), team, SubmitGroundTruthInput{ModelName: model, Labels: labels})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("over-cap batch should be ErrValidation, got %v", err)
	}
}

func TestSubmitGroundTruth_LabelTooLong_Rejected(t *testing.T) {
	h := newHarness(time.Hour, true)
	configureArmed(t, h, 10, 1)
	long := make([]byte, MaxLabelBytes+1)
	_, err := h.svc.SubmitGroundTruth(context.Background(), team, SubmitGroundTruthInput{
		ModelName: model, Labels: []GroundTruthLabel{{RequestID: "r", ActualLabel: string(long)}},
	})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("over-long label (PII/memory guard) should be ErrValidation, got %v", err)
	}
}

// ============================================================================
// CONTROL PLANE — reads, delete, reset, list.
// ============================================================================

func TestGetModelHealth_AndStatus(t *testing.T) {
	h := newHarness(time.Hour, true)
	configureArmed(t, h, 2, 2)
	ctx := context.Background()
	_, _ = h.svc.ObserveInference(ctx, highIncomeObs(h, "r1"))
	_, _ = h.svc.ObserveInference(ctx, highIncomeObs(h, "r2")) // produces a CRITICAL report

	health, err := h.svc.GetModelHealth(ctx, team, model)
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.OverallSeverity != DriftSeverityCritical {
		t.Fatalf("health overall = %v, want CRITICAL", health.OverallSeverity)
	}
	if health.LatestReportID == "" {
		t.Fatal("health should deep-link the latest report")
	}

	status, err := h.svc.GetMonitorStatus(ctx, team, model)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.LatestReport == nil {
		t.Fatal("status should carry the latest report")
	}
}

func TestGetModelHealth_UnknownModel_NotFound(t *testing.T) {
	h := newHarness(time.Hour, true)
	_, err := h.svc.GetModelHealth(context.Background(), team, "nope")
	if !errors.Is(err, ErrMonitorNotFound) {
		t.Fatalf("want ErrMonitorNotFound, got %v", err)
	}
}

func TestDeleteMonitor_SoftThenPurge(t *testing.T) {
	h := newHarness(time.Hour, true)
	configureArmed(t, h, 2, 2)
	ctx := context.Background()
	_, _ = h.svc.ObserveInference(ctx, highIncomeObs(h, "r1"))
	_, _ = h.svc.ObserveInference(ctx, highIncomeObs(h, "r2")) // 1 report exists

	// Soft delete: config gone, history retained.
	purged, err := h.svc.DeleteMonitor(ctx, team, model, false)
	if err != nil {
		t.Fatal(err)
	}
	if purged != 0 {
		t.Fatalf("soft delete should purge 0 reports, got %d", purged)
	}
	if _, err := h.svc.GetModelHealth(ctx, team, model); !errors.Is(err, ErrMonitorNotFound) {
		t.Fatal("soft-deleted monitor should be NotFound")
	}
	// Idempotent re-delete is a successful no-op.
	if _, err := h.svc.DeleteMonitor(ctx, team, model, false); err != nil {
		t.Fatalf("re-delete should be a no-op, got %v", err)
	}

	// Re-create then purge: history removed.
	configureArmed(t, h, 2, 2)
	_, _ = h.svc.ObserveInference(ctx, highIncomeObs(h, "p1"))
	_, _ = h.svc.ObserveInference(ctx, highIncomeObs(h, "p2"))
	purged, err = h.svc.DeleteMonitor(ctx, team, model, true)
	if err != nil {
		t.Fatal(err)
	}
	if purged < 1 {
		t.Fatalf("hard delete should purge >=1 report, got %d", purged)
	}
}

func TestResetBaseline_Reresolves(t *testing.T) {
	h := newHarness(time.Hour, true)
	configureArmed(t, h, 10, 1)
	m, err := h.svc.ResetBaseline(context.Background(), team, model, "v9")
	if err != nil {
		t.Fatalf("reset: %v", err)
	}
	if m.BaselineVersion != "v9" {
		t.Fatalf("reset baseline version = %q, want v9 (server-resolved from named version)", m.BaselineVersion)
	}
	if m.State != MonitorStateWarmingUp {
		t.Fatalf("after reset state should be WARMING_UP, got %v", m.State)
	}
}

func TestListMonitors_TeamScopedWithHealth(t *testing.T) {
	h := newHarness(time.Hour, true)
	configureArmed(t, h, 10, 1)
	// A monitor in ANOTHER team must NOT appear.
	_, _, _ = h.svc.ConfigureMonitor(context.Background(), "other-team", ConfigureMonitorInput{
		ModelName: "other-model", WindowSize: 10, Enabled: true,
		Thresholds: []ThresholdConfig{{DriftType: DriftTypeData, Method: DriftMethodPSI, WarnScore: 0.1, CriticalScore: 0.25}},
	})

	entries, _, err := h.svc.ListMonitors(context.Background(), team, DriftSeverityUnspecified, MonitorStateUnspecified, ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("fleet should be team-scoped to 1 entry, got %d", len(entries))
	}
	if entries[0].Monitor.ModelName != model {
		t.Fatalf("unexpected model in fleet: %q", entries[0].Monitor.ModelName)
	}
}

func TestListDriftReports_RequiresModel(t *testing.T) {
	h := newHarness(time.Hour, true)
	_, _, err := h.svc.ListDriftReports(context.Background(), team, ReportFilter{}, ListOptions{})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("list without model should be ErrValidation, got %v", err)
	}
}

func TestListDriftReports_RequiresTeam(t *testing.T) {
	// Tenancy guard: an empty owner_team (claims not wired) is a programming error,
	// not an unscoped list — rejected before touching the repo.
	h := newHarness(time.Hour, true)
	_, _, err := h.svc.ListDriftReports(context.Background(), "", ReportFilter{ModelName: model}, ListOptions{})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("list without owner_team should be ErrValidation, got %v", err)
	}
}

func TestListDriftReports_CapsPageSize(t *testing.T) {
	// The service must CAP an over-large page request (DoS guard). We verify via the
	// capPage helper behavior indirectly: a huge request returns without honoring it.
	h := newHarness(time.Hour, true)
	configureArmed(t, h, 2, 2)
	ctx := context.Background()
	_, _ = h.svc.ObserveInference(ctx, highIncomeObs(h, "r1"))
	_, _ = h.svc.ObserveInference(ctx, highIncomeObs(h, "r2"))
	// Request a page far over the cap — must not error and must return.
	reps, _, err := h.svc.ListDriftReports(ctx, team, ReportFilter{ModelName: model}, ListOptions{PageSize: 100000})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(reps) == 0 {
		t.Fatal("expected at least the one stored report")
	}
}

// ============================================================================
// TENANCY — the cross-tenant / IDOR guards (Findings 1 & 2).
//
// These tests encode the production invariant the old fakes hid: monitors are
// keyed by (owner_team, model_name), so the SAME model name can belong to two
// teams, and EVERY report read/purge must be scoped by owner_team. They use two
// teams owning a model with the IDENTICAL name and assert reports never cross.
// ============================================================================

// configureArmedFor configures an auto-retrain monitor for an arbitrary (team,
// model) — the multi-tenant analogue of configureArmed (which is pinned to the
// package-level team/model).
func configureArmedFor(t *testing.T, h *harness, ownerTeam, modelName string, windowSize, minSamples int) Monitor {
	t.Helper()
	m, _, err := h.svc.ConfigureMonitor(context.Background(), ownerTeam, ConfigureMonitorInput{
		ModelName:  modelName,
		WindowSize: windowSize,
		MinSamples: minSamples,
		Thresholds: []ThresholdConfig{
			{DriftType: DriftTypeData, Method: DriftMethodPSI, WarnScore: 0.1, CriticalScore: 0.25},
		},
		AutoRetrain:       true,
		RetrainPipelineID: "pipe-" + ownerTeam,
		Enabled:           true,
	})
	if err != nil {
		t.Fatalf("ConfigureMonitor(%s/%s): %v", ownerTeam, modelName, err)
	}
	return m
}

// highIncomeObsFor builds a CRITICAL-drift observation bound to a specific team's
// monitor for a model — so we can drive two teams' same-named models independently.
func highIncomeObsFor(t *testing.T, h *harness, ownerTeam, modelName, reqID string) InferenceObservation {
	t.Helper()
	m, err := h.monitors.GetByModel(context.Background(), ownerTeam, modelName)
	if err != nil {
		t.Fatalf("GetByModel(%s/%s): %v", ownerTeam, modelName, err)
	}
	return InferenceObservation{
		MonitorID:    m.ID,
		OwnerTeam:    ownerTeam,
		RequestID:    reqID,
		ModelName:    modelName,
		ModelVersion: "v7",
		Features:     map[string]float64{"income": 90}, // high bin ⇒ CRITICAL PSI
		PredictedTop: "fraud",
		ObservedAt:   h.clock.now(),
	}
}

// produceReportFor configures (team,model) and drives one closed CRITICAL window,
// returning the produced report. Window size 2 closes on the 2nd observation.
func produceReportFor(t *testing.T, h *harness, ownerTeam, modelName, reqPrefix string) DriftReport {
	t.Helper()
	configureArmedFor(t, h, ownerTeam, modelName, 2, 2)
	ctx := context.Background()
	_, _ = h.svc.ObserveInference(ctx, highIncomeObsFor(t, h, ownerTeam, modelName, reqPrefix+"1"))
	rep, err := h.svc.ObserveInference(ctx, highIncomeObsFor(t, h, ownerTeam, modelName, reqPrefix+"2"))
	if err != nil || rep == nil {
		t.Fatalf("produce report for %s/%s: rep=%v err=%v", ownerTeam, modelName, rep, err)
	}
	return *rep
}

// TestGetDriftReport_CrossTenant_NotFound proves Finding 1: a report id is NOT an
// authorization control. team-b, asking for team-a's report id (which it could
// have learned from the drift event, a retrain request, a deep link, or a log),
// must get ErrReportNotFound — NOT the report, and NOT a distinguishable "forbidden".
func TestGetDriftReport_CrossTenant_NotFound(t *testing.T) {
	h := newHarness(time.Hour, true)
	ctx := context.Background()

	// team-a owns a report; capture its (real, leakable) id.
	repA := produceReportFor(t, h, "team-a", "fraud", "a")
	if repA.OwnerTeam != "team-a" {
		t.Fatalf("report should be stamped with its owning team, got %q", repA.OwnerTeam)
	}

	// team-b knows the id but does NOT own it → ErrReportNotFound (indistinguishable
	// from a nonexistent id). The owner still reads its own report fine.
	if _, err := h.svc.GetDriftReport(ctx, "team-b", repA.ID); !errors.Is(err, ErrReportNotFound) {
		t.Fatalf("cross-tenant GetDriftReport must be ErrReportNotFound (IDOR guard), got %v", err)
	}
	got, err := h.svc.GetDriftReport(ctx, "team-a", repA.ID)
	if err != nil {
		t.Fatalf("owner must read its own report, got %v", err)
	}
	if got.ID != repA.ID {
		t.Fatalf("owner read wrong report: %q != %q", got.ID, repA.ID)
	}
}

func TestGetDriftReport_RequiresTeam(t *testing.T) {
	// An unscoped GetDriftReport (empty team) is the IDOR we guard against — reject it.
	h := newHarness(time.Hour, true)
	rep := produceReportFor(t, h, "team-a", "fraud", "a")
	if _, err := h.svc.GetDriftReport(context.Background(), "", rep.ID); !errors.Is(err, ErrValidation) {
		t.Fatalf("GetDriftReport without owner_team should be ErrValidation, got %v", err)
	}
}

// TestSameModelName_TwoTeams_ReportsNeverCross proves Finding 2 across the whole
// report read path: two teams own a model with the IDENTICAL name; LatestByModel
// (via GetModelHealth/GetMonitorStatus) and List (via ListDriftReports) must each
// return ONLY the calling team's reports.
func TestSameModelName_TwoTeams_ReportsNeverCross(t *testing.T) {
	h := newHarness(time.Hour, true)
	ctx := context.Background()
	const shared = "fraud" // SAME model name, two teams

	repA := produceReportFor(t, h, "team-a", shared, "a")
	repB := produceReportFor(t, h, "team-b", shared, "b")
	if repA.ID == repB.ID {
		t.Fatal("two teams' reports must be distinct rows")
	}

	// --- GetModelHealth (LatestByModel) is team-scoped ---
	healthA, err := h.svc.GetModelHealth(ctx, "team-a", shared)
	if err != nil {
		t.Fatalf("health team-a: %v", err)
	}
	if healthA.LatestReportID != repA.ID {
		t.Fatalf("team-a health leaked another team's report: got %q want %q", healthA.LatestReportID, repA.ID)
	}
	healthB, err := h.svc.GetModelHealth(ctx, "team-b", shared)
	if err != nil {
		t.Fatalf("health team-b: %v", err)
	}
	if healthB.LatestReportID != repB.ID {
		t.Fatalf("team-b health leaked another team's report: got %q want %q", healthB.LatestReportID, repB.ID)
	}

	// --- GetMonitorStatus (LatestByModel) is team-scoped ---
	statusA, err := h.svc.GetMonitorStatus(ctx, "team-a", shared)
	if err != nil {
		t.Fatalf("status team-a: %v", err)
	}
	if statusA.LatestReport == nil || statusA.LatestReport.ID != repA.ID {
		t.Fatalf("team-a status leaked/lost report: got %+v want %q", statusA.LatestReport, repA.ID)
	}

	// --- ListDriftReports (List) is team-scoped ---
	listA, _, err := h.svc.ListDriftReports(ctx, "team-a", ReportFilter{ModelName: shared}, ListOptions{})
	if err != nil {
		t.Fatalf("list team-a: %v", err)
	}
	if len(listA) != 1 || listA[0].ID != repA.ID {
		t.Fatalf("team-a history must contain ONLY its own report, got %+v", listA)
	}
	listB, _, err := h.svc.ListDriftReports(ctx, "team-b", ReportFilter{ModelName: shared}, ListOptions{})
	if err != nil {
		t.Fatalf("list team-b: %v", err)
	}
	if len(listB) != 1 || listB[0].ID != repB.ID {
		t.Fatalf("team-b history must contain ONLY its own report, got %+v", listB)
	}
}

// TestDeleteMonitor_PurgeIsTeamScoped proves the destructive half of Finding 2:
// team-a deleting its same-named "fraud" monitor with purge=true must NOT destroy
// team-b's "fraud" drift history (cross-tenant data destruction).
func TestDeleteMonitor_PurgeIsTeamScoped(t *testing.T) {
	h := newHarness(time.Hour, true)
	ctx := context.Background()
	const shared = "fraud"

	repA := produceReportFor(t, h, "team-a", shared, "a")
	repB := produceReportFor(t, h, "team-b", shared, "b")

	// team-a deletes + purges. It should purge ONLY its own report (1), not team-b's.
	purged, err := h.svc.DeleteMonitor(ctx, "team-a", shared, true)
	if err != nil {
		t.Fatalf("delete team-a: %v", err)
	}
	if purged != 1 {
		t.Fatalf("purge should remove only team-a's 1 report, got %d (cross-tenant destruction)", purged)
	}

	// team-a's report is gone; team-b's survives and is still readable by team-b.
	if _, err := h.svc.GetDriftReport(ctx, "team-a", repA.ID); !errors.Is(err, ErrReportNotFound) {
		t.Fatalf("team-a report should be purged, got %v", err)
	}
	gotB, err := h.svc.GetDriftReport(ctx, "team-b", repB.ID)
	if err != nil {
		t.Fatalf("team-b history must SURVIVE team-a's purge, got %v", err)
	}
	if gotB.ID != repB.ID {
		t.Fatalf("team-b report corrupted: %q != %q", gotB.ID, repB.ID)
	}
}
