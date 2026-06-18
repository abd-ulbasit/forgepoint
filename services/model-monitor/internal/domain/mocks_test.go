package domain

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"
)

// ============================================================================
// HAND-WRITTEN MOCK PORTS — in-memory fakes that implement the domain ports with
// REAL behavior (actual storage, actual idempotency), so the service tests verify
// the loop's effects on state, not just "was this method called". Per the TDD
// rule: tests assert state transitions actually happen and invariants hold.
// ============================================================================

// fakeMonitorRepo is an in-memory MonitorRepository keyed by (team,model) and by id.
type fakeMonitorRepo struct {
	mu      sync.Mutex
	byKey   map[string]Monitor // team|model → monitor
	byID    map[string]Monitor
	deleted map[string]bool
}

func newFakeMonitorRepo() *fakeMonitorRepo {
	return &fakeMonitorRepo{byKey: map[string]Monitor{}, byID: map[string]Monitor{}, deleted: map[string]bool{}}
}

func monKey(team, model string) string { return team + "|" + model }

func (f *fakeMonitorRepo) Upsert(_ context.Context, m Monitor) (Monitor, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := monKey(m.OwnerTeam, m.ModelName)
	_, existed := f.byKey[k]
	f.byKey[k] = m
	f.byID[m.ID] = m
	delete(f.deleted, k)
	return m, !existed, nil
}

func (f *fakeMonitorRepo) GetByModel(_ context.Context, team, model string) (Monitor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := monKey(team, model)
	if f.deleted[k] {
		return Monitor{}, ErrRepoNotFound
	}
	m, ok := f.byKey[k]
	if !ok {
		return Monitor{}, ErrRepoNotFound
	}
	return m, nil
}

func (f *fakeMonitorRepo) GetByID(_ context.Context, id string) (Monitor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, ok := f.byID[id]
	if !ok {
		return Monitor{}, ErrRepoNotFound
	}
	return m, nil
}

func (f *fakeMonitorRepo) Delete(_ context.Context, team, model string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted[monKey(team, model)] = true // idempotent: deleting twice is fine
	return nil
}

func (f *fakeMonitorRepo) List(_ context.Context, ff FleetFilter, _ ListOptions) ([]Monitor, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []Monitor
	for k, m := range f.byKey {
		if f.deleted[k] || m.OwnerTeam != ff.OwnerTeam {
			continue
		}
		if ff.State != MonitorStateUnspecified && m.State != ff.State {
			continue
		}
		out = append(out, m)
	}
	return out, "", nil
}

// fakeReportRepo is an in-memory DriftReportRepository with REAL window-id
// idempotency (the heart of exactly-once-in-effect) AND REAL tenant partitioning.
//
// The model history is keyed by (owner_team, model_name) — NOT model_name alone —
// to faithfully model the production invariant: monitors are per (team, model), so
// the same model name in two teams is two distinct histories. Keying the fake by
// model name alone is exactly what hid the cross-tenant bug from the old tests, so
// the fake now keys the way the real Postgres adapter must (WHERE owner_team = $1
// AND model_name = $2). GetByID is also team-scoped: a report id that exists but
// belongs to another team reads as ErrRepoNotFound (no IDOR, no enumeration oracle).
type fakeReportRepo struct {
	mu       sync.Mutex
	byID     map[string]DriftReport // id → report (lookup still by id, then team-checked)
	byWindow map[string]DriftReport // window_id → report (idempotency key)
	byScope  map[string][]DriftReport // team|model → reports (tenant-partitioned history)
}

func newFakeReportRepo() *fakeReportRepo {
	return &fakeReportRepo{byID: map[string]DriftReport{}, byWindow: map[string]DriftReport{}, byScope: map[string][]DriftReport{}}
}

// reportScope is the (team, model) partition key — mirrors the real adapter's
// composite key. Reusing monKey would conflate the two concepts; a dedicated
// helper documents that report partitioning is the SAME shape as monitor keying.
func reportScope(team, model string) string { return team + "|" + model }

func (f *fakeReportRepo) Save(_ context.Context, r DriftReport) (DriftReport, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// IDEMPOTENCY on window_id: a second save for the same window returns the
	// existing report and inserted=false — so the loop emits the event only once.
	if existing, ok := f.byWindow[r.WindowID]; ok {
		return existing, false, nil
	}
	f.byWindow[r.WindowID] = r
	f.byID[r.ID] = r
	f.byScope[reportScope(r.OwnerTeam, r.ModelName)] = append(f.byScope[reportScope(r.OwnerTeam, r.ModelName)], r)
	return r, true, nil
}

func (f *fakeReportRepo) GetByID(_ context.Context, ownerTeam, id string) (DriftReport, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.byID[id]
	// TENANT SCOPE: a wrong-team (or absent) id is INDISTINGUISHABLE — both return
	// ErrRepoNotFound — so a leaked id can't be used to read another tenant's report
	// and a caller can't enumerate which ids exist elsewhere.
	if !ok || r.OwnerTeam != ownerTeam {
		return DriftReport{}, ErrRepoNotFound
	}
	return r, nil
}

func (f *fakeReportRepo) List(_ context.Context, ff ReportFilter, _ ListOptions) ([]DriftReport, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Gather the candidate partitions. model_name is OPTIONAL (mirrors the real adapter):
	//   - set   → exactly the one (team, model) partition.
	//   - empty → EVERY partition owned by this team (the fleet "recent drift" view).
	// owner_team is always the mandatory scope, so the empty-model case still never
	// crosses tenants — we only collect partitions whose team prefix matches.
	var scopes [][]DriftReport
	if ff.ModelName != "" {
		scopes = append(scopes, f.byScope[reportScope(ff.OwnerTeam, ff.ModelName)])
	} else {
		prefix := ff.OwnerTeam + "|"
		for key, rs := range f.byScope {
			if strings.HasPrefix(key, prefix) {
				scopes = append(scopes, rs)
			}
		}
	}
	var out []DriftReport
	for _, rs := range scopes {
		for _, r := range rs {
			if ff.MinSeverity != DriftSeverityUnspecified && r.Severity < ff.MinSeverity {
				continue
			}
			out = append(out, r)
		}
	}
	// Newest-first by window_end (then id), mirroring the adapter's ORDER BY — important
	// once we merge multiple partitions so the cross-model page is deterministically
	// ordered rather than dependent on map iteration order.
	sort.Slice(out, func(i, j int) bool {
		if !out[i].WindowEnd.Equal(out[j].WindowEnd) {
			return out[i].WindowEnd.After(out[j].WindowEnd)
		}
		return out[i].ID > out[j].ID
	})
	return out, "", nil
}

func (f *fakeReportRepo) LatestByModel(_ context.Context, ownerTeam, model string) (DriftReport, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rs := f.byScope[reportScope(ownerTeam, model)]
	if len(rs) == 0 {
		return DriftReport{}, ErrRepoNotFound
	}
	return rs[len(rs)-1], nil
}

func (f *fakeReportRepo) PurgeByModel(_ context.Context, ownerTeam, model string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	scope := reportScope(ownerTeam, model)
	n := len(f.byScope[scope])
	for _, r := range f.byScope[scope] {
		delete(f.byID, r.ID)
		delete(f.byWindow, r.WindowID)
	}
	// Only THIS team's partition is removed — another team's same-named history
	// (a different scope key) is untouched.
	delete(f.byScope, scope)
	return n, nil
}

// fakeWindowStore keeps one open window per monitor in memory.
type fakeWindowStore struct {
	mu   sync.Mutex
	open map[string]*Window // monitorID → open window
}

func newFakeWindowStore() *fakeWindowStore {
	return &fakeWindowStore{open: map[string]*Window{}}
}

func (f *fakeWindowStore) LoadOrOpen(_ context.Context, monitorID, model string, openedAt time.Time) (*Window, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if w, ok := f.open[monitorID]; ok {
		return w, nil
	}
	w := NewWindow(monitorID, model, openedAt)
	f.open[monitorID] = w
	return w, nil
}

func (f *fakeWindowStore) Save(_ context.Context, w *Window) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.open[w.MonitorID] = w
	return nil
}

func (f *fakeWindowStore) Close(_ context.Context, monitorID, windowID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if w, ok := f.open[monitorID]; ok && w.ID == windowID {
		delete(f.open, monitorID) // next LoadOrOpen opens fresh
	}
	return nil
}

func (f *fakeWindowStore) CurrentSampleCount(_ context.Context, monitorID string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if w, ok := f.open[monitorID]; ok {
		return w.SampleCount, nil
	}
	return 0, nil
}

// fakeTruthStore records predictions and joins delayed labels.
type fakeTruthStore struct {
	mu          sync.Mutex
	predictions map[string]string // model|reqID → predicted
	pairs       map[string][]LabelPair
}

func newFakeTruthStore() *fakeTruthStore {
	return &fakeTruthStore{predictions: map[string]string{}, pairs: map[string][]LabelPair{}}
}

func truthKey(model, reqID string) string { return model + "|" + reqID }

func (f *fakeTruthStore) RecordPrediction(_ context.Context, model, reqID, predicted string, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.predictions[truthKey(model, reqID)] = predicted
	return nil
}

func (f *fakeTruthStore) RecordLabel(_ context.Context, model, reqID, actual string, _ time.Time) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	pred, ok := f.predictions[truthKey(model, reqID)]
	if !ok {
		return false, nil // ANTI-FABRICATION: unknown request_id is unmatched
	}
	f.pairs[model] = append(f.pairs[model], LabelPair{Predicted: pred, Actual: actual})
	return true, nil
}

func (f *fakeTruthStore) RecentPairs(_ context.Context, model string, _ time.Duration) ([]LabelPair, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pairs[model], nil
}

// fakeBaselineProvider returns a fixed baseline.
type fakeBaselineProvider struct {
	baseline  Baseline
	available bool
}

func (f *fakeBaselineProvider) GetBaseline(_ context.Context, model, version string) (Baseline, error) {
	if !f.available {
		return Baseline{}, ErrBaselineUnavailable
	}
	b := f.baseline
	b.ModelName = model
	if version != "" {
		b.Version = version
	}
	return b, nil
}

// fakePublisher records emitted drift events.
type fakePublisher struct {
	mu     sync.Mutex
	events []DriftEvent
}

func (f *fakePublisher) PublishDrift(_ context.Context, e DriftEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, e)
	return nil
}

func (f *fakePublisher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.events)
}

// fakeOrchestrator records retrain triggers.
type fakeOrchestrator struct {
	mu       sync.Mutex
	triggers []RetrainRequest
}

func (f *fakeOrchestrator) TriggerRetrain(_ context.Context, in RetrainRequest) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.triggers = append(f.triggers, in)
	return "exec-" + in.ReportID, nil
}

func (f *fakeOrchestrator) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.triggers)
}

// fakeGate is an in-memory RetrainGate (the cooldown record).
type fakeGate struct {
	mu   sync.Mutex
	last map[string]time.Time
}

func newFakeGate() *fakeGate { return &fakeGate{last: map[string]time.Time{}} }

func (f *fakeGate) LastTriggered(_ context.Context, model string) (time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.last[model], nil
}

func (f *fakeGate) MarkTriggered(_ context.Context, model string, at time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.last[model] = at
	return nil
}

// harness bundles a wired service + its fakes for the service-level tests.
type harness struct {
	svc       MonitorService
	monitors  *fakeMonitorRepo
	reports   *fakeReportRepo
	windows   *fakeWindowStore
	truth     *fakeTruthStore
	baselines *fakeBaselineProvider
	publisher *fakePublisher
	orch      *fakeOrchestrator
	gate      *fakeGate
	clock     *fakeClock
}

// fakeClock is an injectable, advanceable clock so time-based window closes and
// cooldowns are deterministic (no real time.Now in the domain tests).
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newHarness(cooldown time.Duration, baselineAvailable bool) *harness {
	h := &harness{
		monitors:  newFakeMonitorRepo(),
		reports:   newFakeReportRepo(),
		windows:   newFakeWindowStore(),
		truth:     newFakeTruthStore(),
		baselines: &fakeBaselineProvider{available: baselineAvailable, baseline: defaultBaseline()},
		publisher: &fakePublisher{},
		orch:      &fakeOrchestrator{},
		gate:      newFakeGate(),
		clock:     &fakeClock{t: time.Unix(1_000_000, 0)},
	}
	h.svc = NewMonitorService(h.monitors, h.reports, h.windows, h.truth, h.baselines,
		h.publisher, h.orch, h.gate, cooldown, h.clock.now)
	return h
}

// defaultBaseline is a baseline whose income distribution sits in the LOW bins, so
// a current window shifted to HIGH income produces a large, CRITICAL PSI.
func defaultBaseline() Baseline {
	return Baseline{
		Version:    "v7",
		CapturedAt: time.Unix(900_000, 0),
		FeatureEdges: map[string][]float64{
			"income": {0, 25, 50, 75, 100},
		},
		FeatureBaselines: map[string]Histogram{
			// Mostly low-income at training time.
			"income": {Counts: []float64{700, 200, 70, 30}, Edges: []float64{0, 25, 50, 75, 100}},
		},
		PredictionOrder:    []string{"legit", "fraud"},
		PredictionBaseline: Histogram{Counts: []float64{900, 100}}, // mostly legit at training
		Accuracy:           0.95,
	}
}
