// monitor_service_impl.go — the concrete MonitorService. This is where the
// streaming-aggregation + closed-loop-control pattern actually RUNS.
//
// ============================================================================
// CLEAN ARCHITECTURE PLACEMENT
// ============================================================================
//
// monitorService lives in the domain package and depends ONLY on the PORTS
// (ports.go) and the pure math/window/policy in this package. It imports NO gRPC,
// NO NATS, NO SQL driver, NO proto. The adapters (Postgres reports, Redis windows,
// NATS publisher, the orchestrator gRPC client, the baseline gRPC client) are
// injected at wire time (main.go); tests inject hand-written fakes. The business
// logic dictates the port contracts; the infrastructure conforms.
//
// ============================================================================
// THE LOOP THIS FILE CLOSES (the whole reason this service exists)
// ============================================================================
//
//	serve → InferenceCompleted ─→ Model Monitor (windows) ─→ drift? ─yes─→ ModelDriftDetected
//	   ↑                                                                        │
//	   └────── promote new version ←── canary ←── retrain DAG ←── Orchestrator ←─┘
//
// ObserveInference is the top of that loop: fold → (on close) score → policy →
// emit + (maybe) trigger. Everything else on this service is the control panel.
package domain

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// defaultRetrainCooldown is the anti-storm gap between auto-retrains for one
// model when the monitor doesn't override it. WHY 1 hour by default: a retrain is
// expensive (a whole training DAG) and a sustained drift breaches CRITICAL on
// every window close — without a cooldown a busy model would fire a retrain every
// few seconds. An hour is long enough that one retrain can finish, canary, and
// promote (resetting the baseline) before another is even considered. Operators
// can shorten/lengthen it via config. See DecideRetrain's cooldown gate.
const defaultRetrainCooldown = time.Hour

// performanceWindow is the look-back over which rolling accuracy is computed for
// performance decay. Ground truth is delayed, so this is intentionally generous.
const performanceWindow = 24 * time.Hour

// monitorService is the production MonitorService. Unexported: callers receive it
// only as the MonitorService interface from NewMonitorService (program-to-interface).
type monitorService struct {
	monitors  MonitorRepository
	reports   DriftReportRepository
	windows   WindowStore
	truth     GroundTruthStore
	baselines BaselineProvider
	publisher DriftPublisher
	orch      Orchestrator
	gate      RetrainGate

	cooldown time.Duration
	now      func() time.Time // injectable clock — tests pin it; prod passes time.Now
}

// NewMonitorService wires the ports and returns the MonitorService interface.
// WHY return the interface, not *monitorService: callers depend on the
// abstraction (dependency inversion); the compile-time assertion below guarantees
// the concrete type satisfies it so a signature drift fails the build, not a call.
//
// `now` may be nil, in which case time.Now is used — tests pass a fixed clock so
// the time-based window close and the cooldown are deterministic.
func NewMonitorService(
	monitors MonitorRepository,
	reports DriftReportRepository,
	windows WindowStore,
	truth GroundTruthStore,
	baselines BaselineProvider,
	publisher DriftPublisher,
	orch Orchestrator,
	gate RetrainGate,
	cooldown time.Duration,
	now func() time.Time,
) MonitorService {
	if now == nil {
		now = time.Now
	}
	if cooldown <= 0 {
		cooldown = defaultRetrainCooldown
	}
	return &monitorService{
		monitors:  monitors,
		reports:   reports,
		windows:   windows,
		truth:     truth,
		baselines: baselines,
		publisher: publisher,
		orch:      orch,
		gate:      gate,
		cooldown:  cooldown,
		now:       now,
	}
}

// Compile-time proof the concrete type satisfies the interface.
var _ MonitorService = (*monitorService)(nil)

// ============================================================================
// CONTROL PLANE — ConfigureMonitor (the only config write path)
// ============================================================================

func (s *monitorService) ConfigureMonitor(ctx context.Context, ownerTeam string, in ConfigureMonitorInput) (Monitor, bool, error) {
	// --- VALIDATION (fail before touching any port) -----------------------
	// owner_team comes from auth claims (the handler passes it); a monitor with no
	// owning team would be unauthorizable, so reject it as a programming error.
	if strings.TrimSpace(ownerTeam) == "" {
		return Monitor{}, false, fmt.Errorf("%w: owner_team is required (from auth claims)", ErrValidation)
	}
	if strings.TrimSpace(in.ModelName) == "" {
		return Monitor{}, false, fmt.Errorf("%w: model_name is required", ErrValidation)
	}
	if err := validateWindowShape(in.WindowDuration, in.WindowSize, in.MinSamples); err != nil {
		return Monitor{}, false, err
	}
	if err := validateThresholds(in.Thresholds); err != nil {
		return Monitor{}, false, err
	}
	// Closed-loop guard: arming auto_retrain without a pipeline would silently
	// no-op every breach. Fail-fast so the misconfiguration is visible at write.
	if in.AutoRetrain && strings.TrimSpace(in.RetrainPipelineID) == "" {
		return Monitor{}, false, ErrRetrainPipelineRequired
	}

	now := s.now()

	// --- LOAD-OR-INIT the monitor (upsert) --------------------------------
	existing, err := s.monitors.GetByModel(ctx, ownerTeam, in.ModelName)
	creating := false
	if err != nil {
		if !errors.Is(err, ErrRepoNotFound) {
			return Monitor{}, false, fmt.Errorf("load monitor: %w", err)
		}
		creating = true
	}

	m := existing
	if creating {
		// SERVER-AUTHORITATIVE identity & audit set here, never from the request.
		m = Monitor{
			ID:        uuid.NewString(),
			ModelName: in.ModelName,
			OwnerTeam: ownerTeam, // from auth claims — the tenancy fact
			State:     MonitorStatePendingBaseline,
			CreatedAt: now,
		}
	}

	// Apply ONLY the operator-owned fields (mass-assignment guard realized in code:
	// we never copy id/owner_team/state/baseline_* from `in` — `in` has no such
	// fields). owner_team is never reassigned on update (a model's monitor cannot
	// change tenant via configure).
	m.WindowDuration = in.WindowDuration
	m.WindowSize = in.WindowSize
	m.MinSamples = in.MinSamples
	m.Thresholds = in.Thresholds
	m.AutoRetrain = in.AutoRetrain
	m.RetrainPipelineID = in.RetrainPipelineID
	m.UpdatedAt = now

	// --- LIFECYCLE STATE (server-advanced) --------------------------------
	// enabled=false ⇒ PAUSED (events still windowed, nothing scored/fired).
	// enabled=true ⇒ resume into the appropriate non-paused state. We resolve the
	// baseline lazily here so a brand-new monitor can reach WARMING_UP/ACTIVE; if
	// the baseline isn't available yet the monitor stays PENDING_BASELINE (it will
	// be re-pinned on the next ModelPromoted or a manual ResetBaseline).
	if !in.Enabled {
		m.State = MonitorStatePaused
	} else {
		if err := s.resolveBaselineInto(ctx, &m, ""); err != nil {
			if errors.Is(err, ErrBaselineUnavailable) {
				m.State = MonitorStatePendingBaseline // not an error — just not ready
			} else {
				return Monitor{}, false, err
			}
		} else {
			// Baseline present. WARMING_UP until the live window reaches min_samples;
			// the scorer promotes to ACTIVE when it scores. We can't know the live
			// fill cheaply here, so we set WARMING_UP and let the data plane advance.
			m.State = MonitorStateWarmingUp
		}
	}

	stored, created, err := s.monitors.Upsert(ctx, m)
	if err != nil {
		return Monitor{}, false, fmt.Errorf("upsert monitor: %w", err)
	}
	return stored, created, nil
}

func (s *monitorService) DeleteMonitor(ctx context.Context, ownerTeam, modelName string, purgeReports bool) (int, error) {
	if strings.TrimSpace(ownerTeam) == "" || strings.TrimSpace(modelName) == "" {
		return 0, fmt.Errorf("%w: owner_team and model_name are required", ErrValidation)
	}
	// Soft-delete the config. Delete is idempotent (deleting an absent monitor is a
	// successful no-op), so a retried DeleteMonitor doesn't NOT_FOUND-on-retry.
	if err := s.monitors.Delete(ctx, ownerTeam, modelName); err != nil {
		return 0, fmt.Errorf("delete monitor: %w", err)
	}
	purged := 0
	if purgeReports {
		// TENANCY: purge is scoped by (ownerTeam, modelName). A bug here would let
		// team-a's "delete my fraud monitor + purge" hard-delete team-b's same-named
		// "fraud" drift history — cross-tenant data destruction. The team scope makes
		// the purge touch ONLY this caller's reports.
		n, err := s.reports.PurgeByModel(ctx, ownerTeam, modelName)
		if err != nil {
			return 0, fmt.Errorf("purge reports: %w", err)
		}
		purged = n
	}
	return purged, nil
}

func (s *monitorService) ResetBaseline(ctx context.Context, ownerTeam, modelName, version string) (Monitor, error) {
	if strings.TrimSpace(ownerTeam) == "" || strings.TrimSpace(modelName) == "" {
		return Monitor{}, fmt.Errorf("%w: owner_team and model_name are required", ErrValidation)
	}
	m, err := s.monitors.GetByModel(ctx, ownerTeam, modelName)
	if err != nil {
		if errors.Is(err, ErrRepoNotFound) {
			return Monitor{}, ErrMonitorNotFound
		}
		return Monitor{}, fmt.Errorf("load monitor: %w", err)
	}
	// SERVER-RESOLVED baseline: the caller named a VERSION (or "" = current prod),
	// never a distribution. resolveBaselineInto fetches the version's training-time
	// summary itself, so a client cannot supply a hand-crafted baseline.
	if err := s.resolveBaselineInto(ctx, &m, version); err != nil {
		return Monitor{}, err // ErrBaselineUnavailable surfaces as-is
	}
	m.State = MonitorStateWarmingUp // fresh baseline ⇒ re-warm before scoring
	m.UpdatedAt = s.now()
	stored, _, err := s.monitors.Upsert(ctx, m)
	if err != nil {
		return Monitor{}, fmt.Errorf("persist reset baseline: %w", err)
	}
	return stored, nil
}

func (s *monitorService) ResetBaselineFromPromotion(ctx context.Context, ownerTeam, modelName, newProdVersion string) error {
	if strings.TrimSpace(ownerTeam) == "" || strings.TrimSpace(modelName) == "" || strings.TrimSpace(newProdVersion) == "" {
		return fmt.Errorf("%w: owner_team, model_name and new prod version are required", ErrValidation)
	}
	// Reuse the team-scoped manual path — the only difference is the trigger (an
	// event vs an operator). A promoted model that isn't monitored is a no-op: there
	// is simply no baseline to reset, which on an event path is success, not an error.
	_, err := s.ResetBaseline(ctx, ownerTeam, modelName, newProdVersion)
	if errors.Is(err, ErrMonitorNotFound) {
		return nil
	}
	return err
}

// ============================================================================
// CONTROL PLANE — reads
// ============================================================================

func (s *monitorService) GetModelHealth(ctx context.Context, ownerTeam, modelName string) (ModelHealth, error) {
	m, err := s.monitors.GetByModel(ctx, ownerTeam, modelName)
	if err != nil {
		if errors.Is(err, ErrRepoNotFound) {
			return ModelHealth{}, ErrMonitorNotFound
		}
		return ModelHealth{}, fmt.Errorf("load monitor: %w", err)
	}
	return s.healthFor(ctx, m)
}

// healthFor builds the at-a-glance health rollup from a monitor + its latest
// report. Reused by GetModelHealth and the fleet view (no N+1).
func (s *monitorService) healthFor(ctx context.Context, m Monitor) (ModelHealth, error) {
	h := ModelHealth{
		ModelName:      m.ModelName,
		State:          m.State,
		SeverityByType: map[DriftType]DriftSeverity{},
	}
	// TENANCY: scope the latest-report lookup by the monitor's own team. Without it,
	// a same-named model in another team could surface its latest verdict on THIS
	// team's health tile (cross-tenant read). m.OwnerTeam is server-set on the
	// monitor, so it's the authoritative scope.
	latest, err := s.reports.LatestByModel(ctx, m.OwnerTeam, m.ModelName)
	if err != nil {
		if errors.Is(err, ErrRepoNotFound) {
			return h, nil // no reports yet — healthy-but-unknown, state carries WARMING_UP
		}
		return ModelHealth{}, fmt.Errorf("latest report: %w", err)
	}
	h.ModelVersion = latest.ModelVersion
	h.OverallSeverity = latest.Severity
	h.LatestReportID = latest.ID
	h.LastEventAt = latest.WindowEnd
	// Per-type lights from the latest report's metrics (max severity per type is the
	// report-level severity for its type; here we record the report's headline type).
	h.SeverityByType[latest.DriftType] = latest.Severity
	return h, nil
}

func (s *monitorService) GetMonitorStatus(ctx context.Context, ownerTeam, modelName string) (MonitorStatus, error) {
	m, err := s.monitors.GetByModel(ctx, ownerTeam, modelName)
	if err != nil {
		if errors.Is(err, ErrRepoNotFound) {
			return MonitorStatus{}, ErrMonitorNotFound
		}
		return MonitorStatus{}, fmt.Errorf("load monitor: %w", err)
	}
	st := MonitorStatus{Monitor: m, State: m.State}
	fill, err := s.windows.CurrentSampleCount(ctx, m.ID)
	if err != nil {
		return MonitorStatus{}, fmt.Errorf("window fill: %w", err)
	}
	st.CurrentWindowSamples = fill
	// TENANCY: same team scope as healthFor — the live status' latest report must be
	// this team's, not a same-named model's in another tenant.
	latest, err := s.reports.LatestByModel(ctx, m.OwnerTeam, m.ModelName)
	if err == nil {
		st.LatestReport = &latest
		st.LastEventAt = latest.WindowEnd
	} else if !errors.Is(err, ErrRepoNotFound) {
		return MonitorStatus{}, fmt.Errorf("latest report: %w", err)
	}
	return st, nil
}

func (s *monitorService) ListMonitors(ctx context.Context, ownerTeam string, minSeverity DriftSeverity, state MonitorState, opts ListOptions) ([]FleetEntry, string, error) {
	if strings.TrimSpace(ownerTeam) == "" {
		return nil, "", fmt.Errorf("%w: owner_team is required (from auth claims)", ErrValidation)
	}
	opts = capPage(opts)
	f := FleetFilter{OwnerTeam: ownerTeam, MinSeverity: minSeverity, State: state}
	ms, next, err := s.monitors.List(ctx, f, opts)
	if err != nil {
		return nil, "", fmt.Errorf("list monitors: %w", err)
	}
	entries := make([]FleetEntry, 0, len(ms))
	for _, m := range ms {
		h, err := s.healthFor(ctx, m)
		if err != nil {
			return nil, "", err
		}
		entries = append(entries, FleetEntry{Monitor: m, Health: h})
	}
	return entries, next, nil
}

func (s *monitorService) GetDriftReport(ctx context.Context, ownerTeam, reportID string) (DriftReport, error) {
	// owner_team comes from auth claims (handler-set). Empty ⇒ programming error: an
	// unscoped GetByID would be the IDOR we're guarding against.
	if strings.TrimSpace(ownerTeam) == "" {
		return DriftReport{}, fmt.Errorf("%w: owner_team is required (from auth claims)", ErrValidation)
	}
	if strings.TrimSpace(reportID) == "" {
		return DriftReport{}, fmt.Errorf("%w: report_id is required", ErrValidation)
	}
	// AUTHZ, not obscurity: report ids are NOT secret (they ride on events, retrain
	// requests, deep links, logs). The team-scoped GetByID returns ErrRepoNotFound
	// when the id exists but belongs to another team — INDISTINGUISHABLE from "no
	// such id", so a caller can neither read nor enumerate other tenants' reports.
	r, err := s.reports.GetByID(ctx, ownerTeam, reportID)
	if err != nil {
		if errors.Is(err, ErrRepoNotFound) {
			return DriftReport{}, ErrReportNotFound
		}
		return DriftReport{}, fmt.Errorf("get report: %w", err)
	}
	return r, nil
}

func (s *monitorService) ListDriftReports(ctx context.Context, ownerTeam string, f ReportFilter, opts ListOptions) ([]DriftReport, string, error) {
	if strings.TrimSpace(ownerTeam) == "" {
		return nil, "", fmt.Errorf("%w: owner_team is required (from auth claims)", ErrValidation)
	}
	// model_name is now an OPTIONAL filter, not a hard requirement. WHY the change:
	// the dashboard's "recent drift across the fleet" tile and the Monitoring page's
	// unfiltered list both need a team-wide history with no single model in mind. The
	// old hard reject made those callers pass an empty model_name and get a 400, so the
	// tile was permanently broken. TENANCY is still fully enforced below: owner_team is
	// always set from claims and is the mandatory partition key, so an empty model_name
	// means "every model THIS team owns", never a cross-tenant read. When model_name IS
	// provided it still narrows to that one model (a same-named model in another team
	// stays invisible because owner_team scopes first).
	//
	// TENANCY: OVERWRITE the filter's owner_team with the claim-derived value (never
	// trust a client-supplied OwnerTeam on the filter). It is the always-present scope;
	// model_name, when set, narrows within it.
	f.OwnerTeam = ownerTeam
	opts = capPage(opts)
	rs, next, err := s.reports.List(ctx, f, opts)
	if err != nil {
		return nil, "", fmt.Errorf("list reports: %w", err)
	}
	return rs, next, nil
}

// ============================================================================
// FEEDBACK PLANE — SubmitGroundTruth (delayed labels → performance decay)
// ============================================================================

func (s *monitorService) SubmitGroundTruth(ctx context.Context, ownerTeam string, in SubmitGroundTruthInput) (SubmitGroundTruthResult, error) {
	if strings.TrimSpace(ownerTeam) == "" || strings.TrimSpace(in.ModelName) == "" {
		return SubmitGroundTruthResult{}, fmt.Errorf("%w: owner_team and model_name are required", ErrValidation)
	}
	// CONTRACT CAP: a larger batch is rejected (bounds the transactional write and
	// protects the server from an unbounded request). Not silently truncated, so the
	// caller notices and pages.
	if len(in.Labels) > MaxGroundTruthBatch {
		return SubmitGroundTruthResult{}, fmt.Errorf("%w: batch of %d exceeds cap %d", ErrValidation, len(in.Labels), MaxGroundTruthBatch)
	}
	// Verify the team owns the monitor before recording (tenant scope; a model with
	// no monitor isn't accepting ground truth).
	if _, err := s.monitors.GetByModel(ctx, ownerTeam, in.ModelName); err != nil {
		if errors.Is(err, ErrRepoNotFound) {
			return SubmitGroundTruthResult{}, ErrMonitorNotFound
		}
		return SubmitGroundTruthResult{}, fmt.Errorf("load monitor: %w", err)
	}

	res := SubmitGroundTruthResult{}
	for _, lbl := range in.Labels {
		// SECURITY: cap label length and reject empties (a label is a class name, not
		// a payload; an unbounded label is a memory + PII-smuggling footgun).
		if strings.TrimSpace(lbl.RequestID) == "" {
			res.UnmatchedRequestIDs = append(res.UnmatchedRequestIDs, lbl.RequestID)
			continue
		}
		if len(lbl.ActualLabel) > MaxLabelBytes {
			return SubmitGroundTruthResult{}, fmt.Errorf("%w: label for %q exceeds %d bytes", ErrValidation, lbl.RequestID, MaxLabelBytes)
		}
		observedAt := lbl.ObservedAt
		if observedAt.IsZero() {
			observedAt = s.now()
		}
		// ANTI-FABRICATION: RecordLabel returns matched=false for a request_id the
		// monitor never observed (or that aged out). We report it back, never
		// trusting a fabricated id to skew measured accuracy.
		matched, err := s.truth.RecordLabel(ctx, in.ModelName, lbl.RequestID, lbl.ActualLabel, observedAt)
		if err != nil {
			return SubmitGroundTruthResult{}, fmt.Errorf("record label: %w", err)
		}
		if matched {
			res.Accepted++
		} else {
			res.UnmatchedRequestIDs = append(res.UnmatchedRequestIDs, lbl.RequestID)
		}
	}
	return res, nil
}

// ============================================================================
// DATA PLANE — ObserveInference: the streaming-aggregation + closed-loop heartbeat
// ============================================================================
//
// FLOW (per consumed InferenceCompleted event):
//
//  1. Resolve the monitor for the model (team-agnostic on the event path — the
//     events adapter passes the team it resolved; here we look up by the obs's
//     monitor binding). If no monitor / paused, fold-only or skip.
//  2. LOAD-OR-OPEN the live window; FOLD the observation in (idempotent on request_id).
//  3. RECORD the (request_id → prediction) for later ground-truth joining.
//  4. If the window is now CLOSED (count OR time bound):
//     a. If it has < MinSamples → close it without scoring (stay WARMING_UP).
//     b. Else SCORE it against the baseline (data + prediction + performance).
//     c. PERSIST the report idempotently on window_id (Save returns inserted=false
//     on a redelivery → we do NOT re-emit).
//     d. Run the POLICY (DecideRetrain): emit ModelDriftDetected; if CRITICAL +
//     auto_retrain + cooldown-elapsed → TriggerRetrain (closes the loop) and
//     record the cooldown.
//     e. CLOSE the window so the next event opens a fresh one.
//
// Returns the produced report when a window scored, else nil (a plain fold).
func (s *monitorService) ObserveInference(ctx context.Context, obs InferenceObservation) (*DriftReport, error) {
	// The events adapter resolves model → team → monitor and stamps MonitorID. An
	// empty MonitorID means the model has no monitor → nothing to do (folding an
	// unmonitored model's traffic is meaningless and would leak memory). This is the
	// AUTHORIZED-BINDING boundary: the domain never derives tenancy from event data.
	if strings.TrimSpace(obs.MonitorID) == "" {
		return nil, nil
	}
	monitorID, modelName := obs.MonitorID, obs.ModelName

	now := s.now()
	w, err := s.windows.LoadOrOpen(ctx, monitorID, modelName, now)
	if err != nil {
		return nil, fmt.Errorf("load/open window: %w", err)
	}
	// FOLD (streaming aggregation). Duplicate request_ids within the window are
	// ignored so a redelivered event doesn't skew the score it feeds.
	w.Add(obs)
	// Record the prediction for delayed ground-truth joining (performance decay).
	if obs.RequestID != "" && obs.PredictedTop != "" {
		if err := s.truth.RecordPrediction(ctx, modelName, obs.RequestID, obs.PredictedTop, obs.ObservedAt); err != nil {
			return nil, fmt.Errorf("record prediction: %w", err)
		}
	}
	if err := s.windows.Save(ctx, w); err != nil {
		return nil, fmt.Errorf("save window: %w", err)
	}

	// Load the monitor config (by the adapter-resolved id) to evaluate close + score.
	m, err := s.monitors.GetByID(ctx, monitorID)
	if err != nil {
		if errors.Is(err, ErrRepoNotFound) {
			return nil, nil // monitor deleted mid-window — drop the orphaned window quietly
		}
		return nil, fmt.Errorf("load monitor by id: %w", err)
	}
	// Paused monitors window but never score/fire.
	if m.State == MonitorStatePaused {
		return nil, nil
	}
	if !w.IsClosed(m, now) {
		return nil, nil // window still open — just folded
	}

	// Window closed. Below MinSamples → retire without scoring (stay WARMING_UP).
	if !w.HasEnoughSamples(m) {
		if err := s.windows.Close(ctx, monitorID, w.ID); err != nil {
			return nil, fmt.Errorf("close window: %w", err)
		}
		return nil, nil
	}

	// SCORE the closed window against the baseline.
	report, err := s.scoreWindow(ctx, m, w, now)
	if err != nil {
		if errors.Is(err, ErrBaselineUnavailable) || errors.Is(err, ErrInsufficientSamples) {
			// Cannot score yet — retire the window, no report.
			_ = s.windows.Close(ctx, monitorID, w.ID)
			return nil, nil
		}
		return nil, err
	}

	// PERSIST idempotently on window_id. inserted=false ⇒ a redelivery already
	// produced this report; do NOT re-emit (exactly-once-in-effect).
	stored, inserted, err := s.reports.Save(ctx, report)
	if err != nil {
		return nil, fmt.Errorf("save report: %w", err)
	}
	if err := s.windows.Close(ctx, monitorID, w.ID); err != nil {
		return nil, fmt.Errorf("close window: %w", err)
	}
	if !inserted {
		return &stored, nil // already handled on a prior delivery
	}

	// CLOSED-LOOP POLICY: emit + (maybe) trigger.
	if err := s.applyPolicy(ctx, m, stored, now); err != nil {
		return &stored, err
	}
	return &stored, nil
}

// scoreWindow computes the drift report for a closed window against its baseline.
// It evaluates each configured drift type, builds per-metric scores, derives each
// metric's severity from the matching threshold, and rolls up the report's
// headline type + max severity.
func (s *monitorService) scoreWindow(ctx context.Context, m Monitor, w *Window, now time.Time) (DriftReport, error) {
	base, err := s.baselines.GetBaseline(ctx, m.ModelName, m.BaselineVersion)
	if err != nil {
		return DriftReport{}, err // ErrBaselineUnavailable surfaces
	}

	var metrics []DriftMetric

	// --- DATA DRIFT (per feature) -----------------------------------------
	if th, ok := m.ThresholdFor(DriftTypeData); ok {
		for _, feat := range w.Features() {
			edges := base.FeatureEdges[feat]
			baseHist, hasBase := base.FeatureBaselines[feat]
			if !hasBase || len(edges) < 2 {
				continue // baseline didn't capture this feature — skip, don't invent drift
			}
			currHist, ok := w.FeatureHistogram(feat, edges)
			if !ok {
				continue
			}
			score, err := ComputeDrift(th.Method, baseHist, currHist)
			if err != nil {
				if errors.Is(err, ErrEmptyDistribution) || errors.Is(err, ErrBinMismatch) {
					continue // not scorable for this feature — skip
				}
				return DriftReport{}, err
			}
			metrics = append(metrics, DriftMetric{
				Name:          feat,
				Method:        th.Method,
				Score:         score,
				BaselineValue: histMean(baseHist),
				CurrentValue:  histMean(currHist),
				Severity:      th.Severity(score),
			})
		}
	}

	// --- PREDICTION DRIFT (output distribution) ---------------------------
	if th, ok := m.ThresholdFor(DriftTypePrediction); ok && len(base.PredictionOrder) > 0 {
		currHist := w.PredictionHistogram(base.PredictionOrder)
		score, err := ComputeDrift(th.Method, base.PredictionBaseline, currHist)
		if err == nil {
			metrics = append(metrics, DriftMetric{
				Name:     "prediction_distribution",
				Method:   th.Method,
				Score:    score,
				Severity: th.Severity(score),
			})
		} else if !errors.Is(err, ErrEmptyDistribution) && !errors.Is(err, ErrBinMismatch) {
			return DriftReport{}, err
		}
	}

	// --- PERFORMANCE DECAY (rolling accuracy vs baseline) -----------------
	if th, ok := m.ThresholdFor(DriftTypePerformance); ok {
		pairs, err := s.truth.RecentPairs(ctx, m.ModelName, performanceWindow)
		if err != nil {
			return DriftReport{}, fmt.Errorf("recent pairs: %w", err)
		}
		if len(pairs) > 0 {
			drop := PerformanceDrop(base.Accuracy, pairs)
			metrics = append(metrics, DriftMetric{
				Name:          "accuracy",
				Method:        th.Method, // method is informational for performance (a drop magnitude)
				Score:         drop,
				BaselineValue: base.Accuracy,
				CurrentValue:  Accuracy(pairs),
				Severity:      th.Severity(drop),
			})
		}
	}

	// Roll up: headline type = the type of the worst metric; severity = max.
	headline, overall := rollup(metrics)

	return DriftReport{
		ID:        uuid.NewString(),
		MonitorID: m.ID,
		// TENANCY: stamp the owning team onto the report (server-authoritative, from
		// the monitor — never from event data). This is what makes every later report
		// read/purge tenant-scopable; without it, reports keyed by model name alone
		// collide across teams that share a model name.
		OwnerTeam:    m.OwnerTeam,
		ModelName:    m.ModelName,
		ModelVersion: w.ModelVersion,
		DriftType:    headline,
		Severity:     overall,
		Metrics:      metrics,
		WindowID:     w.ID, // idempotency key
		SampleCount:  w.SampleCount,
		WindowStart:  w.OpenedAt,
		WindowEnd:    now,
		CreatedAt:    now,
	}, nil
}

// applyPolicy runs the closed-loop control decision and acts on it: emit the
// drift event, and if the policy says so, trigger the retrain pipeline (recording
// the cooldown atomically so concurrent breaches can't double-fire).
func (s *monitorService) applyPolicy(ctx context.Context, m Monitor, report DriftReport, now time.Time) error {
	last, err := s.gate.LastTriggered(ctx, m.ModelName)
	if err != nil {
		return fmt.Errorf("cooldown lookup: %w", err)
	}
	decision := DecideRetrain(report, m, last, s.cooldown, now)

	if decision.Emit {
		ev := DriftEvent{
			Report:            report,
			AutoRetrain:       m.AutoRetrain,
			RetrainPipelineID: m.RetrainPipelineID,
			OwnerTeam:         m.OwnerTeam,
		}
		if err := s.publisher.PublishDrift(ctx, ev); err != nil {
			return fmt.Errorf("publish drift: %w", err)
		}
	}

	if decision.Trigger {
		// Record the cooldown BEFORE triggering so a crash after the trigger doesn't
		// leave the gate open for an immediate re-fire on redelivery. MarkTriggered is
		// atomic set-if-absent-within-cooldown in the adapter, so two concurrent
		// breaches race to set it and only one proceeds.
		if err := s.gate.MarkTriggered(ctx, m.ModelName, now); err != nil {
			return fmt.Errorf("mark retrain cooldown: %w", err)
		}
		_, err := s.orch.TriggerRetrain(ctx, RetrainRequest{
			PipelineID:   m.RetrainPipelineID, // opaque, operator-configured (NOT from event data → no SSRF)
			ModelName:    m.ModelName,
			ModelVersion: report.ModelVersion,
			ReportID:     report.ID,
			Reason:       retrainReason(report),
		})
		if err != nil {
			return fmt.Errorf("trigger retrain: %w", err)
		}
	}
	return nil
}

// ============================================================================
// HELPERS
// ============================================================================

// resolveBaselineInto fetches a version's baseline and writes the resolved
// baseline_* fields onto the monitor (server-authoritative). version "" = current
// prod. Surfaces ErrBaselineUnavailable so the caller can keep the monitor
// PENDING_BASELINE instead of failing the whole configure.
func (s *monitorService) resolveBaselineInto(ctx context.Context, m *Monitor, version string) error {
	base, err := s.baselines.GetBaseline(ctx, m.ModelName, version)
	if err != nil {
		return err
	}
	m.BaselineVersion = base.Version
	m.BaselineCapturedAt = base.CapturedAt
	return nil
}

// validateWindowShape enforces the server-side window bounds. WHY each bound:
//   - size in [0, MaxWindowSize]: the upper cap is a MEMORY guard (the live window
//     is in Redis; an absurd size pins gigabytes). Rejected, not clamped, so the
//     operator notices.
//   - 0 <= min_samples <= size (when size>0): you cannot require more samples than
//     the window can hold, else it never scores.
//   - at least one of duration/size set: with neither, a window never closes and
//     the monitor never scores — a silent dead config we reject up front.
func validateWindowShape(dur time.Duration, size, minSamples int) error {
	if size < 0 || size > MaxWindowSize {
		return fmt.Errorf("%w: window_size %d out of range [0, %d]", ErrValidation, size, MaxWindowSize)
	}
	if minSamples < 0 {
		return fmt.Errorf("%w: min_samples must be >= 0", ErrValidation)
	}
	if size > 0 && minSamples > size {
		return fmt.Errorf("%w: min_samples %d cannot exceed window_size %d", ErrValidation, minSamples, size)
	}
	if dur < 0 {
		return fmt.Errorf("%w: window_duration must be >= 0", ErrValidation)
	}
	if dur == 0 && size == 0 {
		return fmt.Errorf("%w: at least one of window_duration or window_size must be set (else windows never close)", ErrValidation)
	}
	return nil
}

// validateThresholds enforces warn <= critical per threshold so the severity
// ladder is monotone (a critical below its warn would make WARNING unreachable).
func validateThresholds(ths []ThresholdConfig) error {
	for _, t := range ths {
		if t.DriftType == DriftTypeUnspecified {
			return fmt.Errorf("%w: threshold has unspecified drift_type", ErrValidation)
		}
		if t.Method == DriftMethodUnspecified {
			return fmt.Errorf("%w: threshold for %v has unspecified method", ErrValidation, t.DriftType)
		}
		if t.WarnScore > t.CriticalScore {
			return fmt.Errorf("%w: warn_score %g exceeds critical_score %g for %v", ErrValidation, t.WarnScore, t.CriticalScore, t.DriftType)
		}
	}
	return nil
}

// capPage applies the default + the platform-wide cap to a page size. A larger
// client request is CAPPED (DoS / accidental-firehose guard), never honored.
func capPage(opts ListOptions) ListOptions {
	if opts.PageSize <= 0 {
		opts.PageSize = DefaultListPageSize
	}
	if opts.PageSize > MaxListPageSize {
		opts.PageSize = MaxListPageSize
	}
	return opts
}

// rollup reduces per-metric results to the report's headline (the drift type of
// the worst metric) and overall severity (the max across metrics). Empty metrics
// ⇒ a degenerate OK report (data flowed but nothing was scorable/drifted).
func rollup(metrics []DriftMetric) (DriftType, DriftSeverity) {
	overall := DriftSeverityOK
	headline := DriftTypeUnspecified
	for _, m := range metrics {
		if m.Severity > overall {
			overall = m.Severity
			headline = driftTypeForMetric(m.Name)
		}
	}
	if headline == DriftTypeUnspecified && len(metrics) > 0 {
		headline = driftTypeForMetric(metrics[0].Name)
	}
	return headline, overall
}

// driftTypeForMetric maps a metric name back to its drift type for the report
// headline. The named outputs ("prediction_distribution", "accuracy") map to
// their types; everything else is a feature ⇒ data drift.
func driftTypeForMetric(name string) DriftType {
	switch name {
	case "prediction_distribution":
		return DriftTypePrediction
	case "accuracy", "f1":
		return DriftTypePerformance
	default:
		return DriftTypeData
	}
}

// histMean returns the count-weighted midpoint mean of a histogram for the
// report's "was X, now Y" context value. For a categorical histogram (no edges)
// it returns 0 (the UI uses the per-class breakdown instead).
func histMean(h Histogram) float64 {
	if len(h.Edges) != len(h.Counts)+1 {
		return 0
	}
	total := h.Total()
	if total == 0 {
		return 0
	}
	var sum float64
	for i, c := range h.Counts {
		mid := (h.Edges[i] + h.Edges[i+1]) / 2
		sum += mid * c
	}
	return sum / total
}

// retrainReason builds the human-readable audit string carried on the retrain
// trigger ("data drift PSI=0.41 on income"). It names the worst metric.
func retrainReason(r DriftReport) string {
	worst := DriftMetric{}
	for _, m := range r.Metrics {
		if m.Score > worst.Score {
			worst = m
		}
	}
	return fmt.Sprintf("%v drift: %s=%.4f (severity %v) over %d samples",
		r.DriftType, worst.Name, worst.Score, r.Severity, r.SampleCount)
}
