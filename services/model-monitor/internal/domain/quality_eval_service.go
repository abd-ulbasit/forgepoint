// quality_eval_service.go — the L4 QUALITY-EVAL use-case: judge a sampled
// completion, record the eval, and (when quality has drifted) raise a FIRST-CLASS
// drift report through the EXISTING alert + retrain machinery.
//
// ============================================================================
// WHY A SEPARATE SERVICE (not more methods on monitorService)
// ============================================================================
//
// monitorService owns the tabular streaming-aggregation loop (windows in Redis,
// PSI/KL/KS scoring, ground-truth joins). L4 quality eval is a DIFFERENT pipeline —
// it has no Redis window, no histogram baseline; its "window" is the last N judged
// scores in Postgres. Bolting it onto monitorService would bloat that type's port
// set (it already needs eight ports) and entangle two unrelated aggregation models.
//
// So QualityEvalService is its own use-case that COMPOSES the eval-specific ports
// (Judge, EvalStore, Sampler) with the SHARED closed-loop ports the tabular loop
// already defines and main.go already wires (DriftReportRepository, DriftPublisher,
// RetrainGate, Orchestrator, MonitorRepository). The payoff: quality drift travels
// the EXACT SAME path as data/prediction/performance drift —
//
//	build DriftReport → reports.Save (idempotent) → DecideRetrain (the SAME policy)
//	→ publisher.PublishDrift (fp.models.drift.detected) → (CRITICAL+auto) TriggerRetrain
//
// — so the serve→monitor→retrain loop closes for LLM quality with ZERO new event,
// ZERO new proto, and ZERO new schema for the alert/retrain side. The consumer hands
// this service a Completion; the service does sample-judge-record-drift-act.
package domain

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// QualityEvalService is the L4 use-case port. The consumer (events adapter) drives
// it; tests inject fakes for every dependency.
type QualityEvalService interface {
	// EvaluateCompletion is the per-sampled-completion entry point. It judges the
	// completion, records the eval (scored or unscored), then evaluates quality drift
	// over the rolling window and — if drifted — raises a drift report through the
	// shared alert/retrain loop. monitorID is the AUTHORIZED binding the events adapter
	// resolved (model → monitor); the service loads the monitor by id for owner_team +
	// the auto_retrain/pipeline config (server-authoritative, never from event data).
	//
	// Returns the drift report when a quality-drift report was raised, else nil (a
	// plain record). An UNSCORED judgment is recorded and returns (nil, nil) — judging
	// is best-effort and an un-judgeable completion is not an error.
	EvaluateCompletion(ctx context.Context, monitorID string, c Completion) (*DriftReport, error)
}

// qualityEvalService is the concrete use-case.
type qualityEvalService struct {
	judge     Judge
	evals     EvalStore
	monitors  MonitorRepository
	reports   DriftReportRepository
	publisher DriftPublisher
	orch      Orchestrator
	gate      RetrainGate

	cfg      QualityDriftConfig
	warn     float64 // score (drop magnitude) at/above which a report is WARNING
	critical float64 // score at/above which a report is CRITICAL (arms retrain)
	cooldown time.Duration
	now      func() time.Time
}

// QualityEvalDeps is the constructor injection bag (all ports + config).
type QualityEvalDeps struct {
	Judge     Judge
	Evals     EvalStore
	Monitors  MonitorRepository
	Reports   DriftReportRepository
	Publisher DriftPublisher
	Orch      Orchestrator
	Gate      RetrainGate

	// Config governs the rolling window + the floor/baseline drift rules.
	Config QualityDriftConfig
	// WarnDrop / CriticalDrop map the quality DROP (the bigger-is-worse score from
	// QualityDriftScore) onto the severity ladder, exactly like a ThresholdConfig's
	// warn/critical. WarnDrop <= CriticalDrop. e.g. warn at a 0.5-point drop, critical
	// at a 1.0-point drop below the reference. A drift verdict is at LEAST WARNING (the
	// floor/baseline rule already decided it drifted); these decide WARNING vs CRITICAL.
	WarnDrop     float64
	CriticalDrop float64

	// Cooldown is the anti-storm gap between auto-retrains for one model (shared with
	// the tabular loop's cooldown semantics).
	Cooldown time.Duration
	// Now is the injected clock (nil → time.Now). Tests pin it.
	Now func() time.Time
}

// NewQualityEvalService wires the use-case. A nil Now → time.Now; a non-positive
// cooldown → defaultRetrainCooldown (shared with the tabular loop).
func NewQualityEvalService(deps QualityEvalDeps) QualityEvalService {
	now := deps.Now
	if now == nil {
		now = time.Now
	}
	cooldown := deps.Cooldown
	if cooldown <= 0 {
		cooldown = defaultRetrainCooldown
	}
	return &qualityEvalService{
		judge:     deps.Judge,
		evals:     deps.Evals,
		monitors:  deps.Monitors,
		reports:   deps.Reports,
		publisher: deps.Publisher,
		orch:      deps.Orch,
		gate:      deps.Gate,
		cfg:       deps.Config,
		warn:      deps.WarnDrop,
		critical:  deps.CriticalDrop,
		cooldown:  cooldown,
		now:       now,
	}
}

// Compile-time proof the concrete type satisfies the interface.
var _ QualityEvalService = (*qualityEvalService)(nil)

// qualityMetricName is the metric name that marks a DriftReport as an LLM-quality
// verdict (vs a tabular accuracy drop). Both are DriftTypePerformance; the NAME is
// the discriminator the UI / alert templates branch on. Kept as a const so the
// producer here and any future reader agree on the exact string.
const qualityMetricName = "llm_quality"

// EvaluateCompletion: judge → record → drift → act. See the interface doc.
func (s *qualityEvalService) EvaluateCompletion(ctx context.Context, monitorID string, c Completion) (*DriftReport, error) {
	// The events adapter stamps the authorized monitor binding. An empty id means the
	// model isn't monitored → nothing to do (the consumer normally short-circuits
	// before calling us, but defend anyway — never fabricate a tenant from event data).
	if monitorID == "" {
		return nil, nil
	}

	now := s.now()

	// --- 1. JUDGE -----------------------------------------------------------
	// Best-effort: the judge returns Unscored (never an error we must surface) for any
	// failure it can absorb. We log-and-continue on a transient transport error (the
	// returned scores are Unscored regardless), so judging never blocks the pipeline.
	scores, judgeErr := s.judge.Score(ctx, c)
	// judgeErr is purely informational here (the consumer logs it); scores is already
	// Unscored on any failure, which is the value we persist. We deliberately do NOT
	// propagate judgeErr as a method error: a flaky judge must not NAK-storm the
	// consumer. A rising UNSCORED ratio is the observable signal instead.
	_ = judgeErr

	// --- 2. RECORD ----------------------------------------------------------
	// Persist the eval (scored or unscored), idempotent on request_id so a redelivered
	// completion doesn't double-count in the rolling average.
	eval := Eval{
		Team:      c.Team,
		Model:     c.Model,
		RequestID: c.RequestID,
		Scores:    scores,
		CreatedAt: now,
	}
	if err := s.evals.Record(ctx, eval); err != nil {
		// A store error IS retryable (NAK): we want the eval persisted. The consumer
		// turns this into a NAK; idempotency makes the retry safe.
		return nil, fmt.Errorf("record eval: %w", err)
	}

	// An UNSCORED eval contributes nothing to the average and cannot drift the score —
	// stop here (recorded, no verdict). This is the graceful-degradation outcome when
	// the gateway sent no text or the judge couldn't parse the answer.
	if !scores.Scored {
		return nil, nil
	}

	// Quality-drift detection OFF (Window<=0) → record-only mode. The service still
	// judges + records (useful as an eval log) but never raises drift. This is the
	// self-gate: a deployment can run the judge for observability without arming the
	// quality-drift alert/retrain.
	if !s.cfg.Enabled() {
		return nil, nil
	}

	// --- 3. EVALUATE QUALITY DRIFT over the rolling window ------------------
	recent, err := s.evals.RecentOverall(ctx, c.Team, c.Model, s.cfg.Window)
	if err != nil {
		return nil, fmt.Errorf("load recent eval scores: %w", err)
	}
	verdict := EvaluateQualityDrift(recent, s.cfg)
	if !verdict.Drifted {
		return nil, nil // healthy or still warming up — recorded, no report
	}

	// --- 4. RAISE a FIRST-CLASS drift report (reuse the shared loop) -------
	// Load the monitor by the resolved id for owner_team + the closed-loop config
	// (auto_retrain + pipeline). Server-authoritative — never derived from the event.
	m, err := s.monitors.GetByID(ctx, monitorID)
	if err != nil {
		if errors.Is(err, ErrRepoNotFound) {
			return nil, nil // monitor deleted between resolve and now — drop quietly
		}
		return nil, fmt.Errorf("load monitor by id: %w", err)
	}
	if m.State == MonitorStatePaused {
		return nil, nil // paused monitors record evals but never alert/fire
	}

	report := s.buildQualityReport(m, c, verdict, now)

	// PERSIST idempotently on window_id. We synthesize a STABLE window id from the
	// triggering completion's request_id so a redelivered completion that re-drifts
	// produces the SAME window_id → Save returns inserted=false → we do NOT re-emit
	// (exactly-once-in-effect, the same discipline the tabular loop uses on window_id).
	stored, inserted, err := s.reports.Save(ctx, report)
	if err != nil {
		return nil, fmt.Errorf("save quality drift report: %w", err)
	}
	if !inserted {
		return &stored, nil // already handled on a prior delivery
	}

	// --- 5. CLOSED-LOOP POLICY — the SAME DecideRetrain + publish + trigger -
	if err := s.applyPolicy(ctx, m, stored, now); err != nil {
		return &stored, err
	}
	return &stored, nil
}

// buildQualityReport constructs a DriftReport for a quality-drift verdict. It is a
// DriftTypePerformance report (an LLM's judged quality decaying IS a performance
// regression) whose single metric is named qualityMetricName ("llm_quality") — that
// name is what distinguishes it from a tabular accuracy drop while reusing the SAME
// report type, schema, event, and policy. The score is the quality DROP (bigger =
// worse), mapped to a severity by the configured warn/critical drop thresholds.
func (s *qualityEvalService) buildQualityReport(m Monitor, c Completion, v QualityDriftResult, now time.Time) DriftReport {
	score := QualityDriftScore(v.Average, s.cfg)
	th := ThresholdConfig{
		DriftType:     DriftTypePerformance,
		Method:        DriftMethodUnspecified, // quality is a judged drop, not a statistical test
		WarnScore:     s.warn,
		CriticalScore: s.critical,
	}
	severity := th.Severity(score)
	// The floor/baseline rule already declared this a drift; if the configured warn
	// threshold is so high that the mapped severity came back OK, floor it to WARNING
	// so a real drift is never silently swallowed (alert a human at minimum).
	if severity < DriftSeverityWarning {
		severity = DriftSeverityWarning
	}

	metric := DriftMetric{
		Name:          qualityMetricName,
		Method:        DriftMethodUnspecified,
		Score:         score,
		BaselineValue: refScore(s.cfg), // the reference (floor/baseline) the avg is measured against
		CurrentValue:  v.Average,       // the rolling average that drifted
		Severity:      severity,
	}

	return DriftReport{
		ID:        uuid.NewString(),
		MonitorID: m.ID,
		// TENANCY: owner_team is server-set from the monitor (never from the event),
		// exactly like the tabular scorer — this is what makes the report tenant-scopable.
		OwnerTeam:    m.OwnerTeam,
		ModelName:    m.ModelName,
		ModelVersion: "", // LLM completions carry no served model VERSION on this event; left blank
		DriftType:    DriftTypePerformance,
		Severity:     severity,
		Metrics:      []DriftMetric{metric},
		// WINDOW ID is the idempotency key. We derive it deterministically from the
		// triggering completion's request_id so a redelivery collapses to one report.
		WindowID:    "llmq-" + c.RequestID,
		SampleCount: v.SampleCount,
		WindowStart: now, // the rolling window is "last N scores"; we record the verdict instant
		WindowEnd:   now,
		CreatedAt:   now,
	}
}

// applyPolicy runs the SHARED closed-loop decision for a quality-drift report — the
// IDENTICAL DecideRetrain → emit → (maybe) trigger path the tabular loop uses (see
// monitorService.applyPolicy). Duplicated here (not shared as a free function) only
// because the two services own their own port instances; the LOGIC is the same, so a
// reviewer sees quality drift gets no special-cased treatment — it alerts and retrains
// through the exact same gates (severity, auto_retrain, pipeline, cooldown).
func (s *qualityEvalService) applyPolicy(ctx context.Context, m Monitor, report DriftReport, now time.Time) error {
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
			return fmt.Errorf("publish quality drift: %w", err)
		}
	}

	if decision.Trigger {
		// Record the cooldown BEFORE triggering (same crash-safety reasoning as the
		// tabular loop: a crash after the trigger must not leave the gate open for an
		// immediate re-fire on redelivery).
		if err := s.gate.MarkTriggered(ctx, m.ModelName, now); err != nil {
			return fmt.Errorf("mark retrain cooldown: %w", err)
		}
		if _, err := s.orch.TriggerRetrain(ctx, RetrainRequest{
			PipelineID:   m.RetrainPipelineID, // opaque, operator-configured (no SSRF)
			ModelName:    m.ModelName,
			ModelVersion: report.ModelVersion,
			ReportID:     report.ID,
			Reason:       qualityRetrainReason(report),
		}); err != nil {
			return fmt.Errorf("trigger retrain: %w", err)
		}
	}
	return nil
}

// refScore returns the reference the rolling average is measured against (the max of
// the configured floor and baseline) — the BaselineValue surfaced on the metric for
// the "was X, now Y" context.
func refScore(cfg QualityDriftConfig) float64 {
	ref := cfg.FloorScore
	if cfg.Baseline > ref {
		ref = cfg.Baseline
	}
	return ref
}

// qualityRetrainReason builds the human-readable audit string carried on the retrain
// trigger for a quality drift ("llm quality drift: avg 2.40 (severity critical) over
// 30 evals").
func qualityRetrainReason(r DriftReport) string {
	var avg float64
	if len(r.Metrics) > 0 {
		avg = r.Metrics[0].CurrentValue
	}
	return fmt.Sprintf("llm quality drift: avg %.2f (severity %v) over %d evals",
		avg, r.Severity, r.SampleCount)
}
