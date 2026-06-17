// policy.go — the CLOSED-LOOP CONTROL decision: given a scored report and the
// monitor's config, decide whether to (a) emit the drift event and (b) fire the
// auto-retrain action. The "act" half of sense → decide → act.
//
// This is split out as PURE functions so the storm-prevention logic — the part an
// interviewer will poke at ("how do you stop it retraining every 5 seconds?") —
// is unit-testable with no clocks or I/O: pass the report, the config, the last
// trigger time, and `now`; get back a decision.
package domain

import "time"

// RetrainDecision is the policy's verdict for one scored report. The service acts
// on it: always persist the report; emit the event iff Emit; trigger the retrain
// iff Trigger (and then record the cooldown).
type RetrainDecision struct {
	// Emit is true when the report is actionable (severity >= WARNING) and should
	// be published as ModelDriftDetected. WARNING emits (so a human is alerted) but
	// does NOT trigger — only CRITICAL triggers. OK reports are persisted for the
	// time series but neither emitted nor triggered.
	Emit bool
	// Trigger is true when the closed loop should fire a retrain: the report is
	// CRITICAL, the monitor has auto_retrain enabled with a pipeline configured,
	// AND the cooldown has elapsed since the last trigger.
	Trigger bool
	// Suppressed explains a CRITICAL report that did NOT trigger, for observability
	// ("would have retrained but we're in cooldown" / "auto_retrain disabled"). It
	// is purely diagnostic — the service logs/metrics on it.
	Suppressed SuppressReason
}

// SuppressReason enumerates WHY a CRITICAL breach did not fire a retrain. WHY
// surface it: without it, "model is CRITICAL but didn't retrain" looks like a bug;
// with it, the operator sees "in cooldown for 12 more minutes" or "auto_retrain
// off — human in the loop". Drives a `retrain_suppressed_total{reason}` metric.
type SuppressReason int

const (
	SuppressNone        SuppressReason = iota // 0 — not suppressed (either triggered or not CRITICAL)
	SuppressNotCritical                       // report was OK/WARNING — by design
	SuppressAutoOff                           // auto_retrain disabled — human in the loop
	SuppressNoPipeline                        // auto_retrain on but no pipeline configured (misconfig)
	SuppressCooldown                          // within the cooldown after a recent trigger (anti-storm)
)

// DecideRetrain is the pure control-loop policy. It takes everything as
// parameters (no clock, no store) so it is trivially testable:
//
//	report        — the scored window's verdict (severity drives the branch)
//	monitor       — auto_retrain switch + configured pipeline
//	lastTriggered — when this model last fired a retrain (zero = never)
//	cooldown      — the minimum gap between retrains (the anti-storm interval)
//	now           — injected clock
//
// DECISION TABLE (interview-ready):
//
//	severity   auto  pipeline  cooldown-elapsed │ Emit  Trigger  Suppressed
//	────────────────────────────────────────────┼───────────────────────────
//	OK         *     *         *                │ no    no       NotCritical
//	WARNING    *     *         *                │ YES   no       NotCritical
//	CRITICAL   off   *         *                │ YES   no       AutoOff
//	CRITICAL   on    missing   *                │ YES   no       NoPipeline
//	CRITICAL   on    set       no               │ YES   no       Cooldown
//	CRITICAL   on    set       yes              │ YES   YES      None
//
// Note Emit is YES for every actionable (>=WARNING) report regardless of the
// trigger outcome — alerting and acting are independent. A CRITICAL in cooldown
// still ALERTS (humans should know it's still bad), it just doesn't re-trigger.
func DecideRetrain(report DriftReport, monitor Monitor, lastTriggered time.Time, cooldown time.Duration, now time.Time) RetrainDecision {
	// Not actionable → persist only.
	if !report.IsActionable() {
		return RetrainDecision{Emit: false, Trigger: false, Suppressed: SuppressNotCritical}
	}

	// Actionable (WARNING or CRITICAL) → always emit the event.
	d := RetrainDecision{Emit: true}

	// Only CRITICAL is eligible to trigger a retrain.
	if !report.FiresRetrain() {
		d.Suppressed = SuppressNotCritical // WARNING: alert only, by design
		return d
	}

	// CRITICAL: walk the gates.
	if !monitor.AutoRetrain {
		d.Suppressed = SuppressAutoOff // human in the loop
		return d
	}
	if monitor.RetrainPipelineID == "" {
		d.Suppressed = SuppressNoPipeline // misconfiguration; ConfigureMonitor should have blocked this
		return d
	}
	// COOLDOWN / DEBOUNCE — the anti-storm gate. If we triggered recently, suppress.
	// `lastTriggered.IsZero()` means never triggered → always allowed.
	if !lastTriggered.IsZero() && now.Sub(lastTriggered) < cooldown {
		d.Suppressed = SuppressCooldown
		return d
	}

	// All gates passed → fire the closed loop.
	d.Trigger = true
	d.Suppressed = SuppressNone
	return d
}
