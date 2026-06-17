package domain

import (
	"testing"
	"time"
)

// ============================================================================
// CLOSED-LOOP POLICY — the decision table from DecideRetrain, exercised as a
// pure function. This is where an interviewer probes "how do you stop a retrain
// storm?". Each case is one row of the documented table.
// ============================================================================

// report builds a report at a given severity for the decision tests.
func reportAt(sev DriftSeverity) DriftReport {
	return DriftReport{Severity: sev, DriftType: DriftTypeData}
}

// criticalMonitor is auto-retrain-enabled with a pipeline configured.
func armedMonitor() Monitor {
	return Monitor{AutoRetrain: true, RetrainPipelineID: "pipe-train-fraud"}
}

func TestDecide_OK_PersistOnly(t *testing.T) {
	d := DecideRetrain(reportAt(DriftSeverityOK), armedMonitor(), time.Time{}, time.Hour, time.Unix(0, 0))
	if d.Emit || d.Trigger {
		t.Fatalf("OK report: want no emit/no trigger, got %+v", d)
	}
	if d.Suppressed != SuppressNotCritical {
		t.Fatalf("OK report: want SuppressNotCritical, got %v", d.Suppressed)
	}
}

func TestDecide_Warning_EmitsButNoTrigger(t *testing.T) {
	// WARNING is the "alert a human, do NOT auto-retrain" rung.
	d := DecideRetrain(reportAt(DriftSeverityWarning), armedMonitor(), time.Time{}, time.Hour, time.Unix(0, 0))
	if !d.Emit {
		t.Fatal("WARNING should emit (alert a human)")
	}
	if d.Trigger {
		t.Fatal("WARNING must NOT trigger a retrain")
	}
	if d.Suppressed != SuppressNotCritical {
		t.Fatalf("want SuppressNotCritical, got %v", d.Suppressed)
	}
}

func TestDecide_Critical_AutoOff_EmitsNoTrigger(t *testing.T) {
	m := armedMonitor()
	m.AutoRetrain = false // human in the loop
	d := DecideRetrain(reportAt(DriftSeverityCritical), m, time.Time{}, time.Hour, time.Unix(0, 0))
	if !d.Emit {
		t.Fatal("CRITICAL should still emit even with auto_retrain off")
	}
	if d.Trigger {
		t.Fatal("auto_retrain off must not trigger")
	}
	if d.Suppressed != SuppressAutoOff {
		t.Fatalf("want SuppressAutoOff, got %v", d.Suppressed)
	}
}

func TestDecide_Critical_NoPipeline_Suppressed(t *testing.T) {
	m := armedMonitor()
	m.RetrainPipelineID = "" // misconfig that ConfigureMonitor should have blocked
	d := DecideRetrain(reportAt(DriftSeverityCritical), m, time.Time{}, time.Hour, time.Unix(0, 0))
	if d.Trigger {
		t.Fatal("no pipeline must not trigger")
	}
	if d.Suppressed != SuppressNoPipeline {
		t.Fatalf("want SuppressNoPipeline, got %v", d.Suppressed)
	}
}

func TestDecide_Critical_FirstTime_Triggers(t *testing.T) {
	// Never triggered before (zero lastTriggered) → fire.
	d := DecideRetrain(reportAt(DriftSeverityCritical), armedMonitor(), time.Time{}, time.Hour, time.Unix(1000, 0))
	if !d.Emit || !d.Trigger {
		t.Fatalf("first CRITICAL should emit AND trigger, got %+v", d)
	}
	if d.Suppressed != SuppressNone {
		t.Fatalf("want SuppressNone, got %v", d.Suppressed)
	}
}

func TestDecide_Critical_WithinCooldown_Suppressed(t *testing.T) {
	// THE ANTI-STORM GATE. Last triggered 10 minutes ago, cooldown is 1 hour →
	// suppress the retrain (but still emit the alert — it's still bad).
	now := time.Unix(10_000, 0)
	last := now.Add(-10 * time.Minute)
	d := DecideRetrain(reportAt(DriftSeverityCritical), armedMonitor(), last, time.Hour, now)
	if !d.Emit {
		t.Fatal("CRITICAL in cooldown should STILL emit the alert")
	}
	if d.Trigger {
		t.Fatal("CRITICAL within cooldown must NOT re-trigger (anti-storm)")
	}
	if d.Suppressed != SuppressCooldown {
		t.Fatalf("want SuppressCooldown, got %v", d.Suppressed)
	}
}

func TestDecide_Critical_AfterCooldown_Triggers(t *testing.T) {
	// Cooldown elapsed (last trigger 2h ago, cooldown 1h) → fire again.
	now := time.Unix(20_000, 0)
	last := now.Add(-2 * time.Hour)
	d := DecideRetrain(reportAt(DriftSeverityCritical), armedMonitor(), last, time.Hour, now)
	if !d.Trigger {
		t.Fatal("CRITICAL after cooldown should trigger again")
	}
	if d.Suppressed != SuppressNone {
		t.Fatalf("want SuppressNone, got %v", d.Suppressed)
	}
}

func TestDecide_Critical_ExactlyAtCooldownBoundary_Triggers(t *testing.T) {
	// Boundary: elapsed == cooldown. We use `< cooldown` to suppress, so EXACTLY at
	// the boundary the cooldown has elapsed → trigger. (Documenting the inclusive
	// edge so the behavior is intentional, not accidental.)
	now := time.Unix(30_000, 0)
	last := now.Add(-time.Hour)
	d := DecideRetrain(reportAt(DriftSeverityCritical), armedMonitor(), last, time.Hour, now)
	if !d.Trigger {
		t.Fatal("at the exact cooldown boundary the retrain should fire")
	}
}
