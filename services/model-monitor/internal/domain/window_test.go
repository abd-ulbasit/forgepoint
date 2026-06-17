package domain

import (
	"testing"
	"time"
)

// ============================================================================
// WINDOWING CORRECTNESS — the streaming-aggregation fold + close conditions.
// These verify REAL state transitions (the window actually accumulates and
// closes), not mock interactions.
// ============================================================================

func mkObs(reqID, version string, features map[string]float64, pred string, at time.Time) InferenceObservation {
	return InferenceObservation{
		MonitorID:    "mon-1",
		OwnerTeam:    "team-a",
		RequestID:    reqID,
		ModelName:    "fraud-detector",
		ModelVersion: version,
		Features:     features,
		PredictedTop: pred,
		ObservedAt:   at,
	}
}

func TestWindow_Add_AccumulatesSummaries(t *testing.T) {
	w := NewWindow("mon-1", "fraud-detector", time.Unix(0, 0))
	w.Add(mkObs("r1", "v7", map[string]float64{"income": 5.0}, "fraud", time.Unix(1, 0)))
	w.Add(mkObs("r2", "v7", map[string]float64{"income": 7.0}, "legit", time.Unix(2, 0)))

	if w.SampleCount != 2 {
		t.Fatalf("SampleCount = %d, want 2", w.SampleCount)
	}
	if w.ModelVersion != "v7" {
		t.Fatalf("window pinned version = %q, want v7", w.ModelVersion)
	}
	// Feature values accumulated for binning at close.
	h, ok := w.FeatureHistogram("income", []float64{0, 6, 10})
	if !ok {
		t.Fatal("expected income feature present")
	}
	if h.Total() != 2 {
		t.Fatalf("income histogram total = %g, want 2", h.Total())
	}
	// Prediction tally.
	ph := w.PredictionHistogram([]string{"legit", "fraud"})
	if ph.Counts[0] != 1 || ph.Counts[1] != 1 {
		t.Fatalf("prediction tally = %v, want [1 1]", ph.Counts)
	}
}

func TestWindow_Add_IdempotentOnRequestID(t *testing.T) {
	// A redelivered event (same request_id) must NOT double-count — it would skew
	// the very drift score it feeds. This is per-window idempotency.
	w := NewWindow("mon-1", "fraud-detector", time.Unix(0, 0))
	first := w.Add(mkObs("dup", "v7", map[string]float64{"x": 1}, "fraud", time.Unix(1, 0)))
	second := w.Add(mkObs("dup", "v7", map[string]float64{"x": 1}, "fraud", time.Unix(1, 0)))

	if !first {
		t.Fatal("first Add should report newly-folded")
	}
	if second {
		t.Fatal("second Add of same request_id should report duplicate (false)")
	}
	if w.SampleCount != 1 {
		t.Fatalf("SampleCount = %d, want 1 (duplicate ignored)", w.SampleCount)
	}
}

func TestWindow_IsClosed_CountBound(t *testing.T) {
	w := NewWindow("mon-1", "m", time.Unix(0, 0))
	m := Monitor{WindowSize: 3} // count-only
	for i := 0; i < 2; i++ {
		w.Add(mkObs("", "v1", nil, "", time.Unix(int64(i), 0)))
	}
	if w.IsClosed(m, time.Unix(10, 0)) {
		t.Fatal("window of 2 should not be closed at size 3")
	}
	w.Add(mkObs("", "v1", nil, "", time.Unix(3, 0)))
	if !w.IsClosed(m, time.Unix(10, 0)) {
		t.Fatal("window of 3 should be closed at size 3")
	}
}

func TestWindow_IsClosed_TimeBound(t *testing.T) {
	opened := time.Unix(100, 0)
	w := NewWindow("mon-1", "m", opened)
	m := Monitor{WindowDuration: 60 * time.Second} // time-only
	w.Add(mkObs("", "v1", nil, "", opened))

	if w.IsClosed(m, opened.Add(59*time.Second)) {
		t.Fatal("should not close before duration elapses")
	}
	if !w.IsClosed(m, opened.Add(60*time.Second)) {
		t.Fatal("should close exactly at the duration boundary")
	}
}

func TestWindow_IsClosed_EitherBoundWhicheverFirst(t *testing.T) {
	opened := time.Unix(0, 0)
	w := NewWindow("mon-1", "m", opened)
	m := Monitor{WindowSize: 100, WindowDuration: 10 * time.Second} // both set
	// Only 2 samples (far below 100) but the time bound is hit → closes on time.
	w.Add(mkObs("", "v1", nil, "", opened))
	w.Add(mkObs("", "v1", nil, "", opened))
	if !w.IsClosed(m, opened.Add(10*time.Second)) {
		t.Fatal("should close on the time bound even though count bound not met")
	}
}

func TestWindow_HasEnoughSamples(t *testing.T) {
	w := NewWindow("mon-1", "m", time.Unix(0, 0))
	m := Monitor{MinSamples: 2}
	w.Add(mkObs("", "v1", nil, "", time.Unix(0, 0)))
	if w.HasEnoughSamples(m) {
		t.Fatal("1 sample should be below MinSamples 2 (stay WARMING_UP)")
	}
	w.Add(mkObs("", "v1", nil, "", time.Unix(1, 0)))
	if !w.HasEnoughSamples(m) {
		t.Fatal("2 samples should meet MinSamples 2")
	}
}

func TestWindow_RequestIDs_TrackedForGroundTruthJoin(t *testing.T) {
	w := NewWindow("mon-1", "m", time.Unix(0, 0))
	w.Add(mkObs("r1", "v1", nil, "fraud", time.Unix(0, 0)))
	w.Add(mkObs("r2", "v1", nil, "legit", time.Unix(1, 0)))
	ids := w.RequestIDs()
	if len(ids) != 2 {
		t.Fatalf("expected 2 tracked request_ids, got %d (%v)", len(ids), ids)
	}
}
