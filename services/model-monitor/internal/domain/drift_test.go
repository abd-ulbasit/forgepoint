package domain

import (
	"errors"
	"math"
	"testing"
)

// ============================================================================
// PSI — pinned against HAND-COMPUTED values on known distributions.
// ============================================================================
//
// WHY hand-compute: an interviewer will ask "is your PSI right?". A test that
// only checks "score > 0" proves nothing. Each case below states the arithmetic
// so the expected value is auditable, and we assert to a tight tolerance.

// floatNear asserts a and b are within tol — the right way to compare floats.
func floatNear(t *testing.T, got, want, tol float64, msg string) {
	t.Helper()
	if math.Abs(got-want) > tol {
		t.Fatalf("%s: got %.8f, want %.8f (tol %g)", msg, got, want, tol)
	}
}

func TestPSI_IdenticalDistributions_IsZero(t *testing.T) {
	// Identical baseline and current ⇒ every (curr%-base%) term is 0 ⇒ PSI = 0.
	// (Smoothing is symmetric so it cancels exactly.)
	base := Histogram{Counts: []float64{50, 30, 20}}
	curr := Histogram{Counts: []float64{50, 30, 20}}
	got, err := PSI(base, curr)
	if err != nil {
		t.Fatalf("PSI error: %v", err)
	}
	floatNear(t, got, 0, 1e-9, "identical PSI")
}

func TestPSI_KnownShift_MatchesHandComputed(t *testing.T) {
	// Baseline proportions: [0.6, 0.3, 0.1]; current: [0.4, 0.4, 0.2].
	// (Counts chosen so proportions are exact and smoothing is negligible.)
	// PSI = Σ (c-b)·ln(c/b):
	//   bin0: (0.4-0.6)·ln(0.4/0.6) = -0.2·ln(0.6667) = -0.2·(-0.405465) =  0.0810930
	//   bin1: (0.4-0.3)·ln(0.4/0.3) =  0.1·ln(1.3333) =  0.1·( 0.287682) =  0.0287682
	//   bin2: (0.2-0.1)·ln(0.2/0.1) =  0.1·ln(2.0)    =  0.1·( 0.693147) =  0.0693147
	//   PSI  ≈ 0.1791759
	base := Histogram{Counts: []float64{600, 300, 100}}
	curr := Histogram{Counts: []float64{400, 400, 200}}
	got, err := PSI(base, curr)
	if err != nil {
		t.Fatalf("PSI error: %v", err)
	}
	// Tolerance loose enough for the 1e-6 smoothing perturbation, tight enough to
	// catch a wrong formula.
	floatNear(t, got, 0.1791759, 1e-4, "known-shift PSI")

	// Sanity: this is in the "moderate" band (0.1–0.25), not "significant".
	if got <= 0.1 || got >= 0.25 {
		t.Fatalf("expected moderate-band PSI, got %.6f", got)
	}
}

func TestPSI_IsSymmetric(t *testing.T) {
	// PSI(base, curr) == PSI(curr, base) — a defining property. If this fails the
	// formula likely lost the (curr-base) sign coupling.
	a := Histogram{Counts: []float64{70, 20, 10}}
	b := Histogram{Counts: []float64{30, 40, 30}}
	ab, err := PSI(a, b)
	if err != nil {
		t.Fatal(err)
	}
	ba, err := PSI(b, a)
	if err != nil {
		t.Fatal(err)
	}
	floatNear(t, ab, ba, 1e-9, "PSI symmetry")
}

func TestPSI_EmptyBin_DoesNotExplode(t *testing.T) {
	// THE CLASSIC GOTCHA: a bin empty in the baseline but populated in current.
	// Without smoothing this is ln(x/0) = +∞. With smoothing it must be large but
	// FINITE. This guards the smoothing fix.
	base := Histogram{Counts: []float64{100, 0}} // bin1 empty in baseline
	curr := Histogram{Counts: []float64{50, 50}} // bin1 now half the mass
	got, err := PSI(base, curr)
	if err != nil {
		t.Fatalf("PSI error: %v", err)
	}
	if math.IsInf(got, 0) || math.IsNaN(got) {
		t.Fatalf("PSI must be finite with smoothing, got %v", got)
	}
	// It SHOULD be large (a big shift) — well into the significant band.
	if got < 0.25 {
		t.Fatalf("expected significant drift for an emptied→filled bin, got %.6f", got)
	}
}

func TestPSI_BinMismatch_Errors(t *testing.T) {
	_, err := PSI(Histogram{Counts: []float64{1, 2}}, Histogram{Counts: []float64{1, 2, 3}})
	if !errors.Is(err, ErrBinMismatch) {
		t.Fatalf("want ErrBinMismatch, got %v", err)
	}
}

func TestPSI_EmptyDistribution_Errors(t *testing.T) {
	_, err := PSI(Histogram{Counts: []float64{0, 0}}, Histogram{Counts: []float64{1, 1}})
	if !errors.Is(err, ErrEmptyDistribution) {
		t.Fatalf("want ErrEmptyDistribution, got %v", err)
	}
}

// ============================================================================
// KL — relative entropy. Pinned values + asymmetry property.
// ============================================================================

func TestKL_IdenticalDistributions_IsZero(t *testing.T) {
	base := Histogram{Counts: []float64{40, 40, 20}}
	curr := Histogram{Counts: []float64{40, 40, 20}}
	got, err := KL(base, curr)
	if err != nil {
		t.Fatal(err)
	}
	floatNear(t, got, 0, 1e-9, "identical KL")
}

func TestKL_KnownValue_MatchesHandComputed(t *testing.T) {
	// P (current) = [0.5, 0.5]; Q (baseline) = [0.25, 0.75].
	// KL(P‖Q) = Σ P·ln(P/Q)
	//   = 0.5·ln(0.5/0.25) + 0.5·ln(0.5/0.75)
	//   = 0.5·ln(2)        + 0.5·ln(0.6667)
	//   = 0.5·0.693147     + 0.5·(-0.405465)
	//   = 0.3465736        - 0.2027326
	//   = 0.1438410  (nats)
	base := Histogram{Counts: []float64{25, 75}} // Q
	curr := Histogram{Counts: []float64{50, 50}} // P
	got, err := KL(base, curr)
	if err != nil {
		t.Fatal(err)
	}
	floatNear(t, got, 0.1438410, 1e-4, "known KL(P‖Q)")
}

func TestKL_IsAsymmetric(t *testing.T) {
	// KL(P‖Q) != KL(Q‖P) in general — the defining asymmetry. We assert the two
	// directions differ (the value order of args flips which is P and which is Q).
	a := Histogram{Counts: []float64{25, 75}}
	b := Histogram{Counts: []float64{50, 50}}
	klAB, err := KL(a, b) // P=b, Q=a
	if err != nil {
		t.Fatal(err)
	}
	klBA, err := KL(b, a) // P=a, Q=b
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(klAB-klBA) < 1e-6 {
		t.Fatalf("KL should be asymmetric, but both directions ≈ %.6f", klAB)
	}
}

func TestKL_NonNegative(t *testing.T) {
	// Gibbs' inequality: KL >= 0 always. Spot-check on a shifted pair.
	got, err := KL(Histogram{Counts: []float64{10, 90}}, Histogram{Counts: []float64{60, 40}})
	if err != nil {
		t.Fatal(err)
	}
	if got < 0 {
		t.Fatalf("KL must be >= 0, got %.6f", got)
	}
}

// ============================================================================
// KS — max empirical-CDF gap. Pinned values + range.
// ============================================================================

func TestKS_IdenticalDistributions_IsZero(t *testing.T) {
	base := Histogram{Counts: []float64{10, 20, 30, 40}}
	curr := Histogram{Counts: []float64{10, 20, 30, 40}}
	got, err := KS(base, curr)
	if err != nil {
		t.Fatal(err)
	}
	floatNear(t, got, 0, 1e-12, "identical KS")
}

func TestKS_KnownGap_MatchesHandComputed(t *testing.T) {
	// baseline props: [0.5, 0.5] → CDF after bin0 = 0.5, after bin1 = 1.0
	// current  props: [0.1, 0.9] → CDF after bin0 = 0.1, after bin1 = 1.0
	// gaps: |0.1-0.5| = 0.4 ; |1.0-1.0| = 0.0 → D = 0.4
	base := Histogram{Counts: []float64{50, 50}}
	curr := Histogram{Counts: []float64{10, 90}}
	got, err := KS(base, curr)
	if err != nil {
		t.Fatal(err)
	}
	floatNear(t, got, 0.4, 1e-12, "known KS D")
}

func TestKS_RangeIsZeroToOne(t *testing.T) {
	// Disjoint support → D = 1 (CDFs maximally apart at the boundary).
	base := Histogram{Counts: []float64{100, 0}}
	curr := Histogram{Counts: []float64{0, 100}}
	got, err := KS(base, curr)
	if err != nil {
		t.Fatal(err)
	}
	floatNear(t, got, 1.0, 1e-12, "disjoint KS D")
}

func TestKS_NoSmoothingNeeded_EmptyBinsHarmless(t *testing.T) {
	// KS never divides/logs, so empty bins are fine (unlike PSI/KL). This must NOT
	// error on a zero bin that has mass elsewhere.
	got, err := KS(Histogram{Counts: []float64{0, 100, 0}}, Histogram{Counts: []float64{50, 0, 50}})
	if err != nil {
		t.Fatalf("KS should tolerate empty bins, got error %v", err)
	}
	if got <= 0 || got > 1 {
		t.Fatalf("KS out of (0,1], got %.6f", got)
	}
}

// ============================================================================
// ComputeDrift dispatch
// ============================================================================

func TestComputeDrift_Dispatches(t *testing.T) {
	base := Histogram{Counts: []float64{60, 40}}
	curr := Histogram{Counts: []float64{40, 60}}
	for _, method := range []DriftMethod{DriftMethodPSI, DriftMethodKL, DriftMethodKS} {
		got, err := ComputeDrift(method, base, curr)
		if err != nil {
			t.Fatalf("method %v: %v", method, err)
		}
		if got <= 0 {
			t.Fatalf("method %v: expected positive drift, got %.6f", method, got)
		}
	}
	if _, err := ComputeDrift(DriftMethodUnspecified, base, curr); !errors.Is(err, ErrValidation) {
		t.Fatalf("unspecified method should be ErrValidation, got %v", err)
	}
}

// ============================================================================
// Histogram construction
// ============================================================================

func TestNewNumericHistogram_BinsCorrectly(t *testing.T) {
	// edges [0,1,2,3] → bins [0,1),[1,2),[2,3). Values on a left edge go right.
	edges := []float64{0, 1, 2, 3}
	values := []float64{0.5, 1.0, 1.5, 2.0, 2.9, 5.0 /*clamps to last*/, -1 /*clamps to first*/}
	h := NewNumericHistogram(edges, values)
	if len(h.Counts) != 3 {
		t.Fatalf("expected 3 bins, got %d", len(h.Counts))
	}
	// bin0 [0,1): 0.5 and -1(clamped) = 2
	// bin1 [1,2): 1.0(left edge) and 1.5 = 2
	// bin2 [2,3): 2.0(left edge), 2.9, 5.0(clamped) = 3
	want := []float64{2, 2, 3}
	for i := range want {
		if h.Counts[i] != want[i] {
			t.Fatalf("bin %d: got %g, want %g (all=%v)", i, h.Counts[i], want[i], h.Counts)
		}
	}
	if h.Total() != float64(len(values)) {
		t.Fatalf("total %g != value count %d (no value dropped)", h.Total(), len(values))
	}
}

func TestNewCategoricalHistogram_AlignsToBaselineOrder(t *testing.T) {
	// The window has counts for "fraud" and a class "unknown" the baseline lacks.
	// Projection onto the baseline order ["legit","fraud"] keeps the two aligned
	// bins and DROPS "unknown" from alignment (it can't compare to a missing
	// baseline bin); a class the baseline has but the window lacks ("legit") is 0.
	order := []string{"legit", "fraud"}
	counts := map[string]float64{"fraud": 30, "unknown": 5}
	h := NewCategoricalHistogram(order, counts)
	if len(h.Counts) != 2 {
		t.Fatalf("expected 2 aligned bins, got %d", len(h.Counts))
	}
	if h.Counts[0] != 0 { // legit absent in window → 0
		t.Fatalf("legit bin: got %g want 0", h.Counts[0])
	}
	if h.Counts[1] != 30 { // fraud present
		t.Fatalf("fraud bin: got %g want 30", h.Counts[1])
	}
}

// ============================================================================
// Performance decay math
// ============================================================================

func TestAccuracyAndPerformanceDrop(t *testing.T) {
	pairs := []LabelPair{
		{Predicted: "fraud", Actual: "fraud"}, // correct
		{Predicted: "fraud", Actual: "legit"}, // wrong
		{Predicted: "legit", Actual: "legit"}, // correct
		{Predicted: "legit", Actual: "legit"}, // correct
	}
	acc := Accuracy(pairs) // 3/4 = 0.75
	floatNear(t, acc, 0.75, 1e-12, "accuracy")

	// baseline 0.95, current 0.75 → drop 0.20
	floatNear(t, PerformanceDrop(0.95, pairs), 0.20, 1e-12, "performance drop")

	// An IMPROVEMENT over baseline must clamp to 0 (better-than-baseline is not drift).
	floatNear(t, PerformanceDrop(0.50, pairs), 0, 1e-12, "improvement clamps to 0")

	// Empty pairs → accuracy 0 and (by clamp) no negative drop weirdness.
	floatNear(t, Accuracy(nil), 0, 1e-12, "empty accuracy")
}
