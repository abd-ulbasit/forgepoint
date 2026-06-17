// drift.go — THE STATISTICS. This is the teaching centerpiece an interviewer
// will probe ("which test, and why? what's the PSI formula? how do you handle an
// empty bin?"). Every function here is pure: inputs in, score out, no I/O — which
// is exactly why the tests in drift_test.go can pin them to KNOWN distributions
// with KNOWN expected values.
//
// ============================================================================
// THE PROBLEM: "did the distribution move?"
// ============================================================================
//
// We have a BASELINE distribution (captured at training time) and a CURRENT
// distribution (the live sliding window). Both are summarized as binned counts /
// proportions over the SAME bin layout. We need a single number — bigger = more
// drift — so a threshold can fire the control loop.
//
// Three established tests, each with a different shape (see the cheat-sheet on
// each function). All three are implemented because the proto exposes the METHOD
// per metric and an interviewer expects you to know the tradeoffs.
//
// ============================================================================
// REPRESENTING A DISTRIBUTION: the histogram
// ============================================================================
//
// A Histogram is a fixed set of bins with counts. We compare two histograms that
// share the SAME edges. For categorical signals (prediction class labels) the
// "bins" are the categories. The monitor builds these histograms incrementally as
// inference events arrive (streaming aggregation) — never storing raw values.
package domain

import (
	"math"
	"sort"
)

// Histogram is a binned distribution: Counts[i] is the number of observations in
// bin i. For PSI/KL the bins must align between baseline and current (same edges,
// same order, same length). We carry the raw COUNTS (not proportions) so the
// streaming aggregator can keep folding in new events with a simple increment, and
// so SampleCount (statistical confidence) is recoverable as the sum.
//
// Edges is optional context for continuous features: len(Edges) == len(Counts)+1
// describes [Edges[i], Edges[i+1]) for bin i. For categorical signals Edges is nil
// and Labels names each bin instead. Neither is needed for the math — only Counts
// alignment matters — but they make a Histogram self-describing for the UI.
type Histogram struct {
	Counts []float64 // observations per bin (float so proportions/smoothing are exact)
	Edges  []float64 // optional bin edges for continuous features (len = len(Counts)+1)
	Labels []string  // optional category names for categorical signals
}

// Total returns the number of observations across all bins (the sample count).
func (h Histogram) Total() float64 {
	var s float64
	for _, c := range h.Counts {
		s += c
	}
	return s
}

// Proportions converts counts to a probability mass that sums to 1, applying
// LAPLACE (additive) SMOOTHING with `eps` so NO bin is exactly zero.
//
// WHY smoothing is non-negotiable for PSI/KL (the classic gotcha):
//
//	PSI's per-bin term is (curr% - base%) * ln(curr%/base%) and KL's is
//	curr% * ln(curr%/base%). Both take ln of a ratio of proportions. If a bin is
//	empty in the BASELINE (base%==0) the ratio is +∞; if empty in the CURRENT
//	(curr%==0) for KL the term is 0·ln(0)=0 (fine), but for PSI ln(0/base) = -∞.
//	Either way an empty bin blows the score up to ±∞ — a single rare bin would
//	dominate every verdict. Adding a tiny eps to every bin BEFORE normalizing
//	bounds the ratio and keeps the statistic finite and stable. This is the
//	standard industry fix (the same reason NLP language models smooth n-gram
//	counts). eps is small (1e-6) so it barely perturbs well-populated bins.
//
// Returns ErrEmptyDistribution if the histogram has no observations at all (every
// bin zero) — that is "no data to compare", not "drift".
func (h Histogram) Proportions(eps float64) ([]float64, error) {
	if h.Total() == 0 {
		return nil, ErrEmptyDistribution
	}
	// Smooth first, then normalize over the smoothed total so the result still
	// sums to exactly 1.
	smoothedTotal := h.Total() + eps*float64(len(h.Counts))
	props := make([]float64, len(h.Counts))
	for i, c := range h.Counts {
		props[i] = (c + eps) / smoothedTotal
	}
	return props, nil
}

// defaultEps is the Laplace smoothing constant. 1e-6 is small enough to leave a
// well-populated bin's proportion essentially unchanged, large enough to keep
// ln(ratio) finite for an empty bin against a 100k-row baseline.
const defaultEps = 1e-6

// PSI computes the Population Stability Index between a baseline and a current
// histogram over the SAME bins.
//
// ┌─ INTERVIEW CHEAT-SHEET ─────────────────────────────────────────────────┐
// │ FORMULA:  PSI = Σ_bins (curr% − base%) · ln(curr% / base%)               │
// │ INTUITION: each bin contributes (difference in mass) × (log ratio of     │
// │   mass). A bin whose proportion barely changed contributes ~0; a bin that │
// │   doubled or vanished contributes a lot. Summing gives one number.        │
// │ SYMMETRY:  PSI is SYMMETRIC — swapping base and curr yields the same      │
// │   value (unlike KL). That is because the (curr−base) factor flips sign     │
// │   exactly when the ln(curr/base) factor does, so the product is invariant. │
// │ RULE OF THUMB:  <0.1 stable · 0.1–0.25 moderate shift · >0.25 significant.│
// │ WHY THE TABULAR DEFAULT:  cheap, interpretable, bin-based, and the         │
// │   0.1/0.25 thresholds are an industry convention reviewers recognize.      │
// └──────────────────────────────────────────────────────────────────────────┘
//
// Returns ErrBinMismatch if the two histograms have different bin counts (PSI
// compares bin-for-bin), and ErrEmptyDistribution if either side has no mass.
func PSI(baseline, current Histogram) (float64, error) {
	if len(baseline.Counts) != len(current.Counts) {
		return 0, ErrBinMismatch
	}
	baseP, err := baseline.Proportions(defaultEps)
	if err != nil {
		return 0, err
	}
	currP, err := current.Proportions(defaultEps)
	if err != nil {
		return 0, err
	}
	var psi float64
	for i := range baseP {
		// (curr% - base%) * ln(curr% / base%). Smoothing guarantees both > 0 so
		// the ln is always finite.
		psi += (currP[i] - baseP[i]) * math.Log(currP[i]/baseP[i])
	}
	// Numerically, identical distributions should give exactly 0; floating-point
	// rounding can yield a tiny negative. PSI is non-negative by construction, so
	// clamp the rounding dust to 0 (never report "negative drift").
	if psi < 0 {
		psi = 0
	}
	return psi, nil
}

// KL computes the Kullback–Leibler divergence D(current ‖ baseline) — the
// relative entropy of the CURRENT distribution with respect to the BASELINE.
//
// ┌─ INTERVIEW CHEAT-SHEET ─────────────────────────────────────────────────┐
// │ FORMULA:  KL(P‖Q) = Σ P(i) · ln(P(i) / Q(i))   with P=current, Q=baseline │
// │ DIRECTION MATTERS:  we use D(current ‖ baseline) — "how surprised is a    │
// │   model that EXPECTS the baseline when it SEES the current traffic?".      │
// │   KL is ASYMMETRIC: D(P‖Q) ≠ D(Q‖P) in general. Always state the order.   │
// │ RANGE:  [0, ∞). 0 iff the distributions are identical. Unbounded above,    │
// │   which is why it has no universal "0.25-style" threshold — its scale is   │
// │   data-dependent (nats here, since we use ln; bits if you use log2).       │
// │ EMPTY-BIN SENSITIVITY:  a bin where baseline Q≈0 but current P>0 explodes  │
// │   the term — the SAME smoothing fix as PSI is required (and applied).      │
// │ GOOD FOR:  prediction-probability drift, where you have a natural          │
// │   reference distribution (the baseline output mix).                        │
// └──────────────────────────────────────────────────────────────────────────┘
func KL(baseline, current Histogram) (float64, error) {
	if len(baseline.Counts) != len(current.Counts) {
		return 0, ErrBinMismatch
	}
	baseP, err := baseline.Proportions(defaultEps) // Q
	if err != nil {
		return 0, err
	}
	currP, err := current.Proportions(defaultEps) // P
	if err != nil {
		return 0, err
	}
	var kl float64
	for i := range currP {
		// P(i) * ln(P(i)/Q(i)). Smoothing keeps both > 0.
		kl += currP[i] * math.Log(currP[i]/baseP[i])
	}
	if kl < 0 { // rounding dust only; KL ≥ 0 by Gibbs' inequality
		kl = 0
	}
	return kl, nil
}

// KS computes the two-sample Kolmogorov–Smirnov statistic: the maximum absolute
// gap between the two empirical CDFs built from the histograms' bin proportions.
//
// ┌─ INTERVIEW CHEAT-SHEET ─────────────────────────────────────────────────┐
// │ STATISTIC:  D = max_x | F_current(x) − F_baseline(x) |                    │
// │   where F is the empirical CDF (running cumulative proportion over bins).  │
// │ RANGE:  [0, 1]. 0 = identical CDFs; 1 = completely disjoint support.       │
// │ NON-PARAMETRIC / DISTRIBUTION-FREE:  makes no assumption about the shape   │
// │   (Gaussian etc.) — great for arbitrary continuous features.               │
// │ vs PSI/KL:  KS is about the CDF GAP, so it's sensitive to a SHIFT in       │
// │   location; it's weaker at detecting a change confined to one interior bin │
// │   that doesn't move the cumulative curve much, and weak on heavy           │
// │   multimodality. PSI/KL weight per-bin mass changes directly.              │
// │ NO SMOOTHING NEEDED:  KS never divides or takes a log, so empty bins are   │
// │   harmless — the CDF just stays flat across them.                          │
// │ NOTE:  the classic KS p-value needs the sample sizes; the raw D statistic  │
// │   here is the drift SCORE the monitor thresholds on (bigger D = more drift).│
// └──────────────────────────────────────────────────────────────────────────┘
//
// We compute D from binned proportions (the streaming summaries we keep) rather
// than raw sorted samples — the cumulative-proportion difference at each bin edge
// is the empirical-CDF gap, and the max over edges is D.
func KS(baseline, current Histogram) (float64, error) {
	if len(baseline.Counts) != len(current.Counts) {
		return 0, ErrBinMismatch
	}
	baseTotal := baseline.Total()
	currTotal := current.Total()
	if baseTotal == 0 || currTotal == 0 {
		return 0, ErrEmptyDistribution
	}
	var baseCDF, currCDF, maxGap float64
	for i := range baseline.Counts {
		baseCDF += baseline.Counts[i] / baseTotal
		currCDF += current.Counts[i] / currTotal
		if gap := math.Abs(currCDF - baseCDF); gap > maxGap {
			maxGap = gap
		}
	}
	return maxGap, nil
}

// ComputeDrift dispatches to the requested method. It is the single seam the
// scorer calls so adding a method (e.g. Jensen–Shannon) is a one-line switch
// extension, not a change at every call site. An unknown/unspecified method is a
// programming error surfaced as ErrValidation — we never silently pick a method.
func ComputeDrift(method DriftMethod, baseline, current Histogram) (float64, error) {
	switch method {
	case DriftMethodPSI:
		return PSI(baseline, current)
	case DriftMethodKL:
		return KL(baseline, current)
	case DriftMethodKS:
		return KS(baseline, current)
	default:
		return 0, ErrValidation
	}
}

// ============================================================================
// PERFORMANCE DECAY — the LAGGING signal, computed from ground truth
// ============================================================================

// Accuracy is the fraction of matched (request_id → prediction, ground-truth)
// pairs whose prediction equals the actual label. The performance-decay SCORE is
// the DROP vs the baseline accuracy: max(0, baselineAccuracy − currentAccuracy),
// so "bigger = worse" like the other methods and the same threshold direction
// applies. Returns 0 for an empty set (no labels yet ⇒ no decay signal, not
// perfect accuracy).
func Accuracy(pairs []LabelPair) float64 {
	if len(pairs) == 0 {
		return 0
	}
	var correct int
	for _, p := range pairs {
		if p.Predicted == p.Actual {
			correct++
		}
	}
	return float64(correct) / float64(len(pairs))
}

// PerformanceDrop is the decay SCORE: how far current accuracy fell below the
// registered baseline accuracy. Clamped at 0 so an IMPROVEMENT never registers as
// drift (a model doing better than baseline is not degradation). This is what the
// performance ThresholdConfig's warn/critical compare against ("a 5-point drop is
// critical" ⇒ critical_score = 0.05).
func PerformanceDrop(baselineAccuracy float64, pairs []LabelPair) float64 {
	drop := baselineAccuracy - Accuracy(pairs)
	if drop < 0 {
		return 0
	}
	return drop
}

// LabelPair is one matched (prediction, ground-truth) observation used for
// performance-decay math. The monitor builds these by joining a recorded
// prediction (from InferenceCompleted, keyed by request_id) with a delayed
// GroundTruthLabel for the same request_id.
type LabelPair struct {
	Predicted string
	Actual    string
}

// ============================================================================
// HISTOGRAM CONSTRUCTION HELPERS (used by the streaming window)
// ============================================================================

// NewCategoricalHistogram builds a histogram over a FIXED ordered set of category
// labels from a counts map. WHY a fixed label order: PSI/KL compare bin-for-bin,
// so baseline and current MUST enumerate categories in the same order. The
// baseline defines the canonical label order; the current window is projected
// onto it (a category the window has but the baseline lacks would otherwise
// misalign the bins). Categories present in `order` but absent from `counts`
// become zero bins (then smoothed) — that is itself a drift signal ("a class the
// model used to predict has disappeared").
func NewCategoricalHistogram(order []string, counts map[string]float64) Histogram {
	h := Histogram{
		Counts: make([]float64, len(order)),
		Labels: append([]string(nil), order...),
	}
	for i, label := range order {
		h.Counts[i] = counts[label] // missing ⇒ 0
	}
	return h
}

// NewNumericHistogram bins raw numeric values into the given (sorted, ascending)
// edges. len(edges) must be >= 2; it produces len(edges)-1 bins where bin i covers
// [edges[i], edges[i+1]). Values below edges[0] go in the first bin and values at
// or above the last edge go in the last bin (clamping the tails) so no observation
// is silently dropped — a dropped tail would understate drift. Used to project a
// window of feature values onto the baseline's edge layout.
func NewNumericHistogram(edges []float64, values []float64) Histogram {
	if len(edges) < 2 {
		return Histogram{}
	}
	nbins := len(edges) - 1
	h := Histogram{
		Counts: make([]float64, nbins),
		Edges:  append([]float64(nil), edges...),
	}
	for _, v := range values {
		// binary search for the bin: largest i with edges[i] <= v.
		idx := sort.SearchFloat64s(edges, v)
		// SearchFloat64s returns the insertion point: the count of edges <= would-be
		// position. Convert to a bin index in [0, nbins-1].
		switch {
		case idx <= 0:
			idx = 0 // below the first edge → clamp into bin 0
		case idx >= len(edges):
			idx = nbins - 1 // at/above the last edge → clamp into the last bin
		default:
			// edges[idx-1] <= v < edges[idx] is NOT guaranteed by SearchFloat64s for
			// exact-edge values; SearchFloat64s returns the first index whose edge is
			// >= v. For v exactly on edges[idx], that is the LEFT edge of bin idx, so
			// the value belongs in bin idx (half-open [edge, next)). For v strictly
			// inside, edges[idx] is the RIGHT edge, so it belongs in bin idx-1.
			if edges[idx] == v {
				// on a left edge → bin idx (but the last edge is the right edge of the
				// last bin, handled by the idx>=len case above).
				if idx > nbins-1 {
					idx = nbins - 1
				}
			} else {
				idx = idx - 1
			}
		}
		h.Counts[idx]++
	}
	return h
}
