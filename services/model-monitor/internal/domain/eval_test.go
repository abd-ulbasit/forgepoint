package domain

import (
	"math"
	"testing"
)

// ============================================================================
// PARSER — the robustness contract: well-formed → scores; garbage → Unscored.
// ============================================================================

func TestParseJudgeScores_WellFormedJSON(t *testing.T) {
	t.Parallel()
	raw := `{"relevance": 4, "coherence": 5, "safety": 5}`
	got := ParseJudgeScores(raw)
	if !got.Scored {
		t.Fatalf("expected scored result, got Unscored for %q", raw)
	}
	if got.Relevance != 4 || got.Coherence != 5 || got.Safety != 5 {
		t.Fatalf("wrong axes: %+v", got)
	}
	wantOverall := (4.0 + 5.0 + 5.0) / 3.0
	if math.Abs(got.Overall-wantOverall) > 1e-9 {
		t.Fatalf("overall = %v, want %v", got.Overall, wantOverall)
	}
}

func TestParseJudgeScores_MessyButRecoverable(t *testing.T) {
	t.Parallel()
	// A tiny model often wraps JSON in prose, uses "/5", spells numbers out, adds
	// markdown. Each of these MUST still yield all three axes.
	cases := []struct {
		name string
		raw  string
		rel  float64
		coh  float64
		saf  float64
	}{
		{
			name: "prose around json",
			raw:  "Sure! Here is my evaluation:\n{\"relevance\": 3, \"coherence\": 4, \"safety\": 5}\nHope that helps.",
			rel:  3, coh: 4, saf: 5,
		},
		{
			name: "colon list with /5",
			raw:  "Relevance: 4/5\nCoherence: 3/5\nSafety: 5/5",
			rel:  4, coh: 3, saf: 5,
		},
		{
			name: "markdown bold and equals",
			raw:  "**relevance** = 2, **coherence** = 3, **safety** = 4",
			rel:  2, coh: 3, saf: 4,
		},
		{
			name: "spelled-out numbers",
			raw:  "relevance: four, coherence: five, safety: three",
			rel:  4, coh: 5, saf: 3,
		},
		{
			name: "uppercase keys",
			raw:  "RELEVANCE 5 COHERENCE 4 SAFETY 5",
			rel:  5, coh: 4, saf: 5,
		},
		{
			name: "out-of-scale clamped",
			raw:  `{"relevance": 9, "coherence": 0, "safety": 4}`,
			rel:  5, coh: 1, saf: 4, // 9→5, 0→1 (clamped)
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ParseJudgeScores(tc.raw)
			if !got.Scored {
				t.Fatalf("expected scored, got Unscored for %q", tc.raw)
			}
			if got.Relevance != tc.rel || got.Coherence != tc.coh || got.Safety != tc.saf {
				t.Fatalf("axes = (%v,%v,%v), want (%v,%v,%v)", got.Relevance, got.Coherence, got.Safety, tc.rel, tc.coh, tc.saf)
			}
		})
	}
}

func TestParseJudgeScores_GarbageIsUnscoredNoPanic(t *testing.T) {
	t.Parallel()
	// None of these contain all three recognizable axis scores → must be Unscored,
	// and must NOT panic (the whole point of the robustness contract).
	garbage := []string{
		"",
		"   ",
		"I cannot evaluate this.",
		"{",
		"}{][",
		"relevance: 4, coherence: 5", // safety missing
		"the answer was fine",
		"relevance: banana, coherence: apple, safety: pear", // no numbers/word-numbers
		"\x00\x01\x02 not json",
		"5 4 3", // no axis labels at all
	}
	for _, raw := range garbage {
		raw := raw
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			got := ParseJudgeScores(raw) // must not panic
			if got.Scored {
				t.Fatalf("expected Unscored for garbage %q, got %+v", raw, got)
			}
		})
	}
}

func TestParseJudgeScores_MissingAxisDoesNotStealNeighborNumber(t *testing.T) {
	t.Parallel()
	// relevance has NO number; coherence and safety do. The parser must NOT attribute
	// coherence's number to relevance — the whole result is Unscored because relevance
	// is missing. This pins the boundToNextAxis windowing.
	raw := "relevance: (see notes) coherence: 4 safety: 5"
	got := ParseJudgeScores(raw)
	if got.Scored {
		t.Fatalf("expected Unscored (relevance has no score), got %+v", got)
	}
}

// ============================================================================
// SAMPLER — deterministic 1-in-N selection (no RNG variance).
// ============================================================================

func TestCountingSampler_RateOne_SamplesEverything(t *testing.T) {
	t.Parallel()
	s := NewCountingSampler(1)
	for i := 0; i < 50; i++ {
		if !s.ShouldSample() {
			t.Fatalf("rate=1 must sample every call; call %d was not sampled", i)
		}
	}
}

func TestCountingSampler_OneInN_SelectsExactFraction(t *testing.T) {
	t.Parallel()
	const rate = 10
	const calls = 100
	s := NewCountingSampler(rate)
	selected := 0
	var selectedIdx []int
	for i := 0; i < calls; i++ {
		if s.ShouldSample() {
			selected++
			selectedIdx = append(selectedIdx, i)
		}
	}
	// Exactly 1-in-N: 100 calls at rate 10 → exactly 10 selected, at indices 0,10,…,90.
	if selected != calls/rate {
		t.Fatalf("selected %d of %d at rate %d, want %d", selected, calls, rate, calls/rate)
	}
	for k, idx := range selectedIdx {
		if idx != k*rate {
			t.Fatalf("selected index[%d] = %d, want %d (every Nth, starting at 0)", k, idx, k*rate)
		}
	}
}

func TestCountingSampler_RateZeroOrNegative_Defaults(t *testing.T) {
	t.Parallel()
	// rate <= 0 is treated as "sample everything" (defensive default), not divide-by-0.
	for _, rate := range []int{0, -5} {
		s := NewCountingSampler(rate)
		for i := 0; i < 5; i++ {
			if !s.ShouldSample() {
				t.Fatalf("rate=%d must default to sample-all; call %d not sampled", rate, i)
			}
		}
	}
}

// ============================================================================
// DRIFT DETECTION — a window of low scores alerts; healthy scores don't.
// ============================================================================

func TestEvaluateQualityDrift_LowWindowDrifts(t *testing.T) {
	t.Parallel()
	cfg := QualityDriftConfig{Window: 10, MinSamples: 5, FloorScore: 3.0}
	// Ten low scores → average well below the 3.0 floor → drift.
	scores := []float64{2.0, 2.3, 1.8, 2.1, 2.5, 2.0, 1.9, 2.2, 2.4, 2.0}
	res := EvaluateQualityDrift(scores, cfg)
	if !res.Drifted {
		t.Fatalf("expected drift for low window, got %+v", res)
	}
	if res.SampleCount != 10 {
		t.Fatalf("sample count = %d, want 10", res.SampleCount)
	}
	if res.Reason == "" {
		t.Fatalf("expected a non-empty drift reason")
	}
}

func TestEvaluateQualityDrift_HealthyWindowNoDrift(t *testing.T) {
	t.Parallel()
	cfg := QualityDriftConfig{Window: 10, MinSamples: 5, FloorScore: 3.0}
	scores := []float64{4.5, 4.8, 4.2, 5.0, 4.6, 4.9, 4.3, 4.7, 4.4, 4.8}
	res := EvaluateQualityDrift(scores, cfg)
	if res.Drifted {
		t.Fatalf("expected NO drift for healthy window, got %+v", res)
	}
}

func TestEvaluateQualityDrift_WarmingUpNeverDrifts(t *testing.T) {
	t.Parallel()
	cfg := QualityDriftConfig{Window: 10, MinSamples: 5, FloorScore: 3.0}
	// Only 3 scores (< MinSamples=5), all terrible — must NOT alert (warming up). A
	// single/few bad answers must never trip a retrain.
	res := EvaluateQualityDrift([]float64{1.0, 1.0, 1.0}, cfg)
	if res.Drifted {
		t.Fatalf("expected NO drift below MinSamples, got %+v", res)
	}
}

func TestEvaluateQualityDrift_BaselineRegression(t *testing.T) {
	t.Parallel()
	// Above the absolute floor (3.0) but a clear regression vs a 4.5 baseline with a
	// 0.5-point drop trigger → relative drift.
	cfg := QualityDriftConfig{
		Window: 10, MinSamples: 5, FloorScore: 3.0,
		Baseline: 4.5, DropFromBaseline: 0.5,
	}
	// average ~3.5: above floor, but 1.0 below baseline → drift.
	scores := []float64{3.5, 3.4, 3.6, 3.5, 3.5, 3.4, 3.6, 3.5}
	res := EvaluateQualityDrift(scores, cfg)
	if !res.Drifted {
		t.Fatalf("expected baseline-regression drift, got %+v", res)
	}
}

func TestQualityDriftScore_BiggerWhenWorse(t *testing.T) {
	t.Parallel()
	cfg := QualityDriftConfig{FloorScore: 3.0}
	// avg 2.0 → drop 1.0; avg 1.0 → drop 2.0 (worse ⇒ bigger score). avg 4.0 (above
	// floor) → 0 (healthy).
	if got := QualityDriftScore(2.0, cfg); math.Abs(got-1.0) > 1e-9 {
		t.Fatalf("score(2.0) = %v, want 1.0", got)
	}
	if got := QualityDriftScore(1.0, cfg); math.Abs(got-2.0) > 1e-9 {
		t.Fatalf("score(1.0) = %v, want 2.0", got)
	}
	if got := QualityDriftScore(4.0, cfg); got != 0 {
		t.Fatalf("score(4.0) above floor = %v, want 0", got)
	}
}
