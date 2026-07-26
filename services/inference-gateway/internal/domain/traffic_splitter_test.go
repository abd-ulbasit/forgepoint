// traffic_splitter_test.go — TDD specification for weighted traffic splitting.
//
// The splitter is the TRAFFIC SPLITTING (canary / A-B) pattern: given a model's
// eligible targets and their basis-point weights, pick ONE version per request
// by weighted selection. These tests assert REAL distribution math — that a
// 9000/1000 split actually routes ~90%/~10% over many draws, that boundaries map
// to the right target, and that draining/unhealthy targets get zero traffic —
// not mock interactions. Determinism comes from injecting the random source.
package domain

import (
	"testing"
)

// activeTarget / canaryTarget / drainingTarget are tiny constructors that keep
// the test tables readable.
func activeTarget(version string, bps int, stable bool) RouteTarget {
	return RouteTarget{Version: version, Endpoint: version + ".svc:9090", WeightBps: bps, Status: TargetStatusActive, IsStable: stable}
}

// TestSplit_BoundaryMapping: with a sorted target list and a cumulative-weight
// scan, each draw value lands in exactly one [lo, hi) band. We pin the draw to
// exact boundary values and assert the chosen version. This proves the core
// selection arithmetic independent of any randomness — the most important
// correctness property (a boundary off-by-one would silently skew a canary).
//
// Layout for stable=9000, canary=1000 (total 10000), scanned in target order:
//
//	draw in [0, 9000)     → stable
//	draw in [9000, 10000) → canary
func TestSplit_BoundaryMapping(t *testing.T) {
	targets := []RouteTarget{
		activeTarget("stable", 9000, true),
		activeTarget("canary", 1000, false),
	}

	cases := []struct {
		draw int
		want string
	}{
		{0, "stable"},    // very first value → first band
		{8999, "stable"}, // last value of the stable band
		{9000, "canary"}, // first value of the canary band (boundary)
		{9999, "canary"}, // last valid value
	}
	for _, c := range cases {
		got, err := selectByDraw(targets, c.draw)
		if err != nil {
			t.Fatalf("draw=%d unexpected error: %v", c.draw, err)
		}
		if got.Version != c.want {
			t.Fatalf("draw=%d chose %q, want %q", c.draw, got.Version, c.want)
		}
	}
}

// TestSplit_Distribution: over many draws from a deterministic counter source,
// the empirical split must match the configured weights within a tight integer
// tolerance. WHY a counter (0,1,2,...mod total) and not a PRNG: it makes the test
// EXACT and reproducible — every bucket is hit exactly proportionally, so we can
// assert precise counts, not just "roughly". This is the canary guarantee:
// 10% weight really sends ~10% of traffic.
func TestSplit_Distribution(t *testing.T) {
	targets := []RouteTarget{
		activeTarget("stable", 9000, true),
		activeTarget("canary", 1000, false),
	}
	// Deterministic source cycling 0..9999 so each basis point is drawn once.
	draw := 0
	src := func(n int) int {
		v := draw % n
		draw++
		return v
	}
	splitter := NewTrafficSplitter(src)

	counts := map[string]int{}
	const n = 10000
	for i := 0; i < n; i++ {
		tgt, err := splitter.Pick(targets)
		if err != nil {
			t.Fatalf("pick %d errored: %v", i, err)
		}
		counts[tgt.Version]++
	}
	// Exactly proportional with a full sweep of the bps space.
	if counts["stable"] != 9000 {
		t.Fatalf("stable served %d/%d, want exactly 9000", counts["stable"], n)
	}
	if counts["canary"] != 1000 {
		t.Fatalf("canary served %d/%d, want exactly 1000", counts["canary"], n)
	}
}

// TestSplit_SkipsIneligible: DRAINING and UNHEALTHY targets must receive ZERO
// traffic regardless of their stored weight_bps. The splitter only considers
// ELIGIBLE (active) targets and normalizes over THEIR weight sum. This is what
// makes graceful drain real — flip a target to DRAINING and traffic stops
// flowing to it immediately, even before its weight is rewritten.
func TestSplit_SkipsIneligible(t *testing.T) {
	targets := []RouteTarget{
		activeTarget("stable", 5000, true),
		{Version: "draining", Endpoint: "d.svc", WeightBps: 4000, Status: TargetStatusDraining},
		{Version: "sick", Endpoint: "s.svc", WeightBps: 1000, Status: TargetStatusUnhealthy},
	}
	// Only "stable" is eligible; every draw must select it no matter the value.
	src := func(n int) int {
		// n here is the ELIGIBLE weight sum (5000), proving normalization.
		if n != 5000 {
			t.Fatalf("splitter drew over n=%d, want eligible sum 5000", n)
		}
		return n - 1 // last valid value in the eligible space
	}
	splitter := NewTrafficSplitter(src)
	tgt, err := splitter.Pick(targets)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tgt.Version != "stable" {
		t.Fatalf("picked %q, want stable (others ineligible)", tgt.Version)
	}
}

// TestSplit_NoEligibleTargets: a route whose targets are all draining/unhealthy
// (or empty) is NOT routable — Pick returns ErrNoRoute so the use-case maps it to
// a NO_ROUTE failure rather than dividing by a zero weight sum.
func TestSplit_NoEligibleTargets(t *testing.T) {
	splitter := NewTrafficSplitter(func(n int) int { return 0 })

	// Empty.
	if _, err := splitter.Pick(nil); err != ErrNoRoute {
		t.Fatalf("empty targets err = %v, want ErrNoRoute", err)
	}
	// All ineligible.
	targets := []RouteTarget{
		{Version: "draining", WeightBps: 5000, Status: TargetStatusDraining},
		{Version: "sick", WeightBps: 5000, Status: TargetStatusUnhealthy},
	}
	if _, err := splitter.Pick(targets); err != ErrNoRoute {
		t.Fatalf("all-ineligible err = %v, want ErrNoRoute", err)
	}
}

// TestSplit_SingleTarget: the common steady-state (one stable version at 100%)
// always returns that target without consuming randomness in a way that could
// skew — a degenerate-but-frequent case worth pinning.
func TestSplit_SingleTarget(t *testing.T) {
	targets := []RouteTarget{activeTarget("v1", 10000, true)}
	splitter := NewTrafficSplitter(func(n int) int { return 0 })
	tgt, err := splitter.Pick(targets)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tgt.Version != "v1" {
		t.Fatalf("picked %q, want v1", tgt.Version)
	}
}
