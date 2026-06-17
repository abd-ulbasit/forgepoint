// traffic_splitter.go — weighted traffic splitting (canary / A-B routing).
//
// ============================================================================
// THE TRAFFIC SPLITTING PATTERN (weighted random selection)
// ============================================================================
//
// A model name fronts N candidate versions, each with a basis-point weight. For
// each request the gateway picks ONE version with probability proportional to its
// weight. A 9000/1000 split sends ~90% to stable and ~10% to a canary; dial the
// canary up (SetTrafficSplit) as it proves out. This is SageMaker production
// variants / Istio weighted clusters / KServe canary percentage.
//
// WHY THE SERVER PICKS (not the client): if clients could pin the version, the
// canary would never receive the traffic it needs to be evaluated — defeating
// the whole point. version_override exists as a PRIVILEGED escape hatch the
// handler authorizes; ordinary traffic is split here.
//
// THE ALGORITHM — cumulative-weight scan over ELIGIBLE targets:
//
//	1. Sum the weights of ELIGIBLE (active) targets → total. Draining/unhealthy
//	   targets are excluded (effective weight 0), so a draining canary stops
//	   receiving traffic the instant its status flips, before any reweight.
//	2. Draw an integer d in [0, total) from the injected source.
//	3. Walk the targets accumulating weight; the first target whose running sum
//	   exceeds d is the winner. d lands in exactly one [lo, hi) band of width =
//	   that target's weight, so P(target) = weight/total. ← the whole guarantee.
//
// WHY integer bps + integer draw (no floats): deterministic across platforms
// (no FP rounding), exact band boundaries (no 0.1+0.2 drift), and it makes the
// distribution test assertable to the exact count. Determinism here is also why
// the test can pin a draw to a boundary and know the winner.
//
// RANDOM SOURCE INJECTION: Pick takes its randomness from an injected
// `func(n int) int` returning a uniform value in [0,n). Production wires
// crypto/math rand; tests wire a deterministic counter to assert exact splits.
// (We do NOT need crypto-grade randomness for load distribution — math/rand is
// the right tool; using crypto/rand would be slower for no security benefit,
// since the split carries no secret.)
// ============================================================================
package domain

// RandSource returns a uniformly-random integer in [0, n). n is always > 0 when
// called (Pick guards the empty/zero-weight case first).
type RandSource func(n int) int

// TrafficSplitter performs weighted version selection. It is stateless apart from
// its random source, so one splitter is shared across all routes/goroutines (the
// source must be safe for concurrent use — math/rand's top-level funcs are).
type TrafficSplitter struct {
	rand RandSource
}

// NewTrafficSplitter builds a splitter over the given random source.
func NewTrafficSplitter(rand RandSource) *TrafficSplitter {
	return &TrafficSplitter{rand: rand}
}

// Pick selects one eligible target weighted by its basis points. Returns
// ErrNoRoute if there are no eligible targets (so the use-case maps it to a
// NO_ROUTE failure instead of dividing by zero).
//
// IsCanary is derived by the caller from the chosen target's IsStable flag — the
// splitter's job is purely selection; canary classification (for the event) is a
// property of the chosen target, not of the draw.
func (s *TrafficSplitter) Pick(targets []RouteTarget) (RouteTarget, error) {
	total := eligibleWeightSum(targets)
	if total <= 0 {
		return RouteTarget{}, ErrNoRoute
	}
	d := s.rand(total) // uniform in [0, total)
	return selectByDraw(targets, d)
}

// eligibleWeightSum sums weights over ELIGIBLE targets only. Defined separately
// so Pick and selectByDraw share one definition of "what counts".
func eligibleWeightSum(targets []RouteTarget) int {
	sum := 0
	for _, t := range targets {
		if t.Status.Eligible() {
			sum += t.WeightBps
		}
	}
	return sum
}

// selectByDraw maps a draw value to the eligible target whose cumulative band it
// falls in. Exposed (unexported) for direct boundary testing without randomness.
//
// PRECONDITION: draw ∈ [0, eligibleWeightSum). The cumulative scan guarantees a
// winner; the trailing ErrNoRoute is an invariant guard that only fires if the
// precondition is violated (e.g. draw ≥ sum), which would be a caller bug.
func selectByDraw(targets []RouteTarget, draw int) (RouteTarget, error) {
	cumulative := 0
	for _, t := range targets {
		if !t.Status.Eligible() {
			continue // skip draining/unhealthy — they occupy no band
		}
		cumulative += t.WeightBps
		if draw < cumulative {
			return t, nil
		}
	}
	// Reached only if draw ≥ total (precondition violated) or no eligible target.
	return RouteTarget{}, ErrNoRoute
}
