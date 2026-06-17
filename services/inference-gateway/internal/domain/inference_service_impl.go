// inference_service_impl.go — the concrete InferenceService: the Predict use-case
// that COMPOSES the four resilience patterns, plus the event-driven route table.
//
// ============================================================================
// THE COMPOSITION (this file is where the patterns meet)
// ============================================================================
//
// Each pattern is its own pure domain object (RateLimiter port, TrafficSplitter,
// CircuitBreaker, RouteStore). This impl is the ORCHESTRATOR that runs them in
// the right order on the hot path and records the outcome. The ordering is
// deliberate and interview-worthy — cheap/early rejections first, the expensive
// backend call last, so a request that will be rejected costs as little as
// possible:
//
//	validate → quota → rate-limit → route → split → breaker → forward → emit
//	 (free)    (cache)  (1 token)   (map)  (math) (gate)   (network)  (async)
//
// WHY this order:
//   - VALIDATE first: a malformed request must not consume a token or trip a
//     breaker (the backend is innocent). Cheapest possible reject.
//   - QUOTA before RATE-LIMIT: a blocked team shouldn't even spend a token; the
//     quota check is a local cache read, cheaper than the (Redis) token take.
//   - RATE-LIMIT before ROUTE/SPLIT: no point resolving a route for a request
//     we're about to 429.
//   - BREAKER immediately before FORWARD: the breaker guards the SPECIFIC backend
//     the split chose, so it can only be consulted after selection.
//   - EMIT last, async, best-effort: the client already has its answer; a publish
//     failure must never fail a successful prediction.
//
// SECURITY DEFAULTS realized here:
//   - request_id / served_version / latency / is_canary are SERVER-set — never
//     from the request.
//   - the rate-limit key and quota team come from the Principal (verified claims),
//     never request fields → no account-takeover-for-billing.
//   - endpoints are taken from the route (server-resolved off deploy events),
//     never a client value → no SSRF; an override naming an unknown version is
//     refused rather than dialed.
//   - overflow-safe weight math (weights are bounded int bps; sums checked).
// ============================================================================
package domain

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ServiceDeps bundles the ports + collaborators the use-case needs. A struct
// (vs a long positional constructor) keeps wiring readable and lets us add a
// dependency later without breaking every caller's call site.
type ServiceDeps struct {
	Routes     RouteStore
	Limiter    RateLimiter
	Backend    ModelServerClient
	Publisher  EventPublisher
	Quota      QuotaChecker
	Breakers   BreakerRegistry
	RandSource RandSource    // randomness for the traffic splitter (math/rand in prod)
	Now        func() time.Time // injectable clock (timestamps + latency); prod: time.Now
	CallTimeout time.Duration // per-backend-call deadline; 0 → defaultCallTimeout
}

// defaultCallTimeout bounds a single backend call. WHY a deadline at all: a hung
// backend must surface as ErrTimeout (and trip the breaker) rather than block a
// gateway worker forever — slow is the new down. 2s is generous for tiny CPU
// ONNX models; tune per model later.
const defaultCallTimeout = 2 * time.Second

// inferenceService is the production InferenceService. Unexported: callers get it
// only through the interface returned by NewInferenceService.
type inferenceService struct {
	routes      RouteStore
	limiter     RateLimiter
	backend     ModelServerClient
	publisher   EventPublisher
	quota       QuotaChecker
	breakers    BreakerRegistry
	splitter    *TrafficSplitter
	now         func() time.Time
	callTimeout time.Duration
}

// NewInferenceService wires the use-case from its dependencies.
func NewInferenceService(d ServiceDeps) InferenceService {
	now := d.Now
	if now == nil {
		now = time.Now
	}
	timeout := d.CallTimeout
	if timeout <= 0 {
		timeout = defaultCallTimeout
	}
	return &inferenceService{
		routes:      d.Routes,
		limiter:     d.Limiter,
		backend:     d.Backend,
		publisher:   d.Publisher,
		quota:       d.Quota,
		breakers:    d.Breakers,
		splitter:    NewTrafficSplitter(d.RandSource),
		now:         now,
		callTimeout: timeout,
	}
}

// ============================================================================
// THE HOT PATH — Predict
// ============================================================================

func (s *inferenceService) Predict(ctx context.Context, p Principal, in PredictInput) (PredictOutput, error) {
	// Mint the server-authoritative request id up front so EVERY outcome
	// (success or any failure) is emitted under one stable id — Billing's dedupe
	// handle and Monitor's join key. Doing it first means even an early reject is
	// traceable.
	requestID := newRequestID()

	// --- 1. VALIDATE (cheapest reject; backend stays innocent → no breaker hit) ---
	if err := validatePredict(in); err != nil {
		return PredictOutput{}, s.fail(ctx, requestID, in.ModelName, "", p, FailureReasonInvalidInput, err)
	}

	// --- 2. QUOTA pre-flight (local cache read; before spending a token) ---
	if s.quota != nil && s.quota.IsBlocked(ctx, p.Team) {
		return PredictOutput{}, s.fail(ctx, requestID, in.ModelName, "", p, FailureReasonQuotaExceeded, ErrQuotaExceeded)
	}

	// --- 3. RATE LIMIT (token bucket, keyed on the PRINCIPAL — never a request field) ---
	allowed, err := s.limiter.Allow(ctx, p.APIKeyID)
	if err != nil {
		// Infra failure taking a token. POLICY: fail OPEN on a limiter outage —
		// a Redis blip must not 429 all traffic platform-wide; we'd rather risk a
		// brief over-admit than a self-inflicted outage. (Contrast auth, which
		// fails CLOSED — there the risk is a security breach, not lost traffic.)
		// We proceed as if allowed.
		allowed = true
	}
	if !allowed {
		return PredictOutput{}, s.fail(ctx, requestID, in.ModelName, "", p, FailureReasonRateLimited, ErrRateLimited)
	}

	// --- 4. ROUTE lookup ---
	route, err := s.routes.Get(ctx, in.ModelName)
	if err != nil {
		// Any store error (incl. ErrNoRoute) at this point = not routable.
		return PredictOutput{}, s.fail(ctx, requestID, in.ModelName, "", p, FailureReasonNoRoute, ErrNoRoute)
	}

	// --- 5. TRAFFIC SPLIT (or honor a privileged override) ---
	target, err := s.selectTarget(route, in.VersionOverride)
	if err != nil {
		return PredictOutput{}, s.fail(ctx, requestID, in.ModelName, "", p, FailureReasonNoRoute, err)
	}

	// --- 6. CIRCUIT BREAKER gate for the CHOSEN backend ---
	breaker := s.breakers.Get(route.ModelName, target.Version)
	if !breaker.Allow() {
		// Fail fast: the backend is known-sick; don't touch it.
		return PredictOutput{}, s.fail(ctx, requestID, in.ModelName, target.Version, p, FailureReasonCircuitOpen, ErrCircuitOpen)
	}

	// --- 7. FORWARD through the breaker (timed; outcome recorded either way) ---
	callCtx, cancel := context.WithTimeout(ctx, s.callTimeout)
	defer cancel()
	start := s.now()
	res, callErr := s.backend.Predict(callCtx, target.Endpoint, in)
	latency := s.now().Sub(start)

	if callErr != nil {
		// A real backend fault (ErrUpstream/ErrTimeout) → count it against the
		// breaker. This is the feedback loop that eventually trips it OPEN.
		breaker.RecordFailure()
		reason := FailureReasonUpstreamError
		outErr := ErrUpstream
		if errors.Is(callErr, ErrTimeout) || errors.Is(callErr, context.DeadlineExceeded) {
			reason = FailureReasonTimeout
			outErr = ErrTimeout
		}
		return PredictOutput{}, s.fail(ctx, requestID, in.ModelName, target.Version, p, reason, outErr)
	}

	// Success: tell the breaker (resets the failure tally / advances HALF_OPEN→CLOSED).
	breaker.RecordSuccess()

	out := PredictOutput{
		Outputs:       res.Outputs,
		ServedVersion: target.Version,
		IsCanary:      !target.IsStable, // a non-stable target that served = canary traffic
		Latency:       latency,
		RequestID:     requestID,
	}

	// --- 8. EMIT InferenceCompleted (async, best-effort, OFF the response path) ---
	// A publish error is logged by the adapter and swallowed here: the client
	// already has its prediction; we will not fail a good request because the
	// bus hiccuped. (At-least-once delivery + request_id dedupe make a later
	// retry safe.)
	_ = s.publisher.PublishCompleted(ctx, InferenceCompleted{
		RequestID:   requestID,
		ModelName:   in.ModelName,
		Version:     target.Version,
		APIKeyID:    p.APIKeyID,
		IsCanary:    out.IsCanary,
		Latency:     latency,
		CompletedAt: s.now(),
	})
	return out, nil
}

// selectTarget resolves which version serves: an authorized override pins a
// specific (and KNOWN) version; otherwise the weighted splitter chooses.
//
// SECURITY: an override is honored ONLY if the named version exists in the route
// AND is routable — the gateway will not forward to an unknown/unresolved
// backend (it has no trusted endpoint for it). The handler separately checks the
// caller holds the elevated scope before the override ever reaches here; this is
// the defense-in-depth backstop at the domain boundary.
func (s *inferenceService) selectTarget(route Route, override string) (RouteTarget, error) {
	if override != "" {
		t := route.findTarget(override)
		if t == nil || !t.Status.Eligible() {
			// Unknown or non-routable version → treat as no route (NOT_FOUND),
			// never an arbitrary dial.
			return RouteTarget{}, ErrNoRoute
		}
		return *t, nil
	}
	return s.splitter.Pick(route.Targets)
}

// fail is the single failure exit: it emits the InferenceFailed event (best
// effort) and returns the matching sentinel. Centralizing this guarantees EVERY
// failure path publishes exactly one failure event with the right reason — no
// path can silently drop the event. The error returned is the typed sentinel the
// handler maps to a gRPC status.
func (s *inferenceService) fail(ctx context.Context, requestID, model, version string, p Principal, reason FailureReason, err error) error {
	_ = s.publisher.PublishFailed(ctx, InferenceFailed{
		RequestID: requestID,
		ModelName: model,
		Version:   version,
		APIKeyID:  p.APIKeyID,
		Reason:    reason,
		Message:   err.Error(),
		FailedAt:  s.now(),
	})
	return err
}

// validatePredict enforces the input invariants BEFORE any backend work.
// Returns ErrInvalidInput (wrapped with a specific message) on any violation.
func validatePredict(in PredictInput) error {
	if in.ModelName == "" {
		return fmt.Errorf("%w: model_name is required", ErrInvalidInput)
	}
	if len(in.Inputs) == 0 {
		return fmt.Errorf("%w: at least one input tensor is required", ErrInvalidInput)
	}
	for name, tensor := range in.Inputs {
		// Length check: a tensor's byte length must equal product(shape)*elemSize.
		// Catching this at the edge turns a cryptic deep-runtime failure into a
		// clear INVALID_ARGUMENT and protects the backend from malformed bytes.
		// elemSize 0 (unknown dtype) skips the multiply check but still requires
		// a non-empty payload.
		if len(tensor.Data) == 0 {
			return fmt.Errorf("%w: tensor %q has empty data", ErrInvalidInput, name)
		}
		if elem := elemSize(tensor.DType); elem > 0 {
			want := elem
			for _, dim := range tensor.Shape {
				if dim < 0 {
					return fmt.Errorf("%w: tensor %q has negative dimension", ErrInvalidInput, name)
				}
				want *= int(dim)
			}
			if want > 0 && len(tensor.Data) != want {
				return fmt.Errorf("%w: tensor %q is %d bytes, want %d for shape/dtype",
					ErrInvalidInput, name, len(tensor.Data), want)
			}
		}
	}
	return nil
}

// elemSize returns the byte width of a dtype, or 0 if unknown (validation then
// skips the exact-length multiply but still requires non-empty data). Keeping
// this as a small lookup avoids importing any proto/runtime types into the domain.
func elemSize(dtype string) int {
	switch dtype {
	case "float32", "int32":
		return 4
	case "float64", "int64":
		return 8
	case "bool":
		return 1
	default:
		// "string" is variable-width; unknown dtypes are length-skipped.
		return 0
	}
}

// ============================================================================
// CONTROL PLANE
// ============================================================================

func (s *inferenceService) GetRoute(ctx context.Context, modelName string) (Route, error) {
	return s.routes.Get(ctx, modelName)
}

func (s *inferenceService) ListRoutes(ctx context.Context, opts ListOptions) ([]Route, string, error) {
	return s.routes.List(ctx, opts)
}

// UpsertRoute is the break-glass create/replace. It accepts ONLY version +
// weight per target; endpoints are SERVER-RESOLVED from the existing route's
// deploy records (anti-SSRF), and the resulting ACTIVE weights must sum to 10000.
func (s *inferenceService) UpsertRoute(ctx context.Context, modelName string, proposed []ProposedTarget) (Route, error) {
	if modelName == "" {
		return Route{}, fmt.Errorf("%w: model_name is required", ErrRouteValidation)
	}
	if len(proposed) == 0 {
		return Route{}, fmt.Errorf("%w: at least one target is required", ErrRouteValidation)
	}

	// Build an endpoint lookup from the EXISTING route — the only trusted source
	// of a backend address. A version with no known endpoint is rejected (we will
	// not invent a host to forward to).
	existing, _ := s.routes.Get(ctx, modelName) // ErrNoRoute → empty route, fine
	endpointOf := map[string]string{}
	stableOf := map[string]bool{}
	for _, t := range existing.Targets {
		endpointOf[t.Version] = t.Endpoint
		stableOf[t.Version] = t.IsStable
	}

	seen := map[string]bool{}
	targets := make([]RouteTarget, 0, len(proposed))
	for _, pt := range proposed {
		if pt.Version == "" {
			return Route{}, fmt.Errorf("%w: a target has an empty version", ErrRouteValidation)
		}
		if seen[pt.Version] {
			return Route{}, fmt.Errorf("%w: duplicate version %q", ErrRouteValidation, pt.Version)
		}
		seen[pt.Version] = true
		if err := validateWeight(pt.WeightBps); err != nil {
			return Route{}, err
		}
		endpoint, ok := endpointOf[pt.Version]
		if !ok || endpoint == "" {
			return Route{}, fmt.Errorf("%w: no resolved endpoint for version %q (deploy it first)", ErrRouteValidation, pt.Version)
		}
		targets = append(targets, RouteTarget{
			Version:   pt.Version,
			Endpoint:  endpoint, // SERVER-resolved
			WeightBps: pt.WeightBps,
			Status:    TargetStatusActive,
			IsStable:  stableOf[pt.Version],
		})
	}

	route := Route{ModelName: modelName, Targets: targets, UpdatedAt: s.now()}
	if err := validateActiveWeightSum(route); err != nil {
		return Route{}, err
	}
	if err := s.routes.Upsert(ctx, route); err != nil {
		return Route{}, err
	}
	return route, nil
}

// SetTrafficSplit reweights existing versions only — the canary dial. Each
// version must already exist; the new ACTIVE weights must sum to 10000.
//
// ATOMICITY (validate-then-commit) — the interview-critical property: we mutate a
// CLONE of the targets, validate the full proposed set, and Upsert ONLY on
// success. WHY this matters and what the old code got wrong: the previous version
// wrote each new weight in place through findTarget pointers and checked the SUM
// afterwards. Because the store's authoritative copy is in-memory and Get returns
// a Route sharing the targets' backing array, those in-place writes landed on the
// STORED route before validation — so a proposal that summed to e.g. 7000 was
// correctly rejected with ErrRouteValidation yet left the stored weights as the
// rejected values, and the next Predict/GetRoute routed on never-committed,
// corrupt weights. Cloning first makes a rejected write a no-op against the store:
// nothing is committed until the entire proposed set validates.
func (s *inferenceService) SetTrafficSplit(ctx context.Context, modelName string, weights []TrafficWeight) (Route, error) {
	route, err := s.routes.Get(ctx, modelName)
	if err != nil {
		return Route{}, ErrNoRoute
	}
	if len(weights) == 0 {
		return Route{}, fmt.Errorf("%w: at least one weight is required", ErrRouteValidation)
	}

	// Work on a deep copy of the targets. Every mutation below touches `targets`,
	// never `route.Targets` (the stored backing array) — so neither a validation
	// reject nor a concurrent Predict reader can observe a half-applied dial.
	proposed := route
	proposed.Targets = route.cloneTargets()

	seen := map[string]bool{}
	for _, w := range weights {
		if seen[w.Version] {
			return Route{}, fmt.Errorf("%w: duplicate version %q", ErrRouteValidation, w.Version)
		}
		seen[w.Version] = true
		if err := validateWeight(w.WeightBps); err != nil {
			return Route{}, err
		}
		// findTarget on the COPY: the pointer aliases the cloned backing array, so
		// writing through it mutates only our private proposal, never the store.
		t := proposed.findTarget(w.Version)
		if t == nil {
			return Route{}, fmt.Errorf("%w: unknown version %q in route", ErrRouteValidation, w.Version)
		}
		t.WeightBps = w.WeightBps
		// Re-activate a target whose weight is being set > 0 (un-drain it); a 0
		// weight leaves it active-but-receiving-nothing (the splitter gives it no
		// band) which is the soft-drain. Status changes via promote/undeploy.
		if t.Status == TargetStatusUnspecified {
			t.Status = TargetStatusActive
		}
	}

	proposed.UpdatedAt = s.now()
	if err := validateActiveWeightSum(proposed); err != nil {
		return Route{}, err // nothing committed — the stored route is untouched
	}
	if err := s.routes.Upsert(ctx, proposed); err != nil {
		return Route{}, err
	}
	return proposed, nil
}

func (s *inferenceService) DeleteRoute(ctx context.Context, modelName string) error {
	return s.routes.Delete(ctx, modelName) // idempotent: deleting absent is a no-op
}

func (s *inferenceService) CircuitStates(modelNameFilter string) []CircuitSnapshot {
	return s.breakers.Snapshot(modelNameFilter)
}

// ============================================================================
// EVENT REACTIONS (the routing table is a pure reactor on the control bus)
//
// All four are IDEMPOTENT: NATS is at-least-once, so a redelivered event must be
// a no-op-or-converge, never a corruption (no duplicate target, no double weight).
// ============================================================================

// ApplyModelDeployed adds (or updates in place) a target from a deploy event. The
// endpoint and weight come from the EVENT (server-resolved by the saga). The
// FIRST version deployed for a model is its stable; later additions are canaries.
func (s *inferenceService) ApplyModelDeployed(ctx context.Context, ev ModelDeployed) error {
	if ev.ModelName == "" || ev.Version == "" || ev.Endpoint == "" {
		return fmt.Errorf("%w: deploy event missing model/version/endpoint", ErrRouteValidation)
	}
	if err := validateWeight(ev.WeightBps); err != nil {
		return err
	}

	route, err := s.routes.Get(ctx, ev.ModelName)
	if errors.Is(err, ErrNoRoute) {
		route = Route{ModelName: ev.ModelName}
	} else if err != nil {
		return err
	}

	// COPY-ON-WRITE: build the next target set on a clone, never mutating the
	// fetched route's backing array. The hot path (Predict → splitter scan) reads
	// that array on other goroutines; in-place writes here would be a data race
	// AND would publish a half-applied state. We assemble the full next set, then
	// Upsert it atomically as one immutable snapshot.
	targets := route.cloneTargets()
	isFirst := len(targets) == 0
	updated := false
	for i := range targets {
		if targets[i].Version == ev.Version {
			// Idempotent update on redelivery: refresh endpoint/weight, keep the
			// stable flag (a redelivered deploy must not flip stability).
			targets[i].Endpoint = ev.Endpoint
			targets[i].WeightBps = ev.WeightBps
			targets[i].Status = TargetStatusActive
			updated = true
			break
		}
	}
	if !updated {
		targets = append(targets, RouteTarget{
			Version:   ev.Version,
			Endpoint:  ev.Endpoint,
			WeightBps: ev.WeightBps,
			Status:    TargetStatusActive,
			IsStable:  isFirst, // first version is the stable; subsequent are canaries
		})
	}
	route.Targets = targets
	route.UpdatedAt = s.now()
	return s.routes.Upsert(ctx, route)
}

// ApplyModelUndeployed removes a target. Removing an absent target (or model) is
// a safe no-op (idempotent). If it was the last target the route is deleted so
// the model becomes non-serving.
func (s *inferenceService) ApplyModelUndeployed(ctx context.Context, ev ModelUndeployed) error {
	route, err := s.routes.Get(ctx, ev.ModelName)
	if errors.Is(err, ErrNoRoute) {
		return nil // already gone — idempotent
	} else if err != nil {
		return err
	}

	// Filter into a FRESH slice — never `route.Targets[:0]`, which reuses (and
	// overwrites) the stored backing array that hot-path readers may be scanning.
	// Copy-on-write keeps the removal atomic and race-free.
	kept := make([]RouteTarget, 0, len(route.Targets))
	for _, t := range route.Targets {
		if t.Version != ev.Version {
			kept = append(kept, t)
		}
	}
	if len(kept) == 0 {
		return s.routes.Delete(ctx, ev.ModelName)
	}
	route.Targets = kept
	route.UpdatedAt = s.now()
	return s.routes.Upsert(ctx, route)
}

// ApplyModelPromoted repoints ALL traffic to the newly-promoted production
// version (100%, marked stable) and DRAINS the auto-demoted prior version (and
// every other target) so the single-production invariant holds. Draining (not
// deleting) the old one lets in-flight requests finish gracefully; a later
// undeploy event removes it entirely.
func (s *inferenceService) ApplyModelPromoted(ctx context.Context, ev ModelPromoted) error {
	route, err := s.routes.Get(ctx, ev.ModelName)
	if err != nil {
		return ErrNoRoute
	}
	if route.findTarget(ev.Version) == nil {
		// The promoted version isn't in our table yet (deploy event not seen).
		// Refuse rather than silently invent a backend — the deploy event carries
		// the trusted endpoint and must land first. (Pure read of the fetched
		// route; we do not write through the returned pointer.)
		return fmt.Errorf("%w: promoted version %q has no route target", ErrRouteValidation, ev.Version)
	}

	// COPY-ON-WRITE repoint: rewrite the whole split on a clone, then swap it in
	// atomically. This was the most damaging in-place mutation — it rewrote EVERY
	// target's weight/status/stable through findTarget-style pointers on the stored
	// backing array, racing every concurrent Predict (splitter scan reads exactly
	// these fields). Building the new set on a copy means a reader always sees
	// either the old split or the new one, never a torn mix.
	targets := route.cloneTargets()
	for i := range targets {
		if targets[i].Version == ev.Version {
			targets[i].WeightBps = TotalWeightBps
			targets[i].Status = TargetStatusActive
			targets[i].IsStable = true
		} else {
			// Everything else (incl. the demoted old prod) drains: no new traffic,
			// in-flight allowed to finish.
			targets[i].WeightBps = 0
			targets[i].Status = TargetStatusDraining
			targets[i].IsStable = false
		}
	}
	route.Targets = targets
	route.UpdatedAt = s.now()
	return s.routes.Upsert(ctx, route)
}

// ApplyModelArchived drops the whole route (the model is going away). Idempotent.
func (s *inferenceService) ApplyModelArchived(ctx context.Context, ev ModelArchived) error {
	return s.routes.Delete(ctx, ev.ModelName)
}

// ============================================================================
// WEIGHT VALIDATION (overflow-safe, bounded)
// ============================================================================

// validateWeight bounds a single target weight to [0, 10000]. Bounding each
// weight individually means the sum of N targets can't overflow an int even with
// a malicious payload (N * 10000 stays far below math.MaxInt), so the sum check
// in validateActiveWeightSum is overflow-safe by construction.
func validateWeight(bps int) error {
	if bps < 0 || bps > TotalWeightBps {
		return fmt.Errorf("%w: weight_bps %d out of range [0,%d]", ErrRouteValidation, bps, TotalWeightBps)
	}
	return nil
}

// validateActiveWeightSum enforces the routing invariant: ACTIVE targets' weights
// sum to exactly 10000 (100%). Draining/unhealthy targets contribute 0 and are
// excluded (Route.ActiveWeightSum). An all-zero / no-active route is rejected —
// a model with no traffic share would silently black-hole predicts.
func validateActiveWeightSum(route Route) error {
	sum := route.ActiveWeightSum()
	if sum != TotalWeightBps {
		return fmt.Errorf("%w: active weights sum to %d, want %d", ErrRouteValidation, sum, TotalWeightBps)
	}
	return nil
}
