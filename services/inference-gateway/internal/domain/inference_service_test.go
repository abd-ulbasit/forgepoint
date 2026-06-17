// inference_service_test.go — TDD spec for the Predict use-case orchestration
// and the event-driven route mutations.
//
// These tests are EXTERNAL (package domain_test) so they exercise only the
// exported surface a real handler depends on, and inject hand-written mock ports
// (no Redis, no real backend, no NATS). They assert REAL behavior: the breaker
// actually trips after the use-case records failures, a rate-limited request
// never reaches the backend, the published event carries the version that
// actually served, and a promote really repoints traffic — not mock call counts.
package domain_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/inference-gateway/internal/domain"
)

// ----------------------------------------------------------------------------
// Hand-written mock ports. Each is a struct with function fields / in-memory
// state; an unset behavior uses a sensible default so each test wires only what
// it cares about. No codegen — every line is interview-explainable.
// ----------------------------------------------------------------------------

// mockRouteStore keeps routes in a map so a route "deployed" via an event can be
// fetched on the predict path — letting us assert the real round-trip.
//
// It models the PRODUCTION in-memory adapter's contract faithfully (ports.go,
// SNAPSHOT ISOLATION): a RWMutex guards the map, and BOTH Get and Upsert DEEP
// COPY the Targets slice. Get-copy hands every reader an immutable snapshot a
// concurrent Upsert can't mutate; Upsert-copy stops the caller's later edits from
// reaching back into the store. Without these copies the slice header would share
// a backing array — exactly the aliasing the two findings exploit — and the
// concurrency test below would (correctly) trip -race even with a perfect domain.
// We use a RWMutex (vs the previous plain Mutex) so many concurrent Predict reads
// can proceed in parallel, matching the real adapter and stressing the read path.
type mockRouteStore struct {
	mu     sync.RWMutex
	routes map[string]domain.Route
}

func newMockRouteStore() *mockRouteStore {
	return &mockRouteStore{routes: map[string]domain.Route{}}
}

// cloneRoute deep-copies a Route's Targets so the stored copy and the returned
// copy never share a backing array. RouteTarget is all value fields, so an
// element-wise copy is a complete deep copy.
func cloneRoute(r domain.Route) domain.Route {
	if r.Targets != nil {
		ts := make([]domain.RouteTarget, len(r.Targets))
		copy(ts, r.Targets)
		r.Targets = ts
	}
	return r
}

func (m *mockRouteStore) Get(_ context.Context, name string) (domain.Route, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.routes[name]
	if !ok {
		return domain.Route{}, domain.ErrNoRoute
	}
	return cloneRoute(r), nil // snapshot isolation: caller gets a private copy
}
func (m *mockRouteStore) Upsert(_ context.Context, r domain.Route) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.routes[r.ModelName] = cloneRoute(r) // store a private copy, not the caller's slice
	return nil
}
func (m *mockRouteStore) Delete(_ context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.routes, name)
	return nil
}
func (m *mockRouteStore) List(_ context.Context, _ domain.ListOptions) ([]domain.Route, string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]domain.Route, 0, len(m.routes))
	for _, r := range m.routes {
		out = append(out, cloneRoute(r))
	}
	return out, "", nil
}

// sharedBackingRouteStore is an ADVERSARIAL store used to lock in the DOMAIN-level
// atomicity fix (Finding 1) independently of the store's snapshot isolation. It
// deliberately returns the stored Route WITHOUT copying — a slice-header copy that
// SHARES the Targets backing array (the worst-case "live Route" the finding
// describes as the design's authoritative in-memory map). If the domain mutated a
// fetched route's targets in place before validating, a REJECTED SetTrafficSplit
// would corrupt the stored weights through this shared array. With the
// copy-on-write fix the domain clones before mutating, so this store can no longer
// be corrupted by a rejected write — proving the fix lives in the domain, not just
// in a well-behaved adapter.
type sharedBackingRouteStore struct {
	mu     sync.Mutex
	routes map[string]domain.Route
}

func newSharedBackingRouteStore() *sharedBackingRouteStore {
	return &sharedBackingRouteStore{routes: map[string]domain.Route{}}
}
func (m *sharedBackingRouteStore) Get(_ context.Context, name string) (domain.Route, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.routes[name]
	if !ok {
		return domain.Route{}, domain.ErrNoRoute
	}
	return r, nil // NO copy: shares the Targets backing array on purpose
}
func (m *sharedBackingRouteStore) Upsert(_ context.Context, r domain.Route) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.routes[r.ModelName] = r
	return nil
}
func (m *sharedBackingRouteStore) Delete(_ context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.routes, name)
	return nil
}
func (m *sharedBackingRouteStore) List(_ context.Context, _ domain.ListOptions) ([]domain.Route, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]domain.Route, 0, len(m.routes))
	for _, r := range m.routes {
		out = append(out, r)
	}
	return out, "", nil
}

// mockLimiter returns a fixed allow/deny (or an error to test infra-failure
// policy). Records the keys it was asked about so we can assert the principal
// (not a request field) is what's rate-limited.
type mockLimiter struct {
	allow   bool
	err     error
	mu      sync.Mutex
	seenKey string
}

func (m *mockLimiter) Allow(_ context.Context, key string) (bool, error) {
	m.mu.Lock()
	m.seenKey = key
	m.mu.Unlock()
	return m.allow, m.err
}

// mockBackend returns a canned result or error, and records how many times it
// was actually called — so we can prove rate-limit/circuit rejections short-
// circuit BEFORE the backend (the whole point of failing fast).
type mockBackend struct {
	mu       sync.Mutex
	calls    int
	result   domain.PredictResult
	err      error
	seenEnds []string
}

func (m *mockBackend) Predict(_ context.Context, endpoint string, _ domain.PredictInput) (domain.PredictResult, error) {
	m.mu.Lock()
	m.calls++
	m.seenEnds = append(m.seenEnds, endpoint)
	m.mu.Unlock()
	return m.result, m.err
}
func (m *mockBackend) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

// mockPublisher captures the last published events so we can assert their
// SERVER-authoritative contents (served version, api_key_id, failure reason).
type mockPublisher struct {
	mu        sync.Mutex
	completed []domain.InferenceCompleted
	failed    []domain.InferenceFailed
}

func (m *mockPublisher) PublishCompleted(_ context.Context, ev domain.InferenceCompleted) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.completed = append(m.completed, ev)
	return nil
}
func (m *mockPublisher) PublishFailed(_ context.Context, ev domain.InferenceFailed) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failed = append(m.failed, ev)
	return nil
}
func (m *mockPublisher) lastCompleted() (domain.InferenceCompleted, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.completed) == 0 {
		return domain.InferenceCompleted{}, false
	}
	return m.completed[len(m.completed)-1], true
}
func (m *mockPublisher) lastFailed() (domain.InferenceFailed, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.failed) == 0 {
		return domain.InferenceFailed{}, false
	}
	return m.failed[len(m.failed)-1], true
}

// mockQuota reports whether a team is blocked. Lets us drive the pre-flight
// quota gate independently.
type mockQuota struct{ blocked map[string]bool }

func (m *mockQuota) IsBlocked(_ context.Context, team string) bool { return m.blocked[team] }

// buildService wires a service with a deterministic split source (always pick
// the FIRST eligible band → the stable target by default unless overridden in a
// test) and small breaker thresholds so trip behavior is reachable in a test.
func buildService(t *testing.T, store domain.RouteStore, lim domain.RateLimiter, be domain.ModelServerClient, pub domain.EventPublisher, quota domain.QuotaChecker) domain.InferenceService {
	t.Helper()
	return domain.NewInferenceService(domain.ServiceDeps{
		Routes:     store,
		Limiter:    lim,
		Backend:    be,
		Publisher:  pub,
		Quota:      quota,
		RandSource: func(n int) int { return 0 }, // draw 0 → first eligible band
		Breakers: domain.NewBreakerRegistry(domain.BreakerTuning{
			FailureThreshold:  3,
			SuccessThreshold:  2,
			ResetTimeout:      10 * time.Second,
			HalfOpenMaxProbes: 1,
		}, time.Now),
		Now: time.Now,
	})
}

// deployStable seeds a single-version (stable, 100%) route through the event
// reaction path — exercising ApplyModelDeployed and giving predict something to
// route to.
func deployStable(t *testing.T, svc domain.InferenceService, model, version, endpoint string) {
	t.Helper()
	if err := svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{
		ModelName: model, Version: version, Endpoint: endpoint, WeightBps: domain.TotalWeightBps,
	}); err != nil {
		t.Fatalf("ApplyModelDeployed: %v", err)
	}
}

func basicInput(model string) domain.PredictInput {
	return domain.PredictInput{
		ModelName: model,
		Inputs:    map[string]domain.Tensor{"features": {Shape: []int64{1, 4}, DType: "float32", Data: make([]byte, 16)}},
	}
}

// ----------------------------------------------------------------------------
// HAPPY PATH
// ----------------------------------------------------------------------------

// TestPredict_HappyPath: a valid request to a routable model forwards to the
// backend at the SERVER-RESOLVED endpoint, returns the served version + a minted
// request id, and publishes InferenceCompleted with the version that actually
// served and the principal's api_key_id (server-authoritative, not from request).
func TestPredict_HappyPath(t *testing.T) {
	store := newMockRouteStore()
	lim := &mockLimiter{allow: true}
	be := &mockBackend{result: domain.PredictResult{Outputs: map[string]domain.Tensor{"probs": {Data: []byte{1}}}}}
	pub := &mockPublisher{}
	svc := buildService(t, store, lim, be, pub, &mockQuota{})
	deployStable(t, svc, "fraud", "v1", "fraud-v1.svc:9090")

	out, err := svc.Predict(context.Background(), domain.Principal{APIKeyID: "key-42", Team: "team-a"}, basicInput("fraud"))
	if err != nil {
		t.Fatalf("Predict: %v", err)
	}
	if out.ServedVersion != "v1" {
		t.Fatalf("served version = %q, want v1", out.ServedVersion)
	}
	if out.RequestID == "" {
		t.Fatal("expected a server-minted request id")
	}
	if be.callCount() != 1 {
		t.Fatalf("backend calls = %d, want 1", be.callCount())
	}
	if got := be.seenEnds[0]; got != "fraud-v1.svc:9090" {
		t.Fatalf("forwarded to %q, want the server-resolved endpoint", got)
	}
	// The rate limiter must be keyed on the PRINCIPAL, never a request field.
	if lim.seenKey != "key-42" {
		t.Fatalf("rate-limit key = %q, want the api_key_id key-42", lim.seenKey)
	}
	ev, ok := pub.lastCompleted()
	if !ok {
		t.Fatal("expected an InferenceCompleted event")
	}
	if ev.Version != "v1" || ev.APIKeyID != "key-42" || ev.RequestID != out.RequestID {
		t.Fatalf("event mismatch: %+v (want v1/key-42/%s)", ev, out.RequestID)
	}
}

// ----------------------------------------------------------------------------
// VALIDATION
// ----------------------------------------------------------------------------

// TestPredict_InvalidInput: missing model name / empty inputs are rejected with
// ErrInvalidInput BEFORE spending a rate-limit token or hitting the backend —
// and crucially WITHOUT recording a breaker failure (the backend is innocent).
func TestPredict_InvalidInput(t *testing.T) {
	store := newMockRouteStore()
	lim := &mockLimiter{allow: true}
	be := &mockBackend{}
	svc := buildService(t, store, lim, be, &mockPublisher{}, &mockQuota{})

	cases := []struct {
		name string
		in   domain.PredictInput
	}{
		{"empty model", domain.PredictInput{Inputs: map[string]domain.Tensor{"f": {Data: []byte{1}}}}},
		{"no inputs", domain.PredictInput{ModelName: "fraud"}},
		{"tensor length mismatch", domain.PredictInput{ModelName: "fraud", Inputs: map[string]domain.Tensor{
			"f": {Shape: []int64{1, 4}, DType: "float32", Data: make([]byte, 8)}, // want 16 bytes, got 8
		}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := svc.Predict(context.Background(), domain.Principal{APIKeyID: "k"}, c.in)
			if !errors.Is(err, domain.ErrInvalidInput) {
				t.Fatalf("err = %v, want ErrInvalidInput", err)
			}
		})
	}
	if be.callCount() != 0 {
		t.Fatalf("backend should not be called on invalid input, got %d calls", be.callCount())
	}
}

// ----------------------------------------------------------------------------
// RATE LIMITING
// ----------------------------------------------------------------------------

// TestPredict_RateLimited: an empty token bucket rejects with ErrRateLimited and
// NEVER reaches the backend (fail before forwarding — the protective point of a
// rate limiter). A failure event is published with the RATE_LIMITED reason.
func TestPredict_RateLimited(t *testing.T) {
	store := newMockRouteStore()
	lim := &mockLimiter{allow: false} // bucket empty
	be := &mockBackend{}
	pub := &mockPublisher{}
	svc := buildService(t, store, lim, be, pub, &mockQuota{})
	deployStable(t, svc, "fraud", "v1", "e")

	_, err := svc.Predict(context.Background(), domain.Principal{APIKeyID: "k"}, basicInput("fraud"))
	if !errors.Is(err, domain.ErrRateLimited) {
		t.Fatalf("err = %v, want ErrRateLimited", err)
	}
	if be.callCount() != 0 {
		t.Fatal("rate-limited request must not reach the backend")
	}
	ev, ok := pub.lastFailed()
	if !ok || ev.Reason != domain.FailureReasonRateLimited {
		t.Fatalf("failure event = %+v, want RATE_LIMITED", ev)
	}
}

// TestPredict_QuotaExceeded: a team flagged over-quota is rejected pre-flight,
// before even spending a rate-limit token. Distinct reason from rate limiting.
func TestPredict_QuotaExceeded(t *testing.T) {
	store := newMockRouteStore()
	lim := &mockLimiter{allow: true}
	be := &mockBackend{}
	svc := buildService(t, store, lim, be, &mockPublisher{}, &mockQuota{blocked: map[string]bool{"team-x": true}})
	deployStable(t, svc, "fraud", "v1", "e")

	_, err := svc.Predict(context.Background(), domain.Principal{APIKeyID: "k", Team: "team-x"}, basicInput("fraud"))
	if !errors.Is(err, domain.ErrQuotaExceeded) {
		t.Fatalf("err = %v, want ErrQuotaExceeded", err)
	}
	if be.callCount() != 0 {
		t.Fatal("quota-blocked request must not reach the backend")
	}
}

// ----------------------------------------------------------------------------
// ROUTING
// ----------------------------------------------------------------------------

// TestPredict_NoRoute: a model with no route fails ErrNoRoute (NOT_FOUND at the
// edge), publishing a NO_ROUTE failure with empty version (it failed before
// version selection).
func TestPredict_NoRoute(t *testing.T) {
	store := newMockRouteStore()
	pub := &mockPublisher{}
	svc := buildService(t, store, &mockLimiter{allow: true}, &mockBackend{}, pub, &mockQuota{})

	_, err := svc.Predict(context.Background(), domain.Principal{APIKeyID: "k"}, basicInput("ghost"))
	if !errors.Is(err, domain.ErrNoRoute) {
		t.Fatalf("err = %v, want ErrNoRoute", err)
	}
	ev, ok := pub.lastFailed()
	if !ok || ev.Reason != domain.FailureReasonNoRoute || ev.Version != "" {
		t.Fatalf("failure event = %+v, want NO_ROUTE with empty version", ev)
	}
}

// ----------------------------------------------------------------------------
// CIRCUIT BREAKER INTEGRATION (the use-case drives the breaker)
// ----------------------------------------------------------------------------

// TestPredict_BackendFailureTripsBreaker: repeated backend failures recorded by
// the use-case TRIP the breaker; once OPEN, the next predict fails fast with
// ErrCircuitOpen and the backend stops being called. This proves the use-case
// wires the breaker correctly end-to-end — the single most important integration
// in the service. failureThreshold is 3 (buildService).
func TestPredict_BackendFailureTripsBreaker(t *testing.T) {
	store := newMockRouteStore()
	be := &mockBackend{err: domain.ErrUpstream} // backend always fails
	pub := &mockPublisher{}
	svc := buildService(t, store, &mockLimiter{allow: true}, be, pub, &mockQuota{})
	deployStable(t, svc, "fraud", "v1", "e")

	p := domain.Principal{APIKeyID: "k"}
	// 3 failing calls trip the breaker. Each reaches the backend (CLOSED/HALF
	// admits) and is classed UPSTREAM_ERROR.
	for i := 0; i < 3; i++ {
		_, err := svc.Predict(context.Background(), p, basicInput("fraud"))
		if !errors.Is(err, domain.ErrUpstream) {
			t.Fatalf("call %d err = %v, want ErrUpstream", i, err)
		}
	}
	callsAfterTrip := be.callCount()
	if callsAfterTrip != 3 {
		t.Fatalf("backend calls before trip = %d, want 3", callsAfterTrip)
	}

	// 4th call: breaker is OPEN → fail fast, backend NOT called.
	_, err := svc.Predict(context.Background(), p, basicInput("fraud"))
	if !errors.Is(err, domain.ErrCircuitOpen) {
		t.Fatalf("post-trip err = %v, want ErrCircuitOpen", err)
	}
	if be.callCount() != callsAfterTrip {
		t.Fatalf("OPEN breaker still called backend: calls went %d → %d", callsAfterTrip, be.callCount())
	}

	// The breaker is observable as OPEN for that exact backend.
	states := svc.CircuitStates("fraud")
	if len(states) != 1 || states[0].State != domain.CircuitOpen {
		t.Fatalf("circuit states = %+v, want one OPEN breaker", states)
	}
	ev, _ := pub.lastFailed()
	if ev.Reason != domain.FailureReasonCircuitOpen {
		t.Fatalf("last failure reason = %v, want CIRCUIT_OPEN", ev.Reason)
	}
}

// TestPredict_SuccessAfterFailuresDoesNotTrip: the breaker counts CONSECUTIVE
// failures — a success between failures must keep it CLOSED. Verifies the
// use-case records successes too, not just failures.
func TestPredict_SuccessAfterFailuresDoesNotTrip(t *testing.T) {
	store := newMockRouteStore()
	be := &mockBackend{}
	svc := buildService(t, store, &mockLimiter{allow: true}, be, &mockPublisher{}, &mockQuota{})
	deployStable(t, svc, "fraud", "v1", "e")
	p := domain.Principal{APIKeyID: "k"}

	be.err = domain.ErrUpstream
	_, _ = svc.Predict(context.Background(), p, basicInput("fraud"))
	_, _ = svc.Predict(context.Background(), p, basicInput("fraud"))
	be.err = nil // backend recovers
	if _, err := svc.Predict(context.Background(), p, basicInput("fraud")); err != nil {
		t.Fatalf("recovered call errored: %v", err)
	}
	be.err = domain.ErrUpstream
	_, _ = svc.Predict(context.Background(), p, basicInput("fraud"))
	_, _ = svc.Predict(context.Background(), p, basicInput("fraud"))

	// 2 fails, success (reset), 2 fails → never 3 consecutive → still CLOSED.
	if states := svc.CircuitStates("fraud"); states[0].State != domain.CircuitClosed {
		t.Fatalf("breaker = %v, want CLOSED (success reset the count)", states[0].State)
	}
}

// ----------------------------------------------------------------------------
// TRAFFIC SPLITTING / OVERRIDE through the use-case
// ----------------------------------------------------------------------------

// TestPredict_VersionOverride: a privileged version_override bypasses weighted
// splitting and pins the named version (the canary executor / debug path). The
// served version + event reflect the override, and the backend gets that
// version's endpoint.
func TestPredict_VersionOverride(t *testing.T) {
	store := newMockRouteStore()
	be := &mockBackend{}
	svc := buildService(t, store, &mockLimiter{allow: true}, be, &mockPublisher{}, &mockQuota{})
	// Two versions: stable 100%, canary 0% (would never be picked by weight).
	if err := svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{ModelName: "fraud", Version: "v1", Endpoint: "v1.svc", WeightBps: domain.TotalWeightBps}); err != nil {
		t.Fatal(err)
	}
	if err := svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{ModelName: "fraud", Version: "v2", Endpoint: "v2.svc", WeightBps: 0}); err != nil {
		t.Fatal(err)
	}

	in := basicInput("fraud")
	in.VersionOverride = "v2"
	out, err := svc.Predict(context.Background(), domain.Principal{APIKeyID: "k"}, in)
	if err != nil {
		t.Fatalf("override predict: %v", err)
	}
	if out.ServedVersion != "v2" {
		t.Fatalf("served %q, want v2 (override)", out.ServedVersion)
	}
	if be.seenEnds[0] != "v2.svc" {
		t.Fatalf("forwarded to %q, want v2.svc", be.seenEnds[0])
	}
}

// TestPredict_OverrideUnknownVersion: an override naming a version not in the
// route is rejected NO_ROUTE (the gateway will not forward to an unknown/
// unresolved backend — anti-SSRF in spirit: only known endpoints are dialable).
func TestPredict_OverrideUnknownVersion(t *testing.T) {
	store := newMockRouteStore()
	svc := buildService(t, store, &mockLimiter{allow: true}, &mockBackend{}, &mockPublisher{}, &mockQuota{})
	deployStable(t, svc, "fraud", "v1", "v1.svc")

	in := basicInput("fraud")
	in.VersionOverride = "v9-does-not-exist"
	if _, err := svc.Predict(context.Background(), domain.Principal{APIKeyID: "k"}, in); !errors.Is(err, domain.ErrNoRoute) {
		t.Fatalf("err = %v, want ErrNoRoute for unknown override version", err)
	}
}

// TestPredict_CanaryFlag: when a non-stable (canary) target serves, the event's
// IsCanary is true so Billing/Experiment-Tracker can bucket A/B outcomes.
func TestPredict_CanaryFlag(t *testing.T) {
	store := newMockRouteStore()
	pub := &mockPublisher{}
	svc := buildService(t, store, &mockLimiter{allow: true}, &mockBackend{}, pub, &mockQuota{})
	// Realistic canary: v1 is deployed first → it is the STABLE target. v2 is
	// deployed next → a CANARY (non-stable). We then dial the split so the canary
	// takes 100% and stable 0%. The draw (0) skips v1's zero-width band and lands
	// in v2's [0,10000) band → v2 (the canary) serves. This proves IsCanary is a
	// property of the SERVED TARGET's stability, set correctly by the deploy-order
	// rule (first deploy = stable), not of the weight.
	if err := svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{ModelName: "fraud", Version: "v1", Endpoint: "v1.svc", WeightBps: domain.TotalWeightBps}); err != nil {
		t.Fatal(err)
	}
	if err := svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{ModelName: "fraud", Version: "v2", Endpoint: "v2.svc", WeightBps: 0}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.SetTrafficSplit(context.Background(), "fraud", []domain.TrafficWeight{
		{Version: "v1", WeightBps: 0}, {Version: "v2", WeightBps: domain.TotalWeightBps},
	}); err != nil {
		t.Fatal(err)
	}
	out, err := svc.Predict(context.Background(), domain.Principal{APIKeyID: "k"}, basicInput("fraud"))
	if err != nil {
		t.Fatalf("predict: %v", err)
	}
	if out.ServedVersion != "v2" || !out.IsCanary {
		t.Fatalf("out = %+v, want v2 served and IsCanary true", out)
	}
	ev, _ := pub.lastCompleted()
	if !ev.IsCanary {
		t.Fatal("event IsCanary should be true for a canary serve")
	}
}

// ----------------------------------------------------------------------------
// CONTROL PLANE: route validation invariants
// ----------------------------------------------------------------------------

// TestUpsertRoute_WeightSumInvariant: ACTIVE weights must sum to exactly 10000,
// and the SERVER resolves endpoints (a client cannot supply one → anti-SSRF).
func TestUpsertRoute_WeightSumInvariant(t *testing.T) {
	store := newMockRouteStore()
	svc := buildService(t, store, &mockLimiter{allow: true}, &mockBackend{}, &mockPublisher{}, &mockQuota{})
	// Seed two known endpoints via deploy events (the trusted endpoint source).
	_ = svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{ModelName: "fraud", Version: "v1", Endpoint: "v1.svc", WeightBps: domain.TotalWeightBps})
	_ = svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{ModelName: "fraud", Version: "v2", Endpoint: "v2.svc", WeightBps: 0})

	// Bad: weights sum to 8000, not 10000.
	_, err := svc.UpsertRoute(context.Background(), "fraud", []domain.ProposedTarget{
		{Version: "v1", WeightBps: 7000}, {Version: "v2", WeightBps: 1000},
	})
	if !errors.Is(err, domain.ErrRouteValidation) {
		t.Fatalf("bad sum err = %v, want ErrRouteValidation", err)
	}

	// Good: 9000 + 1000 = 10000. Endpoints are server-resolved from the deploy
	// records, NOT supplied by the caller.
	r, err := svc.UpsertRoute(context.Background(), "fraud", []domain.ProposedTarget{
		{Version: "v1", WeightBps: 9000}, {Version: "v2", WeightBps: 1000},
	})
	if err != nil {
		t.Fatalf("valid upsert: %v", err)
	}
	for _, tgt := range r.Targets {
		if tgt.Version == "v1" && tgt.Endpoint != "v1.svc" {
			t.Fatalf("endpoint not server-resolved: %+v", tgt)
		}
	}
}

// TestUpsertRoute_RejectsUnknownEndpoint: proposing a version with no known
// deploy record (no resolvable endpoint) is rejected — the gateway refuses to
// invent a backend address (anti-SSRF: only server-resolved endpoints route).
func TestUpsertRoute_RejectsUnknownEndpoint(t *testing.T) {
	store := newMockRouteStore()
	svc := buildService(t, store, &mockLimiter{allow: true}, &mockBackend{}, &mockPublisher{}, &mockQuota{})
	deployStable(t, svc, "fraud", "v1", "v1.svc")

	_, err := svc.UpsertRoute(context.Background(), "fraud", []domain.ProposedTarget{
		{Version: "v1", WeightBps: 5000}, {Version: "phantom", WeightBps: 5000}, // phantom has no endpoint
	})
	if !errors.Is(err, domain.ErrRouteValidation) {
		t.Fatalf("err = %v, want ErrRouteValidation for unknown-endpoint version", err)
	}
}

// TestSetTrafficSplit_Dial: the canary dial reweights existing versions and
// rejects unknown versions / bad sums. Verifies the most common rollout op.
func TestSetTrafficSplit_Dial(t *testing.T) {
	store := newMockRouteStore()
	svc := buildService(t, store, &mockLimiter{allow: true}, &mockBackend{}, &mockPublisher{}, &mockQuota{})
	_ = svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{ModelName: "fraud", Version: "v1", Endpoint: "v1.svc", WeightBps: domain.TotalWeightBps})
	_ = svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{ModelName: "fraud", Version: "v2", Endpoint: "v2.svc", WeightBps: 0})

	// Dial to 50/50.
	r, err := svc.SetTrafficSplit(context.Background(), "fraud", []domain.TrafficWeight{
		{Version: "v1", WeightBps: 5000}, {Version: "v2", WeightBps: 5000},
	})
	if err != nil {
		t.Fatalf("set split: %v", err)
	}
	if r.ActiveWeightSum() != domain.TotalWeightBps {
		t.Fatalf("active sum = %d, want 10000", r.ActiveWeightSum())
	}

	// Unknown version rejected.
	if _, err := svc.SetTrafficSplit(context.Background(), "fraud", []domain.TrafficWeight{
		{Version: "v1", WeightBps: 5000}, {Version: "ghost", WeightBps: 5000},
	}); !errors.Is(err, domain.ErrRouteValidation) {
		t.Fatalf("unknown version err = %v, want ErrRouteValidation", err)
	}
}

// ----------------------------------------------------------------------------
// EVENT REACTIONS (the routing table as a pure reactor)
// ----------------------------------------------------------------------------

// TestApplyModelPromoted_RepointsTraffic: a promote sends 100% to the new
// version (marked stable) and tears down the auto-demoted old one — so a predict
// afterward serves the promoted version.
func TestApplyModelPromoted_RepointsTraffic(t *testing.T) {
	store := newMockRouteStore()
	svc := buildService(t, store, &mockLimiter{allow: true}, &mockBackend{}, &mockPublisher{}, &mockQuota{})
	_ = svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{ModelName: "fraud", Version: "v1", Endpoint: "v1.svc", WeightBps: 9000})
	_ = svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{ModelName: "fraud", Version: "v2", Endpoint: "v2.svc", WeightBps: 1000})

	if err := svc.ApplyModelPromoted(context.Background(), domain.ModelPromoted{
		ModelName: "fraud", Version: "v2", DemotedVersion: "v1",
	}); err != nil {
		t.Fatalf("promote: %v", err)
	}

	r, err := svc.GetRoute(context.Background(), "fraud")
	if err != nil {
		t.Fatalf("get route: %v", err)
	}
	// v2 should now be the only eligible target at 100% and stable; v1 gone/drained.
	var v2 *domain.RouteTarget
	for i := range r.Targets {
		if r.Targets[i].Version == "v2" {
			v2 = &r.Targets[i]
		}
		if r.Targets[i].Version == "v1" && r.Targets[i].Status.Eligible() {
			t.Fatal("demoted v1 should no longer be eligible")
		}
	}
	if v2 == nil || v2.WeightBps != domain.TotalWeightBps || !v2.IsStable {
		t.Fatalf("promoted v2 = %+v, want 10000 bps stable", v2)
	}
	if r.ActiveWeightSum() != domain.TotalWeightBps {
		t.Fatalf("post-promote active sum = %d, want 10000", r.ActiveWeightSum())
	}
}

// TestApplyModelUndeployed_RemovesTarget: undeploy removes a version; if it was
// the last target the model becomes non-serving (predict → NO_ROUTE).
func TestApplyModelUndeployed_RemovesTarget(t *testing.T) {
	store := newMockRouteStore()
	svc := buildService(t, store, &mockLimiter{allow: true}, &mockBackend{}, &mockPublisher{}, &mockQuota{})
	deployStable(t, svc, "fraud", "v1", "v1.svc")

	if err := svc.ApplyModelUndeployed(context.Background(), domain.ModelUndeployed{ModelName: "fraud", Version: "v1", Reason: "teardown"}); err != nil {
		t.Fatalf("undeploy: %v", err)
	}
	if _, err := svc.Predict(context.Background(), domain.Principal{APIKeyID: "k"}, basicInput("fraud")); !errors.Is(err, domain.ErrNoRoute) {
		t.Fatalf("predict after undeploy err = %v, want ErrNoRoute", err)
	}
}

// TestApplyModelArchived_DropsRoute: archive removes the whole route.
func TestApplyModelArchived_DropsRoute(t *testing.T) {
	store := newMockRouteStore()
	svc := buildService(t, store, &mockLimiter{allow: true}, &mockBackend{}, &mockPublisher{}, &mockQuota{})
	deployStable(t, svc, "fraud", "v1", "v1.svc")

	if err := svc.ApplyModelArchived(context.Background(), domain.ModelArchived{ModelName: "fraud"}); err != nil {
		t.Fatalf("archive: %v", err)
	}
	if _, err := svc.GetRoute(context.Background(), "fraud"); !errors.Is(err, domain.ErrNoRoute) {
		t.Fatalf("get route after archive err = %v, want ErrNoRoute", err)
	}
}

// TestApplyEvents_Idempotent: redelivering the SAME deploy event must not create
// a duplicate target or change the weight sum — consumers must be idempotent
// (NATS is at-least-once). Re-applying ModelDeployed for an existing version
// updates in place.
func TestApplyEvents_Idempotent(t *testing.T) {
	store := newMockRouteStore()
	svc := buildService(t, store, &mockLimiter{allow: true}, &mockBackend{}, &mockPublisher{}, &mockQuota{})
	ev := domain.ModelDeployed{ModelName: "fraud", Version: "v1", Endpoint: "v1.svc", WeightBps: domain.TotalWeightBps}
	_ = svc.ApplyModelDeployed(context.Background(), ev)
	_ = svc.ApplyModelDeployed(context.Background(), ev) // redelivery

	r, _ := svc.GetRoute(context.Background(), "fraud")
	if len(r.Targets) != 1 {
		t.Fatalf("idempotency broken: %d targets after duplicate deploy, want 1", len(r.Targets))
	}
}

// ----------------------------------------------------------------------------
// ATOMICITY — validate-then-commit (Finding 1)
// ----------------------------------------------------------------------------

// TestSetTrafficSplit_RejectedWriteDoesNotCorruptStore is the regression test for
// the most damaging in-place-mutation bug. A SetTrafficSplit whose new ACTIVE
// weights do NOT sum to 10000 must be REJECTED with ErrRouteValidation AND leave
// the stored route's weights exactly as they were — true validate-then-commit.
//
// WHY the adversarial sharedBackingRouteStore: it returns the live Route without
// copying its Targets, so a fetched route SHARES the stored backing array — the
// worst case the finding reproduced. Before the fix, SetTrafficSplit wrote each
// proposed weight in place through findTarget pointers and only summed afterward,
// so the rejected 3000/4000 (sum 7000) landed on the stored route before the sum
// check failed; the next GetRoute/Predict then routed on never-committed weights.
// The copy-on-write fix mutates a clone, so a reject is a no-op against the store
// even when the store shares its backing array — proving the atomicity lives in
// the domain, not in a cooperative adapter.
func TestSetTrafficSplit_RejectedWriteDoesNotCorruptStore(t *testing.T) {
	store := newSharedBackingRouteStore()
	svc := buildService(t, store, &mockLimiter{allow: true}, &mockBackend{}, &mockPublisher{}, &mockQuota{})
	// Seed a valid 9000/1000 split (the COMMITTED, correct state).
	_ = svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{ModelName: "fraud", Version: "v1", Endpoint: "v1.svc", WeightBps: domain.TotalWeightBps})
	_ = svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{ModelName: "fraud", Version: "v2", Endpoint: "v2.svc", WeightBps: 0})
	if _, err := svc.SetTrafficSplit(context.Background(), "fraud", []domain.TrafficWeight{
		{Version: "v1", WeightBps: 9000}, {Version: "v2", WeightBps: 1000},
	}); err != nil {
		t.Fatalf("seed split: %v", err)
	}

	// Propose a split that sums to 7000 (3000 + 4000) — MUST be rejected.
	_, err := svc.SetTrafficSplit(context.Background(), "fraud", []domain.TrafficWeight{
		{Version: "v1", WeightBps: 3000}, {Version: "v2", WeightBps: 4000},
	})
	if !errors.Is(err, domain.ErrRouteValidation) {
		t.Fatalf("bad-sum split err = %v, want ErrRouteValidation", err)
	}

	// The stored route must STILL be the committed 9000/1000 — not the rejected
	// 3000/4000. This is the assertion that fails on the old in-place code.
	r, err := svc.GetRoute(context.Background(), "fraud")
	if err != nil {
		t.Fatalf("get route after reject: %v", err)
	}
	got := map[string]int{}
	for _, tgt := range r.Targets {
		got[tgt.Version] = tgt.WeightBps
	}
	if got["v1"] != 9000 || got["v2"] != 1000 {
		t.Fatalf("rejected write corrupted the store: weights = %v, want v1=9000 v2=1000", got)
	}
	if r.ActiveWeightSum() != domain.TotalWeightBps {
		t.Fatalf("stored active sum = %d after reject, want 10000 (uncorrupted)", r.ActiveWeightSum())
	}

	// And a predict still routes on the committed weights (draw 0 → v1's band).
	be := &mockBackend{}
	svc2 := buildService(t, store, &mockLimiter{allow: true}, be, &mockPublisher{}, &mockQuota{})
	out, err := svc2.Predict(context.Background(), domain.Principal{APIKeyID: "k"}, basicInput("fraud"))
	if err != nil {
		t.Fatalf("predict after reject: %v", err)
	}
	if out.ServedVersion != "v1" {
		t.Fatalf("served %q after rejected split, want v1 (uncorrupted route)", out.ServedVersion)
	}
}

// ----------------------------------------------------------------------------
// CONCURRENCY — Predict (hot path) vs event reactions (Finding 2)
// ----------------------------------------------------------------------------

// TestPredict_ConcurrentWithReweight_NoRace runs N Predict goroutines (the hot
// path: route Get → splitter scan of Targets) WHILE event-reaction goroutines
// reweight, promote, and re-deploy the same model. Under `go test -race` this
// pins down the data race the findings describe: the splitter READS
// WeightBps/Status/Endpoint off route.Targets while ApplyModel*/SetTrafficSplit
// WRITE those same fields. Before the fix those writes were in place on a shared
// backing array, so -race flagged "Read at splitter scan vs Write at
// ApplyModelDeployed". The fix is two-layer — the domain clones before mutating,
// and the store hands out deep-copy snapshots — so every reader sees an immutable
// route and no goroutine writes a slice another is reading.
//
// We don't assert exact outputs (the served version legitimately varies as the
// split changes mid-flight); the test's JOB is to be a race detector — it must
// run clean under -race and never panic / index out of range from a torn slice.
func TestPredict_ConcurrentWithReweight_NoRace(t *testing.T) {
	// Deliberately use the ADVERSARIAL shared-backing store (no copy-on-read) so
	// this test depends SOLELY on the domain's copy-on-write discipline, not on the
	// store's snapshot isolation. That makes it a tight regression guard for the
	// domain fix: if any Apply*/SetTrafficSplit reverts to mutating a fetched
	// route's targets in place, -race trips here even though a real (deep-copying)
	// adapter would mask it. (TestPredict_ConcurrentWithReweight is implicitly also
	// covered by the deep-copy mockRouteStore everywhere else.)
	store := newSharedBackingRouteStore()
	be := &mockBackend{result: domain.PredictResult{Outputs: map[string]domain.Tensor{"p": {Data: []byte{1}}}}}
	svc := buildService(t, store, &mockLimiter{allow: true}, be, &mockPublisher{}, &mockQuota{})

	// Seed two versions so reweights/promotes have something to shuffle.
	_ = svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{ModelName: "fraud", Version: "v1", Endpoint: "v1.svc", WeightBps: domain.TotalWeightBps})
	_ = svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{ModelName: "fraud", Version: "v2", Endpoint: "v2.svc", WeightBps: 0})

	ctx := context.Background()
	p := domain.Principal{APIKeyID: "k"}
	const readers = 8
	const iters = 200

	var wg sync.WaitGroup

	// READERS: hammer the hot path (route lookup + splitter scan + forward).
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				// Errors are fine (e.g. a transient all-zero split between writes
				// could yield NoRoute); we only care that no race/panic occurs.
				_, _ = svc.Predict(ctx, p, basicInput("fraud"))
			}
		}()
	}

	// WRITER: continuously mutate the route via the event-reaction + control-plane
	// paths that previously mutated the shared backing array in place.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < iters; j++ {
			switch j % 4 {
			case 0:
				_, _ = svc.SetTrafficSplit(ctx, "fraud", []domain.TrafficWeight{
					{Version: "v1", WeightBps: 7000}, {Version: "v2", WeightBps: 3000},
				})
			case 1:
				_, _ = svc.SetTrafficSplit(ctx, "fraud", []domain.TrafficWeight{
					{Version: "v1", WeightBps: 5000}, {Version: "v2", WeightBps: 5000},
				})
			case 2:
				// Re-deploy v2 with a fresh weight (ApplyModelDeployed update path).
				_ = svc.ApplyModelDeployed(ctx, domain.ModelDeployed{ModelName: "fraud", Version: "v2", Endpoint: "v2.svc", WeightBps: 2000})
				_, _ = svc.SetTrafficSplit(ctx, "fraud", []domain.TrafficWeight{
					{Version: "v1", WeightBps: 8000}, {Version: "v2", WeightBps: 2000},
				})
			case 3:
				// Promote v2 (rewrites the whole target set) then restore a split so
				// v1 stays in the table for the next iteration.
				_ = svc.ApplyModelPromoted(ctx, domain.ModelPromoted{ModelName: "fraud", Version: "v2", DemotedVersion: "v1"})
				_, _ = svc.SetTrafficSplit(ctx, "fraud", []domain.TrafficWeight{
					{Version: "v1", WeightBps: 9000}, {Version: "v2", WeightBps: 1000},
				})
			}
		}
	}()

	wg.Wait()
	// Reaching here clean under -race IS the assertion. A final sanity check that
	// the route is still well-formed after the storm.
	r, err := svc.GetRoute(ctx, "fraud")
	if err != nil {
		t.Fatalf("route gone after concurrent storm: %v", err)
	}
	if len(r.Targets) == 0 {
		t.Fatal("route has no targets after concurrent storm")
	}
}
