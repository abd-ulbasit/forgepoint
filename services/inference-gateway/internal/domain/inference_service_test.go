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
// it cares about. No codegen.
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

// Get/Upsert/Delete are keyed by the OPAQUE domain key (routeKey(team, model)) the
// service composes — mirroring the production adapter, which is also key-opaque.
func (m *mockRouteStore) Get(_ context.Context, key string) (domain.Route, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.routes[key]
	if !ok {
		return domain.Route{}, domain.ErrNoRoute
	}
	return cloneRoute(r), nil // snapshot isolation: caller gets a private copy
}
func (m *mockRouteStore) Upsert(_ context.Context, key string, r domain.Route) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.routes[key] = cloneRoute(r) // store a private copy, not the caller's slice
	return nil
}
func (m *mockRouteStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.routes, key)
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
func (m *sharedBackingRouteStore) Get(_ context.Context, key string) (domain.Route, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.routes[key]
	if !ok {
		return domain.Route{}, domain.ErrNoRoute
	}
	return r, nil // NO copy: shares the Targets backing array on purpose
}
func (m *sharedBackingRouteStore) Upsert(_ context.Context, key string, r domain.Route) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.routes[key] = r
	return nil
}
func (m *sharedBackingRouteStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.routes, key)
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

// testTeam is the owning team used across the single-tenant tests: deploys create
// routes UNDER it and predicts/control-plane calls are made AS it, so the route
// namespacing (the IDOR fix) is satisfied transparently in the existing specs. The
// dedicated cross-tenant tests (TestRouteTenancy_*) use two distinct teams to prove
// isolation.
const testTeam = "team-a"

// deployStable seeds a single-version (stable, 100%) route through the event
// reaction path — exercising ApplyModelDeployed and giving predict something to
// route to. The route is created under testTeam (its owning team).
func deployStable(t *testing.T, svc domain.InferenceService, model, version, endpoint string) {
	t.Helper()
	if err := svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{
		OwnerTeam: testTeam, ModelName: model, Version: version, Endpoint: endpoint, WeightBps: domain.TotalWeightBps,
	}); err != nil {
		t.Fatalf("ApplyModelDeployed: %v", err)
	}
}

// predictAs is a small helper: a Predict from testTeam with the given api key. Most
// tests don't care about the api key value, only that the team matches the deploy.
func predictAs(svc domain.InferenceService, apiKey string, in domain.PredictInput) (domain.PredictOutput, error) {
	return svc.Predict(context.Background(), domain.Principal{APIKeyID: apiKey, Team: testTeam}, in)
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
			_, err := svc.Predict(context.Background(), domain.Principal{APIKeyID: "k", Team: testTeam}, c.in)
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

	_, err := svc.Predict(context.Background(), domain.Principal{APIKeyID: "k", Team: testTeam}, basicInput("fraud"))
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

	_, err := svc.Predict(context.Background(), domain.Principal{APIKeyID: "k", Team: testTeam}, basicInput("ghost"))
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

	p := domain.Principal{APIKeyID: "k", Team: testTeam}
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
	p := domain.Principal{APIKeyID: "k", Team: testTeam}

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
	if err := svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{OwnerTeam: testTeam, ModelName: "fraud", Version: "v1", Endpoint: "v1.svc", WeightBps: domain.TotalWeightBps}); err != nil {
		t.Fatal(err)
	}
	if err := svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{OwnerTeam: testTeam, ModelName: "fraud", Version: "v2", Endpoint: "v2.svc", WeightBps: 0}); err != nil {
		t.Fatal(err)
	}

	in := basicInput("fraud")
	in.VersionOverride = "v2"
	out, err := svc.Predict(context.Background(), domain.Principal{APIKeyID: "k", Team: testTeam}, in)
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
	if _, err := svc.Predict(context.Background(), domain.Principal{APIKeyID: "k", Team: testTeam}, in); !errors.Is(err, domain.ErrNoRoute) {
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
	if err := svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{OwnerTeam: testTeam, ModelName: "fraud", Version: "v1", Endpoint: "v1.svc", WeightBps: domain.TotalWeightBps}); err != nil {
		t.Fatal(err)
	}
	if err := svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{OwnerTeam: testTeam, ModelName: "fraud", Version: "v2", Endpoint: "v2.svc", WeightBps: 0}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.SetTrafficSplit(context.Background(), testTeam, "fraud", []domain.TrafficWeight{
		{Version: "v1", WeightBps: 0}, {Version: "v2", WeightBps: domain.TotalWeightBps},
	}); err != nil {
		t.Fatal(err)
	}
	out, err := svc.Predict(context.Background(), domain.Principal{APIKeyID: "k", Team: testTeam}, basicInput("fraud"))
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
	_ = svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{OwnerTeam: testTeam, ModelName: "fraud", Version: "v1", Endpoint: "v1.svc", WeightBps: domain.TotalWeightBps})
	_ = svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{OwnerTeam: testTeam, ModelName: "fraud", Version: "v2", Endpoint: "v2.svc", WeightBps: 0})

	// Bad: weights sum to 8000, not 10000.
	_, err := svc.UpsertRoute(context.Background(), testTeam, "fraud", []domain.ProposedTarget{
		{Version: "v1", WeightBps: 7000}, {Version: "v2", WeightBps: 1000},
	})
	if !errors.Is(err, domain.ErrRouteValidation) {
		t.Fatalf("bad sum err = %v, want ErrRouteValidation", err)
	}

	// Good: 9000 + 1000 = 10000. Endpoints are server-resolved from the deploy
	// records, NOT supplied by the caller.
	r, err := svc.UpsertRoute(context.Background(), testTeam, "fraud", []domain.ProposedTarget{
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

	_, err := svc.UpsertRoute(context.Background(), testTeam, "fraud", []domain.ProposedTarget{
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
	_ = svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{OwnerTeam: testTeam, ModelName: "fraud", Version: "v1", Endpoint: "v1.svc", WeightBps: domain.TotalWeightBps})
	_ = svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{OwnerTeam: testTeam, ModelName: "fraud", Version: "v2", Endpoint: "v2.svc", WeightBps: 0})

	// Dial to 50/50.
	r, err := svc.SetTrafficSplit(context.Background(), testTeam, "fraud", []domain.TrafficWeight{
		{Version: "v1", WeightBps: 5000}, {Version: "v2", WeightBps: 5000},
	})
	if err != nil {
		t.Fatalf("set split: %v", err)
	}
	if r.ActiveWeightSum() != domain.TotalWeightBps {
		t.Fatalf("active sum = %d, want 10000", r.ActiveWeightSum())
	}

	// Unknown version rejected.
	if _, err := svc.SetTrafficSplit(context.Background(), testTeam, "fraud", []domain.TrafficWeight{
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
	_ = svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{OwnerTeam: testTeam, ModelName: "fraud", Version: "v1", Endpoint: "v1.svc", WeightBps: 9000})
	_ = svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{OwnerTeam: testTeam, ModelName: "fraud", Version: "v2", Endpoint: "v2.svc", WeightBps: 1000})

	if err := svc.ApplyModelPromoted(context.Background(), domain.ModelPromoted{
		OwnerTeam: testTeam,
		ModelName: "fraud", Version: "v2", DemotedVersion: "v1",
	}); err != nil {
		t.Fatalf("promote: %v", err)
	}

	r, err := svc.GetRoute(context.Background(), testTeam, "fraud")
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

	if err := svc.ApplyModelUndeployed(context.Background(), domain.ModelUndeployed{OwnerTeam: testTeam, ModelName: "fraud", Version: "v1", Reason: "teardown"}); err != nil {
		t.Fatalf("undeploy: %v", err)
	}
	if _, err := svc.Predict(context.Background(), domain.Principal{APIKeyID: "k", Team: testTeam}, basicInput("fraud")); !errors.Is(err, domain.ErrNoRoute) {
		t.Fatalf("predict after undeploy err = %v, want ErrNoRoute", err)
	}
}

// TestApplyModelArchived_DropsRoute: archive removes the whole route.
func TestApplyModelArchived_DropsRoute(t *testing.T) {
	store := newMockRouteStore()
	svc := buildService(t, store, &mockLimiter{allow: true}, &mockBackend{}, &mockPublisher{}, &mockQuota{})
	deployStable(t, svc, "fraud", "v1", "v1.svc")

	if err := svc.ApplyModelArchived(context.Background(), domain.ModelArchived{OwnerTeam: testTeam, ModelName: "fraud"}); err != nil {
		t.Fatalf("archive: %v", err)
	}
	if _, err := svc.GetRoute(context.Background(), testTeam, "fraud"); !errors.Is(err, domain.ErrNoRoute) {
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
	ev := domain.ModelDeployed{OwnerTeam: testTeam, ModelName: "fraud", Version: "v1", Endpoint: "v1.svc", WeightBps: domain.TotalWeightBps}
	_ = svc.ApplyModelDeployed(context.Background(), ev)
	_ = svc.ApplyModelDeployed(context.Background(), ev) // redelivery

	r, _ := svc.GetRoute(context.Background(), testTeam, "fraud")
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
	_ = svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{OwnerTeam: testTeam, ModelName: "fraud", Version: "v1", Endpoint: "v1.svc", WeightBps: domain.TotalWeightBps})
	_ = svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{OwnerTeam: testTeam, ModelName: "fraud", Version: "v2", Endpoint: "v2.svc", WeightBps: 0})
	if _, err := svc.SetTrafficSplit(context.Background(), testTeam, "fraud", []domain.TrafficWeight{
		{Version: "v1", WeightBps: 9000}, {Version: "v2", WeightBps: 1000},
	}); err != nil {
		t.Fatalf("seed split: %v", err)
	}

	// Propose a split that sums to 7000 (3000 + 4000) — MUST be rejected.
	_, err := svc.SetTrafficSplit(context.Background(), testTeam, "fraud", []domain.TrafficWeight{
		{Version: "v1", WeightBps: 3000}, {Version: "v2", WeightBps: 4000},
	})
	if !errors.Is(err, domain.ErrRouteValidation) {
		t.Fatalf("bad-sum split err = %v, want ErrRouteValidation", err)
	}

	// The stored route must STILL be the committed 9000/1000 — not the rejected
	// 3000/4000. This is the assertion that fails on the old in-place code.
	r, err := svc.GetRoute(context.Background(), testTeam, "fraud")
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
	out, err := svc2.Predict(context.Background(), domain.Principal{APIKeyID: "k", Team: testTeam}, basicInput("fraud"))
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
	_ = svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{OwnerTeam: testTeam, ModelName: "fraud", Version: "v1", Endpoint: "v1.svc", WeightBps: domain.TotalWeightBps})
	_ = svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{OwnerTeam: testTeam, ModelName: "fraud", Version: "v2", Endpoint: "v2.svc", WeightBps: 0})

	ctx := context.Background()
	p := domain.Principal{APIKeyID: "k", Team: testTeam}
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
				_, _ = svc.SetTrafficSplit(ctx, testTeam, "fraud", []domain.TrafficWeight{
					{Version: "v1", WeightBps: 7000}, {Version: "v2", WeightBps: 3000},
				})
			case 1:
				_, _ = svc.SetTrafficSplit(ctx, testTeam, "fraud", []domain.TrafficWeight{
					{Version: "v1", WeightBps: 5000}, {Version: "v2", WeightBps: 5000},
				})
			case 2:
				// Re-deploy v2 with a fresh weight (ApplyModelDeployed update path).
				_ = svc.ApplyModelDeployed(ctx, domain.ModelDeployed{OwnerTeam: testTeam, ModelName: "fraud", Version: "v2", Endpoint: "v2.svc", WeightBps: 2000})
				_, _ = svc.SetTrafficSplit(ctx, testTeam, "fraud", []domain.TrafficWeight{
					{Version: "v1", WeightBps: 8000}, {Version: "v2", WeightBps: 2000},
				})
			case 3:
				// Promote v2 (rewrites the whole target set) then restore a split so
				// v1 stays in the table for the next iteration.
				_ = svc.ApplyModelPromoted(ctx, domain.ModelPromoted{OwnerTeam: testTeam, ModelName: "fraud", Version: "v2", DemotedVersion: "v1"})
				_, _ = svc.SetTrafficSplit(ctx, testTeam, "fraud", []domain.TrafficWeight{
					{Version: "v1", WeightBps: 9000}, {Version: "v2", WeightBps: 1000},
				})
			}
		}
	}()

	wg.Wait()
	// Reaching here clean under -race IS the assertion. A final sanity check that
	// the route is still well-formed after the storm.
	r, err := svc.GetRoute(ctx, testTeam, "fraud")
	if err != nil {
		t.Fatalf("route gone after concurrent storm: %v", err)
	}
	if len(r.Targets) == 0 {
		t.Fatal("route has no targets after concurrent storm")
	}
}

// ----------------------------------------------------------------------------
// MULTI-TENANT ROUTE ISOLATION (cross-tenant IDOR fix)
// ----------------------------------------------------------------------------
//
// The bug: the routing table used to be a GLOBAL namespace keyed only by
// model_name. The auth interceptor authenticates a JWT from ANY team but does not
// tenancy-authorize, so any caller could read, repoint, or delete a route owned by
// another team purely by naming it — or hijack a name collision (two teams both
// deploy "fraud"). The fix namespaces every route operation by (team, model_name),
// deriving the team from the verified Principal.Team / the owning team on the
// ModelDeployed event. These tests use TWO teams sharing the SAME model name
// "fraud" and assert team A can never touch team B's route, while same-team access
// keeps working. A cross-team miss returns ErrNoRoute (NOT_FOUND) — no oracle.

const (
	teamA = "team-a"
	teamB = "team-b"
)

// deployForTeam seeds a single stable 100% route under a SPECIFIC owning team,
// exercising the event path with the OwnerTeam set (the namespacing source).
func deployForTeam(t *testing.T, svc domain.InferenceService, team, model, version, endpoint string) {
	t.Helper()
	if err := svc.ApplyModelDeployed(context.Background(), domain.ModelDeployed{
		OwnerTeam: team, ModelName: model, Version: version, Endpoint: endpoint, WeightBps: domain.TotalWeightBps,
	}); err != nil {
		t.Fatalf("ApplyModelDeployed(team=%s): %v", team, err)
	}
}

// twoTenantService wires a service and deploys the SAME model name "fraud" for two
// different teams, each pointing at its OWN backend endpoint. The shared mock store
// holds both rows under distinct (team, model) keys.
func twoTenantService(t *testing.T) (domain.InferenceService, *mockBackend) {
	t.Helper()
	store := newMockRouteStore()
	be := &mockBackend{result: domain.PredictResult{Outputs: map[string]domain.Tensor{"p": {Data: []byte{1}}}}}
	svc := buildService(t, store, &mockLimiter{allow: true}, be, &mockPublisher{}, &mockQuota{})
	deployForTeam(t, svc, teamA, "fraud", "vA", "a-backend.svc:9090")
	deployForTeam(t, svc, teamB, "fraud", "vB", "b-backend.svc:9090")
	return svc, be
}

// TestRouteTenancy_PredictIsolation: each team's Predict for "fraud" must resolve to
// its OWN backend version/endpoint — never the other team's. This is the data-plane
// half of the IDOR: a colliding model name must not let team A's traffic reach (or
// even observe) team B's backend.
func TestRouteTenancy_PredictIsolation(t *testing.T) {
	svc, be := twoTenantService(t)

	outA, err := svc.Predict(context.Background(), domain.Principal{APIKeyID: "ka", Team: teamA}, basicInput("fraud"))
	if err != nil {
		t.Fatalf("team A predict: %v", err)
	}
	if outA.ServedVersion != "vA" {
		t.Fatalf("team A served %q, want vA (its own route)", outA.ServedVersion)
	}

	outB, err := svc.Predict(context.Background(), domain.Principal{APIKeyID: "kb", Team: teamB}, basicInput("fraud"))
	if err != nil {
		t.Fatalf("team B predict: %v", err)
	}
	if outB.ServedVersion != "vB" {
		t.Fatalf("team B served %q, want vB (its own route)", outB.ServedVersion)
	}

	// The backend endpoints actually dialed must be each team's own — proof there is
	// no cross-tenant forwarding even with a colliding model name.
	if len(be.seenEnds) != 2 || be.seenEnds[0] != "a-backend.svc:9090" || be.seenEnds[1] != "b-backend.svc:9090" {
		t.Fatalf("dialed endpoints = %v, want [a-backend.svc:9090 b-backend.svc:9090]", be.seenEnds)
	}
}

// TestRouteTenancy_PredictForeignTeamWithoutOwnRouteIsNoRoute: a team that has NOT
// deployed "fraud" gets ErrNoRoute even though ANOTHER team owns a "fraud" route —
// it cannot ride a foreign team's route, and the miss is indistinguishable from
// "never deployed" (no existence oracle).
func TestRouteTenancy_PredictForeignTeamWithoutOwnRouteIsNoRoute(t *testing.T) {
	store := newMockRouteStore()
	svc := buildService(t, store, &mockLimiter{allow: true}, &mockBackend{}, &mockPublisher{}, &mockQuota{})
	deployForTeam(t, svc, teamA, "fraud", "vA", "a-backend.svc:9090") // only team A owns it

	_, err := svc.Predict(context.Background(), domain.Principal{APIKeyID: "kb", Team: teamB}, basicInput("fraud"))
	if !errors.Is(err, domain.ErrNoRoute) {
		t.Fatalf("team B predict on team A's route err = %v, want ErrNoRoute (no cross-tenant ride)", err)
	}
}

// TestRouteTenancy_GetRouteIsolation: team B cannot READ team A's route by name —
// GetRoute(teamB, "fraud") must miss while GetRoute(teamA, "fraud") returns A's
// route with A's endpoint. No oracle: the cross-team read is ErrNoRoute, identical
// to "absent".
func TestRouteTenancy_GetRouteIsolation(t *testing.T) {
	svc, _ := twoTenantService(t)

	rA, err := svc.GetRoute(context.Background(), teamA, "fraud")
	if err != nil {
		t.Fatalf("team A GetRoute: %v", err)
	}
	if rA.OwnerTeam != teamA || len(rA.Targets) != 1 || rA.Targets[0].Endpoint != "a-backend.svc:9090" {
		t.Fatalf("team A route = %+v, want its own (vA / a-backend)", rA)
	}

	// Same name, but team B asking for what is really team A's data → must NOT leak.
	// (Both teams DO own a "fraud" here; we assert B sees B's, never A's.)
	rB, err := svc.GetRoute(context.Background(), teamB, "fraud")
	if err != nil {
		t.Fatalf("team B GetRoute: %v", err)
	}
	if rB.OwnerTeam != teamB || rB.Targets[0].Endpoint != "b-backend.svc:9090" {
		t.Fatalf("team B route = %+v leaked team A data; want vB / b-backend", rB)
	}
}

// TestRouteTenancy_GetRouteForeignIsNoRoute: when ONLY team A owns "fraud", team B's
// GetRoute misses — a pure cross-tenant read attempt yields ErrNoRoute (no oracle).
func TestRouteTenancy_GetRouteForeignIsNoRoute(t *testing.T) {
	store := newMockRouteStore()
	svc := buildService(t, store, &mockLimiter{allow: true}, &mockBackend{}, &mockPublisher{}, &mockQuota{})
	deployForTeam(t, svc, teamA, "fraud", "vA", "a-backend.svc:9090")

	if _, err := svc.GetRoute(context.Background(), teamB, "fraud"); !errors.Is(err, domain.ErrNoRoute) {
		t.Fatalf("team B GetRoute on team A's route err = %v, want ErrNoRoute", err)
	}
}

// TestRouteTenancy_UpsertCannotHijack: team B's UpsertRoute for "fraud" must NOT
// repoint team A's route. Because endpoints are server-resolved from the CALLER'S
// own existing route, B has no endpoint for "fraud" (it never deployed one), so the
// upsert is rejected — and team A's route is provably unchanged afterward.
func TestRouteTenancy_UpsertCannotHijack(t *testing.T) {
	store := newMockRouteStore()
	svc := buildService(t, store, &mockLimiter{allow: true}, &mockBackend{}, &mockPublisher{}, &mockQuota{})
	deployForTeam(t, svc, teamA, "fraud", "vA", "a-backend.svc:9090") // only team A owns it

	// Team B tries to upsert "fraud" naming team A's version vA at 100%. Since B has
	// no deploy record for "fraud", vA has no B-resolvable endpoint → rejected.
	_, err := svc.UpsertRoute(context.Background(), teamB, "fraud", []domain.ProposedTarget{
		{Version: "vA", WeightBps: domain.TotalWeightBps},
	})
	if !errors.Is(err, domain.ErrRouteValidation) {
		t.Fatalf("team B hijack upsert err = %v, want ErrRouteValidation (no endpoint in B's namespace)", err)
	}

	// Team A's route is untouched: still vA → a-backend, owned by team A.
	rA, err := svc.GetRoute(context.Background(), teamA, "fraud")
	if err != nil {
		t.Fatalf("team A GetRoute after B's hijack attempt: %v", err)
	}
	if rA.OwnerTeam != teamA || rA.Targets[0].Version != "vA" || rA.Targets[0].Endpoint != "a-backend.svc:9090" {
		t.Fatalf("team A route corrupted by team B upsert: %+v", rA)
	}

	// And a team B Upsert lands in B's OWN namespace, never colliding with A. Deploy
	// a B-owned endpoint first, then B can validly upsert its own "fraud".
	deployForTeam(t, svc, teamB, "fraud", "vB", "b-backend.svc:9090")
	rBnew, err := svc.UpsertRoute(context.Background(), teamB, "fraud", []domain.ProposedTarget{
		{Version: "vB", WeightBps: domain.TotalWeightBps},
	})
	if err != nil {
		t.Fatalf("team B valid self-upsert: %v", err)
	}
	if rBnew.OwnerTeam != teamB || rBnew.Targets[0].Endpoint != "b-backend.svc:9090" {
		t.Fatalf("team B self-upsert = %+v, want its own b-backend route", rBnew)
	}
	// Team A STILL unchanged after B's valid self-upsert (distinct namespaces).
	rA2, _ := svc.GetRoute(context.Background(), teamA, "fraud")
	if rA2.Targets[0].Endpoint != "a-backend.svc:9090" {
		t.Fatalf("team A route changed by team B self-upsert: %+v", rA2)
	}
}

// TestRouteTenancy_SetTrafficSplitForeignIsNoRoute: team B cannot reweight team A's
// route — SetTrafficSplit(teamB, "fraud", …) misses when B owns no "fraud" → it
// gets ErrNoRoute and cannot dial A's canary split.
func TestRouteTenancy_SetTrafficSplitForeignIsNoRoute(t *testing.T) {
	store := newMockRouteStore()
	svc := buildService(t, store, &mockLimiter{allow: true}, &mockBackend{}, &mockPublisher{}, &mockQuota{})
	deployForTeam(t, svc, teamA, "fraud", "vA", "a-backend.svc:9090")

	_, err := svc.SetTrafficSplit(context.Background(), teamB, "fraud", []domain.TrafficWeight{
		{Version: "vA", WeightBps: domain.TotalWeightBps},
	})
	if !errors.Is(err, domain.ErrNoRoute) {
		t.Fatalf("team B reweight of team A's route err = %v, want ErrNoRoute", err)
	}
}

// TestRouteTenancy_DeleteCannotDestroyForeign: the destruction half of the IDOR.
// team B's DeleteRoute("fraud") must be a no-op against team A's route — A's route
// must SURVIVE and keep serving. Then a same-team delete works as expected.
func TestRouteTenancy_DeleteCannotDestroyForeign(t *testing.T) {
	svc, _ := twoTenantService(t) // both teams own "fraud"

	// Team B deletes "fraud" — only B's own route should go.
	if err := svc.DeleteRoute(context.Background(), teamB, "fraud"); err != nil {
		t.Fatalf("team B DeleteRoute: %v", err)
	}

	// Team A's route MUST still exist and serve vA — B could not destroy it.
	rA, err := svc.GetRoute(context.Background(), teamA, "fraud")
	if err != nil {
		t.Fatalf("team A route destroyed by team B delete: %v", err)
	}
	if rA.Targets[0].Endpoint != "a-backend.svc:9090" {
		t.Fatalf("team A route altered by team B delete: %+v", rA)
	}
	outA, err := svc.Predict(context.Background(), domain.Principal{APIKeyID: "ka", Team: teamA}, basicInput("fraud"))
	if err != nil || outA.ServedVersion != "vA" {
		t.Fatalf("team A predict after team B delete: out=%+v err=%v, want vA served", outA, err)
	}

	// And team B's OWN route is now gone (same-team delete really worked).
	if _, err := svc.GetRoute(context.Background(), teamB, "fraud"); !errors.Is(err, domain.ErrNoRoute) {
		t.Fatalf("team B route after its own delete err = %v, want ErrNoRoute", err)
	}
}

// TestRouteTenancy_ListIsScopedToTeam: ListRoutes returns ONLY the caller's own
// routes — a team must never enumerate another team's models/endpoints.
func TestRouteTenancy_ListIsScopedToTeam(t *testing.T) {
	store := newMockRouteStore()
	svc := buildService(t, store, &mockLimiter{allow: true}, &mockBackend{}, &mockPublisher{}, &mockQuota{})
	deployForTeam(t, svc, teamA, "fraud", "vA", "a-backend.svc:9090")
	deployForTeam(t, svc, teamA, "spam", "vA", "a-spam.svc:9090")
	deployForTeam(t, svc, teamB, "fraud", "vB", "b-backend.svc:9090")

	listA, _, err := svc.ListRoutes(context.Background(), teamA, domain.ListOptions{})
	if err != nil {
		t.Fatalf("team A ListRoutes: %v", err)
	}
	if len(listA) != 2 {
		t.Fatalf("team A sees %d routes, want 2 (its own fraud+spam only)", len(listA))
	}
	for _, r := range listA {
		if r.OwnerTeam != teamA {
			t.Fatalf("team A list leaked a %s route: %+v", r.OwnerTeam, r)
		}
	}

	listB, _, err := svc.ListRoutes(context.Background(), teamB, domain.ListOptions{})
	if err != nil {
		t.Fatalf("team B ListRoutes: %v", err)
	}
	if len(listB) != 1 || listB[0].OwnerTeam != teamB {
		t.Fatalf("team B list = %+v, want exactly its own one route", listB)
	}
}

// TestRouteTenancy_EmptyTeamCannotReachTenantRoute: an unauthenticated / teamless
// principal (Team == "") must not be able to reach a real tenant's route by name —
// the empty-team namespace is disjoint from any real team's. This guards the
// defense-in-depth backstop (the handler rejects anonymous callers up front, but
// the domain must also not serve a "" caller a real route).
func TestRouteTenancy_EmptyTeamCannotReachTenantRoute(t *testing.T) {
	store := newMockRouteStore()
	svc := buildService(t, store, &mockLimiter{allow: true}, &mockBackend{}, &mockPublisher{}, &mockQuota{})
	deployForTeam(t, svc, teamA, "fraud", "vA", "a-backend.svc:9090")

	_, err := svc.Predict(context.Background(), domain.Principal{APIKeyID: "anon", Team: ""}, basicInput("fraud"))
	if !errors.Is(err, domain.ErrNoRoute) {
		t.Fatalf("empty-team predict err = %v, want ErrNoRoute (no reach into a real tenant)", err)
	}
}
