// serving_service_test.go — TDD specification for the model-runtime-registry
// service (the Sidecar + HPA pattern's domain core).
//
// ============================================================================
// WHY package domain_test (external test package)
// ============================================================================
//
// The tests construct hand-written FAKES of the domain ports (InferenceEngine,
// ModelFetcher, Clock) and drive the service through its EXPORTED surface only —
// exactly what the real callers (handler, event controller) depend on. Using the
// external `domain_test` package forces that discipline (no reaching into
// unexported fields) and matches the auth service's convention.
//
// These tests are written BEFORE serving_service_impl.go exists and assert REAL
// behavior, not mock-call counts:
//   - state transitions ACTUALLY happen (Downloading→Loading→Ready; the registry
//     reflects it via GetModelStatus),
//   - the readiness gate ACTUALLY blocks Predict until Ready,
//   - Predict ACTUALLY routes to the engine and returns its tensors,
//   - the inflight/latency/counter METRICS math is correct (we assert exact
//     values using a controllable fake Clock),
//   - the SSRF allow-list ACTUALLY rejects a disallowed URI before any fetch,
//   - idempotency ACTUALLY avoids a second fetch / a second engine call.
//
// ============================================================================
package domain_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/model-serving/internal/domain"
)

// ----------------------------------------------------------------------------
// FAKE PORTS (hand-written).
// ----------------------------------------------------------------------------

// fakeFetcher records each Fetch call and returns a canned local path + digest,
// or an injected error. Recording the calls lets a test PROVE idempotency (a
// second load did NOT fetch again) rather than guessing.
type fakeFetcher struct {
	mu       sync.Mutex
	calls    []string // URIs fetched, in order
	digest   string   // digest to return
	localPth string   // local path to return
	err      error    // if set, Fetch returns it (wrapped semantics tested separately)
}

func (f *fakeFetcher) Fetch(ctx context.Context, uri string) (string, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, uri)
	if f.err != nil {
		return "", "", f.err
	}
	return f.localPth, f.digest, nil
}

func (f *fakeFetcher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// blockingFetcher is a fetcher whose Fetch BLOCKS until the test releases it,
// and that signals (once) when a Fetch first begins. It lets a concurrency test
// pin the in-flight LEADER inside Fetch while followers pile up, so we can PROVE
// the singleflight guard dedups them (exactly one Fetch, one engine Load for N
// concurrent loads of the same Ref). Recording the call count is the load-bearing
// assertion — not a mock expectation but the real "how many fetches happened".
type blockingFetcher struct {
	mu       sync.Mutex
	calls    int
	digest   string
	localPth string

	enteredOnce sync.Once
	entered     chan struct{} // closed when the FIRST Fetch begins (leader confirmed)
	release     chan struct{} // Fetch returns once this is closed
}

func newBlockingFetcher(digest, localPath string) *blockingFetcher {
	return &blockingFetcher{
		digest:   digest,
		localPth: localPath,
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
	}
}

func (f *blockingFetcher) Fetch(ctx context.Context, uri string) (string, string, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	// Announce that a Fetch has begun (only the first close matters) so the test
	// knows the leader has claimed the in-flight slot before it launches followers.
	f.enteredOnce.Do(func() { close(f.entered) })
	<-f.release // hold here until the test releases (keeps the leader in flight)
	return f.localPth, f.digest, nil
}

func (f *blockingFetcher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

var _ domain.ModelFetcher = (*blockingFetcher)(nil)

// fakeEngine records Load/Predict/Unload calls and returns canned results. The
// predictFn lets a test customize the inference output (or inject a delay/error)
// per case.
type fakeEngine struct {
	mu sync.Mutex

	loadCalls    []domain.ModelRef
	predictCalls []domain.ModelRef
	unloadCalls  []domain.ModelRef

	session   domain.LoadedSession
	loadErr   error
	predictFn func(ref domain.ModelRef, inputs map[string]domain.Tensor) (map[string]domain.Tensor, error)
}

func (e *fakeEngine) Load(ctx context.Context, ref domain.ModelRef, localPath string) (domain.LoadedSession, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.loadCalls = append(e.loadCalls, ref)
	if e.loadErr != nil {
		return domain.LoadedSession{}, e.loadErr
	}
	return e.session, nil
}

func (e *fakeEngine) Predict(ctx context.Context, ref domain.ModelRef, inputs map[string]domain.Tensor) (map[string]domain.Tensor, error) {
	e.mu.Lock()
	e.predictCalls = append(e.predictCalls, ref)
	fn := e.predictFn
	e.mu.Unlock()
	if fn != nil {
		return fn(ref, inputs)
	}
	// Default: echo a single output tensor so tests can assert routing happened.
	return map[string]domain.Tensor{
		"output": {Shape: []int64{1, 1}, Data: []byte{1, 2, 3, 4}, DType: domain.DataTypeFloat32},
	}, nil
}

func (e *fakeEngine) Unload(ctx context.Context, ref domain.ModelRef) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.unloadCalls = append(e.unloadCalls, ref)
	return nil
}

func (e *fakeEngine) loadCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.loadCalls)
}
func (e *fakeEngine) predictCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.predictCalls)
}
func (e *fakeEngine) unloadCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.unloadCalls)
}

// fakeClock is a controllable clock. Now() returns the configured instant; each
// Since() call returns the next queued duration (so a test can make a Predict's
// measured latency a known, asserted value). This is how we PROVE the metrics
// math instead of tolerating "approximately".
type fakeClock struct {
	mu         sync.Mutex
	now        time.Time
	sinceQueue []time.Duration // consumed in order by Since()
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}
func (c *fakeClock) Since(t time.Time) time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.sinceQueue) == 0 {
		return 0
	}
	d := c.sinceQueue[0]
	c.sinceQueue = c.sinceQueue[1:]
	return d
}

// Compile-time proof the fakes satisfy the domain ports. If a port signature
// drifts, this fails to build — catching interface skew at test time.
var (
	_ domain.ModelFetcher    = (*fakeFetcher)(nil)
	_ domain.InferenceEngine = (*fakeEngine)(nil)
	_ domain.Clock           = (*fakeClock)(nil)
)

// ----------------------------------------------------------------------------
// Test harness builder.
// ----------------------------------------------------------------------------

// newTestService wires the service with the given fakes and a sane default
// config (allow-list permitting the test bucket, a 1s liveness threshold).
func newTestService(t *testing.T, eng *fakeEngine, fetch *fakeFetcher, clk *fakeClock) domain.ServingService {
	t.Helper()
	return domain.NewServingService(domain.ServiceConfig{
		Engine:               eng,
		Fetcher:              fetch,
		Clock:                clk,
		AllowedArtifactURIs:  []string{"s3://fp-models/"},
		LivenessLatencyMax:   time.Second,
		MaxInputTensors:      64,
		MaxTensorBytes:       16 << 20, // 16 MiB
		DefaultPageSize:      20,
		MaxPageSize:          100,
		IdempotencyCacheSize: 128,
	})
}

func mustLoadReady(t *testing.T, svc domain.ServingService, eng *fakeEngine) domain.ModelRef {
	t.Helper()
	ref := domain.ModelRef{Name: "iris", Version: "v1"}
	eng.session = domain.LoadedSession{
		InputSchema:  []domain.TensorSpec{{Name: "input", Shape: []int64{-1, 4}, DType: domain.DataTypeFloat32}},
		OutputSchema: []domain.TensorSpec{{Name: "output", Shape: []int64{-1, 1}, DType: domain.DataTypeFloat32}},
		MemoryBytes:  4096,
		Digest:       "sha256:abc",
	}
	st, err := svc.LoadModel(context.Background(), domain.LoadModelInput{
		Ref:         ref,
		ArtifactURI: "s3://fp-models/iris/v1.onnx",
	})
	if err != nil {
		t.Fatalf("LoadModel returned error: %v", err)
	}
	if st.State != domain.StateReady {
		t.Fatalf("expected StateReady after load, got %v (msg=%q)", st.State, st.Message)
	}
	return ref
}

// validInput builds a float32 [1,4] tensor whose byte length matches the layout.
func validFloat32Input() map[string]domain.Tensor {
	return map[string]domain.Tensor{
		"input": {Shape: []int64{1, 4}, Data: make([]byte, 4*4), DType: domain.DataTypeFloat32},
	}
}

// ============================================================================
// LOAD lifecycle + readiness
// ============================================================================

func TestLoadModel_DrivesStateToReady(t *testing.T) {
	eng := &fakeEngine{}
	fetch := &fakeFetcher{digest: "sha256:abc", localPth: "/tmp/iris.onnx"}
	clk := &fakeClock{now: time.Unix(1000, 0)}
	svc := newTestService(t, eng, fetch, clk)

	ref := mustLoadReady(t, svc, eng)

	// The engine must have been asked to load exactly once, and the registry must
	// now report the model READY via GetModelStatus (real state, not a mock flag).
	if eng.loadCount() != 1 {
		t.Fatalf("expected 1 engine Load call, got %d", eng.loadCount())
	}
	st, err := svc.GetModelStatus(context.Background(), ref)
	if err != nil {
		t.Fatalf("GetModelStatus: %v", err)
	}
	if st.State != domain.StateReady {
		t.Fatalf("status state = %v, want READY", st.State)
	}
	// LoadedAt/UpdatedAt must be stamped from the clock (deterministic).
	info, err := svc.GetModelInfo(context.Background(), ref)
	if err != nil {
		t.Fatalf("GetModelInfo: %v", err)
	}
	if !info.LoadedAt.Equal(time.Unix(1000, 0)) {
		t.Fatalf("LoadedAt = %v, want clock now", info.LoadedAt)
	}
	if info.ArtifactDigest != "sha256:abc" {
		t.Fatalf("ArtifactDigest = %q, want sha256:abc", info.ArtifactDigest)
	}
	if len(info.InputSchema) != 1 || info.InputSchema[0].Name != "input" {
		t.Fatalf("input schema not recorded from engine: %+v", info.InputSchema)
	}
}

func TestLoadModel_RejectsDisallowedURI_NoFetch(t *testing.T) {
	eng := &fakeEngine{}
	fetch := &fakeFetcher{digest: "sha256:abc"}
	clk := &fakeClock{now: time.Unix(1000, 0)}
	svc := newTestService(t, eng, fetch, clk)

	// A URI outside the allow-list ("s3://fp-models/") must be rejected BEFORE
	// any fetch — the SSRF guard. We assert both the error AND that the fetcher
	// was never called (the guard runs in the domain, not the adapter).
	_, err := svc.LoadModel(context.Background(), domain.LoadModelInput{
		Ref:         domain.ModelRef{Name: "evil", Version: "v1"},
		ArtifactURI: "s3://attacker-bucket/payload.onnx",
	})
	if !errors.Is(err, domain.ErrArtifactURINotAllowed) {
		t.Fatalf("expected ErrArtifactURINotAllowed, got %v", err)
	}
	if fetch.callCount() != 0 {
		t.Fatalf("fetcher was called %d times for a disallowed URI; SSRF guard must run first", fetch.callCount())
	}
	if eng.loadCount() != 0 {
		t.Fatalf("engine was called for a disallowed URI")
	}
}

func TestLoadModel_DigestMismatch_TransitionsToFailed(t *testing.T) {
	eng := &fakeEngine{session: domain.LoadedSession{Digest: "sha256:ACTUAL"}}
	fetch := &fakeFetcher{digest: "sha256:ACTUAL", localPth: "/tmp/x"}
	clk := &fakeClock{now: time.Unix(1000, 0)}
	svc := newTestService(t, eng, fetch, clk)

	// We expect "sha256:EXPECTED" but the artifact's digest is "sha256:ACTUAL".
	// The load must end in StateFailed with the mismatch reason — it must NOT be
	// Ready (serving unverified weights is a supply-chain failure).
	ref := domain.ModelRef{Name: "iris", Version: "v1"}
	st, err := svc.LoadModel(context.Background(), domain.LoadModelInput{
		Ref:            ref,
		ArtifactURI:    "s3://fp-models/iris/v1.onnx",
		ExpectedDigest: "sha256:EXPECTED",
	})
	// A load FAILURE is reported via state, not a returned error (async-loader
	// contract). The returned status must be Failed.
	if err != nil {
		t.Fatalf("LoadModel returned a hard error; digest mismatch should surface as StateFailed: %v", err)
	}
	if st.State != domain.StateFailed {
		t.Fatalf("state = %v, want FAILED on digest mismatch", st.State)
	}
	// Predict against a failed model must be blocked.
	_, perr := svc.Predict(context.Background(), domain.PredictInput{Inputs: validFloat32Input()})
	if !errors.Is(perr, domain.ErrModelNotReady) {
		t.Fatalf("Predict on FAILED model = %v, want ErrModelNotReady", perr)
	}
}

func TestLoadModel_FetchNotFound_TransitionsToFailed(t *testing.T) {
	eng := &fakeEngine{}
	fetch := &fakeFetcher{err: domain.ErrArtifactNotFound}
	clk := &fakeClock{now: time.Unix(1000, 0)}
	svc := newTestService(t, eng, fetch, clk)

	st, err := svc.LoadModel(context.Background(), domain.LoadModelInput{
		Ref:         domain.ModelRef{Name: "iris", Version: "v1"},
		ArtifactURI: "s3://fp-models/iris/v1.onnx",
	})
	if err != nil {
		t.Fatalf("LoadModel hard error: %v", err)
	}
	if st.State != domain.StateFailed {
		t.Fatalf("state = %v, want FAILED when artifact missing", st.State)
	}
	if eng.loadCount() != 0 {
		t.Fatalf("engine Load must not run when the fetch failed")
	}
}

func TestLoadModel_Idempotent_SameRefAndDigest_NoSecondFetch(t *testing.T) {
	eng := &fakeEngine{session: domain.LoadedSession{Digest: "sha256:abc"}}
	fetch := &fakeFetcher{digest: "sha256:abc", localPth: "/tmp/x"}
	clk := &fakeClock{now: time.Unix(1000, 0)}
	svc := newTestService(t, eng, fetch, clk)

	in := domain.LoadModelInput{
		Ref:            domain.ModelRef{Name: "iris", Version: "v1"},
		ArtifactURI:    "s3://fp-models/iris/v1.onnx",
		ExpectedDigest: "sha256:abc",
	}
	if _, err := svc.LoadModel(context.Background(), in); err != nil {
		t.Fatalf("first load: %v", err)
	}
	// Re-load the SAME ready model: must be a no-op (no second fetch, no second
	// engine load) returning the current Ready status.
	st, err := svc.LoadModel(context.Background(), in)
	if err != nil {
		t.Fatalf("second load: %v", err)
	}
	if st.State != domain.StateReady {
		t.Fatalf("idempotent reload state = %v, want READY", st.State)
	}
	if fetch.callCount() != 1 {
		t.Fatalf("idempotent reload fetched %d times, want 1", fetch.callCount())
	}
	if eng.loadCount() != 1 {
		t.Fatalf("idempotent reload engine-loaded %d times, want 1", eng.loadCount())
	}
}

func TestLoadModel_DifferentArtifactSameRef_Rejected(t *testing.T) {
	eng := &fakeEngine{session: domain.LoadedSession{Digest: "sha256:abc"}}
	fetch := &fakeFetcher{digest: "sha256:abc", localPth: "/tmp/x"}
	clk := &fakeClock{now: time.Unix(1000, 0)}
	svc := newTestService(t, eng, fetch, clk)

	ref := domain.ModelRef{Name: "iris", Version: "v1"}
	if _, err := svc.LoadModel(context.Background(), domain.LoadModelInput{
		Ref: ref, ArtifactURI: "s3://fp-models/iris/v1.onnx", ExpectedDigest: "sha256:abc",
	}); err != nil {
		t.Fatalf("first load: %v", err)
	}
	// A second load under the SAME ref but a DIFFERENT expected digest must be
	// rejected: a (name, version) maps to exactly one artifact for the pod's life.
	_, err := svc.LoadModel(context.Background(), domain.LoadModelInput{
		Ref: ref, ArtifactURI: "s3://fp-models/iris/v1.onnx", ExpectedDigest: "sha256:DIFFERENT",
	})
	if !errors.Is(err, domain.ErrModelAlreadyExists) {
		t.Fatalf("expected ErrModelAlreadyExists for a different artifact under same ref, got %v", err)
	}
}

func TestLoadModel_ValidationErrors(t *testing.T) {
	svc := newTestService(t, &fakeEngine{}, &fakeFetcher{}, &fakeClock{now: time.Unix(1, 0)})
	cases := []struct {
		name string
		in   domain.LoadModelInput
	}{
		{"empty name", domain.LoadModelInput{Ref: domain.ModelRef{Version: "v1"}, ArtifactURI: "s3://fp-models/x"}},
		{"empty version", domain.LoadModelInput{Ref: domain.ModelRef{Name: "x"}, ArtifactURI: "s3://fp-models/x"}},
		{"empty uri", domain.LoadModelInput{Ref: domain.ModelRef{Name: "x", Version: "v1"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := svc.LoadModel(context.Background(), tc.in); !errors.Is(err, domain.ErrValidation) {
				t.Fatalf("want ErrValidation, got %v", err)
			}
		})
	}
}

// ============================================================================
// PREDICT routing + readiness gate
// ============================================================================

func TestPredict_RoutesToEngine_WhenReady(t *testing.T) {
	eng := &fakeEngine{}
	fetch := &fakeFetcher{digest: "sha256:abc"}
	clk := &fakeClock{now: time.Unix(1000, 0), sinceQueue: []time.Duration{7 * time.Millisecond}}
	svc := newTestService(t, eng, fetch, clk)
	ref := mustLoadReady(t, svc, eng)

	// The engine returns a specific output; the result must carry it through and
	// echo the version + correlation id + measured latency.
	eng.predictFn = func(_ domain.ModelRef, _ map[string]domain.Tensor) (map[string]domain.Tensor, error) {
		return map[string]domain.Tensor{"output": {Shape: []int64{1, 1}, Data: []byte{9, 9, 9, 9}, DType: domain.DataTypeFloat32}}, nil
	}
	res, err := svc.Predict(context.Background(), domain.PredictInput{
		RequestedRef:  ref,
		Inputs:        validFloat32Input(),
		CorrelationID: "corr-1",
	})
	if err != nil {
		t.Fatalf("Predict: %v", err)
	}
	if eng.predictCount() != 1 {
		t.Fatalf("engine Predict called %d times, want 1", eng.predictCount())
	}
	out, ok := res.Outputs["output"]
	if !ok || string(out.Data) != string([]byte{9, 9, 9, 9}) {
		t.Fatalf("output not routed from engine: %+v", res.Outputs)
	}
	if res.ModelVersion != "v1" {
		t.Fatalf("ModelVersion = %q, want v1", res.ModelVersion)
	}
	if res.CorrelationID != "corr-1" {
		t.Fatalf("CorrelationID = %q, want corr-1", res.CorrelationID)
	}
	if res.InferenceLatency != 7*time.Millisecond {
		t.Fatalf("InferenceLatency = %v, want 7ms (from fake clock)", res.InferenceLatency)
	}
	if res.FromCache {
		t.Fatalf("first call must not be FromCache")
	}
}

func TestPredict_BlockedUntilReady(t *testing.T) {
	// No model loaded at all → ErrModelNotReady (nothing resident to serve).
	svc := newTestService(t, &fakeEngine{}, &fakeFetcher{}, &fakeClock{now: time.Unix(1, 0)})
	_, err := svc.Predict(context.Background(), domain.PredictInput{Inputs: validFloat32Input()})
	if !errors.Is(err, domain.ErrModelNotReady) {
		t.Fatalf("Predict with no model = %v, want ErrModelNotReady", err)
	}
}

func TestPredict_RequestMismatch_Rejected(t *testing.T) {
	eng := &fakeEngine{}
	svc := newTestService(t, eng, &fakeFetcher{digest: "sha256:abc"}, &fakeClock{now: time.Unix(1, 0), sinceQueue: []time.Duration{1}})
	mustLoadReady(t, svc, eng) // resident model is iris/v1

	// Request explicitly targets a DIFFERENT model than the pod serves → reject
	// (defense in depth against a gateway routing bug), and the engine must NOT
	// be called.
	_, err := svc.Predict(context.Background(), domain.PredictInput{
		RequestedRef: domain.ModelRef{Name: "fraud", Version: "v9"},
		Inputs:       validFloat32Input(),
	})
	if !errors.Is(err, domain.ErrModelRequestMismatch) {
		t.Fatalf("Predict mismatch = %v, want ErrModelRequestMismatch", err)
	}
	if eng.predictCount() != 0 {
		t.Fatalf("engine must not run on a request mismatch")
	}
}

func TestPredict_TooManyInputs_Rejected(t *testing.T) {
	eng := &fakeEngine{}
	svc := newTestService(t, eng, &fakeFetcher{digest: "sha256:abc"}, &fakeClock{now: time.Unix(1, 0), sinceQueue: []time.Duration{1}})
	mustLoadReady(t, svc, eng)

	// 65 inputs > the 64 cap → reject (DoS guard) before the engine runs.
	inputs := make(map[string]domain.Tensor, 65)
	for i := 0; i < 65; i++ {
		inputs["t"+itoa(i)] = domain.Tensor{Shape: []int64{1}, Data: make([]byte, 4), DType: domain.DataTypeFloat32}
	}
	_, err := svc.Predict(context.Background(), domain.PredictInput{Inputs: inputs})
	if !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("too many inputs = %v, want ErrValidation", err)
	}
	if eng.predictCount() != 0 {
		t.Fatalf("engine must not run when input cap exceeded")
	}
}

func TestPredict_BadTensorLayout_Rejected(t *testing.T) {
	eng := &fakeEngine{}
	svc := newTestService(t, eng, &fakeFetcher{digest: "sha256:abc"}, &fakeClock{now: time.Unix(1, 0), sinceQueue: []time.Duration{1}})
	mustLoadReady(t, svc, eng)

	// shape [1,4] float32 demands 16 bytes; supplying 8 must be rejected (the
	// overflow-safe layout guard) before the engine sees it.
	bad := map[string]domain.Tensor{
		"input": {Shape: []int64{1, 4}, Data: make([]byte, 8), DType: domain.DataTypeFloat32},
	}
	_, err := svc.Predict(context.Background(), domain.PredictInput{Inputs: bad})
	if !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("bad layout = %v, want ErrValidation", err)
	}
	if eng.predictCount() != 0 {
		t.Fatalf("engine must not run on a bad tensor layout")
	}
}

func TestPredict_EngineError_CountedAsFailure(t *testing.T) {
	eng := &fakeEngine{}
	clk := &fakeClock{now: time.Unix(1, 0), sinceQueue: []time.Duration{3 * time.Millisecond}}
	svc := newTestService(t, eng, &fakeFetcher{digest: "sha256:abc"}, clk)
	mustLoadReady(t, svc, eng)

	eng.predictFn = func(_ domain.ModelRef, _ map[string]domain.Tensor) (map[string]domain.Tensor, error) {
		return nil, domain.ErrEngineBadInput
	}
	_, err := svc.Predict(context.Background(), domain.PredictInput{Inputs: validFloat32Input()})
	if !errors.Is(err, domain.ErrInferenceFailed) {
		t.Fatalf("engine error = %v, want wrapped ErrInferenceFailed", err)
	}
	// The failure must be reflected in metrics (RED errors) AND inflight must
	// have returned to zero even on the error path.
	m := svc.GetServingMetrics(context.Background())
	if m.FailedRequests != 1 {
		t.Fatalf("FailedRequests = %d, want 1", m.FailedRequests)
	}
	if m.InflightRequests != 0 {
		t.Fatalf("InflightRequests = %d after error, want 0 (must decrement on all paths)", m.InflightRequests)
	}
}

// ============================================================================
// IDEMPOTENCY CACHE (the result cache that makes retries cheap)
// ============================================================================

func TestPredict_IdempotencyKey_ReturnsCachedResult(t *testing.T) {
	eng := &fakeEngine{}
	clk := &fakeClock{now: time.Unix(1, 0), sinceQueue: []time.Duration{5 * time.Millisecond, 99 * time.Millisecond}}
	svc := newTestService(t, eng, &fakeFetcher{digest: "sha256:abc"}, clk)
	ref := mustLoadReady(t, svc, eng)

	calls := 0
	eng.predictFn = func(_ domain.ModelRef, _ map[string]domain.Tensor) (map[string]domain.Tensor, error) {
		calls++
		return map[string]domain.Tensor{"output": {Shape: []int64{1, 1}, Data: []byte{byte(calls)}, DType: domain.DataTypeInt32}}, nil
	}
	first, err := svc.Predict(context.Background(), domain.PredictInput{RequestedRef: ref, Inputs: validFloat32Input(), IdempotencyKey: "k1"})
	if err != nil {
		t.Fatalf("first predict: %v", err)
	}
	second, err := svc.Predict(context.Background(), domain.PredictInput{RequestedRef: ref, Inputs: validFloat32Input(), IdempotencyKey: "k1"})
	if err != nil {
		t.Fatalf("second predict: %v", err)
	}
	// The engine must have run ONCE; the second call is a cache hit returning the
	// identical bytes, flagged FromCache.
	if calls != 1 {
		t.Fatalf("engine ran %d times for a repeated idempotency key, want 1", calls)
	}
	if !second.FromCache {
		t.Fatalf("second call must be FromCache")
	}
	if string(second.Outputs["output"].Data) != string(first.Outputs["output"].Data) {
		t.Fatalf("cached result differs from original: %v vs %v", second.Outputs, first.Outputs)
	}
}

// floatInputBytes builds a layout-valid float32 [1,4] tensor with the given 16
// raw bytes, so a test can send the SAME idempotency key with DIFFERENT inputs.
func floatInputBytes(b [16]byte) map[string]domain.Tensor {
	data := make([]byte, 16)
	copy(data, b[:])
	return map[string]domain.Tensor{
		"input": {Shape: []int64{1, 4}, Data: data, DType: domain.DataTypeFloat32},
	}
}

// TestPredict_IdempotencyKey_DifferentInputs_DoesNotReturnStaleResult is the
// regression test for the cache-correctness finding. The OLD cache keyed PURELY
// on the idempotency key: a second Predict with the SAME key but DIFFERENT inputs
// returned the FIRST input's output flagged FromCache — a silent WRONG
// prediction (cross-request data confusion on a multi-tenant gateway).
//
// The fix binds the cache identity to a fingerprint of the inputs. We PROVE it by
// asserting REAL behavior, not a mock flag:
//   - same key + different inputs ⇒ the engine RE-RUNS (a miss, not a stale hit),
//   - the returned outputs are the ones computed FOR THE SECOND INPUTS,
//   - the result is NOT flagged FromCache,
//   - and a subsequent retry of the SECOND inputs under the same key DOES hit
//     (the recomputed result correctly replaced the stale entry).
func TestPredict_IdempotencyKey_DifferentInputs_DoesNotReturnStaleResult(t *testing.T) {
	eng := &fakeEngine{}
	// Four Since durations: two distinct-input predicts + one genuine retry. The
	// fourth is spare (the genuine retry is a cache hit and consumes no Since).
	clk := &fakeClock{now: time.Unix(1, 0), sinceQueue: []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond, time.Millisecond}}
	svc := newTestService(t, eng, &fakeFetcher{digest: "sha256:abc"}, clk)
	ref := mustLoadReady(t, svc, eng)

	// The engine echoes the FIRST input byte as the output, so different inputs
	// produce visibly different outputs — that's how we catch a stale hit.
	eng.predictFn = func(_ domain.ModelRef, inputs map[string]domain.Tensor) (map[string]domain.Tensor, error) {
		first := inputs["input"].Data[0]
		return map[string]domain.Tensor{"output": {Shape: []int64{1, 1}, Data: []byte{first}, DType: domain.DataTypeInt32}}, nil
	}

	inputsA := floatInputBytes([16]byte{0xAA}) // first byte 0xAA
	inputsB := floatInputBytes([16]byte{0xBB}) // first byte 0xBB

	// 1) Predict with key "samekey" + inputs A.
	rA, err := svc.Predict(context.Background(), domain.PredictInput{RequestedRef: ref, Inputs: inputsA, IdempotencyKey: "samekey"})
	if err != nil {
		t.Fatalf("predict A: %v", err)
	}
	if got := rA.Outputs["output"].Data[0]; got != 0xAA {
		t.Fatalf("predict A output = %#x, want 0xAA", got)
	}

	// 2) Predict with the SAME key "samekey" but DIFFERENT inputs B. This MUST NOT
	// return A's cached output. It must recompute and return B's output.
	rB, err := svc.Predict(context.Background(), domain.PredictInput{RequestedRef: ref, Inputs: inputsB, IdempotencyKey: "samekey"})
	if err != nil {
		t.Fatalf("predict B: %v", err)
	}
	if rB.FromCache {
		t.Fatalf("key reused with DIFFERENT inputs must be a cache MISS, got FromCache=true (stale-result bug)")
	}
	if got := rB.Outputs["output"].Data[0]; got != 0xBB {
		t.Fatalf("predict B output = %#x, want 0xBB (got A's stale result if 0xAA)", got)
	}
	// The engine ran for BOTH distinct inputs (no stale hit short-circuited B).
	if eng.predictCount() != 2 {
		t.Fatalf("engine ran %d times, want 2 (A computed, B recomputed — not a stale hit)", eng.predictCount())
	}

	// 3) A genuine retry of inputs B under the same key NOW hits (the recompute
	// replaced the stale entry with B's fingerprint). Proves the cache still
	// optimizes real retries after a key-reuse miss.
	rB2, err := svc.Predict(context.Background(), domain.PredictInput{RequestedRef: ref, Inputs: inputsB, IdempotencyKey: "samekey"})
	if err != nil {
		t.Fatalf("predict B retry: %v", err)
	}
	if !rB2.FromCache {
		t.Fatalf("genuine retry of identical inputs must be FromCache, got false")
	}
	if got := rB2.Outputs["output"].Data[0]; got != 0xBB {
		t.Fatalf("retried B output = %#x, want 0xBB", got)
	}
	if eng.predictCount() != 2 {
		t.Fatalf("engine ran %d times after a genuine retry, want still 2 (retry served from cache)", eng.predictCount())
	}
}

// ============================================================================
// METRICS math (the HPA signal)
// ============================================================================

func TestMetrics_InflightAndCountersAndLatency(t *testing.T) {
	eng := &fakeEngine{}
	// Two successful predicts measure 10ms then 20ms.
	clk := &fakeClock{now: time.Unix(1, 0), sinceQueue: []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}}
	svc := newTestService(t, eng, &fakeFetcher{digest: "sha256:abc"}, clk)
	ref := mustLoadReady(t, svc, eng)

	for i := 0; i < 2; i++ {
		if _, err := svc.Predict(context.Background(), domain.PredictInput{RequestedRef: ref, Inputs: validFloat32Input()}); err != nil {
			t.Fatalf("predict %d: %v", i, err)
		}
	}
	m := svc.GetServingMetrics(context.Background())
	if m.TotalRequests != 2 {
		t.Fatalf("TotalRequests = %d, want 2", m.TotalRequests)
	}
	if m.FailedRequests != 0 {
		t.Fatalf("FailedRequests = %d, want 0", m.FailedRequests)
	}
	if m.InflightRequests != 0 {
		t.Fatalf("InflightRequests = %d after completion, want 0", m.InflightRequests)
	}
	if m.LastInferenceLatency != 20*time.Millisecond {
		t.Fatalf("LastInferenceLatency = %v, want 20ms", m.LastInferenceLatency)
	}
	// Memory is reported from the loaded session.
	if m.ModelMemoryBytes != 4096 {
		t.Fatalf("ModelMemoryBytes = %d, want 4096", m.ModelMemoryBytes)
	}
}

func TestMetrics_InflightObservedDuringInference(t *testing.T) {
	eng := &fakeEngine{}
	clk := &fakeClock{now: time.Unix(1, 0), sinceQueue: []time.Duration{1 * time.Millisecond}}
	svc := newTestService(t, eng, &fakeFetcher{digest: "sha256:abc"}, clk)
	ref := mustLoadReady(t, svc, eng)

	// The engine callback observes inflight WHILE inside Predict — it must be 1
	// (the in-progress call), proving inflight is incremented BEFORE the engine
	// runs (the HPA must see queue depth during the call, not only after).
	var observedInflight int32
	eng.predictFn = func(_ domain.ModelRef, _ map[string]domain.Tensor) (map[string]domain.Tensor, error) {
		observedInflight = svc.GetServingMetrics(context.Background()).InflightRequests
		return map[string]domain.Tensor{"output": {Shape: []int64{1}, Data: []byte{0}, DType: domain.DataTypeBool}}, nil
	}
	if _, err := svc.Predict(context.Background(), domain.PredictInput{RequestedRef: ref, Inputs: validFloat32Input()}); err != nil {
		t.Fatalf("predict: %v", err)
	}
	if observedInflight != 1 {
		t.Fatalf("inflight during inference = %d, want 1", observedInflight)
	}
	if got := svc.GetServingMetrics(context.Background()).InflightRequests; got != 0 {
		t.Fatalf("inflight after inference = %d, want 0", got)
	}
}

func TestMetrics_ConcurrentPredicts_RaceFree(t *testing.T) {
	eng := &fakeEngine{}
	// Provide plenty of Since durations; order doesn't matter for the counter check.
	clk := &fakeClock{now: time.Unix(1, 0)}
	for i := 0; i < 200; i++ {
		clk.sinceQueue = append(clk.sinceQueue, time.Millisecond)
	}
	svc := newTestService(t, eng, &fakeFetcher{digest: "sha256:abc"}, clk)
	ref := mustLoadReady(t, svc, eng)

	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_, _ = svc.Predict(context.Background(), domain.PredictInput{RequestedRef: ref, Inputs: validFloat32Input()})
		}()
	}
	wg.Wait()
	m := svc.GetServingMetrics(context.Background())
	if m.TotalRequests != n {
		t.Fatalf("TotalRequests = %d, want %d (counter must be race-free under -race)", m.TotalRequests, n)
	}
	if m.InflightRequests != 0 {
		t.Fatalf("InflightRequests = %d, want 0 after all complete", m.InflightRequests)
	}
}

// ============================================================================
// HEALTH (readiness/liveness split)
// ============================================================================

func TestHealthCheck_ServingOnlyWhenReadyAndFast(t *testing.T) {
	eng := &fakeEngine{}
	clk := &fakeClock{now: time.Unix(1, 0), sinceQueue: []time.Duration{10 * time.Millisecond}}
	svc := newTestService(t, eng, &fakeFetcher{digest: "sha256:abc"}, clk)

	// Before any load: NOT serving (no ready model).
	if v, _, _ := svc.HealthCheck(context.Background(), ""); v != domain.HealthNotServing {
		t.Fatalf("health before load = %v, want NotServing", v)
	}
	ref := mustLoadReady(t, svc, eng)
	// Ready and no slow inference yet → Serving.
	if v, st, _ := svc.HealthCheck(context.Background(), ""); v != domain.HealthServing || st != domain.StateReady {
		t.Fatalf("health after ready = %v state=%v, want Serving/Ready", v, st)
	}
	// Drive a slow inference (1.5s > 1s threshold) → liveness signal degraded →
	// NotServing even though the model is still Ready.
	clk.sinceQueue = []time.Duration{1500 * time.Millisecond}
	if _, err := svc.Predict(context.Background(), domain.PredictInput{RequestedRef: ref, Inputs: validFloat32Input()}); err != nil {
		t.Fatalf("predict: %v", err)
	}
	v, st, last := svc.HealthCheck(context.Background(), "")
	if v != domain.HealthNotServing {
		t.Fatalf("health after slow inference = %v, want NotServing", v)
	}
	if st != domain.StateReady {
		t.Fatalf("state should still be Ready (latency degraded, not unloaded), got %v", st)
	}
	if last != 1500*time.Millisecond {
		t.Fatalf("last latency = %v, want 1.5s", last)
	}
}

// ============================================================================
// UNLOAD + EVENT REACTIONS (EnsureLoaded / Unload)
// ============================================================================

func TestUnloadModel_FreesAndBlocksPredict(t *testing.T) {
	eng := &fakeEngine{}
	svc := newTestService(t, eng, &fakeFetcher{digest: "sha256:abc"}, &fakeClock{now: time.Unix(1, 0), sinceQueue: []time.Duration{1}})
	ref := mustLoadReady(t, svc, eng)

	st, err := svc.UnloadModel(context.Background(), ref, "undeployed")
	if err != nil {
		t.Fatalf("UnloadModel: %v", err)
	}
	if st.State != domain.StateUnloaded {
		t.Fatalf("state = %v, want UNLOADED", st.State)
	}
	if eng.unloadCount() != 1 {
		t.Fatalf("engine Unload called %d times, want 1", eng.unloadCount())
	}
	_, perr := svc.Predict(context.Background(), domain.PredictInput{RequestedRef: ref, Inputs: validFloat32Input()})
	if !errors.Is(perr, domain.ErrModelNotReady) {
		t.Fatalf("Predict after unload = %v, want ErrModelNotReady", perr)
	}
}

func TestUnloadModel_Idempotent_UnknownModel(t *testing.T) {
	eng := &fakeEngine{}
	svc := newTestService(t, eng, &fakeFetcher{}, &fakeClock{now: time.Unix(1, 0)})
	// Unloading a model that was never loaded must NOT error (controller
	// re-reconcile after archive) — it returns a synthesized Unloaded status.
	st, err := svc.UnloadModel(context.Background(), domain.ModelRef{Name: "ghost", Version: "v1"}, "archived")
	if err != nil {
		t.Fatalf("idempotent unload of unknown model errored: %v", err)
	}
	if st.State != domain.StateUnloaded {
		t.Fatalf("state = %v, want UNLOADED for unknown-model unload", st.State)
	}
}

func TestEnsureLoaded_IsIdempotentReconcile(t *testing.T) {
	eng := &fakeEngine{session: domain.LoadedSession{Digest: "sha256:abc"}}
	fetch := &fakeFetcher{digest: "sha256:abc", localPth: "/tmp/x"}
	svc := newTestService(t, eng, fetch, &fakeClock{now: time.Unix(1, 0)})

	in := domain.LoadModelInput{
		Ref:            domain.ModelRef{Name: "iris", Version: "v1"},
		ArtifactURI:    "s3://fp-models/iris/v1.onnx",
		ExpectedDigest: "sha256:abc",
	}
	// First EnsureLoaded (reaction to ModelDeployed) loads it.
	if st, err := svc.EnsureLoaded(context.Background(), in); err != nil || st.State != domain.StateReady {
		t.Fatalf("EnsureLoaded first = (%v,%v), want Ready/nil", st.State, err)
	}
	// Second EnsureLoaded (a duplicate/redelivered event) is a no-op.
	if st, err := svc.EnsureLoaded(context.Background(), in); err != nil || st.State != domain.StateReady {
		t.Fatalf("EnsureLoaded second = (%v,%v), want Ready/nil", st.State, err)
	}
	if fetch.callCount() != 1 || eng.loadCount() != 1 {
		t.Fatalf("EnsureLoaded not idempotent: fetches=%d loads=%d, want 1/1", fetch.callCount(), eng.loadCount())
	}
}

// TestLoadModel_ConcurrentSameRef_DedupsToOneFetchAndLoad is the regression test
// for the in-flight-load finding. The OLD code released the write lock after
// transitioning to Downloading and re-acquired it only at the final commit, so N
// concurrent LoadModel calls for the same Ref each passed the conflict check
// (none was Ready yet) and ALL fetched + ALL engine-loaded — wasting N× the
// cold-start I/O and ONNX init, and breaking EnsureLoaded's "dedup an in-flight
// load" promise.
//
// We PROVE the fix with a blocking fetcher that pins the LEADER inside Fetch
// while four FOLLOWERS pile on. The load-bearing assertions are REAL counts:
//   - exactly ONE Fetch and ONE engine Load happened for FIVE concurrent loads,
//   - every one of the five callers got the SAME Ready status (the leader's
//     result, fanned out to the followers),
//   - no caller errored.
//
// Run under -race, this also proves the follower fan-out (status/err read after
// <-done) has no data race with the leader's write.
func TestLoadModel_ConcurrentSameRef_DedupsToOneFetchAndLoad(t *testing.T) {
	eng := &fakeEngine{session: domain.LoadedSession{
		MemoryBytes: 4096,
		Digest:      "sha256:abc",
	}}
	fetch := newBlockingFetcher("sha256:abc", "/tmp/iris.onnx")
	clk := &fakeClock{now: time.Unix(1000, 0)}
	svc := domain.NewServingService(domain.ServiceConfig{
		Engine:              eng,
		Fetcher:             fetch,
		Clock:               clk,
		AllowedArtifactURIs: []string{"s3://fp-models/"},
		MaxInputTensors:     64,
		MaxTensorBytes:      16 << 20,
	})

	ref := domain.ModelRef{Name: "iris", Version: "v1"}
	in := domain.LoadModelInput{Ref: ref, ArtifactURI: "s3://fp-models/iris/v1.onnx", ExpectedDigest: "sha256:abc"}

	const n = 5
	results := make([]domain.ModelStatus, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)

	// Goroutine 0 is the LEADER: start it and wait until it is inside Fetch (the
	// in-flight slot is now installed) before launching the followers, so the
	// followers are GUARANTEED to observe the in-flight load rather than racing to
	// become a second leader.
	go func() {
		defer wg.Done()
		results[0], errs[0] = svc.LoadModel(context.Background(), in)
	}()
	<-fetch.entered // leader is now blocked in Fetch with the slot claimed

	// Launch the four followers. Each marks `launched` the instant it starts; we
	// wait for all four, then give them a beat to reach the <-done park before
	// releasing the leader. (If any follower instead started a second fetch — the
	// bug — the callCount assertion below would catch it as 2+.)
	var launched sync.WaitGroup
	launched.Add(n - 1)
	for i := 1; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			launched.Done()
			results[idx], errs[idx] = svc.LoadModel(context.Background(), in)
		}(i)
	}
	launched.Wait()
	time.Sleep(20 * time.Millisecond) // let the followers reach <-inflight.done

	// Release the leader; it completes the one load and fans the result out.
	close(fetch.release)
	wg.Wait()

	// THE PROOF: one fetch, one engine load for five concurrent loads.
	if fetch.callCount() != 1 {
		t.Fatalf("Fetch called %d times for %d concurrent loads of the same Ref, want 1 (singleflight dedup)", fetch.callCount(), n)
	}
	if eng.loadCount() != 1 {
		t.Fatalf("engine Load called %d times, want 1 (followers must not re-init)", eng.loadCount())
	}
	// Every caller (leader + followers) got the SAME Ready result, no errors.
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("caller %d errored: %v", i, errs[i])
		}
		if results[i].State != domain.StateReady {
			t.Fatalf("caller %d state = %v, want READY", i, results[i].State)
		}
		if results[i].Ref != ref {
			t.Fatalf("caller %d ref = %+v, want %+v", i, results[i].Ref, ref)
		}
	}
}

// TestLoadModel_SequentialRetryAfterInFlightCompletes proves the in-flight slot
// is RELEASED when a load finishes: a load issued AFTER an earlier one completed
// must start a FRESH load (it must not dedup onto a stale, finished call). Here
// the first load FAILS (fetch not found), and a second load with a now-working
// fetcher must actually re-fetch and reach Ready — proving the slot was cleared.
func TestLoadModel_SequentialRetryAfterInFlightCompletes(t *testing.T) {
	eng := &fakeEngine{session: domain.LoadedSession{Digest: "sha256:abc", MemoryBytes: 4096}}
	fetch := &fakeFetcher{err: domain.ErrArtifactNotFound}
	clk := &fakeClock{now: time.Unix(1000, 0)}
	svc := newTestService(t, eng, fetch, clk)

	ref := domain.ModelRef{Name: "iris", Version: "v1"}
	in := domain.LoadModelInput{Ref: ref, ArtifactURI: "s3://fp-models/iris/v1.onnx"}

	// First load fails (fetch returns not-found) → StateFailed, slot released.
	st, err := svc.LoadModel(context.Background(), in)
	if err != nil {
		t.Fatalf("first load hard error: %v", err)
	}
	if st.State != domain.StateFailed {
		t.Fatalf("first load state = %v, want FAILED", st.State)
	}

	// Repair the fetcher and retry. If the slot had NOT been released, this would
	// wrongly dedup onto the finished (failed) call; instead it must re-fetch and
	// reach Ready.
	fetch.err = nil
	fetch.digest = "sha256:abc"
	fetch.localPth = "/tmp/iris.onnx"
	st2, err := svc.LoadModel(context.Background(), in)
	if err != nil {
		t.Fatalf("retry load hard error: %v", err)
	}
	if st2.State != domain.StateReady {
		t.Fatalf("retry state = %v, want READY (slot must release so a retry starts fresh)", st2.State)
	}
	if fetch.callCount() != 2 {
		t.Fatalf("Fetch called %d times across a failed load + a retry, want 2", fetch.callCount())
	}
}

func TestUnload_EventReaction_DelegatesToUnloadModel(t *testing.T) {
	eng := &fakeEngine{}
	svc := newTestService(t, eng, &fakeFetcher{digest: "sha256:abc"}, &fakeClock{now: time.Unix(1, 0), sinceQueue: []time.Duration{1}})
	ref := mustLoadReady(t, svc, eng)

	st, err := svc.Unload(context.Background(), ref, "archived")
	if err != nil {
		t.Fatalf("Unload event reaction: %v", err)
	}
	if st.State != domain.StateUnloaded {
		t.Fatalf("state = %v, want UNLOADED", st.State)
	}
	if eng.unloadCount() != 1 {
		t.Fatalf("engine Unload count = %d, want 1", eng.unloadCount())
	}
}

// ============================================================================
// LIST + pagination cap
// ============================================================================

func TestListLoadedModels_FilterAndCap(t *testing.T) {
	eng := &fakeEngine{}
	svc := newTestService(t, eng, &fakeFetcher{digest: "sha256:abc"}, &fakeClock{now: time.Unix(1, 0)})
	mustLoadReady(t, svc, eng)

	// No filter: the single ready model is listed.
	got, _, err := svc.ListLoadedModels(context.Background(), domain.ListOptions{PageSize: 1000})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 1 || got[0].State != domain.StateReady {
		t.Fatalf("list = %+v, want one READY model", got)
	}
	// Filter on FAILED: the ready model is excluded.
	got, _, err = svc.ListLoadedModels(context.Background(), domain.ListOptions{StateFilter: domain.StateFailed})
	if err != nil {
		t.Fatalf("list filtered: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("FAILED filter returned %d, want 0", len(got))
	}
}

// itoa is a tiny stdlib-free int→string for building unique map keys in tests
// without importing strconv (keeps the test imports minimal and obvious).
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}

// silence "imported and not used" if a future edit drops the last uuid use.
var _ = context.Background
