// serving_service_impl.go — the concrete ServingService: a MODEL RUNTIME
// REGISTRY that realizes the Sidecar + HPA-on-custom-metrics pattern.
//
// ============================================================================
// WHAT THIS FILE IS (the pattern, named and explained)
// ============================================================================
//
// A serving pod IS one model. This service is the in-pod registry that:
//  1. tracks each resident model's LIFECYCLE STATE via the ModelState machine
//     (Downloading→Loading→Ready→Unloaded/Failed) — every transition guarded by
//     ModelState.CanTransitionTo so the pod never serves from freed memory;
//  2. ROUTES Predict to the InferenceEngine port ONLY when the targeted model
//     is StateReady (the readiness gate) — the data plane does pure math and
//     emits NOTHING on the bus (metering is the gateway's job);
//  3. maintains the SCALING SIGNAL (inflight/latency/counters) the HPA reads —
//     this is the whole point of "HPA on custom metrics".
//
// It is a PURE EVENT CONSUMER's brain: the event controller (later phase) turns
// consumed events into EnsureLoaded / Unload calls on this service. The business
// behavior behind every consumed event lives HERE, unit-tested, not in a NATS
// callback.
//
// CLEAN ARCHITECTURE PLACEMENT: this lives in `domain` and depends ONLY on:
//   - the PORTS it defines (InferenceEngine, ModelFetcher, Clock), and
//   - the standard library + github.com/google/uuid.
//
// It imports NO gRPC, NO NATS, NO ONNX, NO database. main.go injects the real
// adapters; tests inject fakes. That is dependency inversion: the business logic
// dictates the engine/fetcher contract; the infrastructure conforms.
//
// CONCURRENCY MODEL:
//   - Predict is the hot path and runs concurrently across goroutines. The
//     registry map is guarded by a RWMutex; reads (the resident-model lookup on
//     the Predict fast path) take the read lock so many predicts proceed in
//     parallel, while load/unload take the write lock.
//   - The per-call inflight/latency metrics live in metricsAccumulator (its own
//     lock) so updating them never contends on the registry lock.
//   - The idempotency result cache has its own lock.
//
// Three small, independent locks beat one big lock: a Predict cache hit doesn't
// block a concurrent Load, and metrics updates don't block predict lookups.
// ============================================================================
package domain

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ServiceConfig carries the ports and tunables the service needs. WHY a config
// struct (not a long positional constructor): there are several ports plus
// several caps/thresholds; named fields keep main.go's wiring readable and let
// us add a knob without breaking the signature.
type ServiceConfig struct {
	// Ports (the hexagonal driven dependencies).
	Engine  InferenceEngine
	Fetcher ModelFetcher
	Clock   Clock

	// AllowedArtifactURIs is the SSRF/supply-chain allow-list every LoadModel URI
	// is checked against (scheme://bucket/prefix entries). Empty ⇒ deny all loads.
	AllowedArtifactURIs []string

	// LivenessLatencyMax is the p-threshold above which the liveness signal is
	// considered degraded: HealthCheck returns NotServing once the last inference
	// exceeds it (K8s livenessProbe wedged-pod detection). 0 ⇒ disabled.
	LivenessLatencyMax time.Duration

	// MaxInputTensors / MaxTensorBytes are the per-Predict DoS caps (see the
	// proto's INPUT-SIZE BOUND). A request exceeding either is rejected before
	// the engine runs. 0 ⇒ a sane default is applied.
	MaxInputTensors int
	MaxTensorBytes  int64

	// DefaultPageSize / MaxPageSize bound ListLoadedModels (the contract cap).
	DefaultPageSize int
	MaxPageSize     int

	// IdempotencyCacheSize bounds the Predict result cache (entries). 0 ⇒ a
	// default; a value <0 disables caching.
	IdempotencyCacheSize int
}

// Default caps applied when a ServiceConfig field is left zero. Centralized so
// the contract numbers (matching serving.proto) live in one place.
const (
	defaultMaxInputTensors = 64
	defaultMaxTensorBytes  = 16 << 20 // 16 MiB
	defaultPageSize        = 20
	defaultMaxPageSize     = 100
	defaultIdempCacheSize  = 1024
)

// servingService is the production ServingService. Unexported: callers receive
// it only through the interface returned by NewServingService, enforcing
// program-to-the-interface and keeping the registry internals private.
type servingService struct {
	engine  InferenceEngine
	fetcher ModelFetcher
	clock   Clock

	allowedURIs        []string
	livenessLatencyMax time.Duration
	maxInputTensors    int
	maxTensorBytes     int64
	defaultPageSize    int
	maxPageSize        int

	// registry holds the resident models keyed by Ref. A RWMutex because the
	// Predict fast path only READS it (resident-model lookup) and should not
	// serialize behind other predicts; load/unload WRITE it.
	mu       sync.RWMutex
	registry map[ModelRef]*LoadedModel

	// loading is the IN-FLIGHT LOAD GUARD: at most one *loadInFlight per Ref while
	// a load is running. It is guarded by the SAME mu as the registry, so the
	// "is a load already running for this Ref?" check and the registry conflict
	// check are one atomic decision. The first caller for a Ref becomes the LEADER
	// (creates the entry, does the fetch+engine init); any concurrent caller for
	// the same Ref observes the entry, drops the lock, and WAITS on the leader's
	// completion channel — so N concurrent loads of one Ref do exactly ONE fetch
	// and ONE engine init. This is the singleflight pattern (cf.
	// golang.org/x/sync/singleflight) implemented inline to keep the domain
	// dependency-free. It is what makes EnsureLoaded's "idempotent reconcile"
	// promise true for CONCURRENT redelivered events, not just sequential ones.
	loading map[ModelRef]*loadInFlight

	metrics *metricsAccumulator

	// idempotency result cache (its own lock; see the cache type below).
	cache *resultCache
}

// NewServingService wires the ports + config and returns the ServingService
// interface. The compile-time assertion below proves *servingService satisfies
// the interface; a drifted method signature fails the build rather than at a
// call site.
func NewServingService(cfg ServiceConfig) ServingService {
	maxInputs := cfg.MaxInputTensors
	if maxInputs <= 0 {
		maxInputs = defaultMaxInputTensors
	}
	maxBytes := cfg.MaxTensorBytes
	if maxBytes <= 0 {
		maxBytes = defaultMaxTensorBytes
	}
	pageSize := cfg.DefaultPageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	maxPage := cfg.MaxPageSize
	if maxPage <= 0 {
		maxPage = defaultMaxPageSize
	}
	cacheSize := cfg.IdempotencyCacheSize
	if cacheSize == 0 {
		cacheSize = defaultIdempCacheSize
	}

	return &servingService{
		engine:             cfg.Engine,
		fetcher:            cfg.Fetcher,
		clock:              cfg.Clock,
		allowedURIs:        cfg.AllowedArtifactURIs,
		livenessLatencyMax: cfg.LivenessLatencyMax,
		maxInputTensors:    maxInputs,
		maxTensorBytes:     maxBytes,
		defaultPageSize:    pageSize,
		maxPageSize:        maxPage,
		registry:           make(map[ModelRef]*LoadedModel),
		loading:            make(map[ModelRef]*loadInFlight),
		metrics:            newMetricsAccumulator(),
		cache:              newResultCache(cacheSize),
	}
}

var _ ServingService = (*servingService)(nil)

// loadInFlight tracks a single in-progress load for one Ref so concurrent
// LoadModel callers dedup onto the leader's work instead of each fetching and
// engine-loading the same artifact. It is the singleflight "call" record.
//
//	leader:   creates the entry under mu, runs fetch+init OUTSIDE the lock, then
//	          under mu records (status,err), removes the entry, and close(done).
//	follower: finds the entry under mu, drops the lock, blocks on <-done, then
//	          reads the leader's (status,err). It performs NO fetch, NO init.
//
// `done` is closed exactly once by the leader; closing (not sending) lets ANY
// number of followers observe completion. status/err are written by the leader
// BEFORE close(done) and read by followers AFTER <-done, so the close is the
// happens-before edge that makes those fields safe to read without the lock.
type loadInFlight struct {
	done   chan struct{} // closed by the leader when the load completes
	status ModelStatus   // the leader's result (valid after done is closed)
	err    error         // the leader's error (valid after done is closed)
}

// ============================================================================
// LoadModel — drive a model through the lifecycle to Ready (or Failed)
// ============================================================================

// LoadModel validates input, enforces the SSRF allow-list, then drives the
// state machine: register at Downloading → fetch → Loading → engine init →
// Ready, or → Failed on any step.
//
// IDEMPOTENCY + SAFETY:
//   - Re-loading an already-Ready model with the SAME expected digest is a
//     no-op returning the current status (controller re-reconcile).
//   - CONCURRENT loads of the SAME Ref are DEDUPED by an in-flight guard
//     (singleflight): the first caller does the one fetch + one engine init; any
//     caller that arrives while that load is running blocks on it and returns the
//     SAME result instead of starting a second fetch. This is what makes the
//     controller's concurrent EnsureLoaded (e.g. a redelivered ModelDeployed and
//     ModelVersionReady racing) cost ONE cold start, not N. See step 3 below.
//   - A load under an existing Ref with a DIFFERENT artifact is rejected
//     (ErrModelAlreadyExists) — a (name, version) maps to one artifact for the
//     pod's life, so two deploys can't silently swap weights.
//   - A fetch/init failure does NOT return an error; it transitions the model to
//     StateFailed and returns that status. WHY: this mirrors a real async loader
//     (the proto says "load is async, poll GetModelStatus") — load failures are
//     reported via STATE, not as a synchronous RPC error, so the operator's
//     reconcile loop observes Failed and decides on restart. (A pure-validation
//     or SSRF rejection IS returned synchronously — those are caller errors, not
//     load outcomes.)
func (s *servingService) LoadModel(ctx context.Context, input LoadModelInput) (ModelStatus, error) {
	// --- 1. Validate (caller errors → synchronous error return) ---------------
	if input.Ref.Name == "" {
		return ModelStatus{}, wrapValidation("model name is required")
	}
	if input.Ref.Version == "" {
		return ModelStatus{}, wrapValidation("model version is required")
	}
	if input.ArtifactURI == "" {
		return ModelStatus{}, wrapValidation("artifact URI is required")
	}

	// --- 2. SSRF / supply-chain allow-list (BEFORE any fetch) -----------------
	// The guard runs in the domain so a disallowed URI never reaches the network.
	if !ArtifactURIAllowed(input.ArtifactURI, s.allowedURIs) {
		return ModelStatus{}, fmt.Errorf("%w: %q", ErrArtifactURINotAllowed, input.ArtifactURI)
	}

	// --- 3. Idempotency / conflict check + IN-FLIGHT LOAD GUARD ----------------
	// We take the write lock ONLY long enough to make one atomic decision:
	//   (a) the model is already Ready with the same artifact → no-op, OR
	//   (b) it is Ready with a different digest → conflict, OR
	//   (c) a load for this Ref is ALREADY in flight → become a follower, OR
	//   (d) none of the above → become the LEADER: install the in-flight entry,
	//       transition to Downloading, and proceed to do the fetch+init.
	// We must NOT hold the lock across the fetch/engine init (they are I/O and
	// cold-start latency — holding the write lock there would serialize every
	// Predict on the pod). The in-flight guard is what dedups concurrent loaders
	// WITHOUT holding the lock for the whole load: case (c) is the fix for the
	// "5 concurrent loads each fetch" bug.
	s.mu.Lock()
	if existing, ok := s.registry[input.Ref]; ok {
		// Already Ready with the SAME artifact → idempotent no-op.
		if existing.State == StateReady && digestMatches(existing.ArtifactDigest, input.ExpectedDigest) {
			st := existing.Status()
			s.mu.Unlock()
			return st, nil
		}
		// Ready with a DIFFERENT expected digest → conflict (refuse to swap).
		if existing.State == StateReady && input.ExpectedDigest != "" && !digestMatches(existing.ArtifactDigest, input.ExpectedDigest) {
			s.mu.Unlock()
			return ModelStatus{}, fmt.Errorf("%w: %s/%s", ErrModelAlreadyExists, input.Ref.Name, input.Ref.Version)
		}
		// Otherwise (Failed/Unloaded/Downloading) we fall through and re-load:
		// Load may begin from any state (ModelState.CanTransitionTo allows
		// →Downloading from anywhere). A retry of a failed load is legitimate.
	}

	// (c) FOLLOWER PATH: a load for this Ref is already running. Do NOT start a
	// second fetch/init. Drop the lock and block on the leader's completion, then
	// return the leader's authoritative result. This is the dedup that makes
	// EnsureLoaded idempotent for CONCURRENT redelivered events.
	if inflight, running := s.loading[input.Ref]; running {
		s.mu.Unlock()
		<-inflight.done // happens-before: leader wrote status/err before close
		return inflight.status, inflight.err
	}

	// (d) LEADER PATH: claim the in-flight slot and transition to Downloading. We
	// register the entry under the SAME lock as the conflict check, so no second
	// caller can also become a leader for this Ref (they'll see case (c)).
	inflight := &loadInFlight{done: make(chan struct{})}
	s.loading[input.Ref] = inflight
	now := s.clock.Now()
	model := s.upsertLocked(input.Ref, StateDownloading, "", now)
	model.ArtifactURI = input.ArtifactURI
	s.mu.Unlock()

	// The leader runs the real load and PUBLISHES its outcome to the in-flight
	// entry exactly once (under the lock), then closes `done` to release every
	// follower. Centralizing this in a defer guarantees it happens on EVERY leader
	// return path (fetch error, digest mismatch, engine error, success), so a
	// follower can never block forever on a leader that returned early.
	var (
		st     ModelStatus
		loErr  error
		finish = func() (ModelStatus, error) {
			s.mu.Lock()
			inflight.status = st
			inflight.err = loErr
			// Remove the slot so a LATER load (e.g. a retry after a failed one) can
			// start fresh rather than dedup onto this finished call.
			delete(s.loading, input.Ref)
			s.mu.Unlock()
			close(inflight.done)
			return st, loErr
		}
	)

	// --- 4. Fetch the artifact (outside the lock — it's I/O) -------------------
	localPath, fetchedDigest, err := s.fetcher.Fetch(ctx, input.ArtifactURI)
	if err != nil {
		// Fetch failure → Failed state with reason. Distinct reason for "not
		// found" vs a generic fetch error so the operator can tell them apart.
		reason := "artifact fetch failed: " + err.Error()
		if errors.Is(err, ErrArtifactNotFound) {
			reason = "artifact not found in object storage"
		}
		st = s.fail(input.Ref, reason)
		return finish()
	}

	// --- 5. Verify digest if an expected one was supplied ---------------------
	// We check BOTH the fetcher-reported digest and (after load) the engine's, but
	// the fetcher's is the cheapest place to catch a mismatch — before we spend
	// the cold-start cost of initializing the ONNX session.
	if input.ExpectedDigest != "" && !digestMatches(fetchedDigest, input.ExpectedDigest) {
		st = s.fail(input.Ref, fmt.Sprintf("digest mismatch: got %s want %s", fetchedDigest, input.ExpectedDigest))
		return finish()
	}

	// --- 6. Transition to Loading and initialize the engine session -----------
	s.transition(input.Ref, StateLoading, "")
	session, err := s.engine.Load(ctx, input.Ref, localPath)
	if err != nil {
		st = s.fail(input.Ref, "engine load failed: "+err.Error())
		return finish()
	}

	// The engine also reports the digest of what it actually loaded. If an
	// expected digest was given, verify the ENGINE's digest too (defense in depth
	// against a fetcher that lied or a swap between fetch and load).
	if input.ExpectedDigest != "" && session.Digest != "" && !digestMatches(session.Digest, input.ExpectedDigest) {
		st = s.fail(input.Ref, "engine digest mismatch")
		return finish()
	}

	// --- 7. Commit Ready, record server-authoritative metadata ----------------
	finalDigest := session.Digest
	if finalDigest == "" {
		finalDigest = fetchedDigest
	}
	readyAt := s.clock.Now()

	s.mu.Lock()
	m := s.registry[input.Ref]
	if m == nil {
		// Defensive: if an interleaving Unload removed the entry, re-create it —
		// the load we just completed is authoritative for this Ref.
		m = &LoadedModel{Ref: input.Ref}
		s.registry[input.Ref] = m
	}
	// We are the leader for this Ref's in-flight load and we just finished the
	// engine init, so the model is in Loading. Committing Ready here is the
	// authoritative end of OUR load; the CanTransitionTo guard is enforced on
	// every OTHER transition (Downloading/Loading/Failed), keeping the state
	// machine honest while letting the completing load finalize.
	m.State = StateReady
	m.Message = ""
	m.ArtifactURI = input.ArtifactURI
	m.ArtifactDigest = finalDigest
	m.InputSchema = session.InputSchema
	m.OutputSchema = session.OutputSchema
	m.MemoryBytes = session.MemoryBytes
	m.LoadedAt = readyAt
	m.UpdatedAt = readyAt
	st = m.Status()
	s.mu.Unlock()

	// Record resident memory in the metrics (capacity-planning signal). Done
	// BEFORE finish() so the memory signal is visible by the time followers wake.
	s.metrics.setMemory(session.MemoryBytes)

	// Publish the Ready status to any followers and release the in-flight slot.
	return finish()
}

// upsertLocked creates or transitions a registry entry to `state` with `msg` and
// timestamp `now`, enforcing the state-machine guard. Caller holds the write
// lock. Returns the entry so the caller can set more fields.
func (s *servingService) upsertLocked(ref ModelRef, state ModelState, msg string, now time.Time) *LoadedModel {
	m, ok := s.registry[ref]
	if !ok {
		m = &LoadedModel{Ref: ref, State: StateUnspecified}
		s.registry[ref] = m
	}
	// Guard the transition. StateUnspecified→Downloading and any→Downloading are
	// legal (Load may restart from anywhere), so this never rejects a load start.
	if m.State.CanTransitionTo(state) {
		m.State = state
		m.Message = msg
		m.UpdatedAt = now
	}
	return m
}

// transition moves a registry entry to `state` under the write lock, enforcing
// the guard. Used for the inline Downloading→Loading step.
func (s *servingService) transition(ref ModelRef, state ModelState, msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m, ok := s.registry[ref]; ok && m.State.CanTransitionTo(state) {
		m.State = state
		m.Message = msg
		m.UpdatedAt = s.clock.Now()
	}
}

// fail transitions a model to StateFailed with `reason` and returns its status.
// Centralized so every failure path stamps the reason and timestamp identically.
func (s *servingService) fail(ref ModelRef, reason string) ModelStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.registry[ref]
	if !ok {
		m = &LoadedModel{Ref: ref}
		s.registry[ref] = m
	}
	// Failed is reachable from Downloading/Loading/Ready (CanTransitionTo). We
	// set it directly here because `fail` is the terminal sink for a load attempt.
	m.State = StateFailed
	m.Message = reason
	m.UpdatedAt = s.clock.Now()
	return m.Status()
}

// ============================================================================
// UnloadModel — free a model and mark it Unloaded (idempotent)
// ============================================================================

func (s *servingService) UnloadModel(ctx context.Context, ref ModelRef, reason string) (ModelStatus, error) {
	resolved, err := s.resolveRef(ref)
	if err != nil {
		// Unknown model → idempotent success with a synthesized Unloaded status.
		// A controller re-reconcile after an archive must be a no-op, not an error.
		if errors.Is(err, ErrModelNotFound) {
			return ModelStatus{Ref: ref, State: StateUnloaded, Message: reason, UpdatedAt: s.clock.Now()}, nil
		}
		return ModelStatus{}, err
	}

	// Free the engine session (idempotent at the engine too). Outside the lock —
	// it's I/O.
	if uerr := s.engine.Unload(ctx, resolved); uerr != nil {
		// An engine unload error is non-fatal to the desired end-state: we still
		// mark the registry Unloaded (the model must not serve), but record why.
		s.mu.Lock()
		if m, ok := s.registry[resolved]; ok {
			m.State = StateUnloaded
			m.Message = "unloaded (engine reported: " + uerr.Error() + ")"
			m.UpdatedAt = s.clock.Now()
		}
		st := s.registry[resolved].Status()
		s.mu.Unlock()
		return st, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.registry[resolved]
	m.State = StateUnloaded
	m.Message = reason
	m.UpdatedAt = s.clock.Now()
	return m.Status(), nil
}

// ============================================================================
// Predict — the data plane (readiness gate + routing + metrics + cache)
// ============================================================================

// Predict is the hot path. Order of operations (each step is a guard the engine
// is protected behind):
//
//  1. resolve the resident model + readiness gate (StateReady or ErrModelNotReady)
//  2. request-target match (defense in depth, ErrModelRequestMismatch)
//  3. input caps + tensor layout (DoS + overflow guards, ErrValidation)
//  4. idempotency cache lookup (cheap retry → cached result)
//  5. inflight++ , call engine, measure latency, inflight-- (metrics)
//  6. cache the result, return
//
// The pod emits NO event — it returns the result; the gateway publishes
// events.InferenceCompleted. Metering is intentionally NOT here.
func (s *servingService) Predict(ctx context.Context, input PredictInput) (PredictResult, error) {
	// --- 1. Resolve the pod's resident model INDEPENDENTLY of the request ------
	// A serving pod hosts exactly one model (the Sidecar shape). We resolve what
	// the pod ACTUALLY hosts WITHOUT consulting the request's target — then we
	// validate the request against it (step 2). This ordering is deliberate and
	// is what gives us the right error vocabulary:
	//   - nothing resident at all      → ErrModelNotReady   (nothing to serve)
	//   - resident but not Ready        → ErrModelNotReady   (route elsewhere)
	//   - resident, Ready, but the
	//     request named a DIFFERENT one → ErrModelRequestMismatch (routing bug)
	// If we instead resolved BY the request, a request for a non-hosted model
	// would collapse into a generic not-found and we'd lose the "your gateway
	// routed to the wrong pod" signal the proto's PredictRequest doc calls for.
	//
	// Read lock: many predicts resolve the resident model in parallel.
	s.mu.RLock()
	resident, resolveErr := s.residentModelLocked(ModelRef{}) // empty ⇒ "this pod's model"
	s.mu.RUnlock()
	if resolveErr != nil {
		// No model resident on the pod at all. There is nothing to serve, so this
		// is a readiness failure (the gateway should route elsewhere), NOT a
		// "model not found" — the pod is simply not ready to predict yet.
		return PredictResult{}, fmt.Errorf("%w: no model loaded on this pod", ErrModelNotReady)
	}
	servedRef := resident.Ref

	// --- 2. Request-target match (defense in depth) ---------------------------
	// A non-empty RequestedRef that disagrees with the resident model is a
	// gateway routing bug; refuse rather than mis-serve. Empty fields mean
	// "whatever this pod serves" and always match. We check the MATCH before the
	// readiness gate so a request for the wrong model is reported as a mismatch
	// (the actionable signal) rather than masked by the resident model's state.
	if input.RequestedRef.Name != "" && input.RequestedRef.Name != servedRef.Name {
		return PredictResult{}, fmt.Errorf("%w: requested %q have %q",
			ErrModelRequestMismatch, input.RequestedRef.Name, servedRef.Name)
	}
	if input.RequestedRef.Version != "" && input.RequestedRef.Version != servedRef.Version {
		return PredictResult{}, fmt.Errorf("%w: requested version %q have %q",
			ErrModelRequestMismatch, input.RequestedRef.Version, servedRef.Version)
	}

	// --- 1b. Readiness gate (now that we know the request targets this model) --
	if !resident.State.IsServable() {
		// Right pod, right model, wrong STATE → precondition so the gateway can
		// route elsewhere instead of treating it as a hard failure.
		return PredictResult{}, fmt.Errorf("%w: model %s/%s is %s",
			ErrModelNotReady, resident.Ref.Name, resident.Ref.Version, resident.State)
	}

	// --- 3. Input caps + tensor layout (DoS + overflow guards) ----------------
	if err := s.validateInputs(input.Inputs); err != nil {
		return PredictResult{}, err
	}

	// --- 4. Idempotency cache --------------------------------------------------
	// The cache identity is the (idempotency_key, input-fingerprint) PAIR, not the
	// key alone. We fingerprint the inputs once here and reuse it for both the
	// lookup and the store. Binding to the inputs is what makes the cache
	// correctness-SAFE: a client that reuses a key with DIFFERENT inputs gets a
	// MISS (recompute) instead of the prior input's answer — see cache.go's header
	// for the cross-request data-confusion hazard this closes.
	var inputFingerprint uint64
	if input.IdempotencyKey != "" {
		inputFingerprint = fingerprintInputs(input.Inputs)
		if cached, ok := s.cache.get(input.IdempotencyKey, inputFingerprint); ok {
			// Return the cached outputs, flagged FromCache. No engine call, no
			// metrics increment (we measure compute; a cache hit did none).
			return PredictResult{
				Outputs:          cloneTensorMap(cached),
				ModelVersion:     servedRef.Version,
				InferenceLatency: 0,
				CorrelationID:    input.CorrelationID,
				FromCache:        true,
			}, nil
		}
	}

	// --- 5. Inflight++ → engine → measure → inflight-- -------------------------
	// beginRequest BEFORE the engine runs so the HPA sees queue depth during the
	// call. The defer guarantees endRequest runs on EVERY exit path (success AND
	// the engine-error return), so inflight can never leak.
	start := s.clock.Now()
	s.metrics.beginRequest()
	outputs, engErr := s.engine.Predict(ctx, servedRef, input.Inputs)
	latency := s.clock.Since(start)
	s.metrics.endRequest(latency, engErr != nil)

	if engErr != nil {
		// Wrap as ErrInferenceFailed; preserve ErrEngineBadInput in the chain so
		// the handler can choose InvalidArgument over Internal.
		return PredictResult{}, fmt.Errorf("%w: %s", ErrInferenceFailed, engErr.Error())
	}

	// --- 6. Cache + return -----------------------------------------------------
	// Store under the (key, fingerprint) computed in step 4 so a later retry with
	// the SAME inputs hits, and a later REUSE of the key with different inputs is a
	// fingerprint mismatch (miss → recompute), never a wrong cached answer.
	if input.IdempotencyKey != "" {
		s.cache.put(input.IdempotencyKey, inputFingerprint, cloneTensorMap(outputs))
	}
	return PredictResult{
		Outputs:          outputs,
		ModelVersion:     servedRef.Version,
		InferenceLatency: latency,
		CorrelationID:    input.CorrelationID,
		FromCache:        false,
	}, nil
}

// validateInputs enforces the per-Predict DoS caps and the per-tensor layout
// guard. WHY here and not in the handler: the caps are a domain INVARIANT (the
// engine must never see an oversized fan-out request), so the domain owns them;
// the handler also can't express a map-cardinality cap in proto.
func (s *servingService) validateInputs(inputs map[string]Tensor) error {
	if len(inputs) > s.maxInputTensors {
		return fmt.Errorf("%w: too many input tensors (%d > %d)", ErrValidation, len(inputs), s.maxInputTensors)
	}
	for name, t := range inputs {
		if int64(len(t.Data)) > s.maxTensorBytes {
			return fmt.Errorf("%w: tensor %q exceeds max bytes (%d > %d)", ErrValidation, name, len(t.Data), s.maxTensorBytes)
		}
		// The overflow-safe layout check (shape*dtype == len(data)) lives on the
		// Tensor type so the rule is one source of truth.
		if err := t.ValidateLayout(); err != nil {
			return fmt.Errorf("%w: tensor %q: %v", ErrValidation, name, err)
		}
	}
	return nil
}

// ============================================================================
// Introspection: status / info / list / metrics / health
// ============================================================================

func (s *servingService) GetModelStatus(_ context.Context, ref ModelRef) (ModelStatus, error) {
	resolved, err := s.resolveRef(ref)
	if err != nil {
		return ModelStatus{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.registry[resolved].Status(), nil
}

func (s *servingService) GetModelInfo(_ context.Context, ref ModelRef) (LoadedModel, error) {
	resolved, err := s.resolveRef(ref)
	if err != nil {
		return LoadedModel{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Return a COPY so a caller can't mutate the registry's entry through the
	// returned struct (the slices are shared, but the handler only reads them).
	return *s.registry[resolved], nil
}

func (s *servingService) ListLoadedModels(_ context.Context, opts ListOptions) ([]ModelStatus, string, error) {
	// Clamp the page size — the contract cap (page_size ∈ [1, maxPageSize]).
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = s.defaultPageSize
	}
	if pageSize > s.maxPageSize {
		pageSize = s.maxPageSize
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ModelStatus, 0, len(s.registry))
	for _, m := range s.registry {
		if opts.StateFilter != StateUnspecified && m.State != opts.StateFilter {
			continue
		}
		out = append(out, m.Status())
		if len(out) >= pageSize {
			break
		}
	}
	// On a one-model pod the result is tiny; we return an empty next-page token
	// (single page). A real multi-model pod would encode a cursor here; the
	// contract is honored (token is opaque and may be empty = last page).
	return out, "", nil
}

func (s *servingService) GetServingMetrics(_ context.Context) ServingMetrics {
	return s.metrics.snapshot()
}

// HealthCheck computes the readiness verdict K8s keys on. SERVING iff the
// resolved model is StateReady AND the most recent inference latency is within
// the liveness threshold (when one is configured). This single function is the
// authority both the readinessProbe and the gateway's routing trust.
func (s *servingService) HealthCheck(_ context.Context, modelName string) (HealthVerdict, ModelState, time.Duration) {
	last := s.metrics.lastInferenceLatency()

	resolved, err := s.resolveRef(ModelRef{Name: modelName})
	if err != nil {
		// No resident/ready model to check → NotServing (and Unspecified state).
		return HealthNotServing, StateUnspecified, last
	}
	s.mu.RLock()
	state := s.registry[resolved].State
	s.mu.RUnlock()

	// Readiness: model must be Ready.
	if !state.IsServable() {
		return HealthNotServing, state, last
	}
	// Liveness signal: if a threshold is set and the last inference exceeded it,
	// the pod is degraded → NotServing (the model is still Ready; we just route
	// traffic away until latency recovers).
	if s.livenessLatencyMax > 0 && last > s.livenessLatencyMax {
		return HealthNotServing, state, last
	}
	return HealthServing, state, last
}

// ============================================================================
// Event reactions — EnsureLoaded / Unload (the consumed-event business ops)
// ============================================================================

// EnsureLoaded is the reconcile-to-desired-state operation for
// ModelVersionReady / ModelDeployed / ModelPromoted. It delegates to LoadModel,
// which is already idempotent (a duplicate/redelivered event is a no-op). The
// distinct name documents the INTENT at the controller call site.
func (s *servingService) EnsureLoaded(ctx context.Context, input LoadModelInput) (ModelStatus, error) {
	return s.LoadModel(ctx, input)
}

// Unload is the teardown reaction for ModelUndeployed / ModelArchived. It
// delegates to the idempotent UnloadModel.
func (s *servingService) Unload(ctx context.Context, ref ModelRef, reason string) (ModelStatus, error) {
	return s.UnloadModel(ctx, ref, reason)
}

// ============================================================================
// Ref resolution helpers
// ============================================================================

// resolveRef turns a possibly-partial ModelRef into the concrete Ref of a
// resident model. WHY this exists: the proto lets callers leave name/version
// empty to mean "this pod's single model" (a one-model-pod convenience). This
// helper centralizes that resolution and the not-found error.
//
//   - both fields empty            → the single resident model (error if 0 or >1)
//   - name set, version empty      → the single resident model of that name
//   - both set                     → that exact Ref (error if absent)
func (s *servingService) resolveRef(ref ModelRef) (ModelRef, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	resolved, _, err := s.residentModelLockedRaw(ref)
	return resolved, err
}

// residentModelLocked returns the resident *LoadedModel for a (possibly partial)
// ref. Caller holds at least the read lock. Used on the Predict fast path so the
// readiness gate reads the live state without a second map lookup.
func (s *servingService) residentModelLocked(ref ModelRef) (*LoadedModel, error) {
	_, m, err := s.residentModelLockedRaw(ref)
	return m, err
}

// residentModelLockedRaw is the shared resolution core. Caller holds the lock.
func (s *servingService) residentModelLockedRaw(ref ModelRef) (ModelRef, *LoadedModel, error) {
	// Exact match when both fields are set.
	if ref.Name != "" && ref.Version != "" {
		if m, ok := s.registry[ref]; ok {
			return ref, m, nil
		}
		return ModelRef{}, nil, ErrModelNotFound
	}

	// Partial/empty: scan for a unique match. On a one-model pod this is O(1).
	var found *LoadedModel
	for k, m := range s.registry {
		if ref.Name != "" && k.Name != ref.Name {
			continue
		}
		if found != nil {
			// Ambiguous on a (hypothetical) multi-model pod: the caller must be
			// explicit. Treat as not-found rather than guess which to serve.
			return ModelRef{}, nil, ErrModelNotFound
		}
		found = m
	}
	if found == nil {
		return ModelRef{}, nil, ErrModelNotFound
	}
	return found.Ref, found, nil
}

// ============================================================================
// small pure helpers
// ============================================================================

// digestMatches reports whether two digest strings refer to the same artifact.
// An empty expected digest means "no expectation" and matches anything — the
// caller decides whether an empty expectation is acceptable. We compare the full
// strings (including any "sha256:" prefix) for an exact match.
func digestMatches(actual, expected string) bool {
	if expected == "" {
		return true
	}
	return actual == expected
}

// cloneTensorMap deep-copies a tensor map so the idempotency cache holds an
// independent snapshot. WHY: without this, a cache hit would return the SAME
// underlying byte slices the original caller got; if either mutated them, the
// other's result would silently change (a shared-mutable-state bug). The cache
// must own immutable copies.
func cloneTensorMap(in map[string]Tensor) map[string]Tensor {
	if in == nil {
		return nil
	}
	out := make(map[string]Tensor, len(in))
	for k, t := range in {
		data := make([]byte, len(t.Data))
		copy(data, t.Data)
		shape := make([]int64, len(t.Shape))
		copy(shape, t.Shape)
		out[k] = Tensor{Shape: shape, Data: data, DType: t.DType}
	}
	return out
}
