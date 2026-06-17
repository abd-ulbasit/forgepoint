// serving_service.go defines the ServingService interface — the primary port
// through which the handler (and the event controller) reach the business logic.
//
// ============================================================================
// THE SERVICE INTERFACE AS A "DRIVING PORT"
// ============================================================================
//
// In Hexagonal Architecture this is a DRIVING (primary) port: the interface the
// outside drives the application THROUGH. The handler (gRPC) and the event
// controller (NATS) both depend on this interface, not on the concrete struct.
// That dependency inversion lets us:
//   - swap the implementation (real registry vs an in-memory test stub) without
//     touching the handler, and
//   - unit-test the handler against a mock ServingService (no engine, no
//     storage, no network).
//
// THE TWO PLANES (mirroring the proto) PLUS THE EVENT REACTIONS:
//
//	DATA PLANE     → Predict                      (the hot path; HPA-scaled)
//	CONTROL PLANE  → LoadModel, UnloadModel,      (lifecycle, low-frequency)
//	                 GetModelStatus, GetModelInfo,
//	                 ListLoadedModels
//	AUTOSCALING    → GetServingMetrics            (the HPA signal)
//	PROBES         → HealthCheck                  (readiness/liveness)
//	EVENT REACTION → EnsureLoaded, Unload         (consumed-event business ops)
//
// WHY EnsureLoaded/Unload are on the SERVICE interface (not hidden in the events
// adapter): a serving pod is a PURE EVENT CONSUMER. The events it reacts to
// (ModelVersionReady, ModelDeployed, ModelPromoted → ensure loaded;
// ModelUndeployed, ModelArchived → unload) drive the SAME registry operations
// the control-plane RPCs do. Modeling them as first-class, idempotent service
// methods means:
//   - the business behavior behind an event is unit-tested in the domain (here),
//     not buried in an untested NATS callback;
//   - the events adapter (later phase) is a thin translation layer:
//     unmarshal events.ModelDeployed → call svc.EnsureLoaded(...). It owns
//     idempotency-key dedup and DLQ; the BEHAVIOR lives here.
//
// This is the "domain models the business operation behind the event" rule.
// ============================================================================
package domain

import (
	"context"
	"time"
)

// LoadModelInput carries the validated fields to load (or reload) a model
// version. The handler builds it from LoadModelRequest; the event controller
// builds it from events.ModelVersionReady / events.ModelDeployed.
//
// WHY a struct, not positional args: new fields (e.g. a priority hint) can be
// added without breaking callers, and named fields prevent argument-swap bugs
// (Name vs Version vs URI are all strings).
type LoadModelInput struct {
	Ref ModelRef // name + version to register the artifact under (server-validated)

	// ArtifactURI is the object-storage location to pull from. Validated against
	// the pod's allow-list (SSRF guard) BEFORE any fetch.
	ArtifactURI string

	// ExpectedDigest, when non-empty, must equal the digest of the fetched bytes
	// or the load fails (StateFailed, ErrDigestMismatch). Supply-chain integrity.
	ExpectedDigest string

	// IdempotencyKey makes a retried load (controller re-reconcile) safe: if the
	// requested (Ref, digest) is already StateReady, the load is a no-op
	// returning the current status without re-fetching/re-initializing. Optional.
	IdempotencyKey string
}

// PredictInput carries one inference request through the domain. The handler
// builds it from PredictRequest (after proto→domain tensor conversion).
//
// SECURITY: there are NO auth/owner/principal/billing fields here — the serving
// pod is auth-agnostic by design (the gateway owns identity; see the proto's
// Sidecar rationale). RequestedRef is re-validated against the resident model
// (defense in depth against a gateway routing bug), but it grants no authority.
type PredictInput struct {
	// RequestedRef is what the caller asked for. Empty Name/Version means
	// "whatever this pod serves". If set and it does NOT match the resident
	// model, Predict returns ErrModelRequestMismatch rather than mis-serving.
	RequestedRef ModelRef

	// Inputs are the named input tensors (validated for layout before the engine
	// is called). SERVER-CAPPED at maxInputTensors entries (DoS guard).
	Inputs map[string]Tensor

	// IdempotencyKey, when non-empty, enables a short-lived result cache: a
	// retry of the same key returns the prior result without re-running the
	// engine. PURELY a latency/compute saving — NOT a billing concern (the pod
	// emits no event; metering is the gateway's). Empty = always recompute.
	IdempotencyKey string

	// CorrelationID is propagated for tracing and echoed back. Carries no
	// authority — a pure trace handle.
	CorrelationID string
}

// PredictResult is what Predict returns: the output tensors plus serving
// metadata. Mirrors PredictResponse minus the wire concerns.
type PredictResult struct {
	Outputs          map[string]Tensor
	ModelVersion     string        // the version that actually produced the result
	InferenceLatency time.Duration // server-measured wall time for THIS call
	CorrelationID    string        // echo of the request's correlation id
	FromCache        bool          // true if served from the idempotency cache
}

// ServingService is the primary domain interface for model serving. The gRPC
// handler holds a value of this interface; the event controller calls the same
// methods. The InferenceEngine / ModelFetcher / Clock ports are injected into
// the concrete impl (NewServingService).
type ServingService interface {
	// ------------------------------------------------------------------
	// CONTROL PLANE — model lifecycle
	// ------------------------------------------------------------------

	// LoadModel fetches an artifact and loads it into the engine, registering it
	// under input.Ref and driving the lifecycle Downloading→Loading→Ready (or
	// →Failed). It is SYNCHRONOUS in this domain implementation (the fetch+init
	// run inline and the returned status reflects the outcome); the proto's
	// "load is async, poll GetModelStatus" remark describes the wire contract a
	// future background-load variant would honor — documented as a tradeoff in
	// the impl.
	//
	// IDEMPOTENT: re-loading an already-Ready identical (Ref, expected digest) is
	// a no-op returning the current status. SSRF-GUARDED: input.ArtifactURI is
	// checked against the allow-list before any fetch.
	//
	// Errors: ErrValidation (bad input), ErrArtifactURINotAllowed (SSRF),
	// ErrModelAlreadyExists (a DIFFERENT artifact under the same Ref). A
	// fetch/init failure does NOT error — it transitions the model to StateFailed
	// and returns that status (the caller polls/observes state), matching how a
	// real async loader reports load failures out-of-band.
	LoadModel(ctx context.Context, input LoadModelInput) (ModelStatus, error)

	// UnloadModel frees a model from the engine and marks it StateUnloaded.
	// After unload, Predict against it returns ErrModelNotReady.
	//
	// IDEMPOTENT: unloading an already-unloaded (or unknown) model returns the
	// current/synthesized Unloaded status without error — a controller
	// re-reconcile is a no-op, not a failure. `reason` is recorded on the entry
	// for observability (it is NOT published — serving emits no events).
	UnloadModel(ctx context.Context, ref ModelRef, reason string) (ModelStatus, error)

	// GetModelStatus returns the live lifecycle status of a resident model. An
	// empty Ref means "this pod's model" (resolved to the single resident model).
	// Returns ErrModelNotFound if no such model is resident.
	GetModelStatus(ctx context.Context, ref ModelRef) (ModelStatus, error)

	// GetModelInfo returns the static identity + I/O schema of a resident model
	// (for clients to build valid requests). Returns ErrModelNotFound if absent.
	// All fields are server-authoritative (derived from the loaded artifact).
	GetModelInfo(ctx context.Context, ref ModelRef) (LoadedModel, error)

	// ListLoadedModels returns the live status of resident models, optionally
	// filtered by state, paginated. page<=0 is clamped to a default; an oversized
	// page is clamped to a max (the page-size cap is a contract promise, enforced
	// here). On a single-model pod this returns 0 or 1 entry.
	ListLoadedModels(ctx context.Context, opts ListOptions) ([]ModelStatus, string, error)

	// ------------------------------------------------------------------
	// DATA PLANE — the hot path
	// ------------------------------------------------------------------

	// Predict runs one inference. It enforces the readiness gate (the model must
	// be StateReady, else ErrModelNotReady), the request-target match (defense in
	// depth, else ErrModelRequestMismatch), the input caps and tensor layout
	// (DoS / overflow guards, else ErrValidation), and the idempotency cache,
	// then delegates the actual math to the InferenceEngine port. It maintains
	// the inflight/latency/counter metrics around the call (the HPA signal).
	//
	// NO side effect on the bus — the pod returns the result; the gateway
	// publishes events.InferenceCompleted. Idempotent on IdempotencyKey purely to
	// make gateway retries cheap (result cache, not a billing concern).
	Predict(ctx context.Context, input PredictInput) (PredictResult, error)

	// ------------------------------------------------------------------
	// AUTOSCALING + PROBES
	// ------------------------------------------------------------------

	// GetServingMetrics returns the point-in-time metrics snapshot that drives
	// the HPA (custom-metrics autoscaling) and the gateway's least-inflight
	// routing. The values are maintained authoritatively by Predict.
	GetServingMetrics(ctx context.Context) ServingMetrics

	// HealthCheck computes the serving-readiness verdict for K8s probes:
	// HealthServing iff the (resolved) model is StateReady AND recent inference
	// latency is within the configured liveness threshold; otherwise
	// HealthNotServing. It also returns the resolved model's state and the last
	// observed latency (the raw signals behind the verdict). An empty modelName
	// checks this pod's model.
	HealthCheck(ctx context.Context, modelName string) (verdict HealthVerdict, state ModelState, lastLatency time.Duration)

	// ------------------------------------------------------------------
	// EVENT REACTIONS — the business operations behind consumed events
	// ------------------------------------------------------------------

	// EnsureLoaded is the idempotent reconcile operation the event controller
	// calls for ModelVersionReady / ModelDeployed / ModelPromoted: "make sure
	// this version is loaded and Ready." If it is already Ready with the same
	// artifact, it is a no-op; otherwise it performs a LoadModel. This is the
	// reconcile-to-desired-state half of the controller pattern, expressed as a
	// domain operation so the BEHAVIOR is tested here, not in the NATS callback.
	//
	// It delegates to LoadModel and inherits its idempotency and SSRF guard. The
	// distinct name documents the INTENT (reconcile) at the call site.
	EnsureLoaded(ctx context.Context, input LoadModelInput) (ModelStatus, error)

	// Unload is the idempotent teardown operation the event controller calls for
	// ModelUndeployed / ModelArchived: "this version should no longer be
	// resident." It delegates to UnloadModel (idempotent). Distinct name to read
	// as the event reaction at the call site.
	Unload(ctx context.Context, ref ModelRef, reason string) (ModelStatus, error)
}

// ListOptions carries pagination + an optional state filter for
// ListLoadedModels. WHY mirror the auth service's ListOptions shape: the
// platform uses one cursor-pagination contract everywhere (page size clamped to
// [1, maxPageSize]); reusing the shape keeps the SDK list helpers uniform.
type ListOptions struct {
	PageSize    int        // <=0 ⇒ default; >max ⇒ clamped to max (contract cap)
	PageToken   string     // opaque cursor; empty = start from the beginning
	StateFilter ModelState // StateUnspecified ⇒ no filter (all states)
}
