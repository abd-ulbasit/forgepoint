// Package domain is the innermost ring of the Clean Architecture onion for the
// Model Serving service. It holds the canonical business types and the
// model-runtime-registry logic that realizes the Sidecar + HPA pattern.
//
// ============================================================================
// CLEAN ARCHITECTURE — DOMAIN LAYER (zero framework imports)
// ============================================================================
//
// This package imports ONLY the standard library and github.com/google/uuid.
// It has NO knowledge of gRPC, NATS, the ONNX runtime, object storage, or the
// generated proto code. The dependency rule points inward:
//
//	cmd/server/main.go         (wires everything)
//	   ↓
//	internal/handler           (proto ↔ domain conversion)   ┐
//	   ↓                                                      │ both implement
//	internal/domain  ◄──── internal/runtime (ONNX adapter)   │ the PORTS this
//	(THIS PACKAGE)   ◄──── internal/storage (MinIO fetcher)  │ package defines
//	stdlib + uuid only                                       ┘
//
// The handler converts proto ↔ domain. The runtime/storage adapters implement
// the InferenceEngine / ModelFetcher PORTS (ports.go). The domain itself never
// knows any of them exist — it calls interfaces it owns.
//
// WHY pure Go types instead of the proto-generated serving types: using the
// .pb.go types as business objects couples the model-runtime logic to the wire
// schema (a proto field rename would break the registry), drags grpc-go into
// unit tests of a state machine, and prevents running the logic where proto
// isn't available. The handler is the single proto↔domain anti-corruption layer.
//
// ============================================================================
// THE PATTERN THIS PACKAGE IMPLEMENTS — "model runtime registry" (Sidecar/HPA)
// ============================================================================
//
// A serving pod is "the model": a thin runtime that loads ONE model version's
// weights into memory and answers Predict. The DOMAIN logic here is the
// registry that tracks each loaded model's lifecycle STATE and its live load
// METRICS, and routes Predict to the runtime only when the model is READY. The
// metrics (inflight requests, latency, counters) are not incidental — they are
// the SCALING SIGNAL the HPA consumes ("HPA on custom metrics"). Keeping that
// signal in the domain makes it unit-testable and authoritative.
package domain

import "time"

// ============================================================================
// ModelState — the loaded-model lifecycle state machine
// ============================================================================
//
// WHY an enum and not a bool "loaded": a model in a serving pod moves through a
// lifecycle — pull the artifact from object storage, initialize the ONNX
// session, become ready — and can fail or be intentionally unloaded. A single
// bool cannot express "currently downloading" vs "load failed" vs "deliberately
// unloaded". The gateway's readiness routing, the operator's reconcile loop,
// and the K8s probes all need the distinction.
//
// STATE MACHINE (mirrors the proto's ModelState, kept as a pure domain type):
//
//	(absent) ──Load──► DOWNLOADING ──fetch ok──► LOADING ──init ok──► READY
//	                        │                       │                   │
//	                        │ fetch err             │ init/digest err   │ Unload
//	                        ▼                       ▼                   ▼
//	                      FAILED                  FAILED             UNLOADED
//	                                                                   │ Load
//	                                                                   └─► DOWNLOADING…
//
// READY is the ONLY state in which Predict is allowed; every other state makes
// Predict return ErrModelNotReady so the gateway routes elsewhere. The legal
// transitions are enforced by CanTransitionTo below — an illegal transition is
// a programming error the service refuses, not a silent state corruption.
type ModelState int

const (
	// StateUnspecified is the zero value — a model the registry has never seen.
	// It is NOT a real lifecycle state; it exists so the zero LoadedModel is
	// obviously invalid (a guard against using an uninitialized struct).
	StateUnspecified ModelState = iota

	// StateDownloading: pulling the artifact bytes from object storage to the
	// pod. The first state any load enters.
	StateDownloading

	// StateLoading: artifact present; initializing the ONNX session (allocating
	// the in-memory inference graph). This is the "cold start" — brief but
	// non-zero, which is why GetModelInfo.loaded_at is observed.
	StateLoading

	// StateReady: model is resident and serving. Readiness probe passes; Predict
	// is allowed ONLY here.
	StateReady

	// StateFailed: a load failed (artifact missing, digest mismatch, unsupported
	// op, OOM). LoadedModel.Message carries the reason. Terminal until a new Load
	// is issued.
	StateFailed

	// StateUnloaded: deliberately freed from memory (Unload). Distinct from
	// Failed so operators do not alert on an intentional teardown. A new Load may
	// move it back to Downloading.
	StateUnloaded
)

// String renders the state for logs/metrics labels. WHY a method and not a map:
// a method is exhaustive-checkable by the compiler if we later add a state and
// is allocation-free.
func (s ModelState) String() string {
	switch s {
	case StateDownloading:
		return "DOWNLOADING"
	case StateLoading:
		return "LOADING"
	case StateReady:
		return "READY"
	case StateFailed:
		return "FAILED"
	case StateUnloaded:
		return "UNLOADED"
	default:
		return "UNSPECIFIED"
	}
}

// CanTransitionTo reports whether moving from the receiver state to `next` is a
// legal lifecycle transition. This is the GUARD that keeps the state machine
// honest: the service consults it before every transition and refuses an
// illegal one (e.g. UNLOADED → READY without going through a load), which would
// otherwise let a bug serve from freed memory.
//
// WHY this lives in the domain as a pure method: the transition rules ARE
// business logic (they encode "you may only serve a model you actually
// loaded"). Keeping them here makes them unit-testable without any runtime and
// guarantees every caller (load path, unload path, reconcile loop) obeys the
// same rules.
//
// LEGAL EDGES (see the ASCII diagram above):
//
//	DOWNLOADING → LOADING | FAILED
//	LOADING     → READY   | FAILED
//	READY       → UNLOADED | FAILED            (a ready model can degrade/fail)
//	FAILED      → DOWNLOADING                  (a retry begins a fresh load)
//	UNLOADED    → DOWNLOADING                  (a reload begins a fresh load)
//	(any state) → DOWNLOADING                  (Load always restarts the pull)
func (s ModelState) CanTransitionTo(next ModelState) bool {
	// A Load may begin from ANY state — it always restarts at Downloading. This
	// is what makes Load idempotent-by-retry and lets a Failed/Unloaded model be
	// recovered without special-casing each source state.
	if next == StateDownloading {
		return true
	}
	switch s {
	case StateDownloading:
		return next == StateLoading || next == StateFailed
	case StateLoading:
		return next == StateReady || next == StateFailed
	case StateReady:
		// A ready model can be intentionally unloaded, or can fail (e.g. the
		// engine reports the session is wedged). It cannot silently re-enter
		// Loading without a new Load (which goes through Downloading above).
		return next == StateUnloaded || next == StateFailed
	case StateFailed, StateUnloaded:
		// From a terminal state the ONLY way forward is a fresh Load (handled by
		// the next == StateDownloading short-circuit above). No other edge.
		return false
	default: // StateUnspecified
		return false
	}
}

// IsServable reports whether Predict may run against a model in this state.
// Exactly one state qualifies. Centralizing the check (rather than scattering
// `== StateReady` comparisons) means a future "DRAINING" state only has to be
// taught here.
func (s ModelState) IsServable() bool { return s == StateReady }

// ============================================================================
// ModelRef — the identity of a model version on the pod
// ============================================================================

// ModelRef identifies a model version: its logical name plus version label.
// It is the registry key. WHY a struct and not a "name/version" string: a typed
// pair is unambiguous, comparable (usable as a map key), and prevents the
// classic bug of concatenating name and version into a string that then
// collides (e.g. name="a/b" version="c" vs name="a" version="b/c").
type ModelRef struct {
	Name    string // logical model name, e.g. "fraud-detector"
	Version string // exact version this pod serves, e.g. "v3"
}

// ============================================================================
// LoadedModel — a registry entry: one model version's live runtime state
// ============================================================================

// LoadedModel is the domain's record of a single model version resident (or
// failed/unloaded) on this pod. It is the unit the runtime registry tracks.
//
// AUTHORITY NOTE: every field here is SERVER-authoritative. State, timestamps,
// digest, and schema are derived from the load process and the artifact's own
// metadata — never accepted from a Predict caller. A client cannot set State to
// READY to bypass the readiness gate, nor spoof the digest. This is the
// anti-mass-assignment posture the platform mandates.
type LoadedModel struct {
	Ref ModelRef // identity (name + version)

	// State is the current lifecycle position (the state machine above). Only
	// StateReady permits Predict.
	State ModelState

	// Message is a human-readable detail, ESPECIALLY the reason on StateFailed
	// ("digest mismatch", "artifact not found", "unsupported ONNX op"). Empty
	// when healthy. Surfaced in ModelStatus.message for operators.
	Message string

	// ArtifactURI is the object-storage location the artifact was loaded from
	// (e.g. "s3://fp-models/fraud/v3.onnx"). Recorded so the reconcile loop can
	// detect drift (desired URI vs resident URI) and so a re-load of the same
	// URI+digest is recognized as idempotent.
	ArtifactURI string

	// ArtifactDigest is the content digest (e.g. "sha256:...") of the loaded
	// bytes, verified against any expected digest at load time. The supply-chain
	// integrity handle the gateway/registry can confirm.
	ArtifactDigest string

	// InputSchema / OutputSchema describe the model's tensor I/O contract,
	// reported by the InferenceEngine after a successful load. Clients introspect
	// these via GetModelInfo to build valid requests. Server-derived from the
	// artifact, never client-set.
	InputSchema  []TensorSpec
	OutputSchema []TensorSpec

	// MemoryBytes is the resident memory the engine reports the loaded session
	// occupies. A capacity-planning signal surfaced in ServingMetrics.
	MemoryBytes int64

	// LoadedAt is when the model last transitioned INTO StateReady (cold-start
	// observability). Zero until the first successful load.
	LoadedAt time.Time

	// UpdatedAt is when the State last changed. Lets the operator detect a stuck
	// DOWNLOADING/LOADING (a cold-start watchdog).
	UpdatedAt time.Time
}

// Status projects the LoadedModel down to the lifecycle view callers care about
// (name, version, state, message, updated-at) — the shape GetModelStatus and
// ListLoadedModels return. WHY a projection rather than exposing the full
// struct: the fleet/probe view does not need the I/O schema or memory estimate,
// and a narrow type keeps the proto-conversion in the handler small.
func (m LoadedModel) Status() ModelStatus {
	return ModelStatus{
		Ref:       m.Ref,
		State:     m.State,
		Message:   m.Message,
		UpdatedAt: m.UpdatedAt,
	}
}

// ModelStatus is the live lifecycle view of a model — used by probes, the
// operator's reconcile loop, and admin tooling. It is the projection returned
// by GetModelStatus and (paginated) by ListLoadedModels.
type ModelStatus struct {
	Ref       ModelRef
	State     ModelState
	Message   string
	UpdatedAt time.Time
}

// ============================================================================
// TENSORS — the inference I/O the domain passes THROUGH to the engine port
// ============================================================================
//
// WHY the domain has its own Tensor type (not the proto TensorData): the domain
// must not import generated proto. The handler converts proto TensorData ↔
// domain Tensor at the boundary; the InferenceEngine port speaks domain Tensor.
// The byte-layout CONTRACT (little-endian, row-major, len == product(shape) *
// sizeof(dtype)) is identical to the proto's — see serving.proto's TENSOR
// PRIMITIVES block. The domain validates that invariant (Tensor.ValidateLayout)
// so a malformed tensor is rejected before it reaches the runtime.

// DataType mirrors the proto DataType: the element type of a tensor's raw byte
// buffer. Kept as a pure domain enum so the engine port has no proto dependency.
type DataType int

const (
	DataTypeUnspecified DataType = iota
	DataTypeFloat32
	DataTypeFloat64
	DataTypeInt32
	DataTypeInt64
	DataTypeBool
	DataTypeString
)

// ElementSize returns the fixed byte width of one element of this dtype, and
// ok=false for variable-width types (STRING) and the unspecified zero value.
//
// WHY this matters for SECURITY (overflow-safe sizing): the layout check
// computes product(shape) * ElementSize and compares it to len(data). That
// product must be guarded against integer overflow with hostile shapes (e.g.
// [1<<60, 1<<60]) — see Tensor.ValidateLayout, which uses this width and checked
// multiplication so a crafted shape cannot wrap to a small number and slip a
// short buffer past the check (a classic deserialization-overflow bug).
func (d DataType) ElementSize() (size int, fixed bool) {
	switch d {
	case DataTypeBool:
		return 1, true
	case DataTypeFloat32, DataTypeInt32:
		return 4, true
	case DataTypeFloat64, DataTypeInt64:
		return 8, true
	case DataTypeString:
		return 0, false // variable width: 4-byte length prefix + bytes, per element
	default: // DataTypeUnspecified
		return 0, false
	}
}

// TensorSpec is a model's declared I/O schema entry: a named tensor with an
// expected shape (-1 = dynamic axis, usually batch) and dtype. Reported by the
// engine after load; returned to clients via GetModelInfo.
type TensorSpec struct {
	Name  string
	Shape []int64
	DType DataType
}

// Tensor is a single named tensor passed to / returned from the engine.
// See the TENSORS block above for the byte-layout contract.
type Tensor struct {
	Shape []int64
	Data  []byte
	DType DataType
}

// ValidateLayout checks the tensor's raw buffer length against its declared
// shape and dtype, the way serving.proto specifies. It is a pure domain guard
// that runs BEFORE the engine port is ever called.
//
// SECURITY (overflow-safe math): the element count is product(shape). A hostile
// client could send a shape like [1<<40, 1<<40] whose product overflows int64
// and wraps to a small positive number — which would then "match" a short
// buffer and sail past a naive check, potentially driving an out-of-bounds read
// in the runtime. We therefore:
//   - reject any negative concrete dimension (only -1 is allowed, meaning
//     "dynamic", and a dynamic axis can't be length-checked here so we skip the
//     byte check for dynamic tensors),
//   - compute the product with CHECKED multiplication (detecting overflow), and
//   - reject empty data on a non-empty fixed shape.
//
// Returns nil when the layout is valid (or cannot be checked because a dimension
// is dynamic). Returns a %w-wrapped ErrValidation otherwise.
func (t Tensor) ValidateLayout() error {
	size, fixed := t.DType.ElementSize()
	if !fixed {
		// STRING and UNSPECIFIED: we cannot do a fixed-width length check here.
		// UNSPECIFIED is itself invalid; STRING uses a self-describing
		// length-prefixed encoding the engine validates. We only reject the
		// outright-invalid UNSPECIFIED dtype; STRING passes through.
		if t.DType == DataTypeUnspecified {
			return wrapValidation("tensor dtype is unspecified")
		}
		return nil
	}

	// Compute product(shape) with overflow detection. A dynamic axis (-1) means
	// we cannot know the concrete element count from the spec alone, so we do not
	// byte-check a dynamic tensor (the engine enforces the concrete shape).
	elements := int64(1)
	for _, dim := range t.Shape {
		if dim == dynamicDim {
			return nil // dynamic axis present → skip the fixed-width byte check
		}
		if dim < 0 {
			return wrapValidation("tensor shape has a negative dimension")
		}
		// Checked multiply: if elements*dim would overflow int64, reject. WHY
		// not just multiply and check sign: signed overflow in Go wraps with
		// defined two's-complement behavior but the wrapped value is meaningless
		// and could be positive — so we detect BEFORE multiplying.
		if dim != 0 && elements > maxInt64/dim {
			return wrapValidation("tensor shape product overflows int64")
		}
		elements *= dim
	}

	wantBytes := elements * int64(size)
	// Guard the final multiply too (elements may be large, size up to 8).
	if size != 0 && elements > maxInt64/int64(size) {
		return wrapValidation("tensor byte size overflows int64")
	}
	if int64(len(t.Data)) != wantBytes {
		return wrapValidation("tensor data length does not match shape*dtype")
	}
	return nil
}

// dynamicDim is the sentinel shape entry meaning "dynamic axis" (mirrors ONNX
// and the proto: -1). A dynamic tensor's concrete length is enforced by the
// engine, not by the pure layout check.
const dynamicDim = -1

// maxInt64 is math.MaxInt64 inlined to avoid importing math into a file that is
// otherwise dependency-free (the domain stays lean). Used for overflow guards.
const maxInt64 = int64(^uint64(0) >> 1)

// ============================================================================
// ServingMetrics — the HPA scaling signal (the heart of the pattern)
// ============================================================================
//
// WHY this is a first-class domain type and not just a /metrics counter: in the
// "HPA on custom metrics" pattern the inflight-requests value is part of the
// SERVICE CONTRACT — the gateway reads it for least-inflight routing and a
// metrics adapter surfaces it to the HPA. The domain OWNS the math so it is
// authoritative and unit-testable: a wrong inflight count would scale the
// fleet wrong.
//
// WHY INFLIGHT, NOT CPU, FOR INFERENCE AUTOSCALING:
// inference latency is dominated by request QUEUING once the CPU is busy;
// inflight (concurrency / queue depth) crosses the danger threshold BEFORE CPU
// saturates, giving the HPA an earlier, more stable signal. CPU is a lagging
// proxy; queue depth is the leading indicator. That is the whole point of the
// custom-metrics pattern.
type ServingMetrics struct {
	// InflightRequests is the number of Predict calls currently executing. THE
	// primary custom HPA metric. Maintained by the service as a counter
	// incremented on entry and decremented on exit of Predict (see metrics.go).
	InflightRequests int32

	// TotalRequests is a monotonic counter of all predictions served since
	// process start (RED "Rate" source).
	TotalRequests int64

	// FailedRequests is a monotonic counter of predictions that errored (RED
	// "Errors" source). With TotalRequests it yields an error rate for alerting.
	FailedRequests int64

	// P50Latency / P99Latency are rolling inference latencies (RED "Duration").
	// P99 doubles as the liveness signal: if it exceeds a configured threshold
	// the liveness probe fails and K8s restarts the wedged pod.
	P50Latency time.Duration
	P99Latency time.Duration

	// LastInferenceLatency is the most recent single-call latency — the raw
	// liveness signal the HealthCheck RPC returns.
	LastInferenceLatency time.Duration

	// ModelMemoryBytes is the resident model memory estimate (capacity planning).
	ModelMemoryBytes int64
}

// ============================================================================
// HealthVerdict — the readiness/liveness split for K8s probes
// ============================================================================

// HealthVerdict is the serving-readiness decision the HealthCheck RPC returns,
// mirroring the proto's HealthStatus. WHY the domain computes this (rather than
// the handler eyeballing the state): the readiness rule ("SERVING iff the model
// is StateReady AND recent latency is within threshold") is business logic the
// gateway and the K8s readinessProbe both depend on — it must be one
// authoritative, testable function (see ServingService.HealthCheck).
type HealthVerdict int

const (
	// HealthUnspecified is the zero value (no verdict computed).
	HealthUnspecified HealthVerdict = iota
	// HealthServing: model loaded AND latency within threshold → accept traffic.
	HealthServing
	// HealthNotServing: process alive but model not ready or latency degraded →
	// K8s keeps the pod but routes traffic away (readiness fail).
	HealthNotServing
)
