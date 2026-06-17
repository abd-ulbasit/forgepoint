// ports.go — the PORTS (driven interfaces) the model-serving domain depends on.
//
// ============================================================================
// WHY THESE INTERFACES LIVE IN THE DOMAIN PACKAGE (Hexagonal "consumer-owned ports")
// ============================================================================
//
// In Hexagonal Architecture the CONSUMER of a dependency owns the interface for
// it. The domain service is the consumer of the ONNX runtime and of object
// storage, so the PORT interfaces belong here in `domain`. The concrete
// ADAPTERS — internal/runtime (the real ONNX binding) and internal/storage (the
// MinIO/S3 fetcher) — import `domain` and IMPLEMENT these ports. One inward
// arrow, no cycle:
//
//	domain  (defines ServingService + PORTS + models)         ← stdlib + uuid only
//	   ▲
//	   │ implements
//	runtime adapter (ONNX)   storage adapter (MinIO)   handler (proto ↔ domain)
//
// If the ports lived in the adapter packages instead, the domain service (which
// must reference them) would import the adapter, and the adapter already imports
// the domain for the types — domain → adapter → domain is an import CYCLE Go
// rejects. "Consumer owns the port" is the idiomatic Go fix.
//
// INTERVIEW: "How does the serving domain stay free of ONNX?" It depends on an
// InferenceEngine INTERFACE it defines; the real onnxruntime binding satisfies
// that interface in an outer package. The domain can be fully unit-tested with a
// hand-written fake engine — no native library, no model file. This is the
// "abstract the runtime behind a port" technique (the same way you'd hide a
// database behind a repository).
// ============================================================================
package domain

import (
	"context"
	"fmt"
	"time"
)

// ============================================================================
// InferenceEngine — the ONNX runtime PORT (the abstraction this service is built around)
// ============================================================================
//
// The domain NEVER does real ONNX. It calls this port. The real adapter
// (internal/runtime, using e.g. github.com/yalue/onnxruntime_go) wraps the
// native session; tests inject a fake that returns canned tensors. This is the
// crux of the Sidecar pattern's testability: the serving runtime is "dumb,
// single-purpose, and replaceable" precisely BECAUSE it sits behind this seam.
//
// LIFECYCLE the port models:
//
//	Load(uri)  → fetch+init a session, return its schema + memory + digest
//	Predict()  → run one inference against the loaded session
//	Unload()   → free the session's memory
//
// Each method takes a ModelRef so a future multi-model pod variant (the rejected
// Triton-style server) could host several sessions behind one engine without a
// signature change — forward-compatibility baked into the port.
type InferenceEngine interface {
	// Load initializes an inference session for the artifact already fetched to
	// `localPath` (the ModelFetcher pulled it). It returns the loaded session's
	// observed schema, resident memory estimate, and the digest of the bytes it
	// loaded (which the service compares against any expected digest).
	//
	// WHY split fetch (ModelFetcher) from init (this Load): the two failure modes
	// are different — a fetch failure is a storage/network problem (retryable
	// against another endpoint), an init failure is a model problem (an
	// unsupported op; retrying won't help). Separate ports let the service
	// translate each into the right state/reason. Returns ErrEngineBadInput-class
	// errors wrapped, or a generic error the service maps to StateFailed.
	Load(ctx context.Context, ref ModelRef, localPath string) (LoadedSession, error)

	// Predict runs one inference against the loaded session for `ref`. The
	// service guarantees it only calls this when the model is StateReady. The
	// engine returns the output tensors or an error; a malformed-input error is
	// surfaced as ErrEngineBadInput (wrapped) so the handler can choose
	// InvalidArgument over Internal.
	Predict(ctx context.Context, ref ModelRef, inputs map[string]Tensor) (map[string]Tensor, error)

	// Unload frees the session for `ref`. Idempotent: unloading an
	// already-unloaded session is a no-op (the controller may re-reconcile).
	Unload(ctx context.Context, ref ModelRef) error
}

// LoadedSession is what InferenceEngine.Load reports about a freshly
// initialized session — the server-authoritative metadata the registry records
// on the LoadedModel. It is a port RESULT type (not a request), so it lives with
// the port.
type LoadedSession struct {
	// InputSchema / OutputSchema are the tensor I/O contract the engine read out
	// of the model graph. Surfaced to clients via GetModelInfo.
	InputSchema  []TensorSpec
	OutputSchema []TensorSpec

	// MemoryBytes is the engine's estimate of the session's resident memory.
	MemoryBytes int64

	// Digest is the content digest the engine/fetcher computed over the loaded
	// bytes (e.g. "sha256:..."). The service checks it against any expected
	// digest for supply-chain integrity.
	Digest string
}

// ============================================================================
// ModelFetcher — the object-storage PORT (MinIO/S3)
// ============================================================================
//
// WHY load-by-reference, not by uploading bytes through a control RPC: the
// artifact already lives in object storage (the registry put it there).
// Streaming weights through gRPC would be wasteful and couple the control plane
// to artifact size. The control RPC carries a URI; THIS port pulls it. (KServe's
// storageUri model.)
//
// SECURITY: the SERVICE validates the URI against its allow-list (pure,
// testable — ArtifactURIAllowed below) BEFORE calling Fetch, so a rejected URI
// never reaches this port. The fetcher itself should also enforce its
// configured bucket as defense in depth, but the domain's allow-list check is
// the primary SSRF guard and the one we unit-test here.
type ModelFetcher interface {
	// Fetch downloads the artifact at `uri` to a local path and returns that
	// path plus the content digest it computed while streaming. Returns
	// ErrArtifactNotFound (wrapped) when the object does not exist.
	//
	// The returned digest lets the service verify integrity without re-reading
	// the file. The local path is handed to InferenceEngine.Load.
	Fetch(ctx context.Context, uri string) (localPath string, digest string, err error)
}

// ============================================================================
// Clock — the time PORT (deterministic tests)
// ============================================================================
//
// WHY abstract time at all: the registry stamps LoadedAt/UpdatedAt and the
// metrics measure inference latency. If those used time.Now() directly, tests
// could not assert exact timestamps or latencies — they'd be flaky. A Clock port
// lets tests inject a controllable clock and verify, e.g., that LoadedAt is set
// to the load instant and that a predict's measured latency equals a fixed
// delta. Production wiring passes a realClock (time.Now / time.Since).
//
// This is a tiny but high-leverage seam: it is the difference between
// "metrics math is probably right" and a test that PROVES the latency recorded
// equals the elapsed time.
type Clock interface {
	// Now returns the current instant. Used for state-transition timestamps.
	Now() time.Time
	// Since returns the elapsed time since t, used to measure inference latency.
	// (Equivalent to Now().Sub(t) but lets a fake clock advance deterministically.)
	Since(t time.Time) time.Duration
}

// ============================================================================
// ArtifactURIAllowed — the SSRF / supply-chain allow-list check (pure)
// ============================================================================

// ArtifactURIAllowed reports whether `uri` is permitted by the pod's configured
// allow-list of (scheme://bucket/prefix) entries. This is the SECURITY guard the
// service runs before any LoadModel fetch.
//
// THREAT: LoadModel lets the caller specify WHERE to load from. Without an
// allow-list, a caller (or a replayed/forged control message) could point the
// pod at an attacker-controlled URL — classic SSRF, and a supply-chain hole
// (load arbitrary weights). The allow-list pins the pod to its own bucket/prefix.
//
// SEMANTICS: an entry like "s3://fp-models/" allows any object under that
// bucket+prefix. Matching is a strict PREFIX match on the normalized URI. WHY
// prefix and not substring/glob: substring matching is a footgun
// ("s3://fp-models-evil/" would match a substring rule for "fp-models"); a
// strict prefix on the full "scheme://bucket/prefix" boundary is unambiguous.
// An empty allow-list denies everything (fail-closed) — a misconfigured pod
// refuses all loads rather than accepting any URI.
//
// It is a free function (not a method) because it is a pure predicate over
// inputs with no domain state — easy to test in isolation.
func ArtifactURIAllowed(uri string, allowList []string) bool {
	if uri == "" || len(allowList) == 0 {
		return false // fail-closed: no URI or no allow-list ⇒ deny
	}
	for _, allowed := range allowList {
		if allowed == "" {
			continue // ignore empty entries; they would match everything by prefix
		}
		// Strict prefix match on the full scheme://bucket/prefix boundary.
		if len(uri) >= len(allowed) && uri[:len(allowed)] == allowed {
			return true
		}
	}
	return false
}

// wrapValidation is a tiny helper to produce a %w-wrapped ErrValidation with a
// specific message. Kept here (used by models.go and the service) so the
// wrapping is consistent: callers can errors.Is(err, ErrValidation) while still
// reading a human detail.
func wrapValidation(msg string) error {
	return fmt.Errorf("%w: %s", ErrValidation, msg)
}
