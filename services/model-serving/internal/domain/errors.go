// errors.go — sentinel errors owned by the model-serving domain layer.
//
// ============================================================================
// WHY DOMAIN (BUSINESS) ERRORS ARE DISTINCT FROM PORT (INFRASTRUCTURE) ERRORS
// ============================================================================
//
// The domain service is the single TRANSLATION POINT between two vocabularies:
//
//   - PORT outcomes (ports.go): "the object store returned 404", "the ONNX
//     session failed to initialize", "the configured URI is outside the
//     allow-list". These describe what the infrastructure did.
//   - BUSINESS outcomes (here): "this model is not ready to serve", "the
//     request targets a model this pod does not host", "the artifact URI is
//     not permitted". These are the facts the gRPC handler maps to status codes.
//
// The handler imports ONLY this domain package; it never sees a port error.
// errors.Is(err, domain.ErrModelNotReady) → codes.FailedPrecondition, etc.
//
// WHY errors.Is-friendly sentinels (not error strings): downstream layers must
// branch on the KIND of failure (route elsewhere vs. reject vs. retry) without
// brittle string matching. A sentinel + %w wrapping gives both a stable
// identity (errors.Is) and a specific human message (fmt.Errorf "%w: detail").
package domain

import "errors"

var (
	// ErrModelNotFound is returned when a control/data-plane call targets a
	// (name, version) the pod does not currently host in its registry. The
	// handler maps it to codes.NotFound.
	//
	// On a single-model pod this is the common "you routed to the wrong pod"
	// signal — it is DEFENSE IN DEPTH against a gateway routing bug: rather than
	// silently serving the one model it does have, the pod refuses and surfaces
	// the mismatch so the bug is visible (see the proto's PredictRequest doc).
	ErrModelNotFound = errors.New("serving: model not found on this pod")

	// ErrModelNotReady is returned by Predict when the targeted model exists in
	// the registry but is not in StateReady (it is still Downloading/Loading, or
	// it Failed, or it was Unloaded). The handler maps it to
	// codes.FailedPrecondition so the gateway can route the request elsewhere
	// (another replica / version) instead of treating it as a hard failure.
	//
	// WHY a precondition, not a not-found: the model is the right one, the pod is
	// the right pod — it is simply not in a serveable STATE yet. That distinction
	// lets the gateway retry/route rather than give up.
	ErrModelNotReady = errors.New("serving: model not ready to serve predictions")

	// ErrModelAlreadyExists is returned by a load request that would overwrite a
	// DIFFERENT artifact already registered under the same (name, version). It is
	// NOT returned for an idempotent re-load of the SAME (name, version, digest)
	// — that is a no-op success (see LoadModel). It guards the invariant that a
	// given (name, version) maps to exactly one artifact digest for the pod's
	// lifetime, so two concurrent deploys cannot silently swap weights.
	ErrModelAlreadyExists = errors.New("serving: model version already loaded with a different artifact")

	// ErrArtifactURINotAllowed is returned when a LoadModel request's artifact
	// URI is outside the pod's configured allow-list (bucket/prefix). The handler
	// maps it to codes.PermissionDenied.
	//
	// SECURITY (SSRF / supply-chain): the caller specifies WHERE to load from,
	// but a serving pod must never be pointable at an arbitrary attacker URL.
	// The allow-list is checked in the domain (pure, testable) BEFORE the
	// ModelFetcher port is ever called, so a rejected URI never touches the
	// network. See validateArtifactURI / ArtifactURIAllowed.
	ErrArtifactURINotAllowed = errors.New("serving: artifact URI is not within the allowed bucket/prefix")

	// ErrDigestMismatch is returned when an expected digest was supplied on load
	// but the fetched artifact's digest did not match. The load transitions the
	// model to StateFailed with this reason. Integrity / supply-chain protection:
	// the pod refuses to serve weights it cannot prove are the ones the registry
	// produced.
	ErrDigestMismatch = errors.New("serving: artifact digest does not match expected digest")

	// ErrModelRequestMismatch is returned by Predict when the request's
	// model_name/version does not match the model actually resident on the pod
	// (and the request did not leave them empty to mean "whatever this pod
	// serves"). Maps to codes.FailedPrecondition — see ErrModelNotReady rationale
	// (catch a routing bug instead of mis-serving). Kept distinct from
	// ErrModelNotFound: the pod HOSTS a model, the request just asked for a
	// different one.
	ErrModelRequestMismatch = errors.New("serving: request targets a model this pod does not serve")

	// ErrValidation is returned for malformed input the service rejects before
	// touching any port (empty model name on load, missing artifact URI, a load
	// of zero bytes, etc.). Maps to codes.InvalidArgument. Wrapped with %w so a
	// caller gets a specific message while still being able to
	// errors.Is(err, ErrValidation).
	ErrValidation = errors.New("serving: validation failed")

	// ErrInferenceFailed wraps an InferenceEngine port failure during Predict
	// (bad input shape the runtime rejected, an internal ONNX error). It is
	// distinct from ErrModelNotReady (a pre-flight state check) — the model WAS
	// ready, the inference itself failed. Maps to codes.Internal (or
	// codes.InvalidArgument if the engine signalled a client input error; the
	// handler decides from the wrapped detail).
	ErrInferenceFailed = errors.New("serving: inference failed")
)

// ============================================================================
// PORT (INFRASTRUCTURE) SENTINELS — returned BY the ports, translated by the
// service into the business errors above.
// ============================================================================

var (
	// ErrArtifactNotFound is the STORAGE sentinel a ModelFetcher returns when the
	// requested artifact URI does not exist in object storage. The service
	// translates it into a StateFailed transition with a "artifact not found"
	// reason (a load can't succeed if the bytes aren't there).
	ErrArtifactNotFound = errors.New("serving: artifact not found in object storage")

	// ErrEngineBadInput is the sentinel an InferenceEngine returns when the
	// caller's tensors are malformed for THIS model (wrong dtype, wrong shape,
	// missing a required input). The service wraps it as ErrInferenceFailed but
	// the handler may inspect for it to choose codes.InvalidArgument over
	// codes.Internal — a client error, not a server fault.
	ErrEngineBadInput = errors.New("serving: engine rejected inputs as malformed")
)
