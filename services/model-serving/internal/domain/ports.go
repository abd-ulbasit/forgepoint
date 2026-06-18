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
	"net/url"
	"path"
	"strings"
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
// bucket+prefix. Matching is a STRUCTURED comparison of URL components, NOT a
// raw string prefix:
//
//	scheme   — must match EXACTLY (case-insensitive). "https://" never matches an
//	           "s3://" allow entry. Blocks a scheme-swap (e.g. file:// → SSRF).
//	host     — bucket/host must match EXACTLY (case-insensitive). "fp-models" and
//	           "fp-models-evil" are different hosts, so a prefix-SIBLING bucket can
//	           never sneak in. (User-info / port are also compared so a crafted
//	           authority can't masquerade as the allowed host.)
//	path     — the requested path, after path.Clean, must remain UNDER the allowed
//	           prefix with the boundary aligned on a path SEPARATOR (so prefix
//	           "/models/" matches "/models/a.onnx" but NOT "/models-x/a.onnx"), and
//	           must contain NO parent-dir ("..") segment.
//
// WHY structured and not a raw byte-prefix (the bug this fixes): the old code did
// `uri[:len(allowed)] == allowed`. That is a substring/footgun on three axes:
//
//  1. PATH TRAVERSAL — "s3://fp-models/../secrets/x" starts with "s3://fp-models/"
//     as raw bytes, so it passed; but it resolves OUTSIDE the prefix. We now reject
//     any URI whose RAW path carries a ".." SEGMENT (checked before path.Clean —
//     see splitArtifactURI for why cleaning first would HIDE a root-absorbed "/..")
//     and then prove the cleaned path stays under the prefix.
//  2. PREFIX-SIBLING HOST/BUCKET — an allow entry "s3://fp-models" (no trailing
//     slash) would raw-prefix-match "s3://fp-models-evil/...". Comparing the HOST
//     component for EXACT equality kills this regardless of trailing slashes.
//  3. SCHEME CONFUSION — a raw prefix can't reason about scheme boundaries;
//     comparing url.Scheme exactly does.
//
// An empty allow-list (or empty URI) denies everything (fail-closed) — a
// misconfigured pod refuses all loads rather than accepting any URI. A malformed
// URI, or one that does not parse into a scheme+host, is likewise denied.
//
// INTERVIEW: "How do you stop a path-traversal escape from an allow-list?" Parse
// both sides, compare scheme+host exactly, reject any ".." segment in the raw path
// (because path.Clean would absorb a root-level "/.." and hide the escape), and
// prove the cleaned request path is a child of the allowed prefix with a
// separator-aligned boundary — never a raw string prefix, which conflates
// "fp-models/" with "fp-models-evil/".
//
// It is a free function (not a method) because it is a pure predicate over
// inputs with no domain state — easy to test in isolation.
func ArtifactURIAllowed(uri string, allowList []string) bool {
	if uri == "" || len(allowList) == 0 {
		return false // fail-closed: no URI or no allow-list ⇒ deny
	}
	reqScheme, reqHost, reqPath, ok := splitArtifactURI(uri)
	if !ok {
		return false // unparseable / schemeless / hostless ⇒ deny
	}
	for _, allowed := range allowList {
		if allowed == "" {
			continue // ignore empty entries; they would otherwise match everything
		}
		alScheme, alHost, alPath, ok := splitArtifactURI(allowed)
		if !ok {
			continue // a malformed allow entry can never grant access
		}
		// Scheme + host (authority) must match EXACTLY. EqualFold makes the
		// comparison case-insensitive (schemes and DNS hostnames are), so
		// "S3://FP-Models/" and "s3://fp-models/" are the same entry — but
		// "fp-models" and "fp-models-evil" never are.
		if !strings.EqualFold(reqScheme, alScheme) || !strings.EqualFold(reqHost, alHost) {
			continue
		}
		if pathUnderPrefix(reqPath, alPath) {
			return true
		}
	}
	return false
}

// splitArtifactURI parses an allow-list entry or a requested URI into its
// (scheme, authority, cleaned-path) components. It returns ok=false for anything
// that:
//
//   - fails to parse, OR
//   - has no scheme, OR
//   - has no host AND is not the file scheme (file:///abs/path legitimately has an
//     empty authority — RFC 8089 — so we allow it there but nowhere else; a
//     schemeless/hostless value for any other scheme collapses to a bare path and
//     re-opens the substring footgun), OR
//   - contains a literal parent-dir ("..") segment ANYWHERE in its RAW path.
//
// THE "..": WHY WE CHECK THE RAW PATH AND FAIL CLOSED. This is the crux of the
// traversal fix. Go's path.Clean resolves ".." against earlier segments AND, at
// the root, silently DROPS a leading ".." (path.Clean("/../secrets") == "/secrets").
// So if we cleaned first and only then looked for residual "..", the escape would
// already be absorbed and "/secrets" would look like a legitimate child of the
// bucket root. In object-storage keys and mount paths a ".." segment is NEVER
// legitimate, so the correct, unambiguous guard is: detect ".." in the UN-cleaned
// path and reject the whole URI. (This also covers percent-encoded "%2e%2e" because
// url.Parse decodes u.Path before we inspect it.)
//
// The authority returned is the FULL authority (user-info + host + port), so a
// crafted "user@host" or "host:port" cannot masquerade as the allowed bare host.
func splitArtifactURI(raw string) (scheme, authority, cleanedPath string, ok bool) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", "", false
	}
	if u.Scheme == "" {
		return "", "", "", false
	}
	// host is required for every scheme EXCEPT file:// (file:///abs/path has an
	// empty authority by RFC 8089). For file we substitute a fixed sentinel host so
	// scheme+host comparison still works uniformly (both sides get "" → sentinel).
	host := u.Host
	if host == "" {
		if u.Scheme != "file" {
			return "", "", "", false
		}
		host = "localhost" // canonical empty-authority file host
	}
	// REJECT any ".." segment in the RAW path (see the doc above). hasParentDirSegment
	// inspects the un-cleaned, url.Parse-decoded path so a root-absorbed "/../" can't
	// hide the escape.
	if hasParentDirSegment(u.Path) {
		return "", "", "", false
	}
	// u.Host already includes host[:port]; prepend user-info if present so the
	// whole authority must match (no "evil@fp-models" smuggling).
	auth := host
	if u.User != nil {
		auth = u.User.String() + "@" + host
	}
	// path.Clean (forward-slash semantics — URI paths are always '/'-separated,
	// regardless of the host OS) collapses redundant separators and "." segments.
	// With ".." already rejected above, Clean here is purely cosmetic normalization.
	// An empty path normalizes to "/" so a bucket-root entry ("s3://fp-models") and
	// ("s3://fp-models/") behave alike.
	p := u.Path
	if p == "" {
		p = "/"
	}
	return u.Scheme, auth, path.Clean(p), true
}

// hasParentDirSegment reports whether p contains a ".." path SEGMENT (not merely
// the substring ".." — a filename like "model..onnx" is fine). It splits on "/"
// and looks for an exact ".." element, so "/a/../b", "/../b", "/a/.." and a bare
// ".." all trip it, while "/a..b/c" and "/..foo/x" do not.
func hasParentDirSegment(p string) bool {
	for _, seg := range strings.Split(p, "/") {
		if seg == ".." {
			return true
		}
	}
	return false
}

// pathUnderPrefix reports whether cleanedReq is the allowed prefix itself or a
// descendant of it, with the boundary aligned on a path SEPARATOR. Both inputs are
// already free of ".." segments (splitArtifactURI rejected any), so this is purely
// a structural under-prefix test.
//
//	prefix "/models" (cleans to "/models"):
//	  "/models"          → allowed (exact)
//	  "/models/iris.onnx"→ allowed (child, boundary on '/')
//	  "/models-evil/x"   → DENIED  (boundary not on '/': "/models-evil" ≠ child)
func pathUnderPrefix(cleanedReq, allowedPrefix string) bool {
	prefix := path.Clean(allowedPrefix)
	if cleanedReq == prefix {
		return true // exact match (the prefix root itself)
	}
	// Root prefix "/" is under-prefix for everything absolute (any path is its
	// child). Otherwise require a separator-aligned boundary: cleanedReq must start
	// with prefix + "/", so "/models" does NOT swallow "/models-evil".
	if prefix == "/" {
		return strings.HasPrefix(cleanedReq, "/")
	}
	return strings.HasPrefix(cleanedReq, prefix+"/")
}

// wrapValidation is a tiny helper to produce a %w-wrapped ErrValidation with a
// specific message. Kept here (used by models.go and the service) so the
// wrapping is consistent: callers can errors.Is(err, ErrValidation) while still
// reading a human detail.
func wrapValidation(msg string) error {
	return fmt.Errorf("%w: %s", ErrValidation, msg)
}
