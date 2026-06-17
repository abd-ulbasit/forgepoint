// Package artifactstore holds the ModelFetcher ADAPTERS — the concrete
// implementations of the domain's object-storage PORT (domain.ModelFetcher).
//
// ============================================================================
// WHERE THIS SITS IN THE ARCHITECTURE (the inward arrow)
// ============================================================================
//
//	domain  (defines ModelFetcher PORT + LoadModel logic)   ← stdlib + uuid only
//	   ▲
//	   │ implements
//	repository/artifactstore  (THIS PACKAGE — fetches ONNX bytes)
//
// The domain's LoadModel flow calls fetcher.Fetch(ctx, uri) → (localPath,
// digest, err). It NEVER knows whether the bytes came from a file mount, an
// HTTP endpoint, or (eventually) a native MinIO/S3 client. That seam is the
// whole point of the port: the serving runtime stays a "dumb, replaceable"
// sidecar precisely because the fetch mechanism is abstracted away.
//
// ============================================================================
// WHY TWO STDLIB-ONLY ADAPTERS (file:// and http(s)://) — AND MinIO AS FOLLOW-UP
// ============================================================================
//
// The canonical production source is MinIO/S3 (the registry uploads artifacts
// there; the serving pod pulls them — KServe's storageUri model). A *native*
// MinIO/S3 client (minio-go or aws-sdk-go-v2) would be the ideal adapter, BUT:
//
//   - minio-go is NOT in this module's go.sum (it only appears transitively in
//     pkg/ via the testcontainers minio module), and aws-sdk-go-v2 is nowhere
//     in the workspace. Adding either requires `go get`, which this change is
//     explicitly forbidden from doing (it would cascade go.sum/go.work edits).
//
// So this adapter ships TWO dependency-free fetchers that cover the realistic
// K8s artifact-delivery paths today, and flags the native MinIO client as a
// follow-up wiring concern (see FOLLOW-UP below):
//
//  1. FileFetcher (file:// URIs) — the artifact is on a path the pod can read:
//     a PVC, a CSI-mounted bucket (e.g. mountpoint-s3 / gcsfuse), or an
//     init-container that already pulled it. This is the MOST common pattern in
//     real serving stacks: an init-container does the S3 pull once, the serving
//     container just reads the local file. Zero deps, fully testable on disk.
//
//  2. HTTPFetcher (http:// / https:// URIs) — the artifact is fetched over HTTP:
//     a MinIO/S3 PRESIGNED URL (the registry mints one and passes it in the
//     LoadModel control message), or an in-cluster artifact proxy. This is the
//     path a native MinIO client would ultimately wrap — minio-go's GetObject is
//     an authenticated HTTP GET under the hood. So the HTTP fetcher is not a toy;
//     it is the same wire protocol, minus the SigV4 signing the presigned URL
//     already carries.
//
// FOLLOW-UP (flagged, not hidden in a TODO comment in shipped code): when
// minio-go is added to this module's go.mod (a separate, dependency-bumping
// change), add a third adapter `MinIOFetcher` implementing the same
// domain.ModelFetcher interface with minio-go's GetObject + bucket-scoped
// credentials. It slots in behind the SAME port with NO change to the domain or
// handler — that substitutability is exactly what the port buys us. Until then,
// production wiring uses presigned-URL HTTP (registry-minted) or a file mount.
//
// ============================================================================
// THE DIGEST CONTRACT (supply-chain integrity)
// ============================================================================
//
// Every adapter streams the artifact through a sha256 hasher WHILE copying it to
// the local temp file (io.MultiWriter — one pass over the bytes, no re-read) and
// returns the digest as "sha256:<hex>". The domain compares this string EXACTLY
// (domain.digestMatches is a plain ==, prefix included) against any expected
// digest from config/registry, so the canonical form here MUST match what the
// registry stamps. We standardize on the lowercase "sha256:"-prefixed hex form —
// the same form OCI/containerd/registry tooling uses — so the two sides agree.
package artifactstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/model-serving/internal/domain"
)

// digestPrefix is the canonical algorithm prefix we emit on every digest. Kept
// as a const so the format is defined in exactly one place and the HTTP/file
// adapters can never drift from each other (or from what the registry stamps).
const digestPrefix = "sha256:"

// hashStream copies r → w while computing the sha256 of the bytes in a SINGLE
// pass (io.MultiWriter fans the same byte stream to both the destination file
// and the hasher). It returns the canonical "sha256:<hex>" digest.
//
// WHY one pass and not "copy then re-read to hash": the artifact can be hundreds
// of MB; reading it twice doubles I/O and, for the HTTP case, would require
// buffering the whole body in memory or issuing a second request. MultiWriter
// gives us the digest for free as the bytes flow to disk.
//
// It honors ctx implicitly: the caller passes a ctx-bound reader (the HTTP
// response body, which is cancelled when ctx is) so a cancelled context aborts
// the copy mid-stream with a context error.
func hashStream(dst io.Writer, src io.Reader) (digest string, n int64, err error) {
	h := sha256.New()
	// MultiWriter: every Write to `mw` goes to BOTH dst (the temp file) and h
	// (the hasher). io.Copy pumps src → mw in 32KiB chunks, so memory stays flat
	// regardless of artifact size.
	mw := io.MultiWriter(dst, h)
	n, err = io.Copy(mw, src)
	if err != nil {
		return "", n, err
	}
	return digestPrefix + hex.EncodeToString(h.Sum(nil)), n, nil
}

// ============================================================================
// FileFetcher — file:// adapter (the init-container / PVC / CSI-mount path)
// ============================================================================

// FileFetcher implements domain.ModelFetcher for file:// URIs: the artifact is
// already on a filesystem path the pod can read (a PVC, a CSI-mounted bucket, or
// an init-container that pre-pulled it). Fetch copies it into the pod's own
// scratch dir (so the engine loads from a path THIS process owns) and computes
// the digest in the same pass.
//
// WHY copy at all instead of returning the source path directly: (1) it gives a
// single, predictable streaming point to compute the digest; (2) it decouples
// the engine's load from the source mount's lifetime (a CSI unmount mid-load
// can't yank the bytes out from under the ONNX session); (3) it normalizes the
// path the engine sees regardless of source scheme. The cost is one local copy —
// negligible next to the network pull the init-container already did.
type FileFetcher struct {
	// destDir is the pod-owned scratch directory artifacts are copied into. Each
	// Fetch writes a uniquely-named temp file here so concurrent loads (a future
	// multi-model variant) cannot collide. Created lazily on first Fetch.
	destDir string

	// allowedRoots, when non-empty, restricts the resolved source path to live
	// under one of these absolute directory roots — defense in depth against a
	// path-traversal URI (e.g. file:///etc/shadow). The DOMAIN's allow-list
	// (ArtifactURIAllowed) is the primary SSRF guard and runs before Fetch is
	// ever called; this is the adapter-level belt-and-suspenders the design doc
	// calls for ("the fetcher itself should also enforce its configured bucket as
	// defense in depth"). Empty roots = no extra restriction (the domain guard
	// still applies upstream).
	allowedRoots []string
}

// NewFileFetcher constructs a FileFetcher writing copies into destDir. Pass the
// allow-listed source roots (already resolved to absolute paths) to enable the
// adapter-level traversal guard; pass nil to rely solely on the domain's
// upstream allow-list.
func NewFileFetcher(destDir string, allowedRoots ...string) *FileFetcher {
	abs := make([]string, 0, len(allowedRoots))
	for _, r := range allowedRoots {
		if r == "" {
			continue
		}
		a, err := filepath.Abs(r)
		if err != nil {
			continue
		}
		// Resolve symlinks on the ROOT too, so the under-root check compares both
		// sides in the same (fully-resolved) namespace. WHY this matters: on macOS
		// /var is a symlink to /private/var (and similar aliasing exists on many
		// systems). If we store the root lexically as "/var/x" but EvalSymlinks the
		// source to "/private/var/x", a legitimately-inside file would look like it
		// escapes the root. Resolving both sides removes that false-positive while
		// KEEPING the security property (a symlink that genuinely points outside
		// still resolves outside and is rejected).
		if resolved, rerr := filepath.EvalSymlinks(a); rerr == nil {
			a = resolved
		}
		abs = append(abs, a)
	}
	return &FileFetcher{destDir: destDir, allowedRoots: abs}
}

// Fetch satisfies domain.ModelFetcher. It parses the file:// URI, validates the
// resolved path against allowedRoots (if configured), streams the file into the
// pod's scratch dir computing the sha256, and returns the local copy's path +
// digest. A missing source file is mapped to domain.ErrArtifactNotFound (wrapped)
// so the service can render the "artifact not found" reason distinctly.
func (f *FileFetcher) Fetch(ctx context.Context, uri string) (localPath string, digest string, err error) {
	// Honor an already-cancelled context before doing any work.
	if err = ctx.Err(); err != nil {
		return "", "", err
	}

	srcPath, err := filePathFromURI(uri)
	if err != nil {
		return "", "", err
	}

	// Adapter-level traversal guard: the resolved absolute path must sit under an
	// allowed root. EvalSymlinks defeats a symlink that points outside the root.
	if len(f.allowedRoots) > 0 {
		if err = f.checkUnderRoot(srcPath); err != nil {
			return "", "", err
		}
	}

	src, err := os.Open(srcPath) //nolint:gosec // path validated above + by domain allow-list
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// Map "missing object" to the storage sentinel the service branches on.
			return "", "", fmt.Errorf("%w: %s", domain.ErrArtifactNotFound, srcPath)
		}
		return "", "", fmt.Errorf("artifactstore: open source %q: %w", srcPath, err)
	}
	defer src.Close()

	dst, cleanup, err := f.createTemp(srcPath)
	if err != nil {
		return "", "", err
	}

	digest, _, err = hashStream(dst, src)
	// Close the temp file before returning regardless of outcome (flush + fd).
	closeErr := dst.Close()
	if err != nil {
		cleanup() // remove the partial copy — a half-written artifact must never be loaded
		return "", "", fmt.Errorf("artifactstore: copy %q: %w", srcPath, err)
	}
	if closeErr != nil {
		cleanup()
		return "", "", fmt.Errorf("artifactstore: close temp copy: %w", closeErr)
	}
	return dst.Name(), digest, nil
}

// checkUnderRoot reports whether srcPath resolves (symlinks evaluated) to a
// location under one of the configured allowed roots.
func (f *FileFetcher) checkUnderRoot(srcPath string) error {
	resolved, err := filepath.EvalSymlinks(srcPath)
	if err != nil {
		// If the file doesn't exist yet, EvalSymlinks fails; fall back to the
		// lexical absolute path so a genuinely-missing file still gets the
		// ErrArtifactNotFound treatment in Fetch (not a permission-style reject).
		resolved = srcPath
	}
	resolvedAbs, err := filepath.Abs(resolved)
	if err != nil {
		return fmt.Errorf("artifactstore: resolve path %q: %w", srcPath, err)
	}
	for _, root := range f.allowedRoots {
		// filepath.Rel + a ".." check is the robust "is under" test: if the
		// relative path from root to target starts with "..", target escapes root.
		rel, relErr := filepath.Rel(root, resolvedAbs)
		if relErr != nil {
			continue
		}
		if rel == "." || (!strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != "..") {
			return nil // under (or equal to) an allowed root
		}
	}
	return fmt.Errorf("%w: path %q is outside allowed roots", domain.ErrArtifactNotFound, resolvedAbs)
}

// createTemp opens a fresh, uniquely-named temp file in destDir (created if
// absent) carrying the source's base name as a suffix for debuggability. Returns
// the open file and a cleanup func that removes it (used to discard partials).
func (f *FileFetcher) createTemp(srcPath string) (*os.File, func(), error) {
	dir := f.destDir
	if dir == "" {
		dir = os.TempDir()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, func() {}, fmt.Errorf("artifactstore: ensure dest dir %q: %w", dir, err)
	}
	// Pattern "artifact-*-<base>" → os.CreateTemp inserts random entropy at the
	// "*", guaranteeing uniqueness across concurrent fetches.
	pattern := "artifact-*-" + filepath.Base(srcPath)
	dst, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return nil, func() {}, fmt.Errorf("artifactstore: create temp in %q: %w", dir, err)
	}
	name := dst.Name()
	return dst, func() { _ = os.Remove(name) }, nil
}

// filePathFromURI extracts the filesystem path from a file:// URI. It accepts
// both "file:///abs/path" (the correct RFC-8089 form) and a bare "/abs/path"
// (lenient, since operators frequently pass plain mount paths). It rejects a
// URI with a host component (file://host/path) because a remote file host is
// not something this pod can read and is a likely misconfiguration/SSRF attempt.
func filePathFromURI(uri string) (string, error) {
	if uri == "" {
		return "", fmt.Errorf("%w: empty artifact URI", domain.ErrArtifactNotFound)
	}
	// Bare absolute path (no scheme) — accept as-is.
	if strings.HasPrefix(uri, "/") {
		return filepath.Clean(uri), nil
	}
	u, err := url.Parse(uri)
	if err != nil {
		return "", fmt.Errorf("artifactstore: parse file URI %q: %w", uri, err)
	}
	if u.Scheme != "file" {
		return "", fmt.Errorf("artifactstore: FileFetcher only handles file:// URIs, got scheme %q", u.Scheme)
	}
	if u.Host != "" && u.Host != "localhost" {
		// file://host/path means a remote host — not readable locally; refuse it
		// rather than silently treating the host as part of the path.
		return "", fmt.Errorf("artifactstore: file URI must be local (host=%q not allowed)", u.Host)
	}
	return filepath.Clean(u.Path), nil
}

// ============================================================================
// HTTPFetcher — http:// / https:// adapter (presigned-URL / proxy path)
// ============================================================================

// HTTPFetcher implements domain.ModelFetcher for http(s):// URIs. It is the
// adapter for a MinIO/S3 PRESIGNED URL (the registry mints one and embeds it in
// the LoadModel control message) or an in-cluster artifact proxy. Under the
// hood, minio-go's GetObject is an authenticated HTTP GET — this fetcher is the
// same wire path with the signature already baked into the URL.
//
// SECURITY: as with FileFetcher, the DOMAIN's allow-list (ArtifactURIAllowed)
// is the primary SSRF guard and runs before Fetch. The adapter adds two more
// defenses: a bounded read (maxBytes) so a hostile/buggy endpoint cannot fill
// the pod's disk, and a request timeout so a slow-loris source cannot hang the
// load forever.
type HTTPFetcher struct {
	client   *http.Client // injectable for tests (httptest) and for custom transports/TLS
	destDir  string       // pod-owned scratch dir for the downloaded copy
	maxBytes int64        // hard cap on artifact size (0 = unlimited; production sets a real cap)
}

// HTTPOption configures an HTTPFetcher (functional-options so the constructor
// stays additive — new knobs don't break existing call sites).
type HTTPOption func(*HTTPFetcher)

// WithHTTPClient injects a custom *http.Client. Tests pass httptest's client;
// production can pass one with a tuned transport, mTLS, or proxy settings.
func WithHTTPClient(c *http.Client) HTTPOption {
	return func(f *HTTPFetcher) {
		if c != nil {
			f.client = c
		}
	}
}

// WithMaxBytes caps the number of bytes Fetch will read from a response. A
// response exceeding the cap is an error and the partial file is discarded.
// WHY a cap: without it a misconfigured presigned URL pointing at a huge object
// (or a malicious endpoint streaming /dev/zero) fills the pod's ephemeral disk
// and crashes it — a trivial DoS. The operator sets this to (max model size +
// margin).
func WithMaxBytes(n int64) HTTPOption {
	return func(f *HTTPFetcher) { f.maxBytes = n }
}

// WithRequestTimeout sets a ceiling on the whole fetch (connect + stream). It is
// layered ON TOP of the caller's ctx: whichever fires first cancels the request.
func WithRequestTimeout(d time.Duration) HTTPOption {
	return func(f *HTTPFetcher) {
		if d > 0 {
			// Clone the client so we don't mutate a shared/injected one.
			c := *f.client
			c.Timeout = d
			f.client = &c
		}
	}
}

// NewHTTPFetcher constructs an HTTPFetcher writing downloads into destDir.
// Defaults: a 30s-timeout http.Client and no size cap (set one in production via
// WithMaxBytes). Apply options to override.
func NewHTTPFetcher(destDir string, opts ...HTTPOption) *HTTPFetcher {
	f := &HTTPFetcher{
		client:  &http.Client{Timeout: 30 * time.Second},
		destDir: destDir,
	}
	for _, o := range opts {
		o(f)
	}
	return f
}

// Fetch satisfies domain.ModelFetcher. It issues a GET for the URI, streams the
// body into the pod's scratch dir computing the sha256, and returns the local
// path + digest. A 404 (or 410) is mapped to domain.ErrArtifactNotFound so the
// service renders the distinct "not found" reason; other non-2xx codes and
// transport errors are returned as generic fetch errors (which the service maps
// to a generic StateFailed reason).
func (f *HTTPFetcher) Fetch(ctx context.Context, uri string) (localPath string, digest string, err error) {
	if err = ctx.Err(); err != nil {
		return "", "", err
	}

	u, err := url.Parse(uri)
	if err != nil {
		return "", "", fmt.Errorf("artifactstore: parse http URI %q: %w", uri, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", "", fmt.Errorf("artifactstore: HTTPFetcher only handles http(s):// URIs, got scheme %q", u.Scheme)
	}

	// Bind the request to the caller's ctx so a cancelled load aborts the GET
	// (closes the socket mid-stream) rather than leaking it.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return "", "", fmt.Errorf("artifactstore: build request: %w", err)
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("artifactstore: GET %q: %w", redact(u), err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone:
		// 404/410 → the object isn't there. Map to the storage sentinel.
		return "", "", fmt.Errorf("%w: GET %s returned %d", domain.ErrArtifactNotFound, redact(u), resp.StatusCode)
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		// Any other non-2xx (403 bad signature, 500 backend, …) is a generic
		// fetch failure. We do NOT include the body (could carry credentials in an
		// error page) — only the status code.
		return "", "", fmt.Errorf("artifactstore: GET %s returned status %d", redact(u), resp.StatusCode)
	}

	// Apply the size cap (if any) by wrapping the body in a bounded reader.
	var body io.Reader = resp.Body
	if f.maxBytes > 0 {
		// +1 so reading exactly maxBytes+1 trips the overflow check below.
		body = io.LimitReader(resp.Body, f.maxBytes+1)
	}

	dst, cleanup, err := f.createTemp(u)
	if err != nil {
		return "", "", err
	}

	digest, n, err := hashStream(dst, body)
	closeErr := dst.Close()
	if err != nil {
		cleanup()
		return "", "", fmt.Errorf("artifactstore: stream body from %s: %w", redact(u), err)
	}
	if closeErr != nil {
		cleanup()
		return "", "", fmt.Errorf("artifactstore: close temp copy: %w", closeErr)
	}
	if f.maxBytes > 0 && n > f.maxBytes {
		cleanup()
		return "", "", fmt.Errorf("artifactstore: artifact from %s exceeds max size %d bytes", redact(u), f.maxBytes)
	}
	return dst.Name(), digest, nil
}

// createTemp opens a uniquely-named temp file in destDir, named after the URI's
// last path segment for debuggability. Mirrors FileFetcher.createTemp.
func (f *HTTPFetcher) createTemp(u *url.URL) (*os.File, func(), error) {
	dir := f.destDir
	if dir == "" {
		dir = os.TempDir()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, func() {}, fmt.Errorf("artifactstore: ensure dest dir %q: %w", dir, err)
	}
	base := filepath.Base(u.Path)
	if base == "" || base == "." || base == "/" {
		base = "artifact"
	}
	dst, err := os.CreateTemp(dir, "artifact-*-"+base)
	if err != nil {
		return nil, func() {}, fmt.Errorf("artifactstore: create temp in %q: %w", dir, err)
	}
	name := dst.Name()
	return dst, func() { _ = os.Remove(name) }, nil
}

// redact returns a log/error-safe rendering of the URL with the query string
// stripped. WHY: a presigned S3/MinIO URL carries the signature and access key
// in the query (X-Amz-Credential, X-Amz-Signature). Echoing the full URL into a
// StateFailed reason (which surfaces in ModelStatus.message and logs) would leak
// short-lived credentials. We log only scheme://host/path.
func redact(u *url.URL) string {
	return u.Scheme + "://" + u.Host + u.Path
}

// ============================================================================
// Dispatcher — pick the right adapter by URI scheme
// ============================================================================

// Fetcher is a scheme-dispatching domain.ModelFetcher: it routes file:// to a
// FileFetcher and http(s):// to an HTTPFetcher. This is what main.go wires as
// the single fetcher; the domain sees one ModelFetcher and is oblivious to the
// scheme split. (When a native MinIOFetcher is added, register it for s3:// here
// and nothing else changes — the substitutability the port promises.)
type Fetcher struct {
	file *FileFetcher
	http *HTTPFetcher
}

// NewFetcher builds a scheme dispatcher from a file and an http adapter. Either
// may be nil to refuse that scheme (a pod that should only load from a mount can
// pass a nil http adapter, hard-denying remote fetches as defense in depth).
func NewFetcher(file *FileFetcher, httpF *HTTPFetcher) *Fetcher {
	return &Fetcher{file: file, http: httpF}
}

// Fetch satisfies domain.ModelFetcher by dispatching on the URI scheme.
func (d *Fetcher) Fetch(ctx context.Context, uri string) (string, string, error) {
	scheme := schemeOf(uri)
	switch scheme {
	case "file", "": // "" = bare path → treat as file
		if d.file == nil {
			return "", "", fmt.Errorf("%w: file:// fetching is disabled on this pod", domain.ErrArtifactNotFound)
		}
		return d.file.Fetch(ctx, uri)
	case "http", "https":
		if d.http == nil {
			return "", "", fmt.Errorf("%w: http(s):// fetching is disabled on this pod", domain.ErrArtifactNotFound)
		}
		return d.http.Fetch(ctx, uri)
	default:
		// s3://, gs://, etc. — not yet supported (native client is the follow-up).
		return "", "", fmt.Errorf("%w: unsupported artifact URI scheme %q (file/http only; MinIO native client is a follow-up)", domain.ErrArtifactNotFound, scheme)
	}
}

// schemeOf returns the lowercase scheme of a URI, or "" for a bare path.
func schemeOf(uri string) string {
	if strings.HasPrefix(uri, "/") {
		return ""
	}
	u, err := url.Parse(uri)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Scheme)
}

// Compile-time assertions: all three types satisfy the domain port. If the port
// signature changes, THIS line fails to compile — a cheap, immediate guard that
// the adapters stay in lockstep with the interface they implement.
var (
	_ domain.ModelFetcher = (*FileFetcher)(nil)
	_ domain.ModelFetcher = (*HTTPFetcher)(nil)
	_ domain.ModelFetcher = (*Fetcher)(nil)
)
