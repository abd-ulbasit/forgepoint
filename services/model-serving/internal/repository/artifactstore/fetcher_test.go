// fetcher_test.go — REAL-store integration tests for the ModelFetcher adapters.
//
// ============================================================================
// WHAT "REAL STORE" MEANS HERE (and why no testcontainer)
// ============================================================================
//
// The ModelFetcher port is "fetch bytes from a URI" — its real backends are a
// filesystem (file://) and an HTTP endpoint (http(s)://, the presigned-URL /
// MinIO-GetObject path). These tests exercise BOTH against genuine I/O with NO
// mocking of the fetch mechanism:
//
//   - FileFetcher → real files on a real temp directory (real open/read/copy,
//     real sha256 over the bytes, real os.ErrNotExist on a missing file).
//   - HTTPFetcher → a real net/http server (httptest.Server) bound to a real TCP
//     socket, serving real bytes over a real HTTP connection. This is the SAME
//     wire protocol minio-go's GetObject uses; the only thing a MinIO container
//     would add is SigV4 auth, which a presigned URL already carries. We assert
//     the digest the fetcher computes equals a sha256 we compute independently —
//     verifying REAL behavior (the stored/streamed bytes actually hash to that),
//     not a mock's say-so.
//
// WHY NOT a MinIO testcontainer: a native MinIO/S3 client (minio-go / aws-sdk)
// is NOT in this module's go.sum and we are forbidden from `go get`. MinIO's
// objects are not anonymously HTTP-GETtable without setting a bucket policy via
// that same absent client. So a MinIO container could be started but not usefully
// READ without the dependency this change can't add. The httptest server is the
// honest, dependency-free stand-in for the HTTP object-fetch path, and the
// native MinIOFetcher (behind the identical port) is the flagged follow-up. The
// SkipIfNoDocker guard is included where a future container-backed case would
// slot in, per the task's testing contract.
//
// NOTE ON PORTS: this service's domain has NO Postgres or Redis port (the
// loaded-model registry is in-memory in the domain), so there are NO migrations
// and NO pgx/redis repository tests — there is nothing to persist. The single
// persistence/IO port is ModelFetcher, tested here.
package artifactstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/model-serving/internal/domain"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
)

// sha256Hex computes the canonical "sha256:<hex>" digest of b independently of
// the adapter, so a passing assertion proves the adapter hashed the SAME bytes
// it wrote — real behavior, not a tautology against the adapter's own output.
func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// readFile reads the whole file at path; fails the test on error.
func readFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path) //nolint:gosec // test-controlled path
	if err != nil {
		t.Fatalf("read fetched file %q: %v", path, err)
	}
	return b
}

// ============================================================================
// FileFetcher tests (real filesystem)
// ============================================================================

func TestFileFetcher_Fetch_CopiesAndDigests(t *testing.T) {
	t.Parallel()
	srcDir := t.TempDir()
	destDir := t.TempDir()

	// A non-trivial payload so the digest is meaningful (not the empty hash).
	payload := []byte("ONNX-ARTIFACT-BYTES-\x00\x01\x02-fraud-detector-v3")
	srcPath := filepath.Join(srcDir, "model.onnx")
	if err := os.WriteFile(srcPath, payload, 0o600); err != nil {
		t.Fatalf("write source artifact: %v", err)
	}

	f := NewFileFetcher(destDir)
	localPath, digest, err := f.Fetch(context.Background(), "file://"+srcPath)
	if err != nil {
		t.Fatalf("Fetch returned error: %v", err)
	}

	// The returned path must be a NEW file inside our dest dir (a copy the pod
	// owns), not the source path.
	if localPath == srcPath {
		t.Fatalf("Fetch returned the source path; expected a copy under destDir")
	}
	if !strings.HasPrefix(localPath, destDir) {
		t.Fatalf("copy %q is not under destDir %q", localPath, destDir)
	}

	// REAL behavior: the copied bytes equal the source bytes...
	got := readFile(t, localPath)
	if string(got) != string(payload) {
		t.Fatalf("copied bytes differ from source: got %q want %q", got, payload)
	}
	// ...and the adapter's digest equals an independently-computed sha256.
	if want := sha256Hex(payload); digest != want {
		t.Fatalf("digest mismatch: got %q want %q", digest, want)
	}
	if !strings.HasPrefix(digest, "sha256:") {
		t.Fatalf("digest missing canonical prefix: %q", digest)
	}
}

func TestFileFetcher_Fetch_BarePathAccepted(t *testing.T) {
	t.Parallel()
	srcDir := t.TempDir()
	destDir := t.TempDir()
	payload := []byte("bare-path-artifact")
	srcPath := filepath.Join(srcDir, "m.onnx")
	if err := os.WriteFile(srcPath, payload, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	f := NewFileFetcher(destDir)
	// No file:// scheme — operators frequently pass a plain mount path.
	_, digest, err := f.Fetch(context.Background(), srcPath)
	if err != nil {
		t.Fatalf("Fetch(bare path) error: %v", err)
	}
	if want := sha256Hex(payload); digest != want {
		t.Fatalf("digest mismatch: got %q want %q", digest, want)
	}
}

func TestFileFetcher_Fetch_NotFound_MapsToSentinel(t *testing.T) {
	t.Parallel()
	f := NewFileFetcher(t.TempDir())
	_, _, err := f.Fetch(context.Background(), "file://"+filepath.Join(t.TempDir(), "does-not-exist.onnx"))
	if err == nil {
		t.Fatal("expected error for missing artifact, got nil")
	}
	// The service branches on errors.Is(err, ErrArtifactNotFound) to render the
	// distinct "artifact not found" StateFailed reason — assert that contract.
	if !errors.Is(err, domain.ErrArtifactNotFound) {
		t.Fatalf("missing file should wrap ErrArtifactNotFound; got %v", err)
	}
}

func TestFileFetcher_Fetch_RejectsNonFileScheme(t *testing.T) {
	t.Parallel()
	f := NewFileFetcher(t.TempDir())
	_, _, err := f.Fetch(context.Background(), "s3://bucket/key.onnx")
	if err == nil {
		t.Fatal("expected error for non-file scheme")
	}
	// Not an ErrArtifactNotFound — it's a wrong-adapter/config error.
	if errors.Is(err, domain.ErrArtifactNotFound) {
		t.Fatalf("scheme error should NOT be ErrArtifactNotFound: %v", err)
	}
}

func TestFileFetcher_Fetch_RejectsRemoteHost(t *testing.T) {
	t.Parallel()
	f := NewFileFetcher(t.TempDir())
	// file://evil-host/etc/passwd — a remote host is not locally readable; refuse.
	_, _, err := f.Fetch(context.Background(), "file://evil-host/etc/passwd")
	if err == nil {
		t.Fatal("expected error for file URI with a remote host")
	}
}

// AllowedRoots is the adapter-level defense-in-depth path-traversal guard.
func TestFileFetcher_AllowedRoots_BlocksTraversal(t *testing.T) {
	t.Parallel()
	allowedRoot := t.TempDir() // the only dir the pod may read from
	outsideDir := t.TempDir()  // a sibling dir, NOT allowed

	// A real secret-ish file outside the allowed root.
	secret := filepath.Join(outsideDir, "secret.onnx")
	if err := os.WriteFile(secret, []byte("stolen"), 0o600); err != nil {
		t.Fatalf("write secret: %v", err)
	}

	f := NewFileFetcher(t.TempDir(), allowedRoot)

	// Fetching the outside file must be denied even though it exists.
	if _, _, err := f.Fetch(context.Background(), "file://"+secret); err == nil {
		t.Fatal("expected traversal guard to deny a file outside allowed roots")
	}

	// A file INSIDE the allowed root must succeed.
	inside := filepath.Join(allowedRoot, "ok.onnx")
	if err := os.WriteFile(inside, []byte("legit"), 0o600); err != nil {
		t.Fatalf("write inside: %v", err)
	}
	if _, _, err := f.Fetch(context.Background(), "file://"+inside); err != nil {
		t.Fatalf("file inside allowed root should succeed, got: %v", err)
	}
}

func TestFileFetcher_Fetch_HonorsCancelledContext(t *testing.T) {
	t.Parallel()
	f := NewFileFetcher(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before the call
	if _, _, err := f.Fetch(ctx, "/some/path"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled ctx should abort Fetch with context.Canceled, got: %v", err)
	}
}

// Concurrent fetches of the same source must each produce a DISTINCT temp file
// (no collision) and the SAME digest — verifies the unique-temp-name design.
func TestFileFetcher_ConcurrentFetches_DistinctTempsSameDigest(t *testing.T) {
	t.Parallel()
	srcDir := t.TempDir()
	destDir := t.TempDir()
	payload := []byte("concurrent-artifact-bytes")
	srcPath := filepath.Join(srcDir, "model.onnx")
	if err := os.WriteFile(srcPath, payload, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	f := NewFileFetcher(destDir)

	const n = 8
	type res struct {
		path, digest string
		err          error
	}
	results := make(chan res, n)
	for i := 0; i < n; i++ {
		go func() {
			p, d, err := f.Fetch(context.Background(), "file://"+srcPath)
			results <- res{p, d, err}
		}()
	}
	seen := map[string]bool{}
	want := sha256Hex(payload)
	for i := 0; i < n; i++ {
		r := <-results
		if r.err != nil {
			t.Fatalf("concurrent Fetch error: %v", r.err)
		}
		if r.digest != want {
			t.Fatalf("concurrent digest mismatch: got %q want %q", r.digest, want)
		}
		if seen[r.path] {
			t.Fatalf("two concurrent fetches collided on temp path %q", r.path)
		}
		seen[r.path] = true
	}
}

// ============================================================================
// HTTPFetcher tests (real httptest HTTP server — the presigned-URL path)
// ============================================================================

func TestHTTPFetcher_Fetch_StreamsAndDigests(t *testing.T) {
	t.Parallel()
	payload := []byte("HTTP-DELIVERED-ONNX-BYTES-\xff\xfe-iris-v1")
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	defer srv.Close()

	f := NewHTTPFetcher(t.TempDir(), WithHTTPClient(srv.Client()))
	localPath, digest, err := f.Fetch(context.Background(), srv.URL+"/models/iris/v1.onnx")
	if err != nil {
		t.Fatalf("Fetch error: %v", err)
	}
	if gotPath != "/models/iris/v1.onnx" {
		t.Fatalf("server saw path %q, expected /models/iris/v1.onnx", gotPath)
	}
	// Bytes on disk match what the server sent...
	if got := readFile(t, localPath); string(got) != string(payload) {
		t.Fatalf("downloaded bytes differ: got %q want %q", got, payload)
	}
	// ...and the streamed-while-copying digest matches an independent sha256.
	if want := sha256Hex(payload); digest != want {
		t.Fatalf("digest mismatch: got %q want %q", digest, want)
	}
}

func TestHTTPFetcher_Fetch_404_MapsToSentinel(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no such object", http.StatusNotFound)
	}))
	defer srv.Close()

	f := NewHTTPFetcher(t.TempDir(), WithHTTPClient(srv.Client()))
	_, _, err := f.Fetch(context.Background(), srv.URL+"/missing.onnx")
	if err == nil {
		t.Fatal("expected error for 404")
	}
	if !errors.Is(err, domain.ErrArtifactNotFound) {
		t.Fatalf("404 should wrap ErrArtifactNotFound; got %v", err)
	}
}

func TestHTTPFetcher_Fetch_500_IsGenericNotNotFound(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "backend exploded", http.StatusInternalServerError)
	}))
	defer srv.Close()

	f := NewHTTPFetcher(t.TempDir(), WithHTTPClient(srv.Client()))
	_, _, err := f.Fetch(context.Background(), srv.URL+"/model.onnx")
	if err == nil {
		t.Fatal("expected error for 500")
	}
	// A 5xx is a generic fetch failure, NOT "artifact not found" — the service
	// renders different StateFailed reasons for the two.
	if errors.Is(err, domain.ErrArtifactNotFound) {
		t.Fatalf("500 must NOT map to ErrArtifactNotFound: %v", err)
	}
}

// The size cap is a DoS guard: an oversized body must fail and leave NO file.
func TestHTTPFetcher_Fetch_EnforcesMaxBytes(t *testing.T) {
	t.Parallel()
	destDir := t.TempDir()
	big := make([]byte, 4096)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(big)
	}))
	defer srv.Close()

	f := NewHTTPFetcher(destDir, WithHTTPClient(srv.Client()), WithMaxBytes(1024))
	_, _, err := f.Fetch(context.Background(), srv.URL+"/big.onnx")
	if err == nil {
		t.Fatal("expected error when body exceeds maxBytes")
	}
	if errors.Is(err, domain.ErrArtifactNotFound) {
		t.Fatalf("size-cap error must not be ErrArtifactNotFound: %v", err)
	}
	// The partial download must have been discarded (no leftover files).
	entries, _ := os.ReadDir(destDir)
	if len(entries) != 0 {
		t.Fatalf("expected no leftover files after size-cap failure, found %d", len(entries))
	}
}

// A presigned URL carries credentials in the query string; the error/log
// rendering must strip them. We assert the signature value never appears in the
// returned error.
func TestHTTPFetcher_Fetch_RedactsQueryCredentialsInError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "denied", http.StatusForbidden)
	}))
	defer srv.Close()

	const secretSig = "SUPERSECRETSIGNATURE1234567890"
	uri := srv.URL + "/m.onnx?X-Amz-Signature=" + secretSig + "&X-Amz-Credential=AKIA"
	f := NewHTTPFetcher(t.TempDir(), WithHTTPClient(srv.Client()))
	_, _, err := f.Fetch(context.Background(), uri)
	if err == nil {
		t.Fatal("expected error for 403")
	}
	if strings.Contains(err.Error(), secretSig) {
		t.Fatalf("error leaked the presigned signature: %v", err)
	}
}

func TestHTTPFetcher_Fetch_RejectsNonHTTPScheme(t *testing.T) {
	t.Parallel()
	f := NewHTTPFetcher(t.TempDir())
	if _, _, err := f.Fetch(context.Background(), "file:///tmp/x.onnx"); err == nil {
		t.Fatal("HTTPFetcher must reject a file:// URI")
	}
}

func TestHTTPFetcher_Fetch_HonorsCancelledContext(t *testing.T) {
	t.Parallel()
	// A server that blocks so the ONLY way the call returns is via ctx cancel.
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	defer srv.Close()
	defer close(release)

	f := NewHTTPFetcher(t.TempDir(), WithHTTPClient(srv.Client()))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, _, err := f.Fetch(ctx, srv.URL+"/slow.onnx")
	if err == nil {
		t.Fatal("expected ctx-deadline error from a hanging server")
	}
	if errors.Is(err, domain.ErrArtifactNotFound) {
		t.Fatalf("timeout must not be ErrArtifactNotFound: %v", err)
	}
}

// ============================================================================
// Dispatcher tests (scheme routing)
// ============================================================================

func TestFetcher_Dispatch_RoutesByScheme(t *testing.T) {
	t.Parallel()
	srcDir := t.TempDir()
	destDir := t.TempDir()
	filePayload := []byte("file-routed")
	srcPath := filepath.Join(srcDir, "f.onnx")
	if err := os.WriteFile(srcPath, filePayload, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	httpPayload := []byte("http-routed")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(httpPayload)
	}))
	defer srv.Close()

	d := NewFetcher(
		NewFileFetcher(destDir),
		NewHTTPFetcher(destDir, WithHTTPClient(srv.Client())),
	)

	// file:// routes to FileFetcher.
	_, fd, err := d.Fetch(context.Background(), "file://"+srcPath)
	if err != nil {
		t.Fatalf("file dispatch error: %v", err)
	}
	if fd != sha256Hex(filePayload) {
		t.Fatalf("file dispatch digest mismatch")
	}

	// http:// routes to HTTPFetcher.
	_, hd, err := d.Fetch(context.Background(), srv.URL+"/h.onnx")
	if err != nil {
		t.Fatalf("http dispatch error: %v", err)
	}
	if hd != sha256Hex(httpPayload) {
		t.Fatalf("http dispatch digest mismatch")
	}
}

func TestFetcher_Dispatch_UnsupportedSchemeRejected(t *testing.T) {
	t.Parallel()
	d := NewFetcher(NewFileFetcher(t.TempDir()), nil)
	// s3:// is the follow-up native-client scheme — must be rejected today.
	_, _, err := d.Fetch(context.Background(), "s3://fp-models/x.onnx")
	if err == nil {
		t.Fatal("expected unsupported-scheme error for s3://")
	}
	// http disabled (nil) → http URI must be refused too (hard-deny posture).
	if _, _, err := d.Fetch(context.Background(), "http://example/x.onnx"); err == nil {
		t.Fatal("expected refusal when http adapter is nil")
	}
}

// ============================================================================
// End-to-end with the DOMAIN: the fetcher actually drives a successful LoadModel
// ============================================================================
//
// This wires the REAL FileFetcher into the REAL domain ServingService (with a
// fake engine for the ONNX part — the runtime adapter is out of scope for the
// persistence layer) and asserts the model reaches StateReady with the digest
// the fetcher computed. It proves the adapter satisfies the port in situ, not
// just in isolation — the read-after-write equivalent for this service: load,
// then observe the resident model carries the fetched digest.

// fakeEngine is a minimal domain.InferenceEngine for the e2e load test. It is
// NOT a persistence concern (the ONNX runtime is a separate adapter); we use a
// trivial fake so the LoadModel flow can run end-to-end through the real fetcher.
type fakeEngine struct{ loadedFrom string }

func (e *fakeEngine) Load(_ context.Context, _ domain.ModelRef, localPath string) (domain.LoadedSession, error) {
	e.loadedFrom = localPath
	// Echo a digest of "" so the service keeps the fetcher's digest as final.
	return domain.LoadedSession{
		InputSchema:  []domain.TensorSpec{{Name: "x", Shape: []int64{-1, 4}, DType: domain.DataTypeFloat32}},
		OutputSchema: []domain.TensorSpec{{Name: "y", Shape: []int64{-1, 1}, DType: domain.DataTypeFloat32}},
		MemoryBytes:  1024,
		Digest:       "",
	}, nil
}
func (e *fakeEngine) Predict(_ context.Context, _ domain.ModelRef, in map[string]domain.Tensor) (map[string]domain.Tensor, error) {
	return in, nil
}
func (e *fakeEngine) Unload(_ context.Context, _ domain.ModelRef) error { return nil }

// realClock is the production Clock for the e2e test.
type realClock struct{}

func (realClock) Now() time.Time                  { return time.Now() }
func (realClock) Since(t time.Time) time.Duration { return time.Since(t) }

func TestFileFetcher_DrivesDomainLoadModel_ToReady(t *testing.T) {
	t.Parallel()
	srcDir := t.TempDir()
	destDir := t.TempDir()
	payload := []byte("end-to-end-onnx-bytes")
	srcPath := filepath.Join(srcDir, "model.onnx")
	if err := os.WriteFile(srcPath, payload, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	uri := "file://" + srcPath
	wantDigest := sha256Hex(payload)

	eng := &fakeEngine{}
	svc := domain.NewServingService(domain.ServiceConfig{
		Engine:              eng,
		Fetcher:             NewFileFetcher(destDir),
		Clock:               realClock{},
		AllowedArtifactURIs: []string{"file://" + srcDir}, // SSRF allow-list pins to the source dir
	})

	ref := domain.ModelRef{Name: "fraud-detector", Version: "v3"}
	status, err := svc.LoadModel(context.Background(), domain.LoadModelInput{
		Ref:            ref,
		ArtifactURI:    uri,
		ExpectedDigest: wantDigest, // the fetcher's digest MUST match this for load to succeed
	})
	if err != nil {
		t.Fatalf("LoadModel error: %v", err)
	}
	if status.State != domain.StateReady {
		t.Fatalf("expected StateReady, got %v (%s)", status.State, status.Message)
	}
	// The engine was handed the fetcher's local COPY (in destDir), not the source.
	if !strings.HasPrefix(eng.loadedFrom, destDir) {
		t.Fatalf("engine loaded from %q, expected a copy under %q", eng.loadedFrom, destDir)
	}
}

// TestArtifactStore_NoDockerNeeded documents that these adapters are tested
// without a container (filesystem + httptest). The SkipIfNoDocker call keeps the
// contract the task specifies and makes this the place a future MinIO-container
// case (native client follow-up) would attach.
func TestArtifactStore_ContainerSlotReserved(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	// Intentionally minimal: no container is required for the current adapters.
	// When a native MinIOFetcher lands (go get minio-go), add a MinIO-container
	// case here that uploads an object and Fetches it through the same port.
	_ = io.Discard
}
