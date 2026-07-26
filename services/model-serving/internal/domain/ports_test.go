// ports_test.go — unit tests for the SSRF / supply-chain allow-list predicate
// ArtifactURIAllowed.
//
// ============================================================================
// WHY THESE TESTS EXIST (the hardening they lock in)
// ============================================================================
//
// ArtifactURIAllowed is the PRIMARY SSRF guard: LoadModel runs it BEFORE any
// fetch, so a rejected URI never reaches the network/disk. The earlier
// implementation did a RAW byte-prefix compare (uri[:len(allowed)] == allowed),
// which is a footgun on three axes — path traversal, prefix-sibling host/bucket,
// and scheme confusion. These table-driven tests assert that the STRUCTURED
// comparison (exact scheme + exact host + cleaned-path-under-prefix) rejects each
// of those attacks while still allowing every legitimate URI.
//
// We use the external `domain_test` package and drive only the exported function,
// matching the rest of the service's test discipline.
package domain_test

import (
	"testing"

	"github.com/abd-ulbasit/forgepoint/services/model-serving/internal/domain"
)

func TestArtifactURIAllowed(t *testing.T) {
	// The canonical pod allow-list: one s3 bucket+prefix root.
	s3Allow := []string{"s3://fp-models/"}

	tests := []struct {
		name    string
		uri     string
		allow   []string
		want    bool
		comment string // WHY this case matters
	}{
		// ---- LEGITIMATE URIs (must be ALLOWED) ---------------------------------
		{
			name:    "object directly under prefix",
			uri:     "s3://fp-models/iris/v1.onnx",
			allow:   s3Allow,
			want:    true,
			comment: "the normal happy path: an artifact under the allowed bucket+prefix",
		},
		{
			name:    "deeply nested object under prefix",
			uri:     "s3://fp-models/a/b/c/model.onnx",
			allow:   s3Allow,
			want:    true,
			comment: "arbitrary depth under the prefix is fine",
		},
		{
			name:    "bucket root exact (allow has trailing slash, uri does not)",
			uri:     "s3://fp-models",
			allow:   s3Allow,
			want:    true,
			comment: "trailing-slash normalization: bucket root itself is the prefix root",
		},
		{
			name:    "scheme/host case-insensitive",
			uri:     "S3://FP-Models/iris/v1.onnx",
			allow:   s3Allow,
			want:    true,
			comment: "schemes and DNS hostnames are case-insensitive; same bucket",
		},
		{
			name:    "prefix entry (not just bucket root) allows its children",
			uri:     "s3://fp-models/prod/iris/v1.onnx",
			allow:   []string{"s3://fp-models/prod/"},
			want:    true,
			comment: "a sub-prefix allow entry still matches its descendants",
		},
		{
			name:    "second allow entry matches",
			uri:     "https://artifacts.internal/models/x.onnx",
			allow:   []string{"s3://fp-models/", "https://artifacts.internal/models/"},
			want:    true,
			comment: "multiple roots: any one matching grants access",
		},
		{
			name:    "file scheme legitimate child",
			uri:     "file:///mnt/models/iris/v1.onnx",
			allow:   []string{"file:///mnt/models/"},
			want:    true,
			comment: "file:// mount path under the allowed root",
		},

		// ---- PATH TRAVERSAL (dot-dot escape) — must be REJECTED ----------------
		{
			name:    "parent-dir traversal escapes prefix",
			uri:     "s3://fp-models/../secrets/leak.onnx",
			allow:   s3Allow,
			want:    false,
			comment: "THE BUG: raw prefix passed this; cleaned path resolves OUTSIDE the prefix",
		},
		{
			name:    "deep parent-dir traversal climbs above prefix",
			uri:     "s3://fp-models/a/../../etc/passwd",
			allow:   s3Allow,
			want:    false,
			comment: "multiple ../ climb above the bucket root after cleaning",
		},
		{
			name:    "traversal landing in a sibling bucket-like path",
			uri:     "s3://fp-models/../fp-models-evil/x.onnx",
			allow:   s3Allow,
			want:    false,
			comment: "../ then re-descend into a sibling — still escapes the prefix",
		},

		// ---- PREFIX-SIBLING HOST/BUCKET — must be REJECTED ---------------------
		{
			name:    "prefix-sibling bucket (raw prefix footgun)",
			uri:     "s3://fp-models-evil/payload.onnx",
			allow:   []string{"s3://fp-models"}, // NOTE: no trailing slash — the worst case for raw prefix
			want:    false,
			comment: "raw 'uri[:len]==allowed' matched this; exact HOST compare rejects it",
		},
		{
			name:    "prefix-sibling bucket with trailing-slash allow",
			uri:     "s3://fp-models-evil/payload.onnx",
			allow:   s3Allow,
			want:    false,
			comment: "different host entirely; never under the allowed bucket",
		},
		{
			name:    "host substring but different host",
			uri:     "s3://evil-fp-models/x.onnx",
			allow:   s3Allow,
			want:    false,
			comment: "host contains the allowed name as a substring but is a different bucket",
		},
		{
			name:    "userinfo smuggling cannot impersonate host",
			uri:     "s3://evil@fp-models/x.onnx",
			allow:   s3Allow,
			want:    false,
			comment: "the full authority (user@host) must match; 'evil@fp-models' != 'fp-models'",
		},

		// ---- SCHEME MISMATCH — must be REJECTED --------------------------------
		{
			name:    "scheme mismatch https vs s3",
			uri:     "https://fp-models/iris/v1.onnx",
			allow:   s3Allow,
			want:    false,
			comment: "same host string, wrong scheme: a scheme-swap SSRF attempt",
		},
		{
			name:    "scheme mismatch file vs s3",
			uri:     "file://fp-models/iris/v1.onnx",
			allow:   s3Allow,
			want:    false,
			comment: "file:// against an s3:// allow entry is rejected",
		},

		// ---- FAIL-CLOSED / MALFORMED — must be REJECTED ------------------------
		{
			name:    "empty uri",
			uri:     "",
			allow:   s3Allow,
			want:    false,
			comment: "no URI ⇒ deny",
		},
		{
			name:    "empty allow-list denies everything",
			uri:     "s3://fp-models/iris/v1.onnx",
			allow:   nil,
			want:    false,
			comment: "fail-closed: a misconfigured pod with no allow-list refuses all loads",
		},
		{
			name:    "allow-list of only empty strings denies",
			uri:     "s3://fp-models/iris/v1.onnx",
			allow:   []string{"", ""},
			want:    false,
			comment: "empty entries are ignored, not treated as match-everything",
		},
		{
			name:    "schemeless bare path denied",
			uri:     "/etc/passwd",
			allow:   s3Allow,
			want:    false,
			comment: "a schemeless/hostless value can't be safely component-matched ⇒ deny",
		},
		{
			name:    "schemeless allow entry never grants",
			uri:     "s3://fp-models/iris/v1.onnx",
			allow:   []string{"fp-models"},
			want:    false,
			comment: "a malformed (schemeless) allow entry can never grant access",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := domain.ArtifactURIAllowed(tt.uri, tt.allow)
			if got != tt.want {
				t.Fatalf("ArtifactURIAllowed(%q, %v) = %v, want %v\n  why: %s",
					tt.uri, tt.allow, got, tt.want, tt.comment)
			}
		})
	}
}

// TestArtifactURIAllowed_RootPrefixMatchesAnything documents the edge case where
// the allowed prefix is the bucket root ("/" after cleaning): every object in the
// bucket is then under-prefix. This is intentional — "s3://fp-models/" means "any
// object in fp-models" — but a traversal STILL cannot escape, because the HOST is
// pinned. We assert both halves so the root-prefix behavior is not mistaken for a
// loophole.
func TestArtifactURIAllowed_RootPrefixMatchesAnything(t *testing.T) {
	allow := []string{"s3://fp-models/"}

	// Any object in the bucket is allowed (root prefix).
	if !domain.ArtifactURIAllowed("s3://fp-models/anything/at/all.onnx", allow) {
		t.Fatal("expected any object under the bucket root to be allowed")
	}
	// But a traversal out of the bucket is NOT allowed even with a root prefix,
	// because path cleaning removes the escape and the result leaves the prefix.
	if domain.ArtifactURIAllowed("s3://fp-models/../other/x.onnx", allow) {
		t.Fatal("traversal out of the bucket root must be rejected")
	}
	// And a different host is never allowed regardless of prefix breadth.
	if domain.ArtifactURIAllowed("s3://other-bucket/x.onnx", allow) {
		t.Fatal("a different bucket must be rejected even under a root prefix")
	}
}
