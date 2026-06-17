package observability_test

// ============================================================================
// OBSERVABILITY HARDENING TESTS
// ============================================================================
//
// Tests for:
//   5a. Single-init guard: a second Setup() call must return an error.
//   5b. OTLPInsecure config field: zero value = secure (no WithInsecure).
// ============================================================================

import (
	"context"
	"testing"

	"github.com/abd-ulbasit/forgepoint/pkg/observability"
)

// ============================================================================
// FIX 5a: Single-init guard
// ============================================================================
//
// WHAT: Calling Setup() twice silently replaced global OTel providers and
// leaked the first set's exporters/batchers (goroutines, file descriptors).
//
// FIX: A package-level sync.Once tracks whether Setup has been called.
// A second call returns an error "observability: Setup already called" and
// does NOT replace the existing providers.
//
// WHY this matters: In tests that import multiple packages each calling
// Setup, or in a service that accidentally calls Setup twice, the first
// set of exporters would be orphaned — their Shutdown is never called,
// so they may flush indefinitely or hold connections.
//
// TRADEOFF: This makes Setup non-reentrant. The alternative (silently no-op)
// was considered but rejected because a second call is almost always a bug
// and silent success would mask it. Returning an error forces the caller to
// be explicit. For tests that need multiple isolated setups, call
// observability.ResetForTest() (only available in test builds).
// ============================================================================

func TestSetup_SecondCallReturnsError(t *testing.T) {
	// Reset state so this test is independent of test ordering.
	observability.ResetForTest()

	cfg := observability.Config{
		ServiceName: "test-svc",
		Environment: "test",
	}

	shutdown1, err := observability.Setup(context.Background(), cfg)
	if err != nil {
		t.Fatalf("first Setup() failed: %v", err)
	}
	defer shutdown1(context.Background()) //nolint:errcheck

	// Second call must return an error.
	_, err2 := observability.Setup(context.Background(), cfg)
	if err2 == nil {
		t.Fatal("second Setup() returned nil error — must return an error to prevent provider replacement")
	}
	t.Logf("second Setup() correctly returned: %v", err2)
}

func TestSetup_FirstCallAfterReset(t *testing.T) {
	// After ResetForTest, Setup must succeed again.
	observability.ResetForTest()

	cfg := observability.Config{ServiceName: "test-svc2", Environment: "test"}
	shutdown, err := observability.Setup(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Setup() after reset returned error: %v", err)
	}
	defer shutdown(context.Background()) //nolint:errcheck
}

// ============================================================================
// FIX 5b: OTLPInsecure config field
// ============================================================================
//
// WHAT: The code always called WithInsecure() on the OTLP exporter, even in
// production. Proper TLS should be the default; insecure is opt-in for local
// dev (where no collector TLS is configured).
//
// FIX: Add OTLPInsecure bool to Config. Only use WithInsecure() when true.
// Default zero-value is false → TLS.
//
// LOCAL/DEV: Set OTLPInsecure: true when collector has no TLS cert.
// PROD: Leave false; Istio mTLS or real TLS handles the connection.
//
// WHY zero-value = secure: The Go zero value is the safe default. An engineer
// forgetting to set the field gets TLS, not plaintext — fail safe.
// ============================================================================

func TestConfig_OTLPInsecureField(t *testing.T) {
	// Verify the field exists and zero-value is false (secure by default).
	cfg := observability.Config{
		ServiceName:  "test-svc",
		OTLPEndpoint: "", // no endpoint → stdout fallback, field is irrelevant
	}
	if cfg.OTLPInsecure {
		t.Error("OTLPInsecure zero value must be false (secure by default)")
	}

	// Verify we can set it to true for local dev.
	cfg.OTLPInsecure = true
	if !cfg.OTLPInsecure {
		t.Error("failed to set OTLPInsecure = true")
	}
}

func TestSetup_InsecureField_NoOTLPEndpoint(t *testing.T) {
	// When OTLPEndpoint is empty, the stdout fallback is used regardless of
	// OTLPInsecure — no exporter connection to secure/insecure.
	observability.ResetForTest()

	cfg := observability.Config{
		ServiceName:   "test-svc",
		Environment:   "test",
		OTLPInsecure:  false, // secure (but no endpoint → stdout anyway)
	}

	shutdown, err := observability.Setup(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Setup() returned error: %v", err)
	}
	defer shutdown(context.Background()) //nolint:errcheck
}
