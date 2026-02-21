package testutil

import "time"

// ============================================================================
// TEST FIXTURES
// ============================================================================
//
// WHY centralized fixtures:
//   Multiple services need the same test data (users, models, API keys).
//   Without a shared fixtures package, each service duplicates this data,
//   leading to drift and inconsistent test coverage.
//
//   These fixtures represent valid domain objects for testing. They use
//   deterministic values (not random) so test output is reproducible.
// ============================================================================

// SampleUser returns a test user for authentication tests.
func SampleUser() map[string]any {
	return map[string]any{
		"id":         "usr_test_001",
		"email":      "test@goml.dev",
		"team":       "ml-platform",
		"role":       "admin",
		"scopes":     []string{"models:read", "models:write", "inference:invoke"},
		"created_at": time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

// SampleModelMetadata returns test model metadata for registry tests.
func SampleModelMetadata() map[string]any {
	return map[string]any{
		"id":          "mdl_test_001",
		"name":        "fraud-detector",
		"version":     "v1.2.0",
		"framework":   "pytorch",
		"format":      "onnx",
		"artifact_uri": "s3://goml-models/fraud-detector/v1.2.0/model.onnx",
		"metrics": map[string]float64{
			"accuracy":  0.95,
			"precision": 0.92,
			"recall":    0.89,
			"f1":        0.905,
		},
		"tags": map[string]string{
			"team":        "fraud-detection",
			"environment": "staging",
		},
		"created_at": time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC),
	}
}

// SampleAPIKey returns a test API key for gateway/auth tests.
func SampleAPIKey() map[string]any {
	return map[string]any{
		"id":         "key_test_001",
		"key":        "goml_test_sk_abcdef1234567890",
		"user_id":    "usr_test_001",
		"name":       "test-key",
		"scopes":     []string{"inference:invoke"},
		"rate_limit": 100, // requests per minute
		"created_at": time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC),
		"expires_at": time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
	}
}
