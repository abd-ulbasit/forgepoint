package audit

import "testing"

// TestDefaultSecurityRelevant is the table-driven proof of the security-relevance
// predicate: mutating + auth methods are audited; reads, health, reflection, and
// unrecognized verbs are not. This governs ALLOW capture only — the interceptor
// always audits DENY regardless (tested in interceptor_test.go).
func TestDefaultSecurityRelevant(t *testing.T) {
	cases := []struct {
		method string
		want   bool
	}{
		// Mutations → audited.
		{"/forgepoint.auth.v1.AuthService/CreateUser", true},
		{"/forgepoint.auth.v1.AuthService/CreateAPIKey", true},
		{"/forgepoint.auth.v1.AuthService/AssignRole", true},
		{"/forgepoint.auth.v1.AuthService/RevokeAPIKey", true},
		{"/forgepoint.registry.v1.RegistryService/DeleteModel", true},
		{"/forgepoint.registry.v1.RegistryService/RegisterModel", true},
		{"/forgepoint.pipeline.v1.PipelineService/TriggerRun", true},
		{"/forgepoint.serving.v1.ServingService/PromoteVersion", true},
		// Auth attempts → audited (success too).
		{"/forgepoint.auth.v1.AuthService/Login", true},
		// Pure reads → NOT audited on success.
		{"/forgepoint.auth.v1.AuthService/GetUser", false},
		{"/forgepoint.auth.v1.AuthService/ListUsers", false},
		{"/forgepoint.auth.v1.AuthService/ValidateToken", false},
		{"/forgepoint.auth.v1.AuthService/CheckPermission", false},
		{"/forgepoint.registry.v1.RegistryService/ListModels", false},
		{"/forgepoint.pipeline.v1.PipelineService/WatchExecution", false},
		// Health + reflection → never audited.
		{"/grpc.health.v1.Health/Check", false},
		{"/grpc.health.v1.Health/Watch", false},
		{"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo", false},
		{"/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo", false},
		// Unrecognized verb → default quiet.
		{"/forgepoint.x.v1.Svc/FrobnicateWidget", false},
		// Malformed input → not audited (no panic).
		{"", false},
		{"no-slash", false},
		{"/trailing/", false},
	}

	for _, tc := range cases {
		t.Run(tc.method, func(t *testing.T) {
			if got := DefaultSecurityRelevant(tc.method); got != tc.want {
				t.Errorf("DefaultSecurityRelevant(%q) = %v, want %v", tc.method, got, tc.want)
			}
		})
	}
}

// TestLeafMethod checks the helper that extracts the method name.
func TestLeafMethod(t *testing.T) {
	cases := map[string]string{
		"/pkg.Service/Method": "Method",
		"/a/B":                "B",
		"":                    "",
		"no-slash":            "",
		"/trailing/":          "",
	}
	for in, want := range cases {
		if got := leafMethod(in); got != want {
			t.Errorf("leafMethod(%q) = %q, want %q", in, got, want)
		}
	}
}
