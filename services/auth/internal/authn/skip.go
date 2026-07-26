package authn

import authv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/auth/v1"

// PublicMethods returns the gRPC full-method names that BYPASS authentication on
// the auth service — the auth-service-specific skip list passed to
// grpcutil.WithAuthValidator.
//
// ============================================================================
// WHY THESE THREE METHODS ARE PUBLIC (and nothing else)
// ============================================================================
//
//   - Login: this is the call that MINTS the first token. By definition the
//     caller has no token yet (they send email+password to GET one). Requiring a
//     token to log in would be an impossible chicken-and-egg. The credentials in
//     the request body ARE the identity assertion, verified by bcrypt — not by
//     the interceptor.
//
//   - ValidateToken: the token to check rides in the REQUEST BODY, not the
//     authorization header. This is the workhorse other services' interceptors
//     call to resolve a credential; gating it behind the same interceptor would
//     be circular (you'd need a valid token to ask "is this token valid?").
//
//   - CheckPermission: a service-to-service RBAC decision gate. Its request
//     carries the (user_id, resource, action) to evaluate — no caller credential
//     — so it is meant to be callable by trusted in-cluster services without a
//     user token. (When mTLS/service-identity lands, that becomes the gate here;
//     for now it is intentionally open, matching the proto's S2S contract.)
//
// EVERYTHING ELSE IS DEFAULT-DENY: CreateUser, AssignRole, ListUsers,
// CreateAPIKey, RevokeAPIKey all require a valid token → the interceptor injects
// claims → requireAdmin's CheckPermission makes the admin decision. Adding a new
// RPC to the proto automatically requires a token unless it is consciously added
// here — the secure default (see grpcutil.WithSkipMethods on WHY skip, not require).
//
// NOTE: the gRPC health service and reflection service are skipped SEPARATELY by
// HealthAndReflectionMethods() so this list stays purely auth-domain methods.
func PublicMethods() []string {
	return []string{
		authv1.AuthService_Login_FullMethodName,
		authv1.AuthService_ValidateToken_FullMethodName,
		authv1.AuthService_CheckPermission_FullMethodName,
	}
}

// HealthAndReflectionMethods returns the infrastructure RPC full-method names
// that every service must exempt from authentication:
//
//   - grpc.health.v1.Health/Check + /Watch: the K8s gRPC readiness/liveness
//     probe calls these with no credential. Gating them would make the pod fail
//     its own probe and never become Ready.
//   - grpc.reflection.* : grpcurl/grpcui list/describe the service in dev with no
//     token. (In production reflection is typically disabled entirely; when it is
//     enabled, it must be exempt or discovery breaks.)
//
// These are constant across every service, so they live here once and every
// service appends them to its own PublicMethods() when wiring the validator.
func HealthAndReflectionMethods() []string {
	return []string{
		"/grpc.health.v1.Health/Check",
		"/grpc.health.v1.Health/Watch",
		"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
		"/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo",
	}
}

// SkipMethods is the full set of methods exempt from authentication on the auth
// service: the public auth RPCs plus the health/reflection infrastructure RPCs.
// main.go passes this directly to grpcutil.WithAuthValidator. Sharing it as one
// function means the wiring and the tests can never drift from each other.
func SkipMethods() []string {
	return append(PublicMethods(), HealthAndReflectionMethods()...)
}
