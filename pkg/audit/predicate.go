package audit

import "strings"

// ============================================================================
// SECURITY-RELEVANCE PREDICATE — what to audit (and what to skip)
// ============================================================================
//
// Auditing EVERY RPC would drown the real signal: a ListModels called 10k times a
// minute is not a security event, and recording it floods the chain, the consumer,
// and the auditor's view. An audit log is valuable precisely because it is
// SELECTIVE — it captures the actions that MATTER for security and compliance:
//
//   - MUTATIONS (Create/Update/Delete/...) — they change state; "who changed
//     this?" is the canonical audit question.
//   - ADMIN / privilege operations (AssignRole, Revoke..., GrantAccess) — privilege
//     changes are the highest-value audit target.
//   - AUTH ATTEMPTS (Login, CreateAPIKey, rotate) — both successes and failures;
//     a burst of failed Logins is an attack signature.
//   - DENIED access to ANYTHING — handled by the interceptor, not the predicate:
//     even a denied pure-read is recorded, because the DENY is the signal. The
//     predicate decides what to audit on SUCCESS; the interceptor always audits a
//     DENY regardless of the predicate (see AuditInterceptor).
//
// We SKIP by default: pure reads (Get/List/Watch/Describe/Check/Validate),
// health, and reflection — high-volume, low-security-value calls.
//
// The predicate is a function so a service can supply its own (e.g. a service with
// an unusual naming scheme), but DefaultSecurityRelevant covers the platform's
// consistent proto verb conventions.

// MethodPredicate reports whether a gRPC full-method should be audited on a
// SUCCESSFUL (ALLOW) outcome. fullMethod is the standard
// "/package.Service/Method" string from grpc.UnaryServerInfo.FullMethod.
type MethodPredicate func(fullMethod string) bool

// mutatingVerbs are method-name prefixes that indicate a STATE CHANGE. The
// platform's protos follow Google AIP verb conventions, so a prefix match on the
// method's leaf name is a reliable, allow-list-shaped classifier. Listed as
// prefixes so "CreateUser", "CreateAPIKey", "CreateModelVersion" all match
// "Create".
var mutatingVerbs = []string{
	"Create", "Update", "Delete", "Remove", "Set", "Put",
	"Assign", "Revoke", "Grant", "Rotate", "Register", "Deregister",
	"Promote", "Rollback", "Approve", "Reject", "Suspend", "Activate",
	"Deactivate", "Enable", "Disable", "Cancel", "Retry", "Trigger",
	"Start", "Stop", "Pause", "Resume",
}

// authVerbs are leaf-name prefixes for authentication attempts we ALWAYS audit
// (success too), because the success/failure of an auth attempt is itself the
// security event — not a state mutation per se, but the front door.
var authVerbs = []string{
	"Login", "Logout", "Authenticate",
}

// NOTE ON READS: there is deliberately no readVerbs allow-list. Read prefixes
// (Get/List/Watch/Describe/Query/Search/...) take the same path as any verb the
// predicate does not recognize — the final `return false` below. Enumerating them
// would suggest an unlisted read is audited, which is not the case. (A DENY on a
// read is still audited by the interceptor; this predicate only governs ALLOW.)

// DefaultSecurityRelevant is the platform's default audit predicate. It returns
// true for mutating and auth methods, false for reads/health/reflection and any
// method it doesn't recognize. This is the "sensible default" wired by every
// service unless it overrides it.
//
// DESIGN: default-QUIET (unrecognized → not audited on success) is the opposite of
// the auth interceptor's default-DENY. The reasoning differs: a forgotten audit on
// a success is a missed log line (recoverable — DENYs and recognized mutations are
// still captured), whereas a forgotten AUTH check is an open door. So audit errs
// toward less noise; authentication errs toward more safety.
func DefaultSecurityRelevant(fullMethod string) bool {
	leaf := leafMethod(fullMethod)
	if leaf == "" {
		return false
	}
	// Never audit health/reflection on success (they are also skipped from auth).
	if isHealthOrReflection(fullMethod) {
		return false
	}
	if hasAnyPrefix(leaf, authVerbs) {
		return true
	}
	if hasAnyPrefix(leaf, mutatingVerbs) {
		return true
	}
	// Explicit reads are skipped; so is anything unrecognized (default quiet).
	return false
}

// leafMethod extracts the method name from a "/package.Service/Method" string.
// Returns "" if the input isn't in that form.
func leafMethod(fullMethod string) string {
	i := strings.LastIndexByte(fullMethod, '/')
	if i < 0 || i == len(fullMethod)-1 {
		return ""
	}
	return fullMethod[i+1:]
}

// isHealthOrReflection reports whether the method belongs to the gRPC health or
// reflection services — the same set grpcutil exempts from auth. We never audit
// these (K8s probes and grpcurl discovery are not security events).
func isHealthOrReflection(fullMethod string) bool {
	return strings.HasPrefix(fullMethod, "/grpc.health.v1.Health/") ||
		strings.HasPrefix(fullMethod, "/grpc.reflection.")
}

// hasAnyPrefix reports whether s starts with any of the given prefixes.
func hasAnyPrefix(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}
