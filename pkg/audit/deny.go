package audit

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ============================================================================
// OUTER DENY-CAPTURE INTERCEPTOR — record the denials AUTH short-circuits
// ============================================================================
//
// THE GAP THIS CLOSES (the whole reason this file exists):
//
//	The inner audit interceptor (interceptor.go) sits AFTER auth in the chain, so
//	for an authenticated call it sees the real actor (good) — but when the AUTH
//	interceptor itself REJECTS a request (no token, expired/forged token, or a
//	PermissionDenied from the validator), auth returns BEFORE calling its inner
//	handler. The inner audit interceptor IS that inner handler, so it NEVER RUNS
//	for an auth-rejection denial. The result: a flood of anonymous Unauthenticated
//	probes against AssignRole/CreateUser would leave ZERO rows in the audit log —
//	exactly the attack shape an audit log exists to catch, silently dropped.
//
// THE FIX: a SECOND audit interceptor wired BEFORE auth (the OUTERMOST custom
// interceptor), so it WRAPS auth and observes auth's return value even on a
// short-circuit. The chain becomes:
//
//	recovery → logging → DENY-CAPTURE(this) → auth → AUDIT(inner) → handler
//
// On the way out this interceptor inspects the resulting gRPC code:
//   - DENY codes (Unauthenticated / PermissionDenied) that the INNER interceptor
//     did NOT already record → emit a DENY record here, with the anonymous actor
//     (auth rejected before claims existed, so actorFromContext yields anonymous).
//   - anything else (ALLOW, ERROR, or a DENY the inner interceptor already
//     captured with the real actor) → do nothing; the inner interceptor owns it.
//
// ============================================================================
// EXACTLY-ONCE BETWEEN THE TWO INTERCEPTORS — the shared marker
// ============================================================================
//
// A handler-level DENY (requireAdmin → PermissionDenied on an already-authenticated
// call) flows through BOTH interceptors: the inner one records it (with the real
// actor), then the SAME PermissionDenied bubbles out to this outer one. Without
// coordination, that DENY would be recorded TWICE (once with the real actor, once
// as anonymous) — a corrupted, double-counted trail.
//
// We coordinate with a MARKER the outer interceptor installs in the context: a
// pointer to a bool. Context values are immutable, so we can't "set a flag in the
// context" after the fact; instead we put a *POINTER* in the context up front, and
// the inner interceptor flips the pointed-to bool (markAudited) when it emits. On
// the way out, the outer interceptor reads the flag: if the inner one already
// captured this RPC, the outer one stays silent. So:
//
//	auth-rejection DENY  → inner never ran → flag false → OUTER records (anonymous)
//	handler-level DENY   → inner recorded  → flag true  → outer SILENT (no dup)
//	authenticated ALLOW  → inner recorded  → flag true  → outer SILENT
//
// Net: every DENY is recorded EXACTLY ONCE, by whichever interceptor actually
// observed the request's identity. This is the same "did a downstream layer
// already handle this?" coordination an HTTP middleware uses with a response-
// written flag.
// ============================================================================

// auditedMarkerKey is the private context key under which the outer interceptor
// stashes a *bool the inner interceptor flips when it emits a record.
type auditedMarkerKey struct{}

// contextWithAuditedMarker returns a child context carrying a fresh, unset marker.
// The outer interceptor calls this BEFORE invoking the chain so the marker is
// visible to the inner interceptor (which shares the same context lineage).
func contextWithAuditedMarker(ctx context.Context) (context.Context, *bool) {
	flag := new(bool)
	return context.WithValue(ctx, auditedMarkerKey{}, flag), flag
}

// markAudited flips the shared marker to true, recording that the inner audit
// interceptor has already captured this RPC. It is a no-op when no marker is
// present (e.g. the inner interceptor is used standalone in a test, or the outer
// interceptor is not wired) — so the inner interceptor never depends on the outer
// one existing.
func markAudited(ctx context.Context) {
	if flag, ok := ctx.Value(auditedMarkerKey{}).(*bool); ok && flag != nil {
		*flag = true
	}
}

// alreadyAudited reports whether the inner interceptor flipped the marker.
func alreadyAudited(flag *bool) bool {
	return flag != nil && *flag
}

// DenyUnaryInterceptor returns the OUTER unary interceptor that captures auth-level
// denials (Unauthenticated / PermissionDenied) the inner interceptor never saw.
//
// It MUST be wired BEFORE the auth interceptor (the outermost custom interceptor)
// so it wraps auth and observes auth's short-circuit return. It records a DENY only
// when (a) the result is a DENY code and (b) the inner interceptor did not already
// record this RPC (the shared marker). Like all audit capture it is best-effort:
// a sink failure is logged, never surfaced as an RPC error.
func DenyUnaryInterceptor(sink AuditSink, opts Options) grpc.UnaryServerInterceptor {
	opts.withDefaults()
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		// Install the shared marker so the inner interceptor can tell us it handled
		// this RPC. We thread the marked context inward.
		markedCtx, flag := contextWithAuditedMarker(ctx)

		resp, err := handler(markedCtx, req)

		// Only act on a DENY the inner interceptor did NOT already capture.
		if isDenyCode(status.Code(err)) && !alreadyAudited(flag) {
			// auth rejected before claims existed → actorFromContext yields anonymous.
			// req is passed so a ResourceExtractor (if any) can still derive a target,
			// but it never reads secrets (see Record's secret-free contract).
			if rec, ok := buildRecord(markedCtx, info.FullMethod, req, err, opts); ok {
				emit(markedCtx, sink, rec, opts.Logger)
			}
		}

		return resp, err
	}
}

// DenyStreamInterceptor is the streaming counterpart of DenyUnaryInterceptor: it
// captures an auth-level stream denial the inner stream interceptor never saw
// (auth's stream interceptor short-circuits before the inner audit interceptor for
// a bad/missing token). Same marker coordination, same best-effort discipline.
//
// NOTE on the stream context: a stream interceptor cannot swap the context the
// handler reads (the handler reads ss.Context()), so we wrap the ServerStream to
// return our marked context — the SAME wrappedServerStream idiom grpcutil's auth
// stream interceptor uses. The inner audit interceptor then flips the marker via
// ss.Context(), which is our marked context.
func DenyStreamInterceptor(sink AuditSink, opts Options) grpc.StreamServerInterceptor {
	opts.withDefaults()
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		markedCtx, flag := contextWithAuditedMarker(ss.Context())
		wrapped := &markerServerStream{ServerStream: ss, ctx: markedCtx}

		err := handler(srv, wrapped)

		if isDenyCode(status.Code(err)) && !alreadyAudited(flag) {
			if rec, ok := buildRecord(markedCtx, info.FullMethod, nil, err, opts); ok {
				emit(markedCtx, sink, rec, opts.Logger)
			}
		}

		return err
	}
}

// markerServerStream overrides Context() so the marked context (carrying the
// shared audited marker) is the one inner interceptors and the handler see. This
// mirrors grpcutil.wrappedServerStream; we keep a local copy rather than export
// grpcutil's so pkg/audit does not depend on grpcutil's unexported type.
type markerServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *markerServerStream) Context() context.Context { return w.ctx }

// isDenyCode reports whether a gRPC code is an authn/authz DENIAL — the codes whose
// denial is ALWAYS audited (failed access is the security signal). Kept in sync
// with decisionFor's DENY branch in interceptor.go.
func isDenyCode(code codes.Code) bool {
	return code == codes.Unauthenticated || code == codes.PermissionDenied
}
