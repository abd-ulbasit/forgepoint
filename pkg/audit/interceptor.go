package audit

import (
	"context"
	"log/slog"
	"time"

	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ============================================================================
// AUDIT INTERCEPTOR — capture every security-relevant action
// ============================================================================
//
// The interceptor runs the handler, then builds an audit Record from the
// POST-handler context (so the auth interceptor has already populated claims) and
// hands it to the sink. It is the CAPTURE half of the package's split.
//
// ============================================================================
// TWO CAPTURE POINTS — and WHY one is not enough (interview-critical)
// ============================================================================
//
// The chain the auth service wires is:
//
//	recovery → logging → AUDIT-DENY(outer) → auth → AUDIT(inner) → handler
//	                          ▲                ▲          ▲
//	      catches auth's      │       populates│          │reads claims auth set
//	      short-circuit DENY  │         claims │          │
//
// There are TWO audit interceptors, at two positions, because no single position
// can do both jobs:
//
//   - INNER (this file's UnaryServerInterceptor / StreamServerInterceptor): wired
//     AFTER auth, so for an ALLOWED call ClaimsFromContext returns the REAL caller
//     (the Record's Actor is the authenticated identity, not "anonymous"), and for
//     a HANDLER-level DENY (requireAdmin → PermissionDenied, which already has a
//     valid token) it records the DENY with that authenticated actor. But when the
//     AUTH interceptor itself rejects a request (missing/expired/forged token), it
//     returns BEFORE calling its inner handler — and the inner handler IS this
//     interceptor — so this one NEVER RUNS for a token-rejection denial.
//
//   - OUTER (deny.go's DenyUnaryInterceptor / DenyStreamInterceptor): wired BEFORE
//     auth, so it WRAPS auth and observes auth's Unauthenticated/PermissionDenied
//     return even when auth short-circuits. It records THAT denial (the anonymous
//     probe of a privileged method — the single most valuable audit signal). It
//     fires ONLY when the inner interceptor did NOT already record (a shared marker
//     in the context, set by the inner one), so an authenticated handler-DENY is
//     captured EXACTLY ONCE, by the inner interceptor with the real actor.
//
// WHY NOT just move audit outermost and drop the inner one? Because the inner
// interceptor reads claims from the context AUTH HANDS INWARD; that enriched
// context does NOT propagate back out to an outer interceptor's `ctx` variable. An
// outer-only audit would therefore see "anonymous" even on a successful
// authenticated call — losing the actor on every ALLOW. Two points is the price of
// capturing both (authenticated ALLOW with the real actor) and (anonymous DENY
// that auth rejected before claims existed).
//
// ============================================================================
// BEST-EFFORT ON THE HOT PATH (never break the RPC)
// ============================================================================
//
// Auditing is a side concern; it must not change the RPC's outcome. If the sink
// errors (NATS down, publish failed), we LOG and CONTINUE — the handler's response
// and error are passed through untouched. The alternative (failing the RPC because
// audit failed) would make the audit subsystem a single point of failure for the
// entire platform, which is unacceptable. We accept the documented gap: a sink
// outage loses audit records for that window (mitigated by JetStream durability
// once published, and by alerting on audit-publish errors).
// ============================================================================

// Options configures the audit interceptor.
type Options struct {
	// Source is the producing service name ("auth", "registry", ...). Stamped into
	// Record.SourceService. Required — an empty source makes provenance unauditable.
	Source string

	// Predicate decides which methods are audited on SUCCESS. nil → DefaultSecurityRelevant.
	// DENIED calls are ALWAYS audited regardless of the predicate.
	Predicate MethodPredicate

	// ResourceExtractor best-effort derives the target resource id from the request
	// for the Record.Resource field. nil → no resource is recorded (always safe).
	// It MUST NOT return secrets — it should read only identifier fields. Most
	// services leave this nil; a service that wants richer audit can supply one.
	ResourceExtractor func(fullMethod string, req any) string

	// Logger is used for the best-effort "audit sink failed" log line. nil → slog.Default().
	Logger *slog.Logger
}

func (o *Options) withDefaults() {
	if o.Predicate == nil {
		o.Predicate = DefaultSecurityRelevant
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// UnaryServerInterceptor returns the INNER unary interceptor that captures audit
// records for security-relevant unary RPCs and DENIED unary RPCs that reach it.
//
// FLOW per RPC:
//  1. Run the handler (which, in the chain, is the business handler — auth has
//     already run and populated claims by the time this interceptor is reached).
//  2. Compute the resulting status code + decision.
//  3. If we should audit (predicate says yes on success, OR it was denied/errored
//     in a security-relevant way), build a Record and hand it to the sink, and
//     MARK the context so the OUTER deny-interceptor knows this RPC was already
//     captured (so it does not double-record a handler-level DENY).
//  4. Return the handler's (resp, err) UNCHANGED.
func UnaryServerInterceptor(sink AuditSink, opts Options) grpc.UnaryServerInterceptor {
	opts.withDefaults()
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		resp, err := handler(ctx, req)

		// Build the audit context from the POST-handler ctx: auth has populated
		// claims by now (for allowed calls), and the handler may have enriched ctx.
		if rec, ok := buildRecord(ctx, info.FullMethod, req, err, opts); ok {
			// Flag the shared marker (if the outer deny-interceptor installed one)
			// BEFORE emitting: this RPC's record is ours, so the outer interceptor
			// must NOT also emit one. Marking even covers ALLOW/ERROR records — the
			// marker means "the inner interceptor handled this RPC's audit".
			markAudited(ctx)
			emit(ctx, sink, rec, opts.Logger)
		}

		// Pass the handler's result through untouched — audit never alters the RPC.
		return resp, err
	}
}

// StreamServerInterceptor is the streaming counterpart. It audits when the stream
// terminates (open → close), recording the final status. Streams are audited the
// same way: security-relevant method on success, or any denial.
//
// NOTE on claims for streams: the auth STREAM interceptor injects claims into the
// stream's Context() via a wrappedServerStream, so by the time the stream returns,
// ss.Context() carries the caller's claims — buildRecord reads them from there.
func StreamServerInterceptor(sink AuditSink, opts Options) grpc.StreamServerInterceptor {
	opts.withDefaults()
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		err := handler(srv, ss)

		// A streaming RPC has no single request message to derive a resource from,
		// so req is nil here (ResourceExtractor, if any, sees nil and returns "").
		if rec, ok := buildRecord(ss.Context(), info.FullMethod, nil, err, opts); ok {
			// Mark the stream's context as audited so the outer deny-interceptor
			// (which wraps auth) does not double-record this stream's denial.
			markAudited(ss.Context())
			emit(ss.Context(), sink, rec, opts.Logger)
		}

		return err
	}
}

// buildRecord constructs an audit Record from the post-handler context, method,
// request, and resulting error. It returns ok=false when the call should NOT be
// audited (a SUCCESSFUL, non-security-relevant method — the high-volume read case).
//
// THE DECISION TABLE (interview-worthy):
//
//	resulting code              │ Decision │ audited?
//	────────────────────────────┼──────────┼────────────────────────────────
//	OK (err == nil)             │ ALLOW    │ only if Predicate(method) == true
//	Unauthenticated             │ DENY     │ ALWAYS (failed access = the signal)
//	PermissionDenied            │ DENY     │ ALWAYS
//	any other error             │ ERROR    │ only if Predicate(method) == true
//
// Rationale for the last row: an Internal/NotFound on a pure read is noise, but on
// a mutating method it is "an attempted privileged action that failed" — worth
// keeping. So non-security errors are gated by the SAME predicate as successes,
// while authn/authz denials bypass the predicate entirely.
func buildRecord(ctx context.Context, fullMethod string, req any, err error, opts Options) (Record, bool) {
	code := status.Code(err)
	decision := decisionFor(code)

	// Decide whether to audit.
	switch decision {
	case DecisionDeny:
		// Always audit a denial — even on a read, even from an anonymous caller.
		// This is the core "capture DENIED access" requirement.
	default:
		// ALLOW or ERROR: gate on the security-relevance predicate (skip noisy
		// reads/health/reflection on success or non-security failure).
		if !opts.Predicate(fullMethod) {
			return Record{}, false
		}
	}

	rec := Record{
		Actor:         actorFromContext(ctx),
		Action:        fullMethod,
		Decision:      decision,
		GRPCCode:      code.String(),
		CorrelationID: correlationID(ctx),
		OccurredAt:    time.Now().UTC(),
		SourceService: opts.Source,
	}

	// Sanitized error message ONLY (the gRPC status message, never err.Error()
	// verbatim, never the request payload). Empty on success.
	if err != nil {
		rec.Err = sanitizeErr(err)
	}

	// Best-effort resource extraction. The extractor reads only identifier fields;
	// it must never surface a secret. nil extractor → empty resource (always safe).
	if opts.ResourceExtractor != nil && req != nil {
		rec.Resource = opts.ResourceExtractor(fullMethod, req)
	}

	return rec, true
}

// decisionFor maps a gRPC code to an audit Decision.
func decisionFor(code codes.Code) Decision {
	switch code {
	case codes.OK:
		return DecisionAllow
	case codes.Unauthenticated, codes.PermissionDenied:
		return DecisionDeny
	default:
		return DecisionError
	}
}

// actorFromContext projects grpcutil.Claims into an audit Actor, falling back to
// the anonymous sentinel when the request carried no claims (e.g. an
// unauthenticated request the auth interceptor rejected before populating claims).
//
// WHY anonymous-on-missing rather than erroring: a DENY with no claims is the MOST
// important record to keep (an unauthenticated probe of a privileged method). We
// must always be able to build a Record; "anonymous" is the honest actor for it.
func actorFromContext(ctx context.Context) Actor {
	claims, ok := grpcutil.ClaimsFromContext(ctx)
	if !ok || claims == nil {
		return Actor{UserID: AnonymousActor}
	}
	a := Actor{
		UserID: claims.UserID,
		Email:  claims.Email,
		Team:   claims.Team,
		Role:   claims.Role,
	}
	// Defensive: if claims exist but carry no user id (a malformed token that still
	// validated), record anonymous rather than an empty actor — an empty UserID in
	// the audit log is ambiguous; the sentinel is explicit.
	if a.UserID == "" {
		a.UserID = AnonymousActor
	}
	return a
}

// correlationID pulls the correlation id natsutil stashed in ctx (continuing the
// request's logical workflow), so the audit record joins to the wider trace.
func correlationID(ctx context.Context) string {
	if id, ok := natsutil.CorrelationIDFromContext(ctx); ok {
		return id
	}
	return ""
}

// sanitizeErr returns a safe-to-store message for an RPC error: the gRPC status
// MESSAGE only (which is client-facing and already sanitized at the auth boundary
// for non-client codes), never err.Error() (verbose "rpc error: code = …") and
// never the request payload. For a non-status error we return a constant — the raw
// string of a plain Go error can embed payload/PII.
//
// This mirrors grpcutil.sanitizeGRPCError's discipline: the audit log records that
// an action FAILED and its code; the full internal error lives in the service
// logs, not the durable audit trail.
func sanitizeErr(err error) string {
	if st, ok := status.FromError(err); ok {
		if msg := st.Message(); msg != "" {
			return msg
		}
		return st.Code().String()
	}
	return "non-status error"
}

// emit hands the record to the sink, treating any error as non-fatal: log and
// continue. This is the "audit never breaks the RPC" guarantee in one place.
func emit(ctx context.Context, sink AuditSink, rec Record, logger *slog.Logger) {
	if err := sink.Record(ctx, rec); err != nil {
		// Log at Warn (not Error) with the method + decision but WITHOUT the record's
		// potentially-identifying fields beyond what's already in service logs. An
		// alert on this log line signals the audit pipeline is degraded.
		logger.WarnContext(ctx, "audit sink failed (record not captured; RPC unaffected)",
			slog.String("audit.action", rec.Action),
			slog.String("audit.decision", string(rec.Decision)),
			slog.String("error", err.Error()),
		)
	}
}
