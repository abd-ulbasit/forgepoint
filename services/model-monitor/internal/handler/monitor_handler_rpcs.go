// monitor_handler_rpcs.go holds the per-RPC method bodies for MonitorHandler.
//
// The scaffold (monitor_handler.go) embeds UnimplementedMonitorServiceServer and
// holds the domain.MonitorService field. THIS file OVERRIDES each RPC with a real
// implementation. Once a method is defined here on *MonitorHandler, Go's method-set
// resolution prefers it over the embedded Unimplemented base — so every RPC below
// is now live.
//
// ============================================================================
// THE FIVE-STEP HANDLER CONTRACT (every method follows it)
// ============================================================================
//
//  1. NIL-SVC GUARD. A binary may be wired with svc==nil during the scaffold /
//     pre-repo phase (main.go passes nil until the Postgres/Redis/NATS adapters
//     land). Calling a nil interface's method panics. So every UNARY method first
//     checks h.svc==nil and returns codes.Unimplemented — the SAME code the
//     embedded base would return, so a half-wired binary behaves identically to
//     the scaffold instead of crashing. The SERVER-STREAMING RPC returns the
//     status ERROR directly (no nil response value to return on a stream).
//
//  2. VALIDATE THE REQUEST. Reject malformed input with codes.InvalidArgument and
//     a CLEAN message (no internal detail, no echo of secrets). We fail fast at
//     the transport boundary so the domain only ever sees well-formed input. The
//     domain ALSO validates (defense in depth) but the handler gives the client a
//     precise gRPC code AND avoids burning a DB round-trip on obvious garbage.
//
//  3. CONVERT proto -> domain, pulling SERVER-AUTHORITATIVE identity (owner_team)
//     from the auth interceptor's claims (grpcutil.ClaimsFromContext) — NEVER from
//     a client field. This is the cross-tenant guard: model names are NOT unique
//     across teams (team-a/"fraud" != team-b/"fraud"), so the team that scopes
//     every read/write MUST come from the validated token, not the request. The
//     proto ConfigureMonitorRequest deliberately has no owner_team field at all
//     (mass-assignment guard); even so, the handler is the place identity is
//     resolved.
//
//  4. CALL the domain service.
//
//  5. MAP its sentinel errors -> precise gRPC status codes via toStatusError, and
//     CONVERT the domain result -> proto. Enum conversions go through CHECKED
//     switches (not unchecked int casts) so a future enum divergence between the
//     domain and the proto is caught at the boundary, not silently mis-serialized.
//
// ============================================================================
// WHY ERROR MAPPING IS CENTRALIZED (toStatusError)
// ============================================================================
//
// gRPC clients branch on status.Code(err) — NotFound vs AlreadyExists vs
// FailedPrecondition vs Internal drive retry logic, user-facing messages, and
// alerting. A handler that returned bare domain errors would (a) leak internal
// text ("repository: ...", SQL fragments, PII) and (b) hand the client
// codes.Unknown, which no client can reason about. toStatusError is the single
// anti-corruption point that turns the domain's error vocabulary into the gRPC
// status vocabulary and SANITIZES anything it does not recognize down to
// codes.Internal with a fixed generic message. See toStatusError at the bottom.
//
// KEEPING INTERNAL ERRORS OFF THE WIRE: centralize
// the mapping; whitelist the codes you intend to expose; sanitize everything else
// to Internal with a fixed string; use errors.Is (not ==) so a wrapped sentinel
// still classifies; never str-format the raw error into the status message on the
// default path.
package handler

import (
	"context"
	"errors"
	"math"
	"time"

	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	monitorv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/monitor/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// errServiceNotWired is the canonical response when h.svc is nil (a binary
// running before the domain service is wired). It mirrors what the embedded
// UnimplementedMonitorServiceServer would return, so a half-wired binary behaves
// exactly like the scaffold instead of panicking on a nil-interface call. Unary
// RPCs return (nil, errServiceNotWired); the streaming RPC returns the same error
// value directly.
var errServiceNotWired = status.Error(codes.Unimplemented, "monitor service not wired")

// ============================================================================
// ConfigureMonitor — the only config write path (upsert)
// ============================================================================
//
// SERVER-AUTHORITATIVE TENANCY: owner_team comes from the caller's claims, never
// the request (the request has no such field by design). A monitor is keyed by
// (owner_team, model_name); resolving the team from the token is what prevents a
// caller from configuring a monitor against another tenant's same-named model.
//
// The domain owns the heavy validation (window monotonicity, threshold ladder,
// auto_retrain⇒pipeline). The handler does the cheap, transport-level checks
// (required model_name, non-negative/within-cap window bounds, valid enums) so a
// client gets a precise InvalidArgument before any DB work.
func (h *MonitorHandler) ConfigureMonitor(ctx context.Context, req *monitorv1.ConfigureMonitorRequest) (*monitorv1.ConfigureMonitorResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	ownerTeam, err := callerTeam(ctx)
	if err != nil {
		return nil, err
	}

	// VALIDATE. model_name identifies the monitor for the upsert — required.
	if req.GetModelName() == "" {
		return nil, status.Error(codes.InvalidArgument, "model_name is required")
	}

	// Window bounds: the proto uses int32, the domain uses int. We reject the
	// out-of-range/absurd values at the boundary (the live window is held in Redis;
	// an unbounded window_size is a memory footgun). The domain re-checks the
	// cross-field rules (0 <= min_samples <= window_size), but cheap range checks
	// here give a precise code without a service round-trip.
	if req.GetWindowSize() < 0 {
		return nil, status.Error(codes.InvalidArgument, "window_size must not be negative")
	}
	if req.GetWindowSize() > domain.MaxWindowSize {
		return nil, status.Error(codes.InvalidArgument, "window_size exceeds the maximum allowed")
	}
	if req.GetMinSamples() < 0 {
		return nil, status.Error(codes.InvalidArgument, "min_samples must not be negative")
	}

	// window_duration is optional, but if present it must be a valid, non-negative
	// duration. A malformed or negative duration could never close a window.
	var windowDuration = req.GetWindowDuration()
	if windowDuration != nil {
		if err := windowDuration.CheckValid(); err != nil {
			return nil, status.Error(codes.InvalidArgument, "window_duration is not a valid duration")
		}
		if windowDuration.AsDuration() < 0 {
			return nil, status.Error(codes.InvalidArgument, "window_duration must not be negative")
		}
	}

	// Convert thresholds with CHECKED enum mapping; an unknown enum value on the
	// wire is a client error, not something to silently coerce to "unspecified".
	thresholds, err := thresholdsFromProto(req.GetThresholds())
	if err != nil {
		return nil, err
	}

	in := domain.ConfigureMonitorInput{
		ModelName:         req.GetModelName(),
		WindowDuration:    durationFromProto(windowDuration),
		WindowSize:        int(req.GetWindowSize()),
		MinSamples:        int(req.GetMinSamples()),
		Thresholds:        thresholds,
		AutoRetrain:       req.GetAutoRetrain(),
		RetrainPipelineID: req.GetRetrainPipelineId(),
		Enabled:           req.GetEnabled(),
		IdempotencyKey:    req.GetIdempotencyKey(),
	}

	monitor, created, err := h.svc.ConfigureMonitor(ctx, ownerTeam, in)
	if err != nil {
		return nil, toStatusError(err)
	}

	return &monitorv1.ConfigureMonitorResponse{
		Monitor: monitorToProto(monitor),
		Created: created,
	}, nil
}

// ============================================================================
// DeleteMonitor — soft-delete (optionally purge history)
// ============================================================================
//
// Idempotent by contract: deleting an absent monitor is a successful no-op (the
// domain returns nil), so a retried DeleteMonitor does NOT surface NotFound. The
// purge path is team-scoped in the domain so a purge of team-a's "fraud" monitor
// can never destroy team-b's "fraud" history.
func (h *MonitorHandler) DeleteMonitor(ctx context.Context, req *monitorv1.DeleteMonitorRequest) (*monitorv1.DeleteMonitorResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	ownerTeam, err := callerTeam(ctx)
	if err != nil {
		return nil, err
	}

	if req.GetModelName() == "" {
		return nil, status.Error(codes.InvalidArgument, "model_name is required")
	}

	purged, err := h.svc.DeleteMonitor(ctx, ownerTeam, req.GetModelName(), req.GetPurgeReports())
	if err != nil {
		return nil, toStatusError(err)
	}

	return &monitorv1.DeleteMonitorResponse{
		PurgedReportCount: int32(purged),
	}, nil
}

// ============================================================================
// ResetBaseline — operator escape hatch to re-pin the drift baseline
// ============================================================================
//
// SECURITY: the client names a VERSION (empty = current production), never a
// distribution. The baseline distribution is SERVER-resolved from that version —
// a client that could upload a baseline could hand-craft one matching current
// traffic to suppress a real drift signal. ErrBaselineUnavailable (no captured
// distribution for the version) maps to FailedPrecondition: the request is
// well-formed but the system is not in a state to satisfy it yet.
func (h *MonitorHandler) ResetBaseline(ctx context.Context, req *monitorv1.ResetBaselineRequest) (*monitorv1.ResetBaselineResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	ownerTeam, err := callerTeam(ctx)
	if err != nil {
		return nil, err
	}

	if req.GetModelName() == "" {
		return nil, status.Error(codes.InvalidArgument, "model_name is required")
	}

	// baseline_version is intentionally optional: empty = the current PRODUCTION
	// version (the "refresh to latest" case). We forward it verbatim.
	monitor, err := h.svc.ResetBaseline(ctx, ownerTeam, req.GetModelName(), req.GetBaselineVersion())
	if err != nil {
		return nil, toStatusError(err)
	}

	return &monitorv1.ResetBaselineResponse{
		Monitor: monitorToProto(monitor),
	}, nil
}

// ============================================================================
// GetModelHealth — the at-a-glance "is this model healthy?" read
// ============================================================================
func (h *MonitorHandler) GetModelHealth(ctx context.Context, req *monitorv1.GetModelHealthRequest) (*monitorv1.GetModelHealthResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	ownerTeam, err := callerTeam(ctx)
	if err != nil {
		return nil, err
	}

	if req.GetModelName() == "" {
		return nil, status.Error(codes.InvalidArgument, "model_name is required")
	}

	health, err := h.svc.GetModelHealth(ctx, ownerTeam, req.GetModelName())
	if err != nil {
		return nil, toStatusError(err)
	}

	return &monitorv1.GetModelHealthResponse{
		Health: modelHealthToProto(health),
	}, nil
}

// ============================================================================
// GetMonitorStatus — the operator drill-in (live window + latest report)
// ============================================================================
func (h *MonitorHandler) GetMonitorStatus(ctx context.Context, req *monitorv1.GetMonitorStatusRequest) (*monitorv1.GetMonitorStatusResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	ownerTeam, err := callerTeam(ctx)
	if err != nil {
		return nil, err
	}

	if req.GetModelName() == "" {
		return nil, status.Error(codes.InvalidArgument, "model_name is required")
	}

	st, err := h.svc.GetMonitorStatus(ctx, ownerTeam, req.GetModelName())
	if err != nil {
		return nil, toStatusError(err)
	}

	return &monitorv1.GetMonitorStatusResponse{
		Status: monitorStatusToProto(st),
	}, nil
}

// ============================================================================
// ListMonitors — the caller team's fleet (paginated)
// ============================================================================
//
// PAGINATION normalization is the interesting part: the client's page_size is
// defaulted (when 0) and clamped (when over the cap) at the boundary. The
// min_severity / state filters are OPTIONAL — an _UNSPECIFIED enum means "no
// filter on that dimension", which is exactly the zero value the domain expects,
// so an unset filter needs no special-casing. Unknown enum values ARE rejected.
func (h *MonitorHandler) ListMonitors(ctx context.Context, req *monitorv1.ListMonitorsRequest) (*monitorv1.ListMonitorsResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	ownerTeam, err := callerTeam(ctx)
	if err != nil {
		return nil, err
	}

	opts, err := paginationFromProto(req.GetPagination())
	if err != nil {
		return nil, err
	}

	minSeverity, err := severityFromProto(req.GetMinSeverity())
	if err != nil {
		return nil, err
	}
	state, err := monitorStateFromProto(req.GetState())
	if err != nil {
		return nil, err
	}

	entries, nextToken, err := h.svc.ListMonitors(ctx, ownerTeam, minSeverity, state, opts)
	if err != nil {
		return nil, toStatusError(err)
	}

	out := make([]*monitorv1.ListMonitorsResponse_Entry, 0, len(entries))
	for _, e := range entries {
		out = append(out, &monitorv1.ListMonitorsResponse_Entry{
			Monitor: monitorToProto(e.Monitor),
			Health:  modelHealthToProto(e.Health),
		})
	}

	return &monitorv1.ListMonitorsResponse{
		Entries:    out,
		Pagination: paginationResponse(nextToken),
	}, nil
}

// ============================================================================
// GetDriftReport — fetch one report by id (team-scoped, IDOR-safe)
// ============================================================================
//
// SECURITY: report ids are NOT secret (they ride on events, retrain requests,
// deep links, and logs), so id alone is not an authorization control. The handler
// passes the claim-derived team to a team-scoped lookup; a report owned by another
// team comes back as ErrReportNotFound — INDISTINGUISHABLE from "no such id"
// (no enumeration oracle, and not a distinguishable "forbidden").
func (h *MonitorHandler) GetDriftReport(ctx context.Context, req *monitorv1.GetDriftReportRequest) (*monitorv1.GetDriftReportResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	ownerTeam, err := callerTeam(ctx)
	if err != nil {
		return nil, err
	}

	if req.GetReportId() == "" {
		return nil, status.Error(codes.InvalidArgument, "report_id is required")
	}

	report, err := h.svc.GetDriftReport(ctx, ownerTeam, req.GetReportId())
	if err != nil {
		return nil, toStatusError(err)
	}

	return &monitorv1.GetDriftReportResponse{
		Report: driftReportToProto(report),
	}, nil
}

// ============================================================================
// ListDriftReports — a model's drift history (paginated, filtered)
// ============================================================================
//
// TENANCY: the filter's OwnerTeam is set from claims and combined with model_name
// to scope the history — model names are not unique across teams, so scoping by
// model name alone would leak another team's same-named model's history.
func (h *MonitorHandler) ListDriftReports(ctx context.Context, req *monitorv1.ListDriftReportsRequest) (*monitorv1.ListDriftReportsResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	ownerTeam, err := callerTeam(ctx)
	if err != nil {
		return nil, err
	}

	// model_name is OPTIONAL. An empty value is the team-wide "recent drift across the
	// fleet" view (the dashboard tile + the unfiltered Monitoring page); a non-empty
	// value narrows to that one model. Tenancy is unaffected: ownerTeam (from claims) is
	// always the mandatory scope set on the filter below, so empty model_name lists only
	// THIS team's reports, never another tenant's. We therefore do NOT reject an empty
	// model_name here — that hard 400 was the bug that broke the dashboard tile.
	opts, err := paginationFromProto(req.GetPagination())
	if err != nil {
		return nil, err
	}

	minSeverity, err := severityFromProto(req.GetMinSeverity())
	if err != nil {
		return nil, err
	}

	// since/until are optional bounds; a zero (nil) timestamp means "unbounded on
	// that side". Reject a malformed timestamp at the boundary rather than letting
	// it become a confusing time deep in the domain. We also reject an inverted
	// range (since > until) as a clear client bug.
	since, err := optionalTime(req.GetSince(), "since")
	if err != nil {
		return nil, err
	}
	until, err := optionalTime(req.GetUntil(), "until")
	if err != nil {
		return nil, err
	}
	if !since.IsZero() && !until.IsZero() && since.After(until) {
		return nil, status.Error(codes.InvalidArgument, "since must not be after until")
	}

	f := domain.ReportFilter{
		OwnerTeam:   ownerTeam,
		ModelName:   req.GetModelName(),
		MinSeverity: minSeverity,
		Since:       since,
		Until:       until,
	}

	reports, nextToken, err := h.svc.ListDriftReports(ctx, ownerTeam, f, opts)
	if err != nil {
		return nil, toStatusError(err)
	}

	out := make([]*monitorv1.DriftReport, 0, len(reports))
	for _, r := range reports {
		out = append(out, driftReportToProto(r))
	}

	return &monitorv1.ListDriftReportsResponse{
		Reports:    out,
		Pagination: paginationResponse(nextToken),
	}, nil
}

// ============================================================================
// ListEvalScores — the L4 eval dashboard's read model (paginated, filtered)
// ============================================================================
//
// TENANCY: the filter's Team is set from claims (NEVER the request) and is the
// mandatory scope. model_name is OPTIONAL — empty is the team-wide cross-model eval
// view (the dashboard default); a non-empty value narrows to one model. Because Team
// always scopes the read, an empty model_name lists only THIS team's scores, never
// another tenant's same-named model. since is an OPTIONAL lower bound on created_at.
//
// SHAPE: this mirrors ListDriftReports exactly — same pagination normalization (default
// 20, cap 100), same "empty model_name is valid, not a 400", same claim-derived team —
// because an eval-score list is the same kind of team-scoped, keyset-paginated history
// read as a drift-report list, just over the eval_scores table.
func (h *MonitorHandler) ListEvalScores(ctx context.Context, req *monitorv1.ListEvalScoresRequest) (*monitorv1.ListEvalScoresResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	ownerTeam, err := callerTeam(ctx)
	if err != nil {
		return nil, err
	}

	// page_size is defaulted (0) and clamped (over cap) at the boundary; a negative
	// size is a client bug and is rejected (shared paginationFromProto).
	opts, err := paginationFromProto(req.GetPagination())
	if err != nil {
		return nil, err
	}

	// since is an OPTIONAL lower bound; nil means "unbounded". Reject a malformed
	// timestamp at the boundary rather than letting it become a confusing time in SQL.
	since, err := optionalTime(req.GetSince(), "since")
	if err != nil {
		return nil, err
	}

	f := domain.EvalFilter{
		// Team is intentionally NOT set from the request — the service overwrites it with
		// the claim-derived team; we set it here too for clarity, but the service is the
		// authority (defense in depth against a future caller that forgets to scope).
		Team:  ownerTeam,
		Model: req.GetModelName(),
		Since: since,
	}

	scores, nextToken, err := h.svc.ListEvalScores(ctx, ownerTeam, f, opts)
	if err != nil {
		return nil, toStatusError(err)
	}

	out := make([]*monitorv1.EvalScore, 0, len(scores))
	for _, e := range scores {
		out = append(out, evalScoreToProto(e))
	}

	return &monitorv1.ListEvalScoresResponse{
		Scores:     out,
		Pagination: paginationResponse(nextToken),
	}, nil
}

// ============================================================================
// SubmitGroundTruth — backfill delayed labels (batched)
// ============================================================================
//
// The response is NOT a bare ack: ground truth is fuzzy (some ids won't match an
// observed prediction, some aged out), so the caller needs to know what stuck.
// We cap the batch at the platform contract (MaxGroundTruthBatch) at the boundary
// to bound the transactional write, and we cap each label's length to keep a label
// a class name, not a PII smuggling vector.
func (h *MonitorHandler) SubmitGroundTruth(ctx context.Context, req *monitorv1.SubmitGroundTruthRequest) (*monitorv1.SubmitGroundTruthResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	ownerTeam, err := callerTeam(ctx)
	if err != nil {
		return nil, err
	}

	if req.GetModelName() == "" {
		return nil, status.Error(codes.InvalidArgument, "model_name is required")
	}
	if len(req.GetLabels()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one label is required")
	}
	if len(req.GetLabels()) > domain.MaxGroundTruthBatch {
		return nil, status.Error(codes.InvalidArgument, "label batch exceeds the maximum allowed")
	}

	labels := make([]domain.GroundTruthLabel, 0, len(req.GetLabels()))
	for _, l := range req.GetLabels() {
		// Each label must carry a join key (request_id) and an outcome; a label
		// with neither cannot be applied and is a client error.
		if l.GetRequestId() == "" {
			return nil, status.Error(codes.InvalidArgument, "label request_id is required")
		}
		if l.GetActualLabel() == "" {
			return nil, status.Error(codes.InvalidArgument, "label actual_label is required")
		}
		if len(l.GetActualLabel()) > domain.MaxLabelBytes {
			return nil, status.Error(codes.InvalidArgument, "label actual_label exceeds the maximum length")
		}
		observedAt, err := optionalTime(l.GetObservedAt(), "observed_at")
		if err != nil {
			return nil, err
		}
		labels = append(labels, domain.GroundTruthLabel{
			RequestID:   l.GetRequestId(),
			ActualLabel: l.GetActualLabel(),
			ObservedAt:  observedAt,
		})
	}

	result, err := h.svc.SubmitGroundTruth(ctx, ownerTeam, domain.SubmitGroundTruthInput{
		ModelName:      req.GetModelName(),
		Labels:         labels,
		IdempotencyKey: req.GetIdempotencyKey(),
	})
	if err != nil {
		return nil, toStatusError(err)
	}

	return &monitorv1.SubmitGroundTruthResponse{
		Accepted:            int32(result.Accepted),
		UnmatchedRequestIds: result.UnmatchedRequestIDs,
	}, nil
}

// ============================================================================
// StreamDriftEvents — server-streaming live drift feed
// ============================================================================
//
// SERVER-STREAMING SHAPE: one request in, many responses out, until the client
// cancels or the deadline elapses. The method returns only error — every payload
// flows through stream.Send. So the nil-svc guard returns the status ERROR (no nil
// response), and validation errors are returned (not sent) BEFORE the stream is
// used, which the client sees as the RPC's terminal status.
//
// The live subscription source (a NATS consumer fan-out / Redis pub-sub bridging
// the drift publisher into per-client channels) is part of the events-adapter
// phase and is NOT exposed on the current domain.MonitorService interface. Until
// that lands, this method does its handler-level job in full — nil guard, claims
// resolution (server-authoritative team scoping), request validation, ctx-
// cancellation awareness — and then returns codes.Unimplemented honestly, rather
// than blocking a client forever on a stream that can never produce an event.
//
// CTX CANCELLATION: when the stream source is wired, the loop is:
//
//	for {
//	    select {
//	    case <-stream.Context().Done():   // client canceled / deadline hit
//	        return stream.Context().Err()
//	    case ev, ok := <-sub.C:           // a drift event arrived
//	        if !ok { return nil }         // source closed
//	        if !passesFilter(ev) { continue }
//	        if err := stream.Send(&monitorv1.StreamDriftEventsResponse{
//	            Report: driftReportToProto(ev.Report),
//	        }); err != nil {
//	            return err                 // client gone / transport error
//	        }
//	    }
//	}
//
// We honor cancellation here too: if the client has already canceled before we
// reach the Unimplemented return, surface the context error so the terminal status
// reflects the real cause.
func (h *MonitorHandler) StreamDriftEvents(req *monitorv1.StreamDriftEventsRequest, stream monitorv1.MonitorService_StreamDriftEventsServer) error {
	if h.svc == nil {
		return errServiceNotWired
	}

	ctx := stream.Context()

	// Claims resolution on a stream uses the SAME context-claims path as unary
	// (the AuthStreamInterceptor wraps the ServerStream's Context()). Fail closed
	// if claims are absent — a stream that bypassed the interceptor is unauthorized.
	if _, err := callerTeam(ctx); err != nil {
		return err
	}

	// Validate the optional filters even on the (currently) Unimplemented path so
	// the contract is enforced and testable today: an unknown min_severity is a
	// client error, not a silent "no filter".
	if _, err := severityFromProto(req.GetMinSeverity()); err != nil {
		return err
	}

	// Respect a client that has already canceled / timed out: report that cause
	// rather than a misleading Unimplemented.
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}

	return status.Error(codes.Unimplemented, "StreamDriftEvents is not yet implemented")
}

// ============================================================================
// IDENTITY HELPER
// ============================================================================

// callerTeam resolves the SERVER-AUTHORITATIVE owner team from the auth
// interceptor's claims. Every monitor RPC is authenticated and team-scoped, so
// claims MUST be present and carry a team; their absence means the call bypassed
// the auth interceptor — fail closed with Unauthenticated. This single helper is
// where tenancy enters the domain, so the "team from token, never from request"
// invariant lives in ONE auditable place.
func callerTeam(ctx context.Context) (string, error) {
	claims, ok := grpcutil.ClaimsFromContext(ctx)
	if !ok || claims == nil || claims.Team == "" {
		return "", status.Error(codes.Unauthenticated, "missing authentication")
	}
	return claims.Team, nil
}

// ============================================================================
// REQUEST-SIDE CONVERTERS (proto -> domain)
// ============================================================================

// paginationFromProto normalizes the client's pagination into domain ListOptions.
// page_size is DEFAULTED when 0 and CLAMPED when over the cap (AIP-158: a client
// asking for "as much as possible" gets the max we serve, not an error); a
// NEGATIVE page_size is a clear client bug and is rejected. The page_token is an
// opaque cursor forwarded verbatim. A nil pagination message (the client omitted
// it entirely) is treated as "page one at the default size" — the getters return
// zero values, so the same normalization below handles it without special-casing.
func paginationFromProto(p *commonv1.PaginationRequest) (domain.ListOptions, error) {
	size := p.GetPageSize() // nil-safe: generated getter returns 0 on a nil receiver
	if size < 0 {
		return domain.ListOptions{}, status.Error(codes.InvalidArgument, "page_size must not be negative")
	}
	out := domain.ListOptions{
		PageSize:  int(size),
		PageToken: p.GetPageToken(),
	}
	if out.PageSize == 0 {
		out.PageSize = domain.DefaultListPageSize
	}
	if out.PageSize > domain.MaxListPageSize {
		out.PageSize = domain.MaxListPageSize
	}
	return out, nil
}

// thresholdsFromProto converts the repeated proto ThresholdConfig into domain
// types with CHECKED enum mapping. WHY checked: an unrecognized enum value on the
// wire (a client built against a newer/forked proto) is a client error we surface
// as InvalidArgument, not a value we silently coerce to "unspecified" and then
// fail to score on.
func thresholdsFromProto(in []*monitorv1.ThresholdConfig) ([]domain.ThresholdConfig, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]domain.ThresholdConfig, 0, len(in))
	for _, t := range in {
		dt, err := driftTypeFromProto(t.GetDriftType())
		if err != nil {
			return nil, err
		}
		m, err := driftMethodFromProto(t.GetMethod())
		if err != nil {
			return nil, err
		}
		out = append(out, domain.ThresholdConfig{
			DriftType:     dt,
			Method:        m,
			WarnScore:     t.GetWarnScore(),
			CriticalScore: t.GetCriticalScore(),
		})
	}
	return out, nil
}

// durationFromProto turns an optional proto Duration into a time.Duration (0 when
// absent). The caller has already validated it is non-negative and well-formed.
func durationFromProto(d *durationpb.Duration) time.Duration {
	if d == nil {
		return 0
	}
	return d.AsDuration()
}

// optionalTime converts an optional proto Timestamp into a time.Time, rejecting a
// malformed (out-of-range) timestamp at the boundary. A nil timestamp yields the
// zero time (meaning "unset"/"unbounded"). field is named in the error so the
// client knows which timestamp was bad.
func optionalTime(ts *timestamppb.Timestamp, field string) (time.Time, error) {
	if ts == nil {
		return time.Time{}, nil
	}
	if err := ts.CheckValid(); err != nil {
		return time.Time{}, status.Error(codes.InvalidArgument, field+" is not a valid timestamp")
	}
	return ts.AsTime(), nil
}

// ============================================================================
// CHECKED ENUM CONVERTERS — both directions
// ============================================================================
//
// These are deliberately exhaustive switches, NOT int casts. A cast
// (domain.DriftType(x)) would happily carry an out-of-range wire value into the
// domain (inbound) or emit an out-of-range proto value (outbound) — both silent
// data-corruption bugs. The switch makes any future enum divergence a visible
// failure at the boundary.

func driftTypeFromProto(x monitorv1.DriftType) (domain.DriftType, error) {
	switch x {
	case monitorv1.DriftType_DRIFT_TYPE_UNSPECIFIED:
		return domain.DriftTypeUnspecified, nil
	case monitorv1.DriftType_DRIFT_TYPE_DATA:
		return domain.DriftTypeData, nil
	case monitorv1.DriftType_DRIFT_TYPE_PREDICTION:
		return domain.DriftTypePrediction, nil
	case monitorv1.DriftType_DRIFT_TYPE_PERFORMANCE:
		return domain.DriftTypePerformance, nil
	default:
		return 0, status.Error(codes.InvalidArgument, "unknown drift_type")
	}
}

func driftTypeToProto(x domain.DriftType) monitorv1.DriftType {
	switch x {
	case domain.DriftTypeData:
		return monitorv1.DriftType_DRIFT_TYPE_DATA
	case domain.DriftTypePrediction:
		return monitorv1.DriftType_DRIFT_TYPE_PREDICTION
	case domain.DriftTypePerformance:
		return monitorv1.DriftType_DRIFT_TYPE_PERFORMANCE
	default:
		return monitorv1.DriftType_DRIFT_TYPE_UNSPECIFIED
	}
}

func driftMethodFromProto(x monitorv1.DriftMethod) (domain.DriftMethod, error) {
	switch x {
	case monitorv1.DriftMethod_DRIFT_METHOD_UNSPECIFIED:
		return domain.DriftMethodUnspecified, nil
	case monitorv1.DriftMethod_DRIFT_METHOD_PSI:
		return domain.DriftMethodPSI, nil
	case monitorv1.DriftMethod_DRIFT_METHOD_KL:
		return domain.DriftMethodKL, nil
	case monitorv1.DriftMethod_DRIFT_METHOD_KS:
		return domain.DriftMethodKS, nil
	default:
		return 0, status.Error(codes.InvalidArgument, "unknown drift method")
	}
}

func driftMethodToProto(x domain.DriftMethod) monitorv1.DriftMethod {
	switch x {
	case domain.DriftMethodPSI:
		return monitorv1.DriftMethod_DRIFT_METHOD_PSI
	case domain.DriftMethodKL:
		return monitorv1.DriftMethod_DRIFT_METHOD_KL
	case domain.DriftMethodKS:
		return monitorv1.DriftMethod_DRIFT_METHOD_KS
	default:
		return monitorv1.DriftMethod_DRIFT_METHOD_UNSPECIFIED
	}
}

func severityFromProto(x monitorv1.DriftSeverity) (domain.DriftSeverity, error) {
	switch x {
	case monitorv1.DriftSeverity_DRIFT_SEVERITY_UNSPECIFIED:
		return domain.DriftSeverityUnspecified, nil
	case monitorv1.DriftSeverity_DRIFT_SEVERITY_OK:
		return domain.DriftSeverityOK, nil
	case monitorv1.DriftSeverity_DRIFT_SEVERITY_WARNING:
		return domain.DriftSeverityWarning, nil
	case monitorv1.DriftSeverity_DRIFT_SEVERITY_CRITICAL:
		return domain.DriftSeverityCritical, nil
	default:
		return 0, status.Error(codes.InvalidArgument, "unknown min_severity")
	}
}

func severityToProto(x domain.DriftSeverity) monitorv1.DriftSeverity {
	switch x {
	case domain.DriftSeverityOK:
		return monitorv1.DriftSeverity_DRIFT_SEVERITY_OK
	case domain.DriftSeverityWarning:
		return monitorv1.DriftSeverity_DRIFT_SEVERITY_WARNING
	case domain.DriftSeverityCritical:
		return monitorv1.DriftSeverity_DRIFT_SEVERITY_CRITICAL
	default:
		return monitorv1.DriftSeverity_DRIFT_SEVERITY_UNSPECIFIED
	}
}

func monitorStateFromProto(x monitorv1.MonitorState) (domain.MonitorState, error) {
	switch x {
	case monitorv1.MonitorState_MONITOR_STATE_UNSPECIFIED:
		return domain.MonitorStateUnspecified, nil
	case monitorv1.MonitorState_MONITOR_STATE_PENDING_BASELINE:
		return domain.MonitorStatePendingBaseline, nil
	case monitorv1.MonitorState_MONITOR_STATE_WARMING_UP:
		return domain.MonitorStateWarmingUp, nil
	case monitorv1.MonitorState_MONITOR_STATE_ACTIVE:
		return domain.MonitorStateActive, nil
	case monitorv1.MonitorState_MONITOR_STATE_PAUSED:
		return domain.MonitorStatePaused, nil
	default:
		return 0, status.Error(codes.InvalidArgument, "unknown state")
	}
}

func monitorStateToProto(x domain.MonitorState) monitorv1.MonitorState {
	switch x {
	case domain.MonitorStatePendingBaseline:
		return monitorv1.MonitorState_MONITOR_STATE_PENDING_BASELINE
	case domain.MonitorStateWarmingUp:
		return monitorv1.MonitorState_MONITOR_STATE_WARMING_UP
	case domain.MonitorStateActive:
		return monitorv1.MonitorState_MONITOR_STATE_ACTIVE
	case domain.MonitorStatePaused:
		return monitorv1.MonitorState_MONITOR_STATE_PAUSED
	default:
		return monitorv1.MonitorState_MONITOR_STATE_UNSPECIFIED
	}
}

// ============================================================================
// RESPONSE-SIDE CONVERTERS (domain -> proto)
// ============================================================================
//
// These are the ANTI-CORRUPTION LAYER: the single place domain types become wire
// types. Centralizing them keeps the enum-mapping and time-conversion discipline
// in one auditable spot. timestamppb.New(zeroTime) yields a concrete proto
// timestamp; we map a domain ZERO time to a nil proto timestamp so "unset" stays
// unset on the wire (a 0001-01-01 epoch on the client would be misleading).

func monitorToProto(m domain.Monitor) *monitorv1.Monitor {
	return &monitorv1.Monitor{
		Id:                 m.ID,
		ModelName:          m.ModelName,
		OwnerTeam:          m.OwnerTeam,
		WindowDuration:     durationToProto(m.WindowDuration),
		WindowSize:         int32(m.WindowSize),
		MinSamples:         int32(m.MinSamples),
		Thresholds:         thresholdsToProto(m.Thresholds),
		AutoRetrain:        m.AutoRetrain,
		RetrainPipelineId:  m.RetrainPipelineID,
		State:              monitorStateToProto(m.State),
		BaselineVersion:    m.BaselineVersion,
		BaselineCapturedAt: nilableTimestamp(m.BaselineCapturedAt),
		CreatedAt:          nilableTimestamp(m.CreatedAt),
		UpdatedAt:          nilableTimestamp(m.UpdatedAt),
	}
}

func thresholdsToProto(in []domain.ThresholdConfig) []*monitorv1.ThresholdConfig {
	if len(in) == 0 {
		return nil
	}
	out := make([]*monitorv1.ThresholdConfig, 0, len(in))
	for _, t := range in {
		out = append(out, &monitorv1.ThresholdConfig{
			DriftType:     driftTypeToProto(t.DriftType),
			Method:        driftMethodToProto(t.Method),
			WarnScore:     t.WarnScore,
			CriticalScore: t.CriticalScore,
		})
	}
	return out
}

func driftMetricToProto(m domain.DriftMetric) *monitorv1.DriftMetric {
	return &monitorv1.DriftMetric{
		Name:          m.Name,
		Method:        driftMethodToProto(m.Method),
		Score:         m.Score,
		BaselineValue: m.BaselineValue,
		CurrentValue:  m.CurrentValue,
		Severity:      severityToProto(m.Severity),
	}
}

func driftReportToProto(r domain.DriftReport) *monitorv1.DriftReport {
	metrics := make([]*monitorv1.DriftMetric, 0, len(r.Metrics))
	for _, m := range r.Metrics {
		metrics = append(metrics, driftMetricToProto(m))
	}
	return &monitorv1.DriftReport{
		Id:           r.ID,
		MonitorId:    r.MonitorID,
		ModelName:    r.ModelName,
		ModelVersion: r.ModelVersion,
		DriftType:    driftTypeToProto(r.DriftType),
		Severity:     severityToProto(r.Severity),
		Metrics:      metrics,
		WindowId:     r.WindowID,
		SampleCount:  int32(r.SampleCount),
		WindowStart:  nilableTimestamp(r.WindowStart),
		WindowEnd:    nilableTimestamp(r.WindowEnd),
		CreatedAt:    nilableTimestamp(r.CreatedAt),
	}
}

// evalScoreToProto maps one domain.Eval (the L4 quality-eval row) to the proto
// EvalScore. The domain stores the axes as DOUBLE PRECISION on the 1–5 scale (the judge
// can emit fractional/clamped values, and Overall is the mean of three), but the proto
// EvalScore axes are int32 — a deliberate UI contract (a dashboard renders "4/5 stars",
// not "4.33"). So we ROUND each axis to the nearest whole star. An UNSCORED row carries
// all-zero axes and scored=false; rounding leaves those 0s as-is, so the dashboard can
// render it distinctly from a low (but judged) score. request_id and created_at ride
// along for the per-row drill-in; created_at maps to nil when zero (unset stays unset).
func evalScoreToProto(e domain.Eval) *monitorv1.EvalScore {
	return &monitorv1.EvalScore{
		Model:     e.Model,
		Relevance: roundScore(e.Scores.Relevance),
		Coherence: roundScore(e.Scores.Coherence),
		Safety:    roundScore(e.Scores.Safety),
		Overall:   roundScore(e.Scores.Overall),
		Scored:    e.Scores.Scored,
		RequestId: e.RequestID,
		CreatedAt: nilableTimestamp(e.CreatedAt),
	}
}

// roundScore rounds a 1–5 float axis to the nearest int for the proto's int32 star
// rating. math.Round gives banker's-free half-away-from-zero rounding (4.5 → 5), which
// matches how a UI would round a star average. Negative is impossible on the 1–5 scale
// (an unscored row carries 0), but Round is total so it is safe regardless.
func roundScore(v float64) int32 {
	return int32(math.Round(v))
}

func monitorStatusToProto(s domain.MonitorStatus) *monitorv1.MonitorStatus {
	out := &monitorv1.MonitorStatus{
		Monitor:              monitorToProto(s.Monitor),
		State:                monitorStateToProto(s.State),
		CurrentWindowSamples: int32(s.CurrentWindowSamples),
		LastEventAt:          nilableTimestamp(s.LastEventAt),
		DriftEventsTotal:     s.DriftEventsTotal,
	}
	// LatestReport is nil until the first window closes — keep it nil on the wire.
	if s.LatestReport != nil {
		out.LatestReport = driftReportToProto(*s.LatestReport)
	}
	return out
}

func modelHealthToProto(h domain.ModelHealth) *monitorv1.ModelHealth {
	// SeverityByType is keyed by the DriftType enum's INTEGER value (proto3 map
	// keys cannot be enums). We map each domain key/value through the checked
	// converters so a forged/unknown domain enum cannot smuggle a bad wire value.
	var byType map[int32]monitorv1.DriftSeverity
	if len(h.SeverityByType) > 0 {
		byType = make(map[int32]monitorv1.DriftSeverity, len(h.SeverityByType))
		for dt, sev := range h.SeverityByType {
			byType[int32(driftTypeToProto(dt))] = severityToProto(sev)
		}
	}
	return &monitorv1.ModelHealth{
		ModelName:        h.ModelName,
		ModelVersion:     h.ModelVersion,
		OverallSeverity:  severityToProto(h.OverallSeverity),
		SeverityByType:   byType,
		State:            monitorStateToProto(h.State),
		LatestReportId:   h.LatestReportID,
		LastEventAt:      nilableTimestamp(h.LastEventAt),
		DriftEventsTotal: h.DriftEventsTotal,
	}
}

// paginationResponse wraps the domain's opaque next-page cursor. TotalCount is
// left at 0: the domain's list ports use cursor pagination and do not compute an
// exact total (which can require a full scan), and the proto's PaginationResponse
// explicitly allows omitting it.
func paginationResponse(nextToken string) *commonv1.PaginationResponse {
	return &commonv1.PaginationResponse{NextPageToken: nextToken}
}

// durationToProto turns a domain time.Duration into a proto Duration, or nil when
// zero (0 = "unbounded by time"; we keep that unset on the wire rather than
// emitting a 0s duration that reads like a configured bound).
func durationToProto(d time.Duration) *durationpb.Duration {
	if d == 0 {
		return nil
	}
	return durationpb.New(d)
}

// nilableTimestamp maps a domain zero time to a nil proto timestamp so "unset"
// stays unset on the wire (a concrete 0001-01-01 epoch would mislead the client).
func nilableTimestamp(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// ============================================================================
// ERROR MAPPING — domain sentinels -> gRPC status codes
// ============================================================================
//
//	domain sentinel              -> gRPC code            why
//	-------------------------------------------------------------------------
//	ErrValidation                -> InvalidArgument      client sent bad input
//	ErrMonitorNotFound           -> NotFound             no monitor for that model
//	ErrReportNotFound            -> NotFound             no such report (IDOR-safe)
//	ErrRetrainPipelineRequired   -> InvalidArgument      auto_retrain w/o pipeline
//	ErrBaselineUnavailable       -> FailedPrecondition   not in a state to satisfy
//	(anything else)              -> Internal (generic)   SANITIZED — no leak
//
// WHY ErrBaselineUnavailable -> FailedPrecondition (not NotFound): the request is
// well-formed and the model may well exist; the system is simply not yet in a
// state where a baseline can be resolved (Registry/Experiment-Tracker captured
// none). FailedPrecondition tells a client "valid request, retry once the
// precondition holds" — distinct from NotFound ("the thing you named is gone")
// and from InvalidArgument ("fix your request and it'll work"). This mirrors the
// gRPC code guidance and lets a caller branch correctly (e.g., the CLI prints
// "baseline not captured yet" rather than "model not found").
//
// WHY ErrRetrainPipelineRequired -> InvalidArgument: arming auto_retrain with no
// pipeline is a CLIENT misconfiguration of the request, fixable by the client, so
// it is a 4xx-class argument error, not a server fault.
//
// errors.Is (not ==) is used so a wrapped sentinel (fmt.Errorf("...: %w", X))
// still classifies. The default sanitizes to Internal with a FIXED message — any
// unrecognized error may wrap SQL text, a connection string, a file path, or PII;
// the real error is logged server-side by the logging interceptor.
func toStatusError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, domain.ErrValidation):
		// Wrapped ErrValidation carries a specific, NON-sensitive message
		// describing the client's own bad input (e.g. "monitor: validation failed:
		// warn_score must be <= critical_score"). Safe and useful to forward — it
		// names no internal state. This is the ONLY whitelisted err.Error() path.
		return status.Error(codes.InvalidArgument, err.Error())

	case errors.Is(err, domain.ErrRetrainPipelineRequired):
		// A dedicated validation-class sentinel. Its text is non-sensitive and
		// actionable, so we forward it.
		return status.Error(codes.InvalidArgument, err.Error())

	case errors.Is(err, domain.ErrMonitorNotFound):
		return status.Error(codes.NotFound, "monitor not found")

	case errors.Is(err, domain.ErrReportNotFound):
		return status.Error(codes.NotFound, "drift report not found")

	case errors.Is(err, domain.ErrBaselineUnavailable):
		return status.Error(codes.FailedPrecondition, "baseline unavailable for the requested version")

	default:
		// SANITIZE: never expose err.Error() — it may wrap SQL, secrets, or PII.
		// The real error is logged by the logging interceptor server-side; the
		// client gets a constant, opaque message and codes.Internal.
		return status.Error(codes.Internal, "internal error")
	}
}
