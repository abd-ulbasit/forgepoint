// notification_handler_rpcs.go holds the per-RPC method bodies for
// NotificationHandler.
//
// The scaffold (notification_handler.go) embeds UnimplementedNotificationServiceServer
// and holds the domain.NotificationService field. This file OVERRIDES each RPC
// with a real implementation. Once a method is defined here on
// *NotificationHandler, Go's method-set resolution prefers it over the embedded
// Unimplemented base — so every RPC below is now live, and only RPCs we have NOT
// written fall through to Unimplemented (there are none left; all seven are here).
//
// ============================================================================
// THE FIVE-STEP HANDLER CONTRACT (every method follows it — mirrors auth)
// ============================================================================
//
//  1. NIL-SVC GUARD. A binary may be wired with svc==nil during the scaffold /
//     pre-repo phase (main.go passes nil until the repository phase lands the
//     Postgres adapter). Calling a nil interface's method panics. So every method
//     first checks h.svc==nil and returns codes.Unimplemented — the SAME code the
//     embedded base would return, so a half-wired binary behaves identically to
//     the scaffold instead of crashing. (All RPCs here are unary, so the guard
//     returns (nil, err); this service exposes NO streaming RPC by design — see
//     the proto's "WHY NO STREAMING RPC HERE" note. For a streaming RPC the guard
//     would instead `return errServiceNotWired` with no nil response value.)
//
//  2. VALIDATE THE REQUEST. Reject malformed input with codes.InvalidArgument and
//     a CLEAN message (no internal detail, no echo of secrets/targets). We fail
//     fast at the transport boundary so the domain only ever sees well-formed
//     input. The domain ALSO validates (defense in depth — page-size clamping,
//     SSRF, batch caps) but the handler gives the client a precise gRPC code for
//     the structural checks it owns (enum-in-range, non-negative page size).
//
//  3. RESOLVE IDENTITY FROM CLAIMS. The security-critical step for THIS service:
//     ListNotifications / GetNotification / MarkRead / ListDeliveryAttempts /
//     Get|UpdatePreferences / TestChannel have NO user_id on the wire. The
//     recipient/owner is ALWAYS the authenticated caller, pulled from
//     grpcutil.ClaimsFromContext (set by the auth interceptor). Accepting a
//     user_id from the request body would be an IDOR / mass-assignment hole — one
//     user reading or editing another's inbox/prefs. There is no such field to
//     trust, and we never invent one.
//
//  4. CONVERT proto -> domain input, then CALL the domain service.
//
//  5. MAP sentinel errors -> precise gRPC status codes via toStatusError, and
//     CONVERT the domain result -> proto. Converters live at the bottom; they are
//     the single place a domain type becomes a wire type, and the single place the
//     "never echo a channel target/secret onto a Notification/DeliveryAttempt
//     response" invariant is enforced (DeliveryAttempt carries no target field at
//     all — see the proto's SECURITY note).
//
// ============================================================================
// WHY ERROR MAPPING IS CENTRALIZED (toStatusError)
// ============================================================================
//
// gRPC clients branch on status.Code(err) — NotFound vs FailedPrecondition vs
// Internal drive retry logic, user-facing messages, and alerting. A handler that
// returned bare domain errors would leak internal text ("notification: ...", SQL
// fragments, a webhook URL, PII) to every caller and give them codes.Unknown,
// which no client can reason about. toStatusError is the single anti-corruption
// point that turns domain vocabulary into the gRPC status vocabulary and
// SANITIZES anything it doesn't recognize down to codes.Internal with a fixed
// generic message. See toStatusError at the bottom.
package handler

import (
	"context"
	"errors"
	"strings"

	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	notificationv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/notification/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/services/notification/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ============================================================================
// BOUNDARY CONSTANTS
// ============================================================================

const (
	// maxMarkReadBatch mirrors the proto's documented cap (1000 ids per call) and
	// the domain's own maxMarkReadBatch. We reject over-cap batches at the boundary
	// with InvalidArgument so the client gets a precise code; the domain also caps
	// (defense in depth) but its rejection would round-trip as the same code via
	// toStatusError. Catching it here saves a needless trip into the domain.
	maxMarkReadBatch = 1000

	// maxMutePatterns mirrors the proto's cap (100 mute patterns) and the domain's
	// maxMutePatterns. Same rationale as maxMarkReadBatch.
	maxMutePatterns = 100
)

// errServiceNotWired is the canonical response when h.svc is nil (a binary
// running before the domain service is wired — main.go passes nil today). It
// mirrors what the embedded UnimplementedNotificationServiceServer would return,
// so a half-wired binary behaves exactly like the scaffold instead of panicking
// on a nil-interface method call.
var errServiceNotWired = status.Error(codes.Unimplemented, "notification service not wired")

// ============================================================================
// callerID — the ONE place identity is resolved (anti-IDOR)
// ============================================================================
//
// Every RPC on this surface is scoped to the authenticated caller; none accepts a
// user_id on the wire. callerID resolves that identity from the interceptor-set
// claims. Absence of claims means the call bypassed the auth interceptor — we
// FAIL CLOSED with Unauthenticated rather than defaulting to some user, which
// would be a catastrophic auth bypass. Centralizing this means no RPC can forget
// the check or read the id from anywhere untrusted.
func callerID(ctx context.Context) (string, error) {
	claims, ok := grpcutil.ClaimsFromContext(ctx)
	if !ok || claims == nil || claims.UserID == "" {
		return "", status.Error(codes.Unauthenticated, "missing authentication")
	}
	return claims.UserID, nil
}

// ============================================================================
// ListNotifications (inbox read model)
// ============================================================================
//
// PAGINATION: we forward page_size/token to the domain, which CLAMPS the size
// (0 → default 20, over-cap → 100). We do not re-clamp here; we only reject an
// outright negative page_size with InvalidArgument (a clearly malformed request,
// distinct from "0 means use the default"). The domain is the authority on the
// cap so the contract is enforced in one place regardless of caller.
//
// AUTHORITY: RecipientUserID comes from claims, never the request (the proto has
// no user_id field — see its anti-IDOR note).
func (h *NotificationHandler) ListNotifications(ctx context.Context, req *notificationv1.ListNotificationsRequest) (*notificationv1.ListNotificationsResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	// VALIDATE: enum-in-range + non-negative page size. min_severity is an optional
	// FLOOR; UNSPECIFIED(0) means "no floor", so it is valid — we only reject a
	// value outside the closed enum (a client sending garbage like 99).
	if !validSeverity(req.GetMinSeverity()) {
		return nil, status.Error(codes.InvalidArgument, "min_severity is not a valid severity")
	}
	if p := req.GetPagination(); p != nil && p.GetPageSize() < 0 {
		return nil, status.Error(codes.InvalidArgument, "page_size must not be negative")
	}

	uid, err := callerID(ctx)
	if err != nil {
		return nil, err
	}

	// CONVERT proto -> domain input. RecipientUserID is server-authoritative.
	out, err := h.svc.ListNotifications(ctx, domain.ListNotificationsInput{
		RecipientUserID: uid,
		UnreadOnly:      req.GetUnreadOnly(),
		MinSeverity:     domain.Severity(req.GetMinSeverity()),
		EventTypeFilter: req.GetEventTypeFilter(),
		PageSize:        pageSizeOf(req.GetPagination()),
		PageToken:       pageTokenOf(req.GetPagination()),
	})
	if err != nil {
		return nil, toStatusError(err)
	}

	// CONVERT domain -> proto.
	return &notificationv1.ListNotificationsResponse{
		Notifications: notificationsToProto(out.Notifications),
		Pagination: &commonv1.PaginationResponse{
			NextPageToken: out.NextPageToken,
			// TotalCount is left at 0 — the domain's list does not compute an exact
			// fleet total (an indexed COUNT is the unread badge below, not the total).
			// The proto allows omitting it (see common.proto's note).
		},
		UnreadCount: int32(out.UnreadCount),
	}, nil
}

// ============================================================================
// GetNotification (inbox detail + per-channel delivery history)
// ============================================================================
//
// ANTI-IDOR: the domain returns ErrNotFound for an id that does not exist OR
// belongs to another user — the existence of someone else's notification is never
// leaked. toStatusError maps that single sentinel to codes.NotFound, so the
// caller cannot distinguish "missing" from "not yours". This is the same
// anti-enumeration reasoning as auth's single ErrInvalidCredentials.
func (h *NotificationHandler) GetNotification(ctx context.Context, req *notificationv1.GetNotificationRequest) (*notificationv1.GetNotificationResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	uid, err := callerID(ctx)
	if err != nil {
		return nil, err
	}

	out, err := h.svc.GetNotification(ctx, uid, req.GetId())
	if err != nil {
		return nil, toStatusError(err)
	}

	return &notificationv1.GetNotificationResponse{
		Notification:     notificationToProto(out.Notification),
		DeliveryAttempts: deliveryAttemptsToProto(out.DeliveryAttempts),
	}, nil
}

// ============================================================================
// MarkRead (inbox mutation — the only client write to the inbox)
// ============================================================================
//
// Naturally idempotent (read→read is a no-op), so it carries no idempotency key.
// Foreign/unknown ids are SKIPPED by the domain rather than failing the batch, so
// a partially-stale client list still makes progress. We enforce the proto's
// 1000-id cap at the boundary so an oversized batch is rejected with a precise
// InvalidArgument before any write.
func (h *NotificationHandler) MarkRead(ctx context.Context, req *notificationv1.MarkReadRequest) (*notificationv1.MarkReadResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	// VALIDATE: a call must select SOMETHING — either mark_all or a non-empty id
	// list. A request with neither is a no-op the client almost certainly didn't
	// mean; reject it so a bug isn't masked as a silent success.
	if !req.GetMarkAll() && len(req.GetIds()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "ids must not be empty when mark_all is false")
	}
	// VALIDATE the batch cap (only meaningful when not mark_all).
	if !req.GetMarkAll() && len(req.GetIds()) > maxMarkReadBatch {
		return nil, status.Error(codes.InvalidArgument, "too many ids in MarkRead batch")
	}

	uid, err := callerID(ctx)
	if err != nil {
		return nil, err
	}

	out, err := h.svc.MarkRead(ctx, domain.MarkReadInput{
		RecipientUserID: uid,
		Ids:             req.GetIds(),
		MarkAll:         req.GetMarkAll(),
	})
	if err != nil {
		return nil, toStatusError(err)
	}

	return &notificationv1.MarkReadResponse{
		MarkedCount: int32(out.MarkedCount),
		UnreadCount: int32(out.UnreadCount),
	}, nil
}

// ============================================================================
// ListDeliveryAttempts (the GetDeliveryLog audit surface)
// ============================================================================
//
// The cross-notification delivery-health view ("did my alerts go out, which
// failed?"). Scoped to the caller; channel/status/notification are optional
// predicates. If notification_id is set and is foreign/unknown, the domain
// returns ErrNotFound (no existence leak). Targets/secrets are NEVER echoed (the
// DeliveryAttempt proto has no target field).
//
// PARALLEL SLICE: the proto returns attempts + a positionally-aligned
// notification_ids slice (so the UI can link each row to its inbox entry). The
// id lives on the DOMAIN DeliveryAttempt; the handler splits it into the proto's
// parallel slice at the boundary — the proto keeps the id OFF the attempt message
// because GetNotification already implies it.
func (h *NotificationHandler) ListDeliveryAttempts(ctx context.Context, req *notificationv1.ListDeliveryAttemptsRequest) (*notificationv1.ListDeliveryAttemptsResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	// VALIDATE optional enum filters in range (UNSPECIFIED = "no filter", valid).
	if !validChannel(req.GetChannel()) {
		return nil, status.Error(codes.InvalidArgument, "channel is not a valid channel")
	}
	if !validStatus(req.GetStatus()) {
		return nil, status.Error(codes.InvalidArgument, "status is not a valid delivery status")
	}
	if p := req.GetPagination(); p != nil && p.GetPageSize() < 0 {
		return nil, status.Error(codes.InvalidArgument, "page_size must not be negative")
	}

	uid, err := callerID(ctx)
	if err != nil {
		return nil, err
	}

	out, err := h.svc.ListDeliveryAttempts(ctx, domain.ListDeliveryAttemptsInput{
		RecipientUserID: uid,
		Channel:         domain.NotificationChannel(req.GetChannel()),
		Status:          domain.DeliveryStatus(req.GetStatus()),
		NotificationID:  req.GetNotificationId(),
		PageSize:        pageSizeOf(req.GetPagination()),
		PageToken:       pageTokenOf(req.GetPagination()),
	})
	if err != nil {
		return nil, toStatusError(err)
	}

	// Split the domain attempts into the proto's parallel (attempts, ids) slices.
	attempts := make([]*notificationv1.DeliveryAttempt, len(out.Attempts))
	ids := make([]string, len(out.Attempts))
	for i, a := range out.Attempts {
		attempts[i] = deliveryAttemptToProto(a)
		ids[i] = a.NotificationID
	}

	return &notificationv1.ListDeliveryAttemptsResponse{
		Attempts:        attempts,
		NotificationIds: ids,
		Pagination: &commonv1.PaginationResponse{
			NextPageToken: out.NextPageToken,
		},
	}, nil
}

// ============================================================================
// GetPreferences (settings read)
// ============================================================================
//
// Always the caller's own preferences (the proto has no user_id). A
// never-configured user gets sensible DEFAULTS (IN_APP enabled) from the domain,
// not an error, so the settings UI always has something to render.
func (h *NotificationHandler) GetPreferences(ctx context.Context, _ *notificationv1.GetPreferencesRequest) (*notificationv1.GetPreferencesResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	uid, err := callerID(ctx)
	if err != nil {
		return nil, err
	}

	prefs, err := h.svc.GetPreferences(ctx, uid)
	if err != nil {
		return nil, toStatusError(err)
	}

	return &notificationv1.GetPreferencesResponse{
		Preferences: preferencesToProto(prefs),
	}, nil
}

// ============================================================================
// UpdatePreferences (settings write — PUT semantics + SSRF validation)
// ============================================================================
//
// SECURITY (the headline control for this service): every webhook/slack target is
// SSRF-validated SERVER-SIDE before persisting. The handler does NOT re-implement
// that guard — it lives in the domain (pure scheme/host/IP-literal checks) and the
// delivery adapter (DNS + pinned-IP connect). The handler's job is to forward the
// desired channel set with the OWNER pinned to the caller (anti mass-assignment —
// UserID never comes from the request) and to surface the domain's
// ErrSSRFTargetRejected/ErrValidation as codes.InvalidArgument.
//
// updated_at is server-stamped; any client value is ignored (the proto carries no
// updated_at on the request).
func (h *NotificationHandler) UpdatePreferences(ctx context.Context, req *notificationv1.UpdatePreferencesRequest) (*notificationv1.UpdatePreferencesResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	// VALIDATE the mute-list cap at the boundary (precise InvalidArgument). The
	// domain also caps it (defense in depth).
	if len(req.GetMutedEventPatterns()) > maxMutePatterns {
		return nil, status.Error(codes.InvalidArgument, "too many muted_event_patterns")
	}
	// VALIDATE each channel's enum is in range. We do NOT SSRF-validate targets
	// here — that is the domain/adapter's job (DNS-aware, the authoritative guard).
	// We only reject a structurally-invalid channel enum so the domain isn't handed
	// an out-of-range value.
	for _, cp := range req.GetChannels() {
		if !validChannel(cp.GetChannel()) {
			return nil, status.Error(codes.InvalidArgument, "channel is not a valid channel")
		}
		if !validSeverity(cp.GetMinSeverity()) {
			return nil, status.Error(codes.InvalidArgument, "min_severity is not a valid severity")
		}
	}

	uid, err := callerID(ctx)
	if err != nil {
		return nil, err
	}

	// CONVERT proto -> domain. UserID is the caller (server-authoritative), NEVER
	// from the request body.
	stored, err := h.svc.UpdatePreferences(ctx, domain.UpdatePreferencesInput{
		UserID:             uid,
		Channels:           channelPreferencesFromProto(req.GetChannels()),
		MutedEventPatterns: req.GetMutedEventPatterns(),
		IdempotencyKey:     req.GetIdempotencyKey(),
	})
	if err != nil {
		return nil, toStatusError(err)
	}

	return &notificationv1.UpdatePreferencesResponse{
		Preferences: preferencesToProto(stored),
	}, nil
}

// ============================================================================
// TestChannel (owner self-test — NOT a producer "send" RPC)
// ============================================================================
//
// SECURITY: the request carries NO target URL (so it cannot be an SSRF probe);
// the destination is the caller's stored, already-validated channel preference.
// A delivery failure (including a late SSRF rejection at the adapter) is reported
// as a FAILED test OUTCOME in the response (so the settings UI can show "✗ 401
// from Slack"), NOT as a gRPC error. An UNCONFIGURED channel, by contrast, IS an
// error: ErrChannelNotConfigured → codes.FailedPrecondition ("fix your config
// first"), which is a state problem, not a bad request.
func (h *NotificationHandler) TestChannel(ctx context.Context, req *notificationv1.TestChannelRequest) (*notificationv1.TestChannelResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	// VALIDATE: a test must name a real, in-range channel, and testing the
	// "unspecified" channel is meaningless (there is no such transport to test).
	if !validChannel(req.GetChannel()) || req.GetChannel() == notificationv1.NotificationChannel_NOTIFICATION_CHANNEL_UNSPECIFIED {
		return nil, status.Error(codes.InvalidArgument, "channel must be a valid, specified channel")
	}

	uid, err := callerID(ctx)
	if err != nil {
		return nil, err
	}

	out, err := h.svc.TestChannel(ctx, domain.TestChannelInput{
		UserID:         uid,
		Channel:        domain.NotificationChannel(req.GetChannel()),
		IdempotencyKey: req.GetIdempotencyKey(),
	})
	if err != nil {
		return nil, toStatusError(err)
	}

	// CONVERT outcome -> proto. ErrorMessage here is the operator-facing delivery
	// detail the proto explicitly carries (e.g. "401 from Slack"); it is OUR
	// delivery outcome, never the target URL or an auth header (the domain builds
	// it from the Notifier result, which omits secrets).
	return &notificationv1.TestChannelResponse{
		Status:       notificationv1.DeliveryStatus(out.Status),
		ResponseCode: out.ResponseCode,
		ErrorMessage: out.ErrorMessage,
	}, nil
}

// ============================================================================
// ENUM VALIDATION (structural, boundary-owned)
// ============================================================================
//
// The proto enums are CLOSED sets. A well-behaved client sends a defined value,
// but the wire is untrusted — a buggy or malicious client can send any int32. We
// reject out-of-range values at the boundary so the domain (whose mirrored enums
// have the SAME values) never reasons over a phantom enum. UNSPECIFIED(0) is a
// VALID value for every one of these — as a filter it means "no filter", as a
// floor it means "no floor" — so it is intentionally accepted; only values
// outside the defined range are rejected.

func validChannel(c notificationv1.NotificationChannel) bool {
	switch c {
	case notificationv1.NotificationChannel_NOTIFICATION_CHANNEL_UNSPECIFIED,
		notificationv1.NotificationChannel_NOTIFICATION_CHANNEL_IN_APP,
		notificationv1.NotificationChannel_NOTIFICATION_CHANNEL_WEBHOOK,
		notificationv1.NotificationChannel_NOTIFICATION_CHANNEL_SLACK,
		notificationv1.NotificationChannel_NOTIFICATION_CHANNEL_EMAIL:
		return true
	default:
		return false
	}
}

func validSeverity(s notificationv1.NotificationSeverity) bool {
	switch s {
	case notificationv1.NotificationSeverity_NOTIFICATION_SEVERITY_UNSPECIFIED,
		notificationv1.NotificationSeverity_NOTIFICATION_SEVERITY_INFO,
		notificationv1.NotificationSeverity_NOTIFICATION_SEVERITY_WARNING,
		notificationv1.NotificationSeverity_NOTIFICATION_SEVERITY_ERROR,
		notificationv1.NotificationSeverity_NOTIFICATION_SEVERITY_CRITICAL:
		return true
	default:
		return false
	}
}

func validStatus(s notificationv1.DeliveryStatus) bool {
	switch s {
	case notificationv1.DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED,
		notificationv1.DeliveryStatus_DELIVERY_STATUS_PENDING,
		notificationv1.DeliveryStatus_DELIVERY_STATUS_RETRYING,
		notificationv1.DeliveryStatus_DELIVERY_STATUS_DELIVERED,
		notificationv1.DeliveryStatus_DELIVERY_STATUS_FAILED,
		notificationv1.DeliveryStatus_DELIVERY_STATUS_SUPPRESSED:
		return true
	default:
		return false
	}
}

// ============================================================================
// PAGINATION HELPERS (nil-safe accessors)
// ============================================================================
//
// req.GetPagination() may be nil (the field is optional). These accessors return
// the zero value in that case, which the domain then clamps to its defaults — so
// a client that omits pagination entirely gets the default page size, not a
// nil-deref. (The generated GetPageSize/GetPageToken are themselves nil-safe, but
// going through the *PaginationRequest first keeps the call sites readable.)

func pageSizeOf(p *commonv1.PaginationRequest) int {
	return int(p.GetPageSize())
}

func pageTokenOf(p *commonv1.PaginationRequest) string {
	return p.GetPageToken()
}

// ============================================================================
// PROTO <-> DOMAIN CONVERTERS (the anti-corruption layer)
// ============================================================================
//
// These are the single place a domain type becomes a wire type (and back). Two
// invariants live here so no handler can violate them by hand:
//
//   - ENUM CASTS ARE LOSSLESS. The domain enums mirror the proto enums with
//     byte-identical integer values (asserted in the domain's enum-table test), so
//     each conversion is a trivial int32 cast at the boundary — NOT a switch. This
//     is the payoff of the "mirror the values" decision in domain/models.go: the
//     boundary stays a one-liner per field while the domain keeps zero proto
//     imports.
//
//   - NO SECRET/TARGET EVER REACHES A Notification OR DeliveryAttempt RESPONSE.
//     The DeliveryAttempt proto has no target field by design; ChannelPreference's
//     target IS returned (only to the OWNING caller, on their own prefs) because
//     the settings UI must show/edit it — but it never appears on an inbox or
//     delivery-log row.

func notificationToProto(n domain.Notification) *notificationv1.Notification {
	out := &notificationv1.Notification{
		Id:              n.ID,
		RecipientUserId: n.RecipientUserID,
		Title:           n.Title,
		Body:            n.Body,
		Severity:        notificationv1.NotificationSeverity(n.Severity),
		Channels:        channelsToProto(n.Channels),
		Read:            n.Read,
		EventId:         n.EventID,
		EventType:       n.EventType,
		SourceService:   n.SourceService,
		CreatedAt:       timestamppb.New(n.CreatedAt),
	}
	// ReadAt is unset while unread (zero time). Emitting a zero-epoch timestamp
	// would be misleading ("read at 1970"); leave it nil so the client sees "unset".
	if !n.ReadAt.IsZero() {
		out.ReadAt = timestamppb.New(n.ReadAt)
	}
	return out
}

func notificationsToProto(ns []domain.Notification) []*notificationv1.Notification {
	out := make([]*notificationv1.Notification, len(ns))
	for i, n := range ns {
		out[i] = notificationToProto(n)
	}
	return out
}

// deliveryAttemptToProto converts ONE attempt. It deliberately drops
// NotificationID — the proto DeliveryAttempt has no such field (it is surfaced via
// the parallel notification_ids slice on the list response, or implied by
// GetNotification). It NEVER carries a target/secret (the proto has no field for
// one; this is the structural enforcement of the no-leak rule).
func deliveryAttemptToProto(a domain.DeliveryAttempt) *notificationv1.DeliveryAttempt {
	out := &notificationv1.DeliveryAttempt{
		Channel:      notificationv1.NotificationChannel(a.Channel),
		Status:       notificationv1.DeliveryStatus(a.Status),
		Attempt:      a.Attempt,
		ResponseCode: a.ResponseCode,
		ErrorMessage: a.ErrorMessage,
	}
	if !a.AttemptedAt.IsZero() {
		out.AttemptedAt = timestamppb.New(a.AttemptedAt)
	}
	return out
}

func deliveryAttemptsToProto(as []domain.DeliveryAttempt) []*notificationv1.DeliveryAttempt {
	out := make([]*notificationv1.DeliveryAttempt, len(as))
	for i, a := range as {
		out[i] = deliveryAttemptToProto(a)
	}
	return out
}

func channelsToProto(chs []domain.NotificationChannel) []notificationv1.NotificationChannel {
	out := make([]notificationv1.NotificationChannel, len(chs))
	for i, c := range chs {
		out[i] = notificationv1.NotificationChannel(c)
	}
	return out
}

func channelPreferenceToProto(cp domain.ChannelPreference) *notificationv1.ChannelPreference {
	return &notificationv1.ChannelPreference{
		Channel:     notificationv1.NotificationChannel(cp.Channel),
		Enabled:     cp.Enabled,
		MinSeverity: notificationv1.NotificationSeverity(cp.MinSeverity),
		Target:      cp.Target, // returned only on the OWNER's own prefs (settings UI)
	}
}

func preferencesToProto(p domain.NotificationPreferences) *notificationv1.NotificationPreferences {
	channels := make([]*notificationv1.ChannelPreference, len(p.Channels))
	for i, cp := range p.Channels {
		channels[i] = channelPreferenceToProto(cp)
	}
	out := &notificationv1.NotificationPreferences{
		UserId:             p.UserID,
		Channels:           channels,
		MutedEventPatterns: p.MutedEventPatterns,
	}
	if !p.UpdatedAt.IsZero() {
		out.UpdatedAt = timestamppb.New(p.UpdatedAt)
	}
	return out
}

// channelPreferencesFromProto converts the request's desired channel set into
// domain types. We do NOT trust or copy any wire-supplied owner here (there is no
// per-channel owner field); the OWNER is pinned by the caller in UpdatePreferences.
func channelPreferencesFromProto(cps []*notificationv1.ChannelPreference) []domain.ChannelPreference {
	out := make([]domain.ChannelPreference, len(cps))
	for i, cp := range cps {
		out[i] = domain.ChannelPreference{
			Channel:     domain.NotificationChannel(cp.GetChannel()),
			Enabled:     cp.GetEnabled(),
			MinSeverity: domain.Severity(cp.GetMinSeverity()),
			Target:      cp.GetTarget(),
		}
	}
	return out
}

// ============================================================================
// ERROR MAPPING — domain sentinels -> gRPC status codes
// ============================================================================
//
// toStatusError is the single anti-corruption point between domain errors and the
// gRPC status vocabulary. It is intentionally a small, explicit table:
//
//	domain sentinel            -> gRPC code           why
//	-------------------------------------------------------------------------
//	ErrValidation              -> InvalidArgument     client sent bad input
//	ErrSSRFTargetRejected      -> InvalidArgument     malicious/unsafe target URL
//	ErrNotFound                -> NotFound            absent OR foreign (anti-IDOR)
//	ErrChannelNotConfigured    -> FailedPrecondition  state not ready (fix config)
//	(anything else)            -> Internal (generic)  SANITIZED — no leak
//
// WHY ErrSSRFTargetRejected -> InvalidArgument (NOT a 5xx-class code): a rejected
// webhook URL is the CLIENT's bad input, even though it is security-relevant. We
// forward the sentinel's message because it describes the offending target shape
// in a NON-sensitive way ("target scheme must be https", "target IP is loopback")
// — it names a rule the client violated, never internal state or a resolved
// secret. The named sentinel also lets metrics/alerting single SSRF rejections out
// from ordinary validation failures (a spike may be an attack).
//
// WHY ErrChannelNotConfigured -> FailedPrecondition (NOT InvalidArgument): the
// request was well-formed; the problem is the caller's STATE (no enabled
// preference for that channel). FailedPrecondition tells the client "fix your
// config first", which InvalidArgument ("your request was malformed") would
// mislead. This is the textbook FailedPrecondition-vs-InvalidArgument distinction.
//
// WHY the default is Internal with a FIXED message: any error we don't recognize
// might wrap SQL text, a connection string, a webhook URL, or PII. The safe
// default is to log the real error server-side (the logging interceptor does this)
// and return a constant "internal error" to the client. errors.Is (not ==) is used
// so a wrapped sentinel (fmt.Errorf("...: %w", ErrNotFound)) still maps correctly.
//
// INTERVIEW: "How do you stop internal errors leaking through gRPC?" Centralize
// the mapping; whitelist the codes you intend to expose; sanitize everything else
// to Internal with a fixed string; never str-format the raw error into the status
// message on the default path. For the two whitelisted message-forwarding cases
// (ErrValidation, ErrSSRFTargetRejected) we forward only the CONTEXT the domain
// prepended (via clientMessage), NOT the raw err.Error() — see clientMessage for
// why the package-prefixed sentinel text itself must not reach the client.
func toStatusError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, domain.ErrValidation):
		// Wrapped ErrValidation carries a specific, NON-sensitive context message
		// (e.g. "too many ids in MarkRead batch"). It describes the client's own bad
		// input, never internal state, so it is safe to forward — but we strip the
		// "notification: validation failed" sentinel tail first (clientMessage).
		return status.Error(codes.InvalidArgument, clientMessage(err, domain.ErrValidation, "invalid request"))

	case errors.Is(err, domain.ErrSSRFTargetRejected):
		// Also a client-input rejection, with a secret-free message describing the
		// rule violated (e.g. "target scheme must be https"). Forward the context so
		// the settings UI can show which target was bad — minus the sentinel tail.
		return status.Error(codes.InvalidArgument, clientMessage(err, domain.ErrSSRFTargetRejected, "webhook/slack target rejected"))

	case errors.Is(err, domain.ErrNotFound):
		// Existence-hiding: absent AND foreign-owned both arrive here. One generic
		// message — never reveal which it was.
		return status.Error(codes.NotFound, "notification not found")

	case errors.Is(err, domain.ErrChannelNotConfigured):
		return status.Error(codes.FailedPrecondition, "channel is not configured for this user")

	default:
		// SANITIZE: do not expose err.Error() — it may wrap SQL, secrets, or PII.
		// The real error is logged by the logging interceptor server-side; the
		// client gets a constant, opaque message and codes.Internal.
		return status.Error(codes.Internal, "internal error")
	}
}

// clientMessage extracts the CLIENT-FACING context the domain prepended to a
// sentinel, WITHOUT leaking the sentinel's own package-prefixed text.
//
// The domain wraps as fmt.Errorf("%s: %w", ctx, sentinel), so err.Error() reads
// "<ctx>: notification: <sentinel text>". Forwarding that whole string would leak
// the internal package prefix ("notification: ...") and the sentinel's wording to
// the client — vocabulary the client neither needs nor should see (it hints at
// internal structure and trips secret/leak scanners). So we strip the sentinel's
// own Error() suffix and return just <ctx>, the message the handler/domain author
// wrote FOR the client (e.g. "too many ids in MarkRead batch", "target scheme must
// be https"). If the error is the bare sentinel with no added context (nothing to
// strip), we fall back to a fixed generic message rather than echo the sentinel.
//
// WHY a helper and not strings surgery at each call site: doing it once keeps the
// "never echo the raw sentinel" rule in a single auditable spot, and makes the two
// whitelisted forward-message cases provably safe.
func clientMessage(err, sentinel error, fallback string) string {
	full := err.Error()
	suffix := sentinel.Error() // e.g. "notification: webhook/slack target rejected by SSRF guard"

	// Wrapped form: "<ctx>: <sentinel text>". Trim the sentinel tail (and the ": "
	// separator) to leave just the client-facing context.
	if trimmed, ok := strings.CutSuffix(full, suffix); ok {
		ctx := strings.TrimSuffix(trimmed, ": ")
		if ctx != "" {
			return ctx
		}
	}
	// Bare sentinel (no added context) — never echo the internal sentinel text.
	return fallback
}
