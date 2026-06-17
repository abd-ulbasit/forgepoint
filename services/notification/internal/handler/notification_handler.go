// Package handler implements the gRPC server side of the Notification service.
// It is the outermost layer in Clean Architecture: it speaks proto (wire format)
// and delegates all business logic to the domain.NotificationService interface.
//
// ============================================================================
// HANDLER LAYER RESPONSIBILITIES (filled in the handler phase)
// ============================================================================
//
// The handler layer has exactly three jobs, mirrored from the Auth service:
//
//  1. PROTO → DOMAIN: extract + validate fields from the incoming proto Request,
//     resolve the caller's identity from auth claims (NEVER from a request body),
//     and build the domain input type. For this service that authority rule is
//     load-bearing: ListNotifications/GetPreferences have NO user_id field on the
//     wire, so the handler MUST set RecipientUserID/UserID from
//     grpcutil.ClaimsFromContext(ctx) — accepting one from the request would be an
//     IDOR / mass-assignment hole.
//
//  2. CALL DOMAIN SERVICE: invoke the NotificationService method. The handler
//     holds the interface — it never knows whether it's Postgres- or mock-backed.
//
//  3. DOMAIN → PROTO: map the domain result back to a proto Response, converting
//     the mirrored domain enums to the generated proto enums at the boundary (the
//     values are byte-identical, so it's a trivial cast) and NEVER echoing a
//     channel target/secret onto a Notification/DeliveryAttempt response.
//
// WHAT THE HANDLER DOES NOT DO: business logic (domain), SQL (repository), the
// SSRF network checks or HTTP delivery (the Notifier adapter), NATS consumption
// (the events adapter). The choreography reactor itself is NOT on this gRPC
// surface at all — it runs on the NATS path (see the proto's top comment on why a
// producer-facing "send" RPC would violate choreography).
//
// ============================================================================
// EMBEDDING UnimplementedNotificationServiceServer — WHY THIS IS CORRECT
// ============================================================================
//
// protoc-gen-go-grpc generates NotificationServiceServer with a private method
// mustEmbedUnimplementedNotificationServiceServer(), forcing every implementation
// to embed UnimplementedNotificationServiceServer. That base implements every RPC
// to return codes.Unimplemented. By embedding it, NotificationHandler:
//
//  1. Satisfies notificationv1.NotificationServiceServer at compile time — the
//     server can be registered and started RIGHT NOW (this scaffold phase).
//  2. Returns codes.Unimplemented for any RPC we haven't overridden yet — the
//     client gets a real, correct gRPC error, not a panic or a nil-deref.
//  3. Is forward-compatible: adding a new RPC to the proto doesn't break this
//     server; the embedded base answers it until we implement it.
//
// This is the IDIOMATIC Go/gRPC scaffold, not a placeholder. It is a running,
// production-correct server. Per-RPC method bodies (ListNotifications, MarkRead,
// UpdatePreferences, TestChannel, ...) are added in the handler phase by defining
// methods on *NotificationHandler that convert proto⇄domain and call svc.
//
// INTERVIEW: "How does grpc-go guarantee forward compatibility of service
// servers?" → the mustEmbed... private method forces embedding the Unimplemented
// base; a service that embeds it compiles and returns Unimplemented for any RPC
// added later, instead of failing to satisfy the interface.
// ============================================================================
package handler

import (
	notificationv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/notification/v1"
	"github.com/abd-ulbasit/forgepoint/services/notification/internal/domain"
)

// NotificationHandler is the gRPC server implementation for the Notification
// service.
//
// It embeds notificationv1.UnimplementedNotificationServiceServer to satisfy the
// full notificationv1.NotificationServiceServer interface immediately, with all
// not-yet-overridden RPCs returning codes.Unimplemented until the handler phase
// fills them in.
//
// svc is the domain.NotificationService holding all business logic. In the
// handler phase each RPC body calls svc.ListNotifications, svc.UpdatePreferences,
// etc. and converts proto⇄domain. The svc field is nil during this scaffold phase
// (main.go passes nil); the embedded Unimplemented methods never dereference svc,
// so this is safe. (The control-plane RPCs are all unary; this service exposes no
// streaming RPC by design — see the proto's "WHY NO STREAMING RPC HERE" note.)
type NotificationHandler struct {
	notificationv1.UnimplementedNotificationServiceServer // embedded by value (see grpc-go note above)
	svc                                                   domain.NotificationService
}

// NewNotificationHandler creates a NotificationHandler with the given domain
// service.
//
// The svc parameter is nil during the scaffold phase because the domain service
// implementation and its Postgres/HTTP/NATS adapters are wired in later phases.
// The nil svc is safe here because the embedded UnimplementedNotificationServiceServer
// handles all RPCs without touching svc. Once the adapters land, main.go will
// construct the real service and pass it here, and each overridden RPC method will
// guard a nil svc with:
//
//	if h.svc == nil { return nil, status.Error(codes.Internal, "service not wired") }
//
// (All RPCs are unary, so the guard returns (nil, err); there is no streaming RPC
// on this surface.)
func NewNotificationHandler(svc domain.NotificationService) *NotificationHandler {
	return &NotificationHandler{svc: svc}
}
