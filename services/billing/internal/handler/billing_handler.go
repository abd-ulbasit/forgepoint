// Package handler implements the gRPC server side of the Billing service. It is
// the outermost layer in Clean Architecture: it speaks proto (wire format) and
// delegates ALL business logic to the domain.BillingService interface.
//
// ============================================================================
// HANDLER LAYER RESPONSIBILITIES (filled in the handler phase, not yet)
// ============================================================================
//
// When the per-RPC methods land, the handler will have exactly three jobs:
//
//  1. PROTO → DOMAIN: extract + validate fields from the incoming proto Request,
//     build the domain input type. CRITICAL for billing: the handler fills the
//     SERVER-AUTHORITATIVE `team` from the caller's auth claims (NOT from any
//     request field) before calling RecordUsage — this is the structural guard
//     against a client naming the billed account (mass-assignment for money). It
//     also maps billingv1.MeterType → domain.MeterType at this boundary.
//  2. CALL DOMAIN SERVICE: invoke the BillingService method. The handler holds an
//     interface — it never knows whether Postgres/Redis/NATS or a test stub backs it.
//  3. DOMAIN → PROTO + ERROR MAPPING: map the domain result to the proto Response
//     and the domain sentinel errors to gRPC status codes (ErrValidation/
//     ErrNegativeQuantity/ErrQuantityTooLarge/ErrUnknownMeter → InvalidArgument;
//     ErrRatePlanNotFound → FailedPrecondition or NotFound; ErrInvoiceNotFound →
//     NotFound; ErrAmountOverflow → Internal). It also maps domain.Money →
//     billingv1.Money (micros + currency).
//
// ============================================================================
// EMBEDDING UnimplementedBillingServiceServer — WHY THIS IS CORRECT (NOT A STUB)
// ============================================================================
//
// protoc-gen-go-grpc generates BillingServiceServer with a private
// mustEmbedUnimplementedBillingServiceServer() method, forcing every
// implementation to embed UnimplementedBillingServiceServer. That base
// implements every RPC to return codes.Unimplemented. By embedding it,
// BillingHandler:
//
//  1. Satisfies billingv1.BillingServiceServer at compile time RIGHT NOW — the
//     server registers and runs in this scaffold phase.
//  2. Returns codes.Unimplemented for any RPC not yet overridden — a real,
//     correct gRPC error, never a nil-pointer panic.
//  3. Is forward-compatible: adding a new RPC to the proto doesn't break this
//     server; the embedded base answers it until we implement the override.
//
// This is the IDIOMATIC gRPC scaffold, not a placeholder. Per-RPC
// implementations (RecordUsage, CreateRatePlan, CheckQuota, GetUsage,
// GetInvoice, ListInvoices, GetRatePlan) are added in the handler phase by
// overriding each method on *BillingHandler.
//
// FORWARD COMPATIBILITY OF gRPC SERVICE SERVERS: the mustEmbed... private
// method forces embedding the Unimplemented base; a server that embeds it compiles (and returns Unimplemented) when a new
// RPC is added to the proto, instead of failing to satisfy the interface.
// ============================================================================
package handler

import (
	billingv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/billing/v1"
	"github.com/abd-ulbasit/forgepoint/services/billing/internal/domain"
)

// BillingHandler is the gRPC server implementation for the Billing service.
//
// It embeds billingv1.UnimplementedBillingServiceServer to satisfy the full
// billingv1.BillingServiceServer interface immediately, with all unimplemented
// RPCs returning codes.Unimplemented until the handler phase fills them in.
//
// svc is the domain.BillingService holding all business logic. It is nil during
// the scaffold phase (main.go passes nil); the embedded Unimplemented methods
// never dereference svc, so this is safe. The handler-phase override methods
// will guard against a nil svc:
//   - UNARY RPCs (all of Billing's RPCs are unary):
//     if h.svc == nil { return nil, status.Error(codes.Internal, "service not wired") }
type BillingHandler struct {
	billingv1.UnimplementedBillingServiceServer // embedded by value (see grpc-go note above)
	svc                                         domain.BillingService
}

// NewBillingHandler creates a BillingHandler with the given domain service.
//
// svc is nil during the scaffold phase because the domain service and its
// Postgres-backed stores are wired in later phases. The nil svc is safe here:
// the embedded UnimplementedBillingServiceServer answers every RPC without
// touching svc. Once the stores land, main.go constructs the real service:
//
//	svc := domain.NewBillingService(usageStore, planStore, invoiceStore, ids, clock)
//	billingv1.RegisterBillingServiceServer(srv.GRPC, handler.NewBillingHandler(svc))
func NewBillingHandler(svc domain.BillingService) *BillingHandler {
	return &BillingHandler{svc: svc}
}
