// billing_handler_rpcs.go holds the per-RPC method bodies for BillingHandler.
//
// The scaffold (billing_handler.go) embeds UnimplementedBillingServiceServer and
// holds the domain.BillingService field. This file OVERRIDES each RPC with a real
// implementation. Once a method is defined here on *BillingHandler, Go's method-set
// resolution prefers it over the embedded Unimplemented base — so every RPC below
// is now live, and only RPCs we have NOT written fall through to Unimplemented
// (there are none left: all seven BillingService RPCs are implemented here).
//
// ============================================================================
// THE FOUR-STEP HANDLER CONTRACT (every method follows it)
// ============================================================================
//
//  1. NIL-SVC GUARD. A binary may be wired with svc==nil during the scaffold /
//     pre-repo phase (main.go passes nil until the Postgres adapter lands).
//     Calling a nil interface's method panics, so every method first checks
//     h.svc==nil and returns codes.Unimplemented — the SAME code the embedded
//     base would return, so a half-wired binary behaves identically to the
//     scaffold instead of crashing. All seven RPCs here are UNARY, so the guard
//     returns (nil, errServiceNotWired). (A server-streaming RPC would instead
//     return the status error directly with no response value — BillingService
//     has none; see the proto's "WHY ALL UNARY" note: bounded paginated reads,
//     not unbounded live feeds, so the gRPC surface stays request/reply.)
//
//  2. VALIDATE THE REQUEST. Reject malformed input with codes.InvalidArgument
//     and a CLEAN message (no internal detail, no echo of secrets/PII). This is
//     the handler's job, not the domain's: we fail fast at the transport boundary
//     so the domain only ever sees well-formed input. The domain ALSO validates
//     (defense in depth) but the handler gives the client a precise gRPC code.
//
//  3. CONVERT proto -> domain, pulling SERVER-AUTHORITATIVE identity from the
//     auth interceptor's claims (grpcutil.ClaimsFromContext) — NEVER from
//     client-supplied fields. For a MONEY service this is the load-bearing guard:
//     `team` (whose account is billed) comes from the validated token, not from a
//     request field. Letting a caller name the billed team is mass-assignment with
//     a direct financial blast radius (account-takeover-for-money). The handler is
//     also where billingv1.MeterType -> domain.MeterType conversion happens.
//
//  4. CALL the domain service, then MAP its sentinel errors -> precise gRPC status
//     codes via toStatusError, and CONVERT the domain result -> proto. The
//     proto<->domain converters at the bottom are the single anti-corruption point
//     (Money <-> billingv1.Money, RatePlan/UsageRecord/Invoice <-> proto).
//
// ============================================================================
// THE TENANCY RULE (GetUsage / CheckQuota / ListInvoices) — shared helper
// ============================================================================
//
// The reporting/quota RPCs carry a `team` field so an ADMIN (and the trusted BFF)
// can scope a query to an arbitrary team. A NON-ADMIN must be confined to its OWN
// team (tenant isolation) regardless of what it puts in the field — otherwise a
// caller could read another tenant's spend/quota/invoices by typing their name.
// resolveTeam centralizes that rule: admins may target any team; everyone else is
// FORCED to their claims team, and the request field is ignored. This mirrors the
// proto's per-field "server overrides for non-admins" notes and keeps the policy
// in one auditable place rather than re-derived per RPC.
//
// RBAC vs data-scoping (two distinct guards, both in this handler today):
//   - resolveTeam is the narrower DATA-SCOPING guard for reporting/quota reads
//     (which team's data may a caller see).
//   - The admin ROLE GATE on CreateRatePlan (a real-money write) is enforced
//     directly in that method. A platform-wide per-RPC RBAC interceptor does NOT
//     yet exist in grpcutil (only AuthUnaryInterceptor, which authenticates and
//     injects claims — it does not authorize per RPC). Until one lands and is
//     wired here, the handler is the authoritative admin gate; even after, the
//     in-handler check stays as defense-in-depth on a money-pricing write.
package handler

import (
	"context"
	"errors"

	billingv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/billing/v1"
	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/services/billing/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ============================================================================
// CONSTANTS
// ============================================================================

const (
	// defaultPageSize is applied when the client omits page_size (sends 0).
	// 20 mirrors the proto's documented default for PaginationRequest.
	defaultPageSize = 20

	// maxPageSize caps page_size so a client cannot request a multi-thousand-row
	// page that pins memory and starves other callers. 100 mirrors the proto's
	// documented max. We CLAMP an over-large request rather than reject it (a
	// client asking for "as much as possible" should get the maximum we serve, not
	// an error) — the same AIP-158 choice the auth/registry list RPCs made.
	maxPageSize = 100

	// adminRole is the privileged claims role. It does two things in this handler:
	//   - scopes reporting/quota queries to an ARBITRARY team (resolveTeam); any
	//     other role is confined to its own claims team;
	//   - gates the admin-only CreateRatePlan write (a real-money pricing write) and
	//     authorizes cross-team GetInvoice reads.
	// There is no per-RPC RBAC interceptor in the platform yet, so this constant is
	// the authoritative admin signal at the handler boundary, not just a read knob.
	adminRole = "admin"
)

// errServiceNotWired is the canonical response when h.svc is nil (a binary running
// before the domain service is wired). It mirrors what the embedded
// UnimplementedBillingServiceServer would return, so a half-wired binary behaves
// exactly like the scaffold instead of panicking on a nil-interface call.
var errServiceNotWired = status.Error(codes.Unimplemented, "billing service not wired")

// ============================================================================
// RecordUsage — the outbox write path; the most security-sensitive RPC here.
// ============================================================================
//
// SECURITY (the headline guard): `team` is taken from the caller's auth claims,
// NEVER from the request — RecordUsageRequest has no team field by design (see the
// proto + domain.RecordUsageInput notes), and we do not synthesize one from any
// client value. A caller can only meter usage against ITS OWN account.
//
// VALIDATION the handler owns at the boundary (the domain re-checks, defense in
// depth, but the client deserves a precise code here):
//   - meter_type must be a known, non-UNSPECIFIED meter (a typo'd/zero meter must
//     not fall through to a guessed $0 price — a revenue leak). -> InvalidArgument.
//   - quantity must be NON-NEGATIVE (only server-issued credits are negative; a
//     negative from a client is an attempt to manufacture a credit) and within
//     MaxQuantityPerRecord (the int64 overflow bound on cost = quantity*price).
//     -> InvalidArgument; we REJECT, never truncate or wrap (the "never wrap" rule
//     of a money service).
//
// occurred_at is forwarded raw to the domain, which CLAMPS it (no future, no
// backdating into a closed period); the handler only rejects a structurally
// malformed timestamp so a bad value never becomes a confusing time.Time.
func (h *BillingHandler) RecordUsage(ctx context.Context, req *billingv1.RecordUsageRequest) (*billingv1.RecordUsageResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	// Pull the caller's identity. This RPC is authenticated; absent claims mean the
	// call bypassed the auth interceptor — fail closed with Unauthenticated.
	claims, ok := grpcutil.ClaimsFromContext(ctx)
	if !ok || claims == nil || claims.Team == "" {
		return nil, status.Error(codes.Unauthenticated, "missing authentication")
	}

	// VALIDATE meter. meterTypeFromProto returns ok=false for UNSPECIFIED or an
	// unrecognized enum value, which we reject before touching the domain.
	meter, ok := meterTypeFromProto(req.GetMeterType())
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "meter_type must be a known, non-unspecified meter")
	}

	// VALIDATE quantity. Non-negative AND within the overflow bound. We reject
	// (never truncate) so the cost multiply downstream is provably safe.
	if req.GetQuantity() < 0 {
		return nil, status.Error(codes.InvalidArgument, "quantity must be non-negative")
	}
	if req.GetQuantity() > domain.MaxQuantityPerRecord {
		return nil, status.Error(codes.InvalidArgument, "quantity exceeds per-record maximum")
	}

	// Reject a structurally invalid occurred_at at the boundary; nil means "use
	// server now()" (the domain stamps it). A present-but-malformed timestamp is a
	// client bug we surface precisely rather than letting it decay in the domain.
	var occurredAt = req.GetOccurredAt()
	if occurredAt != nil {
		if err := occurredAt.CheckValid(); err != nil {
			return nil, status.Error(codes.InvalidArgument, "occurred_at is not a valid timestamp")
		}
	}

	// CONVERT proto -> domain input. NOTE the absence of team/rate_plan/cost: those
	// are server-authoritative. team is passed SEPARATELY (from claims), and the
	// price/cost are computed inside the domain from the resolved plan.
	in := domain.RecordUsageInput{
		MeterType:       meter,
		Quantity:        req.GetQuantity(),
		ModelID:         req.GetModelId(),
		ModelVersion:    req.GetModelVersion(),
		SourceRequestID: req.GetSourceRequestId(),
		IdempotencyKey:  req.GetIdempotencyKey(),
	}
	if occurredAt != nil {
		in.OccurredAt = occurredAt.AsTime()
	}

	record, deduplicated, err := h.svc.RecordUsage(ctx, claims.Team, in)
	if err != nil {
		return nil, toStatusError(err)
	}

	return &billingv1.RecordUsageResponse{
		Record:       usageRecordToProto(record),
		Deduplicated: deduplicated,
	}, nil
}

// ============================================================================
// CreateRatePlan — ADMIN write; sets real prices.
// ============================================================================
//
// AUTHORIZATION (the headline guard): the proto marks this ADMIN-ONLY — it sets the
// unit prices, free allowances, and quota caps the metering math applies to REAL
// money. If ANY authenticated caller could create a rate plan, they could set
// platform pricing. The original design delegated this gate to an auth-interceptor
// "CheckPermission" hook — but NO such per-RPC authorization interceptor exists in
// the platform yet (grpcutil ships only AuthUnaryInterceptor: authentication, i.e.
// validate-token-and-inject-claims, with no role/scope gate). Shipping a money-
// pricing write whose only stated guard is a component that does not exist is the
// vulnerability. So the handler enforces the admin gate itself.
//
// DEFENSE-IN-DEPTH, not redundancy: even once a uniform per-RPC RBAC interceptor
// lands, this handler-level check is the kind of guard you WANT belt-and-suspenders
// on a real-money write — a misconfigured skip-list or a forgotten interceptor must
// not silently open pricing. The role already rides in claims (resolveTeam reads
// it), so the check is cheap and local.
//
// WHY PermissionDenied (not Unauthenticated): the caller IS authenticated (valid
// token, claims present) — they simply lack the admin role. Unauthenticated means
// "who are you?"; PermissionDenied means "I know who you are, and you may not do
// this" — the correct gRPC code, and it does not invite a re-auth retry loop.
//
// MASS-ASSIGNMENT GUARD: id and created_at are NOT read from the client (they are
// absent from CreateRatePlanInput by design) — letting a client choose a plan id
// is how you'd hijack an existing plan. The client supplies only the pricing
// DEFINITION (name + the three meter-keyed maps).
//
// VALIDATION: name required; every map key must be a valid MeterType name (a typo
// key would silently create an unpriced meter); the single-currency + non-negative
// price invariants are enforced by the DOMAIN (it owns the money rules) and
// surface here as wrapped ErrValidation -> InvalidArgument.
func (h *BillingHandler) CreateRatePlan(ctx context.Context, req *billingv1.CreateRatePlanRequest) (*billingv1.CreateRatePlanResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	// ADMIN GATE. Resolve the caller's identity and require the admin role before
	// touching the domain. A missing-claims call (auth interceptor bypassed) fails
	// closed with Unauthenticated; an authenticated non-admin gets PermissionDenied.
	// We gate BEFORE any validation/conversion so a non-admin learns nothing about
	// the request's wellformedness (no validation oracle on a privileged write).
	claims, ok := grpcutil.ClaimsFromContext(ctx)
	if !ok || claims == nil || claims.Team == "" {
		return nil, status.Error(codes.Unauthenticated, "missing authentication")
	}
	if claims.Role != adminRole {
		return nil, status.Error(codes.PermissionDenied, "creating a rate plan requires the admin role")
	}

	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}

	// CONVERT the meter-keyed maps proto -> domain, validating each key is a known
	// meter name. We reject an unknown key at the boundary: a plan with an unpriced/
	// mistyped meter is a pricing bug we must not silently persist.
	unitPrices := make(map[domain.MeterType]domain.Money, len(req.GetUnitPrices()))
	for key, price := range req.GetUnitPrices() {
		meter, ok := meterTypeFromName(key)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "unit_prices contains an unknown meter type")
		}
		unitPrices[meter] = moneyFromProto(price)
	}
	includedQuantities := make(map[domain.MeterType]int64, len(req.GetIncludedQuantities()))
	for key, qty := range req.GetIncludedQuantities() {
		meter, ok := meterTypeFromName(key)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "included_quantities contains an unknown meter type")
		}
		includedQuantities[meter] = qty
	}
	quotaLimits := make(map[domain.MeterType]int64, len(req.GetQuotaLimits()))
	for key, limit := range req.GetQuotaLimits() {
		meter, ok := meterTypeFromName(key)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "quota_limits contains an unknown meter type")
		}
		quotaLimits[meter] = limit
	}

	plan, deduplicated, err := h.svc.CreateRatePlan(ctx, domain.CreateRatePlanInput{
		Name:               req.GetName(),
		UnitPrices:         unitPrices,
		IncludedQuantities: includedQuantities,
		QuotaLimits:        quotaLimits,
		IdempotencyKey:     req.GetIdempotencyKey(),
	})
	if err != nil {
		return nil, toStatusError(err)
	}

	return &billingv1.CreateRatePlanResponse{
		RatePlan:     ratePlanToProto(plan),
		Deduplicated: deduplicated,
	}, nil
}

// ============================================================================
// GetRatePlan — read pricing by id.
// ============================================================================
func (h *BillingHandler) GetRatePlan(ctx context.Context, req *billingv1.GetRatePlanRequest) (*billingv1.GetRatePlanResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	if req.GetRatePlanId() == "" {
		return nil, status.Error(codes.InvalidArgument, "rate_plan_id is required")
	}

	plan, err := h.svc.GetRatePlan(ctx, req.GetRatePlanId())
	if err != nil {
		return nil, toStatusError(err)
	}

	return &billingv1.GetRatePlanResponse{
		RatePlan: ratePlanToProto(plan),
	}, nil
}

// ============================================================================
// CheckQuota — the gateway's pre-flight.
// ============================================================================
//
// TENANCY: resolveTeam confines a non-admin to its own claims team, ignoring a
// request-supplied team (so a caller can't probe another tenant's quota). The
// gateway/admin may scope to an arbitrary team. meter_type must be a known meter.
func (h *BillingHandler) CheckQuota(ctx context.Context, req *billingv1.CheckQuotaRequest) (*billingv1.CheckQuotaResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	team, err := resolveTeam(ctx, req.GetTeam())
	if err != nil {
		return nil, err
	}

	meter, ok := meterTypeFromProto(req.GetMeterType())
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "meter_type must be a known, non-unspecified meter")
	}

	st, err := h.svc.CheckQuota(ctx, team, meter)
	if err != nil {
		return nil, toStatusError(err)
	}

	return &billingv1.CheckQuotaResponse{
		Team:         st.Team,
		RatePlanId:   st.RatePlanID,
		MeterType:    meterTypeToProto(st.MeterType),
		QuotaLimit:   st.QuotaLimit,
		CurrentUsage: st.CurrentUsage,
		Remaining:    st.Remaining,
		Exceeded:     st.Exceeded,
	}, nil
}

// ============================================================================
// GetUsage — aggregated, paginated reporting.
// ============================================================================
//
// TENANCY via resolveTeam (non-admins scoped to own team). PAGINATION normalized:
// page_size defaults when 0, CLAMPED at maxPageSize; the opaque cursor is forwarded
// as-is. The window (period_start/end) is forwarded raw; the domain validates it
// (ErrInvalidPeriod on an inverted or too-wide span) — we only reject structurally
// malformed timestamps here.
func (h *BillingHandler) GetUsage(ctx context.Context, req *billingv1.GetUsageRequest) (*billingv1.GetUsageResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	team, err := resolveTeam(ctx, req.GetTeam())
	if err != nil {
		return nil, err
	}

	in := domain.GetUsageInput{
		Team:      team,
		PageSize:  normalizePageSize(req.GetPagination()),
		PageToken: req.GetPagination().GetPageToken(),
	}

	// meter_type_filter is OPTIONAL: UNSPECIFIED means "all meters" (the domain
	// treats the zero-value "" MeterType as no filter). A NON-unspecified value
	// must still be a known meter; an unrecognized non-zero enum is a client bug.
	if pm := req.GetMeterTypeFilter(); pm != billingv1.MeterType_METER_TYPE_UNSPECIFIED {
		meter, ok := meterTypeFromProto(pm)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "meter_type_filter is not a known meter")
		}
		in.MeterFilter = meter
	}

	if ts := req.GetPeriodStart(); ts != nil {
		if err := ts.CheckValid(); err != nil {
			return nil, status.Error(codes.InvalidArgument, "period_start is not a valid timestamp")
		}
		in.PeriodStart = ts.AsTime()
	}
	if ts := req.GetPeriodEnd(); ts != nil {
		if err := ts.CheckValid(); err != nil {
			return nil, status.Error(codes.InvalidArgument, "period_end is not a valid timestamp")
		}
		in.PeriodEnd = ts.AsTime()
	}

	summaries, grandTotal, nextToken, err := h.svc.GetUsage(ctx, in)
	if err != nil {
		return nil, toStatusError(err)
	}

	out := &billingv1.GetUsageResponse{
		Summaries:  make([]*billingv1.UsageSummary, 0, len(summaries)),
		GrandTotal: moneyToProto(grandTotal),
		Pagination: &commonv1.PaginationResponse{NextPageToken: nextToken},
	}
	for _, s := range summaries {
		out.Summaries = append(out.Summaries, usageSummaryToProto(s))
	}
	return out, nil
}

// ============================================================================
// GetInvoice — single invoice by id (TENANT-ISOLATED).
// ============================================================================
//
// SECURITY (the headline guard — this is a MONEY read): the proto contract is
// explicit — "the caller's team owns this invoice (or the caller is admin); no
// cross-team invoice reads". GetInvoiceRequest carries ONLY an id (no team), so a
// non-admin must NOT be able to read team B's invoice by supplying its UUID. An
// invoice exposes line items, per-meter quantities, unit prices, totals, and the
// rate_plan_id — a direct financial tenant-isolation breach if leaked cross-team.
//
// WHY THE OWNERSHIP CHECK LIVES HERE (a post-fetch guard, not a query predicate):
// the ideal shape is GetInvoice(ctx, team, id) so the REPO scopes by owner in the
// WHERE clause and never materializes another tenant's row. That requires changing
// the domain port + repo (out of scope for this handler-only fix). The handler
// still CLOSES the hole completely: it resolves the caller's authoritative team
// from claims, fetches by id, then ENFORCES that a non-admin's claims team owns the
// returned invoice. A mismatch is treated as if the row did not exist.
//
// WHY NotFound (not PermissionDenied) on a wrong-team hit: PermissionDenied would
// CONFIRM the invoice id exists (an enumeration oracle — a caller could probe which
// UUIDs are live invoices). Returning NotFound — byte-for-byte identical to a truly
// missing id — makes existence UNPROBEABLE across the tenant boundary. Crucially we
// return BEFORE invoiceToProto, so not a single field of B's invoice reaches A.
//
// Admins (claims.Role == adminRole) bypass the team check: the proto allows an
// admin to read any team's invoice (back-office/support).
//
// WHY A FORBIDDEN READ MAPS TO NotFound AND NOT Forbidden: to
// avoid a resource-existence side channel. For an isolated, id-addressable resource
// the secure default is "indistinguishable from missing"; 403 vs 404 itself leaks.
func (h *BillingHandler) GetInvoice(ctx context.Context, req *billingv1.GetInvoiceRequest) (*billingv1.GetInvoiceResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	// Resolve the caller's identity FIRST. This RPC is authenticated; absent claims
	// mean the call bypassed the auth interceptor — fail closed with Unauthenticated.
	claims, ok := grpcutil.ClaimsFromContext(ctx)
	if !ok || claims == nil || claims.Team == "" {
		return nil, status.Error(codes.Unauthenticated, "missing authentication")
	}

	if req.GetInvoiceId() == "" {
		return nil, status.Error(codes.InvalidArgument, "invoice_id is required")
	}

	invoice, err := h.svc.GetInvoice(ctx, req.GetInvoiceId())
	if err != nil {
		return nil, toStatusError(err)
	}

	// TENANT-ISOLATION GUARD: a non-admin may only read its OWN team's invoice. A
	// wrong-team hit is surfaced as NotFound — identical to a missing id — and we
	// return before converting, so none of the invoice's financial data leaks.
	if claims.Role != adminRole && invoice.Team != claims.Team {
		return nil, status.Error(codes.NotFound, "invoice not found")
	}

	return &billingv1.GetInvoiceResponse{
		Invoice: invoiceToProto(invoice),
	}, nil
}

// ============================================================================
// ListInvoices — a team's invoices, newest-first, paginated.
// ============================================================================
//
// TENANCY via resolveTeam (non-admins scoped to own team). status_filter is
// optional (UNSPECIFIED = all statuses); a non-unspecified value must be a known
// status. PAGINATION normalized like GetUsage.
func (h *BillingHandler) ListInvoices(ctx context.Context, req *billingv1.ListInvoicesRequest) (*billingv1.ListInvoicesResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	team, err := resolveTeam(ctx, req.GetTeam())
	if err != nil {
		return nil, err
	}

	opts := domain.ListInvoicesOptions{
		Team:      team,
		PageSize:  normalizePageSize(req.GetPagination()),
		PageToken: req.GetPagination().GetPageToken(),
	}

	if ps := req.GetStatusFilter(); ps != billingv1.InvoiceStatus_INVOICE_STATUS_UNSPECIFIED {
		st, ok := invoiceStatusFromProto(ps)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "status_filter is not a known invoice status")
		}
		opts.StatusFilter = st
	}

	invoices, nextToken, err := h.svc.ListInvoices(ctx, opts)
	if err != nil {
		return nil, toStatusError(err)
	}

	out := &billingv1.ListInvoicesResponse{
		Invoices:   make([]*billingv1.Invoice, 0, len(invoices)),
		Pagination: &commonv1.PaginationResponse{NextPageToken: nextToken},
	}
	for _, inv := range invoices {
		out.Invoices = append(out.Invoices, invoiceToProto(inv))
	}
	return out, nil
}

// ============================================================================
// TENANCY + PAGINATION HELPERS
// ============================================================================

// resolveTeam applies the tenant-isolation rule for the reporting/quota RPCs:
//   - The caller MUST be authenticated (claims present) — else Unauthenticated.
//   - A non-admin is FORCED to its own claims team; the request-supplied team is
//     ignored (it cannot read another tenant's data by naming it).
//   - An admin (or the trusted BFF) may target any team; if it supplies an empty
//     team, it defaults to its own.
//
// WHY here and not in the domain: the domain trusts `team` as already-authorized
// (see GetUsageInput's comment). The "who may name which team" decision is an
// auth/transport concern, so it lives at the handler boundary where the claims are.
func resolveTeam(ctx context.Context, requested string) (string, error) {
	claims, ok := grpcutil.ClaimsFromContext(ctx)
	if !ok || claims == nil || claims.Team == "" {
		return "", status.Error(codes.Unauthenticated, "missing authentication")
	}

	// Admins (and the trusted internal gateway, which carries an admin-equivalent
	// role) may scope to any team; an empty request team defaults to their own.
	if claims.Role == adminRole {
		if requested == "" {
			return claims.Team, nil
		}
		return requested, nil
	}

	// Non-admins: confine to the claims team. We do NOT error on a mismatching
	// requested team — we silently scope to the caller's own (the proto documents
	// "server overrides this with the caller's own team"). Erroring would leak
	// whether the named team exists; silent scoping is the tenant-isolation choice.
	return claims.Team, nil
}

// normalizePageSize turns the proto pagination into a sane domain page size:
// default when 0/omitted, CLAMP at the cap, and floor a negative (defensive — the
// proto field is int32 and a negative is a client bug) to the default. We clamp
// rather than reject an over-large size (AIP-158: give the max we serve, not an
// error). The domain ALSO caps at MaxPageSize (defense in depth).
func normalizePageSize(p *commonv1.PaginationRequest) int {
	size := int(p.GetPageSize()) // GetPageSize on a nil *PaginationRequest returns 0
	if size <= 0 {
		return defaultPageSize
	}
	if size > maxPageSize {
		return maxPageSize
	}
	return size
}

// ============================================================================
// ENUM CONVERTERS — billingv1.MeterType / InvoiceStatus <-> domain
// ============================================================================
//
// The domain uses STRING-backed enums (readable logs, stable rate-plan map keys)
// distinct from the proto's int enums. These functions are the single mapping
// point at the wire boundary. meterTypeFromProto returns ok=false for UNSPECIFIED
// or any unrecognized value so callers reject it as InvalidArgument rather than
// silently treating an unknown meter as free.

func meterTypeFromProto(pm billingv1.MeterType) (domain.MeterType, bool) {
	switch pm {
	case billingv1.MeterType_METER_TYPE_INFERENCE_REQUEST:
		return domain.MeterTypeInferenceRequest, true
	case billingv1.MeterType_METER_TYPE_INFERENCE_TOKENS:
		return domain.MeterTypeInferenceTokens, true
	case billingv1.MeterType_METER_TYPE_COMPUTE_SECONDS:
		return domain.MeterTypeComputeSeconds, true
	case billingv1.MeterType_METER_TYPE_STORAGE_BYTES:
		return domain.MeterTypeStorageBytes, true
	default:
		// METER_TYPE_UNSPECIFIED and any unknown enum value.
		return domain.MeterTypeUnspecified, false
	}
}

// meterTypeFromName maps a rate-plan map KEY (the proto enum's STRING name, e.g.
// "METER_TYPE_INFERENCE_TOKENS") to the domain meter. The rate-plan maps are keyed
// by the enum name on the wire (see the proto), so plan creation parses the name,
// not the int. Uses billingv1's generated name<->value table so it stays in lock-
// step with the proto enum (no hand-maintained string list to drift).
func meterTypeFromName(name string) (domain.MeterType, bool) {
	value, ok := billingv1.MeterType_value[name]
	if !ok {
		return domain.MeterTypeUnspecified, false
	}
	return meterTypeFromProto(billingv1.MeterType(value))
}

// meterTypeToProto maps a domain meter back to the proto enum (UNSPECIFIED for an
// empty/unknown domain value, which is the correct zero on the wire).
func meterTypeToProto(dm domain.MeterType) billingv1.MeterType {
	switch dm {
	case domain.MeterTypeInferenceRequest:
		return billingv1.MeterType_METER_TYPE_INFERENCE_REQUEST
	case domain.MeterTypeInferenceTokens:
		return billingv1.MeterType_METER_TYPE_INFERENCE_TOKENS
	case domain.MeterTypeComputeSeconds:
		return billingv1.MeterType_METER_TYPE_COMPUTE_SECONDS
	case domain.MeterTypeStorageBytes:
		return billingv1.MeterType_METER_TYPE_STORAGE_BYTES
	default:
		return billingv1.MeterType_METER_TYPE_UNSPECIFIED
	}
}

func invoiceStatusFromProto(ps billingv1.InvoiceStatus) (domain.InvoiceStatus, bool) {
	switch ps {
	case billingv1.InvoiceStatus_INVOICE_STATUS_DRAFT:
		return domain.InvoiceStatusDraft, true
	case billingv1.InvoiceStatus_INVOICE_STATUS_FINALIZED:
		return domain.InvoiceStatusFinalized, true
	case billingv1.InvoiceStatus_INVOICE_STATUS_PAID:
		return domain.InvoiceStatusPaid, true
	case billingv1.InvoiceStatus_INVOICE_STATUS_OVERDUE:
		return domain.InvoiceStatusOverdue, true
	case billingv1.InvoiceStatus_INVOICE_STATUS_VOID:
		return domain.InvoiceStatusVoid, true
	default:
		return domain.InvoiceStatusUnspecified, false
	}
}

func invoiceStatusToProto(ds domain.InvoiceStatus) billingv1.InvoiceStatus {
	switch ds {
	case domain.InvoiceStatusDraft:
		return billingv1.InvoiceStatus_INVOICE_STATUS_DRAFT
	case domain.InvoiceStatusFinalized:
		return billingv1.InvoiceStatus_INVOICE_STATUS_FINALIZED
	case domain.InvoiceStatusPaid:
		return billingv1.InvoiceStatus_INVOICE_STATUS_PAID
	case domain.InvoiceStatusOverdue:
		return billingv1.InvoiceStatus_INVOICE_STATUS_OVERDUE
	case domain.InvoiceStatusVoid:
		return billingv1.InvoiceStatus_INVOICE_STATUS_VOID
	default:
		return billingv1.InvoiceStatus_INVOICE_STATUS_UNSPECIFIED
	}
}

// ============================================================================
// MONEY / DOMAIN <-> PROTO CONVERTERS (the anti-corruption layer)
// ============================================================================
//
// Centralizing these means the proto<->domain mapping (and the never-trust-client-
// money discipline) lives in ONE auditable spot. Money is the load-bearing one:
// micros + currency travel together on both sides.

// moneyFromProto converts an inbound proto Money (only used on CreateRatePlan, the
// one write that carries prices) to domain.Money. A nil proto Money is the
// zero-value domain.Money{}; the domain validates currency/non-negativity.
func moneyFromProto(m *billingv1.Money) domain.Money {
	if m == nil {
		return domain.Money{}
	}
	return domain.Money{
		AmountMicros: m.GetAmountMicros(),
		CurrencyCode: m.GetCurrencyCode(),
	}
}

// moneyToProto converts domain.Money to proto Money. A zero-value domain.Money
// (empty currency) becomes a proto Money with empty currency — a valid "no amount
// yet" on the wire (e.g. a grand total over an empty window).
func moneyToProto(m domain.Money) *billingv1.Money {
	return &billingv1.Money{
		AmountMicros: m.AmountMicros,
		CurrencyCode: m.CurrencyCode,
	}
}

// ratePlanToProto converts a domain.RatePlan to the proto RatePlan, re-keying the
// meter maps by the proto enum's STRING name (the wire contract for these maps).
func ratePlanToProto(p domain.RatePlan) *billingv1.RatePlan {
	out := &billingv1.RatePlan{
		Id:                 p.ID,
		Name:               p.Name,
		UnitPrices:         make(map[string]*billingv1.Money, len(p.UnitPrices)),
		IncludedQuantities: make(map[string]int64, len(p.IncludedQuantities)),
		QuotaLimits:        make(map[string]int64, len(p.QuotaLimits)),
		CreatedAt:          timestamppb.New(p.CreatedAt),
	}
	for meter, price := range p.UnitPrices {
		out.UnitPrices[meterTypeToProto(meter).String()] = moneyToProto(price)
	}
	for meter, qty := range p.IncludedQuantities {
		out.IncludedQuantities[meterTypeToProto(meter).String()] = qty
	}
	for meter, limit := range p.QuotaLimits {
		out.QuotaLimits[meterTypeToProto(meter).String()] = limit
	}
	return out
}

// usageRecordToProto converts a domain.UsageRecord to the proto UsageRecord. Every
// money/attribution field is server-set on the domain side; the converter just
// projects it onto the wire. cost carries the server-computed price (proving it was
// NOT client-supplied — the request had no price field).
func usageRecordToProto(r domain.UsageRecord) *billingv1.UsageRecord {
	return &billingv1.UsageRecord{
		Id:              r.ID,
		Team:            r.Team,
		RatePlanId:      r.RatePlanID,
		MeterType:       meterTypeToProto(r.MeterType),
		Quantity:        r.Quantity,
		Cost:            moneyToProto(r.Cost),
		ModelId:         r.ModelID,
		ModelVersion:    r.ModelVersion,
		SourceRequestId: r.SourceRequestID,
		OccurredAt:      timestamppb.New(r.OccurredAt),
	}
}

// usageSummaryToProto converts a domain.UsageSummary (the GetUsage rollup) to the
// proto UsageSummary, re-keying the per-meter buckets by enum name.
func usageSummaryToProto(s domain.UsageSummary) *billingv1.UsageSummary {
	out := &billingv1.UsageSummary{
		Team:        s.Team,
		PeriodStart: timestamppb.New(s.PeriodStart),
		PeriodEnd:   timestamppb.New(s.PeriodEnd),
		ByMeter:     make(map[string]*billingv1.MeterUsage, len(s.ByMeter)),
		TotalCost:   moneyToProto(s.TotalCost),
	}
	for meter, bucket := range s.ByMeter {
		out.ByMeter[meterTypeToProto(meter).String()] = &billingv1.MeterUsage{
			MeterType:     meterTypeToProto(bucket.MeterType),
			TotalQuantity: bucket.TotalQuantity,
			TotalCost:     moneyToProto(bucket.TotalCost),
		}
	}
	return out
}

// invoiceToProto converts a domain.Invoice (with its line items) to the proto
// Invoice. finalized_at/due_at are optional *time.Time on the domain — nil maps to
// an unset proto timestamp (not the zero epoch), preserving "DRAFT, not finalized".
func invoiceToProto(inv domain.Invoice) *billingv1.Invoice {
	out := &billingv1.Invoice{
		Id:            inv.ID,
		InvoiceNumber: inv.InvoiceNumber,
		Team:          inv.Team,
		RatePlanId:    inv.RatePlanID,
		Status:        invoiceStatusToProto(inv.Status),
		PeriodStart:   timestamppb.New(inv.PeriodStart),
		PeriodEnd:     timestamppb.New(inv.PeriodEnd),
		LineItems:     make([]*billingv1.InvoiceLineItem, 0, len(inv.LineItems)),
		Total:         moneyToProto(inv.Total),
		CreatedAt:     timestamppb.New(inv.CreatedAt),
	}
	for _, li := range inv.LineItems {
		out.LineItems = append(out.LineItems, &billingv1.InvoiceLineItem{
			MeterType:   meterTypeToProto(li.MeterType),
			Description: li.Description,
			Quantity:    li.Quantity,
			UnitPrice:   moneyToProto(li.UnitPrice),
			Amount:      moneyToProto(li.Amount),
		})
	}
	if inv.FinalizedAt != nil {
		out.FinalizedAt = timestamppb.New(*inv.FinalizedAt)
	}
	if inv.DueAt != nil {
		out.DueAt = timestamppb.New(*inv.DueAt)
	}
	return out
}

// ============================================================================
// ERROR MAPPING — domain sentinels -> gRPC status codes
// ============================================================================
//
// toStatusError is the single anti-corruption point between domain errors and the
// gRPC status vocabulary. gRPC clients branch on status.Code(err), so the mapping
// must be precise AND must never leak internal text (SQL, secrets, PII) to a
// caller. errors.Is (not ==) so a wrapped sentinel still maps correctly.
//
//	domain sentinel          -> gRPC code             why
//	---------------------------------------------------------------------------
//	ErrValidation            -> InvalidArgument        client sent bad input
//	ErrNegativeQuantity      -> InvalidArgument        negative qty (manufacture credit)
//	ErrQuantityTooLarge      -> InvalidArgument        over the overflow bound
//	ErrUnknownMeter          -> InvalidArgument        unset/unknown meter (no $0 guess)
//	ErrInvalidCurrency       -> InvalidArgument        malformed ISO-4217 code
//	ErrCurrencyMismatch      -> InvalidArgument        mixed-currency plan/sum
//	ErrInvalidPeriod         -> InvalidArgument        inverted/too-wide window
//	ErrInvoiceNotFound       -> NotFound               missing invoice
//	ErrRatePlanNotFound      -> FailedPrecondition     cannot price without a plan
//	ErrNoUsage               -> NotFound               nothing to report for period
//	ErrAmountOverflow        -> Internal (sanitized)   we refuse to emit a wrong number
//	(anything else)          -> Internal (sanitized)   no leak
//
// WHY ErrRatePlanNotFound -> FailedPrecondition (not NotFound): on the metering
// path it means "the team has no resolvable plan, so I cannot price this usage" —
// a precondition of the operation is unmet, not "the thing you asked for by id is
// gone". GetRatePlan-by-id missing ALSO funnels through ErrRatePlanNotFound; the
// proto documents both readings, and FailedPrecondition is the safer default for a
// metering caller (it should fix the team's plan, then retry). A dedicated
// GetRatePlan NotFound nuance can be added later if a caller needs to distinguish.
//
// WHY ErrAmountOverflow -> Internal: an overflow is a server-side invariant breach
// (the input bounds should make it unreachable). The client did nothing wrong and
// can't fix it; we refuse to emit a wrong number and return a sanitized Internal,
// relying on server-side alerting. We do NOT echo the sentinel text.
//
// WHY the default is Internal with a FIXED message: an unrecognized error may wrap
// SQL, a connection string, or PII. The real error is logged server-side by the
// logging interceptor; the client gets a constant opaque message.
func toStatusError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, domain.ErrValidation):
		// Wrapped ErrValidation carries a specific, NON-sensitive message describing
		// the client's own bad input (e.g. "...: unit price must be non-negative").
		// Safe to forward; this is the ONE whitelisted err.Error() path.
		return status.Error(codes.InvalidArgument, err.Error())

	case errors.Is(err, domain.ErrNegativeQuantity):
		return status.Error(codes.InvalidArgument, "quantity must be non-negative")

	case errors.Is(err, domain.ErrQuantityTooLarge):
		return status.Error(codes.InvalidArgument, "quantity exceeds per-record maximum")

	case errors.Is(err, domain.ErrUnknownMeter):
		return status.Error(codes.InvalidArgument, "unknown or unpriced meter type")

	case errors.Is(err, domain.ErrInvalidCurrency):
		return status.Error(codes.InvalidArgument, "currency code must be a 3-letter uppercase ISO-4217 code")

	case errors.Is(err, domain.ErrCurrencyMismatch):
		return status.Error(codes.InvalidArgument, "amounts must share a single currency")

	case errors.Is(err, domain.ErrInvalidPeriod):
		return status.Error(codes.InvalidArgument, "invalid or too-wide reporting period")

	case errors.Is(err, domain.ErrInvoiceNotFound):
		return status.Error(codes.NotFound, "invoice not found")

	case errors.Is(err, domain.ErrNoUsage):
		return status.Error(codes.NotFound, "no usage records in period")

	case errors.Is(err, domain.ErrRatePlanNotFound):
		// FailedPrecondition: cannot price/operate without a resolvable plan.
		return status.Error(codes.FailedPrecondition, "rate plan not found")

	default:
		// SANITIZE: never expose err.Error() — it may wrap SQL, secrets, or PII.
		// The logging interceptor records the real error server-side.
		return status.Error(codes.Internal, "internal error")
	}
}
