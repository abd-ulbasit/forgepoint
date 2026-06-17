// billing_handler_test.go — COMPONENT tests for the Billing gRPC handler.
//
// ============================================================================
// WHAT "COMPONENT TEST" MEANS HERE (and why this is the right level)
// ============================================================================
//
// These tests exercise the handler END-TO-END through a REAL gRPC stack (bufconn:
// an in-process HTTP/2 transport — see pkg/testutil/grpc.go) but with the DOMAIN
// SERVICE replaced by a hand-written mock. That isolation is deliberate:
//
//   - We mock the BillingService INTERFACE, not the repositories/outbox. The unit
//     under test is the handler: proto<->domain conversion, validation, error
//     mapping, the server-authoritative team rule, and the never-leak-internals
//     invariant. Mocking the service (one interface) keeps the test about the
//     handler, not the domain's transactional wiring. The domain's own money/
//     outbox logic has its own -race unit tests (billing_service_test.go).
//
//   - bufconn means the proto actually serializes over the wire: a field we forget
//     to map shows up as a zero value client-side, and a status error round-trips
//     through gRPC's status machinery exactly as a real client would see it.
//
// NO testcontainers, NO database, NO network sockets — runs in milliseconds.
//
// ============================================================================
// THE FOUR THINGS EVERY RPC TEST ASSERTS (the task's contract)
// ============================================================================
//
//	(a) HAPPY PATH: proto<->domain conversion is correct (the mock records what
//	    domain input it received; we assert the response fields) — INCLUDING the
//	    money/meter conversions a billing service lives and dies by.
//	(b) VALIDATION: bad requests are rejected with codes.InvalidArgument BEFORE the
//	    domain is ever called (we assert the mock was NOT invoked).
//	(c) ERROR MAPPING: each domain sentinel maps to the right gRPC status code.
//	(d) NO LEAK: error messages never contain internal/SQL/secret/PII text.
//
// PLUS the billing-specific security assertion: the billed `team` comes from auth
// CLAIMS, never from a client field (RecordUsage), and a non-admin cannot read
// another team's data by naming it (resolveTeam).
package handler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	billingv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/billing/v1"
	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/billing/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ----------------------------------------------------------------------------
// Tiny test helpers (kept local so the test file is self-contained).
// ----------------------------------------------------------------------------

// errorsNew fabricates an UNRECOGNIZED domain error (one toStatusError must
// sanitize to Internal — simulating a wrapped SQL/infra failure).
func errorsNew(msg string) error { return errors.New(msg) }

// wrap simulates the domain wrapping a sentinel with a specific message
// (fmt.Errorf("...: %w", sentinel)) so we verify errors.Is-based mapping survives
// wrapping (the domain wraps ErrValidation/ErrUnknownMeter etc. in practice).
func wrap(sentinel error, msg string) error { return fmt.Errorf("%s: %w", msg, sentinel) }

// stubValidator implements grpcutil.TokenValidator by returning fixed claims. We
// drive the REAL grpcutil.AuthUnaryInterceptor with it so the test exercises the
// genuine claims-injection path (the handler reads claims via grpcutil's own
// private context key, which only that interceptor can populate).
type stubValidator struct {
	claims *grpcutil.Claims
	err    error
}

func (s stubValidator) Validate(ctx context.Context, token string) (*grpcutil.Claims, error) {
	return s.claims, s.err
}

// ============================================================================
// MOCK domain.BillingService — a hand-written test double
// ============================================================================
//
// WHY hand-written and not gomock/mockery: function-field mocks are dependency-
// free, read top-to-bottom, and let each test set ONLY the behavior it needs (a
// nil field means "this RPC must not call this method"; calling it panics, which
// the recovery interceptor turns into Internal and the test then flags via call
// counters). For a 7-method interface this is less code than a generated mock.
type mockBillingService struct {
	recordUsageFn    func(ctx context.Context, team string, in domain.RecordUsageInput) (domain.UsageRecord, bool, error)
	createRatePlanFn func(ctx context.Context, in domain.CreateRatePlanInput) (domain.RatePlan, bool, error)
	getRatePlanFn    func(ctx context.Context, id string) (domain.RatePlan, error)
	checkQuotaFn     func(ctx context.Context, team string, meter domain.MeterType) (domain.QuotaStatus, error)
	getUsageFn       func(ctx context.Context, in domain.GetUsageInput) ([]domain.UsageSummary, domain.Money, string, error)
	getInvoiceFn     func(ctx context.Context, id string) (domain.Invoice, error)
	listInvoicesFn   func(ctx context.Context, opts domain.ListInvoicesOptions) ([]domain.Invoice, string, error)
	generateInvFn    func(ctx context.Context, team string, ps, pe time.Time) (domain.Invoice, error)

	// Call recorders — let validation tests assert the domain was NOT reached.
	recordUsageCalls    int
	createRatePlanCalls int
	getRatePlanCalls    int
	checkQuotaCalls     int
	getUsageCalls       int
	getInvoiceCalls     int
	listInvoicesCalls   int

	// Captured inputs for happy-path conversion assertions.
	lastRecordTeam   string
	lastRecordInput  domain.RecordUsageInput
	lastCreatePlanIn domain.CreateRatePlanInput
	lastQuotaTeam    string
	lastQuotaMeter   domain.MeterType
	lastUsageInput   domain.GetUsageInput
	lastListOpts     domain.ListInvoicesOptions
}

func (m *mockBillingService) RecordUsage(ctx context.Context, team string, in domain.RecordUsageInput) (domain.UsageRecord, bool, error) {
	m.recordUsageCalls++
	m.lastRecordTeam = team
	m.lastRecordInput = in
	return m.recordUsageFn(ctx, team, in)
}

func (m *mockBillingService) CreateRatePlan(ctx context.Context, in domain.CreateRatePlanInput) (domain.RatePlan, bool, error) {
	m.createRatePlanCalls++
	m.lastCreatePlanIn = in
	return m.createRatePlanFn(ctx, in)
}

func (m *mockBillingService) GetRatePlan(ctx context.Context, id string) (domain.RatePlan, error) {
	m.getRatePlanCalls++
	return m.getRatePlanFn(ctx, id)
}

func (m *mockBillingService) CheckQuota(ctx context.Context, team string, meter domain.MeterType) (domain.QuotaStatus, error) {
	m.checkQuotaCalls++
	m.lastQuotaTeam = team
	m.lastQuotaMeter = meter
	return m.checkQuotaFn(ctx, team, meter)
}

func (m *mockBillingService) GetUsage(ctx context.Context, in domain.GetUsageInput) ([]domain.UsageSummary, domain.Money, string, error) {
	m.getUsageCalls++
	m.lastUsageInput = in
	return m.getUsageFn(ctx, in)
}

func (m *mockBillingService) GetInvoice(ctx context.Context, id string) (domain.Invoice, error) {
	m.getInvoiceCalls++
	return m.getInvoiceFn(ctx, id)
}

func (m *mockBillingService) ListInvoices(ctx context.Context, opts domain.ListInvoicesOptions) ([]domain.Invoice, string, error) {
	m.listInvoicesCalls++
	m.lastListOpts = opts
	return m.listInvoicesFn(ctx, opts)
}

func (m *mockBillingService) GenerateInvoice(ctx context.Context, team string, ps, pe time.Time) (domain.Invoice, error) {
	return m.generateInvFn(ctx, team, ps, pe)
}

// ============================================================================
// TEST HARNESS
// ============================================================================

// newTestClient wires the handler (backed by the given mock) onto a bufconn gRPC
// server and returns a ready BillingServiceClient. We OPTIONALLY install the real
// auth interceptor to inject claims (most billing RPCs read the claims team).
func newTestClient(t *testing.T, svc domain.BillingService, opts ...grpc.ServerOption) billingv1.BillingServiceClient {
	t.Helper()
	h := NewBillingHandler(svc)
	conn := testutil.NewTestGRPCServer(t, func(s *grpc.Server) {
		billingv1.RegisterBillingServiceServer(s, h)
	}, opts...)
	return billingv1.NewBillingServiceClient(conn)
}

// claimsInjector installs the REAL grpcutil.AuthUnaryInterceptor backed by a stub
// validator returning the given claims — the SAME public path the platform uses to
// put claims in the context, so grpcutil.ClaimsFromContext sees an authenticated
// caller exactly as in production (we are not faking the context plumbing).
func claimsInjector(claims *grpcutil.Claims) grpc.ServerOption {
	return grpc.ChainUnaryInterceptor(grpcutil.AuthUnaryInterceptor(stubValidator{claims: claims}))
}

// ctxWithBearer attaches an "authorization: Bearer <token>" header so the
// server-side AuthUnaryInterceptor (when installed) extracts it and runs the
// validator. The token value is irrelevant (the stub ignores it) but must be
// present, since the interceptor rejects a missing header before the validator.
func ctxWithBearer() context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer test-token")
}

// requireCode asserts that err carries the expected gRPC status code.
func requireCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with code %s, got nil", want)
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected a gRPC status error, got %T: %v", err, err)
	}
	if st.Code() != want {
		t.Fatalf("expected code %s, got %s (message: %q)", want, st.Code(), st.Message())
	}
}

// assertNoLeak fails if the message contains any substring indicating an internal/
// SQL/secret/PII leak. This is the (d) guarantee, checked uniformly.
func assertNoLeak(t *testing.T, msg string) {
	t.Helper()
	lower := strings.ToLower(msg)
	for _, banned := range []string{
		"repository:", // storage-layer sentinel text
		"sql",         // SQL fragments
		"pgx", "pq:",  // driver internals
		"goroutine", // stack-trace leak
		"connection string",
		"password=", // a leaked DSN credential
		"panic",
	} {
		if strings.Contains(lower, banned) {
			t.Fatalf("error message leaks internal detail (%q): %q", banned, msg)
		}
	}
}

// fixedClaims is the common authenticated caller used by most tests (a normal,
// non-admin member of team "ml-platform").
func fixedClaims() *grpcutil.Claims {
	return &grpcutil.Claims{UserID: "caller-uuid", Team: "ml-platform", Role: "engineer"}
}

// adminClaims is an admin caller (may scope reporting/quota to any team).
func adminClaims() *grpcutil.Claims {
	return &grpcutil.Claims{UserID: "admin-uuid", Team: "ops", Role: "admin"}
}

// ============================================================================
// RecordUsage — team-from-claims, meter+quantity validation, conversion
// ============================================================================

func TestRecordUsage_HappyPath_TeamFromClaims_PricesServerSide(t *testing.T) {
	occurred := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	mock := &mockBillingService{
		recordUsageFn: func(ctx context.Context, team string, in domain.RecordUsageInput) (domain.UsageRecord, bool, error) {
			// The server-computed record. Note cost is attached by the SERVER —
			// the request carried no price; we return one to prove it round-trips.
			return domain.UsageRecord{
				ID:              "rec-uuid-1",
				Team:            team, // echo the server-authoritative team
				RatePlanID:      "plan-uuid-9",
				MeterType:       in.MeterType,
				Quantity:        in.Quantity,
				Cost:            domain.Money{AmountMicros: 4_000, CurrencyCode: "USD"},
				ModelID:         in.ModelID,
				ModelVersion:    in.ModelVersion,
				SourceRequestID: in.SourceRequestID,
				IdempotencyKey:  in.IdempotencyKey,
				OccurredAt:      in.OccurredAt,
			}, false, nil
		},
	}
	client := newTestClient(t, mock, claimsInjector(fixedClaims()))

	resp, err := client.RecordUsage(ctxWithBearer(), &billingv1.RecordUsageRequest{
		MeterType:       billingv1.MeterType_METER_TYPE_INFERENCE_TOKENS,
		Quantity:        1000,
		ModelId:         "gpt-mini",
		ModelVersion:    "v3",
		SourceRequestId: "infer-req-42",
		IdempotencyKey:  "idem-42",
		OccurredAt:      timestamppb.New(occurred),
	})
	if err != nil {
		t.Fatalf("RecordUsage returned error: %v", err)
	}

	// SECURITY: the domain was called with the CLAIMS team, never a client value
	// (RecordUsageRequest has no team field — this proves it's claims-derived).
	if mock.lastRecordTeam != "ml-platform" {
		t.Fatalf("billed team = %q; want ml-platform (from claims)", mock.lastRecordTeam)
	}

	// (a) conversion proto -> domain input.
	in := mock.lastRecordInput
	if in.MeterType != domain.MeterTypeInferenceTokens {
		t.Fatalf("meter not converted: %q", in.MeterType)
	}
	if in.Quantity != 1000 || in.ModelID != "gpt-mini" || in.ModelVersion != "v3" ||
		in.SourceRequestID != "infer-req-42" || in.IdempotencyKey != "idem-42" {
		t.Fatalf("input fields not forwarded: %+v", in)
	}
	if !in.OccurredAt.Equal(occurred) {
		t.Fatalf("occurred_at not converted: %v want %v", in.OccurredAt, occurred)
	}

	// (a) conversion domain -> proto response (server-computed cost + meter).
	rec := resp.GetRecord()
	if rec.GetId() != "rec-uuid-1" || rec.GetTeam() != "ml-platform" || rec.GetRatePlanId() != "plan-uuid-9" {
		t.Fatalf("record fields mismatch: %+v", rec)
	}
	if rec.GetMeterType() != billingv1.MeterType_METER_TYPE_INFERENCE_TOKENS {
		t.Fatalf("response meter not converted: %v", rec.GetMeterType())
	}
	if rec.GetCost().GetAmountMicros() != 4_000 || rec.GetCost().GetCurrencyCode() != "USD" {
		t.Fatalf("cost not converted: %+v", rec.GetCost())
	}
	if resp.GetDeduplicated() {
		t.Fatalf("deduplicated = true; want false")
	}
}

func TestRecordUsage_Deduplicated_PropagatesFlag(t *testing.T) {
	mock := &mockBillingService{
		recordUsageFn: func(ctx context.Context, team string, in domain.RecordUsageInput) (domain.UsageRecord, bool, error) {
			return domain.UsageRecord{ID: "orig", Team: team, MeterType: in.MeterType}, true, nil
		},
	}
	client := newTestClient(t, mock, claimsInjector(fixedClaims()))
	resp, err := client.RecordUsage(ctxWithBearer(), &billingv1.RecordUsageRequest{
		MeterType: billingv1.MeterType_METER_TYPE_INFERENCE_REQUEST, Quantity: 1,
	})
	if err != nil {
		t.Fatalf("RecordUsage error: %v", err)
	}
	if !resp.GetDeduplicated() {
		t.Fatalf("deduplicated = false; want true (idempotent replay)")
	}
}

func TestRecordUsage_NoClaims_Unauthenticated(t *testing.T) {
	mock := &mockBillingService{
		recordUsageFn: func(ctx context.Context, team string, in domain.RecordUsageInput) (domain.UsageRecord, bool, error) {
			t.Fatal("domain RecordUsage must NOT be called without claims")
			return domain.UsageRecord{}, false, nil
		},
	}
	client := newTestClient(t, mock) // no claimsInjector
	_, err := client.RecordUsage(context.Background(), &billingv1.RecordUsageRequest{
		MeterType: billingv1.MeterType_METER_TYPE_INFERENCE_REQUEST, Quantity: 1,
	})
	requireCode(t, err, codes.Unauthenticated)
	if mock.recordUsageCalls != 0 {
		t.Fatalf("domain RecordUsage called %d times without claims; want 0", mock.recordUsageCalls)
	}
}

func TestRecordUsage_Validation(t *testing.T) {
	cases := []struct {
		name string
		req  *billingv1.RecordUsageRequest
	}{
		{"unspecified meter", &billingv1.RecordUsageRequest{MeterType: billingv1.MeterType_METER_TYPE_UNSPECIFIED, Quantity: 1}},
		{"negative quantity", &billingv1.RecordUsageRequest{MeterType: billingv1.MeterType_METER_TYPE_INFERENCE_REQUEST, Quantity: -1}},
		{"quantity over max", &billingv1.RecordUsageRequest{MeterType: billingv1.MeterType_METER_TYPE_INFERENCE_REQUEST, Quantity: domain.MaxQuantityPerRecord + 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockBillingService{
				recordUsageFn: func(ctx context.Context, team string, in domain.RecordUsageInput) (domain.UsageRecord, bool, error) {
					t.Fatal("domain RecordUsage must NOT be called for invalid input")
					return domain.UsageRecord{}, false, nil
				},
			}
			client := newTestClient(t, mock, claimsInjector(fixedClaims()))
			_, err := client.RecordUsage(ctxWithBearer(), tc.req)
			requireCode(t, err, codes.InvalidArgument)
			if mock.recordUsageCalls != 0 {
				t.Fatalf("domain RecordUsage called %d times; want 0", mock.recordUsageCalls)
			}
		})
	}
}

func TestRecordUsage_ErrorMapping(t *testing.T) {
	cases := []struct {
		name     string
		domErr   error
		wantCode codes.Code
	}{
		{"negative quantity sentinel", domain.ErrNegativeQuantity, codes.InvalidArgument},
		{"quantity too large sentinel", domain.ErrQuantityTooLarge, codes.InvalidArgument},
		{"unknown meter sentinel", domain.ErrUnknownMeter, codes.InvalidArgument},
		{"no plan -> failed precondition", domain.ErrRatePlanNotFound, codes.FailedPrecondition},
		{"overflow -> internal", domain.ErrAmountOverflow, codes.Internal},
		{"validation wrapped", wrap(domain.ErrValidation, "bad input"), codes.InvalidArgument},
		{"unknown -> internal", errorsNew("repository: deadlock on usage_events password=topsecret"), codes.Internal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockBillingService{
				recordUsageFn: func(ctx context.Context, team string, in domain.RecordUsageInput) (domain.UsageRecord, bool, error) {
					return domain.UsageRecord{}, false, tc.domErr
				},
			}
			client := newTestClient(t, mock, claimsInjector(fixedClaims()))
			_, err := client.RecordUsage(ctxWithBearer(), &billingv1.RecordUsageRequest{
				MeterType: billingv1.MeterType_METER_TYPE_INFERENCE_REQUEST, Quantity: 1,
			})
			requireCode(t, err, tc.wantCode)
			st, _ := status.FromError(err)
			assertNoLeak(t, st.Message())
			// Overflow/internal must NOT echo the underlying text.
			if strings.Contains(st.Message(), "topsecret") || strings.Contains(st.Message(), "overflow") {
				t.Fatalf("internal error leaked detail: %q", st.Message())
			}
		})
	}
}

// ============================================================================
// CreateRatePlan — map conversion + unknown-meter-key rejection
// ============================================================================

func TestCreateRatePlan_HappyPath_ConvertsMaps(t *testing.T) {
	created := domain.RatePlan{
		ID:   "plan-uuid-1",
		Name: "standard",
		UnitPrices: map[domain.MeterType]domain.Money{
			domain.MeterTypeInferenceTokens: {AmountMicros: 4, CurrencyCode: "USD"},
		},
		IncludedQuantities: map[domain.MeterType]int64{domain.MeterTypeInferenceTokens: 1000},
		QuotaLimits:        map[domain.MeterType]int64{domain.MeterTypeInferenceTokens: 1_000_000},
		CreatedAt:          time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	mock := &mockBillingService{
		createRatePlanFn: func(ctx context.Context, in domain.CreateRatePlanInput) (domain.RatePlan, bool, error) {
			return created, false, nil
		},
	}
	client := newTestClient(t, mock, claimsInjector(adminClaims()))

	resp, err := client.CreateRatePlan(ctxWithBearer(), &billingv1.CreateRatePlanRequest{
		Name: "standard",
		UnitPrices: map[string]*billingv1.Money{
			"METER_TYPE_INFERENCE_TOKENS": {AmountMicros: 4, CurrencyCode: "USD"},
		},
		IncludedQuantities: map[string]int64{"METER_TYPE_INFERENCE_TOKENS": 1000},
		QuotaLimits:        map[string]int64{"METER_TYPE_INFERENCE_TOKENS": 1_000_000},
		IdempotencyKey:     "idem-plan-1",
	})
	if err != nil {
		t.Fatalf("CreateRatePlan returned error: %v", err)
	}

	// (a) conversion proto map (string keys) -> domain map (MeterType keys).
	in := mock.lastCreatePlanIn
	if in.Name != "standard" || in.IdempotencyKey != "idem-plan-1" {
		t.Fatalf("scalar fields not forwarded: %+v", in)
	}
	price, ok := in.UnitPrices[domain.MeterTypeInferenceTokens]
	if !ok || price.AmountMicros != 4 || price.CurrencyCode != "USD" {
		t.Fatalf("unit price not converted: %+v", in.UnitPrices)
	}
	if in.IncludedQuantities[domain.MeterTypeInferenceTokens] != 1000 {
		t.Fatalf("included quantity not converted: %+v", in.IncludedQuantities)
	}
	if in.QuotaLimits[domain.MeterTypeInferenceTokens] != 1_000_000 {
		t.Fatalf("quota limit not converted: %+v", in.QuotaLimits)
	}

	// (a) conversion domain -> proto response (map re-keyed by enum name).
	rp := resp.GetRatePlan()
	if rp.GetId() != "plan-uuid-1" || rp.GetName() != "standard" {
		t.Fatalf("plan fields mismatch: %+v", rp)
	}
	if rp.GetUnitPrices()["METER_TYPE_INFERENCE_TOKENS"].GetAmountMicros() != 4 {
		t.Fatalf("response unit price not re-keyed: %+v", rp.GetUnitPrices())
	}
}

func TestCreateRatePlan_UnknownMeterKey_InvalidArgument(t *testing.T) {
	mock := &mockBillingService{
		createRatePlanFn: func(ctx context.Context, in domain.CreateRatePlanInput) (domain.RatePlan, bool, error) {
			t.Fatal("domain CreateRatePlan must NOT be called for an unknown meter key")
			return domain.RatePlan{}, false, nil
		},
	}
	client := newTestClient(t, mock, claimsInjector(adminClaims()))
	_, err := client.CreateRatePlan(ctxWithBearer(), &billingv1.CreateRatePlanRequest{
		Name:       "bad",
		UnitPrices: map[string]*billingv1.Money{"METER_TYPE_TYPO": {AmountMicros: 1, CurrencyCode: "USD"}},
	})
	requireCode(t, err, codes.InvalidArgument)
	if mock.createRatePlanCalls != 0 {
		t.Fatalf("domain CreateRatePlan called %d times; want 0", mock.createRatePlanCalls)
	}
}

func TestCreateRatePlan_Validation_EmptyName(t *testing.T) {
	mock := &mockBillingService{
		createRatePlanFn: func(ctx context.Context, in domain.CreateRatePlanInput) (domain.RatePlan, bool, error) {
			t.Fatal("domain CreateRatePlan must NOT be called with empty name")
			return domain.RatePlan{}, false, nil
		},
	}
	client := newTestClient(t, mock, claimsInjector(adminClaims()))
	_, err := client.CreateRatePlan(ctxWithBearer(), &billingv1.CreateRatePlanRequest{Name: ""})
	requireCode(t, err, codes.InvalidArgument)
	if mock.createRatePlanCalls != 0 {
		t.Fatalf("domain CreateRatePlan called %d times; want 0", mock.createRatePlanCalls)
	}
}

func TestCreateRatePlan_ErrorMapping(t *testing.T) {
	cases := []struct {
		name     string
		domErr   error
		wantCode codes.Code
	}{
		{"currency mismatch", domain.ErrCurrencyMismatch, codes.InvalidArgument},
		{"invalid currency", domain.ErrInvalidCurrency, codes.InvalidArgument},
		{"validation wrapped", wrap(domain.ErrValidation, "unit price must be non-negative"), codes.InvalidArgument},
		{"unknown -> internal", errorsNew("pq: insert into rate_plans failed"), codes.Internal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockBillingService{
				createRatePlanFn: func(ctx context.Context, in domain.CreateRatePlanInput) (domain.RatePlan, bool, error) {
					return domain.RatePlan{}, false, tc.domErr
				},
			}
			client := newTestClient(t, mock, claimsInjector(adminClaims()))
			_, err := client.CreateRatePlan(ctxWithBearer(), &billingv1.CreateRatePlanRequest{
				Name:       "standard",
				UnitPrices: map[string]*billingv1.Money{"METER_TYPE_INFERENCE_TOKENS": {AmountMicros: 4, CurrencyCode: "USD"}},
			})
			requireCode(t, err, tc.wantCode)
			st, _ := status.FromError(err)
			assertNoLeak(t, st.Message())
		})
	}
}

// SECURITY (Finding 2 — admin-only enforcement for a real-money write): a non-admin
// caller must be REJECTED with PermissionDenied and the domain must NOT be called.
// CreateRatePlan sets unit prices/quotas applied to real money; the only previously
// claimed guard was an auth-interceptor "CheckPermission" that does not exist, so an
// authenticated non-admin could set platform pricing. The handler now gates it.
func TestCreateRatePlan_NonAdmin_PermissionDenied(t *testing.T) {
	mock := &mockBillingService{
		createRatePlanFn: func(ctx context.Context, in domain.CreateRatePlanInput) (domain.RatePlan, bool, error) {
			t.Fatal("domain CreateRatePlan must NOT be called by a non-admin")
			return domain.RatePlan{}, false, nil
		},
	}
	// fixedClaims() is a non-admin (Role: "engineer").
	client := newTestClient(t, mock, claimsInjector(fixedClaims()))

	_, err := client.CreateRatePlan(ctxWithBearer(), &billingv1.CreateRatePlanRequest{
		Name:       "standard",
		UnitPrices: map[string]*billingv1.Money{"METER_TYPE_INFERENCE_TOKENS": {AmountMicros: 4, CurrencyCode: "USD"}},
	})

	requireCode(t, err, codes.PermissionDenied)
	if mock.createRatePlanCalls != 0 {
		t.Fatalf("domain CreateRatePlan called %d times by a non-admin; want 0", mock.createRatePlanCalls)
	}
	st, _ := status.FromError(err)
	assertNoLeak(t, st.Message())
}

// The admin gate runs BEFORE request validation: a non-admin sending an INVALID
// request still gets PermissionDenied (not InvalidArgument), so a non-admin learns
// nothing about request wellformedness on this privileged write (no validation oracle).
func TestCreateRatePlan_NonAdmin_GateBeforeValidation(t *testing.T) {
	mock := &mockBillingService{
		createRatePlanFn: func(ctx context.Context, in domain.CreateRatePlanInput) (domain.RatePlan, bool, error) {
			t.Fatal("domain CreateRatePlan must NOT be called by a non-admin")
			return domain.RatePlan{}, false, nil
		},
	}
	client := newTestClient(t, mock, claimsInjector(fixedClaims()))
	// Empty name would be InvalidArgument for an admin; a non-admin must still see
	// PermissionDenied because the role gate precedes validation.
	_, err := client.CreateRatePlan(ctxWithBearer(), &billingv1.CreateRatePlanRequest{Name: ""})
	requireCode(t, err, codes.PermissionDenied)
	if mock.createRatePlanCalls != 0 {
		t.Fatalf("domain CreateRatePlan called %d times; want 0", mock.createRatePlanCalls)
	}
}

// CreateRatePlan without claims (auth interceptor bypassed) fails closed with
// Unauthenticated and never reaches the domain.
func TestCreateRatePlan_NoClaims_Unauthenticated(t *testing.T) {
	mock := &mockBillingService{
		createRatePlanFn: func(ctx context.Context, in domain.CreateRatePlanInput) (domain.RatePlan, bool, error) {
			t.Fatal("domain CreateRatePlan must NOT be called without claims")
			return domain.RatePlan{}, false, nil
		},
	}
	client := newTestClient(t, mock) // no claimsInjector
	_, err := client.CreateRatePlan(context.Background(), &billingv1.CreateRatePlanRequest{
		Name:       "standard",
		UnitPrices: map[string]*billingv1.Money{"METER_TYPE_INFERENCE_TOKENS": {AmountMicros: 4, CurrencyCode: "USD"}},
	})
	requireCode(t, err, codes.Unauthenticated)
	if mock.createRatePlanCalls != 0 {
		t.Fatalf("domain CreateRatePlan called %d times without claims; want 0", mock.createRatePlanCalls)
	}
}

// ============================================================================
// GetRatePlan
// ============================================================================

func TestGetRatePlan_HappyPath(t *testing.T) {
	mock := &mockBillingService{
		getRatePlanFn: func(ctx context.Context, id string) (domain.RatePlan, error) {
			if id != "plan-uuid-7" {
				t.Errorf("domain received id %q, want plan-uuid-7", id)
			}
			return domain.RatePlan{ID: id, Name: "enterprise", CreatedAt: time.Now()}, nil
		},
	}
	client := newTestClient(t, mock, claimsInjector(fixedClaims()))
	resp, err := client.GetRatePlan(ctxWithBearer(), &billingv1.GetRatePlanRequest{RatePlanId: "plan-uuid-7"})
	if err != nil {
		t.Fatalf("GetRatePlan error: %v", err)
	}
	if resp.GetRatePlan().GetId() != "plan-uuid-7" || resp.GetRatePlan().GetName() != "enterprise" {
		t.Fatalf("plan mismatch: %+v", resp.GetRatePlan())
	}
}

func TestGetRatePlan_Validation_EmptyID(t *testing.T) {
	mock := &mockBillingService{
		getRatePlanFn: func(ctx context.Context, id string) (domain.RatePlan, error) {
			t.Fatal("domain GetRatePlan must NOT be called for empty id")
			return domain.RatePlan{}, nil
		},
	}
	client := newTestClient(t, mock, claimsInjector(fixedClaims()))
	_, err := client.GetRatePlan(ctxWithBearer(), &billingv1.GetRatePlanRequest{RatePlanId: ""})
	requireCode(t, err, codes.InvalidArgument)
	if mock.getRatePlanCalls != 0 {
		t.Fatalf("domain GetRatePlan called %d times; want 0", mock.getRatePlanCalls)
	}
}

func TestGetRatePlan_NotFound_MapsFailedPrecondition(t *testing.T) {
	// ErrRatePlanNotFound maps to FailedPrecondition (cannot price without a plan).
	mock := &mockBillingService{
		getRatePlanFn: func(ctx context.Context, id string) (domain.RatePlan, error) {
			return domain.RatePlan{}, domain.ErrRatePlanNotFound
		},
	}
	client := newTestClient(t, mock, claimsInjector(fixedClaims()))
	_, err := client.GetRatePlan(ctxWithBearer(), &billingv1.GetRatePlanRequest{RatePlanId: "ghost"})
	requireCode(t, err, codes.FailedPrecondition)
	st, _ := status.FromError(err)
	assertNoLeak(t, st.Message())
}

// ============================================================================
// CheckQuota — tenancy + meter validation + conversion
// ============================================================================

func TestCheckQuota_HappyPath_NonAdminScopedToOwnTeam(t *testing.T) {
	mock := &mockBillingService{
		checkQuotaFn: func(ctx context.Context, team string, meter domain.MeterType) (domain.QuotaStatus, error) {
			return domain.QuotaStatus{
				Team:         team,
				RatePlanID:   "plan-9",
				MeterType:    meter,
				QuotaLimit:   1000,
				CurrentUsage: 1000,
				Remaining:    0,
				Exceeded:     true,
			}, nil
		},
	}
	client := newTestClient(t, mock, claimsInjector(fixedClaims()))

	// The caller asks about "other-team" but is a non-admin: the handler MUST scope
	// the check to the caller's own team (tenant isolation), ignoring the field.
	resp, err := client.CheckQuota(ctxWithBearer(), &billingv1.CheckQuotaRequest{
		Team:      "other-team",
		MeterType: billingv1.MeterType_METER_TYPE_INFERENCE_TOKENS,
	})
	if err != nil {
		t.Fatalf("CheckQuota error: %v", err)
	}
	if mock.lastQuotaTeam != "ml-platform" {
		t.Fatalf("quota checked for team %q; want ml-platform (claims override)", mock.lastQuotaTeam)
	}
	if mock.lastQuotaMeter != domain.MeterTypeInferenceTokens {
		t.Fatalf("meter not converted: %q", mock.lastQuotaMeter)
	}
	if !resp.GetExceeded() || resp.GetQuotaLimit() != 1000 || resp.GetRemaining() != 0 {
		t.Fatalf("quota response mismatch: %+v", resp)
	}
	if resp.GetMeterType() != billingv1.MeterType_METER_TYPE_INFERENCE_TOKENS {
		t.Fatalf("response meter not converted: %v", resp.GetMeterType())
	}
}

func TestCheckQuota_AdminMayScopeToOtherTeam(t *testing.T) {
	mock := &mockBillingService{
		checkQuotaFn: func(ctx context.Context, team string, meter domain.MeterType) (domain.QuotaStatus, error) {
			return domain.QuotaStatus{Team: team, MeterType: meter}, nil
		},
	}
	client := newTestClient(t, mock, claimsInjector(adminClaims()))
	_, err := client.CheckQuota(ctxWithBearer(), &billingv1.CheckQuotaRequest{
		Team:      "tenant-x",
		MeterType: billingv1.MeterType_METER_TYPE_INFERENCE_REQUEST,
	})
	if err != nil {
		t.Fatalf("CheckQuota error: %v", err)
	}
	if mock.lastQuotaTeam != "tenant-x" {
		t.Fatalf("admin scope: quota checked for %q; want tenant-x", mock.lastQuotaTeam)
	}
}

func TestCheckQuota_Validation_UnspecifiedMeter(t *testing.T) {
	mock := &mockBillingService{
		checkQuotaFn: func(ctx context.Context, team string, meter domain.MeterType) (domain.QuotaStatus, error) {
			t.Fatal("domain CheckQuota must NOT be called for an unspecified meter")
			return domain.QuotaStatus{}, nil
		},
	}
	client := newTestClient(t, mock, claimsInjector(fixedClaims()))
	_, err := client.CheckQuota(ctxWithBearer(), &billingv1.CheckQuotaRequest{
		MeterType: billingv1.MeterType_METER_TYPE_UNSPECIFIED,
	})
	requireCode(t, err, codes.InvalidArgument)
	if mock.checkQuotaCalls != 0 {
		t.Fatalf("domain CheckQuota called %d times; want 0", mock.checkQuotaCalls)
	}
}

func TestCheckQuota_NoClaims_Unauthenticated(t *testing.T) {
	mock := &mockBillingService{
		checkQuotaFn: func(ctx context.Context, team string, meter domain.MeterType) (domain.QuotaStatus, error) {
			t.Fatal("domain CheckQuota must NOT be called without claims")
			return domain.QuotaStatus{}, nil
		},
	}
	client := newTestClient(t, mock) // no claimsInjector
	_, err := client.CheckQuota(context.Background(), &billingv1.CheckQuotaRequest{
		MeterType: billingv1.MeterType_METER_TYPE_INFERENCE_REQUEST,
	})
	requireCode(t, err, codes.Unauthenticated)
}

// ============================================================================
// GetUsage — tenancy, pagination, period validation, rollup conversion
// ============================================================================

func TestGetUsage_HappyPath_PaginationAndConversion(t *testing.T) {
	mock := &mockBillingService{
		getUsageFn: func(ctx context.Context, in domain.GetUsageInput) ([]domain.UsageSummary, domain.Money, string, error) {
			return []domain.UsageSummary{
					{
						Team:        in.Team,
						PeriodStart: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
						PeriodEnd:   time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC),
						ByMeter: map[domain.MeterType]domain.MeterUsage{
							domain.MeterTypeInferenceTokens: {
								MeterType:     domain.MeterTypeInferenceTokens,
								TotalQuantity: 5000,
								TotalCost:     domain.Money{AmountMicros: 20_000, CurrencyCode: "USD"},
							},
						},
						TotalCost: domain.Money{AmountMicros: 20_000, CurrencyCode: "USD"},
					},
				},
				domain.Money{AmountMicros: 20_000, CurrencyCode: "USD"},
				"next-cursor", nil
		},
	}
	client := newTestClient(t, mock, claimsInjector(fixedClaims()))

	resp, err := client.GetUsage(ctxWithBearer(), &billingv1.GetUsageRequest{
		Team:            "ignored-for-nonadmin",
		MeterTypeFilter: billingv1.MeterType_METER_TYPE_INFERENCE_TOKENS,
		Pagination:      &commonv1.PaginationRequest{PageSize: 0, PageToken: "cur-0"}, // 0 -> default
	})
	if err != nil {
		t.Fatalf("GetUsage error: %v", err)
	}

	// Tenancy: non-admin scoped to own team regardless of the request field.
	if mock.lastUsageInput.Team != "ml-platform" {
		t.Fatalf("usage team = %q; want ml-platform (claims override)", mock.lastUsageInput.Team)
	}
	// Pagination: page_size 0 -> default; token forwarded.
	if mock.lastUsageInput.PageSize != defaultPageSize {
		t.Fatalf("page size = %d; want default %d", mock.lastUsageInput.PageSize, defaultPageSize)
	}
	if mock.lastUsageInput.PageToken != "cur-0" {
		t.Fatalf("page token = %q; want cur-0", mock.lastUsageInput.PageToken)
	}
	// Meter filter converted.
	if mock.lastUsageInput.MeterFilter != domain.MeterTypeInferenceTokens {
		t.Fatalf("meter filter not converted: %q", mock.lastUsageInput.MeterFilter)
	}

	// Response conversion: summary, grand total, next token.
	if len(resp.GetSummaries()) != 1 {
		t.Fatalf("summaries len = %d; want 1", len(resp.GetSummaries()))
	}
	bucket := resp.GetSummaries()[0].GetByMeter()["METER_TYPE_INFERENCE_TOKENS"]
	if bucket.GetTotalQuantity() != 5000 || bucket.GetTotalCost().GetAmountMicros() != 20_000 {
		t.Fatalf("meter bucket not converted: %+v", bucket)
	}
	if resp.GetGrandTotal().GetAmountMicros() != 20_000 || resp.GetGrandTotal().GetCurrencyCode() != "USD" {
		t.Fatalf("grand total not converted: %+v", resp.GetGrandTotal())
	}
	if resp.GetPagination().GetNextPageToken() != "next-cursor" {
		t.Fatalf("next token = %q; want next-cursor", resp.GetPagination().GetNextPageToken())
	}
}

func TestGetUsage_PageSizeClampedToMax(t *testing.T) {
	mock := &mockBillingService{
		getUsageFn: func(ctx context.Context, in domain.GetUsageInput) ([]domain.UsageSummary, domain.Money, string, error) {
			return nil, domain.Money{}, "", nil
		},
	}
	client := newTestClient(t, mock, claimsInjector(fixedClaims()))
	_, err := client.GetUsage(ctxWithBearer(), &billingv1.GetUsageRequest{
		Pagination: &commonv1.PaginationRequest{PageSize: 10_000}, // way over the cap
	})
	if err != nil {
		t.Fatalf("GetUsage error: %v", err)
	}
	if mock.lastUsageInput.PageSize != maxPageSize {
		t.Fatalf("page size = %d; want clamped to %d", mock.lastUsageInput.PageSize, maxPageSize)
	}
}

func TestGetUsage_InvalidPeriod_MapsInvalidArgument(t *testing.T) {
	mock := &mockBillingService{
		getUsageFn: func(ctx context.Context, in domain.GetUsageInput) ([]domain.UsageSummary, domain.Money, string, error) {
			return nil, domain.Money{}, "", domain.ErrInvalidPeriod
		},
	}
	client := newTestClient(t, mock, claimsInjector(fixedClaims()))
	_, err := client.GetUsage(ctxWithBearer(), &billingv1.GetUsageRequest{
		PeriodStart: timestamppb.New(time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC)),
		PeriodEnd:   timestamppb.New(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)), // inverted
	})
	requireCode(t, err, codes.InvalidArgument)
	st, _ := status.FromError(err)
	assertNoLeak(t, st.Message())
}

func TestGetUsage_UnknownMeterFilter_InvalidArgument(t *testing.T) {
	mock := &mockBillingService{
		getUsageFn: func(ctx context.Context, in domain.GetUsageInput) ([]domain.UsageSummary, domain.Money, string, error) {
			t.Fatal("domain GetUsage must NOT be called for an unknown meter filter")
			return nil, domain.Money{}, "", nil
		},
	}
	client := newTestClient(t, mock, claimsInjector(fixedClaims()))
	// MeterType(99) is a non-zero value not in the enum -> rejected (not "all").
	_, err := client.GetUsage(ctxWithBearer(), &billingv1.GetUsageRequest{
		MeterTypeFilter: billingv1.MeterType(99),
	})
	requireCode(t, err, codes.InvalidArgument)
	if mock.getUsageCalls != 0 {
		t.Fatalf("domain GetUsage called %d times; want 0", mock.getUsageCalls)
	}
}

// ============================================================================
// GetInvoice
// ============================================================================

func TestGetInvoice_HappyPath_ConvertsLineItemsAndOptionalTimestamps(t *testing.T) {
	finalized := time.Date(2026, 6, 30, 23, 59, 0, 0, time.UTC)
	due := finalized.Add(14 * 24 * time.Hour)
	mock := &mockBillingService{
		getInvoiceFn: func(ctx context.Context, id string) (domain.Invoice, error) {
			return domain.Invoice{
				ID:            id,
				InvoiceNumber: "INV-2026-000042",
				Team:          "ml-platform",
				RatePlanID:    "plan-9",
				Status:        domain.InvoiceStatusFinalized,
				PeriodStart:   time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
				PeriodEnd:     time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
				LineItems: []domain.InvoiceLineItem{
					{
						MeterType:   domain.MeterTypeInferenceTokens,
						Description: "Inference tokens",
						Quantity:    5000,
						UnitPrice:   domain.Money{AmountMicros: 4, CurrencyCode: "USD"},
						Amount:      domain.Money{AmountMicros: 20_000, CurrencyCode: "USD"},
					},
				},
				Total:       domain.Money{AmountMicros: 20_000, CurrencyCode: "USD"},
				CreatedAt:   time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
				FinalizedAt: &finalized,
				DueAt:       &due,
			}, nil
		},
	}
	client := newTestClient(t, mock, claimsInjector(fixedClaims()))

	resp, err := client.GetInvoice(ctxWithBearer(), &billingv1.GetInvoiceRequest{InvoiceId: "inv-uuid-1"})
	if err != nil {
		t.Fatalf("GetInvoice error: %v", err)
	}
	inv := resp.GetInvoice()
	if inv.GetInvoiceNumber() != "INV-2026-000042" || inv.GetStatus() != billingv1.InvoiceStatus_INVOICE_STATUS_FINALIZED {
		t.Fatalf("invoice fields mismatch: %+v", inv)
	}
	if len(inv.GetLineItems()) != 1 {
		t.Fatalf("line items len = %d; want 1", len(inv.GetLineItems()))
	}
	li := inv.GetLineItems()[0]
	if li.GetMeterType() != billingv1.MeterType_METER_TYPE_INFERENCE_TOKENS ||
		li.GetAmount().GetAmountMicros() != 20_000 {
		t.Fatalf("line item not converted: %+v", li)
	}
	if !inv.GetFinalizedAt().AsTime().Equal(finalized) || !inv.GetDueAt().AsTime().Equal(due) {
		t.Fatalf("optional timestamps not converted: finalized=%v due=%v", inv.GetFinalizedAt().AsTime(), inv.GetDueAt().AsTime())
	}
}

func TestGetInvoice_DraftLeavesOptionalTimestampsNil(t *testing.T) {
	mock := &mockBillingService{
		getInvoiceFn: func(ctx context.Context, id string) (domain.Invoice, error) {
			return domain.Invoice{
				ID:        id,
				Team:      "ml-platform", // owned by the caller (fixedClaims) — passes the tenant-isolation guard
				Status:    domain.InvoiceStatusDraft,
				CreatedAt: time.Now(),
				// FinalizedAt / DueAt nil — a DRAFT invoice.
			}, nil
		},
	}
	client := newTestClient(t, mock, claimsInjector(fixedClaims()))
	resp, err := client.GetInvoice(ctxWithBearer(), &billingv1.GetInvoiceRequest{InvoiceId: "draft-1"})
	if err != nil {
		t.Fatalf("GetInvoice error: %v", err)
	}
	if resp.GetInvoice().GetFinalizedAt() != nil {
		t.Fatalf("DRAFT invoice should have nil finalized_at, got %v", resp.GetInvoice().GetFinalizedAt())
	}
	if resp.GetInvoice().GetDueAt() != nil {
		t.Fatalf("DRAFT invoice should have nil due_at, got %v", resp.GetInvoice().GetDueAt())
	}
}

func TestGetInvoice_Validation_EmptyID(t *testing.T) {
	mock := &mockBillingService{
		getInvoiceFn: func(ctx context.Context, id string) (domain.Invoice, error) {
			t.Fatal("domain GetInvoice must NOT be called for empty id")
			return domain.Invoice{}, nil
		},
	}
	client := newTestClient(t, mock, claimsInjector(fixedClaims()))
	_, err := client.GetInvoice(ctxWithBearer(), &billingv1.GetInvoiceRequest{InvoiceId: ""})
	requireCode(t, err, codes.InvalidArgument)
	if mock.getInvoiceCalls != 0 {
		t.Fatalf("domain GetInvoice called %d times; want 0", mock.getInvoiceCalls)
	}
}

func TestGetInvoice_NotFound_MapsNotFound(t *testing.T) {
	mock := &mockBillingService{
		getInvoiceFn: func(ctx context.Context, id string) (domain.Invoice, error) {
			return domain.Invoice{}, domain.ErrInvoiceNotFound
		},
	}
	client := newTestClient(t, mock, claimsInjector(fixedClaims()))
	_, err := client.GetInvoice(ctxWithBearer(), &billingv1.GetInvoiceRequest{InvoiceId: "ghost"})
	requireCode(t, err, codes.NotFound)
	st, _ := status.FromError(err)
	assertNoLeak(t, st.Message())
}

// SECURITY (Finding 1 — cross-tenant invoice exposure): a non-admin in team A
// requesting an invoice that BELONGS to team B must get NotFound (indistinguishable
// from a missing id, so existence is not probeable) and NONE of team B's financial
// data may reach team A. GetInvoiceRequest carries only an id (no team), so this
// tenant-isolation guard is enforced by the handler against the returned invoice's
// owning team. Without the guard the caller would read B's line items, per-meter
// quantities, unit prices, totals and rate_plan_id by supplying B's UUID.
func TestGetInvoice_CrossTeam_NonAdmin_NotFound_NoLeak(t *testing.T) {
	// The domain returns team B's FULL invoice (the mock has no team scoping — it
	// mirrors the real domain.GetInvoice(ctx, id) signature, which cannot scope by
	// owner). The handler must NOT forward any of these secret fields to team A.
	const secretNumber = "INV-TEAMB-SECRET-0001"
	const secretPlan = "teamb-rate-plan-9"
	mock := &mockBillingService{
		getInvoiceFn: func(ctx context.Context, id string) (domain.Invoice, error) {
			return domain.Invoice{
				ID:            id,
				InvoiceNumber: secretNumber,
				Team:          "team-b", // OWNED BY ANOTHER TENANT
				RatePlanID:    secretPlan,
				Status:        domain.InvoiceStatusFinalized,
				LineItems: []domain.InvoiceLineItem{
					{
						MeterType: domain.MeterTypeInferenceTokens,
						Quantity:  999_999,
						UnitPrice: domain.Money{AmountMicros: 7, CurrencyCode: "USD"},
						Amount:    domain.Money{AmountMicros: 6_999_993, CurrencyCode: "USD"},
					},
				},
				Total:     domain.Money{AmountMicros: 6_999_993, CurrencyCode: "USD"},
				CreatedAt: time.Now(),
			}, nil
		},
	}
	// Caller is a NON-ADMIN member of team "ml-platform" (fixedClaims), asking for an
	// invoice id that the domain resolves to team-b's invoice.
	client := newTestClient(t, mock, claimsInjector(fixedClaims()))

	resp, err := client.GetInvoice(ctxWithBearer(), &billingv1.GetInvoiceRequest{InvoiceId: "team-b-invoice-uuid"})

	// Must be NotFound — byte-for-byte identical to a missing id (no existence oracle,
	// not a distinguishable PermissionDenied).
	requireCode(t, err, codes.NotFound)
	st, _ := status.FromError(err)
	assertNoLeak(t, st.Message())

	// The response must be nil and absolutely no field of team B's invoice may have
	// crossed the boundary (defense against a partial/zero-value leak in the error path).
	if resp != nil {
		t.Fatalf("cross-team GetInvoice returned a non-nil response: %+v", resp)
	}
	if strings.Contains(st.Message(), secretNumber) || strings.Contains(st.Message(), secretPlan) ||
		strings.Contains(st.Message(), "team-b") {
		t.Fatalf("error message leaked team B invoice detail: %q", st.Message())
	}
}

// An ADMIN may read ANY team's invoice (back-office/support) — the proto allows it.
// This proves the cross-team guard is scoped to non-admins and does not break the
// legitimate admin path.
func TestGetInvoice_CrossTeam_Admin_Allowed(t *testing.T) {
	mock := &mockBillingService{
		getInvoiceFn: func(ctx context.Context, id string) (domain.Invoice, error) {
			return domain.Invoice{
				ID:            id,
				InvoiceNumber: "INV-TEAMB-0001",
				Team:          "team-b", // a team OTHER than the admin's own ("ops")
				Status:        domain.InvoiceStatusFinalized,
				Total:         domain.Money{AmountMicros: 100, CurrencyCode: "USD"},
				CreatedAt:     time.Now(),
			}, nil
		},
	}
	client := newTestClient(t, mock, claimsInjector(adminClaims()))
	resp, err := client.GetInvoice(ctxWithBearer(), &billingv1.GetInvoiceRequest{InvoiceId: "team-b-invoice-uuid"})
	if err != nil {
		t.Fatalf("admin cross-team GetInvoice returned error: %v", err)
	}
	if resp.GetInvoice().GetInvoiceNumber() != "INV-TEAMB-0001" || resp.GetInvoice().GetTeam() != "team-b" {
		t.Fatalf("admin should read team B's invoice in full: %+v", resp.GetInvoice())
	}
}

// A non-admin reading its OWN team's invoice still works (the guard only blocks the
// cross-team case). This is the positive control for the isolation check.
func TestGetInvoice_SameTeam_NonAdmin_Allowed(t *testing.T) {
	mock := &mockBillingService{
		getInvoiceFn: func(ctx context.Context, id string) (domain.Invoice, error) {
			return domain.Invoice{
				ID:        id,
				Team:      "ml-platform", // SAME as fixedClaims caller's team
				Status:    domain.InvoiceStatusPaid,
				Total:     domain.Money{AmountMicros: 50, CurrencyCode: "USD"},
				CreatedAt: time.Now(),
			}, nil
		},
	}
	client := newTestClient(t, mock, claimsInjector(fixedClaims()))
	resp, err := client.GetInvoice(ctxWithBearer(), &billingv1.GetInvoiceRequest{InvoiceId: "own-invoice"})
	if err != nil {
		t.Fatalf("same-team GetInvoice returned error: %v", err)
	}
	if resp.GetInvoice().GetTeam() != "ml-platform" {
		t.Fatalf("expected own-team invoice, got %+v", resp.GetInvoice())
	}
}

// GetInvoice without claims (auth interceptor bypassed) fails closed with
// Unauthenticated and never reaches the domain.
func TestGetInvoice_NoClaims_Unauthenticated(t *testing.T) {
	mock := &mockBillingService{
		getInvoiceFn: func(ctx context.Context, id string) (domain.Invoice, error) {
			t.Fatal("domain GetInvoice must NOT be called without claims")
			return domain.Invoice{}, nil
		},
	}
	client := newTestClient(t, mock) // no claimsInjector
	_, err := client.GetInvoice(context.Background(), &billingv1.GetInvoiceRequest{InvoiceId: "x"})
	requireCode(t, err, codes.Unauthenticated)
	if mock.getInvoiceCalls != 0 {
		t.Fatalf("domain GetInvoice called %d times without claims; want 0", mock.getInvoiceCalls)
	}
}

// ============================================================================
// ListInvoices — tenancy, status filter, pagination
// ============================================================================

func TestListInvoices_HappyPath_StatusFilterAndScope(t *testing.T) {
	mock := &mockBillingService{
		listInvoicesFn: func(ctx context.Context, opts domain.ListInvoicesOptions) ([]domain.Invoice, string, error) {
			return []domain.Invoice{
				{ID: "inv-1", Team: opts.Team, Status: domain.InvoiceStatusOverdue, CreatedAt: time.Now()},
			}, "next-inv-cursor", nil
		},
	}
	client := newTestClient(t, mock, claimsInjector(fixedClaims()))

	resp, err := client.ListInvoices(ctxWithBearer(), &billingv1.ListInvoicesRequest{
		Team:         "another-team", // ignored for non-admin
		StatusFilter: billingv1.InvoiceStatus_INVOICE_STATUS_OVERDUE,
		Pagination:   &commonv1.PaginationRequest{PageSize: 50, PageToken: "cur-1"},
	})
	if err != nil {
		t.Fatalf("ListInvoices error: %v", err)
	}
	// Tenancy + filter + pagination conversion.
	if mock.lastListOpts.Team != "ml-platform" {
		t.Fatalf("list team = %q; want ml-platform (claims override)", mock.lastListOpts.Team)
	}
	if mock.lastListOpts.StatusFilter != domain.InvoiceStatusOverdue {
		t.Fatalf("status filter not converted: %q", mock.lastListOpts.StatusFilter)
	}
	if mock.lastListOpts.PageSize != 50 || mock.lastListOpts.PageToken != "cur-1" {
		t.Fatalf("pagination not forwarded: size=%d token=%q", mock.lastListOpts.PageSize, mock.lastListOpts.PageToken)
	}
	// Response conversion.
	if len(resp.GetInvoices()) != 1 || resp.GetInvoices()[0].GetStatus() != billingv1.InvoiceStatus_INVOICE_STATUS_OVERDUE {
		t.Fatalf("invoice response mismatch: %+v", resp.GetInvoices())
	}
	if resp.GetPagination().GetNextPageToken() != "next-inv-cursor" {
		t.Fatalf("next token = %q; want next-inv-cursor", resp.GetPagination().GetNextPageToken())
	}
}

func TestListInvoices_UnknownStatusFilter_InvalidArgument(t *testing.T) {
	mock := &mockBillingService{
		listInvoicesFn: func(ctx context.Context, opts domain.ListInvoicesOptions) ([]domain.Invoice, string, error) {
			t.Fatal("domain ListInvoices must NOT be called for an unknown status filter")
			return nil, "", nil
		},
	}
	client := newTestClient(t, mock, claimsInjector(fixedClaims()))
	_, err := client.ListInvoices(ctxWithBearer(), &billingv1.ListInvoicesRequest{
		StatusFilter: billingv1.InvoiceStatus(99), // non-zero, not in the enum
	})
	requireCode(t, err, codes.InvalidArgument)
	if mock.listInvoicesCalls != 0 {
		t.Fatalf("domain ListInvoices called %d times; want 0", mock.listInvoicesCalls)
	}
}

func TestListInvoices_NoClaims_Unauthenticated(t *testing.T) {
	mock := &mockBillingService{
		listInvoicesFn: func(ctx context.Context, opts domain.ListInvoicesOptions) ([]domain.Invoice, string, error) {
			t.Fatal("domain ListInvoices must NOT be called without claims")
			return nil, "", nil
		},
	}
	client := newTestClient(t, mock) // no claimsInjector
	_, err := client.ListInvoices(context.Background(), &billingv1.ListInvoicesRequest{})
	requireCode(t, err, codes.Unauthenticated)
}

// ============================================================================
// NIL-SVC GUARD — a binary wired before the domain service lands
// ============================================================================

func TestNilService_ReturnsUnimplemented(t *testing.T) {
	// Handler with svc==nil: every unary RPC must return Unimplemented instead of
	// panicking on a nil-interface method call. The nil-svc guard runs BEFORE the
	// claims/validation checks, so even an unauthenticated call gets Unimplemented.
	client := newTestClient(t, nil)
	_, err := client.GetRatePlan(context.Background(), &billingv1.GetRatePlanRequest{RatePlanId: "x"})
	requireCode(t, err, codes.Unimplemented)
}
