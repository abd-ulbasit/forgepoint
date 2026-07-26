// featurestore_handler_test.go — COMPONENT tests for the Feature Store gRPC
// handler. They exercise the handler END TO END over a real (in-process) gRPC
// stack — bufconn transport + the real auth interceptor chain — with the DOMAIN
// SERVICE replaced by a hand-written mock.
//
// ============================================================================
// WHY MOCK THE SERVICE, NOT THE REPOS (the unit-under-test boundary)
// ============================================================================
//
// The handler's job is: validate the request, convert proto↔domain, pull identity
// from the auth claims (never the request), call the service, map the result/error
// to the wire. To test JUST that, we replace the domain.FeatureStoreService with a
// mock that records what it was called with and returns whatever we program. We do
// NOT spin up Postgres/Redis (that is the repository phase's integration test) —
// mocking the SERVICE isolates the handler so a failure here is unambiguously a
// handler bug, not an infrastructure flake. (Container-free by design.)
//
// ============================================================================
// WHY THE REAL AUTH INTERCEPTOR (not a hand-rolled claims injector)
// ============================================================================
//
// Identity-from-token is the handler's central security property, so we drive it
// through the REAL grpcutil.AuthUnaryInterceptor / AuthStreamInterceptor with a
// fake TokenValidator that maps a token string → Claims. This proves the whole
// path the production server uses: metadata → interceptor → ClaimsFromContext →
// principalFromContext → service.Principal. A test that hand-injected claims would
// skip the very wiring most likely to break.
package handler

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	featurestorev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/featurestore/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/feature-store/internal/domain"
)

// ============================================================================
// TEST FIXTURES — token → claims, and the principal those claims become
// ============================================================================

const (
	// validToken maps to a fully-formed principal (user + team + scopes). The
	// fakeValidator below turns it into Claims; the auth interceptor injects them.
	validToken = "valid-token"
	// noIdentityToken maps to Claims with an empty team — exercising the handler's
	// "credentials missing required identity" guard (a token that authenticated but
	// carries no usable owner/team).
	noIdentityToken = "no-identity-token"
	// writerToken authenticates a real user+team but carries only read/write scopes
	// (NO features:admin). It exercises the RebuildViews authorization gate — an
	// authenticated-but-under-scoped caller must get PermissionDenied, not
	// InvalidArgument or Unauthenticated.
	writerToken = "writer-token"

	testUserID = "user-123"
	testTeam   = "team-alpha"
)

// fakeValidator is a test TokenValidator: it maps known token strings to Claims
// without any real JWT/crypto. This is the seam grpcutil exposes for exactly this
// purpose — each service plugs in its own validator; tests plug in a fake.
type fakeValidator struct{}

func (fakeValidator) Validate(_ context.Context, token string) (*grpcutil.Claims, error) {
	switch token {
	case validToken:
		return &grpcutil.Claims{
			UserID: testUserID,
			Team:   testTeam,
			Scopes: []string{"features:read", "features:write", "features:admin"},
		}, nil
	case writerToken:
		// Authenticated with a team but WITHOUT features:admin — used to prove the
		// RebuildViews scope gate returns PermissionDenied.
		return &grpcutil.Claims{
			UserID: testUserID,
			Team:   testTeam,
			Scopes: []string{"features:read", "features:write"},
		}, nil
	case noIdentityToken:
		// Authenticated, but no team — the handler must reject this as Unauthenticated.
		return &grpcutil.Claims{UserID: testUserID, Team: ""}, nil
	default:
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}
}

// ctxWithToken attaches a bearer token to the outgoing context so the auth
// interceptor can extract+validate it (the client side of the auth handshake).
func ctxWithToken(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

// ============================================================================
// MOCK DOMAIN SERVICE — records inputs, returns programmed outputs
// ============================================================================
//
// Each method delegates to a function field the test sets, so a test programs ONLY
// the method it exercises. The recorded "last input" fields let a test assert the
// handler converted the request correctly AND passed the principal from the token
// (not the request).
type mockService struct {
	defineFn     func(ctx context.Context, p domain.Principal, in domain.DefineFeatureViewInput) (domain.FeatureView, error)
	getByIDFn    func(ctx context.Context, p domain.Principal, id string) (domain.FeatureView, error)
	getByNameFn  func(ctx context.Context, p domain.Principal, name string) (domain.FeatureView, error)
	listFn       func(ctx context.Context, p domain.Principal, nameFilter string, opts domain.ListOptions) ([]domain.FeatureView, string, error)
	writeFn      func(ctx context.Context, p domain.Principal, in domain.WriteFeaturesInput) (domain.WriteFeaturesResult, error)
	onlineFn     func(ctx context.Context, p domain.Principal, in domain.GetOnlineFeaturesInput) (domain.FeatureVectorPage, error)
	historicalFn func(ctx context.Context, p domain.Principal, in domain.GetHistoricalFeaturesInput) (domain.FeatureVectorPage, error)
	deleteFn     func(ctx context.Context, p domain.Principal, id, idem string) (domain.FeatureView, error)
	rebuildFn    func(ctx context.Context, p domain.Principal, in domain.RebuildViewsInput, emit func(domain.RebuildProgress) error) error

	// Captured inputs for assertions.
	lastPrincipal  domain.Principal
	lastDefineIn   domain.DefineFeatureViewInput
	lastWriteIn    domain.WriteFeaturesInput
	lastOnlineIn   domain.GetOnlineFeaturesInput
	lastHistIn     domain.GetHistoricalFeaturesInput
	lastListFilter string
	lastListOpts   domain.ListOptions
	lastDeleteID   string
	lastDeleteIdem string
	lastRebuildIn  domain.RebuildViewsInput
}

func (m *mockService) DefineFeatureView(ctx context.Context, p domain.Principal, in domain.DefineFeatureViewInput) (domain.FeatureView, error) {
	m.lastPrincipal, m.lastDefineIn = p, in
	return m.defineFn(ctx, p, in)
}
func (m *mockService) GetFeatureViewByID(ctx context.Context, p domain.Principal, id string) (domain.FeatureView, error) {
	m.lastPrincipal = p
	return m.getByIDFn(ctx, p, id)
}
func (m *mockService) GetFeatureViewByName(ctx context.Context, p domain.Principal, name string) (domain.FeatureView, error) {
	m.lastPrincipal = p
	return m.getByNameFn(ctx, p, name)
}
func (m *mockService) ListFeatureViews(ctx context.Context, p domain.Principal, nameFilter string, opts domain.ListOptions) ([]domain.FeatureView, string, error) {
	m.lastPrincipal, m.lastListFilter, m.lastListOpts = p, nameFilter, opts
	return m.listFn(ctx, p, nameFilter, opts)
}
func (m *mockService) WriteFeatures(ctx context.Context, p domain.Principal, in domain.WriteFeaturesInput) (domain.WriteFeaturesResult, error) {
	m.lastPrincipal, m.lastWriteIn = p, in
	return m.writeFn(ctx, p, in)
}
func (m *mockService) GetOnlineFeatures(ctx context.Context, p domain.Principal, in domain.GetOnlineFeaturesInput) (domain.FeatureVectorPage, error) {
	m.lastPrincipal, m.lastOnlineIn = p, in
	return m.onlineFn(ctx, p, in)
}
func (m *mockService) GetHistoricalFeatures(ctx context.Context, p domain.Principal, in domain.GetHistoricalFeaturesInput) (domain.FeatureVectorPage, error) {
	m.lastPrincipal, m.lastHistIn = p, in
	return m.historicalFn(ctx, p, in)
}
func (m *mockService) DeleteFeatureView(ctx context.Context, p domain.Principal, id, idem string) (domain.FeatureView, error) {
	m.lastPrincipal, m.lastDeleteID, m.lastDeleteIdem = p, id, idem
	return m.deleteFn(ctx, p, id, idem)
}
func (m *mockService) RebuildViews(ctx context.Context, p domain.Principal, in domain.RebuildViewsInput, emit func(domain.RebuildProgress) error) error {
	m.lastPrincipal, m.lastRebuildIn = p, in
	return m.rebuildFn(ctx, p, in, emit)
}

// compile-time proof the mock satisfies the interface the handler depends on.
var _ domain.FeatureStoreService = (*mockService)(nil)

// ============================================================================
// TEST HARNESS — wire the handler behind bufconn + the real auth interceptors
// ============================================================================

// newClient starts an in-process gRPC server with the handler wired to mock,
// behind the REAL auth unary+stream interceptors (validator = fakeValidator), and
// returns a connected client. testutil handles teardown.
func newClient(t *testing.T, mock *mockService) featurestorev1.FeatureStoreServiceClient {
	t.Helper()
	h := NewFeatureStoreHandler(mock)
	conn := testutil.NewTestGRPCServer(t,
		func(s *grpc.Server) { featurestorev1.RegisterFeatureStoreServiceServer(s, h) },
		grpc.ChainUnaryInterceptor(grpcutil.AuthUnaryInterceptor(fakeValidator{})),
		grpc.ChainStreamInterceptor(grpcutil.AuthStreamInterceptor(fakeValidator{})),
	)
	return featurestorev1.NewFeatureStoreServiceClient(conn)
}

// sampleView is a fully-populated domain view used to assert domain→proto mapping.
func sampleView() domain.FeatureView {
	return domain.FeatureView{
		ID:          "fv-1",
		Name:        "user_credit",
		Description: "credit features",
		Entity:      domain.Entity{Name: "user", JoinKey: "user_id", Description: "a user"},
		Features: []domain.FeatureSpec{
			{Name: "score", ValueType: domain.FeatureTypeDouble, Description: "credit score"},
			{Name: "tier", ValueType: domain.FeatureTypeInt64},
			{Name: "embedding", ValueType: domain.FeatureTypeDoubleList, Dimension: 3},
		},
		SchemaVersion: 2,
		OwnerUserID:   testUserID,
		OwnerTeam:     testTeam,
		CreatedAt:     time.Unix(1000, 0).UTC(),
		UpdatedAt:     time.Unix(2000, 0).UTC(),
	}
}

// ============================================================================
// DefineFeatureView
// ============================================================================

func TestDefineFeatureView_HappyPath(t *testing.T) {
	mock := &mockService{
		defineFn: func(_ context.Context, _ domain.Principal, _ domain.DefineFeatureViewInput) (domain.FeatureView, error) {
			return sampleView(), nil
		},
	}
	client := newClient(t, mock)

	resp, err := client.DefineFeatureView(ctxWithToken(context.Background(), validToken), &featurestorev1.DefineFeatureViewRequest{
		Name:        "user_credit",
		Description: "credit features",
		Entity:      &featurestorev1.Entity{Name: "user", JoinKey: "user_id"},
		Features: []*featurestorev1.FeatureSpec{
			{Name: "score", ValueType: featurestorev1.FeatureValueType_FEATURE_VALUE_TYPE_DOUBLE},
			{Name: "embedding", ValueType: featurestorev1.FeatureValueType_FEATURE_VALUE_TYPE_DOUBLE_LIST, Dimension: 3},
		},
		IdempotencyKey: "idem-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// (a) proto→domain conversion: the handler must pass the request fields through.
	if got := mock.lastDefineIn.Name; got != "user_credit" {
		t.Errorf("name not passed to service: got %q", got)
	}
	if got := mock.lastDefineIn.Entity.JoinKey; got != "user_id" {
		t.Errorf("entity join key not converted: got %q", got)
	}
	if len(mock.lastDefineIn.Features) != 2 ||
		mock.lastDefineIn.Features[1].ValueType != domain.FeatureTypeDoubleList ||
		mock.lastDefineIn.Features[1].Dimension != 3 {
		t.Errorf("feature specs not converted correctly: %+v", mock.lastDefineIn.Features)
	}
	if mock.lastDefineIn.IdempotencyKey != "idem-1" {
		t.Errorf("idempotency key not passed: %q", mock.lastDefineIn.IdempotencyKey)
	}

	// IDENTITY FROM TOKEN, NOT REQUEST: the principal must come from the claims.
	if mock.lastPrincipal.UserID != testUserID || mock.lastPrincipal.Team != testTeam {
		t.Errorf("principal not sourced from token claims: %+v", mock.lastPrincipal)
	}

	// (a') domain→proto conversion of the response.
	fv := resp.GetFeatureView()
	if fv.GetId() != "fv-1" || fv.GetSchemaVersion() != 2 || fv.GetOwnerTeam() != testTeam {
		t.Errorf("response view fields wrong: %+v", fv)
	}
	if fv.GetDeletedAt() != nil {
		t.Errorf("live view must have nil deleted_at, got %v", fv.GetDeletedAt())
	}
	if fv.GetCreatedAt().AsTime() != time.Unix(1000, 0).UTC() {
		t.Errorf("created_at not mapped: %v", fv.GetCreatedAt().AsTime())
	}
}

func TestDefineFeatureView_Validation(t *testing.T) {
	// Service should NEVER be called for an invalid request — a nil fn would panic
	// if the handler wrongly reached the service, which is the assertion.
	mock := &mockService{}
	client := newClient(t, mock)
	ctx := ctxWithToken(context.Background(), validToken)

	cases := []struct {
		name string
		req  *featurestorev1.DefineFeatureViewRequest
	}{
		{"empty name", &featurestorev1.DefineFeatureViewRequest{Features: []*featurestorev1.FeatureSpec{{Name: "x", ValueType: featurestorev1.FeatureValueType_FEATURE_VALUE_TYPE_DOUBLE}}}},
		{"no features", &featurestorev1.DefineFeatureViewRequest{Name: "v"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.DefineFeatureView(ctx, tc.req)
			requireCode(t, err, codes.InvalidArgument)
		})
	}
}

func TestDefineFeatureView_DomainErrorMapping(t *testing.T) {
	cases := []struct {
		name     string
		domErr   error
		wantCode codes.Code
	}{
		{"validation", domain.ErrValidation, codes.InvalidArgument},
		{"name conflict", domain.ErrViewNameConflict, codes.FailedPrecondition},
		{"unknown leaks nothing", errors.New("pq: password authentication failed for user fp@10.0.0.5"), codes.Internal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockService{
				defineFn: func(_ context.Context, _ domain.Principal, _ domain.DefineFeatureViewInput) (domain.FeatureView, error) {
					return domain.FeatureView{}, tc.domErr
				},
			}
			client := newClient(t, mock)
			_, err := client.DefineFeatureView(ctxWithToken(context.Background(), validToken), &featurestorev1.DefineFeatureViewRequest{
				Name:     "v",
				Features: []*featurestorev1.FeatureSpec{{Name: "x", ValueType: featurestorev1.FeatureValueType_FEATURE_VALUE_TYPE_DOUBLE}},
			})
			requireCode(t, err, tc.wantCode)
			if tc.wantCode == codes.Internal {
				// (d) no internal detail leaks: the raw driver error must be sanitized.
				assertNoLeak(t, err, "password", "10.0.0.5", "pq:")
			}
		})
	}
}

// ============================================================================
// AUTH — the handler's identity guards (no token, no identity in token)
// ============================================================================

func TestDefineFeatureView_Unauthenticated(t *testing.T) {
	mock := &mockService{}
	client := newClient(t, mock)

	// No token at all → the auth interceptor rejects before the handler.
	_, err := client.DefineFeatureView(context.Background(), &featurestorev1.DefineFeatureViewRequest{
		Name: "v", Features: []*featurestorev1.FeatureSpec{{Name: "x", ValueType: featurestorev1.FeatureValueType_FEATURE_VALUE_TYPE_DOUBLE}},
	})
	requireCode(t, err, codes.Unauthenticated)
}

func TestDefineFeatureView_NoIdentityInToken(t *testing.T) {
	mock := &mockService{}
	client := newClient(t, mock)

	// Token authenticates but carries no team → the handler's principalFromContext
	// guard rejects it (the service is never reached).
	_, err := client.DefineFeatureView(ctxWithToken(context.Background(), noIdentityToken), &featurestorev1.DefineFeatureViewRequest{
		Name: "v", Features: []*featurestorev1.FeatureSpec{{Name: "x", ValueType: featurestorev1.FeatureValueType_FEATURE_VALUE_TYPE_DOUBLE}},
	})
	requireCode(t, err, codes.Unauthenticated)
}

// ============================================================================
// GetFeatureView (oneof handle dispatch)
// ============================================================================

func TestGetFeatureView_ByID(t *testing.T) {
	var gotID string
	mock := &mockService{
		getByIDFn: func(_ context.Context, _ domain.Principal, id string) (domain.FeatureView, error) {
			gotID = id
			return sampleView(), nil
		},
	}
	client := newClient(t, mock)
	resp, err := client.GetFeatureView(ctxWithToken(context.Background(), validToken), &featurestorev1.GetFeatureViewRequest{
		Handle: &featurestorev1.GetFeatureViewRequest_FeatureViewId{FeatureViewId: "fv-1"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotID != "fv-1" {
		t.Errorf("id not passed to GetFeatureViewByID: %q", gotID)
	}
	if resp.GetFeatureView().GetName() != "user_credit" {
		t.Errorf("wrong view returned: %+v", resp.GetFeatureView())
	}
}

func TestGetFeatureView_ByName(t *testing.T) {
	var gotName string
	mock := &mockService{
		getByNameFn: func(_ context.Context, _ domain.Principal, name string) (domain.FeatureView, error) {
			gotName = name
			return sampleView(), nil
		},
	}
	client := newClient(t, mock)
	_, err := client.GetFeatureView(ctxWithToken(context.Background(), validToken), &featurestorev1.GetFeatureViewRequest{
		Handle: &featurestorev1.GetFeatureViewRequest_Name{Name: "user_credit"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotName != "user_credit" {
		t.Errorf("name not passed to GetFeatureViewByName: %q", gotName)
	}
}

func TestGetFeatureView_NoHandle(t *testing.T) {
	mock := &mockService{}
	client := newClient(t, mock)
	_, err := client.GetFeatureView(ctxWithToken(context.Background(), validToken), &featurestorev1.GetFeatureViewRequest{})
	requireCode(t, err, codes.InvalidArgument)
}

func TestGetFeatureView_NotFound(t *testing.T) {
	mock := &mockService{
		getByIDFn: func(_ context.Context, _ domain.Principal, _ string) (domain.FeatureView, error) {
			return domain.FeatureView{}, domain.ErrViewNotFound
		},
	}
	client := newClient(t, mock)
	_, err := client.GetFeatureView(ctxWithToken(context.Background(), validToken), &featurestorev1.GetFeatureViewRequest{
		Handle: &featurestorev1.GetFeatureViewRequest_FeatureViewId{FeatureViewId: "missing"},
	})
	requireCode(t, err, codes.NotFound)
}

func TestGetFeatureView_DeletedViewMapsDeletedAt(t *testing.T) {
	deleted := sampleView()
	deleted.DeletedAt = time.Unix(3000, 0).UTC()
	mock := &mockService{
		getByIDFn: func(_ context.Context, _ domain.Principal, _ string) (domain.FeatureView, error) {
			return deleted, nil
		},
	}
	client := newClient(t, mock)
	resp, err := client.GetFeatureView(ctxWithToken(context.Background(), validToken), &featurestorev1.GetFeatureViewRequest{
		Handle: &featurestorev1.GetFeatureViewRequest_FeatureViewId{FeatureViewId: "fv-1"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetFeatureView().GetDeletedAt() == nil {
		t.Fatal("deleted view must carry non-nil deleted_at")
	}
	if resp.GetFeatureView().GetDeletedAt().AsTime() != time.Unix(3000, 0).UTC() {
		t.Errorf("deleted_at not mapped: %v", resp.GetFeatureView().GetDeletedAt().AsTime())
	}
}

// ============================================================================
// ListFeatureViews (pagination)
// ============================================================================

func TestListFeatureViews_HappyPathAndPagination(t *testing.T) {
	mock := &mockService{
		listFn: func(_ context.Context, _ domain.Principal, _ string, _ domain.ListOptions) ([]domain.FeatureView, string, error) {
			return []domain.FeatureView{sampleView()}, "next-cursor", nil
		},
	}
	client := newClient(t, mock)
	resp, err := client.ListFeatureViews(ctxWithToken(context.Background(), validToken), &featurestorev1.ListFeatureViewsRequest{
		NameFilter: "user",
		Pagination: &commonv1.PaginationRequest{PageSize: 50, PageToken: "cur"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mock.lastListFilter != "user" {
		t.Errorf("name filter not passed: %q", mock.lastListFilter)
	}
	if mock.lastListOpts.PageSize != 50 || mock.lastListOpts.PageToken != "cur" {
		t.Errorf("pagination not converted: %+v", mock.lastListOpts)
	}
	if len(resp.GetFeatureViews()) != 1 {
		t.Fatalf("want 1 view, got %d", len(resp.GetFeatureViews()))
	}
	if resp.GetPagination().GetNextPageToken() != "next-cursor" {
		t.Errorf("next token not mapped: %q", resp.GetPagination().GetNextPageToken())
	}
}

func TestListFeatureViews_NilPagination(t *testing.T) {
	mock := &mockService{
		listFn: func(_ context.Context, _ domain.Principal, _ string, opts domain.ListOptions) ([]domain.FeatureView, string, error) {
			// nil pagination → zero ListOptions (service applies its default).
			if opts.PageSize != 0 || opts.PageToken != "" {
				t.Errorf("nil pagination should yield zero opts, got %+v", opts)
			}
			return nil, "", nil
		},
	}
	client := newClient(t, mock)
	_, err := client.ListFeatureViews(ctxWithToken(context.Background(), validToken), &featurestorev1.ListFeatureViewsRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ============================================================================
// WriteFeatures
// ============================================================================

// writeSchemaView is the view the WriteFeatures tests fetch for value
// disambiguation. It declares every feature kind the write tests exercise —
// crucially INT64 ("tier") and TIMESTAMP ("seen_at"), the two lossy-wire types
// that the schema-driven conversion must resolve (a bare number / RFC3339 string
// would otherwise be mistyped as DOUBLE / STRING and rejected by the domain's
// strict Kind check).
func writeSchemaView() domain.FeatureView {
	return domain.FeatureView{
		ID:        "fv-1",
		Name:      "user_credit",
		Entity:    domain.Entity{Name: "user", JoinKey: "user_id"},
		OwnerTeam: testTeam,
		Features: []domain.FeatureSpec{
			{Name: "score", ValueType: domain.FeatureTypeDouble},
			{Name: "flag", ValueType: domain.FeatureTypeBool},
			{Name: "embedding", ValueType: domain.FeatureTypeDoubleList, Dimension: 2},
			{Name: "tier", ValueType: domain.FeatureTypeInt64},
			{Name: "seen_at", ValueType: domain.FeatureTypeTimestamp},
		},
		SchemaVersion: 1,
	}
}

func TestWriteFeatures_HappyPath(t *testing.T) {
	mock := &mockService{
		getByIDFn: func(_ context.Context, _ domain.Principal, _ string) (domain.FeatureView, error) {
			return writeSchemaView(), nil
		},
		writeFn: func(_ context.Context, _ domain.Principal, _ domain.WriteFeaturesInput) (domain.WriteFeaturesResult, error) {
			return domain.WriteFeaturesResult{WrittenCount: 2, WrittenThroughVersion: 42}, nil
		},
	}
	client := newClient(t, mock)

	eventTime := time.Unix(1500, 0).UTC()
	resp, err := client.WriteFeatures(ctxWithToken(context.Background(), validToken), &featurestorev1.WriteFeaturesRequest{
		FeatureViewId: "fv-1",
		Features: []*featurestorev1.FeatureValues{
			{
				EntityId: "u_1",
				Values: map[string]*structpb.Value{
					"score":     structpb.NewNumberValue(0.9),
					"flag":      structpb.NewBoolValue(true),
					"embedding": structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{structpb.NewNumberValue(1), structpb.NewNumberValue(2)}}),
				},
				EventTime: timestamppb.New(eventTime),
			},
		},
		IdempotencyKey: "w-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// (a) proto→domain row conversion.
	if mock.lastWriteIn.FeatureViewID != "fv-1" || mock.lastWriteIn.IdempotencyKey != "w-1" {
		t.Errorf("write input fields wrong: %+v", mock.lastWriteIn)
	}
	if len(mock.lastWriteIn.Rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(mock.lastWriteIn.Rows))
	}
	row := mock.lastWriteIn.Rows[0]
	if row.EntityID != "u_1" || !row.EventTime.Equal(eventTime) {
		t.Errorf("row scalar fields wrong: %+v", row)
	}
	if v := row.Values["score"]; v.Kind != domain.FeatureTypeDouble || v.Double != 0.9 {
		t.Errorf("score value wrong: %+v", v)
	}
	if v := row.Values["flag"]; v.Kind != domain.FeatureTypeBool || !v.Bool {
		t.Errorf("flag value wrong: %+v", v)
	}
	if v := row.Values["embedding"]; v.Kind != domain.FeatureTypeDoubleList || len(v.List) != 2 || v.List[1] != 2 {
		t.Errorf("embedding value wrong: %+v", v)
	}

	// (a') response conversion.
	if resp.GetWrittenCount() != 2 || resp.GetWrittenThroughVersion() != 42 {
		t.Errorf("response wrong: %+v", resp)
	}
}

// TestWriteFeatures_Int64AndTimestamp is the REGRESSION GUARD for the critical bug
// where INT64 and TIMESTAMP features were unwritable end-to-end. A wire number for
// an INT64 feature and an RFC3339 wire string for a TIMESTAMP feature both arrive
// as ambiguous kinds; the schema-driven handler must convert them to the correct
// domain Kind (Int64 / Timestamp) so the value the service receives — and which the
// domain's strict validateRow then accepts — is exactly typed. Before the fix the
// handler emitted Kind=Double / Kind=String for these, which the domain rejected
// with ErrSchemaViolation (InvalidArgument). The earlier happy-path test used only
// Double/Bool/DoubleList, which is precisely why the bug was invisible.
func TestWriteFeatures_Int64AndTimestamp(t *testing.T) {
	mock := &mockService{
		getByIDFn: func(_ context.Context, _ domain.Principal, _ string) (domain.FeatureView, error) {
			return writeSchemaView(), nil
		},
		writeFn: func(_ context.Context, _ domain.Principal, _ domain.WriteFeaturesInput) (domain.WriteFeaturesResult, error) {
			return domain.WriteFeaturesResult{WrittenCount: 1, WrittenThroughVersion: 1}, nil
		},
	}
	client := newClient(t, mock)

	seenAt := time.Unix(1700000000, 0).UTC()
	_, err := client.WriteFeatures(ctxWithToken(context.Background(), validToken), &featurestorev1.WriteFeaturesRequest{
		FeatureViewId: "fv-1",
		Features: []*featurestorev1.FeatureValues{
			{
				EntityId: "u_1",
				Values: map[string]*structpb.Value{
					// INT64 arrives as a wire number; TIMESTAMP as an RFC3339 wire string.
					"tier":    structpb.NewNumberValue(7),
					"seen_at": structpb.NewStringValue(seenAt.Format(time.RFC3339Nano)),
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error writing INT64/TIMESTAMP: %v", err)
	}

	if len(mock.lastWriteIn.Rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(mock.lastWriteIn.Rows))
	}
	row := mock.lastWriteIn.Rows[0]

	// INT64: the handler must produce Kind=Int64 with the exact integer in Int — NOT
	// Kind=Double. This is the half of the bug that made INT64 features unwritable.
	tier := row.Values["tier"]
	if tier.Kind != domain.FeatureTypeInt64 {
		t.Errorf("tier Kind = %v, want INT64 (the schema-driven narrowing)", tier.Kind)
	}
	if tier.Int != 7 {
		t.Errorf("tier Int = %d, want 7", tier.Int)
	}

	// TIMESTAMP: the handler must parse the RFC3339 string into Time with Kind=
	// Timestamp — NOT Kind=String. This is the other half of the bug.
	seen := row.Values["seen_at"]
	if seen.Kind != domain.FeatureTypeTimestamp {
		t.Errorf("seen_at Kind = %v, want TIMESTAMP", seen.Kind)
	}
	if !seen.Time.Equal(seenAt) {
		t.Errorf("seen_at Time = %v, want %v", seen.Time, seenAt)
	}
}

// TestWriteFeatures_Int64NonIntegralRejected proves the INT64 narrowing rejects a
// non-integral wire number rather than silently truncating it (1.5 → 1 would be a
// silent data-corruption bug). The conversion failure is a boundary InvalidArgument
// and the service is never reached (writeFn is nil — a call would panic).
func TestWriteFeatures_Int64NonIntegralRejected(t *testing.T) {
	mock := &mockService{
		getByIDFn: func(_ context.Context, _ domain.Principal, _ string) (domain.FeatureView, error) {
			return writeSchemaView(), nil
		},
	}
	client := newClient(t, mock)
	_, err := client.WriteFeatures(ctxWithToken(context.Background(), validToken), &featurestorev1.WriteFeaturesRequest{
		FeatureViewId: "fv-1",
		Features: []*featurestorev1.FeatureValues{
			{EntityId: "u_1", Values: map[string]*structpb.Value{"tier": structpb.NewNumberValue(1.5)}},
		},
	})
	requireCode(t, err, codes.InvalidArgument)
}

// TestWriteFeatures_TimestampBadFormatRejected proves a TIMESTAMP feature with a
// non-RFC3339 string is rejected at the boundary (InvalidArgument), not forwarded.
func TestWriteFeatures_TimestampBadFormatRejected(t *testing.T) {
	mock := &mockService{
		getByIDFn: func(_ context.Context, _ domain.Principal, _ string) (domain.FeatureView, error) {
			return writeSchemaView(), nil
		},
	}
	client := newClient(t, mock)
	_, err := client.WriteFeatures(ctxWithToken(context.Background(), validToken), &featurestorev1.WriteFeaturesRequest{
		FeatureViewId: "fv-1",
		Features: []*featurestorev1.FeatureValues{
			{EntityId: "u_1", Values: map[string]*structpb.Value{"seen_at": structpb.NewStringValue("not-a-timestamp")}},
		},
	})
	requireCode(t, err, codes.InvalidArgument)
}

// TestWriteFeatures_SchemaFetchError proves the schema fetch's error is mapped (not
// swallowed): a NotFound from GetFeatureViewByID surfaces as NotFound, and the
// write is never attempted (writeFn nil would panic if reached).
func TestWriteFeatures_SchemaFetchError(t *testing.T) {
	mock := &mockService{
		getByIDFn: func(_ context.Context, _ domain.Principal, _ string) (domain.FeatureView, error) {
			return domain.FeatureView{}, domain.ErrViewNotFound
		},
	}
	client := newClient(t, mock)
	_, err := client.WriteFeatures(ctxWithToken(context.Background(), validToken), &featurestorev1.WriteFeaturesRequest{
		FeatureViewId: "missing",
		Features:      []*featurestorev1.FeatureValues{{EntityId: "u", Values: map[string]*structpb.Value{"score": structpb.NewNumberValue(1)}}},
	})
	requireCode(t, err, codes.NotFound)
}

func TestWriteFeatures_Validation(t *testing.T) {
	// A getByIDFn is provided because the schema-driven write path fetches the view
	// before converting rows. The id-present/non-empty/over-cap cases short-circuit
	// BEFORE the fetch; the entity-id and null-value cases reach conversion after it.
	mock := &mockService{
		getByIDFn: func(_ context.Context, _ domain.Principal, _ string) (domain.FeatureView, error) {
			return writeSchemaView(), nil
		},
	}
	client := newClient(t, mock)
	ctx := ctxWithToken(context.Background(), validToken)

	// over-cap batch — build MaxBatchSize+1 rows.
	tooMany := make([]*featurestorev1.FeatureValues, domain.MaxBatchSize+1)
	for i := range tooMany {
		tooMany[i] = &featurestorev1.FeatureValues{EntityId: "u"}
	}

	cases := []struct {
		name string
		req  *featurestorev1.WriteFeaturesRequest
	}{
		{"no view id", &featurestorev1.WriteFeaturesRequest{Features: []*featurestorev1.FeatureValues{{EntityId: "u"}}}},
		{"no rows", &featurestorev1.WriteFeaturesRequest{FeatureViewId: "fv-1"}},
		{"over cap", &featurestorev1.WriteFeaturesRequest{FeatureViewId: "fv-1", Features: tooMany}},
		{"empty entity id", &featurestorev1.WriteFeaturesRequest{FeatureViewId: "fv-1", Features: []*featurestorev1.FeatureValues{{EntityId: ""}}}},
		{"null value", &featurestorev1.WriteFeaturesRequest{FeatureViewId: "fv-1", Features: []*featurestorev1.FeatureValues{{EntityId: "u", Values: map[string]*structpb.Value{"x": structpb.NewNullValue()}}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.WriteFeatures(ctx, tc.req)
			requireCode(t, err, codes.InvalidArgument)
		})
	}
}

func TestWriteFeatures_DomainErrorMapping(t *testing.T) {
	cases := []struct {
		name     string
		domErr   error
		wantCode codes.Code
	}{
		{"schema violation", domain.ErrSchemaViolation, codes.InvalidArgument},
		{"deleted view", domain.ErrViewDeleted, codes.FailedPrecondition},
		{"not found", domain.ErrViewNotFound, codes.NotFound},
		{"batch too large", domain.ErrBatchTooLarge, codes.InvalidArgument},
		{"unknown sanitized", errors.New("redis: dial tcp 10.1.2.3:6379: connection refused"), codes.Internal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockService{
				// The schema fetch precedes the write; return a valid view so the
				// programmed writeFn error is the one under test.
				getByIDFn: func(_ context.Context, _ domain.Principal, _ string) (domain.FeatureView, error) {
					return writeSchemaView(), nil
				},
				writeFn: func(_ context.Context, _ domain.Principal, _ domain.WriteFeaturesInput) (domain.WriteFeaturesResult, error) {
					return domain.WriteFeaturesResult{}, tc.domErr
				},
			}
			client := newClient(t, mock)
			_, err := client.WriteFeatures(ctxWithToken(context.Background(), validToken), &featurestorev1.WriteFeaturesRequest{
				FeatureViewId: "fv-1",
				Features:      []*featurestorev1.FeatureValues{{EntityId: "u", Values: map[string]*structpb.Value{"score": structpb.NewNumberValue(1)}}},
			})
			requireCode(t, err, tc.wantCode)
			if tc.wantCode == codes.Internal {
				assertNoLeak(t, err, "redis", "10.1.2.3", "connection refused")
			}
		})
	}
}

// ============================================================================
// GetOnlineFeatures
// ============================================================================

func TestGetOnlineFeatures_HappyPath(t *testing.T) {
	mock := &mockService{
		onlineFn: func(_ context.Context, _ domain.Principal, _ domain.GetOnlineFeaturesInput) (domain.FeatureVectorPage, error) {
			return domain.FeatureVectorPage{
				Vectors: []domain.FeatureVector{{
					EntityID:      "u_1",
					Values:        map[string]domain.FeatureValue{"score": {Kind: domain.FeatureTypeDouble, Double: 0.5}, "tier": {Kind: domain.FeatureTypeInt64, Int: 7}},
					AsOfVersion:   42,
					EventTime:     time.Unix(1500, 0).UTC(),
					SchemaVersion: 2,
				}},
				MissingEntityIDs: []string{"u_2"},
			}, nil
		},
	}
	client := newClient(t, mock)
	resp, err := client.GetOnlineFeatures(ctxWithToken(context.Background(), validToken), &featurestorev1.GetOnlineFeaturesRequest{
		FeatureViewId: "fv-1",
		EntityIds:     []string{"u_1", "u_2"},
		FeatureNames:  []string{"score", "tier"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mock.lastOnlineIn.FeatureViewID != "fv-1" || len(mock.lastOnlineIn.EntityIDs) != 2 || len(mock.lastOnlineIn.FeatureNames) != 2 {
		t.Errorf("online input not converted: %+v", mock.lastOnlineIn)
	}
	if len(resp.GetVectors()) != 1 {
		t.Fatalf("want 1 vector, got %d", len(resp.GetVectors()))
	}
	v := resp.GetVectors()[0]
	if v.GetEntityId() != "u_1" || v.GetAsOfVersion() != 42 || v.GetSchemaVersion() != 2 {
		t.Errorf("vector provenance wrong: %+v", v)
	}
	// INT64 value must round-trip back as a number on the wire.
	if v.GetValues()["tier"].GetNumberValue() != 7 {
		t.Errorf("int64 value not mapped to number: %v", v.GetValues()["tier"])
	}
	if len(resp.GetMissingEntityIds()) != 1 || resp.GetMissingEntityIds()[0] != "u_2" {
		t.Errorf("missing ids not mapped: %v", resp.GetMissingEntityIds())
	}
}

func TestGetOnlineFeatures_Validation(t *testing.T) {
	mock := &mockService{}
	client := newClient(t, mock)
	ctx := ctxWithToken(context.Background(), validToken)

	tooMany := make([]string, domain.MaxBatchSize+1)
	cases := []struct {
		name string
		req  *featurestorev1.GetOnlineFeaturesRequest
	}{
		{"no view id", &featurestorev1.GetOnlineFeaturesRequest{EntityIds: []string{"u"}}},
		{"no entities", &featurestorev1.GetOnlineFeaturesRequest{FeatureViewId: "fv-1"}},
		{"over cap", &featurestorev1.GetOnlineFeaturesRequest{FeatureViewId: "fv-1", EntityIds: tooMany}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.GetOnlineFeatures(ctx, tc.req)
			requireCode(t, err, codes.InvalidArgument)
		})
	}
}

// ============================================================================
// GetHistoricalFeatures
// ============================================================================

func TestGetHistoricalFeatures_HappyPath(t *testing.T) {
	asOf := time.Unix(5000, 0).UTC()
	mock := &mockService{
		historicalFn: func(_ context.Context, _ domain.Principal, _ domain.GetHistoricalFeaturesInput) (domain.FeatureVectorPage, error) {
			return domain.FeatureVectorPage{
				Vectors:          []domain.FeatureVector{{EntityID: "u_1", Values: map[string]domain.FeatureValue{"score": {Kind: domain.FeatureTypeDouble, Double: 1.0}}, AsOfVersion: 9}},
				MissingEntityIDs: []string{"u_9"},
				NextPageToken:    "next",
			}, nil
		},
	}
	client := newClient(t, mock)
	resp, err := client.GetHistoricalFeatures(ctxWithToken(context.Background(), validToken), &featurestorev1.GetHistoricalFeaturesRequest{
		FeatureViewId: "fv-1",
		EntityIds:     []string{"u_1", "u_9"},
		AsOf:          timestamppb.New(asOf),
		Pagination:    &commonv1.PaginationRequest{PageSize: 10},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mock.lastHistIn.AsOf.Equal(asOf) {
		t.Errorf("as_of not converted: got %v want %v", mock.lastHistIn.AsOf, asOf)
	}
	if mock.lastHistIn.Pagination.PageSize != 10 {
		t.Errorf("pagination not passed: %+v", mock.lastHistIn.Pagination)
	}
	if resp.GetPagination().GetNextPageToken() != "next" {
		t.Errorf("next token not mapped: %q", resp.GetPagination().GetNextPageToken())
	}
	if len(resp.GetMissingEntityIds()) != 1 {
		t.Errorf("missing ids not mapped: %v", resp.GetMissingEntityIds())
	}
}

func TestGetHistoricalFeatures_Validation(t *testing.T) {
	mock := &mockService{}
	client := newClient(t, mock)
	ctx := ctxWithToken(context.Background(), validToken)

	cases := []struct {
		name string
		req  *featurestorev1.GetHistoricalFeaturesRequest
	}{
		{"no view id", &featurestorev1.GetHistoricalFeaturesRequest{EntityIds: []string{"u"}, AsOf: timestamppb.Now()}},
		{"no entities", &featurestorev1.GetHistoricalFeaturesRequest{FeatureViewId: "fv-1", AsOf: timestamppb.Now()}},
		{"missing as_of", &featurestorev1.GetHistoricalFeaturesRequest{FeatureViewId: "fv-1", EntityIds: []string{"u"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.GetHistoricalFeatures(ctx, tc.req)
			requireCode(t, err, codes.InvalidArgument)
		})
	}
}

func TestGetHistoricalFeatures_AsOfRequiredFromDomain(t *testing.T) {
	// Even if the boundary check were bypassed, the domain's ErrAsOfRequired must
	// map to InvalidArgument. We program the service to return it.
	mock := &mockService{
		historicalFn: func(_ context.Context, _ domain.Principal, _ domain.GetHistoricalFeaturesInput) (domain.FeatureVectorPage, error) {
			return domain.FeatureVectorPage{}, domain.ErrAsOfRequired
		},
	}
	client := newClient(t, mock)
	_, err := client.GetHistoricalFeatures(ctxWithToken(context.Background(), validToken), &featurestorev1.GetHistoricalFeaturesRequest{
		FeatureViewId: "fv-1",
		EntityIds:     []string{"u"},
		AsOf:          timestamppb.New(time.Unix(1, 0)), // pass boundary; domain still rejects
	})
	requireCode(t, err, codes.InvalidArgument)
}

// ============================================================================
// DeleteFeatureView
// ============================================================================

func TestDeleteFeatureView_HappyPath(t *testing.T) {
	deleted := sampleView()
	deleted.DeletedAt = time.Unix(9000, 0).UTC()
	mock := &mockService{
		deleteFn: func(_ context.Context, _ domain.Principal, _, _ string) (domain.FeatureView, error) {
			return deleted, nil
		},
	}
	client := newClient(t, mock)
	resp, err := client.DeleteFeatureView(ctxWithToken(context.Background(), validToken), &featurestorev1.DeleteFeatureViewRequest{
		FeatureViewId:  "fv-1",
		IdempotencyKey: "d-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mock.lastDeleteID != "fv-1" || mock.lastDeleteIdem != "d-1" {
		t.Errorf("delete args not passed: id=%q idem=%q", mock.lastDeleteID, mock.lastDeleteIdem)
	}
	if resp.GetFeatureView().GetDeletedAt() == nil {
		t.Error("deleted view response must carry deleted_at")
	}
}

func TestDeleteFeatureView_Validation(t *testing.T) {
	mock := &mockService{}
	client := newClient(t, mock)
	_, err := client.DeleteFeatureView(ctxWithToken(context.Background(), validToken), &featurestorev1.DeleteFeatureViewRequest{})
	requireCode(t, err, codes.InvalidArgument)
}

func TestDeleteFeatureView_DomainErrorMapping(t *testing.T) {
	cases := []struct {
		name     string
		domErr   error
		wantCode codes.Code
	}{
		{"not found", domain.ErrViewNotFound, codes.NotFound},
		{"unknown sanitized", errors.New("sql: transaction deadlock on table feature_events"), codes.Internal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockService{
				deleteFn: func(_ context.Context, _ domain.Principal, _, _ string) (domain.FeatureView, error) {
					return domain.FeatureView{}, tc.domErr
				},
			}
			client := newClient(t, mock)
			_, err := client.DeleteFeatureView(ctxWithToken(context.Background(), validToken), &featurestorev1.DeleteFeatureViewRequest{FeatureViewId: "fv-1"})
			requireCode(t, err, tc.wantCode)
			if tc.wantCode == codes.Internal {
				assertNoLeak(t, err, "deadlock", "feature_events", "sql:")
			}
		})
	}
}

// ============================================================================
// RebuildViews (SERVER-STREAMING)
// ============================================================================

func TestRebuildViews_StreamsProgressFrames(t *testing.T) {
	mock := &mockService{
		rebuildFn: func(_ context.Context, _ domain.Principal, _ domain.RebuildViewsInput, emit func(domain.RebuildProgress) error) error {
			// Push two frames then a final done frame — proving each domain frame
			// becomes a stream Send in order.
			if err := emit(domain.RebuildProgress{EventsReplayed: 5, TotalEvents: 10, CurrentFeatureViewID: "fv-1"}); err != nil {
				return err
			}
			if err := emit(domain.RebuildProgress{EventsReplayed: 10, TotalEvents: 10, CurrentFeatureViewID: "fv-1", Done: true}); err != nil {
				return err
			}
			return nil
		},
	}
	client := newClient(t, mock)
	stream, err := client.RebuildViews(ctxWithToken(context.Background(), validToken), &featurestorev1.RebuildViewsRequest{
		Target: featurestorev1.RebuildTarget_REBUILD_TARGET_ALL,
	})
	if err != nil {
		t.Fatalf("unexpected error opening stream: %v", err)
	}

	var frames []*featurestorev1.RebuildViewsResponse
	for {
		f, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("recv error: %v", err)
		}
		frames = append(frames, f)
	}
	if len(frames) != 2 {
		t.Fatalf("want 2 frames, got %d", len(frames))
	}
	if frames[0].GetEventsReplayed() != 5 || frames[0].GetDone() {
		t.Errorf("first frame wrong: %+v", frames[0])
	}
	if !frames[1].GetDone() || frames[1].GetEventsReplayed() != 10 {
		t.Errorf("final frame must be done: %+v", frames[1])
	}
	// Target ALL must have been converted.
	if mock.lastRebuildIn.Target != domain.RebuildTargetAll {
		t.Errorf("rebuild target not converted: %v", mock.lastRebuildIn.Target)
	}
}

func TestRebuildViews_TargetConversion(t *testing.T) {
	cases := []struct {
		proto featurestorev1.RebuildTarget
		want  domain.RebuildTarget
	}{
		{featurestorev1.RebuildTarget_REBUILD_TARGET_UNSPECIFIED, domain.RebuildTargetAll},
		{featurestorev1.RebuildTarget_REBUILD_TARGET_ALL, domain.RebuildTargetAll},
		{featurestorev1.RebuildTarget_REBUILD_TARGET_ONLINE, domain.RebuildTargetOnline},
		{featurestorev1.RebuildTarget_REBUILD_TARGET_OFFLINE, domain.RebuildTargetOffline},
	}
	for _, tc := range cases {
		mock := &mockService{
			rebuildFn: func(_ context.Context, _ domain.Principal, _ domain.RebuildViewsInput, _ func(domain.RebuildProgress) error) error {
				return nil
			},
		}
		client := newClient(t, mock)
		stream, err := client.RebuildViews(ctxWithToken(context.Background(), validToken), &featurestorev1.RebuildViewsRequest{Target: tc.proto})
		if err != nil {
			t.Fatalf("open stream: %v", err)
		}
		// Drain to completion so the server runs and records the input.
		for {
			if _, err := stream.Recv(); err != nil {
				break
			}
		}
		if mock.lastRebuildIn.Target != tc.want {
			t.Errorf("target %v converted to %v, want %v", tc.proto, mock.lastRebuildIn.Target, tc.want)
		}
	}
}

func TestRebuildViews_DomainErrorMapping(t *testing.T) {
	mock := &mockService{
		rebuildFn: func(_ context.Context, _ domain.Principal, _ domain.RebuildViewsInput, _ func(domain.RebuildProgress) error) error {
			return domain.ErrViewNotFound
		},
	}
	client := newClient(t, mock)
	stream, err := client.RebuildViews(ctxWithToken(context.Background(), validToken), &featurestorev1.RebuildViewsRequest{FeatureViewId: "missing"})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	_, err = stream.Recv()
	requireCode(t, err, codes.NotFound)
}

func TestRebuildViews_UnknownErrorSanitized(t *testing.T) {
	mock := &mockService{
		rebuildFn: func(_ context.Context, _ domain.Principal, _ domain.RebuildViewsInput, _ func(domain.RebuildProgress) error) error {
			return errors.New("postgres: could not connect to host db-internal-7.prod:5432")
		},
	}
	client := newClient(t, mock)
	stream, err := client.RebuildViews(ctxWithToken(context.Background(), validToken), &featurestorev1.RebuildViewsRequest{})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	_, err = stream.Recv()
	requireCode(t, err, codes.Internal)
	assertNoLeak(t, err, "postgres", "db-internal-7", "5432")
}

func TestRebuildViews_Unauthenticated(t *testing.T) {
	mock := &mockService{}
	client := newClient(t, mock)
	stream, err := client.RebuildViews(context.Background(), &featurestorev1.RebuildViewsRequest{})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	_, err = stream.Recv()
	requireCode(t, err, codes.Unauthenticated)
}

// TestRebuildViews_UnderScopedPermissionDenied is the REGRESSION GUARD for the
// mis-mapped authorization code. An AUTHENTICATED caller WITHOUT features:admin
// (writerToken) must get codes.PermissionDenied — not InvalidArgument (the wrong
// code the domain's ErrValidation produced) and not Unauthenticated (that is the
// no-/bad-credentials case). The service must never be reached (rebuildFn is nil —
// a call would panic), proving the gate is at the handler boundary, before svc.
func TestRebuildViews_UnderScopedPermissionDenied(t *testing.T) {
	mock := &mockService{}
	client := newClient(t, mock)
	stream, err := client.RebuildViews(ctxWithToken(context.Background(), writerToken), &featurestorev1.RebuildViewsRequest{
		Target: featurestorev1.RebuildTarget_REBUILD_TARGET_ALL,
	})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	_, err = stream.Recv()
	requireCode(t, err, codes.PermissionDenied)
}

// TestRebuildViews_AdminScopeAllowed is the positive counterpart: a caller WITH
// features:admin (validToken) passes the gate and reaches the service.
func TestRebuildViews_AdminScopeAllowed(t *testing.T) {
	called := false
	mock := &mockService{
		rebuildFn: func(_ context.Context, _ domain.Principal, _ domain.RebuildViewsInput, _ func(domain.RebuildProgress) error) error {
			called = true
			return nil
		},
	}
	client := newClient(t, mock)
	stream, err := client.RebuildViews(ctxWithToken(context.Background(), validToken), &featurestorev1.RebuildViewsRequest{})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	for {
		if _, err := stream.Recv(); err != nil {
			break
		}
	}
	if !called {
		t.Error("admin-scoped caller should reach the service, but it was not called")
	}
}

// ============================================================================
// mapDomainError — direct unit test of the translation table
// ============================================================================
//
// The permission-denied case has no domain sentinel (the domain lacks one); it is a
// HANDLER sentinel (errPermissionDenied). This pins that a permission error reaching
// the mapper translates to codes.PermissionDenied, completing the table the
// RebuildViews gate relies on. The other rows guard against regressions in the
// existing sentinel→code mapping.
func TestMapDomainError_Table(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"nil", nil, codes.OK},
		{"validation", domain.ErrValidation, codes.InvalidArgument},
		{"schema violation", domain.ErrSchemaViolation, codes.InvalidArgument},
		{"as_of required", domain.ErrAsOfRequired, codes.InvalidArgument},
		{"batch too large", domain.ErrBatchTooLarge, codes.InvalidArgument},
		{"permission denied", errPermissionDenied, codes.PermissionDenied},
		{"permission denied wrapped", errors.Join(errPermissionDenied, errors.New("ctx")), codes.PermissionDenied},
		{"not found", domain.ErrViewNotFound, codes.NotFound},
		{"deleted", domain.ErrViewDeleted, codes.FailedPrecondition},
		{"name conflict", domain.ErrViewNameConflict, codes.FailedPrecondition},
		{"unknown", errors.New("pq: secret leak"), codes.Internal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := status.Code(mapDomainError(tc.err))
			if got != tc.want {
				t.Errorf("mapDomainError(%v) code = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// ============================================================================
// NIL-SERVICE GUARD — a not-yet-wired binary must return Unimplemented, not panic
// ============================================================================
//
// We call the handler methods DIRECTLY (no gRPC), with a context carrying claims,
// to prove the nil-svc guard fires before any service access. The streaming guard
// is exercised via a minimal fake stream (it returns only an error).
func TestNilService_UnaryGuards(t *testing.T) {
	h := NewFeatureStoreHandler(nil) // nil service — the scaffold/early-boot state.
	ctx := grpcutilClaimsCtx()

	if _, err := h.DefineFeatureView(ctx, &featurestorev1.DefineFeatureViewRequest{Name: "v", Features: []*featurestorev1.FeatureSpec{{Name: "x", ValueType: featurestorev1.FeatureValueType_FEATURE_VALUE_TYPE_DOUBLE}}}); status.Code(err) != codes.Unimplemented {
		t.Errorf("DefineFeatureView nil guard: got %v", status.Code(err))
	}
	if _, err := h.GetFeatureView(ctx, &featurestorev1.GetFeatureViewRequest{Handle: &featurestorev1.GetFeatureViewRequest_FeatureViewId{FeatureViewId: "x"}}); status.Code(err) != codes.Unimplemented {
		t.Errorf("GetFeatureView nil guard: got %v", status.Code(err))
	}
	if _, err := h.WriteFeatures(ctx, &featurestorev1.WriteFeaturesRequest{FeatureViewId: "x", Features: []*featurestorev1.FeatureValues{{EntityId: "u"}}}); status.Code(err) != codes.Unimplemented {
		t.Errorf("WriteFeatures nil guard: got %v", status.Code(err))
	}
}

func TestNilService_StreamingGuard(t *testing.T) {
	h := NewFeatureStoreHandler(nil)
	err := h.RebuildViews(&featurestorev1.RebuildViewsRequest{}, fakeStream{ctx: grpcutilClaimsCtx()})
	if status.Code(err) != codes.Unimplemented {
		t.Errorf("RebuildViews nil guard: got %v, want Unimplemented", status.Code(err))
	}
}

// grpcutilClaimsCtx returns a context that already carries valid claims, used by
// the direct (non-gRPC) handler calls. We build it by running the request through
// the real auth interceptor would be heavier; instead we rely on the fact that the
// nil-svc guard runs BEFORE principalFromContext, so claims presence is irrelevant
// for those tests — a bare context suffices and proves the guard ordering.
func grpcutilClaimsCtx() context.Context { return context.Background() }

// fakeStream is a minimal grpc.ServerStreamingServer[RebuildViewsResponse] for the
// direct streaming-guard test. Only Context() and Send() are meaningfully used;
// the rest satisfy the embedded grpc.ServerStream interface.
type fakeStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f fakeStream) Context() context.Context                        { return f.ctx }
func (f fakeStream) Send(*featurestorev1.RebuildViewsResponse) error { return nil }
func (f fakeStream) SetHeader(metadata.MD) error                     { return nil }
func (f fakeStream) SendHeader(metadata.MD) error                    { return nil }
func (f fakeStream) SetTrailer(metadata.MD)                          {}

// ============================================================================
// ASSERTION HELPERS
// ============================================================================

// requireCode asserts the gRPC status code of err.
func requireCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("status code = %v, want %v (err=%v)", got, want, err)
	}
}

// assertNoLeak asserts none of the forbidden substrings appear in the error
// message returned TO THE CLIENT — the core sanitization guarantee. If a Postgres
// DSN, Redis host, SQL fragment, or table name reaches the client, this fails.
func assertNoLeak(t *testing.T, err error, forbidden ...string) {
	t.Helper()
	msg := status.Convert(err).Message()
	for _, f := range forbidden {
		if containsFold(msg, f) {
			t.Errorf("client error message leaked %q: %q", f, msg)
		}
	}
}

// containsFold is a tiny case-insensitive substring check (avoids importing
// strings just for this; keeps the leak assertion self-contained and obvious).
func containsFold(haystack, needle string) bool {
	h, n := toLower(haystack), toLower(needle)
	if len(n) == 0 {
		return true
	}
	for i := 0; i+len(n) <= len(h); i++ {
		if h[i:i+len(n)] == n {
			return true
		}
	}
	return false
}

func toLower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}
