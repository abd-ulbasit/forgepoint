// inference_handler_test.go — COMPONENT tests for the Inference Gateway handler.
//
// ============================================================================
// WHAT THESE TESTS ISOLATE (and why this is "component", not "unit" or "integ")
// ============================================================================
//
// We test the HANDLER end-to-end over a REAL in-process gRPC stack (bufconn) with
// the REAL auth interceptor chain — but with a hand-written MOCK of the domain
// InferenceService. That boundary is deliberate:
//
//   - REAL gRPC + REAL auth interceptor: we exercise proto (de)serialization,
//     metadata→claims injection, and the streaming ServerStream wrapper exactly
//     as production does. A mock gRPC client would miss interceptor/serialization
//     bugs. (bufconn = in-memory transport, ~100x faster than TCP, no ports.)
//
//   - MOCK the SERVICE, not the repos: the handler's job is conversion +
//     validation + authz + error mapping. Mocking the service (one seam) lets us
//     drive any domain result/sentinel deterministically and assert the handler's
//     three jobs in isolation — no Redis, no backend, no NATS, no breaker timing.
//     Mocking the repos instead would also pull in the real domain logic, turning
//     these into domain tests (which already exist and pass -race).
//
//   - NO testcontainers: this phase is container-free; the only "infra" is the
//     bufconn listener.
//
// AUTH IN TESTS: we install the genuine grpcutil.AuthUnary/StreamInterceptor with
// a STUB TokenValidator that decodes the bearer token as a JSON grpcutil.Claims.
// So a test "logs in" by attaching a token carrying the exact UserID/Team/Scopes
// it wants — and the handler reads identity/scope through the SAME path it does in
// prod (ClaimsFromContext). This is what lets us assert "identity comes from the
// verified claims, never a request field" and the per-RPC scope gates for real.
// ============================================================================
package handler_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	inferencev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/inference/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/inference-gateway/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/inference-gateway/internal/handler"
)

// Scopes mirrored from the handler's (unexported) constants — kept in sync here
// so the tests document the exact capability strings the gates check.
const (
	scopePredict  = "inference:predict"
	scopeOverride = "inference:override"
	scopeAdmin    = "inference:admin"
)

// ============================================================================
// MOCK DOMAIN SERVICE — the single seam the handler depends on
// ============================================================================

// mockInferenceService implements domain.InferenceService with per-method func
// fields. A test sets only the funcs it needs; an unset func panics if called
// (forcing tests to be explicit about what the handler should invoke). This is a
// hand-written mock (no codegen) per the task: it mocks the SERVICE, isolating
// the handler from all infrastructure.
type mockInferenceService struct {
	predictFn         func(ctx context.Context, p domain.Principal, in domain.PredictInput) (domain.PredictOutput, error)
	getRouteFn        func(ctx context.Context, team, modelName string) (domain.Route, error)
	listRoutesFn      func(ctx context.Context, team string, opts domain.ListOptions) ([]domain.Route, string, error)
	upsertRouteFn     func(ctx context.Context, team, modelName string, proposed []domain.ProposedTarget) (domain.Route, error)
	setTrafficSplitFn func(ctx context.Context, team, modelName string, weights []domain.TrafficWeight) (domain.Route, error)
	deleteRouteFn     func(ctx context.Context, team, modelName string) error
	circuitStatesFn   func(modelNameFilter string) []domain.CircuitSnapshot

	// last* capture what the handler passed the service, so a test can assert the
	// proto→domain conversion AND the server-authoritative identity sourcing.
	lastPrincipal domain.Principal
	lastInput     domain.PredictInput

	// lastTeam records the team the handler threaded into the LAST control-plane /
	// GetRoute call — the value that namespaces the route (the IDOR fix). A test
	// asserts it came from the verified claims, never a request field.
	lastTeam string

	// allInputs records EVERY PredictInput the handler forwarded, in call order.
	// The bulk paths (BatchPredict / StreamPredict) call Predict once per item, so
	// a single lastInput can't prove per-item idempotency-key derivation — this
	// slice lets a test assert the key the handler computed for each item.
	allInputs []domain.PredictInput
}

func (m *mockInferenceService) Predict(ctx context.Context, p domain.Principal, in domain.PredictInput) (domain.PredictOutput, error) {
	m.lastPrincipal = p
	m.lastInput = in
	m.allInputs = append(m.allInputs, in)
	return m.predictFn(ctx, p, in)
}
func (m *mockInferenceService) GetRoute(ctx context.Context, team, modelName string) (domain.Route, error) {
	m.lastTeam = team
	return m.getRouteFn(ctx, team, modelName)
}
func (m *mockInferenceService) ListRoutes(ctx context.Context, team string, opts domain.ListOptions) ([]domain.Route, string, error) {
	m.lastTeam = team
	return m.listRoutesFn(ctx, team, opts)
}
func (m *mockInferenceService) UpsertRoute(ctx context.Context, team, modelName string, proposed []domain.ProposedTarget) (domain.Route, error) {
	m.lastTeam = team
	return m.upsertRouteFn(ctx, team, modelName, proposed)
}
func (m *mockInferenceService) SetTrafficSplit(ctx context.Context, team, modelName string, weights []domain.TrafficWeight) (domain.Route, error) {
	m.lastTeam = team
	return m.setTrafficSplitFn(ctx, team, modelName, weights)
}
func (m *mockInferenceService) DeleteRoute(ctx context.Context, team, modelName string) error {
	m.lastTeam = team
	return m.deleteRouteFn(ctx, team, modelName)
}
func (m *mockInferenceService) CircuitStates(modelNameFilter string) []domain.CircuitSnapshot {
	return m.circuitStatesFn(modelNameFilter)
}
func (m *mockInferenceService) ApplyModelDeployed(context.Context, domain.ModelDeployed) error {
	return nil
}
func (m *mockInferenceService) ApplyModelUndeployed(context.Context, domain.ModelUndeployed) error {
	return nil
}
func (m *mockInferenceService) ApplyModelPromoted(context.Context, domain.ModelPromoted) error {
	return nil
}
func (m *mockInferenceService) ApplyModelArchived(context.Context, domain.ModelArchived) error {
	return nil
}

// compile-time assertion that the mock satisfies the port.
var _ domain.InferenceService = (*mockInferenceService)(nil)

// ============================================================================
// STUB TOKEN VALIDATOR — decodes the bearer token as JSON Claims
// ============================================================================

// stubValidator is the test TokenValidator. The "token" a test sends is a JSON
// encoding of the claims it wants; Validate just decodes it. This drives the REAL
// auth interceptor so the handler reads identity/scope via the genuine
// ClaimsFromContext path. An empty/garbage token → Unauthenticated (mirrors a
// real validator rejecting a bad token).
type stubValidator struct{}

func (stubValidator) Validate(_ context.Context, token string) (*grpcutil.Claims, error) {
	var c grpcutil.Claims
	if err := json.Unmarshal([]byte(token), &c); err != nil {
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}
	return &c, nil
}

// ctxWithClaims attaches a bearer token carrying the given claims to the outgoing
// gRPC metadata — the test's way of "authenticating as" a principal with scopes.
func ctxWithClaims(ctx context.Context, c grpcutil.Claims) context.Context {
	b, _ := json.Marshal(c)
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+string(b))
}

// predictClaims / adminClaims / overrideClaims are convenience principals.
func predictClaims() grpcutil.Claims {
	return grpcutil.Claims{UserID: "key-123", Team: "team-a", Scopes: []string{scopePredict}}
}
func overrideClaims() grpcutil.Claims {
	return grpcutil.Claims{UserID: "key-123", Team: "team-a", Scopes: []string{scopePredict, scopeOverride}}
}
func adminClaims() grpcutil.Claims {
	return grpcutil.Claims{UserID: "op-1", Team: "platform", Scopes: []string{scopeAdmin}}
}

// ============================================================================
// TEST HARNESS — bufconn + real auth interceptor + handler wired to the mock
// ============================================================================

// newClient spins up an in-process gRPC server with the REAL auth interceptor
// chain (unary + stream) and the handler under test wired to the given mock,
// returning a ready client. Cleanup is automatic via testutil.
func newClient(t *testing.T, svc domain.InferenceService) inferencev1.InferenceGatewayServiceClient {
	t.Helper()
	h := handler.NewInferenceHandler(svc)
	conn := testutil.NewTestGRPCServer(t,
		func(s *grpc.Server) {
			inferencev1.RegisterInferenceGatewayServiceServer(s, h)
		},
		grpc.ChainUnaryInterceptor(grpcutil.AuthUnaryInterceptor(stubValidator{})),
		grpc.ChainStreamInterceptor(grpcutil.AuthStreamInterceptor(stubValidator{})),
	)
	return inferencev1.NewInferenceGatewayServiceClient(conn)
}

// requireCode asserts err is a gRPC status with the expected code, returning the
// status so the caller can also assert on the message.
func requireCode(t *testing.T, err error, want codes.Code) *status.Status {
	t.Helper()
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected a gRPC status error, got %v (%T)", err, err)
	}
	if st.Code() != want {
		t.Fatalf("expected code %s, got %s (msg=%q)", want, st.Code(), st.Message())
	}
	return st
}

// assertNoLeak fails if the error message contains any of the forbidden internal
// substrings — the "no internal detail / PII / secrets leak" contract.
func assertNoLeak(t *testing.T, msg string, forbidden ...string) {
	t.Helper()
	low := strings.ToLower(msg)
	for _, f := range forbidden {
		if strings.Contains(low, strings.ToLower(f)) {
			t.Fatalf("error message leaked forbidden substring %q: %q", f, msg)
		}
	}
}

// ============================================================================
// Predict — happy path, conversion, identity sourcing, authz, error mapping
// ============================================================================

func TestPredict_HappyPath_ConversionAndIdentity(t *testing.T) {
	mock := &mockInferenceService{
		predictFn: func(_ context.Context, _ domain.Principal, _ domain.PredictInput) (domain.PredictOutput, error) {
			return domain.PredictOutput{
				Outputs: map[string]domain.Tensor{
					"probs": {Shape: []int64{1, 2}, DType: "DATA_TYPE_FLOAT32", Data: []byte{1, 2, 3, 4, 5, 6, 7, 8}},
				},
				ServedVersion: "v3",
				IsCanary:      true,
				Latency:       42 * time.Millisecond,
				RequestID:     "req-abc",
			}, nil
		},
	}
	client := newClient(t, mock)

	resp, err := client.Predict(ctxWithClaims(context.Background(), predictClaims()), &inferencev1.PredictRequest{
		ModelName: "fraud-detector",
		Inputs: map[string]*inferencev1.TensorData{
			"features": {Shape: []int64{1, 4}, Dtype: inferencev1.DataType_DATA_TYPE_FLOAT32, Data: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
		},
		IdempotencyKey: "idem-1",
	})
	if err != nil {
		t.Fatalf("Predict returned error: %v", err)
	}

	// (a) domain → proto conversion of the result.
	if resp.GetServedVersion() != "v3" {
		t.Errorf("served_version = %q, want v3", resp.GetServedVersion())
	}
	if resp.GetRequestId() != "req-abc" {
		t.Errorf("request_id = %q, want req-abc", resp.GetRequestId())
	}
	if resp.GetLatency().AsDuration() != 42*time.Millisecond {
		t.Errorf("latency = %v, want 42ms", resp.GetLatency().AsDuration())
	}
	out, ok := resp.GetOutputs()["probs"]
	if !ok {
		t.Fatalf("missing 'probs' output; got keys %v", keysOf(resp.GetOutputs()))
	}
	if out.GetDtype() != inferencev1.DataType_DATA_TYPE_FLOAT32 {
		t.Errorf("output dtype = %v, want FLOAT32", out.GetDtype())
	}

	// (b) proto → domain conversion of the request, captured by the mock.
	if mock.lastInput.ModelName != "fraud-detector" {
		t.Errorf("domain ModelName = %q, want fraud-detector", mock.lastInput.ModelName)
	}
	if mock.lastInput.IdempotencyKey != "idem-1" {
		t.Errorf("domain IdempotencyKey = %q, want idem-1", mock.lastInput.IdempotencyKey)
	}
	feat, ok := mock.lastInput.Inputs["features"]
	if !ok || feat.DType != "DATA_TYPE_FLOAT32" || len(feat.Data) != 16 {
		t.Errorf("domain input tensor not converted correctly: %+v", mock.lastInput.Inputs)
	}

	// (c) SERVER-AUTHORITATIVE identity: Principal comes from the verified claims,
	// not any request field (the request carries no api_key/team at all).
	if mock.lastPrincipal.APIKeyID != "key-123" || mock.lastPrincipal.Team != "team-a" {
		t.Errorf("principal = %+v, want {APIKeyID:key-123 Team:team-a} from claims", mock.lastPrincipal)
	}
}

func TestPredict_Unauthenticated_NoClaims(t *testing.T) {
	mock := &mockInferenceService{} // predictFn nil — must never be called
	client := newClient(t, mock)
	// No token attached → the auth interceptor rejects before the handler.
	_, err := client.Predict(context.Background(), &inferencev1.PredictRequest{ModelName: "m"})
	requireCode(t, err, codes.Unauthenticated)
}

func TestPredict_Validation(t *testing.T) {
	mock := &mockInferenceService{
		predictFn: func(context.Context, domain.Principal, domain.PredictInput) (domain.PredictOutput, error) {
			t.Fatal("Predict service must not be called on a validation reject")
			return domain.PredictOutput{}, nil
		},
	}
	client := newClient(t, mock)
	ctx := ctxWithClaims(context.Background(), predictClaims())

	t.Run("missing model_name", func(t *testing.T) {
		_, err := client.Predict(ctx, &inferencev1.PredictRequest{
			Inputs: map[string]*inferencev1.TensorData{"x": {Dtype: inferencev1.DataType_DATA_TYPE_FLOAT32, Data: []byte{1}}},
		})
		requireCode(t, err, codes.InvalidArgument)
	})

	t.Run("unspecified dtype", func(t *testing.T) {
		_, err := client.Predict(ctx, &inferencev1.PredictRequest{
			ModelName: "m",
			Inputs:    map[string]*inferencev1.TensorData{"x": {Data: []byte{1}}}, // dtype defaults UNSPECIFIED
		})
		requireCode(t, err, codes.InvalidArgument)
	})
}

func TestPredict_VersionOverride_RequiresElevatedScope(t *testing.T) {
	mock := &mockInferenceService{
		predictFn: func(_ context.Context, _ domain.Principal, in domain.PredictInput) (domain.PredictOutput, error) {
			return domain.PredictOutput{ServedVersion: in.VersionOverride, RequestID: "r"}, nil
		},
	}
	client := newClient(t, mock)

	// Ordinary caller (predict scope only) sets version_override → PermissionDenied.
	t.Run("denied without override scope", func(t *testing.T) {
		_, err := client.Predict(ctxWithClaims(context.Background(), predictClaims()), &inferencev1.PredictRequest{
			ModelName:       "m",
			Inputs:          map[string]*inferencev1.TensorData{"x": {Dtype: inferencev1.DataType_DATA_TYPE_FLOAT32, Data: []byte{1}}},
			VersionOverride: "v9",
		})
		st := requireCode(t, err, codes.PermissionDenied)
		assertNoLeak(t, st.Message(), "key-123", "team-a")
	})

	// Elevated caller (override scope) → honored, forwarded to the domain.
	t.Run("allowed with override scope", func(t *testing.T) {
		resp, err := client.Predict(ctxWithClaims(context.Background(), overrideClaims()), &inferencev1.PredictRequest{
			ModelName:       "m",
			Inputs:          map[string]*inferencev1.TensorData{"x": {Dtype: inferencev1.DataType_DATA_TYPE_FLOAT32, Data: []byte{1}}},
			VersionOverride: "v9",
		})
		if err != nil {
			t.Fatalf("expected success, got %v", err)
		}
		if mock.lastInput.VersionOverride != "v9" {
			t.Errorf("override not forwarded: %q", mock.lastInput.VersionOverride)
		}
		if resp.GetServedVersion() != "v9" {
			t.Errorf("served_version = %q, want v9", resp.GetServedVersion())
		}
	})
}

func TestPredict_ErrorMapping(t *testing.T) {
	// Each domain sentinel must map to the precise gRPC code AND not leak detail.
	cases := []struct {
		name       string
		domainErr  error
		wantCode   codes.Code
		forbidEcho []string // substrings that must NOT appear in the client message
	}{
		{"no route", domain.ErrNoRoute, codes.NotFound, nil},
		{"invalid input", fmt.Errorf("%w: tensor bad", domain.ErrInvalidInput), codes.InvalidArgument, nil},
		{"rate limited", domain.ErrRateLimited, codes.ResourceExhausted, nil},
		{"quota exceeded", domain.ErrQuotaExceeded, codes.ResourceExhausted, nil},
		{"circuit open", domain.ErrCircuitOpen, codes.Unavailable, nil},
		{"upstream", domain.ErrUpstream, codes.Unavailable, nil},
		{"timeout", domain.ErrTimeout, codes.DeadlineExceeded, nil},
		// An UNRECOGNIZED error carrying a "secret" must surface as Internal with a
		// generic message — the secret must NOT reach the client.
		{"opaque internal", errors.New("dial redis://user:s3cr3t@10.0.0.5:6379 failed"), codes.Internal, []string{"s3cr3t", "redis", "10.0.0.5"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockInferenceService{
				predictFn: func(context.Context, domain.Principal, domain.PredictInput) (domain.PredictOutput, error) {
					return domain.PredictOutput{}, tc.domainErr
				},
			}
			client := newClient(t, mock)
			_, err := client.Predict(ctxWithClaims(context.Background(), predictClaims()), &inferencev1.PredictRequest{
				ModelName: "m",
				Inputs:    map[string]*inferencev1.TensorData{"x": {Dtype: inferencev1.DataType_DATA_TYPE_FLOAT32, Data: []byte{1}}},
			})
			st := requireCode(t, err, tc.wantCode)
			if len(tc.forbidEcho) > 0 {
				assertNoLeak(t, st.Message(), tc.forbidEcho...)
				if st.Message() != "internal error" {
					t.Errorf("Internal message should be generic, got %q", st.Message())
				}
			}
		})
	}
}

// ============================================================================
// BatchPredict — partial success, caps, per-item reason
// ============================================================================

func TestBatchPredict_PartialSuccess(t *testing.T) {
	// item "ok" succeeds; item "bad" fails with ErrTimeout in the domain.
	mock := &mockInferenceService{
		predictFn: func(_ context.Context, _ domain.Principal, in domain.PredictInput) (domain.PredictOutput, error) {
			if _, isBad := in.Inputs["trigger_timeout"]; isBad {
				return domain.PredictOutput{}, domain.ErrTimeout
			}
			return domain.PredictOutput{ServedVersion: "v1", RequestID: "r"}, nil
		},
	}
	client := newClient(t, mock)

	resp, err := client.BatchPredict(ctxWithClaims(context.Background(), predictClaims()), &inferencev1.BatchPredictRequest{
		ModelName: "m",
		Items: []*inferencev1.BatchPredictItem{
			{ItemId: "ok", Inputs: map[string]*inferencev1.TensorData{"x": {Dtype: inferencev1.DataType_DATA_TYPE_FLOAT32, Data: []byte{1}}}},
			{ItemId: "bad", Inputs: map[string]*inferencev1.TensorData{"trigger_timeout": {Dtype: inferencev1.DataType_DATA_TYPE_FLOAT32, Data: []byte{1}}}},
		},
	})
	if err != nil {
		t.Fatalf("BatchPredict (whole call) must not fail on a bad item: %v", err)
	}
	if len(resp.GetResults()) != 2 {
		t.Fatalf("want 2 results, got %d", len(resp.GetResults()))
	}
	if resp.GetRequestId() == "" {
		t.Error("batch request_id should be set (server-authoritative)")
	}
	byID := map[string]*inferencev1.BatchPredictResult{}
	for _, r := range resp.GetResults() {
		byID[r.GetItemId()] = r
	}
	if okR := byID["ok"]; okR == nil || okR.GetError() != nil || okR.GetServedVersion() != "v1" {
		t.Errorf("ok item wrong: %+v", okR)
	}
	badR := byID["bad"]
	if badR == nil || badR.GetError() == nil {
		t.Fatalf("bad item should carry a structured error: %+v", badR)
	}
	if badR.GetFailureReason() != eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_TIMEOUT {
		t.Errorf("bad item failure_reason = %v, want TIMEOUT", badR.GetFailureReason())
	}
	if badR.GetError().GetCode() != codes.DeadlineExceeded.String() {
		t.Errorf("bad item error code = %q, want %q", badR.GetError().GetCode(), codes.DeadlineExceeded.String())
	}
}

// TestBatchPredict_ForwardsPerItemIdempotencyKey proves the billing-correctness
// fix: the CALL-level idempotency_key is expanded into a per-item key
// ("<callKey>:<index>") and threaded into each domain Predict. Before the fix the
// bulk path dropped the key entirely, so a retried batch re-ran every item as
// unique work → a duplicate InferenceCompleted per item → DOUBLE-BILLING. We
// assert the exact key each item carries (so a retry maps to the same keys and
// the domain's idempotency store collapses the duplicate).
func TestBatchPredict_ForwardsPerItemIdempotencyKey(t *testing.T) {
	mock := &mockInferenceService{
		predictFn: func(context.Context, domain.Principal, domain.PredictInput) (domain.PredictOutput, error) {
			return domain.PredictOutput{ServedVersion: "v1", RequestID: "r"}, nil
		},
	}
	client := newClient(t, mock)

	_, err := client.BatchPredict(ctxWithClaims(context.Background(), predictClaims()), &inferencev1.BatchPredictRequest{
		ModelName:      "m",
		IdempotencyKey: "batch-idem-1",
		Items: []*inferencev1.BatchPredictItem{
			{ItemId: "a", Inputs: map[string]*inferencev1.TensorData{"x": {Dtype: inferencev1.DataType_DATA_TYPE_FLOAT32, Data: []byte{1}}}},
			{ItemId: "b", Inputs: map[string]*inferencev1.TensorData{"x": {Dtype: inferencev1.DataType_DATA_TYPE_FLOAT32, Data: []byte{1}}}},
		},
	})
	if err != nil {
		t.Fatalf("BatchPredict error: %v", err)
	}
	if len(mock.allInputs) != 2 {
		t.Fatalf("want 2 forwarded Predict calls, got %d", len(mock.allInputs))
	}
	// Key is derived from POSITION, not item_id, so it is retry-stable even when
	// item_id is empty/duplicated.
	if got, want := mock.allInputs[0].IdempotencyKey, "batch-idem-1:0"; got != want {
		t.Errorf("item 0 idempotency key = %q, want %q", got, want)
	}
	if got, want := mock.allInputs[1].IdempotencyKey, "batch-idem-1:1"; got != want {
		t.Errorf("item 1 idempotency key = %q, want %q", got, want)
	}
}

// TestBatchPredict_NoCallKey_NoPerItemKey proves the opt-out path: a client that
// supplied NO call-level key must NOT get synthesized per-item keys (no ":0" that
// could collide across distinct calls). Empty in → empty out, mirroring unary
// Predict's behavior.
func TestBatchPredict_NoCallKey_NoPerItemKey(t *testing.T) {
	mock := &mockInferenceService{
		predictFn: func(context.Context, domain.Principal, domain.PredictInput) (domain.PredictOutput, error) {
			return domain.PredictOutput{ServedVersion: "v1", RequestID: "r"}, nil
		},
	}
	client := newClient(t, mock)

	_, err := client.BatchPredict(ctxWithClaims(context.Background(), predictClaims()), &inferencev1.BatchPredictRequest{
		ModelName: "m",
		Items: []*inferencev1.BatchPredictItem{
			{ItemId: "a", Inputs: map[string]*inferencev1.TensorData{"x": {Dtype: inferencev1.DataType_DATA_TYPE_FLOAT32, Data: []byte{1}}}},
		},
	})
	if err != nil {
		t.Fatalf("BatchPredict error: %v", err)
	}
	if len(mock.allInputs) != 1 {
		t.Fatalf("want 1 forwarded Predict call, got %d", len(mock.allInputs))
	}
	if got := mock.allInputs[0].IdempotencyKey; got != "" {
		t.Errorf("with no call key, per-item key should be empty, got %q", got)
	}
}

func TestBatchPredict_Validation(t *testing.T) {
	mock := &mockInferenceService{
		predictFn: func(context.Context, domain.Principal, domain.PredictInput) (domain.PredictOutput, error) {
			return domain.PredictOutput{RequestID: "r"}, nil
		},
	}
	client := newClient(t, mock)
	ctx := ctxWithClaims(context.Background(), predictClaims())

	t.Run("empty items", func(t *testing.T) {
		_, err := client.BatchPredict(ctx, &inferencev1.BatchPredictRequest{ModelName: "m"})
		requireCode(t, err, codes.InvalidArgument)
	})
	t.Run("over cap", func(t *testing.T) {
		items := make([]*inferencev1.BatchPredictItem, 257) // > maxBatchItems (256)
		for i := range items {
			items[i] = &inferencev1.BatchPredictItem{ItemId: fmt.Sprintf("i%d", i), Inputs: map[string]*inferencev1.TensorData{"x": {Dtype: inferencev1.DataType_DATA_TYPE_FLOAT32, Data: []byte{1}}}}
		}
		_, err := client.BatchPredict(ctx, &inferencev1.BatchPredictRequest{ModelName: "m", Items: items})
		requireCode(t, err, codes.InvalidArgument)
	})
	t.Run("missing model_name", func(t *testing.T) {
		_, err := client.BatchPredict(ctx, &inferencev1.BatchPredictRequest{
			Items: []*inferencev1.BatchPredictItem{{ItemId: "a", Inputs: map[string]*inferencev1.TensorData{"x": {Dtype: inferencev1.DataType_DATA_TYPE_FLOAT32, Data: []byte{1}}}}},
		})
		requireCode(t, err, codes.InvalidArgument)
	})
}

// ============================================================================
// StreamPredict — server-streaming happy path, cap, nil-svc guard
// ============================================================================

func TestStreamPredict_StreamsPerItem(t *testing.T) {
	mock := &mockInferenceService{
		predictFn: func(_ context.Context, _ domain.Principal, _ domain.PredictInput) (domain.PredictOutput, error) {
			return domain.PredictOutput{ServedVersion: "v1", RequestID: "r"}, nil
		},
	}
	client := newClient(t, mock)

	stream, err := client.StreamPredict(ctxWithClaims(context.Background(), predictClaims()), &inferencev1.StreamPredictRequest{
		ModelName: "m",
		Items: []*inferencev1.BatchPredictItem{
			{ItemId: "a", Inputs: map[string]*inferencev1.TensorData{"x": {Dtype: inferencev1.DataType_DATA_TYPE_FLOAT32, Data: []byte{1}}}},
			{ItemId: "b", Inputs: map[string]*inferencev1.TensorData{"x": {Dtype: inferencev1.DataType_DATA_TYPE_FLOAT32, Data: []byte{1}}}},
		},
	})
	if err != nil {
		t.Fatalf("StreamPredict open error: %v", err)
	}
	var got []string
	var reqID string
	for {
		msg, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			t.Fatalf("stream Recv error: %v", recvErr)
		}
		got = append(got, msg.GetResult().GetItemId())
		reqID = msg.GetRequestId()
	}
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("streamed item order = %v, want [a b]", got)
	}
	if reqID == "" {
		t.Error("stream request_id should be set on each message")
	}
}

// TestStreamPredict_ForwardsPerItemIdempotencyKey is the streaming twin of the
// batch test: the StreamPredictRequest's call-level idempotency_key must be
// expanded into the same "<callKey>:<index>" per-item keys and threaded into each
// domain Predict, so a re-streamed batch dedupes item-by-item instead of
// double-billing the whole job.
func TestStreamPredict_ForwardsPerItemIdempotencyKey(t *testing.T) {
	mock := &mockInferenceService{
		predictFn: func(context.Context, domain.Principal, domain.PredictInput) (domain.PredictOutput, error) {
			return domain.PredictOutput{ServedVersion: "v1", RequestID: "r"}, nil
		},
	}
	client := newClient(t, mock)

	stream, err := client.StreamPredict(ctxWithClaims(context.Background(), predictClaims()), &inferencev1.StreamPredictRequest{
		ModelName:      "m",
		IdempotencyKey: "stream-idem-1",
		Items: []*inferencev1.BatchPredictItem{
			{ItemId: "a", Inputs: map[string]*inferencev1.TensorData{"x": {Dtype: inferencev1.DataType_DATA_TYPE_FLOAT32, Data: []byte{1}}}},
			{ItemId: "b", Inputs: map[string]*inferencev1.TensorData{"x": {Dtype: inferencev1.DataType_DATA_TYPE_FLOAT32, Data: []byte{1}}}},
		},
	})
	if err != nil {
		t.Fatalf("StreamPredict open error: %v", err)
	}
	// Drain the stream so all per-item Predict calls are made before we assert.
	for {
		_, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			t.Fatalf("stream Recv error: %v", recvErr)
		}
	}
	if len(mock.allInputs) != 2 {
		t.Fatalf("want 2 forwarded Predict calls, got %d", len(mock.allInputs))
	}
	if got, want := mock.allInputs[0].IdempotencyKey, "stream-idem-1:0"; got != want {
		t.Errorf("item 0 idempotency key = %q, want %q", got, want)
	}
	if got, want := mock.allInputs[1].IdempotencyKey, "stream-idem-1:1"; got != want {
		t.Errorf("item 1 idempotency key = %q, want %q", got, want)
	}
}

func TestStreamPredict_Validation(t *testing.T) {
	mock := &mockInferenceService{
		predictFn: func(context.Context, domain.Principal, domain.PredictInput) (domain.PredictOutput, error) {
			return domain.PredictOutput{RequestID: "r"}, nil
		},
	}
	client := newClient(t, mock)

	// Validation errors on a server-streaming RPC surface on the first Recv (the
	// handler returns the status before sending anything).
	t.Run("empty items", func(t *testing.T) {
		stream, _ := client.StreamPredict(ctxWithClaims(context.Background(), predictClaims()), &inferencev1.StreamPredictRequest{ModelName: "m"})
		_, err := stream.Recv()
		requireCode(t, err, codes.InvalidArgument)
	})
	t.Run("missing model_name", func(t *testing.T) {
		stream, _ := client.StreamPredict(ctxWithClaims(context.Background(), predictClaims()), &inferencev1.StreamPredictRequest{
			Items: []*inferencev1.BatchPredictItem{{ItemId: "a", Inputs: map[string]*inferencev1.TensorData{"x": {Dtype: inferencev1.DataType_DATA_TYPE_FLOAT32, Data: []byte{1}}}}},
		})
		_, err := stream.Recv()
		requireCode(t, err, codes.InvalidArgument)
	})
}

func TestStreamPredict_NilService_Unimplemented(t *testing.T) {
	// The streaming nil-svc guard returns the STATUS as an error (no response).
	client := newClient(t, nil)
	stream, err := client.StreamPredict(ctxWithClaims(context.Background(), predictClaims()), &inferencev1.StreamPredictRequest{
		ModelName: "m",
		Items:     []*inferencev1.BatchPredictItem{{ItemId: "a", Inputs: map[string]*inferencev1.TensorData{"x": {Dtype: inferencev1.DataType_DATA_TYPE_FLOAT32, Data: []byte{1}}}}},
	})
	if err == nil {
		_, err = stream.Recv()
	}
	requireCode(t, err, codes.Unimplemented)
}

func TestUnary_NilService_Unimplemented(t *testing.T) {
	// A unary RPC's nil-svc guard returns (nil, Unimplemented).
	client := newClient(t, nil)
	_, err := client.Predict(ctxWithClaims(context.Background(), predictClaims()), &inferencev1.PredictRequest{
		ModelName: "m",
		Inputs:    map[string]*inferencev1.TensorData{"x": {Dtype: inferencev1.DataType_DATA_TYPE_FLOAT32, Data: []byte{1}}},
	})
	requireCode(t, err, codes.Unimplemented)
}

// ============================================================================
// GetModelInfo — caller-view projection (NO endpoints leaked)
// ============================================================================

func TestGetModelInfo_ProjectsCallerViewNoEndpoints(t *testing.T) {
	mock := &mockInferenceService{
		getRouteFn: func(_ context.Context, _, _ string) (domain.Route, error) {
			return domain.Route{
				ModelName: "m",
				UpdatedAt: time.Unix(1700000000, 0),
				Targets: []domain.RouteTarget{
					{Version: "v1", Endpoint: "iris-v1.fp-models.svc:9090", WeightBps: 9000, Status: domain.TargetStatusActive, IsStable: true},
					{Version: "v2", Endpoint: "iris-v2.fp-models.svc:9090", WeightBps: 1000, Status: domain.TargetStatusActive, IsStable: false},
				},
			}, nil
		},
	}
	client := newClient(t, mock)

	resp, err := client.GetModelInfo(ctxWithClaims(context.Background(), predictClaims()), &inferencev1.GetModelInfoRequest{ModelName: "m"})
	if err != nil {
		t.Fatalf("GetModelInfo error: %v", err)
	}
	mi := resp.GetModelInfo()
	if !mi.GetIsServing() {
		t.Error("is_serving should be true (has active targets)")
	}
	if len(mi.GetVersions()) != 2 {
		t.Fatalf("want 2 versions, got %d", len(mi.GetVersions()))
	}
	// CRITICAL: the caller view must NOT expose backend endpoints. The proto
	// VersionInfo has no endpoint field, so the projection is structurally safe;
	// we assert the whole serialized message carries no endpoint string.
	if strings.Contains(mi.String(), "fp-models.svc") {
		t.Errorf("ModelInfo leaked a backend endpoint: %s", mi.String())
	}
}

func TestGetModelInfo_NoRoute_NotFound(t *testing.T) {
	mock := &mockInferenceService{
		getRouteFn: func(context.Context, string, string) (domain.Route, error) { return domain.Route{}, domain.ErrNoRoute },
	}
	client := newClient(t, mock)
	_, err := client.GetModelInfo(ctxWithClaims(context.Background(), predictClaims()), &inferencev1.GetModelInfoRequest{ModelName: "ghost"})
	requireCode(t, err, codes.NotFound)
}

// ============================================================================
// Control plane — scope gates + conversion + mass-assignment defense
// ============================================================================

func TestGetRoute_RequiresAdminScope(t *testing.T) {
	mock := &mockInferenceService{
		getRouteFn: func(context.Context, string, string) (domain.Route, error) {
			t.Fatal("GetRoute service must not run for an unauthorized caller")
			return domain.Route{}, nil
		},
	}
	client := newClient(t, mock)
	// predict-only caller hits the admin-gated control plane → PermissionDenied.
	_, err := client.GetRoute(ctxWithClaims(context.Background(), predictClaims()), &inferencev1.GetRouteRequest{ModelName: "m"})
	requireCode(t, err, codes.PermissionDenied)
}

func TestGetRoute_HappyPath_OperatorViewHasEndpoints(t *testing.T) {
	mock := &mockInferenceService{
		getRouteFn: func(_ context.Context, _, _ string) (domain.Route, error) {
			return domain.Route{
				ModelName: "m",
				UpdatedAt: time.Unix(1700000000, 0),
				Targets: []domain.RouteTarget{
					{Version: "v1", Endpoint: "iris-v1.fp-models.svc:9090", WeightBps: 10000, Status: domain.TargetStatusActive, IsStable: true},
				},
			}, nil
		},
	}
	client := newClient(t, mock)
	resp, err := client.GetRoute(ctxWithClaims(context.Background(), adminClaims()), &inferencev1.GetRouteRequest{ModelName: "m"})
	if err != nil {
		t.Fatalf("GetRoute error: %v", err)
	}
	tg := resp.GetRoute().GetTargets()
	if len(tg) != 1 || tg[0].GetEndpoint() != "iris-v1.fp-models.svc:9090" {
		t.Errorf("operator view should include endpoint, got %+v", tg)
	}
	if tg[0].GetStatus() != inferencev1.TargetStatus_TARGET_STATUS_ACTIVE {
		t.Errorf("status conversion wrong: %v", tg[0].GetStatus())
	}
}

// TestControlPlane_TeamComesFromClaimsNotRequest is the handler-level guard for the
// cross-tenant route IDOR fix: the team that namespaces every control-plane / route
// operation MUST be sourced from the verified claims (Principal.Team), never from a
// request field a caller could spoof. The request carries NO team field at all (the
// proto has none), so the only place team can come from is the token. We assert
// that the team the handler threaded into the domain equals the CLAIMS team, across
// GetRoute / ListRoutes / UpsertRoute / SetTrafficSplit / DeleteRoute. adminClaims()
// has Team "platform"; a caller from a DIFFERENT team would namespace differently
// and so could never reach another team's route.
func TestControlPlane_TeamComesFromClaimsNotRequest(t *testing.T) {
	route := domain.Route{OwnerTeam: "platform", ModelName: "m", Targets: []domain.RouteTarget{{Version: "v1", Endpoint: "e:9090", WeightBps: 10000, Status: domain.TargetStatusActive}}}
	mock := &mockInferenceService{
		getRouteFn: func(_ context.Context, _, _ string) (domain.Route, error) { return route, nil },
		listRoutesFn: func(_ context.Context, _ string, _ domain.ListOptions) ([]domain.Route, string, error) {
			return []domain.Route{route}, "", nil
		},
		upsertRouteFn: func(_ context.Context, _, _ string, _ []domain.ProposedTarget) (domain.Route, error) {
			return route, nil
		},
		setTrafficSplitFn: func(_ context.Context, _, _ string, _ []domain.TrafficWeight) (domain.Route, error) {
			return route, nil
		},
		deleteRouteFn: func(_ context.Context, _, _ string) error { return nil },
	}
	client := newClient(t, mock)
	ctx := ctxWithClaims(context.Background(), adminClaims()) // Team: "platform"

	if _, err := client.GetRoute(ctx, &inferencev1.GetRouteRequest{ModelName: "m"}); err != nil {
		t.Fatalf("GetRoute: %v", err)
	}
	if mock.lastTeam != "platform" {
		t.Fatalf("GetRoute threaded team %q, want the claims team %q", mock.lastTeam, "platform")
	}

	if _, err := client.ListRoutes(ctx, &inferencev1.ListRoutesRequest{}); err != nil {
		t.Fatalf("ListRoutes: %v", err)
	}
	if mock.lastTeam != "platform" {
		t.Fatalf("ListRoutes threaded team %q, want %q", mock.lastTeam, "platform")
	}

	if _, err := client.UpsertRoute(ctx, &inferencev1.UpsertRouteRequest{ModelName: "m", Targets: []*inferencev1.RouteTarget{{Version: "v1", WeightBps: 10000}}}); err != nil {
		t.Fatalf("UpsertRoute: %v", err)
	}
	if mock.lastTeam != "platform" {
		t.Fatalf("UpsertRoute threaded team %q, want %q", mock.lastTeam, "platform")
	}

	if _, err := client.SetTrafficSplit(ctx, &inferencev1.SetTrafficSplitRequest{ModelName: "m", Weights: []*inferencev1.TrafficWeight{{Version: "v1", WeightBps: 10000}}}); err != nil {
		t.Fatalf("SetTrafficSplit: %v", err)
	}
	if mock.lastTeam != "platform" {
		t.Fatalf("SetTrafficSplit threaded team %q, want %q", mock.lastTeam, "platform")
	}

	if _, err := client.DeleteRoute(ctx, &inferencev1.DeleteRouteRequest{ModelName: "m"}); err != nil {
		t.Fatalf("DeleteRoute: %v", err)
	}
	if mock.lastTeam != "platform" {
		t.Fatalf("DeleteRoute threaded team %q, want %q", mock.lastTeam, "platform")
	}
}

func TestUpsertRoute_DropsClientEndpointAndStatus(t *testing.T) {
	var gotProposed []domain.ProposedTarget
	mock := &mockInferenceService{
		upsertRouteFn: func(_ context.Context, _, _ string, proposed []domain.ProposedTarget) (domain.Route, error) {
			gotProposed = proposed
			return domain.Route{ModelName: "m", Targets: []domain.RouteTarget{{Version: "v1", Endpoint: "server-resolved:9090", WeightBps: 10000, Status: domain.TargetStatusActive}}}, nil
		},
	}
	client := newClient(t, mock)

	// The client tries to smuggle an endpoint + a status; the handler must DROP
	// both (ProposedTarget has no such fields) — anti mass-assignment / anti-SSRF.
	_, err := client.UpsertRoute(ctxWithClaims(context.Background(), adminClaims()), &inferencev1.UpsertRouteRequest{
		ModelName: "m",
		Targets: []*inferencev1.RouteTarget{
			{Version: "v1", Endpoint: "http://attacker.example", WeightBps: 10000, Status: inferencev1.TargetStatus_TARGET_STATUS_UNHEALTHY},
		},
	})
	if err != nil {
		t.Fatalf("UpsertRoute error: %v", err)
	}
	if len(gotProposed) != 1 || gotProposed[0].Version != "v1" || gotProposed[0].WeightBps != 10000 {
		t.Fatalf("proposed conversion wrong: %+v", gotProposed)
	}
	// ProposedTarget structurally cannot carry endpoint/status — verified by the
	// type, and here by the value the domain received being only version+weight.
}

func TestUpsertRoute_DomainValidation_InvalidArgument(t *testing.T) {
	mock := &mockInferenceService{
		upsertRouteFn: func(context.Context, string, string, []domain.ProposedTarget) (domain.Route, error) {
			return domain.Route{}, fmt.Errorf("%w: active weights sum to 9000, want 10000", domain.ErrRouteValidation)
		},
	}
	client := newClient(t, mock)
	_, err := client.UpsertRoute(ctxWithClaims(context.Background(), adminClaims()), &inferencev1.UpsertRouteRequest{
		ModelName: "m",
		Targets:   []*inferencev1.RouteTarget{{Version: "v1", WeightBps: 9000}},
	})
	st := requireCode(t, err, codes.InvalidArgument)
	if !strings.Contains(st.Message(), "10000") {
		t.Errorf("expected the domain's clean validation message, got %q", st.Message())
	}
}

func TestSetTrafficSplit_ConversionAndNoRoute(t *testing.T) {
	t.Run("happy path conversion", func(t *testing.T) {
		var gotWeights []domain.TrafficWeight
		mock := &mockInferenceService{
			setTrafficSplitFn: func(_ context.Context, _, _ string, w []domain.TrafficWeight) (domain.Route, error) {
				gotWeights = w
				return domain.Route{ModelName: "m"}, nil
			},
		}
		client := newClient(t, mock)
		_, err := client.SetTrafficSplit(ctxWithClaims(context.Background(), adminClaims()), &inferencev1.SetTrafficSplitRequest{
			ModelName: "m",
			Weights:   []*inferencev1.TrafficWeight{{Version: "v1", WeightBps: 5000}, {Version: "v2", WeightBps: 5000}},
		})
		if err != nil {
			t.Fatalf("SetTrafficSplit error: %v", err)
		}
		if len(gotWeights) != 2 || gotWeights[0].Version != "v1" || gotWeights[1].WeightBps != 5000 {
			t.Errorf("weights conversion wrong: %+v", gotWeights)
		}
	})

	t.Run("no route maps to NotFound", func(t *testing.T) {
		mock := &mockInferenceService{
			setTrafficSplitFn: func(context.Context, string, string, []domain.TrafficWeight) (domain.Route, error) {
				return domain.Route{}, domain.ErrNoRoute
			},
		}
		client := newClient(t, mock)
		_, err := client.SetTrafficSplit(ctxWithClaims(context.Background(), adminClaims()), &inferencev1.SetTrafficSplitRequest{
			ModelName: "m",
			Weights:   []*inferencev1.TrafficWeight{{Version: "v1", WeightBps: 10000}},
		})
		requireCode(t, err, codes.NotFound)
	})
}

func TestDeleteRoute_Idempotent(t *testing.T) {
	called := 0
	mock := &mockInferenceService{
		deleteRouteFn: func(context.Context, string, string) error { called++; return nil },
	}
	client := newClient(t, mock)
	for i := 0; i < 2; i++ {
		if _, err := client.DeleteRoute(ctxWithClaims(context.Background(), adminClaims()), &inferencev1.DeleteRouteRequest{ModelName: "m"}); err != nil {
			t.Fatalf("DeleteRoute attempt %d error: %v", i, err)
		}
	}
	if called != 2 {
		t.Errorf("service DeleteRoute called %d times, want 2 (idempotent at domain)", called)
	}
}

// ============================================================================
// Circuit observability — lookup, not-found, filter, conversion
// ============================================================================

func TestGetCircuitState_FoundAndNotFound(t *testing.T) {
	mock := &mockInferenceService{
		circuitStatesFn: func(filter string) []domain.CircuitSnapshot {
			if filter != "m" {
				return nil
			}
			return []domain.CircuitSnapshot{
				{ModelName: "m", Version: "v1", State: domain.CircuitOpen, ConsecutiveFailures: 7, LastTransitionAt: time.Unix(1700000000, 0)},
			}
		},
	}
	client := newClient(t, mock)

	t.Run("found", func(t *testing.T) {
		resp, err := client.GetCircuitState(ctxWithClaims(context.Background(), adminClaims()), &inferencev1.GetCircuitStateRequest{ModelName: "m", Version: "v1"})
		if err != nil {
			t.Fatalf("GetCircuitState error: %v", err)
		}
		cs := resp.GetCircuitState()
		if cs.GetState() != inferencev1.CircuitBreakerState_CIRCUIT_BREAKER_STATE_OPEN {
			t.Errorf("state = %v, want OPEN", cs.GetState())
		}
		if cs.GetConsecutiveFailures() != 7 {
			t.Errorf("consecutive_failures = %d, want 7", cs.GetConsecutiveFailures())
		}
	})

	t.Run("version not found", func(t *testing.T) {
		_, err := client.GetCircuitState(ctxWithClaims(context.Background(), adminClaims()), &inferencev1.GetCircuitStateRequest{ModelName: "m", Version: "v99"})
		requireCode(t, err, codes.NotFound)
	})

	t.Run("validation: missing version", func(t *testing.T) {
		_, err := client.GetCircuitState(ctxWithClaims(context.Background(), adminClaims()), &inferencev1.GetCircuitStateRequest{ModelName: "m"})
		requireCode(t, err, codes.InvalidArgument)
	})
}

func TestListCircuitStates_ConversionAndFilter(t *testing.T) {
	mock := &mockInferenceService{
		circuitStatesFn: func(filter string) []domain.CircuitSnapshot {
			if filter == "only-m" {
				return []domain.CircuitSnapshot{{ModelName: "m", Version: "v1", State: domain.CircuitClosed}}
			}
			return []domain.CircuitSnapshot{
				{ModelName: "m", Version: "v1", State: domain.CircuitClosed},
				{ModelName: "n", Version: "v2", State: domain.CircuitHalfOpen},
			}
		},
	}
	client := newClient(t, mock)

	resp, err := client.ListCircuitStates(ctxWithClaims(context.Background(), adminClaims()), &inferencev1.ListCircuitStatesRequest{ModelNameFilter: "only-m"})
	if err != nil {
		t.Fatalf("ListCircuitStates error: %v", err)
	}
	if len(resp.GetCircuitStates()) != 1 {
		t.Fatalf("filter not forwarded; got %d states", len(resp.GetCircuitStates()))
	}
	if resp.GetPagination().GetTotalCount() != 1 {
		t.Errorf("total_count = %d, want 1", resp.GetPagination().GetTotalCount())
	}
}

func TestListRoutes_PageSizeCapAndConversion(t *testing.T) {
	var gotOpts domain.ListOptions
	mock := &mockInferenceService{
		listRoutesFn: func(_ context.Context, _ string, opts domain.ListOptions) ([]domain.Route, string, error) {
			gotOpts = opts
			return []domain.Route{{ModelName: "m"}}, "next-cursor", nil
		},
	}
	client := newClient(t, mock)

	resp, err := client.ListRoutes(ctxWithClaims(context.Background(), adminClaims()), &inferencev1.ListRoutesRequest{
		Pagination: &commonv1.PaginationRequest{PageSize: 1000, PageToken: "cur"}, // over the 100 cap
	})
	if err != nil {
		t.Fatalf("ListRoutes error: %v", err)
	}
	if gotOpts.PageSize != 100 {
		t.Errorf("page_size cap not applied: got %d, want 100", gotOpts.PageSize)
	}
	if gotOpts.PageToken != "cur" {
		t.Errorf("page_token not forwarded: %q", gotOpts.PageToken)
	}
	if resp.GetPagination().GetNextPageToken() != "next-cursor" {
		t.Errorf("next_page_token = %q, want next-cursor", resp.GetPagination().GetNextPageToken())
	}
	if len(resp.GetRoutes()) != 1 {
		t.Errorf("want 1 route, got %d", len(resp.GetRoutes()))
	}
}

// keysOf is a tiny test helper for clearer failure output on map assertions.
func keysOf[K comparable, V any](m map[K]V) []K {
	out := make([]K, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
