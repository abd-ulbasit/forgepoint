// serving_handler_test.go — COMPONENT tests for the Model Serving gRPC handler.
//
// ============================================================================
// WHAT THESE TESTS COVER (and what they deliberately do NOT)
// ============================================================================
//
// SCOPE: the HANDLER in isolation — the proto↔domain conversion and the
// error→status-code mapping. We mock the domain.ServingService INTERFACE (the
// driving port), NOT the repositories/engine/fetcher. Mocking the service (one
// seam) is what isolates the handler: a test failure here is a handler bug, never
// an engine or storage bug. The domain's own logic (caps, layout, state machine)
// is exhaustively tested in serving_service_test.go; re-testing it through the
// handler would be redundant and couple two layers' tests.
//
// TRANSPORT: a REAL in-process gRPC stack over bufconn (pkg/testutil), so the
// tests exercise actual proto serialization, the generated client/server glue,
// and — for StreamPredict — the real bidi streaming machinery. This catches
// proto-tag/field-name bugs and stream-wiring bugs a direct method call would miss.
//
// FOR EACH RPC we assert:
//
//	(a) HAPPY PATH: the request is converted to the right domain input, and the
//	    domain result is converted to the right proto response (field by field).
//	(b) VALIDATION: a malformed request is rejected with InvalidArgument BEFORE
//	    the domain is called (we assert the mock was NOT invoked).
//	(c) ERROR MAPPING: each domain sentinel maps to the documented status code.
//	(d) NO LEAK: an unknown/internal error yields codes.Internal with a fixed
//	    sanitized message that does not contain the raw error text.
//
// ============================================================================
package handler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	servingv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/serving/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/model-serving/internal/domain"
)

// ============================================================================
// MOCK of the domain.ServingService interface
// ============================================================================
//
// A hand-written mock (not a generated one) keeps the test dependency-free and
// makes the captured inputs / canned outputs explicit and readable. Each method
// is backed by a func field the test sets; a nil func field means "this method
// must not be called" and fails the test if it is — that is how we assert
// validation short-circuits BEFORE the domain (test (b)). The mock also records
// the LAST input it received so the happy-path tests can assert the handler built
// the correct domain input (test (a)).
type mockService struct {
	predictFn      func(ctx context.Context, in domain.PredictInput) (domain.PredictResult, error)
	loadFn         func(ctx context.Context, in domain.LoadModelInput) (domain.ModelStatus, error)
	unloadFn       func(ctx context.Context, ref domain.ModelRef, reason string) (domain.ModelStatus, error)
	getStatusFn    func(ctx context.Context, ref domain.ModelRef) (domain.ModelStatus, error)
	getInfoFn      func(ctx context.Context, ref domain.ModelRef) (domain.LoadedModel, error)
	listFn         func(ctx context.Context, opts domain.ListOptions) ([]domain.ModelStatus, string, error)
	metricsFn      func(ctx context.Context) domain.ServingMetrics
	healthFn       func(ctx context.Context, name string) (domain.HealthVerdict, domain.ModelState, time.Duration)
	ensureLoadedFn func(ctx context.Context, in domain.LoadModelInput) (domain.ModelStatus, error)

	// Captured inputs for happy-path assertions.
	lastPredict domain.PredictInput
	lastLoad    domain.LoadModelInput
	lastList    domain.ListOptions
	lastUnload  struct {
		ref    domain.ModelRef
		reason string
	}
}

// errMockUnexpectedCall is returned by any method whose func field is nil. WHY a
// distinct error: it surfaces as a clear test failure ("the handler called a
// method it should have rejected before") rather than a nil-deref panic.
var errMockUnexpectedCall = errors.New("mock: method called unexpectedly (validation should have short-circuited)")

func (m *mockService) Predict(ctx context.Context, in domain.PredictInput) (domain.PredictResult, error) {
	m.lastPredict = in
	if m.predictFn == nil {
		return domain.PredictResult{}, errMockUnexpectedCall
	}
	return m.predictFn(ctx, in)
}

func (m *mockService) LoadModel(ctx context.Context, in domain.LoadModelInput) (domain.ModelStatus, error) {
	m.lastLoad = in
	if m.loadFn == nil {
		return domain.ModelStatus{}, errMockUnexpectedCall
	}
	return m.loadFn(ctx, in)
}

func (m *mockService) UnloadModel(ctx context.Context, ref domain.ModelRef, reason string) (domain.ModelStatus, error) {
	m.lastUnload.ref = ref
	m.lastUnload.reason = reason
	if m.unloadFn == nil {
		return domain.ModelStatus{}, errMockUnexpectedCall
	}
	return m.unloadFn(ctx, ref, reason)
}

func (m *mockService) GetModelStatus(ctx context.Context, ref domain.ModelRef) (domain.ModelStatus, error) {
	if m.getStatusFn == nil {
		return domain.ModelStatus{}, errMockUnexpectedCall
	}
	return m.getStatusFn(ctx, ref)
}

func (m *mockService) GetModelInfo(ctx context.Context, ref domain.ModelRef) (domain.LoadedModel, error) {
	if m.getInfoFn == nil {
		return domain.LoadedModel{}, errMockUnexpectedCall
	}
	return m.getInfoFn(ctx, ref)
}

func (m *mockService) ListLoadedModels(ctx context.Context, opts domain.ListOptions) ([]domain.ModelStatus, string, error) {
	m.lastList = opts
	if m.listFn == nil {
		return nil, "", errMockUnexpectedCall
	}
	return m.listFn(ctx, opts)
}

func (m *mockService) GetServingMetrics(ctx context.Context) domain.ServingMetrics {
	if m.metricsFn == nil {
		return domain.ServingMetrics{}
	}
	return m.metricsFn(ctx)
}

func (m *mockService) HealthCheck(ctx context.Context, name string) (domain.HealthVerdict, domain.ModelState, time.Duration) {
	if m.healthFn == nil {
		return domain.HealthUnspecified, domain.StateUnspecified, 0
	}
	return m.healthFn(ctx, name)
}

func (m *mockService) EnsureLoaded(ctx context.Context, in domain.LoadModelInput) (domain.ModelStatus, error) {
	if m.ensureLoadedFn == nil {
		return domain.ModelStatus{}, errMockUnexpectedCall
	}
	return m.ensureLoadedFn(ctx, in)
}

func (m *mockService) Unload(ctx context.Context, ref domain.ModelRef, reason string) (domain.ModelStatus, error) {
	return m.UnloadModel(ctx, ref, reason)
}

// compile-time assertion: the mock satisfies the interface the handler holds.
var _ domain.ServingService = (*mockService)(nil)

// ============================================================================
// TEST HARNESS
// ============================================================================

// newTestClient wires the handler (with the given mock) into a bufconn gRPC
// server and returns a connected client. Server + connection are cleaned up by
// the testutil helper's t.Cleanup.
func newTestClient(t *testing.T, svc domain.ServingService) servingv1.ModelServingServiceClient {
	t.Helper()
	conn := testutil.NewTestGRPCServer(t, func(s *grpc.Server) {
		servingv1.RegisterModelServingServiceServer(s, NewServingHandler(svc))
	})
	return servingv1.NewModelServingServiceClient(conn)
}

// assertCode fails unless err carries the expected gRPC status code.
func assertCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with code %s, got nil", want)
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected a gRPC status error, got %T: %v", err, err)
	}
	if st.Code() != want {
		t.Fatalf("expected code %s, got %s (message %q)", want, st.Code(), st.Message())
	}
}

// fp32Tensor builds a valid proto float32 TensorData for a 1-D shape, so tests
// share one well-formed input.
func fp32Tensor(n int) *servingv1.TensorData {
	return &servingv1.TensorData{
		Shape: []int64{int64(n)},
		Data:  make([]byte, n*4), // 4 bytes per float32
		Dtype: servingv1.DataType_DATA_TYPE_FLOAT32,
	}
}

// ============================================================================
// nil-svc guard — every RPC must return Unimplemented (never panic)
// ============================================================================

func TestNilService_ReturnsUnimplemented(t *testing.T) {
	// Handler constructed with a nil domain service (the not-yet-wired binary).
	client := newTestClient(t, nil)
	ctx := context.Background()

	t.Run("Predict", func(t *testing.T) {
		_, err := client.Predict(ctx, &servingv1.PredictRequest{Inputs: map[string]*servingv1.TensorData{"x": fp32Tensor(1)}})
		assertCode(t, err, codes.Unimplemented)
	})
	t.Run("LoadModel", func(t *testing.T) {
		_, err := client.LoadModel(ctx, &servingv1.LoadModelRequest{ModelName: "m", Version: "v1", ArtifactUri: "s3://b/x"})
		assertCode(t, err, codes.Unimplemented)
	})
	t.Run("GetModelInfo", func(t *testing.T) {
		_, err := client.GetModelInfo(ctx, &servingv1.GetModelInfoRequest{})
		assertCode(t, err, codes.Unimplemented)
	})
	t.Run("GetServingMetrics", func(t *testing.T) {
		_, err := client.GetServingMetrics(ctx, &servingv1.GetServingMetricsRequest{})
		assertCode(t, err, codes.Unimplemented)
	})
	t.Run("StreamPredict", func(t *testing.T) {
		stream, err := client.StreamPredict(ctx)
		if err != nil {
			t.Fatalf("opening stream: %v", err)
		}
		// The Unimplemented error surfaces on the first Recv (or the implicit
		// close), since the server returns it immediately.
		_ = stream.Send(&servingv1.StreamPredictRequest{Inputs: map[string]*servingv1.TensorData{"x": fp32Tensor(1)}})
		_, err = stream.Recv()
		assertCode(t, err, codes.Unimplemented)
	})
}

// ============================================================================
// Predict
// ============================================================================

func TestPredict_HappyPath(t *testing.T) {
	mock := &mockService{
		predictFn: func(_ context.Context, _ domain.PredictInput) (domain.PredictResult, error) {
			return domain.PredictResult{
				Outputs: map[string]domain.Tensor{
					"probs": {Shape: []int64{2}, Data: []byte{1, 2, 3, 4, 5, 6, 7, 8}, DType: domain.DataTypeFloat32},
				},
				ModelVersion:     "v7",
				InferenceLatency: 5 * time.Millisecond,
				CorrelationID:    "corr-123",
				FromCache:        true,
			}, nil
		},
	}
	client := newTestClient(t, mock)

	resp, err := client.Predict(context.Background(), &servingv1.PredictRequest{
		ModelName:      "fraud",
		Version:        "v7",
		Inputs:         map[string]*servingv1.TensorData{"features": fp32Tensor(4)},
		IdempotencyKey: "idem-1",
		CorrelationId:  "corr-123",
	})
	if err != nil {
		t.Fatalf("Predict returned error: %v", err)
	}

	// (a1) request → domain input conversion is correct.
	if mock.lastPredict.RequestedRef != (domain.ModelRef{Name: "fraud", Version: "v7"}) {
		t.Errorf("RequestedRef = %+v, want {fraud v7}", mock.lastPredict.RequestedRef)
	}
	if mock.lastPredict.IdempotencyKey != "idem-1" {
		t.Errorf("IdempotencyKey = %q, want idem-1", mock.lastPredict.IdempotencyKey)
	}
	if mock.lastPredict.CorrelationID != "corr-123" {
		t.Errorf("CorrelationID = %q, want corr-123", mock.lastPredict.CorrelationID)
	}
	gotIn, ok := mock.lastPredict.Inputs["features"]
	if !ok {
		t.Fatalf("input tensor 'features' not converted into domain input")
	}
	if gotIn.DType != domain.DataTypeFloat32 || len(gotIn.Data) != 16 {
		t.Errorf("converted input tensor = %+v, want float32 with 16 bytes", gotIn)
	}

	// (a2) domain result → proto response conversion is correct.
	if resp.GetModelVersion() != "v7" {
		t.Errorf("ModelVersion = %q, want v7", resp.GetModelVersion())
	}
	if !resp.GetFromCache() {
		t.Errorf("FromCache = false, want true")
	}
	if resp.GetCorrelationId() != "corr-123" {
		t.Errorf("CorrelationId = %q, want corr-123", resp.GetCorrelationId())
	}
	if resp.GetInferenceLatency().AsDuration() != 5*time.Millisecond {
		t.Errorf("InferenceLatency = %v, want 5ms", resp.GetInferenceLatency().AsDuration())
	}
	out, ok := resp.GetOutputs()["probs"]
	if !ok {
		t.Fatalf("output tensor 'probs' missing from response")
	}
	if out.GetDtype() != servingv1.DataType_DATA_TYPE_FLOAT32 || len(out.GetData()) != 8 {
		t.Errorf("output tensor = %+v, want float32 with 8 bytes", out)
	}
}

func TestPredict_Validation(t *testing.T) {
	// predictFn is nil ⇒ if the handler reaches the domain, the mock errors and
	// the test sees the wrong code, proving validation must short-circuit first.
	mock := &mockService{}
	client := newTestClient(t, mock)

	t.Run("no inputs", func(t *testing.T) {
		_, err := client.Predict(context.Background(), &servingv1.PredictRequest{ModelName: "m"})
		assertCode(t, err, codes.InvalidArgument)
	})

	t.Run("unspecified dtype", func(t *testing.T) {
		_, err := client.Predict(context.Background(), &servingv1.PredictRequest{
			Inputs: map[string]*servingv1.TensorData{
				"x": {Shape: []int64{1}, Data: []byte{0, 0, 0, 0}, Dtype: servingv1.DataType_DATA_TYPE_UNSPECIFIED},
			},
		})
		assertCode(t, err, codes.InvalidArgument)
	})

	t.Run("garbage dtype", func(t *testing.T) {
		_, err := client.Predict(context.Background(), &servingv1.PredictRequest{
			Inputs: map[string]*servingv1.TensorData{
				"x": {Shape: []int64{1}, Data: []byte{0}, Dtype: servingv1.DataType(99)},
			},
		})
		assertCode(t, err, codes.InvalidArgument)
	})
}

func TestPredict_ErrorMapping(t *testing.T) {
	cases := []struct {
		name    string
		domErr  error
		want    codes.Code
		secret  string // a token that must NOT appear in the client message
		hasLeak bool   // whether domErr carries that secret
	}{
		{name: "not ready", domErr: fmt.Errorf("%w: still loading", domain.ErrModelNotReady), want: codes.FailedPrecondition},
		{name: "request mismatch", domErr: fmt.Errorf("%w: wrong pod", domain.ErrModelRequestMismatch), want: codes.FailedPrecondition},
		{name: "validation", domErr: fmt.Errorf("%w: too many input tensors", domain.ErrValidation), want: codes.InvalidArgument},
		{name: "engine bad input", domErr: fmt.Errorf("%w: %w: bad shape", domain.ErrInferenceFailed, domain.ErrEngineBadInput), want: codes.InvalidArgument},
		{
			name:    "engine internal fault",
			domErr:  fmt.Errorf("%w: onnxruntime segfault at /opt/secret/model.onnx", domain.ErrInferenceFailed),
			want:    codes.Internal,
			secret:  "/opt/secret/model.onnx",
			hasLeak: true,
		},
		{
			name:    "unknown error leaks nothing",
			domErr:  errors.New("panic: db password=hunter2 leaked into error"),
			want:    codes.Internal,
			secret:  "hunter2",
			hasLeak: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockService{
				predictFn: func(_ context.Context, _ domain.PredictInput) (domain.PredictResult, error) {
					return domain.PredictResult{}, tc.domErr
				},
			}
			client := newTestClient(t, mock)

			_, err := client.Predict(context.Background(), &servingv1.PredictRequest{
				Inputs: map[string]*servingv1.TensorData{"x": fp32Tensor(1)},
			})
			assertCode(t, err, tc.want)

			// (d) no-leak: for the Internal cases assert the sanitized message and
			// that the secret never reaches the wire.
			st, _ := status.FromError(err)
			if tc.want == codes.Internal {
				if st.Message() != sanitizedInternal {
					t.Errorf("Internal message = %q, want sanitized %q", st.Message(), sanitizedInternal)
				}
			}
			if tc.hasLeak && containsSubstr(st.Message(), tc.secret) {
				t.Errorf("client message %q leaked secret %q", st.Message(), tc.secret)
			}
		})
	}
}

// ============================================================================
// LoadModel
// ============================================================================

func TestLoadModel_HappyPath(t *testing.T) {
	now := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	mock := &mockService{
		loadFn: func(_ context.Context, _ domain.LoadModelInput) (domain.ModelStatus, error) {
			return domain.ModelStatus{
				Ref:       domain.ModelRef{Name: "fraud", Version: "v7"},
				State:     domain.StateDownloading,
				Message:   "pulling",
				UpdatedAt: now,
			}, nil
		},
	}
	client := newTestClient(t, mock)

	resp, err := client.LoadModel(context.Background(), &servingv1.LoadModelRequest{
		ModelName:      "fraud",
		Version:        "v7",
		ArtifactUri:    "s3://fp-models/fraud/v7.onnx",
		ExpectedDigest: "sha256:abc",
		IdempotencyKey: "load-1",
	})
	if err != nil {
		t.Fatalf("LoadModel returned error: %v", err)
	}

	// (a) request → domain input. These are the fields the domain trusts from the
	// caller (a serving pod has no owner field to override).
	if mock.lastLoad.Ref != (domain.ModelRef{Name: "fraud", Version: "v7"}) {
		t.Errorf("Ref = %+v, want {fraud v7}", mock.lastLoad.Ref)
	}
	if mock.lastLoad.ArtifactURI != "s3://fp-models/fraud/v7.onnx" {
		t.Errorf("ArtifactURI = %q", mock.lastLoad.ArtifactURI)
	}
	if mock.lastLoad.ExpectedDigest != "sha256:abc" || mock.lastLoad.IdempotencyKey != "load-1" {
		t.Errorf("digest/idem mis-converted: %+v", mock.lastLoad)
	}

	// (a) domain result → proto response.
	gotSt := resp.GetStatus()
	if gotSt.GetModelName() != "fraud" || gotSt.GetVersion() != "v7" {
		t.Errorf("status ref = %s/%s", gotSt.GetModelName(), gotSt.GetVersion())
	}
	if gotSt.GetState() != servingv1.ModelState_MODEL_STATE_DOWNLOADING {
		t.Errorf("state = %s, want DOWNLOADING", gotSt.GetState())
	}
	if gotSt.GetUpdatedAt().AsTime() != now {
		t.Errorf("updated_at = %v, want %v", gotSt.GetUpdatedAt().AsTime(), now)
	}
}

func TestLoadModel_Validation(t *testing.T) {
	mock := &mockService{} // loadFn nil ⇒ must not be reached
	client := newTestClient(t, mock)

	cases := []struct {
		name string
		req  *servingv1.LoadModelRequest
	}{
		{"missing name", &servingv1.LoadModelRequest{Version: "v1", ArtifactUri: "s3://b/x"}},
		{"missing version", &servingv1.LoadModelRequest{ModelName: "m", ArtifactUri: "s3://b/x"}},
		{"missing uri", &servingv1.LoadModelRequest{ModelName: "m", Version: "v1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.LoadModel(context.Background(), tc.req)
			assertCode(t, err, codes.InvalidArgument)
		})
	}
}

func TestLoadModel_ErrorMapping(t *testing.T) {
	cases := []struct {
		name   string
		domErr error
		want   codes.Code
	}{
		{"ssrf reject", fmt.Errorf("%w: s3://evil/x", domain.ErrArtifactURINotAllowed), codes.PermissionDenied},
		{"already exists", fmt.Errorf("%w: fraud/v7", domain.ErrModelAlreadyExists), codes.AlreadyExists},
		{"validation", fmt.Errorf("%w: bad", domain.ErrValidation), codes.InvalidArgument},
		{"digest mismatch", fmt.Errorf("%w", domain.ErrDigestMismatch), codes.FailedPrecondition},
		{"unknown", errors.New("internal boom"), codes.Internal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockService{
				loadFn: func(_ context.Context, _ domain.LoadModelInput) (domain.ModelStatus, error) {
					return domain.ModelStatus{}, tc.domErr
				},
			}
			client := newTestClient(t, mock)
			_, err := client.LoadModel(context.Background(), &servingv1.LoadModelRequest{
				ModelName: "m", Version: "v1", ArtifactUri: "s3://b/x",
			})
			assertCode(t, err, tc.want)
		})
	}
}

// PermissionDenied must not echo the rejected URI back (don't confirm allow-list
// contents to a prober).
func TestLoadModel_SSRF_DoesNotEchoURI(t *testing.T) {
	mock := &mockService{
		loadFn: func(_ context.Context, _ domain.LoadModelInput) (domain.ModelStatus, error) {
			return domain.ModelStatus{}, fmt.Errorf("%w: s3://attacker-bucket/payload.onnx", domain.ErrArtifactURINotAllowed)
		},
	}
	client := newTestClient(t, mock)
	_, err := client.LoadModel(context.Background(), &servingv1.LoadModelRequest{
		ModelName: "m", Version: "v1", ArtifactUri: "s3://attacker-bucket/payload.onnx",
	})
	assertCode(t, err, codes.PermissionDenied)
	st, _ := status.FromError(err)
	if containsSubstr(st.Message(), "attacker-bucket") {
		t.Errorf("PermissionDenied message %q leaked the rejected URI", st.Message())
	}
}

// ============================================================================
// UnloadModel
// ============================================================================

func TestUnloadModel_HappyPath(t *testing.T) {
	mock := &mockService{
		unloadFn: func(_ context.Context, _ domain.ModelRef, _ string) (domain.ModelStatus, error) {
			return domain.ModelStatus{
				Ref:   domain.ModelRef{Name: "fraud", Version: "v7"},
				State: domain.StateUnloaded,
			}, nil
		},
	}
	client := newTestClient(t, mock)

	resp, err := client.UnloadModel(context.Background(), &servingv1.UnloadModelRequest{
		ModelName: "fraud", Version: "v7", Reason: "undeployed",
	})
	if err != nil {
		t.Fatalf("UnloadModel returned error: %v", err)
	}
	if mock.lastUnload.ref != (domain.ModelRef{Name: "fraud", Version: "v7"}) || mock.lastUnload.reason != "undeployed" {
		t.Errorf("unload args mis-converted: %+v", mock.lastUnload)
	}
	if resp.GetStatus().GetState() != servingv1.ModelState_MODEL_STATE_UNLOADED {
		t.Errorf("state = %s, want UNLOADED", resp.GetStatus().GetState())
	}
}

func TestUnloadModel_ErrorMapping(t *testing.T) {
	mock := &mockService{
		unloadFn: func(_ context.Context, _ domain.ModelRef, _ string) (domain.ModelStatus, error) {
			return domain.ModelStatus{}, errors.New("boom with secret /var/run/token")
		},
	}
	client := newTestClient(t, mock)
	_, err := client.UnloadModel(context.Background(), &servingv1.UnloadModelRequest{})
	assertCode(t, err, codes.Internal)
	st, _ := status.FromError(err)
	if st.Message() != sanitizedInternal {
		t.Errorf("message = %q, want sanitized", st.Message())
	}
}

// ============================================================================
// GetModelStatus
// ============================================================================

func TestGetModelStatus_HappyPath(t *testing.T) {
	mock := &mockService{
		getStatusFn: func(_ context.Context, ref domain.ModelRef) (domain.ModelStatus, error) {
			// Echo the ref so we can assert the handler forwarded it.
			return domain.ModelStatus{Ref: ref, State: domain.StateReady, Message: "ok"}, nil
		},
	}
	client := newTestClient(t, mock)

	resp, err := client.GetModelStatus(context.Background(), &servingv1.GetModelStatusRequest{
		ModelName: "fraud", Version: "v7",
	})
	if err != nil {
		t.Fatalf("GetModelStatus returned error: %v", err)
	}
	if resp.GetStatus().GetState() != servingv1.ModelState_MODEL_STATE_READY {
		t.Errorf("state = %s, want READY", resp.GetStatus().GetState())
	}
	if resp.GetStatus().GetModelName() != "fraud" || resp.GetStatus().GetVersion() != "v7" {
		t.Errorf("ref not forwarded: %s/%s", resp.GetStatus().GetModelName(), resp.GetStatus().GetVersion())
	}
}

func TestGetModelStatus_NotFound(t *testing.T) {
	mock := &mockService{
		getStatusFn: func(_ context.Context, _ domain.ModelRef) (domain.ModelStatus, error) {
			return domain.ModelStatus{}, fmt.Errorf("%w", domain.ErrModelNotFound)
		},
	}
	client := newTestClient(t, mock)
	_, err := client.GetModelStatus(context.Background(), &servingv1.GetModelStatusRequest{})
	assertCode(t, err, codes.NotFound)
}

// ============================================================================
// GetModelInfo
// ============================================================================

func TestGetModelInfo_HappyPath(t *testing.T) {
	loadedAt := time.Date(2026, 6, 17, 8, 30, 0, 0, time.UTC)
	mock := &mockService{
		getInfoFn: func(_ context.Context, _ domain.ModelRef) (domain.LoadedModel, error) {
			return domain.LoadedModel{
				Ref:            domain.ModelRef{Name: "fraud", Version: "v7"},
				ArtifactDigest: "sha256:abc",
				LoadedAt:       loadedAt,
				InputSchema:    []domain.TensorSpec{{Name: "features", Shape: []int64{-1, 4}, DType: domain.DataTypeFloat32}},
				OutputSchema:   []domain.TensorSpec{{Name: "probs", Shape: []int64{-1, 2}, DType: domain.DataTypeFloat32}},
			}, nil
		},
	}
	client := newTestClient(t, mock)

	resp, err := client.GetModelInfo(context.Background(), &servingv1.GetModelInfoRequest{})
	if err != nil {
		t.Fatalf("GetModelInfo returned error: %v", err)
	}
	info := resp.GetModelInfo()
	if info.GetName() != "fraud" || info.GetVersion() != "v7" {
		t.Errorf("identity = %s/%s", info.GetName(), info.GetVersion())
	}
	if info.GetArtifactDigest() != "sha256:abc" {
		t.Errorf("digest = %q", info.GetArtifactDigest())
	}
	if info.GetLoadedAt().AsTime() != loadedAt {
		t.Errorf("loaded_at = %v, want %v", info.GetLoadedAt().AsTime(), loadedAt)
	}
	if len(info.GetInputSchema()) != 1 || info.GetInputSchema()[0].GetName() != "features" {
		t.Errorf("input schema not converted: %+v", info.GetInputSchema())
	}
	if info.GetInputSchema()[0].GetDtype() != servingv1.DataType_DATA_TYPE_FLOAT32 {
		t.Errorf("input dtype = %s", info.GetInputSchema()[0].GetDtype())
	}
	if len(info.GetOutputSchema()) != 1 || info.GetOutputSchema()[0].GetName() != "probs" {
		t.Errorf("output schema not converted: %+v", info.GetOutputSchema())
	}
}

// A never-loaded model has a zero LoadedAt; the handler must map that to a NIL
// timestamp, never a 1970 epoch value.
func TestGetModelInfo_ZeroLoadedAt_IsNilTimestamp(t *testing.T) {
	mock := &mockService{
		getInfoFn: func(_ context.Context, _ domain.ModelRef) (domain.LoadedModel, error) {
			return domain.LoadedModel{Ref: domain.ModelRef{Name: "m", Version: "v1"}}, nil // zero LoadedAt
		},
	}
	client := newTestClient(t, mock)
	resp, err := client.GetModelInfo(context.Background(), &servingv1.GetModelInfoRequest{})
	if err != nil {
		t.Fatalf("GetModelInfo returned error: %v", err)
	}
	if resp.GetModelInfo().GetLoadedAt() != nil {
		t.Errorf("loaded_at = %v, want nil for zero time", resp.GetModelInfo().GetLoadedAt())
	}
}

func TestGetModelInfo_NotFound(t *testing.T) {
	mock := &mockService{
		getInfoFn: func(_ context.Context, _ domain.ModelRef) (domain.LoadedModel, error) {
			return domain.LoadedModel{}, domain.ErrModelNotFound
		},
	}
	client := newTestClient(t, mock)
	_, err := client.GetModelInfo(context.Background(), &servingv1.GetModelInfoRequest{})
	assertCode(t, err, codes.NotFound)
}

// ============================================================================
// ListLoadedModels
// ============================================================================

func TestListLoadedModels_HappyPath(t *testing.T) {
	mock := &mockService{
		listFn: func(_ context.Context, _ domain.ListOptions) ([]domain.ModelStatus, string, error) {
			return []domain.ModelStatus{
				{Ref: domain.ModelRef{Name: "fraud", Version: "v7"}, State: domain.StateReady},
			}, "next-cursor", nil
		},
	}
	client := newTestClient(t, mock)

	resp, err := client.ListLoadedModels(context.Background(), &servingv1.ListLoadedModelsRequest{
		Pagination:  &commonv1.PaginationRequest{PageSize: 10, PageToken: "cur"},
		StateFilter: servingv1.ModelState_MODEL_STATE_READY,
	})
	if err != nil {
		t.Fatalf("ListLoadedModels returned error: %v", err)
	}

	// (a) request → domain options conversion (incl. the enum filter mapping).
	if mock.lastList.PageSize != 10 || mock.lastList.PageToken != "cur" {
		t.Errorf("pagination mis-converted: %+v", mock.lastList)
	}
	if mock.lastList.StateFilter != domain.StateReady {
		t.Errorf("state filter = %v, want StateReady", mock.lastList.StateFilter)
	}

	// (a) domain result → proto response.
	if len(resp.GetModels()) != 1 || resp.GetModels()[0].GetModelName() != "fraud" {
		t.Errorf("models not converted: %+v", resp.GetModels())
	}
	if resp.GetPagination().GetNextPageToken() != "next-cursor" {
		t.Errorf("next token = %q", resp.GetPagination().GetNextPageToken())
	}
	if resp.GetPagination().GetTotalCount() != 1 {
		t.Errorf("total count = %d, want 1", resp.GetPagination().GetTotalCount())
	}
}

func TestListLoadedModels_Validation(t *testing.T) {
	mock := &mockService{} // listFn nil ⇒ must not be reached on a rejected request
	client := newTestClient(t, mock)

	t.Run("negative page size", func(t *testing.T) {
		_, err := client.ListLoadedModels(context.Background(), &servingv1.ListLoadedModelsRequest{
			Pagination: &commonv1.PaginationRequest{PageSize: -5},
		})
		assertCode(t, err, codes.InvalidArgument)
	})

	t.Run("garbage state filter", func(t *testing.T) {
		_, err := client.ListLoadedModels(context.Background(), &servingv1.ListLoadedModelsRequest{
			StateFilter: servingv1.ModelState(42),
		})
		assertCode(t, err, codes.InvalidArgument)
	})
}

// A page_size above the contract cap is NOT rejected by the handler — it is
// passed through for the domain to clamp. We assert the handler forwarded the
// raw value (the clamp is the domain's tested responsibility).
func TestListLoadedModels_OversizePagePassedThroughForClamp(t *testing.T) {
	mock := &mockService{
		listFn: func(_ context.Context, _ domain.ListOptions) ([]domain.ModelStatus, string, error) {
			return nil, "", nil
		},
	}
	client := newTestClient(t, mock)
	_, err := client.ListLoadedModels(context.Background(), &servingv1.ListLoadedModelsRequest{
		Pagination: &commonv1.PaginationRequest{PageSize: 100000},
	})
	if err != nil {
		t.Fatalf("ListLoadedModels returned error: %v", err)
	}
	if mock.lastList.PageSize != 100000 {
		t.Errorf("PageSize = %d, want raw 100000 forwarded for domain clamp", mock.lastList.PageSize)
	}
}

// ============================================================================
// GetServingMetrics
// ============================================================================

func TestGetServingMetrics_HappyPath(t *testing.T) {
	mock := &mockService{
		metricsFn: func(_ context.Context) domain.ServingMetrics {
			return domain.ServingMetrics{
				InflightRequests:     3,
				TotalRequests:        100,
				FailedRequests:       2,
				P50Latency:           5 * time.Millisecond,
				P99Latency:           20 * time.Millisecond,
				LastInferenceLatency: 6 * time.Millisecond,
				ModelMemoryBytes:     1 << 20,
			}
		},
	}
	client := newTestClient(t, mock)

	resp, err := client.GetServingMetrics(context.Background(), &servingv1.GetServingMetricsRequest{})
	if err != nil {
		t.Fatalf("GetServingMetrics returned error: %v", err)
	}
	m := resp.GetMetrics()
	if m.GetInflightRequests() != 3 || m.GetTotalRequests() != 100 || m.GetFailedRequests() != 2 {
		t.Errorf("counters mis-converted: %+v", m)
	}
	if m.GetP50Latency().AsDuration() != 5*time.Millisecond || m.GetP99Latency().AsDuration() != 20*time.Millisecond {
		t.Errorf("latencies mis-converted: %+v", m)
	}
	if m.GetModelMemoryBytes() != 1<<20 {
		t.Errorf("memory = %d", m.GetModelMemoryBytes())
	}
}

// ============================================================================
// HealthCheck
// ============================================================================

func TestHealthCheck_HappyPath(t *testing.T) {
	cases := []struct {
		name      string
		verdict   domain.HealthVerdict
		state     domain.ModelState
		latency   time.Duration
		wantStat  servingv1.HealthStatus
		wantState servingv1.ModelState
	}{
		{
			name: "serving", verdict: domain.HealthServing, state: domain.StateReady, latency: 4 * time.Millisecond,
			wantStat: servingv1.HealthStatus_HEALTH_STATUS_SERVING, wantState: servingv1.ModelState_MODEL_STATE_READY,
		},
		{
			name: "not serving while loading", verdict: domain.HealthNotServing, state: domain.StateLoading, latency: 0,
			wantStat: servingv1.HealthStatus_HEALTH_STATUS_NOT_SERVING, wantState: servingv1.ModelState_MODEL_STATE_LOADING,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockService{
				healthFn: func(_ context.Context, _ string) (domain.HealthVerdict, domain.ModelState, time.Duration) {
					return tc.verdict, tc.state, tc.latency
				},
			}
			client := newTestClient(t, mock)
			resp, err := client.HealthCheck(context.Background(), &servingv1.HealthCheckRequest{})
			if err != nil {
				t.Fatalf("HealthCheck returned error: %v", err)
			}
			if resp.GetStatus() != tc.wantStat {
				t.Errorf("status = %s, want %s", resp.GetStatus(), tc.wantStat)
			}
			if resp.GetModelState() != tc.wantState {
				t.Errorf("model_state = %s, want %s", resp.GetModelState(), tc.wantState)
			}
			if resp.GetLastInferenceLatency().AsDuration() != tc.latency {
				t.Errorf("latency = %v, want %v", resp.GetLastInferenceLatency().AsDuration(), tc.latency)
			}
		})
	}
}

// HealthCheck must forward the model_name selector to the domain.
func TestHealthCheck_ForwardsModelName(t *testing.T) {
	var gotName string
	mock := &mockService{
		healthFn: func(_ context.Context, name string) (domain.HealthVerdict, domain.ModelState, time.Duration) {
			gotName = name
			return domain.HealthServing, domain.StateReady, 0
		},
	}
	client := newTestClient(t, mock)
	if _, err := client.HealthCheck(context.Background(), &servingv1.HealthCheckRequest{ModelName: "fraud"}); err != nil {
		t.Fatalf("HealthCheck returned error: %v", err)
	}
	if gotName != "fraud" {
		t.Errorf("forwarded model_name = %q, want fraud", gotName)
	}
}

// ============================================================================
// StreamPredict (bidi)
// ============================================================================

func TestStreamPredict_HappyPath_MultipleMessages(t *testing.T) {
	mock := &mockService{
		predictFn: func(_ context.Context, in domain.PredictInput) (domain.PredictResult, error) {
			// Echo the idempotency key into the version so we can verify pairing.
			// The real domain echoes the request's CorrelationID into the result;
			// the mock mirrors that so the handler's res.CorrelationID echo is
			// exercised end to end.
			return domain.PredictResult{
				Outputs:       map[string]domain.Tensor{"y": {Shape: []int64{1}, Data: []byte{9}, DType: domain.DataTypeBool}},
				ModelVersion:  "v-" + in.IdempotencyKey,
				CorrelationID: in.CorrelationID,
			}, nil
		},
	}
	client := newTestClient(t, mock)

	stream, err := client.StreamPredict(context.Background())
	if err != nil {
		t.Fatalf("opening stream: %v", err)
	}

	keys := []string{"a", "b", "c"}
	for _, k := range keys {
		if err := stream.Send(&servingv1.StreamPredictRequest{
			Inputs:         map[string]*servingv1.TensorData{"x": fp32Tensor(1)},
			IdempotencyKey: k,
			CorrelationId:  "corr-" + k,
		}); err != nil {
			t.Fatalf("send %q: %v", k, err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	got := map[string]string{} // idempotency_key → model_version
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		got[resp.GetIdempotencyKey()] = resp.GetModelVersion()
		if resp.GetCorrelationId() != "corr-"+resp.GetIdempotencyKey() {
			t.Errorf("correlation_id %q not paired with key %q", resp.GetCorrelationId(), resp.GetIdempotencyKey())
		}
	}
	for _, k := range keys {
		if got[k] != "v-"+k {
			t.Errorf("response for key %q = %q, want v-%s", k, got[k], k)
		}
	}
}

func TestStreamPredict_ValidationTerminatesStream(t *testing.T) {
	mock := &mockService{} // predictFn nil ⇒ a reached domain would error differently
	client := newTestClient(t, mock)

	stream, err := client.StreamPredict(context.Background())
	if err != nil {
		t.Fatalf("opening stream: %v", err)
	}
	// Send a message with no inputs — the handler must reject it and end the stream.
	if err := stream.Send(&servingv1.StreamPredictRequest{}); err != nil {
		t.Fatalf("send: %v", err)
	}
	_, err = stream.Recv()
	assertCode(t, err, codes.InvalidArgument)
}

func TestStreamPredict_DomainErrorTerminatesStream(t *testing.T) {
	mock := &mockService{
		predictFn: func(_ context.Context, _ domain.PredictInput) (domain.PredictResult, error) {
			return domain.PredictResult{}, fmt.Errorf("%w: still loading", domain.ErrModelNotReady)
		},
	}
	client := newTestClient(t, mock)

	stream, err := client.StreamPredict(context.Background())
	if err != nil {
		t.Fatalf("opening stream: %v", err)
	}
	if err := stream.Send(&servingv1.StreamPredictRequest{
		Inputs: map[string]*servingv1.TensorData{"x": fp32Tensor(1)},
	}); err != nil {
		t.Fatalf("send: %v", err)
	}
	_, err = stream.Recv()
	assertCode(t, err, codes.FailedPrecondition)
}

// Cancelling the client context must surface as Canceled, and the handler must
// stop calling the domain (honoring ctx). We block the first Predict until the
// context is cancelled to deterministically exercise the cancellation path.
func TestStreamPredict_ContextCancellation(t *testing.T) {
	mock := &mockService{
		predictFn: func(ctx context.Context, _ domain.PredictInput) (domain.PredictResult, error) {
			<-ctx.Done() // block until the client cancels
			return domain.PredictResult{}, ctx.Err()
		},
	}
	client := newTestClient(t, mock)

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.StreamPredict(ctx)
	if err != nil {
		t.Fatalf("opening stream: %v", err)
	}
	if err := stream.Send(&servingv1.StreamPredictRequest{
		Inputs: map[string]*servingv1.TensorData{"x": fp32Tensor(1)},
	}); err != nil {
		t.Fatalf("send: %v", err)
	}

	// Cancel shortly after sending; the in-flight Predict unblocks via ctx.Done.
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	_, err = stream.Recv()
	// Client-observed code is Canceled when the caller cancels its own context.
	assertCode(t, err, codes.Canceled)
}

// ============================================================================
// helpers
// ============================================================================

// containsSubstr reports whether s contains sub. Tiny local helper to avoid a
// strings import solely for the leak assertions (keeps intent obvious inline).
func containsSubstr(s, sub string) bool {
	if sub == "" {
		return false
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
