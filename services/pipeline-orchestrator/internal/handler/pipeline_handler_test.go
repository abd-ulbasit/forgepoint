// pipeline_handler_test.go — COMPONENT tests for the gRPC handler.
//
// ============================================================================
// WHAT THESE TESTS ISOLATE (and why a MOCK SERVICE, not mock repos)
// ============================================================================
//
// These are HANDLER tests. Their job is to prove the handler's three
// responsibilities in isolation:
//
//	(a) proto → domain conversion is correct (the right domain input reaches
//	    the service, with identity pulled from CONTEXT CLAIMS, never the request);
//	(b) request validation rejects malformed input with codes.InvalidArgument
//	    BEFORE the service is ever called;
//	(c) each domain sentinel error maps to the right gRPC status code;
//	(d) NO internal detail leaks in error messages.
//
// We therefore mock the domain.PipelineService INTERFACE (a hand-written stub
// that records the input it received and returns canned results/errors). We do
// NOT mock the repositories — mocking repos would test the domain service too,
// blurring the layer under test. The real domain service has its own unit tests
// (pipeline_service_test.go) against mock repos; here the service is a black box.
//
// TRANSPORT: a real in-process gRPC server over bufconn (pkg/testutil), so the
// full gRPC stack runs — proto (de)serialization, the auth interceptor, status
// codes on the wire — without a real network or any container. The auth
// interceptor is installed with a stub validator so we control the Claims that
// reach the handler (and can also exercise the unauthenticated path).
// ============================================================================
package handler_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	pipelinev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/pipeline/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/handler"
)

// ============================================================================
// MOCK DOMAIN SERVICE
// ============================================================================
//
// mockService implements domain.PipelineService. Each method returns a function
// field so a test can program per-call behavior, and records the inputs it saw so
// a test can assert the handler converted the request correctly. A nil function
// field means "the test didn't expect this method to be called" → fail loudly.

type mockService struct {
	createFn func(ctx context.Context, actor domain.Actor, in domain.CreatePipelineInput) (domain.PipelineDefinition, error)
	getFn    func(ctx context.Context, actor domain.Actor, id string) (domain.PipelineDefinition, error)
	updateFn func(ctx context.Context, actor domain.Actor, in domain.UpdatePipelineInput) (domain.PipelineDefinition, error)
	deleteFn func(ctx context.Context, actor domain.Actor, id string) error
	listPFn  func(ctx context.Context, actor domain.Actor, f domain.ListPipelinesFilter) ([]domain.PipelineDefinition, string, error)

	triggerFn func(ctx context.Context, actor domain.Actor, in domain.TriggerInput) (domain.Execution, error)
	getExecFn func(ctx context.Context, actor domain.Actor, id string) (domain.Execution, error)
	cancelFn  func(ctx context.Context, actor domain.Actor, id, reason string) (domain.Execution, error)
	listEFn   func(ctx context.Context, actor domain.Actor, f domain.ListExecutionsFilter) ([]domain.Execution, string, error)

	// Recorded inputs (last call) for conversion assertions.
	gotActor     domain.Actor
	gotCreateIn  domain.CreatePipelineInput
	gotUpdateIn  domain.UpdatePipelineInput
	gotTriggerIn domain.TriggerInput
	gotCancelID  string
	gotCancelMsg string
	gotListPFil  domain.ListPipelinesFilter
	gotListEFil  domain.ListExecutionsFilter
	gotGetID     string
	gotGetExecID string
	gotDeleteID  string
	getExecCalls int // for the watch loop: count polls
}

func (m *mockService) CreatePipeline(ctx context.Context, actor domain.Actor, in domain.CreatePipelineInput) (domain.PipelineDefinition, error) {
	m.gotActor, m.gotCreateIn = actor, in
	if m.createFn == nil {
		return domain.PipelineDefinition{}, fmt.Errorf("unexpected CreatePipeline call")
	}
	return m.createFn(ctx, actor, in)
}

func (m *mockService) GetPipeline(ctx context.Context, actor domain.Actor, id string) (domain.PipelineDefinition, error) {
	m.gotActor, m.gotGetID = actor, id
	if m.getFn == nil {
		return domain.PipelineDefinition{}, fmt.Errorf("unexpected GetPipeline call")
	}
	return m.getFn(ctx, actor, id)
}

func (m *mockService) UpdatePipeline(ctx context.Context, actor domain.Actor, in domain.UpdatePipelineInput) (domain.PipelineDefinition, error) {
	m.gotActor, m.gotUpdateIn = actor, in
	if m.updateFn == nil {
		return domain.PipelineDefinition{}, fmt.Errorf("unexpected UpdatePipeline call")
	}
	return m.updateFn(ctx, actor, in)
}

func (m *mockService) DeletePipeline(ctx context.Context, actor domain.Actor, id string) error {
	m.gotActor, m.gotDeleteID = actor, id
	if m.deleteFn == nil {
		return fmt.Errorf("unexpected DeletePipeline call")
	}
	return m.deleteFn(ctx, actor, id)
}

func (m *mockService) ListPipelines(ctx context.Context, actor domain.Actor, f domain.ListPipelinesFilter) ([]domain.PipelineDefinition, string, error) {
	m.gotActor, m.gotListPFil = actor, f
	if m.listPFn == nil {
		return nil, "", fmt.Errorf("unexpected ListPipelines call")
	}
	return m.listPFn(ctx, actor, f)
}

func (m *mockService) TriggerExecution(ctx context.Context, actor domain.Actor, in domain.TriggerInput) (domain.Execution, error) {
	m.gotActor, m.gotTriggerIn = actor, in
	if m.triggerFn == nil {
		return domain.Execution{}, fmt.Errorf("unexpected TriggerExecution call")
	}
	return m.triggerFn(ctx, actor, in)
}

func (m *mockService) GetExecution(ctx context.Context, actor domain.Actor, id string) (domain.Execution, error) {
	m.gotActor, m.gotGetExecID = actor, id
	m.getExecCalls++
	if m.getExecFn == nil {
		return domain.Execution{}, fmt.Errorf("unexpected GetExecution call")
	}
	return m.getExecFn(ctx, actor, id)
}

func (m *mockService) CancelExecution(ctx context.Context, actor domain.Actor, id, reason string) (domain.Execution, error) {
	m.gotActor, m.gotCancelID, m.gotCancelMsg = actor, id, reason
	if m.cancelFn == nil {
		return domain.Execution{}, fmt.Errorf("unexpected CancelExecution call")
	}
	return m.cancelFn(ctx, actor, id, reason)
}

func (m *mockService) ListExecutions(ctx context.Context, actor domain.Actor, f domain.ListExecutionsFilter) ([]domain.Execution, string, error) {
	m.gotActor, m.gotListEFil = actor, f
	if m.listEFn == nil {
		return nil, "", fmt.Errorf("unexpected ListExecutions call")
	}
	return m.listEFn(ctx, actor, f)
}

// compile-time proof the mock satisfies the interface the handler depends on.
var _ domain.PipelineService = (*mockService)(nil)

// ============================================================================
// STUB TOKEN VALIDATOR + TEST RIG
// ============================================================================

// testUserID / testTeam are the identity the stub validator injects. The tests
// assert the handler stamps the Actor from THESE (context claims), never from a
// request field.
const (
	testUserID = "user-abc"
	testTeam   = "team-xyz"
	testToken  = "valid-token"
)

// stubValidator implements grpcutil.TokenValidator. A request carrying the
// "valid-token" bearer gets the test claims; anything else is rejected so we can
// also exercise the unauthenticated path.
type stubValidator struct{}

func (stubValidator) Validate(ctx context.Context, token string) (*grpcutil.Claims, error) {
	if token != testToken {
		return nil, fmt.Errorf("invalid token")
	}
	return &grpcutil.Claims{UserID: testUserID, Team: testTeam, Role: "engineer"}, nil
}

// newClient spins up an in-process gRPC server with the handler wired to the
// given mock service, behind the real auth interceptor (stub validator). Returns
// a ready client. The auth interceptor is included so the Actor-from-claims path
// and the unauthenticated path are exercised through the real chain.
func newClient(t *testing.T, svc domain.PipelineService) pipelinev1.PipelineOrchestratorServiceClient {
	t.Helper()
	h := handler.NewPipelineHandler(svc)
	conn := testutil.NewTestGRPCServer(t,
		func(s *grpc.Server) {
			pipelinev1.RegisterPipelineOrchestratorServiceServer(s, h)
		},
		grpc.ChainUnaryInterceptor(grpcutil.AuthUnaryInterceptor(stubValidator{})),
		grpc.ChainStreamInterceptor(grpcutil.AuthStreamInterceptor(stubValidator{})),
	)
	return pipelinev1.NewPipelineOrchestratorServiceClient(conn)
}

// authCtx returns a context carrying the valid bearer token so the interceptor
// authenticates it and injects the test claims.
func authCtx() context.Context {
	return metadata.NewOutgoingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer "+testToken))
}

// noAuthCtx carries no credentials → the interceptor rejects it as Unauthenticated.
func noAuthCtx() context.Context { return context.Background() }

// mustStruct builds a *structpb.Struct or fails the test.
func mustStruct(t *testing.T, m map[string]any) *structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(m)
	if err != nil {
		t.Fatalf("structpb.NewStruct: %v", err)
	}
	return s
}

// pagReq builds a common PaginationRequest pointer for list-RPC tests.
func pagReq(pageSize int32, token string) *commonv1.PaginationRequest {
	return &commonv1.PaginationRequest{PageSize: pageSize, PageToken: token}
}

// ============================================================================
// CreatePipeline
// ============================================================================

func TestCreatePipeline_HappyPath_ConversionBothDirections(t *testing.T) {
	t.Parallel()

	var captured domain.CreatePipelineInput
	var capturedActor domain.Actor
	created := time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC)

	m := &mockService{
		createFn: func(_ context.Context, actor domain.Actor, in domain.CreatePipelineInput) (domain.PipelineDefinition, error) {
			captured, capturedActor = in, actor
			return domain.PipelineDefinition{
				ID:        "pl-1",
				Name:      in.Name,
				Type:      in.Type,
				Steps:     in.Steps,
				CreatedBy: actor.Subject,
				CreatedAt: created,
				Team:      actor.Team,
			}, nil
		},
	}
	client := newClient(t, m)

	req := &pipelinev1.CreatePipelineRequest{
		Name: "deploy-fraud-model",
		Type: pipelinev1.PipelineType_PIPELINE_TYPE_DEPLOYMENT_SAGA,
		Steps: []*pipelinev1.StepDefinition{
			{
				Id:         "validate",
				Name:       "Validate model",
				Type:       pipelinev1.StepType_STEP_TYPE_VALIDATE,
				MaxRetries: 2,
				Timeout:    nil,
				Config:     mustStruct(t, map[string]any{"model_id": "m-7"}),
			},
			{
				Id:                 "deploy",
				Type:               pipelinev1.StepType_STEP_TYPE_DEPLOY,
				DependsOn:          []string{"validate"},
				CompensationStepId: "validate",
			},
		},
		IdempotencyKey: "idem-1",
	}

	resp, err := client.CreatePipeline(authCtx(), req)
	if err != nil {
		t.Fatalf("CreatePipeline: unexpected error: %v", err)
	}

	// --- (a) inbound conversion: domain input matches the request ---
	if captured.Name != "deploy-fraud-model" {
		t.Errorf("Name = %q, want %q", captured.Name, "deploy-fraud-model")
	}
	if captured.Type != domain.PipelineTypeDeploymentSaga {
		t.Errorf("Type = %v, want DeploymentSaga", captured.Type)
	}
	if captured.IdempotencyKey != "idem-1" {
		t.Errorf("IdempotencyKey = %q, want %q", captured.IdempotencyKey, "idem-1")
	}
	if len(captured.Steps) != 2 {
		t.Fatalf("Steps len = %d, want 2", len(captured.Steps))
	}
	if captured.Steps[0].Type != domain.StepTypeValidate || captured.Steps[0].MaxRetries != 2 {
		t.Errorf("step[0] = %+v, want VALIDATE maxRetries=2", captured.Steps[0])
	}
	if captured.Steps[0].Config["model_id"] != "m-7" {
		t.Errorf("step[0].Config[model_id] = %v, want m-7", captured.Steps[0].Config["model_id"])
	}
	if captured.Steps[1].DependsOn[0] != "validate" || captured.Steps[1].CompensationStepID != "validate" {
		t.Errorf("step[1] edges = %+v, want depends/comp validate", captured.Steps[1])
	}

	// --- identity comes from CLAIMS, not the request (no team field exists) ---
	if capturedActor.Subject != testUserID || capturedActor.Team != testTeam {
		t.Errorf("Actor = %+v, want {%s %s} from claims", capturedActor, testUserID, testTeam)
	}

	// --- (a) outbound conversion: proto response matches the domain result ---
	got := resp.GetPipeline()
	if got.GetId() != "pl-1" || got.GetName() != "deploy-fraud-model" {
		t.Errorf("resp id/name = %q/%q", got.GetId(), got.GetName())
	}
	if got.GetCreatedBy() != testUserID || got.GetTeam() != testTeam {
		t.Errorf("resp createdBy/team = %q/%q, want server-authoritative", got.GetCreatedBy(), got.GetTeam())
	}
	if !got.GetCreatedAt().AsTime().Equal(created) {
		t.Errorf("resp createdAt = %v, want %v", got.GetCreatedAt().AsTime(), created)
	}
	if got.GetType() != pipelinev1.PipelineType_PIPELINE_TYPE_DEPLOYMENT_SAGA {
		t.Errorf("resp type = %v", got.GetType())
	}
	if len(got.GetSteps()) != 2 {
		t.Fatalf("resp steps = %d, want 2", len(got.GetSteps()))
	}
	if got.GetSteps()[1].GetType() != pipelinev1.StepType_STEP_TYPE_DEPLOY {
		t.Errorf("resp step[1] type = %v, want DEPLOY", got.GetSteps()[1].GetType())
	}
}

func TestCreatePipeline_Validation(t *testing.T) {
	t.Parallel()

	// The service must NOT be reached for any of these — a nil-funcs mock makes a
	// stray call fail (returns the "unexpected ..." error → Internal, which would
	// also fail the wantCode==InvalidArgument assertion).
	tests := []struct {
		name string
		req  *pipelinev1.CreatePipelineRequest
	}{
		{
			name: "unspecified type",
			req: &pipelinev1.CreatePipelineRequest{
				Name:  "p",
				Type:  pipelinev1.PipelineType_PIPELINE_TYPE_UNSPECIFIED,
				Steps: []*pipelinev1.StepDefinition{{Id: "s", Type: pipelinev1.StepType_STEP_TYPE_VALIDATE}},
			},
		},
		{
			name: "out-of-range type",
			req: &pipelinev1.CreatePipelineRequest{
				Name:  "p",
				Type:  pipelinev1.PipelineType(999),
				Steps: []*pipelinev1.StepDefinition{{Id: "s", Type: pipelinev1.StepType_STEP_TYPE_VALIDATE}},
			},
		},
		{
			name: "no steps",
			req: &pipelinev1.CreatePipelineRequest{
				Name:  "p",
				Type:  pipelinev1.PipelineType_PIPELINE_TYPE_DEPLOYMENT_SAGA,
				Steps: nil,
			},
		},
		{
			name: "step missing id",
			req: &pipelinev1.CreatePipelineRequest{
				Name:  "p",
				Type:  pipelinev1.PipelineType_PIPELINE_TYPE_DEPLOYMENT_SAGA,
				Steps: []*pipelinev1.StepDefinition{{Id: "", Type: pipelinev1.StepType_STEP_TYPE_VALIDATE}},
			},
		},
		{
			name: "step unspecified type",
			req: &pipelinev1.CreatePipelineRequest{
				Name:  "p",
				Type:  pipelinev1.PipelineType_PIPELINE_TYPE_DEPLOYMENT_SAGA,
				Steps: []*pipelinev1.StepDefinition{{Id: "s", Type: pipelinev1.StepType_STEP_TYPE_UNSPECIFIED}},
			},
		},
		{
			name: "step out-of-range type",
			req: &pipelinev1.CreatePipelineRequest{
				Name:  "p",
				Type:  pipelinev1.PipelineType_PIPELINE_TYPE_DEPLOYMENT_SAGA,
				Steps: []*pipelinev1.StepDefinition{{Id: "s", Type: pipelinev1.StepType(42)}},
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := newClient(t, &mockService{}) // all funcs nil → must not be called
			_, err := client.CreatePipeline(authCtx(), tc.req)
			assertCode(t, err, codes.InvalidArgument)
		})
	}
}

func TestCreatePipeline_DomainErrorMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
		// wantMsgContains is asserted ONLY for client-safe (validation/precondition)
		// errors. For Internal we separately assert the message is sanitized.
		wantMsgContains string
		wantSanitized   bool
	}{
		{"cycle → InvalidArgument", domain.ErrCycleDetected, codes.InvalidArgument, "cycle", false},
		{"dangling → InvalidArgument", domain.ErrDanglingDependency, codes.InvalidArgument, "unknown step", false},
		{"too many → InvalidArgument", domain.ErrTooManySteps, codes.InvalidArgument, "too many steps", false},
		{"bare validation → InvalidArgument", domain.ErrValidation, codes.InvalidArgument, "validation", false},
		{"internal leak is sanitized", errors.New("pq: connection to host db.internal:5432 failed: password=hunter2"), codes.Internal, "", true},
		{"compensation failed → Internal sanitized", domain.ErrCompensationFailed, codes.Internal, "", true},
		{"no executor → Internal sanitized", domain.ErrNoExecutor, codes.Internal, "", true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := &mockService{
				createFn: func(_ context.Context, _ domain.Actor, _ domain.CreatePipelineInput) (domain.PipelineDefinition, error) {
					return domain.PipelineDefinition{}, tc.err
				},
			}
			client := newClient(t, m)
			req := &pipelinev1.CreatePipelineRequest{
				Name:  "p",
				Type:  pipelinev1.PipelineType_PIPELINE_TYPE_DEPLOYMENT_SAGA,
				Steps: []*pipelinev1.StepDefinition{{Id: "s", Type: pipelinev1.StepType_STEP_TYPE_VALIDATE}},
			}
			_, err := client.CreatePipeline(authCtx(), req)
			assertCode(t, err, tc.wantCode)
			st, _ := status.FromError(err)
			if tc.wantMsgContains != "" && !contains(st.Message(), tc.wantMsgContains) {
				t.Errorf("message %q does not contain %q", st.Message(), tc.wantMsgContains)
			}
			if tc.wantSanitized {
				assertSanitized(t, st.Message())
			}
		})
	}
}

func TestCreatePipeline_Unauthenticated(t *testing.T) {
	t.Parallel()
	client := newClient(t, &mockService{})
	_, err := client.CreatePipeline(noAuthCtx(), &pipelinev1.CreatePipelineRequest{
		Name:  "p",
		Type:  pipelinev1.PipelineType_PIPELINE_TYPE_DEPLOYMENT_SAGA,
		Steps: []*pipelinev1.StepDefinition{{Id: "s", Type: pipelinev1.StepType_STEP_TYPE_VALIDATE}},
	})
	assertCode(t, err, codes.Unauthenticated)
}

// ============================================================================
// GetPipeline / UpdatePipeline / DeletePipeline
// ============================================================================

func TestGetPipeline(t *testing.T) {
	t.Parallel()

	t.Run("happy path", func(t *testing.T) {
		t.Parallel()
		m := &mockService{
			getFn: func(_ context.Context, _ domain.Actor, id string) (domain.PipelineDefinition, error) {
				return domain.PipelineDefinition{ID: id, Name: "p", Type: domain.PipelineTypeTrainingDAG}, nil
			},
		}
		client := newClient(t, m)
		resp, err := client.GetPipeline(authCtx(), &pipelinev1.GetPipelineRequest{PipelineId: "pl-9"})
		if err != nil {
			t.Fatalf("GetPipeline: %v", err)
		}
		if resp.GetPipeline().GetId() != "pl-9" {
			t.Errorf("id = %q, want pl-9", resp.GetPipeline().GetId())
		}
		if m.gotGetID != "pl-9" {
			t.Errorf("service got id %q, want pl-9", m.gotGetID)
		}
		if resp.GetPipeline().GetType() != pipelinev1.PipelineType_PIPELINE_TYPE_TRAINING_DAG {
			t.Errorf("type = %v, want TRAINING_DAG", resp.GetPipeline().GetType())
		}
	})

	t.Run("missing id → InvalidArgument", func(t *testing.T) {
		t.Parallel()
		client := newClient(t, &mockService{})
		_, err := client.GetPipeline(authCtx(), &pipelinev1.GetPipelineRequest{PipelineId: ""})
		assertCode(t, err, codes.InvalidArgument)
	})

	t.Run("not found → NotFound", func(t *testing.T) {
		t.Parallel()
		m := &mockService{
			getFn: func(_ context.Context, _ domain.Actor, _ string) (domain.PipelineDefinition, error) {
				return domain.PipelineDefinition{}, domain.ErrPipelineNotFound
			},
		}
		client := newClient(t, m)
		_, err := client.GetPipeline(authCtx(), &pipelinev1.GetPipelineRequest{PipelineId: "nope"})
		assertCode(t, err, codes.NotFound)
	})
}

func TestUpdatePipeline(t *testing.T) {
	t.Parallel()

	t.Run("happy path conversion", func(t *testing.T) {
		t.Parallel()
		var got domain.UpdatePipelineInput
		m := &mockService{
			updateFn: func(_ context.Context, _ domain.Actor, in domain.UpdatePipelineInput) (domain.PipelineDefinition, error) {
				got = in
				return domain.PipelineDefinition{ID: in.ID, Name: in.Name, Type: domain.PipelineTypeDeploymentSaga, Steps: in.Steps}, nil
			},
		}
		client := newClient(t, m)
		resp, err := client.UpdatePipeline(authCtx(), &pipelinev1.UpdatePipelineRequest{
			PipelineId: "pl-2",
			Name:       "renamed",
			Steps:      []*pipelinev1.StepDefinition{{Id: "s1", Type: pipelinev1.StepType_STEP_TYPE_TRAIN}},
		})
		if err != nil {
			t.Fatalf("UpdatePipeline: %v", err)
		}
		if got.ID != "pl-2" || got.Name != "renamed" || len(got.Steps) != 1 {
			t.Errorf("input = %+v", got)
		}
		if got.Steps[0].Type != domain.StepTypeTrain {
			t.Errorf("step type = %v, want TRAIN", got.Steps[0].Type)
		}
		if resp.GetPipeline().GetName() != "renamed" {
			t.Errorf("resp name = %q", resp.GetPipeline().GetName())
		}
	})

	t.Run("missing id → InvalidArgument", func(t *testing.T) {
		t.Parallel()
		client := newClient(t, &mockService{})
		_, err := client.UpdatePipeline(authCtx(), &pipelinev1.UpdatePipelineRequest{
			Steps: []*pipelinev1.StepDefinition{{Id: "s", Type: pipelinev1.StepType_STEP_TYPE_TRAIN}},
		})
		assertCode(t, err, codes.InvalidArgument)
	})

	t.Run("not found → NotFound", func(t *testing.T) {
		t.Parallel()
		m := &mockService{
			updateFn: func(_ context.Context, _ domain.Actor, _ domain.UpdatePipelineInput) (domain.PipelineDefinition, error) {
				return domain.PipelineDefinition{}, domain.ErrPipelineNotFound
			},
		}
		client := newClient(t, m)
		_, err := client.UpdatePipeline(authCtx(), &pipelinev1.UpdatePipelineRequest{
			PipelineId: "pl-x",
			Steps:      []*pipelinev1.StepDefinition{{Id: "s", Type: pipelinev1.StepType_STEP_TYPE_TRAIN}},
		})
		assertCode(t, err, codes.NotFound)
	})
}

func TestDeletePipeline(t *testing.T) {
	t.Parallel()

	t.Run("happy path", func(t *testing.T) {
		t.Parallel()
		m := &mockService{deleteFn: func(_ context.Context, _ domain.Actor, _ string) error { return nil }}
		client := newClient(t, m)
		_, err := client.DeletePipeline(authCtx(), &pipelinev1.DeletePipelineRequest{PipelineId: "pl-3"})
		if err != nil {
			t.Fatalf("DeletePipeline: %v", err)
		}
		if m.gotDeleteID != "pl-3" {
			t.Errorf("service got id %q, want pl-3", m.gotDeleteID)
		}
	})

	t.Run("missing id → InvalidArgument", func(t *testing.T) {
		t.Parallel()
		client := newClient(t, &mockService{})
		_, err := client.DeletePipeline(authCtx(), &pipelinev1.DeletePipelineRequest{})
		assertCode(t, err, codes.InvalidArgument)
	})

	t.Run("not found → NotFound", func(t *testing.T) {
		t.Parallel()
		m := &mockService{deleteFn: func(_ context.Context, _ domain.Actor, _ string) error { return domain.ErrPipelineNotFound }}
		client := newClient(t, m)
		_, err := client.DeletePipeline(authCtx(), &pipelinev1.DeletePipelineRequest{PipelineId: "x"})
		assertCode(t, err, codes.NotFound)
	})
}

// ============================================================================
// ListPipelines / ListExecutions (pagination + filter conversion)
// ============================================================================

func TestListPipelines(t *testing.T) {
	t.Parallel()

	t.Run("happy path with filter and pagination", func(t *testing.T) {
		t.Parallel()
		var gotFilter domain.ListPipelinesFilter
		m := &mockService{
			listPFn: func(_ context.Context, _ domain.Actor, f domain.ListPipelinesFilter) ([]domain.PipelineDefinition, string, error) {
				gotFilter = f
				return []domain.PipelineDefinition{{ID: "a"}, {ID: "b"}}, "next-tok", nil
			},
		}
		client := newClient(t, m)
		resp, err := client.ListPipelines(authCtx(), &pipelinev1.ListPipelinesRequest{
			TypeFilter: pipelinev1.PipelineType_PIPELINE_TYPE_TRAINING_DAG,
			Pagination: pagReq(50, "tok-in"),
		})
		if err != nil {
			t.Fatalf("ListPipelines: %v", err)
		}
		if gotFilter.Type != domain.PipelineTypeTrainingDAG {
			t.Errorf("filter type = %v, want TRAINING_DAG", gotFilter.Type)
		}
		if gotFilter.List.PageSize != 50 || gotFilter.List.PageToken != "tok-in" {
			t.Errorf("filter pagination = %+v, want size 50 token tok-in", gotFilter.List)
		}
		if len(resp.GetPipelines()) != 2 {
			t.Errorf("pipelines = %d, want 2", len(resp.GetPipelines()))
		}
		if resp.GetPagination().GetNextPageToken() != "next-tok" {
			t.Errorf("nextToken = %q, want next-tok", resp.GetPagination().GetNextPageToken())
		}
	})

	t.Run("unspecified type filter is allowed", func(t *testing.T) {
		t.Parallel()
		m := &mockService{
			listPFn: func(_ context.Context, _ domain.Actor, f domain.ListPipelinesFilter) ([]domain.PipelineDefinition, string, error) {
				if f.Type != domain.PipelineTypeUnspecified {
					t.Errorf("want unspecified, got %v", f.Type)
				}
				return nil, "", nil
			},
		}
		client := newClient(t, m)
		if _, err := client.ListPipelines(authCtx(), &pipelinev1.ListPipelinesRequest{}); err != nil {
			t.Fatalf("ListPipelines: %v", err)
		}
	})

	t.Run("out-of-range type filter → InvalidArgument", func(t *testing.T) {
		t.Parallel()
		client := newClient(t, &mockService{})
		_, err := client.ListPipelines(authCtx(), &pipelinev1.ListPipelinesRequest{TypeFilter: pipelinev1.PipelineType(77)})
		assertCode(t, err, codes.InvalidArgument)
	})

	t.Run("negative page size → InvalidArgument", func(t *testing.T) {
		t.Parallel()
		client := newClient(t, &mockService{})
		_, err := client.ListPipelines(authCtx(), &pipelinev1.ListPipelinesRequest{
			Pagination: pagReq(-5, ""),
		})
		assertCode(t, err, codes.InvalidArgument)
	})
}

func TestListExecutions(t *testing.T) {
	t.Parallel()

	t.Run("happy path with filters", func(t *testing.T) {
		t.Parallel()
		var gotFilter domain.ListExecutionsFilter
		m := &mockService{
			listEFn: func(_ context.Context, _ domain.Actor, f domain.ListExecutionsFilter) ([]domain.Execution, string, error) {
				gotFilter = f
				return []domain.Execution{{ID: "e1"}}, "", nil
			},
		}
		client := newClient(t, m)
		resp, err := client.ListExecutions(authCtx(), &pipelinev1.ListExecutionsRequest{
			PipelineId:   "pl-1",
			StatusFilter: pipelinev1.ExecutionStatus_EXECUTION_STATUS_FAILED,
			Pagination:   pagReq(10, ""),
		})
		if err != nil {
			t.Fatalf("ListExecutions: %v", err)
		}
		if gotFilter.PipelineID != "pl-1" || gotFilter.Status != domain.ExecutionStatusFailed {
			t.Errorf("filter = %+v, want pl-1/FAILED", gotFilter)
		}
		if gotFilter.List.PageSize != 10 {
			t.Errorf("pageSize = %d, want 10", gotFilter.List.PageSize)
		}
		if len(resp.GetExecutions()) != 1 {
			t.Errorf("executions = %d, want 1", len(resp.GetExecutions()))
		}
	})

	t.Run("out-of-range status filter → InvalidArgument", func(t *testing.T) {
		t.Parallel()
		client := newClient(t, &mockService{})
		_, err := client.ListExecutions(authCtx(), &pipelinev1.ListExecutionsRequest{
			StatusFilter: pipelinev1.ExecutionStatus(123),
		})
		assertCode(t, err, codes.InvalidArgument)
	})
}

// ============================================================================
// TriggerExecution / GetExecution / CancelExecution
// ============================================================================

func TestTriggerExecution(t *testing.T) {
	t.Parallel()

	t.Run("happy path conversion + identity from claims", func(t *testing.T) {
		t.Parallel()
		var gotIn domain.TriggerInput
		var gotActor domain.Actor
		started := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
		m := &mockService{
			triggerFn: func(_ context.Context, actor domain.Actor, in domain.TriggerInput) (domain.Execution, error) {
				gotIn, gotActor = in, actor
				return domain.Execution{
					ID:          "ex-1",
					PipelineID:  in.PipelineID,
					Status:      domain.ExecutionStatusRunning,
					TriggeredBy: actor.Subject,
					StartedAt:   started,
					Input:       in.Input,
				}, nil
			},
		}
		client := newClient(t, m)
		resp, err := client.TriggerExecution(authCtx(), &pipelinev1.TriggerExecutionRequest{
			PipelineId:     "pl-1",
			Input:          mustStruct(t, map[string]any{"model_id": "m-9", "version": "3"}),
			IdempotencyKey: "trig-idem",
		})
		if err != nil {
			t.Fatalf("TriggerExecution: %v", err)
		}
		if gotIn.PipelineID != "pl-1" || gotIn.IdempotencyKey != "trig-idem" {
			t.Errorf("input = %+v", gotIn)
		}
		if gotIn.Input["model_id"] != "m-9" {
			t.Errorf("input map = %v", gotIn.Input)
		}
		if gotActor.Subject != testUserID {
			t.Errorf("actor subject = %q, want %q (from claims)", gotActor.Subject, testUserID)
		}
		got := resp.GetExecution()
		if got.GetId() != "ex-1" || got.GetStatus() != pipelinev1.ExecutionStatus_EXECUTION_STATUS_RUNNING {
			t.Errorf("resp = id %q status %v", got.GetId(), got.GetStatus())
		}
		if got.GetTriggeredBy() != testUserID {
			t.Errorf("triggeredBy = %q, want server-authoritative %q", got.GetTriggeredBy(), testUserID)
		}
		if !got.GetStartedAt().AsTime().Equal(started) {
			t.Errorf("startedAt = %v, want %v", got.GetStartedAt().AsTime(), started)
		}
	})

	t.Run("missing pipeline_id → InvalidArgument", func(t *testing.T) {
		t.Parallel()
		client := newClient(t, &mockService{})
		_, err := client.TriggerExecution(authCtx(), &pipelinev1.TriggerExecutionRequest{})
		assertCode(t, err, codes.InvalidArgument)
	})

	t.Run("archived pipeline → FailedPrecondition", func(t *testing.T) {
		t.Parallel()
		m := &mockService{
			triggerFn: func(_ context.Context, _ domain.Actor, _ domain.TriggerInput) (domain.Execution, error) {
				return domain.Execution{}, domain.ErrPipelineArchived
			},
		}
		client := newClient(t, m)
		_, err := client.TriggerExecution(authCtx(), &pipelinev1.TriggerExecutionRequest{PipelineId: "pl-1"})
		assertCode(t, err, codes.FailedPrecondition)
	})

	t.Run("pipeline not found → NotFound", func(t *testing.T) {
		t.Parallel()
		m := &mockService{
			triggerFn: func(_ context.Context, _ domain.Actor, _ domain.TriggerInput) (domain.Execution, error) {
				return domain.Execution{}, domain.ErrPipelineNotFound
			},
		}
		client := newClient(t, m)
		_, err := client.TriggerExecution(authCtx(), &pipelinev1.TriggerExecutionRequest{PipelineId: "pl-1"})
		assertCode(t, err, codes.NotFound)
	})

	t.Run("compensation-failed engine error → Internal sanitized", func(t *testing.T) {
		t.Parallel()
		m := &mockService{
			triggerFn: func(_ context.Context, _ domain.Actor, _ domain.TriggerInput) (domain.Execution, error) {
				return domain.Execution{}, domain.ErrCompensationFailed
			},
		}
		client := newClient(t, m)
		_, err := client.TriggerExecution(authCtx(), &pipelinev1.TriggerExecutionRequest{PipelineId: "pl-1"})
		assertCode(t, err, codes.Internal)
		st, _ := status.FromError(err)
		assertSanitized(t, st.Message())
	})
}

func TestGetExecution(t *testing.T) {
	t.Parallel()

	t.Run("happy path with step timeline", func(t *testing.T) {
		t.Parallel()
		startedAt := time.Date(2026, 6, 17, 9, 0, 0, 0, time.UTC)
		m := &mockService{
			getExecFn: func(_ context.Context, _ domain.Actor, id string) (domain.Execution, error) {
				return domain.Execution{
					ID:          id,
					PipelineID:  "pl-1",
					Status:      domain.ExecutionStatusCompensating,
					CurrentStep: "deploy",
					Steps: []domain.StepExecution{
						{ID: "se-1", ExecutionID: id, StepID: "validate", StepType: domain.StepTypeValidate, Status: domain.StepStatusCompleted, StartedAt: &startedAt, Attempt: 1},
						{ID: "se-2", ExecutionID: id, StepID: "deploy", StepType: domain.StepTypeDeploy, Status: domain.StepStatusFailed, Error: "boom", Attempt: 3},
					},
				}, nil
			},
		}
		client := newClient(t, m)
		resp, err := client.GetExecution(authCtx(), &pipelinev1.GetExecutionRequest{ExecutionId: "ex-7"})
		if err != nil {
			t.Fatalf("GetExecution: %v", err)
		}
		got := resp.GetExecution()
		if got.GetStatus() != pipelinev1.ExecutionStatus_EXECUTION_STATUS_COMPENSATING {
			t.Errorf("status = %v, want COMPENSATING", got.GetStatus())
		}
		if len(got.GetStepExecutions()) != 2 {
			t.Fatalf("steps = %d, want 2", len(got.GetStepExecutions()))
		}
		s2 := got.GetStepExecutions()[1]
		if s2.GetStatus() != pipelinev1.StepStatus_STEP_STATUS_FAILED || s2.GetAttempt() != 3 || s2.GetError() != "boom" {
			t.Errorf("step[1] = %+v", s2)
		}
		if !got.GetStepExecutions()[0].GetStartedAt().AsTime().Equal(startedAt) {
			t.Errorf("step[0] startedAt = %v, want %v", got.GetStepExecutions()[0].GetStartedAt().AsTime(), startedAt)
		}
	})

	t.Run("missing id → InvalidArgument", func(t *testing.T) {
		t.Parallel()
		client := newClient(t, &mockService{})
		_, err := client.GetExecution(authCtx(), &pipelinev1.GetExecutionRequest{})
		assertCode(t, err, codes.InvalidArgument)
	})

	t.Run("not found → NotFound", func(t *testing.T) {
		t.Parallel()
		m := &mockService{
			getExecFn: func(_ context.Context, _ domain.Actor, _ string) (domain.Execution, error) {
				return domain.Execution{}, domain.ErrExecutionNotFound
			},
		}
		client := newClient(t, m)
		_, err := client.GetExecution(authCtx(), &pipelinev1.GetExecutionRequest{ExecutionId: "x"})
		assertCode(t, err, codes.NotFound)
	})
}

func TestCancelExecution(t *testing.T) {
	t.Parallel()

	t.Run("happy path passes reason through", func(t *testing.T) {
		t.Parallel()
		m := &mockService{
			cancelFn: func(_ context.Context, _ domain.Actor, id, reason string) (domain.Execution, error) {
				return domain.Execution{ID: id, Status: domain.ExecutionStatusCompensating}, nil
			},
		}
		client := newClient(t, m)
		resp, err := client.CancelExecution(authCtx(), &pipelinev1.CancelExecutionRequest{
			ExecutionId: "ex-9",
			Reason:      "superseded by retrain",
		})
		if err != nil {
			t.Fatalf("CancelExecution: %v", err)
		}
		if m.gotCancelID != "ex-9" || m.gotCancelMsg != "superseded by retrain" {
			t.Errorf("service got id %q reason %q", m.gotCancelID, m.gotCancelMsg)
		}
		if resp.GetExecution().GetStatus() != pipelinev1.ExecutionStatus_EXECUTION_STATUS_COMPENSATING {
			t.Errorf("status = %v, want COMPENSATING", resp.GetExecution().GetStatus())
		}
	})

	t.Run("missing id → InvalidArgument", func(t *testing.T) {
		t.Parallel()
		client := newClient(t, &mockService{})
		_, err := client.CancelExecution(authCtx(), &pipelinev1.CancelExecutionRequest{})
		assertCode(t, err, codes.InvalidArgument)
	})

	t.Run("already terminal → FailedPrecondition", func(t *testing.T) {
		t.Parallel()
		m := &mockService{
			cancelFn: func(_ context.Context, _ domain.Actor, _, _ string) (domain.Execution, error) {
				return domain.Execution{}, domain.ErrExecutionNotCancellable
			},
		}
		client := newClient(t, m)
		_, err := client.CancelExecution(authCtx(), &pipelinev1.CancelExecutionRequest{ExecutionId: "ex"})
		assertCode(t, err, codes.FailedPrecondition)
	})

	t.Run("not found → NotFound", func(t *testing.T) {
		t.Parallel()
		m := &mockService{
			cancelFn: func(_ context.Context, _ domain.Actor, _, _ string) (domain.Execution, error) {
				return domain.Execution{}, domain.ErrExecutionNotFound
			},
		}
		client := newClient(t, m)
		_, err := client.CancelExecution(authCtx(), &pipelinev1.CancelExecutionRequest{ExecutionId: "ex"})
		assertCode(t, err, codes.NotFound)
	})
}

// ============================================================================
// WatchExecution (server-streaming)
// ============================================================================

func TestWatchExecution_AlreadyTerminal_SendsSnapshotAndCloses(t *testing.T) {
	t.Parallel()
	m := &mockService{
		getExecFn: func(_ context.Context, _ domain.Actor, id string) (domain.Execution, error) {
			return domain.Execution{ID: id, Status: domain.ExecutionStatusCompleted}, nil
		},
	}
	client := newClient(t, m)
	stream, err := client.WatchExecution(authCtx(), &pipelinev1.WatchExecutionRequest{ExecutionId: "ex-1"})
	if err != nil {
		t.Fatalf("WatchExecution open: %v", err)
	}
	// One snapshot, then clean EOF (stream closed).
	first, err := stream.Recv()
	if err != nil {
		t.Fatalf("first Recv: %v", err)
	}
	if first.GetExecution().GetStatus() != pipelinev1.ExecutionStatus_EXECUTION_STATUS_COMPLETED {
		t.Errorf("status = %v, want COMPLETED", first.GetExecution().GetStatus())
	}
	if first.GetSequence() != 1 {
		t.Errorf("sequence = %d, want 1", first.GetSequence())
	}
	if _, err := stream.Recv(); err == nil {
		t.Errorf("expected stream to close after terminal snapshot")
	}
}

func TestWatchExecution_StreamsTransitionsToTerminal(t *testing.T) {
	t.Parallel()

	// Program a sequence of GetExecution results: RUNNING (initial) → RUNNING
	// (step retried, attempt 2 — an observable change) → COMPLETED (terminal).
	startedAt := time.Date(2026, 6, 17, 8, 0, 0, 0, time.UTC)
	states := []domain.Execution{
		{ID: "ex-2", Status: domain.ExecutionStatusRunning, CurrentStep: "deploy",
			Steps: []domain.StepExecution{{StepID: "deploy", Status: domain.StepStatusRunning, StartedAt: &startedAt, Attempt: 1}}},
		{ID: "ex-2", Status: domain.ExecutionStatusRunning, CurrentStep: "deploy",
			Steps: []domain.StepExecution{{StepID: "deploy", Status: domain.StepStatusRunning, StartedAt: &startedAt, Attempt: 2}}},
		{ID: "ex-2", Status: domain.ExecutionStatusCompleted, CurrentStep: "deploy",
			Steps: []domain.StepExecution{{StepID: "deploy", Status: domain.StepStatusCompleted, StartedAt: &startedAt, Attempt: 2}}},
	}
	var idx int
	m := &mockService{
		getExecFn: func(_ context.Context, _ domain.Actor, _ string) (domain.Execution, error) {
			s := states[idx]
			if idx < len(states)-1 {
				idx++
			}
			return s, nil
		},
	}
	client := newClient(t, m)

	stream, err := client.WatchExecution(authCtx(), &pipelinev1.WatchExecutionRequest{
		ExecutionId:         "ex-2",
		IncludeCurrentState: true,
	})
	if err != nil {
		t.Fatalf("WatchExecution open: %v", err)
	}

	// Collect all updates until the stream closes (terminal).
	var updates []*pipelinev1.WatchExecutionResponse
	for {
		u, err := stream.Recv()
		if err != nil {
			break // EOF after terminal
		}
		updates = append(updates, u)
		if len(updates) > 10 {
			t.Fatalf("too many updates — stream did not terminate")
		}
	}

	if len(updates) < 2 {
		t.Fatalf("got %d updates, want at least 2 (initial + terminal)", len(updates))
	}
	// First update is the include_current_state snapshot (RUNNING, seq 1).
	if updates[0].GetExecution().GetStatus() != pipelinev1.ExecutionStatus_EXECUTION_STATUS_RUNNING {
		t.Errorf("update[0] status = %v, want RUNNING", updates[0].GetExecution().GetStatus())
	}
	if updates[0].GetSequence() != 1 {
		t.Errorf("update[0] sequence = %d, want 1", updates[0].GetSequence())
	}
	// Sequence numbers are monotonically increasing.
	for i := 1; i < len(updates); i++ {
		if updates[i].GetSequence() <= updates[i-1].GetSequence() {
			t.Errorf("sequence not monotonic at %d: %d <= %d", i, updates[i].GetSequence(), updates[i-1].GetSequence())
		}
	}
	// Final update is the terminal snapshot.
	last := updates[len(updates)-1]
	if last.GetExecution().GetStatus() != pipelinev1.ExecutionStatus_EXECUTION_STATUS_COMPLETED {
		t.Errorf("last status = %v, want COMPLETED", last.GetExecution().GetStatus())
	}
	if last.GetEmittedAt() == nil {
		t.Errorf("last update missing emitted_at")
	}
}

func TestWatchExecution_Validation(t *testing.T) {
	t.Parallel()

	t.Run("missing execution_id → InvalidArgument", func(t *testing.T) {
		t.Parallel()
		client := newClient(t, &mockService{})
		stream, err := client.WatchExecution(authCtx(), &pipelinev1.WatchExecutionRequest{})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		_, err = stream.Recv()
		assertCode(t, err, codes.InvalidArgument)
	})

	t.Run("not found → NotFound", func(t *testing.T) {
		t.Parallel()
		m := &mockService{
			getExecFn: func(_ context.Context, _ domain.Actor, _ string) (domain.Execution, error) {
				return domain.Execution{}, domain.ErrExecutionNotFound
			},
		}
		client := newClient(t, m)
		stream, err := client.WatchExecution(authCtx(), &pipelinev1.WatchExecutionRequest{ExecutionId: "nope"})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		_, err = stream.Recv()
		assertCode(t, err, codes.NotFound)
	})

	t.Run("unauthenticated → Unauthenticated", func(t *testing.T) {
		t.Parallel()
		client := newClient(t, &mockService{})
		stream, err := client.WatchExecution(noAuthCtx(), &pipelinev1.WatchExecutionRequest{ExecutionId: "ex"})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		_, err = stream.Recv()
		assertCode(t, err, codes.Unauthenticated)
	})
}

func TestWatchExecution_ClientCancellation(t *testing.T) {
	t.Parallel()

	// A never-terminal RUNNING execution: the loop would poll forever, so the
	// client cancels and we assert the stream unwinds with a cancellation status
	// (proves the loop honors ctx.Done()).
	m := &mockService{
		getExecFn: func(_ context.Context, _ domain.Actor, id string) (domain.Execution, error) {
			return domain.Execution{ID: id, Status: domain.ExecutionStatusRunning,
				Steps: []domain.StepExecution{{StepID: "s", Status: domain.StepStatusRunning, Attempt: 1}}}, nil
		},
	}
	client := newClient(t, m)

	ctx, cancel := context.WithCancel(authCtx())
	stream, err := client.WatchExecution(ctx, &pipelinev1.WatchExecutionRequest{
		ExecutionId:         "ex-3",
		IncludeCurrentState: true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// Drain the initial snapshot, then cancel.
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("initial Recv: %v", err)
	}
	cancel()
	// Subsequent Recv must error with a cancellation code.
	_, err = stream.Recv()
	if err == nil {
		t.Fatalf("expected error after cancel")
	}
	if code := status.Code(err); code != codes.Canceled && code != codes.Unavailable {
		t.Errorf("after cancel: code = %v, want Canceled/Unavailable", code)
	}
}

// ============================================================================
// NIL-SVC GUARD (the not-yet-wired binary is safe)
// ============================================================================

func TestNilService_UnaryReturnsUnimplemented(t *testing.T) {
	t.Parallel()
	client := newClient(t, nil) // handler.NewPipelineHandler(nil)
	_, err := client.GetExecution(authCtx(), &pipelinev1.GetExecutionRequest{ExecutionId: "x"})
	assertCode(t, err, codes.Unimplemented)
}

func TestNilService_StreamReturnsUnimplemented(t *testing.T) {
	t.Parallel()
	client := newClient(t, nil)
	stream, err := client.WatchExecution(authCtx(), &pipelinev1.WatchExecutionRequest{ExecutionId: "x"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_, err = stream.Recv()
	assertCode(t, err, codes.Unimplemented)
}

// ============================================================================
// TEST HELPERS
// ============================================================================

// assertCode fails unless err carries the wanted gRPC status code.
func assertCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("want error with code %v, got nil", want)
	}
	if got := status.Code(err); got != want {
		t.Fatalf("status code = %v, want %v (err: %v)", got, want, err)
	}
}

// assertSanitized fails if a client-facing message leaks internal detail. We
// check the message is the flat "internal error" and does NOT contain telltale
// internals (SQL, connection strings, secrets, host:port).
func assertSanitized(t *testing.T, msg string) {
	t.Helper()
	if msg != "internal error" {
		t.Errorf("internal error message = %q, want flat %q (no leak)", msg, "internal error")
	}
	for _, bad := range []string{"pq:", "password", "hunter2", "db.internal", "5432", "compensation", "executor"} {
		if contains(msg, bad) {
			t.Errorf("message %q leaks internal token %q", msg, bad)
		}
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
