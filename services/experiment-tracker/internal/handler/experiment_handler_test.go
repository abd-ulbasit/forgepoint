// experiment_handler_test.go — COMPONENT tests for the Experiment Tracker gRPC
// handler.
//
// ============================================================================
// WHAT "COMPONENT TEST" MEANS HERE (and why this is the right level)
// ============================================================================
//
// These tests exercise the handler END-TO-END through a REAL gRPC stack
// (bufconn: an in-process HTTP/2 transport — see pkg/testutil/grpc.go) but with
// the DOMAIN SERVICE replaced by a hand-written mock. The isolation is
// deliberate:
//
//   - We mock the ExperimentService INTERFACE, not the repositories. The unit
//     under test is the handler: proto<->domain conversion, validation, identity
//     lifting, and error mapping. Mocking the service (one interface) instead of
//     the repos + idempotency store + publisher keeps the test about the handler.
//     The domain's own logic has its own -race unit tests.
//
//   - Using bufconn instead of calling handler methods directly means the proto
//     actually serializes over the wire: a field we forget to map shows up as a
//     zero value on the client side, and a status error round-trips through
//     gRPC's status machinery exactly as a real client would see it.
//
//   - Claims are injected via the REAL grpcutil.AuthUnaryInterceptor backed by a
//     stub validator — the SAME public code path the platform uses to populate
//     the context's claims. The handler reads them through grpcutil's private
//     context key, which ONLY that interceptor can set, so we are not faking the
//     plumbing.
//
// NO testcontainers, NO database, NO network sockets — runs in milliseconds.
//
// ============================================================================
// THE FOUR THINGS THE RPC TESTS ASSERT (the task's contract)
// ============================================================================
//
//	(a) HAPPY PATH: proto<->domain conversion is correct (the mock records the
//	    domain input + Actor it received; we assert the response fields).
//	(b) VALIDATION: bad requests are rejected with codes.InvalidArgument BEFORE
//	    the domain is ever called (we assert the mock was NOT invoked).
//	(c) ERROR MAPPING: each domain sentinel maps to the right gRPC status code.
//	(d) NO LEAK: error messages never contain internal/SQL/secret/PII text.
package handler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	experimentv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/experiment/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/experiment-tracker/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// ----------------------------------------------------------------------------
// Tiny test helpers.
// ----------------------------------------------------------------------------

// wrap simulates the domain wrapping a sentinel with a specific message
// (fmt.Errorf("...: %w", sentinel)) so we can verify errors.Is-based mapping
// survives wrapping.
func wrap(sentinel error, msg string) error { return fmt.Errorf("%s: %w", msg, sentinel) }

// stubValidator implements grpcutil.TokenValidator by returning fixed claims.
// We drive the REAL grpcutil.AuthUnaryInterceptor with it so the test exercises
// the genuine claims-injection path (the handler reads claims via grpcutil's own
// private context key, which only that interceptor can populate).
type stubValidator struct {
	claims *grpcutil.Claims
	err    error
}

func (s stubValidator) Validate(ctx context.Context, token string) (*grpcutil.Claims, error) {
	return s.claims, s.err
}

// testClaims is the fixed authenticated caller most tests run as. UserID becomes
// OwnerID; Team is the tenancy boundary the handler lifts into domain.Actor.
//
// SCOPES: this principal holds BOTH experiments:read AND experiments:write so the
// existing happy-path / validation / error-mapping tests (which exercise every
// RPC, read and write) pass the new authorization gate. Authorization is verified
// separately by the under-scoped tests below, which run as deliberately
// narrower-scoped callers.
var testClaims = &grpcutil.Claims{
	UserID: "user-uuid-1",
	Team:   "team-acme",
	Scopes: []string{scopeRead, scopeWrite},
}

// ============================================================================
// MOCK domain.ExperimentService — a hand-written test double
// ============================================================================
//
// WHY hand-written and not gomock/mockery: a hand-written mock with function
// fields is dependency-free, reads top-to-bottom, and lets each test set ONLY
// the behavior it needs (unset fields are nil; an unexpected call panics, which
// the recovery interceptor turns into Internal — surfacing "this RPC should not
// have touched the domain"). For a 15-method interface this is less moving parts
// than a generated mock plus its tooling.
//
// Each method records the Actor and key inputs so happy-path tests can assert the
// handler lifted identity from claims (not the request) and converted fields
// correctly.
type mockService struct {
	// Per-method behavior hooks. A nil hook means "this test does not expect this
	// method to be called".
	createExperimentFn func(ctx context.Context, actor domain.Actor, in domain.CreateExperimentInput) (domain.Experiment, error)
	getExperimentFn    func(ctx context.Context, actor domain.Actor, id string) (domain.Experiment, error)
	listExperimentsFn  func(ctx context.Context, actor domain.Actor, includeArchived bool, opts domain.ListOptions) ([]domain.Experiment, string, error)
	updateExperimentFn func(ctx context.Context, actor domain.Actor, in domain.UpdateExperimentInput) (domain.Experiment, error)
	archiveFn          func(ctx context.Context, actor domain.Actor, id string) (domain.Experiment, error)

	startRunFn func(ctx context.Context, actor domain.Actor, in domain.StartRunInput) (domain.Run, error)
	finishRunFn func(ctx context.Context, actor domain.Actor, in domain.FinishRunInput) (domain.Run, error)
	getRunFn   func(ctx context.Context, actor domain.Actor, id string) (domain.Run, error)
	listRunsFn func(ctx context.Context, actor domain.Actor, experimentID string, statusFilter domain.RunStatus, opts domain.ListOptions) ([]domain.Run, string, error)
	deleteRunFn func(ctx context.Context, actor domain.Actor, runID, idempotencyKey string) error

	logMetricsFn func(ctx context.Context, actor domain.Actor, in domain.LogMetricsInput) (domain.LogMetricsResult, error)
	logParamsFn  func(ctx context.Context, actor domain.Actor, in domain.LogParamsInput) (domain.LogParamsResult, error)
	setArtifactsFn func(ctx context.Context, actor domain.Actor, in domain.SetArtifactsInput) (domain.Run, error)

	getHistoryFn  func(ctx context.Context, actor domain.Actor, in domain.GetMetricHistoryInput) ([]domain.MetricSeries, string, error)
	compareRunsFn func(ctx context.Context, actor domain.Actor, in domain.CompareRunsInput) ([]domain.RunComparison, error)

	// Call counters — let validation tests assert the domain was NOT reached.
	calls int

	// Captured identity + inputs for happy-path assertions.
	lastActor          domain.Actor
	lastCreateInput    domain.CreateExperimentInput
	lastUpdateInput    domain.UpdateExperimentInput
	lastStartRunInput  domain.StartRunInput
	lastFinishInput    domain.FinishRunInput
	lastListOpts       domain.ListOptions
	lastListStatus     domain.RunStatus
	lastLogMetricsIn   domain.LogMetricsInput
	lastLogParamsIn    domain.LogParamsInput
	lastSetArtifactsIn domain.SetArtifactsInput
	lastHistoryInput   domain.GetMetricHistoryInput
	lastCompareInput   domain.CompareRunsInput
}

func (m *mockService) CreateExperiment(ctx context.Context, actor domain.Actor, in domain.CreateExperimentInput) (domain.Experiment, error) {
	m.calls++
	m.lastActor = actor
	m.lastCreateInput = in
	return m.createExperimentFn(ctx, actor, in)
}

func (m *mockService) GetExperiment(ctx context.Context, actor domain.Actor, id string) (domain.Experiment, error) {
	m.calls++
	m.lastActor = actor
	return m.getExperimentFn(ctx, actor, id)
}

func (m *mockService) ListExperiments(ctx context.Context, actor domain.Actor, includeArchived bool, opts domain.ListOptions) ([]domain.Experiment, string, error) {
	m.calls++
	m.lastActor = actor
	m.lastListOpts = opts
	return m.listExperimentsFn(ctx, actor, includeArchived, opts)
}

func (m *mockService) UpdateExperiment(ctx context.Context, actor domain.Actor, in domain.UpdateExperimentInput) (domain.Experiment, error) {
	m.calls++
	m.lastActor = actor
	m.lastUpdateInput = in
	return m.updateExperimentFn(ctx, actor, in)
}

func (m *mockService) ArchiveExperiment(ctx context.Context, actor domain.Actor, id string) (domain.Experiment, error) {
	m.calls++
	m.lastActor = actor
	return m.archiveFn(ctx, actor, id)
}

func (m *mockService) StartRun(ctx context.Context, actor domain.Actor, in domain.StartRunInput) (domain.Run, error) {
	m.calls++
	m.lastActor = actor
	m.lastStartRunInput = in
	return m.startRunFn(ctx, actor, in)
}

func (m *mockService) FinishRun(ctx context.Context, actor domain.Actor, in domain.FinishRunInput) (domain.Run, error) {
	m.calls++
	m.lastActor = actor
	m.lastFinishInput = in
	return m.finishRunFn(ctx, actor, in)
}

func (m *mockService) GetRun(ctx context.Context, actor domain.Actor, id string) (domain.Run, error) {
	m.calls++
	m.lastActor = actor
	return m.getRunFn(ctx, actor, id)
}

func (m *mockService) ListRuns(ctx context.Context, actor domain.Actor, experimentID string, statusFilter domain.RunStatus, opts domain.ListOptions) ([]domain.Run, string, error) {
	m.calls++
	m.lastActor = actor
	m.lastListOpts = opts
	m.lastListStatus = statusFilter
	return m.listRunsFn(ctx, actor, experimentID, statusFilter, opts)
}

func (m *mockService) DeleteRun(ctx context.Context, actor domain.Actor, runID, idempotencyKey string) error {
	m.calls++
	m.lastActor = actor
	return m.deleteRunFn(ctx, actor, runID, idempotencyKey)
}

func (m *mockService) LogMetrics(ctx context.Context, actor domain.Actor, in domain.LogMetricsInput) (domain.LogMetricsResult, error) {
	m.calls++
	m.lastActor = actor
	m.lastLogMetricsIn = in
	return m.logMetricsFn(ctx, actor, in)
}

func (m *mockService) LogParams(ctx context.Context, actor domain.Actor, in domain.LogParamsInput) (domain.LogParamsResult, error) {
	m.calls++
	m.lastActor = actor
	m.lastLogParamsIn = in
	return m.logParamsFn(ctx, actor, in)
}

func (m *mockService) SetRunArtifacts(ctx context.Context, actor domain.Actor, in domain.SetArtifactsInput) (domain.Run, error) {
	m.calls++
	m.lastActor = actor
	m.lastSetArtifactsIn = in
	return m.setArtifactsFn(ctx, actor, in)
}

func (m *mockService) GetMetricHistory(ctx context.Context, actor domain.Actor, in domain.GetMetricHistoryInput) ([]domain.MetricSeries, string, error) {
	m.calls++
	m.lastActor = actor
	m.lastHistoryInput = in
	return m.getHistoryFn(ctx, actor, in)
}

func (m *mockService) CompareRuns(ctx context.Context, actor domain.Actor, in domain.CompareRunsInput) ([]domain.RunComparison, error) {
	m.calls++
	m.lastActor = actor
	m.lastCompareInput = in
	return m.compareRunsFn(ctx, actor, in)
}

// Compile-time proof the mock satisfies the interface the handler depends on.
var _ domain.ExperimentService = (*mockService)(nil)

// ============================================================================
// TEST HARNESS
// ============================================================================

// newTestClient wires the handler (backed by the mock) onto a bufconn server,
// installs the REAL auth interceptor with the given claims, and returns a ready
// client. Passing nil claims simulates an unauthenticated call (the interceptor
// would reject before the handler, but we mostly assert the handler's own
// fail-closed path; the interceptor returns Unauthenticated for a missing token).
func newTestClient(t *testing.T, svc domain.ExperimentService, claims *grpcutil.Claims) experimentv1.ExperimentTrackerServiceClient {
	t.Helper()
	h := NewExperimentHandler(svc)
	opt := grpc.ChainUnaryInterceptor(grpcutil.AuthUnaryInterceptor(stubValidator{claims: claims}))
	conn := testutil.NewTestGRPCServer(t, func(s *grpc.Server) {
		experimentv1.RegisterExperimentTrackerServiceServer(s, h)
	}, opt)
	return experimentv1.NewExperimentTrackerServiceClient(conn)
}

// authCtx attaches an "authorization: Bearer <token>" header so the server-side
// AuthUnaryInterceptor extracts it and runs the stub validator. The token value
// is irrelevant (the stub ignores it) but must be present, since the interceptor
// rejects a missing header before the validator runs.
func authCtx() context.Context {
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

// assertNoLeak fails if the message contains any substring indicating an
// internal/SQL/secret/PII leak. This is the (d) guarantee, checked uniformly.
func assertNoLeak(t *testing.T, msg string) {
	t.Helper()
	lower := strings.ToLower(msg)
	for _, banned := range []string{
		"repository:", // storage-layer sentinel text
		"sql",         // SQL fragments
		"pgx", "pq:",  // driver internals
		"goroutine",   // stack-trace leak
		"connection string",
		"panic",
		"password=", // any credential echo
		"secret",
	} {
		if strings.Contains(lower, banned) {
			t.Fatalf("error message leaks internal detail (%q): %q", banned, msg)
		}
	}
}

// assertActorLifted checks the handler lifted identity from the CLAIMS, never
// from a request field. Every mutating happy-path test calls this.
func assertActorLifted(t *testing.T, m *mockService) {
	t.Helper()
	if m.lastActor.UserID != testClaims.UserID {
		t.Fatalf("Actor.UserID = %q, want %q (must come from claims, not request)", m.lastActor.UserID, testClaims.UserID)
	}
	if m.lastActor.Team != testClaims.Team {
		t.Fatalf("Actor.Team = %q, want %q (must come from claims, not request)", m.lastActor.Team, testClaims.Team)
	}
}

// ============================================================================
// NIL-SVC GUARD — a half-wired binary must answer Unimplemented, not panic
// ============================================================================

func TestNilService_ReturnsUnimplemented(t *testing.T) {
	// svc==nil mirrors the scaffold / pre-repo phase. Every RPC must return
	// Unimplemented (the same code the embedded base returns) rather than
	// dereferencing the nil interface and panicking.
	client := newTestClient(t, nil, testClaims)

	_, err := client.GetExperiment(authCtx(), &experimentv1.GetExperimentRequest{Id: "exp-1"})
	requireCode(t, err, codes.Unimplemented)

	_, err = client.LogMetrics(authCtx(), &experimentv1.LogMetricsRequest{
		RunId:  "run-1",
		Points: []*experimentv1.MetricPoint{{Key: "loss", Value: 0.1}},
	})
	requireCode(t, err, codes.Unimplemented)
}

// ============================================================================
// AUTHENTICATION — missing/empty identity fails closed
// ============================================================================

func TestUnauthenticated_NoClaims(t *testing.T) {
	// Validator returns empty claims (no UserID) — actorFromContext must fail
	// closed with Unauthenticated, and the domain must never be called.
	mock := &mockService{
		getExperimentFn: func(ctx context.Context, actor domain.Actor, id string) (domain.Experiment, error) {
			t.Fatal("domain must NOT be called when the caller is unauthenticated")
			return domain.Experiment{}, nil
		},
	}
	client := newTestClient(t, mock, &grpcutil.Claims{ /* UserID empty */ })

	_, err := client.GetExperiment(authCtx(), &experimentv1.GetExperimentRequest{Id: "exp-1"})
	requireCode(t, err, codes.Unauthenticated)
	if mock.calls != 0 {
		t.Fatalf("domain was called %d times for an unauthenticated request; want 0", mock.calls)
	}
}

func TestUnauthenticated_NoTeam(t *testing.T) {
	// A caller with a user but NO team cannot be tenant-isolated -> Unauthenticated.
	mock := &mockService{
		getExperimentFn: func(ctx context.Context, actor domain.Actor, id string) (domain.Experiment, error) {
			t.Fatal("domain must NOT be called when the caller has no team")
			return domain.Experiment{}, nil
		},
	}
	client := newTestClient(t, mock, &grpcutil.Claims{UserID: "u1" /* Team empty */})

	_, err := client.GetExperiment(authCtx(), &experimentv1.GetExperimentRequest{Id: "exp-1"})
	requireCode(t, err, codes.Unauthenticated)
}

// ============================================================================
// AUTHORIZATION — per-RPC scope enforcement (PermissionDenied, fail-closed)
// ============================================================================
//
// These are the (b)-class tests for the SECOND gate the handler now has:
// authentication answers "who are you?" (Unauthenticated); authorization answers
// "may you?" (PermissionDenied). The two are distinct gRPC codes and a correct
// client reacts differently (re-auth vs. ask-an-admin), so we assert the right
// one and — critically — that the domain is NEVER reached for an under-scoped
// caller (broken-access-control is the finding; "domain not reached" is the proof
// the gate is real, not cosmetic).
//
// The capability model under test:
//   - mutating RPCs require experiments:write
//   - read RPCs require experiments:read (and write satisfies read — write⊇read)

// denyDomainMock is a mockService whose every hook fails the test if invoked. It
// is the proof object for "authorization rejected BEFORE the domain ran": if a
// scope gate let a call through, one of these fires and the test fails loudly.
func denyDomainMock(t *testing.T) *mockService {
	t.Helper()
	fail := func(name string) { t.Fatalf("domain %s must NOT be reached for an under-scoped caller", name) }
	return &mockService{
		createExperimentFn: func(context.Context, domain.Actor, domain.CreateExperimentInput) (domain.Experiment, error) {
			fail("CreateExperiment")
			return domain.Experiment{}, nil
		},
		getExperimentFn: func(context.Context, domain.Actor, string) (domain.Experiment, error) {
			fail("GetExperiment")
			return domain.Experiment{}, nil
		},
		listExperimentsFn: func(context.Context, domain.Actor, bool, domain.ListOptions) ([]domain.Experiment, string, error) {
			fail("ListExperiments")
			return nil, "", nil
		},
		updateExperimentFn: func(context.Context, domain.Actor, domain.UpdateExperimentInput) (domain.Experiment, error) {
			fail("UpdateExperiment")
			return domain.Experiment{}, nil
		},
		archiveFn: func(context.Context, domain.Actor, string) (domain.Experiment, error) {
			fail("ArchiveExperiment")
			return domain.Experiment{}, nil
		},
		startRunFn: func(context.Context, domain.Actor, domain.StartRunInput) (domain.Run, error) {
			fail("StartRun")
			return domain.Run{}, nil
		},
		finishRunFn: func(context.Context, domain.Actor, domain.FinishRunInput) (domain.Run, error) {
			fail("FinishRun")
			return domain.Run{}, nil
		},
		getRunFn: func(context.Context, domain.Actor, string) (domain.Run, error) {
			fail("GetRun")
			return domain.Run{}, nil
		},
		listRunsFn: func(context.Context, domain.Actor, string, domain.RunStatus, domain.ListOptions) ([]domain.Run, string, error) {
			fail("ListRuns")
			return nil, "", nil
		},
		deleteRunFn: func(context.Context, domain.Actor, string, string) error {
			fail("DeleteRun")
			return nil
		},
		logMetricsFn: func(context.Context, domain.Actor, domain.LogMetricsInput) (domain.LogMetricsResult, error) {
			fail("LogMetrics")
			return domain.LogMetricsResult{}, nil
		},
		logParamsFn: func(context.Context, domain.Actor, domain.LogParamsInput) (domain.LogParamsResult, error) {
			fail("LogParams")
			return domain.LogParamsResult{}, nil
		},
		setArtifactsFn: func(context.Context, domain.Actor, domain.SetArtifactsInput) (domain.Run, error) {
			fail("SetRunArtifacts")
			return domain.Run{}, nil
		},
		getHistoryFn: func(context.Context, domain.Actor, domain.GetMetricHistoryInput) ([]domain.MetricSeries, string, error) {
			fail("GetMetricHistory")
			return nil, "", nil
		},
		compareRunsFn: func(context.Context, domain.Actor, domain.CompareRunsInput) ([]domain.RunComparison, error) {
			fail("CompareRuns")
			return nil, nil
		},
	}
}

// allowDomainMock is the inverse of denyDomainMock: every hook is wired to a
// benign success (zero values, nil error) so a call that PASSES the authz gate
// reaches the domain and returns cleanly. Used by the admin tests, where the unit
// under test is "did authorization let the call through?" — not the domain
// result. Each hook increments calls via the mockService methods, so a test can
// assert the domain was reached (calls > 0).
func allowDomainMock() *mockService {
	return &mockService{
		createExperimentFn: func(context.Context, domain.Actor, domain.CreateExperimentInput) (domain.Experiment, error) {
			return domain.Experiment{ID: "e1", Name: "n"}, nil
		},
		getExperimentFn: func(_ context.Context, _ domain.Actor, id string) (domain.Experiment, error) {
			return domain.Experiment{ID: id, Name: "n"}, nil
		},
		listExperimentsFn: func(context.Context, domain.Actor, bool, domain.ListOptions) ([]domain.Experiment, string, error) {
			return nil, "", nil
		},
		updateExperimentFn: func(context.Context, domain.Actor, domain.UpdateExperimentInput) (domain.Experiment, error) {
			return domain.Experiment{ID: "e1", Name: "n"}, nil
		},
		archiveFn: func(_ context.Context, _ domain.Actor, id string) (domain.Experiment, error) {
			return domain.Experiment{ID: id, Name: "n"}, nil
		},
		startRunFn: func(_ context.Context, _ domain.Actor, in domain.StartRunInput) (domain.Run, error) {
			return domain.Run{ID: "r1", ExperimentID: in.ExperimentID, Status: domain.RunStatusRunning}, nil
		},
		finishRunFn: func(_ context.Context, _ domain.Actor, in domain.FinishRunInput) (domain.Run, error) {
			return domain.Run{ID: in.RunID, ExperimentID: "e1", Status: domain.RunStatusFinished}, nil
		},
		getRunFn: func(_ context.Context, _ domain.Actor, id string) (domain.Run, error) {
			return domain.Run{ID: id, ExperimentID: "e1", Status: domain.RunStatusRunning}, nil
		},
		listRunsFn: func(context.Context, domain.Actor, string, domain.RunStatus, domain.ListOptions) ([]domain.Run, string, error) {
			return nil, "", nil
		},
		deleteRunFn: func(context.Context, domain.Actor, string, string) error {
			return nil
		},
		logMetricsFn: func(context.Context, domain.Actor, domain.LogMetricsInput) (domain.LogMetricsResult, error) {
			return domain.LogMetricsResult{}, nil
		},
		logParamsFn: func(context.Context, domain.Actor, domain.LogParamsInput) (domain.LogParamsResult, error) {
			return domain.LogParamsResult{}, nil
		},
		setArtifactsFn: func(_ context.Context, _ domain.Actor, in domain.SetArtifactsInput) (domain.Run, error) {
			return domain.Run{ID: in.RunID, ExperimentID: "e1", Status: domain.RunStatusRunning}, nil
		},
		getHistoryFn: func(context.Context, domain.Actor, domain.GetMetricHistoryInput) ([]domain.MetricSeries, string, error) {
			return nil, "", nil
		},
		compareRunsFn: func(context.Context, domain.Actor, domain.CompareRunsInput) ([]domain.RunComparison, error) {
			return nil, nil
		},
	}
}

// invoke calls one RPC by name on the client with a minimally-VALID request — one
// that would pass field validation — so the test isolates the SCOPE gate. If
// validation rejected the request first we could not tell whether authorization
// fired, so each request below carries the required ids/fields.
func invoke(ctx context.Context, c experimentv1.ExperimentTrackerServiceClient, rpc string) error {
	var err error
	switch rpc {
	// --- mutating (require experiments:write) ---
	case "CreateExperiment":
		_, err = c.CreateExperiment(ctx, &experimentv1.CreateExperimentRequest{Name: "n"})
	case "UpdateExperiment":
		_, err = c.UpdateExperiment(ctx, &experimentv1.UpdateExperimentRequest{Id: "e1", UpdateFields: []string{"name"}, Name: "n"})
	case "ArchiveExperiment":
		_, err = c.ArchiveExperiment(ctx, &experimentv1.ArchiveExperimentRequest{Id: "e1"})
	case "StartRun":
		_, err = c.StartRun(ctx, &experimentv1.StartRunRequest{ExperimentId: "e1"})
	case "UpdateRunStatus":
		_, err = c.UpdateRunStatus(ctx, &experimentv1.UpdateRunStatusRequest{RunId: "r1", Status: experimentv1.RunStatus_RUN_STATUS_FINISHED})
	case "DeleteRun":
		_, err = c.DeleteRun(ctx, &experimentv1.DeleteRunRequest{RunId: "r1"})
	case "LogMetrics":
		_, err = c.LogMetrics(ctx, &experimentv1.LogMetricsRequest{RunId: "r1", Points: []*experimentv1.MetricPoint{{Key: "loss", Value: 0.1}}})
	case "LogParams":
		_, err = c.LogParams(ctx, &experimentv1.LogParamsRequest{RunId: "r1", Params: []*experimentv1.Param{{Key: "lr", Value: "0.01"}}})
	case "SetRunArtifacts":
		s, _ := structpb.NewStruct(map[string]any{"k": "v"})
		_, err = c.SetRunArtifacts(ctx, &experimentv1.SetRunArtifactsRequest{RunId: "r1", Artifacts: s})
	// --- read (require experiments:read or write) ---
	case "GetExperiment":
		_, err = c.GetExperiment(ctx, &experimentv1.GetExperimentRequest{Id: "e1"})
	case "ListExperiments":
		_, err = c.ListExperiments(ctx, &experimentv1.ListExperimentsRequest{})
	case "GetRun":
		_, err = c.GetRun(ctx, &experimentv1.GetRunRequest{Id: "r1"})
	case "ListRuns":
		_, err = c.ListRuns(ctx, &experimentv1.ListRunsRequest{ExperimentId: "e1"})
	case "GetMetricHistory":
		_, err = c.GetMetricHistory(ctx, &experimentv1.GetMetricHistoryRequest{RunId: "r1"})
	case "CompareRuns":
		_, err = c.CompareRuns(ctx, &experimentv1.CompareRunsRequest{RunIds: []string{"r1"}})
	default:
		panic("unknown rpc in invoke: " + rpc)
	}
	return err
}

// mutatingRPCs / readRPCs are the canonical lists of which gate guards which RPC —
// the executable mirror of the proto's documented "Requires experiments:write".
var (
	mutatingRPCs = []string{
		"CreateExperiment", "UpdateExperiment", "ArchiveExperiment",
		"StartRun", "UpdateRunStatus", "DeleteRun",
		"LogMetrics", "LogParams", "SetRunArtifacts",
	}
	readRPCs = []string{
		"GetExperiment", "ListExperiments", "GetRun", "ListRuns",
		"GetMetricHistory", "CompareRuns",
	}
)

// TestAuthorization_WriteRPCs_RequireWriteScope: a caller holding ONLY
// experiments:read is denied EVERY mutating RPC, and the domain is never reached.
// This is the core privilege-escalation finding: previously ANY authenticated
// caller could create/update/archive/log; now read-only callers cannot mutate.
func TestAuthorization_WriteRPCs_RequireWriteScope(t *testing.T) {
	readOnly := &grpcutil.Claims{UserID: "u-reader", Team: "team-acme", Scopes: []string{scopeRead}}
	for _, rpc := range mutatingRPCs {
		t.Run(rpc, func(t *testing.T) {
			mock := denyDomainMock(t)
			client := newTestClient(t, mock, readOnly)

			err := invoke(authCtx(), client, rpc)
			requireCode(t, err, codes.PermissionDenied)
			if mock.calls != 0 {
				t.Fatalf("domain was reached %d times for under-scoped %s; want 0", mock.calls, rpc)
			}
			// Message names the REQUIRED scope, never echoes the caller's grants.
			st, _ := status.FromError(err)
			if !strings.Contains(st.Message(), scopeWrite) {
				t.Fatalf("PermissionDenied message %q should name the required scope %q", st.Message(), scopeWrite)
			}
			if strings.Contains(st.Message(), scopeRead) {
				t.Fatalf("PermissionDenied message %q must NOT echo the caller's granted scope %q", st.Message(), scopeRead)
			}
		})
	}
}

// TestAuthorization_ReadRPCs_RequireReadScope: a caller with NO scopes at all is
// denied every read RPC (fail-closed: absence of a scope means no capability),
// and the domain is never reached.
func TestAuthorization_ReadRPCs_RequireReadScope(t *testing.T) {
	noScopes := &grpcutil.Claims{UserID: "u-noscope", Team: "team-acme" /* Scopes: nil */}
	for _, rpc := range readRPCs {
		t.Run(rpc, func(t *testing.T) {
			mock := denyDomainMock(t)
			client := newTestClient(t, mock, noScopes)

			err := invoke(authCtx(), client, rpc)
			requireCode(t, err, codes.PermissionDenied)
			if mock.calls != 0 {
				t.Fatalf("domain was reached %d times for unscoped %s; want 0", mock.calls, rpc)
			}
			st, _ := status.FromError(err)
			if !strings.Contains(st.Message(), scopeRead) {
				t.Fatalf("PermissionDenied message %q should name the required scope %q", st.Message(), scopeRead)
			}
		})
	}
}

// TestAuthorization_ReadScopeCannotWrite: the same read-only caller that is denied
// writes CAN still read — the positive control proving the read gate is permissive
// to the right capability and the deny above is about WRITE, not a blanket reject.
func TestAuthorization_ReadScopeCanRead(t *testing.T) {
	called := false
	mock := &mockService{
		getExperimentFn: func(ctx context.Context, actor domain.Actor, id string) (domain.Experiment, error) {
			called = true
			return domain.Experiment{ID: id, Name: "ok"}, nil
		},
	}
	readOnly := &grpcutil.Claims{UserID: "u-reader", Team: "team-acme", Scopes: []string{scopeRead}}
	client := newTestClient(t, mock, readOnly)

	resp, err := client.GetExperiment(authCtx(), &experimentv1.GetExperimentRequest{Id: "e1"})
	if err != nil {
		t.Fatalf("read-only caller should be allowed to read: %v", err)
	}
	if !called || resp.GetExperiment().GetId() != "e1" {
		t.Fatalf("expected the read to reach the domain and return e1, got called=%v resp=%+v", called, resp)
	}
}

// TestAuthorization_WriteScopeImpliesRead: a WRITE-only caller (no explicit read
// scope) can still perform reads — encodes the write⊇read rule so a future change
// that drops the implication is caught.
func TestAuthorization_WriteScopeImpliesRead(t *testing.T) {
	called := false
	mock := &mockService{
		getRunFn: func(ctx context.Context, actor domain.Actor, id string) (domain.Run, error) {
			called = true
			return domain.Run{ID: id, ExperimentID: "e1", Status: domain.RunStatusRunning}, nil
		},
	}
	writeOnly := &grpcutil.Claims{UserID: "u-writer", Team: "team-acme", Scopes: []string{scopeWrite}}
	client := newTestClient(t, mock, writeOnly)

	if _, err := client.GetRun(authCtx(), &experimentv1.GetRunRequest{Id: "r1"}); err != nil {
		t.Fatalf("write scope should imply read: %v", err)
	}
	if !called {
		t.Fatal("expected write-scoped read to reach the domain (write implies read)")
	}
}

// TestAuthorization_AdminRole_ListRuns_NoScopes is the REGRESSION TEST for the
// "GET /runs returns 403 for the bootstrap admin" bug.
//
// A human admin JWT (what the UI/BFF mints at Login) carries Role="admin" but an
// EMPTY Scopes slice — the auth service never stuffs per-resource scopes into a
// JWT (it resolves a role's permissions fresh at CheckPermission time). The old
// scope-only gate therefore denied the admin's ListRuns with PermissionDenied,
// making the Experiments page unusable. The admin role is the {*,*} platform
// superuser, so the corrected gate must ALLOW it and reach the domain.
func TestAuthorization_AdminRole_ListRuns_NoScopes(t *testing.T) {
	called := false
	mock := &mockService{
		listRunsFn: func(ctx context.Context, actor domain.Actor, experimentID string, statusFilter domain.RunStatus, opts domain.ListOptions) ([]domain.Run, string, error) {
			called = true
			// The admin is still tenancy-scoped via Actor.Team (no over-open): the
			// handler lifts team from the validated token, not the request.
			if actor.Team != "team-acme" {
				t.Fatalf("admin ListRuns should run as the caller's team, got %q", actor.Team)
			}
			return []domain.Run{{ID: "r1", ExperimentID: experimentID, Status: domain.RunStatusRunning}}, "", nil
		},
	}
	// Role="admin" with NO scopes — exactly the bootstrap admin's JWT shape.
	admin := &grpcutil.Claims{UserID: "admin-uuid", Team: "team-acme", Role: "admin" /* Scopes: nil */}
	client := newTestClient(t, mock, admin)

	resp, err := client.ListRuns(authCtx(), &experimentv1.ListRunsRequest{ExperimentId: "e1"})
	if err != nil {
		t.Fatalf("admin (role=admin, no scopes) must be allowed to ListRuns, got: %v", err)
	}
	if !called {
		t.Fatal("expected admin ListRuns to reach the domain")
	}
	if len(resp.GetRuns()) != 1 || resp.GetRuns()[0].GetId() != "r1" {
		t.Fatalf("expected one run r1 back, got %+v", resp.GetRuns())
	}
}

// TestAuthorization_AdminRole_AllReadAndWriteRPCs proves the admin short-circuit
// is uniform: the admin role (no scopes) satisfies BOTH the read gate and the
// write gate on every RPC — admin is the {*,*} superuser, so it must never trip a
// scope check anywhere in this service.
func TestAuthorization_AdminRole_AllReadAndWriteRPCs(t *testing.T) {
	admin := &grpcutil.Claims{UserID: "admin-uuid", Team: "team-acme", Role: "admin" /* Scopes: nil */}
	for _, rpc := range append(append([]string{}, readRPCs...), mutatingRPCs...) {
		t.Run(rpc, func(t *testing.T) {
			// allowDomainMock returns benign zero values for every method, so the only
			// thing under test is whether the authz gate let the call THROUGH to the
			// domain. We must NOT get PermissionDenied for an admin.
			mock := allowDomainMock()
			client := newTestClient(t, mock, admin)

			err := invoke(authCtx(), client, rpc)
			if err != nil {
				if st, _ := status.FromError(err); st.Code() == codes.PermissionDenied {
					t.Fatalf("admin must not be denied %s, got PermissionDenied: %v", rpc, err)
				}
				// Any other error would be a mock/conversion issue, not authz; surface it.
				t.Fatalf("unexpected error invoking %s as admin: %v", rpc, err)
			}
			if mock.calls == 0 {
				t.Fatalf("admin %s should have reached the domain (authz passed)", rpc)
			}
		})
	}
}

// TestAuthorization_NonAdminNoScope_StillDenied is the NEGATIVE control proving
// the fix did NOT over-open the surface: a non-admin caller (a viewer-role or
// API-key principal) with NO experiments scope is STILL denied — only the admin
// ROLE short-circuits, a non-admin still needs the matching scope. We use a
// non-admin role to prove role!="admin" does not leak the bypass.
func TestAuthorization_NonAdminNoScope_StillDenied(t *testing.T) {
	// role=viewer (a real seeded role) but no experiments:read scope and not admin.
	nonAdmin := &grpcutil.Claims{UserID: "u-viewer", Team: "team-acme", Role: "viewer" /* Scopes: nil */}
	for _, rpc := range readRPCs {
		t.Run(rpc, func(t *testing.T) {
			mock := denyDomainMock(t)
			client := newTestClient(t, mock, nonAdmin)

			err := invoke(authCtx(), client, rpc)
			requireCode(t, err, codes.PermissionDenied)
			if mock.calls != 0 {
				t.Fatalf("domain was reached %d times for non-admin unscoped %s; want 0", mock.calls, rpc)
			}
		})
	}
}

// ============================================================================
// CreateExperiment
// ============================================================================

func TestCreateExperiment_HappyPath(t *testing.T) {
	now := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	mock := &mockService{
		createExperimentFn: func(ctx context.Context, actor domain.Actor, in domain.CreateExperimentInput) (domain.Experiment, error) {
			// The domain receives the client-owned fields verbatim.
			if in.Name != "sweep-1" || in.Description != "desc" || in.Tags["env"] != "prod" {
				t.Errorf("domain received wrong input: %+v", in)
			}
			// owner/team are server-set; here the mock simulates that result.
			return domain.Experiment{
				ID: "exp-1", Name: in.Name, Description: in.Description, Tags: in.Tags,
				OwnerID: actor.UserID, Team: actor.Team, CreatedAt: now, UpdatedAt: now,
			}, nil
		},
	}
	client := newTestClient(t, mock, testClaims)

	resp, err := client.CreateExperiment(authCtx(), &experimentv1.CreateExperimentRequest{
		Name: "sweep-1", Description: "desc", Tags: map[string]string{"env": "prod"},
	})
	if err != nil {
		t.Fatalf("CreateExperiment returned error: %v", err)
	}
	assertActorLifted(t, mock)
	got := resp.GetExperiment()
	if got.GetId() != "exp-1" || got.GetName() != "sweep-1" {
		t.Fatalf("unexpected experiment: %+v", got)
	}
	// owner/team in the response come from the actor (claims), proving the handler
	// did NOT trust client-supplied owner/team (the request has no such fields).
	if got.GetOwnerId() != testClaims.UserID || got.GetTeam() != testClaims.Team {
		t.Fatalf("owner/team = %q/%q, want %q/%q", got.GetOwnerId(), got.GetTeam(), testClaims.UserID, testClaims.Team)
	}
	if got.GetCreatedAt().AsTime().UTC() != now {
		t.Fatalf("created_at = %v, want %v", got.GetCreatedAt().AsTime(), now)
	}
}

func TestCreateExperiment_Validation_EmptyName(t *testing.T) {
	mock := &mockService{
		createExperimentFn: func(ctx context.Context, actor domain.Actor, in domain.CreateExperimentInput) (domain.Experiment, error) {
			t.Fatal("domain must NOT be called for an empty name")
			return domain.Experiment{}, nil
		},
	}
	client := newTestClient(t, mock, testClaims)

	_, err := client.CreateExperiment(authCtx(), &experimentv1.CreateExperimentRequest{Description: "x"})
	requireCode(t, err, codes.InvalidArgument)
	if mock.calls != 0 {
		t.Fatalf("domain called %d times; want 0", mock.calls)
	}
}

func TestCreateExperiment_NameExists_MapsAlreadyExists(t *testing.T) {
	mock := &mockService{
		createExperimentFn: func(ctx context.Context, actor domain.Actor, in domain.CreateExperimentInput) (domain.Experiment, error) {
			return domain.Experiment{}, domain.ErrExperimentNameExists
		},
	}
	client := newTestClient(t, mock, testClaims)

	_, err := client.CreateExperiment(authCtx(), &experimentv1.CreateExperimentRequest{Name: "dup"})
	requireCode(t, err, codes.AlreadyExists)
	st, _ := status.FromError(err)
	assertNoLeak(t, st.Message())
}

func TestCreateExperiment_Validation_FromDomain(t *testing.T) {
	// A wrapped ErrValidation must map to InvalidArgument and its message (which
	// describes the client's own bad input) is forwarded — the one whitelisted
	// err.Error() path. It still must not leak internals.
	mock := &mockService{
		createExperimentFn: func(ctx context.Context, actor domain.Actor, in domain.CreateExperimentInput) (domain.Experiment, error) {
			return domain.Experiment{}, wrap(domain.ErrValidation, "experiment: name too long")
		},
	}
	client := newTestClient(t, mock, testClaims)

	_, err := client.CreateExperiment(authCtx(), &experimentv1.CreateExperimentRequest{Name: "x"})
	requireCode(t, err, codes.InvalidArgument)
	st, _ := status.FromError(err)
	if !strings.Contains(st.Message(), "name too long") {
		t.Fatalf("validation message should be forwarded, got %q", st.Message())
	}
	assertNoLeak(t, st.Message())
}

func TestCreateExperiment_UnknownError_Sanitized(t *testing.T) {
	// An UNRECOGNIZED error (simulating a DB failure wrapping SQL + secret) must
	// map to Internal with a sanitized message.
	mock := &mockService{
		createExperimentFn: func(ctx context.Context, actor domain.Actor, in domain.CreateExperimentInput) (domain.Experiment, error) {
			return domain.Experiment{}, errors.New("pq: connection refused host=db password=topsecret")
		},
	}
	client := newTestClient(t, mock, testClaims)

	_, err := client.CreateExperiment(authCtx(), &experimentv1.CreateExperimentRequest{Name: "x"})
	requireCode(t, err, codes.Internal)
	st, _ := status.FromError(err)
	assertNoLeak(t, st.Message())
}

// ============================================================================
// GetExperiment
// ============================================================================

func TestGetExperiment_HappyPath(t *testing.T) {
	now := time.Date(2026, 6, 17, 9, 0, 0, 0, time.UTC)
	archived := now.Add(time.Hour)
	mock := &mockService{
		getExperimentFn: func(ctx context.Context, actor domain.Actor, id string) (domain.Experiment, error) {
			if id != "exp-9" {
				t.Errorf("domain got id %q, want exp-9", id)
			}
			return domain.Experiment{
				ID: "exp-9", Name: "n", OwnerID: actor.UserID, Team: actor.Team,
				CreatedAt: now, UpdatedAt: now, ArchivedAt: &archived,
			}, nil
		},
	}
	client := newTestClient(t, mock, testClaims)

	resp, err := client.GetExperiment(authCtx(), &experimentv1.GetExperimentRequest{Id: "exp-9"})
	if err != nil {
		t.Fatalf("GetExperiment error: %v", err)
	}
	assertActorLifted(t, mock)
	if resp.GetExperiment().GetArchivedAt().AsTime().UTC() != archived {
		t.Fatalf("archived_at not surfaced correctly: %v", resp.GetExperiment().GetArchivedAt().AsTime())
	}
}

func TestGetExperiment_Validation_EmptyID(t *testing.T) {
	mock := &mockService{}
	client := newTestClient(t, mock, testClaims)
	_, err := client.GetExperiment(authCtx(), &experimentv1.GetExperimentRequest{})
	requireCode(t, err, codes.InvalidArgument)
	if mock.calls != 0 {
		t.Fatalf("domain called %d times; want 0", mock.calls)
	}
}

func TestGetExperiment_NotFound_MapsNotFound(t *testing.T) {
	mock := &mockService{
		getExperimentFn: func(ctx context.Context, actor domain.Actor, id string) (domain.Experiment, error) {
			return domain.Experiment{}, wrap(domain.ErrExperimentNotFound, "experiment: lookup")
		},
	}
	client := newTestClient(t, mock, testClaims)
	_, err := client.GetExperiment(authCtx(), &experimentv1.GetExperimentRequest{Id: "missing"})
	requireCode(t, err, codes.NotFound)
	st, _ := status.FromError(err)
	assertNoLeak(t, st.Message())
}

// ============================================================================
// ListExperiments — pagination normalization
// ============================================================================

func TestListExperiments_PaginationDefaultAndClamp(t *testing.T) {
	cases := []struct {
		name     string
		reqSize  int32
		wantSize int
	}{
		{"zero -> default", 0, defaultPageSize},
		{"over cap -> clamped", 10_000, maxPageSize},
		{"within range -> verbatim", 37, 37},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockService{
				listExperimentsFn: func(ctx context.Context, actor domain.Actor, includeArchived bool, opts domain.ListOptions) ([]domain.Experiment, string, error) {
					if opts.PageSize != tc.wantSize {
						t.Errorf("opts.PageSize = %d, want %d", opts.PageSize, tc.wantSize)
					}
					return []domain.Experiment{{ID: "e1"}}, "next-cursor", nil
				},
			}
			client := newTestClient(t, mock, testClaims)
			resp, err := client.ListExperiments(authCtx(), &experimentv1.ListExperimentsRequest{
				Pagination: &commonv1.PaginationRequest{PageSize: tc.reqSize},
			})
			if err != nil {
				t.Fatalf("ListExperiments error: %v", err)
			}
			if resp.GetPagination().GetNextPageToken() != "next-cursor" {
				t.Fatalf("next_page_token = %q, want next-cursor", resp.GetPagination().GetNextPageToken())
			}
			if len(resp.GetExperiments()) != 1 {
				t.Fatalf("len(experiments) = %d, want 1", len(resp.GetExperiments()))
			}
		})
	}
}

func TestListExperiments_NegativePageSize_InvalidArgument(t *testing.T) {
	mock := &mockService{
		listExperimentsFn: func(ctx context.Context, actor domain.Actor, includeArchived bool, opts domain.ListOptions) ([]domain.Experiment, string, error) {
			t.Fatal("domain must NOT be called for a negative page_size")
			return nil, "", nil
		},
	}
	client := newTestClient(t, mock, testClaims)
	_, err := client.ListExperiments(authCtx(), &experimentv1.ListExperimentsRequest{
		Pagination: &commonv1.PaginationRequest{PageSize: -5},
	})
	requireCode(t, err, codes.InvalidArgument)
	if mock.calls != 0 {
		t.Fatalf("domain called %d times; want 0", mock.calls)
	}
}

// ============================================================================
// UpdateExperiment — field-mask validation
// ============================================================================

func TestUpdateExperiment_HappyPath(t *testing.T) {
	mock := &mockService{
		updateExperimentFn: func(ctx context.Context, actor domain.Actor, in domain.UpdateExperimentInput) (domain.Experiment, error) {
			if in.ID != "exp-1" || len(in.UpdateFields) != 1 || in.UpdateFields[0] != "description" {
				t.Errorf("update input wrong: %+v", in)
			}
			return domain.Experiment{ID: "exp-1", Description: in.Description}, nil
		},
	}
	client := newTestClient(t, mock, testClaims)
	resp, err := client.UpdateExperiment(authCtx(), &experimentv1.UpdateExperimentRequest{
		Id: "exp-1", Description: "new", UpdateFields: []string{"description"},
	})
	if err != nil {
		t.Fatalf("UpdateExperiment error: %v", err)
	}
	assertActorLifted(t, mock)
	if resp.GetExperiment().GetDescription() != "new" {
		t.Fatalf("description = %q, want new", resp.GetExperiment().GetDescription())
	}
}

func TestUpdateExperiment_Validation(t *testing.T) {
	cases := []struct {
		name string
		req  *experimentv1.UpdateExperimentRequest
	}{
		{"empty id", &experimentv1.UpdateExperimentRequest{UpdateFields: []string{"name"}}},
		{"empty mask", &experimentv1.UpdateExperimentRequest{Id: "exp-1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockService{
				updateExperimentFn: func(ctx context.Context, actor domain.Actor, in domain.UpdateExperimentInput) (domain.Experiment, error) {
					t.Fatal("domain must NOT be called for invalid input")
					return domain.Experiment{}, nil
				},
			}
			client := newTestClient(t, mock, testClaims)
			_, err := client.UpdateExperiment(authCtx(), tc.req)
			requireCode(t, err, codes.InvalidArgument)
			if mock.calls != 0 {
				t.Fatalf("domain called %d times; want 0", mock.calls)
			}
		})
	}
}

// ============================================================================
// ArchiveExperiment
// ============================================================================

func TestArchiveExperiment_HappyPath(t *testing.T) {
	archived := time.Now().UTC()
	mock := &mockService{
		archiveFn: func(ctx context.Context, actor domain.Actor, id string) (domain.Experiment, error) {
			return domain.Experiment{ID: id, ArchivedAt: &archived}, nil
		},
	}
	client := newTestClient(t, mock, testClaims)
	resp, err := client.ArchiveExperiment(authCtx(), &experimentv1.ArchiveExperimentRequest{Id: "exp-1"})
	if err != nil {
		t.Fatalf("ArchiveExperiment error: %v", err)
	}
	assertActorLifted(t, mock)
	if resp.GetExperiment().GetArchivedAt() == nil {
		t.Fatal("archived_at should be set on an archived experiment")
	}
}

func TestArchiveExperiment_Validation_EmptyID(t *testing.T) {
	mock := &mockService{}
	client := newTestClient(t, mock, testClaims)
	_, err := client.ArchiveExperiment(authCtx(), &experimentv1.ArchiveExperimentRequest{})
	requireCode(t, err, codes.InvalidArgument)
}

// ============================================================================
// StartRun
// ============================================================================

func TestStartRun_HappyPath(t *testing.T) {
	started := time.Date(2026, 6, 17, 8, 0, 0, 0, time.UTC)
	mock := &mockService{
		startRunFn: func(ctx context.Context, actor domain.Actor, in domain.StartRunInput) (domain.Run, error) {
			if in.ExperimentID != "exp-1" || in.ModelVersionID != "mv-1" || in.IdempotencyKey != "idem-1" {
				t.Errorf("start-run input wrong: %+v", in)
			}
			if len(in.Params) != 1 || in.Params[0].Key != "lr" || in.Params[0].Value != "0.01" {
				t.Errorf("params not converted: %+v", in.Params)
			}
			return domain.Run{
				ID: "run-1", ExperimentID: in.ExperimentID, Status: domain.RunStatusRunning,
				Source: domain.RunSourceAPI, ModelVersionID: in.ModelVersionID,
				OwnerID: actor.UserID, Params: in.Params, StartedAt: started,
			}, nil
		},
	}
	client := newTestClient(t, mock, testClaims)
	resp, err := client.StartRun(authCtx(), &experimentv1.StartRunRequest{
		ExperimentId:   "exp-1",
		ModelVersionId: "mv-1",
		Params:         []*experimentv1.Param{{Key: "lr", Value: "0.01"}},
		IdempotencyKey: "idem-1",
	})
	if err != nil {
		t.Fatalf("StartRun error: %v", err)
	}
	assertActorLifted(t, mock)
	got := resp.GetRun()
	if got.GetStatus() != experimentv1.RunStatus_RUN_STATUS_RUNNING {
		t.Fatalf("status = %v, want RUNNING", got.GetStatus())
	}
	if got.GetSource() != experimentv1.RunSource_RUN_SOURCE_API {
		t.Fatalf("source = %v, want API", got.GetSource())
	}
	if got.GetEndedAt() != nil {
		t.Fatal("ended_at must be nil for a RUNNING run")
	}
	if got.GetStartedAt().AsTime().UTC() != started {
		t.Fatalf("started_at = %v, want %v", got.GetStartedAt().AsTime(), started)
	}
}

func TestStartRun_Validation_EmptyExperimentID(t *testing.T) {
	mock := &mockService{}
	client := newTestClient(t, mock, testClaims)
	_, err := client.StartRun(authCtx(), &experimentv1.StartRunRequest{})
	requireCode(t, err, codes.InvalidArgument)
	if mock.calls != 0 {
		t.Fatalf("domain called %d times; want 0", mock.calls)
	}
}

func TestStartRun_ExperimentNotFound_MapsNotFound(t *testing.T) {
	mock := &mockService{
		startRunFn: func(ctx context.Context, actor domain.Actor, in domain.StartRunInput) (domain.Run, error) {
			return domain.Run{}, domain.ErrExperimentNotFound
		},
	}
	client := newTestClient(t, mock, testClaims)
	_, err := client.StartRun(authCtx(), &experimentv1.StartRunRequest{ExperimentId: "nope"})
	requireCode(t, err, codes.NotFound)
}

// ============================================================================
// UpdateRunStatus (-> domain FinishRun)
// ============================================================================

func TestUpdateRunStatus_HappyPath(t *testing.T) {
	ended := time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC)
	mock := &mockService{
		finishRunFn: func(ctx context.Context, actor domain.Actor, in domain.FinishRunInput) (domain.Run, error) {
			// The proto enum FINISHED must convert to the domain RunStatusFinished.
			if in.RunID != "run-1" || in.TargetStatus != domain.RunStatusFinished {
				t.Errorf("finish input wrong: %+v", in)
			}
			return domain.Run{
				ID: "run-1", Status: domain.RunStatusFinished, EndedAt: &ended,
				FinalMetrics: []domain.MetricPoint{{Key: "acc", Value: 0.94, Step: 10, Timestamp: ended}},
			}, nil
		},
	}
	client := newTestClient(t, mock, testClaims)
	resp, err := client.UpdateRunStatus(authCtx(), &experimentv1.UpdateRunStatusRequest{
		RunId: "run-1", Status: experimentv1.RunStatus_RUN_STATUS_FINISHED,
	})
	if err != nil {
		t.Fatalf("UpdateRunStatus error: %v", err)
	}
	assertActorLifted(t, mock)
	got := resp.GetRun()
	if got.GetStatus() != experimentv1.RunStatus_RUN_STATUS_FINISHED {
		t.Fatalf("status = %v, want FINISHED", got.GetStatus())
	}
	if got.GetEndedAt().AsTime().UTC() != ended {
		t.Fatalf("ended_at = %v, want %v", got.GetEndedAt().AsTime(), ended)
	}
	if len(got.GetFinalMetrics()) != 1 || got.GetFinalMetrics()[0].GetKey() != "acc" {
		t.Fatalf("final_metrics not surfaced: %+v", got.GetFinalMetrics())
	}
}

func TestUpdateRunStatus_Validation(t *testing.T) {
	cases := []struct {
		name string
		req  *experimentv1.UpdateRunStatusRequest
	}{
		{"empty run_id", &experimentv1.UpdateRunStatusRequest{Status: experimentv1.RunStatus_RUN_STATUS_FINISHED}},
		{"unspecified status", &experimentv1.UpdateRunStatusRequest{RunId: "run-1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockService{
				finishRunFn: func(ctx context.Context, actor domain.Actor, in domain.FinishRunInput) (domain.Run, error) {
					t.Fatal("domain must NOT be called for invalid input")
					return domain.Run{}, nil
				},
			}
			client := newTestClient(t, mock, testClaims)
			_, err := client.UpdateRunStatus(authCtx(), tc.req)
			requireCode(t, err, codes.InvalidArgument)
			if mock.calls != 0 {
				t.Fatalf("domain called %d times; want 0", mock.calls)
			}
		})
	}
}

func TestUpdateRunStatus_InvalidTransition_MapsFailedPrecondition(t *testing.T) {
	mock := &mockService{
		finishRunFn: func(ctx context.Context, actor domain.Actor, in domain.FinishRunInput) (domain.Run, error) {
			return domain.Run{}, domain.ErrInvalidStatusTransition
		},
	}
	client := newTestClient(t, mock, testClaims)
	_, err := client.UpdateRunStatus(authCtx(), &experimentv1.UpdateRunStatusRequest{
		RunId: "run-1", Status: experimentv1.RunStatus_RUN_STATUS_RUNNING,
	})
	requireCode(t, err, codes.FailedPrecondition)
	st, _ := status.FromError(err)
	assertNoLeak(t, st.Message())
}

// ============================================================================
// GetRun / ListRuns
// ============================================================================

func TestGetRun_HappyPath_RunningHasNilEndedAt(t *testing.T) {
	mock := &mockService{
		getRunFn: func(ctx context.Context, actor domain.Actor, id string) (domain.Run, error) {
			return domain.Run{ID: id, Status: domain.RunStatusRunning, StartedAt: time.Now().UTC()}, nil
		},
	}
	client := newTestClient(t, mock, testClaims)
	resp, err := client.GetRun(authCtx(), &experimentv1.GetRunRequest{Id: "run-1"})
	if err != nil {
		t.Fatalf("GetRun error: %v", err)
	}
	if resp.GetRun().GetEndedAt() != nil {
		t.Fatal("ended_at must be nil while RUNNING")
	}
}

func TestGetRun_NotFound(t *testing.T) {
	mock := &mockService{
		getRunFn: func(ctx context.Context, actor domain.Actor, id string) (domain.Run, error) {
			return domain.Run{}, domain.ErrRunNotFound
		},
	}
	client := newTestClient(t, mock, testClaims)
	_, err := client.GetRun(authCtx(), &experimentv1.GetRunRequest{Id: "run-x"})
	requireCode(t, err, codes.NotFound)
}

func TestListRuns_StatusFilterConversion(t *testing.T) {
	mock := &mockService{
		listRunsFn: func(ctx context.Context, actor domain.Actor, experimentID string, statusFilter domain.RunStatus, opts domain.ListOptions) ([]domain.Run, string, error) {
			if statusFilter != domain.RunStatusFailed {
				t.Errorf("statusFilter = %q, want FAILED", statusFilter)
			}
			return []domain.Run{{ID: "r1"}}, "", nil
		},
	}
	client := newTestClient(t, mock, testClaims)
	_, err := client.ListRuns(authCtx(), &experimentv1.ListRunsRequest{
		ExperimentId: "exp-1", StatusFilter: experimentv1.RunStatus_RUN_STATUS_FAILED,
	})
	if err != nil {
		t.Fatalf("ListRuns error: %v", err)
	}
}

func TestListRuns_UnspecifiedFilter_MeansNoFilter(t *testing.T) {
	// status_filter == UNSPECIFIED must convert to RunStatusUnspecified (no filter)
	// and must NOT be rejected — it is a valid "all statuses" request.
	mock := &mockService{
		listRunsFn: func(ctx context.Context, actor domain.Actor, experimentID string, statusFilter domain.RunStatus, opts domain.ListOptions) ([]domain.Run, string, error) {
			if statusFilter != domain.RunStatusUnspecified {
				t.Errorf("statusFilter = %q, want unspecified (no filter)", statusFilter)
			}
			return nil, "", nil
		},
	}
	client := newTestClient(t, mock, testClaims)
	_, err := client.ListRuns(authCtx(), &experimentv1.ListRunsRequest{ExperimentId: "exp-1"})
	if err != nil {
		t.Fatalf("ListRuns error: %v", err)
	}
}

func TestListRuns_Validation_EmptyExperimentID(t *testing.T) {
	mock := &mockService{}
	client := newTestClient(t, mock, testClaims)
	_, err := client.ListRuns(authCtx(), &experimentv1.ListRunsRequest{})
	requireCode(t, err, codes.InvalidArgument)
	if mock.calls != 0 {
		t.Fatalf("domain called %d times; want 0", mock.calls)
	}
}

// ============================================================================
// DeleteRun — idempotency key passthrough
// ============================================================================

func TestDeleteRun_HappyPath_PassesIdempotencyKey(t *testing.T) {
	var gotKey string
	mock := &mockService{
		deleteRunFn: func(ctx context.Context, actor domain.Actor, runID, idempotencyKey string) error {
			gotKey = idempotencyKey
			if runID != "run-1" {
				t.Errorf("runID = %q, want run-1", runID)
			}
			return nil
		},
	}
	client := newTestClient(t, mock, testClaims)
	_, err := client.DeleteRun(authCtx(), &experimentv1.DeleteRunRequest{RunId: "run-1", IdempotencyKey: "k-1"})
	if err != nil {
		t.Fatalf("DeleteRun error: %v", err)
	}
	assertActorLifted(t, mock)
	if gotKey != "k-1" {
		t.Fatalf("idempotency key = %q, want k-1", gotKey)
	}
}

func TestDeleteRun_Validation_EmptyRunID(t *testing.T) {
	mock := &mockService{}
	client := newTestClient(t, mock, testClaims)
	_, err := client.DeleteRun(authCtx(), &experimentv1.DeleteRunRequest{})
	requireCode(t, err, codes.InvalidArgument)
}

// ============================================================================
// LogMetrics — the high-throughput batch path
// ============================================================================

func TestLogMetrics_HappyPath_StampsNoClientTimestamp(t *testing.T) {
	mock := &mockService{
		logMetricsFn: func(ctx context.Context, actor domain.Actor, in domain.LogMetricsInput) (domain.LogMetricsResult, error) {
			if in.RunID != "run-1" || len(in.Points) != 2 {
				t.Errorf("log-metrics input wrong: %+v", in)
			}
			// The handler must NOT forward a client timestamp — the domain stamps it.
			for _, p := range in.Points {
				if !p.Timestamp.IsZero() {
					t.Errorf("handler forwarded a client timestamp %v; it must be server-stamped in the domain", p.Timestamp)
				}
			}
			if in.Points[0].Key != "loss" || in.Points[0].Value != 0.5 || in.Points[0].Step != 1 {
				t.Errorf("point[0] not converted: %+v", in.Points[0])
			}
			return domain.LogMetricsResult{AcceptedCount: 2}, nil
		},
	}
	client := newTestClient(t, mock, testClaims)
	resp, err := client.LogMetrics(authCtx(), &experimentv1.LogMetricsRequest{
		RunId: "run-1",
		Points: []*experimentv1.MetricPoint{
			{Key: "loss", Value: 0.5, Step: 1},
			{Key: "acc", Value: 0.8, Step: 1},
		},
		IdempotencyKey: "batch-1",
	})
	if err != nil {
		t.Fatalf("LogMetrics error: %v", err)
	}
	assertActorLifted(t, mock)
	if resp.GetAcceptedCount() != 2 {
		t.Fatalf("accepted_count = %d, want 2", resp.GetAcceptedCount())
	}
}

func TestLogMetrics_Validation(t *testing.T) {
	cases := []struct {
		name string
		req  *experimentv1.LogMetricsRequest
	}{
		{"empty run_id", &experimentv1.LogMetricsRequest{Points: []*experimentv1.MetricPoint{{Key: "loss"}}}},
		{"no points", &experimentv1.LogMetricsRequest{RunId: "run-1"}},
		{"point missing key", &experimentv1.LogMetricsRequest{RunId: "run-1", Points: []*experimentv1.MetricPoint{{Value: 1}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockService{
				logMetricsFn: func(ctx context.Context, actor domain.Actor, in domain.LogMetricsInput) (domain.LogMetricsResult, error) {
					t.Fatal("domain must NOT be called for invalid input")
					return domain.LogMetricsResult{}, nil
				},
			}
			client := newTestClient(t, mock, testClaims)
			_, err := client.LogMetrics(authCtx(), tc.req)
			requireCode(t, err, codes.InvalidArgument)
			if mock.calls != 0 {
				t.Fatalf("domain called %d times; want 0", mock.calls)
			}
		})
	}
}

func TestLogMetrics_RejectsNaNAndInf(t *testing.T) {
	// NaN/Inf corrupt every downstream aggregation; the handler rejects them at
	// the boundary before the domain is touched.
	for _, v := range []float64{nan(), posInf(), negInf()} {
		mock := &mockService{
			logMetricsFn: func(ctx context.Context, actor domain.Actor, in domain.LogMetricsInput) (domain.LogMetricsResult, error) {
				t.Fatal("domain must NOT be called for NaN/Inf metric values")
				return domain.LogMetricsResult{}, nil
			},
		}
		client := newTestClient(t, mock, testClaims)
		_, err := client.LogMetrics(authCtx(), &experimentv1.LogMetricsRequest{
			RunId:  "run-1",
			Points: []*experimentv1.MetricPoint{{Key: "loss", Value: v}},
		})
		requireCode(t, err, codes.InvalidArgument)
		if mock.calls != 0 {
			t.Fatalf("domain called for non-finite value %v; want 0 calls", v)
		}
	}
}

func TestLogMetrics_RunNotRunning_MapsFailedPrecondition(t *testing.T) {
	mock := &mockService{
		logMetricsFn: func(ctx context.Context, actor domain.Actor, in domain.LogMetricsInput) (domain.LogMetricsResult, error) {
			return domain.LogMetricsResult{}, domain.ErrRunNotRunning
		},
	}
	client := newTestClient(t, mock, testClaims)
	_, err := client.LogMetrics(authCtx(), &experimentv1.LogMetricsRequest{
		RunId: "run-1", Points: []*experimentv1.MetricPoint{{Key: "loss", Value: 0.1}},
	})
	requireCode(t, err, codes.FailedPrecondition)
}

func TestLogMetrics_BatchTooLarge_MapsInvalidArgument(t *testing.T) {
	mock := &mockService{
		logMetricsFn: func(ctx context.Context, actor domain.Actor, in domain.LogMetricsInput) (domain.LogMetricsResult, error) {
			return domain.LogMetricsResult{}, domain.ErrBatchTooLarge
		},
	}
	client := newTestClient(t, mock, testClaims)
	_, err := client.LogMetrics(authCtx(), &experimentv1.LogMetricsRequest{
		RunId: "run-1", Points: []*experimentv1.MetricPoint{{Key: "loss", Value: 0.1}},
	})
	requireCode(t, err, codes.InvalidArgument)
}

// ============================================================================
// LogParams
// ============================================================================

func TestLogParams_HappyPath(t *testing.T) {
	mock := &mockService{
		logParamsFn: func(ctx context.Context, actor domain.Actor, in domain.LogParamsInput) (domain.LogParamsResult, error) {
			if in.RunID != "run-1" || len(in.Params) != 1 || in.Params[0].Key != "opt" {
				t.Errorf("log-params input wrong: %+v", in)
			}
			return domain.LogParamsResult{AcceptedCount: 1}, nil
		},
	}
	client := newTestClient(t, mock, testClaims)
	resp, err := client.LogParams(authCtx(), &experimentv1.LogParamsRequest{
		RunId: "run-1", Params: []*experimentv1.Param{{Key: "opt", Value: "adam"}},
	})
	if err != nil {
		t.Fatalf("LogParams error: %v", err)
	}
	if resp.GetAcceptedCount() != 1 {
		t.Fatalf("accepted_count = %d, want 1", resp.GetAcceptedCount())
	}
}

func TestLogParams_Validation(t *testing.T) {
	cases := []struct {
		name string
		req  *experimentv1.LogParamsRequest
	}{
		{"empty run_id", &experimentv1.LogParamsRequest{Params: []*experimentv1.Param{{Key: "a"}}}},
		{"no params", &experimentv1.LogParamsRequest{RunId: "run-1"}},
		{"param missing key", &experimentv1.LogParamsRequest{RunId: "run-1", Params: []*experimentv1.Param{{Value: "v"}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockService{
				logParamsFn: func(ctx context.Context, actor domain.Actor, in domain.LogParamsInput) (domain.LogParamsResult, error) {
					t.Fatal("domain must NOT be called for invalid input")
					return domain.LogParamsResult{}, nil
				},
			}
			client := newTestClient(t, mock, testClaims)
			_, err := client.LogParams(authCtx(), tc.req)
			requireCode(t, err, codes.InvalidArgument)
		})
	}
}

func TestLogParams_Conflict_MapsFailedPrecondition(t *testing.T) {
	mock := &mockService{
		logParamsFn: func(ctx context.Context, actor domain.Actor, in domain.LogParamsInput) (domain.LogParamsResult, error) {
			return domain.LogParamsResult{}, domain.ErrParamConflict
		},
	}
	client := newTestClient(t, mock, testClaims)
	_, err := client.LogParams(authCtx(), &experimentv1.LogParamsRequest{
		RunId: "run-1", Params: []*experimentv1.Param{{Key: "lr", Value: "0.02"}},
	})
	requireCode(t, err, codes.FailedPrecondition)
}

// ============================================================================
// SetRunArtifacts — structpb <-> map[string]any
// ============================================================================

func TestSetRunArtifacts_HappyPath_ConvertsStruct(t *testing.T) {
	mock := &mockService{
		setArtifactsFn: func(ctx context.Context, actor domain.Actor, in domain.SetArtifactsInput) (domain.Run, error) {
			// The structpb must round-trip to a Go map the domain can read.
			if in.Artifacts["manifest_uri"] != "s3://bucket/model" {
				t.Errorf("artifacts not converted: %+v", in.Artifacts)
			}
			return domain.Run{ID: in.RunID, Artifacts: in.Artifacts}, nil
		},
	}
	client := newTestClient(t, mock, testClaims)

	art, err := structpb.NewStruct(map[string]any{"manifest_uri": "s3://bucket/model"})
	if err != nil {
		t.Fatalf("building struct: %v", err)
	}
	resp, err := client.SetRunArtifacts(authCtx(), &experimentv1.SetRunArtifactsRequest{
		RunId: "run-1", Artifacts: art, IdempotencyKey: "a-1",
	})
	if err != nil {
		t.Fatalf("SetRunArtifacts error: %v", err)
	}
	assertActorLifted(t, mock)
	// The returned run's artifacts must round-trip back out to proto.
	if resp.GetRun().GetArtifacts().GetFields()["manifest_uri"].GetStringValue() != "s3://bucket/model" {
		t.Fatalf("artifacts not surfaced in response: %+v", resp.GetRun().GetArtifacts())
	}
}

func TestSetRunArtifacts_Validation(t *testing.T) {
	cases := []struct {
		name string
		req  *experimentv1.SetRunArtifactsRequest
	}{
		{"empty run_id", &experimentv1.SetRunArtifactsRequest{Artifacts: mustStruct(t)}},
		{"nil artifacts", &experimentv1.SetRunArtifactsRequest{RunId: "run-1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockService{
				setArtifactsFn: func(ctx context.Context, actor domain.Actor, in domain.SetArtifactsInput) (domain.Run, error) {
					t.Fatal("domain must NOT be called for invalid input")
					return domain.Run{}, nil
				},
			}
			client := newTestClient(t, mock, testClaims)
			_, err := client.SetRunArtifacts(authCtx(), tc.req)
			requireCode(t, err, codes.InvalidArgument)
		})
	}
}

// ============================================================================
// GetMetricHistory — step window + pagination
// ============================================================================

func TestGetMetricHistory_HappyPath_StepWindow(t *testing.T) {
	mock := &mockService{
		getHistoryFn: func(ctx context.Context, actor domain.Actor, in domain.GetMetricHistoryInput) ([]domain.MetricSeries, string, error) {
			if in.RunID != "run-1" {
				t.Errorf("runID = %q, want run-1", in.RunID)
			}
			// A non-zero window must set the Has* flags so the domain knows the bound
			// is real (vs. "no bound").
			if !in.HasMinStep || !in.HasMaxStep || in.MinStep != 5 || in.MaxStep != 50 {
				t.Errorf("step window not threaded: %+v", in)
			}
			if in.Pagination.PageSize != defaultMetricPageSize {
				t.Errorf("metric page size default = %d, want %d", in.Pagination.PageSize, defaultMetricPageSize)
			}
			return []domain.MetricSeries{
				{Key: "loss", Points: []domain.MetricPoint{{Key: "loss", Value: 0.3, Step: 5, Timestamp: time.Now().UTC()}}},
			}, "cursor-x", nil
		},
	}
	client := newTestClient(t, mock, testClaims)
	resp, err := client.GetMetricHistory(authCtx(), &experimentv1.GetMetricHistoryRequest{
		RunId: "run-1", MinStep: 5, MaxStep: 50,
	})
	if err != nil {
		t.Fatalf("GetMetricHistory error: %v", err)
	}
	if resp.GetPagination().GetNextPageToken() != "cursor-x" {
		t.Fatalf("next token = %q, want cursor-x", resp.GetPagination().GetNextPageToken())
	}
	if len(resp.GetSeries()) != 1 || resp.GetSeries()[0].GetKey() != "loss" {
		t.Fatalf("series not surfaced: %+v", resp.GetSeries())
	}
}

func TestGetMetricHistory_NoWindow_HasFlagsFalse(t *testing.T) {
	// Both steps 0 = no bound: the Has* flags must be false so the domain reads the
	// whole series rather than filtering to step 0.
	mock := &mockService{
		getHistoryFn: func(ctx context.Context, actor domain.Actor, in domain.GetMetricHistoryInput) ([]domain.MetricSeries, string, error) {
			if in.HasMinStep || in.HasMaxStep {
				t.Errorf("Has* flags should be false for an unbounded window: %+v", in)
			}
			return nil, "", nil
		},
	}
	client := newTestClient(t, mock, testClaims)
	_, err := client.GetMetricHistory(authCtx(), &experimentv1.GetMetricHistoryRequest{RunId: "run-1"})
	if err != nil {
		t.Fatalf("GetMetricHistory error: %v", err)
	}
}

func TestGetMetricHistory_Validation(t *testing.T) {
	cases := []struct {
		name string
		req  *experimentv1.GetMetricHistoryRequest
	}{
		{"empty run_id", &experimentv1.GetMetricHistoryRequest{}},
		{"inverted window", &experimentv1.GetMetricHistoryRequest{RunId: "run-1", MinStep: 100, MaxStep: 10}},
		{"negative page size", &experimentv1.GetMetricHistoryRequest{RunId: "run-1", Pagination: &commonv1.PaginationRequest{PageSize: -1}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockService{
				getHistoryFn: func(ctx context.Context, actor domain.Actor, in domain.GetMetricHistoryInput) ([]domain.MetricSeries, string, error) {
					t.Fatal("domain must NOT be called for invalid input")
					return nil, "", nil
				},
			}
			client := newTestClient(t, mock, testClaims)
			_, err := client.GetMetricHistory(authCtx(), tc.req)
			requireCode(t, err, codes.InvalidArgument)
		})
	}
}

func TestGetMetricHistory_MetricPageSizeClamp(t *testing.T) {
	mock := &mockService{
		getHistoryFn: func(ctx context.Context, actor domain.Actor, in domain.GetMetricHistoryInput) ([]domain.MetricSeries, string, error) {
			if in.Pagination.PageSize != maxMetricPageSize {
				t.Errorf("metric page size = %d, want clamp to %d", in.Pagination.PageSize, maxMetricPageSize)
			}
			return nil, "", nil
		},
	}
	client := newTestClient(t, mock, testClaims)
	_, err := client.GetMetricHistory(authCtx(), &experimentv1.GetMetricHistoryRequest{
		RunId: "run-1", Pagination: &commonv1.PaginationRequest{PageSize: 1_000_000},
	})
	if err != nil {
		t.Fatalf("GetMetricHistory error: %v", err)
	}
}

// ============================================================================
// CompareRuns — run-count bound
// ============================================================================

func TestCompareRuns_HappyPath(t *testing.T) {
	mock := &mockService{
		compareRunsFn: func(ctx context.Context, actor domain.Actor, in domain.CompareRunsInput) ([]domain.RunComparison, error) {
			if len(in.RunIDs) != 2 || len(in.MetricKeys) != 1 {
				t.Errorf("compare input wrong: %+v", in)
			}
			return []domain.RunComparison{
				{Run: domain.Run{ID: "r1"}, Series: []domain.MetricSeries{{Key: "loss"}}},
				{Run: domain.Run{ID: "r2"}, Series: []domain.MetricSeries{{Key: "loss"}}},
			}, nil
		},
	}
	client := newTestClient(t, mock, testClaims)
	resp, err := client.CompareRuns(authCtx(), &experimentv1.CompareRunsRequest{
		RunIds: []string{"r1", "r2"}, MetricKeys: []string{"loss"},
	})
	if err != nil {
		t.Fatalf("CompareRuns error: %v", err)
	}
	assertActorLifted(t, mock)
	if len(resp.GetComparisons()) != 2 {
		t.Fatalf("len(comparisons) = %d, want 2", len(resp.GetComparisons()))
	}
	if resp.GetComparisons()[0].GetRun().GetId() != "r1" {
		t.Fatalf("comparison[0] run id = %q, want r1", resp.GetComparisons()[0].GetRun().GetId())
	}
}

func TestCompareRuns_Validation(t *testing.T) {
	tooMany := make([]string, maxCompareRuns+1)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("r%d", i)
	}
	cases := []struct {
		name string
		req  *experimentv1.CompareRunsRequest
	}{
		{"no runs", &experimentv1.CompareRunsRequest{}},
		{"too many runs", &experimentv1.CompareRunsRequest{RunIds: tooMany}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockService{
				compareRunsFn: func(ctx context.Context, actor domain.Actor, in domain.CompareRunsInput) ([]domain.RunComparison, error) {
					t.Fatal("domain must NOT be called for invalid input")
					return nil, nil
				},
			}
			client := newTestClient(t, mock, testClaims)
			_, err := client.CompareRuns(authCtx(), tc.req)
			requireCode(t, err, codes.InvalidArgument)
			if mock.calls != 0 {
				t.Fatalf("domain called %d times; want 0", mock.calls)
			}
		})
	}
}

func TestCompareRuns_TooManyRuns_FromDomain_MapsInvalidArgument(t *testing.T) {
	// Defense in depth: even if the handler's count check were bypassed, a domain
	// ErrTooManyRuns still maps to InvalidArgument.
	mock := &mockService{
		compareRunsFn: func(ctx context.Context, actor domain.Actor, in domain.CompareRunsInput) ([]domain.RunComparison, error) {
			return nil, domain.ErrTooManyRuns
		},
	}
	client := newTestClient(t, mock, testClaims)
	_, err := client.CompareRuns(authCtx(), &experimentv1.CompareRunsRequest{RunIds: []string{"r1"}})
	requireCode(t, err, codes.InvalidArgument)
}

// ----------------------------------------------------------------------------
// non-finite float helpers (named so the test reads "this is a NaN/Inf case").
// We use math.NaN/Inf rather than literal arithmetic: a constant like 1e308*10
// overflows at COMPILE time (Go rejects it), so the non-finite value must be
// produced at runtime via the math package.
// ----------------------------------------------------------------------------

func nan() float64    { return math.NaN() }
func posInf() float64 { return math.Inf(1) }
func negInf() float64 { return math.Inf(-1) }

// mustStruct builds a non-nil structpb for tests that need a present (but
// otherwise irrelevant) artifacts payload.
func mustStruct(t *testing.T) *structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(map[string]any{"k": "v"})
	if err != nil {
		t.Fatalf("building struct: %v", err)
	}
	return s
}
