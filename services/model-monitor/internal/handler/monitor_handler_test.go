// monitor_handler_test.go — COMPONENT tests for the Model Monitor gRPC handler.
//
// ============================================================================
// WHAT "COMPONENT TEST" MEANS HERE (and why this is the right level)
// ============================================================================
//
// These tests exercise the handler END-TO-END through a REAL gRPC stack
// (bufconn: an in-process HTTP/2 transport — see pkg/testutil/grpc.go) but with
// the DOMAIN SERVICE replaced by a hand-written mock. That isolation is
// deliberate:
//
//   - We mock the MonitorService INTERFACE, not the repositories. The unit under
//     test is the HANDLER: proto<->domain conversion, validation, error mapping,
//     the server-authoritative team-from-claims invariant, and the no-leak rule.
//     Mocking the service (one interface) instead of the five+ ports keeps the
//     test about the handler, not the domain's wiring. The domain's own logic has
//     its own -race unit tests.
//
//   - Using bufconn instead of calling handler methods directly means the proto
//     actually serializes over the wire: a field we forget to map shows up as a
//     zero value on the client side, a status error round-trips through gRPC's
//     status machinery exactly as a real client sees it, and the SERVER-STREAM
//     RPC is driven through real stream semantics (RecvMsg/terminal status).
//
// NO testcontainers, NO database, NO network sockets — this whole file runs in
// milliseconds and is safe to run on every change.
//
// ============================================================================
// THE FOUR THINGS EVERY RPC TEST ASSERTS (the task's contract)
// ============================================================================
//
//	(a) HAPPY PATH: proto<->domain conversion is correct (the mock records the
//	    domain input it received; we assert the response fields AND that identity
//	    came from CLAIMS, not the request).
//	(b) VALIDATION: bad requests are rejected with codes.InvalidArgument BEFORE
//	    the domain is ever called (we assert the mock was NOT invoked).
//	(c) ERROR MAPPING: each domain sentinel maps to the right gRPC status code.
//	(d) NO LEAK: error messages never contain internal/SQL/PII text.
package handler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	monitorv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/monitor/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ----------------------------------------------------------------------------
// Tiny test helpers (kept local so the test file is self-contained).
// ----------------------------------------------------------------------------

// errFake fabricates an UNRECOGNIZED domain error (one toStatusError must
// sanitize to Internal) — typically wrapping fake SQL/infra text.
func errFake(msg string) error { return errors.New(msg) }

// wrap simulates the domain wrapping a sentinel with a specific message
// (fmt.Errorf("...: %w", sentinel)) so we can verify errors.Is-based mapping
// survives wrapping.
func wrap(sentinel error, msg string) error { return fmt.Errorf("%s: %w", msg, sentinel) }

// stubValidator implements grpcutil.TokenValidator by returning fixed claims.
// We drive the REAL grpcutil auth interceptors with it so the test exercises the
// genuine claims-injection path (the handler reads claims via grpcutil's own
// private context key, which only those interceptors can populate).
type stubValidator struct {
	claims *grpcutil.Claims
	err    error
}

func (s stubValidator) Validate(ctx context.Context, token string) (*grpcutil.Claims, error) {
	return s.claims, s.err
}

// ============================================================================
// MOCK domain.MonitorService — a hand-written test double
// ============================================================================
//
// WHY hand-written and not gomock/mockery: a hand-written mock with function
// fields is dependency-free, reads top-to-bottom, and lets each test set ONLY the
// behavior it needs. Call recorders make "this RPC must NOT touch the domain"
// (validation tests) precise. The DATA-plane methods (ObserveInference,
// ResetBaselineFromPromotion) are part of the interface but are NOT reachable from
// the gRPC handler, so they are implemented as unreachable stubs that fail the test
// if ever called.
type mockMonitorService struct {
	configureFn       func(ctx context.Context, ownerTeam string, in domain.ConfigureMonitorInput) (domain.Monitor, bool, error)
	deleteFn          func(ctx context.Context, ownerTeam, modelName string, purge bool) (int, error)
	resetBaselineFn   func(ctx context.Context, ownerTeam, modelName, version string) (domain.Monitor, error)
	getHealthFn       func(ctx context.Context, ownerTeam, modelName string) (domain.ModelHealth, error)
	getStatusFn       func(ctx context.Context, ownerTeam, modelName string) (domain.MonitorStatus, error)
	listMonitorsFn    func(ctx context.Context, ownerTeam string, minSeverity domain.DriftSeverity, state domain.MonitorState, opts domain.ListOptions) ([]domain.FleetEntry, string, error)
	getReportFn       func(ctx context.Context, ownerTeam, reportID string) (domain.DriftReport, error)
	listReportsFn     func(ctx context.Context, ownerTeam string, f domain.ReportFilter, opts domain.ListOptions) ([]domain.DriftReport, string, error)
	submitGroundTruth func(ctx context.Context, ownerTeam string, in domain.SubmitGroundTruthInput) (domain.SubmitGroundTruthResult, error)

	// Call recorders — let validation tests assert the domain was NOT reached.
	configureCalls     int
	deleteCalls        int
	resetBaselineCalls int
	getHealthCalls     int
	getStatusCalls     int
	listMonitorsCalls  int
	getReportCalls     int
	listReportsCalls   int
	submitCalls        int

	// Captured inputs for happy-path conversion assertions.
	lastOwnerTeam    string
	lastConfigureIn  domain.ConfigureMonitorInput
	lastDeleteModel  string
	lastDeletePurge  bool
	lastResetVersion string
	lastListSeverity domain.DriftSeverity
	lastListState    domain.MonitorState
	lastListOpts     domain.ListOptions
	lastReportID     string
	lastReportFilter domain.ReportFilter
	lastReportOpts   domain.ListOptions
	lastSubmitInput  domain.SubmitGroundTruthInput
}

// --- CONTROL-PLANE methods (reachable from the handler) ---

func (m *mockMonitorService) ConfigureMonitor(ctx context.Context, ownerTeam string, in domain.ConfigureMonitorInput) (domain.Monitor, bool, error) {
	m.configureCalls++
	m.lastOwnerTeam = ownerTeam
	m.lastConfigureIn = in
	return m.configureFn(ctx, ownerTeam, in)
}

func (m *mockMonitorService) DeleteMonitor(ctx context.Context, ownerTeam, modelName string, purge bool) (int, error) {
	m.deleteCalls++
	m.lastOwnerTeam = ownerTeam
	m.lastDeleteModel = modelName
	m.lastDeletePurge = purge
	return m.deleteFn(ctx, ownerTeam, modelName, purge)
}

func (m *mockMonitorService) ResetBaseline(ctx context.Context, ownerTeam, modelName, version string) (domain.Monitor, error) {
	m.resetBaselineCalls++
	m.lastOwnerTeam = ownerTeam
	m.lastResetVersion = version
	return m.resetBaselineFn(ctx, ownerTeam, modelName, version)
}

func (m *mockMonitorService) GetModelHealth(ctx context.Context, ownerTeam, modelName string) (domain.ModelHealth, error) {
	m.getHealthCalls++
	m.lastOwnerTeam = ownerTeam
	return m.getHealthFn(ctx, ownerTeam, modelName)
}

func (m *mockMonitorService) GetMonitorStatus(ctx context.Context, ownerTeam, modelName string) (domain.MonitorStatus, error) {
	m.getStatusCalls++
	m.lastOwnerTeam = ownerTeam
	return m.getStatusFn(ctx, ownerTeam, modelName)
}

func (m *mockMonitorService) ListMonitors(ctx context.Context, ownerTeam string, minSeverity domain.DriftSeverity, state domain.MonitorState, opts domain.ListOptions) ([]domain.FleetEntry, string, error) {
	m.listMonitorsCalls++
	m.lastOwnerTeam = ownerTeam
	m.lastListSeverity = minSeverity
	m.lastListState = state
	m.lastListOpts = opts
	return m.listMonitorsFn(ctx, ownerTeam, minSeverity, state, opts)
}

func (m *mockMonitorService) GetDriftReport(ctx context.Context, ownerTeam, reportID string) (domain.DriftReport, error) {
	m.getReportCalls++
	m.lastOwnerTeam = ownerTeam
	m.lastReportID = reportID
	return m.getReportFn(ctx, ownerTeam, reportID)
}

func (m *mockMonitorService) ListDriftReports(ctx context.Context, ownerTeam string, f domain.ReportFilter, opts domain.ListOptions) ([]domain.DriftReport, string, error) {
	m.listReportsCalls++
	m.lastOwnerTeam = ownerTeam
	m.lastReportFilter = f
	m.lastReportOpts = opts
	return m.listReportsFn(ctx, ownerTeam, f, opts)
}

func (m *mockMonitorService) SubmitGroundTruth(ctx context.Context, ownerTeam string, in domain.SubmitGroundTruthInput) (domain.SubmitGroundTruthResult, error) {
	m.submitCalls++
	m.lastOwnerTeam = ownerTeam
	m.lastSubmitInput = in
	return m.submitGroundTruth(ctx, ownerTeam, in)
}

// --- DATA-PLANE methods (NOT reachable from the handler) ---
// They satisfy the interface but must never be called via a gRPC RPC. We make
// that an explicit, hard failure rather than a silent no-op.

func (m *mockMonitorService) ObserveInference(ctx context.Context, obs domain.InferenceObservation) (*domain.DriftReport, error) {
	panic("ObserveInference is a data-plane method and must not be reached via the gRPC handler")
}

func (m *mockMonitorService) ResetBaselineFromPromotion(ctx context.Context, ownerTeam, modelName, newProdVersion string) error {
	panic("ResetBaselineFromPromotion is a data-plane method and must not be reached via the gRPC handler")
}

// Compile-time proof the mock fully satisfies the interface the handler depends on.
var _ domain.MonitorService = (*mockMonitorService)(nil)

// ============================================================================
// TEST HARNESS
// ============================================================================

// testTeam is the team carried by the default injected claims; the handler must
// derive owner_team from this, never from request fields.
const testTeam = "ml-platform"

// newTestClient wires the handler (backed by the given service) onto a bufconn
// gRPC server and returns a ready MonitorServiceClient. By default it installs the
// REAL grpcutil auth interceptors (unary + stream) backed by a stub validator that
// returns fixed claims, so every RPC sees an authenticated, team-scoped caller
// exactly as in production. Pass withClaims=false to omit the interceptors (to
// test the fail-closed "missing authentication" path).
func newTestClient(t *testing.T, svc domain.MonitorService, withClaims bool) monitorv1.MonitorServiceClient {
	t.Helper()
	h := NewMonitorHandler(svc)

	var opts []grpc.ServerOption
	if withClaims {
		v := stubValidator{claims: &grpcutil.Claims{UserID: "u-1", Team: testTeam}}
		opts = append(opts,
			grpc.ChainUnaryInterceptor(grpcutil.AuthUnaryInterceptor(v)),
			grpc.ChainStreamInterceptor(grpcutil.AuthStreamInterceptor(v)),
		)
	}

	conn := testutil.NewTestGRPCServer(t, func(s *grpc.Server) {
		monitorv1.RegisterMonitorServiceServer(s, h)
	}, opts...)
	return monitorv1.NewMonitorServiceClient(conn)
}

// authCtx attaches a bearer header so the server-side auth interceptor extracts a
// token and runs the stub validator. The token value is irrelevant (the stub
// ignores it) but it must be present, since extractBearerToken rejects a missing
// header before the validator runs.
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

// assertNoLeak fails if the message contains any substring that would indicate an
// internal/SQL/secret/PII leak. This is the (d) guarantee, checked uniformly.
func assertNoLeak(t *testing.T, msg string) {
	t.Helper()
	lower := strings.ToLower(msg)
	for _, banned := range []string{
		"repository:", // storage-layer sentinel text
		"sql",         // SQL fragments
		"pgx", "pq:",  // driver internals
		"redis", // window store internals
		"deadlock",
		"goroutine", // stack-trace leak
		"connection string",
		"panic",
		"5432", // DB port — infra topology
	} {
		if strings.Contains(lower, banned) {
			t.Fatalf("error message leaks internal detail (%q): %q", banned, msg)
		}
	}
}

// fullMonitor is a representative domain.Monitor used across happy-path tests; it
// exercises every conversion path (durations, timestamps, enums, thresholds).
func fullMonitor() domain.Monitor {
	return domain.Monitor{
		ID:             "mon-uuid-1",
		ModelName:      "fraud-detector",
		OwnerTeam:      testTeam,
		WindowDuration: 5 * time.Minute,
		WindowSize:     1000,
		MinSamples:     200,
		Thresholds: []domain.ThresholdConfig{
			{DriftType: domain.DriftTypeData, Method: domain.DriftMethodPSI, WarnScore: 0.1, CriticalScore: 0.25},
		},
		AutoRetrain:        true,
		RetrainPipelineID:  "pipe-7",
		State:              domain.MonitorStateActive,
		BaselineVersion:    "v7",
		BaselineCapturedAt: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
		CreatedAt:          time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt:          time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
	}
}

// ============================================================================
// ConfigureMonitor
// ============================================================================

func TestConfigureMonitor_HappyPath_ConvertsAndUsesClaimTeam(t *testing.T) {
	mock := &mockMonitorService{
		configureFn: func(ctx context.Context, ownerTeam string, in domain.ConfigureMonitorInput) (domain.Monitor, bool, error) {
			return fullMonitor(), true, nil
		},
	}
	client := newTestClient(t, mock, true)

	resp, err := client.ConfigureMonitor(authCtx(), &monitorv1.ConfigureMonitorRequest{
		ModelName:         "fraud-detector",
		WindowDuration:    durationpb.New(5 * time.Minute),
		WindowSize:        1000,
		MinSamples:        200,
		AutoRetrain:       true,
		RetrainPipelineId: "pipe-7",
		Enabled:           true,
		IdempotencyKey:    "idem-1",
		Thresholds: []*monitorv1.ThresholdConfig{
			{
				DriftType:     monitorv1.DriftType_DRIFT_TYPE_DATA,
				Method:        monitorv1.DriftMethod_DRIFT_METHOD_PSI,
				WarnScore:     0.1,
				CriticalScore: 0.25,
			},
		},
	})
	if err != nil {
		t.Fatalf("ConfigureMonitor error: %v", err)
	}

	// (a) SERVER-AUTHORITATIVE team comes from claims, NOT a request field.
	if mock.lastOwnerTeam != testTeam {
		t.Fatalf("owner team = %q; want %q (from claims)", mock.lastOwnerTeam, testTeam)
	}

	// (a) proto -> domain conversion of the writable surface.
	in := mock.lastConfigureIn
	if in.ModelName != "fraud-detector" || in.WindowDuration != 5*time.Minute ||
		in.WindowSize != 1000 || in.MinSamples != 200 || !in.AutoRetrain ||
		in.RetrainPipelineID != "pipe-7" || !in.Enabled || in.IdempotencyKey != "idem-1" {
		t.Fatalf("domain input mismatch: %+v", in)
	}
	if len(in.Thresholds) != 1 || in.Thresholds[0].DriftType != domain.DriftTypeData ||
		in.Thresholds[0].Method != domain.DriftMethodPSI || in.Thresholds[0].CriticalScore != 0.25 {
		t.Fatalf("threshold conversion mismatch: %+v", in.Thresholds)
	}

	// (a) domain -> proto: created flag + server-authoritative fields round-trip.
	if !resp.GetCreated() {
		t.Fatalf("created = false; want true")
	}
	m := resp.GetMonitor()
	if m.GetId() != "mon-uuid-1" || m.GetState() != monitorv1.MonitorState_MONITOR_STATE_ACTIVE ||
		m.GetBaselineVersion() != "v7" || m.GetOwnerTeam() != testTeam {
		t.Fatalf("proto monitor mismatch: %+v", m)
	}
	if m.GetWindowDuration().AsDuration() != 5*time.Minute {
		t.Fatalf("window_duration not converted: %v", m.GetWindowDuration().AsDuration())
	}
	if !m.GetCreatedAt().AsTime().Equal(fullMonitor().CreatedAt) {
		t.Fatalf("created_at not converted: %v", m.GetCreatedAt().AsTime())
	}
}

func TestConfigureMonitor_Validation(t *testing.T) {
	cases := []struct {
		name string
		req  *monitorv1.ConfigureMonitorRequest
	}{
		{"missing model_name", &monitorv1.ConfigureMonitorRequest{WindowSize: 10}},
		{"negative window_size", &monitorv1.ConfigureMonitorRequest{ModelName: "m", WindowSize: -1}},
		{"window_size over cap", &monitorv1.ConfigureMonitorRequest{ModelName: "m", WindowSize: domain.MaxWindowSize + 1}},
		{"negative min_samples", &monitorv1.ConfigureMonitorRequest{ModelName: "m", MinSamples: -1}},
		{"negative window_duration", &monitorv1.ConfigureMonitorRequest{ModelName: "m", WindowDuration: durationpb.New(-time.Second)}},
		{"unknown threshold drift_type", &monitorv1.ConfigureMonitorRequest{
			ModelName:  "m",
			Thresholds: []*monitorv1.ThresholdConfig{{DriftType: monitorv1.DriftType(99)}},
		}},
		{"unknown threshold method", &monitorv1.ConfigureMonitorRequest{
			ModelName:  "m",
			Thresholds: []*monitorv1.ThresholdConfig{{DriftType: monitorv1.DriftType_DRIFT_TYPE_DATA, Method: monitorv1.DriftMethod(42)}},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockMonitorService{
				configureFn: func(ctx context.Context, ownerTeam string, in domain.ConfigureMonitorInput) (domain.Monitor, bool, error) {
					t.Fatal("domain ConfigureMonitor must NOT be called for invalid input")
					return domain.Monitor{}, false, nil
				},
			}
			client := newTestClient(t, mock, true)
			_, err := client.ConfigureMonitor(authCtx(), tc.req)
			requireCode(t, err, codes.InvalidArgument)
			if mock.configureCalls != 0 {
				t.Fatalf("domain ConfigureMonitor called %d times; want 0", mock.configureCalls)
			}
		})
	}
}

func TestConfigureMonitor_ErrorMapping(t *testing.T) {
	cases := []struct {
		name     string
		domErr   error
		wantCode codes.Code
	}{
		{"validation", wrap(domain.ErrValidation, "warn_score must be <= critical_score"), codes.InvalidArgument},
		{"pipeline required", domain.ErrRetrainPipelineRequired, codes.InvalidArgument},
		{"baseline unavailable", domain.ErrBaselineUnavailable, codes.FailedPrecondition},
		{"unknown -> internal", errFake("repository: deadlock detected on monitors table at db-primary:5432"), codes.Internal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockMonitorService{
				configureFn: func(ctx context.Context, ownerTeam string, in domain.ConfigureMonitorInput) (domain.Monitor, bool, error) {
					return domain.Monitor{}, false, tc.domErr
				},
			}
			client := newTestClient(t, mock, true)
			_, err := client.ConfigureMonitor(authCtx(), &monitorv1.ConfigureMonitorRequest{ModelName: "m"})
			requireCode(t, err, tc.wantCode)
			st, _ := status.FromError(err)
			assertNoLeak(t, st.Message())
		})
	}
}

func TestConfigureMonitor_NoClaims_Unauthenticated(t *testing.T) {
	mock := &mockMonitorService{
		configureFn: func(ctx context.Context, ownerTeam string, in domain.ConfigureMonitorInput) (domain.Monitor, bool, error) {
			t.Fatal("domain must NOT be called without claims")
			return domain.Monitor{}, false, nil
		},
	}
	client := newTestClient(t, mock, false) // no auth interceptor -> no claims
	_, err := client.ConfigureMonitor(context.Background(), &monitorv1.ConfigureMonitorRequest{ModelName: "m"})
	requireCode(t, err, codes.Unauthenticated)
	if mock.configureCalls != 0 {
		t.Fatalf("domain called %d times without claims; want 0", mock.configureCalls)
	}
}

// ============================================================================
// DeleteMonitor
// ============================================================================

func TestDeleteMonitor_HappyPath(t *testing.T) {
	mock := &mockMonitorService{
		deleteFn: func(ctx context.Context, ownerTeam, modelName string, purge bool) (int, error) {
			return 42, nil
		},
	}
	client := newTestClient(t, mock, true)
	resp, err := client.DeleteMonitor(authCtx(), &monitorv1.DeleteMonitorRequest{
		ModelName: "fraud-detector", PurgeReports: true,
	})
	if err != nil {
		t.Fatalf("DeleteMonitor error: %v", err)
	}
	if resp.GetPurgedReportCount() != 42 {
		t.Fatalf("purged_report_count = %d; want 42", resp.GetPurgedReportCount())
	}
	if mock.lastOwnerTeam != testTeam || mock.lastDeleteModel != "fraud-detector" || !mock.lastDeletePurge {
		t.Fatalf("forwarded args wrong: team=%q model=%q purge=%v", mock.lastOwnerTeam, mock.lastDeleteModel, mock.lastDeletePurge)
	}
}

func TestDeleteMonitor_Validation_MissingModel(t *testing.T) {
	mock := &mockMonitorService{
		deleteFn: func(ctx context.Context, ownerTeam, modelName string, purge bool) (int, error) {
			t.Fatal("domain DeleteMonitor must NOT be called for invalid input")
			return 0, nil
		},
	}
	client := newTestClient(t, mock, true)
	_, err := client.DeleteMonitor(authCtx(), &monitorv1.DeleteMonitorRequest{})
	requireCode(t, err, codes.InvalidArgument)
	if mock.deleteCalls != 0 {
		t.Fatalf("domain DeleteMonitor called %d times; want 0", mock.deleteCalls)
	}
}

func TestDeleteMonitor_InternalError_Sanitized(t *testing.T) {
	mock := &mockMonitorService{
		deleteFn: func(ctx context.Context, ownerTeam, modelName string, purge bool) (int, error) {
			return 0, errFake("pq: connection refused to db-primary:5432 password=topsecret")
		},
	}
	client := newTestClient(t, mock, true)
	_, err := client.DeleteMonitor(authCtx(), &monitorv1.DeleteMonitorRequest{ModelName: "m"})
	requireCode(t, err, codes.Internal)
	st, _ := status.FromError(err)
	assertNoLeak(t, st.Message())
	if strings.Contains(st.Message(), "topsecret") || strings.Contains(st.Message(), "db-primary") {
		t.Fatalf("internal error leaked infra detail: %q", st.Message())
	}
}

// ============================================================================
// ResetBaseline
// ============================================================================

func TestResetBaseline_HappyPath_ForwardsVersion(t *testing.T) {
	mock := &mockMonitorService{
		resetBaselineFn: func(ctx context.Context, ownerTeam, modelName, version string) (domain.Monitor, error) {
			return fullMonitor(), nil
		},
	}
	client := newTestClient(t, mock, true)
	resp, err := client.ResetBaseline(authCtx(), &monitorv1.ResetBaselineRequest{
		ModelName: "fraud-detector", BaselineVersion: "v9",
	})
	if err != nil {
		t.Fatalf("ResetBaseline error: %v", err)
	}
	if mock.lastResetVersion != "v9" {
		t.Fatalf("version not forwarded: %q", mock.lastResetVersion)
	}
	if resp.GetMonitor().GetBaselineVersion() != "v7" {
		t.Fatalf("monitor not converted: %+v", resp.GetMonitor())
	}
}

func TestResetBaseline_BaselineUnavailable_FailedPrecondition(t *testing.T) {
	mock := &mockMonitorService{
		resetBaselineFn: func(ctx context.Context, ownerTeam, modelName, version string) (domain.Monitor, error) {
			return domain.Monitor{}, domain.ErrBaselineUnavailable
		},
	}
	client := newTestClient(t, mock, true)
	_, err := client.ResetBaseline(authCtx(), &monitorv1.ResetBaselineRequest{ModelName: "m"})
	requireCode(t, err, codes.FailedPrecondition)
	st, _ := status.FromError(err)
	assertNoLeak(t, st.Message())
}

func TestResetBaseline_NotFound(t *testing.T) {
	mock := &mockMonitorService{
		resetBaselineFn: func(ctx context.Context, ownerTeam, modelName, version string) (domain.Monitor, error) {
			return domain.Monitor{}, domain.ErrMonitorNotFound
		},
	}
	client := newTestClient(t, mock, true)
	_, err := client.ResetBaseline(authCtx(), &monitorv1.ResetBaselineRequest{ModelName: "m"})
	requireCode(t, err, codes.NotFound)
}

func TestResetBaseline_Validation_MissingModel(t *testing.T) {
	mock := &mockMonitorService{
		resetBaselineFn: func(ctx context.Context, ownerTeam, modelName, version string) (domain.Monitor, error) {
			t.Fatal("domain must NOT be called for invalid input")
			return domain.Monitor{}, nil
		},
	}
	client := newTestClient(t, mock, true)
	_, err := client.ResetBaseline(authCtx(), &monitorv1.ResetBaselineRequest{})
	requireCode(t, err, codes.InvalidArgument)
	if mock.resetBaselineCalls != 0 {
		t.Fatalf("domain ResetBaseline called %d times; want 0", mock.resetBaselineCalls)
	}
}

// ============================================================================
// GetModelHealth
// ============================================================================

func TestGetModelHealth_HappyPath_ConvertsSeverityMap(t *testing.T) {
	mock := &mockMonitorService{
		getHealthFn: func(ctx context.Context, ownerTeam, modelName string) (domain.ModelHealth, error) {
			return domain.ModelHealth{
				ModelName:       "fraud-detector",
				ModelVersion:    "v7",
				OverallSeverity: domain.DriftSeverityWarning,
				SeverityByType: map[domain.DriftType]domain.DriftSeverity{
					domain.DriftTypeData:       domain.DriftSeverityWarning,
					domain.DriftTypePrediction: domain.DriftSeverityOK,
				},
				State:            domain.MonitorStateActive,
				LatestReportID:   "rep-1",
				LastEventAt:      time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC),
				DriftEventsTotal: 3,
			}, nil
		},
	}
	client := newTestClient(t, mock, true)
	resp, err := client.GetModelHealth(authCtx(), &monitorv1.GetModelHealthRequest{ModelName: "fraud-detector"})
	if err != nil {
		t.Fatalf("GetModelHealth error: %v", err)
	}
	h := resp.GetHealth()
	if h.GetOverallSeverity() != monitorv1.DriftSeverity_DRIFT_SEVERITY_WARNING {
		t.Fatalf("overall severity = %v", h.GetOverallSeverity())
	}
	if h.GetState() != monitorv1.MonitorState_MONITOR_STATE_ACTIVE || h.GetLatestReportId() != "rep-1" {
		t.Fatalf("health fields mismatch: %+v", h)
	}
	// The severity-by-type map is keyed by the DriftType enum's integer value.
	byType := h.GetSeverityByType()
	if byType[int32(monitorv1.DriftType_DRIFT_TYPE_DATA)] != monitorv1.DriftSeverity_DRIFT_SEVERITY_WARNING {
		t.Fatalf("data light wrong: %v", byType[int32(monitorv1.DriftType_DRIFT_TYPE_DATA)])
	}
	if byType[int32(monitorv1.DriftType_DRIFT_TYPE_PREDICTION)] != monitorv1.DriftSeverity_DRIFT_SEVERITY_OK {
		t.Fatalf("prediction light wrong: %v", byType[int32(monitorv1.DriftType_DRIFT_TYPE_PREDICTION)])
	}
}

func TestGetModelHealth_NotFound(t *testing.T) {
	mock := &mockMonitorService{
		getHealthFn: func(ctx context.Context, ownerTeam, modelName string) (domain.ModelHealth, error) {
			return domain.ModelHealth{}, domain.ErrMonitorNotFound
		},
	}
	client := newTestClient(t, mock, true)
	_, err := client.GetModelHealth(authCtx(), &monitorv1.GetModelHealthRequest{ModelName: "ghost"})
	requireCode(t, err, codes.NotFound)
	st, _ := status.FromError(err)
	assertNoLeak(t, st.Message())
}

func TestGetModelHealth_Validation_MissingModel(t *testing.T) {
	mock := &mockMonitorService{
		getHealthFn: func(ctx context.Context, ownerTeam, modelName string) (domain.ModelHealth, error) {
			t.Fatal("domain must NOT be called for invalid input")
			return domain.ModelHealth{}, nil
		},
	}
	client := newTestClient(t, mock, true)
	_, err := client.GetModelHealth(authCtx(), &monitorv1.GetModelHealthRequest{})
	requireCode(t, err, codes.InvalidArgument)
	if mock.getHealthCalls != 0 {
		t.Fatalf("domain GetModelHealth called %d times; want 0", mock.getHealthCalls)
	}
}

// ============================================================================
// GetMonitorStatus
// ============================================================================

func TestGetMonitorStatus_HappyPath_NilLatestReportStaysNil(t *testing.T) {
	mock := &mockMonitorService{
		getStatusFn: func(ctx context.Context, ownerTeam, modelName string) (domain.MonitorStatus, error) {
			return domain.MonitorStatus{
				Monitor:              fullMonitor(),
				State:                domain.MonitorStateWarmingUp,
				CurrentWindowSamples: 130,
				LatestReport:         nil, // no window closed yet
				LastEventAt:          time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC),
				DriftEventsTotal:     0,
			}, nil
		},
	}
	client := newTestClient(t, mock, true)
	resp, err := client.GetMonitorStatus(authCtx(), &monitorv1.GetMonitorStatusRequest{ModelName: "fraud-detector"})
	if err != nil {
		t.Fatalf("GetMonitorStatus error: %v", err)
	}
	s := resp.GetStatus()
	if s.GetState() != monitorv1.MonitorState_MONITOR_STATE_WARMING_UP || s.GetCurrentWindowSamples() != 130 {
		t.Fatalf("status fields mismatch: %+v", s)
	}
	if s.GetLatestReport() != nil {
		t.Fatalf("latest_report should be nil before a window closes, got: %+v", s.GetLatestReport())
	}
	if s.GetMonitor().GetId() != "mon-uuid-1" {
		t.Fatalf("embedded monitor not converted: %+v", s.GetMonitor())
	}
}

func TestGetMonitorStatus_HappyPath_WithLatestReport(t *testing.T) {
	report := domain.DriftReport{
		ID: "rep-9", MonitorID: "mon-uuid-1", ModelName: "fraud-detector", ModelVersion: "v7",
		DriftType: domain.DriftTypeData, Severity: domain.DriftSeverityCritical,
		Metrics: []domain.DriftMetric{
			{Name: "income", Method: domain.DriftMethodPSI, Score: 0.41, BaselineValue: 5.1, CurrentValue: 7.8, Severity: domain.DriftSeverityCritical},
		},
		WindowID: "win-1", SampleCount: 1200,
		WindowStart: time.Date(2026, 6, 10, 11, 0, 0, 0, time.UTC),
		WindowEnd:   time.Date(2026, 6, 10, 11, 5, 0, 0, time.UTC),
		CreatedAt:   time.Date(2026, 6, 10, 11, 5, 1, 0, time.UTC),
	}
	mock := &mockMonitorService{
		getStatusFn: func(ctx context.Context, ownerTeam, modelName string) (domain.MonitorStatus, error) {
			return domain.MonitorStatus{Monitor: fullMonitor(), State: domain.MonitorStateActive, LatestReport: &report, DriftEventsTotal: 1}, nil
		},
	}
	client := newTestClient(t, mock, true)
	resp, err := client.GetMonitorStatus(authCtx(), &monitorv1.GetMonitorStatusRequest{ModelName: "fraud-detector"})
	if err != nil {
		t.Fatalf("GetMonitorStatus error: %v", err)
	}
	lr := resp.GetStatus().GetLatestReport()
	if lr.GetId() != "rep-9" || lr.GetSeverity() != monitorv1.DriftSeverity_DRIFT_SEVERITY_CRITICAL || lr.GetSampleCount() != 1200 {
		t.Fatalf("latest report mismatch: %+v", lr)
	}
	if len(lr.GetMetrics()) != 1 || lr.GetMetrics()[0].GetName() != "income" || lr.GetMetrics()[0].GetScore() != 0.41 {
		t.Fatalf("metric conversion mismatch: %+v", lr.GetMetrics())
	}
}

func TestGetMonitorStatus_Validation_MissingModel(t *testing.T) {
	mock := &mockMonitorService{
		getStatusFn: func(ctx context.Context, ownerTeam, modelName string) (domain.MonitorStatus, error) {
			t.Fatal("domain must NOT be called for invalid input")
			return domain.MonitorStatus{}, nil
		},
	}
	client := newTestClient(t, mock, true)
	_, err := client.GetMonitorStatus(authCtx(), &monitorv1.GetMonitorStatusRequest{})
	requireCode(t, err, codes.InvalidArgument)
	if mock.getStatusCalls != 0 {
		t.Fatalf("domain GetMonitorStatus called %d times; want 0", mock.getStatusCalls)
	}
}

// ============================================================================
// ListMonitors — pagination normalization + filter conversion
// ============================================================================

func TestListMonitors_HappyPath_ConvertsEntriesAndFilters(t *testing.T) {
	mock := &mockMonitorService{
		listMonitorsFn: func(ctx context.Context, ownerTeam string, minSeverity domain.DriftSeverity, state domain.MonitorState, opts domain.ListOptions) ([]domain.FleetEntry, string, error) {
			return []domain.FleetEntry{
				{Monitor: fullMonitor(), Health: domain.ModelHealth{ModelName: "fraud-detector", OverallSeverity: domain.DriftSeverityOK}},
			}, "next-cursor", nil
		},
	}
	client := newTestClient(t, mock, true)
	resp, err := client.ListMonitors(authCtx(), &monitorv1.ListMonitorsRequest{
		MinSeverity: monitorv1.DriftSeverity_DRIFT_SEVERITY_WARNING,
		State:       monitorv1.MonitorState_MONITOR_STATE_ACTIVE,
		Pagination:  &commonv1.PaginationRequest{PageSize: 10, PageToken: "tok"},
	})
	if err != nil {
		t.Fatalf("ListMonitors error: %v", err)
	}
	// (a) filters converted to domain enums.
	if mock.lastListSeverity != domain.DriftSeverityWarning || mock.lastListState != domain.MonitorStateActive {
		t.Fatalf("filter conversion wrong: sev=%v state=%v", mock.lastListSeverity, mock.lastListState)
	}
	// (a) pagination forwarded.
	if mock.lastListOpts.PageSize != 10 || mock.lastListOpts.PageToken != "tok" {
		t.Fatalf("pagination not forwarded: %+v", mock.lastListOpts)
	}
	// (a) entries + next token converted.
	if len(resp.GetEntries()) != 1 || resp.GetEntries()[0].GetMonitor().GetId() != "mon-uuid-1" {
		t.Fatalf("entry conversion mismatch: %+v", resp.GetEntries())
	}
	if resp.GetPagination().GetNextPageToken() != "next-cursor" {
		t.Fatalf("next token mismatch: %q", resp.GetPagination().GetNextPageToken())
	}
}

func TestListMonitors_PageSizeDefaultedAndClamped(t *testing.T) {
	cases := []struct {
		name     string
		reqSize  int32
		wantSize int
	}{
		{"zero -> default", 0, domain.DefaultListPageSize},
		{"over cap -> clamped", 5000, domain.MaxListPageSize},
		{"within range -> kept", 50, 50},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockMonitorService{
				listMonitorsFn: func(ctx context.Context, ownerTeam string, minSeverity domain.DriftSeverity, state domain.MonitorState, opts domain.ListOptions) ([]domain.FleetEntry, string, error) {
					return nil, "", nil
				},
			}
			client := newTestClient(t, mock, true)
			_, err := client.ListMonitors(authCtx(), &monitorv1.ListMonitorsRequest{
				Pagination: &commonv1.PaginationRequest{PageSize: tc.reqSize},
			})
			if err != nil {
				t.Fatalf("ListMonitors error: %v", err)
			}
			if mock.lastListOpts.PageSize != tc.wantSize {
				t.Fatalf("page size = %d; want %d", mock.lastListOpts.PageSize, tc.wantSize)
			}
		})
	}
}

func TestListMonitors_NilPagination_UsesDefault(t *testing.T) {
	mock := &mockMonitorService{
		listMonitorsFn: func(ctx context.Context, ownerTeam string, minSeverity domain.DriftSeverity, state domain.MonitorState, opts domain.ListOptions) ([]domain.FleetEntry, string, error) {
			return nil, "", nil
		},
	}
	client := newTestClient(t, mock, true)
	_, err := client.ListMonitors(authCtx(), &monitorv1.ListMonitorsRequest{}) // no pagination
	if err != nil {
		t.Fatalf("ListMonitors error: %v", err)
	}
	if mock.lastListOpts.PageSize != domain.DefaultListPageSize {
		t.Fatalf("nil pagination should default page size, got %d", mock.lastListOpts.PageSize)
	}
}

func TestListMonitors_Validation(t *testing.T) {
	cases := []struct {
		name string
		req  *monitorv1.ListMonitorsRequest
	}{
		{"negative page_size", &monitorv1.ListMonitorsRequest{Pagination: &commonv1.PaginationRequest{PageSize: -1}}},
		{"unknown min_severity", &monitorv1.ListMonitorsRequest{MinSeverity: monitorv1.DriftSeverity(99)}},
		{"unknown state", &monitorv1.ListMonitorsRequest{State: monitorv1.MonitorState(99)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockMonitorService{
				listMonitorsFn: func(ctx context.Context, ownerTeam string, minSeverity domain.DriftSeverity, state domain.MonitorState, opts domain.ListOptions) ([]domain.FleetEntry, string, error) {
					t.Fatal("domain must NOT be called for invalid input")
					return nil, "", nil
				},
			}
			client := newTestClient(t, mock, true)
			_, err := client.ListMonitors(authCtx(), tc.req)
			requireCode(t, err, codes.InvalidArgument)
			if mock.listMonitorsCalls != 0 {
				t.Fatalf("domain ListMonitors called %d times; want 0", mock.listMonitorsCalls)
			}
		})
	}
}

// ============================================================================
// GetDriftReport — team-scoped, IDOR-safe
// ============================================================================

func TestGetDriftReport_HappyPath(t *testing.T) {
	mock := &mockMonitorService{
		getReportFn: func(ctx context.Context, ownerTeam, reportID string) (domain.DriftReport, error) {
			return domain.DriftReport{ID: reportID, OwnerTeam: ownerTeam, ModelName: "fraud-detector", Severity: domain.DriftSeverityCritical}, nil
		},
	}
	client := newTestClient(t, mock, true)
	resp, err := client.GetDriftReport(authCtx(), &monitorv1.GetDriftReportRequest{ReportId: "rep-1"})
	if err != nil {
		t.Fatalf("GetDriftReport error: %v", err)
	}
	if mock.lastReportID != "rep-1" || mock.lastOwnerTeam != testTeam {
		t.Fatalf("forwarded args wrong: id=%q team=%q", mock.lastReportID, mock.lastOwnerTeam)
	}
	if resp.GetReport().GetId() != "rep-1" || resp.GetReport().GetSeverity() != monitorv1.DriftSeverity_DRIFT_SEVERITY_CRITICAL {
		t.Fatalf("report conversion mismatch: %+v", resp.GetReport())
	}
}

func TestGetDriftReport_NotFound_NoOracle(t *testing.T) {
	// A report owned by another team comes back as ErrReportNotFound — same as a
	// truly absent id. The client must see NotFound with a generic message (no
	// "forbidden" / no hint the id exists elsewhere).
	mock := &mockMonitorService{
		getReportFn: func(ctx context.Context, ownerTeam, reportID string) (domain.DriftReport, error) {
			return domain.DriftReport{}, domain.ErrReportNotFound
		},
	}
	client := newTestClient(t, mock, true)
	_, err := client.GetDriftReport(authCtx(), &monitorv1.GetDriftReportRequest{ReportId: "someone-elses-id"})
	requireCode(t, err, codes.NotFound)
	st, _ := status.FromError(err)
	if strings.Contains(strings.ToLower(st.Message()), "forbidden") || strings.Contains(strings.ToLower(st.Message()), "permission") {
		t.Fatalf("not-found leaks an authz oracle: %q", st.Message())
	}
	assertNoLeak(t, st.Message())
}

func TestGetDriftReport_Validation_MissingID(t *testing.T) {
	mock := &mockMonitorService{
		getReportFn: func(ctx context.Context, ownerTeam, reportID string) (domain.DriftReport, error) {
			t.Fatal("domain must NOT be called for invalid input")
			return domain.DriftReport{}, nil
		},
	}
	client := newTestClient(t, mock, true)
	_, err := client.GetDriftReport(authCtx(), &monitorv1.GetDriftReportRequest{})
	requireCode(t, err, codes.InvalidArgument)
	if mock.getReportCalls != 0 {
		t.Fatalf("domain GetDriftReport called %d times; want 0", mock.getReportCalls)
	}
}

// ============================================================================
// ListDriftReports — filter + time-range + pagination conversion
// ============================================================================

func TestListDriftReports_HappyPath_ConvertsFilterAndTeamScope(t *testing.T) {
	since := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	mock := &mockMonitorService{
		listReportsFn: func(ctx context.Context, ownerTeam string, f domain.ReportFilter, opts domain.ListOptions) ([]domain.DriftReport, string, error) {
			return []domain.DriftReport{{ID: "rep-1", ModelName: "fraud-detector", Severity: domain.DriftSeverityWarning}}, "cursor-2", nil
		},
	}
	client := newTestClient(t, mock, true)
	resp, err := client.ListDriftReports(authCtx(), &monitorv1.ListDriftReportsRequest{
		ModelName:   "fraud-detector",
		MinSeverity: monitorv1.DriftSeverity_DRIFT_SEVERITY_WARNING,
		Since:       timestamppb.New(since),
		Until:       timestamppb.New(until),
		Pagination:  &commonv1.PaginationRequest{PageSize: 25},
	})
	if err != nil {
		t.Fatalf("ListDriftReports error: %v", err)
	}
	// TENANCY: the filter's OwnerTeam must come from claims, plus the model name.
	f := mock.lastReportFilter
	if f.OwnerTeam != testTeam || f.ModelName != "fraud-detector" || f.MinSeverity != domain.DriftSeverityWarning {
		t.Fatalf("filter conversion mismatch: %+v", f)
	}
	if !f.Since.Equal(since) || !f.Until.Equal(until) {
		t.Fatalf("time range not converted: since=%v until=%v", f.Since, f.Until)
	}
	if mock.lastReportOpts.PageSize != 25 {
		t.Fatalf("page size not forwarded: %d", mock.lastReportOpts.PageSize)
	}
	if len(resp.GetReports()) != 1 || resp.GetPagination().GetNextPageToken() != "cursor-2" {
		t.Fatalf("response conversion mismatch: %+v", resp)
	}
}

func TestListDriftReports_Validation(t *testing.T) {
	now := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		req  *monitorv1.ListDriftReportsRequest
	}{
		{"missing model_name", &monitorv1.ListDriftReportsRequest{}},
		{"unknown min_severity", &monitorv1.ListDriftReportsRequest{ModelName: "m", MinSeverity: monitorv1.DriftSeverity(99)}},
		{"negative page_size", &monitorv1.ListDriftReportsRequest{ModelName: "m", Pagination: &commonv1.PaginationRequest{PageSize: -1}}},
		{"since after until", &monitorv1.ListDriftReportsRequest{
			ModelName: "m",
			Since:     timestamppb.New(now.Add(time.Hour)),
			Until:     timestamppb.New(now),
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockMonitorService{
				listReportsFn: func(ctx context.Context, ownerTeam string, f domain.ReportFilter, opts domain.ListOptions) ([]domain.DriftReport, string, error) {
					t.Fatal("domain must NOT be called for invalid input")
					return nil, "", nil
				},
			}
			client := newTestClient(t, mock, true)
			_, err := client.ListDriftReports(authCtx(), tc.req)
			requireCode(t, err, codes.InvalidArgument)
			if mock.listReportsCalls != 0 {
				t.Fatalf("domain ListDriftReports called %d times; want 0", mock.listReportsCalls)
			}
		})
	}
}

// ============================================================================
// SubmitGroundTruth — batched delayed labels
// ============================================================================

func TestSubmitGroundTruth_HappyPath_ConvertsLabelsAndResult(t *testing.T) {
	observed := time.Date(2026, 6, 9, 0, 0, 0, 0, time.UTC)
	mock := &mockMonitorService{
		submitGroundTruth: func(ctx context.Context, ownerTeam string, in domain.SubmitGroundTruthInput) (domain.SubmitGroundTruthResult, error) {
			return domain.SubmitGroundTruthResult{Accepted: 1, UnmatchedRequestIDs: []string{"req-2"}}, nil
		},
	}
	client := newTestClient(t, mock, true)
	resp, err := client.SubmitGroundTruth(authCtx(), &monitorv1.SubmitGroundTruthRequest{
		ModelName: "fraud-detector",
		Labels: []*monitorv1.GroundTruthLabel{
			{RequestId: "req-1", ActualLabel: "fraud", ObservedAt: timestamppb.New(observed)},
			{RequestId: "req-2", ActualLabel: "not_fraud"},
		},
		IdempotencyKey: "idem-7",
	})
	if err != nil {
		t.Fatalf("SubmitGroundTruth error: %v", err)
	}
	// (a) conversion of labels + identity from claims.
	in := mock.lastSubmitInput
	if mock.lastOwnerTeam != testTeam || in.ModelName != "fraud-detector" || in.IdempotencyKey != "idem-7" {
		t.Fatalf("input header mismatch: team=%q in=%+v", mock.lastOwnerTeam, in)
	}
	if len(in.Labels) != 2 || in.Labels[0].RequestID != "req-1" || in.Labels[0].ActualLabel != "fraud" || !in.Labels[0].ObservedAt.Equal(observed) {
		t.Fatalf("label conversion mismatch: %+v", in.Labels)
	}
	// (a) result conversion (NOT a bare ack).
	if resp.GetAccepted() != 1 || len(resp.GetUnmatchedRequestIds()) != 1 || resp.GetUnmatchedRequestIds()[0] != "req-2" {
		t.Fatalf("result conversion mismatch: %+v", resp)
	}
}

func TestSubmitGroundTruth_Validation(t *testing.T) {
	bigLabel := strings.Repeat("x", domain.MaxLabelBytes+1)
	tooMany := make([]*monitorv1.GroundTruthLabel, domain.MaxGroundTruthBatch+1)
	for i := range tooMany {
		tooMany[i] = &monitorv1.GroundTruthLabel{RequestId: "r", ActualLabel: "y"}
	}
	cases := []struct {
		name string
		req  *monitorv1.SubmitGroundTruthRequest
	}{
		{"missing model_name", &monitorv1.SubmitGroundTruthRequest{Labels: []*monitorv1.GroundTruthLabel{{RequestId: "r", ActualLabel: "y"}}}},
		{"no labels", &monitorv1.SubmitGroundTruthRequest{ModelName: "m"}},
		{"batch over cap", &monitorv1.SubmitGroundTruthRequest{ModelName: "m", Labels: tooMany}},
		{"label missing request_id", &monitorv1.SubmitGroundTruthRequest{ModelName: "m", Labels: []*monitorv1.GroundTruthLabel{{ActualLabel: "y"}}}},
		{"label missing actual_label", &monitorv1.SubmitGroundTruthRequest{ModelName: "m", Labels: []*monitorv1.GroundTruthLabel{{RequestId: "r"}}}},
		{"label too long", &monitorv1.SubmitGroundTruthRequest{ModelName: "m", Labels: []*monitorv1.GroundTruthLabel{{RequestId: "r", ActualLabel: bigLabel}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockMonitorService{
				submitGroundTruth: func(ctx context.Context, ownerTeam string, in domain.SubmitGroundTruthInput) (domain.SubmitGroundTruthResult, error) {
					t.Fatal("domain must NOT be called for invalid input")
					return domain.SubmitGroundTruthResult{}, nil
				},
			}
			client := newTestClient(t, mock, true)
			_, err := client.SubmitGroundTruth(authCtx(), tc.req)
			requireCode(t, err, codes.InvalidArgument)
			if mock.submitCalls != 0 {
				t.Fatalf("domain SubmitGroundTruth called %d times; want 0", mock.submitCalls)
			}
		})
	}
}

func TestSubmitGroundTruth_NotFound(t *testing.T) {
	mock := &mockMonitorService{
		submitGroundTruth: func(ctx context.Context, ownerTeam string, in domain.SubmitGroundTruthInput) (domain.SubmitGroundTruthResult, error) {
			return domain.SubmitGroundTruthResult{}, domain.ErrMonitorNotFound
		},
	}
	client := newTestClient(t, mock, true)
	_, err := client.SubmitGroundTruth(authCtx(), &monitorv1.SubmitGroundTruthRequest{
		ModelName: "m", Labels: []*monitorv1.GroundTruthLabel{{RequestId: "r", ActualLabel: "y"}},
	})
	requireCode(t, err, codes.NotFound)
}

// ============================================================================
// StreamDriftEvents — server-streaming
// ============================================================================

func TestStreamDriftEvents_Unimplemented_WithClaims(t *testing.T) {
	// With claims present and a valid request, the stream method does its full
	// handler-level job and returns Unimplemented (the live source lands in the
	// events phase). The TERMINAL status the client receives must be Unimplemented.
	client := newTestClient(t, &mockMonitorService{}, true)
	stream, err := client.StreamDriftEvents(authCtx(), &monitorv1.StreamDriftEventsRequest{ModelName: "fraud-detector"})
	if err != nil {
		t.Fatalf("opening stream errored synchronously: %v", err)
	}
	_, err = stream.Recv()
	if errors.Is(err, io.EOF) {
		t.Fatal("expected Unimplemented terminal status, got clean EOF")
	}
	requireCode(t, err, codes.Unimplemented)
}

func TestStreamDriftEvents_NoClaims_Unauthenticated(t *testing.T) {
	// No auth interceptor installed -> no claims -> the stream must fail closed
	// with Unauthenticated as its terminal status.
	client := newTestClient(t, &mockMonitorService{}, false)
	stream, err := client.StreamDriftEvents(context.Background(), &monitorv1.StreamDriftEventsRequest{})
	if err != nil {
		t.Fatalf("opening stream errored synchronously: %v", err)
	}
	_, err = stream.Recv()
	requireCode(t, err, codes.Unauthenticated)
}

func TestStreamDriftEvents_UnknownSeverity_InvalidArgument(t *testing.T) {
	// Validation runs before the Unimplemented return: an unknown min_severity is
	// a client error surfaced as the stream's terminal InvalidArgument status.
	client := newTestClient(t, &mockMonitorService{}, true)
	stream, err := client.StreamDriftEvents(authCtx(), &monitorv1.StreamDriftEventsRequest{
		MinSeverity: monitorv1.DriftSeverity(99),
	})
	if err != nil {
		t.Fatalf("opening stream errored synchronously: %v", err)
	}
	_, err = stream.Recv()
	requireCode(t, err, codes.InvalidArgument)
}

// ============================================================================
// NIL-SVC GUARD — a binary wired before the domain service lands
// ============================================================================

func TestNilService_UnaryReturnsUnimplemented(t *testing.T) {
	// Handler with svc==nil: every unary RPC must return Unimplemented instead of
	// panicking on a nil-interface method call. We spot-check a representative RPC.
	client := newTestClient(t, nil, true)
	_, err := client.ConfigureMonitor(authCtx(), &monitorv1.ConfigureMonitorRequest{ModelName: "m"})
	requireCode(t, err, codes.Unimplemented)
}

func TestNilService_StreamReturnsUnimplemented(t *testing.T) {
	// The streaming RPC's nil-svc guard returns the status ERROR directly (no nil
	// response), which the client sees as the stream's terminal status.
	client := newTestClient(t, nil, true)
	stream, err := client.StreamDriftEvents(authCtx(), &monitorv1.StreamDriftEventsRequest{})
	if err != nil {
		t.Fatalf("opening stream errored synchronously: %v", err)
	}
	_, err = stream.Recv()
	requireCode(t, err, codes.Unimplemented)
}
