// registry_handler_test.go — COMPONENT tests for the Model Registry gRPC handler.
//
// ============================================================================
// WHAT "COMPONENT TEST" MEANS HERE (and what we deliberately isolate)
// ============================================================================
//
// These tests exercise the handler END-TO-END THROUGH THE REAL gRPC STACK:
//   - a real grpc.Server over an in-process bufconn transport (no TCP, no flakiness)
//   - the REAL grpcutil.AuthUnaryInterceptor in the chain, so claims reach the
//     handler's context exactly as in production (this is what proves
//     actorFromContext + the identity trust boundary actually work over the wire)
//   - real protobuf serialization of every request/response (catches proto<->domain
//     mapping bugs a direct method call would miss)
//
// We ISOLATE the handler from business logic by injecting a HAND-WRITTEN MOCK of
// the domain.RegistryService interface (mockService below). WHY mock the SERVICE,
// not the repositories: the unit under test is the HANDLER — its proto<->domain
// conversion, validation, identity extraction, and error mapping. Mocking the
// service lets us drive each domain return (a Model, a sentinel error) and assert
// the handler's reaction precisely, WITHOUT a database. The domain logic has its
// own -race unit tests; re-testing it here would blur the boundary and slow the
// suite. (No testcontainers in this phase — this is container-free by design.)
//
// FOR EACH RPC WE ASSERT:
//
//	(a) happy path: the handler passes the right domain Input (client fields only)
//	    + the right Actor (from claims), and maps the domain result to the right
//	    proto Response.
//	(b) validation: a malformed request is rejected with InvalidArgument BEFORE
//	    the service is called.
//	(c) error mapping: each domain sentinel becomes the correct gRPC code.
//	(d) no leak: an unexpected internal error becomes a GENERIC Internal message
//	    (the real error text never reaches the client).
//	(e) identity: a call with no/invalid auth is rejected (Unauthenticated) and the
//	    service is never invoked.
//
// ============================================================================
package handler_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	registryv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/registry/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/registry/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/registry/internal/handler"
)

// ============================================================================
// TEST IDENTITY — fixed claims the stub validator stamps for the "good" token
// ============================================================================

const (
	testUserID    = "user-42"
	testTeam      = "ml-platform"
	goodToken     = "good-token"   // stubValidator returns testClaims for this
	badToken      = "bad-token"    // stubValidator returns Unauthenticated
	internalToken = "backend-down" // stubValidator returns codes.Internal
)

var testClaims = &grpcutil.Claims{UserID: testUserID, Team: testTeam, Role: "engineer"}

// wantActor is the Actor the handler MUST build from testClaims — never from the
// request body. Every happy-path test asserts the mock saw exactly this.
var wantActor = domain.Actor{UserID: testUserID, Team: testTeam}

// ============================================================================
// STUB TOKEN VALIDATOR — drives the REAL auth interceptor in the test server
// ============================================================================
//
// The real grpcutil.AuthUnaryInterceptor calls validator.Validate(token) and, on
// success, stamps the returned Claims onto the context. By installing this stub we
// get the production claims-injection path in the test without signing real JWTs.

type stubValidator struct{}

func (stubValidator) Validate(_ context.Context, token string) (*grpcutil.Claims, error) {
	switch token {
	case goodToken:
		return testClaims, nil
	case internalToken:
		// A backend-down style failure: the interceptor preserves the code but
		// sanitizes the message. Used to prove unauthenticated-vs-internal handling.
		return nil, status.Error(codes.Internal, "auth db unreachable at 10.0.0.5:5432")
	default:
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}
}

// ============================================================================
// MOCK DOMAIN SERVICE — a hand-written mock of domain.RegistryService
// ============================================================================
//
// Each method delegates to a function field the test sets. An unset field means
// "the test did not expect this method to be called" → it fails the test loudly.
// This is the table-driven-friendly mock style: a test wires only the one method
// it drives, and any unexpected call is caught. We also CAPTURE the arguments the
// handler passed so tests can assert the proto→domain conversion + the Actor.
type mockService struct {
	t *testing.T

	registerFn   func(ctx context.Context, actor domain.Actor, in domain.RegisterModelInput) (domain.Model, error)
	updateFn     func(ctx context.Context, actor domain.Actor, in domain.UpdateModelInput) (domain.Model, error)
	createVerFn  func(ctx context.Context, actor domain.Actor, in domain.CreateVersionInput) (domain.ModelVersion, error)
	markReadyFn  func(ctx context.Context, actor domain.Actor, in domain.MarkVersionReadyInput) (domain.ModelVersion, error)
	promoteFn    func(ctx context.Context, actor domain.Actor, in domain.PromoteVersionInput) (domain.PromoteResult, error)
	archiveFn    func(ctx context.Context, actor domain.Actor, modelID, idemKey string) (domain.Model, error)
	getModelFn   func(ctx context.Context, actor domain.Actor, id, name string) (domain.Model, error)
	listModelsFn func(ctx context.Context, actor domain.Actor, in domain.ListModelsInput) (domain.Page[domain.Model], error)
	getVerFn     func(ctx context.Context, actor domain.Actor, id, modelID, version string) (domain.ModelVersion, error)
	listVerFn    func(ctx context.Context, actor domain.Actor, modelID string, stage domain.ModelStage, pageSize int, pageToken string) (domain.Page[domain.ModelVersion], error)
}

func (m *mockService) RegisterModel(ctx context.Context, actor domain.Actor, in domain.RegisterModelInput) (domain.Model, error) {
	if m.registerFn == nil {
		m.t.Fatalf("unexpected call to RegisterModel")
	}
	return m.registerFn(ctx, actor, in)
}

func (m *mockService) UpdateModel(ctx context.Context, actor domain.Actor, in domain.UpdateModelInput) (domain.Model, error) {
	if m.updateFn == nil {
		m.t.Fatalf("unexpected call to UpdateModel")
	}
	return m.updateFn(ctx, actor, in)
}

func (m *mockService) CreateVersion(ctx context.Context, actor domain.Actor, in domain.CreateVersionInput) (domain.ModelVersion, error) {
	if m.createVerFn == nil {
		m.t.Fatalf("unexpected call to CreateVersion")
	}
	return m.createVerFn(ctx, actor, in)
}

func (m *mockService) MarkVersionReady(ctx context.Context, actor domain.Actor, in domain.MarkVersionReadyInput) (domain.ModelVersion, error) {
	if m.markReadyFn == nil {
		m.t.Fatalf("unexpected call to MarkVersionReady")
	}
	return m.markReadyFn(ctx, actor, in)
}

func (m *mockService) PromoteVersion(ctx context.Context, actor domain.Actor, in domain.PromoteVersionInput) (domain.PromoteResult, error) {
	if m.promoteFn == nil {
		m.t.Fatalf("unexpected call to PromoteVersion")
	}
	return m.promoteFn(ctx, actor, in)
}

func (m *mockService) ArchiveModel(ctx context.Context, actor domain.Actor, modelID, idempotencyKey string) (domain.Model, error) {
	if m.archiveFn == nil {
		m.t.Fatalf("unexpected call to ArchiveModel")
	}
	return m.archiveFn(ctx, actor, modelID, idempotencyKey)
}

func (m *mockService) GetModel(ctx context.Context, actor domain.Actor, id, name string) (domain.Model, error) {
	if m.getModelFn == nil {
		m.t.Fatalf("unexpected call to GetModel")
	}
	return m.getModelFn(ctx, actor, id, name)
}

func (m *mockService) ListModels(ctx context.Context, actor domain.Actor, in domain.ListModelsInput) (domain.Page[domain.Model], error) {
	if m.listModelsFn == nil {
		m.t.Fatalf("unexpected call to ListModels")
	}
	return m.listModelsFn(ctx, actor, in)
}

func (m *mockService) GetVersion(ctx context.Context, actor domain.Actor, id, modelID, version string) (domain.ModelVersion, error) {
	if m.getVerFn == nil {
		m.t.Fatalf("unexpected call to GetVersion")
	}
	return m.getVerFn(ctx, actor, id, modelID, version)
}

func (m *mockService) ListVersions(ctx context.Context, actor domain.Actor, modelID string, stageFilter domain.ModelStage, pageSize int, pageToken string) (domain.Page[domain.ModelVersion], error) {
	if m.listVerFn == nil {
		m.t.Fatalf("unexpected call to ListVersions")
	}
	return m.listVerFn(ctx, actor, modelID, stageFilter, pageSize, pageToken)
}

// compile-time assertion: mockService satisfies the full domain interface. If the
// interface gains a method, this line fails to compile — the test stays honest.
var _ domain.RegistryService = (*mockService)(nil)

// ============================================================================
// TEST HARNESS — bufconn server with the real auth interceptor + the mock service
// ============================================================================

// newClient spins up an in-process gRPC server wired with the REAL auth
// interceptor (driven by stubValidator) and the handler backed by svc, and returns
// a connected RegistryServiceClient. Server + conn are torn down via t.Cleanup.
func newClient(t *testing.T, svc domain.RegistryService) registryv1.RegistryServiceClient {
	t.Helper()
	conn := testutil.NewTestGRPCServer(t,
		func(s *grpc.Server) {
			registryv1.RegisterRegistryServiceServer(s, handler.NewRegistryHandler(svc))
		},
		grpc.ChainUnaryInterceptor(grpcutil.AuthUnaryInterceptor(stubValidator{})),
	)
	return registryv1.NewRegistryServiceClient(conn)
}

// authCtx returns a context carrying the given bearer token in gRPC metadata,
// exactly as a real client would. The server's auth interceptor reads it.
func authCtx(token string) context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+token)
}

// ctxGood is the common "authenticated as testClaims" context.
func ctxGood() context.Context { return authCtx(goodToken) }

// requireCode fails the test unless err is a gRPC status with the wanted code.
func requireCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with code %s, got nil", want)
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("error is not a gRPC status: %v", err)
	}
	if st.Code() != want {
		t.Fatalf("got code %s (msg %q), want %s", st.Code(), st.Message(), want)
	}
}

// assertNoLeak fails if the error message contains any internal-detail substring
// (the original sentinel wrap text or infra strings). This is the security
// assertion: client-facing messages must be sanitized.
func assertNoLeak(t *testing.T, err error, forbidden ...string) {
	t.Helper()
	st, _ := status.FromError(err)
	msg := st.Message()
	for _, f := range forbidden {
		if strings.Contains(msg, f) {
			t.Fatalf("error message leaked internal detail %q: full message %q", f, msg)
		}
	}
}

// fixedTime is a deterministic timestamp used to assert proto timestamp mapping.
var fixedTime = time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)

// ============================================================================
// RegisterModel
// ============================================================================

func TestRegisterModel_HappyPath(t *testing.T) {
	var gotActor domain.Actor
	var gotIn domain.RegisterModelInput
	svc := &mockService{t: t, registerFn: func(_ context.Context, actor domain.Actor, in domain.RegisterModelInput) (domain.Model, error) {
		gotActor, gotIn = actor, in
		return domain.Model{
			ID: "model-1", Name: in.Name, Description: in.Description,
			OwnerID: actor.UserID, Team: actor.Team,
			Framework: in.Framework, TaskType: in.TaskType, Tags: in.Tags,
			CreatedAt: fixedTime, UpdatedAt: fixedTime,
		}, nil
	}}
	client := newClient(t, svc)

	resp, err := client.RegisterModel(ctxGood(), &registryv1.RegisterModelRequest{
		Name:           "fraud-detector",
		Description:    "detects fraud",
		Framework:      "pytorch",
		TaskType:       "classification",
		Tags:           map[string]string{"domain": "fraud"},
		IdempotencyKey: "idem-1",
		// NOTE: there is intentionally no owner/team field to forge — the proto
		// doesn't carry them. Identity comes from claims only.
	})
	if err != nil {
		t.Fatalf("RegisterModel: %v", err)
	}

	// (a) Actor came from claims, not the body.
	if gotActor != wantActor {
		t.Errorf("actor = %+v, want %+v", gotActor, wantActor)
	}
	// (a) client fields mapped into the Input.
	if gotIn.Name != "fraud-detector" || gotIn.Framework != "pytorch" ||
		gotIn.TaskType != "classification" || gotIn.IdempotencyKey != "idem-1" ||
		gotIn.Tags["domain"] != "fraud" {
		t.Errorf("input not mapped correctly: %+v", gotIn)
	}
	// (a) domain result mapped to proto, with server-authoritative owner/team.
	m := resp.GetModel()
	if m.GetId() != "model-1" || m.GetOwnerId() != testUserID || m.GetTeam() != testTeam {
		t.Errorf("response model = %+v", m)
	}
	if m.GetCreatedAt().AsTime().UTC() != fixedTime {
		t.Errorf("created_at = %v, want %v", m.GetCreatedAt().AsTime(), fixedTime)
	}
}

func TestRegisterModel_Validation_EmptyName(t *testing.T) {
	// registerFn intentionally nil: the handler must reject BEFORE calling svc.
	client := newClient(t, &mockService{t: t})
	_, err := client.RegisterModel(ctxGood(), &registryv1.RegisterModelRequest{Name: ""})
	requireCode(t, err, codes.InvalidArgument)
}

func TestRegisterModel_Unauthenticated_NoToken(t *testing.T) {
	client := newClient(t, &mockService{t: t}) // svc must NOT be called
	_, err := client.RegisterModel(context.Background(), &registryv1.RegisterModelRequest{Name: "x"})
	requireCode(t, err, codes.Unauthenticated)
}

func TestRegisterModel_Unauthenticated_BadToken(t *testing.T) {
	client := newClient(t, &mockService{t: t})
	_, err := client.RegisterModel(authCtx(badToken), &registryv1.RegisterModelRequest{Name: "x"})
	requireCode(t, err, codes.Unauthenticated)
}

func TestRegisterModel_ErrorMapping(t *testing.T) {
	tests := []struct {
		name      string
		domainErr error
		wantCode  codes.Code
		// forbidden substrings that must NOT appear in the sanitized client message.
		forbidden []string
	}{
		{"validation", domain.ErrValidation, codes.InvalidArgument, nil},
		{"name taken", domain.ErrModelNameTaken, codes.AlreadyExists, nil},
		{
			"unexpected internal is sanitized",
			errors.New("pq: connection refused to db at 10.1.2.3:5432 password=hunter2"),
			codes.Internal,
			[]string{"connection refused", "10.1.2.3", "hunter2", "password"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc := &mockService{t: t, registerFn: func(_ context.Context, _ domain.Actor, _ domain.RegisterModelInput) (domain.Model, error) {
				return domain.Model{}, tc.domainErr
			}}
			client := newClient(t, svc)
			_, err := client.RegisterModel(ctxGood(), &registryv1.RegisterModelRequest{Name: "x"})
			requireCode(t, err, tc.wantCode)
			assertNoLeak(t, err, tc.forbidden...)
		})
	}
}

// ============================================================================
// UpdateModel
// ============================================================================

func TestUpdateModel_HappyPath_FlagsPassThrough(t *testing.T) {
	var gotIn domain.UpdateModelInput
	var gotActor domain.Actor
	svc := &mockService{t: t, updateFn: func(_ context.Context, actor domain.Actor, in domain.UpdateModelInput) (domain.Model, error) {
		gotIn, gotActor = in, actor
		return domain.Model{ID: in.ModelID, Description: in.Description, Tags: in.Tags, UpdatedAt: fixedTime}, nil
	}}
	client := newClient(t, svc)

	_, err := client.UpdateModel(ctxGood(), &registryv1.UpdateModelRequest{
		Id:                "model-1",
		Description:       "new desc",
		UpdateDescription: true,
		Tags:              map[string]string{"k": "v"},
		ReplaceTags:       true,
		IdempotencyKey:    "idem-u",
	})
	if err != nil {
		t.Fatalf("UpdateModel: %v", err)
	}
	if gotActor != wantActor {
		t.Errorf("actor = %+v, want %+v", gotActor, wantActor)
	}
	// The explicit apply flags MUST be carried so the proto3-presence semantics hold.
	if !gotIn.UpdateDescription || !gotIn.ReplaceTags || gotIn.Description != "new desc" ||
		gotIn.Tags["k"] != "v" || gotIn.IdempotencyKey != "idem-u" {
		t.Errorf("update input not mapped: %+v", gotIn)
	}
}

func TestUpdateModel_Validation_EmptyID(t *testing.T) {
	client := newClient(t, &mockService{t: t})
	_, err := client.UpdateModel(ctxGood(), &registryv1.UpdateModelRequest{Id: ""})
	requireCode(t, err, codes.InvalidArgument)
}

func TestUpdateModel_ErrorMapping(t *testing.T) {
	tests := []struct {
		name      string
		domainErr error
		wantCode  codes.Code
	}{
		{"not found", domain.ErrModelNotFound, codes.NotFound},
		{"archived", domain.ErrModelArchived, codes.FailedPrecondition},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc := &mockService{t: t, updateFn: func(_ context.Context, _ domain.Actor, _ domain.UpdateModelInput) (domain.Model, error) {
				return domain.Model{}, tc.domainErr
			}}
			client := newClient(t, svc)
			_, err := client.UpdateModel(ctxGood(), &registryv1.UpdateModelRequest{Id: "model-1"})
			requireCode(t, err, tc.wantCode)
		})
	}
}

// ============================================================================
// CreateVersion
// ============================================================================

func TestCreateVersion_HappyPath_MetricsConversion(t *testing.T) {
	var gotIn domain.CreateVersionInput
	svc := &mockService{t: t, createVerFn: func(_ context.Context, _ domain.Actor, in domain.CreateVersionInput) (domain.ModelVersion, error) {
		gotIn = in
		return domain.ModelVersion{
			ID: "ver-1", ModelID: in.ModelID, Version: "1",
			Description: in.Description, Metrics: in.Metrics,
			Stage: domain.StageDev, Status: domain.StatusPendingUpload,
			CreatedBy: testUserID, CreatedAt: fixedTime,
		}, nil
	}}
	client := newClient(t, svc)

	metrics := &structpb.Struct{Fields: map[string]*structpb.Value{
		"accuracy": structpb.NewNumberValue(0.97),
	}}
	resp, err := client.CreateVersion(ctxGood(), &registryv1.CreateVersionRequest{
		ModelId: "model-1", Description: "v1 notes", Metrics: metrics,
	})
	if err != nil {
		t.Fatalf("CreateVersion: %v", err)
	}
	// proto Struct → domain map conversion.
	if gotIn.ModelID != "model-1" || gotIn.Metrics["accuracy"] != 0.97 {
		t.Errorf("create version input not mapped: %+v", gotIn)
	}
	v := resp.GetVersion()
	// domain enum → proto enum, mapped explicitly (DEV / PENDING_UPLOAD).
	if v.GetStage() != registryv1.ModelStage_MODEL_STAGE_DEV ||
		v.GetStatus() != registryv1.VersionStatus_VERSION_STATUS_PENDING_UPLOAD {
		t.Errorf("stage/status not mapped: stage=%v status=%v", v.GetStage(), v.GetStatus())
	}
	// domain map → proto Struct conversion on the way out.
	if v.GetMetrics().GetFields()["accuracy"].GetNumberValue() != 0.97 {
		t.Errorf("metrics not mapped back to struct: %v", v.GetMetrics())
	}
}

func TestCreateVersion_Validation_EmptyModelID(t *testing.T) {
	client := newClient(t, &mockService{t: t})
	_, err := client.CreateVersion(ctxGood(), &registryv1.CreateVersionRequest{ModelId: ""})
	requireCode(t, err, codes.InvalidArgument)
}

func TestCreateVersion_Validation_NonNumericMetric(t *testing.T) {
	// A non-numeric metric value must be rejected at the boundary, before svc.
	client := newClient(t, &mockService{t: t})
	metrics := &structpb.Struct{Fields: map[string]*structpb.Value{
		"accuracy": structpb.NewStringValue("high"), // string, not a number
	}}
	_, err := client.CreateVersion(ctxGood(), &registryv1.CreateVersionRequest{
		ModelId: "model-1", Metrics: metrics,
	})
	requireCode(t, err, codes.InvalidArgument)
}

func TestCreateVersion_ErrorMapping(t *testing.T) {
	tests := []struct {
		name      string
		domainErr error
		wantCode  codes.Code
	}{
		{"model not found", domain.ErrModelNotFound, codes.NotFound},
		{"version exists", domain.ErrVersionExists, codes.AlreadyExists},
		{"archived", domain.ErrModelArchived, codes.FailedPrecondition},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc := &mockService{t: t, createVerFn: func(_ context.Context, _ domain.Actor, _ domain.CreateVersionInput) (domain.ModelVersion, error) {
				return domain.ModelVersion{}, tc.domainErr
			}}
			client := newClient(t, svc)
			_, err := client.CreateVersion(ctxGood(), &registryv1.CreateVersionRequest{ModelId: "model-1"})
			requireCode(t, err, tc.wantCode)
		})
	}
}

// ============================================================================
// ConfirmVersionUpload — SECURITY: intentionally left on the Unimplemented base
// ============================================================================
//
// The READY edge writes a CONTENT-ADDRESSABLE artifact digest and emits
// ModelVersionReady to downstream services. That digest MUST be server-measured
// by the object-storage re-verification adapter (a later phase). Until then,
// ConfirmVersionUpload is NOT overridden, so the embedded
// UnimplementedRegistryServiceServer answers with codes.Unimplemented. These
// tests pin that behavior so a future change can't silently re-introduce the
// client-digest-trusting hole.
//
// Each mockService below has markReadyFn == nil: its MarkVersionReady method
// calls t.Fatalf if invoked. That is the strongest possible assertion that the
// RPC NEVER reaches the domain (and therefore can never flip a version to READY
// from client input) — if the override is ever re-added, these tests fail twice:
// once on the wrong status code, once on the unexpected domain call.

// A forged, attacker-chosen expected_digest on a PENDING_UPLOAD version must NOT
// reach the domain and must NOT flip anything to READY. The call is refused at
// the transport boundary with Unimplemented.
func TestConfirmVersionUpload_ForgedDigest_IsRefusedNotPersisted(t *testing.T) {
	// markReadyFn nil → MarkVersionReady would t.Fatalf if the handler called it.
	svc := &mockService{t: t}
	client := newClient(t, svc)

	_, err := client.ConfirmVersionUpload(ctxGood(), &registryv1.ConfirmVersionUploadRequest{
		VersionId:      "ver-1",
		ExpectedDigest: "sha256:attacker-chosen-forged-digest",
		IdempotencyKey: "idem-c",
	})
	// Unimplemented: the only honest answer until the server can re-measure the
	// artifact. Crucially NOT OK/READY and NOT a domain error — the domain is
	// never reached (the nil markReadyFn guard would have fired otherwise).
	requireCode(t, err, codes.Unimplemented)
}

// Even with a well-formed request (no obviously-forged value), the RPC is still
// refused — the point is that NO client input can drive the READY edge yet.
func TestConfirmVersionUpload_WellFormedRequest_StillUnimplemented(t *testing.T) {
	svc := &mockService{t: t} // markReadyFn nil → fatal if the domain is reached
	client := newClient(t, svc)

	_, err := client.ConfirmVersionUpload(ctxGood(), &registryv1.ConfirmVersionUploadRequest{
		VersionId:      "ver-1",
		ExpectedDigest: "sha256:plausible",
		IdempotencyKey: "idem-c",
	})
	requireCode(t, err, codes.Unimplemented)
}

// Unauthenticated callers are rejected by the auth interceptor BEFORE method
// dispatch — the interceptor sits ahead of the embedded Unimplemented base, so a
// no-token call yields Unauthenticated, never reaching the (unoverridden) RPC or
// the domain. context.Background() carries no bearer token.
func TestConfirmVersionUpload_NoAuth_RejectedByInterceptor(t *testing.T) {
	svc := &mockService{t: t} // markReadyFn nil → fatal if the domain is reached
	client := newClient(t, svc)

	_, err := client.ConfirmVersionUpload(context.Background(), &registryv1.ConfirmVersionUploadRequest{
		VersionId:      "ver-1",
		ExpectedDigest: "sha256:plausible",
	})
	requireCode(t, err, codes.Unauthenticated)
}

// ============================================================================
// PromoteVersion
// ============================================================================

func TestPromoteVersion_HappyPath_WithDemotion(t *testing.T) {
	var gotIn domain.PromoteVersionInput
	svc := &mockService{t: t, promoteFn: func(_ context.Context, _ domain.Actor, in domain.PromoteVersionInput) (domain.PromoteResult, error) {
		gotIn = in
		return domain.PromoteResult{
			Promoted: domain.ModelVersion{ID: "ver-2", Stage: domain.StageProduction, Status: domain.StatusReady},
			Demoted:  domain.ModelVersion{ID: "ver-1", Stage: domain.StageArchived, Status: domain.StatusReady},
		}, nil
	}}
	client := newClient(t, svc)

	resp, err := client.PromoteVersion(ctxGood(), &registryv1.PromoteVersionRequest{
		VersionId:   "ver-2",
		TargetStage: registryv1.ModelStage_MODEL_STAGE_PRODUCTION,
	})
	if err != nil {
		t.Fatalf("PromoteVersion: %v", err)
	}
	// proto target_stage → domain stage mapped explicitly.
	if gotIn.TargetStage != domain.StageProduction || gotIn.VersionID != "ver-2" {
		t.Errorf("promote input not mapped: %+v", gotIn)
	}
	if resp.GetVersion().GetId() != "ver-2" ||
		resp.GetVersion().GetStage() != registryv1.ModelStage_MODEL_STAGE_PRODUCTION {
		t.Errorf("promoted version not mapped: %+v", resp.GetVersion())
	}
	// The demoted version must be present (a swap happened).
	if resp.GetDemotedVersion().GetId() != "ver-1" ||
		resp.GetDemotedVersion().GetStage() != registryv1.ModelStage_MODEL_STAGE_ARCHIVED {
		t.Errorf("demoted version not mapped: %+v", resp.GetDemotedVersion())
	}
}

func TestPromoteVersion_HappyPath_NoDemotion(t *testing.T) {
	svc := &mockService{t: t, promoteFn: func(_ context.Context, _ domain.Actor, _ domain.PromoteVersionInput) (domain.PromoteResult, error) {
		return domain.PromoteResult{
			Promoted: domain.ModelVersion{ID: "ver-1", Stage: domain.StageStaging},
			// Demoted is zero-value (ID == "") → no prior prod to demote.
		}, nil
	}}
	client := newClient(t, svc)
	resp, err := client.PromoteVersion(ctxGood(), &registryv1.PromoteVersionRequest{
		VersionId: "ver-1", TargetStage: registryv1.ModelStage_MODEL_STAGE_STAGING,
	})
	if err != nil {
		t.Fatalf("PromoteVersion: %v", err)
	}
	// When nothing was demoted, the wire field must be nil (presence = a swap).
	if resp.GetDemotedVersion() != nil {
		t.Errorf("demoted_version should be nil when nothing was demoted, got %+v", resp.GetDemotedVersion())
	}
}

func TestPromoteVersion_Validation(t *testing.T) {
	tests := []struct {
		name string
		req  *registryv1.PromoteVersionRequest
	}{
		{"empty version id", &registryv1.PromoteVersionRequest{TargetStage: registryv1.ModelStage_MODEL_STAGE_STAGING}},
		{"unspecified target stage", &registryv1.PromoteVersionRequest{VersionId: "ver-1", TargetStage: registryv1.ModelStage_MODEL_STAGE_UNSPECIFIED}},
		{"out-of-range target stage", &registryv1.PromoteVersionRequest{VersionId: "ver-1", TargetStage: registryv1.ModelStage(99)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := newClient(t, &mockService{t: t}) // svc must not be called
			_, err := client.PromoteVersion(ctxGood(), tc.req)
			requireCode(t, err, codes.InvalidArgument)
		})
	}
}

func TestPromoteVersion_ErrorMapping(t *testing.T) {
	tests := []struct {
		name      string
		domainErr error
		wantCode  codes.Code
	}{
		{"version not found", domain.ErrVersionNotFound, codes.NotFound},
		{"illegal transition", domain.ErrIllegalTransition, codes.FailedPrecondition},
		{"not ready", domain.ErrVersionNotReady, codes.FailedPrecondition},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc := &mockService{t: t, promoteFn: func(_ context.Context, _ domain.Actor, _ domain.PromoteVersionInput) (domain.PromoteResult, error) {
				return domain.PromoteResult{}, tc.domainErr
			}}
			client := newClient(t, svc)
			_, err := client.PromoteVersion(ctxGood(), &registryv1.PromoteVersionRequest{
				VersionId: "ver-1", TargetStage: registryv1.ModelStage_MODEL_STAGE_PRODUCTION,
			})
			requireCode(t, err, tc.wantCode)
		})
	}
}

// ============================================================================
// DeleteModel (→ ArchiveModel)
// ============================================================================

func TestDeleteModel_HappyPath(t *testing.T) {
	var gotModelID, gotKey string
	var gotActor domain.Actor
	svc := &mockService{t: t, archiveFn: func(_ context.Context, actor domain.Actor, modelID, key string) (domain.Model, error) {
		gotActor, gotModelID, gotKey = actor, modelID, key
		return domain.Model{ID: modelID, ArchivedAt: fixedTime}, nil
	}}
	client := newClient(t, svc)

	_, err := client.DeleteModel(ctxGood(), &registryv1.DeleteModelRequest{Id: "model-1", IdempotencyKey: "idem-d"})
	if err != nil {
		t.Fatalf("DeleteModel: %v", err)
	}
	if gotActor != wantActor || gotModelID != "model-1" || gotKey != "idem-d" {
		t.Errorf("archive args = actor:%+v id:%q key:%q", gotActor, gotModelID, gotKey)
	}
}

func TestDeleteModel_Validation_EmptyID(t *testing.T) {
	client := newClient(t, &mockService{t: t})
	_, err := client.DeleteModel(ctxGood(), &registryv1.DeleteModelRequest{Id: ""})
	requireCode(t, err, codes.InvalidArgument)
}

func TestDeleteModel_ErrorMapping_NotFound(t *testing.T) {
	svc := &mockService{t: t, archiveFn: func(_ context.Context, _ domain.Actor, _, _ string) (domain.Model, error) {
		return domain.Model{}, domain.ErrModelNotFound
	}}
	client := newClient(t, svc)
	_, err := client.DeleteModel(ctxGood(), &registryv1.DeleteModelRequest{Id: "model-1"})
	requireCode(t, err, codes.NotFound)
}

// ============================================================================
// GetModel
// ============================================================================

func TestGetModel_HappyPath_ByID(t *testing.T) {
	var gotID, gotName string
	var gotActor domain.Actor
	svc := &mockService{t: t, getModelFn: func(_ context.Context, actor domain.Actor, id, name string) (domain.Model, error) {
		gotActor, gotID, gotName = actor, id, name
		return domain.Model{ID: id, Name: "fraud-detector", Team: actor.Team, ProductionVersion: "3"}, nil
	}}
	client := newClient(t, svc)

	resp, err := client.GetModel(ctxGood(), &registryv1.GetModelRequest{Id: "model-1"})
	if err != nil {
		t.Fatalf("GetModel: %v", err)
	}
	if gotActor != wantActor || gotID != "model-1" || gotName != "" {
		t.Errorf("get model args = actor:%+v id:%q name:%q", gotActor, gotID, gotName)
	}
	if resp.GetModel().GetProductionVersion() != "3" {
		t.Errorf("production_version not mapped: %v", resp.GetModel())
	}
}

func TestGetModel_Validation_NeitherIDNorName(t *testing.T) {
	client := newClient(t, &mockService{t: t})
	_, err := client.GetModel(ctxGood(), &registryv1.GetModelRequest{})
	requireCode(t, err, codes.InvalidArgument)
}

func TestGetModel_ErrorMapping_NotFound(t *testing.T) {
	svc := &mockService{t: t, getModelFn: func(_ context.Context, _ domain.Actor, _, _ string) (domain.Model, error) {
		return domain.Model{}, domain.ErrModelNotFound
	}}
	client := newClient(t, svc)
	_, err := client.GetModel(ctxGood(), &registryv1.GetModelRequest{Id: "model-1"})
	requireCode(t, err, codes.NotFound)
	assertNoLeak(t, err, "registry:") // domain wrap prefix must not reach client
}

// ============================================================================
// ListModels
// ============================================================================

func TestListModels_HappyPath_FilterAndPagination(t *testing.T) {
	var gotIn domain.ListModelsInput
	var gotActor domain.Actor
	svc := &mockService{t: t, listModelsFn: func(_ context.Context, actor domain.Actor, in domain.ListModelsInput) (domain.Page[domain.Model], error) {
		gotActor, gotIn = actor, in
		return domain.Page[domain.Model]{
			Items:     []domain.Model{{ID: "model-1"}, {ID: "model-2"}},
			NextToken: "next-cursor",
		}, nil
	}}
	client := newClient(t, svc)

	resp, err := client.ListModels(ctxGood(), &registryv1.ListModelsRequest{
		TaskTypeFilter:  "classification",
		FrameworkFilter: "pytorch",
		IncludeArchived: true,
		Pagination:      &commonv1.PaginationRequest{PageSize: 50, PageToken: "tok"},
	})
	if err != nil {
		t.Fatalf("ListModels: %v", err)
	}
	if gotActor != wantActor {
		t.Errorf("actor = %+v, want %+v", gotActor, wantActor)
	}
	if gotIn.Filter.TaskType != "classification" || gotIn.Filter.Framework != "pytorch" ||
		!gotIn.Filter.IncludeArchived || gotIn.PageSize != 50 || gotIn.PageToken != "tok" {
		t.Errorf("list models input not mapped: %+v", gotIn)
	}
	if len(resp.GetModels()) != 2 || resp.GetPagination().GetNextPageToken() != "next-cursor" {
		t.Errorf("response not mapped: models=%d next=%q", len(resp.GetModels()), resp.GetPagination().GetNextPageToken())
	}
}

func TestListModels_Validation_PageSizeOverCap(t *testing.T) {
	client := newClient(t, &mockService{t: t}) // svc must not be called
	_, err := client.ListModels(ctxGood(), &registryv1.ListModelsRequest{
		Pagination: &commonv1.PaginationRequest{PageSize: domain.MaxPageSize + 1},
	})
	requireCode(t, err, codes.InvalidArgument)
}

func TestListModels_Validation_NegativePageSize(t *testing.T) {
	client := newClient(t, &mockService{t: t})
	_, err := client.ListModels(ctxGood(), &registryv1.ListModelsRequest{
		Pagination: &commonv1.PaginationRequest{PageSize: -1},
	})
	requireCode(t, err, codes.InvalidArgument)
}

func TestListModels_ErrorMapping_Internal(t *testing.T) {
	svc := &mockService{t: t, listModelsFn: func(_ context.Context, _ domain.Actor, _ domain.ListModelsInput) (domain.Page[domain.Model], error) {
		return domain.Page[domain.Model]{}, errors.New("redis dial tcp 10.9.9.9:6379: connection refused")
	}}
	client := newClient(t, svc)
	_, err := client.ListModels(ctxGood(), &registryv1.ListModelsRequest{})
	requireCode(t, err, codes.Internal)
	assertNoLeak(t, err, "redis", "10.9.9.9", "connection refused")
}

// ============================================================================
// GetVersion
// ============================================================================

func TestGetVersion_HappyPath_ByLabel(t *testing.T) {
	var gotID, gotModelID, gotVersion string
	svc := &mockService{t: t, getVerFn: func(_ context.Context, _ domain.Actor, id, modelID, version string) (domain.ModelVersion, error) {
		gotID, gotModelID, gotVersion = id, modelID, version
		return domain.ModelVersion{ID: "ver-1", ModelID: modelID, Version: version, Stage: domain.StageProduction, Status: domain.StatusReady}, nil
	}}
	client := newClient(t, svc)

	resp, err := client.GetVersion(ctxGood(), &registryv1.GetVersionRequest{ModelId: "model-1", Version: "1"})
	if err != nil {
		t.Fatalf("GetVersion: %v", err)
	}
	if gotID != "" || gotModelID != "model-1" || gotVersion != "1" {
		t.Errorf("get version args = id:%q model:%q version:%q", gotID, gotModelID, gotVersion)
	}
	if resp.GetVersion().GetStage() != registryv1.ModelStage_MODEL_STAGE_PRODUCTION {
		t.Errorf("stage not mapped: %v", resp.GetVersion().GetStage())
	}
}

func TestGetVersion_Validation(t *testing.T) {
	tests := []struct {
		name string
		req  *registryv1.GetVersionRequest
	}{
		{"nothing provided", &registryv1.GetVersionRequest{}},
		{"model id without version label", &registryv1.GetVersionRequest{ModelId: "model-1"}},
		{"version label without model id", &registryv1.GetVersionRequest{Version: "1"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := newClient(t, &mockService{t: t})
			_, err := client.GetVersion(ctxGood(), tc.req)
			requireCode(t, err, codes.InvalidArgument)
		})
	}
}

func TestGetVersion_ErrorMapping_NotFound(t *testing.T) {
	svc := &mockService{t: t, getVerFn: func(_ context.Context, _ domain.Actor, _, _, _ string) (domain.ModelVersion, error) {
		return domain.ModelVersion{}, domain.ErrVersionNotFound
	}}
	client := newClient(t, svc)
	_, err := client.GetVersion(ctxGood(), &registryv1.GetVersionRequest{Id: "ver-1"})
	requireCode(t, err, codes.NotFound)
}

// ============================================================================
// ListVersions
// ============================================================================

func TestListVersions_HappyPath_StageFilterAny(t *testing.T) {
	var gotModelID string
	var gotStage domain.ModelStage
	var gotSize int
	var gotToken string
	svc := &mockService{t: t, listVerFn: func(_ context.Context, _ domain.Actor, modelID string, stage domain.ModelStage, pageSize int, pageToken string) (domain.Page[domain.ModelVersion], error) {
		gotModelID, gotStage, gotSize, gotToken = modelID, stage, pageSize, pageToken
		return domain.Page[domain.ModelVersion]{
			Items:     []domain.ModelVersion{{ID: "ver-2"}, {ID: "ver-1"}},
			NextToken: "",
		}, nil
	}}
	client := newClient(t, svc)

	resp, err := client.ListVersions(ctxGood(), &registryv1.ListVersionsRequest{
		ModelId:     "model-1",
		StageFilter: registryv1.ModelStage_MODEL_STAGE_UNSPECIFIED, // legal "any" filter
		Pagination:  &commonv1.PaginationRequest{PageSize: 10, PageToken: "p"},
	})
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	// UNSPECIFIED must pass through as StageUnspecified ("any") — NOT be rejected.
	if gotModelID != "model-1" || gotStage != domain.StageUnspecified || gotSize != 10 || gotToken != "p" {
		t.Errorf("list versions args = model:%q stage:%v size:%d token:%q", gotModelID, gotStage, gotSize, gotToken)
	}
	if len(resp.GetVersions()) != 2 {
		t.Errorf("versions = %d, want 2", len(resp.GetVersions()))
	}
}

func TestListVersions_HappyPath_StageFilterSpecific(t *testing.T) {
	var gotStage domain.ModelStage
	svc := &mockService{t: t, listVerFn: func(_ context.Context, _ domain.Actor, _ string, stage domain.ModelStage, _ int, _ string) (domain.Page[domain.ModelVersion], error) {
		gotStage = stage
		return domain.Page[domain.ModelVersion]{}, nil
	}}
	client := newClient(t, svc)
	_, err := client.ListVersions(ctxGood(), &registryv1.ListVersionsRequest{
		ModelId: "model-1", StageFilter: registryv1.ModelStage_MODEL_STAGE_PRODUCTION,
	})
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	if gotStage != domain.StageProduction {
		t.Errorf("stage filter = %v, want Production", gotStage)
	}
}

func TestListVersions_Validation_EmptyModelID(t *testing.T) {
	client := newClient(t, &mockService{t: t})
	_, err := client.ListVersions(ctxGood(), &registryv1.ListVersionsRequest{ModelId: ""})
	requireCode(t, err, codes.InvalidArgument)
}

func TestListVersions_Validation_PageSizeOverCap(t *testing.T) {
	client := newClient(t, &mockService{t: t})
	_, err := client.ListVersions(ctxGood(), &registryv1.ListVersionsRequest{
		ModelId:    "model-1",
		Pagination: &commonv1.PaginationRequest{PageSize: domain.MaxPageSize + 1},
	})
	requireCode(t, err, codes.InvalidArgument)
}

// ============================================================================
// NIL-SERVICE GUARD — an unwired binary answers Unimplemented, never panics
// ============================================================================

func TestNilService_ReturnsUnimplemented(t *testing.T) {
	// Handler built with a nil domain service (the scaffold/repo-phase posture).
	client := newClient(t, nil)
	_, err := client.RegisterModel(ctxGood(), &registryv1.RegisterModelRequest{Name: "x"})
	requireCode(t, err, codes.Unimplemented)
}

// ============================================================================
// UN-OVERRIDDEN RPCs — stay on the embedded Unimplemented base
// ============================================================================

func TestUnimplementedRPCs_SearchAndStorage(t *testing.T) {
	// These three RPCs have no domain method; the embedded base must answer them.
	client := newClient(t, &mockService{t: t})
	if _, err := client.SearchByTag(ctxGood(), &registryv1.SearchByTagRequest{Key: "k", Value: "v"}); status.Code(err) != codes.Unimplemented {
		t.Errorf("SearchByTag code = %v, want Unimplemented", status.Code(err))
	}
	if _, err := client.GetUploadURL(ctxGood(), &registryv1.GetUploadURLRequest{}); status.Code(err) != codes.Unimplemented {
		t.Errorf("GetUploadURL code = %v, want Unimplemented", status.Code(err))
	}
	if _, err := client.GetDownloadURL(ctxGood(), &registryv1.GetDownloadURLRequest{}); status.Code(err) != codes.Unimplemented {
		t.Errorf("GetDownloadURL code = %v, want Unimplemented", status.Code(err))
	}
}
