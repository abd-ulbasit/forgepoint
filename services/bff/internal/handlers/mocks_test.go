package handlers

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	aiv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/ai/v1"
	authv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/auth/v1"
	billingv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/billing/v1"
	experimentv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/experiment/v1"
	monitorv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/monitor/v1"
	notificationv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/notification/v1"
	pipelinev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/pipeline/v1"
	registryv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/registry/v1"
)

// ============================================================================
// HAND-ROLLED MOCKS of the small per-resource client interfaces (ports.go).
//
// We mock the SEGMENTED interfaces, not the fat generated *ServiceClient — that
// is the whole reason those small interfaces exist (interface segregation makes
// the mocks tiny). Each mock is a struct of function fields, so a test sets only
// the method it exercises; an unexpected call hits a nil func and panics, which
// surfaces a wrong-RPC bug loudly.
//
// capturedMD captures the OUTGOING gRPC metadata each mock saw, so a test can
// assert the BFF forwarded "authorization: Bearer <token>" — the core
// token-propagation guarantee.
// ============================================================================

// authzFromCtx pulls the outgoing "authorization" metadata a handler attached.
// Returns "" if none. This is what a real downstream interceptor would read.
func authzFromCtx(ctx context.Context) string {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return ""
	}
	v := md.Get("authorization")
	if len(v) == 0 {
		return ""
	}
	return v[0]
}

// ---- Auth mock --------------------------------------------------------------

type mockAuth struct {
	loginFn func(ctx context.Context, in *authv1.LoginRequest) (*authv1.LoginResponse, error)
}

func (m *mockAuth) Login(ctx context.Context, in *authv1.LoginRequest, _ ...grpc.CallOption) (*authv1.LoginResponse, error) {
	return m.loginFn(ctx, in)
}

// ---- Registry mock ----------------------------------------------------------

type mockRegistry struct {
	capturedAuthz   string
	listModelsFn    func(ctx context.Context, in *registryv1.ListModelsRequest) (*registryv1.ListModelsResponse, error)
	getModelFn      func(ctx context.Context, in *registryv1.GetModelRequest) (*registryv1.GetModelResponse, error)
	registerModelFn func(ctx context.Context, in *registryv1.RegisterModelRequest) (*registryv1.RegisterModelResponse, error)
	listVersionsFn  func(ctx context.Context, in *registryv1.ListVersionsRequest) (*registryv1.ListVersionsResponse, error)
}

func (m *mockRegistry) ListModels(ctx context.Context, in *registryv1.ListModelsRequest, _ ...grpc.CallOption) (*registryv1.ListModelsResponse, error) {
	m.capturedAuthz = authzFromCtx(ctx)
	return m.listModelsFn(ctx, in)
}
func (m *mockRegistry) GetModel(ctx context.Context, in *registryv1.GetModelRequest, _ ...grpc.CallOption) (*registryv1.GetModelResponse, error) {
	m.capturedAuthz = authzFromCtx(ctx)
	return m.getModelFn(ctx, in)
}
func (m *mockRegistry) RegisterModel(ctx context.Context, in *registryv1.RegisterModelRequest, _ ...grpc.CallOption) (*registryv1.RegisterModelResponse, error) {
	m.capturedAuthz = authzFromCtx(ctx)
	return m.registerModelFn(ctx, in)
}
func (m *mockRegistry) ListVersions(ctx context.Context, in *registryv1.ListVersionsRequest, _ ...grpc.CallOption) (*registryv1.ListVersionsResponse, error) {
	m.capturedAuthz = authzFromCtx(ctx)
	return m.listVersionsFn(ctx, in)
}

// ---- Pipeline mock ----------------------------------------------------------

type mockPipeline struct {
	listFn       func(ctx context.Context, in *pipelinev1.ListPipelinesRequest) (*pipelinev1.ListPipelinesResponse, error)
	createFn     func(ctx context.Context, in *pipelinev1.CreatePipelineRequest) (*pipelinev1.CreatePipelineResponse, error)
	triggerFn    func(ctx context.Context, in *pipelinev1.TriggerExecutionRequest) (*pipelinev1.TriggerExecutionResponse, error)
	getExecFn    func(ctx context.Context, in *pipelinev1.GetExecutionRequest) (*pipelinev1.GetExecutionResponse, error)
	listExecFn   func(ctx context.Context, in *pipelinev1.ListExecutionsRequest) (*pipelinev1.ListExecutionsResponse, error)
	watchFn      func(ctx context.Context, in *pipelinev1.WatchExecutionRequest) (grpc.ServerStreamingClient[pipelinev1.WatchExecutionResponse], error)
}

func (m *mockPipeline) ListPipelines(ctx context.Context, in *pipelinev1.ListPipelinesRequest, _ ...grpc.CallOption) (*pipelinev1.ListPipelinesResponse, error) {
	return m.listFn(ctx, in)
}
func (m *mockPipeline) CreatePipeline(ctx context.Context, in *pipelinev1.CreatePipelineRequest, _ ...grpc.CallOption) (*pipelinev1.CreatePipelineResponse, error) {
	return m.createFn(ctx, in)
}
func (m *mockPipeline) TriggerExecution(ctx context.Context, in *pipelinev1.TriggerExecutionRequest, _ ...grpc.CallOption) (*pipelinev1.TriggerExecutionResponse, error) {
	return m.triggerFn(ctx, in)
}
func (m *mockPipeline) GetExecution(ctx context.Context, in *pipelinev1.GetExecutionRequest, _ ...grpc.CallOption) (*pipelinev1.GetExecutionResponse, error) {
	return m.getExecFn(ctx, in)
}
func (m *mockPipeline) ListExecutions(ctx context.Context, in *pipelinev1.ListExecutionsRequest, _ ...grpc.CallOption) (*pipelinev1.ListExecutionsResponse, error) {
	return m.listExecFn(ctx, in)
}
func (m *mockPipeline) WatchExecution(ctx context.Context, in *pipelinev1.WatchExecutionRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[pipelinev1.WatchExecutionResponse], error) {
	return m.watchFn(ctx, in)
}

// ---- Experiment mock --------------------------------------------------------

type mockExperiment struct {
	listRunsFn func(ctx context.Context, in *experimentv1.ListRunsRequest) (*experimentv1.ListRunsResponse, error)
	getRunFn   func(ctx context.Context, in *experimentv1.GetRunRequest) (*experimentv1.GetRunResponse, error)
}

func (m *mockExperiment) ListRuns(ctx context.Context, in *experimentv1.ListRunsRequest, _ ...grpc.CallOption) (*experimentv1.ListRunsResponse, error) {
	return m.listRunsFn(ctx, in)
}
func (m *mockExperiment) GetRun(ctx context.Context, in *experimentv1.GetRunRequest, _ ...grpc.CallOption) (*experimentv1.GetRunResponse, error) {
	return m.getRunFn(ctx, in)
}

// ---- Monitor mock -----------------------------------------------------------

type mockMonitor struct {
	listMonitorsFn func(ctx context.Context, in *monitorv1.ListMonitorsRequest) (*monitorv1.ListMonitorsResponse, error)
	listDriftFn    func(ctx context.Context, in *monitorv1.ListDriftReportsRequest) (*monitorv1.ListDriftReportsResponse, error)
}

func (m *mockMonitor) ListMonitors(ctx context.Context, in *monitorv1.ListMonitorsRequest, _ ...grpc.CallOption) (*monitorv1.ListMonitorsResponse, error) {
	return m.listMonitorsFn(ctx, in)
}
func (m *mockMonitor) ListDriftReports(ctx context.Context, in *monitorv1.ListDriftReportsRequest, _ ...grpc.CallOption) (*monitorv1.ListDriftReportsResponse, error) {
	return m.listDriftFn(ctx, in)
}

// ---- Billing mock -----------------------------------------------------------

type mockBilling struct {
	getUsageFn func(ctx context.Context, in *billingv1.GetUsageRequest) (*billingv1.GetUsageResponse, error)
}

func (m *mockBilling) GetUsage(ctx context.Context, in *billingv1.GetUsageRequest, _ ...grpc.CallOption) (*billingv1.GetUsageResponse, error) {
	return m.getUsageFn(ctx, in)
}

// ---- Notification mock ------------------------------------------------------

type mockNotification struct {
	listFn func(ctx context.Context, in *notificationv1.ListNotificationsRequest) (*notificationv1.ListNotificationsResponse, error)
}

func (m *mockNotification) ListNotifications(ctx context.Context, in *notificationv1.ListNotificationsRequest, _ ...grpc.CallOption) (*notificationv1.ListNotificationsResponse, error) {
	return m.listFn(ctx, in)
}

// ---- AI Gateway mock --------------------------------------------------------

type mockAIGateway struct {
	chatFn      func(ctx context.Context, in *aiv1.ChatCompletionRequest) (grpc.ServerStreamingClient[aiv1.ChatCompletionResponse], error)
	providersFn func(ctx context.Context, in *aiv1.ListProvidersRequest) (*aiv1.ListProvidersResponse, error)
	usageFn     func(ctx context.Context, in *aiv1.GetUsageRequest) (*aiv1.GetUsageResponse, error)
}

func (m *mockAIGateway) ChatCompletion(ctx context.Context, in *aiv1.ChatCompletionRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[aiv1.ChatCompletionResponse], error) {
	return m.chatFn(ctx, in)
}
func (m *mockAIGateway) ListProviders(ctx context.Context, in *aiv1.ListProvidersRequest, _ ...grpc.CallOption) (*aiv1.ListProvidersResponse, error) {
	return m.providersFn(ctx, in)
}
func (m *mockAIGateway) GetUsage(ctx context.Context, in *aiv1.GetUsageRequest, _ ...grpc.CallOption) (*aiv1.GetUsageResponse, error) {
	return m.usageFn(ctx, in)
}

// Compile-time assertions that the mocks satisfy the handler ports.
var (
	_ AuthClient         = (*mockAuth)(nil)
	_ RegistryClient     = (*mockRegistry)(nil)
	_ PipelineClient     = (*mockPipeline)(nil)
	_ ExperimentClient   = (*mockExperiment)(nil)
	_ MonitorClient      = (*mockMonitor)(nil)
	_ BillingClient      = (*mockBilling)(nil)
	_ NotificationClient = (*mockNotification)(nil)
	_ AIGatewayClient    = (*mockAIGateway)(nil)
)
