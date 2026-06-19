// Package handlers contains the BFF's HTTP handlers. Each handler does exactly
// three things (ADR guardrail — ZERO business logic):
//  1. parse the HTTP request (path vars, query, JSON body) into a proto request,
//  2. call the downstream gRPC stub with the CALLER'S token forwarded
//     (httpx.ContextWithToken),
//  3. map the proto response (or gRPC error) back to JSON (httpx helpers).
//
// ============================================================================
// WHY SMALL PER-RESOURCE INTERFACES INSTEAD OF THE FAT *ServiceClient
// ============================================================================
//
// The generated *ServiceClient interfaces have 7-15 methods each; a handler uses
// 1-4 of them. We declare a MINIMAL interface naming only the methods a given
// handler actually calls (interface segregation). Two payoffs:
//
//   - TESTABILITY: a unit test mocks a 2-method interface, not a 15-method one.
//     The mock is tiny and the test states exactly which RPCs the handler may
//     touch — calling an unexpected one fails the test.
//   - HONESTY: the type signature documents the handler's real dependency
//     surface. A reviewer sees ModelLister needs only ListModels/GetModel/...,
//     not the whole registry API.
//
// The real *registryv1.RegistryServiceClient SATISFIES these because Go
// interfaces are structural — no adapter, no wiring. The clients layer hands the
// concrete stub straight in.
// ============================================================================
package handlers

import (
	"context"

	"google.golang.org/grpc"

	aiv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/ai/v1"
	authv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/auth/v1"
	billingv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/billing/v1"
	experimentv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/experiment/v1"
	monitorv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/monitor/v1"
	notificationv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/notification/v1"
	pipelinev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/pipeline/v1"
	registryv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/registry/v1"
)

// AuthClient is the slice of the auth API the BFF uses: just Login. The BFF does
// NOT call ValidateToken (it doesn't validate tokens — services do) or any admin
// RPC (those belong to an admin UI flow, out of scope here).
type AuthClient interface {
	Login(ctx context.Context, in *authv1.LoginRequest, opts ...grpc.CallOption) (*authv1.LoginResponse, error)
}

// RegistryClient is the slice of the registry API the models endpoints use.
type RegistryClient interface {
	ListModels(ctx context.Context, in *registryv1.ListModelsRequest, opts ...grpc.CallOption) (*registryv1.ListModelsResponse, error)
	GetModel(ctx context.Context, in *registryv1.GetModelRequest, opts ...grpc.CallOption) (*registryv1.GetModelResponse, error)
	RegisterModel(ctx context.Context, in *registryv1.RegisterModelRequest, opts ...grpc.CallOption) (*registryv1.RegisterModelResponse, error)
	ListVersions(ctx context.Context, in *registryv1.ListVersionsRequest, opts ...grpc.CallOption) (*registryv1.ListVersionsResponse, error)
}

// PipelineClient is the slice of the pipeline API the pipelines/executions
// endpoints use, including the WatchExecution server-stream relayed as SSE.
type PipelineClient interface {
	ListPipelines(ctx context.Context, in *pipelinev1.ListPipelinesRequest, opts ...grpc.CallOption) (*pipelinev1.ListPipelinesResponse, error)
	CreatePipeline(ctx context.Context, in *pipelinev1.CreatePipelineRequest, opts ...grpc.CallOption) (*pipelinev1.CreatePipelineResponse, error)
	TriggerExecution(ctx context.Context, in *pipelinev1.TriggerExecutionRequest, opts ...grpc.CallOption) (*pipelinev1.TriggerExecutionResponse, error)
	GetExecution(ctx context.Context, in *pipelinev1.GetExecutionRequest, opts ...grpc.CallOption) (*pipelinev1.GetExecutionResponse, error)
	ListExecutions(ctx context.Context, in *pipelinev1.ListExecutionsRequest, opts ...grpc.CallOption) (*pipelinev1.ListExecutionsResponse, error)
	WatchExecution(ctx context.Context, in *pipelinev1.WatchExecutionRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[pipelinev1.WatchExecutionResponse], error)
}

// ExperimentClient is the slice of the experiment-tracker API the runs endpoints
// use.
type ExperimentClient interface {
	ListRuns(ctx context.Context, in *experimentv1.ListRunsRequest, opts ...grpc.CallOption) (*experimentv1.ListRunsResponse, error)
	GetRun(ctx context.Context, in *experimentv1.GetRunRequest, opts ...grpc.CallOption) (*experimentv1.GetRunResponse, error)
}

// MonitorClient is the slice of the model-monitor API the monitors/drift/evals
// endpoints use.
//
// ListEvalScores (L4) is the LLM-eval read path: the monitor service judges a
// sample of LLM responses (an LLM-as-judge step) and stores per-response
// relevance/coherence/safety/overall scores. The eval dashboard lists them; the
// monitor ALSO emits an `llm_quality` performance-drift report off these scores,
// which is why the dashboard cross-links to /drift-reports rather than
// recomputing drift in the browser (the BFF stays logic-free — the monitor owns
// the threshold math).
type MonitorClient interface {
	ListMonitors(ctx context.Context, in *monitorv1.ListMonitorsRequest, opts ...grpc.CallOption) (*monitorv1.ListMonitorsResponse, error)
	ListDriftReports(ctx context.Context, in *monitorv1.ListDriftReportsRequest, opts ...grpc.CallOption) (*monitorv1.ListDriftReportsResponse, error)
	ListEvalScores(ctx context.Context, in *monitorv1.ListEvalScoresRequest, opts ...grpc.CallOption) (*monitorv1.ListEvalScoresResponse, error)
}

// BillingClient is the slice of the billing API the usage endpoint uses.
type BillingClient interface {
	GetUsage(ctx context.Context, in *billingv1.GetUsageRequest, opts ...grpc.CallOption) (*billingv1.GetUsageResponse, error)
}

// NotificationClient is the slice of the notification API the notifications
// endpoint uses.
type NotificationClient interface {
	ListNotifications(ctx context.Context, in *notificationv1.ListNotificationsRequest, opts ...grpc.CallOption) (*notificationv1.ListNotificationsResponse, error)
}

// AIGatewayClient is the slice of the AI Gateway API the chat/playground AND
// prompt-registry endpoints use. ChatCompletion is the server-streaming RPC the
// BFF relays to the browser as SSE (the one piece of LLM real-time the BFF owns);
// everything else is a plain unary proxy.
//
// The four Prompt RPCs (L3 — the prompt registry) are the new additions:
// Create/Get/List version prompt templates, and Render expands a template's
// {{variables}} against a supplied map server-side. We deliberately keep
// rendering on the SERVER (not in the browser) so the SAME templating engine the
// gateway uses at completion time produces the preview — a client-side {{ }}
// replace would risk drifting from the real render and would also be a place to
// accidentally introduce an injection sink. The BFF just forwards the variables
// map and relays the rendered text as data.
type AIGatewayClient interface {
	ChatCompletion(ctx context.Context, in *aiv1.ChatCompletionRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[aiv1.ChatCompletionResponse], error)
	ListProviders(ctx context.Context, in *aiv1.ListProvidersRequest, opts ...grpc.CallOption) (*aiv1.ListProvidersResponse, error)
	GetUsage(ctx context.Context, in *aiv1.GetUsageRequest, opts ...grpc.CallOption) (*aiv1.GetUsageResponse, error)
	CreatePrompt(ctx context.Context, in *aiv1.CreatePromptRequest, opts ...grpc.CallOption) (*aiv1.CreatePromptResponse, error)
	GetPrompt(ctx context.Context, in *aiv1.GetPromptRequest, opts ...grpc.CallOption) (*aiv1.GetPromptResponse, error)
	ListPrompts(ctx context.Context, in *aiv1.ListPromptsRequest, opts ...grpc.CallOption) (*aiv1.ListPromptsResponse, error)
	RenderPrompt(ctx context.Context, in *aiv1.RenderPromptRequest, opts ...grpc.CallOption) (*aiv1.RenderPromptResponse, error)
}
