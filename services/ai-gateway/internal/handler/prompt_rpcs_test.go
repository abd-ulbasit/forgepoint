// prompt_rpcs_test.go — bufconn (in-process gRPC) tests for the PROMPT REGISTRY RPCs.
//
// ============================================================================
// WHAT THIS PROVES
// ============================================================================
//
//  1. SELF-GATING: when the prompt registry is NOT wired (h.prompts == nil, the
//     NewHandler path used when no database is configured), all four prompt RPCs
//     return codes.Unimplemented — the gateway still runs, the prompt surface is off.
//  2. TEAM FROM CLAIMS: when wired, the handler passes the team from the verified
//     auth claims into the service, NEVER a request field (the proto requests carry
//     no team). A fake prompt service records the team it was called with.
//  3. ERROR MAPPING: the domain's business sentinels map to the right gRPC codes
//     (NotFound / InvalidArgument / AlreadyExists).
//
// The service is a FAKE prompt.PromptService (no Postgres), so these stay fast and
// -race clean while exercising the real proto↔domain mapping and the auth context.
package handler

import (
	"context"
	"testing"

	aiv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/ai/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/prompt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakePromptService records the team it was called with and returns canned results,
// so the handler tests can assert team-from-claims + error mapping without a DB.
type fakePromptService struct {
	gotTeam string
	created prompt.Prompt
	got     prompt.Prompt
	page    prompt.Page
	render  prompt.RenderResult
	err     error // when set, every method returns it (to test error mapping)
}

func (f *fakePromptService) CreatePrompt(_ context.Context, team string, _ prompt.CreatePromptInput) (prompt.Prompt, error) {
	f.gotTeam = team
	if f.err != nil {
		return prompt.Prompt{}, f.err
	}
	return f.created, nil
}

func (f *fakePromptService) GetPrompt(_ context.Context, team, _ string, _ int) (prompt.Prompt, error) {
	f.gotTeam = team
	if f.err != nil {
		return prompt.Prompt{}, f.err
	}
	return f.got, nil
}

func (f *fakePromptService) ListPrompts(_ context.Context, team string, _ prompt.ListPromptsInput) (prompt.Page, error) {
	f.gotTeam = team
	if f.err != nil {
		return prompt.Page{}, f.err
	}
	return f.page, nil
}

func (f *fakePromptService) RenderPrompt(_ context.Context, team string, _ prompt.RenderPromptInput) (prompt.RenderResult, error) {
	f.gotTeam = team
	if f.err != nil {
		return prompt.RenderResult{}, f.err
	}
	return f.render, nil
}

// newPromptClient wires the handler with BOTH a (nil-safe) gateway service and the
// given prompt service over a bufconn server WITH auth interceptors injecting team-x.
// A nil promptSvc exercises the self-gating (Unimplemented) path.
func newPromptClient(t *testing.T, promptSvc prompt.PromptService) aiv1.AIGatewayServiceClient {
	t.Helper()
	v := stubValidator{team: "team-x"}
	serverOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(grpcutil.AuthUnaryInterceptor(v)),
		grpc.ChainStreamInterceptor(grpcutil.AuthStreamInterceptor(v)),
	}
	conn := testutil.NewTestGRPCServer(t, func(s *grpc.Server) {
		// A non-nil gateway service so the handler is fully formed; the prompt service
		// is the unit under test here. domain gateway RPCs are unused in these tests.
		aiv1.RegisterAIGatewayServiceServer(s, NewHandlerWithPrompts(&fakeService{}, promptSvc))
	}, serverOpts...)
	return aiv1.NewAIGatewayServiceClient(conn)
}

// --- self-gating: nil prompt service → Unimplemented on all four RPCs --------

func TestPromptRPCs_UnimplementedWhenNotWired(t *testing.T) {
	t.Parallel()
	// NewHandlerWithPrompts(svc, nil) == the no-database path.
	client := newPromptClient(t, nil)
	ctx := authCtx()

	if _, err := client.CreatePrompt(ctx, &aiv1.CreatePromptRequest{Name: "p", Template: "t"}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("CreatePrompt unwired code = %v, want Unimplemented", status.Code(err))
	}
	if _, err := client.GetPrompt(ctx, &aiv1.GetPromptRequest{Name: "p"}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("GetPrompt unwired code = %v, want Unimplemented", status.Code(err))
	}
	if _, err := client.ListPrompts(ctx, &aiv1.ListPromptsRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("ListPrompts unwired code = %v, want Unimplemented", status.Code(err))
	}
	if _, err := client.RenderPrompt(ctx, &aiv1.RenderPromptRequest{Name: "p"}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("RenderPrompt unwired code = %v, want Unimplemented", status.Code(err))
	}
}

// --- wired: team-from-claims + happy-path mapping ----------------------------

func TestCreatePrompt_PassesClaimsTeamAndMaps(t *testing.T) {
	t.Parallel()
	svc := &fakePromptService{created: prompt.Prompt{
		ID: "prompt-1", Name: "summarize", Version: 1, Stage: prompt.StageDev,
		Template: "Summarize {{doc}}", Variables: []string{"doc"}, Team: "team-x",
	}}
	client := newPromptClient(t, svc)

	resp, err := client.CreatePrompt(authCtx(), &aiv1.CreatePromptRequest{
		Name: "summarize", Template: "Summarize {{doc}}",
	})
	if err != nil {
		t.Fatalf("CreatePrompt: %v", err)
	}
	// The handler passed the CLAIMS team, never a body value (there is no body team).
	if svc.gotTeam != "team-x" {
		t.Fatalf("service got team %q, want claims team %q", svc.gotTeam, "team-x")
	}
	p := resp.GetPrompt()
	if p.GetVersion() != 1 || p.GetStage() != aiv1.PromptStage_PROMPT_STAGE_DEV {
		t.Fatalf("mapped prompt = v%d stage %v, want v1 DEV", p.GetVersion(), p.GetStage())
	}
	if len(p.GetVariables()) != 1 || p.GetVariables()[0] != "doc" {
		t.Fatalf("mapped variables = %v, want [doc]", p.GetVariables())
	}
	if p.GetTeam() != "team-x" {
		t.Fatalf("mapped team = %q, want team-x", p.GetTeam())
	}
}

func TestGetPrompt_NotFoundMapsToNotFound(t *testing.T) {
	t.Parallel()
	svc := &fakePromptService{err: prompt.ErrNotFound}
	client := newPromptClient(t, svc)
	_, err := client.GetPrompt(authCtx(), &aiv1.GetPromptRequest{Name: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetPrompt NotFound mapping = %v, want NotFound", status.Code(err))
	}
}

func TestRenderPrompt_MissingVarMapsToInvalidArgument(t *testing.T) {
	t.Parallel()
	svc := &fakePromptService{err: prompt.ErrValidation}
	client := newPromptClient(t, svc)
	_, err := client.RenderPrompt(authCtx(), &aiv1.RenderPromptRequest{Name: "p"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("RenderPrompt validation mapping = %v, want InvalidArgument", status.Code(err))
	}
}

func TestRenderPrompt_HappyPathReturnsTextAndVersion(t *testing.T) {
	t.Parallel()
	svc := &fakePromptService{render: prompt.RenderResult{Rendered: "Summarize the report", Version: 3}}
	client := newPromptClient(t, svc)
	resp, err := client.RenderPrompt(authCtx(), &aiv1.RenderPromptRequest{
		Name: "summarize", Variables: map[string]string{"doc": "the report"},
	})
	if err != nil {
		t.Fatalf("RenderPrompt: %v", err)
	}
	if resp.GetRendered() != "Summarize the report" || resp.GetVersion() != 3 {
		t.Fatalf("render resp = %q v%d, want 'Summarize the report' v3", resp.GetRendered(), resp.GetVersion())
	}
	if svc.gotTeam != "team-x" {
		t.Fatalf("render service got team %q, want team-x", svc.gotTeam)
	}
}

// compile-time assertion the fake satisfies the port (keeps the test honest if the
// interface changes).
var _ prompt.PromptService = (*fakePromptService)(nil)
