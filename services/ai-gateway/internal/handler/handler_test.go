// handler_test.go — bufconn (in-process gRPC) tests for the AI Gateway handler.
//
// ============================================================================
// WHAT THIS PROVES (the handler + the stream-auth mechanic, end to end)
// ============================================================================
//
// These tests run a REAL in-process gRPC stack over bufconn (pkg/testutil) WITH the
// shared auth interceptors installed, so they exercise the interview-critical
// STREAM-AUTH path: the client sets an "authorization: Bearer <token>" metadata, the
// AuthStreamInterceptor validates it (via a stub validator returning fixed claims)
// and wraps the ServerStream so its Context() carries the claims, and the handler's
// ChatCompletion reads the TEAM from that context via grpcutil.ClaimsFromContext —
// NOT from the request body. A unary GetUsage test proves the same for the unary path.
//
// The domain is a tiny FAKE GatewayService (no Redis/NATS/HTTP), so the test is fast,
// deterministic, and -race clean while still proving the proto↔domain mapping and the
// streamed terminal frame.
package handler

import (
	"context"
	"errors"
	"io"
	"testing"

	aiv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/ai/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// --- fake domain service ----------------------------------------------------

// fakeService is a minimal domain.GatewayService for the handler tests. It records
// the team it was called with (to prove the handler passed the claims-derived team,
// never a body value) and streams a canned set of deltas via the sink.
type fakeService struct {
	gotTeam    string
	streamErr  error // returned from ChatCompletion (e.g. ErrBudgetExceeded)
	deltas     []domain.Delta
	completion domain.Completion
	usage      domain.UsageSummary
}

func (f *fakeService) ChatCompletion(_ context.Context, team string, _ domain.ChatRequest, sink domain.Sink) (domain.Completion, error) {
	f.gotTeam = team
	if f.streamErr != nil {
		return domain.Completion{FinishReason: domain.FinishReasonError}, f.streamErr
	}
	for _, d := range f.deltas {
		if err := sink(d); err != nil {
			return domain.Completion{}, err
		}
	}
	return f.completion, nil
}

func (f *fakeService) Providers() []domain.ProviderSnapshot {
	return []domain.ProviderSnapshot{
		{Kind: domain.ProviderKindOllama, Name: "ollama", Enabled: true, CircuitState: domain.CircuitOpen},
		{Kind: domain.ProviderKindStub, Name: "stub", Enabled: true, CircuitState: domain.CircuitClosed},
	}
}

func (f *fakeService) Usage(_ context.Context, _ string) (domain.UsageSummary, error) {
	return f.usage, nil
}

// --- auth stub --------------------------------------------------------------

// stubValidator returns fixed claims for any non-empty token (the "team-x" tenant),
// and an error for an empty token — so the auth interceptor injects a real team into
// the context, exactly as the JWT validator would in production.
type stubValidator struct{ team string }

func (v stubValidator) Validate(_ context.Context, token string) (*grpcutil.Claims, error) {
	if token == "" {
		return nil, errors.New("no token")
	}
	return &grpcutil.Claims{UserID: "user-1", Team: v.team, Role: "member"}, nil
}

// newClient wires the handler over the fake service into a bufconn server WITH the
// auth interceptors, and returns a connected client. Passing team="" installs no
// auth (to test the Unauthenticated path); otherwise the stub injects that team.
func newClient(t *testing.T, svc domain.GatewayService, team string) aiv1.AIGatewayServiceClient {
	t.Helper()
	var serverOpts []grpc.ServerOption
	if team != "" {
		v := stubValidator{team: team}
		serverOpts = append(serverOpts,
			grpc.ChainUnaryInterceptor(grpcutil.AuthUnaryInterceptor(v)),
			grpc.ChainStreamInterceptor(grpcutil.AuthStreamInterceptor(v)),
		)
	}
	conn := testutil.NewTestGRPCServer(t, func(s *grpc.Server) {
		aiv1.RegisterAIGatewayServiceServer(s, NewHandler(svc))
	}, serverOpts...)
	return aiv1.NewAIGatewayServiceClient(conn)
}

// authCtx attaches a bearer token so the auth interceptor validates + injects claims.
func authCtx() context.Context {
	return metadata.NewOutgoingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer test-token"))
}

// --- tests ------------------------------------------------------------------

func TestChatCompletion_StreamsDeltasAndTerminalFrame(t *testing.T) {
	t.Parallel()
	svc := &fakeService{
		deltas: []domain.Delta{{Text: "Hello "}, {Text: "world"}},
		completion: domain.Completion{
			ServedBy:     domain.ProviderKindStub,
			FinishReason: domain.FinishReasonStop,
			Usage:        domain.TokenUsage{PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5, CostMicroUSD: 42},
		},
	}
	client := newClient(t, svc, "team-x")

	stream, err := client.ChatCompletion(authCtx(), &aiv1.ChatCompletionRequest{
		Model:    "smollm2",
		Messages: []*aiv1.ChatMessage{{Role: aiv1.ChatRole_CHAT_ROLE_USER, Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("ChatCompletion start error: %v", err)
	}

	var text string
	var terminal *aiv1.ChatCompletionResponse
	for {
		frame, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("stream recv error: %v", err)
		}
		if frame.GetDone() {
			terminal = frame
			continue
		}
		text += frame.GetDelta()
	}

	if text != "Hello world" {
		t.Fatalf("streamed text = %q, want %q", text, "Hello world")
	}
	if terminal == nil {
		t.Fatalf("never received a terminal (done) frame")
	}
	if terminal.GetFinishReason() != aiv1.FinishReason_FINISH_REASON_STOP {
		t.Fatalf("terminal finish = %v, want STOP", terminal.GetFinishReason())
	}
	if terminal.GetServedBy() != aiv1.ProviderKind_PROVIDER_KIND_STUB {
		t.Fatalf("served_by = %v, want STUB", terminal.GetServedBy())
	}
	if terminal.GetUsage().GetTotalTokens() != 5 || terminal.GetUsage().GetCostMicroUsd() != 42 {
		t.Fatalf("terminal usage = %+v, want total=5 cost=42", terminal.GetUsage())
	}
	if terminal.GetRequestId() == "" {
		t.Fatalf("terminal frame missing the server-minted request_id")
	}
	// The handler passed the CLAIMS team, never a body value.
	if svc.gotTeam != "team-x" {
		t.Fatalf("service called with team %q, want claims team %q", svc.gotTeam, "team-x")
	}
}

func TestChatCompletion_BudgetExceededMapsToResourceExhausted(t *testing.T) {
	t.Parallel()
	svc := &fakeService{streamErr: domain.ErrBudgetExceeded}
	client := newClient(t, svc, "team-x")

	stream, err := client.ChatCompletion(authCtx(), &aiv1.ChatCompletionRequest{
		Messages: []*aiv1.ChatMessage{{Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("start error: %v", err)
	}
	// The error surfaces on the first Recv (the RPC returns the status).
	_, err = stream.Recv()
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted, got %v", err)
	}
}

func TestChatCompletion_AllProvidersFailedSendsTerminalErrorFrame(t *testing.T) {
	t.Parallel()
	// ErrAllProvidersFailed must arrive IN-BAND as a terminal error frame (finish=ERROR)
	// and the RPC itself completes cleanly (io.EOF after the frame), per the contract.
	svc := &fakeService{streamErr: domain.ErrAllProvidersFailed}
	client := newClient(t, svc, "team-x")

	stream, err := client.ChatCompletion(authCtx(), &aiv1.ChatCompletionRequest{
		Messages: []*aiv1.ChatMessage{{Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("start error: %v", err)
	}
	frame, err := stream.Recv()
	if err != nil {
		t.Fatalf("expected a terminal error frame, got err %v", err)
	}
	if !frame.GetDone() || frame.GetFinishReason() != aiv1.FinishReason_FINISH_REASON_ERROR {
		t.Fatalf("expected done+ERROR frame, got %+v", frame)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected clean EOF after the terminal error frame, got %v", err)
	}
}

func TestChatCompletion_UnauthenticatedWhenNoClaims(t *testing.T) {
	t.Parallel()
	// No auth interceptor installed (team=="") → the handler's defense-in-depth claims
	// check fires → Unauthenticated.
	svc := &fakeService{}
	client := newClient(t, svc, "")

	stream, err := client.ChatCompletion(context.Background(), &aiv1.ChatCompletionRequest{
		Messages: []*aiv1.ChatMessage{{Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("start error: %v", err)
	}
	_, err = stream.Recv()
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", err)
	}
}

func TestGetUsage_ReturnsTeamBudget(t *testing.T) {
	t.Parallel()
	svc := &fakeService{}
	// The breakdown (prompt 150 + completion 100 = total 250) comes from the usage
	// accumulator; budget 1000 / remaining 750 from the budget bucket.
	svc.usage = domain.UsageSummary{
		PromptTokens: 150, CompletionTokens: 100, TotalTokens: 250,
		BudgetTokens: 1000, RemainingTokens: 750,
	}
	client := newClient(t, svc, "team-x")

	resp, err := client.GetUsage(authCtx(), &aiv1.GetUsageRequest{})
	if err != nil {
		t.Fatalf("GetUsage error: %v", err)
	}
	if resp.GetTeam() != "team-x" {
		t.Fatalf("team = %q, want team-x", resp.GetTeam())
	}
	if resp.GetBudgetTokens() != 1000 || resp.GetRemainingTokens() != 750 {
		t.Fatalf("budget=%d remaining=%d, want 1000/750", resp.GetBudgetTokens(), resp.GetRemainingTokens())
	}
	if resp.GetTotal().GetTotalTokens() != 250 {
		t.Fatalf("total tokens = %d, want 250", resp.GetTotal().GetTotalTokens())
	}
	// THE BREAKDOWN FIX: prompt/completion must be the real split, not 0/0.
	if resp.GetTotal().GetPromptTokens() != 150 || resp.GetTotal().GetCompletionTokens() != 100 {
		t.Fatalf("breakdown prompt=%d completion=%d, want 150/100 (the bug fix)",
			resp.GetTotal().GetPromptTokens(), resp.GetTotal().GetCompletionTokens())
	}
}

func TestListProviders_MapsRegistryWithCircuitState(t *testing.T) {
	t.Parallel()
	svc := &fakeService{}
	client := newClient(t, svc, "team-x")

	resp, err := client.ListProviders(authCtx(), &aiv1.ListProvidersRequest{})
	if err != nil {
		t.Fatalf("ListProviders error: %v", err)
	}
	if len(resp.GetProviders()) != 2 {
		t.Fatalf("expected 2 providers, got %d", len(resp.GetProviders()))
	}
	// Order is failover order: ollama first (OPEN), stub second (CLOSED).
	if resp.GetProviders()[0].GetKind() != aiv1.ProviderKind_PROVIDER_KIND_OLLAMA ||
		resp.GetProviders()[0].GetCircuitState() != "OPEN" {
		t.Fatalf("provider[0] = %+v, want ollama/OPEN", resp.GetProviders()[0])
	}
	if resp.GetProviders()[1].GetCircuitState() != "CLOSED" {
		t.Fatalf("provider[1] circuit = %q, want CLOSED", resp.GetProviders()[1].GetCircuitState())
	}
}

func TestPromptRPCs_Unimplemented(t *testing.T) {
	t.Parallel()
	// The four prompt RPCs return Unimplemented (L3 fills them) via the embedded base.
	svc := &fakeService{}
	client := newClient(t, svc, "team-x")

	_, err := client.CreatePrompt(authCtx(), &aiv1.CreatePromptRequest{})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("CreatePrompt code = %v, want Unimplemented", status.Code(err))
	}
	_, err = client.RenderPrompt(authCtx(), &aiv1.RenderPromptRequest{})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("RenderPrompt code = %v, want Unimplemented", status.Code(err))
	}
}
