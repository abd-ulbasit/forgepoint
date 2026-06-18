package handlers

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	aiv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/ai/v1"
)

// fakeChatStream is a canned server-streaming client for ChatCompletion: it
// yields the queued frames in order, then io.EOF — driving the SSE relay
// deterministically without a real gRPC stream. Mirrors fakeWatchStream.
type fakeChatStream struct {
	grpc.ServerStreamingClient[aiv1.ChatCompletionResponse]
	frames []*aiv1.ChatCompletionResponse
	i      int
}

func (s *fakeChatStream) Recv() (*aiv1.ChatCompletionResponse, error) {
	if s.i >= len(s.frames) {
		return nil, io.EOF
	}
	f := s.frames[s.i]
	s.i++
	return f, nil
}

func (s *fakeChatStream) Context() context.Context     { return context.Background() }
func (s *fakeChatStream) Header() (metadata.MD, error) { return nil, nil }
func (s *fakeChatStream) Trailer() metadata.MD         { return nil }
func (s *fakeChatStream) CloseSend() error             { return nil }
func (s *fakeChatStream) SendMsg(any) error            { return nil }
func (s *fakeChatStream) RecvMsg(any) error            { return io.EOF }

// TestChat_BridgesDeltasAndTerminalDone is the core SSE-bridge test: each
// streamed delta becomes an "event: delta" frame, the terminal done=true frame
// becomes an "event: done" frame carrying usage/servedBy/cacheHit, the content
// type is text/event-stream, and the caller's token is forwarded upstream.
func TestChat_BridgesDeltasAndTerminalDone(t *testing.T) {
	t.Parallel()

	var sawAuthz string
	var sawModel string
	ai := &mockAIGateway{
		chatFn: func(ctx context.Context, in *aiv1.ChatCompletionRequest) (grpc.ServerStreamingClient[aiv1.ChatCompletionResponse], error) {
			sawAuthz = authzFromCtx(ctx)
			sawModel = in.GetModel()
			return &fakeChatStream{frames: []*aiv1.ChatCompletionResponse{
				{Delta: "Hello", ServedBy: aiv1.ProviderKind_PROVIDER_KIND_OLLAMA, RequestId: "r-1"},
				{Delta: " world", ServedBy: aiv1.ProviderKind_PROVIDER_KIND_OLLAMA, RequestId: "r-1"},
				{
					Done:         true,
					FinishReason: aiv1.FinishReason_FINISH_REASON_STOP,
					ServedBy:     aiv1.ProviderKind_PROVIDER_KIND_OLLAMA,
					CacheHit:     false,
					RequestId:    "r-1",
					Usage: &aiv1.TokenUsage{
						PromptTokens:     5,
						CompletionTokens: 2,
						TotalTokens:      7,
						CostMicroUsd:     42,
					},
				},
			}}, nil
		},
	}
	h := NewAIHandler(ai, testLogger(), testSSELimits())

	// POST the prompt as proto-JSON; protojson parses model + messages into the
	// request. The body is small and well-formed so we exercise the happy path.
	body := `{"model":"smollm2:135m","messages":[{"role":"CHAT_ROLE_USER","content":"hi"}]}`
	r := requestWithToken(http.MethodPost, "/api/v1/chat", "chat-tok", strings.NewReader(body))

	// Route through RequireAuth so the token lands in the request context exactly
	// as in production; httptest.ResponseRecorder implements http.Flusher so the
	// SSE Flush path runs.
	w := serveThroughAuth(h.Chat, r)

	if ct := w.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type = %q, want text/event-stream", ct)
	}
	if sawAuthz != "Bearer chat-tok" {
		t.Errorf("chat did not forward token; saw %q", sawAuthz)
	}
	if sawModel != "smollm2:135m" {
		t.Errorf("model not parsed from body; saw %q", sawModel)
	}

	out := w.Body.String()

	// Two delta frames + one done frame = three data frames total.
	if got := strings.Count(out, "data: "); got != 3 {
		t.Errorf("expected 3 data frames, got %d; body=%q", got, out)
	}
	if got := strings.Count(out, "event: delta"); got != 2 {
		t.Errorf("expected 2 delta events, got %d; body=%q", got, out)
	}
	if !strings.Contains(out, "event: done") {
		t.Errorf("expected a terminal 'done' event; body=%q", out)
	}
	// The deltas must carry the incremental text.
	if !strings.Contains(out, "Hello") || !strings.Contains(out, "world") {
		t.Errorf("expected delta text in the stream; body=%q", out)
	}
	// The done frame must carry the usage readout (total tokens + cost). protojson
	// encodes int64 cost as a string and int32 tokens as numbers.
	if !strings.Contains(out, `"totalTokens":7`) {
		t.Errorf("expected totalTokens in the done frame; body=%q", out)
	}
	if !strings.Contains(out, `"costMicroUsd":"42"`) {
		t.Errorf("expected costMicroUsd in the done frame; body=%q", out)
	}
	// served_by rides along so the UI can show the backend.
	if !strings.Contains(out, "PROVIDER_KIND_OLLAMA") {
		t.Errorf("expected servedBy in the stream; body=%q", out)
	}
}

// TestChat_TerminalDoneOnEOF verifies that when the upstream ends with a clean
// io.EOF WITHOUT an explicit done=true frame, the bridge still emits a terminal
// "done" event so the SPA always gets a completion signal.
func TestChat_TerminalDoneOnEOF(t *testing.T) {
	t.Parallel()

	ai := &mockAIGateway{
		chatFn: func(_ context.Context, _ *aiv1.ChatCompletionRequest) (grpc.ServerStreamingClient[aiv1.ChatCompletionResponse], error) {
			return &fakeChatStream{frames: []*aiv1.ChatCompletionResponse{
				{Delta: "only-frame", ServedBy: aiv1.ProviderKind_PROVIDER_KIND_STUB},
			}}, nil
		},
	}
	h := NewAIHandler(ai, testLogger(), testSSELimits())

	r := requestWithToken(http.MethodPost, "/api/v1/chat", "tok", strings.NewReader(`{"model":"x"}`))
	w := serveThroughAuth(h.Chat, r)

	out := w.Body.String()
	if !strings.Contains(out, "event: delta") {
		t.Errorf("expected the delta frame; body=%q", out)
	}
	if !strings.Contains(out, "event: done") {
		t.Errorf("expected a synthesized terminal done event on EOF; body=%q", out)
	}
}

// TestChat_StreamOpenError_MapsHTTPStatus verifies that when the upstream stream
// never opens (e.g. ResourceExhausted = budget exceeded), we return a normal JSON
// error status rather than a half-open SSE — we haven't committed to a 200 yet.
func TestChat_StreamOpenError_MapsHTTPStatus(t *testing.T) {
	t.Parallel()

	ai := &mockAIGateway{
		chatFn: func(_ context.Context, _ *aiv1.ChatCompletionRequest) (grpc.ServerStreamingClient[aiv1.ChatCompletionResponse], error) {
			return nil, status.Error(codes.PermissionDenied, "not allowed")
		},
	}
	h := NewAIHandler(ai, testLogger(), testSSELimits())

	r := requestWithToken(http.MethodPost, "/api/v1/chat", "tok", strings.NewReader(`{"model":"x"}`))
	w := serveThroughAuth(h.Chat, r)

	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body=%s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); strings.HasPrefix(ct, "text/event-stream") {
		t.Errorf("should not have committed to an SSE stream on open error; ct=%q", ct)
	}
}

// TestChat_Providers_ProxiesAndForwardsToken verifies the unary providers proxy
// forwards the token and relays the provider list.
func TestChat_Providers_ProxiesAndForwardsToken(t *testing.T) {
	t.Parallel()

	var sawAuthz string
	ai := &mockAIGateway{
		providersFn: func(ctx context.Context, _ *aiv1.ListProvidersRequest) (*aiv1.ListProvidersResponse, error) {
			sawAuthz = authzFromCtx(ctx)
			return &aiv1.ListProvidersResponse{Providers: []*aiv1.Provider{
				{Kind: aiv1.ProviderKind_PROVIDER_KIND_OLLAMA, Name: "ollama-local", Enabled: true, CircuitState: "CLOSED", Models: []string{"smollm2:135m"}},
			}}, nil
		},
	}
	h := NewAIHandler(ai, testLogger(), testSSELimits())

	r := requestWithToken(http.MethodGet, "/api/v1/ai/providers", "prov-tok", nil)
	w := serveThroughAuth(h.Providers, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	if sawAuthz != "Bearer prov-tok" {
		t.Errorf("providers did not forward token; saw %q", sawAuthz)
	}
	if !strings.Contains(w.Body.String(), "ollama-local") {
		t.Errorf("expected the provider in the body; got %q", w.Body.String())
	}
}

// TestChat_Usage_ProxiesAndForwardsToken verifies the unary usage proxy forwards
// the token and relays the team usage readout.
func TestChat_Usage_ProxiesAndForwardsToken(t *testing.T) {
	t.Parallel()

	var sawAuthz string
	ai := &mockAIGateway{
		usageFn: func(ctx context.Context, _ *aiv1.GetUsageRequest) (*aiv1.GetUsageResponse, error) {
			sawAuthz = authzFromCtx(ctx)
			return &aiv1.GetUsageResponse{
				Team:            "platform",
				Total:           &aiv1.TokenUsage{TotalTokens: 100, CostMicroUsd: 1000},
				BudgetTokens:    10000,
				RemainingTokens: 9900,
			}, nil
		},
	}
	h := NewAIHandler(ai, testLogger(), testSSELimits())

	r := requestWithToken(http.MethodGet, "/api/v1/ai/usage", "usage-tok", nil)
	w := serveThroughAuth(h.Usage, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	if sawAuthz != "Bearer usage-tok" {
		t.Errorf("usage did not forward token; saw %q", sawAuthz)
	}
	if !strings.Contains(w.Body.String(), "platform") {
		t.Errorf("expected the team in the body; got %q", w.Body.String())
	}
}
