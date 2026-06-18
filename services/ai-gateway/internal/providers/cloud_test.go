// cloud_test.go — UNIT tests for the CLOUD providers (OpenAI, Anthropic).
//
// NO real network and NO real API key: every streaming test points the provider at an
// httptest.Server that replays a canned SSE body, and the key-gating test only checks
// the registration matrix (presence, not value). These prove the two behaviors the
// task calls out:
//   - KEY-GATING: a cloud provider is registered ONLY when its key is set.
//   - STREAM MAPPING: a sample SSE chunk stream + usage maps to the domain Delta/usage.
package providers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/domain"
)

// --- KEY-GATING -------------------------------------------------------------

func TestCloudProviders_KeyGating(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name         string
		openAIKey    string
		anthropicKey string
		wantKinds    []domain.ProviderKind // in order
	}{
		{"neither key set → no cloud providers", "", "", nil},
		{"only openai key → openai only", "sk-test", "", []domain.ProviderKind{domain.ProviderKindOpenAI}},
		{"only anthropic key → anthropic only", "", "ant-test", []domain.ProviderKind{domain.ProviderKindAnthropic}},
		{"both keys → openai then anthropic", "sk-test", "ant-test", []domain.ProviderKind{domain.ProviderKindOpenAI, domain.ProviderKindAnthropic}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			got := CloudProviders(c.openAIKey, "", c.anthropicKey, "")
			if len(got) != len(c.wantKinds) {
				t.Fatalf("got %d providers, want %d", len(got), len(c.wantKinds))
			}
			for i, p := range got {
				if p.Kind() != c.wantKinds[i] {
					t.Errorf("provider[%d] kind = %v, want %v", i, p.Kind(), c.wantKinds[i])
				}
			}
		})
	}
}

// --- OPENAI STREAM MAPPING ---------------------------------------------------

// openAISSEBody is a canned OpenAI Chat Completions SSE stream: two content chunks, a
// finish chunk, a usage-only chunk (stream_options.include_usage), and the [DONE]
// sentinel.
const openAISSEBody = `data: {"choices":[{"delta":{"content":"Hello"},"finish_reason":null}]}

data: {"choices":[{"delta":{"content":" world"},"finish_reason":null}]}

data: {"choices":[{"delta":{},"finish_reason":"stop"}]}

data: {"choices":[],"usage":{"prompt_tokens":11,"completion_tokens":7,"total_tokens":18}}

data: [DONE]

`

func TestOpenAIProvider_StreamMapping(t *testing.T) {
	t.Parallel()
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(openAISSEBody))
	}))
	defer srv.Close()

	p := NewOpenAIProvider("sk-fake-test-key", WithOpenAIBaseURL(srv.URL))
	text, usage, finish := drainProvider(t, p)

	if text != "Hello world" {
		t.Errorf("streamed text = %q, want %q", text, "Hello world")
	}
	if usage.PromptTokens != 11 || usage.CompletionTokens != 7 {
		t.Errorf("usage = %+v, want prompt=11 completion=7", usage)
	}
	if finish != domain.FinishReasonStop {
		t.Errorf("finish = %v, want Stop", finish)
	}
	// The key was sent as a Bearer token (the only place it's used) — proves we wire
	// the secret into the request, and the test never used a real key.
	if gotAuth != "Bearer sk-fake-test-key" {
		t.Errorf("Authorization header = %q, want Bearer sk-fake-test-key", gotAuth)
	}
}

func TestOpenAIProvider_Non200IsStartError(t *testing.T) {
	t.Parallel()
	// A non-200 (e.g. 401 bad key) is a START error → the failover loop tries the next
	// provider. Chat must return an error and NO channel.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	p := NewOpenAIProvider("sk-bad", WithOpenAIBaseURL(srv.URL))
	ch, err := p.Chat(context.Background(), sampleReq())
	if err == nil {
		t.Fatal("expected a START error on non-200, got nil")
	}
	if ch != nil {
		t.Fatal("expected a nil channel on a START error")
	}
}

// --- ANTHROPIC STREAM MAPPING ------------------------------------------------

// anthropicSSEBody is a canned Anthropic Messages SSE stream: message_start carries
// input_tokens, two content_block_delta carry text, message_delta carries the
// stop_reason + output_tokens, message_stop ends it. The two-event usage split is the
// interesting mapping (input on message_start, output on message_delta).
const anthropicSSEBody = `event: message_start
data: {"type":"message_start","message":{"usage":{"input_tokens":9,"output_tokens":0}}}

event: content_block_delta
data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"Hi"}}

event: content_block_delta
data: {"type":"content_block_delta","delta":{"type":"text_delta","text":" there"}}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":5}}

event: message_stop
data: {"type":"message_stop"}

`

func TestAnthropicProvider_StreamMapping(t *testing.T) {
	t.Parallel()
	var gotKey, gotVersion string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("x-api-key")
		gotVersion = r.Header.Get("anthropic-version")
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(anthropicSSEBody))
	}))
	defer srv.Close()

	p := NewAnthropicProvider("ant-fake-test-key", WithAnthropicBaseURL(srv.URL))
	text, usage, finish := drainProvider(t, p)

	if text != "Hi there" {
		t.Errorf("streamed text = %q, want %q", text, "Hi there")
	}
	// The usage was ACCUMULATED across two events: input from message_start (9),
	// output from message_delta (5).
	if usage.PromptTokens != 9 || usage.CompletionTokens != 5 {
		t.Errorf("usage = %+v, want prompt=9 completion=5", usage)
	}
	if finish != domain.FinishReasonStop {
		t.Errorf("finish = %v, want Stop (end_turn)", finish)
	}
	if gotKey != "ant-fake-test-key" {
		t.Errorf("x-api-key header = %q, want ant-fake-test-key", gotKey)
	}
	if gotVersion != anthropicVersion {
		t.Errorf("anthropic-version header = %q, want %q", gotVersion, anthropicVersion)
	}
}

func TestAnthropicProvider_MaxTokensFinish(t *testing.T) {
	t.Parallel()
	// stop_reason "max_tokens" must map to FinishReasonLength (hit the cap), not Stop.
	body := `event: message_start
data: {"type":"message_start","message":{"usage":{"input_tokens":3,"output_tokens":0}}}

event: content_block_delta
data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"x"}}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"max_tokens"},"usage":{"output_tokens":1}}

event: message_stop
data: {"type":"message_stop"}

`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	p := NewAnthropicProvider("ant-fake", WithAnthropicBaseURL(srv.URL))
	_, _, finish := drainProvider(t, p)
	if finish != domain.FinishReasonLength {
		t.Errorf("finish = %v, want Length (max_tokens)", finish)
	}
}

// --- shared test helpers -----------------------------------------------------

// sampleReq is a minimal domain request with a system + user turn (so the Anthropic
// system-hoisting path is exercised too).
func sampleReq() domain.ChatRequest {
	return domain.ChatRequest{
		Model: "test-model",
		Messages: []domain.Message{
			{Role: domain.RoleSystem, Content: "be brief"},
			{Role: domain.RoleUser, Content: "hello"},
		},
	}
}

// drainProvider runs Chat against sampleReq, ranges the channel to the terminal frame,
// and returns the concatenated text, the terminal usage, and the finish reason. It
// fails the test on a START error or a missing terminal frame.
func drainProvider(t *testing.T, p domain.Provider) (string, domain.TokenUsage, domain.FinishReason) {
	t.Helper()
	ch, err := p.Chat(context.Background(), sampleReq())
	if err != nil {
		t.Fatalf("Chat START error: %v", err)
	}
	var (
		text    string
		usage   domain.TokenUsage
		finish  domain.FinishReason
		sawDone bool
	)
	for d := range ch {
		if d.Done {
			usage = d.Usage
			finish = d.FinishReason
			sawDone = true
			continue
		}
		text += d.Text
	}
	if !sawDone {
		t.Fatal("stream ended without a terminal Done frame")
	}
	return text, usage, finish
}
