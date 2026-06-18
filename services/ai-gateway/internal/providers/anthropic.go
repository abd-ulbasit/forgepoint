// anthropic.go — AnthropicProvider: a streaming HTTP adapter to the Anthropic
// Messages API (Claude).
//
// ============================================================================
// THE ANTHROPIC STREAMING CONTRACT (what this adapter translates)
// ============================================================================
//
// POST /v1/messages with {"stream": true} returns Server-Sent Events (SSE), but with
// a richer, TYPED event model than OpenAI: each event has an `event:` line naming its
// type and a `data:` line with the JSON. The types we care about:
//
//	event: message_start
//	data: {"type":"message_start","message":{"usage":{"input_tokens":12,...}}}
//
//	event: content_block_delta
//	data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"Hel"}}
//	... (more content_block_delta) ...
//
//	event: message_delta
//	data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},
//	       "usage":{"output_tokens":34}}
//
//	event: message_stop
//	data: {"type":"message_stop"}
//
// THE TOKEN-ACCOUNTING SUBTLETY (interview-worthy): Anthropic splits usage across two
// events. INPUT tokens arrive on `message_start` (message.usage.input_tokens); OUTPUT
// tokens arrive on `message_delta` (usage.output_tokens) near the end. So unlike
// OpenAI (one final usage block) we must ACCUMULATE: stash input_tokens from
// message_start, output_tokens from message_delta, and combine them into the terminal
// frame. Both are AUTHORITATIVE (Claude's tokenizer), so cost metering is exact.
//
// We map each content_block_delta's text to a domain.Delta{Text} and message_stop (or
// the body ending) to the terminal Delta{Done:true, Usage{Prompt:input, Completion:
// output}, FinishReason(stop_reason)}.
//
// ============================================================================
// SAME PORT, SAME FAILOVER + BREAKER, SECRET-SAFE KEY (as openai.go)
// ============================================================================
//
// Identical Provider contract to Ollama/Stub/OpenAI: a START error means "fail over",
// an in-band terminal error means "began then failed". The per-provider CIRCUIT
// BREAKER keys on Kind() (anthropic), so a flaky Claude trips OPEN and is skipped like
// any other backend. The API key (FP_ANTHROPIC_API_KEY, from a K8s Secret) is sent
// ONLY as the x-api-key header — never logged, never in an error/event/audit. The
// provider is REGISTERED ONLY when the key is set (main.go); absent ⇒ not wired ⇒ the
// failover order stays Ollama+Stub. No cold-start retry (Claude is always-on);
// cancellation is via the request ctx on every read.
package providers

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/domain"
)

// DefaultAnthropicBaseURL is the public Anthropic API root. Overridable for a proxy
// and for tests (which point it at an httptest.Server, so no real network/key).
const DefaultAnthropicBaseURL = "https://api.anthropic.com"

// anthropicVersion is the required API version header value. Pinned: Anthropic gates
// breaking changes behind this header, so a fixed value keeps the wire shape stable.
const anthropicVersion = "2023-06-01"

// defaultAnthropicMaxTokens is sent when the request doesn't set MaxTokens. The
// Anthropic Messages API REQUIRES max_tokens (unlike OpenAI, where it's optional), so
// we must always send one — this is a sane cap that a request can raise.
const defaultAnthropicMaxTokens = 1024

// AnthropicProvider streams completions from the Anthropic Messages API.
type AnthropicProvider struct {
	baseURL string
	apiKey  string // the x-api-key value (from a Secret). NEVER logged.
	client  *http.Client
}

// AnthropicOption configures the provider (functional options, sane defaults).
type AnthropicOption func(*AnthropicProvider)

// WithAnthropicHTTPClient injects a custom *http.Client (tests wire one to an
// httptest.Server — no real network, no real key).
func WithAnthropicHTTPClient(c *http.Client) AnthropicOption {
	return func(p *AnthropicProvider) { p.client = c }
}

// WithAnthropicBaseURL overrides the API root (a proxy / a test server). Empty is
// ignored (keeps the default).
func WithAnthropicBaseURL(u string) AnthropicOption {
	return func(p *AnthropicProvider) {
		if u != "" {
			p.baseURL = u
		}
	}
}

// NewAnthropicProvider builds the adapter. apiKey is the x-api-key (from a Secret).
// Short dial timeout (fail fast into failover), no overall timeout (streaming;
// cancellation via ctx). Key-gating is the CALLER's job (main.go registers this only
// when the key is non-empty); the constructor stays key-agnostic so a test can build
// it with a fake key + fake transport.
func NewAnthropicProvider(apiKey string, opts ...AnthropicOption) *AnthropicProvider {
	p := &AnthropicProvider{
		baseURL: DefaultAnthropicBaseURL,
		apiKey:  apiKey,
		client: &http.Client{
			Transport: &http.Transport{
				DialContext: defaultDialContext(),
			},
		},
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Kind identifies the provider (breaker key + routing/event label).
func (p *AnthropicProvider) Kind() domain.ProviderKind { return domain.ProviderKindAnthropic }

// Models returns nil — a WILDCARD provider. We forward whatever model string the
// request carries (e.g. "claude-3-5-haiku-latest"); an unknown model surfaces as a
// backend error the loop handles. The MODEL ALLOW-LIST governs which names are
// requestable, not this adapter.
func (p *AnthropicProvider) Models() []string { return nil }

// --- the Anthropic wire types (only the fields we use) ----------------------

type anthropicRequest struct {
	Model       string             `json:"model"`
	Messages    []anthropicMessage `json:"messages"`
	System      string             `json:"system,omitempty"` // Anthropic carries the system prompt OUT of messages.
	Stream      bool               `json:"stream"`
	MaxTokens   int32              `json:"max_tokens"` // REQUIRED by the API.
	Temperature *float32           `json:"temperature,omitempty"`
}

type anthropicMessage struct {
	Role    string `json:"role"` // only "user" | "assistant" (system goes in the top-level field).
	Content string `json:"content"`
}

// anthropicStreamEvent is the union of the SSE event shapes we read. The `type`
// discriminates; the other fields are populated per type (Go leaves the rest zero).
type anthropicStreamEvent struct {
	Type string `json:"type"`
	// message_start: the input token count rides on message.usage.input_tokens.
	Message *struct {
		Usage *anthropicUsage `json:"usage"`
	} `json:"message"`
	// content_block_delta: delta.text is the incremental token; message_delta:
	// delta.stop_reason is the finish reason.
	Delta *struct {
		Text       string `json:"text"`
		StopReason string `json:"stop_reason"`
	} `json:"delta"`
	// message_delta: the output token count rides on the top-level usage.output_tokens.
	Usage *anthropicUsage `json:"usage"`
}

type anthropicUsage struct {
	InputTokens  int32 `json:"input_tokens"`
	OutputTokens int32 `json:"output_tokens"`
}

// Chat starts a streamed Anthropic completion. Returns a START error (no token yet →
// fail over) on a build/transport/non-200 failure; otherwise a live channel of deltas.
func (p *AnthropicProvider) Chat(ctx context.Context, req domain.ChatRequest) (<-chan domain.Delta, error) {
	body, err := json.Marshal(p.buildRequest(req))
	if err != nil {
		return nil, &providerError{provider: "anthropic", op: "marshal", msg: "encode request", cause: err}
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/v1/messages", bytes.NewReader(body))
	if err != nil {
		return nil, &providerError{provider: "anthropic", op: "chat", msg: "build http request", cause: err}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")
	httpReq.Header.Set("anthropic-version", anthropicVersion)
	// The API key as x-api-key (Anthropic's scheme, NOT Bearer). The ONLY use of the
	// key; never logged or surfaced in an error.
	httpReq.Header.Set("x-api-key", p.apiKey)

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, &providerError{provider: "anthropic", op: "chat", msg: "request failed", cause: err}
	}

	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, &providerError{
			provider: "anthropic", op: "chat",
			msg: fmt.Sprintf("backend returned status %d", resp.StatusCode),
		}
	}

	out := make(chan domain.Delta)
	go p.streamBody(ctx, resp, req, out)
	return out, nil
}

// buildRequest maps the domain request to the Anthropic Messages shape. The SYSTEM
// turn(s) are pulled OUT into the top-level `system` field (Anthropic's API does not
// accept a system role inside messages); user/assistant turns stay in messages.
// max_tokens is always set (required by the API). stream is always true.
func (p *AnthropicProvider) buildRequest(req domain.ChatRequest) anthropicRequest {
	var system string
	msgs := make([]anthropicMessage, 0, len(req.Messages))
	for _, m := range req.Messages {
		if m.Role == domain.RoleSystem {
			// Concatenate multiple system turns (rare) with a newline; Anthropic takes a
			// single system string.
			if system != "" {
				system += "\n"
			}
			system += m.Content
			continue
		}
		msgs = append(msgs, anthropicMessage{Role: roleToAnthropic(m.Role), Content: m.Content})
	}

	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = defaultAnthropicMaxTokens
	}
	out := anthropicRequest{
		Model:     req.Model,
		Messages:  msgs,
		System:    system,
		Stream:    true,
		MaxTokens: maxTokens,
	}
	if req.Temperature > 0 {
		t := req.Temperature
		out.Temperature = &t
	}
	return out
}

// streamBody decodes the Anthropic SSE stream into deltas and closes out when done.
// It OWNS the body's lifetime (always closes body + channel).
//
// ACCUMULATION (the two-event usage split): input_tokens come on message_start,
// output_tokens on message_delta. We stash both as they arrive and emit ONE terminal
// frame combining them at message_stop (or body end). We only read `data:` lines and
// dispatch on the parsed event's `type` (the `event:` line is redundant with it).
func (p *AnthropicProvider) streamBody(ctx context.Context, resp *http.Response, req domain.ChatRequest, out chan<- domain.Delta) {
	defer close(out)
	defer func() { _ = resp.Body.Close() }()

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var (
		usage        domain.TokenUsage
		finishReason = domain.FinishReasonStop
	)

	for scanner.Scan() {
		if ctx.Err() != nil {
			return
		}
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		data, ok := bytes.CutPrefix(line, []byte("data:"))
		if !ok {
			continue // skip `event:` lines and comments — `type` in the JSON is authoritative.
		}
		data = bytes.TrimSpace(data)

		var ev anthropicStreamEvent
		if err := json.Unmarshal(data, &ev); err != nil {
			// Malformed event: backend broke contract mid-stream. Close WITHOUT a Done
			// frame; streamOne treats the early close as a failure / mid-stream abort.
			return
		}

		switch ev.Type {
		case "message_start":
			if ev.Message != nil && ev.Message.Usage != nil {
				usage.PromptTokens = ev.Message.Usage.InputTokens
			}
		case "content_block_delta":
			if ev.Delta != nil && ev.Delta.Text != "" {
				select {
				case <-ctx.Done():
					return
				case out <- domain.Delta{Text: ev.Delta.Text}:
				}
			}
		case "message_delta":
			if ev.Delta != nil && ev.Delta.StopReason != "" {
				finishReason = anthropicFinish(ev.Delta.StopReason)
			}
			if ev.Usage != nil {
				usage.CompletionTokens = ev.Usage.OutputTokens
			}
		case "message_stop":
			// Terminal: emit the combined-usage Done frame and finish.
			if usage.PromptTokens == 0 {
				usage.PromptTokens = promptTokenCount(req.Messages) // defensive fallback.
			}
			select {
			case <-ctx.Done():
			case out <- domain.Delta{Done: true, Usage: usage, FinishReason: finishReason}:
			}
			return
		}
	}

	// Body ended without an explicit message_stop (e.g. a proxy truncated it). If we
	// have any usage, still emit a terminal frame so a clean completion isn't dropped;
	// otherwise the channel closes with no Done frame and streamOne handles it.
	if usage.PromptTokens == 0 {
		usage.PromptTokens = promptTokenCount(req.Messages)
	}
	select {
	case <-ctx.Done():
	case out <- domain.Delta{Done: true, Usage: usage, FinishReason: finishReason}:
	}
}

// roleToAnthropic maps the domain role to Anthropic's message role. Only user /
// assistant are valid INSIDE messages (system is hoisted to the top-level field in
// buildRequest), so anything non-assistant maps to "user".
func roleToAnthropic(r domain.Role) string {
	if r == domain.RoleAssistant {
		return "assistant"
	}
	return "user"
}

// anthropicFinish maps Anthropic's stop_reason to the domain FinishReason.
// "max_tokens" = hit the cap; anything else (incl. "end_turn", "stop_sequence") =
// natural stop.
func anthropicFinish(reason string) domain.FinishReason {
	switch reason {
	case "max_tokens":
		return domain.FinishReasonLength
	default:
		return domain.FinishReasonStop
	}
}
