// openai.go — OpenAIProvider: a streaming HTTP adapter to the OpenAI Chat API.
//
// ============================================================================
// THE OPENAI STREAMING CONTRACT (what this adapter translates)
// ============================================================================
//
// POST /v1/chat/completions with {"stream": true} returns Server-Sent Events
// (SSE): lines of the form `data: {json}\n`, a blank line between events, and a
// final sentinel line `data: [DONE]`. Each JSON event is a chunk:
//
//	data: {"choices":[{"delta":{"content":"Hel"},"finish_reason":null}]}
//	data: {"choices":[{"delta":{"content":"lo"}, "finish_reason":null}]}
//	...
//	data: {"choices":[{"delta":{},"finish_reason":"stop"}],
//	       "usage":{"prompt_tokens":12,"completion_tokens":34,"total_tokens":46}}
//	data: [DONE]
//
// We map each chunk's choices[0].delta.content to a domain.Delta{Text}, and the
// final usage block (requested via stream_options.include_usage=true) to the
// terminal Delta{Done:true, Usage{Prompt:prompt_tokens, Completion:completion_tokens},
// FinishReason}. Those counts are AUTHORITATIVE (OpenAI's own tokenizer), so cost
// metering is exact — same guarantee as Ollama's eval counts.
//
// ============================================================================
// WHY THIS LOOKS LIKE ollama.go (and why that is the point)
// ============================================================================
//
// This adapter implements the SAME domain.Provider streaming contract as Ollama and
// the stub: Chat returns (<-chan Delta, error); a START error means "didn't begin,
// fail over to the next provider"; an in-band terminal error frame means "began then
// failed". So the failover loop and the per-provider CIRCUIT BREAKER treat OpenAI
// exactly like any other backend — the breaker key is its Kind(), and a flaky OpenAI
// trips OPEN and is skipped just like a cold Ollama. The ONLY differences from Ollama
// are the wire format (SSE vs NDJSON) and the Authorization header (the API key).
//
// ============================================================================
// SECRETS — the API key is read from config (a K8s Secret), NEVER logged
// ============================================================================
//
// The key is injected via the struct (from FP_OPENAI_API_KEY, sourced from a K8s
// Secret) and used ONLY as the Bearer token on the request. It is never put in a log
// line, an error message, the audit record, or an event — the same discipline the
// rest of the platform applies to JWT secrets and DSN passwords. The provider is
// REGISTERED ONLY when the key is non-empty (see NewOpenAIProvider's caller in
// main.go): absent key ⇒ provider not wired ⇒ failover order stays Ollama+Stub.
//
// COLD-START / CANCELLATION: unlike Ollama (scaled-to-zero, needs cold-start retry),
// OpenAI is always-on, so there is NO connection-refused retry loop — a transport
// error is a real failure the loop fails over from. Every dial/read uses the request
// ctx, so a client disconnect or gateway shutdown stops the stream and drains the
// goroutine (no leak). No http.Client.Timeout for the same reason as Ollama: a long
// completion legitimately streams for a while; cancellation is via ctx.
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

// DefaultOpenAIBaseURL is the public OpenAI API root. Overridable (WithOpenAIBaseURL)
// for an Azure OpenAI endpoint or a local-compatible gateway, and for tests (which
// point it at an httptest.Server).
const DefaultOpenAIBaseURL = "https://api.openai.com"

// OpenAIProvider streams completions from the OpenAI Chat Completions API.
type OpenAIProvider struct {
	baseURL string
	apiKey  string // the Bearer token (from a Secret). NEVER logged.
	client  *http.Client
}

// OpenAIOption configures the provider (functional options, sane defaults).
type OpenAIOption func(*OpenAIProvider)

// WithOpenAIHTTPClient injects a custom *http.Client. Tests inject one wired to an
// httptest.Server (so no real network and no real key is ever used).
func WithOpenAIHTTPClient(c *http.Client) OpenAIOption {
	return func(p *OpenAIProvider) { p.client = c }
}

// WithOpenAIBaseURL overrides the API root (Azure OpenAI / a compatible proxy / a
// test server). Empty is ignored (keeps the default).
func WithOpenAIBaseURL(u string) OpenAIOption {
	return func(p *OpenAIProvider) {
		if u != "" {
			p.baseURL = u
		}
	}
}

// NewOpenAIProvider builds the adapter. apiKey is the Bearer token (from a Secret).
// The default client has a short DIAL timeout (fail fast into failover on an
// unreachable endpoint) but NO overall timeout (a long stream is legitimate;
// cancellation is via ctx). See the file header for why.
//
// PRECONDITION (enforced by the caller, not here): main.go only constructs and
// registers this provider when the key is non-empty — key-gating. We keep the
// constructor key-agnostic so a test can build it with a fake key + a fake transport.
func NewOpenAIProvider(apiKey string, opts ...OpenAIOption) *OpenAIProvider {
	p := &OpenAIProvider{
		baseURL: DefaultOpenAIBaseURL,
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

// Kind identifies the provider (the breaker key + the routing/event label).
func (p *OpenAIProvider) Kind() domain.ProviderKind { return domain.ProviderKindOpenAI }

// Models returns nil — a WILDCARD provider. We forward whatever model string the
// request carries (e.g. "gpt-4o-mini") to OpenAI; an unknown model surfaces as a
// backend error the failover loop handles. The MODEL ALLOW-LIST (allowlist.go), not
// this adapter, is what restricts which model names may be requested.
func (p *OpenAIProvider) Models() []string { return nil }

// --- the OpenAI wire types (only the fields we use) -------------------------

type openAIChatRequest struct {
	Model         string            `json:"model"`
	Messages      []openAIChatMsg   `json:"messages"`
	Stream        bool              `json:"stream"`
	StreamOptions *openAIStreamOpts `json:"stream_options,omitempty"`
	Temperature   *float32          `json:"temperature,omitempty"`
	MaxTokens     *int32            `json:"max_tokens,omitempty"`
}

type openAIChatMsg struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// openAIStreamOpts.include_usage asks OpenAI to emit a final usage block on the last
// streamed chunk — WITHOUT it a streamed response carries NO token counts and cost
// metering would silently fall back to the estimator. We always request it.
type openAIStreamOpts struct {
	IncludeUsage bool `json:"include_usage"`
}

// openAIStreamChunk is one SSE `data:` event. choices may be empty on the final
// usage-only chunk; usage is present only on that final chunk (include_usage).
type openAIStreamChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
		FinishReason string `json:"finish_reason"` // "stop" | "length" | "content_filter" | "" (mid-stream)
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int32 `json:"prompt_tokens"`
		CompletionTokens int32 `json:"completion_tokens"`
		TotalTokens      int32 `json:"total_tokens"`
	} `json:"usage"`
}

// Chat starts a streamed OpenAI completion. Returns a START error (no token streamed
// yet → the loop fails over) on a build/transport/non-200 failure; otherwise a live
// channel of deltas the caller ranges to the terminal Done frame.
func (p *OpenAIProvider) Chat(ctx context.Context, req domain.ChatRequest) (<-chan domain.Delta, error) {
	body, err := json.Marshal(p.buildRequest(req))
	if err != nil {
		return nil, &providerError{provider: "openai", op: "marshal", msg: "encode request", cause: err}
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, &providerError{provider: "openai", op: "chat", msg: "build http request", cause: err}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")
	// The API key as a Bearer token. This is the ONLY place the key is used; it is
	// never logged or surfaced in an error (a providerError carries provider/op/msg,
	// never the header).
	httpReq.Header.Set("Authorization", "Bearer "+p.apiKey)

	resp, err := p.client.Do(httpReq)
	if err != nil {
		// A transport error (DNS, refused, TLS) is a real failure: fail over. NO
		// cold-start retry (OpenAI is always-on, unlike scaled-to-zero Ollama).
		return nil, &providerError{provider: "openai", op: "chat", msg: "request failed", cause: err}
	}

	// A non-200 from OpenAI is a real error (bad key → 401, rate limit → 429, bad
	// model → 404). We do NOT echo the body (it can carry detail we don't want in
	// logs); the status code is enough for the loop to fail over. We DO close the body.
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, &providerError{
			provider: "openai", op: "chat",
			msg: fmt.Sprintf("backend returned status %d", resp.StatusCode),
		}
	}

	out := make(chan domain.Delta)
	go p.streamBody(ctx, resp, req, out)
	return out, nil
}

// buildRequest maps the domain request to the OpenAI shape. stream is always true
// (the gateway is a streaming front-end) and we always include_usage so the terminal
// chunk carries authoritative token counts.
func (p *OpenAIProvider) buildRequest(req domain.ChatRequest) openAIChatRequest {
	msgs := make([]openAIChatMsg, 0, len(req.Messages))
	for _, m := range req.Messages {
		msgs = append(msgs, openAIChatMsg{Role: roleToOpenAI(m.Role), Content: m.Content})
	}
	out := openAIChatRequest{
		Model:         req.Model,
		Messages:      msgs,
		Stream:        true,
		StreamOptions: &openAIStreamOpts{IncludeUsage: true},
	}
	// Pointers so a zero value is OMITTED (let OpenAI apply its own default) rather
	// than sent as 0 (which would pin temperature=0 / max_tokens=0 unintentionally).
	if req.Temperature > 0 {
		t := req.Temperature
		out.Temperature = &t
	}
	if req.MaxTokens > 0 {
		mt := req.MaxTokens
		out.MaxTokens = &mt
	}
	return out
}

// streamBody decodes the SSE response into deltas and closes out when done. It OWNS
// the response body's lifetime: it ALWAYS closes the body and the channel, even on
// ctx-cancel or a decode error — neither leaks.
//
// SSE PARSING: we scan line by line. A content line is `data: {json}`. The sentinel
// `data: [DONE]` ends the stream. Blank lines (event separators) and any non-`data:`
// lines (comments, `event:`) are ignored. The final pre-[DONE] chunk carries the
// usage block (include_usage); we stash the latest usage + finish_reason and emit the
// terminal frame when we hit [DONE] (or the body ends).
func (p *OpenAIProvider) streamBody(ctx context.Context, resp *http.Response, req domain.ChatRequest, out chan<- domain.Delta) {
	defer close(out)
	defer func() { _ = resp.Body.Close() }()

	scanner := bufio.NewScanner(resp.Body)
	// A single SSE chunk can be larger than the default 64KB token (a long content
	// burst); raise the cap so a big chunk doesn't error the scan.
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	forwarded := false
	var (
		finalUsage   domain.TokenUsage
		finishReason = domain.FinishReasonStop
		sawUsage     bool
	)

	for scanner.Scan() {
		if ctx.Err() != nil {
			return // caller went away; stop promptly (the deferred closes run).
		}
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue // SSE event separator.
		}
		// Only `data:` lines carry payload. Ignore comments (`:`), `event:`, etc.
		data, ok := bytes.CutPrefix(line, []byte("data:"))
		if !ok {
			continue
		}
		data = bytes.TrimSpace(data)

		// The terminal sentinel: the stream is complete. Emit the terminal frame from
		// the usage/finish we accumulated.
		if bytes.Equal(data, []byte("[DONE]")) {
			break
		}

		var chunk openAIStreamChunk
		if err := json.Unmarshal(data, &chunk); err != nil {
			// A malformed chunk means OpenAI broke its contract mid-stream. Close WITHOUT
			// a Done frame; streamOne treats the early close as a provider failure (and,
			// if tokens were already forwarded, as a terminal mid-stream abort).
			return
		}

		// Capture the authoritative usage from the final usage-bearing chunk.
		if chunk.Usage != nil {
			finalUsage = domain.TokenUsage{
				PromptTokens:     chunk.Usage.PromptTokens,
				CompletionTokens: chunk.Usage.CompletionTokens,
			}
			sawUsage = true
		}

		if len(chunk.Choices) > 0 {
			c := chunk.Choices[0]
			if c.FinishReason != "" {
				finishReason = openAIFinish(c.FinishReason)
			}
			if c.Delta.Content != "" {
				select {
				case <-ctx.Done():
					return
				case out <- domain.Delta{Text: c.Delta.Content}:
				}
				forwarded = true
			}
		}
	}

	// Defensive: if OpenAI omitted the usage block (e.g. a proxy stripped
	// stream_options), estimate the prompt tokens from the input so cost metering is
	// never silently zeroed — the same fallback Ollama uses.
	if !sawUsage && finalUsage.PromptTokens == 0 {
		finalUsage.PromptTokens = promptTokenCount(req.Messages)
	}

	// Emit the terminal frame. (If we returned early on a decode error / cancel above,
	// we never reach here and the channel closes with no Done frame — the failover-or-
	// abort path in streamOne, identical to Ollama.)
	_ = forwarded
	select {
	case <-ctx.Done():
	case out <- domain.Delta{Done: true, Usage: finalUsage, FinishReason: finishReason}:
	}
}

// roleToOpenAI maps the domain role to OpenAI's role string. Unspecified → "user".
func roleToOpenAI(r domain.Role) string {
	switch r {
	case domain.RoleSystem:
		return "system"
	case domain.RoleAssistant:
		return "assistant"
	default:
		return "user"
	}
}

// openAIFinish maps OpenAI's finish_reason to the domain FinishReason. "length" =
// hit max_tokens; "content_filter" = guardrail; anything else (incl. "stop") =
// natural stop.
func openAIFinish(reason string) domain.FinishReason {
	switch reason {
	case "length":
		return domain.FinishReasonLength
	case "content_filter":
		return domain.FinishReasonContentFilter
	default:
		return domain.FinishReasonStop
	}
}
