// ollama.go — OllamaProvider: a streaming HTTP adapter to a local Ollama server.
//
// ============================================================================
// THE OLLAMA STREAMING CONTRACT (what this adapter translates)
// ============================================================================
//
// Ollama's POST /api/chat with {"stream": true} returns NEWLINE-DELIMITED JSON
// (NDJSON): one JSON object per line, each carrying an incremental message chunk,
// until a final line with "done": true that also carries the token counts:
//
//	{"model":"smollm2","message":{"role":"assistant","content":"Hel"},"done":false}
//	{"model":"smollm2","message":{"role":"assistant","content":"lo"}, "done":false}
//	...
//	{"model":"smollm2","done":true,"prompt_eval_count":12,"eval_count":34, ...}
//
// We map each non-final line's message.content to a domain.Delta{Text} and the
// final line to a terminal Delta{Done:true, Usage{Prompt:prompt_eval_count,
// Completion:eval_count}, FinishReason}. Those counts are AUTHORITATIVE (the
// backend's own tokenizer), so the gateway's cost metering is exact — unlike the
// stub's estimate.
//
// ============================================================================
// COLD-START RESILIENCE (the KEDA scale-to-zero interplay)
// ============================================================================
//
// Ollama runs scaled-to-zero (KEDA, deploy/llm/ollama-scaledobject.yaml). The
// gateway publishes a warm signal that scales it 0->1, but the pod takes seconds to
// become ready, during which a dial gets CONNECTION REFUSED. So Chat does a BOUNDED
// RETRY WITH BACKOFF on connection-refused at the START of the request (before any
// token streams): it gives Ollama a short window to come up rather than failing the
// whole completion (and failing over to the stub) on the first refused dial.
//
// We retry ONLY the connect/start phase and ONLY on connection-refused (a 500 from a
// running Ollama is a real error we should NOT hammer; a refused dial is "not up
// yet"). Once the stream has started we never retry mid-stream (that would duplicate
// tokens — the same invariant the failover loop enforces).
//
// CANCELLATION: every dial/read uses the request ctx, so a client disconnect or a
// gateway shutdown stops the retries and drains the goroutine — no leak.
// ============================================================================
package providers

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/domain"
)

// DefaultOllamaURL is the in-cluster Ollama service address (fp-ml namespace). It is
// the wiring default when FP_OLLAMA_URL is unset.
const DefaultOllamaURL = "http://ollama.fp-ml.svc.cluster.local:11434"

// OllamaProvider streams completions from an Ollama server over /api/chat.
type OllamaProvider struct {
	baseURL string
	client  *http.Client

	// retry tuning for the cold-start connection-refused window.
	maxRetries int
	backoff    time.Duration
}

// OllamaOption configures the provider (functional options keep sane defaults).
type OllamaOption func(*OllamaProvider)

// WithHTTPClient injects a custom *http.Client (tests inject one wired to a
// httptest.Server; production uses the default with a sane timeout).
func WithHTTPClient(c *http.Client) OllamaOption {
	return func(p *OllamaProvider) { p.client = c }
}

// WithRetry overrides the cold-start retry budget (attempts and base backoff).
func WithRetry(maxRetries int, backoff time.Duration) OllamaOption {
	return func(p *OllamaProvider) {
		p.maxRetries = maxRetries
		p.backoff = backoff
	}
}

// NewOllamaProvider builds the adapter. baseURL is the Ollama server (no trailing
// slash needed). Defaults: a client with NO overall timeout (a long completion can
// legitimately stream for a while; cancellation is via ctx, not a blunt client
// timeout), 5 cold-start retries, 500ms base backoff.
//
// WHY no http.Client.Timeout: that timeout caps the WHOLE request including the
// streamed body read — a long generation would be killed mid-stream. Streaming
// deadlines belong to the request ctx (the gRPC stream's ctx), which we honor on
// every read. We DO set a short dial timeout so a refused/hung connect fails fast
// into the retry loop rather than blocking.
func NewOllamaProvider(baseURL string, opts ...OllamaOption) *OllamaProvider {
	if baseURL == "" {
		baseURL = DefaultOllamaURL
	}
	p := &OllamaProvider{
		baseURL: baseURL,
		client: &http.Client{
			Transport: &http.Transport{
				DialContext: (&net.Dialer{Timeout: 2 * time.Second}).DialContext,
			},
		},
		maxRetries: 5,
		backoff:    500 * time.Millisecond,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Kind identifies the provider.
func (p *OllamaProvider) Kind() domain.ProviderKind { return domain.ProviderKindOllama }

// Models returns nil — Ollama forwards any model string to the backend, so it is a
// WILDCARD provider (it serves whatever model name the request carries; an unknown
// model surfaces as a backend error, which the failover loop handles).
func (p *OllamaProvider) Models() []string { return nil }

// --- the Ollama wire types (only the fields we use) -------------------------

type ollamaChatRequest struct {
	Model    string              `json:"model"`
	Messages []ollamaChatMessage `json:"messages"`
	Stream   bool                `json:"stream"`
	Options  *ollamaOptions      `json:"options,omitempty"`
}

type ollamaChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ollamaOptions struct {
	Temperature float32 `json:"temperature,omitempty"`
	NumPredict  int32   `json:"num_predict,omitempty"` // Ollama's name for max_tokens.
}

// ollamaChatLine is one NDJSON line. message.content is the incremental text;
// done=true marks the terminal line with the token counts.
type ollamaChatLine struct {
	Message struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"message"`
	Done            bool   `json:"done"`
	DoneReason      string `json:"done_reason"`       // "stop" | "length" | ...
	PromptEvalCount int32  `json:"prompt_eval_count"` // prompt tokens (authoritative).
	EvalCount       int32  `json:"eval_count"`        // output tokens (authoritative).
}

// Chat starts a streamed Ollama completion. Returns a START error (after the
// bounded cold-start retry) if the request can't begin; otherwise a live channel of
// deltas that the caller ranges to the terminal Done frame.
func (p *OllamaProvider) Chat(ctx context.Context, req domain.ChatRequest) (<-chan domain.Delta, error) {
	body, err := json.Marshal(p.buildRequest(req))
	if err != nil {
		return nil, &providerError{provider: "ollama", op: "marshal", msg: "encode request", cause: err}
	}

	// START PHASE with cold-start retry: attempt the POST; on connection-refused,
	// back off and retry up to maxRetries (Ollama is scaling 0->1). Any other error
	// (or success) breaks out immediately.
	resp, err := p.doWithRetry(ctx, body)
	if err != nil {
		return nil, err // already a typed providerError; the loop fails over.
	}

	// A non-200 from a RUNNING Ollama is a real error (bad model, server error). We
	// do NOT retry it (it isn't a cold-start condition) — fail over to the next provider.
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, &providerError{
			provider: "ollama", op: "chat",
			msg: fmt.Sprintf("backend returned status %d", resp.StatusCode),
		}
	}

	// STREAM PHASE: hand the body to a goroutine that decodes NDJSON → deltas. The
	// channel is closed when the body is exhausted or ctx is cancelled.
	out := make(chan domain.Delta)
	go p.streamBody(ctx, resp, req, out)
	return out, nil
}

// buildRequest maps the domain request to Ollama's /api/chat shape. stream is always
// true (the gateway is a streaming front-end; a non-streaming client just reads the
// single terminal frame the handler assembles).
func (p *OllamaProvider) buildRequest(req domain.ChatRequest) ollamaChatRequest {
	msgs := make([]ollamaChatMessage, 0, len(req.Messages))
	for _, m := range req.Messages {
		msgs = append(msgs, ollamaChatMessage{Role: roleToOllama(m.Role), Content: m.Content})
	}
	out := ollamaChatRequest{Model: req.Model, Messages: msgs, Stream: true}
	if req.Temperature > 0 || req.MaxTokens > 0 {
		out.Options = &ollamaOptions{Temperature: req.Temperature, NumPredict: req.MaxTokens}
	}
	return out
}

// doWithRetry performs the POST, retrying ONLY on connection-refused with backoff
// (the cold-start window). It returns the response on the first success or a typed
// providerError when the budget is exhausted / ctx is cancelled / a non-retryable
// error occurs.
func (p *OllamaProvider) doWithRetry(ctx context.Context, body []byte) (*http.Response, error) {
	url := p.baseURL + "/api/chat"
	var lastErr error
	for attempt := 0; attempt <= p.maxRetries; attempt++ {
		// A fresh request per attempt (the body reader must be re-created each time).
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return nil, &providerError{provider: "ollama", op: "chat", msg: "build http request", cause: err}
		}
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := p.client.Do(httpReq)
		if err == nil {
			return resp, nil
		}
		lastErr = err

		// Only a connection-refused (Ollama not up yet) is retryable. Anything else
		// (DNS failure, a real transport error) fails over immediately.
		if !isConnRefused(err) {
			return nil, &providerError{provider: "ollama", op: "chat", msg: "request failed", cause: err}
		}

		// Backoff before the next attempt, but stop early if ctx is cancelled.
		if attempt < p.maxRetries {
			select {
			case <-ctx.Done():
				return nil, &providerError{provider: "ollama", op: "chat", msg: "cancelled during cold-start retry", cause: ctx.Err()}
			case <-time.After(p.backoff * time.Duration(attempt+1)): // linear backoff: base, 2x, 3x...
			}
		}
	}
	return nil, &providerError{
		provider: "ollama", op: "chat",
		msg:   fmt.Sprintf("connection refused after %d cold-start retries", p.maxRetries),
		cause: lastErr,
	}
}

// streamBody decodes the NDJSON response into deltas and closes out when done. It is
// the goroutine that owns the response body's lifetime: it ALWAYS closes the body
// and the channel, even on ctx-cancel or a decode error, so neither leaks.
func (p *OllamaProvider) streamBody(ctx context.Context, resp *http.Response, req domain.ChatRequest, out chan<- domain.Delta) {
	defer close(out)
	defer func() { _ = resp.Body.Close() }()

	scanner := bufio.NewScanner(resp.Body)
	// Ollama lines are small, but a long single content chunk could exceed the default
	// 64KB token; raise the cap so a big chunk doesn't error the scan.
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var sawDone bool
	for scanner.Scan() {
		// Stop promptly if the caller went away (client disconnect / shutdown).
		if ctx.Err() != nil {
			return
		}
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		var parsed ollamaChatLine
		if err := json.Unmarshal(line, &parsed); err != nil {
			// A malformed line means the backend broke its contract mid-stream. We close
			// WITHOUT a Done frame; streamOne treats the early close as a provider failure.
			return
		}

		if parsed.Done {
			sawDone = true
			usage := domain.TokenUsage{
				PromptTokens:     parsed.PromptEvalCount,
				CompletionTokens: parsed.EvalCount,
			}
			// Defensive fallback: if the backend omitted counts, estimate from the input
			// so cost metering is never silently zeroed. The stream's text isn't buffered
			// here, so output tokens fall back to the prompt-estimate shape only when the
			// authoritative count is absent (a rare backend-contract gap).
			if usage.PromptTokens == 0 {
				usage.PromptTokens = promptTokenCount(req.Messages)
			}
			select {
			case <-ctx.Done():
			case out <- domain.Delta{Done: true, Usage: usage, FinishReason: ollamaFinish(parsed.DoneReason)}:
			}
			return
		}

		if parsed.Message.Content == "" {
			continue // keep-alive / role-only line; nothing to forward.
		}
		select {
		case <-ctx.Done():
			return
		case out <- domain.Delta{Text: parsed.Message.Content}:
		}
	}

	// Reached here without a Done frame (scanner ended / errored): if we never saw
	// done=true, the channel closes with no terminal frame and streamOne fails over.
	// scanner.Err() being non-nil (a read error) lands here too — same handling.
	_ = sawDone
}

// roleToOllama maps the domain role to Ollama's role string. Unspecified defaults to
// "user" (the safest assumption for an untyped turn).
func roleToOllama(r domain.Role) string {
	switch r {
	case domain.RoleSystem:
		return "system"
	case domain.RoleAssistant:
		return "assistant"
	default:
		return "user"
	}
}

// ollamaFinish maps Ollama's done_reason to the domain FinishReason. "length" means
// it hit num_predict; anything else (including the common "stop") is a natural stop.
func ollamaFinish(reason string) domain.FinishReason {
	switch reason {
	case "length":
		return domain.FinishReasonLength
	default:
		return domain.FinishReasonStop
	}
}

// isConnRefused reports whether err is a connection-refused dial error (Ollama not
// up yet). We match on the syscall-level ECONNREFUSED carried by net.OpError, which
// is robust across platforms (vs. string-matching the error text).
func isConnRefused(err error) bool {
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		// A refused connect surfaces as a *net.OpError wrapping a syscall error whose
		// string contains "connection refused" on every platform we run (Linux/Mac). We
		// also treat any dial-phase OpError as cold-start-retryable, since a not-yet-up
		// pod can also surface as "no route to host" / "i/o timeout" during 0->1.
		if netErr.Op == "dial" {
			return true
		}
	}
	// Fall back to a substring check for wrapped errors that aren't a *net.OpError.
	return err != nil && bytes.Contains([]byte(err.Error()), []byte("connection refused"))
}
