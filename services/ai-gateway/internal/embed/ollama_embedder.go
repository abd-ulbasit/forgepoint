// ollama_embedder.go — OllamaEmbedder: an HTTP adapter to Ollama's /api/embeddings.
// Implements domain.Embedder (text → dense vector) for the semantic cache.
//
// ============================================================================
// WHAT THIS ADAPTER DOES
// ============================================================================
//
// The semantic cache compares prompt MEANINGS, which requires turning text into a
// vector. Ollama serves small CPU embedding models (e.g. all-minilm, ~384 dims) at
// POST /api/embeddings: {"model":"all-minilm","prompt":"..."} → {"embedding":[...]}.
// This adapter is a thin translation: domain text in, []float32 out. It is the
// embedding twin of OllamaProvider (the chat adapter) and lives one ring out from the
// domain, which only knows the domain.Embedder PORT (it never imports net/http).
//
// ============================================================================
// FAIL-OPEN BY CONTRACT (the cache is an optimization, never a dependency)
// ============================================================================
//
// Embed RETURNS its error (it never panics or blocks) so the use-case can treat a
// cold/down embedder as "cache unavailable" and serve the request uncached — see the
// ChatCompletion cache block's fail-open path. We therefore do NOT do the chat
// adapter's long cold-start retry-with-backoff here: a slow embedder must not add
// latency to the hot path of EVERY request. We give it ONE short, ctx-bounded attempt
// with a tight timeout; if the embedding model isn't up, we fail FAST into the
// fail-open path (serve uncached) rather than stalling the completion to warm a cache.
// This is the right tradeoff: the cache saves cost on a HIT, but a slow embed on every
// MISS would make the cache a net latency LOSS.
//
// CANCELLATION: the request ctx (the gRPC stream's ctx) bounds the call, so a client
// disconnect/shutdown stops the embed immediately.
package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/domain"
)

// DefaultEmbeddingModel is a tiny, fast CPU embedding model. all-minilm (~384 dims)
// is the standard lightweight choice for semantic caching (the same family GPTCache
// defaults to) — small enough to run scaled-to-zero-friendly alongside the chat model.
const DefaultEmbeddingModel = "all-minilm"

// defaultEmbedTimeout bounds a SINGLE embed call. Short on purpose: an embed that
// can't finish quickly should fail-open (serve uncached) rather than add latency to
// the request. The request ctx still takes precedence if it is tighter / cancelled.
const defaultEmbedTimeout = 3 * time.Second

// OllamaEmbedder calls Ollama's /api/embeddings to vectorize text.
type OllamaEmbedder struct {
	baseURL string
	model   string
	client  *http.Client
}

// Option configures the embedder (functional options keep sane defaults).
type Option func(*OllamaEmbedder)

// WithHTTPClient injects a custom *http.Client (tests point one at a httptest.Server).
func WithHTTPClient(c *http.Client) Option {
	return func(e *OllamaEmbedder) { e.client = c }
}

// WithModel overrides the embedding model name.
func WithModel(model string) Option {
	return func(e *OllamaEmbedder) {
		if model != "" {
			e.model = model
		}
	}
}

// NewOllamaEmbedder builds the adapter. baseURL is the Ollama server (same server the
// chat provider uses). A nil/zero client gets a default with the short embed timeout —
// note this timeout caps the WHOLE embed call (unlike the chat client, an embed is a
// single non-streamed request, so a blunt client timeout is correct here).
func NewOllamaEmbedder(baseURL, model string, opts ...Option) *OllamaEmbedder {
	if model == "" {
		model = DefaultEmbeddingModel
	}
	e := &OllamaEmbedder{
		baseURL: baseURL,
		model:   model,
		client:  &http.Client{Timeout: defaultEmbedTimeout},
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Compile-time proof we satisfy the domain port.
var _ domain.Embedder = (*OllamaEmbedder)(nil)

// the Ollama /api/embeddings wire types (only the fields we use).
type embedRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
}

type embedResponse struct {
	Embedding []float32 `json:"embedding"`
}

// Embed vectorizes text via Ollama. A non-nil error means the embedder is unavailable;
// the use-case falls back to an uncached completion (fail-open). It NEVER returns a
// nil error with an empty vector — an empty embedding would make every cosine
// comparison degenerate to 0 (a guaranteed miss) silently, so we treat it as an error.
func (e *OllamaEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	body, err := json.Marshal(embedRequest{Model: e.model, Prompt: text})
	if err != nil {
		return nil, fmt.Errorf("embed: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.baseURL+"/api/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("embed: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embed: request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("embed: backend returned status %d", resp.StatusCode)
	}

	var parsed embedResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("embed: decode response: %w", err)
	}
	if len(parsed.Embedding) == 0 {
		// An empty embedding is unusable (every cosine comparison would be 0). Surface it
		// as an error so the use-case fails open instead of caching against a zero vector.
		return nil, fmt.Errorf("embed: backend returned an empty embedding for model %q", e.model)
	}
	return parsed.Embedding, nil
}
