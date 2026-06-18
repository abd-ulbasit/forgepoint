// ollama_test.go — UNIT tests for the OllamaProvider NDJSON streaming + cold-start
// retry. Uses httptest (in-process HTTP), NO real Ollama, NO testcontainers.
package providers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/domain"
)

// ndjsonHandler returns an http.Handler that streams the given NDJSON lines as an
// Ollama /api/chat response would.
func ndjsonHandler(lines []string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for _, line := range lines {
			_, _ = w.Write([]byte(line + "\n"))
			if flusher != nil {
				flusher.Flush()
			}
		}
	}
}

func TestOllama_StreamsNDJSONAndAuthoritativeCounts(t *testing.T) {
	t.Parallel()
	lines := []string{
		`{"model":"smollm2","message":{"role":"assistant","content":"Hel"},"done":false}`,
		`{"model":"smollm2","message":{"role":"assistant","content":"lo"},"done":false}`,
		`{"model":"smollm2","message":{"role":"assistant","content":"!"},"done":false}`,
		`{"model":"smollm2","done":true,"done_reason":"stop","prompt_eval_count":11,"eval_count":23}`,
	}
	srv := httptest.NewServer(ndjsonHandler(lines))
	defer srv.Close()

	p := NewOllamaProvider(srv.URL, WithHTTPClient(srv.Client()))
	ch, err := p.Chat(context.Background(), domain.ChatRequest{
		Model:    "smollm2",
		Messages: []domain.Message{{Role: domain.RoleUser, Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("unexpected start error: %v", err)
	}

	text, usage, finish, sawDone := drain(t, ch)
	if !sawDone {
		t.Fatalf("stream ended without a Done frame")
	}
	if text != "Hello!" {
		t.Fatalf("reassembled text = %q, want %q", text, "Hello!")
	}
	// The counts are the BACKEND's authoritative values, used verbatim.
	if usage.PromptTokens != 11 || usage.CompletionTokens != 23 {
		t.Fatalf("usage = %+v, want prompt=11 completion=23", usage)
	}
	if finish != domain.FinishReasonStop {
		t.Fatalf("finish = %v, want Stop", finish)
	}
}

func TestOllama_LengthFinishReasonMapped(t *testing.T) {
	t.Parallel()
	lines := []string{
		`{"message":{"content":"x"},"done":false}`,
		`{"done":true,"done_reason":"length","prompt_eval_count":3,"eval_count":1}`,
	}
	srv := httptest.NewServer(ndjsonHandler(lines))
	defer srv.Close()

	p := NewOllamaProvider(srv.URL, WithHTTPClient(srv.Client()))
	ch, _ := p.Chat(context.Background(), domain.ChatRequest{Messages: []domain.Message{{Content: "x"}}})
	_, _, finish, _ := drain(t, ch)
	if finish != domain.FinishReasonLength {
		t.Fatalf("finish = %v, want Length", finish)
	}
}

func TestOllama_Non200IsStartError(t *testing.T) {
	t.Parallel()
	// A RUNNING Ollama returning 500 is a real (non-retryable) error → start error so
	// the failover loop tries the next provider.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	p := NewOllamaProvider(srv.URL, WithHTTPClient(srv.Client()))
	ch, err := p.Chat(context.Background(), domain.ChatRequest{Messages: []domain.Message{{Content: "x"}}})
	if err == nil {
		t.Fatalf("expected a start error on HTTP 500")
	}
	if ch != nil {
		t.Fatalf("expected nil channel on start error")
	}
	if !strings.Contains(err.Error(), "status 500") {
		t.Fatalf("error should mention the status, got %v", err)
	}
}

func TestOllama_ColdStartRetrySucceedsAfterRefusals(t *testing.T) {
	t.Parallel()
	// Simulate Ollama scaling 0->1: the first N dials are refused, then it comes up.
	// We model "refused" by pointing at a CLOSED port for the first attempts via a
	// handler that the test server only starts answering after a few tries. Simpler:
	// use a counting handler on a live server, but to exercise the connection-refused
	// path we instead start with a server that is closed, then... that's hard to time.
	//
	// Practical deterministic approach: a live server whose handler FAILS the first
	// two requests by hijacking+closing the connection (which surfaces to the client as
	// a transport error containing "connection refused"-class behavior is not
	// guaranteed). Since reliably forcing ECONNREFUSED in-process is flaky, we instead
	// verify the retry BUDGET is honored against a truly-dead address and that it gives
	// up with the cold-start error after the configured attempts (the negative path),
	// and separately verify the happy path above. This keeps the test deterministic.
	dead := "http://127.0.0.1:1" // port 1 is reserved/closed → connection refused.
	var attempts int32
	p := NewOllamaProvider(dead,
		WithRetry(2, 1*time.Millisecond),
		WithHTTPClient(&http.Client{
			Transport: roundTripCounter{n: &attempts, rt: http.DefaultTransport},
		}),
	)
	_, err := p.Chat(context.Background(), domain.ChatRequest{Messages: []domain.Message{{Content: "x"}}})
	if err == nil {
		t.Fatalf("expected a cold-start exhaustion error against a dead address")
	}
	if !strings.Contains(err.Error(), "connection refused after 2 cold-start retries") {
		t.Fatalf("expected cold-start exhaustion message, got %v", err)
	}
	// maxRetries=2 → attempts at indexes 0,1,2 = 3 total Do() calls.
	if got := atomic.LoadInt32(&attempts); got != 3 {
		t.Fatalf("expected 3 dial attempts (1 + 2 retries), got %d", got)
	}
}

func TestOllama_ColdStartRetryRecoversWhenBackendComesUp(t *testing.T) {
	t.Parallel()
	// A live server, but the client's RoundTripper refuses the first attempt with a
	// synthetic connection-refused error, then lets the second through. This exercises
	// the RECOVERY path: retry, then succeed and stream.
	lines := []string{
		`{"message":{"content":"ok"},"done":false}`,
		`{"done":true,"done_reason":"stop","prompt_eval_count":2,"eval_count":1}`,
	}
	srv := httptest.NewServer(ndjsonHandler(lines))
	defer srv.Close()

	var attempts int32
	rt := refuseFirstN{n: 1, attempts: &attempts, inner: srv.Client().Transport}
	p := NewOllamaProvider(srv.URL,
		WithRetry(3, 1*time.Millisecond),
		WithHTTPClient(&http.Client{Transport: rt}),
	)
	ch, err := p.Chat(context.Background(), domain.ChatRequest{Messages: []domain.Message{{Content: "x"}}})
	if err != nil {
		t.Fatalf("expected recovery after one refusal, got %v", err)
	}
	text, _, _, sawDone := drain(t, ch)
	if !sawDone || text != "ok" {
		t.Fatalf("expected to stream 'ok' after recovery, got text=%q done=%v", text, sawDone)
	}
	if atomic.LoadInt32(&attempts) != 2 {
		t.Fatalf("expected 2 attempts (1 refused + 1 success), got %d", atomic.LoadInt32(&attempts))
	}
}

// --- test RoundTrippers ------------------------------------------------------

// roundTripCounter counts Do() invocations (each retry is one) while delegating to
// the inner transport (which fails with connection-refused against a dead address).
type roundTripCounter struct {
	n  *int32
	rt http.RoundTripper
}

func (c roundTripCounter) RoundTrip(req *http.Request) (*http.Response, error) {
	atomic.AddInt32(c.n, 1)
	return c.rt.RoundTrip(req)
}

// refuseFirstN returns a synthetic connection-refused error for the first n
// attempts, then delegates to the inner transport. It lets us deterministically
// exercise the cold-start retry-then-recover path without real socket timing.
type refuseFirstN struct {
	n        int
	attempts *int32
	inner    http.RoundTripper
}

func (r refuseFirstN) RoundTrip(req *http.Request) (*http.Response, error) {
	a := atomic.AddInt32(r.attempts, 1)
	if int(a) <= r.n {
		// A *net.OpError with Op "dial" is what isConnRefused recognizes as retryable.
		return nil, &dialRefusedError{}
	}
	return r.inner.RoundTrip(req)
}

// dialRefusedError mimics a refused dial: its message contains "connection refused"
// so the adapter's isConnRefused substring fallback classifies it as retryable.
type dialRefusedError struct{}

func (e *dialRefusedError) Error() string {
	return "dial tcp 127.0.0.1:11434: connect: connection refused"
}
