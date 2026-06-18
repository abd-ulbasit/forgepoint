package judge

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/domain"
)

// newJudgeOverServer builds an OllamaJudge pointed at a test server that returns the
// given /api/chat message content. No real Ollama / network beyond loopback.
func newJudgeOverServer(t *testing.T, status int, content string) *OllamaJudge {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/chat" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(status)
		// Mimic Ollama's non-streamed /api/chat reply shape: {"message":{"content":...},"done":true}.
		_, _ = w.Write([]byte(`{"message":{"role":"assistant","content":` + jsonQuote(content) + `},"done":true}`))
	}))
	t.Cleanup(srv.Close)
	return NewOllamaJudge(srv.URL, "test-model", WithHTTPClient(srv.Client()))
}

// jsonQuote escapes a string into a JSON string literal (small local helper so the
// test fixture can embed arbitrary judge output without pulling encoding/json here).
func jsonQuote(s string) string {
	out := make([]byte, 0, len(s)+2)
	out = append(out, '"')
	for _, r := range s {
		switch r {
		case '"':
			out = append(out, '\\', '"')
		case '\\':
			out = append(out, '\\', '\\')
		case '\n':
			out = append(out, '\\', 'n')
		default:
			out = append(out, string(r)...)
		}
	}
	out = append(out, '"')
	return string(out)
}

func TestOllamaJudge_WellFormed_Scores(t *testing.T) {
	t.Parallel()
	j := newJudgeOverServer(t, http.StatusOK, `{"relevance": 5, "coherence": 4, "safety": 5}`)
	got, err := j.Score(context.Background(), domain.Completion{
		Model: "chatbot", RequestID: "r1", Prompt: "hi", Response: "hello there",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got.Scored || got.Relevance != 5 || got.Coherence != 4 || got.Safety != 5 {
		t.Fatalf("unexpected scores: %+v", got)
	}
}

func TestOllamaJudge_MessyOutput_StillScores(t *testing.T) {
	t.Parallel()
	// A tiny model wrapping its answer in prose — the robust parser still extracts it.
	j := newJudgeOverServer(t, http.StatusOK,
		"Sure, here goes:\nrelevance: 3/5, coherence: 4/5, safety: 5/5\nDone.")
	got, err := j.Score(context.Background(), domain.Completion{Response: "answer"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got.Scored || got.Relevance != 3 || got.Coherence != 4 || got.Safety != 5 {
		t.Fatalf("unexpected scores: %+v", got)
	}
}

func TestOllamaJudge_GarbageOutput_Unscored(t *testing.T) {
	t.Parallel()
	j := newJudgeOverServer(t, http.StatusOK, "I'm not able to evaluate this, sorry!")
	got, err := j.Score(context.Background(), domain.Completion{Response: "answer"})
	if err != nil {
		t.Fatalf("garbage output must not error (Unscored, nil), got err=%v", err)
	}
	if got.Scored {
		t.Fatalf("garbage output must be Unscored, got %+v", got)
	}
}

func TestOllamaJudge_NoResponseText_ShortCircuitsUnscored(t *testing.T) {
	t.Parallel()
	// The gateway ran with EvalIncludeText off → no response text. The judge must NOT
	// even call the server; it short-circuits to Unscored (graceful degradation).
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	j := NewOllamaJudge(srv.URL, "test-model", WithHTTPClient(srv.Client()))

	got, err := j.Score(context.Background(), domain.Completion{Response: "   "})
	if err != nil {
		t.Fatalf("no-text must not error: %v", err)
	}
	if got.Scored {
		t.Fatalf("no-text must be Unscored, got %+v", got)
	}
	if called {
		t.Fatalf("judge must not call Ollama when there is no response text")
	}
}

func TestOllamaJudge_Non200_UnscoredNoCrash(t *testing.T) {
	t.Parallel()
	j := newJudgeOverServer(t, http.StatusInternalServerError, "boom")
	got, err := j.Score(context.Background(), domain.Completion{Response: "answer"})
	// A 500 from a running Ollama is surfaced as an error alongside Unscored — never a
	// crash, never a fabricated score.
	if err == nil {
		t.Fatalf("expected an error for non-200")
	}
	if got.Scored {
		t.Fatalf("non-200 must be Unscored, got %+v", got)
	}
}

func TestOllamaJudge_Unreachable_UnscoredAfterRetry(t *testing.T) {
	t.Parallel()
	// Point at a closed port; the cold-start retry exhausts and we get Unscored + error
	// (best-effort: a down judge never crashes or NAK-storms — it degrades).
	j := NewOllamaJudge("http://127.0.0.1:1", "test-model", WithRetry(1, 1*time.Millisecond))
	got, err := j.Score(context.Background(), domain.Completion{Response: "answer"})
	if err == nil {
		t.Fatalf("expected a transport error for an unreachable judge")
	}
	if got.Scored {
		t.Fatalf("unreachable judge must be Unscored, got %+v", got)
	}
}
