// stub_test.go — UNIT tests for the deterministic StubProvider.
package providers

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/domain"
)

// drain ranges the provider's channel, returning the concatenated streamed text and
// the terminal usage/finish. It asserts the channel closes after exactly one Done.
func drain(t *testing.T, ch <-chan domain.Delta) (text string, usage domain.TokenUsage, finish domain.FinishReason, sawDone bool) {
	t.Helper()
	var b strings.Builder
	for d := range ch {
		if d.Done {
			if sawDone {
				t.Fatalf("provider sent more than one Done frame")
			}
			sawDone = true
			usage = d.Usage
			finish = d.FinishReason
			continue
		}
		if sawDone {
			t.Fatalf("provider sent a token AFTER the Done frame")
		}
		b.WriteString(d.Text)
	}
	return b.String(), usage, finish, sawDone
}

func TestStub_StreamsDeterministicAnswerAndCounts(t *testing.T) {
	t.Parallel()
	stub := NewStubProvider(WithAnswer("one two three four"))
	ch, err := stub.Chat(context.Background(), domain.ChatRequest{
		Messages: []domain.Message{{Role: domain.RoleUser, Content: "hello world"}}, // 2 words → 2 prompt tokens.
	})
	if err != nil {
		t.Fatalf("unexpected start error: %v", err)
	}

	text, usage, finish, sawDone := drain(t, ch)
	if !sawDone {
		t.Fatalf("stream ended without a Done frame")
	}
	if text != "one two three four" {
		t.Fatalf("streamed text = %q, want %q", text, "one two three four")
	}
	// 4 answer words → 4 completion tokens; "hello world" → 2 prompt tokens.
	if usage.CompletionTokens != 4 {
		t.Fatalf("completion tokens = %d, want 4", usage.CompletionTokens)
	}
	if usage.PromptTokens != 2 {
		t.Fatalf("prompt tokens = %d, want 2", usage.PromptTokens)
	}
	if finish != domain.FinishReasonStop {
		t.Fatalf("finish = %v, want FinishReasonStop", finish)
	}
}

func TestStub_DeterministicAcrossRuns(t *testing.T) {
	t.Parallel()
	// The SAME input yields the SAME counts every run — the property that makes the
	// stub a usable test oracle.
	stub := NewStubProvider()
	req := domain.ChatRequest{Messages: []domain.Message{{Content: "the quick brown fox"}}}

	_, u1, _, _ := drain(t, mustChat(t, stub, req))
	_, u2, _, _ := drain(t, mustChat(t, stub, req))
	if u1 != u2 {
		t.Fatalf("usage not deterministic: %+v vs %+v", u1, u2)
	}
}

func TestStub_FailStartReturnsSyncError(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("simulated cold provider")
	stub := NewStubProvider(WithFailStart(sentinel))
	ch, err := stub.Chat(context.Background(), domain.ChatRequest{Messages: []domain.Message{{Content: "x"}}})
	if err == nil {
		t.Fatalf("expected a synchronous start error")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected wrapped sentinel, got %v", err)
	}
	if ch != nil {
		t.Fatalf("expected nil channel on start failure, got non-nil")
	}
}

func TestStub_HonorsContextCancellation(t *testing.T) {
	t.Parallel()
	// A cancelled context must stop the stream promptly; the channel closes WITHOUT a
	// Done frame (an aborted stream), which the service treats as a provider failure.
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before draining.
	stub := NewStubProvider(WithAnswer("a b c d e"))
	ch, err := stub.Chat(ctx, domain.ChatRequest{Messages: []domain.Message{{Content: "x"}}})
	if err != nil {
		t.Fatalf("start should still succeed (cancellation is observed while streaming): %v", err)
	}
	// Drain: we may get zero-or-more tokens then a close; we just assert it terminates
	// and the goroutine doesn't block (the test would hang/-race would catch a leak).
	for range ch { //nolint:revive // intentionally draining to completion
	}
}

func mustChat(t *testing.T, p domain.Provider, req domain.ChatRequest) <-chan domain.Delta {
	t.Helper()
	ch, err := p.Chat(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected chat error: %v", err)
	}
	return ch
}
