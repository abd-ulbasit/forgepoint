// stub.go — StubProvider: a DETERMINISTIC, dependency-free model backend.
//
// ============================================================================
// WHY A STUB PROVIDER EXISTS (and why it's a first-class provider, not a mock)
// ============================================================================
//
// The stub is the platform's "smollm2 stand-in": it streams a CANNED answer
// word-by-word with computed token counts, deterministically, with zero external
// dependencies. It is:
//
//   - The DEFAULT backend when no real Ollama is configured (local dev / CI), so
//     `make test` and a fresh checkout produce a working ChatCompletion with no
//     model server running.
//   - The ULTIMATE FAILOVER FALLBACK: registered LAST in the failover order, it is
//     the provider that always succeeds when Ollama is cold/down — so the gateway
//     never returns "all providers failed" as long as the stub is enabled. This is
//     exactly what makes the failover demo work on a RAM-tight node with Ollama at
//     zero replicas.
//   - The TEST ORACLE: because its output and token counts are a pure function of
//     the input, unit tests assert exact streamed text and exact usage/cost without
//     mocking the channel protocol — the stub IS a faithful Provider implementation,
//     so a test that passes against it exercises the real streaming contract.
//
// It deliberately implements the SAME domain.Provider streaming contract as Ollama
// (a goroutine pushing deltas, a single terminal Done frame, ctx-cancellation), so
// the failover loop can't tell a stub from a real backend — which is the point.
package providers

import (
	"context"
	"strings"

	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/domain"
)

// StubProvider is the deterministic in-process provider.
type StubProvider struct {
	// answer is the canned completion streamed word-by-word. A fixed sentence keeps
	// token counts stable for assertions.
	answer string
	// failStart, when true, makes Chat return a synchronous error WITHOUT producing a
	// stream — used by failover tests to simulate a provider that can't start (the
	// recoverable case the loop fails over from). Off by default.
	failStart bool
	// startErr is the error returned when failStart is set (defaults to a generic one).
	startErr error
}

// StubOption configures a StubProvider (functional options keep the zero value the
// "happy deterministic provider" and let tests opt into failure modes).
type StubOption func(*StubProvider)

// WithAnswer overrides the canned answer (tests asserting specific token counts).
func WithAnswer(answer string) StubOption {
	return func(s *StubProvider) { s.answer = answer }
}

// WithFailStart makes the stub fail synchronously at Chat (simulating a provider
// that can't start — the failover loop should skip to the next provider). err is
// the synchronous error the loop records as a provider failure.
func WithFailStart(err error) StubOption {
	return func(s *StubProvider) {
		s.failStart = true
		s.startErr = err
	}
}

// defaultStubAnswer is short and stable so token-count assertions are exact.
const defaultStubAnswer = "Hello from the Forgepoint AI gateway stub provider."

// NewStubProvider builds a deterministic stub. With no options it streams the
// default answer successfully.
func NewStubProvider(opts ...StubOption) *StubProvider {
	s := &StubProvider{answer: defaultStubAnswer}
	for _, opt := range opts {
		opt(s)
	}
	if s.failStart && s.startErr == nil {
		s.startErr = errStubFailStart
	}
	return s
}

// errStubFailStart is the default synchronous start error for WithFailStart().
var errStubFailStart = &providerError{provider: "stub", op: "chat", msg: "stub configured to fail start"}

// Kind identifies the stub.
func (s *StubProvider) Kind() domain.ProviderKind { return domain.ProviderKindStub }

// Models returns nil — the stub is a WILDCARD that answers any model name (so it is
// always an eligible failover fallback regardless of the requested model).
func (s *StubProvider) Models() []string { return nil }

// Chat streams the canned answer word-by-word, then a terminal Done frame carrying
// the computed token usage. It honors ctx cancellation between words.
//
// TOKEN ACCOUNTING (deterministic): prompt tokens = estimateTokens over all input
// message contents; completion tokens = estimateTokens over the answer. estimate is
// a stable word/char heuristic (see tokens.go), so the same input always yields the
// same counts — which is what lets a test assert exact usage and cost.
func (s *StubProvider) Chat(ctx context.Context, req domain.ChatRequest) (<-chan domain.Delta, error) {
	if s.failStart {
		// Synchronous start failure: nothing streamed; the failover loop tries the next.
		return nil, s.startErr
	}

	promptTokens := promptTokenCount(req.Messages)
	words := strings.Fields(s.answer)
	completionTokens := int32(len(words))

	out := make(chan domain.Delta)
	go func() {
		defer close(out)

		// Stream each word as its own delta (with a trailing space to reconstruct the
		// sentence on the client), checking ctx between sends so a cancelled stream
		// stops promptly and the goroutine exits (no leak).
		for i, w := range words {
			text := w
			if i < len(words)-1 {
				text += " "
			}
			select {
			case <-ctx.Done():
				// Cancelled: close the channel WITHOUT a Done frame; streamOne treats an
				// early close as a provider failure, which is the honest outcome for an
				// aborted stream. We do not try to send a terminal frame into a cancelled ctx.
				return
			case out <- domain.Delta{Text: text}:
			}
		}

		// Terminal frame: Done with the usage. Total/cost are filled by the service
		// from the served provider's rate table — the provider reports only the raw
		// prompt/completion counts it observed.
		usage := domain.TokenUsage{
			PromptTokens:     promptTokens,
			CompletionTokens: completionTokens,
		}
		select {
		case <-ctx.Done():
			return
		case out <- domain.Delta{Done: true, Usage: usage, FinishReason: domain.FinishReasonStop}:
		}
	}()

	return out, nil
}
