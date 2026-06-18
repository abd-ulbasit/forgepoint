// tokens.go — a deterministic token-count ESTIMATOR for providers/usage.
//
// ============================================================================
// WHY ESTIMATE TOKENS HERE (and where the real counts come from)
// ============================================================================
//
// The AUTHORITATIVE token counts for a real backend come FROM the backend: Ollama
// returns prompt_eval_count (prompt tokens) and eval_count (output tokens) on its
// `done:true` line; OpenAI returns a usage block. The gateway uses those verbatim
// (see ollama.go). This estimator is only for:
//
//   - the StubProvider, which has no real tokenizer (it fabricates a deterministic
//     count so tests can assert exact usage/cost), and
//   - a FALLBACK when a real backend omits a count (defensive; a missing count
//     should never zero out cost metering).
//
// THE HEURISTIC: ~1 token per whitespace-separated word, with a char-based floor so
// a long single "word" (a URL, a code blob) isn't undercounted. This is the common
// "tokens ≈ 0.75 * words ≈ chars/4" rule of thumb. It is NOT a real BPE tokenizer
// (that would need the model's vocab) — and it doesn't need to be: for the stub it
// only needs to be DETERMINISTIC and roughly proportional, and for the fallback it
// only needs to be non-zero and sane. Real metering uses the backend's exact count.
package providers

import (
	"strings"

	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/domain"
)

// estimateTokens returns a deterministic, non-negative token estimate for a string.
// max(word count, char/4) so neither a sentence of short words nor a single long
// token is undercounted. Empty string → 0.
func estimateTokens(s string) int32 {
	if s == "" {
		return 0
	}
	words := len(strings.Fields(s))
	chars := len([]rune(s)) / 4
	n := words
	if chars > n {
		n = chars
	}
	if n < 1 {
		n = 1 // any non-empty content is at least one token.
	}
	return int32(n)
}

// promptTokenCount sums the estimated tokens across all input message contents. This
// is the stub's prompt-token figure and the fallback when a real backend omits its
// prompt_eval_count.
func promptTokenCount(messages []domain.Message) int32 {
	var total int32
	for _, m := range messages {
		total += estimateTokens(m.Content)
	}
	return total
}
