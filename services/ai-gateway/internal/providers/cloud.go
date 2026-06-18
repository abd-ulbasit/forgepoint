// cloud.go — small shared helpers for the CLOUD provider adapters (OpenAI, Anthropic).
//
// The cloud adapters (openai.go, anthropic.go) are both always-on HTTPS streaming
// clients with the same dial profile, distinct from the local Ollama adapter (which
// has a cold-start retry loop because it is scaled-to-zero). This file holds the
// dial config they share so the two cloud adapters stay byte-for-byte consistent on
// the transport and a future cloud adapter inherits the same posture.
package providers

import (
	"context"
	"net"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/domain"
)

// CloudProviders returns the OPTIONAL cloud providers to append to the failover
// order, KEY-GATED: a provider is included ONLY when its API key is non-empty. An
// absent key ⇒ that provider is omitted ⇒ the order stays whatever the caller already
// built (Ollama + Stub). The returned slice is in a stable order (OpenAI before
// Anthropic) so the failover order is deterministic across restarts.
//
// WHY a helper (not inline in main.go): the key-gating is the governance-critical
// decision ("absent key = provider not registered"), and pulling it into a pure,
// dependency-free function makes it UNIT-TESTABLE without standing up main() — the
// test asserts the exact registration matrix (neither key, one key, both keys). The
// keys are SECRETS; this function only branches on their PRESENCE and passes them into
// the adapter constructor (which uses them solely as the auth header) — it never logs
// or returns them.
func CloudProviders(openAIKey, openAIBaseURL, anthropicKey, anthropicBaseURL string) []domain.Provider {
	var out []domain.Provider
	if openAIKey != "" {
		out = append(out, NewOpenAIProvider(openAIKey, WithOpenAIBaseURL(openAIBaseURL)))
	}
	if anthropicKey != "" {
		out = append(out, NewAnthropicProvider(anthropicKey, WithAnthropicBaseURL(anthropicBaseURL)))
	}
	return out
}

// defaultDialContext returns the DialContext used by the cloud HTTP clients: a short
// connect timeout so an unreachable endpoint fails FAST into the failover loop rather
// than hanging the request. We bound only the DIAL — the overall request has NO
// client timeout because a long completion legitimately streams for a while;
// cancellation is via the request ctx (honored on every read), not a blunt timeout
// that would kill a healthy long stream mid-flight.
func defaultDialContext() func(ctx context.Context, network, addr string) (net.Conn, error) {
	return (&net.Dialer{Timeout: 5 * time.Second}).DialContext
}
