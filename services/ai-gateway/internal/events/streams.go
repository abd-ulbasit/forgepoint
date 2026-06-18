// streams.go — idempotent provisioning of the two streams this service produces into.
//
// ============================================================================
// "ENSURE MY OWN PRODUCED STREAM" (the platform convention)
// ============================================================================
//
// Each service ensures the stream(s) it PRODUCES into on boot — the same pattern
// auth (AUTH), registry (MODELS), experiment-tracker (EXPERIMENTS), and
// model-monitor use. CreateOrUpdateStream behaves like `kubectl apply`: absent →
// created; present with the same config → no-op; present with different config →
// reconciled. That makes EnsureStreams safe to call on EVERY boot and safe to race
// against a peer pod or a GitOps bootstrap job doing the same.
//
// WHY FAIL FAST on error (the caller os.Exit's): an AI Gateway that cannot
// guarantee its streams would drop every cost event (breaking L2 metering) and
// every warm signal (Ollama never scales up → all completions fall back to the stub
// silently). Better to refuse to start than to serve in that degraded state.
//
// SUBJECT BINDING: we bind StreamAI to the COMPLETION subject only (not the fp.ai.>
// wildcard) so it does not also claim the warm subject — a subject may belong to
// only one stream, and the warm subject must live on AI_REQUESTS for KEDA. This
// explicit binding is the difference between "the scaler sees clean lag" and "the
// cost log and the scaler fight over fp.ai.>".
package events

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// EnsureStreams creates (or reconciles) the AI and AI_REQUESTS streams. The caller
// passes the jetstream.JetStream from natsutil.Connect and a BOUNDED context (a hung
// NATS must not wedge boot forever — main.go uses a short timeout, mirroring the dep
// pings). On any error the caller fails fast.
func EnsureStreams(ctx context.Context, js jetstream.JetStream) error {
	// AI — the durable cost/audit log. Bound to the completion subject explicitly.
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     StreamAI,
		Subjects: []string{SubjectCompletionServed},
	}); err != nil {
		return fmt.Errorf("ai-gateway/events: ensure stream %s (%s): %w", StreamAI, SubjectCompletionServed, err)
	}

	// AI_REQUESTS — the warm-signal log KEDA's ollama-warmer consumer binds to. Bound
	// to the warm subject so its pending count is a clean 0->1 scaling signal.
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     StreamAIRequests,
		Subjects: []string{SubjectWarmRequest},
	}); err != nil {
		return fmt.Errorf("ai-gateway/events: ensure stream %s (%s): %w", StreamAIRequests, SubjectWarmRequest, err)
	}
	return nil
}
