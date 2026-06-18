package events

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// ============================================================================
// STREAM PROVISIONING
// ============================================================================
//
// A JetStream CONSUMER can only attach to a stream that already exists, and a
// PUBLISH only persists if the subject is captured by some stream. The trees this
// service touches are OWNED (in terms of who publishes the bulk of them) by other
// services — fp.inference.> by the gateway, fp.features.> by the feature store,
// fp.models.> by the registry (and, for drift, by us). But stream creation is a
// cluster-bootstrap concern, not a per-producer one: any service that needs a
// stream may CreateOrUpdate it (the op is idempotent + convergent, like
// `kubectl apply`). We provision the four streams this adapter depends on so a
// fresh cluster — or an integration test — has somewhere for events to land, for
// our durable consumers to bind, and for our OWN drift events to persist.
//
// WHY CreateOrUpdateStream (not CreateStream): convergent + idempotent. If the
// stream already exists with the same subjects this is a no-op; if another service
// created it first, ours simply reconciles. CreateStream errors on "already
// exists", forcing every caller to special-case the race.
//
// RETENTION: defaults (limits-based, file storage). Events are durable facts; a
// late-joining monitor should be able to replay recent inference/promotion events
// to rebuild its windows/baseline. Per-tree MaxAge/MaxBytes tuning is a later ops
// concern (M3); the contract here is just "the stream exists and captures the
// whole tree". EnsureStreams is split from Start so main.go can provision once at
// boot (or skip it if a GitOps step already declared the streams); tests call it
// directly against a testcontainers JetStream.

// EnsureStreams creates (or reconciles) the four streams this adapter binds to:
// MODELS (drift produced + promoted consumed), INFERENCE (completed/failed
// consumed), FEATURES (written consumed). Idempotent: safe to call repeatedly and
// concurrently with other services doing the same.
func EnsureStreams(ctx context.Context, js jetstream.JetStream) error {
	streams := []jetstream.StreamConfig{
		{Name: StreamModels, Subjects: []string{subjectModelsAll}},
		{Name: StreamInference, Subjects: []string{subjectInferenceAll}},
		{Name: StreamFeatures, Subjects: []string{subjectFeaturesAll}},
	}
	for _, cfg := range streams {
		if _, err := js.CreateOrUpdateStream(ctx, cfg); err != nil {
			return fmt.Errorf("events: ensure stream %s: %w", cfg.Name, err)
		}
	}
	return nil
}

// EnsureAIStream reconciles the AI stream the L4 quality-eval consumer binds to. It
// is SEPARATE from EnsureStreams (and called only when L4 is enabled) because the AI
// stream is OWNED by the ai-gateway — we ensure it only to allow a monitor-first boot
// to attach the consumer even if the gateway hasn't booted yet (graceful degrade).
//
// CRITICAL — IDENTICAL SUBJECT, NOT fp.ai.>: the gateway binds StreamAI to the single
// completion subject (SubjectAICompletionServed) and keeps its warm-signal stream
// (AI_REQUESTS=fp.ai.warm.requested) separate so KEDA's scaling lag is clean. If we
// declared fp.ai.> here it would OVERLAP the gateway's AI + AI_REQUESTS streams and
// JetStream would reject it (err_code=10065 subjects overlap) — a fatal boot crash.
// Declaring the IDENTICAL subject makes the two ensure calls idempotent: whoever boots
// first creates it, the other sees the same config (CreateOrUpdateStream is convergent).
// This is the exact billing fix the L4 task references.
func EnsureAIStream(ctx context.Context, js jetstream.JetStream) error {
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     StreamAI,
		Subjects: []string{SubjectAICompletionServed},
	}); err != nil {
		return fmt.Errorf("events: ensure stream %s (%s): %w", StreamAI, SubjectAICompletionServed, err)
	}
	return nil
}
