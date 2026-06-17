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
// A JetStream CONSUMER can only attach to a stream that already exists. The two
// trees this service consumes (fp.models.>, fp.pipelines.>) are owned — in terms
// of who PUBLISHES into them — by the registry and the pipeline-orchestrator. But
// stream creation is a cluster-bootstrap concern, not a per-producer one: any
// service that needs a stream may CreateOrUpdate it (the operation is idempotent
// and convergent, like `kubectl apply`). We provision the streams this consumer
// depends on so a fresh cluster — or an integration test — has somewhere for the
// lifecycle events to land and for our durable consumers to bind.
//
// WHY CreateOrUpdateStream (not CreateStream): convergent + idempotent. If the
// stream already exists with the same subjects this is a no-op; if another
// service created it first, ours simply reconciles the config. CreateStream would
// error on "already exists", forcing every caller to special-case the race.
//
// RETENTION: we keep the defaults (limits-based, file storage) — events are
// durable facts and a late-joining serving pod should be able to replay recent
// lifecycle events to rebuild its desired state. Tuning MaxAge/MaxBytes per tree
// is a later ops concern (M3); the contract here is just "the stream exists and
// captures the whole tree".
//
// EnsureStreams is split out from Start so main.go can choose to provision once
// at boot (or skip it if a GitOps step already declared the streams). Tests call
// it directly against a testcontainers JetStream.

// EnsureStreams creates (or reconciles) the MODELS and PIPELINES streams this
// consumer binds to. Idempotent: safe to call repeatedly and concurrently with
// other services doing the same.
func EnsureStreams(ctx context.Context, js jetstream.JetStream) error {
	streams := []jetstream.StreamConfig{
		{
			Name:     StreamModels,
			Subjects: []string{subjectModelsAll},
		},
		{
			Name:     StreamPipelines,
			Subjects: []string{subjectPipelinesAll},
		},
	}
	for _, cfg := range streams {
		if _, err := js.CreateOrUpdateStream(ctx, cfg); err != nil {
			return fmt.Errorf("events: ensure stream %s: %w", cfg.Name, err)
		}
	}
	return nil
}
