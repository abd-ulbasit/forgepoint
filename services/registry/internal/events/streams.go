// streams.go — PRODUCER-OWNED JetStream stream provisioning for the registry.
//
// ============================================================================
// WHY THE PRODUCER OWNS ITS STREAM (the bug this file fixes)
// ============================================================================
//
// JetStream is NOT core-NATS fire-and-forget: a publish whose subject NO stream
// captures does not silently float into the void — the server REJECTS it with an
// error ("no stream matches subject", code 10073). natsutil.Publisher.Publish
// uses the synchronous PubAck path (js.Publish + WithMsgID), so that rejection
// comes back as an error from Emit. But the registry's documented failure posture
// (ports.go / publisher.go) is correct-in-isolation: the WriteStore commit is the
// truth, so the service LOGS an Emit error and does NOT roll back the already-
// committed command. Net effect on a fresh cluster with no MODELS stream: every
// model-lifecycle event is dropped with only a log line while the write commits —
// the entire downstream fp.models.* flow silently dies (serving never learns a
// version is READY/loadable, billing never meters storage, inference-gateway never
// updates routing on promote/archive, model-monitor never re-baselines on promote,
// and the pipeline-orchestrator deploy/promote saga that blocks on
// ModelVersionReady never proceeds).
//
// The fix follows the project's OWN explicit, documented convention — "the PRODUCER
// owns its stream" — which auth (authevents.EnsureStream(AUTH)) and feature-store
// (CreateOrUpdateStream FEATURES at boot) both follow. The registry is the OWNER
// and sole fp.models.* producer (registered / version.created / version.ready /
// promoted / archived), so it is the service that must guarantee the MODELS stream
// exists before it publishes. main.go calls EnsureStream right after Connect, under
// a bounded context, and FAILS FAST if it errors — turning "unprovisionable stream"
// into a clean boot failure K8s surfaces, instead of a silent runtime event leak.
//
// ============================================================================
// WHY CreateOrUpdateStream (not CreateStream) — convergent + idempotent
// ============================================================================
//
// CreateOrUpdateStream behaves like `kubectl apply`: if the stream is absent it is
// created; if it already exists (an infra-as-code/GitOps job, a model-serving
// bootstrap that provisions MODELS as a consumer convenience, or a concurrent
// registry peer during a rolling deploy declared it first) it RECONCILES the config
// rather than erroring on "already exists". That makes EnsureStream safe to call on
// EVERY boot and from MULTIPLE pods at once — no caller has to special-case the
// race. CreateStream would force exactly that special-casing.
//
// ============================================================================
// WHY fp.models.> (the whole tree) — resource-ownership, not just our subjects
// ============================================================================
//
// We bind the MODELS stream to the entire fp.models.> wildcard, which captures all
// five registry subjects AND fp.models.drift.detected (produced by model-monitor).
// That is intentional and matches the contract's resource-ownership model: the
// MODELS stream is the durable home of the whole fp.models.* resource tree, owned
// by the registry. model-monitor can CreateOrUpdate the SAME stream convergently
// for its drift subject — both converge on one stream config, no conflict — so
// keeping the registry authoritative over the full tree is correct, not greedy.
package events

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// StreamName is the JetStream stream the registry owns: the durable home of the
	// fp.models.* resource tree. Named as an exported constant so main.go, the tests,
	// and any consumer that wants to reference the registry's stream share one
	// canonical value (no stringly-typed drift between producer and tests).
	StreamName = "MODELS"

	// StreamSubjects is the wildcard the MODELS stream binds. fp.models.> captures
	// all five registry lifecycle subjects (registered / version.created /
	// version.ready / promoted / archived) plus fp.models.drift.detected — the whole
	// resource tree in one stream. See the package header for why the registry owns
	// the full tree rather than only its own five subjects.
	StreamSubjects = "fp.models.>"
)

// EnsureStream creates (or reconciles) the MODELS stream this service produces
// into. It is idempotent and convergent: safe to call on every boot and safe to
// race against other services (model-monitor, a GitOps job, a peer registry pod
// during a rolling deploy) doing the same CreateOrUpdate.
//
// The caller passes the jetstream.JetStream returned by natsutil.Connect and a
// BOUNDED context — a hung NATS server must not wedge boot forever (main.go uses a
// 10s timeout, mirroring feature-store). On error the caller fails fast (os.Exit),
// because a registry that cannot guarantee its stream would otherwise drop every
// lifecycle event at runtime with only a log line.
func EnsureStream(ctx context.Context, js jetstream.JetStream) error {
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     StreamName,
		Subjects: []string{StreamSubjects},
	}); err != nil {
		return fmt.Errorf("registry/events: ensure stream %s (%s): %w", StreamName, StreamSubjects, err)
	}
	return nil
}
