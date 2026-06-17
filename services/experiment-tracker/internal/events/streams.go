// streams.go — PRODUCER-OWNED JetStream stream provisioning for experiment-tracker.
//
// ============================================================================
// WHY THE PRODUCER OWNS ITS STREAM (the bug this file fixes)
// ============================================================================
//
// JetStream is NOT core-NATS fire-and-forget: a publish whose subject NO stream
// captures is REJECTED by the server ("no stream matches subject", code 10073) —
// it does not float into the void, it comes back as a PubAck error from Emit. The
// experiment-tracker is the OWNER and sole producer of the fp.experiments.* tree
// (publisher.go: fp.experiments.run.created / fp.experiments.run.finished), yet
// NO service was provisioning the EXPERIMENTS stream. Net effect on a real
// cluster: every run-lifecycle event this service published was DROPPED — and the
// notification reactor, which CONSUMES fp.experiments.run.* (a per-subject durable
// bound to the EXPERIMENTS stream), found no stream to bind and degrade-SKIPPED
// those two subjects, so run alerts never reached anyone.
//
// The fix follows the project's OWN explicit convention — "the PRODUCER owns its
// stream" — which auth (authevents.EnsureStream(AUTH)), registry
// (events.EnsureStream(MODELS)), feature-store, billing, and pipeline-orchestrator
// all follow at boot. Experiment-tracker is the fp.experiments.* producer, so it is
// the service that must guarantee the EXPERIMENTS stream exists before publishing.
// main.go calls EnsureStream right after Connect under a bounded context and FAILS
// FAST on error — turning an unprovisionable stream into a clean boot failure K8s
// surfaces, not a silent runtime event leak.
//
// NOTE ON THE CONSUME SIDE: experiment-tracker also CONSUMES fp.notifications.*
// (the wide lineage sink) bound to the NOTIFICATIONS stream — but that stream is
// owned and provisioned by the notification service (its producer), NOT here. We
// only ensure the stream we PRODUCE. This keeps the ownership graph acyclic: each
// fp.<domain>.> tree has exactly one owning provisioner.
//
// ============================================================================
// WHY CreateOrUpdateStream (not CreateStream) — convergent + idempotent
// ============================================================================
//
// CreateOrUpdateStream behaves like `kubectl apply`: absent → created; present (a
// GitOps/infra-as-code job, or a peer experiment-tracker pod during a rolling
// deploy declared it first) → RECONCILES rather than erroring on "already exists".
// That makes EnsureStream safe to call on EVERY boot and from MULTIPLE pods at once
// — no caller special-cases the race. CreateStream would force that special-casing.
//
// ============================================================================
// WHY fp.experiments.> (the whole tree) — resource-ownership
// ============================================================================
//
// We bind EXPERIMENTS to the entire fp.experiments.> wildcard, matching the
// contract's resource-ownership model and every sibling stream (MODELS=fp.models.>,
// PIPELINES=fp.pipelines.>, …). It captures both produced subjects today
// (run.created / run.finished) and any future fp.experiments.* subject without a
// config change, and it OVERLAPS no other stream (no other stream owns
// fp.experiments.*), so JetStream's "subjects must not overlap" rule (err 10065) is
// satisfied.
package events

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// StreamName is the JetStream stream experiment-tracker owns: the durable home
	// of the fp.experiments.* resource tree. Exported so main.go, the tests, and any
	// consumer (notification's reactor binds here) share one canonical value with no
	// stringly-typed drift between producer and consumer.
	StreamName = "EXPERIMENTS"

	// StreamSubjects is the wildcard the EXPERIMENTS stream binds. fp.experiments.>
	// captures both produced subjects (run.created / run.finished) plus any future
	// fp.experiments.* subject — the whole resource tree in one stream.
	StreamSubjects = "fp.experiments.>"
)

// EnsureStream creates (or reconciles) the EXPERIMENTS stream this service produces
// into. It is idempotent and convergent: safe to call on every boot and safe to
// race against a peer pod / GitOps job doing the same CreateOrUpdate.
//
// The caller passes the jetstream.JetStream from natsutil.Connect and a BOUNDED
// context — a hung NATS server must not wedge boot forever (main.go uses a 10s
// timeout, mirroring the dep pings). On error the caller fails fast (os.Exit),
// because an experiment-tracker that cannot guarantee its stream would otherwise
// drop every run-lifecycle event at runtime with only a log line.
func EnsureStream(ctx context.Context, js jetstream.JetStream) error {
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     StreamName,
		Subjects: []string{StreamSubjects},
	}); err != nil {
		return fmt.Errorf("experiment-tracker/events: ensure stream %s (%s): %w", StreamName, StreamSubjects, err)
	}
	return nil
}
