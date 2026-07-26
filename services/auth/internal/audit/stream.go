package audit

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	pkgaudit "github.com/abd-ulbasit/forgepoint/pkg/audit"
)

// ============================================================================
// AUDIT JETSTREAM STREAM — provisioned by the auth service (its consumer's owner)
// ============================================================================
//
// fp.audit.> is a CROSS-CUTTING tree: EVERY service publishes onto it
// (fp.audit.recorded), and exactly ONE service consumes it — auth, which hosts
// the persistent sink. Per the platform's "producer owns its stream" rule the
// owner of a stream is the side that GUARANTEES it exists before publishing.
// Audit is the special case where there is no single domain producer: the records
// are emitted platform-wide. The CONSUMER's host (auth) therefore owns the stream
// — it is the one service that must ensure the stream exists so that (a) its own
// emitted audit events are captured, and (b) every other service's audit publish
// lands in a real stream instead of being dropped (JetStream rejects a publish no
// stream captures).
//
// This mirrors experiment-tracker's EnsureStream(EXPERIMENTS): idempotent,
// convergent (CreateOrUpdateStream behaves like `kubectl apply`), safe on every
// boot and from multiple replicas. main.go calls it right after Connect under a
// bounded context and fails fast on error.
//
// OVERLAP SAFETY: fp.audit.> overlaps no other stream — no per-domain stream owns
// fp.audit.* — so JetStream's "subjects must not overlap" rule (err 10065) holds.

const (
	// StreamName is the JetStream stream that persists all audit events.
	StreamName = "AUDIT"

	// StreamSubjects is the wildcard the AUDIT stream binds: every audit subject.
	StreamSubjects = "fp.audit.>"

	// ConsumerGroup is the durable consumer name the auth audit consumer uses.
	// Replicas sharing this name form one consumer group (each event handled once).
	ConsumerGroup = "auth-audit-persist"

	// DLQSubject parks audit events that fail to persist after the retry cap. It is
	// OUTSIDE the fp.audit.> tree (the "fp_dlq." root) so a parked message is never
	// re-fed to this consumer — the same anti-amplification rule notification uses.
	DLQSubject = "fp_dlq.audit"
)

// SubjectRecorded re-exports pkg/audit's canonical publish subject so the auth
// consumer binds to the EXACT subject producers publish to (one source of truth;
// no stringly-typed drift between emit and persist).
const SubjectRecorded = pkgaudit.SubjectRecorded

// EnsureStream creates (or reconciles) the AUDIT stream. Idempotent and
// convergent; the caller passes a bounded context and fails fast on error.
func EnsureStream(ctx context.Context, js jetstream.JetStream) error {
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     StreamName,
		Subjects: []string{StreamSubjects},
	}); err != nil {
		return fmt.Errorf("auth/audit: ensure stream %s (%s): %w", StreamName, StreamSubjects, err)
	}
	return nil
}
