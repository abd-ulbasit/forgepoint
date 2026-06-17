// dedup.go — PRODUCER-EDGE publish deduplication for the Feature Store.
//
// ============================================================================
// THE PROBLEM THIS SOLVES (idempotent PRODUCER, not just idempotent consumer)
// ============================================================================
//
// The domain's EventLog.Append is idempotent on the IdempotencyKey: a retried
// command (e.g. a lost gRPC response → client retries WriteFeatures) is a no-op
// that returns the ORIGINAL version range with replayed=true (see
// domain/ports.go:79-86). NO new fact is appended. But the publishing decorator
// in main.go used to publish fp.features.written / fp.features.view.defined on
// EVERY successful call — so a replayed command RE-EMITTED its event even though
// nothing new happened.
//
// Transport-level JetStream dedup does NOT catch this: natsutil.NewEnvelope mints
// a fresh uuid per publish (envelope.go), so Nats-Msg-Id differs between the
// original publish and the replayed publish — JetStream's dedup window never sees
// a collision. Downstream consumers (experiment-tracker, model-monitor) dedupe on
// EventEnvelope.id, so they observe TWO distinct events and apply the effect
// twice (e.g. experiment-tracker records the same schema-version twice). That
// violates the platform's "exactly-once in EFFECT" standard at the PRODUCER edge.
//
// ============================================================================
// THE FIX — dedupe the PUBLISH on the SAME key the domain dedupes the APPEND on
// ============================================================================
//
// The wiring-edge decorator already has the one signal it needs on every call:
// the command's IdempotencyKey (in.IdempotencyKey). The contract guarantees a
// retried command carries the SAME key (ports.go:81-85). So we mark a key as
// "already published" the first time we successfully emit its event, and SKIP the
// publish for any later call bearing that key. This is the producer-side mirror
// of the domain's replayed short-circuit, implemented entirely at the publishing
// ring where the NATS concern belongs.
//
// WHY THIS, NOT "read a replayed bool off the result": gating on the key is
// strictly SAFER than gating on replayed. Consider a crash AFTER Append commits
// but BEFORE the original publish: the domain's append is durable (replayed=true
// on the retry), but the event was NEVER published. A replayed-bool gate would
// then SKIP the retry's publish too and the event is LOST. The key gate instead
// only records the key AFTER a successful publish, so that retry still publishes
// exactly once — at-least-once-to-publish, exactly-once-in-effect. (A future
// hardening pass replaces this in-memory marker with the transactional outbox the
// design calls out; the PORT below is the seam that swap slots into unchanged.)
//
// EMPTY KEY = NO DEDUP: per the same contract, an empty IdempotencyKey disables
// dedup (a caller that opted out of idempotency). MarkPublished/AlreadyPublished
// treat "" as never-seen, so such calls always publish — matching the domain's
// "empty key disables dedup" behavior so the two layers agree.
package events

import "sync"

// PublishDeduper is the OUTBOUND-EDGE dedup PORT the wiring decorator depends on.
// It records which command idempotency keys have already had their event
// published, so a replayed (retried) command does not re-emit.
//
// It is a PORT (interface) so main.go can inject a fake in a wiring test, and so a
// later hardening pass can replace the in-memory implementation with a durable one
// (Redis SET-NX / a Postgres outbox dedup table) without touching the decorator.
//
// CONTRACT:
//   - AlreadyPublished(key) reports whether MarkPublished(key) was called before.
//     An empty key is ALWAYS "not published" (dedup disabled for that command).
//   - MarkPublished(key) records the key as published. An empty key is a no-op.
//   - Implementations MUST be safe for concurrent use: the decorator runs per-RPC
//     across many goroutines.
type PublishDeduper interface {
	AlreadyPublished(key string) bool
	MarkPublished(key string)
}

// memDeduper is the default in-memory PublishDeduper: a mutex-guarded set of seen
// keys. Suitable for a single-replica service or a best-effort guard; it does NOT
// survive a restart and is NOT shared across replicas (a replica that didn't
// publish the original would re-publish a retry routed to it). That residual
// duplication is acceptable for THIN notification events whose consumers are
// already idempotent — and is exactly why the design schedules a durable outbox
// later. The interface above is the seam for that upgrade.
//
// WHY a plain map+Mutex and not sync.Map: the access pattern is "check then set"
// under a single critical section in MarkPublishedIfNew below; sync.Map shines for
// disjoint read/write key sets, not for this read-modify-write, where a Mutex is
// simpler and just as fast at this scale.
type memDeduper struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

// NewInMemoryDeduper builds the default in-memory PublishDeduper. The caller wires
// ONE instance into the publishing decorator (see main.go) so all RPCs share the
// same seen-set for the process lifetime.
func NewInMemoryDeduper() PublishDeduper {
	return &memDeduper{seen: make(map[string]struct{})}
}

// AlreadyPublished reports whether this key's event was already published. An empty
// key is never considered published, so dedup is disabled for it (contract above).
func (d *memDeduper) AlreadyPublished(key string) bool {
	if key == "" {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.seen[key]
	return ok
}

// MarkPublished records the key as having had its event published. Empty key is a
// no-op (dedup disabled). Idempotent: marking an already-marked key is harmless.
func (d *memDeduper) MarkPublished(key string) {
	if key == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seen[key] = struct{}{}
}
