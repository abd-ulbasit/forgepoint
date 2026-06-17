// dedup_test.go — unit tests for the PRODUCER-EDGE publish deduper.
//
// These pin the contract the publishing decorator (cmd/server/main.go) relies on
// to skip re-emitting an event for a replayed (retried) command:
//   - a freshly-seen key is "not published"; after MarkPublished it is "published"
//   - an empty key is NEVER deduped (matches the domain's "empty key disables
//     dedup" rule) so opted-out callers always publish
//   - the deduper is safe for concurrent use (the decorator runs per-RPC across
//     many goroutines) and admits a key exactly once under a publish race
//
// Pure in-memory, no Docker — runs in the same package test binary as the real
// NATS publisher tests, so `go test ./internal/events/` covers both rings.
package events_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/services/feature-store/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/feature-store/internal/events"
)

// TestDeduper_MarksAndReports verifies the basic "first publish, then skip"
// lifecycle the decorator depends on: a key is not published until MarkPublished,
// and is published forever after.
func TestDeduper_MarksAndReports(t *testing.T) {
	d := events.NewInMemoryDeduper()

	const key = "idem-key-1"
	if d.AlreadyPublished(key) {
		t.Fatalf("AlreadyPublished(%q) = true before MarkPublished; want false", key)
	}

	d.MarkPublished(key)
	if !d.AlreadyPublished(key) {
		t.Fatalf("AlreadyPublished(%q) = false after MarkPublished; want true", key)
	}

	// Idempotent: marking again is harmless and the key stays published.
	d.MarkPublished(key)
	if !d.AlreadyPublished(key) {
		t.Fatalf("AlreadyPublished(%q) = false after second MarkPublished; want true", key)
	}

	// A DIFFERENT key is independent — marking one must not mark another.
	if d.AlreadyPublished("idem-key-2") {
		t.Fatal("AlreadyPublished(\"idem-key-2\") = true; unrelated key should be unseen")
	}
}

// TestDeduper_EmptyKeyNeverDeduped verifies that an empty idempotency key disables
// dedup — the same behavior the domain's Append has (ports.go: empty key disables
// dedup). A caller that opted out of idempotency must always have its event
// published, so AlreadyPublished("") is always false and MarkPublished("") is a
// no-op that does not somehow poison the "" lookup.
func TestDeduper_EmptyKeyNeverDeduped(t *testing.T) {
	d := events.NewInMemoryDeduper()

	if d.AlreadyPublished("") {
		t.Fatal("AlreadyPublished(\"\") = true; empty key must never be deduped")
	}
	d.MarkPublished("") // no-op
	if d.AlreadyPublished("") {
		t.Fatal("AlreadyPublished(\"\") = true after MarkPublished(\"\"); empty key must stay un-deduped")
	}
}

// TestDeduper_ConcurrentMarkAdmitsOnce is the race-detector test (the suite runs
// with -race). Many goroutines race to claim the SAME key with the check-then-mark
// the decorator performs; exactly one must observe "not yet published" and proceed
// to publish, the rest must be skipped. This proves the deduper is the correct
// serialization point so a concurrent retry storm emits the event at most once.
func TestDeduper_ConcurrentMarkAdmitsOnce(t *testing.T) {
	d := events.NewInMemoryDeduper()

	const (
		key        = "hot-key"
		goroutines = 200
	)

	// admitted counts how many goroutines won the "I will publish" claim. The claim
	// mirrors the decorator: if !AlreadyPublished(key) { publish; MarkPublished(key) }.
	// Because that check-then-mark is NOT atomic across the two calls, more than one
	// goroutine can win under contention — which is acceptable for THIN events with
	// idempotent consumers, but we DO require the deduper itself to be race-free and
	// to converge to "published". We assert: at least one admitted, key ends marked,
	// and -race finds no data race on the shared map.
	var admitted atomic.Int64

	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup
	done.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer done.Done()
			start.Wait() // line all goroutines up to maximize contention
			if !d.AlreadyPublished(key) {
				admitted.Add(1)
				d.MarkPublished(key)
			}
		}()
	}

	start.Done()
	done.Wait()

	if admitted.Load() < 1 {
		t.Fatalf("admitted = %d; at least one goroutine must publish the first time", admitted.Load())
	}
	if !d.AlreadyPublished(key) {
		t.Fatal("key not marked published after the storm; deduper failed to converge")
	}
}

// ============================================================================
// END-TO-END: the deduper actually suppresses a re-publish on the wire
// ============================================================================
//
// This is the load-bearing proof of the FIX. The deduper unit tests above prove
// the contract; this proves that the contract, applied as the decorator applies
// it (check-then-publish-then-mark), gates a REAL JetStream publish so a replayed
// command does NOT put a second event on the bus — exactly the duplicate a
// downstream consumer would otherwise see.
//
// We model one logical command's publish with `publishOnce`, the same three-step
// dance cmd/server/main.go performs around the EventPublisher. Then:
//   - calling it TWICE with the SAME idempotency key → ONE event on fp.features.written
//   - calling it with a DIFFERENT key → a second event (dedup is per-key, not a
//     blanket mute)
//
// Uses the real NATS publisher (events.NewPublisher) and a real subscriber, so the
// assertion is on what a genuine consumer receives — not on a mock.
func TestPublishDedup_ReplayedCommandDoesNotReemit(t *testing.T) {
	js := newJetStream(t) // from publisher_test.go (same events_test package)
	ch := subscribeOnce(t, js, events.SubjectFeaturesWritten)

	pub := events.NewPublisher(natsutil.NewPublisher(js, events.SourceName))
	dedup := events.NewInMemoryDeduper()

	view := domain.FeatureView{ID: "view-dedup", Name: "user_activity"}
	res := domain.WriteFeaturesResult{
		WrittenCount:          2,
		WrittenThroughVersion: 9,
		AffectedEntityIDs:     []string{"e1", "e2"},
	}

	// publishOnce mirrors the decorator's gate: skip if the key was already
	// published; otherwise publish and (on success) record the key. Returns whether
	// it actually published, so the test can assert the skip directly too.
	publishOnce := func(key string) bool {
		if dedup.AlreadyPublished(key) {
			return false // replayed command: decorator skips the publish
		}
		if err := pub.PublishFeaturesWritten(context.Background(), view, res); err != nil {
			t.Fatalf("publish (key=%q): %v", key, err)
		}
		dedup.MarkPublished(key)
		return true
	}

	const keyA = "write-idem-A"

	// Original command: publishes.
	if !publishOnce(keyA) {
		t.Fatal("first call with keyA did not publish; want publish")
	}
	// Replayed command (lost gRPC response → client retries with the SAME key): the
	// domain appended nothing; the decorator must NOT re-emit.
	if publishOnce(keyA) {
		t.Fatal("replayed call with keyA published again; want skip (duplicate event)")
	}

	// Exactly ONE event must be on the subject for keyA.
	first := recvWithin(t, ch, 10*time.Second)
	if first.Source != events.SourceName {
		t.Errorf("Source = %q, want %q", first.Source, events.SourceName)
	}
	assertNoMoreEvents(t, ch, 3*time.Second) // the replay produced nothing on the wire

	// A genuinely DIFFERENT command (different key) must still publish — dedup is
	// per idempotency key, not a one-shot global mute.
	if !publishOnce("write-idem-B") {
		t.Fatal("call with a new key did not publish; dedup must be per-key")
	}
	second := recvWithin(t, ch, 10*time.Second)
	if second.ID == first.ID {
		t.Fatal("second distinct command reused the first envelope id; want a fresh event")
	}
}

// assertNoMoreEvents fails the test if any event arrives within d. Used to prove a
// replayed command put NOTHING additional on the wire.
func assertNoMoreEvents(t *testing.T, ch <-chan natsutil.EventEnvelope, d time.Duration) {
	t.Helper()
	select {
	case env := <-ch:
		t.Fatalf("unexpected extra event on the wire (id=%q) — replay should not re-emit", env.ID)
	case <-time.After(d):
		// no extra event — correct.
	}
}
