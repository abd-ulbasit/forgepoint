package events_test

import (
	"context"
	"strings"
	"testing"
	"time"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/services/notification/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/notification/internal/events"
	"github.com/nats-io/nats.go/jetstream"
)

// ============================================================================
// REACTOR (SUBSCRIBER) INTEGRATION TESTS — real NATS JetStream
// ============================================================================
//
// These prove the INBOUND choreography adapter end-to-end against a real broker:
//   • a published fp.> event is consumed, the recipient resolved, ReactToEvent run,
//     and the decision executed (dispatch);
//   • a DUPLICATE redelivery of the same envelope id produces a SINGLE effect
//     (idempotent consumer);
//   • a poison message (executor permanently fails) lands on the DLQ after the cap;
//   • an event with no recipient is ACKed and skipped (no execution).

// newReactor wires a Reactor with a real natsutil.Subscriber pointed at the test
// NATS, the SAME options main.go uses (durable group + idempotency store + bounded
// retries + DLQ), plus a short AckWait so redelivery-driven tests run fast.
func newReactor(t *testing.T, js jetstream.JetStream, deps events.ReactorDeps, store natsutil.ProcessedStore, dlq string) *events.Reactor {
	t.Helper()
	cfg := events.ReactorConfig{
		ConsumerGroup:  "notification-test",
		MaxRetries:     2, // total 3 attempts before DLQ
		DLQSubject:     dlq,
		MessageTimeout: 5 * time.Second,
		AckWait:        2 * time.Second, // short so the redelivery/DLQ tests are quick
	}
	sub := events.NewReactorSubscriber(js, store, cfg)
	t.Cleanup(sub.Close)
	r := events.NewReactor(sub, deps, store, cfg, nil)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start reactor: %v", err)
	}
	return r
}

// sampleEvent is an opaque platform event body the reactor never decodes — any
// events.v1 message works as "some event arrived". We use ModelDriftDetected so the
// body is realistic, but the reactor only reads envelope metadata.
func sampleEvent() *eventsv1.ModelDriftDetected {
	return &eventsv1.ModelDriftDetected{
		ModelName:    "iris",
		ModelVersion: "3.0.0",
		Severity:     eventsv1.DriftSeverity_DRIFT_SEVERITY_CRITICAL,
		ReportId:     "report-1",
	}
}

// A consumed event is decoded (opaquely), routed to a recipient, reacted to, and
// the decision executed — exactly once — and a delivery-health event is published
// per external delivery the executor reported.
func TestReactor_ConsumesAndDispatches(t *testing.T) {
	js := newJS(t)

	exec := &fakeExecutor{result: events.ExecutionResult{
		NotificationID: "notif-100",
		Deliveries: []events.ExecutedDelivery{
			{Channel: domain.ChannelWebhook, Delivered: true},
			{Channel: domain.ChannelSlack, Delivered: false, Attempts: 4, ErrorMessage: "circuit open"},
		},
	}}
	health := &fakeHealth{}
	deps := events.ReactorDeps{
		Service:  realService(),
		Router:   &fakeRouter{recipient: "user-1", fullType: "fp.models.drift.detected"},
		Prefs:    &fakePrefs{prefs: fullPrefs("user-1")},
		Executor: exec,
		Health:   health,
	}
	newReactor(t, js, deps, natsutil.NewMemoryProcessedStore(), events.SubjectDLQ)

	publishCanonical(t, js, "fp.models.drift.detected", "model-monitor", sampleEvent())

	waitFor(t, func() bool { return exec.count() == 1 }, 15*time.Second, "one Execute call")

	// The reactor built the InboundEvent OPAQUELY from the envelope: the full subject
	// came from the router (FullType), the recipient from the router, and the payload
	// bytes rode through verbatim (never decoded).
	ev, ok := exec.lastEvent()
	if !ok {
		t.Fatal("no event recorded by executor")
	}
	if ev.RecipientUserID != "user-1" {
		t.Errorf("recipient = %q, want user-1", ev.RecipientUserID)
	}
	if ev.Type != "fp.models.drift.detected" {
		t.Errorf("event type = %q, want fp.models.drift.detected (router-supplied full subject)", ev.Type)
	}
	if len(ev.Payload) == 0 {
		t.Error("opaque payload bytes must ride through to the executor")
	}

	// Delivery-health: one delivered (webhook) + one failed (slack) event published.
	waitFor(t, func() bool { return health.deliveredCount() == 1 && health.failedCount() == 1 },
		10*time.Second, "delivery-health events (1 delivered + 1 failed)")
}

// IDEMPOTENCY: the SAME envelope id redelivered produces a SINGLE Execute call.
// We publish twice with the SAME Nats-Msg-Id (envelope id) so JetStream's publish-
// window dedup AND the consumer's ProcessedStore both guard the duplicate — the
// platform's "idempotent consumer" rule under at-least-once delivery.
func TestReactor_DuplicateRedelivery_SingleEffect(t *testing.T) {
	js := newJS(t)
	exec := &fakeExecutor{result: events.ExecutionResult{NotificationID: "notif-200"}}
	deps := events.ReactorDeps{
		Service:  realService(),
		Router:   &fakeRouter{recipient: "user-2", fullType: "fp.pipelines.failed"},
		Prefs:    &fakePrefs{prefs: fullPrefs("user-2")},
		Executor: exec,
	}
	store := natsutil.NewMemoryProcessedStore()
	newReactor(t, js, deps, store, events.SubjectDLQ)

	// Publish the SAME envelope id twice. natsutil.Publisher sets Nats-Msg-Id =
	// envelope.ID; reusing the id makes the SECOND publish a transport-layer
	// duplicate. We craft the envelope by hand so both publishes share an id.
	env := buildEnvelope(t, "evt-dup-1", "fp.pipelines.failed", "pipeline-orchestrator", sampleEvent())
	rawPublish(t, js, "fp.pipelines.failed", env, "evt-dup-1")
	rawPublish(t, js, "fp.pipelines.failed", env, "evt-dup-1")

	// Wait for the first to be processed, then give the duplicate ample time to (not)
	// be processed.
	waitFor(t, func() bool { return exec.count() == 1 }, 15*time.Second, "first Execute")
	time.Sleep(2 * time.Second)
	if exec.count() != 1 {
		t.Fatalf("Execute called %d times; duplicate redelivery must produce a single effect", exec.count())
	}
}

// IDEMPOTENCY (domain layer): even when the transport store would NOT recognize the
// duplicate (a redelivery with a DIFFERENT envelope id but the SAME (eventID,
// recipient) business key — modeling cross-replica/store-miss), the reactor's
// per-(eventID,recipient) dedup via domain.EventDedupKey still produces a single
// effect. We simulate this by publishing two DIFFERENT envelopes whose router maps
// them to the same (event id is the envelope id here, so we instead assert the
// store records the domain key) — covered by the same-id test above; this test
// asserts the domain dedup key is what the reactor records.
func TestReactor_RecordsDomainDedupKey(t *testing.T) {
	js := newJS(t)
	exec := &fakeExecutor{result: events.ExecutionResult{NotificationID: "notif-300"}}
	store := natsutil.NewMemoryProcessedStore()
	deps := events.ReactorDeps{
		Service:  realService(),
		Router:   &fakeRouter{recipient: "user-3", fullType: "fp.experiments.run.finished"},
		Prefs:    &fakePrefs{prefs: fullPrefs("user-3")},
		Executor: exec,
	}
	newReactor(t, js, deps, store, events.SubjectDLQ)

	env := buildEnvelope(t, "evt-key-1", "fp.experiments.run.finished", "experiment-tracker", sampleEvent())
	rawPublish(t, js, "fp.experiments.run.finished", env, "evt-key-1")

	waitFor(t, func() bool { return exec.count() == 1 }, 15*time.Second, "Execute")

	// The reactor must have recorded the per-(eventID,recipient) business key, so a
	// store-level check on that exact key reports processed.
	key := domain.EventDedupKey("evt-key-1", "user-3")
	waitFor(t, func() bool {
		seen, _ := store.IsProcessed(context.Background(), key)
		return seen
	}, 5*time.Second, "domain dedup key recorded")
}

// DLQ: a poison message (the executor permanently fails) is retried up to the cap
// and then parked on the DLQ subject; redelivery stops.
func TestReactor_PoisonMessage_RoutedToDLQ(t *testing.T) {
	js := newJS(t)
	exec := &fakeExecutor{failalways: true} // every Execute fails permanently
	deps := events.ReactorDeps{
		Service:  realService(),
		Router:   &fakeRouter{recipient: "user-4", fullType: "fp.pipelines.failed"},
		Prefs:    &fakePrefs{prefs: fullPrefs("user-4")},
		Executor: exec,
	}
	// The DLQ subject MUST be the out-of-firehose one (events.SubjectDLQ) so the parked
	// poison message lands in the dedicated NOTIFICATION_DLQ stream, NOT back in the
	// fp.> firehose the reactor consumes.
	dlq := events.SubjectDLQ
	newReactor(t, js, deps, natsutil.NewMemoryProcessedStore(), dlq)

	// Subscribe to the DLQ FIRST so we capture the parked message — on the DEDICATED
	// DLQ stream (events.StreamDLQ), the only stream that owns events.SubjectDLQ.
	got := make(chan natsutil.EventEnvelope, 1)
	dlqSub := natsutil.NewSubscriber(js)
	t.Cleanup(dlqSub.Close)
	if err := dlqSub.Subscribe(context.Background(), events.StreamDLQ, dlq,
		func(_ context.Context, env natsutil.EventEnvelope) error {
			select {
			case got <- env:
			default:
			}
			return nil
		}); err != nil {
		t.Fatalf("subscribe DLQ: %v", err)
	}

	publishCanonical(t, js, "fp.pipelines.failed", "pipeline-orchestrator", sampleEvent())

	select {
	case env := <-got:
		if env.Type != "failed" { // prefix-stripped type of fp.pipelines.failed
			t.Errorf("DLQ envelope type = %q, want 'failed'", env.Type)
		}
	case <-time.After(25 * time.Second):
		t.Fatal("timeout: poison message never reached the DLQ")
	}

	// The handler was attempted MaxRetries+1 times (it always failed) before DLQ.
	if exec.count() < 3 {
		t.Errorf("Execute attempts = %d, want >= 3 (retried before DLQ)", exec.count())
	}
	// REGRESSION (DLQ poison re-consumption): the executor must run AT MOST
	// MaxRetries+1 times for a forever-poison message. Before the fix, parking the
	// poison message on "fp.dlq.notification" (which matches the reactor's fp.>
	// consumer and shared the EVENTS stream) fed the DLQ copy back to the reactor,
	// which re-decoded the SAME envelope, re-failed, and re-parked it — driving the
	// executor through a SECOND full retry-round (~6 calls for a cap of 3). With the
	// DLQ routed outside fp.> into its own stream, the message leaves the firehose for
	// good after the cap, so the count is bounded at MaxRetries+1 (=3). We give the
	// old re-loop time to manifest (a full AckWait round) before asserting the bound.
	time.Sleep(4 * time.Second) // > AckWait (2s) so any re-consumption round would have fired
	if got := exec.count(); got > 3 {
		t.Fatalf("Execute ran %d times; want <= MaxRetries+1 (=3). "+
			"A count above the cap means the parked DLQ message was re-consumed by the "+
			"reactor's own fp.> consumer (DLQ poison re-consumption / amplification).", got)
	}
}

// REGRESSION (DLQ poison re-consumption / amplification — the CONFIRMED finding):
// a FOREVER-poison message must drive the executor AT MOST MaxRetries+1 times. This
// is the tight, dedicated guard for the bug: with the old DLQ subject
// ("fp.dlq.notification", which matches the reactor's fp.> consumer and lived in the
// same EVENTS stream), parking the poison copy re-fed it to the reactor — the SAME
// envelope was re-decoded, re-failed, and re-parked, so the executor ran ~2x its cap
// (one extra full retry-round). Crucially, this test does NOT drain the DLQ: it
// leaves the parked message sitting in the DLQ stream, so if that stream were the
// firehose (or shared the reactor's consumer) the re-consumption WOULD fire here.
// With the DLQ in its own out-of-fp.> stream, the parked message is inert and the
// count stays at exactly MaxRetries+1.
//
// WHY MaxRetries:1 (cap = 2): a smaller cap makes the "extra full round" gap (which
// would push the count to ~4) unmistakable while keeping the redelivery test fast.
func TestReactor_PoisonMessage_ExecutorBoundedByCap(t *testing.T) {
	js := newJS(t)
	exec := &fakeExecutor{failalways: true} // forever-poison: every Execute fails permanently

	const maxRetries = 1
	const wantCap = maxRetries + 1 // 2 total attempts before DLQ

	cfg := events.ReactorConfig{
		ConsumerGroup:  "notification-bound-test",
		MaxRetries:     maxRetries,
		DLQSubject:     events.SubjectDLQ, // OUTSIDE fp.> → parked copy is never re-consumed
		MessageTimeout: 5 * time.Second,
		AckWait:        1 * time.Second, // short so retries + any re-loop happen fast
	}
	sub := events.NewReactorSubscriber(js, natsutil.NewMemoryProcessedStore(), cfg)
	t.Cleanup(sub.Close)
	r := events.NewReactor(sub, events.ReactorDeps{
		Service:  realService(),
		Router:   &fakeRouter{recipient: "user-poison", fullType: "fp.pipelines.failed"},
		Prefs:    &fakePrefs{prefs: fullPrefs("user-poison")},
		Executor: exec,
	}, natsutil.NewMemoryProcessedStore(), cfg, nil)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start reactor: %v", err)
	}

	publishCanonical(t, js, "fp.pipelines.failed", "pipeline-orchestrator", sampleEvent())

	// Wait until the cap is reached (the message has been delivered+failed wantCap times).
	waitFor(t, func() bool { return exec.count() >= wantCap }, 15*time.Second,
		"executor reached the retry cap")

	// Now hold past SEVERAL AckWait windows. If the DLQ copy were re-consumable by the
	// reactor (the bug), a fresh retry-round would start here and push count past the
	// cap. With the DLQ outside fp.> in its own stream, no further deliveries happen.
	time.Sleep(5 * time.Second) // 5x AckWait — generous room for any re-consumption round

	if got := exec.count(); got != wantCap {
		t.Fatalf("Execute ran %d times for a forever-poison message; want exactly %d (MaxRetries+1). "+
			"A higher count means the parked DLQ message was re-delivered to the reactor's "+
			"own fp.> consumer (DLQ poison re-consumption / amplification regressed).", got, wantCap)
	}
}

// REGRESSION (structural): the reactor's DLQ subject must NOT be matched by its own
// consumer filter (SubjectAllEvents = "fp.>"). This is the root-cause invariant of
// the DLQ re-consumption bug, asserted directly and without a broker: if a future
// edit moves the DLQ back under fp.* (e.g. "fp.dlq.notification"), this fails
// immediately — a cheap canary in front of the slower broker test above.
func TestSubjectDLQ_NotUnderFirehose(t *testing.T) {
	if !subjectMatches(events.SubjectAllEvents, "fp.notifications.delivered") {
		t.Fatalf("sanity: %q should match a real fp.* subject", events.SubjectAllEvents)
	}
	if subjectMatches(events.SubjectAllEvents, events.SubjectDLQ) {
		t.Fatalf("DLQ subject %q matches the reactor's own consumer filter %q — "+
			"it would be re-consumed (DLQ poison re-consumption). The DLQ must be rooted "+
			"outside fp.> (e.g. the fp_dlq.* token).", events.SubjectDLQ, events.SubjectAllEvents)
	}
}

// subjectMatches reports whether a concrete NATS subject is covered by a filter that
// may end in the ">" multi-token wildcard or contain "*" single-token wildcards. It
// mirrors JetStream's token-wise matching closely enough to assert the fp.> vs
// fp_dlq.* containment invariant (the only thing this canary needs).
func subjectMatches(filter, subject string) bool {
	f := strings.Split(filter, ".")
	s := strings.Split(subject, ".")
	for i, ft := range f {
		if ft == ">" { // ">" matches the rest of the subject (one or more tokens)
			return i < len(s)
		}
		if i >= len(s) {
			return false
		}
		if ft == "*" { // "*" matches exactly one token
			continue
		}
		if ft != s[i] {
			return false
		}
	}
	return len(f) == len(s)
}

// NO RECIPIENT: an event the router declines is ACKed and skipped — no Execute call,
// no NAK-loop, no DLQ. This is the normal case for platform events with no per-user
// target (e.g. fp.pipelines.step.completed).
func TestReactor_NoRecipient_Skips(t *testing.T) {
	js := newJS(t)
	exec := &fakeExecutor{result: events.ExecutionResult{}}
	deps := events.ReactorDeps{
		Service:  realService(),
		Router:   &fakeRouter{recipient: "user-5", fullType: "fp.pipelines.step.completed", declineType: "step.completed"},
		Prefs:    &fakePrefs{prefs: fullPrefs("user-5")},
		Executor: exec,
	}
	newReactor(t, js, deps, natsutil.NewMemoryProcessedStore(), events.SubjectDLQ)

	// fp.pipelines.step.completed → natsutil derives type "step.completed", which the
	// router declines.
	publishCanonical(t, js, "fp.pipelines.step.completed", "pipeline-orchestrator", sampleEvent())

	// Publish a SECOND event the router DOES accept, to prove the consumer is alive
	// and progressing (so the absence of an Execute for the first is a SKIP, not a stall).
	publishCanonical(t, js, "fp.pipelines.failed", "pipeline-orchestrator", sampleEvent())

	waitFor(t, func() bool { return exec.count() == 1 }, 15*time.Second, "one Execute (only the accepted event)")
	time.Sleep(1 * time.Second)
	if exec.count() != 1 {
		t.Fatalf("Execute called %d times; the declined event must be skipped (only the accepted one runs)", exec.count())
	}
	ev, _ := exec.lastEvent()
	if ev.Type != "fp.pipelines.step.completed" && ev.Type != "fp.pipelines.failed" {
		t.Errorf("unexpected executed event type %q", ev.Type)
	}
}
