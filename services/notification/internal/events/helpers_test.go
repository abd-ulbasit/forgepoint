package events_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/notification/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/notification/internal/events"
)

// ============================================================================
// SHARED TEST HELPERS (real NATS JetStream via testcontainers)
// ============================================================================
//
// Every test in this package exercises REAL pub/sub against a real NATS server —
// the contract we care about is the WIRE behavior (subject routing + envelope +
// canonical encoding + at-least-once redelivery → idempotency → DLQ), which a mock
// broker cannot faithfully reproduce. testutil.StartNATS spins a nats:2.11 with
// JetStream; SkipIfNoDocker skips cleanly when Docker is unavailable.

// streamNotifications is the NOTIFICATIONS stream over fp.notifications.> so the
// publisher tests (drainOne) can read this service's OWN delivery-health output. In
// PRODUCTION notification now OWNS and provisions this stream at boot (it is the sole
// producer of fp.notifications.* — see events.EnsureStream / streams_test.go); these
// helpers create it directly to stand in for that boot-time provisioning. The reactor
// never CONSUMES it — it is the publisher's output, read here only to assert the wire.
// It MUST equal events.StreamName so producer and test agree on one name.
const streamNotifications = events.StreamName

// newJS spins up a real NATS and provisions the per-domain streams the reactor's
// per-subject consumers BIND to — exactly the production topology, where each owning
// service creates its own narrow stream (MODELS=fp.models.>, PIPELINES=fp.pipelines.>,
// INFERENCE=fp.inference.>, BILLING=fp.billing.>, EXPERIMENTS=fp.experiments.>). It
// ALSO creates the dedicated NOTIFICATION_DLQ stream (events.SubjectDLQ,
// "fp_dlq.notification" — OUTSIDE every fp.<domain>.> tree) and the test-only
// NOTIFICATIONS stream for the publisher tests. Returns a JetStream handle; torn down
// with the container by t.Cleanup inside StartNATS.
//
// WHY per-domain streams (NOT one fp.> firehose): a JetStream stream's subjects must
// not OVERLAP another stream's, so a single fp.> stream cannot coexist with the narrow
// per-domain streams the rest of the platform creates (it would fail with err 10065).
// The reactor therefore binds ONE durable consumer per subject to the EXISTING owning
// stream — these CreateStream calls stand in for the producers' boot-time provisioning.
//
// IMPORTANT: ALL of ConsumedSubjects' owning streams must exist, because Reactor.Start
// resolves every subject via StreamNameBySubject and fails if any is missing. We create
// the full per-domain set up front so a test that only publishes to one subject still
// lets the reactor start all its consumers.
//
// WHY a SEPARATE DLQ stream: a JetStream subject is bound to exactly ONE stream, and
// the DLQ subject is deliberately rooted OUTSIDE every fp.<domain>.> tree so it never
// lands in a stream a reactor consumer reads (the DLQ poison re-consumption fix). It
// therefore needs its own stream — mirroring main.go, which provisions it.
func newJS(t *testing.T) jetstream.JetStream {
	t.Helper()
	testutil.SkipIfNoDocker(t)
	url := testutil.StartNATS(t)

	conn, js, err := natsutil.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(conn.Close)

	ctx := context.Background()
	// The narrow per-domain streams that own the subjects in events.ConsumedSubjects.
	// One stream per top-level domain tree, exactly as each producing service creates.
	perDomain := []jetstream.StreamConfig{
		{Name: "MODELS", Subjects: []string{"fp.models.>"}},
		{Name: "PIPELINES", Subjects: []string{"fp.pipelines.>"}},
		{Name: "INFERENCE", Subjects: []string{"fp.inference.>"}},
		{Name: "BILLING", Subjects: []string{"fp.billing.>"}},
		{Name: "EXPERIMENTS", Subjects: []string{"fp.experiments.>"}},
		{Name: streamNotifications, Subjects: []string{"fp.notifications.>"}},
	}
	for _, sc := range perDomain {
		if _, err := js.CreateStream(ctx, sc); err != nil {
			t.Fatalf("create %s stream: %v", sc.Name, err)
		}
	}
	// The dead-letter stream owns the fp_dlq.* subject (outside every fp.<domain>.>
	// tree), so parked poison messages have a home no reactor consumer can re-read.
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     events.StreamDLQ,
		Subjects: []string{events.SubjectDLQ},
	}); err != nil {
		t.Fatalf("create NOTIFICATION_DLQ stream: %v", err)
	}
	return js
}

// newJSPartial spins up a real NATS but provisions ONLY the PIPELINES stream (plus the
// DLQ stream). It models a fresh/partial cluster where some producers have created their
// streams and others (MODELS, INFERENCE, BILLING, EXPERIMENTS) have NOT yet. It exists
// to exercise the reactor's DEGRADE policy: Start must SKIP the subjects whose owning
// stream is absent (with a warning) and still bind the ones that exist — never crash and
// never create an overlapping stream.
func newJSPartial(t *testing.T) jetstream.JetStream {
	t.Helper()
	testutil.SkipIfNoDocker(t)
	url := testutil.StartNATS(t)

	conn, js, err := natsutil.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(conn.Close)

	ctx := context.Background()
	// ONLY pipelines exists — the other domains' streams are deliberately absent.
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "PIPELINES", Subjects: []string{"fp.pipelines.>"},
	}); err != nil {
		t.Fatalf("create PIPELINES stream: %v", err)
	}
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: events.StreamDLQ, Subjects: []string{events.SubjectDLQ},
	}); err != nil {
		t.Fatalf("create NOTIFICATION_DLQ stream: %v", err)
	}
	return js
}

// drainOne subscribes to a subject and returns the first envelope, failing on
// timeout. Used by the publisher tests to capture exactly what reached the bus.
func drainOne(t *testing.T, js jetstream.JetStream, subject string) natsutil.EventEnvelope {
	t.Helper()
	got := make(chan natsutil.EventEnvelope, 1)
	sub := natsutil.NewSubscriber(js)
	t.Cleanup(sub.Close)
	if err := sub.Subscribe(context.Background(), streamNotifications, subject,
		func(_ context.Context, env natsutil.EventEnvelope) error {
			select {
			case got <- env:
			default:
			}
			return nil
		}); err != nil {
		t.Fatalf("subscribe %s: %v", subject, err)
	}
	select {
	case env := <-got:
		return env
	case <-time.After(15 * time.Second):
		t.Fatalf("timeout waiting for an envelope on %s", subject)
		return natsutil.EventEnvelope{}
	}
}

// protojsonUnmarshal decodes a canonical proto-JSON envelope payload, mirroring a
// real cross-language consumer. Asserts the produced events carry canonical encoding.
func protojsonUnmarshal(t *testing.T, data json.RawMessage, msg proto.Message) {
	t.Helper()
	if err := protojson.Unmarshal(data, msg); err != nil {
		t.Fatalf("protojson unmarshal %T: %v", msg, err)
	}
}

// waitFor polls cond until true or the deadline, failing with msg on timeout.
// Polling (not a fixed sleep) keeps the tests fast when the broker is quick and
// robust when it is slow under CI load.
func waitFor(t *testing.T, cond func() bool, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", msg)
}

// ----------------------------------------------------------------------------
// publishCanonical marshals an events.v1 payload exactly as a real PRODUCER would
// (canonical proto-JSON in the envelope's data) and publishes it on subject with
// the producer's source. Returns the envelope id so a test can re-publish the SAME
// id to exercise transport-layer idempotency. For the choreography reactor the
// payload is OPAQUE — the reactor never decodes it — so any proto message works as
// the body; we use it to model "some platform event arrived".
// ----------------------------------------------------------------------------
func publishCanonical(t *testing.T, js jetstream.JetStream, subject, source string, payload proto.Message) {
	t.Helper()
	raw, err := protojson.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	pub := natsutil.NewPublisher(js, source)
	if err := pub.Publish(context.Background(), subject, json.RawMessage(raw)); err != nil {
		t.Fatalf("publish %s: %v", subject, err)
	}
}

// ----------------------------------------------------------------------------
// buildEnvelope / rawPublish let a test publish with a CONTROLLED, REUSED envelope
// id so the duplicate-redelivery test can put the SAME Nats-Msg-Id on the bus twice
// (the standard natsutil.Publisher mints a fresh UUID per publish, so it cannot
// reuse an id). We construct the envelope ourselves (matching natsutil's shape:
// canonical proto-JSON in data) and publish the raw bytes with WithMsgID(id) so the
// envelope id, the Nats-Msg-Id, and the consumer dedup key all agree.
// ----------------------------------------------------------------------------
func buildEnvelope(t *testing.T, id, fullSubject, source string, payload proto.Message) natsutil.EventEnvelope {
	t.Helper()
	raw, err := protojson.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	// Derive the prefix-stripped type exactly as natsutil.Publisher would, so the
	// hand-built envelope is indistinguishable from a real one.
	env := natsutil.NewEnvelope(deriveType(fullSubject), source, json.RawMessage(raw))
	env.ID = id
	return env
}

// deriveType mirrors natsutil's deriveEventType: strip the "fp.<svc>." prefix.
//
//	fp.pipelines.failed → failed ; fp.models.drift.detected → drift.detected
func deriveType(subject string) string {
	parts := strings.SplitN(subject, ".", 3)
	if len(parts) >= 3 {
		return parts[2]
	}
	return subject
}

// rawPublish marshals an envelope to JSON and publishes it to subject with a fixed
// Nats-Msg-Id (the envelope id) so a re-publish with the same id is a transport
// duplicate. Uses the JetStream handle directly to control the Msg-Id.
func rawPublish(t *testing.T, js jetstream.JetStream, subject string, env natsutil.EventEnvelope, msgID string) {
	t.Helper()
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	if _, err := js.Publish(context.Background(), subject, b, jetstream.WithMsgID(msgID)); err != nil {
		t.Fatalf("raw publish %s: %v", subject, err)
	}
}

// ============================================================================
// FAKES for the reactor's ports (decode + dedup + dispatch + delivery-health)
// ============================================================================

// fakeRouter is a RecipientRouting that maps EVERY event to one fixed recipient
// with a fixed full subject type, unless declineType matches env.Type (to exercise
// the no-recipient ACK-and-skip path). The router is the platform-policy seam; the
// reactor stays opaque regardless.
type fakeRouter struct {
	recipient   string
	fullType    string
	declineType string // if env.Type == declineType → Route returns ok=false
}

func (f *fakeRouter) Route(env natsutil.EventEnvelope) (events.RoutedRecipient, bool) {
	if f.declineType != "" && env.Type == f.declineType {
		return events.RoutedRecipient{}, false
	}
	return events.RoutedRecipient{
		RecipientUserID: f.recipient,
		FullType:        f.fullType,
		Title:           "rendered title",
		Body:            "rendered body",
	}, true
}

// fakePrefs is a PreferenceLoader returning fixed prefs (default fall-back built in).
type fakePrefs struct {
	prefs domain.NotificationPreferences
}

func (f *fakePrefs) LoadPreferences(_ context.Context, userID string) (domain.NotificationPreferences, error) {
	p := f.prefs
	if p.UserID == "" {
		p.UserID = userID
	}
	return p, nil
}

// fakeExecutor records every Execute call (so a test asserts SINGLE-effect under
// duplicate redelivery) and returns a configurable result/error. failN makes the
// first N calls fail with a PERMANENT (poison) error so the message heads to DLQ;
// after failN calls it succeeds, modelling a transient-then-recover or a forever-poison.
type fakeExecutor struct {
	mu         sync.Mutex
	calls      []domain.RoutingDecision
	events     []domain.InboundEvent
	result     events.ExecutionResult
	failalways bool // every Execute returns a poison error
	failN      int  // first N Execute calls fail (poison); rest succeed
	n          int  // call counter
}

func (f *fakeExecutor) Execute(_ context.Context, decision domain.RoutingDecision, ev domain.InboundEvent) (events.ExecutionResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	f.calls = append(f.calls, decision)
	f.events = append(f.events, ev)
	if f.failalwaysOrFirstN() {
		// Wrap ErrProcessingFailed so natsutil treats it as a permanent fault and,
		// after the retry cap, routes to the DLQ rather than NAK-looping forever.
		return events.ExecutionResult{}, errPoison
	}
	return f.result, nil
}

func (f *fakeExecutor) failalwaysOrFirstN() bool {
	if f.failalways {
		return true
	}
	return f.n <= f.failN
}

func (f *fakeExecutor) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeExecutor) lastEvent() (domain.InboundEvent, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.events) == 0 {
		return domain.InboundEvent{}, false
	}
	return f.events[len(f.events)-1], true
}

// errPoison is a permanent fault: wrapping natsutil.ErrProcessingFailed signals the
// subscriber to send the message to the DLQ after the retry cap (vs a transient
// error that should be retried indefinitely).
var errPoison = wrapProcessing("executor permanently failed")

func wrapProcessing(msg string) error { return &poisonErr{msg: msg} }

type poisonErr struct{ msg string }

func (e *poisonErr) Error() string { return "processing failed: " + e.msg }
func (e *poisonErr) Unwrap() error { return natsutil.ErrProcessingFailed }

// fakeHealth records the delivery-health events the reactor publishes (so a test
// asserts the delivered/failed feed fires per external delivery).
type fakeHealth struct {
	mu        sync.Mutex
	delivered []events.DeliveredInput
	failed    []events.FailedInput
}

func (f *fakeHealth) PublishDelivered(_ context.Context, in events.DeliveredInput) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delivered = append(f.delivered, in)
	return nil
}

func (f *fakeHealth) PublishFailed(_ context.Context, in events.FailedInput) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed = append(f.failed, in)
	return nil
}

func (f *fakeHealth) deliveredCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.delivered)
}

func (f *fakeHealth) failedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.failed)
}

// realService builds a real domain.NotificationService so ReactToEvent is exercised
// for real (the reactor's job is to drive the REAL brain, not a stubbed one). The
// brain is pure, so the unused ports can be nil — ReactToEvent touches none of them.
func realService() domain.NotificationService {
	return domain.NewNotificationService(nil, nil, nil, nil, nil, nil)
}

// fullPrefs is a recipient with webhook + slack + email enabled so a decision
// exercises external channels (which produce delivery-health events).
func fullPrefs(user string) domain.NotificationPreferences {
	return domain.NotificationPreferences{
		UserID: user,
		Channels: []domain.ChannelPreference{
			{Channel: domain.ChannelWebhook, Enabled: true, MinSeverity: domain.SeverityUnspecified, Target: "https://hooks.example.com/x"},
			{Channel: domain.ChannelSlack, Enabled: true, MinSeverity: domain.SeverityUnspecified, Target: "https://hooks.slack.com/services/T/B/x"},
		},
	}
}
