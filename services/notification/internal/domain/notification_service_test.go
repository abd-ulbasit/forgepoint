// notification_service_test.go — TDD unit tests for the choreography reactor and
// the control-plane methods.
//
// ============================================================================
// WHAT THESE TESTS PROVE (real behavior, not mock choreography)
// ============================================================================
//
// The centerpiece is ReactToEvent — the pure routing brain. Its tests assert the
// ACTUAL decisions (which channels deliver, which are suppressed and WHY, what
// severity was derived, whether an event was muted) by inspecting the returned
// RoutingDecision value — no mocks needed for the brain, because it is pure. That
// is the payoff of "functional core, imperative shell": the hardest logic is the
// easiest to test.
//
// The control-plane tests use hand-written mock ports (no testing framework, no
// gomock) and verify REAL outcomes: the stored notification actually has the
// server-set fields; MarkRead actually reports the right count; UpdatePreferences
// actually REJECTS an SSRF target before it reaches the repo (we assert the repo
// was never called); GetNotification hides another user's row as NotFound.
//
// Severity-derivation, mute matching, the unspecified-floor rule, and the SSRF
// pre-checks are each pinned with table tests so a future refactor that breaks
// one of these business invariants fails loudly.
package domain

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// ============================================================================
// HAND-WRITTEN MOCK PORTS
// ============================================================================
//
// WHY hand-written rather than a mock-gen library: the tests verify
// REAL behavior. A hand-written stub backed by a map (mockNotificationRepo) lets
// us assert the actual stored state, not just "Create was called with X". Each
// mock also records call counts so we can prove a NEGATIVE — e.g. that an SSRF
// rejection short-circuits BEFORE Upsert is ever invoked.

// fixedClock returns a constant time so timestamp assertions are exact.
type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

// seqIDGen returns deterministic ids ("id-1", "id-2", ...) so a test can assert
// the exact id written.
type seqIDGen struct{ n int }

func (g *seqIDGen) NewID() string {
	g.n++
	return "id-" + itoa(g.n)
}

// itoa is a tiny stdlib-free int→string (strconv would do; this keeps the helper
// obvious and dependency-light for the test).
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// mockNotificationRepo is a map-backed NotificationRepository that records real
// state so assertions inspect what was actually stored.
type mockNotificationRepo struct {
	byID     map[string]Notification // keyed by notification id
	attempts []DeliveryAttempt

	createCalls int
	appendCalls int
}

func newMockNotificationRepo() *mockNotificationRepo {
	return &mockNotificationRepo{byID: map[string]Notification{}}
}

func (m *mockNotificationRepo) Create(_ context.Context, n Notification) (Notification, error) {
	m.createCalls++
	m.byID[n.ID] = n
	return n, nil
}

func (m *mockNotificationRepo) GetByIDForUser(_ context.Context, id, user string) (Notification, error) {
	n, ok := m.byID[id]
	if !ok || n.RecipientUserID != user {
		// CRUCIAL anti-IDOR behavior: a foreign row is indistinguishable from a
		// missing one at this port — same ErrRepoNotFound.
		return Notification{}, ErrRepoNotFound
	}
	return n, nil
}

func (m *mockNotificationRepo) ListForUser(_ context.Context, user string, opts ListOptions) ([]Notification, string, error) {
	var out []Notification
	for _, n := range m.byID {
		if n.RecipientUserID != user {
			continue
		}
		if opts.UnreadOnly && n.Read {
			continue
		}
		if !n.Severity.MeetsFloor(opts.MinSeverity) {
			continue
		}
		if opts.EventTypeFilter != "" && n.EventType != opts.EventTypeFilter {
			continue
		}
		out = append(out, n)
	}
	return out, "", nil
}

func (m *mockNotificationRepo) CountUnread(_ context.Context, user string) (int, error) {
	c := 0
	for _, n := range m.byID {
		if n.RecipientUserID == user && !n.Read {
			c++
		}
	}
	return c, nil
}

func (m *mockNotificationRepo) MarkRead(_ context.Context, user string, ids []string, now time.Time) (int, error) {
	marked := 0
	for _, id := range ids {
		n, ok := m.byID[id]
		if !ok || n.RecipientUserID != user || n.Read {
			continue // foreign / unknown / already-read → not counted (idempotent)
		}
		n.Read = true
		n.ReadAt = now
		m.byID[id] = n
		marked++
	}
	return marked, nil
}

func (m *mockNotificationRepo) MarkAllRead(_ context.Context, user string, now time.Time) (int, error) {
	marked := 0
	for id, n := range m.byID {
		if n.RecipientUserID == user && !n.Read {
			n.Read = true
			n.ReadAt = now
			m.byID[id] = n
			marked++
		}
	}
	return marked, nil
}

func (m *mockNotificationRepo) AppendDeliveryAttempt(_ context.Context, a DeliveryAttempt) error {
	m.appendCalls++
	m.attempts = append(m.attempts, a)
	return nil
}

func (m *mockNotificationRepo) GetAttemptsForNotification(_ context.Context, user, nid string) ([]DeliveryAttempt, error) {
	n, ok := m.byID[nid]
	if !ok || n.RecipientUserID != user {
		return nil, ErrRepoNotFound
	}
	var out []DeliveryAttempt
	for _, a := range m.attempts {
		if a.NotificationID == nid {
			out = append(out, a)
		}
	}
	return out, nil
}

func (m *mockNotificationRepo) ListDeliveryAttemptsForUser(_ context.Context, user string, opts ListOptions) ([]DeliveryAttempt, string, error) {
	var out []DeliveryAttempt
	for _, a := range m.attempts {
		n, ok := m.byID[a.NotificationID]
		if !ok || n.RecipientUserID != user {
			continue
		}
		if opts.Channel != ChannelUnspecified && a.Channel != opts.Channel {
			continue
		}
		if opts.Status != StatusUnspecified && a.Status != opts.Status {
			continue
		}
		if opts.NotificationID != "" && a.NotificationID != opts.NotificationID {
			continue
		}
		out = append(out, a)
	}
	return out, "", nil
}

// mockPrefRepo is a map-backed PreferenceRepository.
type mockPrefRepo struct {
	byUser      map[string]NotificationPreferences
	upsertCalls int
}

func newMockPrefRepo() *mockPrefRepo {
	return &mockPrefRepo{byUser: map[string]NotificationPreferences{}}
}

func (m *mockPrefRepo) GetByUser(_ context.Context, user string) (NotificationPreferences, error) {
	p, ok := m.byUser[user]
	if !ok {
		return NotificationPreferences{}, ErrRepoNotFound
	}
	return p, nil
}

func (m *mockPrefRepo) Upsert(_ context.Context, p NotificationPreferences) (NotificationPreferences, error) {
	m.upsertCalls++
	m.byUser[p.UserID] = p
	return p, nil
}

// mockIdem is a map-backed IdempotencyStore.
type mockIdem struct {
	seen map[string][]byte
}

func newMockIdem() *mockIdem { return &mockIdem{seen: map[string][]byte{}} }

func (m *mockIdem) Seen(_ context.Context, key string) (bool, []byte, error) {
	v, ok := m.seen[key]
	return ok, v, nil
}

func (m *mockIdem) Record(_ context.Context, key string, result []byte, _ time.Duration) error {
	m.seen[key] = result
	return nil
}

// mockNotifier records the targets it was asked to deliver and returns a scripted
// result. It lets a test assert (a) the SSRF obligation was carried through and
// (b) TestChannel actually invoked delivery on the stored target.
type mockNotifier struct {
	calls  []DeliveryTarget
	result DeliveryResult
	err    error
}

func (m *mockNotifier) Deliver(_ context.Context, t DeliveryTarget) (DeliveryResult, error) {
	m.calls = append(m.calls, t)
	if m.err != nil {
		return DeliveryResult{}, m.err
	}
	return m.result, nil
}

// newTestService wires the impl with all mocks and a fixed clock/idgen.
func newTestService() (*notificationService, *mockNotificationRepo, *mockPrefRepo, *mockIdem, *mockNotifier) {
	nrepo := newMockNotificationRepo()
	prepo := newMockPrefRepo()
	idem := newMockIdem()
	notifier := &mockNotifier{result: DeliveryResult{Status: StatusDelivered, Attempts: 1, ResponseCode: 200}}
	clk := fixedClock{t: time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)}
	svc := NewNotificationService(nrepo, prepo, idem, notifier, clk, &seqIDGen{}).(*notificationService)
	return svc, nrepo, prepo, idem, notifier
}

// ============================================================================
// SEVERITY DERIVATION (server-authoritative — client never sets severity)
// ============================================================================

func TestDeriveSeverity(t *testing.T) {
	cases := []struct {
		eventType string
		want      Severity
	}{
		// CRITICAL: hard-threshold drift, canary/promotion failure.
		{"fp.models.drift.detected", SeverityCritical},
		{"fp.models.drift.critical", SeverityCritical},
		{"fp.pipelines.canary.failed", SeverityCritical},
		// ERROR: failures.
		{"fp.pipelines.failed", SeverityError},
		{"fp.notifications.failed", SeverityError},
		{"fp.inference.failed", SeverityError},
		{"fp.billing.quota.exceeded", SeverityError},
		// WARNING: soft problems.
		{"fp.billing.quota.approaching", SeverityWarning},
		{"fp.models.drift.warning", SeverityWarning},
		// INFO: routine lifecycle.
		{"fp.pipelines.started", SeverityInfo},
		{"fp.models.registered", SeverityInfo},
		{"fp.models.promoted", SeverityInfo},
		// Unknown event → INFO (safe default; never crashes the reactor).
		{"fp.something.brandnew", SeverityInfo},
		{"", SeverityInfo},
	}
	for _, c := range cases {
		if got := deriveSeverity(c.eventType); got != c.want {
			t.Errorf("deriveSeverity(%q) = %d, want %d", c.eventType, got, c.want)
		}
	}
}

// ============================================================================
// PATTERN MATCHING (the wildcard grammar muting + routing share)
// ============================================================================

func TestMatchPattern(t *testing.T) {
	cases := []struct {
		pattern   string
		eventType string
		want      bool
	}{
		// Exact match.
		{"fp.pipelines.failed", "fp.pipelines.failed", true},
		{"fp.pipelines.failed", "fp.pipelines.started", false},
		// Trailing wildcard matches one segment.
		{"fp.pipelines.*", "fp.pipelines.failed", true},
		{"fp.pipelines.*", "fp.pipelines.started", true},
		// '*' is a SINGLE-segment wildcard — it must NOT cross a dot.
		{"fp.pipelines.*", "fp.pipelines.failed.detail", false},
		// '>' is the multi-segment tail wildcard (NATS-style).
		{"fp.>", "fp.pipelines.failed", true},
		{"fp.>", "fp.models.drift.detected", true},
		{"fp.pipelines.>", "fp.pipelines.failed.detail", true},
		// Mismatched prefix.
		{"fp.models.*", "fp.pipelines.failed", false},
		// Wildcard in the middle.
		{"fp.*.failed", "fp.pipelines.failed", true},
		{"fp.*.failed", "fp.models.failed", true},
		{"fp.*.failed", "fp.pipelines.started", false},
		// Segment-count mismatch without '>'.
		{"fp.pipelines", "fp.pipelines.failed", false},
		// Empty pattern matches nothing (defensive).
		{"", "fp.pipelines.failed", false},
	}
	for _, c := range cases {
		if got := matchPattern(c.pattern, c.eventType); got != c.want {
			t.Errorf("matchPattern(%q, %q) = %v, want %v", c.pattern, c.eventType, got, c.want)
		}
	}
}

// ============================================================================
// THE REACTOR — ReactToEvent (the choreography centerpiece)
// ============================================================================

func baseEvent() InboundEvent {
	return InboundEvent{
		EventID:         "evt-1",
		Type:            "fp.pipelines.failed", // → ERROR
		Source:          "pipeline",
		RecipientUserID: "user-1",
		Title:           "Pipeline failed",
		Body:            "nightly-retrain failed at step train",
		Payload:         []byte(`{"pipeline":"nightly-retrain"}`),
		OccurredAt:      time.Date(2026, 6, 17, 11, 0, 0, 0, time.UTC),
	}
}

// A fully-configured recipient: webhook + slack + email all enabled, email gated
// to ERROR+. Lets one decision exercise every branch.
func fullPrefs() NotificationPreferences {
	return NotificationPreferences{
		UserID: "user-1",
		Channels: []ChannelPreference{
			{Channel: ChannelWebhook, Enabled: true, MinSeverity: SeverityUnspecified, Target: "https://hooks.example.com/x"},
			{Channel: ChannelSlack, Enabled: true, MinSeverity: SeverityWarning, Target: "https://hooks.slack.com/services/T/B/x"},
			{Channel: ChannelEmail, Enabled: true, MinSeverity: SeverityError, Target: "ops@example.com"},
		},
	}
}

// findDecision is a test helper to pull one channel's verdict out of the slice.
func findDecision(r RoutingDecision, ch NotificationChannel) (ChannelDecision, bool) {
	for _, d := range r.Channels {
		if d.Channel == ch {
			return d, true
		}
	}
	return ChannelDecision{}, false
}

func TestReactToEvent_DeliversEnabledChannelsAndSeverity(t *testing.T) {
	svc, _, _, _, _ := newTestService()

	got, err := svc.ReactToEvent(context.Background(), baseEvent(), fullPrefs())
	if err != nil {
		t.Fatalf("ReactToEvent error: %v", err)
	}

	// Severity must be SERVER-DERIVED as ERROR (from fp.pipelines.failed).
	if got.Severity != SeverityError {
		t.Fatalf("severity = %d, want ERROR(%d)", got.Severity, SeverityError)
	}
	if got.Muted {
		t.Fatalf("event should not be muted")
	}
	if got.RecipientUserID != "user-1" {
		t.Fatalf("recipient = %q, want user-1", got.RecipientUserID)
	}

	// IN_APP must ALWAYS deliver (implicit always-on inbox).
	if d, ok := findDecision(got, ChannelInApp); !ok || !d.Deliver {
		t.Fatalf("IN_APP must always deliver, got %+v ok=%v", d, ok)
	}
	// WEBHOOK: enabled, no floor → delivers, and its target must be carried.
	d, ok := findDecision(got, ChannelWebhook)
	if !ok || !d.Deliver {
		t.Fatalf("WEBHOOK should deliver, got %+v", d)
	}
	if d.Target != "https://hooks.example.com/x" {
		t.Fatalf("WEBHOOK target not carried: %q", d.Target)
	}
	if !d.RequiresSSRFGuard() {
		t.Fatalf("WEBHOOK decision must be flagged SSRF-guarded")
	}
	// SLACK: enabled, floor WARNING, ERROR >= WARNING → delivers, SSRF-guarded.
	if d, _ := findDecision(got, ChannelSlack); !d.Deliver || !d.RequiresSSRFGuard() {
		t.Fatalf("SLACK should deliver and be SSRF-guarded, got %+v", d)
	}
	// EMAIL: enabled, floor ERROR, ERROR >= ERROR → delivers, NOT SSRF-guarded.
	if d, _ := findDecision(got, ChannelEmail); !d.Deliver {
		t.Fatalf("EMAIL should deliver at ERROR floor, got %+v", d)
	} else if d.RequiresSSRFGuard() {
		t.Fatalf("EMAIL must NOT be SSRF-guarded")
	}

	// And the convenience accessor agrees.
	delivering := got.DeliveringChannels()
	if len(delivering) != 4 {
		t.Fatalf("expected 4 delivering channels (in_app, webhook, slack, email), got %d: %v", len(delivering), delivering)
	}
}

func TestReactToEvent_SuppressesBelowSeverityFloor(t *testing.T) {
	svc, _, _, _, _ := newTestService()

	ev := baseEvent()
	ev.Type = "fp.pipelines.started" // → INFO

	got, err := svc.ReactToEvent(context.Background(), ev, fullPrefs())
	if err != nil {
		t.Fatalf("ReactToEvent error: %v", err)
	}
	if got.Severity != SeverityInfo {
		t.Fatalf("severity = %d, want INFO", got.Severity)
	}
	// IN_APP always delivers (no floor).
	if d, _ := findDecision(got, ChannelInApp); !d.Deliver {
		t.Fatalf("IN_APP should deliver")
	}
	// WEBHOOK has no floor → delivers even for INFO.
	if d, _ := findDecision(got, ChannelWebhook); !d.Deliver {
		t.Fatalf("WEBHOOK (no floor) should deliver INFO")
	}
	// SLACK floor WARNING: INFO < WARNING → SUPPRESSED with the right reason.
	d, _ := findDecision(got, ChannelSlack)
	if d.Deliver {
		t.Fatalf("SLACK should be suppressed for INFO (floor WARNING)")
	}
	if d.Suppressed != SuppressBelowSeverity {
		t.Fatalf("SLACK suppress reason = %q, want %q", d.Suppressed, SuppressBelowSeverity)
	}
	// EMAIL floor ERROR: INFO < ERROR → SUPPRESSED.
	if d, _ := findDecision(got, ChannelEmail); d.Deliver || d.Suppressed != SuppressBelowSeverity {
		t.Fatalf("EMAIL should be suppressed below ERROR, got %+v", d)
	}
}

func TestReactToEvent_SuppressesDisabledChannel(t *testing.T) {
	svc, _, _, _, _ := newTestService()

	prefs := fullPrefs()
	prefs.Channels[0].Enabled = false // disable WEBHOOK

	got, _ := svc.ReactToEvent(context.Background(), baseEvent(), prefs)
	d, _ := findDecision(got, ChannelWebhook)
	if d.Deliver {
		t.Fatalf("disabled WEBHOOK must not deliver")
	}
	if d.Suppressed != SuppressDisabled {
		t.Fatalf("disabled reason = %q, want %q", d.Suppressed, SuppressDisabled)
	}
}

func TestReactToEvent_SuppressesExternalChannelWithNoTarget(t *testing.T) {
	svc, _, _, _, _ := newTestService()

	prefs := fullPrefs()
	prefs.Channels[0].Target = "" // WEBHOOK enabled but no URL

	got, _ := svc.ReactToEvent(context.Background(), baseEvent(), prefs)
	d, _ := findDecision(got, ChannelWebhook)
	if d.Deliver {
		t.Fatalf("WEBHOOK with empty target must not deliver")
	}
	if d.Suppressed != SuppressNoTarget {
		t.Fatalf("no-target reason = %q, want %q", d.Suppressed, SuppressNoTarget)
	}
}

func TestReactToEvent_MuteSilencesEntireEvent(t *testing.T) {
	svc, _, _, _, _ := newTestService()

	prefs := fullPrefs()
	prefs.MutedEventPatterns = []string{"fp.pipelines.*"} // mutes fp.pipelines.failed

	got, err := svc.ReactToEvent(context.Background(), baseEvent(), prefs)
	if err != nil {
		t.Fatalf("ReactToEvent error: %v", err)
	}
	if !got.Muted {
		t.Fatalf("event matching a mute pattern must be Muted")
	}
	// A muted event delivers NOTHING — not even IN_APP.
	if len(got.DeliveringChannels()) != 0 {
		t.Fatalf("muted event must deliver nothing, got %v", got.DeliveringChannels())
	}
	// Each per-channel decision should carry the muted reason for a uniform log.
	for _, d := range got.Channels {
		if d.Suppressed != SuppressMuted {
			t.Fatalf("muted channel %d reason = %q, want %q", d.Channel, d.Suppressed, SuppressMuted)
		}
	}
}

func TestReactToEvent_NonMatchingMuteDoesNotSilence(t *testing.T) {
	svc, _, _, _, _ := newTestService()

	prefs := fullPrefs()
	prefs.MutedEventPatterns = []string{"fp.inference.*"} // does NOT match pipelines

	got, _ := svc.ReactToEvent(context.Background(), baseEvent(), prefs)
	if got.Muted {
		t.Fatalf("non-matching mute must not silence the event")
	}
	if len(got.DeliveringChannels()) == 0 {
		t.Fatalf("expected deliveries when mute doesn't match")
	}
}

// ============================================================================
// CONSUMER-PATH IDEMPOTENCY HELPER (dedup key) — proves the anti-dup contract
// ============================================================================

func TestEventDedupKeyIsStableAndScopedPerRecipient(t *testing.T) {
	k1 := EventDedupKey("evt-1", "user-1")
	k2 := EventDedupKey("evt-1", "user-1")
	k3 := EventDedupKey("evt-1", "user-2")
	if k1 != k2 {
		t.Fatalf("dedup key must be stable for same (event,recipient)")
	}
	if k1 == k3 {
		t.Fatalf("dedup key must differ per recipient (same event, two users)")
	}
	if !strings.Contains(k1, "evt-1") || !strings.Contains(k1, "user-1") {
		t.Fatalf("dedup key should embed both ids, got %q", k1)
	}
}

// ============================================================================
// INBOX — ListNotifications / GetNotification / MarkRead
// ============================================================================

// seedNotification creates a notification row directly in the repo for read tests.
func seedNotification(repo *mockNotificationRepo, id, user string, read bool, sev Severity, etype string) {
	repo.byID[id] = Notification{
		ID: id, RecipientUserID: user, Read: read, Severity: sev, EventType: etype,
		CreatedAt: time.Now(),
	}
}

func TestListNotifications_ScopedToCallerWithUnreadCount(t *testing.T) {
	svc, nrepo, _, _, _ := newTestService()
	seedNotification(nrepo, "n1", "user-1", false, SeverityError, "fp.pipelines.failed")
	seedNotification(nrepo, "n2", "user-1", true, SeverityInfo, "fp.pipelines.started")
	seedNotification(nrepo, "n3", "user-2", false, SeverityError, "fp.pipelines.failed") // other user

	out, err := svc.ListNotifications(context.Background(), ListNotificationsInput{RecipientUserID: "user-1"})
	if err != nil {
		t.Fatalf("ListNotifications error: %v", err)
	}
	if len(out.Notifications) != 2 {
		t.Fatalf("expected 2 notifications for user-1, got %d", len(out.Notifications))
	}
	for _, n := range out.Notifications {
		if n.RecipientUserID != "user-1" {
			t.Fatalf("leaked another user's notification: %+v", n)
		}
	}
	if out.UnreadCount != 1 {
		t.Fatalf("unread count = %d, want 1", out.UnreadCount)
	}
}

func TestListNotifications_CapsPageSize(t *testing.T) {
	svc, _, _, _, _ := newTestService()
	// Over the max → capped to 100. Negative/zero → default 20.
	for _, c := range []struct{ in, want int }{
		{0, defaultPageSize},
		{-5, defaultPageSize},
		{50, 50},
		{1000, maxPageSize},
	} {
		got := svc.clampPageSize(c.in)
		if got != c.want {
			t.Errorf("clampPageSize(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestGetNotification_ForeignIDIsNotFound(t *testing.T) {
	svc, nrepo, _, _, _ := newTestService()
	seedNotification(nrepo, "n1", "user-2", false, SeverityError, "fp.pipelines.failed")

	// user-1 asking for user-2's notification must get ErrNotFound (no leak).
	_, err := svc.GetNotification(context.Background(), "user-1", "n1")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign id should be ErrNotFound, got %v", err)
	}
}

func TestGetNotification_ReturnsAttempts(t *testing.T) {
	svc, nrepo, _, _, _ := newTestService()
	seedNotification(nrepo, "n1", "user-1", false, SeverityError, "fp.pipelines.failed")
	nrepo.attempts = []DeliveryAttempt{
		{NotificationID: "n1", Channel: ChannelWebhook, Status: StatusDelivered, Attempt: 1, ResponseCode: 200},
		{NotificationID: "n1", Channel: ChannelSlack, Status: StatusFailed, Attempt: 3, ResponseCode: 503},
	}
	out, err := svc.GetNotification(context.Background(), "user-1", "n1")
	if err != nil {
		t.Fatalf("GetNotification error: %v", err)
	}
	if out.Notification.ID != "n1" {
		t.Fatalf("wrong notification returned: %+v", out.Notification)
	}
	if len(out.DeliveryAttempts) != 2 {
		t.Fatalf("expected 2 delivery attempts, got %d", len(out.DeliveryAttempts))
	}
}

func TestMarkRead_CountsOnlyRealTransitions(t *testing.T) {
	svc, nrepo, _, _, _ := newTestService()
	seedNotification(nrepo, "n1", "user-1", false, SeverityInfo, "x") // unread → will flip
	seedNotification(nrepo, "n2", "user-1", true, SeverityInfo, "x")  // already read → no flip
	seedNotification(nrepo, "n3", "user-2", false, SeverityInfo, "x") // foreign → ignored

	out, err := svc.MarkRead(context.Background(), MarkReadInput{
		RecipientUserID: "user-1",
		Ids:             []string{"n1", "n2", "n3", "missing"},
	})
	if err != nil {
		t.Fatalf("MarkRead error: %v", err)
	}
	if out.MarkedCount != 1 {
		t.Fatalf("marked = %d, want 1 (only n1 transitioned)", out.MarkedCount)
	}
	// n1 must ACTUALLY be read now with the injected clock time.
	if n := nrepo.byID["n1"]; !n.Read || !n.ReadAt.Equal(time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("n1 not actually marked read with clock time: %+v", n)
	}
	if out.UnreadCount != 0 {
		t.Fatalf("unread after = %d, want 0", out.UnreadCount)
	}
}

func TestMarkRead_MarkAll(t *testing.T) {
	svc, nrepo, _, _, _ := newTestService()
	seedNotification(nrepo, "n1", "user-1", false, SeverityInfo, "x")
	seedNotification(nrepo, "n2", "user-1", false, SeverityInfo, "x")

	out, err := svc.MarkRead(context.Background(), MarkReadInput{RecipientUserID: "user-1", MarkAll: true})
	if err != nil {
		t.Fatalf("MarkRead(all) error: %v", err)
	}
	if out.MarkedCount != 2 || out.UnreadCount != 0 {
		t.Fatalf("MarkAll marked=%d unread=%d, want 2/0", out.MarkedCount, out.UnreadCount)
	}
}

func TestMarkRead_RejectsOversizedBatch(t *testing.T) {
	svc, _, _, _, _ := newTestService()
	ids := make([]string, maxMarkReadBatch+1)
	for i := range ids {
		ids[i] = "x"
	}
	_, err := svc.MarkRead(context.Background(), MarkReadInput{RecipientUserID: "user-1", Ids: ids})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("oversized batch should be ErrValidation, got %v", err)
	}
}

// ============================================================================
// PREFERENCES — Get / Update (with SSRF validation) / defaults
// ============================================================================

func TestGetPreferences_ReturnsDefaultsWhenUnset(t *testing.T) {
	svc, _, _, _, _ := newTestService()
	prefs, err := svc.GetPreferences(context.Background(), "new-user")
	if err != nil {
		t.Fatalf("GetPreferences error: %v", err)
	}
	if prefs.UserID != "new-user" {
		t.Fatalf("default prefs user = %q, want new-user", prefs.UserID)
	}
	// Default = IN_APP enabled, nothing external.
	inapp, ok := prefs.ChannelFor(ChannelInApp)
	if !ok || !inapp.Enabled {
		t.Fatalf("default prefs must have IN_APP enabled")
	}
}

func TestUpdatePreferences_ValidWebhookPersists(t *testing.T) {
	svc, _, prepo, _, _ := newTestService()
	out, err := svc.UpdatePreferences(context.Background(), UpdatePreferencesInput{
		UserID: "user-1",
		Channels: []ChannelPreference{
			{Channel: ChannelWebhook, Enabled: true, Target: "https://hooks.example.com/abc"},
			{Channel: ChannelSlack, Enabled: true, Target: "https://hooks.slack.com/services/T/B/C"},
			{Channel: ChannelEmail, Enabled: true, Target: "me@example.com"},
		},
	})
	if err != nil {
		t.Fatalf("UpdatePreferences error: %v", err)
	}
	if prepo.upsertCalls != 1 {
		t.Fatalf("expected one upsert, got %d", prepo.upsertCalls)
	}
	// UpdatedAt must be SERVER-stamped from the clock (not client-set).
	if !out.UpdatedAt.Equal(time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("UpdatedAt not server-stamped: %v", out.UpdatedAt)
	}
	// UserID must be the authenticated caller (server-set).
	if out.UserID != "user-1" {
		t.Fatalf("UserID = %q, want user-1", out.UserID)
	}
}

// THE SECURITY-CRITICAL TEST: an SSRF-shaped webhook target must be REJECTED
// before it ever reaches storage. We assert the repo's Upsert was NEVER called.
func TestUpdatePreferences_RejectsSSRFTargetsBeforeStorage(t *testing.T) {
	cases := []struct {
		name   string
		target string
	}{
		{"plain http (not https)", "http://hooks.example.com/x"},
		{"loopback host", "https://localhost/x"},
		{"loopback ip", "https://127.0.0.1/x"},
		{"ipv6 loopback", "https://[::1]/x"},
		{"metadata link-local", "https://169.254.169.254/latest/meta-data"},
		{"private 10.x", "https://10.0.0.5/x"},
		{"private 192.168", "https://192.168.1.1/x"},
		{"private 172.16", "https://172.16.0.1/x"},
		{"file scheme", "file:///etc/passwd"},
		{"userinfo credentials", "https://user:pass@hooks.example.com/x"},
		{"empty host", "https:///x"},
		{"not a url", "::::::"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			svc, _, prepo, _, _ := newTestService()
			_, err := svc.UpdatePreferences(context.Background(), UpdatePreferencesInput{
				UserID:   "user-1",
				Channels: []ChannelPreference{{Channel: ChannelWebhook, Enabled: true, Target: c.target}},
			})
			if err == nil {
				t.Fatalf("expected rejection for %q, got nil", c.target)
			}
			if !errors.Is(err, ErrSSRFTargetRejected) && !errors.Is(err, ErrValidation) {
				t.Fatalf("expected SSRF/validation error for %q, got %v", c.target, err)
			}
			// THE KEY ASSERTION: nothing reached storage.
			if prepo.upsertCalls != 0 {
				t.Fatalf("SSRF target %q reached storage (upsertCalls=%d)", c.target, prepo.upsertCalls)
			}
		})
	}
}

func TestUpdatePreferences_RejectsSlackHostNotAllowlisted(t *testing.T) {
	svc, _, prepo, _, _ := newTestService()
	// A SLACK channel whose host is NOT hooks.slack.com must be rejected even
	// though the URL is otherwise a valid public https URL.
	_, err := svc.UpdatePreferences(context.Background(), UpdatePreferencesInput{
		UserID:   "user-1",
		Channels: []ChannelPreference{{Channel: ChannelSlack, Enabled: true, Target: "https://evil.example.com/webhook"}},
	})
	if err == nil {
		t.Fatalf("expected rejection for non-Slack host")
	}
	if prepo.upsertCalls != 0 {
		t.Fatalf("non-allowlisted Slack host reached storage")
	}
}

func TestUpdatePreferences_RejectsBadEmail(t *testing.T) {
	svc, _, prepo, _, _ := newTestService()
	_, err := svc.UpdatePreferences(context.Background(), UpdatePreferencesInput{
		UserID:   "user-1",
		Channels: []ChannelPreference{{Channel: ChannelEmail, Enabled: true, Target: "not-an-email"}},
	})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("bad email should be ErrValidation, got %v", err)
	}
	if prepo.upsertCalls != 0 {
		t.Fatalf("bad email reached storage")
	}
}

func TestUpdatePreferences_RejectsTooManyMutePatterns(t *testing.T) {
	svc, _, prepo, _, _ := newTestService()
	patterns := make([]string, maxMutePatterns+1)
	for i := range patterns {
		patterns[i] = "fp.x.*"
	}
	_, err := svc.UpdatePreferences(context.Background(), UpdatePreferencesInput{
		UserID:             "user-1",
		MutedEventPatterns: patterns,
	})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("oversized mute list should be ErrValidation, got %v", err)
	}
	if prepo.upsertCalls != 0 {
		t.Fatalf("oversized mute list reached storage")
	}
}

func TestUpdatePreferences_IdempotencyReturnsFirstResultWithoutReapplying(t *testing.T) {
	svc, _, prepo, _, _ := newTestService()
	in := UpdatePreferencesInput{
		UserID:         "user-1",
		Channels:       []ChannelPreference{{Channel: ChannelEmail, Enabled: true, Target: "me@example.com"}},
		IdempotencyKey: "key-123",
	}
	if _, err := svc.UpdatePreferences(context.Background(), in); err != nil {
		t.Fatalf("first update error: %v", err)
	}
	// Second call, same key → must NOT upsert again (returns the first result).
	if _, err := svc.UpdatePreferences(context.Background(), in); err != nil {
		t.Fatalf("second update error: %v", err)
	}
	if prepo.upsertCalls != 1 {
		t.Fatalf("idempotent retry re-applied: upsertCalls=%d, want 1", prepo.upsertCalls)
	}
}

// ============================================================================
// TestChannel — owner self-test (no target URL → not an SSRF probe)
// ============================================================================

func TestTestChannel_DeliversToStoredTarget(t *testing.T) {
	svc, _, prepo, _, notifier := newTestService()
	// Seed the caller's stored, already-validated webhook preference.
	prepo.byUser["user-1"] = NotificationPreferences{
		UserID:   "user-1",
		Channels: []ChannelPreference{{Channel: ChannelWebhook, Enabled: true, Target: "https://hooks.example.com/x"}},
	}
	out, err := svc.TestChannel(context.Background(), TestChannelInput{UserID: "user-1", Channel: ChannelWebhook})
	if err != nil {
		t.Fatalf("TestChannel error: %v", err)
	}
	if out.Status != StatusDelivered {
		t.Fatalf("test status = %d, want DELIVERED", out.Status)
	}
	// The notifier must have been asked to deliver to the STORED target (and to
	// carry the SSRF obligation through) — proving TestChannel uses the stored,
	// already-validated target, not a client-supplied URL.
	if len(notifier.calls) != 1 {
		t.Fatalf("expected one delivery, got %d", len(notifier.calls))
	}
	if notifier.calls[0].Target != "https://hooks.example.com/x" {
		t.Fatalf("delivered to %q, want stored target", notifier.calls[0].Target)
	}
	if !notifier.calls[0].MustSSRFValidate() {
		t.Fatalf("webhook test delivery must carry SSRF obligation")
	}
}

func TestTestChannel_UnconfiguredChannelFails(t *testing.T) {
	svc, _, prepo, _, _ := newTestService()
	prepo.byUser["user-1"] = NotificationPreferences{UserID: "user-1"} // no channels

	_, err := svc.TestChannel(context.Background(), TestChannelInput{UserID: "user-1", Channel: ChannelSlack})
	if !errors.Is(err, ErrChannelNotConfigured) {
		t.Fatalf("expected ErrChannelNotConfigured, got %v", err)
	}
}

func TestTestChannel_InApp_IsNoOpSuccess(t *testing.T) {
	svc, _, prepo, _, notifier := newTestService()
	prepo.byUser["user-1"] = NotificationPreferences{UserID: "user-1"}

	out, err := svc.TestChannel(context.Background(), TestChannelInput{UserID: "user-1", Channel: ChannelInApp})
	if err != nil {
		t.Fatalf("IN_APP test error: %v", err)
	}
	if out.Status != StatusDelivered {
		t.Fatalf("IN_APP test should be DELIVERED no-op, got %d", out.Status)
	}
	// IN_APP must NOT touch the external notifier (the inbox is always reachable).
	if len(notifier.calls) != 0 {
		t.Fatalf("IN_APP test must not call the external notifier")
	}
}

// ============================================================================
// compile-time interface assertion (kept in the test so the impl always
// satisfies the port even before the handler exists)
// ============================================================================

var _ NotificationService = (*notificationService)(nil)
