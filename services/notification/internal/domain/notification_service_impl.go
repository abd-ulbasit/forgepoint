// notification_service_impl.go — the concrete NotificationService implementation.
//
// ============================================================================
// CLEAN ARCHITECTURE PLACEMENT
// ============================================================================
//
// notificationService lives in the domain package and depends ONLY on:
//   - the repository/Notifier/Clock/IDGenerator PORTS (ports.go), and
//   - stdlib (net/url for SSRF pre-checks, net for IP parsing, strings) +
//     github.com/google/uuid.
//
// It imports NO gRPC, NO NATS, NO database driver, NO generated proto. The
// handler injects the real Postgres/HTTP adapters at wire time; tests inject
// mocks. Dependency inversion: the business logic dictates the port contracts;
// infrastructure conforms.
//
// IMPORTANT — net/url & net are STDLIB, so they DO NOT violate the purity rule
// (the rule forbids grpc/nats/sql/gen-proto, not the standard library). We use
// them ONLY for the PURE, network-free parts of SSRF validation: parsing the URL
// and classifying an IP LITERAL. We never resolve DNS or open a socket here —
// that (and the pinned-IP connect) is the adapter's job behind the Notifier port.
//
// ============================================================================
// PATTERN — CHOREOGRAPHY (pure event reactor), realized in ReactToEvent
// ============================================================================
//
// ReactToEvent is the brain. It is a PURE function of (event, preferences):
//
//  1. derive severity from event.Type (server-authoritative; client never sets)
//  2. if any mute pattern matches event.Type → Muted, deliver NOTHING
//  3. else, for each channel the user configured (+ implicit IN_APP), decide
//     DELIVER vs SUPPRESSED(reason): disabled? below floor? no target?
//
// No I/O, no clock, no mocks needed to test it. The consumer ADAPTER executes the
// returned decision (write inbox row, fire Notifier, log SUPPRESSED). The dedup
// against NATS redelivery (EventDedupKey + IdempotencyStore) also lives in the
// adapter, BEFORE it executes the decision — see EventDedupKey's doc.
package domain

import (
	"context"
	"errors"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ============================================================================
// TUNABLES (bounds the security defaults require; named so tests pin them)
// ============================================================================

const (
	// defaultPageSize / maxPageSize bound every list. The SERVICE enforces the cap
	// regardless of what the client sends — a client can never demand an unbounded
	// page (a DoS / memory-exhaustion vector). Mirrors the proto's documented
	// 20/100 contract.
	defaultPageSize = 20
	maxPageSize     = 100

	// maxMarkReadBatch bounds a single MarkRead write (the proto says 1000). An
	// unbounded id list would let one call issue an arbitrarily large UPDATE.
	maxMarkReadBatch = 1000

	// maxMutePatterns bounds the mute list (the proto says 100). A runaway mute
	// list is both storage abuse and a sign of misuse; real muting needs a handful.
	maxMutePatterns = 100

	// idempotencyTTL is how long a sync idempotency key is remembered. Long enough
	// to cover realistic client-retry windows; not forever (dedup is a bounded-time
	// guarantee). The async event-dedup TTL is the consumer adapter's concern.
	idempotencyTTL = 24 * time.Hour

	// slackWebhookHost is the ONLY host a SLACK channel target may point at. Slack
	// incoming webhooks are always on this host; pinning it means a "Slack" channel
	// can never be turned into an SSRF probe of an arbitrary host.
	slackWebhookHost = "hooks.slack.com"
)

// ============================================================================
// notificationService — the production impl (unexported; callers get the iface)
// ============================================================================

type notificationService struct {
	notifications NotificationRepository
	prefs         PreferenceRepository
	idem          IdempotencyStore
	notifier      Notifier
	clock         Clock
	ids           IDGenerator
}

// NewNotificationService wires the ports and returns the NotificationService
// interface (programming-to-the-interface; callers can't reach private fields).
// The compile-time assertion below guarantees *notificationService satisfies the
// port, so a signature drift fails the build rather than a call site.
func NewNotificationService(
	notifications NotificationRepository,
	prefs PreferenceRepository,
	idem IdempotencyStore,
	notifier Notifier,
	clock Clock,
	ids IDGenerator,
) NotificationService {
	return &notificationService{
		notifications: notifications,
		prefs:         prefs,
		idem:          idem,
		notifier:      notifier,
		clock:         clock,
		ids:           ids,
	}
}

var _ NotificationService = (*notificationService)(nil)

// ============================================================================
// THE REACTOR — ReactToEvent (pure; the choreography centerpiece)
// ============================================================================

func (s *notificationService) ReactToEvent(_ context.Context, event InboundEvent, prefs NotificationPreferences) (RoutingDecision, error) {
	// (1) SERVER-AUTHORITATIVE severity, derived from the event TYPE alone. The
	// client/producer never sets severity — this is the anti-mass-assignment rule
	// for a derived field. deriveSeverity is a pure string classifier (below).
	sev := deriveSeverity(event.Type)

	decision := RoutingDecision{
		RecipientUserID: event.RecipientUserID,
		EventID:         event.EventID,
		EventType:       event.Type,
		Source:          event.Source,
		Severity:        sev,
	}

	// (2) MUTE — the coarsest gate. If ANY mute pattern matches the event type,
	// the WHOLE event is silenced: nothing delivers, not even IN_APP. We still emit
	// a per-channel decision (SuppressMuted) for each configured channel + IN_APP
	// so the delivery log uniformly records "we chose not to notify" rather than a
	// gap. WHY mute beats everything: the user explicitly said "never bother me
	// about this", which must override even a CRITICAL severity.
	if muted := matchesAnyPattern(prefs.MutedEventPatterns, event.Type); muted {
		decision.Muted = true
		for _, ch := range s.candidateChannels(prefs) {
			decision.Channels = append(decision.Channels, ChannelDecision{
				Channel:    ch,
				Deliver:    false,
				Suppressed: SuppressMuted,
			})
		}
		return decision, nil
	}

	// (3) PER-CHANNEL routing. For each candidate channel (every configured one
	// plus the implicit IN_APP), decide DELIVER vs SUPPRESSED(reason). The order of
	// checks matters and is interview-relevant:
	//   disabled  → SUPPRESSED(disabled)      (user turned it off)
	//   below floor → SUPPRESSED(below_severity)
	//   external & no target → SUPPRESSED(no_target)
	//   else → DELIVER (carry the target so the adapter knows where + SSRF flag)
	for _, ch := range s.candidateChannels(prefs) {
		cp, configured := prefs.ChannelFor(ch)
		d := ChannelDecision{Channel: ch}

		switch {
		case !configured || !cp.Enabled:
			// IN_APP is special-cased to always-on by ChannelFor, so this branch
			// only fires for explicitly-disabled or unconfigured external channels.
			d.Deliver = false
			d.Suppressed = SuppressDisabled
		case !sev.MeetsFloor(cp.MinSeverity):
			d.Deliver = false
			d.Suppressed = SuppressBelowSeverity
		case ch.IsExternal() && strings.TrimSpace(cp.Target) == "":
			// Enabled, clears the floor, but there's nowhere to send it. We record
			// SUPPRESSED(no_target) rather than erroring the whole event — a missing
			// URL is a config gap, not a reason to drop the inbox notification.
			d.Deliver = false
			d.Suppressed = SuppressNoTarget
		default:
			d.Deliver = true
			d.Suppressed = SuppressNone
			if ch.IsExternal() {
				// Carry the resolved target so the adapter knows WHERE to deliver
				// and, via ChannelDecision.RequiresSSRFGuard(), that it MUST
				// SSRF-validate before connecting. The domain does not connect; it
				// hands the adapter both the destination and the obligation.
				d.Target = cp.Target
			}
		}
		decision.Channels = append(decision.Channels, d)
	}

	return decision, nil
}

// candidateChannels returns the channels to evaluate for an event: the implicit
// IN_APP first (always-on inbox), then each channel the user configured (skipping
// a duplicate IN_APP if they configured one explicitly). Keeping this in one place
// guarantees IN_APP is always considered, even for a user with no preferences.
func (s *notificationService) candidateChannels(prefs NotificationPreferences) []NotificationChannel {
	out := []NotificationChannel{ChannelInApp}
	seen := map[NotificationChannel]bool{ChannelInApp: true}
	for _, cp := range prefs.Channels {
		if cp.Channel == ChannelUnspecified || seen[cp.Channel] {
			continue
		}
		seen[cp.Channel] = true
		out = append(out, cp.Channel)
	}
	return out
}

// EventDedupKey builds the idempotency key the CONSUMER ADAPTER uses to dedup a
// NATS redelivery before executing a RoutingDecision. WHY keyed on
// (eventID, recipient): the SAME event may legitimately notify several recipients
// (one inbox row each), so the dedup unit is per-recipient, not per-event. A
// redelivered (eventID, recipient) pair must not create a second inbox row.
//
// WHY this lives in the domain (not the adapter): the key SHAPE is a business
// rule (per-recipient dedup), and the matching Notification.EventID field is a
// domain field. Defining the key here keeps the rule testable and authoritative;
// the adapter just calls IdempotencyStore.Seen(EventDedupKey(...)). Interview note:
// this is the "idempotent consumer" platform rule made concrete — at-least-once
// delivery is made effectively-once by a dedup key derived from the event id.
func EventDedupKey(eventID, recipientUserID string) string {
	return "event:" + eventID + ":" + recipientUserID
}

// ============================================================================
// SEVERITY DERIVATION (pure string classifier — server-authoritative)
// ============================================================================
//
// WHY a string classifier and not a lookup table of every event type: the whole
// point of choreography is that this service has ZERO per-event-type knowledge.
// A new event type must not require a code change. So we classify by the SHAPE of
// the subject (its suffix/keywords), not by enumerating types. This stays correct
// for events that don't exist yet — `fp.anything.failed` is ERROR without us ever
// having heard of `anything`.
//
// Ordering matters: CRITICAL signals are checked before generic "failed" so a
// canary failure or hard-threshold drift outranks a plain failure.
func deriveSeverity(eventType string) Severity {
	t := strings.ToLower(eventType)
	switch {
	case strings.Contains(t, "drift.detected"),
		strings.Contains(t, "drift.critical"),
		strings.Contains(t, "canary.failed"),
		strings.Contains(t, "rollback"):
		// Page-someone-now class: production drift past the hard threshold, a
		// canary failing mid-promotion, an automated rollback firing.
		return SeverityCritical
	case strings.HasSuffix(t, ".failed"),
		strings.Contains(t, ".failed."),
		strings.Contains(t, "quota.exceeded"),
		strings.Contains(t, "error"):
		return SeverityError
	case strings.Contains(t, "drift"), // non-critical drift signal
		strings.Contains(t, "quota.approaching"),
		strings.Contains(t, "warning"),
		strings.Contains(t, "degraded"):
		return SeverityWarning
	default:
		// Routine lifecycle (started/registered/promoted/...) and ANYTHING we
		// don't recognize → INFO. A safe, non-noisy default that never panics on
		// an unknown event — essential for a zero-knowledge reactor.
		return SeverityInfo
	}
}

// ============================================================================
// PATTERN MATCHING (NATS-style wildcards: '*' one segment, '>' tail)
// ============================================================================

// matchesAnyPattern is true if event matches ANY pattern in the list.
func matchesAnyPattern(patterns []string, eventType string) bool {
	for _, p := range patterns {
		if matchPattern(p, eventType) {
			return true
		}
	}
	return false
}

// matchPattern implements the subject-matching grammar shared by mutes and the
// routing engine, mirroring NATS subject wildcards (which is intentional — the
// subjects ARE NATS subjects):
//   - '*' matches EXACTLY ONE segment (does not cross a '.').
//   - '>' matches ONE OR MORE trailing segments (only valid as the last token).
//   - everything else is a literal segment.
//
// WHY reuse NATS semantics rather than invent our own: the patterns describe NATS
// subjects (`fp.pipelines.failed`), so using NATS's own wildcard rules means a
// user's mental model ("fp.pipelines.* like my NATS sub") is exactly right, and
// the same grammar can later drive an actual JetStream filter subject with no
// translation. An empty pattern matches nothing (defensive — an empty mute entry
// should never silence everything).
func matchPattern(pattern, eventType string) bool {
	if pattern == "" {
		return false
	}
	pSegs := strings.Split(pattern, ".")
	eSegs := strings.Split(eventType, ".")

	for i, ps := range pSegs {
		if ps == ">" {
			// '>' must be the final token and matches the remaining tail — which
			// must be NON-EMPTY (at least one segment), matching NATS semantics.
			return i < len(eSegs)
		}
		if i >= len(eSegs) {
			// Pattern still has literal/'*' tokens but the subject ran out.
			return false
		}
		if ps == "*" {
			continue // single-segment wildcard: any one segment is fine
		}
		if ps != eSegs[i] {
			return false
		}
	}
	// All pattern tokens consumed; it's a match ONLY if the subject also ended
	// (no trailing unmatched segments — that's the segment-count rule).
	return len(pSegs) == len(eSegs)
}

// ============================================================================
// INBOX — ListNotifications / GetNotification / MarkRead
// ============================================================================

func (s *notificationService) ListNotifications(ctx context.Context, in ListNotificationsInput) (ListNotificationsOutput, error) {
	opts := ListOptions{
		PageSize:        s.clampPageSize(in.PageSize),
		PageToken:       in.PageToken,
		UnreadOnly:      in.UnreadOnly,
		MinSeverity:     in.MinSeverity,
		EventTypeFilter: in.EventTypeFilter,
	}
	items, next, err := s.notifications.ListForUser(ctx, in.RecipientUserID, opts)
	if err != nil {
		return ListNotificationsOutput{}, err
	}
	// The unread badge is a separate cheap COUNT independent of the page filters,
	// so the UI badge is the TRUE total unread, not "unread on this page".
	unread, err := s.notifications.CountUnread(ctx, in.RecipientUserID)
	if err != nil {
		return ListNotificationsOutput{}, err
	}
	return ListNotificationsOutput{Notifications: items, NextPageToken: next, UnreadCount: unread}, nil
}

func (s *notificationService) GetNotification(ctx context.Context, recipientUserID, id string) (GetNotificationOutput, error) {
	n, err := s.notifications.GetByIDForUser(ctx, id, recipientUserID)
	if err != nil {
		// TRANSLATE storage "not found" (which is ALSO what a foreign-owned row
		// returns — see the repo port) into the existence-hiding business error.
		// This is the anti-IDOR guarantee: another user's id is indistinguishable
		// from a non-existent one.
		if isRepoNotFound(err) {
			return GetNotificationOutput{}, ErrNotFound
		}
		return GetNotificationOutput{}, err
	}
	attempts, err := s.notifications.GetAttemptsForNotification(ctx, recipientUserID, id)
	if err != nil {
		if isRepoNotFound(err) {
			return GetNotificationOutput{}, ErrNotFound
		}
		return GetNotificationOutput{}, err
	}
	return GetNotificationOutput{Notification: n, DeliveryAttempts: attempts}, nil
}

func (s *notificationService) MarkRead(ctx context.Context, in MarkReadInput) (MarkReadOutput, error) {
	now := s.clock.Now()

	var marked int
	var err error
	if in.MarkAll {
		marked, err = s.notifications.MarkAllRead(ctx, in.RecipientUserID, now)
	} else {
		// Bound the batch (security default: cap batch sizes). An unbounded id list
		// would let one call issue an arbitrarily large UPDATE.
		if len(in.Ids) > maxMarkReadBatch {
			return MarkReadOutput{}, wrap(ErrValidation, "too many ids in MarkRead batch")
		}
		marked, err = s.notifications.MarkRead(ctx, in.RecipientUserID, in.Ids, now)
	}
	if err != nil {
		return MarkReadOutput{}, err
	}
	unread, err := s.notifications.CountUnread(ctx, in.RecipientUserID)
	if err != nil {
		return MarkReadOutput{}, err
	}
	return MarkReadOutput{MarkedCount: marked, UnreadCount: unread}, nil
}

// ============================================================================
// DELIVERY LOG — ListDeliveryAttempts
// ============================================================================

func (s *notificationService) ListDeliveryAttempts(ctx context.Context, in ListDeliveryAttemptsInput) (ListDeliveryAttemptsOutput, error) {
	opts := ListOptions{
		PageSize:       s.clampPageSize(in.PageSize),
		PageToken:      in.PageToken,
		Channel:        in.Channel,
		Status:         in.Status,
		NotificationID: in.NotificationID,
	}
	// If a specific notification is requested, verify it belongs to the caller
	// FIRST so we return the existence-hiding ErrNotFound for a foreign id rather
	// than silently returning an empty list (which would leak nothing, but the
	// explicit check matches the proto's documented NOT_FOUND behavior and gives
	// the client a clear signal).
	if in.NotificationID != "" {
		if _, err := s.notifications.GetByIDForUser(ctx, in.NotificationID, in.RecipientUserID); err != nil {
			if isRepoNotFound(err) {
				return ListDeliveryAttemptsOutput{}, ErrNotFound
			}
			return ListDeliveryAttemptsOutput{}, err
		}
	}
	attempts, next, err := s.notifications.ListDeliveryAttemptsForUser(ctx, in.RecipientUserID, opts)
	if err != nil {
		return ListDeliveryAttemptsOutput{}, err
	}
	return ListDeliveryAttemptsOutput{Attempts: attempts, NextPageToken: next}, nil
}

// ============================================================================
// PREFERENCES — GetPreferences / UpdatePreferences (+ SSRF validation)
// ============================================================================

func (s *notificationService) GetPreferences(ctx context.Context, userID string) (NotificationPreferences, error) {
	p, err := s.prefs.GetByUser(ctx, userID)
	if err != nil {
		if isRepoNotFound(err) {
			// Never-configured user → sensible DEFAULTS, not an error, so the
			// settings UI always renders something. Default = IN_APP enabled,
			// nothing external. (IN_APP is implicitly on via ChannelFor anyway,
			// but we make it explicit here so a GetPreferences round-trips a
			// concrete, editable document.)
			return s.defaultPreferences(userID), nil
		}
		return NotificationPreferences{}, err
	}
	return p, nil
}

func (s *notificationService) defaultPreferences(userID string) NotificationPreferences {
	return NotificationPreferences{
		UserID: userID,
		Channels: []ChannelPreference{
			{Channel: ChannelInApp, Enabled: true, MinSeverity: SeverityUnspecified},
		},
		UpdatedAt: s.clock.Now(),
	}
}

func (s *notificationService) UpdatePreferences(ctx context.Context, in UpdatePreferencesInput) (NotificationPreferences, error) {
	// IDEMPOTENCY (sync path): if this exact key was already applied, return the
	// first result WITHOUT re-applying. WHY before validation: a retried call with
	// the same key should be a no-op success even if, e.g., the client double-fires
	// — we want exactly-once apply semantics keyed on the client's token. We key it
	// in a per-user idem namespace so one user's key can't collide with another's.
	var idemKey string
	if in.IdempotencyKey != "" {
		idemKey = "idem:prefs:" + in.UserID + ":" + in.IdempotencyKey
		seen, _, err := s.idem.Seen(ctx, idemKey)
		if err != nil {
			return NotificationPreferences{}, err
		}
		if seen {
			// First apply already happened. Return the CURRENT stored state (the
			// result of that first apply). We re-read rather than caching the blob
			// to keep the store simple; the value is the same last-write document.
			return s.GetPreferences(ctx, in.UserID)
		}
	}

	// VALIDATE the mute list size (security default: cap list sizes).
	if len(in.MutedEventPatterns) > maxMutePatterns {
		return NotificationPreferences{}, wrap(ErrValidation, "too many muted_event_patterns")
	}

	// VALIDATE EVERY channel target BEFORE persisting. This is the headline
	// security control for this service: a webhook/slack target that fails the
	// SSRF guard rejects the WHOLE update (so a malicious URL never reaches
	// storage), and a malformed email is rejected too. We do the PURE, network-free
	// checks here (scheme/host/userinfo/IP-literal); the delivery adapter re-checks
	// with DNS resolution + a pinned-IP connect at send time (TOCTOU defense).
	for _, cp := range in.Channels {
		if err := validateChannelTarget(cp); err != nil {
			return NotificationPreferences{}, err
		}
	}

	// SERVER-AUTHORITATIVE fields: UserID is the authenticated caller (NEVER from
	// the request body — anti mass-assignment) and UpdatedAt is the server clock
	// (any client-sent value is ignored).
	prefs := NotificationPreferences{
		UserID:             in.UserID,
		Channels:           in.Channels,
		MutedEventPatterns: in.MutedEventPatterns,
		UpdatedAt:          s.clock.Now(),
	}
	stored, err := s.prefs.Upsert(ctx, prefs)
	if err != nil {
		return NotificationPreferences{}, err
	}

	// Record the idempotency key AFTER a successful apply (so a failed apply can be
	// legitimately retried with the same key).
	if idemKey != "" {
		if err := s.idem.Record(ctx, idemKey, nil, idempotencyTTL); err != nil {
			return NotificationPreferences{}, err
		}
	}
	return stored, nil
}

// ============================================================================
// TestChannel — owner self-test
// ============================================================================

func (s *notificationService) TestChannel(ctx context.Context, in TestChannelInput) (TestChannelOutput, error) {
	// IDEMPOTENCY: a retried test (same key) must not double-fire (spamming the
	// user's Slack). If seen, report a delivered no-op rather than sending again.
	var idemKey string
	if in.IdempotencyKey != "" {
		idemKey = "idem:test:" + in.UserID + ":" + in.IdempotencyKey
		seen, _, err := s.idem.Seen(ctx, idemKey)
		if err != nil {
			return TestChannelOutput{}, err
		}
		if seen {
			return TestChannelOutput{Status: StatusDelivered}, nil
		}
	}

	// IN_APP is always reachable — testing it is a no-op success that never touches
	// the external notifier.
	if in.Channel == ChannelInApp {
		if idemKey != "" {
			if err := s.idem.Record(ctx, idemKey, nil, idempotencyTTL); err != nil {
				return TestChannelOutput{}, err
			}
		}
		return TestChannelOutput{Status: StatusDelivered}, nil
	}

	// Resolve the caller's STORED, already-validated preference for this channel.
	// The request carries NO target URL — so this RPC can never be turned into an
	// SSRF probe of an arbitrary host. The only reachable destination is one the
	// user already configured (and which passed the write-time SSRF guard).
	prefs, err := s.prefs.GetByUser(ctx, in.UserID)
	if err != nil && !isRepoNotFound(err) {
		return TestChannelOutput{}, err
	}
	cp, ok := prefs.ChannelFor(in.Channel)
	if !ok || !cp.Enabled || strings.TrimSpace(cp.Target) == "" {
		// Nothing to test → FAILED_PRECONDITION at the handler. Distinct from a
		// validation error: the request was well-formed, the STATE isn't ready.
		return TestChannelOutput{}, ErrChannelNotConfigured
	}

	// Deliver a synthetic test through the Notifier. The adapter runs the SSRF
	// guard again (pinned-IP connect) for webhook/slack — defense in depth even
	// though the stored target was validated on write (DNS may have rebound since).
	res, derr := s.notifier.Deliver(ctx, DeliveryTarget{
		Channel:   in.Channel,
		Target:    cp.Target,
		Title:     "Forgepoint test notification",
		Body:      "This is a test of your " + channelName(in.Channel) + " channel. If you can read this, it works.",
		EventType: "fp.notifications.test",
	})
	if derr != nil {
		// A delivery error (including a late SSRF rejection) is reported to the
		// caller as a FAILED test outcome, not a 500 — the settings UI wants to
		// SHOW "✗ <reason>" inline, so we surface it in the response, not as an err.
		return TestChannelOutput{Status: StatusFailed, ErrorMessage: derr.Error()}, nil
	}

	if idemKey != "" {
		if err := s.idem.Record(ctx, idemKey, nil, idempotencyTTL); err != nil {
			return TestChannelOutput{}, err
		}
	}
	return TestChannelOutput{
		Status:       res.Status,
		ResponseCode: res.ResponseCode,
		ErrorMessage: res.ErrorMessage,
	}, nil
}

// ============================================================================
// SSRF PRE-VALIDATION (the PURE, network-free half — adapter does the rest)
// ============================================================================
//
// validateChannelTarget validates ONE channel preference's target by channel
// type. This runs at WRITE time so a bad target never reaches storage.
//
// SPLIT OF RESPONSIBILITY (interview-critical): the DOMAIN does everything that
// needs no network — parse the URL, enforce https-only, reject userinfo creds,
// allowlist the Slack host, and reject IP-LITERAL targets that are loopback/
// link-local/private/etc. The ADAPTER does the network-dependent half at delivery
// time: resolve DNS, deny if the resolved IP is in the same denylist, and PIN the
// IP for the connect (so DNS can't rebind between this check and the POST —
// TOCTOU). Both halves are mandatory; this function is the first line.
func validateChannelTarget(cp ChannelPreference) error {
	switch cp.Channel {
	case ChannelInApp, ChannelUnspecified:
		// No target needed. (Unspecified is itself odd but the routing engine
		// ignores it; we don't reject here to keep update tolerant.)
		return nil

	case ChannelEmail:
		if !cp.Enabled {
			return nil // a disabled channel's target is irrelevant
		}
		if !looksLikeEmail(cp.Target) {
			return wrap(ErrValidation, "invalid email target")
		}
		return nil

	case ChannelWebhook, ChannelSlack:
		if !cp.Enabled {
			return nil
		}
		return validateWebhookTarget(cp.Channel, cp.Target)

	default:
		return wrap(ErrValidation, "unknown channel")
	}
}

// validateWebhookTarget is the pure SSRF pre-check for an HTTP delivery target.
func validateWebhookTarget(channel NotificationChannel, target string) error {
	target = strings.TrimSpace(target)
	if target == "" {
		return wrap(ErrValidation, "webhook/slack target is required when the channel is enabled")
	}

	u, err := url.Parse(target)
	if err != nil {
		return wrap(ErrSSRFTargetRejected, "target is not a valid URL")
	}

	// (1) SCHEME ALLOWLIST — https only. Blocks http (downgrade), file:// (local
	// file read), gopher://, ftp://, and anything else.
	if u.Scheme != "https" {
		return wrap(ErrSSRFTargetRejected, "target scheme must be https")
	}

	// (2) NO USERINFO CREDENTIALS — `https://user:pass@host` can smuggle creds or
	// confuse host parsing in downstream clients; reject outright.
	if u.User != nil {
		return wrap(ErrSSRFTargetRejected, "target must not contain userinfo credentials")
	}

	host := u.Hostname() // strips any :port and []-brackets for IPv6
	if host == "" {
		return wrap(ErrSSRFTargetRejected, "target must have a host")
	}

	// (3) SLACK HOST ALLOWLIST — a SLACK channel can ONLY point at hooks.slack.com.
	if channel == ChannelSlack && !strings.EqualFold(host, slackWebhookHost) {
		return wrap(ErrSSRFTargetRejected, "slack target host must be "+slackWebhookHost)
	}

	// (4) WELL-KNOWN LOOPBACK HOSTNAMES — "localhost" (and the *.localhost TLD,
	// reserved by RFC 6761 to resolve to loopback) name the loopback interface
	// without being IP literals, so the literal denylist below misses them. We
	// reject them by NAME at write time too. WHY here and not only at the adapter:
	// catching the single most common SSRF string ("https://localhost/...") at
	// write time is cheap and gives the user an immediate, clear rejection. The
	// adapter's DNS-resolution step is still the authoritative catch-all for every
	// OTHER hostname that resolves into a denied range.
	lower := strings.ToLower(host)
	if lower == "localhost" || strings.HasSuffix(lower, ".localhost") {
		return wrap(ErrSSRFTargetRejected, "target host resolves to loopback")
	}

	// (5) IP-LITERAL DENYLIST — if the host is an IP LITERAL (no DNS needed), apply
	// the denylist now. WHY only literals here: resolving a HOSTNAME requires DNS,
	// which is the adapter's job (and must be re-done at connect time to defeat
	// rebinding). But an attacker who hard-codes 169.254.169.254 or 127.0.0.1 is
	// caught immediately, at write time, with no network call.
	if ip := net.ParseIP(host); ip != nil {
		if isDeniedIP(ip) {
			return wrap(ErrSSRFTargetRejected, "target IP is loopback/link-local/private/reserved")
		}
	}
	// Hostname (non-literal) targets pass the pure checks and are FULLY validated
	// (DNS-resolved + pinned-IP connect) by the adapter at delivery time.
	return nil
}

// isDeniedIP reports whether an IP is in the SSRF denylist: loopback, link-local
// (incl. the cloud metadata endpoint 169.254.169.254), private, unspecified, or
// multicast. This same classification is re-applied by the adapter against the
// RESOLVED IP at connect time.
//
// WHY each range is denied (the interview-critical "why"):
//   - 127.0.0.0/8, ::1            loopback → internal-only services / admin APIs
//   - 169.254.0.0/16, fe80::/10   link-local → 169.254.169.254 is the AWS/GCP
//     metadata endpoint; an SSRF here steals IAM credentials. THE classic attack.
//   - 10/8, 172.16/12, 192.168/16, fc00::/7  private → in-cluster services, DBs
//   - 0.0.0.0/::                  unspecified → can resolve to localhost
//   - multicast                  not a unicast delivery target; abuse vector
func isDeniedIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified() || ip.IsMulticast() || ip.IsInterfaceLocalMulticast() {
		return true
	}
	// IsPrivate covers RFC1918 (10/8, 172.16/12, 192.168/16) and RFC4193 (fc00::/7).
	if ip.IsPrivate() {
		return true
	}
	return false
}

// looksLikeEmail is a deliberately CONSERVATIVE structural check: exactly one '@',
// non-empty local and domain parts, and at least one dot in the domain. WHY not a
// full RFC 5322 parser: full email validation is famously a rabbit hole and most
// "valid" RFC addresses can't actually receive mail. We reject the obviously
// broken (no '@', no domain) at write time; true deliverability is proven by the
// EMAIL channel's own delivery + the user's TestChannel. This is enough to stop a
// typo'd address from silently black-holing notifications.
func looksLikeEmail(addr string) bool {
	addr = strings.TrimSpace(addr)
	at := strings.IndexByte(addr, '@')
	if at <= 0 || at != strings.LastIndexByte(addr, '@') {
		return false // zero or multiple '@', or empty local part
	}
	local, domain := addr[:at], addr[at+1:]
	if local == "" || domain == "" {
		return false
	}
	if !strings.Contains(domain, ".") || strings.HasPrefix(domain, ".") || strings.HasSuffix(domain, ".") {
		return false
	}
	if strings.ContainsAny(addr, " \t\r\n") {
		return false
	}
	return true
}

// ============================================================================
// SMALL HELPERS
// ============================================================================

// clampPageSize enforces the page-size bounds (security default: cap list sizes).
// 0/negative → default; over the max → max. A client can never demand an
// unbounded page regardless of what it sends.
func (s *notificationService) clampPageSize(n int) int {
	if n <= 0 {
		return defaultPageSize
	}
	if n > maxPageSize {
		return maxPageSize
	}
	return n
}

// isRepoNotFound centralizes the storage→business not-found check so every caller
// translates consistently (and so a future repo that wraps the sentinel still
// matches via errors.Is).
func isRepoNotFound(err error) bool {
	return errors.Is(err, ErrRepoNotFound)
}

// wrappedErr lets a specific message ride on a sentinel while remaining
// errors.Is-comparable to that sentinel (via Unwrap).
type wrappedErr struct {
	sentinel error
	msg      string
}

func (w *wrappedErr) Error() string { return w.sentinel.Error() + ": " + w.msg }
func (w *wrappedErr) Unwrap() error { return w.sentinel }

// wrap attaches a human message to a sentinel. Callers above use it so the
// handler can still errors.Is(err, ErrValidation) etc. while logs get detail.
func wrap(sentinel error, msg string) error {
	return &wrappedErr{sentinel: sentinel, msg: msg}
}

// channelName is a tiny display helper for the synthetic test message body.
func channelName(c NotificationChannel) string {
	switch c {
	case ChannelInApp:
		return "in-app"
	case ChannelWebhook:
		return "webhook"
	case ChannelSlack:
		return "Slack"
	case ChannelEmail:
		return "email"
	default:
		return "unknown"
	}
}

// newUUID is the production IDGenerator's backing call, exposed as a package
// helper so the default generator (used by main.go) is one line. Tests inject a
// deterministic generator instead.
func newUUID() string { return uuid.NewString() }
