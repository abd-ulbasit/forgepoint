// models.go — the pure domain types for the Notification service.
//
// ============================================================================
// CLEAN ARCHITECTURE: WHAT LIVES HERE (AND WHAT MUST NOT)
// ============================================================================
//
// These are the BUSINESS types of the notification domain. They depend on
// stdlib + github.com/google/uuid ONLY — no proto, no gRPC, no NATS, no SQL.
// The handler converts notificationv1.* proto messages into these types (and
// back); the Postgres adapter persists them; the NATS adapter consumes events
// and produces these. The domain itself never imports any of those packages.
//
// WHY a separate set of types from the generated proto:
//
//	The proto is a WIRE contract — it changes for wire-compat reasons (field
//	numbers, JSON names, oneof wrapping). The domain models change for BUSINESS
//	reasons. Decoupling them means a wire-format tweak never ripples into the
//	business logic, and the business logic never accidentally serializes a proto
//	internal (e.g. the unexported state/sizeCache fields). This is the standard
//	"anti-corruption layer" boundary.
//
// ============================================================================
// PATTERN — CHOREOGRAPHY (pure event reactor) — WHERE IT SHOWS UP IN THE TYPES
// ============================================================================
//
// The reactor's job: an InboundEvent arrives (opaque platform event), we look up
// the recipient's NotificationPreferences, and DECIDE which channels to deliver
// over (a RoutingDecision). Crucially the domain treats the event OPAQUELY — it
// matches on Type only and forwards the payload bytes verbatim. It never decodes
// what a `fp.pipelines.failed` MEANS. That opacity is the whole payoff of
// choreography: a brand-new event type needs ZERO code change here — a user just
// writes a mute/preference whose pattern matches the new Type.
//
// The types below are deliberately the MINIMUM the routing brain needs. Heavy
// concerns (retry budgets, circuit breakers, the actual HTTP POST) live in the
// delivery ADAPTER behind the Notifier port — not in these models.
package domain

import "time"

// ============================================================================
// ENUMS — closed value sets mirrored 1:1 from the proto (byte-identical values)
// ============================================================================
//
// WHY redeclare the enums instead of importing the generated ones: importing
// notificationv1 here would drag the protobuf runtime into the domain and break
// the purity rule (no gen/go in domain). We mirror the SAME integer values the
// proto uses so the handler's conversion is a trivial, lossless cast at the
// boundary. The values are pinned in a test-visible table so drift is caught.

// NotificationChannel is the delivery transport. Mirrors
// notificationv1.NotificationChannel AND events.v1.NotificationChannel
// (all three share UNSPECIFIED=0, IN_APP=1, WEBHOOK=2, SLACK=3, EMAIL=4).
type NotificationChannel int32

const (
	// ChannelUnspecified is the zero value. A well-formed preference never uses
	// it; it exists so an unset channel is explicitly "not set", not a real one.
	ChannelUnspecified NotificationChannel = 0
	// ChannelInApp is the platform web inbox — always available, no external
	// config, no secret, no SSRF surface. Every Notification row implicitly
	// includes it even when a fan-out channel also fires.
	ChannelInApp NotificationChannel = 1
	// ChannelWebhook is a generic HTTPS POST to a CALLER-SUPPLIED URL. This is
	// the SSRF-bearing channel — its target must be SSRF-validated (see Notifier).
	ChannelWebhook NotificationChannel = 2
	// ChannelSlack is a Slack Incoming Webhook (Slack-shaped JSON over HTTPS).
	// Its target host is allowlisted to hooks.slack.com (see RequiresSSRFGuard).
	ChannelSlack NotificationChannel = 3
	// ChannelEmail is SMTP delivery. Target is an email address (no SSRF surface).
	ChannelEmail NotificationChannel = 4
)

// IsExternal reports whether the channel leaves the platform (i.e. needs a
// configured target + delivery via the Notifier port). IN_APP and UNSPECIFIED
// are internal/no-op; everything else fans out externally.
//
// WHY a method on the domain type: the routing logic asks "does this channel
// need a target and a delivery attempt?" in several places; centralizing the
// answer here keeps the rule in one auditable spot rather than scattering
// `ch == ChannelWebhook || ch == ChannelSlack || ch == ChannelEmail` literals.
func (c NotificationChannel) IsExternal() bool {
	switch c {
	case ChannelWebhook, ChannelSlack, ChannelEmail:
		return true
	default:
		return false
	}
}

// RequiresSSRFGuard reports whether this channel's target is an attacker-
// influenced URL we will make an OUTBOUND HTTP request to — i.e. the channels
// where Server-Side Request Forgery is the headline risk.
//
// WHY this is a domain method even though the domain never makes the HTTP call:
// the domain is what MARKS a target as "must be validated before storage/use".
// The delivery adapter performs the actual scheme/host/IP checks, but the domain
// is the single place that decides WHICH targets carry the SSRF obligation — so
// the obligation can never be silently forgotten by an adapter author. EMAIL is
// excluded (SMTP to an address is not an HTTP-fetch SSRF primitive).
func (c NotificationChannel) RequiresSSRFGuard() bool {
	return c == ChannelWebhook || c == ChannelSlack
}

// Severity is the coarse priority of a notification, derived SERVER-SIDE from
// the event type. Mirrors notificationv1.NotificationSeverity
// (UNSPECIFIED=0, INFO=1, WARNING=2, ERROR=3, CRITICAL=4). The ordering is
// meaningful: a higher value is more severe, which is what the min-severity
// floor comparison relies on.
type Severity int32

const (
	// SeverityUnspecified is the zero value. As a min-severity FLOOR it means
	// "no floor" (deliver everything). As a notification's OWN severity it is an
	// unclassified event (treated as the lowest rung for floor comparisons).
	SeverityUnspecified Severity = 0
	// SeverityInfo is a routine lifecycle signal (e.g. pipeline started).
	SeverityInfo Severity = 1
	// SeverityWarning is "look at this soon" (mild drift, approaching quota).
	SeverityWarning Severity = 2
	// SeverityError is a failure (pipeline failed, delivery failed, quota over).
	SeverityError Severity = 3
	// SeverityCritical is page-someone-now (hard-threshold drift, canary failure).
	SeverityCritical Severity = 4
)

// MeetsFloor reports whether a notification of severity c clears a min-severity
// FLOOR. A zero (unspecified) floor means "no floor" → always true. Otherwise
// the notification's severity must be >= the floor.
//
// WHY a method (not an inline `>=`): the "unspecified floor = no floor" rule is
// a subtle business invariant. Encoding it once here means every caller (channel
// gating, list filtering) gets it right and the rule is unit-tested in one place.
// NOTE: this is the classic "0 means absent, not minimum" gotcha —
// if you naively did `c >= floor`, an INFO(1) notification would clear an
// unspecified(0) floor anyway, but a CRITICAL notification would ALSO clear it,
// so the bug only bites when someone later treats unspecified as a real rung.
// Making the rule explicit prevents that future regression.
func (c Severity) MeetsFloor(floor Severity) bool {
	if floor == SeverityUnspecified {
		return true
	}
	return c >= floor
}

// DeliveryStatus is the per-channel outcome of a notification. Mirrors
// notificationv1.DeliveryStatus
// (UNSPECIFIED=0, PENDING=1, RETRYING=2, DELIVERED=3, FAILED=4, SUPPRESSED=5).
type DeliveryStatus int32

const (
	// StatusUnspecified is the zero value (never a meaningful outcome).
	StatusUnspecified DeliveryStatus = 0
	// StatusPending — accepted, not yet attempted on a fan-out channel.
	StatusPending DeliveryStatus = 1
	// StatusRetrying — within the exponential-backoff retry window.
	StatusRetrying DeliveryStatus = 2
	// StatusDelivered — succeeded on the channel (e.g. webhook 2xx).
	StatusDelivered DeliveryStatus = 3
	// StatusFailed — permanently failed (retries exhausted / breaker open).
	StatusFailed DeliveryStatus = 4
	// StatusSuppressed — deliberately NOT delivered (channel disabled, below the
	// severity floor, or muted). Recorded so the inbox can show "we chose not to
	// page you" — a SUPPRESSED is a decision, not an error.
	StatusSuppressed DeliveryStatus = 5
)

// ============================================================================
// CORE READ MODEL
// ============================================================================

// Notification is the materialized, human-facing record produced when an inbound
// platform event matched a recipient's preferences. It is the central read model
// the inbox (List/Get) returns and MarkRead mutates — one row in a GitHub-style
// notification center.
//
// SECURITY / AUTHORITY: every field is SERVER-AUTHORITATIVE. The recipient,
// severity, status, timestamps and provenance are derived from the event and the
// routing engine — there is NO client write path that sets them. The only client
// mutation is MarkRead (flipping Read/ReadAt). This is the anti-mass-assignment
// rule from the architecture's security defaults, realized in the type: there is
// simply no constructor or setter that accepts a client-chosen recipient.
type Notification struct {
	// ID is a UUIDv4, the stable inbox-entry identifier (NOT the event id).
	ID string
	// RecipientUserID is the user this was delivered to — set from the matched
	// preference's owner, NEVER from a request body.
	RecipientUserID string
	// Title is the short headline for the inbox row.
	Title string
	// Body is the longer human-readable detail, rendered server-side.
	Body string
	// Severity is the server-derived priority (drives sorting + suppression).
	Severity Severity
	// Channels are the channels this notification was (or will be) delivered
	// over — IN_APP plus zero or more fan-out channels per the recipient's prefs.
	Channels []NotificationChannel
	// Read is whether the user has read it (flipped by MarkRead; defaults false).
	Read bool

	// --- PROVENANCE (back-pointer to the originating EventEnvelope) ---

	// EventID is the EventEnvelope.id that triggered this. It is ALSO the
	// idempotency anchor: re-processing the same EventID for the same recipient
	// must not create a duplicate row (idempotent consumer — see ReactToEvent).
	EventID string
	// EventType is the EventEnvelope.type the engine matched (e.g.
	// "fp.pipelines.failed"). The exact subject the routing patterns matched.
	EventType string
	// SourceService is the producing service ("pipeline", "billing", ...), from
	// EventEnvelope.source. For filtering and support triage.
	SourceService string

	// CreatedAt is when the matching event was processed (server clock, immutable).
	CreatedAt time.Time
	// ReadAt is when the user marked it read (zero while unread). Server clock.
	ReadAt time.Time
}

// DeliveryAttempt is the typed view of one row in the delivery_log — the audit
// trail of "did this channel actually go out, and if not, why". One Notification
// fans out to N channels, each retried K times, so attempts are a SEPARATE type
// from Notification (flattening would lose the per-channel/per-try structure).
//
// SECURITY: ResponseCode/ErrorMessage describe OUR delivery outcome only. We
// deliberately do NOT carry the target URL or any auth header here — only enough
// to debug delivery health without echoing a secret or PII.
type DeliveryAttempt struct {
	// NotificationID links this attempt back to its Notification. (On the proto
	// GetNotification path the id is implied; the fleet ListDeliveryAttempts view
	// needs it, so we always carry it on the domain type.)
	NotificationID string
	// Channel is the transport this attempt used.
	Channel NotificationChannel
	// Status is the outcome of this attempt.
	Status DeliveryStatus
	// Attempt is the 1-based try number within the retry budget (0 = not yet
	// attempted, e.g. a PENDING/SUPPRESSED record).
	Attempt int32
	// ResponseCode is the HTTP status for WEBHOOK/SLACK (e.g. 200, 503); 0 for
	// non-HTTP channels or when no response was received (timeout).
	ResponseCode int32
	// ErrorMessage is human-readable failure detail for logs (empty on success).
	// NOT for end-user display.
	ErrorMessage string
	// AttemptedAt is when this attempt was made (server clock).
	AttemptedAt time.Time
}

// ============================================================================
// PREFERENCES (the per-user routing config — where choreography lives for users)
// ============================================================================

// ChannelPreference is a user's choice for ONE channel: enabled?, the minimum
// severity that should reach it, and the channel-specific Target (webhook URL,
// Slack URL, or email address).
//
// SECURITY — SSRF (the headline risk for THIS service): for WEBHOOK/SLACK the
// Target is a CLIENT-SUPPLIED URL the delivery layer will POST to. The DOMAIN
// validates the SHAPE it can validate purely (non-empty, channel/target coherence)
// and MARKS the target as SSRF-guarded (see Channel.RequiresSSRFGuard); the
// adapter performs the network-level checks (https-only, hooks.slack.com host
// allowlist, private/loopback/link-local IP denylist, pinned-IP connect to defeat
// DNS-rebinding TOCTOU). The split is deliberate: the domain stays pure (no DNS,
// no net) yet still OWNS the rule that such a target must never be trusted raw.
type ChannelPreference struct {
	// Channel is which transport this configures.
	Channel NotificationChannel
	// Enabled is the master on/off. A disabled channel is SUPPRESSED, not FAILED.
	Enabled bool
	// MinSeverity is the floor for this channel; events below it are SUPPRESSED
	// for this channel. SeverityUnspecified = no floor (deliver everything).
	MinSeverity Severity
	// Target is the channel-specific routing target:
	//   WEBHOOK/SLACK → the HTTPS URL to POST to (SSRF-guarded).
	//   EMAIL         → the destination email address.
	//   IN_APP        → ignored (the inbox target is the user themselves).
	Target string
}

// NotificationPreferences is the per-user aggregate the routing engine consults
// on each inbound event. Returning/updating it as ONE document keeps the settings
// API+UI simple and makes updates atomic (no half-saved partial state).
//
// SECURITY: UserID is server-set from the authenticated caller on read/write —
// NEVER taken from a request body (prevents one user editing another's prefs via
// mass-assignment). IN_APP is implicitly always-on even if absent from Channels.
type NotificationPreferences struct {
	// UserID is the owner (server-set from auth claims, never client-set).
	UserID string
	// Channels is the per-channel settings (typically one entry per configured
	// channel). An absent channel means "not configured".
	Channels []ChannelPreference
	// MutedEventPatterns are event-type wildcard patterns muted across ALL
	// channels (e.g. "fp.inference.*"). Empty = mute nothing. Same wildcard
	// grammar as the routing matcher.
	MutedEventPatterns []string
	// UpdatedAt is when the prefs were last written (server clock).
	UpdatedAt time.Time
}

// ChannelFor returns the ChannelPreference for the given channel and whether one
// was configured. IN_APP is special-cased to an implicit always-on, no-floor
// preference even when absent — the architecture's "IN_APP is always reachable"
// rule, encoded so callers don't each re-implement it.
//
// WHY return (pref, ok) rather than a bare value: the caller needs to distinguish
// "configured but disabled" (→ SUPPRESSED) from "not configured at all" (→ the
// channel simply doesn't fire). The bool carries that distinction without a
// sentinel value.
func (p NotificationPreferences) ChannelFor(ch NotificationChannel) (ChannelPreference, bool) {
	for _, cp := range p.Channels {
		if cp.Channel == ch {
			return cp, true
		}
	}
	// IN_APP is implicitly always-on with no severity floor, even if the user
	// never explicitly configured it. This guarantees the web inbox is reachable
	// for every recipient — the inbox is the one channel we never let a user
	// accidentally turn off by omission.
	if ch == ChannelInApp {
		return ChannelPreference{Channel: ChannelInApp, Enabled: true, MinSeverity: SeverityUnspecified}, true
	}
	return ChannelPreference{}, false
}

// ============================================================================
// THE INBOUND EVENT (what the choreography reactor consumes)
// ============================================================================

// InboundEvent is the domain's OPAQUE view of a platform event pulled off NATS
// (`fp.>`). The NATS adapter unwraps the common.v1.EventEnvelope into this — the
// domain sees identifiers + type + raw payload bytes, never a decoded
// producer-specific message.
//
// WHY opaque Payload []byte (the crux of choreography): decoding the payload
// would couple this service to every producer's schema and DESTROY the
// "zero knowledge of what events mean" property that makes adding a new event
// type a zero-code change here. We carry the bytes through to the delivery layer
// verbatim. The ONLY thing the domain interprets is Type (for pattern matching)
// and the three provenance fields (for the inbox row).
//
// A brand-new event type added by another team next quarter needs no deploy
// here: the reactor matches Type against user patterns and forwards Payload
// opaquely. The
// new producer needs no awareness of us; a user just writes a preference/mute
// pattern. That is choreography vs. orchestration in one sentence.
type InboundEvent struct {
	// EventID is the EventEnvelope.id (idempotency anchor downstream).
	EventID string
	// Type is the EventEnvelope.type, e.g. "fp.pipelines.failed" — the ONLY field
	// the routing engine interprets (pattern matching).
	Type string
	// Source is the producing service (EventEnvelope.source).
	Source string
	// RecipientUserID is the user this event should notify. WHY the domain takes
	// this as a given: mapping event → recipient set (e.g. "the pipeline owner")
	// is the PLATFORM's concern carried in the envelope/payload metadata; the
	// reactor's job is "given this recipient, how do they want it?" Keeping the
	// recipient resolution OUTSIDE the routing brain keeps the brain pure and
	// testable, and matches the proto's authority model (recipient is never
	// client-chosen on a read path).
	RecipientUserID string
	// Title/Body are the server-rendered human-facing strings for the inbox row.
	// Rendered by the consuming adapter (it knows the envelope shape); the domain
	// just carries them onto the Notification.
	Title string
	Body  string
	// Payload is the OPAQUE event bytes (EventEnvelope.data), forwarded to
	// external channels verbatim. The domain never decodes it.
	Payload []byte
	// OccurredAt is the event's timestamp (EventEnvelope.timestamp).
	OccurredAt time.Time
}

// ============================================================================
// ROUTING DECISION (the output of the reactor's brain)
// ============================================================================

// ChannelDecision is the per-channel verdict the routing engine reaches for one
// inbound event: deliver, or suppress (and why). It is the explainable unit of
// the decision — every channel the user has configured gets one, so the audit
// trail can show exactly which channels fired and which were intentionally held.
type ChannelDecision struct {
	// Channel is the transport this verdict is about.
	Channel NotificationChannel
	// Deliver is true if the engine decided to attempt delivery on this channel.
	// false means SUPPRESSED for the reason in Suppressed (and a SUPPRESSED
	// DeliveryAttempt should be recorded — a held decision, not a silent drop).
	Deliver bool
	// Suppressed is the machine-readable reason a channel did NOT fire (empty when
	// Deliver is true). One of the SuppressReason constants. WHY a typed reason
	// rather than a free string: it lets the delivery log and metrics aggregate
	// "how often did the severity floor hold back an email?" without string
	// parsing, and keeps the test assertions exact.
	Suppressed SuppressReason
	// Target is the resolved delivery target for external channels (copied from
	// the matched ChannelPreference). Empty for IN_APP. RequiresSSRFGuard reports
	// whether the adapter MUST SSRF-validate it before connecting.
	Target string
}

// RequiresSSRFGuard reports whether this decision's target must be SSRF-validated
// by the delivery adapter before any outbound request. It is a convenience that
// pairs the channel's SSRF obligation with the concrete target the adapter will
// dial — so the adapter has one struct that says both "where" and "you must
// validate this". A decision that does not deliver carries no obligation.
func (d ChannelDecision) RequiresSSRFGuard() bool {
	return d.Deliver && d.Channel.RequiresSSRFGuard() && d.Target != ""
}

// RoutingDecision is the full verdict for one inbound event: the recipient, the
// derived severity, and the per-channel decisions. It is what ReactToEvent
// returns — a PURE, side-effect-free value the caller (the NATS consumer adapter)
// then executes (write the inbox row, fire the Notifier for each delivering
// channel, record SUPPRESSED attempts for the rest).
//
// WHY the brain returns a DECISION instead of performing the delivery: keeping
// the routing logic a pure function of (event, preferences) makes it trivially
// unit-testable (no mocks needed for the decision itself — see the tests) and
// keeps all the I/O (DB writes, HTTP POSTs, NATS publishes) in the adapter where
// it belongs. This is "functional core, imperative shell".
type RoutingDecision struct {
	// RecipientUserID is who this event notifies (from the InboundEvent).
	RecipientUserID string
	// EventID/EventType/Source are provenance carried onto the resulting
	// Notification and the idempotency key.
	EventID   string
	EventType string
	Source    string
	// Severity is the server-derived severity for this event.
	Severity Severity
	// Muted is true if the ENTIRE event was muted by a MutedEventPatterns match —
	// in which case Channels is empty and NOTHING (not even IN_APP) is delivered.
	// WHY a top-level flag: a mute is a different fact from "every channel was
	// individually suppressed"; the caller may choose to not even create an inbox
	// row for a fully-muted event, whereas per-channel suppression still creates
	// the IN_APP row with SUPPRESSED fan-out attempts.
	Muted bool
	// Channels is the per-channel decisions (one per configured channel, plus the
	// implicit IN_APP). Empty when Muted.
	Channels []ChannelDecision
}

// DeliveringChannels returns just the channels the decision chose to deliver on.
// Convenience for the adapter (which populates Notification.Channels) and for
// tests asserting the positive routing outcome.
func (r RoutingDecision) DeliveringChannels() []NotificationChannel {
	out := make([]NotificationChannel, 0, len(r.Channels))
	for _, d := range r.Channels {
		if d.Deliver {
			out = append(out, d.Channel)
		}
	}
	return out
}

// SuppressReason is the typed, enumerable reason a channel was not delivered on.
type SuppressReason string

const (
	// SuppressNone means the channel WAS delivered (no suppression).
	SuppressNone SuppressReason = ""
	// SuppressDisabled — the user's ChannelPreference.Enabled was false.
	SuppressDisabled SuppressReason = "channel_disabled"
	// SuppressBelowSeverity — the event severity was below the channel's floor.
	SuppressBelowSeverity SuppressReason = "below_min_severity"
	// SuppressNoTarget — an external channel had no configured/valid target, so
	// there was nowhere to deliver (we record it rather than erroring the event).
	SuppressNoTarget SuppressReason = "no_target_configured"
	// SuppressMuted — the whole event matched a mute pattern (set on each channel
	// decision when RoutingDecision.Muted is true, for a uniform per-channel log).
	SuppressMuted SuppressReason = "event_muted"
)
