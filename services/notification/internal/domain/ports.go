// ports.go — the PORTS (interfaces) the Notification domain depends on.
//
// ============================================================================
// WHY THESE INTERFACES LIVE IN THE DOMAIN (Hexagonal "consumer-owned ports")
// ============================================================================
//
// The domain is the CONSUMER of persistence and of the external-delivery
// transport, so — per Hexagonal Architecture — it OWNS the interfaces it needs.
// The Postgres adapter (later phase, package `postgres`) and the delivery adapter
// (package `delivery`) IMPORT this domain and IMPLEMENT these ports. A single
// inward arrow, no import cycle:
//
//	          implements                 implements
//	postgres ───────────▶ domain ◀─────────── delivery (HTTP/Slack/SMTP, SSRF guard)
//	                        ▲
//	                        │ holds the ports
//	            notificationService (the reactor brain)
//
// Putting the ports in the `repository` package instead would create
// domain → repository → domain the moment the service impl (which lives in
// domain) references them — exactly the cycle the auth service hit. So they live
// here. (See docs/design/service-architecture.md, "Ports live in the DOMAIN".)
//
// ============================================================================
// PORT GRANULARITY — WHY SEVERAL SMALL PORTS, NOT ONE GOD-REPOSITORY
// ============================================================================
//
// We split persistence into NotificationRepository, PreferenceRepository and
// IdempotencyStore (and keep Notifier + Clock + IDGenerator separate). WHY:
//   - Interface Segregation: the reactor path needs the idempotency check + the
//     notification write; the settings path needs only preferences. A consumer
//     depends on the narrow port it uses, so a test stubs only what it touches.
//   - Each can be backed differently in production (notifications + prefs in
//     Postgres; idempotency keys in Redis with a TTL). Small ports make that
//     substitution a wiring change, not a refactor.
package domain

import (
	"context"
	"time"
)

// ============================================================================
// STORAGE SENTINELS (returned by repository ports; the service translates them)
// ============================================================================

// ErrRepoNotFound is the STORAGE sentinel a repository returns when a row is
// absent. The service translates it to the BUSINESS error ErrNotFound (or, on a
// path where existence must not leak, swallows it). Distinct from the business
// ErrNotFound so the two vocabularies (storage vs business) never blur — see
// errors.go for the same split.
var ErrRepoNotFound = sentinelError("notification: repository: not found")

// sentinelError is a tiny stdlib-only error type. WHY not errors.New at package
// scope like errors.go does: ports.go and errors.go are the same package, so we
// keep the storage sentinel HERE (next to the ports that return it) for locality,
// while still being errors.Is-comparable. A named type also documents intent.
type sentinelError string

func (e sentinelError) Error() string { return string(e) }

// ============================================================================
// PAGINATION (cursor-based — stable under concurrent inbox writes)
// ============================================================================

// ListOptions carries cursor pagination + the inbox/delivery-log filters. WHY
// cursor (opaque PageToken) over LIMIT/OFFSET: the inbox is written concurrently
// by the event reactor, and OFFSET pagination skips or duplicates rows when rows
// are inserted between page fetches. A cursor anchored on (created_at, id) is
// stable. The repository owns the cursor's encoding; the domain treats it opaquely.
type ListOptions struct {
	// PageSize is the requested page size. The SERVICE caps it (default 20, max
	// 100) before passing it down — a client can never demand an unbounded page.
	PageSize int
	// PageToken is the opaque cursor (empty = first page).
	PageToken string

	// --- inbox / delivery-log filters (all optional; zero value = no filter) ---

	// UnreadOnly restricts an inbox list to unread notifications.
	UnreadOnly bool
	// MinSeverity restricts to notifications at or above this floor (Unspecified =
	// no floor — interpreted via Severity.MeetsFloor semantics by the adapter).
	MinSeverity Severity
	// EventTypeFilter restricts to a single exact event type (empty = all).
	EventTypeFilter string
	// Channel restricts a delivery-log list to one channel (Unspecified = all).
	Channel NotificationChannel
	// Status restricts a delivery-log list to one outcome (Unspecified = all).
	Status DeliveryStatus
	// NotificationID restricts a delivery-log list to one notification (empty =
	// all of the caller's notifications).
	NotificationID string
}

// ============================================================================
// REPOSITORY PORTS
// ============================================================================

// NotificationRepository is the persistence port for the inbox read model and
// the delivery log. The Postgres adapter implements it; tests inject mocks.
//
// AUTHORITY NOTE baked into the signatures: every read/mutate is scoped by
// recipientUserID, which the service fills from the auth claims — there is no
// method that fetches a notification by id ALONE. That makes cross-user access a
// type-level impossibility on this port: you cannot even ask for a row without
// saying whose it is.
type NotificationRepository interface {
	// Create persists a new inbox Notification. The service sets all
	// server-authoritative fields (id, recipient, severity, timestamps) before
	// calling; the repo just writes the row.
	Create(ctx context.Context, n Notification) (Notification, error)

	// GetByIDForUser returns the notification with that id IFF it belongs to
	// recipientUserID; otherwise ErrRepoNotFound (the service surfaces the
	// existence-hiding ErrNotFound). The user scope is a parameter, not an
	// afterthought — this is the anti-IDOR guarantee in the port itself.
	GetByIDForUser(ctx context.Context, id, recipientUserID string) (Notification, error)

	// ListForUser returns a page of the user's inbox (created_at DESC, newest
	// first) plus a nextToken cursor, honoring the filters in opts.
	ListForUser(ctx context.Context, recipientUserID string, opts ListOptions) (items []Notification, nextToken string, err error)

	// CountUnread returns the user's total unread count (for the UI badge),
	// independent of any page. Cheap with an indexed partial COUNT.
	CountUnread(ctx context.Context, recipientUserID string) (int, error)

	// MarkRead flips the given ids to read for recipientUserID and returns how
	// many actually transitioned (already-read and foreign/unknown ids are not
	// counted — MarkRead is idempotent). `now` is injected (Clock) so the read
	// timestamp is testable.
	MarkRead(ctx context.Context, recipientUserID string, ids []string, now time.Time) (markedCount int, err error)

	// MarkAllRead flips ALL of the user's unread notifications to read and returns
	// the count transitioned. Separate from MarkRead so the adapter can do it as a
	// single indexed UPDATE rather than first enumerating ids.
	MarkAllRead(ctx context.Context, recipientUserID string, now time.Time) (markedCount int, err error)

	// AppendDeliveryAttempt records one delivery_log row (the audit trail). Called
	// by the consumer adapter after each attempt AND for SUPPRESSED decisions.
	AppendDeliveryAttempt(ctx context.Context, attempt DeliveryAttempt) error

	// GetAttemptsForNotification returns the per-channel attempts for one of the
	// user's notifications (oldest first), for the GetNotification detail view.
	// Scoped by user for the same anti-IDOR reason as GetByIDForUser.
	GetAttemptsForNotification(ctx context.Context, recipientUserID, notificationID string) ([]DeliveryAttempt, error)

	// ListDeliveryAttemptsForUser returns a page of the user's delivery log
	// (newest first) honoring the channel/status/notification filters in opts —
	// the cross-notification fleet view.
	ListDeliveryAttemptsForUser(ctx context.Context, recipientUserID string, opts ListOptions) (attempts []DeliveryAttempt, nextToken string, err error)
}

// PreferenceRepository is the persistence port for per-user NotificationPreferences.
type PreferenceRepository interface {
	// GetByUser returns the user's stored preferences, or ErrRepoNotFound if the
	// user has never configured any (the service then returns sane defaults rather
	// than erroring — the settings UI always has something to render).
	GetByUser(ctx context.Context, userID string) (NotificationPreferences, error)

	// Upsert replaces the user's preferences wholesale (PUT semantics) and returns
	// the stored result (with the server-stamped UpdatedAt). The service has
	// already SSRF-validated every external target before this is called.
	Upsert(ctx context.Context, prefs NotificationPreferences) (NotificationPreferences, error)
}

// IdempotencyStore guards against duplicate processing on BOTH paths (the
// platform's "idempotent consumer" rule).
//
//   - ASYNC consumer: NATS is at-least-once, so the same event can be redelivered.
//     The reactor checks SeenEvent(eventID, recipient) before creating an inbox
//     row, so a redelivered event never duplicates a notification or a delivery.
//   - SYNC mutation: UpdatePreferences/TestChannel carry an idempotency_key so a
//     client retry after a timeout returns the first result instead of re-applying.
//
// WHY one small port for both: both are "have I already done this (key)?" with a
// stored outcome. In production this is Redis SET NX with a TTL; in tests it is a
// map. The key namespace differentiates the two uses (event:<id>:<user> vs
// idem:<key>), which the service constructs.
type IdempotencyStore interface {
	// Seen reports whether key has been recorded already AND, if so, the stored
	// result bytes from the first time (nil if none stored). A false `seen` means
	// the caller should proceed and then call Record.
	Seen(ctx context.Context, key string) (seen bool, storedResult []byte, err error)
	// Record marks key as processed, optionally storing a small result blob for
	// retry-returns, with a TTL after which the key may be reclaimed (dedup is a
	// bounded-time guarantee, not forever — sized to exceed redelivery windows).
	Record(ctx context.Context, key string, result []byte, ttl time.Duration) error
}

// ============================================================================
// EXTERNAL DELIVERY PORT (the SSRF boundary)
// ============================================================================

// DeliveryTarget is the fully-resolved instruction the domain hands the Notifier:
// deliver this rendered content over this channel to this (SSRF-guarded) target.
// It is built from a ChannelDecision + the InboundEvent's rendered content.
type DeliveryTarget struct {
	// Channel is the transport (WEBHOOK/SLACK/EMAIL — never IN_APP, which is a DB
	// write, not an external delivery).
	Channel NotificationChannel
	// Target is the destination: an HTTPS URL (WEBHOOK/SLACK) or email address
	// (EMAIL). For WEBHOOK/SLACK this is attacker-influenced and MUST be
	// SSRF-validated by the adapter (see MustSSRFValidate).
	Target string
	// Title/Body are the rendered human-facing content.
	Title string
	Body  string
	// Payload is the OPAQUE original event bytes, forwarded verbatim for webhook
	// consumers that want the raw event (the domain never decoded it).
	Payload []byte
	// EventType is provenance the adapter may include in the outbound message.
	EventType string
}

// MustSSRFValidate reports whether the adapter is OBLIGATED to run the full SSRF
// guard on this target before any network call. This is the domain TELLING the
// adapter "this one is attacker-influenced" — the adapter must not skip it.
//
// THE MANDATED ADAPTER-SIDE GUARD (documented here because the domain can't run
// it without importing net, which would break purity — so it MUST live in the
// adapter, and this method is the contract that it WILL):
//  1. Scheme allowlist: https only (reject http/file/gopher/ftp/...).
//  2. Host allowlist for SLACK: must be hooks.slack.com.
//  3. Resolve DNS, reject if the IP is loopback (127/8, ::1), link-local
//     (169.254/16 — the cloud metadata endpoint — and fe80::/10), private
//     (10/8, 172.16/12, 192.168/16, fc00::/7), unspecified/multicast/reserved.
//  4. PIN the resolved IP and connect to it (defeats DNS-rebinding TOCTOU).
//  5. No credentials in userinfo; disable/cap redirects (a 302 must not bounce
//     to a denied address).
//
// A target that fails is rejected with ErrSSRFTargetRejected — it never reaches
// an outbound socket.
func (t DeliveryTarget) MustSSRFValidate() bool {
	return t.Channel.RequiresSSRFGuard()
}

// DeliveryResult is what the Notifier returns for one attempt: the outcome the
// adapter observed, mapped back to the domain's DeliveryStatus + the response
// detail for the audit log. It carries NO secret/target — only health detail.
type DeliveryResult struct {
	// Status is the outcome (DELIVERED / FAILED — RETRYING is an internal state of
	// the adapter's retry loop; the Notifier returns the TERMINAL result).
	Status DeliveryStatus
	// Attempts is how many tries the adapter made (1..retryBudget).
	Attempts int32
	// ResponseCode is the final HTTP status (0 for non-HTTP / no response).
	ResponseCode int32
	// ErrorMessage is the terminal failure detail (empty on success). Health
	// detail only — never the target URL or an auth header.
	ErrorMessage string
}

// Notifier is the external-delivery port: it performs the actual webhook/Slack/
// SMTP delivery WITH retries + circuit breaker, and (for HTTP channels) the
// mandated SSRF guard. The domain depends ONLY on this interface — it never
// imports net/http, so it cannot itself make an outbound request, which is
// exactly why the SSRF surface is contained in the adapter.
//
// WHY the retry/backoff/circuit-breaker logic lives in the ADAPTER, not the
// domain: those are I/O-timing concerns (sleep, clock, per-URL breaker state)
// that would force time and goroutines into the pure routing brain. The domain
// DECIDES what to deliver; the Notifier adapter DELIVERS it robustly. The
// reactor calls Notifier once per delivering channel and records the result.
type Notifier interface {
	// Deliver attempts delivery to t, running the SSRF guard first when
	// t.MustSSRFValidate() is true, retrying transient failures per the adapter's
	// budget, and returning the terminal DeliveryResult. It returns
	// ErrSSRFTargetRejected (wrapped) if the SSRF guard rejects the target — the
	// caller records a FAILED attempt with that reason and never retries it.
	Deliver(ctx context.Context, t DeliveryTarget) (DeliveryResult, error)
}

// ============================================================================
// AMBIENT PORTS (Clock / IDGenerator) — injected so the domain is deterministic
// ============================================================================

// Clock supplies the current time. WHY inject it rather than call time.Now()
// inside the service: the service stamps CreatedAt/ReadAt/UpdatedAt, and tests
// must assert exact values. An injected Clock makes time a controllable input —
// the difference between a flaky test and a deterministic one.
type Clock interface {
	Now() time.Time
}

// IDGenerator mints new UUIDs for notification ids. WHY a port (not uuid.NewString
// inline): same testability reason — a test injects a deterministic generator so
// it can assert the exact id written, and the domain stays free of a hard
// dependency on a specific id scheme.
type IDGenerator interface {
	NewID() string
}
