// notification_service.go — the NotificationService interface (the primary port
// the handler and the NATS consumer reach the business logic through).
//
// ============================================================================
// TWO SURFACES, ONE BRAIN
// ============================================================================
//
// This service is unusual among the 10: its real work is ASYNC (the choreography
// reactor), and its gRPC surface is a tiny CONTROL PLANE. The interface reflects
// that split:
//
//	REACTOR (async path, called by the NATS consumer adapter):
//	  ReactToEvent — the pure routing brain. Decide which channels fire for an
//	  inbound event, given the recipient's preferences. THIS is the choreography.
//
//	CONTROL PLANE (sync path, called by the gRPC handler):
//	  ListNotifications / GetNotification / MarkRead   — the inbox read model
//	  ListDeliveryAttempts                             — the delivery-log audit
//	  GetPreferences / UpdatePreferences / TestChannel — settings
//
// WHY both on ONE interface rather than two: they share the same repositories,
// the same severity-derivation, and the same SSRF validation. Splitting them
// would duplicate that or force a shared helper struct anyway. One interface, two
// method groups, clearly labelled.
//
// WHY the interface (port) lives in the domain: dependency inversion — the
// handler and the consumer depend on this ABSTRACTION, never on the concrete
// notificationService. Tests inject a stub; production injects the real impl.
package domain

import "context"

// ============================================================================
// INPUT / OUTPUT TYPES (handler converts proto ⇄ these; consumer builds these)
// ============================================================================
//
// WHY dedicated input structs instead of bare params: adding a field later
// (e.g. a new list filter) is backward-compatible, and callers construct by name
// so they can't transpose positional args. The service NEVER sees proto types.

// ListNotificationsInput is the validated input for the inbox list. RecipientUserID
// is set by the handler from auth claims — it is NOT a wire field (the proto has
// no user_id; accepting one would be an IDOR hole). The service caps PageSize.
type ListNotificationsInput struct {
	RecipientUserID string // from auth claims, server-authoritative
	UnreadOnly      bool
	MinSeverity     Severity
	EventTypeFilter string
	PageSize        int
	PageToken       string
}

// ListNotificationsOutput is one inbox page plus the badge count.
type ListNotificationsOutput struct {
	Notifications []Notification
	NextPageToken string
	UnreadCount   int // total unread across the whole inbox, for the UI badge
}

// MarkReadInput marks ids (or all) read for the caller. MarkAll ignores Ids.
type MarkReadInput struct {
	RecipientUserID string
	Ids             []string
	MarkAll         bool
}

// MarkReadOutput reports how many flipped and the new unread badge count.
type MarkReadOutput struct {
	MarkedCount int
	UnreadCount int
}

// GetNotificationOutput is one notification plus its per-channel attempt history.
type GetNotificationOutput struct {
	Notification     Notification
	DeliveryAttempts []DeliveryAttempt
}

// ListDeliveryAttemptsInput is the cross-notification delivery-log query.
type ListDeliveryAttemptsInput struct {
	RecipientUserID string
	Channel         NotificationChannel
	Status          DeliveryStatus
	NotificationID  string
	PageSize        int
	PageToken       string
}

// ListDeliveryAttemptsOutput is one page of the delivery log. NotificationIDs is
// carried on each DeliveryAttempt (domain type) so no parallel slice is needed
// here; the handler builds the proto's parallel slice at the boundary.
type ListDeliveryAttemptsOutput struct {
	Attempts      []DeliveryAttempt
	NextPageToken string
}

// UpdatePreferencesInput is the wholesale (PUT) preferences replacement. UserID is
// server-set from auth claims (never client-set). IdempotencyKey makes a retry
// safe. The service SSRF-validates every external Target before persisting.
type UpdatePreferencesInput struct {
	UserID             string
	Channels           []ChannelPreference
	MutedEventPatterns []string
	IdempotencyKey     string
}

// TestChannelInput asks to send a synthetic test over one of the CALLER'S OWN
// configured channels. It carries NO target URL (so it cannot be an SSRF probe);
// the destination is the caller's stored, already-validated ChannelPreference.
type TestChannelInput struct {
	UserID         string
	Channel        NotificationChannel
	IdempotencyKey string
}

// TestChannelOutput is the synthetic delivery's outcome for the settings UI.
type TestChannelOutput struct {
	Status       DeliveryStatus
	ResponseCode int32
	ErrorMessage string
}

// ============================================================================
// THE SERVICE INTERFACE
// ============================================================================

// NotificationService is the primary domain port. The handler holds one and
// calls its control-plane methods; the NATS consumer holds one and calls
// ReactToEvent. The concrete impl (notificationService) is in
// notification_service_impl.go and is wired in main.go once the repos exist.
type NotificationService interface {
	// ------------------------------------------------------------------
	// REACTOR (the choreography centerpiece — async path)
	// ------------------------------------------------------------------

	// ReactToEvent is the pure routing brain. Given an opaque inbound platform
	// event and the recipient's preferences, it decides — with NO side effects —
	// which channels should fire and which are suppressed (and why), deriving the
	// event's severity from its type. It returns a RoutingDecision the caller (the
	// NATS consumer adapter) then EXECUTES: write the inbox row, fire the Notifier
	// for each delivering channel, record a SUPPRESSED attempt for the rest.
	//
	// IDEMPOTENCY: the method itself is pure and idempotent by construction (same
	// inputs → same decision). The DEDUP against NATS redelivery happens in the
	// consumer adapter via IdempotencyStore keyed on (event.EventID, recipient)
	// BEFORE it executes the decision — documented on the impl. We keep ReactToEvent
	// side-effect-free so it is trivially testable without any mock.
	//
	// WHY this signature takes preferences as a PARAMETER rather than loading them:
	// it keeps the brain a pure function of its inputs. The consumer loads the
	// recipient's prefs (one cached lookup) and passes them in, so the decision
	// logic has zero I/O and the tests need zero mocks. This is "functional core,
	// imperative shell" made concrete.
	ReactToEvent(ctx context.Context, event InboundEvent, prefs NotificationPreferences) (RoutingDecision, error)

	// ------------------------------------------------------------------
	// INBOX (read model — sync path)
	// ------------------------------------------------------------------

	// ListNotifications returns a page of the caller's inbox (newest first) with
	// optional filters and the unread badge count. Scoped to the caller.
	ListNotifications(ctx context.Context, in ListNotificationsInput) (ListNotificationsOutput, error)

	// GetNotification returns one of the caller's notifications in full, including
	// its per-channel delivery attempts. Returns ErrNotFound for ids that don't
	// exist OR belong to another user (no existence leak).
	GetNotification(ctx context.Context, recipientUserID, id string) (GetNotificationOutput, error)

	// MarkRead marks one, many, or all of the caller's notifications read and
	// returns the new unread count. Naturally idempotent; foreign/unknown ids are
	// skipped (the batch still makes progress).
	MarkRead(ctx context.Context, in MarkReadInput) (MarkReadOutput, error)

	// ------------------------------------------------------------------
	// DELIVERY LOG (audit — sync path)
	// ------------------------------------------------------------------

	// ListDeliveryAttempts returns a page of the caller's delivery log (newest
	// first) with optional channel/status/notification filters. Targets/secrets
	// are never echoed. Scoped to the caller.
	ListDeliveryAttempts(ctx context.Context, in ListDeliveryAttemptsInput) (ListDeliveryAttemptsOutput, error)

	// ------------------------------------------------------------------
	// PREFERENCES (config — sync path)
	// ------------------------------------------------------------------

	// GetPreferences returns the caller's preferences, falling back to sensible
	// defaults (IN_APP enabled, all else off) if they've never configured any.
	GetPreferences(ctx context.Context, userID string) (NotificationPreferences, error)

	// UpdatePreferences replaces the caller's preferences wholesale (PUT) and
	// returns the stored result. Every external target is SSRF-validated server-
	// side before persisting; a bad target rejects the WHOLE update with
	// ErrValidation/ErrSSRFTargetRejected (so a malicious URL never reaches
	// storage). The idempotency key makes a retry safe.
	UpdatePreferences(ctx context.Context, in UpdatePreferencesInput) (NotificationPreferences, error)

	// TestChannel sends a synthetic test to one of the caller's OWN configured
	// channels and returns the outcome. Carries no target URL (not an SSRF vector);
	// the destination is the stored, already-validated target. ErrChannelNotConfigured
	// if the caller has no enabled preference for that channel.
	TestChannel(ctx context.Context, in TestChannelInput) (TestChannelOutput, error)
}
