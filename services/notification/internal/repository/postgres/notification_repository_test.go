// notification_repository_test.go — integration tests for the NotificationRepository
// Postgres adapter against a REAL Postgres. Every domain port method is exercised:
// CRUD, the idempotent-create unique constraint, the anti-IDOR user scoping,
// keyset pagination + filters, the idempotent MarkRead/MarkAllRead transitions, and
// the delivery_log append/read paths (incl. the FK-missing-parent case). Run -race.
package postgres

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/abd-ulbasit/forgepoint/services/notification/internal/domain"
)

// ----------------------------------------------------------------------------
// Create + GetByIDForUser — round-trip + the anti-IDOR guarantee.
// ----------------------------------------------------------------------------

func TestNotificationRepo_CreateAndGet_RoundTrip(t *testing.T) {
	store, ctx := newStore(t)
	repo := store.Notifications()

	n := sampleNotification(uuid.NewString(), "user-alice", uuid.NewString())
	created, err := repo.Create(ctx, n)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := repo.GetByIDForUser(ctx, created.ID, "user-alice")
	if err != nil {
		t.Fatalf("GetByIDForUser: %v", err)
	}

	// Every server-authoritative field must round-trip, including the SMALLINT[]
	// channels (pgx array codec) and the NULL read_at → zero time mapping.
	if got.ID != n.ID || got.RecipientUserID != n.RecipientUserID || got.Title != n.Title ||
		got.Body != n.Body || got.Severity != n.Severity || got.Read != false ||
		got.EventID != n.EventID || got.EventType != n.EventType || got.SourceService != n.SourceService {
		t.Fatalf("round-trip mismatch:\n got %+v\nwant %+v", got, n)
	}
	if !got.CreatedAt.Equal(n.CreatedAt) {
		t.Fatalf("created_at = %v, want %v", got.CreatedAt, n.CreatedAt)
	}
	if !got.ReadAt.IsZero() {
		t.Fatalf("unread row read_at = %v, want zero time", got.ReadAt)
	}
	if len(got.Channels) != 2 || got.Channels[0] != domain.ChannelInApp || got.Channels[1] != domain.ChannelSlack {
		t.Fatalf("channels = %v, want [IN_APP SLACK]", got.Channels)
	}
}

// TestNotificationRepo_GetByIDForUser_AntiIDOR proves the user scope is enforced in
// the query: a notification owned by alice is INVISIBLE to bob, returned as the
// generic ErrRepoNotFound — indistinguishable from a truly-missing id, closing the
// enumeration side channel.
func TestNotificationRepo_GetByIDForUser_AntiIDOR(t *testing.T) {
	store, ctx := newStore(t)
	repo := store.Notifications()

	n := sampleNotification(uuid.NewString(), "user-alice", uuid.NewString())
	if _, err := repo.Create(ctx, n); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// bob asks for alice's notification by its real id → not found (not "forbidden").
	_, err := repo.GetByIDForUser(ctx, n.ID, "user-bob")
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("cross-user Get err = %v, want ErrRepoNotFound", err)
	}

	// A genuinely missing id for the rightful owner → the SAME error.
	_, err = repo.GetByIDForUser(ctx, uuid.NewString(), "user-alice")
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("missing-id Get err = %v, want ErrRepoNotFound", err)
	}
}

// TestNotificationRepo_Create_IdempotentOnEventRecipient verifies the durable
// idempotent-consumer backstop: a second Create with the SAME (event_id, recipient)
// trips the unique index and returns ErrRepoAlreadyExists — so a redelivered NATS
// event never duplicates an inbox row. A DIFFERENT recipient for the same event is
// allowed (the same event can notify many users).
func TestNotificationRepo_Create_IdempotentOnEventRecipient(t *testing.T) {
	store, ctx := newStore(t)
	repo := store.Notifications()

	eventID := uuid.NewString()
	first := sampleNotification(uuid.NewString(), "user-alice", eventID)
	if _, err := repo.Create(ctx, first); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	// Same event, same recipient, DIFFERENT notification id → still a dup (the
	// dedup key is (event, recipient), not the row id).
	dup := sampleNotification(uuid.NewString(), "user-alice", eventID)
	_, err := repo.Create(ctx, dup)
	if !errors.Is(err, ErrRepoAlreadyExists) {
		t.Fatalf("duplicate (event,recipient) Create err = %v, want ErrRepoAlreadyExists", err)
	}

	// Same event, DIFFERENT recipient → allowed.
	other := sampleNotification(uuid.NewString(), "user-bob", eventID)
	if _, err := repo.Create(ctx, other); err != nil {
		t.Fatalf("same-event different-recipient Create: %v", err)
	}
}

// ----------------------------------------------------------------------------
// CountUnread + MarkRead / MarkAllRead — idempotent read transitions.
// ----------------------------------------------------------------------------

func TestNotificationRepo_MarkRead_Idempotent(t *testing.T) {
	store, ctx := newStore(t)
	repo := store.Notifications()

	ids := make([]string, 3)
	for i := range ids {
		ids[i] = uuid.NewString()
		n := sampleNotification(ids[i], "user-alice", uuid.NewString())
		if _, err := repo.Create(ctx, n); err != nil {
			t.Fatalf("Create #%d: %v", i, err)
		}
	}

	if c, err := repo.CountUnread(ctx, "user-alice"); err != nil || c != 3 {
		t.Fatalf("CountUnread before = %d (err %v), want 3", c, err)
	}

	readAt := fixedTime.Add(time.Hour)

	// Mark two of them read; the count returned is the number that TRANSITIONED.
	marked, err := repo.MarkRead(ctx, "user-alice", ids[:2], readAt)
	if err != nil || marked != 2 {
		t.Fatalf("MarkRead first = %d (err %v), want 2", marked, err)
	}

	// IDEMPOTENT: re-marking the same two transitions NOTHING (already read).
	marked, err = repo.MarkRead(ctx, "user-alice", ids[:2], readAt)
	if err != nil || marked != 0 {
		t.Fatalf("MarkRead repeat = %d (err %v), want 0 (idempotent)", marked, err)
	}

	// The read_at timestamp was stamped from the injected `now` and the row is read.
	got, err := repo.GetByIDForUser(ctx, ids[0], "user-alice")
	if err != nil {
		t.Fatalf("Get after MarkRead: %v", err)
	}
	if !got.Read || !got.ReadAt.Equal(readAt) {
		t.Fatalf("after MarkRead read=%v read_at=%v, want true/%v", got.Read, got.ReadAt, readAt)
	}

	if c, err := repo.CountUnread(ctx, "user-alice"); err != nil || c != 1 {
		t.Fatalf("CountUnread after MarkRead = %d (err %v), want 1", c, err)
	}
}

// TestNotificationRepo_MarkRead_ForeignIdsIgnored proves MarkRead won't flip another
// user's notification even if the caller passes its real id — the user scope is in
// the UPDATE predicate, so a foreign id transitions nothing and is not counted.
func TestNotificationRepo_MarkRead_ForeignIdsIgnored(t *testing.T) {
	store, ctx := newStore(t)
	repo := store.Notifications()

	aliceID := uuid.NewString()
	if _, err := repo.Create(ctx, sampleNotification(aliceID, "user-alice", uuid.NewString())); err != nil {
		t.Fatalf("Create alice: %v", err)
	}

	// bob tries to mark alice's id (and a random one) read.
	marked, err := repo.MarkRead(ctx, "user-bob", []string{aliceID, uuid.NewString()}, fixedTime)
	if err != nil || marked != 0 {
		t.Fatalf("foreign MarkRead = %d (err %v), want 0", marked, err)
	}
	// alice's notification is still unread.
	got, _ := repo.GetByIDForUser(ctx, aliceID, "user-alice")
	if got.Read {
		t.Fatal("alice's notification was marked read by bob — anti-IDOR broken")
	}

	// Empty id list is a no-op (no pointless round-trip), returns 0.
	if marked, err := repo.MarkRead(ctx, "user-alice", nil, fixedTime); err != nil || marked != 0 {
		t.Fatalf("empty MarkRead = %d (err %v), want 0", marked, err)
	}
}

func TestNotificationRepo_MarkAllRead(t *testing.T) {
	store, ctx := newStore(t)
	repo := store.Notifications()

	for i := 0; i < 4; i++ {
		if _, err := repo.Create(ctx, sampleNotification(uuid.NewString(), "user-alice", uuid.NewString())); err != nil {
			t.Fatalf("Create #%d: %v", i, err)
		}
	}
	// A different user's unread row must NOT be touched by alice's MarkAllRead.
	bobID := uuid.NewString()
	if _, err := repo.Create(ctx, sampleNotification(bobID, "user-bob", uuid.NewString())); err != nil {
		t.Fatalf("Create bob: %v", err)
	}

	marked, err := repo.MarkAllRead(ctx, "user-alice", fixedTime)
	if err != nil || marked != 4 {
		t.Fatalf("MarkAllRead = %d (err %v), want 4", marked, err)
	}
	// IDEMPOTENT: a second call finds nothing unread.
	if marked, err := repo.MarkAllRead(ctx, "user-alice", fixedTime); err != nil || marked != 0 {
		t.Fatalf("MarkAllRead repeat = %d (err %v), want 0", marked, err)
	}
	// bob untouched.
	if c, err := repo.CountUnread(ctx, "user-bob"); err != nil || c != 1 {
		t.Fatalf("bob unread = %d (err %v), want 1 (cross-user isolation)", c, err)
	}
}

// ----------------------------------------------------------------------------
// ListForUser — keyset pagination + filters.
// ----------------------------------------------------------------------------

// TestNotificationRepo_List_PaginationStable walks the inbox a page at a time and
// asserts newest-first order, no gaps/dupes across page boundaries, and an empty
// final token. Distinct created_at per row gives a total sort order so the cursor is
// unambiguous.
func TestNotificationRepo_List_PaginationStable(t *testing.T) {
	store, ctx := newStore(t)
	repo := store.Notifications()

	const total = 7
	// Insert with increasing created_at so newest-first is a known order.
	want := make([]string, 0, total)
	for i := 0; i < total; i++ {
		id := uuid.NewString()
		n := sampleNotification(id, "user-alice", uuid.NewString())
		n.CreatedAt = fixedTime.Add(time.Duration(i) * time.Minute)
		if _, err := repo.Create(ctx, n); err != nil {
			t.Fatalf("Create #%d: %v", i, err)
		}
		want = append([]string{id}, want...) // prepend → newest-first expected order
	}

	var got []string
	token := ""
	pages := 0
	for {
		items, next, err := repo.ListForUser(ctx, "user-alice", domain.ListOptions{PageSize: 3, PageToken: token})
		if err != nil {
			t.Fatalf("ListForUser page %d: %v", pages, err)
		}
		for _, it := range items {
			got = append(got, it.ID)
		}
		pages++
		if next == "" {
			break
		}
		token = next
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
	}
	if len(got) != total {
		t.Fatalf("paged %d ids, want %d", len(got), total)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("page order mismatch at %d: got %s want %s", i, got[i], want[i])
		}
	}
}

// TestNotificationRepo_List_Filters covers UnreadOnly, MinSeverity (the numeric
// floor, including the "unspecified floor = no filter" rule), and EventTypeFilter.
func TestNotificationRepo_List_Filters(t *testing.T) {
	store, ctx := newStore(t)
	repo := store.Notifications()

	mk := func(sev domain.Severity, etype string, read bool) string {
		id := uuid.NewString()
		n := sampleNotification(id, "user-alice", uuid.NewString())
		n.Severity = sev
		n.EventType = etype
		n.Read = read
		if _, err := repo.Create(ctx, n); err != nil {
			t.Fatalf("Create: %v", err)
		}
		return id
	}

	infoID := mk(domain.SeverityInfo, "fp.pipelines.started", false)
	_ = mk(domain.SeverityWarning, "fp.billing.quota", true) // read → excluded by UnreadOnly
	critID := mk(domain.SeverityCritical, "fp.monitor.drift", false)

	// UnreadOnly: excludes the read WARNING row.
	items, _, err := repo.ListForUser(ctx, "user-alice", domain.ListOptions{UnreadOnly: true})
	if err != nil {
		t.Fatalf("List UnreadOnly: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("UnreadOnly returned %d, want 2", len(items))
	}

	// MinSeverity = ERROR(3): only the CRITICAL(4) row clears it (INFO/WARNING below).
	items, _, err = repo.ListForUser(ctx, "user-alice", domain.ListOptions{MinSeverity: domain.SeverityError})
	if err != nil {
		t.Fatalf("List MinSeverity: %v", err)
	}
	if len(items) != 1 || items[0].ID != critID {
		t.Fatalf("MinSeverity=ERROR returned %d (ids %v), want 1 (crit)", len(items), idsOf(items))
	}

	// MinSeverity = Unspecified(0): no floor → all 3 rows (the "0 means absent" rule).
	items, _, err = repo.ListForUser(ctx, "user-alice", domain.ListOptions{MinSeverity: domain.SeverityUnspecified})
	if err != nil {
		t.Fatalf("List no-floor: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("no-floor returned %d, want 3", len(items))
	}

	// EventTypeFilter: exact match on one type.
	items, _, err = repo.ListForUser(ctx, "user-alice", domain.ListOptions{EventTypeFilter: "fp.pipelines.started"})
	if err != nil {
		t.Fatalf("List EventTypeFilter: %v", err)
	}
	if len(items) != 1 || items[0].ID != infoID {
		t.Fatalf("EventTypeFilter returned %d (ids %v), want 1 (info)", len(items), idsOf(items))
	}
}

// TestNotificationRepo_List_RejectsBadToken confirms a malformed cursor is a client
// error surfaced (not silently treated as page 1).
func TestNotificationRepo_List_RejectsBadToken(t *testing.T) {
	store, ctx := newStore(t)
	repo := store.Notifications()
	if _, _, err := repo.ListForUser(ctx, "user-alice", domain.ListOptions{PageToken: "!!!not-base64!!!"}); err == nil {
		t.Fatal("ListForUser with a malformed token returned nil error, want a parse error")
	}
}

// ----------------------------------------------------------------------------
// delivery_log — append + read + the missing-parent FK case.
// ----------------------------------------------------------------------------

func TestNotificationRepo_DeliveryLog_AppendAndGet(t *testing.T) {
	store, ctx := newStore(t)
	repo := store.Notifications()

	notifID := uuid.NewString()
	if _, err := repo.Create(ctx, sampleNotification(notifID, "user-alice", uuid.NewString())); err != nil {
		t.Fatalf("Create notification: %v", err)
	}

	// Two attempts on the same notification, oldest first.
	a1 := sampleAttempt(notifID, domain.ChannelSlack, domain.StatusFailed, 1, fixedTime)
	a1.ResponseCode = 503
	a1.ErrorMessage = "upstream 503"
	a2 := sampleAttempt(notifID, domain.ChannelSlack, domain.StatusDelivered, 2, fixedTime.Add(time.Second))
	for _, a := range []domain.DeliveryAttempt{a1, a2} {
		if err := repo.AppendDeliveryAttempt(ctx, a); err != nil {
			t.Fatalf("AppendDeliveryAttempt: %v", err)
		}
	}
	// A SUPPRESSED record (attempt 0) for a different channel — a held decision is
	// recorded, not dropped.
	supp := domain.DeliveryAttempt{
		NotificationID: notifID,
		Channel:        domain.ChannelEmail,
		Status:         domain.StatusSuppressed,
		Attempt:        0,
		AttemptedAt:    fixedTime.Add(2 * time.Second),
	}
	if err := repo.AppendDeliveryAttempt(ctx, supp); err != nil {
		t.Fatalf("AppendDeliveryAttempt suppressed: %v", err)
	}

	got, err := repo.GetAttemptsForNotification(ctx, "user-alice", notifID)
	if err != nil {
		t.Fatalf("GetAttemptsForNotification: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d attempts, want 3", len(got))
	}
	// Oldest-first ordering.
	if got[0].Status != domain.StatusFailed || got[1].Status != domain.StatusDelivered || got[2].Status != domain.StatusSuppressed {
		t.Fatalf("attempt order/status wrong: %+v", got)
	}
	if got[0].ResponseCode != 503 || got[0].ErrorMessage != "upstream 503" {
		t.Fatalf("first attempt detail = %d/%q, want 503/upstream 503", got[0].ResponseCode, got[0].ErrorMessage)
	}

	// Anti-IDOR: bob cannot read alice's notification's attempts.
	bobAttempts, err := repo.GetAttemptsForNotification(ctx, "user-bob", notifID)
	if err != nil {
		t.Fatalf("GetAttemptsForNotification(bob): %v", err)
	}
	if len(bobAttempts) != 0 {
		t.Fatalf("bob saw %d of alice's attempts, want 0", len(bobAttempts))
	}
}

// TestNotificationRepo_DeliveryLog_MissingParent verifies that appending an attempt
// for a notification id that doesn't exist returns ErrRepoNotFound (no orphan rows).
func TestNotificationRepo_DeliveryLog_MissingParent(t *testing.T) {
	store, ctx := newStore(t)
	repo := store.Notifications()

	err := repo.AppendDeliveryAttempt(ctx, sampleAttempt(uuid.NewString(), domain.ChannelSlack, domain.StatusFailed, 1, fixedTime))
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("append to missing parent err = %v, want ErrRepoNotFound", err)
	}
}

// TestNotificationRepo_ListDeliveryAttempts_FiltersAndPaging covers the fleet view:
// the channel/status/notification filters and keyset pagination newest-first.
func TestNotificationRepo_ListDeliveryAttempts_FiltersAndPaging(t *testing.T) {
	store, ctx := newStore(t)
	repo := store.Notifications()

	notifA := uuid.NewString()
	notifB := uuid.NewString()
	for _, id := range []string{notifA, notifB} {
		if _, err := repo.Create(ctx, sampleNotification(id, "user-alice", uuid.NewString())); err != nil {
			t.Fatalf("Create %s: %v", id, err)
		}
	}

	// 6 attempts across two notifications/channels/statuses, increasing time.
	type spec struct {
		notif  string
		ch     domain.NotificationChannel
		st     domain.DeliveryStatus
		offset time.Duration
	}
	specs := []spec{
		{notifA, domain.ChannelSlack, domain.StatusFailed, 0},
		{notifA, domain.ChannelSlack, domain.StatusDelivered, time.Second},
		{notifA, domain.ChannelEmail, domain.StatusDelivered, 2 * time.Second},
		{notifB, domain.ChannelSlack, domain.StatusFailed, 3 * time.Second},
		{notifB, domain.ChannelWebhook, domain.StatusDelivered, 4 * time.Second},
		{notifB, domain.ChannelWebhook, domain.StatusFailed, 5 * time.Second},
	}
	for i, s := range specs {
		a := sampleAttempt(s.notif, s.ch, s.st, int32(i+1), fixedTime.Add(s.offset))
		if err := repo.AppendDeliveryAttempt(ctx, a); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// No filter: all 6, newest-first (last appended first).
	all, _, err := repo.ListDeliveryAttemptsForUser(ctx, "user-alice", domain.ListOptions{})
	if err != nil {
		t.Fatalf("List all: %v", err)
	}
	if len(all) != 6 {
		t.Fatalf("List all returned %d, want 6", len(all))
	}
	if !all[0].AttemptedAt.After(all[1].AttemptedAt) {
		t.Fatalf("not newest-first: %v then %v", all[0].AttemptedAt, all[1].AttemptedAt)
	}

	// Channel filter = SLACK → 3 (slack-A-failed, slack-A-delivered, slack-B-failed).
	slack, _, err := repo.ListDeliveryAttemptsForUser(ctx, "user-alice", domain.ListOptions{Channel: domain.ChannelSlack})
	if err != nil {
		t.Fatalf("List slack: %v", err)
	}
	if len(slack) != 3 {
		t.Fatalf("channel=SLACK returned %d, want 3", len(slack))
	}

	// Status filter = FAILED → 3 (slack-A, slack-B, webhook-B).
	failed, _, err := repo.ListDeliveryAttemptsForUser(ctx, "user-alice", domain.ListOptions{Status: domain.StatusFailed})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(failed) != 3 {
		t.Fatalf("status=FAILED returned %d, want 3", len(failed))
	}

	// Notification filter = notifB → 3.
	bOnly, _, err := repo.ListDeliveryAttemptsForUser(ctx, "user-alice", domain.ListOptions{NotificationID: notifB})
	if err != nil {
		t.Fatalf("List notifB: %v", err)
	}
	if len(bOnly) != 3 {
		t.Fatalf("notification=B returned %d, want 3", len(bOnly))
	}

	// Pagination: page size 2 across the 6, no dupes/gaps.
	seen := map[string]int{}
	token := ""
	count := 0
	for {
		items, next, err := repo.ListDeliveryAttemptsForUser(ctx, "user-alice", domain.ListOptions{PageSize: 2, PageToken: token})
		if err != nil {
			t.Fatalf("paged list: %v", err)
		}
		for _, it := range items {
			key := it.NotificationID + it.AttemptedAt.String()
			seen[key]++
			count++
		}
		if next == "" {
			break
		}
		token = next
	}
	if count != 6 || len(seen) != 6 {
		t.Fatalf("paged %d attempts (%d unique), want 6/6 — gap or dup", count, len(seen))
	}
}

// idsOf is a tiny test helper to print notification ids in failure messages.
func idsOf(ns []domain.Notification) []string {
	out := make([]string, len(ns))
	for i, n := range ns {
		out[i] = n.ID
	}
	return out
}
