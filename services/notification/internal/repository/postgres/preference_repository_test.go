// preference_repository_test.go — integration tests for the PreferenceRepository
// Postgres adapter against a REAL Postgres. Focus: the transactional Upsert
// (aggregate-in-two-tables written atomically with PUT semantics), the not-found →
// service-default boundary, and the full round-trip of the channel rows + muted
// patterns. Run -race.
package postgres

import (
	"errors"
	"testing"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/notification/internal/domain"
)

// samplePrefs builds a valid two-channel preference document for a user.
func samplePrefs(userID string) domain.NotificationPreferences {
	return domain.NotificationPreferences{
		UserID: userID,
		Channels: []domain.ChannelPreference{
			{Channel: domain.ChannelSlack, Enabled: true, MinSeverity: domain.SeverityWarning, Target: "https://hooks.slack.com/services/T/B/X"},
			{Channel: domain.ChannelEmail, Enabled: true, MinSeverity: domain.SeverityError, Target: "alice@example.com"},
		},
		MutedEventPatterns: []string{"fp.inference.*", "fp.debug.*"},
		UpdatedAt:          fixedTime,
	}
}

func TestPreferenceRepo_GetByUser_NotFound(t *testing.T) {
	store, ctx := newStore(t)
	repo := store.Preferences()

	_, err := repo.GetByUser(ctx, "never-configured")
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("GetByUser(absent) err = %v, want ErrRepoNotFound", err)
	}
}

// TestPreferenceRepo_Upsert_RoundTrip writes a document and reads it back, asserting
// the channel rows (order, enabled, floor, target) and muted patterns survive the
// aggregate split intact.
func TestPreferenceRepo_Upsert_RoundTrip(t *testing.T) {
	store, ctx := newStore(t)
	repo := store.Preferences()

	in := samplePrefs("user-alice")
	stored, err := repo.Upsert(ctx, in)
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if stored.UserID != "user-alice" || !stored.UpdatedAt.Equal(fixedTime) {
		t.Fatalf("Upsert returned %+v, want user-alice / %v", stored, fixedTime)
	}

	got, err := repo.GetByUser(ctx, "user-alice")
	if err != nil {
		t.Fatalf("GetByUser: %v", err)
	}
	if len(got.MutedEventPatterns) != 2 || got.MutedEventPatterns[0] != "fp.inference.*" {
		t.Fatalf("muted patterns = %v, want [fp.inference.* fp.debug.*]", got.MutedEventPatterns)
	}
	// Channels come back ordered by channel int (SLACK=3 before EMAIL=4).
	if len(got.Channels) != 2 {
		t.Fatalf("channels = %d, want 2", len(got.Channels))
	}
	if got.Channels[0].Channel != domain.ChannelSlack || got.Channels[0].MinSeverity != domain.SeverityWarning ||
		got.Channels[0].Target != "https://hooks.slack.com/services/T/B/X" || !got.Channels[0].Enabled {
		t.Fatalf("slack channel pref wrong: %+v", got.Channels[0])
	}
	if got.Channels[1].Channel != domain.ChannelEmail || got.Channels[1].MinSeverity != domain.SeverityError ||
		got.Channels[1].Target != "alice@example.com" {
		t.Fatalf("email channel pref wrong: %+v", got.Channels[1])
	}
}

// TestPreferenceRepo_Upsert_ReplacesChildSet is the PUT-semantics test: a second
// Upsert with a DIFFERENT channel set must fully replace the first — omitted
// channels disappear, new ones appear, and the root fields update. This proves the
// delete-then-insert inside the transaction (not a per-row merge).
func TestPreferenceRepo_Upsert_ReplacesChildSet(t *testing.T) {
	store, ctx := newStore(t)
	repo := store.Preferences()

	// First: SLACK + EMAIL.
	if _, err := repo.Upsert(ctx, samplePrefs("user-alice")); err != nil {
		t.Fatalf("first Upsert: %v", err)
	}

	// Second: ONLY WEBHOOK, different muted list, newer updated_at. SLACK+EMAIL must vanish.
	second := domain.NotificationPreferences{
		UserID: "user-alice",
		Channels: []domain.ChannelPreference{
			{Channel: domain.ChannelWebhook, Enabled: false, MinSeverity: domain.SeverityCritical, Target: "https://example.com/hook"},
		},
		MutedEventPatterns: []string{"fp.everything.*"},
		UpdatedAt:          fixedTime.Add(time.Hour),
	}
	if _, err := repo.Upsert(ctx, second); err != nil {
		t.Fatalf("second Upsert: %v", err)
	}

	got, err := repo.GetByUser(ctx, "user-alice")
	if err != nil {
		t.Fatalf("GetByUser after replace: %v", err)
	}
	if len(got.Channels) != 1 || got.Channels[0].Channel != domain.ChannelWebhook {
		t.Fatalf("after replace channels = %+v, want only WEBHOOK", got.Channels)
	}
	if got.Channels[0].Enabled != false || got.Channels[0].MinSeverity != domain.SeverityCritical {
		t.Fatalf("webhook pref wrong after replace: %+v", got.Channels[0])
	}
	if len(got.MutedEventPatterns) != 1 || got.MutedEventPatterns[0] != "fp.everything.*" {
		t.Fatalf("muted after replace = %v, want [fp.everything.*]", got.MutedEventPatterns)
	}
	if !got.UpdatedAt.Equal(fixedTime.Add(time.Hour)) {
		t.Fatalf("updated_at after replace = %v, want %v", got.UpdatedAt, fixedTime.Add(time.Hour))
	}
}

// TestPreferenceRepo_Upsert_EmptyChannels stores a document with NO channel rows
// (valid: a user who muted everything / cleared their channels). It must read back
// as an empty-channels document, NOT as not-found (the root row exists).
func TestPreferenceRepo_Upsert_EmptyChannels(t *testing.T) {
	store, ctx := newStore(t)
	repo := store.Preferences()

	in := domain.NotificationPreferences{
		UserID:             "user-empty",
		Channels:           nil,
		MutedEventPatterns: nil,
		UpdatedAt:          fixedTime,
	}
	if _, err := repo.Upsert(ctx, in); err != nil {
		t.Fatalf("Upsert empty: %v", err)
	}
	got, err := repo.GetByUser(ctx, "user-empty")
	if err != nil {
		t.Fatalf("GetByUser empty: %v", err)
	}
	if len(got.Channels) != 0 {
		t.Fatalf("empty doc channels = %d, want 0", len(got.Channels))
	}
	if len(got.MutedEventPatterns) != 0 {
		t.Fatalf("empty doc muted = %d, want 0", len(got.MutedEventPatterns))
	}
}

// TestPreferenceRepo_Upsert_AtomicNoPartialWrite proves the transaction boundary: a
// failing Upsert (a duplicate channel in the input trips the (user_id, channel) PK
// inside the tx) must leave the PRIOR state untouched — no partial write of the new
// root or a subset of the new children. This is the all-or-nothing aggregate
// guarantee the tx provides.
func TestPreferenceRepo_Upsert_AtomicNoPartialWrite(t *testing.T) {
	store, ctx := newStore(t)
	repo := store.Preferences()

	// Establish a known-good prior state.
	if _, err := repo.Upsert(ctx, samplePrefs("user-alice")); err != nil {
		t.Fatalf("seed Upsert: %v", err)
	}

	// A bad update: the SAME channel twice → the second child INSERT violates the
	// (user_id, channel) primary key, aborting the whole tx (root upsert + deletes
	// roll back too).
	bad := domain.NotificationPreferences{
		UserID: "user-alice",
		Channels: []domain.ChannelPreference{
			{Channel: domain.ChannelWebhook, Enabled: true, Target: "https://a.example.com"},
			{Channel: domain.ChannelWebhook, Enabled: true, Target: "https://b.example.com"}, // dup PK
		},
		MutedEventPatterns: []string{"fp.broken.*"},
		UpdatedAt:          fixedTime.Add(2 * time.Hour),
	}
	if _, err := repo.Upsert(ctx, bad); err == nil {
		t.Fatal("Upsert with a duplicate channel succeeded, want a constraint error")
	}

	// The PRIOR state must be fully intact — the failed tx wrote nothing.
	got, err := repo.GetByUser(ctx, "user-alice")
	if err != nil {
		t.Fatalf("GetByUser after failed Upsert: %v", err)
	}
	if len(got.Channels) != 2 {
		t.Fatalf("after failed Upsert channels = %d, want 2 (prior state intact)", len(got.Channels))
	}
	if !got.UpdatedAt.Equal(fixedTime) {
		t.Fatalf("after failed Upsert updated_at = %v, want prior %v (root not partially written)", got.UpdatedAt, fixedTime)
	}
	if len(got.MutedEventPatterns) != 2 || got.MutedEventPatterns[0] != "fp.inference.*" {
		t.Fatalf("after failed Upsert muted = %v, want prior [fp.inference.* fp.debug.*]", got.MutedEventPatterns)
	}
}
