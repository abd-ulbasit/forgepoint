// harness_test.go — shared integration-test setup for the Postgres adapters.
//
// Every test in this package runs against a REAL Postgres (testcontainers) with the
// production .up.sql migrations applied — no mocks, no in-memory fake. That is the
// brief: verify the adapters against the actual store so SQL syntax, array codecs,
// constraints, transaction atomicity, and keyset pagination are exercised exactly
// as they ship. SkipIfNoDocker keeps the suite green on a machine without a Docker
// engine (it skips, doesn't fail).
package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/notification/internal/domain"
)

// newStore spins up a fresh Postgres container, applies the migrations, and returns
// a Store wired to the pool plus a context. The container and pool are torn down via
// t.Cleanup. Each test gets its OWN container — total isolation, no shared state, so
// tests can run in any order (and -race-parallel) without cross-contamination.
func newStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	testutil.SkipIfNoDocker(t) // skip cleanly when the remote engine is unavailable

	ctx := context.Background()
	dsn := testutil.StartPostgres(t)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	t.Cleanup(pool.Close)

	applyMigrations(t, ctx, pool)
	return NewWithPool(pool), ctx
}

// fixedTime is a stable, timezone-explicit instant tests use so timestamp
// assertions are exact and deterministic (the domain injects a Clock; here we just
// pass concrete times into the repo methods). UTC so the TIMESTAMPTZ round-trip is
// unambiguous.
var fixedTime = time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)

// sampleNotification builds a valid, fully-populated inbox row for a recipient. The
// caller overrides id/event id as needed for the specific case under test. All
// fields are set the way the routing engine would set them (server-authoritative).
func sampleNotification(id, recipient, eventID string) domain.Notification {
	return domain.Notification{
		ID:              id,
		RecipientUserID: recipient,
		Title:           "Pipeline failed",
		Body:            "Your training pipeline fraud-v3 failed at the eval step.",
		Severity:        domain.SeverityError,
		Channels:        []domain.NotificationChannel{domain.ChannelInApp, domain.ChannelSlack},
		Read:            false,
		EventID:         eventID,
		EventType:       "fp.pipelines.failed",
		SourceService:   "pipeline",
		CreatedAt:       fixedTime,
		// ReadAt left zero → stored as NULL (unread).
	}
}

// sampleAttempt builds a delivery_log attempt for a notification.
func sampleAttempt(notificationID string, ch domain.NotificationChannel, st domain.DeliveryStatus, attempt int32, at time.Time) domain.DeliveryAttempt {
	return domain.DeliveryAttempt{
		NotificationID: notificationID,
		Channel:        ch,
		Status:         st,
		Attempt:        attempt,
		ResponseCode:   200,
		AttemptedAt:    at,
	}
}
