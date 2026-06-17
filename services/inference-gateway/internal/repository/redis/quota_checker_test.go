// quota_checker_test.go — integration tests for the QuotaChecker against a REAL
// Redis. Verifies the real flag semantics (block → IsBlocked true; unblock → false),
// per-team isolation, idempotent unblock, TTL-based self-clearing, and the
// fail-open contract when Redis is unreachable.
package redisrepo

import (
	"context"
	"testing"
	"time"
)

func TestQuotaChecker_BlockUnblockCycle(t *testing.T) {
	rdb := newRedis(t)
	ctx := context.Background()
	q := NewQuotaChecker(rdb)

	// Default: a team with no flag is allowed.
	if q.IsBlocked(ctx, "team-acme") {
		t.Fatal("fresh team IsBlocked = true, want false")
	}

	// QuotaExceeded consumer flips the flag (sticky: ttl=0).
	if err := q.Block(ctx, "team-acme", 0); err != nil {
		t.Fatalf("Block: %v", err)
	}
	if !q.IsBlocked(ctx, "team-acme") {
		t.Fatal("after Block IsBlocked = false, want true")
	}

	// QuotaRestored consumer clears it.
	if err := q.Unblock(ctx, "team-acme"); err != nil {
		t.Fatalf("Unblock: %v", err)
	}
	if q.IsBlocked(ctx, "team-acme") {
		t.Fatal("after Unblock IsBlocked = true, want false")
	}
}

// TestQuotaChecker_TeamsIsolated confirms blocking one team does not block another.
func TestQuotaChecker_TeamsIsolated(t *testing.T) {
	rdb := newRedis(t)
	ctx := context.Background()
	q := NewQuotaChecker(rdb)

	if err := q.Block(ctx, "team-blocked", 0); err != nil {
		t.Fatalf("Block: %v", err)
	}
	if !q.IsBlocked(ctx, "team-blocked") {
		t.Fatal("team-blocked should be blocked")
	}
	if q.IsBlocked(ctx, "team-other") {
		t.Fatal("team-other was blocked by another team's flag (isolation broken)")
	}
}

// TestQuotaChecker_UnblockIdempotent confirms clearing an already-clear flag is a
// no-op, not an error (redelivered QuotaRestored must be safe).
func TestQuotaChecker_UnblockIdempotent(t *testing.T) {
	rdb := newRedis(t)
	ctx := context.Background()
	q := NewQuotaChecker(rdb)

	if err := q.Unblock(ctx, "never-blocked"); err != nil {
		t.Fatalf("Unblock(absent): %v", err)
	}
	if err := q.Block(ctx, "t", 0); err != nil {
		t.Fatalf("Block: %v", err)
	}
	if err := q.Unblock(ctx, "t"); err != nil {
		t.Fatalf("Unblock #1: %v", err)
	}
	if err := q.Unblock(ctx, "t"); err != nil {
		t.Fatalf("Unblock #2 (idempotent): %v", err)
	}
}

// TestQuotaChecker_TTLSelfClears proves a TTL-bounded block self-clears even if no
// QuotaRestored event ever arrives (the billing-period-reset safety net).
func TestQuotaChecker_TTLSelfClears(t *testing.T) {
	rdb := newRedis(t)
	ctx := context.Background()
	q := NewQuotaChecker(rdb)

	if err := q.Block(ctx, "team-ttl", 150*time.Millisecond); err != nil {
		t.Fatalf("Block: %v", err)
	}
	if !q.IsBlocked(ctx, "team-ttl") {
		t.Fatal("immediately after Block should be blocked")
	}
	time.Sleep(250 * time.Millisecond) // outlive the TTL
	if q.IsBlocked(ctx, "team-ttl") {
		t.Fatal("block did not self-clear after TTL expiry")
	}
}

// TestQuotaChecker_FailOpenOnRedisDown is the contract test: when Redis is
// unreachable, IsBlocked returns false (fail-open) rather than blocking all
// traffic. We close the client to simulate the outage. This is the deliberate
// soft-cap policy the adapter OWNS (the port returns a pure bool for exactly this).
func TestQuotaChecker_FailOpenOnRedisDown(t *testing.T) {
	rdb := newRedis(t)
	ctx := context.Background()
	q := NewQuotaChecker(rdb)

	// Block the team while Redis is up...
	if err := q.Block(ctx, "team-x", 0); err != nil {
		t.Fatalf("Block: %v", err)
	}
	if !q.IsBlocked(ctx, "team-x") {
		t.Fatal("precondition: team-x should be blocked")
	}

	// ...then kill the connection. A subsequent IsBlocked errors internally and
	// must FAIL OPEN (false), not propagate the error or default to blocked.
	if err := rdb.Close(); err != nil {
		t.Fatalf("close redis client: %v", err)
	}
	if q.IsBlocked(ctx, "team-x") {
		t.Fatal("IsBlocked returned true with Redis down — must fail OPEN (false)")
	}

	// Sanity: the closed client really does error (guards against a false pass
	// where Close didn't actually break the connection).
	if err := rdb.Ping(context.Background()).Err(); err == nil {
		t.Fatal("expected closed client to error on Ping (test setup invalid)")
	}
}
