// quota_checker.go — the QuotaChecker adapter: an eventually-consistent per-team
// "blocked" flag. Implements domain.QuotaChecker.
//
// ============================================================================
// WHAT THIS IS (and what it deliberately is NOT)
// ============================================================================
//
// This is the FAST PRE-FLIGHT billing gate the use-case checks FIRST — before it
// spends a rate-limit token — to stop doing unbillable work for a team that is
// over its billing-period quota. It is a SOFT cap, not a ledger:
//
//   - The flag is flipped to "blocked" when the gateway consumes a QuotaExceeded
//     event off the bus, and cleared when usage resets (new billing period) or a
//     QuotaRestored event arrives.
//   - Billing owns the EXACT usage count (Postgres + outbox); the gateway is not
//     the billing source of truth. A team that just crossed its quota may get a
//     few more predicts through before the cache flips — acceptable for a soft cap,
//     and Billing reconciles the precise count from the metering events.
//
// Distinct from the rate limiter: quota is a billing-period cap (slow, per-TEAM);
// rate limiting is throughput shaping (fast, per-KEY). All of a team's keys share
// one quota, so this is keyed by team (from verified claims, never a request field).
//
// ============================================================================
// FAIL-OPEN IS OWNED HERE (a contract decision, not laziness)
// ============================================================================
//
// IsBlocked returns a pure bool — no error. The port (ports.go) deliberately puts
// the fail-open policy INSIDE this adapter: a cache miss, a Redis blip, or a
// timeout is treated as "not blocked". WHY fail-open here (the opposite of, say, an
// auth check):
//   - This is a SOFT usage cap, not a security boundary. Letting a few extra
//     predicts through during a Redis hiccup costs at most a little un-pre-rejected
//     work that Billing still meters correctly.
//   - Fail-CLOSED would mean a transient Redis blip rejects ALL inference traffic
//     platform-wide — turning a cache wobble into a total outage. That blast radius
//     is unacceptable for a soft cap. So the safe default is unambiguously fail-open,
//     and the adapter owns it so no caller can get the policy wrong.
//
// (Contrast the rate limiter, whose port returns an error so the use-case picks the
// policy — there the right default is genuinely context-dependent; here it is not.)
//
// ============================================================================
// STATE LAYOUT
// ============================================================================
//
//	KEY                       TYPE    VALUE   PURPOSE
//	────────────────────────────────────────────────────────────────────────
//	fp:ig:quota:blocked:<team> STRING  "1"    presence = blocked; absent = allowed
//
// Presence-as-truth: the key EXISTS iff the team is blocked. The QuotaExceeded
// consumer SETs it (optionally with a TTL = time until the billing period resets,
// so it self-clears even if no QuotaRestored event arrives); the QuotaRestored
// consumer DELs it. A simple EXISTS on the hot path — O(1), one round trip.
package redisrepo

import (
	"context"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// QuotaChecker is the Redis-backed implementation of domain.QuotaChecker.
type QuotaChecker struct {
	rdb    *goredis.Client
	prefix string
}

// NewQuotaChecker builds the adapter over a shared go-redis client.
func NewQuotaChecker(rdb *goredis.Client) *QuotaChecker {
	return &QuotaChecker{rdb: rdb, prefix: "fp:ig:quota:blocked:"}
}

// IsBlocked reports whether the team is currently flagged over quota. Fail-open by
// contract: any error (Redis down, timeout) returns false ("not blocked"). The
// team comes from the verified Principal.Team, never a request field.
func (q *QuotaChecker) IsBlocked(ctx context.Context, team string) bool {
	n, err := q.rdb.Exists(ctx, q.key(team)).Result()
	if err != nil {
		// Fail-open: a Redis hiccup must never reject all traffic for the team.
		// (We swallow the error rather than returning it — the port is a pure bool
		// precisely so this policy can't leak to callers.)
		return false
	}
	return n > 0
}

// key builds the namespaced, team-scoped flag key. team is server-authoritative
// (from claims), so there's no injection surface, but scoping under a fixed prefix
// guarantees a team can only ever address its own quota flag.
func (q *QuotaChecker) key(team string) string { return q.prefix + team }

// ----------------------------------------------------------------------------
// CONTROL-PLANE WRITERS — used by the event consumers, not the hot path.
// ----------------------------------------------------------------------------
//
// These are NOT part of domain.QuotaChecker (the use-case only READS the flag).
// They are the adapter-side helpers the QuotaExceeded / QuotaRestored event
// consumers call to flip the flag. Exposed here (rather than in the events layer)
// so the key layout lives in exactly one place — the events adapter shouldn't know
// the Redis key format.

// Block flags a team as over-quota. ttl optionally bounds the block (e.g. time
// until the billing period resets) so the flag self-clears even if a QuotaRestored
// event is lost; pass ttl <= 0 for a sticky flag cleared only by Unblock.
func (q *QuotaChecker) Block(ctx context.Context, team string, ttl time.Duration) error {
	return q.rdb.Set(ctx, q.key(team), "1", ttl).Err()
}

// Unblock clears a team's over-quota flag (QuotaRestored / period reset). DEL of an
// absent key is a no-op, so a redelivered QuotaRestored is naturally idempotent.
func (q *QuotaChecker) Unblock(ctx context.Context, team string) error {
	return q.rdb.Del(ctx, q.key(team)).Err()
}
