// rate_limiter.go — the RateLimiter adapter: a token bucket implemented as ONE
// atomic Redis Lua script. Implements domain.RateLimiter.
//
// ============================================================================
// TOKEN BUCKET — the algorithm, and why it MUST be one atomic operation
// ============================================================================
//
// A bucket holds up to `burst` tokens and refills at `rate` tokens/sec. Each
// request tries to take one token: available → allow (consume it); empty →
// reject (429). The bucket lets a caller spend a short burst (up to `burst` at
// once) while bounding the SUSTAINED rate — the standard API rate-limit shape
// (Stripe, AWS API Gateway, Envoy local rate limit). Superior to a fixed-window
// counter (which permits a 2× spike across a window boundary) and to a leaky
// bucket (which can't burst at all).
//
// THE CONCURRENCY HAZARD this adapter exists to close: the authoritative bucket
// is SHARED across all gateway replicas (per api-key), so two replicas can serve
// the same key's requests simultaneously. The bucket op is a read-modify-write:
//
//	tokens = refill(stored_tokens, elapsed); if tokens >= 1 { tokens -= 1; allow }
//
// If two replicas do that as separate GET then SET round trips, they can BOTH
// read the last token and BOTH allow — the classic check-then-act race that lets
// a tenant exceed its limit by a factor of the replica count. The fix is to make
// the whole read-modify-write ATOMIC on the Redis side. Two ways:
//   - WATCH/MULTI/EXEC optimistic transaction — retries on contention; more round
//     trips; more client code.
//   - A LUA SCRIPT — Redis runs it atomically (single-threaded execution model:
//     no other command interleaves while the script runs), in ONE round trip, with
//     the refill math done server-side against the Redis clock.
//
// We use Lua: fewer round trips on the hot path, no client-side retry loop, and the
// time source is the Redis server clock (TIME) so all replicas agree on "now" —
// they can't disagree about how much the bucket refilled. This is precisely how
// production rate limiters (e.g. the redis-cell module, Kong, Stripe's limiter)
// are built.
//
// ============================================================================
// STATE LAYOUT
// ============================================================================
//
//	KEY                       TYPE  FIELDS                 PURPOSE
//	─────────────────────────────────────────────────────────────────────────
//	fp:ig:rl:<key>            HASH  tokens (float)         remaining tokens
//	                                ts     (float, ms)     last refill timestamp
//
// One hash per principal (api_key_id). A TTL is set on each touch so idle buckets
// evict themselves — we never accumulate keys for principals that stopped calling
// (bounded memory without a sweeper). The TTL is the time to refill a full bucket
// from empty (burst/rate seconds) plus a margin: long enough that an active
// caller's bucket never expires mid-use, short enough that abandoned keys vanish.
package redisrepo

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// tokenBucketScript is the atomic token-bucket step. It is loaded once (EVALSHA
// with an EVAL fallback handled by go-redis's *redis.Script.Run) and executed on
// every Allow.
//
// ARGS:
//
//	KEYS[1]            the bucket hash key (fp:ig:rl:<principal>)
//	ARGV[1] rate       tokens added per second (float)
//	ARGV[2] burst      bucket capacity (max tokens, float)
//	ARGV[3] now_ms     caller-independent "now" — we read the REDIS clock instead
//	                   (see redis.call('TIME')) so all replicas share one clock.
//	ARGV[4] ttl_ms     key TTL to expire idle buckets
//
// RETURNS: 1 if a token was consumed (allow), 0 if the bucket was empty (reject).
//
// WHY refill is computed lazily on read (not by a background filler): a timer per
// key across millions of keys is infeasible; "compute elapsed since last touch and
// add rate*elapsed, capped at burst" gives the identical result with zero timers —
// the standard lazy-refill token bucket.
const tokenBucketScript = `
-- Read current bucket state (tokens, last-touch ms). Missing => full bucket.
local data   = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local rate   = tonumber(ARGV[1])
local burst  = tonumber(ARGV[2])
local ttl_ms = tonumber(ARGV[4])

-- Use the Redis server clock so every replica agrees on "now" (TIME returns
-- {seconds, microseconds}); convert to milliseconds.
local t      = redis.call('TIME')
local now_ms = (tonumber(t[1]) * 1000) + (tonumber(t[2]) / 1000)

local tokens = tonumber(data[1])
local ts     = tonumber(data[2])
if tokens == nil then
  -- First sighting of this key: start with a full bucket (allow an initial burst).
  tokens = burst
  ts     = now_ms
end

-- Lazy refill: add rate*elapsed tokens, capped at burst. elapsed is clamped at >=0
-- to be robust against any clock skew making now_ms < ts.
local elapsed = now_ms - ts
if elapsed < 0 then elapsed = 0 end
tokens = math.min(burst, tokens + (rate * (elapsed / 1000.0)))

local allowed = 0
if tokens >= 1.0 then
  tokens  = tokens - 1.0   -- consume one token for this request
  allowed = 1
end

-- Persist the new state and refresh the idle-eviction TTL atomically.
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now_ms)
redis.call('PEXPIRE', KEYS[1], ttl_ms)
return allowed
`

// RateLimiter is the Redis token-bucket implementation of domain.RateLimiter.
type RateLimiter struct {
	rdb    *goredis.Client
	script *goredis.Script // wraps EVALSHA-with-EVAL-fallback; loaded lazily by go-redis

	rate   float64 // tokens per second (sustained rate)
	burst  float64 // bucket capacity (max instantaneous burst)
	ttl    time.Duration
	prefix string
}

// RateLimiterConfig is the tuning the wiring passes in. Defaults are applied for
// zero values so a caller can set only what it cares about.
type RateLimiterConfig struct {
	// RatePerSec is the sustained tokens/sec each principal may spend. Default 50.
	RatePerSec float64
	// Burst is the bucket capacity (max tokens spendable at once). Default = 2×rate
	// (a 2-second burst), a common "smooth but forgiving" choice.
	Burst float64
}

// NewRateLimiter builds the adapter. The Lua script is compiled into a
// *redis.Script here but only SHIPPED to Redis on first Run (go-redis tries
// EVALSHA and falls back to EVAL+cache on NOSCRIPT), so construction does no I/O.
func NewRateLimiter(rdb *goredis.Client, cfg RateLimiterConfig) *RateLimiter {
	if cfg.RatePerSec <= 0 {
		cfg.RatePerSec = 50
	}
	if cfg.Burst <= 0 {
		cfg.Burst = cfg.RatePerSec * 2
	}
	// TTL = time to refill a full bucket from empty (burst/rate seconds) + a 1s
	// margin. An actively-spending caller refreshes the TTL on every request so it
	// never expires mid-use; an abandoned key evicts itself shortly after the
	// bucket would have fully refilled anyway (so re-creating it as "full" loses
	// nothing).
	ttl := time.Duration((cfg.Burst/cfg.RatePerSec)*float64(time.Second)) + time.Second
	return &RateLimiter{
		rdb:    rdb,
		script: goredis.NewScript(tokenBucketScript),
		rate:   cfg.RatePerSec,
		burst:  cfg.Burst,
		ttl:    ttl,
		prefix: "fp:ig:rl:",
	}
}

// Compile-time interface assertion.
var _ interface {
	Allow(ctx context.Context, key string) (bool, error)
} = (*RateLimiter)(nil)

// Allow attempts to consume one token for key (the principal — api_key_id).
// Returns (true, nil) if a token was available (request may proceed) and
// (false, nil) if the bucket was empty (the use-case rejects with ErrRateLimited).
// A non-nil error is an INFRASTRUCTURE failure (Redis down / script error): per
// the port contract the limiter does NOT decide fail-open vs fail-closed — it
// surfaces the error and the use-case applies its policy.
func (l *RateLimiter) Allow(ctx context.Context, key string) (bool, error) {
	// The bucket key is namespaced + principal-scoped. key comes from the verified
	// claims (api_key_id), never a client request field, so there is no injection
	// surface — but we still scope it under a fixed prefix so a principal can never
	// address another principal's bucket or a non-rate-limit key.
	bucketKey := l.prefix + key

	// now_ms is passed as ARGV[3] for parity/debuggability, but the script uses the
	// Redis TIME clock as the authoritative source so replicas can't disagree.
	res, err := l.script.Run(ctx, l.rdb,
		[]string{bucketKey},
		l.rate,
		l.burst,
		time.Now().UnixMilli(),
		l.ttl.Milliseconds(),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("redisrepo: token-bucket script for key %q: %w", key, err)
	}
	return res == 1, nil
}
