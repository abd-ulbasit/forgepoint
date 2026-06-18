// redis_budget.go — the per-team TOKEN BUDGET store over Redis. Implements
// domain.BudgetStore.
//
// ============================================================================
// TOKEN BUDGET vs. REQUEST RATE LIMIT (why this is its own adapter)
// ============================================================================
//
// The inference gateway rate-limits REQUESTS per api-key (a fast throughput shaper).
// The AI gateway budgets TOKENS per TEAM over a rolling window (a cost cap). The
// unit of spend is the LLM token, which is UNKNOWN until a completion finishes, so
// the budget is two-phase (Check pre-flight, Deduct post-serve) — see the
// domain.BudgetStore port doc for why a single atomic reserve is impossible here.
//
// This is the SAME Redis token-bucket family the inference gateway uses (atomic Lua,
// lazy refill, server-clock, self-evicting TTL), re-denominated from "request
// tokens" to "LLM tokens" and re-keyed from api-key to team. Reusing the proven
// shape (not reinventing) is the explicit instruction and the right call: the
// concurrency hazard (two replicas double-spending the last of a team's budget) and
// its fix (one atomic Lua read-modify-write on the Redis server clock) are identical.
//
// ============================================================================
// STATE LAYOUT
// ============================================================================
//
//	KEY                       TYPE  FIELDS              PURPOSE
//	─────────────────────────────────────────────────────────────────────────
//	fp:ai:budget:<team>       HASH  tokens (float)      remaining tokens in window
//	                                ts     (float, ms)   last refill timestamp
//
// One hash per team. `tokens` is REMAINING budget; it refills toward `budget` at
// budget/windowSeconds tokens/sec (a rolling refill that approximates a per-window
// cap without a hard window reset, so a team isn't fully blocked the instant before
// a window boundary then flooded the instant after — the fixed-window 2x-burst
// problem). A Deduct subtracts the used tokens; a Check (with a 0-cost peek) just
// refills-and-reads without consuming.
package budget

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// budgetScript is the atomic refill-and-spend step. It is the token bucket re-shaped
// for a LARGE bucket (a team's whole window budget) where each "spend" is N tokens
// (the completion's usage), not 1.
//
// ARGS:
//
//	KEYS[1]            the budget hash key (fp:ai:budget:<team>)
//	ARGV[1] rate       tokens refilled per second (budget / windowSeconds)
//	ARGV[2] budget     bucket capacity (the team's window token budget)
//	ARGV[3] spend      tokens to consume THIS call (0 for a pre-flight Check)
//	ARGV[4] ttl_ms     key TTL to evict idle teams
//
// RETURNS: {allowed, remaining} — allowed=1 if (after refill) tokens >= spend (and
// for spend>0 the tokens were consumed); remaining is the post-op token balance.
//
// A spend of 0 (Check) NEVER consumes: it returns allowed=1 only if remaining >= 1
// (the team has at least some budget left), which is the "not already exhausted"
// pre-flight semantics the domain wants (we can't know the real cost yet).
const budgetScript = `
local data    = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local rate    = tonumber(ARGV[1])
local budget  = tonumber(ARGV[2])
local spend   = tonumber(ARGV[3])
local ttl_ms  = tonumber(ARGV[4])

local t       = redis.call('TIME')
local now_ms  = (tonumber(t[1]) * 1000) + (tonumber(t[2]) / 1000)

local tokens = tonumber(data[1])
local ts     = tonumber(data[2])
if tokens == nil then
  tokens = budget   -- first sighting: full budget for the window.
  ts     = now_ms
end

-- Lazy refill toward the budget cap (rolling window).
local elapsed = now_ms - ts
if elapsed < 0 then elapsed = 0 end
tokens = math.min(budget, tokens + (rate * (elapsed / 1000.0)))

local allowed = 0
if spend <= 0 then
  -- Pre-flight Check: allowed iff the team is not already exhausted.
  if tokens >= 1.0 then allowed = 1 end
else
  -- Deduct: consume spend tokens (allowed iff enough remain). We always SUBTRACT
  -- the used tokens even if it drives the balance to/below zero on the boundary
  -- request, so an over-budget overshoot is recorded (the soft-cap allows one
  -- in-flight request's overshoot; the next Check then rejects).
  tokens = tokens - spend
  if tokens >= 0 then allowed = 1 end
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now_ms)
redis.call('PEXPIRE', KEYS[1], ttl_ms)
return {allowed, math.floor(tokens)}
`

// RedisBudget is the Redis token-budget implementation of domain.BudgetStore.
type RedisBudget struct {
	rdb    *goredis.Client
	script *goredis.Script

	budget     int64   // per-team window token budget (capacity).
	ratePerSec float64 // refill rate = budget / windowSeconds.
	ttl        time.Duration
	prefix     string
}

// Config tunes the budget. WindowSeconds is the rolling window over which a team may
// spend Budget tokens; the refill rate is derived so the bucket fully refills in one
// window. Defaults make it runnable locally.
type Config struct {
	// Budget is the per-team token allowance per window. 0 → unlimited (the store
	// then always allows and never blocks; Usage reports budget=0).
	Budget int64
	// WindowSeconds is the rolling window length. Default 3600 (1 hour).
	WindowSeconds int64
}

// NewRedisBudget builds the adapter. A zero Budget means UNLIMITED — Check always
// allows, Deduct is a no-op-ish record, and Usage reports budget 0 (the proto's
// "0 = unlimited" convention).
func NewRedisBudget(rdb *goredis.Client, cfg Config) *RedisBudget {
	if cfg.WindowSeconds <= 0 {
		cfg.WindowSeconds = 3600
	}
	var rate float64
	if cfg.Budget > 0 {
		rate = float64(cfg.Budget) / float64(cfg.WindowSeconds)
	}
	// TTL = one window + a margin so an active team's key never expires mid-window and
	// an idle team's key self-evicts shortly after a full window of inactivity.
	ttl := time.Duration(cfg.WindowSeconds)*time.Second + time.Minute
	return &RedisBudget{
		rdb:        rdb,
		script:     goredis.NewScript(budgetScript),
		budget:     cfg.Budget,
		ratePerSec: rate,
		ttl:        ttl,
		prefix:     "fp:ai:budget:",
	}
}

// Compile-time interface assertion against the domain port shape.
var _ interface {
	Check(ctx context.Context, team string) (bool, int64, error)
	Deduct(ctx context.Context, team string, tokens int64) (int64, error)
	Usage(ctx context.Context, team string) (int64, int64, error)
} = (*RedisBudget)(nil)

// Check refills-and-peeks (spend=0): allowed iff the team is not already exhausted.
// An UNLIMITED budget (0) always allows with remaining 0 (meaningless for unlimited).
func (b *RedisBudget) Check(ctx context.Context, team string) (bool, int64, error) {
	if b.budget <= 0 {
		return true, 0, nil // unlimited
	}
	allowed, remaining, err := b.run(ctx, team, 0)
	if err != nil {
		return false, 0, err
	}
	return allowed, remaining, nil
}

// Deduct subtracts tokens used by a completion (spend>0). Returns the new remaining.
// An UNLIMITED budget records nothing and returns 0 (cost is metered via events, not
// the budget bucket, when there is no cap).
func (b *RedisBudget) Deduct(ctx context.Context, team string, tokens int64) (int64, error) {
	if b.budget <= 0 || tokens <= 0 {
		return 0, nil
	}
	_, remaining, err := b.run(ctx, team, tokens)
	if err != nil {
		return 0, err
	}
	return remaining, nil
}

// Usage returns (consumed, budget). consumed = budget - remaining (clamped >= 0). It
// refills-and-peeks (spend=0) to get the current remaining without consuming.
func (b *RedisBudget) Usage(ctx context.Context, team string) (int64, int64, error) {
	if b.budget <= 0 {
		return 0, 0, nil // unlimited: 0 budget per the proto convention, 0 consumed reported.
	}
	_, remaining, err := b.run(ctx, team, 0)
	if err != nil {
		return 0, 0, err
	}
	consumed := b.budget - remaining
	if consumed < 0 {
		consumed = 0
	}
	return consumed, b.budget, nil
}

// run executes the Lua script for team with the given spend and returns
// (allowed, remaining). The team key is namespaced + team-scoped; team comes from
// the verified claims (never a request field), so there is no injection surface, but
// the fixed prefix still bounds the keyspace a principal can address.
func (b *RedisBudget) run(ctx context.Context, team string, spend int64) (bool, int64, error) {
	key := b.prefix + team
	res, err := b.script.Run(ctx, b.rdb,
		[]string{key},
		b.ratePerSec,
		b.budget,
		spend,
		b.ttl.Milliseconds(),
	).Int64Slice()
	if err != nil {
		return false, 0, fmt.Errorf("budget: script for team %q: %w", team, err)
	}
	if len(res) != 2 {
		return false, 0, fmt.Errorf("budget: unexpected script result length %d for team %q", len(res), team)
	}
	return res[0] == 1, res[1], nil
}
