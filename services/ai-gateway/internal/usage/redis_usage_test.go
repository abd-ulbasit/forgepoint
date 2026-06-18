// redis_usage_test.go — UNIT tests for the usage accumulator's PURE branches.
//
// The Redis-backed HINCRBY/HMGET paths need a real Redis (integration territory); the
// per-team isolation + accumulation over real Redis is covered alongside the cache
// adapter's integration suite. These tests cover the logic that runs WITHOUT Redis:
// the zero-token no-op short-circuit (which must not touch Redis) and the Redis-value
// parser, with no external dependency.
package usage

import (
	"context"
	"testing"
)

func TestAddZeroTokensIsNoop(t *testing.T) {
	t.Parallel()
	// A completion with no tokens has nothing to accumulate and must NOT touch Redis
	// (rdb is nil here — any Redis call would panic, proving the short-circuit holds).
	u := NewRedisUsage(nil, Config{})
	if err := u.Add(context.Background(), "team-a", 0, 0); err != nil {
		t.Fatalf("Add(0,0) = %v, want nil (no-op, no Redis touch)", err)
	}
}

func TestParseRedisInt(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   any
		want int64
	}{
		{"missing field (nil)", nil, 0},
		{"decimal string", "42", 42},
		{"zero string", "0", 0},
		{"garbage", "not-a-number", 0},
		{"unexpected type", 123, 0}, // HMGet always returns strings/nil; defensive.
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if got := parseRedisInt(c.in); got != c.want {
				t.Fatalf("parseRedisInt(%v) = %d, want %d", c.in, got, c.want)
			}
		})
	}
}
