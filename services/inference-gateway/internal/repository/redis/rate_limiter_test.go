// rate_limiter_test.go — integration tests for the token-bucket RateLimiter
// against a REAL Redis. These verify the algorithm's real semantics: an initial
// burst is allowed up to capacity, an empty bucket rejects, the bucket refills
// over wall-clock time, buckets are isolated per key, and — the load-bearing
// property — concurrent Allow calls never over-grant (the atomicity the Lua script
// exists to guarantee). Run with -race.
package redisrepo

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRateLimiter_BurstThenReject(t *testing.T) {
	rdb := newRedis(t)
	ctx := context.Background()

	// rate 1/s (one token per second), burst 5: a fresh key may spend 5 immediately,
	// then is dry. WHY rate=1/s: each Allow is a Redis round trip to the remote
	// engine; over the 5 drain calls + the 6th, well under a second elapses, so at
	// 1/s strictly less than one token refills — the bucket is unambiguously empty on
	// the 6th call. A higher rate would let round-trip latency refill a token and
	// make the "reject" assertion flaky.
	rl := NewRateLimiter(rdb, RateLimiterConfig{RatePerSec: 1, Burst: 5})

	allowed := 0
	for i := 0; i < 5; i++ {
		ok, err := rl.Allow(ctx, "key-burst")
		if err != nil {
			t.Fatalf("Allow #%d: %v", i, err)
		}
		if ok {
			allowed++
		}
	}
	if allowed != 5 {
		t.Fatalf("initial burst allowed %d, want 5 (full bucket)", allowed)
	}

	// 6th immediate request: bucket empty → reject.
	ok, err := rl.Allow(ctx, "key-burst")
	if err != nil {
		t.Fatalf("Allow #6: %v", err)
	}
	if ok {
		t.Fatal("6th request allowed, want reject (empty bucket)")
	}
}

// TestRateLimiter_RefillsOverTime proves the lazy refill is real: after the bucket
// is drained, waiting long enough for ~rate*elapsed tokens lets requests through
// again. We use a high rate so the wait is short and the test stays fast.
func TestRateLimiter_RefillsOverTime(t *testing.T) {
	rdb := newRedis(t)
	ctx := context.Background()

	// rate 5/s (one token every 200ms), burst 2. WHY a LOW rate: the bucket refills
	// rate*elapsed continuously, and each Allow incurs a Redis round trip to the
	// remote engine (single-digit-to-tens of ms). At a high rate (e.g. 50/s = 1
	// token per 20ms) that round-trip latency alone refills a token between the
	// drain calls and the bucket is never observably empty — a flaky assertion. At
	// 5/s a token takes 200ms, far longer than any round trip, so "empty right after
	// draining" is robustly true, and a deliberate 400ms wait reliably refills ~2.
	rl := NewRateLimiter(rdb, RateLimiterConfig{RatePerSec: 5, Burst: 2})

	for i := 0; i < 2; i++ {
		if ok, err := rl.Allow(ctx, "key-refill"); err != nil || !ok {
			t.Fatalf("draining Allow #%d: ok=%v err=%v", i, ok, err)
		}
	}
	if ok, _ := rl.Allow(ctx, "key-refill"); ok {
		t.Fatal("expected empty bucket immediately after draining")
	}

	time.Sleep(400 * time.Millisecond) // ~2 tokens refill at 5/s (capped at burst=2)

	if ok, err := rl.Allow(ctx, "key-refill"); err != nil || !ok {
		t.Fatalf("after refill wait Allow = ok=%v err=%v, want allowed", ok, err)
	}
}

// TestRateLimiter_KeysAreIsolated confirms one principal draining its bucket does
// not affect another's (per-key buckets, no cross-tenant interference).
func TestRateLimiter_KeysAreIsolated(t *testing.T) {
	rdb := newRedis(t)
	ctx := context.Background()
	rl := NewRateLimiter(rdb, RateLimiterConfig{RatePerSec: 1, Burst: 2})

	// Drain key A.
	for i := 0; i < 2; i++ {
		if ok, err := rl.Allow(ctx, "tenant-A"); err != nil || !ok {
			t.Fatalf("drain A #%d: ok=%v err=%v", i, ok, err)
		}
	}
	if ok, _ := rl.Allow(ctx, "tenant-A"); ok {
		t.Fatal("A should be drained")
	}
	// B is untouched: full bucket.
	for i := 0; i < 2; i++ {
		if ok, err := rl.Allow(ctx, "tenant-B"); err != nil || !ok {
			t.Fatalf("B Allow #%d should be allowed: ok=%v err=%v", i, ok, err)
		}
	}
}

// TestRateLimiter_ConcurrentAllowNeverOverGrants is the atomicity test — the whole
// reason for the Lua script. With burst=20 and 200 concurrent Allow calls (across
// goroutines, simulating multiple replicas hitting the shared bucket), EXACTLY 20
// may be granted. A non-atomic GET-then-SET would let several goroutines read the
// same token count and over-grant; the single-threaded Lua execution makes that
// impossible. Run with -race for the data-race half of the guarantee.
func TestRateLimiter_ConcurrentAllowNeverOverGrants(t *testing.T) {
	rdb := newRedis(t)
	ctx := context.Background()

	// Low rate so negligible refill happens during the burst of concurrent calls;
	// the only tokens available are the initial burst capacity.
	const burst = 20
	rl := NewRateLimiter(rdb, RateLimiterConfig{RatePerSec: 1, Burst: burst})

	const goroutines = 200
	var granted int64
	var wg sync.WaitGroup
	start := make(chan struct{}) // release all goroutines at once to maximize contention

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ok, err := rl.Allow(ctx, "hot-key")
			if err != nil {
				t.Errorf("Allow: %v", err)
				return
			}
			if ok {
				atomic.AddInt64(&granted, 1)
			}
		}()
	}
	close(start)
	wg.Wait()

	// EXACTLY burst granted — never more (over-grant = broken atomicity), and not
	// fewer (the bucket started full). A tiny refill (rate=1/s over the few ms of
	// the test) rounds to <1 token, so the bucket can't yield a 21st.
	if granted != burst {
		t.Fatalf("concurrent Allow granted %d, want exactly %d (atomic token bucket)", granted, burst)
	}
}

// TestRateLimiter_SetsTTL confirms each touched bucket gets an eviction TTL so idle
// keys self-clean (bounded memory without a sweeper).
func TestRateLimiter_SetsTTL(t *testing.T) {
	rdb := newRedis(t)
	ctx := context.Background()
	rl := NewRateLimiter(rdb, RateLimiterConfig{RatePerSec: 10, Burst: 5})

	if _, err := rl.Allow(ctx, "ttl-key"); err != nil {
		t.Fatalf("Allow: %v", err)
	}
	ttl, err := rdb.PTTL(ctx, "fp:ig:rl:ttl-key").Result()
	if err != nil {
		t.Fatalf("PTTL: %v", err)
	}
	if ttl <= 0 {
		t.Fatalf("bucket TTL = %v, want a positive expiry (idle eviction)", ttl)
	}
}
