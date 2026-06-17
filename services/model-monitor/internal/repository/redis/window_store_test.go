// window_store_test.go — integration tests for the Redis WindowStore against a REAL
// Redis (redis:7) on the remote Docker engine. These verify the actual streaming-
// state behavior: open→fold→save→reload round-trips the publicly-recoverable window
// state, cross-restart request-id DEDUP survives, Close is idempotent and id-guarded,
// CurrentSampleCount is cheap and correct, and the TTL is set. Never a mock — the
// MULTI/EXEC lock-step and SET/HASH semantics only show up against real Redis.
package redis

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/domain"
)

// newTestStore starts a real Redis container and returns a WindowStore wired to it.
// Each test gets its OWN container — no shared state across tests.
func newTestStore(t *testing.T) (*WindowStore, *goredis.Client, context.Context) {
	t.Helper()
	ctx := context.Background()
	addr := testutil.StartRedis(t) // SkipIfNoDocker is called inside

	// StartRedis returns a "redis://host:port" URL; parse it into client options so a
	// scheme prefix doesn't get treated as part of the host.
	opts, err := goredis.ParseURL(addr)
	if err != nil {
		t.Fatalf("parse redis url %q: %v", addr, err)
	}
	rdb := goredis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping redis: %v", err)
	}
	return NewWindowStoreFromClient(rdb), rdb, ctx
}

// foldObs folds one observation into the window via the DOMAIN's public Add — the
// same call the live consumer makes. Tests build windows this way (never by poking
// fields) so they exercise the real fold the adapter must persist.
func foldObs(w *domain.Window, reqID, version string, features map[string]float64, predicted string) {
	w.Add(domain.InferenceObservation{
		MonitorID:    w.MonitorID,
		RequestID:    reqID,
		ModelName:    w.ModelName,
		ModelVersion: version,
		Features:     features,
		PredictedTop: predicted,
		ObservedAt:   time.Now().UTC(),
	})
}

// TestWindow_OpenSaveReload_RoundTrip is the core persistence test: open a window,
// fold several observations, Save, then LoadOrOpen and assert the publicly-recoverable
// state (id, model, version, opened_at, sample_count, request-id set) came back intact.
func TestWindow_OpenSaveReload_RoundTrip(t *testing.T) {
	s, _, ctx := newTestStore(t)
	const monitorID, model = "mon-1", "fraud"
	opened := time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Nanosecond)

	// First LoadOrOpen on an empty store opens a FRESH window (new id, zero samples).
	w, err := s.LoadOrOpen(ctx, monitorID, model, opened)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if w.ID == "" || w.SampleCount != 0 {
		t.Fatalf("fresh window malformed: id=%q count=%d", w.ID, w.SampleCount)
	}
	freshID := w.ID

	// Fold three distinct observations (the domain pins ModelVersion from the first).
	foldObs(w, "req-1", "v3", map[string]float64{"income": 5.0}, "fraud")
	foldObs(w, "req-2", "v3", map[string]float64{"income": 6.0}, "not_fraud")
	foldObs(w, "req-3", "v3", map[string]float64{"income": 7.0}, "fraud")
	if w.SampleCount != 3 {
		t.Fatalf("after 3 folds SampleCount=%d, want 3", w.SampleCount)
	}
	if err := s.Save(ctx, w); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Reload: must RESUME the same window (same id), not open a fresh one.
	reloaded, err := s.LoadOrOpen(ctx, monitorID, model, time.Now())
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if reloaded.ID != freshID {
		t.Fatalf("reload opened a new window: id=%q want resumed %q", reloaded.ID, freshID)
	}
	if reloaded.MonitorID != monitorID || reloaded.ModelName != model {
		t.Fatalf("reload identity wrong: %+v", reloaded)
	}
	if reloaded.ModelVersion != "v3" {
		t.Fatalf("reload ModelVersion=%q, want v3 (pinned from first obs)", reloaded.ModelVersion)
	}
	if !reloaded.OpenedAt.Equal(opened) {
		t.Fatalf("reload OpenedAt=%v, want %v", reloaded.OpenedAt, opened)
	}
	if reloaded.SampleCount != 3 {
		t.Fatalf("reload SampleCount=%d, want 3", reloaded.SampleCount)
	}
	// The request-id SET survived (dedup keys preserved across the round-trip).
	gotReqs := map[string]bool{}
	for _, r := range reloaded.RequestIDs() {
		gotReqs[r] = true
	}
	for _, want := range []string{"req-1", "req-2", "req-3"} {
		if !gotReqs[want] {
			t.Fatalf("request-id %q lost across round-trip; got %v", want, reloaded.RequestIDs())
		}
	}
}

// TestWindow_CrossRestartDedup proves the load-bearing reason we persist the request-
// id set: after a reload (simulating a consumer restart), a REDELIVERED event with an
// already-seen request_id is still suppressed by the domain's Add — so the window's
// drift score can't be skewed by a duplicate that arrives after a restart.
func TestWindow_CrossRestartDedup(t *testing.T) {
	s, _, ctx := newTestStore(t)
	const monitorID, model = "mon-dedup", "fraud"

	w, err := s.LoadOrOpen(ctx, monitorID, model, time.Now().UTC())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	foldObs(w, "req-A", "v1", map[string]float64{"x": 1}, "a")
	foldObs(w, "req-B", "v1", map[string]float64{"x": 2}, "b")
	if err := s.Save(ctx, w); err != nil {
		t.Fatalf("save: %v", err)
	}

	// "Restart": reload the window from Redis.
	w2, err := s.LoadOrOpen(ctx, monitorID, model, time.Now())
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if w2.SampleCount != 2 {
		t.Fatalf("reloaded count=%d, want 2", w2.SampleCount)
	}
	// A redelivered req-A: Add must return false (duplicate) and NOT advance the count.
	added := w2.Add(domain.InferenceObservation{
		MonitorID: monitorID, RequestID: "req-A", ModelName: model, ModelVersion: "v1",
		Features: map[string]float64{"x": 99}, PredictedTop: "a", ObservedAt: time.Now(),
	})
	if added {
		t.Fatal("redelivered req-A after restart was folded again (dedup set not restored)")
	}
	if w2.SampleCount != 2 {
		t.Fatalf("count advanced on a duplicate after restart: %d", w2.SampleCount)
	}
	// A genuinely new id IS folded.
	if !w2.Add(domain.InferenceObservation{
		MonitorID: monitorID, RequestID: "req-C", ModelName: model, ModelVersion: "v1",
		ObservedAt: time.Now(),
	}) {
		t.Fatal("new req-C should have been folded")
	}
	if w2.SampleCount != 3 {
		t.Fatalf("count=%d after one new fold, want 3", w2.SampleCount)
	}
}

// TestWindow_CurrentSampleCount checks the cheap status read: 0 with no window, the
// live fill after a Save, and 0 again after Close.
func TestWindow_CurrentSampleCount(t *testing.T) {
	s, _, ctx := newTestStore(t)
	const monitorID, model = "mon-count", "m"

	// No window yet ⇒ 0.
	n, err := s.CurrentSampleCount(ctx, monitorID)
	if err != nil {
		t.Fatalf("count(empty): %v", err)
	}
	if n != 0 {
		t.Fatalf("count with no window=%d, want 0", n)
	}

	w, _ := s.LoadOrOpen(ctx, monitorID, model, time.Now().UTC())
	foldObs(w, "r1", "v1", nil, "")
	foldObs(w, "r2", "v1", nil, "")
	if err := s.Save(ctx, w); err != nil {
		t.Fatalf("save: %v", err)
	}
	n, err = s.CurrentSampleCount(ctx, monitorID)
	if err != nil {
		t.Fatalf("count(live): %v", err)
	}
	if n != 2 {
		t.Fatalf("count(live)=%d, want 2", n)
	}

	if err := s.Close(ctx, monitorID, w.ID); err != nil {
		t.Fatalf("close: %v", err)
	}
	n, err = s.CurrentSampleCount(ctx, monitorID)
	if err != nil {
		t.Fatalf("count(after close): %v", err)
	}
	if n != 0 {
		t.Fatalf("count after close=%d, want 0", n)
	}
}

// TestWindow_Close_IdempotentAndFreshAfter verifies: Close removes the window so the
// next LoadOrOpen opens a FRESH one (new id); a second Close is a no-op; and Close of a
// never-opened monitor is a no-op.
func TestWindow_Close_IdempotentAndFreshAfter(t *testing.T) {
	s, _, ctx := newTestStore(t)
	const monitorID, model = "mon-close", "m"

	w, _ := s.LoadOrOpen(ctx, monitorID, model, time.Now().UTC())
	foldObs(w, "r1", "v1", nil, "")
	if err := s.Save(ctx, w); err != nil {
		t.Fatalf("save: %v", err)
	}
	firstID := w.ID

	if err := s.Close(ctx, monitorID, firstID); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Second close: idempotent no-op.
	if err := s.Close(ctx, monitorID, firstID); err != nil {
		t.Fatalf("second close should be a no-op: %v", err)
	}
	// Close of a never-opened monitor: no-op.
	if err := s.Close(ctx, "never-opened", "whatever"); err != nil {
		t.Fatalf("close of never-opened should be nil: %v", err)
	}

	// Next LoadOrOpen opens a FRESH window with a new id and zero fill.
	w2, err := s.LoadOrOpen(ctx, monitorID, model, time.Now().UTC())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if w2.ID == firstID {
		t.Fatal("reopen after close reused the closed window id; expected a fresh window")
	}
	if w2.SampleCount != 0 {
		t.Fatalf("reopened window has %d samples, want 0", w2.SampleCount)
	}
}

// TestWindow_Close_IDGuard is the rebalance-race test: a stale Close for an OLD window
// id must NOT delete the LIVE (newer) window that already replaced it.
func TestWindow_Close_IDGuard(t *testing.T) {
	s, _, ctx := newTestStore(t)
	const monitorID, model = "mon-guard", "m"

	// Open + save window A, then close it and open + save window B (the live one).
	wA, _ := s.LoadOrOpen(ctx, monitorID, model, time.Now().UTC())
	foldObs(wA, "a", "v1", nil, "")
	_ = s.Save(ctx, wA)
	_ = s.Close(ctx, monitorID, wA.ID)

	wB, _ := s.LoadOrOpen(ctx, monitorID, model, time.Now().UTC())
	foldObs(wB, "b1", "v1", nil, "")
	foldObs(wB, "b2", "v1", nil, "")
	if err := s.Save(ctx, wB); err != nil {
		t.Fatalf("save B: %v", err)
	}

	// A STALE close for window A (id no longer live) must be a no-op — B survives.
	if err := s.Close(ctx, monitorID, wA.ID); err != nil {
		t.Fatalf("stale close: %v", err)
	}
	live, err := s.LoadOrOpen(ctx, monitorID, model, time.Now())
	if err != nil {
		t.Fatalf("load after stale close: %v", err)
	}
	if live.ID != wB.ID {
		t.Fatalf("stale close wiped the live window: got id %q, want B's %q", live.ID, wB.ID)
	}
	if live.SampleCount != 2 {
		t.Fatalf("live window B lost samples: count=%d, want 2", live.SampleCount)
	}
}

// TestWindow_TTLSet asserts Save puts a positive TTL on both window keys so an
// abandoned window self-reaps (the memory-safety backstop).
func TestWindow_TTLSet(t *testing.T) {
	s, rdb, ctx := newTestStore(t)
	const monitorID, model = "mon-ttl", "m"

	w, _ := s.LoadOrOpen(ctx, monitorID, model, time.Now().UTC())
	foldObs(w, "r1", "v1", nil, "")
	if err := s.Save(ctx, w); err != nil {
		t.Fatalf("save: %v", err)
	}

	metaTTL, err := rdb.TTL(ctx, winMetaKey(monitorID)).Result()
	if err != nil {
		t.Fatalf("ttl meta: %v", err)
	}
	if metaTTL <= 0 {
		t.Fatalf("meta key TTL=%v, want a positive expiry", metaTTL)
	}
	reqsTTL, err := rdb.TTL(ctx, winReqsKey(monitorID)).Result()
	if err != nil {
		t.Fatalf("ttl reqs: %v", err)
	}
	if reqsTTL <= 0 {
		t.Fatalf("reqs key TTL=%v, want a positive expiry", reqsTTL)
	}
}
