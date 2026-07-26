// Package redis is the LIVE-WINDOW persistence adapter for the Model Monitor. It
// implements domain.WindowStore against Redis using redis/go-redis/v9 — the durable
// home of the streaming-aggregation state (the per-model sliding windows).
//
// ============================================================================
// WHY REDIS FOR THE WINDOWS (and Postgres for reports/configs)
// ============================================================================
//
// The window is HOT, EPHEMERAL, mutated on EVERY inference event, and discarded the
// instant it is scored. That is the opposite of the report history (cold, durable,
// audited). Redis fits the window:
//
//   - Per-event write amplitude: LoadOrOpen → Add → Save runs on every consumed
//     InferenceCompleted. Redis does that in microseconds; a Postgres row UPDATE per
//     event would be the bottleneck of the whole pipeline.
//   - Natural expiry: a window that never fills (a model that goes silent) must not
//     leak memory forever. A Redis TTL reaps it automatically — no GC job.
//   - Shared across replicas: if the NATS consumer scales out, a restart/rebalance
//     must find the in-flight window keyed by monitorID.
//
// ============================================================================
// REHYDRATING A domain.Window — WHAT SURVIVES A ROUND-TRIP, AND WHY (read this)
// ============================================================================
//
// domain.Window holds its aggregation state in UNEXPORTED fields (featureValues,
// predictionCounts, requestIDs) and exposes NO setter and NO raw getter for them —
// by design, so the fold logic (Window.Add) is the single, unit-tested way state
// changes (see window.go). The in-memory fake in the domain tests round-trips
// trivially because it keeps the live *Window pointer in a map; a REAL adapter that
// serializes across a process boundary cannot.
//
// THE DOMAIN-PURE REHYDRATION WE USE — REBUILD THROUGH THE PUBLIC FOLD:
//
// We persist the window's METADATA (id, model_name, model_version, opened_at,
// sample_count) and its REQUEST-ID SET (the join keys, recoverable via
// Window.RequestIDs()). On LoadOrOpen we NewWindow(...) with the persisted id +
// opened_at and REPLAY the request-id set through the domain's own Window.Add(...).
// This faithfully restores, using ONLY the public API:
//
//	ID, MonitorID, ModelName, ModelVersion, OpenedAt, SampleCount, and the
//	request-id set (so cross-restart duplicate suppression keeps working).
//
// WHAT IS DELIBERATELY NOT ROUND-TRIPPED (and the honest reason):
//
// The per-feature RAW VALUES and per-class PREDICTION COUNTS are NOT recoverable
// through Window's public surface — Window.Features() returns only names, and
// Window.FeatureHistogram(feat, edges) / PredictionHistogram(order) need the baseline
// edges/class order that aren't known until SCORE time. The domain exposes no
// accessor for the raw distributions. Faithfully persisting them would require a
// domain-level Snapshot()/Restore() pair on Window (an EXPLICIT, tested serialization
// surface) — which is the correct fix but a DOMAIN change, out of scope for this
// persistence adapter (the domain package is frozen here). We do NOT reach into the
// private maps via reflection/unsafe: that is fragile, silently breaks on any domain
// refactor, and is not defensible. So this adapter persists the
// publicly-recoverable window state with full fidelity and documents the gap loudly
// rather than faking completeness. (In the live single-process path the window is
// folded and scored without ever round-tripping through Redis between Add and score;
// the round-trip matters only for restart/rebalance recovery, where resuming the
// count + dedup set + version + id is the load-bearing behavior.)
//
// ============================================================================
// THE REDIS KEY / DATA-STRUCTURE DESIGN
// ============================================================================
//
//	fp:mon:win:{monitorID}            HASH  — the OPEN window's metadata:
//	                                          id, model_name, model_version,
//	                                          opened_at (unix ns), sample_count.
//	                                          One open window per monitor (tumbling).
//	fp:mon:win:{monitorID}:reqs       SET   — the request-id join-key set (dedup).
//
// Both keys share a TTL (refreshed on every Save) so an abandoned window self-reaps.
// Close DELs both keys atomically (guarded by an id match), so the next LoadOrOpen
// opens a fresh window.
//
// WHY a HASH for metadata + a SET for request-ids: CurrentSampleCount reads ONE field
// (HGET sample_count, one round-trip) without loading the id set; the set gives O(1)
// SADD on each event and a single SMEMBERS to rehydrate. A SET (not a LIST) because
// request-ids are a SET in the domain (membership, not order) — SADD is naturally
// idempotent, mirroring Window.Add's request-id dedup.
package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/domain"
)

// keyPrefix namespaces every key this service owns in Redis. A shared Redis stays
// collision-free across services because each owns a distinct prefix. "mon" = monitor.
const keyPrefix = "fp:mon"

// defaultWindowTTL bounds how long an OPEN window survives without activity. A window
// is normally closed (keys deleted) the moment it is scored; the TTL is the backstop
// for a window that never fills because its model went silent — without it a transient
// model would leak Redis keys forever. Refreshed on every Save so an actively-folding
// window never expires mid-flight. 24h comfortably exceeds any reasonable
// WindowDuration while still reaping dead state within a day.
const defaultWindowTTL = 24 * time.Hour

// WindowStore is the Redis-backed implementation of domain.WindowStore. It holds a
// go-redis client (itself a connection pool, safe for concurrent use) and the TTL.
type WindowStore struct {
	rdb *goredis.Client
	ttl time.Duration
}

// Compile-time proof the adapter satisfies the domain port. Drift fails the build here.
var _ domain.WindowStore = (*WindowStore)(nil)

// NewWindowStore builds a WindowStore from a Redis address ("host:port"), pinging to
// fail fast on an unreachable server (same fail-fast rationale as the Postgres
// constructor — a bad address should crash at startup, not on the first event).
func NewWindowStore(ctx context.Context, addr string) (*WindowStore, error) {
	rdb := goredis.NewClient(&goredis.Options{Addr: addr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("model-monitor/redis: ping: %w", err)
	}
	return &WindowStore{rdb: rdb, ttl: defaultWindowTTL}, nil
}

// NewWindowStoreFromClient wraps an existing go-redis client (tests share one client
// against a testcontainer). The store does NOT own the client's lifecycle in this form.
func NewWindowStoreFromClient(rdb *goredis.Client) *WindowStore {
	return &WindowStore{rdb: rdb, ttl: defaultWindowTTL}
}

// Shutdown releases the underlying client (only for a store built by NewWindowStore).
// NOT named Close: domain.WindowStore.Close(ctx, monitorID, windowID) is the window-
// retirement method (below), and Go forbids two methods of the same name on one type.
// The composition root calls Shutdown at process exit; the consumer calls Close per
// scored window.
func (s *WindowStore) Shutdown() error { return s.rdb.Close() }

// ----------------------------------------------------------------------------
// KEY BUILDERS — one place that knows the key schema, so a typo can't fragment a
// window across two spellings.
// ----------------------------------------------------------------------------

func winMetaKey(monitorID string) string { return keyPrefix + ":win:" + monitorID }
func winReqsKey(monitorID string) string { return keyPrefix + ":win:" + monitorID + ":reqs" }

// LoadOrOpen returns the current open window for a monitor, REBUILDING it through the
// domain's public fold (see the package doc). If no window exists, it opens a fresh
// one (NewWindow) seeded with openedAt.
func (s *WindowStore) LoadOrOpen(ctx context.Context, monitorID, modelName string, openedAt time.Time) (*domain.Window, error) {
	meta, err := s.rdb.HGetAll(ctx, winMetaKey(monitorID)).Result()
	if err != nil {
		return nil, fmt.Errorf("model-monitor/redis: load window meta: %w", err)
	}

	// No open window ⇒ open a fresh one. NewWindow assigns a stable WindowID (the
	// report idempotency key) — the adapter never invents the id, the domain does.
	if len(meta) == 0 {
		return domain.NewWindow(monitorID, modelName, openedAt), nil
	}

	// Resume the SAME window: NewWindow with the persisted opened_at, then set the
	// persisted id (ID is an EXPORTED field — safe to assign from outside the package).
	storedOpenedAt := parseUnixNanos(meta["opened_at"])
	w := domain.NewWindow(monitorID, meta["model_name"], storedOpenedAt)
	w.ID = meta["id"]

	// Replay the request-id set through the public fold to restore SampleCount, the
	// dedup set, and ModelVersion (Add pins the version from the first observation).
	// We feed the stored model_version on each replayed observation so the version is
	// restored even though the raw feature values are (by documented design) not.
	reqIDs, err := s.rdb.SMembers(ctx, winReqsKey(monitorID)).Result()
	if err != nil {
		return nil, fmt.Errorf("model-monitor/redis: load window reqs: %w", err)
	}
	for _, rid := range reqIDs {
		w.Add(domain.InferenceObservation{
			MonitorID:    monitorID,
			RequestID:    rid,
			ModelName:    meta["model_name"],
			ModelVersion: meta["model_version"],
			ObservedAt:   storedOpenedAt,
		})
	}

	// Self-check against the persisted count. WHY: the count and the req-set are
	// written together in one MULTI/EXEC (Save), so a divergence means a torn write or
	// an out-of-band SET mutation — surface it rather than silently scoring a corrupt
	// window. NOTE: observations with an EMPTY request_id are counted by Add but not
	// in the SET (Add only sets the dedup key when request_id != ""); the platform
	// stamps a server-authoritative request_id on every inference event, so in
	// practice every folded observation has one and the counts match. We therefore
	// reconcile to the persisted count (the source of truth) rather than failing on a
	// benign empty-id gap.
	wantCount := parseInt(meta["sample_count"])
	if w.SampleCount < wantCount {
		// Fewer reqs than samples ⇒ some observations carried an empty request_id
		// (no dedup-set member). Top up the count via empty-id Adds so the resumed
		// window's fill matches what was persisted (the count drives the close bound).
		for i := w.SampleCount; i < wantCount; i++ {
			w.Add(domain.InferenceObservation{
				MonitorID:    monitorID,
				ModelName:    meta["model_name"],
				ModelVersion: meta["model_version"],
				ObservedAt:   storedOpenedAt,
			})
		}
	} else if w.SampleCount > wantCount {
		return nil, fmt.Errorf("model-monitor/redis: torn window for monitor %s: replayed %d samples > metadata %d",
			monitorID, w.SampleCount, wantCount)
	}
	return w, nil
}

// Save persists the window after a fold. Called on EVERY consumed event, so it stays
// cheap and O(1)-amortized: it OVERWRITES the small metadata hash and SADDs the
// window's current request-id set (SADD of already-present members is a no-op, so
// re-adding the full set each Save is idempotent and bounded by the window size).
//
// All writes go in ONE MULTI/EXEC transaction so the metadata count and the req-set
// stay in lock-step — a concurrent LoadOrOpen never sees a count that disagrees with
// the set (the invariant the LoadOrOpen self-check relies on). The TTL is refreshed
// on both keys so an actively-folding window never expires mid-flight.
//
// WHY re-SADD the whole set rather than just the delta: the port hands us the folded
// *Window, not the single new observation, and Window exposes its request-ids only as
// the FULL set (RequestIDs()). SADD is idempotent and the set is bounded by WindowSize
// (capped at MaxWindowSize), so re-adding it is cheap and correct. If profiling ever
// shows this set write dominates, the right fix is a domain Snapshot delta — not a
// private-field hack here.
func (s *WindowStore) Save(ctx context.Context, w *domain.Window) error {
	metaKey := winMetaKey(w.MonitorID)
	reqsKey := winReqsKey(w.MonitorID)
	reqIDs := w.RequestIDs()

	_, err := s.rdb.TxPipelined(ctx, func(p goredis.Pipeliner) error {
		p.HSet(ctx, metaKey, map[string]any{
			"id":            w.ID,
			"model_name":    w.ModelName,
			"model_version": w.ModelVersion,
			"opened_at":     formatUnixNanos(w.OpenedAt),
			"sample_count":  strconv.Itoa(w.SampleCount),
		})
		if len(reqIDs) > 0 {
			members := make([]any, len(reqIDs))
			for i, r := range reqIDs {
				members[i] = r
			}
			p.SAdd(ctx, reqsKey, members...)
			p.Expire(ctx, reqsKey, s.ttl)
		}
		p.Expire(ctx, metaKey, s.ttl)
		return nil
	})
	if err != nil {
		return fmt.Errorf("model-monitor/redis: save window: %w", err)
	}
	return nil
}

// Close atomically retires the current window so the next LoadOrOpen opens fresh.
// Idempotent: closing an already-closed (or never-opened) window deletes nothing and
// returns nil.
//
// THE ID GUARD (the rebalance race): we DEL only if the persisted id still equals the
// windowID the caller is closing. If a newer window already replaced this one (a
// consumer rebalance briefly double-owned the monitor), the stored id differs and we
// no-op — so a stale Close cannot wipe the LIVE window. This makes Close safe to retry
// and safe under at-least-once event delivery.
func (s *WindowStore) Close(ctx context.Context, monitorID, windowID string) error {
	metaKey := winMetaKey(monitorID)
	storedID, err := s.rdb.HGet(ctx, metaKey, "id").Result()
	if err == goredis.Nil {
		return nil // already closed / never opened — idempotent no-op
	}
	if err != nil {
		return fmt.Errorf("model-monitor/redis: read window id for close: %w", err)
	}
	if storedID != windowID {
		return nil // a newer window is live — do not touch it
	}
	if err := s.rdb.Del(ctx, metaKey, winReqsKey(monitorID)).Err(); err != nil {
		return fmt.Errorf("model-monitor/redis: close window: %w", err)
	}
	return nil
}

// CurrentSampleCount returns the fill of the open window (for MonitorStatus) WITHOUT
// loading the request-id set — a single HGET of the metadata count. 0 if no open
// window (the Redis-nil case), the documented "no window yet" answer.
func (s *WindowStore) CurrentSampleCount(ctx context.Context, monitorID string) (int, error) {
	v, err := s.rdb.HGet(ctx, winMetaKey(monitorID), "sample_count").Result()
	if err == goredis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("model-monitor/redis: current sample count: %w", err)
	}
	return parseInt(v), nil
}

// ----------------------------------------------------------------------------
// small encoding helpers
// ----------------------------------------------------------------------------

func formatUnixNanos(t time.Time) string {
	if t.IsZero() {
		return "0"
	}
	return strconv.FormatInt(t.UnixNano(), 10)
}

func parseUnixNanos(s string) time.Time {
	if s == "" || s == "0" {
		return time.Time{}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

func parseInt(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}
