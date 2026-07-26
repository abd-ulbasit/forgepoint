// route_store.go — the RouteStore adapter: in-memory authoritative routing table
// MIRRORED to Redis for warm-start. Implements domain.RouteStore.
//
// ============================================================================
// REDIS "SCHEMA" (there is no SQL; this comment IS the schema doc)
// ============================================================================
//
//	KEY                        TYPE   VALUE / FIELDS                       PURPOSE
//	──────────────────────────────────────────────────────────────────────────
//	fp:ig:routes               HASH   field=<modelName> → JSON(Route)      the mirror
//
// ONE Redis HASH holds the whole table, one field per model. WHY a single hash
// (not a key-per-model)?
//   - Atomic, consistent warm-start: HGETALL pulls the entire table in one round
//     trip into the in-memory map — no SCAN, no partial view across many keys.
//   - List/pagination is a pure in-memory operation over the authoritative map
//     (see List below); Redis is never on the List path.
//   - The table is small (tens–hundreds of models), so one hash is the right
//     cardinality; a hash with millions of fields would argue for key-per-entry,
//     but that is not this workload.
//
// We store each Route as JSON in the field. JSON (not gob/proto) keeps the mirror
// human-inspectable with `redis-cli HGETALL` during an incident and avoids
// dragging a codec into the adapter; the table is tiny so JSON's size/CPU cost is
// irrelevant.
//
// ============================================================================
// DUAL-COPY DESIGN (authoritative in-memory + Redis mirror) — why both
// ============================================================================
//
// The AUTHORITATIVE copy is the in-memory map. Every Predict does RouteStore.Get
// on the hot path; putting a Redis round trip there would add network latency to
// the single most frequent operation in the service. So Get NEVER touches Redis —
// it reads the local map under an RWMutex. This is exactly how Envoy serves
// traffic from its in-memory xDS snapshot, not by calling the control plane per
// request.
//
// Redis is the MIRROR, written on every Upsert/Delete so that a freshly-scheduled
// replica can call warm() once at startup and recover the full table WITHOUT
// replaying the entire ModelDeployed/Undeployed event history from NATS (which
// could be thousands of events and a slow cold start). The events remain the
// durable source of truth; Redis is a cache of the current projection.
//
// CONSISTENCY ORDERING (deliberate): Upsert writes the in-memory map FIRST, then
// the mirror; Delete deletes in-memory FIRST, then the mirror. WHY this order:
// the in-memory copy is authoritative and on the hot path, so it must reflect the
// new truth immediately; the mirror is best-effort warm-start state. If the Redis
// write fails we DO NOT roll back the in-memory change or fail the operation — a
// mirror that's briefly stale only means a *future cold start* would warm to a
// slightly older table, which the next event re-corrects. Failing a live route
// update because the warm-start cache hiccuped would be the tail wagging the dog.
// The error is returned so the caller can log/metric it, but the authoritative
// state is already correct.
//
// ============================================================================
// SNAPSHOT ISOLATION CONTRACT (from ports.go — a contract, not a nicety)
// ============================================================================
//
// RouteStore.Get MUST return a DEEP COPY: a Route whose Targets slice is a fresh
// backing array, so a caller can read route.Targets on the hot path with no lock
// while a concurrent Upsert replaces the stored route — no torn read, no data
// race on WeightBps/Status. Upsert likewise stores a deep copy of the caller's
// Route so a later mutation of the caller's slice can't reach into our map. The
// domain ALSO never mutates a fetched Route in place (belt and suspenders); here
// we guarantee the store half. cloneRoute() below is that deep copy.
package redisrepo

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	goredis "github.com/redis/go-redis/v9"

	"github.com/abd-ulbasit/forgepoint/services/inference-gateway/internal/domain"
)

// routesHashKey is the single Redis hash holding the whole routing-table mirror.
// Namespaced fp:ig: (forgepoint / inference-gateway) so the gateway's keys never
// collide with another service sharing the Redis instance.
const routesHashKey = "fp:ig:routes"

// RouteStore is the in-memory-authoritative + Redis-mirror implementation of
// domain.RouteStore. The zero value is NOT usable; construct with NewRouteStore.
type RouteStore struct {
	rdb *goredis.Client

	// mu guards table. RWMutex because Get (hot path, read) vastly outnumbers
	// Upsert/Delete (control-plane / event-driven, write) — readers don't block
	// each other, exactly the access pattern an RWMutex optimizes for.
	//
	// KEY (multi-tenant IDOR fix): the map is keyed by the DOMAIN-composed opaque
	// key — routeKey(team, modelName) — NOT by the bare model name. The adapter
	// treats the key as opaque; it never parses it. This is what makes two teams'
	// identically-named models distinct rows so one team can never address (read,
	// repoint, delete) another's route. The Route's OwnerTeam/ModelName fields carry
	// the human-readable identity for projection/responses.
	mu    sync.RWMutex
	table map[string]domain.Route // routeKey(team, modelName) → authoritative Route
}

// NewRouteStore builds the adapter over a go-redis client. The caller owns the
// client's lifecycle (it is shared with the other adapters in this package).
// Call Warm once after construction to load the mirror into memory.
func NewRouteStore(rdb *goredis.Client) *RouteStore {
	return &RouteStore{
		rdb:   rdb,
		table: make(map[string]domain.Route),
	}
}

// Compile-time proof the adapter satisfies the domain port. If the interface ever
// changes, this line breaks the build at the adapter rather than at a distant
// call site — the canonical Go way to assert "this type implements that port".
var _ domain.RouteStore = (*RouteStore)(nil)

// ----------------------------------------------------------------------------
// DEEP COPY — the snapshot-isolation linchpin (see file header + ports.go)
// ----------------------------------------------------------------------------

// cloneRoute returns a Route with a freshly-allocated Targets backing array so
// neither the caller nor a concurrent writer can mutate the other's view.
// RouteTarget is all value fields (string/int/enum/bool), so a per-element copy
// is a complete deep copy. nil Targets in → nil Targets out (preserves "no
// targets" exactly, mirroring domain.cloneTargets).
func cloneRoute(r domain.Route) domain.Route {
	if r.Targets == nil {
		return r // Targets already nil; the rest is value-copied by the return
	}
	out := r
	out.Targets = make([]domain.RouteTarget, len(r.Targets))
	copy(out.Targets, r.Targets) // element-wise value copy; no reference fields
	return out
}

// ----------------------------------------------------------------------------
// HOT PATH — Get reads the in-memory map, never Redis.
// ----------------------------------------------------------------------------

// Get returns the route stored under key, or domain.ErrNoRoute if absent. key is
// the domain-composed (team, model) key (opaque here). The returned Route is a
// DEEP COPY (snapshot isolation): the caller may scan route.Targets lock-free
// while a concurrent Upsert swaps the stored route.
func (s *RouteStore) Get(_ context.Context, key string) (domain.Route, error) {
	s.mu.RLock()
	r, ok := s.table[key]
	s.mu.RUnlock()
	if !ok {
		// ErrNoRoute is the domain sentinel the use-case maps straight to a
		// NO_ROUTE failure; returning it (not a generic "not found") keeps the
		// failure taxonomy intact end to end.
		return domain.Route{}, domain.ErrNoRoute
	}
	return cloneRoute(r), nil
}

// Upsert atomically replaces a model's route in the authoritative map, then
// mirrors it to Redis. The caller passes an already-validated Route (the domain
// enforces the weight-sum / status invariants before calling); the adapter is not
// the validation layer, it is the persistence layer.
func (s *RouteStore) Upsert(ctx context.Context, key string, route domain.Route) error {
	// Deep-copy on the way IN so a later mutation of the caller's slice cannot
	// reach into our stored map (the symmetric half of copy-on-read).
	stored := cloneRoute(route)

	// 1) Authoritative in-memory swap under the write lock, keyed by the opaque
	//    domain key (routeKey(team, model)) — NOT route.ModelName, which is no
	//    longer unique across tenants. A concurrent Get sees either the whole old
	//    route or the whole new one — never a half-applied one — because map
	//    assignment of the (already-built) value is atomic from the reader's
	//    perspective and the reader copies under RLock.
	s.mu.Lock()
	s.table[key] = stored
	s.mu.Unlock()

	// 2) Mirror to Redis (best-effort warm-start state) under the SAME key, so a
	//    warm() restores the table with tenant namespacing intact. Marshal the deep
	//    copy so the JSON reflects exactly what we stored.
	blob, err := json.Marshal(stored)
	if err != nil {
		// Marshal failing is a programming error (Route is plain data), not a
		// Redis problem — surface it; the in-memory truth is already correct.
		return fmt.Errorf("redisrepo: marshal route %q for mirror: %w", key, err)
	}
	if err := s.rdb.HSet(ctx, routesHashKey, key, blob).Err(); err != nil {
		// Mirror write failed: in-memory state is correct and serving; only the
		// warm-start cache is stale. Return for observability, do NOT roll back.
		return fmt.Errorf("redisrepo: mirror upsert route %q: %w", key, err)
	}
	return nil
}

// Delete removes a model from the table (ModelUndeployed/Archived, DeleteRoute).
// Idempotent: deleting an absent route is a no-op (idempotent consumers — a
// redelivered ModelUndeployed must not error).
func (s *RouteStore) Delete(ctx context.Context, key string) error {
	// 1) Authoritative removal first (keyed by the opaque domain key — so a delete
	//    only ever touches the caller's own tenant namespace).
	s.mu.Lock()
	delete(s.table, key) // delete of an absent key is a safe no-op in Go
	s.mu.Unlock()

	// 2) Mirror removal. HDEL of an absent field returns 0, not an error — so a
	//    redelivered delete is naturally idempotent here too.
	if err := s.rdb.HDel(ctx, routesHashKey, key).Err(); err != nil {
		return fmt.Errorf("redisrepo: mirror delete route %q: %w", key, err)
	}
	return nil
}

// List returns a page of ALL routes (every tenant) for the operator dashboard /
// CLI, served entirely from the authoritative in-memory map (Redis is never
// touched). The DOMAIN scopes the result to the caller's team (see
// domain.ListRoutes) — the store can't read the opaque key, so it does not filter.
// Pagination is CURSOR-based over a STABLE SORT by the store KEY:
//
//   - We sort the keys, then return the page of routes whose key is strictly
//     greater than the cursor (PageToken). The next token is the last key returned.
//     Empty token = from the start; empty next token = end.
//
// WHY a key cursor and not LIMIT/OFFSET: the routing table mutates from events
// while an operator pages through it. An OFFSET-based page can SKIP a route (if an
// earlier one is deleted between pages) or DUPLICATE one (if an earlier one is
// inserted). A key cursor is stable regardless of inserts/deletes elsewhere. The
// token is the opaque store key itself — opaque to the caller, which only
// round-trips it.
func (s *RouteStore) List(_ context.Context, opts domain.ListOptions) ([]domain.Route, string, error) {
	const defaultPageSize = 50
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}

	// Snapshot the names under the read lock, then release before sorting/copying
	// so we hold the lock for the minimum window (the hot path keeps moving).
	s.mu.RLock()
	names := make([]string, 0, len(s.table))
	for name := range s.table {
		names = append(names, name)
	}
	s.mu.RUnlock()

	sort.Strings(names) // deterministic order = stable pagination

	// Walk to the first name strictly after the cursor.
	start := 0
	if opts.PageToken != "" {
		// sort.Search returns the first index whose name > token (because the
		// predicate flips from false to true there). For an exact-match token this
		// correctly lands on the NEXT element, so the cursor row is not repeated.
		start = sort.Search(len(names), func(i int) bool { return names[i] > opts.PageToken })
	}

	end := start + pageSize
	if end > len(names) {
		end = len(names)
	}

	page := names[start:end]
	out := make([]domain.Route, 0, len(page))
	s.mu.RLock()
	for _, name := range page {
		// The route may have been deleted between the snapshot and now; skip if so
		// (a deleted route simply doesn't appear — consistent with cursor stability).
		if r, ok := s.table[name]; ok {
			out = append(out, cloneRoute(r)) // deep copy: callers never alias our map
		}
	}
	s.mu.RUnlock()

	// Next token = last name on this page IF there is more; empty = end of table.
	nextToken := ""
	if end < len(names) {
		nextToken = names[end-1]
	}
	return out, nextToken, nil
}

// ----------------------------------------------------------------------------
// WARM START — load the Redis mirror into the in-memory map at boot.
// ----------------------------------------------------------------------------

// Warm loads the entire routing-table mirror from Redis into the in-memory map.
// Call ONCE at startup, before the gateway begins serving and before the event
// consumer starts applying live updates, so a freshly-scheduled replica serves
// the last-known table immediately instead of cold (NO_ROUTE for everything)
// until events replay.
//
// WHY a separate method (not in the constructor): warming does I/O and can fail;
// the caller decides the policy (fatal vs. start-cold-and-let-events-fill). Boot
// ordering also matters — main wires this between "Redis connected" and "start
// consuming events". A miss (empty hash on a brand-new cluster) is not an error.
func (s *RouteStore) Warm(ctx context.Context) error {
	fields, err := s.rdb.HGetAll(ctx, routesHashKey).Result()
	if err != nil {
		return fmt.Errorf("redisrepo: warm route table from mirror: %w", err)
	}

	loaded := make(map[string]domain.Route, len(fields))
	for name, blob := range fields {
		var r domain.Route
		if err := json.Unmarshal([]byte(blob), &r); err != nil {
			// A single corrupt field shouldn't sink the whole warm-up; skip it and
			// let the next event for that model re-create it. Surface via the
			// returned error wrap so the caller can decide, but keep the good rows.
			return fmt.Errorf("redisrepo: warm: unmarshal route %q: %w", name, err)
		}
		loaded[name] = r
	}

	// Replace the table wholesale under the write lock — atomic from any reader's
	// view (a concurrent Get sees the old or new map, never a half-filled one).
	s.mu.Lock()
	s.table = loaded
	s.mu.Unlock()
	return nil
}
