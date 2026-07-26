// Package redisrepo holds the Inference Gateway's REDIS persistence ADAPTERS —
// the outer-ring implementations of the domain ports declared in
// internal/domain/ports.go. It is the only place in the service that imports a
// Redis client; the domain stays pure (stdlib + uuid) and depends only on the
// interfaces these types satisfy.
//
// ============================================================================
// WHY THIS SERVICE IS REDIS-ONLY (no Postgres, no migrations/ SQL)
// ============================================================================
//
// The Inference Gateway is on the HOT PATH of every online prediction. Its state
// is all ephemeral, high-churn, and latency-critical — the exact profile Redis
// serves and Postgres does not:
//
//   - RATE-LIMIT token buckets — per api-key, refilled continuously, read+written
//     on every single request. A relational row update per predict would add a
//     fsync to the critical path and melt under load. Redis does the whole
//     check-refill-consume atomically in a single in-memory Lua call (sub-ms).
//   - CIRCUIT-BREAKER state — per (model,version), shared across replicas so one
//     replica's observation of a sick backend can trip the breaker for all.
//     Small, hot, write-heavy counters with a natural TTL.
//   - ROUTE-TABLE mirror — the authoritative routing table is held IN PROCESS for
//     zero-network hot-path lookups (exactly how Envoy holds xDS-pushed config);
//     Redis is only the warm-start MIRROR so a freshly-scheduled replica recovers
//     its table without replaying every past ModelDeployed event from NATS.
//   - QUOTA cache — an eventually-consistent per-team "blocked" flag flipped by
//     the QuotaExceeded event. A soft, fast pre-flight reject, not a ledger.
//
// None of this is a SYSTEM OF RECORD. The authoritative copies live elsewhere:
// Billing owns the real usage ledger (Postgres + outbox), the Registry owns the
// real deployment state, and NATS events are the durable source the gateway
// rebuilds its caches from. The gateway's stores are derived, disposable
// projections — losing the whole Redis instance costs a brief warm-up, not data.
// Putting any of it in Postgres would be miscategorizing a cache as a database.
//
// Hence there is NO migrations/ directory and NO *.up.sql for this service: there
// is no relational schema to migrate. (The repo-wide convention "migrations/ in
// golang-migrate file format" is explicitly scoped to a service's *Postgres*
// database; a Redis-only service has none. See the README note in this package's
// route_store.go for the key/structure layout that documents the Redis "schema"
// instead.) The integration tests below therefore use only testutil.StartRedis.
//
// ============================================================================
// THE FOUR ADAPTERS IN THIS PACKAGE (one file each)
// ============================================================================
//
//	route_store.go   RouteStore       — in-memory authoritative table + Redis mirror
//	rate_limiter.go  RateLimiter      — atomic token bucket via a single Lua script
//	quota_checker.go QuotaChecker     — eventually-consistent per-team blocked flag
//	breaker_store.go (shared breaker) — cross-replica circuit-breaker counters
//
// Each file carries its own WHY/HOW/tradeoff comments; this doc is the map.
package redisrepo
