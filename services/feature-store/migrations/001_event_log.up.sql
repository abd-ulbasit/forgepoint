-- 001_event_log.up.sql — the append-only EVENT LOG (the source of truth) and the
-- two Postgres-backed projections (name index + offline as-of view).
--
-- ============================================================================
-- PATTERN: EVENT SOURCING — the log is the truth, everything else is derived
-- ============================================================================
--
-- This service does NOT store current state and UPDATE it. Every change is an
-- immutable row INSERTed into feature_events. The "current value" of a feature is
-- a PROJECTION computed by folding (replaying) those rows. Three tables here:
--
--   feature_events     the immutable, append-only log (INSERT-only; never UPDATE/DELETE)
--   view_name_index    a derived catalog projection: (team,name) -> view id
--   idempotency_keys   exactly-once-EFFECT dedup for retried Append commands
--   offline_view       the durable point-in-time/baseline read model (CQRS read side)
--
-- WHY split the read models out of the log table rather than computing everything
-- on the fly: the hot read paths (catalog lookup, online serving, point-in-time
-- pulls) must not scan the entire log per request. The log stays pure and append-
-- only; the projections are caches rebuildable from it (RebuildViews replays the
-- log to regenerate them — the documented recovery path).

-- ----------------------------------------------------------------------------
-- feature_events — THE LOG
-- ----------------------------------------------------------------------------
--
-- One homogeneous row shape for all three event variants (ViewDefined,
-- ValuesWritten, ViewDeleted), discriminated by event_type. WHY one table with a
-- discriminator instead of a table per event type: the domain models the log as a
-- single ordered stream of FeatureEvent records replayed in version order; one
-- table maps 1:1 to that stream and keeps replay a single ordered SELECT. A
-- per-type table would force a UNION-and-sort on every replay for no benefit.
CREATE TABLE feature_events (
    -- version is the MONOTONIC, GAP-FREE total order the projection folds in. It is
    -- the PRIMARY KEY: it uniquely identifies a log position and is the replay sort
    -- key. We assign it explicitly in the Append transaction (max(version)+1 under a
    -- transaction-scoped advisory lock) rather than via a SERIAL/SEQUENCE, because a
    -- SEQUENCE leaks numbers on rollback (nextval is not transactional) and the
    -- domain contract requires GAP-FREE versions for read-your-writes
    -- (WrittenThroughVersion must equal the count of committed events). See the
    -- EventLog adapter's Append for the locking rationale.
    version BIGINT PRIMARY KEY,

    -- id is the server-assigned UUID v4 event id. UNIQUE so a buggy double-insert of
    -- the same logical event is caught by the database, not just trusted in code.
    id UUID NOT NULL UNIQUE,

    -- event_type discriminates the fold: 1=ViewDefined, 2=ValuesWritten,
    -- 3=ViewDeleted (mirrors domain.EventType's iota+1). A CHECK pins the domain
    -- enum into the schema so an out-of-range type can never be stored.
    event_type SMALLINT NOT NULL CHECK (event_type IN (1, 2, 3)),

    -- feature_view_id is the aggregate root the log is partitioned by. Every replay
    -- (Load/LoadView) filters on this; it is the leading column of the replay index.
    feature_view_id UUID NOT NULL,

    -- entity_id is set ONLY for ValuesWritten events (which entity the row is for);
    -- empty string for view-definition/deletion events (those are view-scoped). We
    -- store '' rather than NULL so equality predicates and indexes stay simple and
    -- the column is never three-valued.
    entity_id TEXT NOT NULL DEFAULT '',

    -- feature_values is the JSONB payload for ValuesWritten (name -> tagged-union
    -- value); NULL otherwise. JSONB (not JSON) so it is stored decomposed/binary and
    -- could be indexed/queried if a future feature needs it; we treat it as an opaque
    -- blob the domain marshals/unmarshals, keeping the SQL schema-agnostic to the
    -- evolving feature set (a new feature name needs NO migration — the whole point
    -- of putting the variable-shape payload in JSONB instead of wide columns).
    feature_values JSONB,

    -- event_time is when the values became TRUE in the real world (caller-owned
    -- domain time) — the timestamp ProjectAsOf compares against. For definition/
    -- deletion events it carries the definition/retirement time. timestamptz so it is
    -- stored as UTC and never silently shifted by a session timezone.
    event_time TIMESTAMPTZ NOT NULL,

    -- view_def is the schema snapshot carried by ViewDefined events (so a replay can
    -- reconstruct the FeatureView without a second source); NULL otherwise.
    view_def JSONB,

    -- appended_at is the server COMMIT/ingest time — distinct from event_time. Kept
    -- for audit; the fold never orders by it (point-in-time uses event_time, total
    -- order uses version).
    appended_at TIMESTAMPTZ NOT NULL
);

-- Replay index: Load/LoadView/ProjectAsOf all read one view's events in version
-- order. (feature_view_id, version) makes "all events for a view, ordered" an index
-- range scan instead of a filter+sort. version is already globally unique (PK), so
-- adding it here gives the per-view ordering for free.
CREATE INDEX idx_feature_events_view_version
    ON feature_events (feature_view_id, version);

-- Definition-subset index: LoadView needs ONLY the ViewDefined/ViewDeleted events
-- (types 1 and 3) to fold the schema, avoiding loading millions of value events to
-- read a definition. A PARTIAL index on those two types keeps it tiny (it indexes
-- only the handful of definition rows, never the high-volume value rows).
CREATE INDEX idx_feature_events_view_def
    ON feature_events (feature_view_id, version)
    WHERE event_type IN (1, 3);

-- Point-in-time scan support: GetAsOf-style SQL projections filter value events by
-- (feature_view_id, entity_id) and order by event_time/version. This index serves
-- the per-entity, as-of scan. Partial on type 2 (value events only) since
-- definition events have no entity.
CREATE INDEX idx_feature_events_entity_time
    ON feature_events (feature_view_id, entity_id, event_time, version)
    WHERE event_type = 2;

-- ----------------------------------------------------------------------------
-- view_name_index — the team-scoped catalog projection (name -> id)
-- ----------------------------------------------------------------------------
--
-- A DERIVED projection maintained on every ViewDefined/ViewDeleted append. It
-- exists so ResolveViewIDByName and ListViewIDs are O(index) lookups instead of a
-- full log scan. It is NOT a second source of truth: RebuildViews can recompute it
-- from the log. We keep it in the SAME database as the log so it can be updated in
-- the SAME transaction as the append (read-your-writes for the catalog).
CREATE TABLE view_name_index (
    feature_view_id UUID PRIMARY KEY,
    owner_team TEXT NOT NULL,
    -- name is stored as given; name_lower is the case-folded form we enforce
    -- uniqueness and do case-insensitive substring filtering on. Storing both avoids
    -- recomputing lower() per query and lets the UNIQUE constraint be case-insensitive.
    name TEXT NOT NULL,
    name_lower TEXT NOT NULL,
    -- is_deleted mirrors the view's soft-delete state so ListViewIDs can choose to
    -- include retired views (the catalog still lists them; their history is queryable).
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

-- TEAM-SCOPED UNIQUENESS: a (team, name) pair identifies at most one view. This is
-- the database-level guarantee behind the domain's create-vs-evolve decision and
-- the anti-cross-team-collision rule. Case-insensitive via name_lower so
-- "User_Features" and "user_features" cannot coexist in one team. A unique_violation
-- here surfaces to the adapter as domain.ErrViewNameConflict — but in normal flow
-- the service resolves first and UPSERTs, so this is a backstop against races.
CREATE UNIQUE INDEX uq_view_name_index_team_name
    ON view_name_index (owner_team, name_lower);

-- ListViewIDs paginates by id within a team (cursor = last id seen). (owner_team,
-- feature_view_id) supports the keyset range scan "team's views with id > cursor".
CREATE INDEX idx_view_name_index_team_id
    ON view_name_index (owner_team, feature_view_id);

-- ----------------------------------------------------------------------------
-- idempotency_keys — exactly-once EFFECT for retried Append commands
-- ----------------------------------------------------------------------------
--
-- IDEMPOTENCY (the interview-critical bit): NATS/gRPC give AT-LEAST-once delivery.
-- A producer whose response was lost will retry WriteFeatures with the SAME
-- idempotency key. Without dedup the retry would append duplicate value events and
-- corrupt counts/history. This table records, per key, the version RANGE the
-- original append produced. A repeat append with the same key is a no-op that
-- REPLAYS the original range (replayed=true) instead of writing again — turning
-- at-least-once delivery into exactly-once EFFECT.
--
-- WHY store the range (not just "seen"): the domain's Append contract must RETURN
-- the original events + writtenThrough on a replay so the service can re-run its
-- (idempotent) online projection update. We reload the events in [first,last] to
-- reconstruct that return value.
CREATE TABLE idempotency_keys (
    -- The key is scoped to a view so the same opaque key string can't collide across
    -- unrelated views. (idempotency_key, feature_view_id) is the natural PK.
    idempotency_key TEXT NOT NULL,
    feature_view_id UUID NOT NULL,
    -- The inclusive version range the original append wrote. last_version doubles as
    -- the writtenThrough to return on replay.
    first_version BIGINT NOT NULL,
    last_version BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (idempotency_key, feature_view_id)
);

-- ----------------------------------------------------------------------------
-- offline_view — the durable point-in-time / baseline READ MODEL (CQRS read side)
-- ----------------------------------------------------------------------------
--
-- Backs the OfflineViewStore port. RebuildViews(offline) materializes a baseline
-- snapshot here (entity -> latest vector) and GetAsOf reads it. WHY a separate
-- table from the log: a real point-in-time pull over a large log belongs in the
-- database as a precomputed projection, not an in-process replay per request. It is
-- a CACHE — fully rebuildable from feature_events — so we may TRUNCATE+rewrite it on
-- Rebuild without touching the log.
CREATE TABLE offline_view (
    feature_view_id UUID NOT NULL,
    entity_id TEXT NOT NULL,
    -- The materialized vector: values payload + provenance (which log version/
    -- event_time/schema_version produced it). Provenance travels with the row so a
    -- reader can detect staleness and reproduce the read.
    feature_values JSONB NOT NULL,
    as_of_version BIGINT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    schema_version BIGINT NOT NULL,
    -- One row per (view, entity): the latest materialized vector. Rebuild UPSERTs.
    PRIMARY KEY (feature_view_id, entity_id)
);
