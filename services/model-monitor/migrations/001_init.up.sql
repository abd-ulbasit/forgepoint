-- 001_init.up.sql — the Model Monitor's durable Postgres schema.
--
-- ============================================================================
-- WHAT THIS SCHEMA IS FOR: monitor CONFIG + the durable drift-verdict HISTORY
-- ============================================================================
--
-- The Model Monitor is the closed-loop "did the model rot?" service. Two of its
-- three stores are Postgres (the live sliding WINDOWS live in Redis — they are
-- ephemeral aggregation state, not durable record):
--
--   monitors        the CONFIG aggregate (the write model). One row per
--                   (owner_team, model_name): the window shape, per-type
--                   thresholds, the baseline reference, and the auto-retrain
--                   switch. Soft-deleted (deleted_at) so DeleteMonitor is a
--                   reversible, idempotent no-op-on-repeat.
--
--   drift_reports   the durable VERDICT history (the audit trail + UI time
--                   series + "why did the model retrain at 03:14?"). One row per
--                   CLOSED window, made IDEMPOTENT on window_id so a redelivered
--                   inference stream or a double-scored window yields ONE report,
--                   not duplicates — the persistence half of exactly-once.
--
-- ----------------------------------------------------------------------------
-- THE TWO LOAD-BEARING INVARIANTS (each is interview-probed)
-- ----------------------------------------------------------------------------
--
-- 1) TENANCY KEY = (owner_team, model_name), NOT model_name alone.
--    Monitors are keyed by (owner_team, model_name), so a model name is NOT
--    globally unique: team-a/"fraud" and team-b/"fraud" are different models.
--    Every report therefore CARRIES owner_team (copied from the monitor at score
--    time, server-authoritative) and every read/purge in the adapter filters on
--    (owner_team, …). Keying reports by model name alone would let team-a read or
--    DESTROY team-b's "fraud" history (cross-tenant IDOR / data loss). The
--    composite indexes below exist to make those team-scoped reads fast.
--
-- 2) IDEMPOTENCY KEY = window_id (UNIQUE) on drift_reports.
--    Each closed window has a stable UUID. The adapter's Save does
--    INSERT … ON CONFLICT (window_id) DO NOTHING + a RETURNING/refetch so a
--    second Save of the same window returns the EXISTING row with inserted=false.
--    The caller emits the ModelDriftDetected event only on a TRUE insert — so the
--    event fires exactly once per window even under stream redelivery.
--
-- ----------------------------------------------------------------------------
-- DESIGN CHOICES (each interview-defensible, consistent with the other services)
-- ----------------------------------------------------------------------------
--
-- IDs are TEXT (UUIDv4 strings), not native uuid — the domain carries ids as
-- strings (uuid.NewString()) everywhere, so TEXT keeps the adapter a pass-through
-- with no boundary translation (the same call registry/experiment-tracker make).
--
-- ENUMS (severity, drift_type, state) are stored as SMALLINT holding the domain's
-- iota value, with a CHECK constraint pinning the legal range. WHY int, not a
-- Postgres ENUM type or TEXT: the domain's enums ARE small iota ints
-- (DriftSeverityCritical == 3) that mirror the proto enum by value, so the column
-- is a trivial int cast with no lookup table and no CREATE TYPE migration churn
-- every time a rung is added. The CHECK gives the "bad value can't be written"
-- guarantee. (We trade psql self-describing rows for a friction-free schema — the
-- adapter and the UI translate the int to a label.)
--
-- THRESHOLDS and METRICS are JSONB. Thresholds are a small per-drift-type array
-- ([]ThresholdConfig); a report's metrics are a per-feature/output breakdown
-- ([]DriftMetric). Both are read/written whole (never queried by an inner field),
-- so JSONB keeps the schema stable as the shapes evolve and the adapter marshals
-- the slice ⇄ JSONB in exactly one place (mapping.go). A child table per metric
-- would add a join and a write amplification for data we always fetch together.
--
-- WINDOW bounds are stored as their raw units: window_duration_ns is the Go
-- time.Duration as an int64 nanosecond count (lossless round-trip of the domain
-- type), window_size / min_samples are plain ints. NULL is not used — 0 means
-- "unbounded by that dimension", exactly as the domain models it.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- monitors — the CONFIG aggregate and the TENANCY ANCHOR (write model).
--
-- The natural key is (owner_team, model_name): one monitor per model within a
-- team. We keep a surrogate TEXT id (UUIDv4) as the PRIMARY KEY because reports
-- reference a monitor by id (GetByID is on the data plane: the events adapter
-- resolves model→monitor id, the scorer loads config by id), and an id is stable
-- even if a model were ever renamed. The (owner_team, model_name) uniqueness is a
-- PARTIAL unique index so a soft-deleted monitor frees its name for a fresh one.
-- ----------------------------------------------------------------------------
CREATE TABLE monitors (
    id                    TEXT PRIMARY KEY,
    model_name            TEXT        NOT NULL CHECK (model_name <> ''),
    owner_team            TEXT        NOT NULL CHECK (owner_team <> ''),

    -- Window shape. 0 = unbounded by that dimension (the domain's convention).
    window_duration_ns    BIGINT      NOT NULL DEFAULT 0,  -- Go time.Duration (ns)
    window_size           INTEGER     NOT NULL DEFAULT 0,  -- count bound
    min_samples           INTEGER     NOT NULL DEFAULT 0,  -- refuse-to-score floor

    -- Per-drift-type thresholds: a JSONB array of {drift_type, method, warn, critical}.
    thresholds            JSONB       NOT NULL DEFAULT '[]'::jsonb,

    -- The closed-loop switch + the opaque pipeline it triggers.
    auto_retrain          BOOLEAN     NOT NULL DEFAULT FALSE,
    retrain_pipeline_id   TEXT        NOT NULL DEFAULT '',

    -- Server-authoritative lifecycle + baseline reference.
    state                 SMALLINT    NOT NULL DEFAULT 0
                              CHECK (state BETWEEN 0 AND 4),
    baseline_version      TEXT        NOT NULL DEFAULT '',
    baseline_captured_at  TIMESTAMPTZ,                    -- NULL until a baseline loads

    created_at            TIMESTAMPTZ NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL,
    deleted_at            TIMESTAMPTZ                      -- soft delete; NULL = live
);

-- The (owner_team, model_name) business key — PARTIAL so it applies only to LIVE
-- rows. WHY partial: a soft-deleted monitor must not block re-configuring the same
-- model later, and we keep the old row for audit. Upsert targets THIS index.
CREATE UNIQUE INDEX monitors_team_model_live_uniq
    ON monitors (owner_team, model_name)
    WHERE deleted_at IS NULL;

-- The fleet list (ListMonitors) is team-scoped, newest-first, over LIVE rows.
-- (owner_team, created_at DESC, id) backs the keyset pagination total order.
CREATE INDEX monitors_team_created_idx
    ON monitors (owner_team, created_at DESC, id)
    WHERE deleted_at IS NULL;

-- ----------------------------------------------------------------------------
-- drift_reports — the durable VERDICT history (one row per closed window).
--
-- owner_team is stored on EVERY report (the tenancy key) and is part of every
-- read/purge. monitor_id ties the report to its monitor (FK, ON DELETE CASCADE
-- so purging a hard-deleted monitor's reports is automatic — though the normal
-- path is the explicit PurgeByModel). window_id is the UNIQUE idempotency key.
-- ----------------------------------------------------------------------------
CREATE TABLE drift_reports (
    id            TEXT PRIMARY KEY,
    monitor_id    TEXT        NOT NULL REFERENCES monitors (id) ON DELETE CASCADE,
    owner_team    TEXT        NOT NULL CHECK (owner_team <> ''),  -- TENANCY KEY
    model_name    TEXT        NOT NULL CHECK (model_name <> ''),
    model_version TEXT        NOT NULL DEFAULT '',

    drift_type    SMALLINT    NOT NULL CHECK (drift_type BETWEEN 0 AND 3),
    severity      SMALLINT    NOT NULL CHECK (severity BETWEEN 0 AND 3),

    -- The per-thing breakdown: a JSONB array of DriftMetric
    -- {name, method, score, baseline_value, current_value, severity}.
    metrics       JSONB       NOT NULL DEFAULT '[]'::jsonb,

    -- window_id is the IDEMPOTENCY KEY: a stable UUID per closed window. UNIQUE so
    -- a redelivered/double-scored window collapses to one row (Save's ON CONFLICT).
    window_id     TEXT        NOT NULL,
    sample_count  INTEGER     NOT NULL DEFAULT 0,
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL,

    CONSTRAINT drift_reports_window_uniq UNIQUE (window_id)
);

-- The history query (List) and the health tile (LatestByModel) both scan a
-- (team, model)'s reports NEWEST-window-END first. (owner_team, model_name,
-- window_end DESC, id) is the total sort key for the keyset cursor — id breaks
-- the tie when two windows share an end instant, so pages never skip/dup.
CREATE INDEX drift_reports_team_model_window_idx
    ON drift_reports (owner_team, model_name, window_end DESC, id);

-- GetByID is team-scoped (a report id is not secret — it rides on events, retrain
-- requests, deep links, logs). (id, owner_team) lets the adapter look up by id AND
-- assert tenancy in one indexed predicate, so a guessed id from another tenant
-- returns not-found (anti-IDOR) without a full-table scan.
CREATE INDEX drift_reports_id_team_idx
    ON drift_reports (id, owner_team);
