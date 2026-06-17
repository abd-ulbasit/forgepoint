-- 001_init.up.sql — the Experiment Tracker's durable schema.
--
-- ============================================================================
-- WHAT THIS SCHEMA IS FOR: experiments → runs → (params + metric time-series)
-- ============================================================================
--
-- The Experiment Tracker is the MLflow of the platform: it records WHICH model
-- configs were tried (params), WHAT they scored over training (metrics), and how
-- runs group under a named experiment. Five tables, one per concern:
--
--   experiments        the NAMED GROUPING (tenancy anchor; unique name per team)
--   runs               one EXECUTION within an experiment (the unit you compare)
--   run_params         write-once INPUTS (hyperparameters) — one row per (run,key)
--   run_metrics        the METRIC TIME-SERIES — (run, key, step, value, ts);
--                      the high-throughput, append-only, dedup-by-(run,key,step)
--                      table the event-driven ingestion pattern is built around
--   idempotency_keys   (operation, key) → result_id — Stripe-style request dedup
--                      shared by StartRun / LogMetrics / DeleteRun / SetArtifacts
--
-- The defining table is run_metrics. Everything about its shape — the composite
-- UNIQUE index, the server-stamped ts, the ON CONFLICT path the adapter uses —
-- exists to make a RETRIED batch double-write NOTHING while keeping one INSERT
-- per batch (not per point). That is the persistence half of the idempotency
-- guarantee the domain's DedupMetricPoints starts in memory.
--
-- ----------------------------------------------------------------------------
-- DESIGN CHOICES (so each is interview-defensible)
-- ----------------------------------------------------------------------------
--
-- IDs are TEXT (UUIDv4 strings), not native uuid. The domain carries ids as
-- strings everywhere (uuid.NewString()); TEXT keeps the adapter a pass-through
-- with no type translation at the boundary. (Native uuid saves 28 bytes/row and
-- gains validation; we trade that for a dead-simple adapter and the domain owning
-- the id format — the same call the other Forgepoint services make.)
--
-- ENUMS (run status/source) are stored as TEXT holding the domain's own string
-- constant ("RUNNING", "API"), NOT a Postgres ENUM type and NOT an int. WHY:
--   * The domain already uses human-readable string enums (models.go) so the
--     column is a trivial cast (string(run.Status)) — no lookup table.
--   * TEXT + a CHECK constraint pins the legal set without a CREATE TYPE
--     migration every time the domain adds a status (Postgres ENUM ALTER is
--     awkward and locks). The CHECK gives the same "bad value can't be written"
--     guarantee with zero migration friction.
--   * Rows are self-describing in psql ("FINISHED", not "2").
--
-- TAGS / FINAL_METRICS / ARTIFACTS are JSONB. Their shape is free-form
-- (tags: map[string]string; artifacts: arbitrary JSON; final_metrics: a small
-- denormalized array). JSONB keeps the schema stable as these evolve and is
-- indexable/queryable if we ever need it. The adapter marshals map[string]any ⇄
-- JSONB in one place (mapping.go) so neither SQL nor domain leaks into the other.
--
-- final_metrics is DENORMALIZED onto runs (a JSONB snapshot computed at FinishRun
-- by ComputeFinalMetrics). This is the CQRS-flavored read model: ListRuns and the
-- leaderboard render the headline number per key WITHOUT scanning the full
-- run_metrics series. The series in run_metrics is the write-side source of truth;
-- final_metrics is the read-optimized projection of it.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- experiments — the NAMED GROUPING and the TENANCY ANCHOR.
-- ----------------------------------------------------------------------------
CREATE TABLE experiments (
    -- Server-generated UUIDv4 (domain sets uuid.NewString()). TEXT, see header.
    id          TEXT PRIMARY KEY,

    -- Human name, unique per team (the partial index below). NOT NULL: the
    -- service rejects an empty name before it reaches here.
    name        TEXT        NOT NULL,
    description TEXT        NOT NULL DEFAULT '',

    -- Organizational labels (NOT metrics). map[string]string in the domain,
    -- stored as a JSONB object. Defaults to an empty object so a read never has
    -- to special-case NULL — tags are always a (possibly empty) map.
    tags        JSONB       NOT NULL DEFAULT '{}'::jsonb,

    -- SERVER-authoritative tenancy/ownership: set from auth claims, never client
    -- input (the anti mass-assignment rule). team is the tenancy boundary every
    -- read/write is scoped by at the SERVICE layer.
    owner_id    TEXT        NOT NULL,
    team        TEXT        NOT NULL,

    created_at  TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL,

    -- Soft-delete marker. NULL = active; non-NULL = archived (the domain's
    -- *time.Time ArchivedAt). We soft-delete because runs reference the
    -- experiment for lineage; a hard delete would orphan them.
    archived_at TIMESTAMPTZ
);

-- UNIQUE NAME PER TEAM — but only among ACTIVE (non-archived) experiments.
--
-- WHY a PARTIAL unique index (WHERE archived_at IS NULL) and not a plain
-- UNIQUE (team, name): archiving "fraud-v3" then creating a fresh "fraud-v3"
-- must succeed (the old one is soft-deleted and out of the way). A full unique
-- constraint would forbid ever reusing a name after archive — surprising and
-- wrong. Scoping uniqueness to live rows gives "one ACTIVE experiment per
-- (team, name)" while letting archived namesakes coexist. A violation here
-- surfaces as SQLSTATE 23505, which the adapter maps to ErrRepoConflict →
-- the service's ErrExperimentNameExists.
CREATE UNIQUE INDEX experiments_team_name_active_uniq
    ON experiments (team, name)
    WHERE archived_at IS NULL;

-- Keyset-list support: the List port pages a team's experiments newest-first by
-- (created_at, id). This composite index makes that ORDER BY ... LIMIT a clean
-- backwards index scan (no sort), and the (created_at, id) tuple is the cursor.
CREATE INDEX experiments_team_created_idx
    ON experiments (team, created_at DESC, id DESC);

-- ----------------------------------------------------------------------------
-- runs — one EXECUTION within an experiment (the unit you compare).
-- ----------------------------------------------------------------------------
CREATE TABLE runs (
    id              TEXT PRIMARY KEY,

    -- The owning experiment. ON DELETE CASCADE: experiments are SOFT-deleted in
    -- normal operation, so this cascade is a belt-and-suspenders cleanup for a
    -- genuine hard delete (e.g. a test teardown or a future admin purge) — it
    -- keeps runs from being orphaned. The FK also makes "create a run under a
    -- missing experiment" a 23503 the adapter can surface, though the service
    -- already verifies the parent first.
    experiment_id   TEXT        NOT NULL
                                REFERENCES experiments(id) ON DELETE CASCADE,

    display_name    TEXT        NOT NULL DEFAULT '',

    -- Lifecycle state, stored as the domain's string enum. CHECK pins the legal
    -- set so a typo/forged value can't land even though the column is plain TEXT.
    -- '' (RunStatusUnspecified) is deliberately NOT allowed — a run is always
    -- born RUNNING and only ever moves to a terminal state.
    status          TEXT        NOT NULL
                                CHECK (status IN ('RUNNING','FINISHED','FAILED','KILLED')),

    -- Which ingestion path created the run (API = sync StartRun, EVENT = async
    -- NATS consumer). CHECK pins it; '' is rejected (a run always has a source).
    source          TEXT        NOT NULL
                                CHECK (source IN ('API','EVENT')),

    -- Opaque Registry model-version id (NOT an FK — services are decoupled;
    -- database-per-service means no cross-service FK). '' when the run yields no
    -- model; stored as '' (not NULL) since the domain carries it as a string.
    model_version_id TEXT       NOT NULL DEFAULT '',

    owner_id        TEXT        NOT NULL,

    -- DENORMALIZED headline metrics (the CQRS read model). A JSONB array of
    -- {key,value,step,ts} objects, computed at FinishRun. Empty array while the
    -- run is RUNNING (no finals yet). NOT NULL DEFAULT '[]' so a read is never
    -- NULL — finals are always a (possibly empty) array.
    final_metrics   JSONB       NOT NULL DEFAULT '[]'::jsonb,

    -- Free-form side artifacts (size-capped by the service). NULL = none set.
    artifacts       JSONB,

    started_at      TIMESTAMPTZ NOT NULL,

    -- Terminal timestamp. NULL while RUNNING; set at the terminal transition
    -- (the domain's *time.Time EndedAt).
    ended_at        TIMESTAMPTZ,

    -- ------------------------------------------------------------------------
    -- StartRun idempotency NATURAL-KEY BACKSTOP (the second line of defense).
    -- ------------------------------------------------------------------------
    --
    -- The PRIMARY defense against a duplicate run on a retried StartRun is the
    -- idempotency_keys row written in the SAME transaction as the run insert
    -- (see CreateRunIdem): a run can't commit without its key, so a replay always
    -- finds the key and returns the existing run. These two columns + the partial
    -- UNIQUE index below are the BACKSTOP: even if the idempotency_keys row were
    -- ever lost or bypassed, the database itself refuses to commit a SECOND run
    -- carrying the same (operation, key). StartRun is the only mutation with no
    -- other natural-key backstop (LogMetrics has (run_id,key,step); params have
    -- (run_id,key)), so it is exactly the one that needs this.
    --
    -- NULLable because not every run is created with an idempotency key (an empty
    -- key means "no idempotency requested"); the partial index only constrains
    -- rows that actually carry one. operation is stored alongside so the same
    -- client UUID reused across RPCs (start_run vs another op) never collides.
    idempotency_operation TEXT,
    idempotency_key       TEXT
);

-- Keyset-list support for ListRuns (runs within an experiment, newest-first).
-- Includes status so the optional status filter stays index-friendly.
CREATE INDEX runs_experiment_started_idx
    ON runs (experiment_id, started_at DESC, id DESC);

-- The natural-key backstop: at most ONE run per (operation, key) among rows that
-- carry an idempotency key. PARTIAL (WHERE idempotency_key IS NOT NULL) so the
-- many keyless runs are unconstrained and don't all collide on a single NULL key.
-- A violation surfaces as SQLSTATE 23505 on the constraint runs_idempotency_uniq;
-- CreateRunIdem recognizes it as "a concurrent creator already committed a run with
-- this key" and returns ErrRepoConflict rather than a raw DB error. This index makes
-- a duplicate StartRun run physically impossible even if the idempotency_keys-row
-- path were ever bypassed.
CREATE UNIQUE INDEX runs_idempotency_uniq
    ON runs (idempotency_operation, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- ----------------------------------------------------------------------------
-- run_params — write-once INPUTS (hyperparameters).
-- ----------------------------------------------------------------------------
--
-- One row per (run, key). The UNIQUE (run_id, key) is the write-once enforcer at
-- the storage layer: the service reads existing params and rejects a CHANGED
-- value (ErrParamConflict) before writing, but this index is the backstop so a
-- racing double-log can't create two rows for one key. Value is TEXT because
-- params are heterogeneous (ints/floats/bools/strings) used for display/group/
-- equality, never math — one string column keeps the schema trivial (MLflow does
-- the same).
CREATE TABLE run_params (
    run_id  TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    key     TEXT NOT NULL,
    value   TEXT NOT NULL,
    PRIMARY KEY (run_id, key)
);

-- ----------------------------------------------------------------------------
-- run_metrics — the METRIC TIME-SERIES (the heart of the service).
-- ----------------------------------------------------------------------------
--
-- Each row is ONE sample: (run, key, step, value, ts). A metric is a CURVE over
-- training steps, so we store points, not a single number.
--
--   * value is DOUBLE PRECISION — ML metrics are reals. The service rejects
--     NaN/±Inf before insert (finite-math guard), so the column only ever holds
--     finite doubles.
--   * step is BIGINT — the client-owned semantic X axis ("loss at epoch 10"),
--     can be large over a long run.
--   * ts is TIMESTAMPTZ, SERVER-stamped on ingest (the domain overwrites any
--     client value). It is the PHYSICAL axis: the retention/partition key and
--     what time-window queries filter on. Server-authoritative so a client can't
--     backdate a point into a dropped partition or the future.
--
-- THE CRITICAL CONSTRAINT — UNIQUE (run_id, key, step):
--   This is the DB half of metric idempotency. The adapter's AppendMetrics does
--   INSERT ... ON CONFLICT (run_id, key, step) DO NOTHING, so re-logging "loss at
--   step 100" (a retried batch, a redelivered NATS message) double-writes NOTHING
--   — the already-stored point wins, matching the domain's "first wins" dedup.
--   The returned row count is exactly how many points were NEW, which the service
--   surfaces as accepted_count. Keying on (run,key,step) and NOT value means a
--   buggy double-log with a different value is ALSO rejected (it's the same
--   logical sample) — exactly the curve-corruption we want to prevent.
--
-- PARTITIONING NOTE (why ts exists as a separate axis): at production scale this
-- table would be RANGE-partitioned on ts so old partitions drop cheaply (the
-- design doc calls it "time-partitioned tables for metrics"). We ship a single
-- table here — partitioning is an operational/scale concern layered on later via
-- a migration — but the schema is already partition-READY: ts is a first-class
-- column and the unique key leads with run_id (a local index per partition still
-- enforces dedup within the hot partition). INTERVIEW: "How would you scale
-- run_metrics?" Declarative RANGE partitioning on ts (monthly), drop old
-- partitions for retention; the (run,key,step) uniqueness becomes a per-partition
-- local index. We left it unpartitioned to keep the teaching schema readable.
CREATE TABLE run_metrics (
    run_id  TEXT             NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    key     TEXT             NOT NULL,
    step    BIGINT           NOT NULL,
    value   DOUBLE PRECISION NOT NULL,
    ts      TIMESTAMPTZ      NOT NULL,
    -- The dedup identity of a metric sample within a run (see the long note above).
    PRIMARY KEY (run_id, key, step)
);

-- GetMetricHistory orders by (key, step, ts) and filters by run + optional key/
-- step range. The PRIMARY KEY (run_id, key, step) already serves that exact
-- ordered scan — no extra index needed: a query "WHERE run_id=$1 [AND key=...]
-- ORDER BY key, step" walks the PK in order. We add NO redundant index (the PK
-- is the access path) — fewer indexes = faster inserts on the hot write table.

-- ----------------------------------------------------------------------------
-- idempotency_keys — Stripe-style request dedup, shared across mutating RPCs.
-- ----------------------------------------------------------------------------
--
-- (operation, key) → result_id. The composite PRIMARY KEY scopes keys PER-RPC:
-- the same client-generated UUID reused across StartRun and LogMetrics never
-- collides because operation differs. result_id is the entity the original call
-- produced (the run id) so a replay can return/reference it.
--
-- WHY a Postgres table and not Redis for THIS service: the task pins "idempotency
-- table for batch dedup" here, and a durable table gives exactly-once semantics
-- that survive a restart (a Redis TTL'd key could expire and let a late retry
-- duplicate). The IdempotencyStore port is backend-agnostic, so a Redis adapter
-- could be swapped in for a different deployment without touching the domain —
-- but the durable table is the right default for "don't create a second run on
-- retry, ever."
CREATE TABLE idempotency_keys (
    operation  TEXT        NOT NULL,
    key        TEXT        NOT NULL,
    result_id  TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (operation, key)
);
