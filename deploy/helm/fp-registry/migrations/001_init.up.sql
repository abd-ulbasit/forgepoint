-- 001_init.up.sql — the WRITE model-of-record schema for the Model Registry.
--
-- ============================================================================
-- THIS IS THE POSTGRES SOURCE OF TRUTH (the WRITE side of CQRS)
-- ============================================================================
--
-- The registry is a CQRS service: Postgres is the strongly-consistent WRITE
-- store (normalized rows, real constraints, transactions) and Redis is the
-- eventually-consistent READ projection (built from emitted events). This file
-- defines ONLY the write side. The Redis projection has NO schema migration —
-- it is rebuildable state, derived from the events the commands emit.
--
-- THREE TABLES, mapping 1:1 to the three durable concerns in ports.go:
--   models               — the Model aggregate root (write store)
--   model_versions       — the immutable ModelVersion children (write store)
--   command_idempotency   — the generic MUTATION-idempotency ledger
--
-- DESIGN PRINCIPLE — the DB enforces the invariants the domain ALSO enforces.
-- "Defense in depth": the service validates (team uniqueness, single-prod), but
-- a UNIQUE INDEX / FK / partial index is the LAST line that no buggy code path
-- or concurrent race can slip past. Where the domain and the DB both guard an
-- invariant, the DB constraint is the one that holds under concurrency.

-- ----------------------------------------------------------------------------
-- models — the aggregate root: a named, versioned thing owned by a team.
-- ----------------------------------------------------------------------------
--
-- WHY each column the way it is (mapping domain.Model → row):
--   id              the server-minted id (server-authoritative). Primary key. TYPED
--                   AS TEXT, NOT uuid: the domain treats Model.ID as an OPAQUE string
--                   produced by the injected IDGenerator port (production mints a
--                   UUIDv4; tests inject a deterministic sequence like "model-1"). The
--                   write store must persist WHATEVER the domain hands it without
--                   imposing a format the port does not — a `uuid` column would reject
--                   the deterministic test ids and, worse, couple the storage schema to
--                   an id-format decision that belongs to the domain. TEXT keeps the
--                   adapter faithful to the port's "id is an opaque string" contract.
--   name            human handle, UNIQUE WITHIN A TEAM (the (team,name) index
--                   below) — not globally, so two teams may both own "fraud".
--   team / owner_id come from the caller's auth claims (server-authoritative);
--                   stored so every read is team-scopable without a join.
--   tags            JSONB — a free-form string→string map. JSONB (not hstore or
--                   a side table) because tags are queried as a whole object on
--                   the write side (the read side indexes them in Redis), JSONB
--                   is indexable if we ever need it, and it round-trips cleanly
--                   to Go's map[string]string via encoding/json.
--   production_version / latest_version are READ-MODEL conveniences. They live
--                   here too (not only in Redis) so the write store can answer
--                   "what's prod" without scanning versions during a rebuild and
--                   so a projection rebuild has a denormalized seed. They hold the
--                   version LABEL ("" when none). They are advisory on the write
--                   side — the authoritative prod pointer is the version row whose
--                   stage=PRODUCTION (enforced by the partial unique index below).
--   idempotency_key the CREATE-idempotency key recorded INLINE with the row
--                   (ports.go: "the two genuinely-creating commands record the
--                   key inline"). A retried RegisterModel with the same key finds
--                   THIS row via LookupModelByIdempotencyKey and returns it instead
--                   of minting a duplicate. NULL = the caller opted out of dedup.
--   archived_at     soft-delete timestamp (NULL = active). We keep WHEN, not just
--                   a bool, for audit — the same posture as Stripe.
CREATE TABLE models (
    id                 TEXT         PRIMARY KEY,
    name               TEXT         NOT NULL,
    description        TEXT         NOT NULL DEFAULT '',
    owner_id           TEXT         NOT NULL,
    team               TEXT         NOT NULL,
    framework          TEXT         NOT NULL DEFAULT '',
    task_type          TEXT         NOT NULL DEFAULT '',
    tags               JSONB        NOT NULL DEFAULT '{}'::jsonb,
    production_version TEXT         NOT NULL DEFAULT '',
    latest_version     TEXT         NOT NULL DEFAULT '',
    idempotency_key    TEXT,        -- NULL when the caller supplied no key
    created_at         TIMESTAMPTZ  NOT NULL,
    updated_at         TIMESTAMPTZ  NOT NULL,
    archived_at        TIMESTAMPTZ  -- NULL = active; set on soft-delete
);

-- THE (team, name) UNIQUENESS INVARIANT — the heart of the namespace guarantee.
-- A model name is unique WITHIN a team, not globally. A violating INSERT raises
-- SQLSTATE 23505 (unique_violation), which the adapter maps to domain.ErrWriteConflict,
-- which the service maps to ErrModelNameTaken. This index is what makes the create
-- race-safe: two concurrent RegisterModel("fraud") for the same team — one wins the
-- INSERT, the other gets 23505 (the domain's idempotency path having already let a
-- same-KEY retry through earlier). NOTE: it covers archived rows too, so a name stays
-- reserved after soft-delete (a rename is a new model; we do not recycle names).
CREATE UNIQUE INDEX models_team_name_uniq ON models (team, name);

-- CREATE-idempotency lookup index. Partial (WHERE idempotency_key IS NOT NULL) so the
-- many NULL-key rows don't bloat it, and UNIQUE on (team, idempotency_key) so the same
-- key can't be reused for two different models within a team — the structural guard
-- behind LookupModelByIdempotencyKey returning exactly one row.
CREATE UNIQUE INDEX models_team_idempotency_uniq
    ON models (team, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- ListModels is team-scoped and newest-first with cursor pagination on (created_at,id).
-- This composite index makes that page a backward index range scan, not a sort. The id
-- tiebreaker makes the cursor TOTAL (two rows with the same created_at still order
-- deterministically) — essential for stable cursor pagination.
CREATE INDEX models_team_created_idx ON models (team, created_at DESC, id DESC);

-- ----------------------------------------------------------------------------
-- model_versions — immutable point-in-time snapshots of a model's artifact.
-- ----------------------------------------------------------------------------
--
-- WHY a separate table (not a JSONB array on models): versions are queried,
-- paginated, filtered by stage, and uniquely constrained per (model,label) — all
-- relational operations a normalized child table does natively and an array does not.
--
-- stage / status are SMALLINT mirroring the domain int enums (StageDev=1.. ,
-- StatusPendingUpload=1..). We store the INT, not the string, because the enum is a
-- closed set whose numeric values match the proto byte-for-byte (models.go), so the
-- mapping is a trivial cast and the column is compact + index-friendly. A CHECK keeps
-- a bad value out at the DB boundary.
--
-- metrics JSONB ← domain map[string]float64. artifact_* are SERVER-MEASURED storage
-- facts, NULL/zero until MarkVersionReady populates them (never set on create).
-- id / model_id are TEXT (not uuid) for the same reason as models.id above: the
-- domain hands the adapter opaque string ids from its IDGenerator port, and the store
-- persists them verbatim. The FK to models(id) still enforces referential integrity.
CREATE TABLE model_versions (
    id              TEXT         PRIMARY KEY,
    model_id        TEXT         NOT NULL REFERENCES models (id),
    version         TEXT         NOT NULL,
    description     TEXT         NOT NULL DEFAULT '',
    metrics         JSONB        NOT NULL DEFAULT '{}'::jsonb,
    artifact_path   TEXT         NOT NULL DEFAULT '',
    artifact_digest TEXT         NOT NULL DEFAULT '',
    size_bytes      BIGINT       NOT NULL DEFAULT 0,
    stage           SMALLINT     NOT NULL,
    status          SMALLINT     NOT NULL,
    idempotency_key TEXT,        -- CREATE-idempotency key for CreateVersion (inline)
    created_by      TEXT         NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL,
    -- stage ∈ {DEV..ARCHIVED} (1..4); never persisted as UNSPECIFIED (0).
    CONSTRAINT model_versions_stage_chk  CHECK (stage  BETWEEN 1 AND 4),
    -- status ∈ {PENDING_UPLOAD..FAILED} (1..3); never UNSPECIFIED (0).
    CONSTRAINT model_versions_status_chk CHECK (status BETWEEN 1 AND 3)
);

-- THE (model_id, version) UNIQUENESS INVARIANT — a label is unique within a model.
-- This is the index the service's collision-safe insert loop races against: an
-- auto-assigned label that collides raises 23505 → ErrWriteConflict → the service
-- retries with the next label (so an unpinned caller never sees ALREADY_EXISTS); a
-- PINNED label that collides surfaces as ErrVersionExists.
CREATE UNIQUE INDEX model_versions_model_version_uniq ON model_versions (model_id, version);

-- THE SINGLE-PRODUCTION INVARIANT — at most ONE PRODUCTION version per model.
-- A PARTIAL UNIQUE INDEX (only over rows where stage = 3/PRODUCTION) is the DB-level
-- enforcement of the invariant the service guards in PromoteVersionTx. Even if two
-- promotions raced past the application check, this index would reject the second
-- commit with 23505 — the invariant holds under concurrency, not just under correct
-- code. THIS is what stops two production versions: the
-- service demotes-then-promotes in one tx, AND this index is the backstop.
CREATE UNIQUE INDEX model_versions_one_production_per_model
    ON model_versions (model_id)
    WHERE stage = 3;

-- Per-model CREATE-idempotency lookup (LookupVersionByIdempotencyKey), scoped to the
-- model so keys are namespaced per model. Partial + unique, same rationale as models.
CREATE UNIQUE INDEX model_versions_model_idempotency_uniq
    ON model_versions (model_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- ListVersions is per-model, newest-first, optionally stage-filtered, cursor-paginated
-- on (created_at,id). This composite serves both the unfiltered and the stage-filtered
-- page (stage is an equality the planner can apply atop the range).
CREATE INDEX model_versions_model_created_idx
    ON model_versions (model_id, created_at DESC, id DESC);

-- ----------------------------------------------------------------------------
-- command_idempotency — the generic MUTATION-idempotency ledger.
-- ----------------------------------------------------------------------------
--
-- WHY THIS TABLE EXISTS (the centerpiece of idempotency, from ports.go):
--
--   RegisterModel/CreateVersion are CREATES — their idempotency key hangs on the
--   created row (the inline idempotency_key columns above). But the three MUTATION
--   commands the proto also promises key-dedup for — MarkVersionReady, PromoteVersion,
--   ArchiveModel — do NOT create a row, so there is no row to carry the key. They
--   record it HERE: a generic (team, command, key) → entity_id ledger.
--
--   The service checks this FIRST (LookupCommandIdempotency): on a hit it returns the
--   referenced entity's CURRENT state with NO re-write and NO re-emit — so a duplicate
--   webhook can't double-fire ModelVersionReady or double-meter billing (the exactly-
--   once side-effect guarantee state-based idempotency alone cannot give).
--
--   ONE generic table serves all three commands with no per-command schema churn:
--   `command` (a SMALLINT mirroring domain.Command) namespaces the key so a promote
--   key and an archive key that happen to be equal never collide.
--
-- PRIMARY KEY (team, command, key): the natural key IS the dedup key. Writing it as
-- INSERT ... ON CONFLICT DO NOTHING (the adapter) makes the first writer win a race;
-- a second writer for the SAME (team,command,key) but a DIFFERENT entity is the genuine
-- key-reuse race the adapter surfaces as ErrWriteConflict (RecordCommandIdempotency's
-- contract). In production this row is written in the SAME tx as the mutation so the
-- key and the side effect commit atomically.
CREATE TABLE command_idempotency (
    team       TEXT         NOT NULL,
    command    SMALLINT     NOT NULL,
    key        TEXT         NOT NULL,
    entity_id  TEXT         NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (team, command, key)
);
