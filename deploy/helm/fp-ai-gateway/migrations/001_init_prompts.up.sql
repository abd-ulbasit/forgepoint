-- 001_init_prompts.up.sql — the PROMPT REGISTRY schema (M7/L3) for the AI Gateway.
--
-- ============================================================================
-- WHAT THIS IS: a versioned, team-scoped prompt template store.
-- ============================================================================
--
-- The AI Gateway's CORE (ChatCompletion, budgets, breakers) is Redis-only — it
-- owns no Postgres. The PROMPT REGISTRY is the ONE durable, relational concern
-- the gateway grows, so it gets its OWN small Postgres database (database-per-
-- service still holds: this DB belongs to ai-gateway alone). The shape is a
-- direct mirror of the Model Registry's versioned, per-team-unique aggregate —
-- a prompt is to the AI Gateway what a model is to the Registry.
--
-- ONE TABLE — `prompts`. Unlike the Model Registry (models + model_versions as
-- separate aggregate-root/child tables), a prompt version IS the unit: each row
-- is a complete, immutable (template, variables, stage) snapshot at a version
-- number. We do NOT split "prompt" from "prompt_version" because a prompt has no
-- mutable header fields that live independently of a version — the name+team is
-- the identity, and every create cuts a NEW row (a new version). This is the
-- append-mostly, "newest version wins" model MLflow's prompt registry uses.
--
-- DESIGN PRINCIPLE (same as the Registry): the DB enforces the invariants the
-- domain ALSO enforces. The per-(team,name) version uniqueness is guarded in the
-- service (it reads MAX(version)+1 inside a tx) AND by a UNIQUE index here — the
-- index is the last line no concurrent race can slip past.

-- ----------------------------------------------------------------------------
-- prompts — a versioned, team-owned prompt template.
-- ----------------------------------------------------------------------------
--
-- WHY each column (mapping domain.Prompt → row):
--   id              server-minted opaque id (UUIDv4 in prod, a deterministic
--                   sequence in tests). TEXT, not uuid — the domain treats the id
--                   as an OPAQUE string from its IDGenerator port, exactly as the
--                   Model Registry does; a `uuid` column would reject the test ids
--                   and couple storage to an id-format decision the domain owns.
--   name            human handle, UNIQUE WITHIN A TEAM per version (the
--                   (team,name,version) index below) — not globally, so two teams
--                   may both own a "summarizer" prompt.
--   version         MONOTONIC per (team,name): version 1 for a brand-new name,
--                   N+1 for an existing name. INTEGER (the proto carries int32).
--                   The newest version is the "current" one for that name.
--   stage           the lifecycle stage as a SMALLINT mirroring the domain enum
--                   (StageDev=1, StageProduction=2, StageArchived=3), matching the
--                   proto's PromptStage numeric values byte-for-byte so the mapping
--                   is a trivial cast. A CHECK keeps a bad value out at the boundary.
--                   A new version is born DEV (the service stamps it).
--   template        the prompt body with {{variable}} placeholders. TEXT.
--   variables       the DECLARED placeholders parsed out of the template at create
--                   time (the distinct {{name}} tokens). TEXT[] — a Postgres array,
--                   the natural fit for "a list of strings" we never query INTO,
--                   only read back whole. pgx round-trips a Go []string ↔ text[]
--                   natively, so no JSON envelope is needed (contrast the Registry's
--                   tags, which are a string→string MAP and therefore JSONB).
--   description     free-form human note. TEXT.
--   team            the TENANCY BOUNDARY, stamped from the caller's auth claims
--                   (server-authoritative) — on EVERY row so every read is team-
--                   scopable with a WHERE team = $1 and NO cross-tenant leak.
--   idempotency_key the CREATE-idempotency key recorded INLINE on the row. A
--                   retried CreatePrompt with the same key finds THIS row (via the
--                   per-team idempotency lookup) and returns its version instead of
--                   cutting a duplicate version. NULL = the caller opted out of dedup.
--   created_at      creation timestamp (audit + the list sort key).
CREATE TABLE prompts (
    id              TEXT         PRIMARY KEY,
    name            TEXT         NOT NULL,
    version         INTEGER      NOT NULL,
    stage           SMALLINT     NOT NULL,
    template        TEXT         NOT NULL,
    variables       TEXT[]       NOT NULL DEFAULT '{}',
    description     TEXT         NOT NULL DEFAULT '',
    team            TEXT         NOT NULL,
    idempotency_key TEXT,        -- NULL when the caller supplied no key
    created_at      TIMESTAMPTZ  NOT NULL,
    -- stage ∈ {DEV, PRODUCTION, ARCHIVED} (1..3); never persisted as UNSPECIFIED (0).
    CONSTRAINT prompts_stage_chk CHECK (stage BETWEEN 1 AND 3)
);

-- THE (team, name, version) UNIQUENESS INVARIANT — the heart of versioning.
-- A version number is unique within a (team, name). This is what makes the
-- create RACE-SAFE: two concurrent CreatePrompt("summarizer") for the same team
-- both compute "next = MAX+1" and try to INSERT the same version; one wins, the
-- other hits SQLSTATE 23505 (unique_violation) → domain.ErrWriteConflict → the
-- service retries with the recomputed next version (or, on a same-key retry, the
-- idempotency path already returned the original). A PARTIAL index is unnecessary
-- here (version is never NULL), so this is a plain composite UNIQUE index.
CREATE UNIQUE INDEX prompts_team_name_version_uniq ON prompts (team, name, version);

-- CREATE-idempotency lookup index. Partial (WHERE idempotency_key IS NOT NULL) so
-- the many NULL-key rows don't bloat it, and UNIQUE on (team, idempotency_key) so
-- the same key can't map to two different prompt rows within a team — the
-- structural guard behind LookupByIdempotencyKey returning exactly one row.
CREATE UNIQUE INDEX prompts_team_idempotency_uniq
    ON prompts (team, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- NEWEST-VERSION-PER-NAME lookup. GetPrompt's "latest" path and the service's
-- "next version = MAX(version)+1" both want the highest version for a (team,name)
-- fast. (team, name, version DESC) makes that a one-row index seek, not a scan+sort.
CREATE INDEX prompts_team_name_version_desc_idx ON prompts (team, name, version DESC);

-- LIST is team-scoped, NEWEST-FIRST, keyset-paginated on (created_at, id). This
-- composite serves that page as a backward index range scan. The id tiebreaker
-- makes the cursor TOTAL (two rows with the same created_at still order
-- deterministically) — essential for stable keyset pagination with no gaps/overlaps,
-- mirroring the Model Registry's (team, created_at DESC, id DESC) list index.
CREATE INDEX prompts_team_created_idx ON prompts (team, created_at DESC, id DESC);
