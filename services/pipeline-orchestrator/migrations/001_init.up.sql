-- 001_init.up.sql — the Pipeline Orchestrator's durable schema.
--
-- ============================================================================
-- WHAT THIS SCHEMA IS FOR: DURABLE SAGA / DAG STATE (the whole point)
-- ============================================================================
--
-- The orchestrator is a workflow engine. Its single most important property is
-- DURABILITY: every step transition is written to disk BEFORE the side effect
-- runs (write-ahead), so a crashed orchestrator can reload the last persisted
-- state and RESUME — never re-applying a side effect it already completed, never
-- losing track of a step it must still compensate. This schema is that durable
-- log. Four concerns, four (+2 dedup) tables:
--
--   pipelines              the TEMPLATE   (the "program": author once, run many)
--   pipeline_idempotency   Create dedup   (idempotency-key → pipeline_id)
--   executions             the INSTANCE   (one run; the COARSE saga state machine)
--   execution_idempotency  Trigger dedup  (idempotency-key → execution_id)
--   step_executions        the CHECKPOINTS(the FINE-grained write-ahead log —
--                                          the rows crash recovery actually reads)
--
-- TEMPLATE vs INSTANCE is the Airflow DAG/DagRun (or Temporal Workflow/
-- WorkflowExecution) split: a pipeline is edited rarely; an execution is created
-- on every trigger and carries the live state. We store the step GRAPH on the
-- template and a step-RUN row per execution.
--
-- WHY two state-bearing tables (executions + step_executions) and not one:
-- the saga has two granularities of state. executions.status is the COARSE
-- machine (PENDING→RUNNING→COMPENSATING→FAILED/CANCELLED/COMPLETED). Each
-- step_executions.status is the FINE machine (PENDING→RUNNING→COMPLETED/FAILED/
-- SKIPPED, and the compensation sub-states COMPENSATING→COMPENSATED/
-- COMPENSATION_FAILED). The engine persists a step transition individually so a
-- crash between steps loses at most the in-flight step, not the whole run. That
-- maps exactly to the domain's ExecutionRepository.Save (coarse) vs SaveStep
-- (fine) split — see services/.../domain/ports.go.
--
-- WHY JSONB for steps/config/output/input: their shape varies per StepType and
-- per CUSTOM step (canary %, hyperparams, model_uri, ...). A rigid column-per-
-- field schema would force a migration for every new step kind. JSONB keeps the
-- engine generic while still being queryable/indexable if we ever need it. The
-- domain models these as map[string]any; the adapter marshals to/from JSONB.
--
-- WHY enums are stored as SMALLINT (the domain's integer enum values), not
-- Postgres ENUM types or text: the domain owns the enum vocabulary (models.go),
-- and its integer values are deliberately kept aligned with the proto. Storing
-- the int keeps the adapter a trivial cast and avoids a CREATE TYPE migration
-- every time the domain adds a status. The CHECK constraints below pin the legal
-- range so a bad value can't be written even though the column is a plain int.
-- (Tradeoff: a raw int is less self-describing in psql than a text label; we
-- accept that for the zero-migration enum evolution and the trivial mapping.)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- pipelines — the TEMPLATE table.
-- ----------------------------------------------------------------------------
CREATE TABLE pipelines (
    -- Server-assigned UUIDv4 (the domain's IDGenerator). TEXT not native uuid so
    -- the adapter never has to translate the type at the boundary — the domain
    -- carries ids as strings everywhere. (uuid would save a few bytes; string
    -- keeps the adapter dead simple and the id format owned by the domain.)
    id            TEXT PRIMARY KEY,

    -- Human-readable label, unique PER TEAM (a team can't have two "deploy-prod"
    -- templates). Enforced by the partial unique index below (archived rows are
    -- excluded so an archived name can be reused).
    name          TEXT NOT NULL,

    -- PipelineType (saga / DAG / batch). SMALLINT = the domain enum's int value.
    -- 1..3 are the known non-zero values; 0 (Unspecified) is rejected by the
    -- service before it ever reaches here, but the CHECK pins the range anyway.
    type          SMALLINT NOT NULL CHECK (type BETWEEN 1 AND 3),

    -- The step GRAPH (StepDefinition[]) as JSONB. This is the immutable program
    -- a run instantiates. Stored whole (not split into a step_definitions table)
    -- because a template's steps are only ever read/written as a unit and the
    -- engine validates the graph before persisting — there is no per-step query
    -- need on the template. NOT NULL: the service rejects empty pipelines.
    steps         JSONB NOT NULL,

    -- SERVER-AUTHORITATIVE provenance: the authenticated creator and owning team
    -- come from auth claims in the service, NEVER from client input (anti
    -- mass-assignment / identity spoofing). The adapter just persists what the
    -- service set.
    created_by    TEXT NOT NULL,
    team          TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL,

    -- Soft-delete flag. WHY soft-delete and not DELETE: past executions reference
    -- this pipeline id for audit/lineage (the run's pipeline name must still
    -- resolve). Archiving hides the template from List/Trigger while preserving
    -- the foreign anchor. The repository sets this via Archive().
    archived      BOOLEAN NOT NULL DEFAULT FALSE
);

-- Name uniqueness PER TEAM, but only among LIVE (non-archived) templates. A
-- partial unique index is exactly the right tool: it lets a team archive
-- "deploy-prod" and create a fresh "deploy-prod" (the archived row no longer
-- participates in the constraint) while still preventing two live duplicates.
CREATE UNIQUE INDEX pipelines_team_name_live_uniq
    ON pipelines (team, name)
    WHERE NOT archived;

-- List is "this team's live pipelines, newest first, keyset-paginated by
-- (created_at, id)". This composite index serves both the team+archived filter
-- and the DESC keyset order in one structure. id is the tiebreaker that makes
-- the cursor total-ordered even when two rows share created_at.
CREATE INDEX pipelines_team_created_idx
    ON pipelines (team, created_at DESC, id DESC)
    WHERE NOT archived;

-- ----------------------------------------------------------------------------
-- pipeline_idempotency — Create dedup (the Stripe idempotency-key pattern).
-- ----------------------------------------------------------------------------
-- A retried CreatePipeline with the same key must return the ORIGINAL pipeline,
-- never a duplicate. We store key → pipeline_id in its own table (scoped by team
-- so two teams' keys never collide) and look it up first on Create. WHY a
-- separate table and not a column on pipelines: the key is an idempotency
-- concern, not a property of the template; keeping it separate means a template
-- with no key (key = "") simply has no row here, and the lookup is a clean PK
-- hit. The FK cascades so archiving never strands a key (and a future hard-purge
-- of a pipeline cleans its key with it).
CREATE TABLE pipeline_idempotency (
    team           TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    pipeline_id    TEXT NOT NULL REFERENCES pipelines (id) ON DELETE CASCADE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (team, idempotency_key)
);

-- ----------------------------------------------------------------------------
-- executions — the INSTANCE table: one row per run, the COARSE saga state.
-- ----------------------------------------------------------------------------
CREATE TABLE executions (
    id            TEXT PRIMARY KEY,

    -- The template this run instantiates. FK (no cascade-delete: we soft-delete
    -- templates, never hard-delete, so this anchor always resolves). RESTRICT
    -- makes the soft-delete discipline a hard guarantee — you physically cannot
    -- DELETE a pipeline that has executions, so lineage can't be silently broken.
    pipeline_id   TEXT NOT NULL REFERENCES pipelines (id) ON DELETE RESTRICT,

    -- ExecutionStatus — THE coarse saga state machine. 1..6 = PENDING, RUNNING,
    -- COMPENSATING, COMPLETED, FAILED, CANCELLED (the domain's iota order). The
    -- CHECK pins the legal range; the engine drives the transitions.
    status        SMALLINT NOT NULL CHECK (status BETWEEN 1 AND 6),

    -- Coarse "where are we" pointer: the template step id currently/last running.
    -- Nullable because a brand-new PENDING run hasn't entered a step yet.
    current_step  TEXT,

    -- SERVER-AUTHORITATIVE: who/what triggered the run (user id or service id).
    triggered_by  TEXT NOT NULL,

    -- Run timeline. started_at is stamped at trigger; completed_at is NULL until
    -- the run reaches a terminal state (the IS NULL / IS NOT NULL of this column
    -- is a cheap "is this run still in flight?" predicate for a recovery scan).
    started_at    TIMESTAMPTZ NOT NULL,
    completed_at  TIMESTAMPTZ,

    -- The trigger input, echoed for traceability (no secrets — it is returned by
    -- GetExecution). JSONB for the same shape-varies reason as steps/config.
    -- Nullable: a trigger may carry no input.
    input         JSONB,

    -- Human-readable failure reason when FAILED (triage/audit, not end-user
    -- display). Empty/NULL on success.
    error         TEXT
);

-- ListExecutions is "this team's runs, optionally filtered by pipeline and/or
-- status, newest first, keyset-paginated". Team is NOT a column on executions
-- (tenancy is a property of the parent pipeline), so the list query JOINs
-- pipelines and filters on pipelines.team — see the adapter. These indexes serve
-- the common access paths: by pipeline (+ keyset order) and by status.
CREATE INDEX executions_pipeline_started_idx
    ON executions (pipeline_id, started_at DESC, id DESC);
CREATE INDEX executions_status_idx
    ON executions (status);

-- ----------------------------------------------------------------------------
-- execution_idempotency — Trigger dedup (exactly-once trigger from the caller's
-- view: a retried TriggerExecution never double-runs a deployment saga).
-- ----------------------------------------------------------------------------
CREATE TABLE execution_idempotency (
    team            TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    execution_id    TEXT NOT NULL REFERENCES executions (id) ON DELETE CASCADE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (team, idempotency_key)
);

-- ----------------------------------------------------------------------------
-- step_executions — the FINE-grained, write-ahead CHECKPOINT log.
-- ----------------------------------------------------------------------------
-- This is the durability backbone: one row per step-run, updated on every
-- transition. On crash recovery the engine reads these to know which steps
-- already COMPLETED (don't re-run, but DO compensate on rollback) and which were
-- only PENDING (safe to (re)start). Without these rows, recovery is blind.
CREATE TABLE step_executions (
    -- UUIDv4 of THIS step-run, distinct from step_id (the template step id).
    id            TEXT PRIMARY KEY,

    -- The run this step belongs to. CASCADE: a step-run has no meaning without
    -- its execution, so purging an execution (if ever) takes its steps with it.
    execution_id  TEXT NOT NULL REFERENCES executions (id) ON DELETE CASCADE,

    -- The StepDefinition.id (template id) this run corresponds to. Unique WITHIN
    -- an execution (one runtime row per template step per run) — enforced below.
    step_id       TEXT NOT NULL,

    -- StepType, denormalized from the template so routing/events/compensation
    -- don't need a template lookup. 1..9 = the domain's known step types.
    step_type     SMALLINT NOT NULL CHECK (step_type BETWEEN 1 AND 9),

    -- StepStatus — the FINE state machine incl. compensation sub-states. 1..8 =
    -- PENDING, RUNNING, COMPLETED, FAILED, SKIPPED, COMPENSATING, COMPENSATED,
    -- COMPENSATION_FAILED (the domain's iota order). COMPENSATION_FAILED (8) is
    -- the "stuck saga" the engine surfaces loudly.
    status        SMALLINT NOT NULL CHECK (status BETWEEN 1 AND 8),

    -- started_at: stamped on the first RUNNING transition (NULL while PENDING).
    -- completed_at: stamped on reaching a terminal/sub-terminal state. The
    -- compensator orders completed steps by completed_at ASC and undoes them in
    -- REVERSE — so this timestamp is load-bearing for rollback correctness, not
    -- just display. (See Execution.completedStepsInOrder in models.go.)
    started_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ,

    -- The step's result, fed to dependent steps as UpstreamOutputs (DAG data
    -- flow) and echoed in StepCompleted events. JSONB; no secrets. Nullable
    -- until the step completes.
    output        JSONB,

    -- Failure reason when FAILED / COMPENSATION_FAILED. Empty on success.
    error         TEXT,

    -- How many times this step has been tried (>= 1 once it ran). Surfaces retry
    -- behavior to operators ("succeeded on attempt 3").
    attempt       INTEGER NOT NULL DEFAULT 0
);

-- One runtime row per (execution, template step). This UNIQUE constraint is what
-- makes SaveStep's UPSERT correct: the engine creates the PENDING checkpoint rows
-- at trigger time (inside the same tx as the execution), then UPDATEs the SAME
-- row on each transition. The constraint guarantees there is exactly one row to
-- update and prevents a duplicate checkpoint sneaking in.
CREATE UNIQUE INDEX step_executions_exec_step_uniq
    ON step_executions (execution_id, step_id);

-- GetByID loads an execution's steps ordered for replay; this index serves the
-- "all steps of this execution, in completion order" read the engine and
-- GetExecution both do.
CREATE INDEX step_executions_exec_completed_idx
    ON step_executions (execution_id, completed_at);
