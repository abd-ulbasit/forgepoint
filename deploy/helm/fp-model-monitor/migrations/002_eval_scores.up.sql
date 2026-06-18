-- 002_eval_scores.up.sql — M7/L4: the LLM QUALITY-EVALUATION score log.
--
-- ============================================================================
-- WHAT THIS TABLE IS FOR
-- ============================================================================
--
-- eval_scores is the durable log of LLM-as-judge quality evaluations. One row per
-- JUDGED (sampled) completion: the three judged axes (relevance / coherence /
-- safety), the derived overall, the team + model it belongs to, the served
-- completion's request_id, and when it was judged.
--
-- It is the "window" of the L4 quality-drift loop the way drift_reports' source
-- windows are for tabular drift: the monitor reads back the rolling average of
-- recent OVERALL scores per (team, model) and raises a quality-drift report (a
-- DriftTypePerformance report named "llm_quality") when that average falls below a
-- floor or regresses from a baseline — feeding the SAME alert + retrain loop as
-- data/prediction/performance drift. No new event, no new proto: this table is the
-- only new persistent state L4 adds.
--
-- ----------------------------------------------------------------------------
-- THE LOAD-BEARING INVARIANTS (mirrors drift_reports' discipline)
-- ----------------------------------------------------------------------------
--
-- 1) TENANCY KEY = (team, model), NOT model alone. A model name is not globally
--    unique — team-a/"chatbot" and team-b/"chatbot" are different models. Every
--    read (the rolling average) filters on (team, model) TOGETHER, so one team's
--    quality scores can never bleed into another team's average / drift verdict.
--    `team` is on every row and is NOT NULL/empty.
--
-- 2) IDEMPOTENCY KEY = request_id (UNIQUE). The completion's gateway-minted
--    request_id is the natural dedup key: a redelivered fp.ai.completion.served
--    event must not record the SAME completion twice (which would double-count it
--    in the rolling average and could nudge a false drift). The repository's
--    Record does INSERT … ON CONFLICT (request_id) DO NOTHING, so a redelivery is
--    a no-op. request_id is server-authoritative (the gateway stamps it), never a
--    user id — safe to key on.
--
-- ----------------------------------------------------------------------------
-- DESIGN CHOICES (consistent with 001_init)
-- ----------------------------------------------------------------------------
--
-- IDs/keys are TEXT (the request_id is the gateway's UUID string) — a pass-through
-- with no boundary translation, same as the monitors/drift_reports tables.
--
-- Scores are DOUBLE PRECISION on the 1–5 scale. `scored` distinguishes a JUDGED
-- row (overall is meaningful) from an UNSCORED one (the judge failed / its output
-- couldn't be parsed). Unscored rows are STILL stored (so the volume of
-- un-judgeable traffic is observable) but EXCLUDED from the rolling average by the
-- repository (WHERE scored = TRUE) — a parse failure must never read as "low
-- quality" and trip a false retrain. A CHECK pins the scored axes into [1,5];
-- unscored rows carry 0s (the CHECK allows 0 only when scored=false).
CREATE TABLE eval_scores (
    request_id  TEXT             PRIMARY KEY,                 -- IDEMPOTENCY KEY (gateway-minted)
    team        TEXT             NOT NULL CHECK (team <> ''), -- TENANCY KEY
    model       TEXT             NOT NULL CHECK (model <> ''),

    relevance   DOUBLE PRECISION NOT NULL DEFAULT 0,
    coherence   DOUBLE PRECISION NOT NULL DEFAULT 0,
    safety      DOUBLE PRECISION NOT NULL DEFAULT 0,
    overall     DOUBLE PRECISION NOT NULL DEFAULT 0,
    scored      BOOLEAN          NOT NULL DEFAULT FALSE,

    created_at  TIMESTAMPTZ      NOT NULL,

    -- A SCORED row must carry sane 1–5 axes; an UNSCORED row carries 0s. This keeps a
    -- garbage/hallucinated score out of the table entirely (the parser clamps, but the
    -- CHECK is the last line of defense at the storage boundary).
    CONSTRAINT eval_scores_range CHECK (
        (scored = FALSE)
        OR (relevance BETWEEN 1 AND 5
            AND coherence BETWEEN 1 AND 5
            AND safety BETWEEN 1 AND 5
            AND overall BETWEEN 1 AND 5)
    )
);

-- The rolling-average read scans the most-recent SCORED rows for a (team, model),
-- newest-first. (team, model, created_at DESC) backs that ORDER BY … LIMIT N as an
-- index-only-ish range scan; the partial predicate keeps the index to the rows the
-- query actually reads (scored only).
CREATE INDEX eval_scores_team_model_created_idx
    ON eval_scores (team, model, created_at DESC)
    WHERE scored = TRUE;
