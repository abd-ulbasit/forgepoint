-- 001_init.up.sql — the billing service's own Postgres schema (database-per-service).
--
-- ============================================================================
-- WHAT THIS SCHEMA ENCODES (and the patterns it makes durable)
-- ============================================================================
--
-- Four tables, each mapping 1:1 to a domain concern:
--
--   rate_plans      The SERVER-AUTHORITATIVE price source. A client never sets a
--                   price; pricing math reads from here. Prices/allowances/quotas
--                   are JSONB maps (meter -> value) so adding a meter needs no
--                   schema change (the domain's RatePlan.UnitPrices is a map).
--
--   usage_records   The append-only LEDGER. One row per metered, priced fact. We
--                   INSERT, never UPDATE — invoices are aggregations over these.
--                   The (team, idempotency_key) UNIQUE index is the DB-side half
--                   of exactly-once-in-effect (the service's dedup is the other).
--
--   invoices        Server-built bills (a period rollup). Line items are JSONB
--                   (a small, read-mostly document that always loads with the
--                   invoice — no separate child table / join needed).
--
--   outbox          THE TRANSACTIONAL OUTBOX. Every business write that must emit
--                   an event inserts the event row HERE in the SAME tx as the
--                   business row. A separate relay (events phase) publishes
--                   unpublished rows to NATS and stamps published_at. This is how
--                   we get atomic "write + publish-intent" WITHOUT a distributed
--                   transaction across Postgres and NATS.
--
-- Every monetary amount is stored as BIGINT micro-units (1e-6 of the currency's
-- major unit) + a separate CHAR(3) currency code — never a float. This mirrors
-- the domain's Money{AmountMicros int64; CurrencyCode string}: integer money is
-- EXACT (0.10 + 0.20 == 0.30), which a money service must guarantee.
--
-- All timestamps are TIMESTAMPTZ (UTC). The domain truncates to microseconds
-- before storing because Postgres TIMESTAMPTZ has microsecond resolution — so a
-- read-back equals what was written (Go's time.Time has nanoseconds; an un-
-- truncated nanosecond value would fail an exact-equality assertion after round
-- trip). Tests truncate their fixtures to match.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- rate_plans — the pricing catalog (server-authoritative price source).
-- ----------------------------------------------------------------------------
CREATE TABLE rate_plans (
    id          UUID PRIMARY KEY,
    name        TEXT NOT NULL,

    -- The three meter->value maps as JSONB. WHY JSONB (not a child rate_plan_prices
    -- table): a plan's price map is small, always read whole (you never query "all
    -- plans priced at $X for tokens"), and the domain models it AS a map. A JSONB
    -- column keeps the read a single row fetch and lets a new meter be priced with
    -- no migration. Keys are the MeterType strings ("INFERENCE_TOKENS"); price
    -- values are {"micros": <int>, "currency": "USD"} objects (see mapping.go).
    unit_prices         JSONB NOT NULL,
    included_quantities JSONB NOT NULL DEFAULT '{}'::jsonb, -- meter -> free allowance (int)
    quota_limits        JSONB NOT NULL DEFAULT '{}'::jsonb, -- meter -> hard cap (int)

    created_at  TIMESTAMPTZ NOT NULL
);

-- Idempotency for CreateRatePlan: an admin tool that retries with the same key
-- must NOT create a duplicate priced plan. A separate mapping table (rather than
-- a column on rate_plans) keeps the key out of the business row and lets the key
-- be NULL-free here (only keyed creations get a row). The UNIQUE(idempotency_key)
-- is what makes a concurrent double-create resolve to one winner: the loser hits
-- 23505 (unique_violation) and re-reads the winner's plan.
CREATE TABLE rate_plan_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    rate_plan_id    UUID NOT NULL REFERENCES rate_plans(id) ON DELETE CASCADE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- team -> plan assignment. ResolvePlanForTeam reads this to find the plan that
-- prices a team's usage (the client never names a plan). One active plan per team
-- (team is the PK) — reassigning a team's plan is an UPSERT. Kept as its own
-- table (not a column on some teams table billing doesn't own) because billing
-- owns the team->plan binding for PRICING; team identity itself lives in Auth.
CREATE TABLE team_rate_plans (
    team         TEXT PRIMARY KEY,
    rate_plan_id UUID NOT NULL REFERENCES rate_plans(id) ON DELETE RESTRICT,
    assigned_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ----------------------------------------------------------------------------
-- usage_records — the append-only priced ledger.
-- ----------------------------------------------------------------------------
CREATE TABLE usage_records (
    id            UUID PRIMARY KEY,
    team          TEXT NOT NULL,
    rate_plan_id  UUID NOT NULL,           -- pinned at write time (reproducible pricing)
    meter_type    TEXT NOT NULL,           -- the MeterType string
    quantity      BIGINT NOT NULL,         -- BILLED units (after free allowance)

    cost_micros   BIGINT NOT NULL,         -- server-computed = quantity * unit price
    currency_code CHAR(3) NOT NULL,        -- ISO-4217, travels with the amount

    model_id          TEXT NOT NULL DEFAULT '',
    model_version     TEXT NOT NULL DEFAULT '',
    source_request_id TEXT NOT NULL DEFAULT '',
    idempotency_key   TEXT,                -- client-supplied; NULL when absent

    occurred_at   TIMESTAMPTZ NOT NULL,    -- event time (server-clamped)
    created_at    TIMESTAMPTZ NOT NULL     -- insert time (server-stamped)
);

-- THE idempotency guard, DB-enforced. A PARTIAL UNIQUE index on
-- (team, idempotency_key) WHERE idempotency_key IS NOT NULL means:
--   * two records with the same (team, key) cannot both exist — a concurrent
--     double-write resolves to one winner (the loser gets 23505 → ErrRepoDuplicate
--     → the service re-reads the winner), and
--   * records WITHOUT a key (idempotency_key IS NULL) are exempt — many keyless
--     records can coexist (the partial WHERE excludes NULLs from the constraint).
-- Scoped by team because client keys are only unique within a tenant.
CREATE UNIQUE INDEX usage_records_team_idem_key_uq
    ON usage_records (team, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- The hot read path is CurrentPeriodUsage / AggregateUsageForPeriod:
-- "SUM(quantity) WHERE team=? AND meter_type=? AND occurred_at IN [start,end)".
-- This composite index covers that predicate ordering (team, meter, time) so the
-- period rollups are an index range scan, not a full table sweep of the ledger.
CREATE INDEX usage_records_team_meter_time_idx
    ON usage_records (team, meter_type, occurred_at);

-- ----------------------------------------------------------------------------
-- invoices — server-built period bills.
-- ----------------------------------------------------------------------------
CREATE TABLE invoices (
    id             UUID PRIMARY KEY,
    invoice_number TEXT NOT NULL UNIQUE,   -- human-facing "INV-2026-..."; unique by contract
    team           TEXT NOT NULL,
    rate_plan_id   UUID NOT NULL,
    status         TEXT NOT NULL,          -- InvoiceStatus string (DRAFT/FINALIZED/...)

    period_start   TIMESTAMPTZ NOT NULL,   -- inclusive
    period_end     TIMESTAMPTZ NOT NULL,   -- exclusive

    -- Line items as a JSONB array. They are immutable once finalized, always read
    -- with the invoice, and never queried independently — a JSONB document is the
    -- right shape (no child table / join). Each element carries meter, qty, unit
    -- price (micros+currency), and amount (micros+currency).
    line_items     JSONB NOT NULL DEFAULT '[]'::jsonb,

    total_micros   BIGINT NOT NULL,        -- Σ line items, rounded to cents at finalize
    currency_code  CHAR(3) NOT NULL,

    created_at     TIMESTAMPTZ NOT NULL,
    finalized_at   TIMESTAMPTZ,            -- NULL while DRAFT
    due_at         TIMESTAMPTZ             -- set on finalization
);

-- ListInvoices returns a team's invoices newest-first, keyset-paginated on
-- (created_at, id). This index serves that ORDER BY within the team scope.
CREATE INDEX invoices_team_created_idx
    ON invoices (team, created_at DESC, id DESC);

-- ----------------------------------------------------------------------------
-- outbox — the transactional outbox (the pattern's durable spine).
-- ----------------------------------------------------------------------------
--
-- An outbox row is a PUBLISH INTENT committed in the SAME tx as the business
-- write it describes. The relay (events phase) does:
--   SELECT ... WHERE published_at IS NULL ORDER BY created_at  -- claim unpublished
--   <publish to NATS>
--   UPDATE outbox SET published_at = now() WHERE id = ...      -- mark done
-- so an event publishes AT LEAST ONCE (a crash between publish and mark re-
-- publishes; consumers dedupe on id). The row is NEVER deleted by the writer —
-- only the relay marks it; a retention job prunes old published rows later.
CREATE TABLE outbox (
    id           UUID PRIMARY KEY,        -- becomes EventEnvelope.id (consumer dedupe key)
    aggregate_id UUID NOT NULL,           -- the business entity id (usage record / invoice)
    event_type   TEXT NOT NULL,           -- "fp.billing.usage.recorded", etc. (NATS subject)
    payload      JSONB NOT NULL,          -- the flattened domain payload (mapping.go marshals it)
    created_at   TIMESTAMPTZ NOT NULL,    -- == the business write time
    published_at TIMESTAMPTZ              -- NULL until the relay publishes; the relay's WHERE key
);

-- The relay's claim query is "unpublished rows, oldest first". A PARTIAL index on
-- the unpublished set keeps that scan tiny (it only indexes rows still needing
-- work — published rows drop out of the index), and ordering by created_at gives
-- the relay a stable FIFO-ish drain order.
CREATE INDEX outbox_unpublished_idx
    ON outbox (created_at)
    WHERE published_at IS NULL;
