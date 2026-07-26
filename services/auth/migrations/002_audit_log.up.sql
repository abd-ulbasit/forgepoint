-- 002_audit_log.up.sql — the TAMPER-EVIDENT, APPEND-ONLY audit log.
--
-- ============================================================================
-- WHY THE AUTH SERVICE HOSTS THE AUDIT LOG
-- ============================================================================
-- The audit trail is the platform's security record of WHO did WHAT. The IAM
-- service (auth) is the natural owner: it already owns identity, it is the trust
-- root, and database-per-service means exactly one service persists the log.
-- Every OTHER service only EMITS audit events (one-line interceptor → NATS); the
-- auth-hosted consumer appends them all here. See docs/adr/0009.
--
-- ============================================================================
-- APPEND-ONLY, ENFORCED AT THE DATABASE (defense in depth) — AND ITS LIMITS
-- ============================================================================
-- An audit log is only trustworthy if history cannot be rewritten. We enforce
-- append-only in layers, and we are HONEST about exactly what each layer buys:
--
--   1. The repository only ever INSERTs (never UPDATE/DELETE/TRUNCATE).
--   2. Triggers that RAISE on row mutation AND on TRUNCATE (below):
--        - BEFORE UPDATE OR DELETE ... FOR EACH ROW  → blocks per-row tampering.
--        - BEFORE TRUNCATE ... FOR EACH STATEMENT    → blocks `TRUNCATE audit_log`
--          (a row-level trigger CANNOT fire on TRUNCATE, so TRUNCATE needs its own
--          statement-level trigger — without it, TRUNCATE silently erases the whole
--          chain and no row trigger ever fires).
--      These fire for EVERY caller, INCLUDING the table owner and a superuser — a
--      trigger is not a table ACL, so owner/superuser do not bypass it.
--   3. Least-privilege role hardening (the guarded block at the bottom): when a
--      dedicated non-owner application role exists, REVOKE UPDATE/DELETE/TRUNCATE
--      from it and leave it only INSERT+SELECT, so it cannot even attempt a
--      mutation, and — because it does NOT own the table — cannot DISABLE or DROP
--      the triggers either.
--
-- WHAT THIS DOES NOT BUY (read before claiming "immutable"):
--   The triggers stop UPDATE/DELETE/TRUNCATE, but an actor with DDL rights on this
--   table (the table OWNER, or a superuser) can still `ALTER TABLE audit_log
--   DISABLE TRIGGER ALL` / `DROP TRIGGER ...` and THEN mutate freely. So the
--   trigger only fails closed for a role that is NEITHER the owner NOR a superuser.
--   That is precisely why layer 3 matters: if the app connects as the table OWNER
--   (the default single-role dev/test setup, where POSTGRES_USER owns everything),
--   the trigger is bypassable by that very role and the append-only guarantee is
--   WEAKER than it looks. Run the app under a dedicated non-owner role (init-
--   postgres.sh provisions one in production) to make the trigger truly binding.
--
--   Even with all three layers, a sufficiently privileged attacker who can drop a
--   trigger can recompute the hash chain forward and forge a consistent history.
--   The hash chain keeps a SURGICAL edit DETECTABLE; closing the recompute-forward
--   / TRUNCATE-and-restart residual requires shipping the head hash OFF-BOX to a
--   store the DB role cannot reach (ADR 0009 follow-up — NOT yet implemented, so it
--   is not claimed here as in-scope protection).
--
-- This is the database-level complement to the cryptographic hash chain: the hash
-- chain makes tampering DETECTABLE; the triggers make the common mutation paths
-- (UPDATE/DELETE/TRUNCATE through a non-owner role) IMPOSSIBLE.
--
-- ============================================================================
-- THE HASH CHAIN COLUMNS (prev_hash / entry_hash / seq)
-- ============================================================================
-- Each row commits to all prior rows:
--   entry_hash = hex(sha256( prev_hash || canonicalJSON(record) ))   [pkg/audit]
-- seq is a strictly monotonic position; prev_hash is the previous row's
-- entry_hash (the GENESIS row chains onto the empty string). The repository
-- computes these UNDER A SINGLE-WRITER LOCK (pg_advisory_xact_lock) so concurrent
-- appenders cannot fork the chain. A verifier re-walks seq order and recomputes
-- the chain; the first mismatch pinpoints tampering.

-- pgcrypto/citext already exist from 001; nothing new to enable here.

-- ----------------------------------------------------------------------------
-- audit_log — one row per security-relevant action, hash-chained
-- ----------------------------------------------------------------------------
CREATE TABLE audit_log (
    -- id is a bigserial surface key. seq (below) is the chain position; we keep a
    -- separate bigserial id as the conventional PK so the chain logic owns seq
    -- explicitly rather than leaning on the serial's gap-prone allocation.
    id             BIGSERIAL    PRIMARY KEY,

    -- event_id is the NATS EventEnvelope.id — the IDEMPOTENCY KEY. A duplicate
    -- redelivery of the same audit event carries the same event_id; the UNIQUE
    -- constraint + ON CONFLICT DO NOTHING in the repo makes re-delivery a no-op,
    -- so a duplicate never double-appends (which would corrupt the chain).
    event_id       UUID         NOT NULL UNIQUE,

    -- seq is the strictly monotonic chain position (1, 2, 3, ...). It is assigned
    -- by the repository under the single-writer advisory lock, NOT by a sequence,
    -- so prev_hash/entry_hash/seq are always computed together and consistently.
    -- UNIQUE guards against a logic bug ever reusing a position.
    seq            BIGINT       NOT NULL UNIQUE,

    -- WHO (snapshot of the actor at action time — see pkg/audit.Actor for why we
    -- snapshot role/team rather than join to users at read time).
    actor_user_id  TEXT         NOT NULL,
    actor_email    TEXT         NOT NULL DEFAULT '',
    actor_team     TEXT         NOT NULL DEFAULT '',
    actor_role     TEXT         NOT NULL DEFAULT '',

    -- WHAT.
    action         TEXT         NOT NULL,              -- full gRPC method
    resource       TEXT         NOT NULL DEFAULT '',   -- best-effort target id
    decision       TEXT         NOT NULL,              -- ALLOW | DENY | ERROR
    grpc_code      TEXT         NOT NULL DEFAULT '',   -- resulting gRPC code
    err            TEXT         NOT NULL DEFAULT '',   -- sanitized status message
    correlation_id TEXT         NOT NULL DEFAULT '',

    -- WHEN.
    occurred_at    TIMESTAMPTZ  NOT NULL,              -- when the action happened
    recorded_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),-- when we persisted it

    -- TAMPER-EVIDENCE chain links.
    prev_hash      TEXT         NOT NULL,              -- '' for the genesis row
    entry_hash     TEXT         NOT NULL UNIQUE,       -- this row's chain hash

    -- decision is constrained to the known vocabulary so a malformed event can
    -- never quietly widen it. (A CHECK, not an enum, so adding a value later is a
    -- migration, not a type rebuild.)
    CONSTRAINT audit_log_decision_chk CHECK (decision IN ('ALLOW', 'DENY', 'ERROR'))
);

-- occurred_at index: the auditor's primary query is time-ranged ("everything on
-- 2026-06-18"). Without it that scan is sequential over the whole log.
CREATE INDEX idx_audit_log_occurred_at ON audit_log (occurred_at);

-- actor index: the second-most-common query is "everything user X did".
CREATE INDEX idx_audit_log_actor_user_id ON audit_log (actor_user_id);

-- ----------------------------------------------------------------------------
-- APPEND-ONLY ENFORCEMENT — reject UPDATE, DELETE, and TRUNCATE at the database
-- ----------------------------------------------------------------------------
-- The trigger function raises on ANY attempt to mutate the table. It is one
-- function reused by two triggers because BEFORE UPDATE/DELETE is a ROW-level
-- event while TRUNCATE is a STATEMENT-level event — a single trigger cannot cover
-- both timings. TG_OP tells us which operation was attempted for the message.
--
-- WHY a trigger AND a REVOKE (belt + braces), and the honest limit:
--   The trigger fails CLOSED for any role that is NEITHER the table owner NOR a
--   superuser — it cannot be bypassed by table privileges (a trigger is not an
--   ACL). But the table OWNER (and a superuser) can `DISABLE`/`DROP` the trigger
--   and then mutate, so the trigger ALONE is only as strong as "the writer is not
--   the owner". The REVOKE in the guarded block below is the complementary control
--   that strips even the ATTEMPT from a dedicated non-owner app role. Use BOTH; the
--   trigger is the in-schema guarantee that travels with the table, the REVOKE is
--   the privilege floor for the role that connects in production.
CREATE OR REPLACE FUNCTION audit_log_block_mutations()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'audit_log is append-only: % is not permitted', TG_OP
        USING ERRCODE = 'insufficient_privilege';
END;
$$ LANGUAGE plpgsql;

-- Row-level guard: blocks UPDATE and DELETE of individual rows.
CREATE TRIGGER audit_log_no_update_delete
    BEFORE UPDATE OR DELETE ON audit_log
    FOR EACH ROW
    EXECUTE FUNCTION audit_log_block_mutations();

-- Statement-level guard: blocks TRUNCATE, which a row-level trigger CANNOT see.
-- Without this, `TRUNCATE audit_log` erases the ENTIRE chain and the row trigger
-- never fires — the single most damaging gap in a row-only append-only design.
-- FOR EACH STATEMENT is mandatory here (TRUNCATE has no per-row context).
CREATE TRIGGER audit_log_no_truncate
    BEFORE TRUNCATE ON audit_log
    FOR EACH STATEMENT
    EXECUTE FUNCTION audit_log_block_mutations();

-- ----------------------------------------------------------------------------
-- LEAST-PRIVILEGE ROLE HARDENING — strip mutation rights from the app role
-- ----------------------------------------------------------------------------
-- This block makes the trigger truly binding by ensuring the APP connects as a
-- role that (a) does NOT own audit_log (so it cannot DISABLE/DROP the trigger) and
-- (b) holds ONLY INSERT + SELECT (so it cannot even attempt UPDATE/DELETE/TRUNCATE).
--
-- WHY GUARDED (DO block, role-exists check): the migration must run identically in
-- two worlds:
--   - PRODUCTION: init-postgres.sh provisions a dedicated non-owner role
--     'fp_audit_app' and the service's DSN uses it. The block REVOKEs mutation
--     rights from it and grants the INSERT+SELECT floor.
--   - LOCAL DEV / TESTS: a single owner role (POSTGRES_USER) creates the schema and
--     also runs the app. There is no separate 'fp_audit_app' role, so this block is
--     a NO-OP — it must not fail the migration (the testcontainer applies the exact
--     same .up.sql). The owner-as-app caveat is documented above and in ADR 0009;
--     the TRUNCATE/UPDATE/DELETE triggers still apply to that owner unless it does
--     deliberate DDL.
--
-- Idempotent: REVOKE/GRANT are declarative and safe to re-run on every migrate.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fp_audit_app') THEN
        -- The role exists (production): it is a non-owner app identity. Strip every
        -- mutation right and leave exactly the append-only working set.
        REVOKE UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON audit_log FROM fp_audit_app;
        GRANT  INSERT, SELECT                                ON audit_log TO   fp_audit_app;
        RAISE NOTICE 'audit_log: hardened privileges for non-owner role fp_audit_app (INSERT+SELECT only)';
    ELSE
        -- Single-role dev/test: no dedicated app role to harden. The triggers above
        -- are the guarantee here; the owner-as-writer caveat is documented.
        RAISE NOTICE 'audit_log: role fp_audit_app not present; skipping privilege hardening (single-role dev/test)';
    END IF;
END
$$;
