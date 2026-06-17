-- 001_init_auth_schema.up.sql — the Auth service's owned Postgres schema.
--
-- ============================================================================
-- DATABASE-PER-SERVICE (microservices data ownership)
-- ============================================================================
-- This schema is owned SOLELY by the Auth service. No other service reads or
-- writes these tables; cross-service data is exchanged over gRPC/NATS, never by
-- a shared database. That isolation is what lets Auth evolve its schema (add a
-- column, change an index) without coordinating a lock-step migration across
-- all 10 services — the core operational payoff of database-per-service.
--
-- ============================================================================
-- golang-migrate FILE FORMAT
-- ============================================================================
-- golang-migrate applies NNN_name.up.sql to migrate forward and NNN_name.down.sql
-- to roll back. The numeric prefix is the version; migrate records the highest
-- applied version in a `schema_migrations` table. We keep ONE logical change per
-- migration pair so a rollback is a precise inverse. At deploy time an
-- initContainer (or the migrate CLI) runs these against the real database; in
-- tests we read and exec the .up.sql directly (no migrate Go library — its
-- driver deps break `-mod=readonly` in this workspace).

-- ----------------------------------------------------------------------------
-- EXTENSIONS
-- ----------------------------------------------------------------------------
-- pgcrypto gives us gen_random_uuid() for server-side UUID v4 primary keys.
-- WHY UUIDs over bigserial: IDs are exposed in JWTs, API responses, and URLs.
--   Sequential integers leak volume ("user 42") and are guessable/enumerable;
--   random UUIDs are not. The cost is a wider key (16 bytes) and slightly worse
--   index locality — acceptable for an identity table that is not write-hot.
-- citext gives us a case-INSENSITIVE text type for the email column, so the
--   unique constraint and lookups treat "Ada@x.dev" and "ada@x.dev" as one
--   account. The domain ALSO lowercases emails (defense in depth), but citext
--   guarantees the invariant at the storage layer even if a future caller forgets.
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS citext;

-- ----------------------------------------------------------------------------
-- roles — named permission sets (flat RBAC, design D5)
-- ----------------------------------------------------------------------------
-- permissions is JSONB: an array of {"resource","action"} objects. WHY JSONB and
-- not a separate `permissions` table joined to roles:
--   - A role's permission set is small, read as a whole, and mutated as a whole
--     (you replace a role's grants, you don't query "all permissions with
--     action=write" across roles). Embedding avoids an N+1 join on the hot
--     CheckPermission path — GetUserRoles returns the role AND its permissions in
--     a single row.
--   - JSONB (not JSON) is stored decomposed/binary, so it is queryable and
--     indexable if we later need `permissions @> '[{"resource":"models"}]'`.
--   - TRADEOFF: we lose a foreign-key-enforced permission catalog. Acceptable:
--     the permission vocabulary is validated in the domain, and the wildcard
--     semantics ("*") are pure business logic, not a DB concern.
CREATE TABLE roles (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT        NOT NULL UNIQUE,            -- "admin", "engineer", "viewer"
    permissions JSONB       NOT NULL DEFAULT '[]'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ----------------------------------------------------------------------------
-- users — authenticated identities
-- ----------------------------------------------------------------------------
-- password_hash holds a bcrypt hash (never plaintext; the domain hashes before
-- it ever reaches this table). email is citext + UNIQUE: the case-insensitive
-- login identifier. active=false is a SOFT delete / suspension — we keep the row
-- for audit and to honor the "suspended user can't authenticate" rule, rather
-- than hard-deleting identity records.
CREATE TABLE users (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    email         CITEXT      NOT NULL UNIQUE,
    name          TEXT        NOT NULL DEFAULT '',
    team          TEXT        NOT NULL DEFAULT '',
    password_hash TEXT        NOT NULL,
    active        BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ----------------------------------------------------------------------------
-- user_roles — the user↔role assignment (design: explicit join table)
-- ----------------------------------------------------------------------------
-- WHY a join table when the domain models a SINGLE role per user:
--   The platform's RBAC is conceptually many-to-many (a join table is the
--   schema that supports growth to multi-role without a migration), but the
--   current domain rule is "a user has exactly one role" and AssignRole UPSERTS
--   to "exactly this role". We encode THAT rule by making user_id the PRIMARY
--   KEY of the join table: at most one row per user, so an upsert on the PK
--   atomically REPLACES the user's role. If we later allow multiple roles, we
--   change the PK to (user_id, role_id) and drop this single-row constraint —
--   a localized migration.
--
-- ON DELETE CASCADE on user_id: deleting a user removes their assignment row.
-- ON DELETE RESTRICT on role_id: you cannot delete a role that is still assigned
--   to users — prevents orphaning a user's authorization out from under them.
CREATE TABLE user_roles (
    user_id    UUID        NOT NULL PRIMARY KEY REFERENCES users (id) ON DELETE CASCADE,
    role_id    UUID        NOT NULL REFERENCES roles (id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Index the FK side so "which users have role X?" and the RESTRICT check on
-- role deletion don't require a sequential scan. Postgres does NOT auto-index
-- foreign keys (only primary keys / unique constraints), so we add it explicitly.
CREATE INDEX idx_user_roles_role_id ON user_roles (role_id);

-- ----------------------------------------------------------------------------
-- api_keys — long-lived machine credentials (design D3: prefix + hash)
-- ----------------------------------------------------------------------------
-- key_hash stores hex(SHA-256(raw_key)) — the ONLY key material persisted. It is
--   UNIQUE: two distinct raw keys hashing to the same value is a SHA-256
--   collision (infeasible), so uniqueness is a correctness guard and lets the
--   hash lookup return at most one row.
-- key_prefix is the first 8 chars of the raw key, stored for UI display and as an
--   indexed NARROWING column for the design's "narrow by prefix, then verify
--   hash" lookup path. The current port looks up directly by the unique hash
--   (simpler, one index hit), but the prefix index is kept for display queries
--   and the documented prefix-narrowing approach.
-- scopes is TEXT[] — a Postgres array of "resource:action" strings. WHY a native
--   array over JSONB or a child table: scopes are a flat list read/written as a
--   whole with the key; the array type is the lightest representation and avoids
--   a join on the validate-token hot path.
-- expires_at / revoked_at are NULLABLE timestamps: NULL expires_at = never
--   expires (long-lived service key); NULL revoked_at = active. Setting
--   revoked_at = now() is the SOFT delete that takes effect on the very next
--   GetByKeyHash (the domain's IsValid sees a non-nil RevokedAt and rejects).
CREATE TABLE api_keys (
    id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id    UUID        NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    key_hash   TEXT        NOT NULL UNIQUE,
    key_prefix TEXT        NOT NULL,
    scopes     TEXT[]      NOT NULL DEFAULT '{}',
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Index on key_prefix: supports the design's prefix-narrowing lookup and any
-- "show me the key starting fp_a1b2" UI query.
CREATE INDEX idx_api_keys_key_prefix ON api_keys (key_prefix);

-- Index on user_id: ListByUser (the "my keys" UI) and the ON DELETE CASCADE
-- check both filter by owner; without this they sequential-scan api_keys.
CREATE INDEX idx_api_keys_user_id ON api_keys (user_id);

-- ----------------------------------------------------------------------------
-- Seed the three baseline roles (design: admin, engineer, viewer)
-- ----------------------------------------------------------------------------
-- These are infrastructure data, not test fixtures: every environment needs the
-- standard roles to exist so AssignRole has something to resolve by name. We
-- seed them in the migration (idempotent via ON CONFLICT) so a fresh database is
-- immediately usable. Permissions use the wildcard grammar the domain
-- understands ("*" = any).
--   admin    → {*,*}                full platform access
--   engineer → write+read on the ML resources they operate
--   viewer   → read-only across those resources
INSERT INTO roles (name, permissions) VALUES
    ('admin', '[{"resource":"*","action":"*"}]'::jsonb),
    ('engineer', '[
        {"resource":"models","action":"write"},
        {"resource":"models","action":"read"},
        {"resource":"experiments","action":"write"},
        {"resource":"experiments","action":"read"},
        {"resource":"pipelines","action":"write"},
        {"resource":"pipelines","action":"read"}
    ]'::jsonb),
    ('viewer', '[
        {"resource":"models","action":"read"},
        {"resource":"experiments","action":"read"},
        {"resource":"pipelines","action":"read"}
    ]'::jsonb)
ON CONFLICT (name) DO NOTHING;
