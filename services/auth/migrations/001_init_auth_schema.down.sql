-- 001_init_auth_schema.down.sql — exact inverse of 001_init_auth_schema.up.sql.
--
-- WHY a precise inverse matters: `migrate down` must leave the database in the
-- state it had BEFORE the up migration ran, so a botched deploy can be rolled
-- back deterministically. We drop in REVERSE dependency order: tables with
-- foreign keys first (api_keys, user_roles) before the tables they reference
-- (users, roles). Dropping users before user_roles would fail on the FK.
--
-- `IF EXISTS` makes the down migration tolerant of a partially-applied up (e.g.
-- the up failed halfway): we never error trying to drop something that was never
-- created. `DROP TABLE` also drops that table's own indexes, so we don't drop
-- idx_* explicitly.
--
-- We deliberately do NOT drop the citext / pgcrypto EXTENSIONS: other schemas or
-- future migrations may rely on them, and dropping a shipped extension is a
-- heavier, riskier operation than this migration's scope. Extensions are
-- effectively idempotent infrastructure (CREATE EXTENSION IF NOT EXISTS).

DROP TABLE IF EXISTS api_keys;
DROP TABLE IF EXISTS user_roles;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS roles;
