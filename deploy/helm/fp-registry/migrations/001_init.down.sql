-- 001_init.down.sql — reverse 001_init.up.sql.
--
-- Drop order is the REVERSE of create order and respects the FK: model_versions
-- references models, so versions must go first. command_idempotency is independent.
-- DROP TABLE removes the table's indexes and constraints with it, so we don't drop
-- those separately. IF EXISTS keeps the down migration idempotent (re-runnable),
-- which golang-migrate's force/retry flows rely on.
DROP TABLE IF EXISTS command_idempotency;
DROP TABLE IF EXISTS model_versions;
DROP TABLE IF EXISTS models;
