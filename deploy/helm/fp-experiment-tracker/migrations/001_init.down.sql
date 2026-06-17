-- 001_init.down.sql — reverse of 001_init.up.sql.
--
-- Drop in REVERSE dependency order (children before parents) so the foreign keys
-- never block a drop. run_metrics and run_params reference runs; runs references
-- experiments; idempotency_keys is standalone. Explicit ordering documents the
-- dependency graph rather than relying on a surprising CASCADE.
DROP TABLE IF EXISTS run_metrics;
DROP TABLE IF EXISTS run_params;
DROP TABLE IF EXISTS idempotency_keys;
DROP TABLE IF EXISTS runs;
DROP TABLE IF EXISTS experiments;
