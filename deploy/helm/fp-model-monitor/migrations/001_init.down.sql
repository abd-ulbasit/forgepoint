-- 001_init.down.sql — exact inverse of 001_init.up.sql.
--
-- Drop in REVERSE dependency order: drift_reports first (it FK-references
-- monitors), then monitors. DROP TABLE removes the table's own indexes and
-- constraints, so we do not drop those separately. IF EXISTS keeps the down
-- migration idempotent — re-running a partial rollback never errors.
DROP TABLE IF EXISTS drift_reports;
DROP TABLE IF EXISTS monitors;
