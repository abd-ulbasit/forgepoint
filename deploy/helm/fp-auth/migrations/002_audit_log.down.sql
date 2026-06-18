-- 002_audit_log.down.sql — exact inverse of 002_audit_log.up.sql.
--
-- Drop in reverse creation order: the triggers (which depend on the table and the
-- function), then the function, then the table. DROP TABLE removes the table's
-- own indexes, so we don't drop idx_* explicitly. IF EXISTS keeps the down
-- migration tolerant of a partially-applied up.
--
-- NOTE: the privilege-hardening DO block has nothing to undo here — REVOKE/GRANT on
-- a table evaporate when the table is dropped, and we deliberately do NOT drop the
-- fp_audit_app role (it is provisioned by init-postgres.sh, not this migration, and
-- may be shared/owned by infra). DROP TABLE reclaims its grants.
--
-- NOTE: rolling this back DESTROYS the audit history. In production an audit log
-- is typically archived (shipped off-box) before any teardown — see ADR 0009 on
-- off-box head hashes. This down migration exists for clean dev/test rollback.

DROP TRIGGER IF EXISTS audit_log_no_truncate ON audit_log;
DROP TRIGGER IF EXISTS audit_log_no_update_delete ON audit_log;
DROP FUNCTION IF EXISTS audit_log_block_mutations();
DROP TABLE IF EXISTS audit_log;
