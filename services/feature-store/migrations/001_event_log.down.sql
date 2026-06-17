-- 001_event_log.down.sql — reverse of 001_event_log.up.sql.
--
-- Drop in reverse dependency order. None of these have FKs between them (the log is
-- deliberately decoupled from its projections so a projection can be rebuilt without
-- touching the log), so order only matters for readability. IF EXISTS makes the
-- rollback idempotent (re-running down twice is a no-op, not an error).
DROP TABLE IF EXISTS offline_view;
DROP TABLE IF EXISTS idempotency_keys;
DROP TABLE IF EXISTS view_name_index;
DROP TABLE IF EXISTS feature_events;
