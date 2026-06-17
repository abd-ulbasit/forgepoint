-- 001_init_notification_schema.down.sql — exact inverse of the .up migration.
--
-- Drop order is the REVERSE of create order so foreign keys never block a drop:
-- delivery_log and notification_channel_prefs reference their parents, so they go
-- first; then the parents. IF EXISTS makes the rollback idempotent (re-running a
-- partial down is safe). DROP TABLE removes the table's own indexes automatically,
-- so we do not drop indexes separately.
DROP TABLE IF EXISTS delivery_log;
DROP TABLE IF EXISTS notification_channel_prefs;
DROP TABLE IF EXISTS notification_preferences;
DROP TABLE IF EXISTS notifications;
