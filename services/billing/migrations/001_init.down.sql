-- 001_init.down.sql — reverse of 001_init.up.sql (golang-migrate rollback).
--
-- Drop in REVERSE dependency order so a rollback never hits a "still referenced"
-- error: the FK-holding / dependent tables go before the ones they reference.
-- team_rate_plans and rate_plan_idempotency reference rate_plans, so they drop
-- first; usage_records, invoices and outbox are independent (no FKs across the
-- service's own tables, by design — the ledger and outbox stand alone so a write
-- never blocks on a parent lookup on the hot path). IF EXISTS makes the down
-- migration idempotent (safe to re-run against a partially-rolled-back schema).
DROP TABLE IF EXISTS outbox;
DROP TABLE IF EXISTS invoices;
DROP TABLE IF EXISTS usage_records;
DROP TABLE IF EXISTS team_rate_plans;
DROP TABLE IF EXISTS rate_plan_idempotency;
DROP TABLE IF EXISTS rate_plans;
