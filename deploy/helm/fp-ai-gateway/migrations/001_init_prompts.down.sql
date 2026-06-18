-- 001_init_prompts.down.sql — the reverse of 001_init_prompts.up.sql.
--
-- DROP TABLE removes the table AND all its indexes/constraints in one shot (the
-- indexes have no independent existence once the table is gone), so a single
-- statement fully reverses the up migration. IF EXISTS makes the down idempotent —
-- running it twice (or against a partially-applied schema) is a safe no-op rather
-- than an error. golang-migrate runs this on `migrate down`.
DROP TABLE IF EXISTS prompts;
