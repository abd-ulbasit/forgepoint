-- 001_init.down.sql — reverse of 001_init.up.sql.
--
-- Drop in REVERSE dependency order (children before parents) so foreign keys
-- never block a drop. step_executions and execution_idempotency reference
-- executions; execution_idempotency and pipeline_idempotency reference their
-- parents; executions references pipelines. DROP ... CASCADE would also work but
-- explicit ordering documents the dependency graph and avoids surprising cascades.
DROP TABLE IF EXISTS step_executions;
DROP TABLE IF EXISTS execution_idempotency;
DROP TABLE IF EXISTS executions;
DROP TABLE IF EXISTS pipeline_idempotency;
DROP TABLE IF EXISTS pipelines;
