-- 002_eval_scores.down.sql — exact inverse of 002_eval_scores.up.sql.
--
-- DROP TABLE removes the table's own index (eval_scores_team_model_created_idx) and
-- constraints, so we do not drop those separately. IF EXISTS keeps the down
-- migration idempotent — re-running a partial rollback never errors.
DROP TABLE IF EXISTS eval_scores;
