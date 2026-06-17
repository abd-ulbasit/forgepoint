# ADR 0003: Closed-loop model monitoring (Model Monitor service)

**Status:** Accepted
**Date:** 2026-06-17
**Deciders:** Abdul Basit Sajid
**Context phase:** Implementation Plan — Phase 16 (Milestone M2)

## Context

The platform's headline is the **full ML lifecycle**: `train → register → deploy → serve →
monitor → retrain`. But the original design had a hole: no component **monitors** deployed
models for degradation, and nothing **triggers** retraining. The tell was concrete — the
Notification service already subscribes to a `ModelDriftDetected` event that **no service ever
publishes**. Without a monitor, the lifecycle is open-ended (it stops at "serve"), and the
"retrain" stage is decorative.

We need something that observes production inference, detects drift/decay, and drives
retraining — closing the loop.

## Options Considered

### Option A — Integrate an external ML-monitoring tool (Evidently / Arize / WhyLabs)
- **Pro:** batteries-included drift metrics and dashboards; less code.
- **Con:** off-loads the most interesting distributed-systems learning (streaming aggregation,
  windowing, closed-loop control); adds a heavy external dependency; weaker interview story
  ("I wired up a SaaS" vs "I built the loop"); doesn't fit the self-contained Go-microservices thesis.

### Option B — Fold monitoring into the Experiment Tracker
- **Pro:** reuses an existing event-driven consumer; no new service.
- **Con:** conflates two responsibilities (offline experiment logging vs online drift + control);
  the Experiment Tracker is a passive sink, while monitoring must take an **action** (trigger
  retraining) — different concern, different failure modes; muddies both services.

### Option C — A dedicated **Model Monitor** service (chosen)
- **Pro:** closes the loop with a clear owner; introduces a genuinely new pattern (windowed
  streaming aggregation + a control loop that acts on the platform); produces the missing
  `ModelDriftDetected` event; the most "alive" story in the system (models that self-heal).
- **Con:** a 10th service to build and operate.

## Decision

Adopt **Option C — a dedicated Model Monitor service** (`services/model-monitor`). It consumes
`InferenceCompleted`, maintains per-model sliding windows (data drift via PSI/KL/KS, prediction
drift, and performance decay from delayed ground-truth), emits `ModelDriftDetected`, and — when
auto-retrain is enabled — calls the Pipeline Orchestrator to run the model's training pipeline.
The new version flows through the existing canary→promote saga, closing the loop.

Deciding factor: this is the single change that makes "full ML lifecycle" *true* rather than
aspirational, and it does so by adding learning value (streaming + control loop) instead of
out-sourcing it.

## Consequences

- **Positive:** the lifecycle is genuinely closed; `ModelDriftDetected` now has a producer; a
  distinctive self-healing demo; a new pattern in the portfolio.
- **Negative:** more drift-math to implement and test; another service to deploy/monitor;
  ground-truth labels arrive late, so accuracy metrics lag (acknowledged in the windowing design).
- **Follow-ups:** Progressive delivery (Phase 21) can consume the same drift signal as a canary
  analysis metric; the monitor's reports surface in the Web UI (Phase 14) and runbooks (Phase 15).
