# ADR 0007: One distributed-systems pattern per service (the teaching thesis)

**Status:** Accepted
**Date:** 2026-06-17
**Deciders:** Abdul Basit Sajid
**Context phase:** Implementation Plan — platform-wide service design (M1–M2)

## Context

Forgepoint exists primarily to build **interview-explainable** depth in microservices
patterns (see CLAUDE.md, "Coding Approach"). A full ML lifecycle platform — train → register →
deploy → serve → monitor → retrain — is a wide enough surface that almost any distributed-
systems pattern has a *plausible* home somewhere in it. That breadth is an opportunity and a
trap.

The trap: in a real production system, services **blend** patterns. A registry service might
do CQRS *and* the outbox *and* a saga for promotion. If each Forgepoint service did everything
a production version would, the patterns would smear across the codebase — no service would be
the clean place to *learn* any single one, and the interview story ("show me how you'd build
X") would have no crisp anchor.

The question: how do we map the 10 services to patterns so the platform is a **teaching
instrument** — each pattern has exactly one canonical home you can read end-to-end — without
the result feeling contrived?

## Options Considered

### Option A — Build each service the way production would (organic, blended patterns)

- **Pro:** most realistic; mirrors how real systems accrete patterns.
- **Con:** no service is the *clean* exemplar of any pattern — the saga logic, the outbox, the
  circuit breaker all appear in several services partially. Learning depth is diluted; there's
  no single file to point an interviewer at.
- **Con:** more total complexity per service, slower to a polished, explainable state.

### Option B — Deliberately assign ONE headline pattern per service (chosen)

Each service is designed so that **one** distributed-systems pattern is its reason for existing
and its dominant, fully-realized concern. Secondary mechanics still appear where unavoidable
(idempotency is everywhere; the registry has a promotion swap), but the service is *built to
teach* its one pattern cleanly.

- **Pro:** every pattern has a **single canonical home** you can read top-to-bottom — the
  teaching contract of the project. "Where's CQRS?" → `services/registry`. "Where's the
  outbox?" → `services/billing`.
- **Pro:** each service stays small enough to polish to interview-grade and explain line-by-line.
- **Pro:** the 10 patterns together still compose a *coherent, working* platform (the lifecycle
  closes), so it doesn't read as a toy catalog of disconnected demos.
- **Con:** **less realistic** — a production registry would blend CQRS with an outbox and a
  promotion saga; here those live in *different* services. We accept the simplification and
  name it (see Consequences) rather than hide it.

### Option C — A pattern catalog detached from a real product

Standalone demos, one per pattern, not wired into a lifecycle.

- **Pro:** maximal pattern isolation.
- **Con:** no end-to-end system, no "alive" story, nothing that demonstrates the patterns
  *cooperating* — the weakest portfolio narrative.

## Decision

Adopt **Option B**: **one headline distributed-systems pattern per service**, deliberately, so
each pattern has a clean canonical home, while the 10 services still compose one working closed-
loop platform.

The mapping:

| Service | Headline pattern | Why it's the canonical home |
|---|---|---|
| **auth** | **Centralized auth — JWT + RBAC** | One service mints/validates JWTs and owns roles/permissions; every other service trusts its claims (identity from claims, never request fields). |
| **registry** | **CQRS** | Write model (Postgres, source of truth, idempotency ledgers) split from a read projection (Redis) fed by emitted events — the dual-write problem avoided by emit-then-project. |
| **inference-gateway** | **Circuit breaker + rate limiting + traffic splitting** | The edge of the serving path: trips on failing model backends, enforces per-tenant quotas, and weight-splits canary↔stable traffic. |
| **model-serving** | **Sidecar + HPA on custom metrics** | Model runtime (ONNX) scaled by a custom-metrics HPA; the deploy/runtime concerns isolated from platform logic. |
| **pipeline-orchestrator** | **Saga (orchestration + compensation) + DAG execution** | Multi-step deploy/retrain workflows with ordered compensation on failure; owns the deploy lifecycle outcomes. |
| **feature-store** | **Event sourcing** | Append-only feature-write log as the source of truth; online/offline views are materializations of that log. |
| **experiment-tracker** | **Event-driven (async batch ingestion)** | A passive sink: subscribes broadly across `fp.>` and ingests runs/metrics asynchronously; takes no platform action. |
| **billing** | **Outbox pattern** | State change + event written in one transaction; a relay publishes — reliable, exactly-once-effect event publishing for money. |
| **notification** | **Choreography** | A pure event reactor — subscribes to `fp.>` and fans out alerts; no service calls it, it issues no commands. |
| **model-monitor** | **Streaming drift detection + closed-loop retrain** | Windowed streaming aggregation over `InferenceCompleted` → emits `ModelDriftDetected` → triggers the retrain saga, closing serve → monitor → retrain (see ADR 0003). |

Deciding factor: the project's purpose is **learning depth that survives an interview**, and a
one-pattern-per-service map is what makes each pattern individually readable while the whole
still closes the lifecycle. Realism (Option A) is the right call for a production system, not
for a teaching instrument.

## Consequences

- **Positive:** a single canonical, line-by-line-explainable home for each of 10 patterns;
  small, polishable services; the patterns still cooperate in one working closed-loop platform
  (not a disconnected catalog).
- **Negative:** **deliberately less realistic than production** — real services blend patterns
  (a production registry would also carry an outbox and a promotion saga). We own this
  explicitly: the *primary* concern of each service is its headline pattern; secondary
  mechanics appear only where the service genuinely needs them.
- **Interview framing:** the honest answer to "would you really split it this way in prod?" is
  *no — I split one-pattern-per-service so each is a clean exemplar; in production the registry
  would also use an outbox for its event emit, which is exactly what the billing service
  demonstrates in isolation.* Naming the simplification is itself the senior-engineer signal.
- **Follow-ups:** cross-pattern seams are documented where they touch — e.g. the registry's
  `ProjectionEmitter` port (ADR 0005) is noted as the exact seam an outbox-backed adapter
  (billing's pattern) would slot into without changing the service.
