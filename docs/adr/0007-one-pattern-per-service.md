# ADR 0007: One distributed-systems pattern per service

**Status:** Accepted
**Date:** 2026-06-17
**Deciders:** Abdul Basit Sajid
**Context phase:** platform-wide service design (M1–M2)

## Context

Forgepoint exists to implement microservices patterns at depth — each one properly, not
sketched. A full ML lifecycle platform — train → register →
deploy → serve → monitor → retrain — is a wide enough surface that almost any distributed-
systems pattern has a *plausible* home somewhere in it. That breadth is an opportunity and a
trap.

The trap: in a real production system, services **blend** patterns. A registry service might
do CQRS *and* the outbox *and* a saga for promotion. If each Forgepoint service did everything
a production version would, the patterns would smear across the codebase — no service would be
the clean place to read any single one, and "where is X implemented?" would have no crisp
answer.

The question: how do we map the services to patterns so that each pattern has exactly one
canonical home, readable end-to-end, without the result feeling contrived?

## Options Considered

### Option A — Build each service the way production would (organic, blended patterns)

- **Pro:** most realistic; mirrors how real systems accrete patterns.
- **Con:** no service is the *clean* exemplar of any pattern — the saga logic, the outbox, the
  circuit breaker all appear in several services partially. Depth is diluted and no single
  file shows a pattern end-to-end.
- **Con:** more total complexity per service, and a change to one pattern touches several
  services instead of one.

### Option B — Deliberately assign ONE headline pattern per service (chosen)

Each service is designed so that **one** distributed-systems pattern is its reason for existing
and its dominant, fully-realized concern. Secondary mechanics still appear where unavoidable
(idempotency is everywhere; the registry has a promotion swap), but the service is built
around its one pattern.

- **Pro:** every pattern has a **single canonical home** you can read top-to-bottom.
  "Where's CQRS?" → `services/registry`. "Where's the outbox?" → `services/billing`.
- **Pro:** each service stays small enough that its whole pattern fits in one review.
- **Pro:** the patterns together still compose a *coherent, working* platform (the lifecycle
  closes), so the seams between them are exercised rather than assumed.
- **Con:** **less realistic** — a production registry would blend CQRS with an outbox and a
  promotion saga; here those live in *different* services. We accept the simplification and
  name it (see Consequences) rather than hide it.

### Option C — A pattern catalog detached from a real product

Standalone demos, one per pattern, not wired into a lifecycle.

- **Pro:** maximal pattern isolation.
- **Con:** the interesting failures only happen where patterns meet — a saga compensating over
  an outbox, a CQRS projection lagging the write model, a circuit breaker tripping mid-saga.
  Isolated demos never exercise those seams, so the hard parts stay untested.

## Decision

Adopt **Option B**: **one headline distributed-systems pattern per service**, deliberately, so
each pattern has a clean canonical home, while the services still compose one working closed-
loop platform.

The mapping:

| Service | Headline pattern | Why it's the canonical home |
|---|---|---|
| **auth** | **Centralized auth — JWT + RBAC** | One service mints/validates JWTs and owns roles/permissions; every other service trusts its claims (identity from claims, never request fields). |
| **registry** | **CQRS** | Write model (Postgres, source of truth, idempotency ledgers) split from a read projection (Redis) fed by emitted events — the dual-write problem avoided by emit-then-project. |
| **inference-gateway** | **Circuit breaker + rate limiting + traffic splitting** | The edge of the serving path: trips on failing model backends, enforces per-tenant quotas, and weight-splits canary↔stable traffic. |
| **model-serving** | **Replaceable runtime behind a port + custom-metric HPA** | The ONNX runtime sits behind the domain's `InferenceEngine`/`ModelFetcher` ports, so the domain never imports ONNX; load/unload are driven by consumed events, and the chart templates a custom-metric HPA (`autoscaling.customMetrics`). Note: the chart ships a single container — "sidecar" describes the *role* the runtime plays, not a second container in the pod. |
| **pipeline-orchestrator** | **Saga (orchestration + compensation) + DAG execution** | Multi-step deploy/retrain workflows with ordered compensation on failure; owns the deploy lifecycle outcomes. |
| **feature-store** | **Event sourcing** | Append-only feature-write log as the source of truth; online/offline views are materializations of that log. |
| **experiment-tracker** | **Event-driven (async batch ingestion)** | A passive sink: subscribes across the per-domain streams (`fp.models.>`, `fp.pipelines.>`, `fp.inference.>`, …) and ingests runs/metrics asynchronously; takes no platform action. |
| **billing** | **Outbox pattern** | State change + event written in one transaction; a relay publishes — reliable, exactly-once-effect event publishing for money. |
| **notification** | **Choreography** | An event reactor — subscribes to a curated subject set bound to the per-domain streams (a single `fp.>` consumer would overlap them) and fans out alerts; no service calls it, it issues no commands. Its gRPC surface is user-facing preferences/delivery-log reads only. |
| **model-monitor** | **Streaming drift detection + closed-loop retrain** | Windowed streaming aggregation over `InferenceCompleted` → emits `ModelDriftDetected` → triggers the retrain saga, closing serve → monitor → retrain (see ADR 0003). |

Deciding factor: the goal is depth per pattern, and a one-pattern-per-service map is what
makes each pattern individually readable while the whole still closes the lifecycle. Realism
(Option A) is the right call for a production system carrying production constraints; this
codebase optimizes for one legible implementation of each pattern.

## Consequences

- **Positive:** a single canonical, reviewable home for each pattern; small services; the
  patterns still cooperate in one working closed-loop platform, so cross-pattern seams get
  exercised rather than assumed.
- **Negative:** **deliberately less realistic than production** — real services blend patterns
  (a production registry would also carry an outbox and a promotion saga). We own this
  explicitly: the *primary* concern of each service is its headline pattern; secondary
  mechanics appear only where the service genuinely needs them.
- **"Would you really split it this way in prod?"** No. The split is one-pattern-per-service
  so each is a clean exemplar; in production the registry would also use an outbox for its
  event emit — which is exactly what the billing service demonstrates in isolation.
- **Follow-ups:** cross-pattern seams are documented where they touch — e.g. the registry's
  `ProjectionEmitter` port (ADR 0005) is noted as the exact seam an outbox-backed adapter
  (billing's pattern) would slot into without changing the service.

## Addendum (M7, 2026-06)

M7 added an 11th service, **ai-gateway**, which deliberately breaks the one-pattern rule: it
*reuses* patterns already built rather than introducing a new one — circuit-breaker failover
across LLM providers, a distributed rate limiter as per-tenant token budgets, traffic splitting
for prompt/model A-B, and outbox-published usage for cost metering. That is the point of the
addition: it tests whether the M0–M6 patterns transfer to a different workload without being
re-derived. See `docs/plans/forgepoint-llmops-extension-design.md`.
