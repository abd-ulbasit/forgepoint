# Forgepoint

[![CI](https://github.com/abd-ulbasit/forgepoint/actions/workflows/ci.yml/badge.svg)](https://github.com/abd-ulbasit/forgepoint/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A full ML‑lifecycle platform built as **11 Go microservices** (plus a BFF), each implementing **one** distributed‑systems pattern to production depth rather than sketching several: saga with ordered compensation, CQRS, event sourcing, transactional outbox, circuit breaker, choreography, streaming drift detection. gRPC for sync, NATS JetStream for async, one Postgres per service, on Kubernetes. The lifecycle runs as a **closed loop** (serve → monitor → retrain) and ships via **GitOps**.

> **Domain as a vehicle.** Forgepoint models an MLOps platform (train → register → deploy → serve → monitor → retrain), but the ML parts are deliberately thin (pre‑trained CPU ONNX models, no GPU). The real subject is **distributed‑systems engineering**: one well‑known pattern implemented properly per service, instrumented and deployed the way you would in production.

---

## The parts worth reading

The concurrency and durability edges, with the specific failure each one closes:

- **A saga cancelled mid-flight still has to record that it was cancelled.** When a user cancels, the incoming context is already dead, so a real pgx adapter rejects the compensating writes and the terminal row — the API would answer `CANCELLED` while the durable row stayed `RUNNING`. The terminal write detaches onto `context.WithoutCancel` under a bounded `SettlementTimeout`, and a failing terminal `Save` is retried and then surfaced, never swallowed. → `services/pipeline-orchestrator/internal/domain/pipeline_service_impl.go`

- **One PRODUCTION version per model, enforced in two independent places.** A partial unique index `ON model_versions (model_id) WHERE stage = 3` holds the invariant, and the promote transaction must demote the incumbent *before* promoting the candidate — a partial index cannot be `DEFERRABLE` in Postgres, so it is checked per statement and every intermediate state has to be legal on its own. The ordering is the rule; the index is the backstop for the call site that forgets it. → `services/registry/migrations/001_init.up.sql`, `services/registry/internal/repository/postgres/write_store.go`

- **A Postgres sequence leaks numbers on rollback; an event log wants a dense total order.** The version is assigned *inside* the transaction that inserts the row, under `pg_advisory_xact_lock`, so a rollback returns the number and the lock releases with the transaction even if the client dies mid-append. → `services/feature-store/internal/repository/postgres/eventlog.go`

- **Monotonic version assignment without paying for SERIALIZABLE.** `MAX(version)+1` and the INSERT run in one transaction; a `(team, name, version)` unique index catches the interleaving that READ COMMITTED still permits, and the service retries on `23505`. Correctness comes from the index plus a retry loop, not from a heavier isolation level. → `services/ai-gateway/internal/repository/postgres/prompt_store.go`

- **Transactional outbox, including the crash between publish and stamp.** `RecordUsageTx` writes the usage row and its outbox rows in one transaction; the relay publishes and then stamps `published_at`. A crash in the gap leaves the row unpublished, the relay re-publishes on restart, and consumers dedupe on the outbox id — which *is* the `EventEnvelope.id`. At-least-once, never at-most-once. → `services/billing/internal/repository/postgres/usage_store.go`, `internal/events/relay.go`

- **Drift statistics asserted against hand-computed arithmetic.** PSI, KL and KS are pure functions; the tests pin them to known distributions with the arithmetic written out in the test, because `score > 0` proves nothing about a drift detector. → `services/model-monitor/internal/domain/drift.go`, `drift_test.go`

---

## Architecture

```
External Clients → API Gateway → Services (gRPC) ⇄ NATS JetStream (events) → Infrastructure
                                      │
                       Postgres (per service) · Redis · MinIO · OTel → Prometheus/Tempo/Loki
```

Each service follows **Clean Architecture** (`handler → domain → repository`) with a framework‑free domain layer, owns its **own database**, and communicates via **gRPC** (sync) and **NATS JetStream event envelopes** (async).

### One pattern per service

Each service is the single canonical home for its pattern — "where is CQRS?" has exactly one answer. See [ADR 0007](docs/adr/0007-one-pattern-per-service.md) for why, and what that costs in realism.

| Service | Pattern | Data store |
|---|---|---|
| Auth / IAM | Centralized auth, JWT, RBAC | PostgreSQL |
| Model Registry | **CQRS** (write Postgres, read projection in Redis) | PostgreSQL + Redis |
| Inference Gateway | **Circuit breaker**, rate limiting, traffic splitting | Redis |
| Pipeline Orchestrator | **Saga** (orchestration + ordered compensation), DAG execution | PostgreSQL |
| Feature Store | **Event sourcing** (append‑only log, materialized views) | PostgreSQL + Redis |
| Experiment Tracker | **Event‑driven** async batch ingestion | PostgreSQL |
| Billing / Usage | **Transactional outbox** (reliable event publishing) | PostgreSQL |
| Notification | **Choreography** (event reactor; no service calls it) | PostgreSQL |
| Model Serving | ONNX runtime behind a domain port; **event-driven load/unload**, custom-metric HPA | In‑memory (ONNX) + artifact fetch |
| Model Monitor | **Streaming drift detection** + closed‑loop retrain trigger | PostgreSQL + Redis |
| AI Gateway (LLM) | The same patterns applied to LLM infra: provider **failover**, per-tenant **token budgets**, **semantic cache**, prompt versioning | PostgreSQL + Redis |

A **Backend‑for‑Frontend (BFF)** + web UI and an `fp` **CLI** sit in front of these; the platform is delivered via **GitOps (ArgoCD)**, secured with policy-as-code + a signed supply chain, and autoscaled with **KEDA**. See the [ADRs](docs/adr/) — [BFF](docs/adr/0001-bff-for-web-ui.md), [GitOps](docs/adr/0002-gitops-with-argocd.md), [closed-loop Model Monitor](docs/adr/0003-closed-loop-model-monitor.md).

---

## Status

The **10 M0–M6 services** are code‑complete across every layer — built test‑first with adversarial bug/security verification, and validated with the race detector against real Postgres/Redis/NATS (testcontainers). **All 10 are deployed and running on Kubernetes (k3s), JWT-authenticated end to end** — with a live cross-service E2E + k6 load suite, a Web UI / `fp` CLI / Python SDK product surface, and SRE / service-mesh / audit / backup operability.

The **AI Gateway (M7, LLMOps)** is the 11th service: built, tested and Helm-charted, but not yet added to the ArgoCD app-of-apps — so the counts below are M0–M6 unless stated. See the table.

| Area | State |
|---|---|
| `pkg/` shared libraries | ✅ Built & tested (build · vet · `-race`) |
| Proto contracts + `events/v1` event schema | ✅ All 10 services, `buf` lint/breaking |
| Service **domains** (one pattern each) | ✅ All 10 — TDD + adversarial review |
| Service **handlers** (gRPC, proto↔domain) | ✅ All 10 — bufconn component tests |
| **Persistence** (Postgres/Redis adapters + migrations) | ✅ All 10 — testcontainers `-race` |
| **Events** (NATS pub/sub, idempotent, DLQ, **protojson** wire format) + wiring | ✅ All 10 — testcontainers `-race` |
| Helm charts | ✅ All 11 + BFF |
| ArgoCD app‑of‑apps | ✅ 10 (M0–M6); AI Gateway chart not yet registered |
| k8s data infra (NATS/Postgres/Redis/MinIO) | ✅ Deployed to k3s `fp-infra` |
| Observability stack · CI/CD + supply‑chain · M6 (Kyverno/GitOps/secrets) · Terraform (AWS) | ✅ Authored |
| **Deployed & running** (image → k3s → Helm) | ✅ 10/10 M0–M6 services + BFF + infra; JWT-auth end to end |
| Cross-service **E2E** + **k6** load | ✅ Live (login → register → read-back → cross-service event propagation; CQRS read model + DLQ-clean; thresholds green) |
| **Web UI** (BFF + React SPA) · **`fp` CLI** · **Python SDK** + API docs | ✅ M4 product surface |
| **AI Gateway** (M7 — provider failover · token budgets · semantic cache · prompt registry · LLM-as-judge evals) | ✅ Built & tested; chart authored |
| **SRE** (SLOs · burn-rate alerts · runbooks) · **Istio** mesh (STRICT mTLS · deny-by-default authz) · tamper-evident **audit log** · **Backup/DR** | ✅ M3 / M6 / M5 |

See the [implementation plan](docs/plans/forgepoint-implementation-plan.md) for the full phased roadmap and the [ADRs](docs/adr/) for the key decisions.

### Shared libraries (`pkg/`)

Reusable building blocks every service imports:

- **`grpcutil`** — server factory with the full interceptor chain (recovery → logging → auth) for **both unary and streaming**, OpenTelemetry tracing via `otelgrpc`, graceful shutdown, and readiness‑aware health.
- **`natsutil`** — JetStream publisher/subscriber with standard **event envelopes**, **DLQ**, **consumer‑side idempotency**, and trace/correlation propagation across the async hop.
- **`observability`** — single‑call OpenTelemetry setup (traces + metrics + trace‑correlated logs), OTLP exporters that fall back to stdout for local dev.
- **`config`** — zero‑dependency env‑var loader (struct tags, defaults, `required`, durations, slices).
- **`health`** — `/healthz` + `/readyz` with concurrent, timeout‑bounded dependency checks.
- **`testutil`** — testcontainers helpers + in‑process (`bufconn`) gRPC test server.

---

## Tech stack

**Go · gRPC + Buf · NATS JetStream · PostgreSQL · Redis · MinIO** · Docker (distroless) · **Kubernetes** (Kind → EKS) · Helm · Skaffold · **OpenTelemetry** → Prometheus / Grafana / Loki / Tempo · Istio · Terraform · GitHub Actions · k6 · testcontainers.

---

## Getting started

Prerequisites: Go 1.26+, Docker, `make`.

```bash
# Start local infrastructure (NATS, Postgres, Redis, MinIO, monitoring)
make up

# Run the shared-library tests (race detector on)
cd pkg && go test -race ./...

# Tear down
make down
```

Common targets (see the [Makefile](Makefile)): `make proto`, `make build SVC=<svc>`, `make test`, `make lint`, `make up` / `make down`.

---

## Repository layout

```
proto/        Source of truth for all APIs (Buf)
gen/go/       Generated proto code (do not edit)
pkg/          Shared libraries (built & tested)
services/     The 11 microservices + BFF
deploy/       Helm, K8s manifests, Terraform, Skaffold
docs/
  plans/      Platform design + phased implementation plan
  adr/        Architecture Decision Records
docker-compose.yaml   Local infrastructure
```

---

## Documentation

- **[Platform design](docs/plans/forgepoint-platform-design.md)** — architecture, services, communication, infra.
- **[Implementation plan](docs/plans/forgepoint-implementation-plan.md)** — phased roadmap with Core/Stretch tiers.
- **[Architecture Decision Records](docs/adr/)** — non‑obvious decisions and their tradeoffs.

---

## License

[MIT](LICENSE) © 2026 Abdul Basit Sajid
