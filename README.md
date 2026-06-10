# Forgepoint

[![CI](https://github.com/abd-ulbasit/forgepoint/actions/workflows/ci.yml/badge.svg)](https://github.com/abd-ulbasit/forgepoint/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A full ML‑lifecycle platform built as **9 Go microservices** — a hands‑on study of production microservices patterns (saga, CQRS, event sourcing, outbox, circuit breaker, choreography) on Kubernetes, with gRPC for sync and NATS JetStream for async.

> **Domain as a vehicle.** Forgepoint models an MLOps platform (train → register → deploy → serve → monitor → retrain), but the ML parts are deliberately thin (pre‑trained CPU ONNX models, no GPU). The real subject is **distributed‑systems engineering**: one well‑known pattern implemented properly per service, instrumented and deployed the way you would in production.

---

## Architecture

```
External Clients → API Gateway → Services (gRPC) ⇄ NATS JetStream (events) → Infrastructure
                                      │
                       Postgres (per service) · Redis · MinIO · OTel → Prometheus/Tempo/Loki
```

Each service follows **Clean Architecture** (`handler → domain → repository`) with a framework‑free domain layer, owns its **own database**, and communicates via **gRPC** (sync) and **NATS JetStream event envelopes** (async).

### Services & the pattern each one teaches

| Service | Pattern | Data store |
|---|---|---|
| Auth / IAM | Centralized auth, JWT, RBAC | PostgreSQL |
| Model Registry | **CQRS** (write Postgres, read Redis) | PostgreSQL + Redis |
| Inference Gateway | **Circuit breaker**, rate limiting, traffic splitting | Redis |
| Pipeline Orchestrator | **Saga** (orchestration + compensation), DAG execution | PostgreSQL |
| Feature Store | **Event sourcing** (append‑only log, materialized views) | PostgreSQL + Redis |
| Experiment Tracker | **Event‑driven** async batch ingestion | PostgreSQL |
| Billing / Usage | **Outbox pattern** (reliable event publishing) | PostgreSQL |
| Notification | **Choreography** (pure event reactor) | PostgreSQL |
| Model Serving | **Sidecar**, HPA on custom metrics | In‑memory (ONNX) |

A **Backend‑for‑Frontend (BFF)** + web UI sit in front of these — see [`docs/adr/0001-bff-for-web-ui.md`](docs/adr/0001-bff-for-web-ui.md).

---

## Status

This is an actively‑built project. The **shared foundation (`pkg/`) is complete and tested**; the services are being built one at a time (depth over breadth).

| Area | State |
|---|---|
| `pkg/` shared libraries | ✅ Built & tested (build · vet · `-race`) |
| Local infra (docker‑compose) | ✅ NATS, Postgres, Redis, MinIO, Prometheus, Tempo, Grafana, Loki |
| Proto tooling (Buf) | ✅ Lint + codegen wired |
| Services (auth → …) | 🚧 In progress, per the implementation plan |

See the [implementation plan](docs/plans/forgepoint-implementation-plan.md) for the phased roadmap (Core vs Stretch).

### Shared libraries (`pkg/`)

Reusable building blocks every service imports — designed to be read and explained, not just used:

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
services/     The 9 microservices (in progress)
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
