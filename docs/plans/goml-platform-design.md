# GoML — ML Platform as Microservices

**Date:** 2026-02-21
**Status:** Approved
**Author:** Abdul Basit Sajid

## Overview

GoML is a full ML lifecycle platform built as a distributed microservices system in Go. It covers model training, registration, deployment, serving, monitoring, and retraining — orchestrated across 9 independent services communicating via gRPC (sync) and NATS JetStream (async), deployed on Kubernetes.

**Goals:**
- Learn every major microservices pattern (saga, CQRS, event sourcing, choreography, outbox, etc.) with a real, production-standard project
- Build a portfolio piece targeting remote high-paying roles at YC startups and platform engineering positions
- Demonstrate mastery of Go, Kubernetes, distributed systems, and cloud-native infrastructure
- Cover the full MLOps lifecycle: train → register → deploy → serve → monitor → retrain

**Non-goals:**
- Building actual ML models (we use pre-trained ONNX CPU models)
- GPU infrastructure (all inference runs on CPU, keeping costs near zero)
- Web UI (CLI + gRPC/HTTP APIs are the interface)

## Architecture

```
                            ┌──────────────────────┐
                            │    External Clients   │
                            │  (CLI, SDK, Web UI)   │
                            └──────────┬───────────┘
                                       │
                            ┌──────────▼───────────┐
                            │    API Gateway        │  ← Traefik/Kong
                            │  (routing, TLS, auth) │
                            └──────────┬───────────┘
                                       │ gRPC / HTTP
                 ┌─────────────────────┼─────────────────────┐
                 │                     │                     │
    ┌────────────▼──┐   ┌─────────────▼──┐   ┌─────────────▼──────┐
    │  Auth/IAM     │   │ Model Registry │   │ Inference Gateway  │
    │  Service      │   │ Service        │   │ Service            │
    │               │   │                │   │                    │
    │ • API keys    │   │ • CQRS pattern │   │ • Circuit breaker  │
    │ • RBAC        │   │ • Versioning   │   │ • Rate limiting    │
    │ • JWT         │   │ • S3 artifacts │   │ • A/B routing      │
    └───────────────┘   └────────────────┘   └─────────┬──────────┘
                                                       │
                                              ┌────────▼────────┐
                                              │  Model Serving  │
                                              │  Service(s)     │
                                              │  • ONNX Runtime │
                                              │  • Autoscaling  │
                                              │  • Health checks│
                                              └─────────────────┘

    ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
    │   Pipeline      │   │  Feature Store  │   │  Experiment     │
    │   Orchestrator  │   │  Service        │   │  Tracker        │
    │                 │   │                 │   │                 │
    │ • Saga pattern  │   │ • Event sourcing│   │ • Event-driven  │
    │ • DAG execution │   │ • Versioned     │   │ • Metrics store │
    │ • Compensation  │   │ • Online/offline│   │ • Comparisons   │
    └─────────────────┘   └─────────────────┘   └─────────────────┘

    ┌─────────────────┐   ┌─────────────────┐
    │   Billing /     │   │  Notification   │
    │   Usage Service │   │  Service        │
    │                 │   │                 │
    │ • Metering      │   │ • Choreography  │
    │ • Quotas        │   │ • Model drift   │
    │ • Outbox pattern│   │ • Pipeline alerts│
    └─────────────────┘   └─────────────────┘

    ════════════════════════════════════════════════════
    Shared Infrastructure:
    ┌──────┐ ┌──────────┐ ┌───────┐ ┌─────┐ ┌───────┐
    │ NATS │ │PostgreSQL│ │ Redis │ │MinIO│ │ Tempo │
    │ JS   │ │(per-svc) │ │       │ │(S3) │ │       │
    └──────┘ └──────────┘ └───────┘ └─────┘ └───────┘
    ┌────────────┐  ┌─────────┐  ┌──────────────────┐
    │ Prometheus  │  │ Grafana │  │ Istio / Linkerd  │
    └────────────┘  └─────────┘  └──────────────────┘
```

## ML Lifecycle Coverage

The platform is generic — not limited to inference. It covers the full ML lifecycle:

| Workflow Type | Steps | Pattern |
|--------------|-------|---------|
| **Model Deployment** | validate → build → canary → promote | Saga with compensation |
| **Training Pipeline** | fetch data → preprocess → train → evaluate → register | DAG execution |
| **Batch Inference** | load model → partition data → fan-out → aggregate | Fan-out/fan-in |
| **Fine-tuning** | load base → train on custom data → evaluate → register | DAG execution |
| **A/B Experiment** | deploy variant → split traffic → collect → pick winner | Long-running saga |

## Services

### Service 1: Auth/IAM
**Pattern:** Centralized Authentication + Token-based AuthZ

- **Purpose:** API key management, JWT issuance, RBAC (who can deploy models, who can query)
- **Data Store:** PostgreSQL (users, api_keys, roles, permissions, teams)
- **gRPC API:** `CreateAPIKey`, `ValidateToken`, `CheckPermission`, `AssignRole`
- **Events published:** `UserCreated`, `APIKeyRotated`, `PermissionChanged`
- **Why:** Every other service calls this. Cross-cutting auth via shared interceptor in `pkg/auth/`.

### Service 2: Model Registry
**Pattern:** CQRS (Command Query Responsibility Segregation)

- **Purpose:** Register models, version them, store artifacts (weights) in MinIO/S3
- **Write Store:** PostgreSQL (normalized: models, versions, tags, metadata)
- **Read Store:** Redis (denormalized, optimized for "give me latest version of model X")
- **gRPC API:** `RegisterModel`, `CreateVersion`, `GetModel`, `ListModels`, `SearchByTag`
- **Events published:** `ModelRegistered`, `ModelVersionCreated`, `ModelArchived`
- **CQRS flow:** Write → Postgres → publish event → NATS → read-side consumer updates Redis. Reads hit Redis. Demonstrates eventual consistency, read/write separation, projection rebuilding.

### Service 3: Inference Gateway
**Pattern:** API Gateway + Circuit Breaker + Rate Limiting + Traffic Splitting

- **Purpose:** Route inference requests to the right model server, protect backends
- **Data Store:** Redis (rate limit counters, routing table cache)
- **APIs:** HTTP `POST /v1/models/{model}/predict` (external) + gRPC internal
- **Events consumed:** `ModelDeployed`, `ModelUndeployed`, `CanaryStarted`
- **Events published:** `InferenceCompleted`
- **Patterns:** Circuit breaker (3-state), token bucket rate limiting, weighted traffic splitting (90/10 canary), retry with exponential backoff, bulkhead (per-model connection pools)

### Service 4: Pipeline Orchestrator
**Pattern:** Saga (Orchestration with Compensation) + DAG Execution

This is the star service — a generic workflow engine.

- **Purpose:** Orchestrate multi-step ML workflows of any type
- **Data Store:** PostgreSQL (pipeline_definitions, executions, saga_state, step_logs)
- **gRPC API:** `CreatePipeline`, `TriggerExecution`, `GetStatus`, `CancelExecution`, `WatchExecution` (server-streaming)
- **Events published:** `PipelineStarted`, `StepCompleted`, `StepFailed`, `CompensationTriggered`

**Deployment Saga (compensation flow):**
```
  Trigger Pipeline
       │
       ▼
  ┌─ Validate Model ──────────── fail → (nothing to compensate)
  │    │ success
  │    ▼
  ├─ Create Serving Instance ─── fail → Destroy Instance
  │    │ success
  │    ▼
  ├─ Run Canary (10% traffic) ── fail → Rollback Traffic + Destroy Instance
  │    │ success (metrics pass threshold)
  │    ▼
  ├─ Promote to 100% ────────── fail → Rollback to Previous Version
  │    │ success
  │    ▼
  └─ Mark Deployment Complete
```

Each step is durable (persisted to DB). If the orchestrator crashes mid-saga, it resumes from last checkpoint. Compensation runs in reverse order on failure.

**Training DAG (fan-out execution):**
```
  Fetch Dataset ─→ Preprocess ─→ ┌─ Train Fold 1 ─┐
                                  ├─ Train Fold 2 ─┤─→ Aggregate ─→ Evaluate ─→ Register
                                  └─ Train Fold 3 ─┘
```

### Service 5: Feature Store
**Pattern:** Event Sourcing

- **Purpose:** Store, version, and serve features for model inference
- **Event Store:** PostgreSQL append-only table (feature_events: Created, Updated, Deleted)
- **Materialized Views:** Redis (online serving: low-latency lookups) + PostgreSQL (offline: batch queries)
- **gRPC API:** `CreateFeatureSet`, `IngestFeatures`, `GetOnlineFeatures`, `GetHistoricalFeatures`
- **Events published:** `FeatureSetCreated`, `FeaturesIngested`, `FeatureVersionCreated`
- **Event Sourcing flow:** All mutations stored as events. Current state = replay events. Can rebuild materialized views from event log. Point-in-time feature snapshots for reproducibility.

### Service 6: Experiment Tracker
**Pattern:** Event-Driven (Async Ingestion)

- **Purpose:** Track ML experiments — log metrics, parameters, compare model performance
- **Data Store:** PostgreSQL (experiments, runs) + time-partitioned tables for metrics
- **gRPC API:** `CreateExperiment`, `StartRun`, `LogMetrics`, `LogParameters`, `CompareRuns`
- **Events consumed:** `InferenceCompleted`, `ModelDeployed`, `PipelineCompleted`
- **Why event-driven:** Metrics arrive at high volume. Service subscribes to NATS, batches writes. No synchronous path in the hot inference loop. Demonstrates back-pressure handling, batch consumers.

### Service 7: Billing/Usage
**Pattern:** Eventual Consistency + Outbox Pattern

- **Purpose:** Meter every inference call, enforce quotas, generate usage reports
- **Data Store:** PostgreSQL (usage_events, quotas, rate_plans, invoices)
- **gRPC API:** `GetUsage`, `CheckQuota`, `CreateRatePlan`, `GetInvoice`
- **Events consumed:** `InferenceCompleted`
- **Events published:** `QuotaExceeded`, `InvoiceGenerated`
- **Outbox Pattern:** When quota is exceeded: write to `outbox` table in same DB transaction as usage update → separate poller reads outbox → publishes to NATS. Guarantees exactly-once event publishing without distributed transactions.

### Service 8: Notification Service
**Pattern:** Choreography (Pure Event Reactor)

- **Purpose:** React to platform events and deliver notifications (webhook, Slack, email)
- **Data Store:** PostgreSQL (notification_rules, delivery_log)
- **gRPC API:** `CreateRule`, `ListRules` (minimal)
- **Events consumed:** `PipelineFailed`, `QuotaExceeded`, `ModelDriftDetected`, `CanaryFailed`
- **Choreography:** No orchestrator tells this service what to do. It subscribes to events and independently decides what to notify on. Users configure rules. Demonstrates decoupled choreography vs orchestration.

### Service 9: Model Serving
**Pattern:** Sidecar + Horizontal Autoscaling

- **Purpose:** Run model inference (ONNX Runtime, CPU-only, tiny models)
- **Data:** In-memory loaded model, pulls artifacts from MinIO on startup
- **gRPC API:** `Predict`, `GetModelInfo`, `HealthCheck`
- **Events published:** `ModelLoaded`, `PredictionCompleted`
- **K8s patterns:** One Deployment per model version. HPA scales on custom metric (inflight requests). Readiness probe = model loaded. Liveness probe = inference latency < threshold.

## Pattern Coverage

| Pattern | Service | What It Teaches |
|---------|---------|-----------------|
| **Saga (Orchestration)** | Pipeline Orchestrator | Distributed transactions, compensation, durability |
| **CQRS** | Model Registry | Read/write separation, projections, eventual consistency |
| **Event Sourcing** | Feature Store | Append-only logs, state reconstruction, temporal queries |
| **Choreography** | Notification Service | Decoupled event reactions, no central coordinator |
| **Event-Driven** | Experiment Tracker | Async processing, back-pressure, batch consumers |
| **Outbox Pattern** | Billing Service | Reliable event publishing, exactly-once semantics |
| **API Gateway** | Inference Gateway | Routing, circuit breaking, rate limiting, canary |
| **Sidecar** | Model Serving | Per-pod patterns, health probes, autoscaling |
| **Centralized Auth** | Auth/IAM | Cross-cutting concerns, token propagation |

## Communication Architecture

### Sync: gRPC

- All APIs defined in `proto/` with Buf linting and breaking change detection
- Unary + server-streaming (for pipeline status watching)
- Deadlines propagated via gRPC metadata
- Interceptor chain: `logging → tracing → auth → recovery`
- Retries with exponential backoff via `grpc-retry` interceptor

### Async: NATS JetStream

```
NATS JetStream Subjects:

goml.models.>              Model Registry events
  goml.models.registered
  goml.models.version.created
  goml.models.archived

goml.pipelines.>           Pipeline Orchestrator events
  goml.pipelines.started
  goml.pipelines.step.completed
  goml.pipelines.step.failed
  goml.pipelines.completed
  goml.pipelines.compensation.triggered

goml.inference.>           Inference Gateway events
  goml.inference.completed
  goml.inference.failed

goml.features.>            Feature Store events
  goml.features.set.created
  goml.features.ingested

goml.billing.>             Billing events
  goml.billing.quota.exceeded
  goml.billing.invoice.generated

goml.notifications.>       Notification delivery events
  goml.notifications.delivered
  goml.notifications.failed
```

### Event Flow Map

```
Producer                Event                    Consumers
─────────────────────────────────────────────────────────────
Model Registry    → ModelRegistered         → Pipeline Orchestrator
                  → ModelVersionCreated     → Experiment Tracker

Pipeline Orch.    → PipelineStarted         → Notification Service
                  → StepCompleted           → Experiment Tracker
                  → PipelineFailed          → Notification Service
                  → ModelDeployed           → Inference Gateway
                                            → Experiment Tracker

Inference Gateway → InferenceCompleted      → Billing (meter usage)
                                            → Experiment Tracker

Feature Store     → FeaturesIngested        → Experiment Tracker

Billing           → QuotaExceeded           → Notification Service
                                            → Inference Gateway
```

### Communication Patterns

| Pattern | Where | Purpose |
|---------|-------|---------|
| **Request-Reply** | gRPC calls | Synchronous queries and commands |
| **Pub-Sub** | NATS subjects | Decoupled event broadcasting |
| **Consumer Groups** | NATS queue groups | Scalable event processing |
| **Outbox** | Billing Service | DB transaction + event atomicity |
| **Dead Letter Queue** | All NATS consumers | Failed messages after N retries |
| **Event Envelope** | All events | `{id, type, source, timestamp, correlation_id, data}` |
| **Idempotency** | All consumers | Duplicate event handling via idempotency key |

## Repository Structure

Go monorepo with workspaces:

```
goml/
├── proto/                          # Single source of truth for ALL APIs
│   ├── buf.yaml                    # Buf for proto linting + breaking changes
│   ├── buf.gen.yaml
│   ├── auth/v1/auth.proto
│   ├── registry/v1/registry.proto
│   ├── inference/v1/inference.proto
│   ├── pipeline/v1/pipeline.proto
│   ├── feature/v1/feature.proto
│   ├── experiment/v1/experiment.proto
│   ├── billing/v1/billing.proto
│   └── notification/v1/notification.proto
│
├── services/                       # Each service is an independent Go module
│   ├── auth/
│   │   ├── cmd/server/main.go
│   │   ├── internal/
│   │   │   ├── handler/           # gRPC handlers
│   │   │   ├── domain/            # Business logic (no framework deps)
│   │   │   ├── repository/        # Data access layer
│   │   │   └── events/            # NATS publishers/subscribers
│   │   ├── migrations/
│   │   ├── Dockerfile
│   │   ├── go.mod
│   │   └── go.sum
│   ├── registry/
│   ├── inference-gateway/
│   ├── pipeline-orchestrator/
│   ├── feature-store/
│   ├── experiment-tracker/
│   ├── billing/
│   ├── notification/
│   └── model-serving/
│
├── pkg/                            # Shared libraries
│   ├── grpcutil/                   # gRPC interceptors, middleware
│   ├── natsutil/                   # NATS connection helpers
│   ├── observability/              # OTel tracer/meter setup
│   ├── auth/                       # JWT validation middleware
│   ├── health/                     # Standardized health checks
│   ├── config/                     # Env-based config loading
│   └── testutil/                   # Shared test helpers
│
├── deploy/
│   ├── helm/                       # Helm chart per service
│   ├── k8s/                        # Namespaces, RBAC, NetworkPolicies
│   ├── terraform/                  # AWS infra (EKS, RDS, S3, etc.)
│   └── skaffold.yaml               # Local K8s dev workflow
│
├── tools/
│   ├── goml-cli/                   # CLI for interacting with the platform
│   └── loadtest/                   # k6 scripts per service
│
├── scripts/
├── docs/
│   ├── adr/                        # Architecture Decision Records
│   └── plans/
│
├── .github/workflows/              # CI/CD per service + shared
├── go.work                         # Go workspace
├── Makefile
├── buf.yaml
└── docker-compose.yaml             # Local infra only
```

### Standards

| Standard | Implementation |
|----------|---------------|
| **API-first** | Proto definitions before code. Buf enforces linting + backward compat |
| **Database per service** | Each service owns its Postgres schema and migrations |
| **Clean Architecture** | handler → domain → repository. Domain has zero framework imports |
| **Go workspace** | `go.work` links modules. Each service builds independently |
| **12-Factor App** | Config from env, stateless, logs to stdout, port binding |
| **ADRs** | Every non-obvious decision in `docs/adr/` |
| **Hermetic builds** | Multi-stage Dockerfiles, pinned deps, distroless base |

## Infrastructure

### Kubernetes Namespaces

```
goml-system    Platform services (auth, registry, gateway, etc.)
goml-models    Model serving workloads (dynamic, per model)
goml-jobs      Training and batch jobs (K8s Jobs)
goml-infra     NATS, Postgres, Redis, MinIO, monitoring
```

Each service has: Deployment, Service, ServiceAccount, ConfigMap, Secret, PDB, NetworkPolicy, ServiceMonitor.

Model serving: one Deployment per model version, HPA on inflight_requests metric.
Training/batch: K8s Jobs managed by Pipeline Orchestrator.

### Observability (Three Pillars)

| Pillar | Tool | Implementation |
|--------|------|---------------|
| **Metrics** | Prometheus + Grafana | RED metrics per service, custom business metrics, per-service dashboards |
| **Logs** | Loki + Promtail | Structured JSON via `slog` to stdout, collected by Promtail |
| **Traces** | Tempo/Jaeger | Distributed traces across all 9 services via OpenTelemetry SDK |

All services instrumented via `pkg/observability/` — single setup function, all three pillars.

Every service exposes:
- RED metrics: `goml_{service}_grpc_requests_total`, `goml_{service}_grpc_duration_seconds`
- Business metrics: `goml_inference_predictions_total{model,version}`, `goml_pipeline_active_sagas`
- Health probes: `/healthz` (liveness), `/readyz` (readiness)

### Service Mesh (Istio)

- mTLS: all service-to-service traffic encrypted
- Traffic management: canary deployments for model versions
- Network policies: strict service-to-service access control
- Retry/timeout: mesh-level resilience on top of application-level

### Local Development

Docker Compose for infrastructure dependencies:
- NATS JetStream, PostgreSQL, Redis, MinIO, Prometheus, Grafana, Tempo, Loki

Skaffold for K8s dev loop: file change → rebuild → redeploy → tail logs.

### AWS Deployment (Terraform)

```
deploy/terraform/
├── modules/
│   ├── eks/           # EKS cluster + node groups
│   ├── rds/           # PostgreSQL per-service instances
│   ├── elasticache/   # Redis
│   ├── s3/            # Model artifact storage
│   ├── nats/          # NATS on EKS via Helm
│   ├── monitoring/    # Prometheus stack via Helm
│   └── networking/    # VPC, subnets, security groups
├── environments/
│   ├── dev/
│   └── prod/
└── backend.tf         # S3 + DynamoDB state locking
```

Budget: ~$100 for a few days on t3.medium nodes, db.t3.micro RDS, cache.t3.micro ElastiCache.

## CI/CD

Per-service pipeline triggered by path filter:

```
push to services/{service}/**
  → Lint (golangci-lint) + Buf lint (proto)     [parallel]
  → Unit tests (go test -race)
  → Integration tests (testcontainers)
  → Build Docker image (multi-stage, distroless)
  → Push to GHCR
  → Helm upgrade (dev Kind cluster in CI)
  → E2E smoke test
```

Plus platform-wide cross-service integration tests.

## Testing Strategy

| Level | Tool | Scope |
|-------|------|-------|
| **Unit** | `go test` | Domain logic, pure functions |
| **Integration** | Testcontainers | Service + real Postgres/NATS/Redis |
| **Contract** | Buf breaking | Proto backward compatibility |
| **Component** | envtest-style | Single service end-to-end |
| **E2E** | Kind cluster | Full workflow across services |
| **Load** | k6 | Throughput, latency under pressure |
| **Chaos** | Chaos Mesh (optional) | Kill pods mid-saga, network partitions |

## Tech Stack Summary

| Category | Technology |
|----------|-----------|
| **Language** | Go |
| **Sync Communication** | gRPC + Buf |
| **Async Communication** | NATS JetStream |
| **Data Stores** | PostgreSQL (per service), Redis, MinIO (S3) |
| **Container Runtime** | Docker (multi-stage, distroless) |
| **Orchestration** | Kubernetes (Kind local, EKS prod) |
| **Service Mesh** | Istio |
| **Observability** | OpenTelemetry → Prometheus + Grafana + Loki + Tempo |
| **CI/CD** | GitHub Actions |
| **IaC** | Terraform |
| **Dev Workflow** | Skaffold + Kind |
| **API Linting** | Buf |
| **DB Migrations** | golang-migrate |
| **Load Testing** | k6 |
