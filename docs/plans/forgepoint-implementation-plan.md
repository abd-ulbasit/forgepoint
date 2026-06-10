# Forgepoint Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a full ML lifecycle platform as 9 microservices in Go — covering saga, CQRS, event sourcing, choreography, outbox, circuit breakers, and more — deployed on Kubernetes. Deliver it as a polished, working product with good UX, including a web UI as a first-class interface alongside the CLI and gRPC/HTTP APIs.

**Architecture:** Go monorepo with workspaces. 9 services communicating via gRPC (sync) and NATS JetStream (async). Each service owns its PostgreSQL database. Clean Architecture (handler → domain → repository). Deployed on Kind locally, EKS for production.

**Tech Stack:** Go, gRPC, Buf, NATS JetStream, PostgreSQL, Redis, MinIO, Docker, Kubernetes, Helm, Skaffold, OpenTelemetry, Prometheus, Grafana, Loki, Tempo, Istio, Terraform, GitHub Actions, k6, Testcontainers

**Design Doc:** `docs/plans/forgepoint-platform-design.md`

---

## Scope Tiers

This plan is intentionally larger than what should be built first. Depth on a focused
core interviews better than breadth across a half-finished 9-service repo. Phases are
tagged:

- **Core** — build to polish. Every distinct distributed-systems pattern, the BFF + web
  UI, full local Kubernetes, observability *with real alerts/SLOs + a runbook*, and CI.
  This alone covers every interview talking point (saga, CQRS, circuit breaker, BFF,
  observability, K8s, IaC-lite) with enough depth to defend on a shared screen.
- **Stretch** — build only if the Core is genuinely polished. Services 6-9 are mostly
  event-consumer *repeats* with diminishing learning return; pick 1-2 for pattern variety
  (Outbox and Event Sourcing are the most interview-worth). EKS/Terraform is high-effort,
  low-novel-learning, and invisible in a portfolio — make it last.

> **Rule:** a recorded demo of a polished local Kind deployment demonstrates the same
> competency as a torn-down EKS cluster nobody can see. Don't let EKS gate the project.

## Phase Overview

| Phase | Tier | Focus | Services/Components | Key Pattern | Depends On |
|-------|------|-------|--------------------|----|------------|
| **0** | Core | Foundation | Repo scaffold, shared libs, local infra | — | Nothing |
| **1** | Core | Auth/IAM | `services/auth` | Centralized Auth, JWT | Phase 0 |
| **2** | Core | Model Registry | `services/registry` | CQRS | Phase 1 |
| **3** | Core | Model Serving | `services/model-serving` | Sidecar, HPA | Phase 2 |
| **4** | Core | Inference Gateway | `services/inference-gateway` | Circuit Breaker, Rate Limiting | Phase 3 |
| **5** | Core | Pipeline Orchestrator | `services/pipeline-orchestrator` | Saga, DAG Execution | Phase 2, 3 |
| **6** | Stretch | Feature Store | `services/feature-store` | Event Sourcing | Phase 1 |
| **7** | Stretch | Experiment Tracker | `services/experiment-tracker` | Event-Driven, Batch Consumer | Phase 1 |
| **8** | Stretch | Billing/Usage | `services/billing` | Outbox Pattern | Phase 1 |
| **9** | Stretch | Notification | `services/notification` | Choreography | Phase 1 |
| **10** | Core | Observability & Mesh | Grafana dashboards, Istio | Three Pillars, mTLS | Phase 1-5 |
| **11** | Core | CI/CD | GitHub Actions | Path-filtered pipelines | Phase 0-5 |
| **12** | Stretch | AWS Deployment | Terraform EKS, RDS, S3 | IaC | Phase 0-11 |
| **13** | Core | E2E & Load Testing | Cross-service tests, k6 | Full lifecycle validation | Phase 1-5 |
| **14** | Core | Web UI (BFF + frontend) | `services/bff`, `web/` dashboard | Backend-for-Frontend, gRPC→SSE | Phase 1-5 |
| **15** | Core | Operability & SRE | SLOs, alert rules, runbook | SLO/error-budget, on-call signal | Phase 10 |

> Recommended Core sequence: **0 → 1 → 2 → 3 → 4 → 5 → 10 → 11 → 13 → 14 → 15**, then
> Stretch (pick 1-2 of 6-9, then 12) only if time allows.

---

## Phase 0: Foundation

> Scaffolds the entire repo, sets up proto tooling, shared libraries, local infrastructure, and K8s dev environment. Everything else depends on this.

---

### Task 0.1: Repository Scaffold

**Files:**
- Create: `go.work`
- Create: `Makefile`
- Create: `.gitignore`
- Create: `.golangci.yml`
- Create: `README.md`

**Step 1: Initialize Go workspace**

```go
// go.work
go 1.24

use (
    ./pkg
    ./services/auth
)
```

We start with only `pkg` and `auth` — services are added to `go.work` as they're built.

**Step 2: Create root Makefile**

Top-level targets:
- `make proto` — generate Go code from proto files via Buf
- `make lint` — run golangci-lint on all modules
- `make test` — run tests across all services
- `make build-all` — build all service binaries
- `make docker-build SVC=auth` — build Docker image for a specific service
- `make up` — start local infra via docker-compose
- `make down` — stop local infra
- `make kind-create` — create local Kind cluster
- `make kind-delete` — delete Kind cluster
- `make skaffold-dev` — start Skaffold dev loop

**Step 3: Create golangci-lint config**

Standard linters: `errcheck`, `govet`, `staticcheck`, `unused`, `gosimple`, `ineffassign`, `misspell`, `gofmt`, `goimports`. Disable `exhaustruct` (too noisy for proto-generated code).

**Step 4: Create .gitignore**

Ignore: `bin/`, `*.out`, `.env`, `vendor/`, `*.exe`, `cover.out`, `gen/` (generated proto code).

**Step 5: Commit**

```bash
git add go.work Makefile .gitignore .golangci.yml README.md
git commit -m "feat: scaffold repository with go workspace and tooling"
```

---

### Task 0.2: Proto Setup with Buf

**Files:**
- Create: `proto/buf.yaml`
- Create: `proto/buf.gen.yaml`
- Create: `proto/buf.lock`
- Create: `proto/forgepoint/common/v1/common.proto` (shared event envelope, pagination, errors)

**Step 1: Install Buf CLI**

Run: `go install github.com/bufbuild/buf/cmd/buf@latest`

Verify: `buf --version`

**Step 2: Create buf.yaml**

```yaml
# proto/buf.yaml
version: v2
modules:
  - path: .
lint:
  use:
    - STANDARD
breaking:
  use:
    - FILE
```

**Step 3: Create buf.gen.yaml**

Generate Go code + gRPC stubs:

```yaml
# proto/buf.gen.yaml
version: v2
plugins:
  - remote: buf.build/protocolbuffers/go
    out: ../gen/go
    opt: paths=source_relative
  - remote: buf.build/grpc/go
    out: ../gen/go
    opt: paths=source_relative
```

**Step 4: Create common proto definitions**

`proto/forgepoint/common/v1/common.proto`:
- `EventEnvelope` message: `id`, `type`, `source`, `timestamp`, `correlation_id`, `data` (bytes)
- `PaginationRequest`: `page_size`, `page_token`
- `PaginationResponse`: `next_page_token`, `total_count`
- `ErrorDetail`: `code`, `message`, `field`

**Step 5: Generate and verify**

Run: `cd proto && buf lint && buf generate`

Verify: `ls ../gen/go/forgepoint/common/v1/` contains generated `.pb.go` files.

**Step 6: Commit**

```bash
git add proto/ gen/
git commit -m "feat: setup buf proto tooling with common definitions"
```

---

### Task 0.3: Shared Library — pkg/config

**Files:**
- Create: `pkg/go.mod`
- Create: `pkg/config/config.go`
- Create: `pkg/config/config_test.go`

**Step 1: Write the failing test**

`pkg/config/config_test.go`:
- Test loading config from env vars with `FP_` prefix
- Test default values when env vars are not set
- Test required fields returning error when missing

**Step 2: Run test to verify it fails**

Run: `cd pkg && go test ./config/ -v`

Expected: FAIL — `config.go` doesn't exist yet.

**Step 3: Implement config loader**

`pkg/config/config.go`:
- `Load[T any](prefix string) (*T, error)` — generic config loader using `envconfig` or manual `os.Getenv`
- Struct tags: `env:"PORT" default:"8080" required:"true"`
- Supports nested structs for service-specific config
- `BaseConfig` struct: `Port`, `GRPCPort`, `LogLevel`, `OTelEndpoint`, `NATSUrl`, `DatabaseURL`

**Step 4: Run test to verify it passes**

Run: `cd pkg && go test ./config/ -v -race`

Expected: PASS

**Step 5: Commit**

```bash
git add pkg/
git commit -m "feat(pkg): add env-based config loader with struct tags"
```

---

### Task 0.4: Shared Library — pkg/observability

**Files:**
- Create: `pkg/observability/tracer.go`
- Create: `pkg/observability/meter.go`
- Create: `pkg/observability/logger.go`
- Create: `pkg/observability/setup.go`
- Create: `pkg/observability/setup_test.go`

**Step 1: Write the failing test**

Test that `Setup()` returns a shutdown function, initializes tracer provider and meter provider.

**Step 2: Run test to verify it fails**

Run: `cd pkg && go test ./observability/ -v`

**Step 3: Implement observability setup**

- `Setup(serviceName string, cfg OTelConfig) (shutdown func(context.Context) error, err error)`
- Initializes OTLP exporter (gRPC) for traces → Tempo
- Initializes Prometheus exporter for metrics
- Creates `slog.Logger` with `service`, `trace_id`, `span_id` attributes
- Returns a single shutdown function that flushes all providers

Key dependencies: `go.opentelemetry.io/otel`, `go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc`, `go.opentelemetry.io/otel/exporters/prometheus`

**Step 4: Run test to verify it passes**

Run: `cd pkg && go test ./observability/ -v -race`

**Step 5: Commit**

```bash
git add pkg/observability/
git commit -m "feat(pkg): add OpenTelemetry observability setup (traces, metrics, logs)"
```

---

### Task 0.5: Shared Library — pkg/grpcutil

**Files:**
- Create: `pkg/grpcutil/interceptors.go`
- Create: `pkg/grpcutil/server.go`
- Create: `pkg/grpcutil/interceptors_test.go`

**Step 1: Write the failing test**

Test the logging interceptor adds trace_id to logs. Test the recovery interceptor catches panics and returns Internal error. Test the auth interceptor extracts bearer token from metadata.

**Step 2: Run test to verify it fails**

Run: `cd pkg && go test ./grpcutil/ -v`

**Step 3: Implement gRPC utilities**

`interceptors.go`:
- `LoggingUnaryInterceptor` — logs method, duration, status code with slog
- `RecoveryUnaryInterceptor` — catches panics, logs stack trace, returns codes.Internal
- `AuthUnaryInterceptor(validator TokenValidator)` — extracts `authorization` metadata, validates token, injects claims into context
- `TracingUnaryInterceptor` — extracts/injects OTel span context (uses otelgrpc)
- Streaming variants for each

`server.go`:
- `NewServer(opts ...ServerOption) *grpc.Server` — creates server with standard interceptor chain: recovery → logging → tracing → auth
- `ServerOption` functional options: `WithAuthValidator()`, `WithReflection()`, `WithHealthCheck()`
- Registers gRPC reflection and health service by default

**Step 4: Run test to verify it passes**

Run: `cd pkg && go test ./grpcutil/ -v -race`

**Step 5: Commit**

```bash
git add pkg/grpcutil/
git commit -m "feat(pkg): add gRPC server utilities with interceptor chain"
```

---

### Task 0.6: Shared Library — pkg/natsutil

**Files:**
- Create: `pkg/natsutil/connection.go`
- Create: `pkg/natsutil/publisher.go`
- Create: `pkg/natsutil/subscriber.go`
- Create: `pkg/natsutil/envelope.go`
- Create: `pkg/natsutil/natsutil_test.go`

**Step 1: Write the failing test (use testcontainers for real NATS)**

Test publishing an event and receiving it on a subscriber. Test event envelope serialization/deserialization. Test consumer group (queue subscription) — 2 subscribers, only one receives each message. Test DLQ — after max retries, message goes to `.dlq` subject.

**Step 2: Run test to verify it fails**

Run: `cd pkg && go test ./natsutil/ -v`

**Step 3: Implement NATS utilities**

`connection.go`:
- `Connect(url string, opts ...nats.Option) (*nats.Conn, nats.JetStreamContext, error)`
- Reconnect handler logging, disconnect handler logging
- Returns both raw conn and JetStream context

`publisher.go`:
- `Publisher` struct with `Publish(ctx context.Context, subject string, event any) error`
- Wraps event in `EventEnvelope` (adds id, timestamp, correlation_id from ctx, source from service name)
- Serializes to JSON
- Publishes via JetStream with deduplication ID (using event envelope ID)

`subscriber.go`:
- `Subscriber` struct with `Subscribe(subject string, handler EventHandler, opts ...SubOption) error`
- `EventHandler = func(ctx context.Context, envelope EventEnvelope) error`
- `SubOption`: `WithQueueGroup(name)`, `WithMaxRetries(n)`, `WithDLQSubject(subject)`
- On handler error: NAK with backoff. After max retries → publish to DLQ subject.
- Extracts correlation_id from envelope, injects into context for tracing.

`envelope.go`:
- `EventEnvelope` struct matching proto definition: `ID`, `Type`, `Source`, `Timestamp`, `CorrelationID`, `Data`
- `NewEnvelope(ctx, eventType, source, data)` — generates UUID for ID, extracts correlation ID from ctx

**Step 4: Run test to verify it passes (requires Docker for testcontainers)**

Run: `cd pkg && go test ./natsutil/ -v -race -timeout 60s`

**Step 5: Commit**

```bash
git add pkg/natsutil/
git commit -m "feat(pkg): add NATS JetStream publisher/subscriber with event envelopes and DLQ"
```

---

### Task 0.7: Shared Library — pkg/health

**Files:**
- Create: `pkg/health/health.go`
- Create: `pkg/health/health_test.go`

**Step 1: Write the failing test**

Test health check handler returns 200 when all checks pass. Test readiness returns 503 when any check fails. Test adding custom checks (e.g., DB ping, NATS connection).

**Step 2: Implement**

- `Handler` struct with `AddCheck(name string, check CheckFunc)`
- `CheckFunc = func(ctx context.Context) error`
- `LivenessHandler() http.HandlerFunc` — returns 200 if process is alive
- `ReadinessHandler() http.HandlerFunc` — runs all checks, returns 200 or 503 with JSON body showing which checks failed

**Step 3: Run test, verify passes, commit**

```bash
git add pkg/health/
git commit -m "feat(pkg): add standardized health check handler"
```

---

### Task 0.8: Shared Library — pkg/testutil

**Files:**
- Create: `pkg/testutil/containers.go`
- Create: `pkg/testutil/grpc.go`
- Create: `pkg/testutil/fixtures.go`

No tests needed — this IS test infrastructure.

**Step 1: Implement test utilities**

`containers.go`:
- `StartPostgres(t *testing.T) (dsn string, cleanup func())` — starts Postgres via testcontainers, runs migrations
- `StartRedis(t *testing.T) (addr string, cleanup func())` — starts Redis via testcontainers
- `StartNATS(t *testing.T) (url string, cleanup func())` — starts NATS via testcontainers
- `StartMinIO(t *testing.T) (endpoint string, cleanup func())` — starts MinIO via testcontainers

`grpc.go`:
- `NewTestGRPCServer(t *testing.T, register func(s *grpc.Server)) (*grpc.ClientConn, func())`
- Starts an in-process gRPC server with bufconn, returns a client connection

`fixtures.go`:
- Common test data: sample model metadata, sample user, sample API key

**Step 2: Commit**

```bash
git add pkg/testutil/
git commit -m "feat(pkg): add test utilities (testcontainers, in-process gRPC, fixtures)"
```

---

### Task 0.9: Docker Compose for Local Infrastructure

**Files:**
- Create: `docker-compose.yaml`

**Step 1: Create compose file**

Services:
- `nats`: nats:latest with JetStream enabled (`-js`), ports 4222, 8222
- `postgres`: postgres:17, port 5432, init script creates per-service databases (fp_auth, fp_registry, fp_pipeline, fp_feature, fp_experiment, fp_billing, fp_notification)
- `redis`: redis:7, port 6379
- `minio`: minio/minio, ports 9000 (API), 9001 (console), creates `fp-models` bucket on start
- `prometheus`: prom/prometheus, port 9090, mounts `deploy/prometheus/prometheus.yml`
- `grafana`: grafana/grafana, port 3000, auto-provisions Prometheus + Tempo + Loki datasources
- `tempo`: grafana/tempo, port 4317 (OTLP gRPC), 3200 (HTTP)
- `loki`: grafana/loki, port 3100

Create: `deploy/prometheus/prometheus.yml` — scrape config for all services.
Create: `deploy/grafana/provisioning/datasources/datasources.yaml` — auto-provision datasources.
Create: `scripts/init-postgres.sh` — creates per-service databases.

**Step 2: Start and verify**

Run: `docker compose up -d`

Verify:
- `curl http://localhost:8222/healthz` (NATS)
- `psql -h localhost -U fp -c "SELECT 1"` (Postgres)
- `redis-cli -h localhost ping` (Redis)
- `curl http://localhost:9000/minio/health/live` (MinIO)
- `curl http://localhost:9090/-/healthy` (Prometheus)
- `curl http://localhost:3000/api/health` (Grafana)

**Step 3: Commit**

```bash
git add docker-compose.yaml deploy/prometheus/ deploy/grafana/ scripts/
git commit -m "feat: add docker-compose for local infrastructure stack"
```

---

### Task 0.10: Kind Cluster + Skaffold Setup

**Files:**
- Create: `deploy/kind/kind-config.yaml`
- Create: `deploy/skaffold.yaml`
- Create: `deploy/k8s/namespaces.yaml`

**Step 1: Create Kind config**

Kind cluster with:
- 1 control plane, 2 worker nodes
- Extra port mappings for NodePort access (30000-30010)
- containerd config for local image registry

**Step 2: Create namespace manifests**

```yaml
# deploy/k8s/namespaces.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: fp-system
  labels:
    istio-injection: enabled
---
apiVersion: v1
kind: Namespace
metadata:
  name: fp-models
---
apiVersion: v1
kind: Namespace
metadata:
  name: fp-jobs
---
apiVersion: v1
kind: Namespace
metadata:
  name: fp-infra
```

**Step 3: Create Skaffold config**

Skeleton with the auth service as first module. More services added as they're built.

**Step 4: Create cluster and verify**

Run: `kind create cluster --config deploy/kind/kind-config.yaml --name fp`

Run: `kubectl apply -f deploy/k8s/namespaces.yaml`

Verify: `kubectl get ns` shows fp-system, fp-models, fp-jobs, fp-infra.

**Step 5: Commit**

```bash
git add deploy/kind/ deploy/skaffold.yaml deploy/k8s/
git commit -m "feat: add Kind cluster config, namespaces, and Skaffold skeleton"
```

---

### Task 0.11: ADR for Foundational Decisions

**Files:**
- Create: `docs/adr/001-monorepo-with-go-workspaces.md`
- Create: `docs/adr/002-grpc-sync-nats-async.md`
- Create: `docs/adr/003-clean-architecture-per-service.md`

Write concise ADRs (Status, Context, Decision, Consequences format) for:
1. Why monorepo with Go workspaces over polyrepo
2. Why gRPC + NATS JetStream over REST + Kafka
3. Why Clean Architecture (handler → domain → repository) per service

**Commit:**

```bash
git add docs/adr/
git commit -m "docs: add foundational architecture decision records"
```

---

## Phase 1: Auth/IAM Service

> First service. Every other service depends on it for token validation. Teaches centralized auth patterns, JWT, RBAC, and gRPC interceptors.

---

### Task 1.1: Auth Proto Definitions

**Files:**
- Create: `proto/forgepoint/auth/v1/auth.proto`

**Step 1: Define the auth service proto**

Messages:
- `User`: id, email, name, team, role, created_at
- `APIKey`: id, key_prefix, user_id, scopes, expires_at, created_at
- `Role`: id, name, permissions[]
- `Permission`: resource (e.g., "models"), action (e.g., "write")
- `TokenClaims`: user_id, email, team, role, scopes[], exp, iat

RPCs:
- `CreateUser(CreateUserRequest) returns (User)`
- `CreateAPIKey(CreateAPIKeyRequest) returns (CreateAPIKeyResponse)` — returns full key only once
- `RevokeAPIKey(RevokeAPIKeyRequest) returns (google.protobuf.Empty)`
- `ValidateToken(ValidateTokenRequest) returns (TokenClaims)` — called by other services' auth interceptors
- `CheckPermission(CheckPermissionRequest) returns (CheckPermissionResponse)` — resource + action → allowed/denied
- `AssignRole(AssignRoleRequest) returns (google.protobuf.Empty)`
- `ListUsers(ListUsersRequest) returns (ListUsersResponse)` — paginated
- `Login(LoginRequest) returns (LoginResponse)` — returns JWT

**Step 2: Lint and generate**

Run: `cd proto && buf lint && buf generate`

Verify: `ls ../gen/go/forgepoint/auth/v1/` contains `auth.pb.go` and `auth_grpc.pb.go`.

**Step 3: Commit**

```bash
git add proto/forgepoint/auth/ gen/go/forgepoint/auth/
git commit -m "feat(auth): define auth service proto with user, API key, and RBAC RPCs"
```

---

### Task 1.2: Auth Service Scaffold

**Files:**
- Create: `services/auth/go.mod`
- Create: `services/auth/cmd/server/main.go`
- Create: `services/auth/internal/handler/auth_handler.go`
- Create: `services/auth/internal/domain/auth_service.go`
- Create: `services/auth/internal/domain/models.go`
- Create: `services/auth/internal/repository/interfaces.go`
- Update: `go.work` — add `./services/auth`

**Step 1: Create go.mod for auth service**

```
module github.com/abd-ulbasit/forgepoint/services/auth

go 1.24
```

**Step 2: Create Clean Architecture layers**

`internal/domain/models.go`:
- Domain models: `User`, `APIKey`, `Role`, `Permission`, `TokenClaims`
- NO proto imports — pure Go structs

`internal/domain/auth_service.go`:
- `AuthService` interface:
  - `CreateUser(ctx, input) (User, error)`
  - `CreateAPIKey(ctx, userID, scopes, expiresAt) (APIKey, rawKey, error)`
  - `ValidateToken(ctx, token) (TokenClaims, error)`
  - `CheckPermission(ctx, userID, resource, action) (bool, error)`
  - `Login(ctx, email, password) (jwtToken, error)`
- `authService` struct implementing the interface
- Constructor: `NewAuthService(repo UserRepository, keyRepo APIKeyRepository, jwtSecret []byte)`

`internal/repository/interfaces.go`:
- `UserRepository` interface: `Create`, `GetByID`, `GetByEmail`, `List`
- `APIKeyRepository` interface: `Create`, `GetByKeyHash`, `Revoke`, `ListByUser`
- `RoleRepository` interface: `Create`, `GetByName`, `AssignToUser`, `GetUserRoles`

`internal/handler/auth_handler.go`:
- `AuthHandler` struct implementing the generated `AuthServiceServer` interface
- Each RPC method: converts proto → domain, calls service, converts domain → proto
- Constructor: `NewAuthHandler(svc domain.AuthService)`

`cmd/server/main.go`:
- Load config via `pkg/config`
- Setup observability via `pkg/observability`
- Connect to Postgres
- Create repository, service, handler
- Create gRPC server via `pkg/grpcutil.NewServer()`
- Register auth handler
- Start health check HTTP server on separate port
- Graceful shutdown on SIGTERM

**Step 3: Verify it compiles**

Run: `cd services/auth && go build ./...`

**Step 4: Commit**

```bash
git add services/auth/ go.work
git commit -m "feat(auth): scaffold auth service with clean architecture layers"
```

---

### Task 1.3: Auth Domain Logic + Tests

**Files:**
- Create: `services/auth/internal/domain/auth_service_impl.go`
- Create: `services/auth/internal/domain/auth_service_test.go`
- Create: `services/auth/internal/domain/jwt.go`
- Create: `services/auth/internal/domain/jwt_test.go`

**Step 1: Write failing tests for JWT**

`jwt_test.go`:
- `TestGenerateToken_ValidClaims` — generates token, decodes, verifies claims match
- `TestValidateToken_ExpiredToken` — creates expired token, validation returns error
- `TestValidateToken_InvalidSignature` — tamper with token, validation fails
- `TestValidateToken_ValidToken` — happy path

**Step 2: Run tests to verify they fail**

Run: `cd services/auth && go test ./internal/domain/ -v -run TestJWT`

**Step 3: Implement JWT module**

`jwt.go`:
- `GenerateToken(claims TokenClaims, secret []byte, ttl time.Duration) (string, error)`
- `ValidateToken(tokenString string, secret []byte) (*TokenClaims, error)`
- Uses `golang-jwt/jwt/v5`

**Step 4: Run tests, verify pass**

**Step 5: Write failing tests for AuthService**

`auth_service_test.go`:
- Use mock repositories (define mocks in test file or separate mock package)
- `TestCreateUser_Success` — calls repo.Create, returns user
- `TestCreateUser_DuplicateEmail` — repo returns duplicate error, service wraps it
- `TestCreateAPIKey_Success` — generates key, hashes it, stores hash, returns raw key
- `TestCreateAPIKey_InvalidScopes` — rejects invalid scope strings
- `TestValidateToken_WithAPIKey` — validates API key by looking up hash
- `TestCheckPermission_Allowed` — user has matching role+permission
- `TestCheckPermission_Denied` — user lacks permission
- `TestLogin_Success` — verifies password hash, returns JWT
- `TestLogin_WrongPassword` — returns auth error

**Step 6: Implement AuthService**

`auth_service_impl.go`:
- `CreateUser`: hash password with bcrypt, store via repo
- `CreateAPIKey`: generate 32-byte random key, SHA256 hash for storage, return raw key once
- `ValidateToken`: try JWT first, if fails try API key lookup by prefix + hash verification
- `CheckPermission`: load user roles, check if any role has the requested permission
- `Login`: get user by email, bcrypt compare, generate JWT

**Step 7: Run all tests, verify pass**

Run: `cd services/auth && go test ./internal/domain/ -v -race`

**Step 8: Commit**

```bash
git add services/auth/internal/domain/
git commit -m "feat(auth): implement auth domain logic with JWT and API key validation"
```

---

### Task 1.4: Auth Repository (PostgreSQL)

**Files:**
- Create: `services/auth/internal/repository/postgres/user_repo.go`
- Create: `services/auth/internal/repository/postgres/apikey_repo.go`
- Create: `services/auth/internal/repository/postgres/role_repo.go`
- Create: `services/auth/internal/repository/postgres/postgres_test.go`
- Create: `services/auth/migrations/001_initial.up.sql`
- Create: `services/auth/migrations/001_initial.down.sql`

**Step 1: Write migration**

`001_initial.up.sql`:
```sql
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    team VARCHAR(255) NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE roles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(100) UNIQUE NOT NULL,
    permissions JSONB NOT NULL DEFAULT '[]'
);

CREATE TABLE user_roles (
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    role_id UUID REFERENCES roles(id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, role_id)
);

CREATE TABLE api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key_prefix VARCHAR(8) NOT NULL,
    key_hash VARCHAR(64) NOT NULL UNIQUE,
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    scopes TEXT[] NOT NULL DEFAULT '{}',
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_api_keys_prefix ON api_keys(key_prefix);
CREATE INDEX idx_api_keys_user ON api_keys(user_id);

-- Seed default roles
INSERT INTO roles (name, permissions) VALUES
    ('admin', '[{"resource":"*","action":"*"}]'),
    ('engineer', '[{"resource":"models","action":"read"},{"resource":"models","action":"write"},{"resource":"pipelines","action":"*"}]'),
    ('viewer', '[{"resource":"models","action":"read"},{"resource":"experiments","action":"read"}]');
```

**Step 2: Write failing integration tests (testcontainers)**

`postgres_test.go`:
- Use `pkg/testutil.StartPostgres(t)` to spin up real Postgres
- Run migrations with golang-migrate
- `TestUserRepo_CreateAndGet` — create user, get by ID, verify fields
- `TestUserRepo_DuplicateEmail` — create same email twice, expect error
- `TestAPIKeyRepo_CreateAndLookup` — create key, lookup by hash
- `TestRoleRepo_AssignAndCheck` — assign role to user, check permissions

**Step 3: Run tests to verify they fail**

Run: `cd services/auth && go test ./internal/repository/postgres/ -v -timeout 120s`

**Step 4: Implement repositories**

Each repo: takes `*sql.DB`, uses `database/sql` with `pgx` driver. Implement all methods from the interface.

**Step 5: Run tests, verify pass**

**Step 6: Commit**

```bash
git add services/auth/internal/repository/ services/auth/migrations/
git commit -m "feat(auth): implement PostgreSQL repositories with migrations"
```

---

### Task 1.5: Auth gRPC Handler + Integration Test

**Files:**
- Modify: `services/auth/internal/handler/auth_handler.go`
- Create: `services/auth/internal/handler/auth_handler_test.go`

**Step 1: Write failing integration tests**

`auth_handler_test.go`:
- Use `pkg/testutil.NewTestGRPCServer` for in-process gRPC
- Wire up real domain service with mock repos (or testcontainers for full integration)
- `TestCreateUser_gRPC` — call CreateUser RPC, verify response
- `TestLogin_gRPC` — create user, login, get valid JWT
- `TestValidateToken_gRPC` — login, then validate the returned token
- `TestCreateAPIKey_gRPC` — create key, verify prefix returned

**Step 2: Run tests, verify fail**

**Step 3: Implement handler**

Each RPC method:
1. Validate request (required fields)
2. Convert proto request → domain input
3. Call domain service method
4. Convert domain result → proto response
5. Return response or gRPC status error

**Step 4: Run tests, verify pass**

**Step 5: Commit**

```bash
git add services/auth/internal/handler/
git commit -m "feat(auth): implement gRPC handler with request validation"
```

---

### Task 1.6: Auth NATS Event Publishing

**Files:**
- Create: `services/auth/internal/events/publisher.go`
- Create: `services/auth/internal/events/publisher_test.go`

**Step 1: Write failing test**

- Start NATS via testcontainers
- Subscribe to `fp.auth.>`
- Call publisher methods, verify events received with correct envelope

**Step 2: Implement**

`publisher.go`:
- `AuthEventPublisher` wrapping `pkg/natsutil.Publisher`
- `PublishUserCreated(ctx, user)` — publishes to `fp.auth.user.created`
- `PublishAPIKeyRotated(ctx, keyID, userID)` — publishes to `fp.auth.apikey.rotated`
- Wire into domain service — service calls publisher after successful operations

**Step 3: Run tests, verify pass, commit**

```bash
git add services/auth/internal/events/
git commit -m "feat(auth): add NATS event publishing for user and API key events"
```

---

### Task 1.7: Auth Dockerfile + Helm Chart

**Files:**
- Create: `services/auth/Dockerfile`
- Create: `deploy/helm/fp-auth/Chart.yaml`
- Create: `deploy/helm/fp-auth/values.yaml`
- Create: `deploy/helm/fp-auth/templates/deployment.yaml`
- Create: `deploy/helm/fp-auth/templates/service.yaml`
- Create: `deploy/helm/fp-auth/templates/configmap.yaml`
- Create: `deploy/helm/fp-auth/templates/secret.yaml`
- Create: `deploy/helm/fp-auth/templates/pdb.yaml`
- Create: `deploy/helm/fp-auth/templates/serviceaccount.yaml`
- Create: `deploy/helm/fp-auth/templates/servicemonitor.yaml`
- Create: `deploy/helm/fp-auth/templates/networkpolicy.yaml`

**Step 1: Create multi-stage Dockerfile**

```dockerfile
FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /auth ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /auth /auth
COPY migrations/ /migrations/
USER nonroot:nonroot
EXPOSE 8080 9090
ENTRYPOINT ["/auth"]
```

**Step 2: Create Helm chart**

Standard K8s resources: Deployment (2 replicas), Service (ClusterIP, gRPC port), ConfigMap (env vars), Secret (DB password, JWT secret), PDB (minAvailable: 1), ServiceAccount, ServiceMonitor (Prometheus scrape), NetworkPolicy (allow ingress only from fp-system namespace).

**Step 3: Build and test locally**

Run: `docker build -t fp-auth:dev -f services/auth/Dockerfile services/auth/`

Run: `helm template fp-auth deploy/helm/fp-auth/ | kubectl apply --dry-run=client -f -`

**Step 4: Commit**

```bash
git add services/auth/Dockerfile deploy/helm/fp-auth/
git commit -m "feat(auth): add Dockerfile and Helm chart with full K8s resources"
```

---

### Task 1.8: Deploy Auth to Kind + Smoke Test

**Step 1: Build and load image into Kind**

```bash
docker build -t fp-auth:dev -f services/auth/Dockerfile services/auth/
kind load docker-image fp-auth:dev --name fp
```

**Step 2: Deploy with Helm**

```bash
helm install fp-auth deploy/helm/fp-auth/ \
  --namespace fp-system \
  --set image.repository=fp-auth \
  --set image.tag=dev
```

**Step 3: Verify**

```bash
kubectl get pods -n fp-system -l app=fp-auth
kubectl logs -n fp-system -l app=fp-auth
```

Port-forward and smoke test:
```bash
kubectl port-forward -n fp-system svc/fp-auth 8080:8080
grpcurl -plaintext localhost:8080 list
grpcurl -plaintext -d '{"email":"test@fp.io","name":"Test","password":"secret123","team":"platform"}' \
  localhost:8080 forgepoint.auth.v1.AuthService/CreateUser
```

**Step 4: Commit any fixes**

```bash
git commit -m "fix(auth): deployment fixes from Kind smoke test"
```

---

## Phase 2: Model Registry Service (CQRS)

> The CQRS pattern: writes go to PostgreSQL, events flow through NATS, read projections are built in Redis. Demonstrates eventual consistency.

---

### Task 2.1: Registry Proto Definitions

**Files:**
- Create: `proto/forgepoint/registry/v1/registry.proto`

Messages: `Model`, `ModelVersion`, `ModelArtifact`, `ModelTag`

RPCs:
- `RegisterModel(RegisterModelRequest) returns (Model)`
- `GetModel(GetModelRequest) returns (Model)` — reads from Redis projection
- `ListModels(ListModelsRequest) returns (ListModelsResponse)` — paginated, reads from Redis
- `SearchByTag(SearchByTagRequest) returns (ListModelsResponse)`
- `CreateVersion(CreateVersionRequest) returns (ModelVersion)`
- `GetVersion(GetVersionRequest) returns (ModelVersion)`
- `ArchiveModel(ArchiveModelRequest) returns (google.protobuf.Empty)`
- `GetUploadURL(GetUploadURLRequest) returns (GetUploadURLResponse)` — presigned S3/MinIO URL

Lint, generate, commit.

---

### Task 2.2: Registry Service Scaffold

Same Clean Architecture pattern as auth:
- `services/registry/cmd/server/main.go`
- `services/registry/internal/handler/` — gRPC handlers
- `services/registry/internal/domain/` — business logic + interfaces
- `services/registry/internal/repository/` — Postgres (write), Redis (read)
- `services/registry/internal/events/` — NATS publisher + projection consumer
- `services/registry/internal/storage/` — MinIO/S3 client for artifacts
- `services/registry/migrations/`

Update `go.work` to include `./services/registry`.

---

### Task 2.3: Registry Write Path (PostgreSQL)

**Files:**
- Create: `services/registry/migrations/001_initial.up.sql`
- Create: `services/registry/internal/repository/postgres/write_repo.go`
- Create: `services/registry/internal/repository/postgres/write_repo_test.go`

Tables: `models` (id, name, description, owner_id, team, framework, task_type, created_at, updated_at, archived_at), `model_versions` (id, model_id, version, description, artifact_path, metrics JSONB, status, created_at), `model_tags` (model_id, key, value).

TDD: write tests first with testcontainers, then implement.

---

### Task 2.4: Registry Read Path (Redis Projection)

**Files:**
- Create: `services/registry/internal/repository/redis/read_repo.go`
- Create: `services/registry/internal/repository/redis/read_repo_test.go`
- Create: `services/registry/internal/events/projection.go`

**Key CQRS implementation:**

`projection.go`:
- Subscribes to `fp.models.>` NATS subjects
- On `ModelRegistered`: serialize model to JSON, store in Redis as `model:{id}`, add to `models:list` sorted set, update `models:tag:{key}:{value}` sets for tag-based search
- On `ModelVersionCreated`: update `model:{id}:latest_version`, add to `model:{id}:versions` list
- On `ModelArchived`: remove from `models:list`, keep in `model:{id}` (soft delete)

`read_repo.go`:
- `GetModel(id) → Model` — reads from `model:{id}`
- `ListModels(page, pageSize) → []Model` — reads from `models:list` sorted set with ZRANGE
- `SearchByTag(key, value) → []Model` — reads from `models:tag:{key}:{value}` set, then fetches each model

TDD: test projection updates Redis correctly, test read repo queries Redis correctly.

---

### Task 2.5: Registry Domain + MinIO Storage

**Files:**
- Create: `services/registry/internal/domain/registry_service.go`
- Create: `services/registry/internal/domain/registry_service_test.go`
- Create: `services/registry/internal/storage/minio_store.go`
- Create: `services/registry/internal/storage/minio_store_test.go`

Domain service:
- `RegisterModel`: write to Postgres, publish `ModelRegistered` event
- `CreateVersion`: validate model exists, generate presigned upload URL, write version to Postgres, publish `ModelVersionCreated`
- `GetModel`: read from Redis (read path)
- `ListModels`: read from Redis
- `ArchiveModel`: update Postgres, publish `ModelArchived`

MinIO store:
- `GenerateUploadURL(bucket, key, expiry) → presignedURL`
- `GenerateDownloadURL(bucket, key, expiry) → presignedURL`

TDD throughout.

---

### Task 2.6: Registry Handler, Dockerfile, Helm, Deploy

Follow same pattern as auth (Task 1.5 → 1.8):
- Implement gRPC handler with tests
- Create Dockerfile (multi-stage, distroless)
- Create Helm chart (deploy/helm/fp-registry/)
- Deploy to Kind, smoke test with grpcurl
- Verify CQRS: register a model → check NATS event → check Redis projection → query via read path

---

## Phase 3: Model Serving Service

> Loads ONNX models and serves predictions. One deployment per model version. HPA on custom metrics.

---

### Task 3.1: Serving Proto Definitions

**Files:**
- Create: `proto/forgepoint/serving/v1/serving.proto`

Messages: `PredictRequest` (model_name, version, inputs as map<string, TensorData>), `PredictResponse` (outputs, latency_ms), `TensorData` (shape[], data bytes, dtype), `ModelInfo` (name, version, input_schema, output_schema, loaded_at)

RPCs:
- `Predict(PredictRequest) returns (PredictResponse)`
- `GetModelInfo(GetModelInfoRequest) returns (ModelInfo)`
- `HealthCheck(HealthCheckRequest) returns (HealthCheckResponse)`

---

### Task 3.2: ONNX Runtime Integration

**Files:**
- Create: `services/model-serving/internal/runtime/onnx.go`
- Create: `services/model-serving/internal/runtime/onnx_test.go`

Use `onnxruntime-go` binding (or `github.com/yalue/onnxruntime_go`):
- `LoadModel(path string) (*Model, error)` — loads ONNX model from disk
- `Predict(inputs map[string]Tensor) (map[string]Tensor, error)` — runs inference
- Include a tiny test model (e.g., iris classifier, ~5KB ONNX file) in `services/model-serving/testdata/`

TDD: load test model, predict, verify output shape and values.

---

### Task 3.3: Serving Domain + Handler + gRPC Server

- Domain: `ServingService` that manages loaded model, routes predict calls
- Handler: gRPC handler calling domain service
- Main: on startup, download model artifact from MinIO (model name + version from env vars), load into ONNX runtime, mark ready
- Health: readiness = model loaded, liveness = last inference < timeout threshold

TDD, Dockerfile, Helm chart, deploy to Kind.

---

## Phase 4: Inference Gateway Service

> Routes external inference requests to model serving instances. Circuit breaker, rate limiting, traffic splitting.

---

### Task 4.1: Inference Gateway Proto + HTTP API

**Files:**
- Create: `proto/forgepoint/inference/v1/inference.proto`
- Create: `services/inference-gateway/internal/handler/http_handler.go`

Proto (internal gRPC to model serving):
- Reuses serving proto for downstream calls

HTTP API (external-facing):
- `POST /v1/models/{model_name}/predict` — JSON body with inputs
- `POST /v1/models/{model_name}/versions/{version}/predict` — specific version
- `GET /v1/models/{model_name}/info` — model metadata
- Response includes: outputs, model version used, latency_ms, request_id

---

### Task 4.2: Routing Table + NATS Consumer

**Files:**
- Create: `services/inference-gateway/internal/router/router.go`
- Create: `services/inference-gateway/internal/router/router_test.go`
- Create: `services/inference-gateway/internal/events/subscriber.go`

Router:
- `RouteTable` struct: model_name → list of `{version, address, weight}`
- Consumes `fp.pipelines.model.deployed` → adds route
- Consumes `fp.pipelines.model.undeployed` → removes route
- Supports weighted routing (e.g., canary: v1=90%, v2=10%)
- Thread-safe (RWMutex or atomic swap)

TDD: test route addition, removal, weighted selection.

---

### Task 4.3: Circuit Breaker

**Files:**
- Create: `services/inference-gateway/internal/resilience/circuit_breaker.go`
- Create: `services/inference-gateway/internal/resilience/circuit_breaker_test.go`

3-state circuit breaker (closed → open → half-open):
- Configurable failure threshold, success threshold, timeout
- Per-backend (each model version endpoint has its own breaker)
- Metrics: `fp_gateway_circuit_breaker_state{model, version}` gauge

TDD: test state transitions, concurrent access, half-open probe.

---

### Task 4.4: Rate Limiter

**Files:**
- Create: `services/inference-gateway/internal/resilience/rate_limiter.go`
- Create: `services/inference-gateway/internal/resilience/rate_limiter_test.go`

Token bucket rate limiter:
- Per-API-key rate limiting (backed by Redis for distributed)
- Configurable rate + burst per key
- Returns `429 Too Many Requests` with `Retry-After` header

TDD: test token bucket behavior, burst, distributed consistency.

---

### Task 4.5: Traffic Splitter + Bulkhead

**Files:**
- Create: `services/inference-gateway/internal/resilience/traffic_splitter.go`
- Create: `services/inference-gateway/internal/resilience/bulkhead.go`

Traffic splitter:
- Weighted random selection based on route weights (canary support)

Bulkhead:
- Per-model semaphore limiting concurrent requests (prevent one model from starving others)
- Configurable max concurrent per model

TDD for both.

---

### Task 4.6: Gateway Handler, Dockerfile, Helm, Deploy

Wire everything together:
- HTTP handler: parse request → auth check → rate limit → route → circuit breaker → bulkhead → forward to model serving via gRPC → return response
- Publish `InferenceCompleted` event to NATS (async, non-blocking)
- Dockerfile, Helm chart, deploy to Kind
- Smoke test: register model → deploy → send predict request through gateway

---

## Phase 5: Pipeline Orchestrator Service (Saga)

> The star service. Generic workflow engine supporting sagas (with compensation) and DAG execution. Durable state, crash recovery.

---

### Task 5.1: Pipeline Proto Definitions

**Files:**
- Create: `proto/forgepoint/pipeline/v1/pipeline.proto`

Messages:
- `PipelineDefinition`: id, name, type (DEPLOYMENT_SAGA | TRAINING_DAG | BATCH_INFERENCE), steps[], created_by
- `Step`: id, name, type (VALIDATE | BUILD | DEPLOY | CANARY | PROMOTE | TRAIN | EVALUATE | REGISTER | CUSTOM), config map, depends_on[] (for DAGs), compensation_step_id (for sagas)
- `Execution`: id, pipeline_id, status (PENDING | RUNNING | COMPLETED | FAILED | COMPENSATING | CANCELLED), current_step, started_at, completed_at
- `StepExecution`: id, execution_id, step_id, status, started_at, completed_at, output, error

RPCs:
- `CreatePipeline(CreatePipelineRequest) returns (PipelineDefinition)`
- `TriggerExecution(TriggerExecutionRequest) returns (Execution)`
- `GetExecution(GetExecutionRequest) returns (Execution)`
- `CancelExecution(CancelExecutionRequest) returns (Execution)`
- `WatchExecution(WatchExecutionRequest) returns (stream ExecutionEvent)` — server-streaming
- `ListExecutions(ListExecutionsRequest) returns (ListExecutionsResponse)`

---

### Task 5.2: Saga State Machine

**Files:**
- Create: `services/pipeline-orchestrator/internal/domain/saga.go`
- Create: `services/pipeline-orchestrator/internal/domain/saga_test.go`

Core saga implementation:
- `SagaOrchestrator` struct: manages saga lifecycle
- State machine: `PENDING → RUNNING → step1 → step2 → ... → COMPLETED`
- On step failure: transitions to `COMPENSATING → comp_stepN → comp_stepN-1 → ... → FAILED`
- Each step execution persisted to DB BEFORE execution (write-ahead)
- On crash recovery: load last persisted state, resume from last completed step
- `ExecuteStep(ctx, step) (output, error)` — calls appropriate step executor

TDD: test happy path completion, failure at each step triggers correct compensation order, crash recovery resumes correctly.

---

### Task 5.3: DAG Execution Engine

**Files:**
- Create: `services/pipeline-orchestrator/internal/domain/dag.go`
- Create: `services/pipeline-orchestrator/internal/domain/dag_test.go`

DAG execution:
- Topological sort of steps based on `depends_on`
- Parallel execution of independent steps (steps with all deps met run concurrently)
- Fan-out/fan-in: multiple workers for parallel steps, barrier synchronization
- Step output passed as input to dependent steps

TDD: test linear DAG, fan-out/fan-in, cycle detection, failure propagation.

---

### Task 5.4: Step Executors

**Files:**
- Create: `services/pipeline-orchestrator/internal/executor/validate.go`
- Create: `services/pipeline-orchestrator/internal/executor/deploy.go`
- Create: `services/pipeline-orchestrator/internal/executor/canary.go`
- Create: `services/pipeline-orchestrator/internal/executor/train.go`
- Create: `services/pipeline-orchestrator/internal/executor/executor.go`
- Create: `services/pipeline-orchestrator/internal/executor/executor_test.go`

`executor.go`:
- `StepExecutor` interface: `Execute(ctx, step, input) (output, error)`, `Compensate(ctx, step, input) error`
- `ExecutorRegistry` map: step type → executor implementation

Individual executors:
- `ValidateExecutor`: calls Model Registry to verify model exists and is valid
- `DeployExecutor`: creates K8s Deployment for model serving (calls K8s API)
- `CanaryExecutor`: updates Inference Gateway routing weights via gRPC/event
- `TrainExecutor`: creates K8s Job for training workload
- Each has a `Compensate` method: destroy what was created

TDD with mocked K8s client and service clients.

---

### Task 5.5: Pipeline Repository + Migrations

Postgres tables: `pipeline_definitions`, `pipeline_steps`, `executions`, `step_executions`

`step_executions` tracks saga state — each row is a checkpoint. On crash recovery, query: `SELECT * FROM step_executions WHERE execution_id = ? ORDER BY started_at DESC LIMIT 1`.

TDD with testcontainers.

---

### Task 5.6: Pipeline Handler, Events, Dockerfile, Helm, Deploy

- gRPC handler with server-streaming for `WatchExecution`
- NATS events: `PipelineStarted`, `StepCompleted`, `StepFailed`, `CompensationTriggered`, `ModelDeployed`
- Dockerfile, Helm chart (NOTE: this service uses leader election — only one instance runs sagas at a time)
- Deploy to Kind

**Smoke test the full saga:**
1. Register a model (via registry service)
2. Create a deployment pipeline
3. Trigger execution
4. Watch saga progress through: validate → deploy → canary → promote
5. Verify model is now routable through inference gateway

---

## Phase 6: Feature Store Service (Event Sourcing)

> All mutations stored as events. Current state = replay. Point-in-time snapshots.

---

### Task 6.1: Feature Store Proto + Scaffold

Proto RPCs: `CreateFeatureSet`, `IngestFeatures`, `GetOnlineFeatures`, `GetHistoricalFeatures`

### Task 6.2: Event Store (Append-Only PostgreSQL)

Tables:
- `feature_events` (id, feature_set_id, event_type, data JSONB, version BIGINT, created_at) — append-only, never updated
- `feature_sets` (id, name, schema JSONB, created_at) — metadata only

Event types: `FeatureSetCreated`, `FeaturesIngested`, `FeatureSchemaUpdated`

Key pattern: every mutation appends to `feature_events`. To get current state, replay events for a feature set. For performance, maintain materialized views.

### Task 6.3: Materialized Views (Redis Online + Postgres Offline)

- **Online (Redis):** After each `FeaturesIngested` event, update Redis hash `feature:{set_id}:{entity_id}` with latest feature values. Serves low-latency lookups.
- **Offline (Postgres):** Materialized view query: `SELECT DISTINCT ON (entity_id) * FROM feature_events WHERE feature_set_id = ? ORDER BY entity_id, version DESC`. Serves batch/historical queries.
- **Rebuild:** can replay ALL events from scratch to rebuild both views (demonstrates event sourcing recovery).

### Task 6.4: Historical Queries (Point-in-Time)

`GetHistoricalFeatures(feature_set_id, entity_ids[], timestamp)`:
- Query event store: for each entity, get the latest event BEFORE the given timestamp
- This enables reproducible ML training — "what were the features at the time this model was trained?"

TDD, handler, Dockerfile, Helm, deploy.

---

## Phase 7: Experiment Tracker Service (Event-Driven)

> Async ingestion of metrics via NATS. Batch writes. No sync path in the hot inference loop.

---

### Task 7.1: Experiment Tracker Proto + Scaffold

Proto RPCs: `CreateExperiment`, `StartRun`, `LogMetrics`, `LogParameters`, `CompareRuns`, `GetRun`

### Task 7.2: NATS Batch Consumer

Key pattern: subscribe to `fp.inference.completed`, buffer events, flush to DB in batches every N seconds or when buffer reaches M events. Demonstrates back-pressure handling — if DB is slow, buffer grows (up to limit), then NAK to slow down NATS delivery.

### Task 7.3: Metrics Storage (Time-Partitioned)

Postgres with time-partitioned `metrics` table:
```sql
CREATE TABLE metrics (
    id UUID DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES runs(id),
    key VARCHAR(255) NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    step BIGINT,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (timestamp);
```

Create monthly partitions. Enables efficient range queries for "show me accuracy over the last 30 days."

### Task 7.4: Comparison Queries

`CompareRuns(run_ids[])`: returns metrics side-by-side for multiple runs. Useful for "which model version performs better?"

TDD, handler, Dockerfile, Helm, deploy.

---

## Phase 8: Billing/Usage Service (Outbox Pattern)

> Meters every inference call. Outbox pattern for reliable event publishing.

---

### Task 8.1: Billing Proto + Scaffold

Proto RPCs: `RecordUsage`, `GetUsage`, `CheckQuota`, `CreateRatePlan`, `GetInvoice`

### Task 8.2: Usage Metering via NATS

Subscribe to `fp.inference.completed`. For each event, increment usage counter for the API key's rate plan.

### Task 8.3: Outbox Pattern Implementation

**This is the key pattern for this service.**

When quota is exceeded:
1. BEGIN transaction
2. UPDATE usage counter
3. INSERT into `outbox` table: `{id, event_type: "QuotaExceeded", payload, created_at, published_at: NULL}`
4. COMMIT transaction

Separate goroutine (outbox poller):
1. SELECT from outbox WHERE published_at IS NULL ORDER BY created_at LIMIT 100
2. Publish each to NATS
3. UPDATE outbox SET published_at = NOW() WHERE id IN (...)

This guarantees: if the DB write succeeds, the event WILL eventually be published. No dual-write problem.

Tables: `usage_events`, `quotas`, `rate_plans`, `invoices`, `outbox`

### Task 8.4: Quota Enforcement

`CheckQuota(api_key)`: returns remaining quota. Inference gateway calls this before forwarding (cached in Redis for performance, eventual consistency with DB is acceptable).

TDD, handler, Dockerfile, Helm, deploy.

---

## Phase 9: Notification Service (Choreography)

> Pure event reactor. No orchestrator tells it what to do. Subscribes to events, matches rules, delivers notifications.

---

### Task 9.1: Notification Proto + Scaffold

Proto RPCs: `CreateRule`, `ListRules`, `GetDeliveryLog`

### Task 9.2: Rule Engine

Tables: `notification_rules` (id, user_id, event_type pattern, channel, config JSONB, enabled)

Channels: `webhook` (HTTP POST), `slack` (webhook URL), `email` (SMTP — optional, can mock)

Rule matching: event type supports wildcards. E.g., rule `fp.pipelines.*` matches `fp.pipelines.started` AND `fp.pipelines.failed`.

### Task 9.3: Event Subscriber (Choreography)

Subscribe to ALL `fp.>` events. For each:
1. Query matching rules (cached in memory, refreshed periodically)
2. For each matching rule, deliver notification via appropriate channel
3. Log delivery attempt to `delivery_log` table (success/failure, response code, retry count)

Key choreography principle: this service has ZERO knowledge of what the events mean. It just matches patterns and delivers. Adding a new event type requires zero code changes — just create a new rule.

### Task 9.4: Webhook Delivery with Retries

For webhook channel:
- HTTP POST to configured URL with event payload
- Retry with exponential backoff (3 attempts)
- Log all attempts
- Circuit breaker per webhook URL (don't keep hammering a dead endpoint)

TDD, handler, Dockerfile, Helm, deploy.

---

## Phase 10: Observability & Service Mesh

> Wire up the three observability pillars across all services. Deploy Istio for mTLS and traffic management.

---

### Task 10.1: Grafana Dashboards

**Files:**
- Create: `deploy/grafana/dashboards/platform-overview.json`
- Create: `deploy/grafana/dashboards/per-service-red.json`
- Create: `deploy/grafana/dashboards/inference-gateway.json`
- Create: `deploy/grafana/dashboards/pipeline-orchestrator.json`

Dashboards:
- **Platform Overview:** request rate, error rate, p99 latency across all services
- **Per-Service RED:** template variable for service name, shows RED metrics
- **Inference Gateway:** predictions/sec by model, circuit breaker states, rate limit rejections
- **Pipeline Orchestrator:** active sagas, step durations, failure rates, compensation events

### Task 10.2: Distributed Tracing Verification

Verify end-to-end trace propagation:
1. Send predict request to inference gateway
2. Open Tempo/Jaeger UI
3. Verify trace spans: `gateway.predict → auth.validate → serving.predict → billing.record → experiment.log`

Fix any gaps in trace propagation (missing context passing in NATS messages, missing interceptors).

### Task 10.3: Istio Service Mesh

```bash
istioctl install --set profile=demo
kubectl label namespace fp-system istio-injection=enabled
kubectl rollout restart deployment -n fp-system
```

Verify:
- mTLS between all services (check `istioctl proxy-status`)
- Traffic management: create VirtualService for canary routing
- NetworkPolicy: only inference-gateway can reach model-serving

### Task 10.4: Alerting Rules

**Files:**
- Create: `deploy/prometheus/alerting-rules.yaml`

Alerts:
- `HighErrorRate`: > 5% error rate for any service over 5 minutes
- `HighLatency`: p99 > 500ms for inference gateway
- `SagaStuck`: saga execution running > 10 minutes
- `QuotaExhausted`: any API key at > 90% quota
- `CircuitBreakerOpen`: any circuit breaker in open state

---

## Phase 11: CI/CD (GitHub Actions)

---

### Task 11.1: Per-Service CI Pipeline

**Files:**
- Create: `.github/workflows/service-ci.yaml`

Triggered by: push to `services/{service}/**` or `pkg/**` or `proto/**`

Matrix strategy to detect which services changed and only build those.

Steps: lint → buf lint → unit test → integration test (testcontainers) → build Docker → push to GHCR.

### Task 11.2: Proto Breaking Change Detection

**Files:**
- Create: `.github/workflows/proto-check.yaml`

On PR: run `buf breaking --against origin/main` to detect backward-incompatible proto changes.

### Task 11.3: Platform E2E Pipeline

**Files:**
- Create: `.github/workflows/e2e.yaml`

Triggered on merge to main. Spins up Kind cluster in CI, deploys all services via Helm, runs E2E test suite.

---

## Phase 12: Terraform AWS Deployment

---

### Task 12.1: Networking Module

**Files:**
- Create: `deploy/terraform/modules/networking/main.tf`

VPC with public + private subnets, NAT gateway, security groups.

### Task 12.2: EKS Module

**Files:**
- Create: `deploy/terraform/modules/eks/main.tf`

EKS cluster with managed node group (2-3 t3.medium), OIDC provider for IRSA.

### Task 12.3: Data Store Modules

**Files:**
- Create: `deploy/terraform/modules/rds/main.tf`
- Create: `deploy/terraform/modules/elasticache/main.tf`
- Create: `deploy/terraform/modules/s3/main.tf`

RDS PostgreSQL (db.t3.micro), ElastiCache Redis (cache.t3.micro), S3 bucket for model artifacts.

### Task 12.4: Environment Composition

**Files:**
- Create: `deploy/terraform/environments/dev/main.tf`

Wire all modules together with dev-sized resources. Backend: S3 + DynamoDB for state locking.

### Task 12.5: Deploy and Verify

```bash
cd deploy/terraform/environments/dev
terraform init
terraform plan
terraform apply
```

Deploy services via Helm to EKS. Run E2E smoke tests against live cluster.

---

## Phase 13: E2E & Load Testing

---

### Task 13.1: Full Lifecycle E2E Test

**Files:**
- Create: `test/e2e/lifecycle_test.go`

Test the complete happy path:
1. Create user + API key (auth)
2. Register model + upload artifact (registry)
3. Create deployment pipeline (pipeline orchestrator)
4. Trigger deployment → saga completes (orchestrator)
5. Send prediction request (inference gateway → model serving)
6. Verify usage metered (billing)
7. Verify experiment metrics logged (experiment tracker)
8. Verify notification delivered (notification)

### Task 13.2: Saga Failure E2E Test

Test compensation:
1. Deploy pipeline with a model that will fail canary (inject failure)
2. Verify saga rolls back: destroys serving instance, reverts routing
3. Verify `PipelineFailed` event triggers notification

### Task 13.3: k6 Load Tests

**Files:**
- Create: `tools/loadtest/inference_load.js`
- Create: `tools/loadtest/registry_load.js`

Inference gateway load test:
- Ramp to 1000 concurrent users
- Measure p50, p95, p99 latency
- Verify circuit breaker triggers under backend failures
- Verify rate limiter enforces quotas

### Task 13.4: Chaos Testing (Optional)

Using Chaos Mesh (if installed on Kind):
- Kill pipeline orchestrator mid-saga → verify recovery on restart
- Network partition between gateway and model serving → verify circuit breaker
- Slow down NATS → verify back-pressure in experiment tracker

---

## Phase 14: Web UI (BFF + Frontend)

> The browser can't speak gRPC directly. A **Backend-for-Frontend** service speaks gRPC to
> the platform services and exposes a JSON/HTTP + SSE API *shaped for the UI's screens*,
> not for the services. See `docs/adr/0001-bff-for-web-ui.md` for why BFF over
> grpc-gateway / gRPC-Web+Envoy.
>
> **Hard boundary (enforced in review): the BFF contains ZERO business logic.** It does
> composition (fan-out + aggregate), protocol/stream translation (gRPC ↔ JSON/SSE), and
> browser-session concerns (cookies, CSRF, CORS) only. All domain logic stays in the
> services. If you're tempted to add a rule or a calculation in the BFF, it belongs in a
> service.

---

### Task 14.1: BFF Scaffold

**Files:**
- Create: `services/bff/cmd/server/main.go`
- Create: `services/bff/go.mod` (add to `go.work`)
- Create: `services/bff/internal/clients/` — typed gRPC clients to each service
- Create: `services/bff/internal/http/` — chi/stdlib HTTP router, handlers, middleware

The BFF is a Go module like any service, but it has **no domain/ or repository/ layer** —
it holds gRPC client connections (auth, registry, orchestrator, gateway) and an HTTP server.
Reuse `pkg/observability`, `pkg/health`, `pkg/grpcutil` (for the outbound client interceptor
chain). Config via `pkg/config`: one upstream address per service.

### Task 14.2: Browser Auth Bridge

Services authenticate with bearer JWT (from `pkg/auth`). Browsers want cookies. The BFF:
1. `POST /api/login` → calls `auth.ValidateToken`/issues session → sets an **HttpOnly,
   Secure, SameSite** session cookie (server-side session or signed cookie holding the JWT).
2. On every `/api/*` request → reads cookie → attaches the JWT to outbound gRPC metadata.
3. CSRF protection (double-submit token) on mutating routes. CORS locked to the UI origin.

This is the one place the bearer-vs-cookie impedance mismatch is solved — keep it out of
the services.

### Task 14.3: View-Composition Endpoints (the reason BFF exists)

Aggregate across services server-side so the browser makes ONE call per screen:
- `GET /api/dashboard` → fan-out (registry.ListModels + orchestrator.ListExecutions +
  experiment.RecentRuns + billing.GetUsage) → one view-shaped JSON payload.
- `GET /api/models/{id}` → registry.GetModel + experiment runs for that model + serving
  status → one payload.

Use `errgroup` for the fan-out; partial failure degrades gracefully (return the panels that
resolved, mark the failed panel, don't 500 the whole page). This is the UX win over 1:1
edge translation, and a clean place to show idiomatic Go concurrency.

### Task 14.4: Real-Time Updates (gRPC server-stream → SSE)

Pipeline Orchestrator exposes server-streaming `WatchExecution`. The BFF bridges it to the
browser:
- `GET /api/executions/{id}/stream` → opens the gRPC stream → relays each update as a
  **Server-Sent Event**. Handle client disconnect (ctx cancel → close upstream stream).
- SSE chosen over WebSocket: one-directional server→client fits status updates, works over
  plain HTTP/2, no extra protocol. (Note this tradeoff in the ADR.)

### Task 14.5: Frontend Dashboard

**Files:**
- Create: `web/` (Vite + React + TypeScript, Tailwind)

Screens: Models (list/detail/versions), Pipelines (list + live execution view via SSE),
Experiments (runs + metric comparison), Usage/Billing. Talks ONLY to the BFF over JSON/SSE
— it never sees a gRPC stub or a service boundary. Served as static assets (the BFF or an
nginx sidecar can serve them); Dockerfile + Helm chart like any other workload.

### Task 14.6: BFF Tests + Deploy

- Unit: composition handlers with mocked gRPC clients (table-driven, including partial-failure).
- Component: in-process BFF with `bufconn` fakes for upstream services.
- Dockerfile (multi-stage, distroless), Helm chart, deploy to Kind, smoke test the dashboard.

---

## Phase 15: Operability & SRE

> Services that run are not the same as services you can *operate*. This phase produces the
> operational artifacts platform-engineering interviews probe for: SLOs with error budgets,
> actionable alerts, and a runbook. This is the layer that signals "platform engineer," not
> "backend engineer who knows K8s." (Phase 10 added dashboards + raw alert rules; this phase
> makes them SLO-driven and adds the human-facing operational docs.)

---

### Task 15.1: Define SLOs + Error Budgets

**Files:**
- Create: `docs/slo/inference-gateway.md`, `docs/slo/pipeline-orchestrator.md`

For the two user-facing critical paths, define: SLI (e.g., gateway availability =
non-5xx/total; latency = p99 < 300ms), SLO target (e.g., 99.5% over 30d), and the resulting
**error budget**. State what burning the budget means (freeze feature work, focus on
reliability). Reference Google SRE workbook conventions.

### Task 15.2: SLO-Based Alerting (multi-window burn rate)

**Files:**
- Edit: `deploy/prometheus/alerting-rules.yaml` (created in Task 10.4)

Replace/augment the static threshold alerts with **multi-window, multi-burn-rate** alerts
(fast burn: 2% budget in 1h → page; slow burn: 10% in 6h → ticket). Recording rules for the
SLIs. This is the difference between "p99 > 500ms" (noisy) and "we are burning our error
budget fast enough to miss the SLO" (actionable).

### Task 15.3: Runbook

**Files:**
- Create: `docs/runbooks/README.md` + one runbook per page-able alert

For each alert that pages (HighErrorRate, SagaStuck, CircuitBreakerOpen, fast-burn SLO):
symptom → dashboards/queries to check → likely causes → mitigation steps → escalation. Link
each Prometheus alert's annotation to its runbook URL. An interviewer asking "what happens at
3am when this fires?" should be answerable by pointing at this doc.

### Task 15.4: Graceful Degradation + Readiness Discipline

Verify the platform degrades instead of cascading:
- BFF dashboard renders with a failed panel (Task 14.3) rather than 500ing.
- `/readyz` actually gates traffic: a service with a dead Postgres dep reports NOT ready and
  is pulled from the Service endpoints (verify with `kubectl get endpoints`).
- Document the degradation matrix: which dependency failing degrades which feature.

---

## Execution Notes

### Service Build Order (Dependency Chain)

```
Phase 0 (Foundation)
    ↓
Phase 1 (Auth) ← everything depends on this
    ↓
Phase 2 (Registry) ← models must exist before serving/deploying
    ↓
Phase 3 (Model Serving) ← must exist before gateway can route to it
    ↓
Phase 4 (Inference Gateway) ← routes to model serving
    ↓
Phase 5 (Pipeline Orchestrator) ← orchestrates registry + serving + gateway
    ↓
Phase 10, 11, 13 (Observability, CI/CD, E2E) ← cross-cutting over the Core services
    ↓
Phase 14 (BFF + Web UI) ← composes Core services into screens
    ↓
Phase 15 (Operability) ← SLOs/alerts/runbook over the running platform
    · · · · · · · · · · · · · · · · · · · · · · · · ·  CORE complete
    ↓
Phase 6-9 (Feature Store, Experiment, Billing, Notification) ← STRETCH, pick 1-2
    ↓
Phase 12 (Terraform EKS) ← STRETCH, last (high effort, low novel learning)
```

Phases 6-9 can be built in parallel or any order — they only depend on Phase 1 (auth) and
NATS events. When you build a Stretch service, fold it into the BFF dashboard and add its
alerts/runbook entries so the Core artifacts stay consistent.

### Per-Service Checklist

Every service MUST have before moving to the next:
- [ ] Proto definition (buf lint passes)
- [ ] Domain layer with unit tests (>80% coverage)
- [ ] Repository layer with integration tests (testcontainers)
- [ ] gRPC handler with integration tests
- [ ] NATS events (publisher and/or subscriber)
- [ ] Migrations
- [ ] Dockerfile (multi-stage, distroless)
- [ ] Helm chart (all K8s resources)
- [ ] Deployed to Kind and smoke tested
- [ ] Health checks working (/healthz, /readyz)
- [ ] Prometheus metrics exposed
- [ ] OpenTelemetry traces propagated

### Key Dependencies (Go modules)

| Module | Purpose |
|--------|---------|
| `google.golang.org/grpc` | gRPC framework |
| `google.golang.org/protobuf` | Proto serialization |
| `github.com/jackc/pgx/v5` | PostgreSQL driver |
| `github.com/redis/go-redis/v9` | Redis client |
| `github.com/nats-io/nats.go` | NATS client |
| `github.com/minio/minio-go/v7` | MinIO/S3 client |
| `github.com/golang-jwt/jwt/v5` | JWT implementation |
| `github.com/golang-migrate/migrate/v4` | DB migrations |
| `go.opentelemetry.io/otel` | OpenTelemetry SDK |
| `github.com/testcontainers/testcontainers-go` | Integration test containers |
| `github.com/bufbuild/buf` | Proto tooling (CLI) |
| `github.com/yalue/onnxruntime_go` | ONNX model inference |
