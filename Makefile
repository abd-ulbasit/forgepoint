# ============================================================================
# Forgepoint Platform — Root Makefile
# ============================================================================
#
# WHY: A single entry point for all build, test, lint, and deploy operations.
# Developers and CI run the same commands — no "works on my machine" drift.
#
# HOW IT WORKS:
#   - Top-level targets delegate to per-service builds when SVC= is provided
#   - `make test` runs tests across ALL services in the workspace
#   - `make build SVC=auth` builds only the auth service binary
#   - `make docker-build SVC=auth` builds the Docker image for auth
#   - Infrastructure targets manage docker-compose and Kind cluster
#
# CONVENTION: Targets are ordered by frequency of use during development:
#   proto → build → test → lint → docker → infra → k8s
#
# ============================================================================

# Default shell for recipes
SHELL := /bin/bash
.DEFAULT_GOAL := help

# ============================================================================
# Variables
# ============================================================================

# Service name passed via `make <target> SVC=auth`
SVC ?=

# All services in the monorepo (updated as services are added)
SERVICES := auth

# Docker image registry prefix (override for GHCR/ECR in CI)
REGISTRY ?= fp
IMAGE_TAG ?= dev

# Go build flags
# -s: omit symbol table  -w: omit DWARF debug info  → smaller binary
LDFLAGS := -ldflags="-s -w"

# ============================================================================
# Proto Generation
# ============================================================================
#
# WHY buf over protoc:
#   - protoc requires manual plugin management (protoc-gen-go, protoc-gen-go-grpc)
#   - buf handles deps, linting, breaking change detection in one tool
#   - buf.yaml is declarative — no shell scripts with long protoc flags
#
# HOW: `buf lint` enforces style (STANDARD rules), `buf generate` runs
# plugins defined in buf.gen.yaml to produce Go code in gen/go/
# ============================================================================

.PHONY: proto
proto: proto-lint proto-generate ## Lint and generate proto code

.PHONY: proto-lint
proto-lint: ## Lint proto files
	@echo "==> Linting proto files..."
	cd proto && buf lint

.PHONY: proto-generate
proto-generate: ## Generate Go code from proto files
	@echo "==> Generating proto code..."
	cd proto && buf generate

.PHONY: proto-breaking
proto-breaking: ## Check for breaking proto changes against main
	@echo "==> Checking for breaking changes..."
	cd proto && buf breaking --against '../.git#subdir=proto'

# ============================================================================
# Build
# ============================================================================

.PHONY: build
build: ## Build a service binary: make build SVC=auth
	@if [ -z "$(SVC)" ]; then echo "ERROR: specify SVC=<service>"; exit 1; fi
	@echo "==> Building $(SVC)..."
	cd services/$(SVC) && go build $(LDFLAGS) -o ../../bin/$(SVC) ./cmd/server

.PHONY: build-all
build-all: ## Build all service binaries
	@for svc in $(SERVICES); do \
		echo "==> Building $$svc..."; \
		cd services/$$svc && go build $(LDFLAGS) -o ../../bin/$$svc ./cmd/server && cd ../..; \
	done

# ============================================================================
# Testing
# ============================================================================
#
# WHY -race flag: Go's race detector instruments memory accesses at compile
# time and detects data races at runtime. Essential for concurrent code
# (gRPC handlers, NATS consumers, etc.). Small performance cost (~2-10x
# slower) but catches bugs that are nearly impossible to find otherwise.
#
# WHY -timeout: Integration tests with testcontainers can be slow on first
# run (pulling Docker images). 5 minutes prevents CI timeouts on cold cache.
# ============================================================================

.PHONY: test
test: ## Run tests: make test [SVC=auth]
	@if [ -n "$(SVC)" ]; then \
		echo "==> Testing $(SVC)..."; \
		cd services/$(SVC) && go test -race -v ./...; \
	else \
		echo "==> Testing all modules..."; \
		go test -race -v ./...; \
	fi

.PHONY: test-integration
test-integration: ## Run integration tests (requires Docker for testcontainers)
	@if [ -n "$(SVC)" ]; then \
		echo "==> Integration testing $(SVC)..."; \
		cd services/$(SVC) && go test -race -v -tags=integration -timeout 300s ./...; \
	else \
		echo "==> Integration testing all modules..."; \
		go test -race -v -tags=integration -timeout 300s ./...; \
	fi

.PHONY: test-e2e
test-e2e: ## Run E2E tests on Kind cluster
	@echo "==> Running E2E tests..."
	go test -race -v -tags=e2e -timeout 600s ./test/e2e/...

.PHONY: test-cover
test-cover: ## Run tests with coverage report
	@if [ -n "$(SVC)" ]; then \
		cd services/$(SVC) && go test -race -coverprofile=cover.out ./... && go tool cover -html=cover.out -o cover.html; \
	else \
		go test -race -coverprofile=cover.out ./... && go tool cover -html=cover.out -o cover.html; \
	fi
	@echo "==> Coverage report: cover.html"

# ============================================================================
# Linting
# ============================================================================
#
# WHY golangci-lint over individual linters:
#   - Runs 50+ linters in parallel with shared AST parsing
#   - Single config file (.golangci.yml) controls everything
#   - 5-10x faster than running linters individually
# ============================================================================

.PHONY: lint
lint: ## Run golangci-lint across all modules
	@echo "==> Linting..."
	golangci-lint run ./...

.PHONY: proto-lint
# (defined above in Proto section)

# ============================================================================
# Docker
# ============================================================================
#
# WHY multi-stage builds:
#   - Stage 1 (builder): Full Go toolchain, compiles binary
#   - Stage 2 (runtime): distroless/static — NO shell, NO package manager
#   - Result: ~10-20MB images vs ~800MB with golang base
#   - Attack surface: minimal (no shell for attackers to exec into)
#
# HOW NETFLIX/GOOGLE DO IT:
#   - Google: distroless images (they created the concept)
#   - Netflix: minimal Alpine-based images
#   - We use Google's distroless — industry gold standard
# ============================================================================

.PHONY: docker-build
docker-build: ## Build Docker image: make docker-build SVC=auth
	@if [ -z "$(SVC)" ]; then echo "ERROR: specify SVC=<service>"; exit 1; fi
	@echo "==> Building Docker image for $(SVC)..."
	docker build -t $(REGISTRY)-$(SVC):$(IMAGE_TAG) -f services/$(SVC)/Dockerfile services/$(SVC)/

.PHONY: docker-build-all
docker-build-all: ## Build Docker images for all services
	@for svc in $(SERVICES); do \
		echo "==> Building Docker image for $$svc..."; \
		docker build -t $(REGISTRY)-$$svc:$(IMAGE_TAG) -f services/$$svc/Dockerfile services/$$svc/; \
	done

# ============================================================================
# Local Infrastructure (Docker Compose)
# ============================================================================
#
# WHY docker-compose for local dev:
#   - Matches production topology: real Postgres, NATS, Redis, MinIO
#   - No mocks — integration tests hit real services
#   - Single command to bring up/down the entire stack
#
# WHAT'S INCLUDED:
#   PostgreSQL (per-service databases), NATS (JetStream), Redis, MinIO,
#   Prometheus, Grafana, Tempo (traces), Loki (logs)
# ============================================================================

.PHONY: up
up: ## Start local infrastructure via docker-compose
	@echo "==> Starting local infrastructure..."
	docker compose up -d

.PHONY: down
down: ## Stop local infrastructure
	@echo "==> Stopping local infrastructure..."
	docker compose down

.PHONY: down-clean
down-clean: ## Stop and remove all volumes (fresh start)
	@echo "==> Stopping and cleaning local infrastructure..."
	docker compose down -v

# ============================================================================
# Kubernetes (Kind)
# ============================================================================
#
# WHY Kind over Minikube:
#   - Runs K8s nodes as Docker containers (fast startup: ~30s vs ~2min)
#   - Supports multi-node clusters locally
#   - CI-friendly (no VM hypervisor needed)
#   - Used by K8s SIG for upstream testing
#
# ALTERNATIVES:
#   - Minikube: VM-based, heavier, but better GPU passthrough
#   - k3s: Lightweight K8s, great for edge, less K8s-compatible
#   - Docker Desktop K8s: Convenient but single-node only
# ============================================================================

.PHONY: kind-create
kind-create: ## Create Kind cluster
	@echo "==> Creating Kind cluster..."
	kind create cluster --config deploy/kind/kind-config.yaml --name fp

.PHONY: kind-delete
kind-delete: ## Delete Kind cluster
	@echo "==> Deleting Kind cluster..."
	kind delete cluster --name fp

.PHONY: kind-load
kind-load: ## Load Docker image into Kind: make kind-load SVC=auth
	@if [ -z "$(SVC)" ]; then echo "ERROR: specify SVC=<service>"; exit 1; fi
	kind load docker-image $(REGISTRY)-$(SVC):$(IMAGE_TAG) --name fp

# ============================================================================
# Helm
# ============================================================================

.PHONY: helm-install
helm-install: ## Install service via Helm: make helm-install SVC=auth
	@if [ -z "$(SVC)" ]; then echo "ERROR: specify SVC=<service>"; exit 1; fi
	@echo "==> Installing $(SVC) via Helm..."
	helm install fp-$(SVC) deploy/helm/fp-$(SVC)/ \
		--namespace fp-system \
		--create-namespace \
		--set image.repository=$(REGISTRY)-$(SVC) \
		--set image.tag=$(IMAGE_TAG)

.PHONY: helm-upgrade
helm-upgrade: ## Upgrade service via Helm: make helm-upgrade SVC=auth
	@if [ -z "$(SVC)" ]; then echo "ERROR: specify SVC=<service>"; exit 1; fi
	@echo "==> Upgrading $(SVC) via Helm..."
	helm upgrade fp-$(SVC) deploy/helm/fp-$(SVC)/ \
		--namespace fp-system \
		--set image.repository=$(REGISTRY)-$(SVC) \
		--set image.tag=$(IMAGE_TAG)

# ============================================================================
# Skaffold
# ============================================================================

.PHONY: skaffold-dev
skaffold-dev: ## Start Skaffold dev loop (file watch -> rebuild -> deploy)
	@echo "==> Starting Skaffold dev loop..."
	skaffold dev --filename deploy/skaffold.yaml

# ============================================================================
# Utilities
# ============================================================================

.PHONY: clean
clean: ## Remove build artifacts
	@echo "==> Cleaning..."
	rm -rf bin/ cover.out cover.html

.PHONY: tidy
tidy: ## Run go mod tidy on all modules
	@for svc in $(SERVICES); do \
		echo "==> Tidying services/$$svc..."; \
		cd services/$$svc && go mod tidy && cd ../..; \
	done
	@echo "==> Tidying pkg..."
	cd pkg && go mod tidy

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
