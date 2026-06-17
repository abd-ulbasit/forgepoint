# syntax=docker/dockerfile:1.7
# =============================================================================
# Forgepoint service Dockerfile — REUSABLE TEMPLATE (worked example: auth)
# =============================================================================
#
# This file is the canonical Dockerfile every Forgepoint service copies. To make
# a new service's image, copy this file to services/<svc>/Dockerfile and change
# the THREE marked `### PER-SERVICE` lines (the service module path and the two
# COPY lines for that service's source + migrations). Nothing else changes.
#
# -----------------------------------------------------------------------------
# WHY THE BUILD CONTEXT MUST BE THE REPO ROOT (the whole point of this file)
# -----------------------------------------------------------------------------
# Forgepoint is a Go WORKSPACE monorepo. Each service's go.mod imports sibling
# modules (pkg, gen/go) that are NOT on any public registry — they only exist as
# directories in this repo. So the naive single-module recipe:
#
#     COPY go.mod go.sum ./        # <-- WRONG for a workspace
#     RUN  go mod download
#
# fails: `go mod download` for services/model-serving tries to FETCH github.com/.../pkg
# from the network (unknown revision) because there is no go.work in scope to
# redirect that import to local source. We need a workspace to build correctly.
#
# That is why `make docker-build` (and CI) invoke:
#
#     docker build -f services/model-serving/Dockerfile  <REPO_ROOT>
#                  ^------ Dockerfile lives in the service dir
#                                                ^------ context is the ROOT
#
# A repo-root .dockerignore keeps that large context cheap (it strips .git, bin/,
# node_modules, test output, etc.) so the daemon only receives source.
#
# -----------------------------------------------------------------------------
# WHY WE GENERATE A MINIMAL go.work (not COPY the repo's go.work)
# -----------------------------------------------------------------------------
# The repo's go.work currently lists 10+ modules (all services + pkg + gen/go).
# Copying it into the builder would make this image's build FAIL as soon as any
# sibling service is added to go.work but NOT copied into this image's context —
# the Go toolchain would try to resolve every `use` directive and error on the
# missing directories.
#
# Instead, after copying the three per-service go.mod/go.sum files, we GENERATE
# a minimal workspace on-the-fly:
#
#     RUN go work init ./pkg ./gen/go ./services/model-serving
#
# This workspace contains ONLY the modules this image needs. It is:
#   - Self-contained: adding a 10th or 20th service to go.work never breaks this
#     build.
#   - Correct: Go resolves ./pkg and ./gen/go to local source exactly as it would
#     on a developer laptop with go.work present.
#   - Fast: `go work init` is instant; no sync step is needed because we will run
#     `go mod download` next to prime the module cache.
#
# Layer-caching is preserved: the `go work init && go mod download` layer is
# keyed on the three go.mod/go.sum pairs copied above it; a source-only edit
# doesn't invalidate that layer.
#
# -----------------------------------------------------------------------------
# WHY WE COPY MODULES SELECTIVELY (not just `COPY . .`)
# -----------------------------------------------------------------------------
# We copy ONLY the modules this service compiles (pkg, gen/go, services/model-serving).
# Two payoffs:
#   1. Layer caching: the `go mod download` layer is reused across rebuilds until
#      a dependency actually changes — source edits don't re-download the world.
#   2. Smaller, deterministic context surface: a service never accidentally
#      depends on another service's source (database-per-service / module
#      isolation is enforced at build time, not just by convention).
#
# -----------------------------------------------------------------------------
# WHY golang:1.26-alpine (NOT 1.25)
# -----------------------------------------------------------------------------
# `go.work` declares `go 1.26.0`. The Go toolchain refuses to build a workspace
# whose go directive is NEWER than the toolchain, so a 1.25 image cannot build
# this repo (it would either error or trigger a slow toolchain auto-download).
# We pin the builder to 1.26 to match the workspace. Bump this line in lockstep
# with the `go` directive in go.work when the platform upgrades Go.
#
# -----------------------------------------------------------------------------
# WHY distroless/static + nonroot for the final image
# -----------------------------------------------------------------------------
# CGO_ENABLED=0 produces a fully static binary, so the runtime needs no libc and
# no shell — `gcr.io/distroless/static-debian12` ships only CA certs, tzdata, and
# /etc/passwd. Result: a ~10-20MB image with a tiny attack surface (no shell, no
# package manager, nothing for an attacker to exec into) and `:nonroot` runs as
# UID 65532 so the container can't write outside its volumes. This is Google's
# distroless pattern — the industry baseline for Go service images.
#
# -----------------------------------------------------------------------------
# WHY NO `HEALTHCHECK` INSTRUCTION
# -----------------------------------------------------------------------------
# A Docker `HEALTHCHECK` runs a command INSIDE the container — but distroless has
# no shell and no curl/wget, so there is nothing to run, and Kubernetes ignores
# Docker HEALTHCHECK entirely anyway. Liveness/readiness are owned by K8s probes
# hitting the service's HTTP health server: GET /healthz (liveness) and GET
# /readyz (readiness with real dependency checks) on the health port (8080).
# See pkg/health and cmd/server/main.go.
# =============================================================================


# ----------------------------------------------------------------------------
# Stage 1: builder — full Go toolchain, compiles the static binary.
# ----------------------------------------------------------------------------
# Base images are pinned by DIGEST (not just a floating tag) for hermetic,
# reproducible builds: the same Dockerfile resolves to the identical bytes today
# and a year from now. These are multi-arch index digests, so the build works on
# amd64 and arm64 hosts alike. Refresh with:
#   docker buildx imagetools inspect golang:1.26-alpine
FROM golang:1.26-alpine@sha256:f1ddd9fe14fffc091dd98cb4bfa999f32c5fc77d2f2305ea9f0e2595c5437c14 AS builder

# git: some Go tooling/VCS stamping needs it. ca-certificates: for module fetches
# over HTTPS. We disable VCS stamping at build time anyway (-buildvcs=false) so
# the .git dir need not be in the context.
RUN apk add --no-cache git ca-certificates

WORKDIR /workspace

# --- 1. Per-module go.mod/go.sum (the cache-key layer) ---------------------------
# Copy ONLY the dependency manifests first so the expensive `go mod download`
# layer is cached and reused until a dependency actually changes. We do NOT copy
# the repo's go.work here — see the long header above for why we generate one.
COPY pkg/go.mod pkg/go.sum ./pkg/
COPY gen/go/go.mod gen/go/go.sum ./gen/go/
### PER-SERVICE: this service's module manifests
COPY services/model-serving/go.mod services/model-serving/go.sum ./services/model-serving/

# Generate a minimal workspace containing only the three modules this image
# needs. `go work init` writes a go.work file in /workspace that tells the Go
# toolchain to resolve pkg and gen/go to local source (not the network). This
# makes the build robust: adding more services to the repo's go.work never
# breaks this image because we never read that file. `go mod download` then
# primes the module cache for all three modules' external dependencies.
RUN --mount=type=cache,target=/go/pkg/mod \
    go work init ./pkg ./gen/go ./services/model-serving && \
    go mod download -x all

# --- 2. Source — copy only the modules this service compiles --------------------
# Copying selectively (not `COPY . .`) keeps module isolation honest: a service
# can only build against pkg, gen/go, and its own source — never another
# service's. Source edits invalidate from here down, NOT the download layer above.
COPY pkg/        ./pkg/
COPY gen/go/     ./gen/go/
### PER-SERVICE: this service's source tree
COPY services/model-serving/ ./services/model-serving/

# --- 3. Build the static binary -------------------------------------------------
# CGO_ENABLED=0  : pure-Go static binary (no libc) → runs on distroless/static.
# GOFLAGS=-mod=readonly : build must not mutate go.mod/go.sum (fails if out of sync).
# -ldflags "-s -w" : LINKER flags — strip symbol table (-s) and DWARF debug info
#                    (-w) → smaller binary. (Note: -s/-w go to the linker; the next
#                    two flags are `go build` flags and must live OUTSIDE -ldflags.)
# -buildvcs=false  : don't stamp VCS info (the .git dir isn't in the context). This
#                    is a `go build` flag, NOT a linker flag — putting it inside
#                    -ldflags makes the linker error out with a usage dump.
# -trimpath        : strip absolute /workspace paths from the binary → reproducible.
# The second cache mount (/root/.cache/go-build) preserves the compiler's build
# cache across rebuilds, turning a cold ~minute build into a warm few seconds.
# `-o /out/service` uses a generic name so the final stage is service-agnostic.
### PER-SERVICE: the `./services/model-serving/cmd/server` path below is the only build arg to change.
ENV CGO_ENABLED=0 GOFLAGS=-mod=readonly
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags="-s -w" -buildvcs=false -trimpath \
        -o /out/service ./services/model-serving/cmd/server


# ----------------------------------------------------------------------------
# Stage 2: final — distroless static, runs as nonroot.
# ----------------------------------------------------------------------------
# `static-debian12:nonroot` already includes CA certs, tzdata, and a nonroot
# user (UID 65532). No shell, no package manager. Pinned by multi-arch index
# digest for reproducibility; refresh with:
#   docker buildx imagetools inspect gcr.io/distroless/static-debian12:nonroot
FROM gcr.io/distroless/static-debian12:nonroot@sha256:d093aa3e30dbadd3efe1310db061a14da60299baff8450a17fe0ccc514a16639

# OCI metadata — discoverable provenance for registries and scanners.
LABEL org.opencontainers.image.source="https://github.com/abd-ulbasit/forgepoint" \
      org.opencontainers.image.title="forgepoint-model-serving" \
      org.opencontainers.image.description="Forgepoint Auth/IAM service (gRPC + health)"

WORKDIR /

# The compiled binary. Copied to a fixed path /auth used by ENTRYPOINT below.
COPY --from=builder /out/service /service-bin

# golang-migrate SQL lives alongside the binary so the same image can run
# migrations (e.g. an initContainer invoking the migrate tool against /migrations)
# without a second artifact. Phase 1.4 fills this dir; the .gitkeep keeps the
# COPY valid until then.
### PER-SERVICE: this service's migrations
COPY services/model-serving/migrations /migrations

# Run as the unprivileged nonroot user baked into the distroless image (UID
# 65532). Defense in depth: even if the process is compromised it has no shell,
# no root, and a read-only-friendly filesystem.
USER nonroot:nonroot

# Document the ports the service listens on (informational; does not publish).
#   9090 — gRPC API   (GRPC_PORT default, see pkg/config BaseConfig)
#   8080 — HTTP health /healthz + /readyz   (PORT default)
EXPOSE 9090 8080

# Exec form (no shell wrapper) so the binary is PID 1 and receives SIGTERM
# directly — required for the graceful-shutdown drain in main.go to run on a
# K8s pod termination.
ENTRYPOINT ["/service-bin"]
