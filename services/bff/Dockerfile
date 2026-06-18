# syntax=docker/dockerfile:1.7
# =============================================================================
# Forgepoint BFF Dockerfile — derived from the reusable template (services/auth)
# =============================================================================
#
# This is the canonical Forgepoint service image (see services/auth/Dockerfile for
# the long-form rationale on EVERY design choice — repo-root build context, the
# generated minimal go.work, selective module copies, the 1.26 builder pin, and
# the distroless/nonroot final stage). It is reproduced here with only the
# PER-SERVICE lines changed for the BFF, plus ONE structural difference:
#
#   ── THE BFF HAS NO DATABASE ──────────────────────────────────────────────
#   Every other service COPYs its golang-migrate SQL into /migrations so the same
#   image can run schema migrations from an initContainer. The BFF is a STATELESS
#   HTTP↔gRPC translator (a gRPC CLIENT of the 10 domain services that forwards the
#   caller's JWT) — it owns NO relational schema. So we DROP the `COPY .../migrations`
#   line entirely. There is nothing to migrate, and the Helm chart renders no
#   migrate initContainer (migrations.enabled=false), so the image never needs that
#   directory. Keeping a COPY for a dir that doesn't exist would just break the build.
#
# WHY THE BUILD CONTEXT MUST BE THE REPO ROOT: the BFF's go.mod imports sibling
# workspace modules (pkg, gen/go) that exist only as directories in this repo, not
# on any registry. The build must run from the repo root with a generated minimal
# go.work so the Go toolchain resolves those imports to local source:
#
#     docker build -f services/bff/Dockerfile  <REPO_ROOT>
#
# See services/auth/Dockerfile's header for the full explanation of why a naive
# single-module `COPY go.mod && go mod download` recipe fails for a workspace.
# =============================================================================


# ----------------------------------------------------------------------------
# Stage 1: builder — full Go toolchain, compiles the static binary.
# ----------------------------------------------------------------------------
# Pinned by multi-arch index DIGEST (not a floating tag) for hermetic, reproducible
# builds across amd64/arm64. The builder is 1.26 to match go.work's `go 1.26.0`
# directive (a 1.25 toolchain refuses to build a workspace declaring a newer Go).
# Refresh with: docker buildx imagetools inspect golang:1.26-alpine
FROM golang:1.26-alpine@sha256:f1ddd9fe14fffc091dd98cb4bfa999f32c5fc77d2f2305ea9f0e2595c5437c14 AS builder

# git: VCS stamping/tooling. ca-certificates: HTTPS module fetches. We pass
# -buildvcs=false below so the .git dir need not be in the context.
RUN apk add --no-cache git ca-certificates

WORKDIR /workspace

# --- 1. Per-module go.mod/go.sum (the cache-key layer) ---------------------------
# Copy ONLY the dependency manifests first so the expensive `go mod download`
# layer is cached and reused until a dependency actually changes. We do NOT copy
# the repo's go.work — we generate a minimal one below (see auth Dockerfile header).
COPY pkg/go.mod pkg/go.sum ./pkg/
COPY gen/go/go.mod gen/go/go.sum ./gen/go/
### PER-SERVICE: this service's module manifests
COPY services/bff/go.mod services/bff/go.sum ./services/bff/

# Generate a minimal workspace containing ONLY the three modules this image needs
# (pkg, gen/go, services/bff). `go work init` makes the toolchain resolve pkg and
# gen/go to LOCAL source rather than the network; adding more services to the
# repo's real go.work can never break this image because we never read it.
# `go mod download` then primes the module cache for all external deps.
RUN --mount=type=cache,target=/go/pkg/mod \
    go work init ./pkg ./gen/go ./services/bff && \
    go mod download -x all

# --- 2. Source — copy only the modules this service compiles --------------------
# Selective copy (not `COPY . .`) keeps module isolation honest: the BFF can only
# build against pkg, gen/go, and its own source. Source edits invalidate from here
# down, NOT the download layer above.
COPY pkg/        ./pkg/
COPY gen/go/     ./gen/go/
### PER-SERVICE: this service's source tree
COPY services/bff/ ./services/bff/

# --- 3. Build the static binary -------------------------------------------------
# CGO_ENABLED=0 → pure-Go static binary (no libc) for distroless/static.
# GOFLAGS=-mod=readonly → build must not mutate go.mod/go.sum.
# -ldflags "-s -w" → strip symbol table + DWARF (smaller binary).
# -buildvcs=false → don't stamp VCS (the .git dir isn't in the context).
# -trimpath → strip absolute /workspace paths → reproducible.
### PER-SERVICE: the `./services/bff/cmd/server` build path is the only build arg to change.
ENV CGO_ENABLED=0 GOFLAGS=-mod=readonly
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags="-s -w" -buildvcs=false -trimpath \
        -o /out/service ./services/bff/cmd/server


# ----------------------------------------------------------------------------
# Stage 2: final — distroless static, runs as nonroot.
# ----------------------------------------------------------------------------
# `static-debian12:nonroot` ships only CA certs, tzdata, and a nonroot user (UID
# 65532) — no shell, no package manager. Pinned by multi-arch index digest.
# Refresh with: docker buildx imagetools inspect gcr.io/distroless/static-debian12:nonroot
FROM gcr.io/distroless/static-debian12:nonroot@sha256:d093aa3e30dbadd3efe1310db061a14da60299baff8450a17fe0ccc514a16639

# OCI metadata — discoverable provenance for registries and scanners.
LABEL org.opencontainers.image.source="https://github.com/abd-ulbasit/forgepoint" \
      org.opencontainers.image.title="forgepoint-bff" \
      org.opencontainers.image.description="Forgepoint BFF (Backend-for-Frontend): stateless HTTP/SSE↔gRPC gateway for the SPA; forwards the caller's JWT to the 10 services"

WORKDIR /

# The compiled binary at a fixed path /bff used by ENTRYPOINT below.
COPY --from=builder /out/service /bff

# NOTE: NO `COPY services/bff/migrations /migrations` here — the BFF owns no
# database (see header). The Helm chart sets migrations.enabled=false so no
# migrate initContainer references a /migrations dir that doesn't exist.

# Run as the unprivileged nonroot user (UID 65532) baked into distroless.
USER nonroot:nonroot

# Document the ports the BFF listens on (informational; does not publish).
#   8081 — HTTP API for the SPA (JSON + SSE)   (FP_HTTP_PORT default, see main.go)
#   8080 — HTTP health /healthz + /readyz       (FP_PORT default, BaseConfig)
# The BFF serves NO gRPC (it is a gRPC CLIENT, not server), so no 9090 here.
EXPOSE 8081 8080

# Exec form (no shell) so the binary is PID 1 and receives SIGTERM directly —
# required for the graceful-shutdown drain in main.go to run on pod termination.
ENTRYPOINT ["/bff"]
