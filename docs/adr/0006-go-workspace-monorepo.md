# ADR 0006: Go-workspace monorepo with one module per service

**Status:** Accepted
**Date:** 2026-06-17
**Deciders:** Abdul Basit Sajid
**Context phase:** M0 Foundation (repo layout, build tooling)

## Context

Forgepoint is 11 microservices plus a BFF, shared libraries (`pkg/`) and generated proto code
(`gen/go/`). They must (a) **share code** — every service imports `pkg/config`,
`pkg/observability`, `pkg/grpcutil`, `pkg/natsutil`, `pkg/health`, and the generated types in
`gen/go/` — while (b) staying **independently versioned and independently built**, so a Redis
bump for one service does not drag every other service's dependency graph, and each service
produces its own minimal container image (database-per-service / module isolation extends to
the build).

The constraint that shapes everything: `pkg/` and `gen/go/` are **unpublished** modules — they
exist only as directories in this repo, never on a registry. So however we wire the repo, a
service importing `github.com/abd-ulbasit/forgepoint/pkg` must resolve that import to **local
source**, both on a developer laptop and inside a Docker build, without a network fetch.

We need a repository and build topology that gives shared code + independent modules + clean
per-service images.

## Options Considered

### Option A — Single-module monolith (one `go.mod` for the whole repo)

- **Pro:** dead simple; every import just works; no workspace machinery.
- **Con:** **one dependency graph for every service** — a version bump any service needs is
  forced on all of them. No independent evolution; no module isolation; the antithesis of
  "database/dependencies per service."

### Option B — Polyrepo (each service its own Git repo) + a published `pkg`

- **Pro:** maximal isolation; each service truly standalone.
- **Con:** cross-cutting changes (a proto edit, a `pkg` API change) become a multi-repo,
  multi-PR, version-bump-and-publish dance. With a single team owning every module, that
  coordination overhead buys nothing — it exists to decouple teams that ship on different
  cadences, which is not the situation here.
- **Con:** `pkg`/`gen/go` must be **published and versioned** for services to consume them —
  every shared-lib change is a release.

### Option C — `replace` directives (pre-1.18 multi-module monorepo)

Each service's `go.mod` carries `replace github.com/.../pkg => ../../pkg` for every sibling.

- **Pro:** works without Go workspaces; keeps modules separate.
- **Con:** **O(modules²) and fragile** — every module needs a `replace` for every other module
  it touches, and those `replace` lines must NOT ship to any external consumer. Doesn't scale
  past a handful of modules. This is the pattern Go workspaces were introduced to retire.

### Option D — Go workspace + one module per service (chosen)

A top-level `go.work` lists every module via `use(...)`; each service is its own module with
its own `go.mod`; `pkg/` and `gen/go/` are sibling modules the workspace resolves locally.

- **Pro:** shared code with **zero `replace` lines and zero publishing** — `use` redirects the
  unpublished sibling imports to local source for `go build`/`test`/`vet`.
- **Pro:** **independent dependency graphs** — each `go.mod` owns its own requires; a bump in
  one service doesn't touch another.
- **Pro:** the same module boundaries let each Docker image copy **only** the modules it
  compiles, enforcing isolation at build time.
- **Pro:** it is the modern, idiomatic answer (Uber's monorepo, Buf's OSS) — a defensible,
  current-practice story.
- **Con:** a few **workspace-specific gotchas** (below) that bite once and must be encoded as
  discipline.

## Decision

Adopt **Option D**: a **Go workspace monorepo** — `go.work` + a module per service +
`gen/go` + `pkg`. The `go.work` `use(...)` block lists `./gen/go`, `./pkg`, `./cli` and every
`./services/*` module (15 entries today); the workspace declares `go 1.26.0`.

Three gotchas are part of the decision (each cost real debugging; all are now encoded in
`docs/design/service-architecture.md` and `services/auth/Dockerfile`):

### 1. Unpublished siblings resolve via `use` — never via an explicit `require … v0.0.0`

Sibling modules (`pkg`, `gen/go`) are resolved by the `use` directives. You must **not** add
`require github.com/abd-ulbasit/forgepoint/{pkg,gen/go} v0.0.0` to a service's `go.mod`: an
explicit version on an *unpublished* module makes Go attempt a **VCS lookup** for that version
and fail with `unknown revision …/v0.0.0`. Let `go work sync` manage the requires; add only
**external** deps explicitly with `go get <pkg>@<version>`.

### 2. Version-cascade discipline — pin to the versions `pkg` already uses

The workspace shares one resolved version per dependency across modules, so a careless bump in
one module cascades platform-wide. Concretely: a newer **gRPC** pulled a newer **otel** that
**broke observability**. Rule: pin external deps to the versions `pkg` already uses
(gRPC `v1.79.1`, `x/crypto`, otel `v1.40.0`), and after adding deps confirm
`cd pkg && go test ./observability/` still passes. (This is why `services/auth/go.mod` pins
`google.golang.org/grpc v1.79.1` and `go.opentelemetry.io/otel v1.40.0`.)

### 3. Each service's Docker build generates a **minimal** `go.work` — never copies the repo's

The repo's `go.work` lists every module in the workspace. Copying it into an image would make the build **fail**
the moment any sibling listed in `use(...)` isn't copied into that image's context (Go tries to
resolve every `use` directive and errors on the missing directory). Instead each `Dockerfile`
copies only the three modules the service compiles (`pkg`, `gen/go`, `services/<svc>`) and
**generates** a minimal workspace on the fly:

```dockerfile
RUN go work init ./pkg ./gen/go ./services/auth && go mod download -x all
```

This is **self-contained** (adding another service never breaks an existing image), **correct**
(`./pkg`/`./gen/go` resolve to local source exactly as on a laptop), and keeps **module
isolation honest** — a service image can only build against `pkg`, `gen/go`, and its own
source, never another service's. The builder image is pinned to `golang:1.26-alpine` to match
the workspace's `go 1.26.0` directive (an older toolchain refuses to build a newer workspace).

Deciding factor: Option D is the only one that delivers share-without-publish **and**
per-service dependency/build isolation; the gotchas are one-time costs already paid and
documented.

## Consequences

- **Positive:** shared `pkg`/`gen/go` with no `replace` lines and no releases; independent
  per-service dependency graphs; build-time module isolation that mirrors the runtime
  database-per-service boundary; current-practice monorepo story.
- **Negative:** three non-obvious workspace rules that must be held by discipline (no
  `v0.0.0` requires; pin against `pkg`'s versions; minimal in-build `go.work`); the builder Go
  version must be bumped in lockstep with the `go.work` `go` directive.
- **Follow-ups:** CI and `make docker-build` must invoke `docker build` with the **repo root**
  as context (`-f services/<svc>/Dockerfile <REPO_ROOT>`) so the sibling modules are reachable;
  a repo-root `.dockerignore` keeps that context cheap.
