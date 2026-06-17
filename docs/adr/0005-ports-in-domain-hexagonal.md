# ADR 0005: Hexagonal ports live in the domain package

**Status:** Accepted
**Date:** 2026-06-17
**Deciders:** Abdul Basit Sajid
**Context phase:** Implementation Plan — Phase 1.3/1.4 (Auth domain + repositories), generalized to every service

## Context

Every Forgepoint service follows Clean / Hexagonal Architecture with the dependency rule
`handler → domain ← repository`: everything points **inward** at the domain, and the domain
imports nothing but the standard library (plus, by exception, `google/uuid`). The domain
defines the **business logic** and the **interfaces it needs** to reach persistence and
external systems — repository ports (`UserRepository`, `WriteStore`, `ReadStore`), and other
collaborator ports (`ProjectionEmitter`, `Clock`, `IDGenerator`).

The open question is purely about **placement**: in which Go *package* do those port
interfaces live? There are two natural-sounding homes — next to the data-access code they
describe (`internal/repository`), or inside the domain that consumes them (`internal/domain`).
This looks like a style nit. It is not — Go's package-level import graph turns it into a
build-or-no-build decision, and we **hit the failure concretely** while scaffolding Auth.

## Options Considered

### Option A — Ports in the `repository` package (data layer owns the interface)

The interface `UserRepository` lives in `internal/repository`; the Postgres adapter
implements it there; the domain service imports `repository` to call it.

- **Pro:** superficially intuitive — "the repository package defines what a repository is."
- **Pro:** matches how some other-language stacks (a Java `…repository` package) are laid out.
- **Con — fatal:** it creates a **compile-time import cycle** the moment a domain consumer
  uses the port:
  - The port signatures reference **domain types** (`Create(ctx, User) (User, error)`), so
    `repository` must `import domain`.
  - The domain service impl (`auth_service_impl.go`, which lives **in** `domain`) must call
    the port, so `domain` must `import repository`.
  - `domain → repository → domain` is a cycle. Go **rejects it at build time** — this is not a
    test-only or lint-only artifact; the package will not compile.
- **Con:** the cycle is not hypothetical. The Auth scaffold originally placed the interfaces
  in `internal/repository`, and the build broke exactly here (documented in
  `services/auth/internal/domain/ports.go`).

### Option B — Ports in the `domain` package (consumer owns the port — chosen)

The domain defines the interfaces it needs (`domain/ports.go`); the adapters in
`internal/repository/postgres` (and Redis, NATS, etc.) `import domain` and **implement** them.

- **Pro:** **no cycle** — a single inward arrow. `domain` depends on nothing; `postgres`,
  `handler`, and `events` all depend on `domain` one-way:

  ```
  domain  (AuthService + repository PORTS + models)   ← stdlib only
     ▲
     │ implements / converts
  postgres adapter        handler (proto ↔ domain)        events adapter
  ```
- **Pro:** it is the **idiomatic Go / Hexagonal** rule — *"the consumer defines the interface
  it needs."* The domain is the consumer of persistence, so the port belongs to the domain.
  The adapter is a detail that conforms to the port, not the other way around (Dependency
  Inversion: both sides depend on the abstraction, and the abstraction lives with the
  high-level policy).
- **Pro:** tests mock the ports trivially from within `domain` (no cross-package mock
  packages, no cycle in tests either).
- **Con:** developers arriving from layouts where interfaces sit beside their implementations
  may expect to find `UserRepository` under `repository/` — surprising at first.

## Decision

Adopt **Option B**: **repository and external-collaborator PORT interfaces live IN the
`domain` package** (`internal/domain/ports.go`); **adapters in `internal/repository/…` (and
`internal/events/`) implement them**. This is binding for every one of the 10 services and is
the worked example in `docs/design/service-architecture.md`.

A thin `internal/repository` package may exist as **optional sugar** — type aliases that
re-export the domain ports so `repository.UserRepository` still resolves — but the **source of
truth is `domain`**. The alias shim must never define the interface itself, or the cycle
returns.

Deciding factor: Option A does not compile. Beyond that brute fact, Option B is the correct
Hexagonal placement — the high-level domain owns its ports and the low-level adapters conform,
which is the whole point of "ports and adapters."

## Consequences

- **Positive:** the domain compiles against the standard library alone; one inward dependency
  arrow per service; adapters and handlers are swappable details; mocks live with the domain
  for friction-free TDD.
- **Negative:** a one-time orientation cost ("why is `UserRepository` in `domain`, not
  `repository`?") — mitigated by the teaching header in every `ports.go` and the canonical
  doc.
- **Interview framing:** *"Where do repository interfaces belong — the data layer or the
  business layer?"* In Hexagonal Architecture the **consumer owns the port**; the domain is
  the consumer of persistence, so the interface lives in the domain and the database
  *implements* it. Defining the port in the DB package while the domain consumes it forces
  `domain → repository → domain` — a cycle Go rejects the instant a domain type satisfies
  another's contract, which is exactly what we hit in the Auth scaffold.
- **Follow-ups:** the same rule extends to non-repository ports — `ProjectionEmitter`
  (registry's CQRS sync seam), `Clock`, and `IDGenerator` are all consumer-owned ports in
  `domain`, injected for determinism and decoupling, implemented by adapters in `main`/`events`.
