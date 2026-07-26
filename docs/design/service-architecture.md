# Canonical Service Architecture (the template every service follows)

> This is the reference structure for all 10 microservices, distilled from the Auth service
> (the first one built). Every new service mirrors this so the platform stays uniform. When
> in doubt, read `services/auth/` — it is the worked example.

## Clean Architecture / Hexagonal layers

```
services/<svc>/
├── cmd/server/main.go              entrypoint: wiring only (config, otel, grpc, health, shutdown)
├── internal/
│   ├── domain/                     BUSINESS CORE — zero framework imports
│   │   ├── models.go               pure domain types (no proto/grpc/sql/nats)
│   │   ├── <svc>_service.go        the service INTERFACE (the use-case contract)
│   │   ├── <svc>_service_impl.go   the business logic implementing that interface
│   │   ├── ports.go                repository/event PORT interfaces (see "Ports live in domain")
│   │   ├── errors.go               domain sentinel errors (errors.Is-friendly)
│   │   └── *_test.go               unit tests (mock the ports; TDD)
│   ├── repository/
│   │   └── postgres/               ADAPTER: implements domain ports with pgx (+ testcontainers tests)
│   ├── handler/                    ADAPTER: gRPC handler — proto↔domain conversion + validation
│   │   └── <svc>_handler.go
│   └── events/                     ADAPTER: NATS publishers/subscribers (pkg/natsutil)
├── migrations/                     golang-migrate SQL (NNN_name.up.sql / .down.sql)
├── Dockerfile                      multi-stage, distroless, nonroot
└── go.mod                          module github.com/abd-ulbasit/forgepoint/services/<svc>
```

**The dependency rule:** `handler → domain ← repository/postgres`, `events → domain`. Everything
points *inward* at the domain. The domain depends on nothing but the standard library.

## Ports live in the DOMAIN (not in repository/)  — learned the hard way

The domain defines the interfaces it needs (`UserRepository`, etc.) in `domain/ports.go`. The
Postgres adapter in `internal/repository/postgres` *implements* them. **Do NOT** put the port
interfaces in the `repository` package: the domain's service impl must reference them, and the
port signatures reference domain types — so `domain → repository → domain` is a compile-time
**import cycle**. "Consumer owns the port" (hexagonal) avoids it. (A thin `internal/repository`
alias shim re-exporting the domain ports is optional sugar; the source of truth is `domain`.)

## Workspace / module wiring (already-solved gotchas — don't repeat)

- Each service is its own module in `go.work`'s `use(...)`. Sibling modules (`pkg`, `gen/go`)
  resolve via `use` — **do NOT add `require github.com/abd-ulbasit/forgepoint/{pkg,gen/go} v0.0.0`**;
  an explicit version on an unpublished module makes Go do a VCS lookup and fail
  (`unknown revision .../v0.0.0`). Let `go work sync` manage requires; add only *external* deps
  with `go get <pkg>@<version>`.
- Pin external deps to the versions `pkg` already uses (grpc v1.79.1, x/crypto v0.47.0, otel
  v1.40.0) to avoid a workspace-wide version cascade (a newer grpc pulled a newer otel that broke
  observability). After adding deps, confirm `cd pkg && go test ./observability/` still passes.

## Generated code & contracts

- gRPC types come from `github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/<pkg>/v1`.
- Cross-service async events use the canonical, self-contained schema package
  `forgepoint/events/v1` (see `docs/design/event-contract.md`) — the event schema is decoupled
  from API/domain types on purpose. Producers/consumers share these payload messages; the NATS
  `EventEnvelope.data` (Any) carries them.

## Shared libraries (always reuse — never reinvent)

`pkg/config` (env loader), `pkg/observability` (one-call OTel; set `OTLPInsecure` from config),
`pkg/grpcutil` (server + recovery→logging→auth interceptor chain, bounded graceful drain),
`pkg/natsutil` (envelope, DLQ, idempotency, trace propagation), `pkg/health` (/healthz,/readyz),
`pkg/testutil` (testcontainers + bufconn).

## Security defaults (every service)

Server-authoritative fields (ids/owner/timestamps/status/amounts) — never client-set
(anti mass-assignment). Identity/team from auth claims, not request fields. No secrets/PII in
logs or error responses (interceptor sanitizes status messages). Validate/cap all list page
sizes and batch sizes. Idempotency keys on mutating RPCs. crypto/rand for tokens/ids;
constant-time compares for secrets.

## Testing (TDD, per CLAUDE.md)

Unit (domain, mock ports, `-race`) → Integration (testcontainers Postgres/NATS/Redis on the
remote thinkpad engine; see the offload memory) → Component (bufconn in-process gRPC). Tests must
verify **real behavior** (e.g. the stored hash actually verifies), not just mock interactions.
Coverage target: 80%+ domain, 70%+ handlers.

## Observability & health

`observability.Setup` once in `main`; expose `/healthz` (liveness) + `/readyz` (readiness with
real dependency checks). RED metrics + structured slog (trace-correlated) + traces across the
sync (gRPC) and async (NATS) hops.
