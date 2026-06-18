# Architecture Decision Records

Every non-obvious architectural decision is captured here as an ADR (see the project
standards in `CLAUDE.md`). ADRs are immutable once Accepted — to change a decision, add a new
ADR that supersedes the old one and update the status.

**Format:** Context → Options Considered → Decision → Consequences. Number sequentially,
zero-padded (`0001`, `0002`, …). Use `docs/adr/template.md` as the starting point.

| # | Title | Status | Date |
|---|-------|--------|------|
| [0001](0001-bff-for-web-ui.md) | Backend-for-Frontend (BFF) for the Web UI | Accepted | 2026-06-10 |
| [0002](0002-gitops-with-argocd.md) | GitOps delivery with ArgoCD | Accepted | 2026-06-17 |
| [0003](0003-closed-loop-model-monitor.md) | Closed-loop model monitoring (Model Monitor service) | Accepted | 2026-06-17 |
| [0004](0004-decoupled-event-schema.md) | Decoupled event schema (`forgepoint.events.v1`) | Accepted | 2026-06-17 |
| [0005](0005-ports-in-domain-hexagonal.md) | Hexagonal ports live in the domain package | Accepted | 2026-06-17 |
| [0006](0006-go-workspace-monorepo.md) | Go-workspace monorepo with one module per service | Accepted | 2026-06-17 |
| [0007](0007-one-pattern-per-service.md) | One distributed-systems pattern per service | Accepted | 2026-06-17 |
| [0008](0008-istio-service-mesh.md) | Istio service mesh — STRICT mTLS, deny-by-default authz, mesh resilience | Accepted | 2026-06-18 |
| [0009](0009-tamper-evident-audit-log.md) | Tamper-evident, hash-chained audit log (interceptor capture + append-only sink) | Accepted | 2026-06-18 |
