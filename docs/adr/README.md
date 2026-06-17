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
