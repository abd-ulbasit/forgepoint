# ADR 0002: GitOps delivery with ArgoCD

**Status:** Accepted
**Date:** 2026-06-17
**Deciders:** Abdul Basit Sajid
**Context phase:** Phase 20 (Milestone M6)

## Context

The initial CI/CD (Phase 11) deploys with a **push** step: a GitHub Actions job runs
`helm upgrade` against the cluster after building an image. This works, but it has the classic
push-deploy problems: the cluster's actual state can drift from Git undetected; CI needs
long-lived cluster credentials; rollbacks are ad-hoc; and there's no single, auditable source
of truth for "what is deployed where." With 11 services plus infrastructure charts, the
drift-detection gap is the one that actually bites: a manual `kubectl edit` during an incident
survives indefinitely and silently.

## Options Considered

### Option A — CI push (`helm upgrade` from GitHub Actions)
- **Pro:** simplest; already working; no extra cluster components.
- **Con:** state drift goes unnoticed; CI holds cluster creds (blast radius); imperative
  rollback; no reconciliation/self-healing; "what's running?" requires querying the cluster.

### Option B — GitOps pull with **ArgoCD** (chosen)
- **Pro:** Git is the single source of truth; a controller continuously **reconciles** and
  **self-heals** drift; rollback = `git revert`; no cluster creds in CI; great UI + audit;
  app-of-apps scales cleanly to N services.
- **Con:** another platform component to run; a learning curve.

### Option C — GitOps pull with **Flux**
- **Pro:** lighter, more composable, strong image-automation story (Image Update Automation is
  first-party rather than a separate component).
- **Con:** no first-party UI, so sync and health state across 11 services is read through
  `flux get` per-kind rather than one view; the app-of-apps equivalent is a Kustomization tree,
  which works but expresses the parent/child relationship less directly.

## Decision

Adopt **Option B — ArgoCD** using the **app-of-apps** pattern (`deploy/argocd/`). CI's
responsibility ends at **build → scan → sign → push → bump the image tag in Git**; ArgoCD
deploys. `dev` auto-syncs; `prod` is manual/PR-gated. The `helm upgrade` step is removed from
Phase 11.

Deciding factors: self-healing reconciliation and Git-as-truth are the substantive wins, and
ArgoCD's app-of-apps ergonomics make the delivery state legible. Flux would be a fine
alternative; the choice is about ergonomics, not capability.

## Consequences

- **Positive:** auditable, declarative deploys; revert-based rollback; no cluster creds in CI;
  drift detection; one view of sync/health state across every service.
- **Negative:** ArgoCD must be installed/operated; image-tag bumps must land in Git (CI commit
  or ArgoCD Image Updater).
- **Follow-ups:** progressive delivery (Argo Rollouts, ADR-adjacent Phase 21) layers on top;
  Sealed Secrets (Phase 17) is what makes secrets safe to keep in the GitOps repo.
