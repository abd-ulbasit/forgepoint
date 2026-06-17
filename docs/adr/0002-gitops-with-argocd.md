# ADR 0002: GitOps delivery with ArgoCD

**Status:** Accepted
**Date:** 2026-06-17
**Deciders:** Abdul Basit Sajid
**Context phase:** Implementation Plan — Phase 20 (Milestone M6)

## Context

The initial CI/CD (Phase 11) deploys with a **push** step: a GitHub Actions job runs
`helm upgrade` against the cluster after building an image. This works, but it has the classic
push-deploy problems: the cluster's actual state can drift from Git undetected; CI needs
long-lived cluster credentials; rollbacks are ad-hoc; and there's no single, auditable source
of truth for "what is deployed where." For a platform-engineering portfolio, declarative
delivery is also one of the highest-signal capabilities to demonstrate.

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
- **Pro:** lighter, more composable, strong image-automation story.
- **Con:** less of a visual/demoable artifact than ArgoCD; for a portfolio, ArgoCD's UI and
  app-of-apps tell the story better.

## Decision

Adopt **Option B — ArgoCD** using the **app-of-apps** pattern (`deploy/argocd/`). CI's
responsibility ends at **build → scan → sign → push → bump the image tag in Git**; ArgoCD
deploys. `dev` auto-syncs; `prod` is manual/PR-gated. The `helm upgrade` step is removed from
Phase 11.

Deciding factors: self-healing reconciliation and Git-as-truth are the substantive wins, and
ArgoCD is the more demonstrable artifact in an interview. Flux would be a fine alternative; the
choice is about presentation and the app-of-apps ergonomics, not capability.

## Consequences

- **Positive:** auditable, declarative deploys; revert-based rollback; no cluster creds in CI;
  drift detection; a strong visual demo.
- **Negative:** ArgoCD must be installed/operated; image-tag bumps must land in Git (CI commit
  or ArgoCD Image Updater).
- **Follow-ups:** progressive delivery (Argo Rollouts, ADR-adjacent Phase 21) layers on top;
  Sealed Secrets (Phase 17) is what makes secrets safe to keep in the GitOps repo.
