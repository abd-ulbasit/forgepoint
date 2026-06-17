# Forgepoint GitOps — ArgoCD (app-of-apps)

Declarative, **pull-based** delivery for the whole platform. Git is the single
source of truth; an in-cluster ArgoCD controller continuously **reconciles** the
cluster to match Git and **self-heals** drift. This is the realization of
[ADR-0002](../../docs/adr/0002-gitops-with-argocd.md) and Milestone **M6 (GitOps)**.

> **This REPLACES the CI `helm upgrade` push step.** CI no longer deploys.
> CI's job ends at *build → scan → sign → push image → bump the image tag in Git*.
> ArgoCD does the deploying. No cluster credentials live in CI anymore.

`REPO_URL` placeholder used throughout: `https://github.com/abd-ulbasit/forgepoint.git`
(swap for your fork in `project.yaml`, `root.yaml`, and each `apps/*.yaml`).

---

## Layout

```
deploy/argocd/
├── project.yaml        AppProject "forgepoint" — the guardrail (least privilege):
│                         allowed source repos, destination namespaces (fp-*),
│                         and allowed resource kinds. Denies cluster RBAC.
├── root.yaml           ROOT Application (app-of-apps). The ONE thing you apply by
│                         hand. Watches apps/ and manages the child Applications.
├── apps/               Child Applications (root reconciles this directory):
│   ├── infra.yaml        Kustomize → deploy/k8s/infra  (NATS/PG/Redis/MinIO/ns)
│   └── fp-auth.yaml      Helm      → deploy/helm/fp-auth (THE per-service template)
└── README.md           this file
```

### How it fits together

```
   operator (once)                 ArgoCD controller (forever)
   ──────────────                  ───────────────────────────
   kubectl apply project.yaml  ─►  AppProject enforces guardrails on every app
   kubectl apply root.yaml     ─►  root watches apps/ ──► creates child Apps
                                        │
                                        ├─ fp-infra (wave -1) ─► fp-infra ns
                                        └─ fp-auth  (wave  0) ─► fp-system ns
                                              ... +9 services, one file each
```

**Pull, not push:** nothing outside the cluster reaches in to deploy. The
controller *pulls* desired state from Git and applies it from inside the cluster.
That's why CI needs no kubeconfig/cluster creds — a major blast-radius reduction.

**Reconciliation & self-heal:** ArgoCD diffs live state vs. Git on a loop
(default ~3 min, plus a Git webhook for instant sync). `selfHeal: true` reverts
out-of-band `kubectl` edits; `prune: true` deletes resources whose manifests left
Git. Rollback is therefore just `git revert` — the controller converges to the
reverted state. No imperative `helm rollback`.

---

## Bootstrap (do NOT run as part of this task — reference only)

```bash
# 1. Install ArgoCD into the argocd namespace (Helm or upstream manifests).
# 2. Apply the guardrail, then the root. That's the entire bootstrap.
kubectl apply -n argocd -f deploy/argocd/project.yaml
kubectl apply -n argocd -f deploy/argocd/root.yaml
# Everything else (infra + every service) appears via the app-of-apps.
```

Validate manifests parse without a cluster:

```bash
# YAML well-formedness for all argocd manifests:
find deploy/argocd -name '*.yaml' -print0 | xargs -0 -I{} sh -c \
  'python3 -c "import sys,yaml; list(yaml.safe_load_all(open(sys.argv[1])))" {} && echo OK {}'
```

---

## Adding the other 9 services — a one-file copy

`apps/fp-auth.yaml` is the template (Chart.yaml already says every service
mirrors the `fp-auth` chart; the delivery layer mirrors it too):

```bash
cp deploy/argocd/apps/fp-auth.yaml deploy/argocd/apps/fp-registry.yaml
# edit three fields:
#   metadata.name      : fp-registry
#   source.path        : deploy/helm/fp-registry
#   helm.releaseName   : fp-registry
git add deploy/argocd/apps/fp-registry.yaml && git commit && git push
# root app-of-apps notices the new file and creates the Application. No kubectl.
```

project/destination/sync-policy stay identical for every stateless service, so
the diff per service is ~3 lines. (Once all 10 are truly uniform, an ArgoCD
**ApplicationSet** with a list generator collapses these into one templated file
— the natural next refactor; kept as explicit files now for readability.)

---

## How images get updated (the deploy trigger)

Two supported paths. **Git is always the trigger** — ArgoCD only ever reacts to
a commit.

**1. CI bumps the tag in Git (default, used here).**
After build → scan → sign → push, CI rewrites the image tag in Git and pushes:

```
service code change
   → CI builds image  ghcr.io/abd-ulbasit/forgepoint/auth:<git-sha>
   → CI scans + signs + pushes the image
   → CI commits the new tag to Git:
        apps/fp-auth.yaml  helm.parameters[image.tag] = <git-sha>
        (or a per-env values file:  values-prod.yaml  image.tag: <git-sha>)
   → ArgoCD sees the commit → syncs → new pods roll out
```

The image tag is the *one* value CI writes. Use immutable tags (git SHA / semver),
never `:latest` — then rollback is a `git revert` of the tag bump.

**2. ArgoCD Image Updater (optional automation).**
The Image Updater controller watches the registry for new tags matching a policy
(e.g. semver range, or newest by build time) and writes the bump back to Git
*for* you via a `write-back` commit — so even the tag bump becomes hands-off
while staying Git-auditable. Enable per-app with annotations, e.g.:

```yaml
metadata:
  annotations:
    argocd-image-updater.argoproj.io/image-list: auth=ghcr.io/abd-ulbasit/forgepoint/auth
    argocd-image-updater.argoproj.io/auth.update-strategy: semver
    argocd-image-updater.argoproj.io/write-back-method: git
```

Recommendation: Image Updater on **dev** (continuous), CI-commit + PR on **prod**
(so the tag bump is reviewed like any change).

---

## Dev vs Prod sync strategy (values-per-env)

CLAUDE.md M6: **dev auto-syncs; prod is manual / PR-gated.** Same manifests, a
different `syncPolicy`. The dev variant is what's committed in `apps/`.

| Aspect            | **dev** (`apps/*.yaml`, committed) | **prod** (overlay / branch)              |
|-------------------|------------------------------------|------------------------------------------|
| `syncPolicy.automated` | present                       | **absent** → manual sync only            |
| selfHeal          | `true` (drift auto-reverted)       | `false` (don't fight an incident edit)   |
| prune (services)  | `true`                             | `false` (no deletes without approval)    |
| prune (infra)     | `false` always (data-loss guard)   | `false`                                  |
| deploy trigger    | commit → auto-sync (~min)          | commit shows **OutOfSync** → human Syncs |
| image tag source  | Image Updater or CI commit         | CI commit via **reviewed PR**            |
| Helm values       | `values.yaml`                      | `values.yaml` + `values-prod.yaml`       |
| `targetRevision`  | `main`                             | pinned tag/branch (e.g. `release-1.4`)   |

**Why prod is manual:** auto-sync to prod means any merge to the tracked branch
deploys to production unreviewed. PR-gating puts a human approval (the PR review)
between "merged" and "in prod". ArgoCD still *detects* the drift and renders the
exact diff; a person decides *when* to apply it (click Sync / `argocd app sync`).

**Recommended prod variant of a service app** (illustrative — not applied here):

```yaml
# deploy/argocd/apps-prod/fp-auth.yaml   (a parallel prod directory or branch)
spec:
  source:
    repoURL: https://github.com/abd-ulbasit/forgepoint.git
    targetRevision: release-1.4          # pinned, not main
    path: deploy/helm/fp-auth
    helm:
      releaseName: fp-auth
      valueFiles:
        - values.yaml
        - values-prod.yaml               # External Secrets, real tag, more replicas
  destination:
    server: https://kubernetes.default.svc   # or a remote prod cluster URL
    namespace: fp-system
  syncPolicy: {}                         # MANUAL: no `automated:` block at all
```

Wire it with a **prod root** app-of-apps whose `path: deploy/argocd/apps-prod`
and whose own `syncPolicy` is also manual, so even adding a prod app is reviewed.

---

## Guardrails recap (`project.yaml`)

The AppProject is least privilege for the delivery layer itself:

- **sourceRepos** — only the forgepoint repo may be a source (supply-chain guard).
- **destinations** — only `fp-system / fp-infra / fp-models / fp-jobs` (+ `argocd`
  for the child Application CRs). Nothing lands in `kube-system` / `default`.
- **clusterResourceWhitelist** — only `Namespace` and `CustomResourceDefinition`.
  **No ClusterRole/ClusterRoleBinding** — a child app can't mint cluster-wide RBAC.
- **namespacedResourceWhitelist** — exactly the kinds the fp-auth chart + infra
  kustomize render (Deployment, Service, ConfigMap, Secret, HPA, PDB,
  NetworkPolicy, namespaced Role/RoleBinding, ServiceMonitor, ...). Deny-by-default:
  add a kind here the day a chart needs it.

---

## Secrets note

GitOps means manifests live in Git — so plaintext secrets must NOT. The fp-auth
chart's in-chart Secret holds **dev-only placeholders** (see its values.yaml
SECURITY CONTRACT). In prod, `secrets.create: false` and the real Secret comes
from **External Secrets Operator** or **Sealed Secrets** (ADR-0002 follow-up,
Phase 17) — the encrypted/sealed material is safe to commit, the plaintext never is.
```
