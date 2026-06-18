# Forgepoint Policy-as-Code (Kyverno)

Admission-time **policy-as-code** for the Forgepoint platform. These
[Kyverno](https://kyverno.io) `ClusterPolicy` objects enforce — at the
Kubernetes API server, before anything is written to etcd — the same security
posture we already hand-wrote into the Helm chart
(`deploy/helm/fp-auth/templates/`) and the infra manifests
(`deploy/k8s/infra/**`). Without them, that hardening is only a *convention* any
future manifest could quietly drop. With them, a non-conforming pod is either
**flagged** (audit) or **rejected** (enforce).

This is part of milestone **M6 — Platform security, GitOps & autoscaling**
(policy-as-code with Kyverno).

---

## What the policies enforce

| Policy | What it requires | Mirrors |
|--------|------------------|---------|
| `require-run-as-nonroot` | `runAsNonRoot=true` (pod or every container) | fp-auth podSecurityContext; infra `runAsUser` 999/1000 |
| `disallow-privilege-escalation` | `allowPrivilegeEscalation=false` on all containers | every container securityContext |
| `drop-all-capabilities` | `capabilities.drop` contains `ALL` | every container securityContext |
| `require-readonly-rootfs` | `readOnlyRootFilesystem=true` (**exception:** `app.kubernetes.io/name=postgres`) | fp-auth, redis, nats, minio |
| `require-seccomp-runtimedefault` | `seccompProfile.type` is `RuntimeDefault`/`Localhost` | all pods' podSecurityContext |
| `require-resource-requests-and-limits` | cpu+memory **requests AND limits** on every container | fp-auth `resources`; all infra pods |
| `disallow-latest-tag` | image is tagged and not `:latest` | pinned infra tags; appVersion tag |
| `restrict-image-registries` | image from the **allowlist** (parameterized) | ghcr today, ECR in prod, distroless, pinned infra |
| `require-part-of-label` | `app.kubernetes.io/part-of=forgepoint` on pod templates | recommended labels everywhere |
| `disallow-host-namespaces` | no `hostPID`/`hostIPC`/`hostNetwork` | (none of ours use them) |
| `disallow-host-path` | no `hostPath` volumes | we use PVCs/emptyDir only |
| `disallow-host-ports` | no `hostPort` | we use ClusterIP Services only |
| `require-cloud-keys-from-secret` | cloud LLM API keys (`FP_OPENAI_API_KEY`/`FP_ANTHROPIC_API_KEY`) never inline — Secret only | fp-ai-gateway `templates/secret.yaml` (envFrom a Secret) |
| `require-ai-allowlist` | fp-ai-gateway ConfigMap sets a non-empty `FP_AI_ALLOWED_MODELS` outside `fp-dev` (no wildcard-allow in non-dev) | the gateway's runtime allow-list (`domain/allowlist.go`) |

### AI governance (M7/L5) — `40-ai-gateway-policies.yaml`

Two policies govern the **AI gateway** at admission, complementing its in-process
governance (the runtime model/provider **allow-list** and the **prompt/response
audit** trail):

- **`require-cloud-keys-from-secret`** forbids an inline env `value:` for the cloud
  LLM keys (`FP_OPENAI_API_KEY` / `FP_ANTHROPIC_API_KEY`) on any fp-* container — they
  must be sourced from a Secret (the chart uses `envFrom` a Secret). An inline
  credential would leak into the manifest, git, and every `kubectl -o yaml`. The
  key-gated default (key not set at all) passes trivially.
- **`require-ai-allowlist`** requires the **fp-ai-gateway ConfigMap** (selected by the
  `app.kubernetes.io/name: fp-ai-gateway` label) to carry a **non-empty
  `FP_AI_ALLOWED_MODELS`** outside the **`fp-dev`** namespace — a wildcard-allow
  (empty = allow-all) data plane is the ungoverned posture this milestone closes, so
  it's disallowed in non-dev. `fp-dev` is excluded so the permissive dev default stays
  frictionless locally.

### The documented `readOnlyRootFilesystem` exception

`require-readonly-rootfs` **excludes** pods labeled
`app.kubernetes.io/name=postgres`. The official `postgres` image's entrypoint
writes to transient paths and does not cleanly support a fully read-only root FS
(see the comments in `deploy/k8s/infra/postgres/postgres.yaml`), so postgres
confines writes to its PVC + an emptyDir socket dir instead. The carve-out is a
**narrow, named, label-selected** exclusion — not a blanket hole. Adding another
genuinely-incompatible image means extending that exclusion explicitly and
documenting why.

### The parameterized registry allowlist

`restrict-image-registries` keeps its allowlist in **one** place — the
`allowedRegistries` context variable in
`base/20-image-policies.yaml`. Add or remove a trusted registry by editing that
single list. It currently allows:

- `*.dkr.ecr.*.amazonaws.com/*` — prod ECR (any account/region; M5 AWS plan)
- `ghcr.io/abd-ulbasit/forgepoint/*` — where the chart points **today**
- `gcr.io/distroless/*` — the distroless runtime base (hermetic builds)
- the **specific pinned infra images**: `postgres`, `redis`, `nats`,
  `minio/minio`, `minio/mc` (both short and `docker.io/...` spellings)

---

## Scope: `fp-*` namespaces only

Every rule **matches** only `fp-system`, `fp-models`, `fp-jobs`, `fp-infra` and
**excludes** `kube-system`, `kube-node-lease`, `kube-public`. System pods (CNI,
kube-proxy, CSI) legitimately need privilege/hostPath/root — blocking them would
break the cluster. We constrain blast radius to what we own. (We `match` on
`Pod`; Kyverno's **auto-gen** transparently extends each rule to the pod
templates of Deployment/StatefulSet/DaemonSet/Job/CronJob/ReplicaSet, so we
don't enumerate every workload kind.)

---

## Rollout: audit first, then enforce

The two overlays exist so you can **observe before you block**. Same policy
logic in both; they differ only in `validationFailureAction` (`Audit` vs
`Enforce`).

```
base/ (Audit)
  ├── overlays/audit/    → pass-through (Audit): log violations, allow pods
  └── overlays/enforce/  → patch Audit→Enforce: DENY violating pods
```

### Step 0 — install Kyverno (once, out of band)

```bash
# Not done by these manifests. e.g.:
helm repo add kyverno https://kyverno.github.io/kyverno/
helm install kyverno kyverno/kyverno -n kyverno --create-namespace
```

### Step 1 — deploy in AUDIT and watch

```bash
kubectl apply -k deploy/policies/overlays/audit

# What WOULD be blocked? (background scan covers existing resources too)
kubectl get clusterpolicyreport -A
kubectl get policyreport -A -o wide
# Drill into a specific failing result:
kubectl get policyreport -n fp-system -o yaml | grep -A6 'result: fail'
```

Fix every reported `fail` (or add a documented exception) until the reports are
clean. **Do not skip this** — flipping straight to enforce can wedge a rollout.

### Step 2 — flip to ENFORCE

```bash
kubectl apply -k deploy/policies/overlays/enforce
```

Now the API server **rejects** new violating pods at admission. (Already-running
pods aren't retroactively killed; they're caught on their next create/update —
e.g. the next rollout.)

### Rollback (instant)

```bash
kubectl apply -k deploy/policies/overlays/audit   # sets the field back to Audit
```

No deletion needed — re-applying the audit overlay rewrites
`validationFailureAction` back to `Audit`.

---

## How this complements PodSecurity admission

Kubernetes ships a built-in **Pod Security admission** controller that enforces
the three [Pod Security Standards](https://kubernetes.io/docs/concepts/security/pod-security-standards/)
(`privileged` / `baseline` / `restricted`) via **namespace labels**:

```yaml
metadata:
  labels:
    pod-security.kubernetes.io/enforce: restricted
```

These Kyverno policies **complement** — they don't replace — PodSecurity:

| | Pod Security admission | Kyverno (these policies) |
|---|---|---|
| Granularity | 3 fixed profiles, all-or-nothing per namespace | per-rule, fine-grained, individually toggled |
| Exceptions | none built in (exempt = whole namespace) | **narrow, labeled** (e.g. postgres readonly-rootfs) |
| Coverage | pod securityContext only | + **registries, image tags, resource limits, labels, hostPort** |
| Reporting | deny only (no "audit-but-allow" inventory of *all* violations) | rich PolicyReports, background scan of existing objects |
| Custom rules | not possible | yes (any field, JMESPath conditions) |

**Defense in depth:** label the `fp-*` namespaces
`pod-security.kubernetes.io/enforce: restricted` as a coarse backstop, and run
these Kyverno policies for the parts PSA can't express — registry allowlists,
`:latest` bans, mandatory resource limits, the `part-of` label, and the
**documented per-workload exceptions** PSA has no way to model. If one layer is
misconfigured, the other still holds.

> Interview probe — "Why Kyverno *and* PodSecurity?" PSA is the cheap,
> always-on baseline (no extra component); Kyverno covers everything PSA can't
> express and gives auditable reports + surgical exceptions. They overlap on the
> securityContext basics on purpose — that overlap is the defense-in-depth.

---

## Files

```
deploy/policies/
├── README.md                      ← this file
├── base/                          ← all policies, validationFailureAction: Audit
│   ├── kustomization.yaml
│   ├── 00-pod-security.yaml        ← nonroot, no-priv-esc, drop-all-caps,
│   │                                  readonly-rootfs (+postgres exception),
│   │                                  seccomp RuntimeDefault
│   ├── 10-require-resources.yaml   ← requests + limits (cpu & memory)
│   ├── 20-image-policies.yaml      ← disallow-latest-tag, restrict-registries
│   ├── 30-workload-policies.yaml   ← part-of label, host ns/path/port
│   └── 40-ai-gateway-policies.yaml ← AI governance: cloud-keys-from-secret,
│                                       require-ai-allowlist (non-dev)
└── overlays/
    ├── audit/kustomization.yaml    ← Step 1: observe (pass-through)
    └── enforce/kustomization.yaml  ← Step 2: patch Audit→Enforce (block)
```

## Validating locally

```bash
# YAML parses for every file:
find deploy/policies -name '*.yaml' -print0 | xargs -0 -I{} python3 -c \
  'import sys,yaml; list(yaml.safe_load_all(open(sys.argv[1])))' {}

# Overlays render (requires kubectl/kustomize):
kubectl kustomize deploy/policies/overlays/audit
kubectl kustomize deploy/policies/overlays/enforce | grep validationFailureAction
# → every line should read "Enforce" for the enforce overlay.
```
