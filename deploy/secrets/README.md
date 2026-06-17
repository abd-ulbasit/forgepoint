# Forgepoint secrets management

How Forgepoint keeps credentials (DB passwords, the JWT signing key, the Redis auth
token) **out of Git while still doing GitOps**. Two paths, picked by environment.

> **The cardinal rule: NO plaintext secret is ever committed to this repo.**
> A Kubernetes `Secret` in Git stores its values as `base64`, which is *encoding,
> not encryption* — trivially reversible. Committing one leaks the credential to
> anyone with repo (or fork, or git-history) access, forever. Everything in this
> directory is either a **pointer** to a secret (ESO) or **ciphertext** that only a
> specific cluster can decrypt (Sealed Secrets). Both are safe to commit; neither
> contains a readable secret. Placeholders (`<AWS_ACCOUNT_ID>`, `<AWS_REGION>`,
> `REDACTED_...`) stand in for anything environment-specific — there are no real
> ARNs, account IDs, or secret values anywhere here.

## The GitOps secrets problem (why this directory exists)

GitOps (ArgoCD — see [`docs/adr/0002-gitops-with-argocd.md`](../../docs/adr/0002-gitops-with-argocd.md))
makes **Git the single source of truth** and has a controller reconcile the cluster
to match. That is ideal for Deployments and ConfigMaps — but a secret can't go in
Git in plaintext. So we commit only *metadata about* the secret and let a
controller supply the real value at runtime, from a backend that Git never sees.
ADR-0002 explicitly notes: *"Sealed Secrets is what makes secrets safe to keep in
the GitOps repo."* This directory implements that, plus the cloud-native
alternative (External Secrets Operator) for the AWS environment.

## Two paths — when to use which

| | **External Secrets Operator (ESO)** | **Sealed Secrets** |
|---|---|---|
| **Use for** | Cloud (AWS EKS) — `dev`/`prod` | Homelab / local **k3s** (no cloud) |
| **Backend** | AWS Secrets Manager (KMS-encrypted) | The cluster's Sealed Secrets controller |
| **Auth to backend** | IRSA (no static keys) | n/a — asymmetric crypto, public key |
| **What's in Git** | A *pointer* (which secret, which keys) | *Ciphertext* (encrypted to the controller) |
| **Secret value lives** | In AWS; ESO syncs it into a K8s Secret | Encrypted in Git; controller decrypts in-cluster |
| **Rotation** | Auto-refresh from the store (e.g. RDS rotation) | Manual: re-seal + commit |
| **Directory** | [`external-secrets/`](./external-secrets/) | [`sealed-secrets/`](./sealed-secrets/) |

**Decision rule:** if the cluster is on AWS and Terraform has provisioned RDS/Redis
+ IRSA (see [`deploy/terraform/`](../terraform/)), use **ESO** — the credentials
already live in Secrets Manager and ESO reads them with a least-privilege IRSA role.
If the cluster has **no cloud backend** (the k3s homelab), use **Sealed Secrets** —
encrypt locally with `kubeseal`, commit the ciphertext, let the in-cluster
controller decrypt it.

Both paths converge on the **same K8s Secret name + keys**, so the application and
the Helm charts are identical across environments — only the fill mechanism differs.

## Cloud path: External Secrets Operator (`external-secrets/`)

```
AWS Secrets Manager            EKS (fp-system)                       Pod
┌──────────────────────┐   ┌──────────────────────────────────┐   ┌──────────────┐
│ <prefix>/rds/postgres│   │ ClusterSecretStore                │   │ fp-auth      │
│ <prefix>/elasticache/│◄──┤  aws/SecretsManager, auth=IRSA    │   │  envFrom ───┐│
│   redis              │   │   (SA external-secrets → role)    │   │  Secret     ││
│ <prefix>/auth/jwt    │   ├──────────────────────────────────┤   │  "fp-auth"  ◄┘
│  (operator-created)  │   │ ExternalSecret fp-auth ──────────►│───► K8s Secret  │
└──────────────────────┘   │ ExternalSecret fp-infra-creds ───►│   │  (ESO-owned) │
   (KMS-encrypted)         └──────────────────────────────────┘   └──────────────┘
```

Files:
- [`cluster-secret-store.yaml`](./external-secrets/cluster-secret-store.yaml) — one
  cluster-scoped `ClusterSecretStore` pointing at AWS Secrets Manager, authenticated
  via the **`external_secrets` IRSA role** (Terraform `modules/iam` +
  `environments/dev/main.tf`). No AWS keys in the manifest — ESO presents the
  controller SA's OIDC token to STS. Placeholders: `<AWS_REGION>`, and the role ARN
  `arn:aws:iam::<AWS_ACCOUNT_ID>:role/forgepoint-dev-irsa-external_secrets`.
- [`externalsecret-fp-auth.yaml`](./external-secrets/externalsecret-fp-auth.yaml) —
  materializes Secret `fp-auth` with `FP_JWT_SECRET` (from `<prefix>/auth/jwt`) and
  `FP_DATABASE_URL` (the `dsn` field of the RDS module's `<prefix>/rds/postgres`).
- [`externalsecret-fp-infra-creds.yaml`](./external-secrets/externalsecret-fp-infra-creds.yaml)
  — materializes Secret `fp-infra-creds` with the shared Postgres + Redis connection
  info (RDS `dsn`/host/port + ElastiCache `url`/host/port/`auth_token`).

### Secret name ↔ Terraform source (no values, only names)

| K8s Secret key | AWS SM secret (name) | JSON property | Terraform source |
|---|---|---|---|
| `FP_DATABASE_URL` | `<prefix>/rds/postgres` | `dsn` | `modules/rds` |
| `POSTGRES_HOST`/`PORT` | `<prefix>/rds/postgres` | `host`/`port` | `modules/rds` |
| `FP_REDIS_URL` | `<prefix>/elasticache/redis` | `url` | `modules/elasticache` |
| `REDIS_*` | `<prefix>/elasticache/redis` | `host`/`port`/`auth_token` | `modules/elasticache` |
| `FP_JWT_SECRET` | `<prefix>/auth/jwt` | `jwt_secret` | operator-created (see note) |

> **Note on `FP_JWT_SECRET`:** the HMAC signing key is an *application* secret, not
> a cloud resource, so Terraform does not mint it. An operator creates the
> `<prefix>/auth/jwt` Secrets Manager entry once (`aws secretsmanager create-secret
> --name forgepoint-dev/auth/jwt --secret-string '{"jwt_secret":"<openssl rand>"}'`)
> and adds its ARN to the `external_secrets` IRSA role's `secret_arns` in
> `environments/dev/main.tf`. The generated key never touches Git.

## Local path: Sealed Secrets (`sealed-secrets/`)

For the k3s homelab with no AWS. Encrypt a Secret locally with `kubeseal`, commit
the ciphertext, the controller decrypts it in-cluster. Full walkthrough (install,
seal, apply, rotate) in [`sealed-secrets/README.md`](./sealed-secrets/README.md);
example in [`sealed-secrets/sealedsecret-fp-auth.example.yaml`](./sealed-secrets/sealedsecret-fp-auth.example.yaml)
(placeholder ciphertext — regenerate against your own controller).

## Wiring `fp-auth` (same for both paths)

The `fp-auth` Helm chart already supports an externally-managed Secret. In any real
environment install it with:

```yaml
secrets:
  create: false           # chart renders NO Secret of its own (no plaintext in values)
  existingSecret: fp-auth  # the Deployment envFrom's this Secret instead
```

- **Cloud:** ESO's `ExternalSecret fp-auth` creates the `fp-auth` Secret.
- **Local:** the Sealed Secrets controller decrypts the committed `SealedSecret`
  into the `fp-auth` Secret.

Either way the pod consumes a Secret named `fp-auth` with `FP_JWT_SECRET` +
`FP_DATABASE_URL` via `envFrom` — the chart and app don't know or care which
mechanism filled it. The chart's `secrets.create=true` mode with placeholder values
remains **DEV/Kind only**; never put real credentials in `values.yaml`. See
[`deploy/helm/fp-auth/templates/secret.yaml`](../helm/fp-auth/templates/secret.yaml).

## What this repo does NOT do

- Does **not** install ESO or the Sealed Secrets controller (operator action).
- Does **not** `kubectl apply` any of these manifests (ArgoCD / operator does).
- Does **not** contain any real ARN, account ID, region, or secret value — only
  placeholders.
