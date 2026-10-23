# Forgepoint — AWS Infrastructure as Code (Terraform)

This tree provisions the **cloud** home for Forgepoint (milestone **M5**). Locally the
platform runs on k3s with self-hosted infra (`deploy/k8s/infra/**`); in AWS each piece of
that infra is replaced by a managed service so the *application code path is identical*:

| Local (k3s, `deploy/k8s/infra`) | AWS (this Terraform)            | Why it's the same to the app                     |
| ------------------------------- | ------------------------------- | ------------------------------------------------ |
| Postgres StatefulSet            | **RDS for PostgreSQL** (`rds`)  | Same DSN `postgres://…:5432/<db>?sslmode=require`|
| Redis StatefulSet               | **ElastiCache for Redis** (`elasticache`) | Same `redis://host:6379` client path   |
| MinIO StatefulSet               | **S3 bucket** (`s3`)            | Same S3 API; only the endpoint changes           |
| Local container images          | **ECR** (`ecr`)                 | Same image refs, now `<acct>.dkr.ecr…`           |
| k3s nodes                       | **EKS** managed node group (`eks`) | Same Helm charts (`deploy/helm`) deploy onto it |
| In-cluster secrets (placeholder)| **Secrets Manager** (per `rds`/`elasticache`) | App reads a real secret store    |

## Layout

```
deploy/terraform/
├── README.md                 ← you are here
├── versions.tf               ← required_terraform + provider version pins (root)
├── backend.tf                ← remote-state backend PATTERN (commented; see note)
├── modules/                  ← reusable building blocks (no provider/backend config inside)
│   ├── network/              ← VPC, 3 AZs, public+private subnets, NAT, IGW, VPC endpoints, flow logs
│   ├── eks/                  ← EKS control plane + managed node group, IRSA/OIDC, KMS secret encryption
│   ├── rds/                  ← PostgreSQL: private, encrypted, SG-restricted, creds in Secrets Manager
│   ├── elasticache/          ← Redis: encryption in transit+rest, auth token in Secrets Manager
│   ├── s3/                   ← model-artifacts bucket: versioned, SSE-KMS, public-access blocked
│   ├── ecr/                  ← one repo per service, scan-on-push, immutable tags, lifecycle
│   ├── iam/                  ← IRSA roles per workload, least-privilege, bound to K8s SAs via OIDC
│   ├── github-oidc/          ← opt-in keyless GitHub Actions role that can push to ECR (main + v* tags only)
│   └── state-backend/        ← S3 bucket (versioned, SSE-KMS CMK, TLS-only, prevent_destroy) FOR remote state
├── bootstrap/                ← the one root with LOCAL state: applies state-backend once per account
└── environments/
    └── dev/                  ← composes the modules with small/cheap defaults
        ├── versions.tf       (symlink-style copy of root pins — kept per-env so each env is hermetic)
        ├── backend.tf        (remote-state config; -backend=false for offline validate)
        ├── providers.tf      ← aws + kubernetes provider config, default_tags
        ├── variables.tf      ← env-level knobs (region, sizes, CIDRs…)
        ├── main.tf           ← the wiring: module "network" → "eks" → "rds"/"elasticache"/"s3"/"ecr"/"iam"
        ├── outputs.tf        ← cluster name, endpoints, secret ARNs, ECR URLs…
        └── terraform.tfvars.example  ← copy to terraform.tfvars and fill in (gitignored)
```

`staging/` and `prod/` are produced by **copying `dev/` and tuning the variables** (bigger
instances, `multi_az = true`, restricted API CIDRs). Destructive settings (deletion
protection off, no final snapshot, ECR force-delete, immediate secret deletion) are
variables whose **defaults are the safe values**; only the dev `terraform.tfvars.example`
turns them on, so a copied directory starts safe. The modules themselves do not change
between environments — that's the whole point of the split.

## Bootstrapping order (chicken-and-egg with remote state)

The `state-backend` module creates the S3 bucket + KMS key that the *other* stacks store
their state in — but Terraform needs a backend before it can store state. Resolve it the
standard way:

1. **First run, local state:** `terraform -chdir=deploy/terraform/bootstrap init && … apply`.
   The bootstrap root calls `state-backend` with the default *local* backend and creates
   the bucket (versioned, SSE-KMS with its own CMK, TLS-only policy, public access blocked,
   `prevent_destroy`) and the key (rotated, `prevent_destroy`, 30-day deletion window).
   Set `state_access_principal_arns` to restrict state reads/writes to the roles that run
   Terraform.
2. **Migrate:** copy the bootstrap `backend_config` output into `environments/<env>/backend.tf`
   (or a gitignored `*.backend.hcl`) and run `terraform init -migrate-state`.
3. Everything afterwards (network/eks/rds/…) uses the S3 backend from the start.

Locking uses the S3-native lock file (`use_lockfile = true`, Terraform ≥ 1.11). There is no
DynamoDB lock table: DynamoDB locking is deprecated in the S3 backend.

`dev/backend.tf` ships **commented out** so `terraform init -backend=false` works offline
(no AWS creds, no real bucket). CI (`.github/workflows/terraform.yml`) runs `fmt -check`,
`init -backend=false` and `validate` on every root; it does **not** plan, because CI has
no AWS credentials and nothing has been applied.

## Conventions enforced everywhere

- **Tagging:** every taggable resource carries `Project=forgepoint`, `Environment=<env>`,
  `ManagedBy=terraform` (applied once via the provider `default_tags` — see `dev/providers.tf`).
- **No hardcoded secrets / account IDs / regions:** all of these are variables. No credential
  is ever passed in as `.tfvars`.
  - The **Postgres master password** is generated, stored and rotated by **RDS itself**
    (`manage_master_user_password`). It never passes through Terraform, so it is not in
    state.
  - The **Redis AUTH token** is generated by Terraform (`random_password`) and stored in
    Secrets Manager. ElastiCache has no managed-password option, and AWS provider 5.x has
    no write-only `auth_token`, so **this token is in state**. That is one reason the state
    bucket is encrypted with a customer-managed KMS key. It is readable only by the roles
    in `state_access_principal_arns` **once you set that variable**: it defaults to `[]`,
    and an empty list adds no deny statement, so until then any principal in the account
    with S3 and KMS read access can read state (see `modules/state-backend`).
  - **Outputs** expose ARNs and endpoints only. None is marked `sensitive`, because none is
    a secret, and `sensitive` only hides a value from CLI output: it does not keep it out
    of state. (An earlier version of this README said the outputs were marked sensitive;
    they never were.)
- **Encryption everywhere:** KMS for EKS secrets, RDS storage, S3 (SSE-KMS), ElastiCache
  at-rest + in-transit; S3 + RDS + ElastiCache are all private-subnet only.
- **Cost guardrail:** each environment has a monthly `aws_budgets_budget` (default $250) that
  emails `budget_alert_emails` at 50/80/100% of actual spend and 100% of forecast. The
  variable has no default, so an environment cannot be applied without a recipient.
- **Private-by-default networking:** workloads live in private subnets; only the NAT gateway
  and (optionally) the EKS public API endpoint touch the internet. S3/ECR reach via VPC
  endpoints to cut NAT data-processing cost.

## Validate (no AWS account needed)

```bash
terraform -chdir=deploy/terraform/environments/dev init -backend=false
terraform -chdir=deploy/terraform/environments/dev validate
terraform fmt -recursive deploy/terraform
```

`terraform apply` is intentionally **not** part of this deliverable — there is no live AWS
account wired up. The code is authored to pass `validate` and to be `apply`-ready once a
backend bucket and credentials exist.
