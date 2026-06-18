# Forgepoint Disaster Recovery Runbook

**Phase:** 24 — Backup & Disaster Recovery
**Owner:** Platform Engineering
**Last reviewed:** 2026-06-18
**Backup manifests:** `deploy/backup/`

---

## RPO and RTO targets

| Environment | Mechanism | RPO | RTO |
|-------------|-----------|-----|-----|
| Homelab (k3s) | pg_dump daily + Velero daily | 24 hours | 2–4 hours |
| Production AWS (Phase 12) | CNPG WAL archiving + Velero daily + RDS PITR | 5 minutes | 30–60 minutes |

**RPO (Recovery Point Objective):** How much data can be lost. "24h RPO" means
you accept losing up to 24 hours of data in a homelab disaster. In production
the CNPG WAL archiving drops this to ~5 minutes (the last WAL segment that was
shipped to S3 before the failure).

**RTO (Recovery Time Objective):** How long it takes to restore service.
"2–4 hours RTO" for homelab means the platform is back online within 4 hours of
starting the recovery procedure. In production the target is 30–60 minutes
because Kubernetes comes up faster on EKS (pre-provisioned node groups) and RDS
PITR is faster than a pg_dump restore.

---

## Disaster scenarios and response matrix

| Scenario | Impact | Primary recovery mechanism | Doc section |
|----------|--------|-----------------------------|-------------|
| Single pod crash / crash-loop | Service degraded | Kubernetes self-heals (restart) | service-down.md |
| Postgres pod lost (data on PVC intact) | Service down | Restart pod; PVC intact | postgres-down.md |
| Postgres PVC wiped / node lost | Data loss for affected service(s) | pg_dump restore from MinIO | Section 3 |
| All cluster state lost (control plane gone) | Full platform down | Velero restore + pg_dump restore | Section 4 |
| MinIO PVC lost (model artifacts) | Model serving broken | Velero restic restore | Section 5 |
| NATS JetStream state lost | Event consumers reset to beginning | NATS JetStream persistence note | Section 6 |
| AWS: RDS data corruption | Service data corrupted | RDS PITR | Section 7 |
| AWS: Full EKS cluster loss | Full platform down | Terraform + ArgoCD + RDS PITR | Section 8 |

---

## Section 1 — Pre-restore checklist

Before starting any restore:

```bash
# 1. Confirm the backup exists in MinIO and is recent:
kubectl port-forward -n fp-infra svc/minio 9000:9000 &
mc alias set minio http://localhost:9000 minioadmin minioadmin --quiet
mc ls minio/fp-backups/postgres/ | sort | tail -10

# 2. Note the backup timestamp you want to restore FROM (most recent, or
#    a specific point in time if this is a partial data-corruption recovery).

# 3. Confirm no services are writing to the affected database during restore.
#    Scale down the affected service to 0 replicas:
kubectl scale deployment/<service-name> -n fp-system --replicas=0

# 4. Notify the team that DR is in progress.
```

---

## Section 2 — Restore order (critical — must follow this sequence)

The platform has dependency ordering. Restoring services in the wrong order
causes cascade failures.

```
1. Infrastructure (NATS, Postgres, Redis, MinIO) — in fp-infra
       ↓
2. Auth service — everything depends on it for token validation
       ↓
3. Registry — gateway and pipeline-orchestrator depend on it
       ↓
4. Model Serving — must exist before gateway can route
       ↓
5. Inference Gateway — the external API
       ↓
6. Pipeline Orchestrator — depends on registry + serving
       ↓
7. Remaining services (feature-store, experiment-tracker, billing,
   notification, model-monitor) — only depend on Auth + NATS,
   can be restored in any order
```

**For a full cluster restore:** use ArgoCD (Phase 20) to sync all applications
from Git after infrastructure is healthy. ArgoCD knows the correct dependency
order via sync waves. Without ArgoCD (Phases 0–19), apply Helm charts manually
in the order above.

---

## Section 3 — Postgres restore from pg_dump (homelab)

Use this when a service's Postgres PVC has been lost or corrupted.

### Step 1 — Start a fresh Postgres pod (if the StatefulSet is gone)

```bash
# Apply the Postgres StatefulSet manifest:
kubectl apply -f deploy/k8s/infra/postgres.yaml

# Wait for Postgres to be ready:
kubectl rollout status statefulset/postgres -n fp-infra
```

### Step 2 — Create the target database

```bash
kubectl exec -it -n fp-infra postgres-0 -- \
  psql -U fp postgres << 'EOF'
-- Recreate the database (replace fp_auth with the target DB):
DROP DATABASE IF EXISTS fp_auth;
CREATE DATABASE fp_auth OWNER fp;
EOF
```

### Step 3 — Download and decrypt the backup

```bash
# Port-forward MinIO:
kubectl port-forward -n fp-infra svc/minio 9000:9000 &

# Configure mc:
mc alias set minio http://localhost:9000 minioadmin minioadmin --quiet

# List available backups (newest last):
mc ls minio/fp-backups/postgres/ | sort | tail -5

# Set the backup timestamp to restore from:
BACKUP_DATE="2026-06-17T020000Z"   # Replace with actual timestamp

# Download and decrypt the per-database dump:
mc cat "minio/fp-backups/postgres/${BACKUP_DATE}/fp_auth.pgdump.gpg" | \
  gpg --batch --passphrase "<BACKUP_ENCRYPTION_KEY>" --decrypt \
  > /tmp/fp_auth.pgdump
```

Retrieve the encryption key from the Kubernetes Secret:
```bash
kubectl get secret backup-credentials -n fp-infra \
  -o jsonpath='{.data.BACKUP_ENCRYPTION_KEY}' | base64 -d
```

### Step 4 — Restore the dump

```bash
# Port-forward Postgres:
kubectl port-forward -n fp-infra svc/postgres 5432:5432 &

# Restore using pg_restore (custom format):
pg_restore \
  --host=localhost \
  --port=5432 \
  --username=fp \
  --password \
  --dbname=fp_auth \
  --no-owner \
  --no-acl \
  --jobs=4 \
  /tmp/fp_auth.pgdump

# Verify: connect and check row counts:
psql "postgresql://fp:<password>@localhost:5432/fp_auth" << 'EOF'
SELECT schemaname, tablename, n_live_tup AS row_count
FROM pg_stat_user_tables
ORDER BY n_live_tup DESC;
EOF
```

### Step 5 — Restore globals (roles and tablespaces)

If the Postgres instance is fresh (not just a single DB restore):

```bash
mc cat "minio/fp-backups/postgres/${BACKUP_DATE}/globals.sql.gz.gpg" | \
  gpg --batch --passphrase "<key>" --decrypt | \
  gunzip | \
  psql "postgresql://fp:<password>@localhost:5432/postgres"
```

### Step 6 — Restart the service and verify

```bash
kubectl scale deployment/<service-name> -n fp-system --replicas=1
kubectl rollout status deployment/<service-name> -n fp-system

# Test the readiness endpoint:
kubectl port-forward -n fp-system svc/<service-name> 8081:8081 &
curl http://localhost:8081/readyz
# Expected: {"status":"ok","checks":{"postgres":"ok","nats":"ok"}}
```

---

## Section 4 — Full cluster restore (Velero + pg_dump)

Use this when the entire Kubernetes cluster has been lost (node failure, accidental
`kubectl delete namespace`, etc.).

### Step 1 — Recreate the cluster

```bash
# Homelab:
k3s server --cluster-init   # or reinstall k3s from scratch

# Apply cluster foundation:
kubectl apply -f deploy/k8s/namespaces.yaml
```

### Step 2 — Install Velero

```bash
velero install \
  --provider aws \
  --plugins velero/velero-plugin-for-aws:v1.10.0 \
  --bucket fp-velero \
  --secret-file ./velero-credentials \
  --use-volume-snapshots=false \
  --backup-location-config region=us-east-1,s3ForcePathStyle=true,s3Url=http://<minio-nodeport>:9000 \
  --namespace velero
```

Note: MinIO itself is not restored from Velero (chicken-and-egg). The MinIO
StatefulSet must be applied manually first:

```bash
kubectl apply -f deploy/k8s/infra/minio.yaml
# Wait for MinIO to start:
kubectl rollout status statefulset/minio -n fp-infra
# MinIO will start with an empty PVC — Velero will restore the data in Step 4.
```

### Step 3 — Restore cluster state from Velero

```bash
# List available Velero backups:
velero backup get

# Restore the most recent daily cluster-state backup:
velero restore create \
  --from-schedule forgepoint-cluster-state-daily \
  --wait

# Monitor restore progress:
velero restore get
velero restore describe <restore-name>
```

### Step 4 — Restore MinIO PV data from Velero

```bash
velero restore create \
  --from-schedule forgepoint-minio-pv-daily \
  --include-namespaces fp-infra \
  --selector app.kubernetes.io/name=minio \
  --wait
```

### Step 5 — Restore all Postgres databases

Follow Section 3 for each of the 8 service databases, in this priority order:
1. fp_auth (blocking — all services need auth)
2. fp_registry
3. fp_pipeline
4. fp_features, fp_experiments, fp_billing, fp_notifications, fp_monitor (any order)

### Step 6 — Restart services in dependency order

Without ArgoCD (Phases 0–19):
```bash
for SVC in auth registry pipeline-orchestrator inference-gateway \
           model-serving feature-store experiment-tracker billing \
           notification model-monitor; do
  kubectl rollout restart deployment/${SVC} -n fp-system
  kubectl rollout status deployment/${SVC} -n fp-system
done
```

With ArgoCD (Phase 20+):
```bash
# ArgoCD will detect the restored cluster state and sync all applications.
# Force a full sync if ArgoCD doesn't auto-detect:
argocd app sync forgepoint-app-of-apps --prune
```

The GitOps repo is the cluster's source of truth — after the cluster and
infrastructure are up, ArgoCD recreates the service state from Git automatically
without manual Helm installs. This is the core DR benefit of Phase 20.

---

## Section 5 — MinIO (model artifacts) restore

MinIO stores ONNX model files uploaded via the Registry service. These are
restored by Velero's restic volume backup (forgepoint-minio-pv-daily schedule).

The Velero restore in Section 4 Step 4 covers this. To restore only MinIO
(without a full cluster restore):

```bash
# List MinIO-specific Velero backups:
velero backup get --selector tier=pv-minio

# Restore:
velero restore create \
  --from-backup <backup-name> \
  --include-namespaces fp-infra \
  --include-resources persistentvolumeclaims,persistentvolumes \
  --wait
```

After MinIO restoration, verify model artifacts are present:
```bash
kubectl port-forward -n fp-infra svc/minio 9000:9000 &
mc alias set minio http://localhost:9000 minioadmin minioadmin --quiet
mc ls minio/fp-models/
# Should list model artifacts that were present before the disaster
```

---

## Section 6 — NATS JetStream durability note

NATS JetStream stores streams and messages on a PVC in fp-infra. This PVC is
backed up by Velero (included in the cluster-state daily schedule as a PVC
definition, but NOT the data — we do not restic-backup NATS data).

**Why we do not back up NATS stream data:**

NATS JetStream is a message bus, not a system of record. The DATA lives in
Postgres; NATS is the transport. If NATS stream data is lost:

- Inflight messages (not yet ACKed by consumers) are lost. The consumer
  services (billing, experiment-tracker, notification) will process the next
  batch of events from the point they reconnect — no historical replay needed.
- Consumers that were mid-delivery will restart their subscription from the
  last acknowledged sequence, which NATS tracks per-consumer. If the sequence
  data is lost too, consumers restart from the stream head (latest), which is
  safe because all consumers are idempotent.

**What to do after NATS PVC loss:**

```bash
# Apply the NATS StatefulSet (creates a fresh JetStream store):
kubectl apply -f deploy/k8s/infra/nats.yaml

# Recreate the JetStream streams (these are defined in each service's startup
# code via natsutil — they are idempotent and re-created on service start).
# Just restart the services and they will recreate their streams:
kubectl rollout restart deployment -n fp-system
```

NATS durability during NORMAL OPERATION (not DR): JetStream persists messages
to the PVC until they are ACKed. If a consumer is temporarily down, messages
accumulate (the NATSConsumerLagHigh alert fires) but are NOT lost. Restarting
the consumer drains the backlog. Only a PVC WIPE causes NATS data loss.

---

## Section 7 — AWS RDS PITR restore (production, Phase 12)

For the AWS environment, each service uses an RDS PostgreSQL instance. RDS
Automated Backups enable PITR to any second within the retention window (7 days
default, configurable up to 35 days in the Terraform RDS module).

```bash
# Identify the restore point (e.g., 5 minutes before data was corrupted):
RESTORE_TIME="2026-06-17T23:55:00Z"

# Restore to a new RDS instance (cannot restore in-place):
aws rds restore-db-instance-to-point-in-time \
  --source-db-instance-identifier forgepoint-auth-db \
  --target-db-instance-identifier forgepoint-auth-db-restored \
  --restore-time "${RESTORE_TIME}" \
  --db-instance-class db.t3.micro \
  --no-multi-az \
  --region us-east-1

# Wait for the restore to complete (10–30 minutes):
aws rds wait db-instance-available \
  --db-instance-identifier forgepoint-auth-db-restored

# Get the new instance endpoint:
aws rds describe-db-instances \
  --db-instance-identifier forgepoint-auth-db-restored \
  --query 'DBInstances[0].Endpoint.Address' \
  --output text

# Update the service to use the restored instance:
# (In Phase 17 with External Secrets Operator, update the secret in
#  AWS Secrets Manager — the service picks up the new DB URL on restart.)
kubectl set env deployment/auth -n fp-system \
  DATABASE_URL="postgresql://fp:<password>@<new-endpoint>:5432/fp_auth"
kubectl rollout restart deployment/auth -n fp-system
```

After verifying the restored instance is correct, rename it to replace the
original (or update Terraform state):
```bash
# In Terraform (Phase 12 — the authoritative approach):
# 1. Update the RDS module to point to the restored instance identifier.
# 2. terraform apply — Terraform reconciles the DNS alias.
# This avoids a manual AWS console operation and keeps Terraform as the
# source of truth (consistent with the GitOps principle from Phase 20).
```

---

## Section 8 — AWS full EKS cluster restore (production, Phase 12)

If the entire EKS cluster is lost:

```bash
# Step 1: Recreate infrastructure with Terraform (Phase 12):
cd deploy/terraform/environments/dev
terraform init
terraform apply   # recreates VPC, EKS, RDS, ElastiCache, S3

# Step 2: Install ArgoCD (Phase 20):
kubectl apply -n argocd -f \
  https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml

# Step 3: Point ArgoCD at the Git repo:
argocd app create forgepoint-app-of-apps \
  --repo https://github.com/abd-ulbasit/forgepoint \
  --path deploy/argocd \
  --dest-server https://kubernetes.default.svc \
  --dest-namespace argocd \
  --sync-policy automated

# ArgoCD will sync ALL services from Git.
# RDS data is already restored via PITR (Section 7) or Automated Backups.
# The cluster is the application of Git state to infrastructure — this is
# the full value of GitOps: the cluster is RECREATABLE FROM GIT.
```

---

## Tested-restore checklist

Run this checklist after every DR drill (quarterly minimum):

### Postgres restore drill

- [ ] Identified the most recent pg_dump backup in MinIO (`mc ls`)
- [ ] Decrypted a single backup file successfully
- [ ] Restored fp_auth to a fresh test namespace
- [ ] Verified row counts match pre-restore snapshot
- [ ] Auth service `/readyz` returns OK against the restored DB
- [ ] Login and ValidateToken RPCs work with the restored data
- [ ] Noted actual restore duration: ________ minutes
- [ ] Updated RTO estimate if actual duration differs

### Velero cluster-state drill

- [ ] `velero backup get` shows recent daily backup
- [ ] Restored a single namespace (`fp-system`) to a test cluster
- [ ] All deployments came up healthy after restore
- [ ] Noted Velero restore duration: ________ minutes

### Full DR drill (annual)

- [ ] Wiped test cluster entirely (or used a separate cluster)
- [ ] Followed Section 4 (full cluster restore) from scratch
- [ ] All 10 services healthy and responding to grpcurl probes
- [ ] Inference gateway returned a correct prediction
- [ ] Noted total restore duration: ________ hours
- [ ] Filed post-drill issues in GitHub (backup gaps, documentation errors)

---

## On-call quick reference

```bash
# Is there a recent backup?
kubectl port-forward -n fp-infra svc/minio 9000:9000 &
mc alias set minio http://localhost:9000 minioadmin minioadmin --quiet
mc ls minio/fp-backups/postgres/ | sort | tail -3

# Are Velero backups completing?
velero backup get | head -5

# Check Velero backup job status:
kubectl get jobs -n velero

# Emergency: take an on-demand backup right now (before a risky operation):
velero backup create pre-operation-$(date +%Y%m%d%H%M) \
  --include-namespaces fp-system,fp-infra,fp-models \
  --wait
```
