# Runbook: PostgresDown

**Alert:** `PostgresDown`
**Threshold:** `fp_db_up{service_name="<svc>"} == 0` for 1 minute
**Severity:** Critical
**SLO impact:** Yes — any service whose DB is down will burn its SLO error budget
at maximum rate (100% errors on writes; reads that hit Postgres also fail)
**Last reviewed:** 2026-06-18

---

## Symptom

A Forgepoint service's database health check has been failing for at least 1
minute. The service is reporting NOT ready (`/readyz` returns 503) and Kubernetes
is removing it from Service endpoints.

The `service_name` label in the alert identifies which service's DB is down.

Each service owns its own Postgres database on the shared cluster (homelab) or
dedicated RDS instance (AWS, Phase 12). The database names are:

| Service | Database |
|---------|----------|
| auth | fp_auth |
| registry | fp_registry |
| pipeline-orchestrator | fp_pipeline |
| feature-store | fp_features |
| experiment-tracker | fp_experiments |
| billing | fp_billing |
| notification | fp_notifications |
| model-monitor | fp_monitor |

---

## Impact

- All WRITES to the affected service fail (gRPC Internal or Unavailable errors).
- Reads that bypass the Redis cache (CQRS write path, consistent reads) also fail.
- The service's `/readyz` is failing, so Kubernetes has stopped routing traffic
  to it — the service effectively has 0 available endpoints.
- Downstream services that call it will see `Unavailable` gRPC errors.

---

## Diagnosis steps

### Step 1 — Is the Postgres pod/StatefulSet healthy?

```bash
# Homelab (single Postgres StatefulSet in fp-infra):
kubectl get pods -n fp-infra -l app.kubernetes.io/name=postgres
kubectl describe pod postgres-0 -n fp-infra

# Check Postgres pod logs for crash reason:
kubectl logs postgres-0 -n fp-infra --tail=100

# Check PVC for the Postgres data:
kubectl get pvc -n fp-infra
kubectl exec -n fp-infra postgres-0 -- df -h /var/lib/postgresql/data
```

### Step 2 — Is Postgres accepting connections?

```bash
# Port-forward Postgres from the cluster:
kubectl port-forward -n fp-infra svc/postgres 5432:5432 &

# Test connectivity with psql:
psql "postgresql://fp:<password>@localhost:5432/postgres" -c "SELECT version();"

# If psql hangs or connection refused: Postgres is not accepting connections.
# If psql connects but hangs on a query: check for lock contention (Step 4).
```

### Step 3 — Is the target database reachable?

```bash
# Connect to the specific service database:
psql "postgresql://fp:<password>@localhost:5432/fp_auth" -c "SELECT 1;"

# If the database does not exist, it may need to be created:
# (This shouldn't happen post-migration, but could occur after a PVC wipe)
psql "postgresql://fp:<password>@localhost:5432/postgres" -c "\l"
```

### Step 4 — Check for lock contention or long-running transactions

```bash
psql "postgresql://fp:<password>@localhost:5432/fp_<service>" << 'EOF'
-- Long-running queries (> 30s):
SELECT pid, now() - query_start AS duration, state, query
FROM pg_stat_activity
WHERE state != 'idle'
  AND (now() - query_start) > interval '30 seconds'
ORDER BY duration DESC;

-- Blocked queries (waiting on locks):
SELECT bl.pid AS blocked_pid, a.query AS blocked_query,
       kl.pid AS blocking_pid, ka.query AS blocking_query
FROM pg_catalog.pg_locks bl
JOIN pg_catalog.pg_stat_activity a ON a.pid = bl.pid
JOIN pg_catalog.pg_locks kl ON kl.transactionid = bl.transactionid
  AND kl.pid != bl.pid
JOIN pg_catalog.pg_stat_activity ka ON ka.pid = kl.pid
WHERE NOT bl.granted;
EOF
```

If there are blocking queries, terminate the blocker (carefully):
```bash
# Terminate a specific backend (replace PID):
psql "postgresql://fp:<password>@localhost:5432/fp_<service>" \
  -c "SELECT pg_terminate_backend(<blocking_pid>);"
```

### Step 5 — Check disk space (Postgres will refuse writes when disk is full)

```bash
# On the Postgres pod:
kubectl exec -n fp-infra postgres-0 -- df -h /var/lib/postgresql/data

# On the host node (homelab):
kubectl debug node/<node-name> -it --image=busybox -- df -h
```

If disk is > 90%, Postgres may be entering read-only mode. Delete old data or
expand the PVC.

### Step 6 — Check Postgres connection pool exhaustion

Each service uses pgx with a connection pool. If a service has a connection
leak (connections not returned to the pool), Postgres may reject new connections
from that service.

```bash
# Max connections and current usage:
psql "postgresql://fp:<password>@localhost:5432/postgres" << 'EOF'
SELECT max_conn, used, res_for_super, max_conn - used - res_for_super AS available
FROM (SELECT count(*) used FROM pg_stat_activity) t1,
     (SELECT setting::int res_for_super FROM pg_settings WHERE name='superuser_reserved_connections') t2,
     (SELECT setting::int max_conn FROM pg_settings WHERE name='max_connections') t3;
EOF

# Per-database connection count:
SELECT datname, count(*) as connections
FROM pg_stat_activity
GROUP BY datname
ORDER BY connections DESC;
```

If a single service is consuming all connections, restart that service pod to
close its connections and let the pool reset.

---

## Mitigation

### Postgres pod is down (crash or eviction)

```bash
# Restart the Postgres pod:
kubectl delete pod postgres-0 -n fp-infra
# The StatefulSet controller will restart it with the same PVC (data preserved).

# Monitor the restart:
kubectl rollout status statefulset/postgres -n fp-infra
```

### Postgres disk full

```bash
# Immediate relief: delete old WAL files (ONLY if you understand the risk):
kubectl exec -n fp-infra postgres-0 -- \
  pg_ctl -D /var/lib/postgresql/data stop
# Then expand the PVC and restart.

# Better: expand the PVC:
kubectl edit pvc postgres-data -n fp-infra
# Increase storage — requires a StorageClass with allowVolumeExpansion: true.
# On local-path (homelab k3s), this may not be supported; manual data cleanup needed.
```

### Postgres PVC was wiped (disaster scenario)

If the PVC has no data (e.g., node was replaced and local-path PV was lost):
Go directly to `docs/runbooks/dr.md` — this is a data loss event requiring the
full disaster recovery procedure.

### AWS environment (Phase 12) — RDS failover

In the AWS environment, each service uses RDS PostgreSQL with Multi-AZ:

```bash
# Trigger a failover from primary to standby (AWS CLI):
aws rds failover-db-instance --db-instance-identifier forgepoint-<service>-db

# Check RDS instance status:
aws rds describe-db-instances \
  --db-instance-identifier forgepoint-<service>-db \
  --query 'DBInstances[0].{Status:DBInstanceStatus,Endpoint:Endpoint.Address}'
```

RDS Multi-AZ failover typically completes in 60–120 seconds. Services will
reconnect automatically via the DNS endpoint (which updates after failover).

### Service cannot reconnect after Postgres recovery

If Postgres is healthy but the service's `fp_db_up` metric is still 0:

```bash
# The service may have a dead connection pool. Restart the service pod:
kubectl rollout restart deployment/<service-name> -n fp-system
```

The pgx pool reconnects on the next request (the pool is lazy by default), but
if pgx cached a failed state, a pod restart is the fastest fix.

---

## AWS RDS PITR (Point-in-Time Recovery) — Phase 12/24 context

For accidental data deletion or corruption on AWS:

```bash
# Restore to a point in time (creates a NEW RDS instance):
aws rds restore-db-instance-to-point-in-time \
  --source-db-instance-identifier forgepoint-<service>-db \
  --target-db-instance-identifier forgepoint-<service>-db-restored \
  --restore-time "2026-06-17T23:00:00Z"

# After restoration, update the service's DATABASE_URL env var to point
# to the new instance. See docs/runbooks/dr.md for the full procedure.
```

RDS automated backups retain 7 days by default (configurable in the Terraform
RDS module). RPO is therefore up to the last automated snapshot or up to 5
minutes before the incident for continuous PITR.

---

## Escalation

| Condition | Action |
|-----------|--------|
| Postgres PVC data missing | P0 — open dr.md runbook immediately |
| Auth DB down > 5 minutes | Platform-wide outage; escalate immediately |
| RDS failover fails | Page AWS infrastructure owner |
| Data corruption suspected | Stop writes immediately; preserve logs; open DR runbook |
