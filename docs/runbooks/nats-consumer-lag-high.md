# Runbook: NATSConsumerLagHigh

**Alert:** `NATSConsumerLagHigh`
**Threshold:** `fp:nats:max_consumer_lag:5m > 10000` messages for 5 minutes
**Severity:** Warning
**SLO impact:** Indirect — no SLO on async consumers, but billing under-metering
and delayed notifications are operational risks
**Last reviewed:** 2026-06-18

---

## Symptom

A NATS JetStream consumer has over 10,000 messages pending (unconsumed) and has
sustained that lag for 5+ minutes. The consumer is not keeping up with producers.

The alert label `consumer_name` and `stream_name` identify the specific consumer.

---

## NATS JetStream architecture in Forgepoint

```
Producers (fp.inference.completed, fp.models.registered, etc.)
    │  publish to JetStream streams
    ▼
NATS JetStream (persists messages on PVC in fp-infra)
    │  consumers pull / push messages
    ├─► Experiment Tracker (subscribes fp.inference.completed — batch ingest)
    ├─► Billing (subscribes fp.inference.completed — usage metering)
    ├─► Notification (subscribes fp.> — rule matching)
    ├─► Registry Projection (subscribes fp.models.> — Redis CQRS read)
    └─► Model Monitor (subscribes fp.inference.completed — drift windows)
```

JetStream PERSISTS messages until they are acknowledged OR the retention limit
expires. HIGH LAG = slow consumer, NOT message loss (unless retention is hit).

---

## Impact by consumer

| Consumer | Stream | Impact at high lag |
|----------|--------|--------------------|
| billing | fp.inference.completed | Usage under-metered; quota enforcement delayed; invoices inaccurate |
| experiment-tracker | fp.inference.completed | Experiment metrics delayed; dashboards stale |
| notification | fp.> | Notifications delayed (model drift alerts, quota exceeded alerts) |
| registry projection | fp.models.> | Redis read path stale; GetModel returns old data |
| model-monitor | fp.inference.completed | Drift detection windows delayed; auto-retrain may trigger late |

---

## Diagnosis steps

### Step 1 — Identify which consumer is lagging

```bash
# Port-forward NATS monitoring:
kubectl port-forward -n fp-infra svc/nats 8222:8222

# List all JetStream consumers and their pending counts:
curl -s http://localhost:8222/jsz?consumers=true | \
  python3 -m json.tool | \
  grep -A5 '"num_pending"'

# Or use the nats CLI (install from: https://github.com/nats-io/natscli):
nats consumer ls --server nats://localhost:4222

# Show pending count for a specific consumer:
nats consumer info <stream-name> <consumer-name> --server nats://localhost:4222
```

### Step 2 — Is the consumer service running?

```bash
# Check pods for the relevant consumer service:
kubectl get pods -n fp-system -l app.kubernetes.io/name=<service-name>
# Expected: 1/1 Running

# If crash-looping, go to service-down.md runbook first.
# The lag will persist until the consumer is healthy.
```

### Step 3 — Check consumer processing throughput

```bash
# Look at the consumer service's logs for processing rate or errors:
kubectl logs -n fp-system -l app.kubernetes.io/name=<service-name> --tail=100 | \
  grep -E "processed|consumed|error|nak|timeout"

# The batch consumer (experiment-tracker) logs a flush every N seconds.
# If you see no flush logs, the buffer may be full (back-pressure applied).
```

### Step 4 — Check for slow DB writes behind the consumer

A consumer can be running but slow if its DB writes are slow (e.g., Postgres
disk I/O saturated from other services writing simultaneously).

```bash
# Check Postgres from within the consumer pod:
kubectl exec -n fp-system <service-pod> -- \
  sh -c 'time psql "$DATABASE_URL" -c "SELECT 1"'

# Postgres slow query log (if enabled):
kubectl exec -n fp-infra <postgres-pod> -- \
  psql -U fp -c "SELECT pid, now() - pg_stat_activity.query_start AS duration, query
                 FROM pg_stat_activity
                 WHERE state = 'active' AND (now() - pg_stat_activity.query_start) > interval '5 seconds';"
```

### Step 5 — Check NATS stream retention limits

If the lag is not dropping even with the consumer running, check if the stream's
retention limit is being hit (oldest messages being dropped):

```bash
nats stream info <stream-name> --server nats://localhost:4222
# Look for: "Lost:" in the output — messages evicted before consumption
# If max_bytes or max_msgs limit is being hit, increase the limit or
# add more disk space to the NATS PVC.
```

### Step 6 — Check NATS PVC usage

```bash
kubectl get pvc -n fp-infra
kubectl exec -n fp-infra <nats-pod> -- df -h /data
```

If the PVC is > 80% full, NATS will reject new publications once it hits the limit.

---

## Mitigation

### Consumer is down or crash-looping

Fix the service health first (service-down.md), then the lag will drain as the
consumer catches up. JetStream retains all pending messages.

### Consumer running but slow — scaling up

```bash
# Increase the number of consumer replicas (if the consumer supports queue groups):
kubectl scale deployment/<service-name> -n fp-system --replicas=3

# The pkg/natsutil subscriber uses queue groups (WithQueueGroup) so multiple
# replicas WILL share the work (each message goes to only one consumer).
# This is the primary mitigation for sustained lag without a service outage.
```

### Experiment-tracker buffer back-pressure

The experiment tracker batches events (flushes every N seconds or M events).
If it's applying back-pressure (NAKing messages to slow NATS), reduce the flush
interval to drain the buffer faster:

```bash
# The FP_EXPERIMENT_BATCH_FLUSH_INTERVAL env var controls this:
kubectl set env deployment/experiment-tracker -n fp-system \
  FP_EXPERIMENT_BATCH_FLUSH_INTERVAL=2s
# Default is likely 10s — 2s will increase DB write frequency but drain the queue.
# Revert to default after the incident.
```

### NATS stream retention limit reached (messages being dropped)

```bash
# Increase stream max_bytes (requires nats CLI admin permissions):
nats stream edit <stream-name> --server nats://localhost:4222 \
  --max-bytes=10GB

# Or expand the NATS PVC (requires a storage class that supports expansion):
kubectl edit pvc nats-data -n fp-infra
# Change spec.resources.requests.storage from 5Gi to 20Gi
# (only works if storageClassName supports volume expansion)
```

---

## Escalation

| Condition | Action |
|-----------|--------|
| Lag exceeding 100k messages | Consider temporary producer throttling; escalate to senior engineer |
| NATS disk full | P1 — new events being rejected; all async consumers blocked |
| Billing consumer lagged > 1 hour | Alert finance/product team about metering gap |
| Messages being dropped (retention exceeded) | Escalate immediately — data loss occurred |

---

## Prevention

- Enable KEDA (Phase 22) for event-consumer autoscaling on NATS consumer lag.
  With KEDA, consumers auto-scale when lag exceeds threshold — this alert would
  fire only if KEDA itself is misconfigured or the node has no capacity.
- Review NATS stream retention limits after each load test (Phase 13) — ensure
  the limits account for the worst-case lag recovery scenario.
