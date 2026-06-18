# Runbook: ModelDriftStorm

**Alert:** `ModelDriftStorm`
**Threshold:** `sum(increase(fp_drift_detections_total[15m])) > 5`
**Severity:** Warning
**SLO impact:** Indirect — a drift storm triggers multiple simultaneous pipeline
executions, which can overload the pipeline-orchestrator and training infra
**Last reviewed:** 2026-06-18

---

## Symptom

The model-monitor service has published more than 5 `ModelDriftDetected` events
in a 15-minute window across all monitored models. This is abnormal — typical
steady-state is 0–2 drift detections per hour.

---

## Background: how drift detection works

```
Inference Gateway
  │  publishes fp.inference.completed events
  ▼
Model Monitor (services/model-monitor)
  │  maintains sliding windows per model (PSI, KL-divergence, KS-test)
  │  compares current distribution to training-time baseline
  ▼
  ├── Drift detected (threshold breached)
  │     └── publishes fp.models.drift.detected (NATS)
  │           ├── Notification service → alerts
  │           ├── Experiment Tracker → records drift report
  │           └── If auto-retrain enabled:
  │                 └── Pipeline Orchestrator → TriggerExecution (training pipeline)
  └── No drift → window slides forward
```

A **drift storm** = multiple models crossing their drift thresholds simultaneously.
This can be:
1. **Real:** a genuine input distribution shift (new data source, upstream data
   pipeline change, seasonal effect) affecting many models at once.
2. **False:** misconfigured thresholds (too sensitive), a bug in the drift math,
   or a sudden burst of anomalous inference requests (load test, data poisoning
   attempt).

---

## Impact

- If auto-retrain is enabled for the drifting models, the pipeline-orchestrator
  will receive N simultaneous `TriggerExecution` calls. At 5+ concurrent sagas,
  the training infra (K8s Jobs in fp-jobs) may be resource-saturated.
- The notification service will fire N alerts (email/Slack/webhook) — if the
  volume is high enough, this can overwhelm a webhook endpoint or hit rate limits.
- Experiment tracker will receive N drift report events simultaneously.

---

## Diagnosis steps

### Step 1 — Identify which models drifted and the drift type

```bash
# Connect to the model-monitor's Postgres DB:
kubectl port-forward -n fp-infra svc/postgres 5432:5432 &
psql "postgresql://fp:<password>@localhost:5432/fp_monitor"

-- Which models have recent drift reports?
SELECT model_id, model_version, drift_type, drift_score, threshold,
       detected_at
FROM drift_reports
WHERE detected_at > NOW() - INTERVAL '30 minutes'
ORDER BY detected_at DESC;

-- Are the drift scores just barely over threshold (noisy) or far over?
SELECT model_id,
       drift_score,
       threshold,
       drift_score / threshold AS severity_ratio
FROM drift_reports
WHERE detected_at > NOW() - INTERVAL '30 minutes'
ORDER BY severity_ratio DESC;
```

A `severity_ratio` close to 1.0 (barely over threshold) suggests the thresholds
are too tight. A ratio > 2.0 suggests real drift.

### Step 2 — Compare the inference distribution to the baseline

```bash
# Look at the model-monitor logs for the distribution stats:
kubectl logs -n fp-system -l app.kubernetes.io/name=model-monitor --tail=200 | \
  grep -E "drift|window|psi|ks_test|model_id"

# Check the feature store for recent data ingestion patterns:
kubectl port-forward -n fp-infra svc/postgres 5432:5432 &
psql "postgresql://fp:<password>@localhost:5432/fp_features"

-- Was there a large feature ingestion event recently?
SELECT feature_set_id, count(*), min(created_at), max(created_at)
FROM feature_events
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY feature_set_id
ORDER BY count DESC;
```

### Step 3 — Check if auto-retrain triggered pipeline executions

```bash
psql "postgresql://fp:<password>@localhost:5432/fp_pipeline"

-- Active and recent executions triggered in the last 30 minutes:
SELECT id, pipeline_id, status, started_at,
       EXTRACT(EPOCH FROM (NOW() - started_at)) / 60 AS age_minutes
FROM executions
WHERE started_at > NOW() - INTERVAL '30 minutes'
ORDER BY started_at DESC;
```

If 5+ pipeline executions are RUNNING simultaneously, check the training job
resource usage:

```bash
kubectl get jobs -n fp-jobs
kubectl top pods -n fp-jobs
```

### Step 4 — Check if this is a load test or abnormal traffic pattern

```bash
# Request rate to the inference gateway over the last hour:
kubectl port-forward -n fp-infra svc/prometheus 9090:9090

# PromQL:
sum(rate(http_server_duration_milliseconds_count{
  service_name="inference-gateway"
}[5m]))
```

If the request rate is 5–10× normal, a load test or traffic spike may be
producing artificial inference patterns that look like drift.

---

## Mitigation

### False drift storm — thresholds too sensitive

```bash
# Update the monitor configuration for the affected models.
# The ConfigureMonitor RPC adjusts per-model thresholds:
kubectl port-forward -n fp-system svc/model-monitor 8080:8080 &
grpcurl -plaintext \
  -H "authorization: Bearer <admin-token>" \
  -d '{
    "model_id": "<model-id>",
    "drift_threshold": 0.2,
    "auto_retrain": false
  }' \
  localhost:8080 forgepoint.monitor.v1.MonitorService/ConfigureMonitor
```

Temporarily disable auto-retrain while investigating to prevent the pipeline
orchestrator from being flooded:

```bash
# Disable auto-retrain for all affected models (psql):
psql "postgresql://fp:<password>@localhost:5432/fp_monitor" \
  -c "UPDATE monitors SET auto_retrain = false WHERE model_id IN ('<id1>', '<id2>');"
```

### Real drift — throttle parallel retraining

If drift is real and auto-retrain is desired, but the training infra cannot
handle N simultaneous jobs:

```bash
# Cancel all but 1 active training execution via the pipeline-orchestrator:
grpcurl -plaintext \
  -H "authorization: Bearer <admin-token>" \
  -d '{"execution_id": "<id>"}' \
  localhost:8080 forgepoint.pipeline.v1.PipelineService/CancelExecution

# Let one retrain complete first; re-enable for others after it succeeds.
```

### Notification flood — webhook rate limiting

If downstream webhooks are being flooded, pause the notification service
temporarily while investigating:

```bash
kubectl scale deployment/notification -n fp-system --replicas=0
# Investigate and resolve the drift storm.
# NATS will retain the notification events — they will be processed when
# the notification service is scaled back up.
kubectl scale deployment/notification -n fp-system --replicas=1
```

---

## Post-incident review questions

1. Was the drift real or a false positive? (severity_ratio > 2.0 = real)
2. Were the thresholds appropriate for the model's expected production variance?
3. Did auto-retrain help or cause additional load at a bad time?
4. Should there be a per-model rate limit on drift detections (e.g., max 1 per hour per model)?
5. Was the data change upstream communicated to the platform team? (Process gap)

---

## Escalation

| Condition | Action |
|-----------|--------|
| 10+ simultaneous retrain jobs saturating fp-jobs namespace | Disable auto-retrain; alert ML team |
| Drift detected on the auth or billing models (if any) | Escalate to security — may indicate adversarial input |
| No root cause found for widespread drift | Escalate to ML engineering for data pipeline investigation |
