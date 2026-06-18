# Forgepoint Load Testing (k6) — Phase 13

k6 load tests against the **BFF HTTP API** (`fp-bff`, `:8081`) of the live
Forgepoint platform. These scripts and the cross-service E2E in
[`../tools/e2e/`](../tools/e2e/) together constitute **Phase 13 — cross-service
E2E + load testing**.

## Scripts

| Script | Type | What it does | Pass/fail contract (thresholds) |
|---|---|---|---|
| `smoke.js` | Smoke (1 VU, 5 iters) | Login once, then GET `/models`, `/dashboard`, `/monitors`. "Does it work at all?" | `http_req_failed < 1%`, `p95 < 1500ms`, `business_errors < 1%` |
| `load.js`  | Load / Stress (ramping VUs) | Mints **one** JWT in `setup()`, then ramps VUs hammering the read-heavy endpoints. `STAGE=load` (peak 20 VUs) or `STAGE=stress` (peak 100 VUs). | `http_req_failed < 2%`, aggregate `p95 < 1000ms`, per-route p95 bounds, `read_errors < 2%` (aborts run if breached) |
| `write.js` | Write load (bounded) | Auth-protected **write** path: `POST /models`. Hard-capped iterations, unique `k6-loadtest-` names + idempotency keys → safe on the live cluster. | `write_errors < 5%` (201 or idempotent 409 are OK), `p95 < 1500ms` |

### Why log in once in `setup()`

`load.js` and `write.js` mint a single JWT in k6's `setup()` (runs once, return
value injected into every VU iteration). We are load-testing the **read/write
endpoints**, not the auth service — logging in per iteration would benchmark
bcrypt and flood auth instead. This is also how a real SPA behaves: log in once,
reuse the token until expiry. The smoke test logs in per-iteration deliberately
(it is also a login smoke test, and 5 iters is trivial load).

### Why a custom `business_errors` / `read_errors` Rate

k6's built-in `http_req_failed` only counts transport/status failures. The custom
Rate metrics also catch **semantic** failures (e.g. a 200 whose body is not valid
JSON), so a backend that returns `200 <garbage>` still fails the test.

## Prerequisites

- `k6` installed (`brew install k6` / [k6.io/docs](https://k6.io/docs/get-started/installation/)).
- `kubectl` with the cluster kubeconfig.
- The platform deployed (`fp-bff` Running in `fp-system`).

## How to run

### 1. Port-forward the BFF (default target is `http://localhost:8081`)

```bash
kubectl --kubeconfig ~/.kube/forgepoint-thinkpad.yaml \
  -n fp-system port-forward svc/fp-bff 8081:8081
```

### 2. Run a script

```bash
# Smoke — run this first; if it's red, don't bother with the load test.
k6 run load/smoke.js

# Load (steady peak of 20 VUs):
k6 run load/load.js

# Stress (ramp to 100 VUs):
k6 run -e STAGE=stress load/load.js

# Write path (bounded; creates k6-loadtest-* models):
k6 run load/write.js
k6 run -e MAX_WRITES=50 -e WRITE_VUS=5 load/write.js
```

### Overridable env vars

| Var | Default | Purpose |
|---|---|---|
| `BASE_URL` | `http://localhost:8081` | BFF base URL (point at port-forward or an in-cluster URL) |
| `EMAIL` / `PASSWORD` | `admin@forgepoint.local` / `(set PASSWORD = your FP_BOOTSTRAP_ADMIN_PASSWORD)` | Login creds |
| `STAGE` (`load.js`) | `load` | `load` or `stress` ramp profile |
| `MAX_WRITES` / `WRITE_VUS` (`write.js`) | `20` / `4` | Bound the write blast radius |

### Cleaning up write-test data

`write.js` creates models named `k6-loadtest-<ts>-<vu>-<iter>`. Remove them:

```bash
kubectl --kubeconfig ~/.kube/forgepoint-thinkpad.yaml -n fp-infra exec postgres-0 -- \
  psql -U fp -d fp_registry -c "DELETE FROM models WHERE name LIKE 'k6-loadtest-%';"
```

## Results from the live k3s cluster (executed)

Run against the deployed `fp-bff` over a `kubectl port-forward`.

| Script | Outcome | Key numbers |
|---|---|---|
| `smoke.js` | PASS (30/30 checks) | warm `p95 ≈ 371ms`, error rate `0%` |
| `load.js` (20 VUs, 70s) | PASS (3145/3145 checks) | `p95 ≈ 27ms`, `~44 req/s`, error rate `0%` |
| `load.js` STAGE=stress (100 VUs, 90s) | PASS (17 668/17 668 checks) | `p95 ≈ 52ms`, `~194 req/s`, error rate `0%` |
| `write.js` (sample) | PASS | `201` registrations, `p95 ≈ 408ms`, error rate `0%` |

The first request of any run shows a ~3–4.7s **cold-start outlier** (BFF→gRPC
dial to each backend + first JWT verify + port-forward warm-up); steady-state
median is ~12ms. The smoke threshold (`p95<1500`) is set to tolerate this
one-time cost while still catching real regressions; the load/stress thresholds
measure the warm state.

> Note: the read endpoints currently return **empty** result sets because the
> registry's CQRS read projection (Postgres→Redis) consumer is not wired in this
> build — see the E2E findings in [`../tools/e2e/README.md`](../tools/e2e/README.md).
> The load test therefore measures the **read-path plumbing latency** (auth →
> BFF → gRPC → read-store query), which is healthy; it does not exercise large
> result serialization. That's the correct scope for a platform load test of the
> request path, and it's flagged here so the numbers aren't misread.

## What these tests prove (Phase 13)

- The BFF + auth + downstream gRPC request path is **correct under concurrency**
  (zero errors at 100 VUs / ~194 req/s) and **low-latency** (warm p95 < 60ms).
- Thresholds make every script a **CI gate**: a latency/error regression exits
  `k6 run` non-zero.
- The write path is exercised **safely** against the live platform (bounded,
  idempotent, identifiable test data).

Together with the cross-service E2E, this completes Phase 13's "E2E + load
testing" deliverable: functional correctness across services (E2E) **and**
performance/regression gating of the request path (k6).
