// load/load.js — k6 LOAD + STRESS test for the Forgepoint BFF read API.
//
// ============================================================================
// LOAD vs STRESS vs SMOKE (the three points on the curve)
// ============================================================================
//   smoke  (smoke.js)  : 1 VU, "does it work at all?"            -> correctness
//   load   (this file) : ramp to an EXPECTED peak, hold, ramp down -> SLO check
//   stress (this file, STAGE=stress) : ramp PAST expected peak    -> find the knee
//
// This script does both load and stress via the STAGE env var so there is one
// canonical scenario definition. It targets the READ-HEAVY endpoints an SPA
// dashboard polls — GET /models, /dashboard, /monitors — because those are the
// hot path: a browser refreshes them far more often than it writes. Each is a
// CQRS/aggregation read, so this is really a load test of the registry read
// projection, the model-monitor read path, and the BFF's 4-way dashboard fan-out.
//
// ============================================================================
// KEY DESIGN: log in ONCE in setup(), share the token to all VUs
// ============================================================================
// setup() runs a SINGLE time before the load and its return value is passed to
// every VU iteration. We mint ONE JWT there instead of logging in per-iteration.
// WHY: (1) we are load-testing the READ endpoints, not the auth service — if we
// logged in every iteration we'd be benchmarking bcrypt, not the read path, and
// hammering auth with thousands of logins; (2) a real SPA logs in once and
// reuses the token until expiry. This is the canonical k6 pattern for testing
// authenticated APIs. The token must outlive the test (default JWT TTL >> test
// duration) — if it expired mid-run we'd see 401s, which the threshold catches.
//
// RUN:
//   port-forward first:
//     kubectl --kubeconfig ~/.kube/forgepoint-thinkpad.yaml -n fp-system \
//       port-forward svc/fp-bff 8081:8081
//   load (default):   k6 run load/load.js
//   stress:           k6 run -e STAGE=stress load/load.js
//   custom target:    k6 run -e BASE_URL=http://localhost:8081 load/load.js
// ============================================================================

import http from 'k6/http';
import { check, sleep, group } from 'k6';
import { Rate, Trend } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8081';
const EMAIL = __ENV.EMAIL || 'admin@forgepoint.local';
const PASSWORD = __ENV.PASSWORD || '';
const STAGE = __ENV.STAGE || 'load'; // 'load' | 'stress'

// Per-endpoint latency Trends so the summary breaks p95 DOWN by route — the
// aggregate http_req_duration hides which endpoint is the slow one. The
// dashboard fan-out is the prime suspect, so we want it isolated.
const tModels = new Trend('lat_models', true);
const tDashboard = new Trend('lat_dashboard', true);
const tMonitors = new Trend('lat_monitors', true);
const readErrors = new Rate('read_errors');

// Ramping-VU profiles. 'load' climbs to a modest steady peak and holds (the SLO
// measurement). 'stress' climbs higher to find where latency/error-rate degrade.
const STAGES = {
  load: [
    { duration: '20s', target: 20 }, // ramp up to 20 concurrent users
    { duration: '40s', target: 20 }, // hold at peak — this is where the SLO is judged
    { duration: '10s', target: 0 },  // ramp down
  ],
  stress: [
    { duration: '20s', target: 50 },
    { duration: '30s', target: 100 }, // push well past expected peak
    { duration: '30s', target: 100 },
    { duration: '10s', target: 0 },
  ],
};

export const options = {
  scenarios: {
    read_heavy: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: STAGES[STAGE],
      gracefulRampDown: '5s',
    },
  },
  // SLO thresholds — the PASS/FAIL contract for the read path. abortOnFail on the
  // error rate stops a doomed stress run early instead of burning the full budget.
  thresholds: {
    http_req_failed: ['rate<0.02'], // <2% of requests may fail under load
    http_req_duration: ['p(95)<1000'], // warm-state p95 under 1s across all reads
    read_errors: [{ threshold: 'rate<0.02', abortOnFail: true, delayAbortEval: '10s' }],
    lat_models: ['p(95)<800'],
    lat_dashboard: ['p(95)<1200'], // fan-out endpoint gets a looser bound (4 backends)
    lat_monitors: ['p(95)<800'],
  },
};

// setup(): mint the shared JWT exactly once. Its return value is handed to every
// VU iteration as the `data` argument. If login fails here the whole test aborts
// (no token => nothing to test), which is the correct fail-fast behavior.
export function setup() {
  const res = http.post(
    `${BASE_URL}/api/v1/login`,
    JSON.stringify({ email: EMAIL, password: PASSWORD }),
    { headers: { 'Content-Type': 'application/json' }, tags: { name: 'login' } },
  );
  check(res, { 'setup login -> 200': (r) => r.status === 200 });
  let token = '';
  try { token = JSON.parse(res.body).accessToken || ''; } catch (_) { /* fallthrough */ }
  if (!token) {
    throw new Error(`setup login failed: status=${res.status} body=${res.body}`);
  }
  return { token };
}

export default function (data) {
  // Every VU reuses the single token minted in setup().
  const params = (name) => ({
    headers: { Authorization: `Bearer ${data.token}` },
    tags: { name },
  });

  // group() labels requests in the summary so each route's stats are legible.
  group('read_models', () => {
    const r = http.get(`${BASE_URL}/api/v1/models`, params('models'));
    tModels.add(r.timings.duration);
    readErrors.add(!check(r, { 'models 200': (x) => x.status === 200 }));
  });

  group('read_dashboard', () => {
    const r = http.get(`${BASE_URL}/api/v1/dashboard`, params('dashboard'));
    tDashboard.add(r.timings.duration);
    readErrors.add(!check(r, { 'dashboard 200': (x) => x.status === 200 }));
  });

  group('read_monitors', () => {
    const r = http.get(`${BASE_URL}/api/v1/monitors`, params('monitors'));
    tMonitors.add(r.timings.duration);
    readErrors.add(!check(r, { 'monitors 200': (x) => x.status === 200 }));
  });

  // A short think-time so VUs model real users (continuous polling), not a
  // closed-loop benchmark that just measures server max throughput. This keeps
  // the request rate proportional to VU count, which is what makes the ramp a
  // meaningful concurrency sweep.
  sleep(1);
}
