// load/smoke.js — k6 SMOKE test for the Forgepoint BFF HTTP API.
//
// ============================================================================
// WHAT A SMOKE TEST IS (and why it is separate from the load test)
// ============================================================================
// A smoke test answers ONE question: "does the system work AT ALL under a
// trivial, single-user load?" It runs a handful of iterations with 1 VU and
// FAILS LOUDLY if any core endpoint is broken. It is the cheapest possible
// gate — run it first; if it is red, running the ramping load test is pointless.
// This mirrors the test pyramid: smoke (1 VU) -> load (steady) -> stress (ramp).
//
// It exercises the read-heavy BFF surface that an SPA hammers on every page
// load: login (mint a JWT once in setup), then GET /models, /dashboard,
// /monitors with that token. The thresholds are deliberately STRICT because a
// single user on an idle cluster should see fast, error-free responses; any
// breach means something is actually wrong, not just "under load".
//
// RUN:
//   1) port-forward the BFF:  kubectl --kubeconfig ~/.kube/forgepoint-thinkpad.yaml \
//                               -n fp-system port-forward svc/fp-bff 8081:8081
//   2) k6 run load/smoke.js
//   Override the target:  k6 run -e BASE_URL=http://localhost:8081 load/smoke.js
//   Override creds:       k6 run -e EMAIL=... -e PASSWORD=... load/smoke.js
// ============================================================================

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

// BASE_URL points at the BFF. Default assumes a local port-forward on 8081.
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8081';
const EMAIL = __ENV.EMAIL || 'admin@forgepoint.local';
const PASSWORD = __ENV.PASSWORD || '';

// A custom Rate metric for "business" failures (a 2xx we still consider wrong,
// e.g. a models list that is not valid JSON). k6's built-in http_req_failed only
// counts transport/status failures; this captures semantic ones too.
const bizErrors = new Rate('business_errors');

export const options = {
  vus: 1,
  iterations: 5,
  // Thresholds are the PASS/FAIL contract. If any is breached, `k6 run` exits
  // non-zero — which is what makes this usable as a CI gate.
  thresholds: {
    // 99% of ALL requests must succeed (transport + status < 400).
    http_req_failed: ['rate<0.01'],
    // p95 end-to-end latency under 1500ms. OBSERVED on the live k3s cluster: the
    // FIRST request of a run spikes to ~3s (cold paths: BFF->gRPC dial to each
    // backend, first JWT verify, and the kubectl port-forward's own warm-up),
    // while the steady-state median is ~12ms. 1500ms keeps the smoke test a real
    // regression gate (catches a genuinely slow path) without flapping on the
    // documented one-time cold start. The ramping load test (load.js) measures
    // warm-state p95 against a tighter bound. k6 method syntax is p(95), not p95.
    http_req_duration: ['p(95)<1500'],
    // Zero business-level errors tolerated in a smoke run.
    business_errors: ['rate<0.01'],
  },
};

// login() is shared by smoke and load scripts: POST /api/v1/login -> JWT.
// We do it ONCE per VU-init in setup() (see load.js) or per-iteration here.
function login() {
  const res = http.post(
    `${BASE_URL}/api/v1/login`,
    JSON.stringify({ email: EMAIL, password: PASSWORD }),
    { headers: { 'Content-Type': 'application/json' }, tags: { name: 'login' } },
  );
  check(res, {
    'login -> 200': (r) => r.status === 200,
    'login returns accessToken': (r) => {
      try { return !!JSON.parse(r.body).accessToken; } catch (_) { return false; }
    },
  });
  try { return JSON.parse(res.body).accessToken || ''; } catch (_) { return ''; }
}

export default function () {
  const token = login();
  if (!token) {
    bizErrors.add(1);
    return; // no point hammering protected routes without a token
  }
  const authHeaders = {
    headers: { Authorization: `Bearer ${token}` },
  };

  // GET /api/v1/models — the CQRS read projection (registry read side).
  const models = http.get(`${BASE_URL}/api/v1/models`, { ...authHeaders, tags: { name: 'models' } });
  const modelsOk = check(models, {
    'models -> 200': (r) => r.status === 200,
    'models body is JSON': (r) => {
      try { JSON.parse(r.body); return true; } catch (_) { return false; }
    },
  });
  bizErrors.add(!modelsOk);

  // GET /api/v1/dashboard — the BFF aggregation (fan-out to registry, pipeline,
  // monitor, billing). The single endpoint most likely to expose a slow/failing
  // downstream because it touches FOUR services in one request.
  const dash = http.get(`${BASE_URL}/api/v1/dashboard`, { ...authHeaders, tags: { name: 'dashboard' } });
  bizErrors.add(!check(dash, { 'dashboard -> 200': (r) => r.status === 200 }));

  // GET /api/v1/monitors — model-monitor read path.
  const monitors = http.get(`${BASE_URL}/api/v1/monitors`, { ...authHeaders, tags: { name: 'monitors' } });
  bizErrors.add(!check(monitors, { 'monitors -> 200': (r) => r.status === 200 }));

  sleep(0.5);
}
