// load/write.js — k6 load test for the auth-protected WRITE path:
// POST /api/v1/models (model registration).
//
// ============================================================================
// WHY A SEPARATE WRITE SCRIPT (and why it is SAFE to run on the live cluster)
// ============================================================================
// Reads are cheap and side-effect-free; writes create durable rows AND publish
// NATS events (fp.models.registered -> pipeline-orchestrator + experiment-tracker).
// We keep them in a separate, LOW-VU, capped-iteration scenario so a write load
// test can be run deliberately without accidentally being bundled into a read
// stress run that would flood the registry with junk rows.
//
// SAFETY MEASURES (so this is safe against the deployed platform):
//   1. Every model name is prefixed `k6-loadtest-` + timestamp + VU/iter, so the
//      test data is trivially identifiable and cleanable (DELETE WHERE name LIKE
//      'k6-loadtest-%'), and cannot collide with real models.
//   2. A unique idempotencyKey per request so a k6 retry never double-creates,
//      AND a duplicate name returns 409 AlreadyExists (which we tolerate, not
//      fail) — registration is idempotent by the registry's own contract.
//   3. shared-iterations executor with a HARD cap (MAX_WRITES) so the blast
//      radius is bounded no matter how the ramp behaves — you know the exact
//      maximum number of rows created before you start.
//
// RUN:
//   port-forward first (svc/fp-bff 8081:8081), then:
//     k6 run load/write.js
//     k6 run -e MAX_WRITES=50 -e WRITE_VUS=5 load/write.js
//   Clean up afterward (psql into postgres-0, db fp_registry):
//     DELETE FROM models WHERE name LIKE 'k6-loadtest-%';
// ============================================================================

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8081';
const EMAIL = __ENV.EMAIL || 'admin@forgepoint.local';
const PASSWORD = __ENV.PASSWORD || '';
const MAX_WRITES = parseInt(__ENV.MAX_WRITES || '20', 10);
const WRITE_VUS = parseInt(__ENV.WRITE_VUS || '4', 10);

const writeErrors = new Rate('write_errors');
const created = new Counter('models_created'); // 201s
const conflicts = new Counter('models_conflict'); // 409s (idempotent dup — OK)

export const options = {
  scenarios: {
    register_models: {
      executor: 'shared-iterations', // HARD cap on total work = bounded blast radius
      vus: WRITE_VUS,
      iterations: MAX_WRITES,
      maxDuration: '60s',
    },
  },
  thresholds: {
    // A write either succeeds (201) or idempotently conflicts (409); anything
    // else (5xx, 401, timeout) is a real error. Allow <5% under write load.
    write_errors: ['rate<0.05'],
    // Writes are heavier than reads (DB insert + outbox/event publish), so a
    // looser p95 than the read path.
    http_req_duration: ['p(95)<1500'],
  },
};

export function setup() {
  const res = http.post(
    `${BASE_URL}/api/v1/login`,
    JSON.stringify({ email: EMAIL, password: PASSWORD }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  let token = '';
  try { token = JSON.parse(res.body).accessToken || ''; } catch (_) { /* fallthrough */ }
  if (!token) throw new Error(`setup login failed: status=${res.status}`);
  return { token };
}

export default function (data) {
  // Unique, identifiable, collision-proof name + idempotency key.
  const uniq = `${Date.now()}-${__VU}-${__ITER}`;
  const name = `k6-loadtest-${uniq}`;
  const body = JSON.stringify({
    name,
    description: 'k6 load test model — safe to delete',
    framework: 'onnx',
    taskType: 'classification',
    tags: { source: 'k6-loadtest' },
    idempotencyKey: `k6-${uniq}`,
  });

  const res = http.post(`${BASE_URL}/api/v1/models`, body, {
    headers: { Authorization: `Bearer ${data.token}`, 'Content-Type': 'application/json' },
    tags: { name: 'register_model' },
  });

  // 201 = created, 409 = idempotent duplicate (acceptable). Both are "not an error".
  const ok = res.status === 201 || res.status === 409;
  if (res.status === 201) created.add(1);
  if (res.status === 409) conflicts.add(1);
  check(res, {
    'register -> 201 or 409': () => ok,
    'register body is JSON': (r) => {
      try { JSON.parse(r.body); return true; } catch (_) { return false; }
    },
  });
  writeErrors.add(!ok);

  sleep(0.3);
}
