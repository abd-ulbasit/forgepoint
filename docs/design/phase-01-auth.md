# Phase 1 Design Note — Auth/IAM Service

> Per-phase design doc. The task-by-task mechanics live in
> `docs/plans/forgepoint-implementation-plan.md` (Phase 1). This note captures the
> **architectural decisions and their tradeoffs** — the things an interviewer probes,
> and the things future-me needs to be able to defend line-by-line.

## What this service is

The Auth/IAM service is the platform's identity authority. It owns users, API keys,
roles/permissions, issues JWTs on login, and exposes `ValidateToken` / `CheckPermission`
RPCs that **every other service** calls from its gRPC auth interceptor. It is the first
service built because everything else depends on it for authn/authz.

Pattern headline: **Centralized authentication, stateless JWT propagation, RBAC.**

## Key decisions

### D1 — Centralized auth service (not per-service, not pure gateway)
- **Chosen:** one service owns identity; other services validate tokens via a shared
  `pkg/grpcutil` auth interceptor.
- **Alternatives:** (a) auth logic duplicated per service — inconsistent, unmaintainable;
  (b) only at the API gateway — internal service-to-service calls would be unauthenticated
  (the "hard shell, soft center" anti-pattern). We want zero-trust between services.
- **Tradeoff:** the Auth service is a dependency for login/issuance, but **token validation
  is local** (see D2), so Auth being down does not block already-authenticated traffic.

### D2 — Stateless JWT, verified locally by each service
- **Chosen:** services verify the JWT signature locally with the shared secret/public key;
  they do **not** call Auth on every request.
- **Why:** a remote `ValidateToken` call per request would make Auth a latency- and
  availability-bottleneck on the hot path of all 10 services. Local verification is O(1) crypto.
- **Tradeoff:** revocation is not instant — a JWT is valid until `exp`. Mitigation: short TTLs
  (minutes), and `ValidateToken` RPC still exists for the API-key path and for callers that
  want centralized checks. This is the classic **JWT vs opaque-token-introspection** tradeoff:
  we trade instant revocation for availability + latency. Interview framing: "stateless JWT,
  short TTL, accept eventual revocation; if we needed instant revocation we'd add a denylist
  in Redis checked by the interceptor."

### D3 — API keys: prefix + hash, raw shown once
- Store `key_prefix` (first 8 chars, for lookup/display) + `key_hash` (SHA256 of the full key).
  Return the **raw key exactly once** at creation. This is the Stripe/GitHub model.
- **Why SHA256, not bcrypt, for API keys:** API keys are high-entropy random 32-byte values,
  so brute-force isn't a threat the way it is for human passwords — a fast hash is fine and
  lets us index/lookup by hash. Passwords (low entropy) use **bcrypt** (D4).
- Lookup path: client sends key → we take its prefix → narrow candidates by `key_prefix`
  index → verify SHA256 hash → constant-time compare.

### D4 — Passwords: bcrypt
- `golang.org/x/crypto/bcrypt` with default cost. Per-hash salt is built in. Never store or log
  plaintext. Login = `bcrypt.CompareHashAndPassword`.

### D5 — RBAC: roles → permissions(resource, action), wildcards
- A user has roles; a role has permissions; a permission is `{resource, action}` with `*`
  wildcard support (`admin` = `{*,*}`). `CheckPermission(user, resource, action)` loads the
  user's roles and returns allowed if any permission matches.
- **Why RBAC over ABAC:** RBAC is the right complexity for this platform — explainable,
  enough granularity (models/pipelines/experiments × read/write/*). ABAC (attribute policies,
  OPA/Rego) is the next step if we needed per-resource ownership rules; noted as future work.

### D6 — `ValidateToken` accepts both JWTs and API keys
- Try JWT verification first (cheap, local-style); on failure, treat the credential as an API
  key and look it up by prefix + hash. One entry point for the interceptor regardless of
  credential type.

## Clean Architecture boundary (enforced)
- `internal/domain` — `User/APIKey/Role/Permission/TokenClaims`, `AuthService`, JWT logic.
  **Zero** imports of gRPC, NATS, or `database/sql`. Defines repository interfaces.
- `internal/repository/postgres` — implements those interfaces with `pgx`.
- `internal/handler` — proto↔domain conversion, request validation, gRPC status errors.
- `internal/events` — publishes `fp.auth.user.created`, `fp.auth.apikey.rotated` via `pkg/natsutil`.

## Data model (Postgres, owned solely by Auth)
`users`, `roles`, `user_roles` (join), `api_keys`. Seeded roles: `admin`, `engineer`, `viewer`.
Permissions stored as JSONB on `roles`. Indexes on `api_keys(key_prefix)` and `api_keys(user_id)`.

## Testing strategy
- **Unit (domain):** JWT generate/validate (expired, tampered, valid), AuthService with mock
  repos (create user, duplicate email, API key create/verify, permission allow/deny, login).
- **Integration (testcontainers Postgres):** repositories against real Postgres + migrations.
- **Component (bufconn):** handler RPCs end-to-end in-process.
- TDD throughout (test first, watch fail, implement).

## Security posture
- Internal error details never returned to clients (interceptor sanitizes non-auth status
  messages — see `pkg/grpcutil`). Tokens/PII never logged. JWT secret + DB password from env/
  Secret, never in code. Constant-time hash comparison for keys.
