# Forgepoint Web UI

The single-page console for the Forgepoint ML platform. It talks **only** to the
BFF (`services/bff`) over the same-origin `/api` path; the BFF fans out to the 10
backend gRPC services. The UI never speaks gRPC and holds no service addresses.

## Stack

- **Vite 5** + **React 18** + **TypeScript** (strict)
- **Tailwind CSS 3** — design system in `tailwind.config.js` + `src/index.css`
- **react-router-dom 6** — routing + protected routes
- **@tanstack/react-query 5** — server state, caching, dedup
- **axios** — HTTP client with bearer-token attach + global 401 handling
- **recharts** — run-metric charts
- Native **SSE over `fetch()`** for the live execution (saga) stream

## Pages

| Route | Page | What it shows |
|-------|------|---------------|
| `/login` | Login | Email + password → `POST /api/v1/login` |
| `/` | Dashboard | `GET /dashboard` stat cards + recent activity (partial-failure tolerant) |
| `/models` | Models | Paginated table + register-model modal (`GET`/`POST /models`) |
| `/models/:id` | Model detail | Metadata + version history (`GET /models/{id}`, `/versions`) |
| `/pipelines` | Pipelines | Definitions list + one-click **Run** (`POST /pipelines/{id}/start`) |
| `/executions/:id` | Execution | **LIVE saga** via SSE `GET /executions/{id}/watch` — step states update in real time |
| `/experiments` | Experiments | Runs list (`GET /runs`) |
| `/runs/:id` | Run detail | Params + final metrics with a chart (`GET /runs/{id}`) |
| `/monitoring` | Monitoring | Monitors + drift reports with PSI/KL/KS scores (`GET /monitors`, `/drift-reports`) |
| `/billing` | Billing | Usage summary per team/meter (`GET /usage`) |
| `/notifications` | Notifications | Read/unread list (`GET /notifications`) |

## Prerequisites

- **Node 18+** (developed against Node available in the repo toolchain). If your
  shell uses nvm/pyenv and `npm` misbehaves non-interactively, invoke node/npm by
  absolute path or run commands under `bash -lc`.
- The **BFF reachable on `http://localhost:8081`** (its `HTTP_PORT` default).

### Point the dev proxy at the BFF

The Vite dev server proxies `/api/*` to the BFF. Reach a cluster BFF locally with
a port-forward (the BFF's HTTP port is **8081**):

```bash
kubectl -n fp-system port-forward svc/fp-bff 8081:8081
```

…or run the BFF binary directly so it listens on `:8081`. Override the target by
setting `VITE_BFF_URL` in `.env` (see `.env.example`). No secrets live in env —
the only var is the BFF base URL, and the JWT is obtained at runtime via login.

## Run

```bash
cd web
npm install
npm run dev          # http://localhost:5173 (proxies /api -> $VITE_BFF_URL)
```

Open http://localhost:5173, sign in, and the app calls the same-origin `/api`
which Vite forwards to the BFF.

## Production build

```bash
npm run build        # tsc -b (type-check) && vite build  -> dist/
npm run preview      # serve dist/ locally to smoke-test the build
```

`npm run build` runs the TypeScript project build **first** (type errors fail the
build) and then `vite build`, emitting a static bundle to `dist/`. In production,
serve `dist/` from the **same origin** as the BFF (or behind one gateway) so the
relative `/api` path resolves without CORS — exactly what the dev proxy models.

## Authentication flow

1. **Login** — `POST /api/v1/login` returns `{ accessToken, expiresAt, user }`.
2. **Token storage** — kept in memory (`src/auth/tokenStore.ts`) and mirrored to
   `sessionStorage` so a page reload doesn't log you out, but closing the tab
   does. We deliberately avoid `localStorage` (larger XSS blast radius).
3. **Attach** — an axios request interceptor adds
   `Authorization: Bearer <token>` to every `/api` call. The BFF forwards that
   token verbatim to the downstream gRPC service (identity propagation; the BFF
   does not mint or validate it). The token never appears in a URL or a log.
4. **401 handling** — a `401` from any call clears the token and redirects to
   `/login`, preserving the attempted path via `?from=`.
5. **Route guard** — `ProtectedRoute` gates the app shell; unauthenticated users
   are bounced to `/login`. This is UX only — real authZ is server-side.
6. **Logout** — clears the in-memory + sessionStorage token and returns to login.

### Production hardening note (httpOnly cookie)

The dev flow keeps the JWT in JS (memory/sessionStorage), which is reachable by
XSS. The production upgrade is to have the BFF set an **httpOnly + Secure +
SameSite cookie** on login (its `RequireAuth` already accepts the `fp_token`
cookie). The token then becomes invisible to JS; switch the SSE stream to a
native `EventSource` (the cookie rides automatically) and add a CSRF token for
state-changing calls. This is a BFF-side change with minimal SPA impact.

## Security posture

- The token is only ever sent in the `Authorization` header — never in a URL,
  query string, or log line.
- All server-provided strings render as **text** (JSX auto-escapes); the app uses
  **no** `dangerouslySetInnerHTML`.
- The API base URL is configurable (`VITE_BFF_URL`); no backend hosts or secrets
  are baked into the bundle.

## Project layout

```
src/
  api/         types.ts (BFF JSON contract), client.ts (axios), endpoints.ts
  auth/        tokenStore.ts, AuthContext.tsx, ProtectedRoute.tsx
  components/  ui.tsx, states.tsx, StatusPill.tsx, Toast.tsx, layout/
  hooks/       queries.ts (react-query), useExecutionWatch.ts (SSE), usePagination.ts
  lib/         format.ts (display formatters)
  pages/       one file per route
```
