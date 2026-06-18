import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'node:path'

// ============================================================================
// VITE CONFIG — dev server, BFF proxy, and the '@' path alias.
// ============================================================================
//
// THE DEV PROXY (the important bit): in development the SPA talks to a
// SAME-ORIGIN "/api" path. Vite's dev server then transparently forwards every
// "/api/*" request to the Forgepoint BFF. This buys us two things:
//
//   1. No CORS in dev. Because the browser only ever sees http://localhost:5173,
//      the cross-origin call to the BFF (a different port) is made server-side
//      by Vite, so the browser's same-origin policy never trips. In production
//      the SPA is served from the same origin as the BFF (or behind one
//      gateway), so "/api" is genuinely same-origin there too — the dev proxy
//      faithfully models prod rather than papering over a difference.
//   2. EventSource (SSE) works. The execution-watch stream uses the browser
//      EventSource API, which cannot set custom headers and is strict about
//      origin; proxying it same-origin keeps it simple.
//
// The BFF target is configurable via VITE_BFF_URL. Default is
// http://localhost:8081 — the BFF's HTTP_PORT default (see
// services/bff/cmd/server/main.go). Locally you reach it with:
//   kubectl -n fp-system port-forward svc/fp-bff 8081:8081
// or by running the BFF binary directly. `ws: true` lets the proxy also carry
// the SSE/streaming connection.
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  const bffUrl = env.VITE_BFF_URL || 'http://localhost:8081'

  return {
    plugins: [react()],
    resolve: {
      alias: {
        '@': path.resolve(__dirname, './src'),
      },
    },
    server: {
      port: 5173,
      proxy: {
        '/api': {
          target: bffUrl,
          changeOrigin: true,
          ws: true,
          // Do NOT buffer — SSE frames must flush to the browser immediately.
          // http-proxy passes through chunked responses by default; we leave
          // compression off on this path so the stream isn't held back.
        },
      },
    },
    build: {
      outDir: 'dist',
      sourcemap: true,
    },
  }
})
