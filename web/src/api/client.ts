// ============================================================================
// HTTP CLIENT — one axios instance for the whole app.
// ============================================================================
//
// This module centralizes THREE cross-cutting concerns so no page has to think
// about them:
//
//   1. BASE URL = "/api". Always a RELATIVE, same-origin path. In dev the Vite
//      proxy forwards "/api/*" to the BFF (see vite.config.ts); in prod the SPA
//      is served from the same origin as the BFF. The browser therefore never
//      makes a cross-origin call, so there is no CORS preflight and no hardcoded
//      backend host shipped in the bundle (no secrets, no environment coupling).
//
//   2. TOKEN ATTACH (request interceptor): every outgoing call gets
//      "Authorization: Bearer <token>" if we have one. This is the SPA side of
//      the BFF's token-forwarding contract (httpx.requestToken reads exactly
//      this header). The token comes from the in-memory tokenStore — never from
//      a URL param — so it can't leak through logs, referrers, or history.
//
//   3. 401 HANDLING (response interceptor): a 401 means the token is missing,
//      expired, or rejected downstream (gRPC Unauthenticated -> HTTP 401 via the
//      BFF mapping). We clear the token and hard-redirect to /login. Doing it in
//      ONE place means no page has to handle session expiry — it's structural,
//      the same "secure by construction" idea as the BFF's protected mux.
//
// We also normalize the BFF's sanitized error envelope ({error:{code,message}})
// into a typed ApiError so pages can show a clean message instead of digging
// through axios internals.

import axios, { AxiosError, type AxiosInstance } from 'axios'
import { tokenStore } from '@/auth/tokenStore'
import type { ApiErrorBody } from './types'

/** A normalized error the UI can render directly. */
export class ApiError extends Error {
  readonly status: number
  constructor(status: number, message: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

// Guards against redirect loops: if we're already on /login, a 401 shouldn't
// bounce us again.
function isOnLoginRoute(): boolean {
  return window.location.pathname.startsWith('/login')
}

function createClient(): AxiosInstance {
  const instance = axios.create({
    baseURL: '/api',
    headers: { 'Content-Type': 'application/json' },
    // 15s is generous for a BFF aggregate (the dashboard fans out to 4 services
    // with a 3s per-call budget); long enough to be safe, short enough to fail.
    timeout: 15_000,
  })

  // --- Request: attach the bearer token -----------------------------------
  instance.interceptors.request.use((config) => {
    const token = tokenStore.get()
    if (token) {
      config.headers.set('Authorization', `Bearer ${token}`)
    }
    return config
  })

  // --- Response: normalize errors + handle 401 ----------------------------
  instance.interceptors.response.use(
    (resp) => resp,
    (error: AxiosError<ApiErrorBody>) => {
      const status = error.response?.status ?? 0

      // Session expiry / unauthenticated: clear and redirect ONCE.
      if (status === 401 && !isOnLoginRoute()) {
        tokenStore.clear()
        // Preserve where the user was so login can send them back.
        const from = encodeURIComponent(window.location.pathname + window.location.search)
        window.location.assign(`/login?from=${from}`)
      }

      // Pull the BFF's sanitized message if present; otherwise a generic one.
      const message =
        error.response?.data?.error?.message ||
        (status === 0 ? 'Network error — is the BFF reachable?' : error.message) ||
        'Request failed'

      return Promise.reject(new ApiError(status, message))
    },
  )

  return instance
}

export const apiClient = createClient()
