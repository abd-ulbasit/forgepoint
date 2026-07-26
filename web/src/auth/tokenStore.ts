// ============================================================================
// TOKEN STORE — the single source of truth for the access token.
// ============================================================================
//
// SECURITY DESIGN (the critical decision, mirroring the BFF's auth
// handler comment): we keep the JWT in MEMORY (a module-level variable) AND
// mirror it to sessionStorage. Each choice is deliberate:
//
//   * IN MEMORY: the axios request interceptor reads the token synchronously
//     from here on every call — no storage round-trip in the hot path, and the
//     token is gone the instant the tab's JS context is torn down.
//
//   * sessionStorage (NOT localStorage): survives a page RELOAD (so an
//     accidental F5 doesn't log you out) but is cleared when the TAB CLOSES and
//     is NOT shared across tabs/origins. localStorage would persist forever and
//     be readable by any script on the origin — a bigger XSS blast radius. We
//     accept the residual XSS exposure of any JS-readable token because the
//     mitigations below cap it; see the production note.
//
//   * THE PRODUCTION UPGRADE (documented, one BFF flip away): the more secure
//     option is an httpOnly + Secure + SameSite cookie set by the BFF on login.
//     httpOnly makes the token invisible to JS entirely (XSS cannot read it),
//     and the BFF's RequireAuth already accepts a cookie transport
//     (httpx.cookieName = "fp_token"). To switch: have /login Set-Cookie instead
//     of returning the body token, drop this store, and rely on the browser to
//     send the cookie automatically (add a CSRF token for state-changing calls).
//     We ship the body-token flow for local dev simplicity and document the
//     cookie path as the production hardening — exactly the BFF's stance.
//
// We NEVER log the token and NEVER place it in a URL (only the Authorization
// header), so it cannot leak via referrer headers, server logs, or browser
// history.

const STORAGE_KEY = 'fp_access_token'

// In-memory copy — authoritative during the tab's lifetime.
let inMemoryToken: string | null = null

// Subscribers (the AuthProvider) are notified when the token is cleared by the
// 401 interceptor, so React state stays in sync with this non-React store.
type Listener = (token: string | null) => void
const listeners = new Set<Listener>()

function notify(token: string | null) {
  for (const l of listeners) l(token)
}

export const tokenStore = {
  /** Read the current token (memory first, then sessionStorage on cold load). */
  get(): string | null {
    if (inMemoryToken) return inMemoryToken
    try {
      inMemoryToken = sessionStorage.getItem(STORAGE_KEY)
    } catch {
      // sessionStorage can throw in private-mode / sandboxed iframes; degrade to
      // memory-only rather than crash.
      inMemoryToken = null
    }
    return inMemoryToken
  },

  /** Persist a freshly minted token (memory + sessionStorage). */
  set(token: string): void {
    inMemoryToken = token
    try {
      sessionStorage.setItem(STORAGE_KEY, token)
    } catch {
      /* memory-only fallback */
    }
    notify(token)
  },

  /** Clear on logout or 401. */
  clear(): void {
    inMemoryToken = null
    try {
      sessionStorage.removeItem(STORAGE_KEY)
    } catch {
      /* ignore */
    }
    notify(null)
  },

  /** Subscribe to token changes; returns an unsubscribe fn. */
  subscribe(listener: Listener): () => void {
    listeners.add(listener)
    return () => listeners.delete(listener)
  },
}
