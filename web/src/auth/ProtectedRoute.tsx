// ============================================================================
// ProtectedRoute — the client-side route guard.
// ============================================================================
//
// Wraps the authenticated section of the app. If there's no session it redirects
// to /login, preserving the attempted location in router state so login can
// bounce the user back. NOTE: this is UX-only, not a security boundary — the
// REAL authorization happens server-side (the BFF forwards the token; each gRPC
// service validates it). A user who bypasses this guard just hits 401s from the
// API. We still gate the UI so unauthenticated users see the login page, not a
// flash of empty dashboards.

import { Navigate, useLocation } from 'react-router-dom'
import type { ReactNode } from 'react'
import { useAuth } from './AuthContext'

export function ProtectedRoute({ children }: { children: ReactNode }) {
  const { isAuthenticated, isBootstrapping } = useAuth()
  const location = useLocation()

  // While hydrating the token from sessionStorage, render nothing (a blank
  // frame for a few ms) rather than briefly redirecting an actually-logged-in
  // user to /login on a hard refresh.
  if (isBootstrapping) {
    return (
      <div className="flex h-full items-center justify-center text-ink-400">
        <span className="text-sm">Loading session…</span>
      </div>
    )
  }

  if (!isAuthenticated) {
    const from = encodeURIComponent(location.pathname + location.search)
    return <Navigate to={`/login?from=${from}`} replace />
  }

  return <>{children}</>
}
