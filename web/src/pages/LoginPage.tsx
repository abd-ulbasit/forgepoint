// ============================================================================
// LoginPage — email + password -> POST /api/v1/login.
// ============================================================================
// On success the AuthContext stores the token and we navigate to the page the
// user originally requested (the `from` query param set by ProtectedRoute / the
// 401 interceptor), defaulting to the dashboard. The form is the only place the
// password lives; we never log it and never put credentials in the URL.

import { useState, type FormEvent } from 'react'
import { Navigate, useNavigate, useSearchParams } from 'react-router-dom'
import { useAuth } from '@/auth/AuthContext'
import { ApiError } from '@/api/client'
import { Spinner } from '@/components/states'

export function LoginPage() {
  const { login, isAuthenticated } = useAuth()
  const navigate = useNavigate()
  const [params] = useSearchParams()

  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // The path to return to after login (sanitized — only allow same-app paths).
  const rawFrom = params.get('from')
  const from = rawFrom && rawFrom.startsWith('/') && !rawFrom.startsWith('//') ? rawFrom : '/'

  // Already logged in (e.g. navigated to /login manually) -> go home.
  // WHY <Navigate> instead of navigate(): calling navigate() during render is
  // a React anti-pattern — it triggers a Router state update as a side-effect
  // of rendering, which React 18 Strict Mode flags and which can cause double
  // renders / infinite loops in transitions. <Navigate> is declarative: it
  // returns a redirect element that React Router handles cleanly via its own
  // renderer, with no side-effects in the component's render phase.
  if (isAuthenticated) return <Navigate to={from} replace />

  async function handleSubmit(e: FormEvent) {
    e.preventDefault()
    setError(null)
    setSubmitting(true)
    try {
      await login(email, password)
      navigate(from, { replace: true })
    } catch (err) {
      // The BFF maps invalid credentials to 401 with "unauthenticated".
      if (err instanceof ApiError && err.status === 401) {
        setError('Invalid email or password.')
      } else {
        setError(err instanceof Error ? err.message : 'Login failed. Please try again.')
      }
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div className="flex min-h-full items-center justify-center bg-gradient-to-br from-ink-900 via-ink-900 to-brand-950 px-4 py-12">
      <div className="w-full max-w-md">
        {/* Brand */}
        <div className="mb-8 flex flex-col items-center text-center">
          <div className="mb-3 flex h-12 w-12 items-center justify-center rounded-xl bg-brand-600 text-xl font-bold text-white shadow-lg">
            F
          </div>
          <h1 className="text-2xl font-bold text-white">Forgepoint Console</h1>
          <p className="mt-1 text-sm text-ink-400">Sign in to the ML lifecycle platform</p>
        </div>

        <form onSubmit={handleSubmit} className="card space-y-5 p-6 sm:p-8" noValidate>
          {error && (
            <div
              role="alert"
              className="flex items-start gap-2 rounded-lg border border-red-200 bg-red-50 px-3 py-2.5 text-sm text-red-700"
            >
              <svg className="mt-0.5 h-4 w-4 flex-shrink-0" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126z" />
              </svg>
              <span>{error}</span>
            </div>
          )}

          <div>
            <label htmlFor="email" className="label">
              Email
            </label>
            <input
              id="email"
              type="email"
              autoComplete="username"
              required
              className="input"
              placeholder="you@company.com"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
            />
          </div>

          <div>
            <label htmlFor="password" className="label">
              Password
            </label>
            <input
              id="password"
              type="password"
              autoComplete="current-password"
              required
              className="input"
              placeholder="••••••••"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
            />
          </div>

          <button type="submit" className="btn-primary w-full" disabled={submitting}>
            {submitting ? (
              <>
                <Spinner className="h-4 w-4 text-white" /> Signing in…
              </>
            ) : (
              'Sign in'
            )}
          </button>
        </form>

        <p className="mt-6 text-center text-xs text-ink-500">
          Authenticated via the BFF. Your session token is held in memory for this tab only.
        </p>
      </div>
    </div>
  )
}
