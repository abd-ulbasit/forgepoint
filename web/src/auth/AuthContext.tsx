// ============================================================================
// AUTH CONTEXT — the React-facing view of the session.
// ============================================================================
//
// The non-React tokenStore is the source of truth (the axios interceptors need
// it synchronously, outside React). This context is a thin REACTIVE MIRROR of
// that store plus the cached user profile, so components can render "logged in
// as <name>" and ProtectedRoute can gate routes. On mount we hydrate from
// sessionStorage (survives reload) and subscribe to the store so an interceptor
// 401 (which clears the token) also flips this context to logged-out.

import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react'
import type { ReactNode } from 'react'
import { tokenStore } from './tokenStore'
import { login as loginRequest } from '@/api/endpoints'
import type { User } from '@/api/types'

const USER_STORAGE_KEY = 'fp_user'

interface AuthState {
  user: User | null
  isAuthenticated: boolean
  /** True only during the initial sessionStorage hydration. */
  isBootstrapping: boolean
  login: (email: string, password: string) => Promise<void>
  logout: () => void
}

const AuthContext = createContext<AuthState | undefined>(undefined)

function loadStoredUser(): User | null {
  try {
    const raw = sessionStorage.getItem(USER_STORAGE_KEY)
    return raw ? (JSON.parse(raw) as User) : null
  } catch {
    return null
  }
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null)
  const [hasToken, setHasToken] = useState<boolean>(false)
  const [isBootstrapping, setIsBootstrapping] = useState<boolean>(true)

  // Hydrate once on mount, and keep in sync with the token store (so a 401-driven
  // clear from the axios interceptor logs the UI out reactively).
  useEffect(() => {
    const token = tokenStore.get()
    if (token) {
      setHasToken(true)
      setUser(loadStoredUser())
    }
    setIsBootstrapping(false)

    const unsub = tokenStore.subscribe((t) => {
      setHasToken(Boolean(t))
      if (!t) {
        setUser(null)
        try {
          sessionStorage.removeItem(USER_STORAGE_KEY)
        } catch {
          /* ignore */
        }
      }
    })
    return unsub
  }, [])

  const login = useCallback(async (email: string, password: string) => {
    const resp = await loginRequest(email, password)
    // Persist the token (memory + sessionStorage) BEFORE setting React state so
    // any immediate subsequent request already carries the bearer header.
    tokenStore.set(resp.accessToken)
    setHasToken(true)
    setUser(resp.user)
    try {
      sessionStorage.setItem(USER_STORAGE_KEY, JSON.stringify(resp.user))
    } catch {
      /* ignore */
    }
  }, [])

  const logout = useCallback(() => {
    // Clearing the store fires the subscription above, which resets user state.
    tokenStore.clear()
    setHasToken(false)
    setUser(null)
  }, [])

  const value = useMemo<AuthState>(
    () => ({
      user,
      isAuthenticated: hasToken,
      isBootstrapping,
      login,
      logout,
    }),
    [user, hasToken, isBootstrapping, login, logout],
  )

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}

// eslint-disable-next-line react-refresh/only-export-components
export function useAuth(): AuthState {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error('useAuth must be used within <AuthProvider>')
  return ctx
}
