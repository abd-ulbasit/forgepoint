// ============================================================================
// ENTRY POINT — provider composition.
// ============================================================================
// The provider order matters and encodes the app's dependencies:
//   QueryClientProvider  — server-state cache (used by every page)
//     BrowserRouter      — routing (AuthProvider reads location indirectly)
//       AuthProvider     — session state (gates routes, drives the topbar)
//         ToastProvider  — global notifications (mutations report here)
//           App          — routes
// React Query is configured with conservative defaults: no refetch-on-focus
// (avoids surprise reloads), one retry, and a 15s stale time so navigating back
// to a list doesn't refetch instantly.

import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { BrowserRouter } from 'react-router-dom'
import './index.css'
import App from './App'
import { AuthProvider } from '@/auth/AuthContext'
import { ToastProvider } from '@/components/Toast'
import { ApiError } from '@/api/client'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      staleTime: 15_000,
      // Don't retry auth/permission/not-found — they won't fix themselves and a
      // 401 has already triggered a redirect. Retry transient 5xx/network once.
      retry: (failureCount, error) => {
        if (error instanceof ApiError && [400, 401, 403, 404, 409].includes(error.status)) {
          return false
        }
        return failureCount < 1
      },
    },
  },
})

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <AuthProvider>
          <ToastProvider>
            <App />
          </ToastProvider>
        </AuthProvider>
      </BrowserRouter>
    </QueryClientProvider>
  </StrictMode>,
)
