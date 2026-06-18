// ============================================================================
// STATE COMPONENTS — the loading / empty / error trio every list & page reuses.
// ============================================================================
// A product feels finished when EVERY async surface handles its three states
// gracefully. Standardizing them here means no page reinvents (or forgets) one.

import type { ReactNode } from 'react'

/** A spinner sized to its container. */
export function Spinner({ className = 'h-5 w-5' }: { className?: string }) {
  return (
    <svg className={`animate-spin text-brand-500 ${className}`} viewBox="0 0 24 24" fill="none" aria-hidden="true">
      <circle className="opacity-20" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
      <path className="opacity-90" fill="currentColor" d="M4 12a8 8 0 018-8v4a4 4 0 00-4 4H4z" />
    </svg>
  )
}

/** Full-panel loading state with a centered spinner + label. */
export function LoadingState({ label = 'Loading…' }: { label?: string }) {
  return (
    <div className="flex flex-col items-center justify-center gap-3 py-16 text-ink-400" role="status">
      <Spinner className="h-7 w-7" />
      <span className="text-sm font-medium">{label}</span>
    </div>
  )
}

/** Skeleton rows for table loading — preserves layout while data loads. */
export function TableSkeleton({ rows = 5, cols = 4 }: { rows?: number; cols?: number }) {
  return (
    <div className="divide-y divide-ink-100" aria-hidden="true">
      {Array.from({ length: rows }).map((_, r) => (
        <div key={r} className="flex items-center gap-4 px-4 py-3.5">
          {Array.from({ length: cols }).map((_, c) => (
            <div
              key={c}
              className="skeleton h-4"
              style={{ width: c === 0 ? '30%' : `${18 - c * 2}%` }}
            />
          ))}
        </div>
      ))}
    </div>
  )
}

/** Error panel with an optional retry. The message is the API's sanitized text. */
export function ErrorState({
  message,
  onRetry,
}: {
  message?: string
  onRetry?: () => void
}) {
  return (
    <div className="flex flex-col items-center justify-center gap-3 py-14 text-center" role="alert">
      <div className="flex h-12 w-12 items-center justify-center rounded-full bg-red-50 text-red-600">
        <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor" aria-hidden="true">
          <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m9-.75a9 9 0 11-18 0 9 9 0 0118 0zm-9 3.75h.008v.008H12v-.008z" />
        </svg>
      </div>
      <div>
        <p className="text-sm font-semibold text-ink-800">Something went wrong</p>
        <p className="mt-1 max-w-md text-sm text-ink-500">{message || 'Unable to load data.'}</p>
      </div>
      {onRetry && (
        <button type="button" onClick={onRetry} className="btn-secondary mt-1">
          Try again
        </button>
      )}
    </div>
  )
}

/** Empty state with an icon, a message, and an optional call to action. */
export function EmptyState({
  title,
  description,
  icon,
  action,
}: {
  title: string
  description?: string
  icon?: ReactNode
  action?: ReactNode
}) {
  return (
    <div className="flex flex-col items-center justify-center gap-3 py-16 text-center">
      <div className="flex h-12 w-12 items-center justify-center rounded-full bg-ink-100 text-ink-400">
        {icon ?? (
          <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" strokeWidth={1.8} stroke="currentColor" aria-hidden="true">
            <path strokeLinecap="round" strokeLinejoin="round" d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2h7" />
            <path strokeLinecap="round" strokeLinejoin="round" d="M16 16h6m-3-3v6" />
          </svg>
        )}
      </div>
      <div>
        <p className="text-sm font-semibold text-ink-800">{title}</p>
        {description && <p className="mt-1 max-w-md text-sm text-ink-500">{description}</p>}
      </div>
      {action}
    </div>
  )
}
