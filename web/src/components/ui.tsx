// ============================================================================
// UI PRIMITIVES — Card, PageHeader, StatCard, KeyValue, Modal.
// ============================================================================
// Small, composable building blocks shared across pages. They encode the
// design-system spacing/typography so pages stay declarative and consistent.

import { useEffect, type ReactNode } from 'react'

// ---- Card ------------------------------------------------------------------

export function Card({ children, className = '' }: { children: ReactNode; className?: string }) {
  return <div className={`card ${className}`}>{children}</div>
}

export function CardHeader({
  title,
  subtitle,
  action,
}: {
  title: ReactNode
  subtitle?: ReactNode
  action?: ReactNode
}) {
  return (
    <div className="flex items-start justify-between gap-4 border-b border-ink-100 px-5 py-4">
      <div>
        <h3 className="text-sm font-semibold text-ink-900">{title}</h3>
        {subtitle && <p className="mt-0.5 text-xs text-ink-500">{subtitle}</p>}
      </div>
      {action}
    </div>
  )
}

// ---- PageHeader ------------------------------------------------------------

export function PageHeader({
  title,
  description,
  actions,
}: {
  title: string
  description?: string
  actions?: ReactNode
}) {
  return (
    <div className="mb-6 flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
      <div>
        <h1 className="text-xl font-bold tracking-tight text-ink-900">{title}</h1>
        {description && <p className="mt-1 text-sm text-ink-500">{description}</p>}
      </div>
      {actions && <div className="flex items-center gap-2">{actions}</div>}
    </div>
  )
}

// ---- StatCard --------------------------------------------------------------

export function StatCard({
  label,
  value,
  hint,
  icon,
  tone = 'brand',
  degraded = false,
}: {
  label: string
  value: ReactNode
  hint?: ReactNode
  icon?: ReactNode
  tone?: 'brand' | 'emerald' | 'amber' | 'sky'
  degraded?: boolean
}) {
  const toneRing: Record<string, string> = {
    brand: 'bg-brand-50 text-brand-600',
    emerald: 'bg-emerald-50 text-emerald-600',
    amber: 'bg-amber-50 text-amber-600',
    sky: 'bg-sky-50 text-sky-600',
  }
  return (
    <div className="card flex items-center gap-4 p-5 transition-shadow hover:shadow-card-hover">
      {icon && (
        <div className={`flex h-11 w-11 flex-shrink-0 items-center justify-center rounded-lg ${toneRing[tone]}`}>
          {icon}
        </div>
      )}
      <div className="min-w-0">
        <p className="truncate text-xs font-medium uppercase tracking-wide text-ink-500">{label}</p>
        {degraded ? (
          <p className="mt-1 text-sm font-medium text-amber-600">Unavailable</p>
        ) : (
          <p className="mt-0.5 text-2xl font-bold tracking-tight text-ink-900">{value}</p>
        )}
        {hint && !degraded && <p className="mt-0.5 truncate text-xs text-ink-400">{hint}</p>}
      </div>
    </div>
  )
}

// ---- KeyValue (definition list) -------------------------------------------

export function KeyValue({ items }: { items: { label: string; value: ReactNode }[] }) {
  return (
    <dl className="grid grid-cols-1 gap-x-6 gap-y-4 sm:grid-cols-2">
      {items.map((it) => (
        <div key={it.label}>
          <dt className="text-xs font-medium uppercase tracking-wide text-ink-400">{it.label}</dt>
          <dd className="mt-1 break-words text-sm text-ink-800">{it.value}</dd>
        </div>
      ))}
    </dl>
  )
}

// ---- Modal -----------------------------------------------------------------

export function Modal({
  open,
  onClose,
  title,
  children,
  footer,
}: {
  open: boolean
  onClose: () => void
  title: string
  children: ReactNode
  footer?: ReactNode
}) {
  // Close on Escape; lock body scroll while open (accessibility & polish).
  useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    document.addEventListener('keydown', onKey)
    const prev = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    return () => {
      document.removeEventListener('keydown', onKey)
      document.body.style.overflow = prev
    }
  }, [open, onClose])

  if (!open) return null

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center p-4"
      role="dialog"
      aria-modal="true"
      aria-label={title}
    >
      {/* Backdrop — click to dismiss. */}
      <div className="absolute inset-0 bg-ink-900/40 backdrop-blur-sm" onClick={onClose} aria-hidden="true" />
      <div className="relative z-10 w-full max-w-lg animate-fade-in rounded-xl bg-white shadow-xl">
        <div className="flex items-center justify-between border-b border-ink-100 px-5 py-4">
          <h2 className="text-base font-semibold text-ink-900">{title}</h2>
          <button
            type="button"
            onClick={onClose}
            className="rounded-md p-1 text-ink-400 hover:bg-ink-100 hover:text-ink-700"
            aria-label="Close dialog"
          >
            <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
        <div className="px-5 py-5">{children}</div>
        {footer && <div className="flex justify-end gap-2 border-t border-ink-100 px-5 py-4">{footer}</div>}
      </div>
    </div>
  )
}

// ---- Table shell -----------------------------------------------------------

export function Table({ children }: { children: ReactNode }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left text-sm">{children}</table>
    </div>
  )
}

export function Th({ children, className = '' }: { children?: ReactNode; className?: string }) {
  return (
    <th className={`whitespace-nowrap px-5 py-3 text-xs font-semibold uppercase tracking-wide text-ink-400 ${className}`}>
      {children}
    </th>
  )
}

export function Td({ children, className = '' }: { children?: ReactNode; className?: string }) {
  return <td className={`px-5 py-3.5 align-middle text-ink-700 ${className}`}>{children}</td>
}

// ---- Pagination bar --------------------------------------------------------

export function PaginationBar({
  total,
  shown,
  hasNext,
  onNext,
  onReset,
  page,
}: {
  total: number
  shown: number
  hasNext: boolean
  onNext: () => void
  onReset: () => void
  page: number
}) {
  return (
    <div className="flex items-center justify-between border-t border-ink-100 px-5 py-3 text-sm">
      <p className="text-ink-500">
        Showing <span className="font-medium text-ink-700">{shown}</span>
        {total > 0 && <> of <span className="font-medium text-ink-700">{total}</span></>}
      </p>
      <div className="flex items-center gap-2">
        <button
          type="button"
          className="btn-secondary px-3 py-1.5"
          onClick={onReset}
          disabled={page === 0}
        >
          First
        </button>
        <button type="button" className="btn-secondary px-3 py-1.5" onClick={onNext} disabled={!hasNext}>
          Next
        </button>
      </div>
    </div>
  )
}

// ---- Mono code chip --------------------------------------------------------

export function Mono({ children }: { children: ReactNode }) {
  return <code className="rounded bg-ink-100 px-1.5 py-0.5 font-mono text-xs text-ink-700">{children}</code>
}
