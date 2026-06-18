// ============================================================================
// FORMAT HELPERS — pure display formatters. No React, no side effects.
// ============================================================================
// Centralized so every page renders timestamps, bytes, and money the same way.

/** RFC3339 timestamp -> a short, locale-aware "Jun 18, 10:30" style string. */
export function formatTime(ts?: string | null): string {
  if (!ts) return '—'
  const d = new Date(ts)
  if (Number.isNaN(d.getTime())) return '—'
  return d.toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

/** Relative time like "3m ago", "2h ago". Falls back to absolute for old dates. */
export function formatRelative(ts?: string | null): string {
  if (!ts) return '—'
  const d = new Date(ts)
  if (Number.isNaN(d.getTime())) return '—'
  const diffMs = Date.now() - d.getTime()
  const sec = Math.round(diffMs / 1000)
  if (sec < 5) return 'just now'
  if (sec < 60) return `${sec}s ago`
  const min = Math.round(sec / 60)
  if (min < 60) return `${min}m ago`
  const hr = Math.round(min / 60)
  if (hr < 24) return `${hr}h ago`
  const day = Math.round(hr / 24)
  if (day < 30) return `${day}d ago`
  return formatTime(ts)
}

/** Duration between two RFC3339 timestamps as "1m 12s". null end = "running". */
export function formatDuration(start?: string | null, end?: string | null): string {
  if (!start) return '—'
  const s = new Date(start).getTime()
  const e = end ? new Date(end).getTime() : Date.now()
  if (Number.isNaN(s) || Number.isNaN(e)) return '—'
  let ms = Math.max(0, e - s)
  const h = Math.floor(ms / 3_600_000)
  ms -= h * 3_600_000
  const m = Math.floor(ms / 60_000)
  ms -= m * 60_000
  const sec = Math.floor(ms / 1000)
  const parts: string[] = []
  if (h) parts.push(`${h}h`)
  if (m) parts.push(`${m}m`)
  parts.push(`${sec}s`)
  return parts.join(' ')
}

/** int64-as-string byte count -> "1.4 MB". */
export function formatBytes(bytes?: string | number | null): string {
  if (bytes == null) return '—'
  const n = typeof bytes === 'string' ? Number(bytes) : bytes
  if (!Number.isFinite(n) || n <= 0) return n === 0 ? '0 B' : '—'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.min(units.length - 1, Math.floor(Math.log(n) / Math.log(1024)))
  return `${(n / Math.pow(1024, i)).toFixed(i === 0 ? 0 : 1)} ${units[i]}`
}

/** Money {amountMicros (int64 string), currencyCode} -> "$12.34". */
export function formatMoney(money?: { amountMicros: string; currencyCode: string } | null): string {
  if (!money) return '—'
  const micros = Number(money.amountMicros)
  if (!Number.isFinite(micros)) return '—'
  const amount = micros / 1_000_000
  try {
    return new Intl.NumberFormat(undefined, {
      style: 'currency',
      currency: money.currencyCode || 'USD',
    }).format(amount)
  } catch {
    return `${amount.toFixed(2)} ${money.currencyCode}`
  }
}

/** Compact integer like "1.2k", "3.4M". */
export function formatCompact(n?: number | string | null): string {
  if (n == null) return '—'
  const num = typeof n === 'string' ? Number(n) : n
  if (!Number.isFinite(num)) return '—'
  return new Intl.NumberFormat(undefined, { notation: 'compact', maximumFractionDigits: 1 }).format(
    num,
  )
}

/**
 * Turn a SCREAMING_SNAKE enum value into a friendly label by stripping a known
 * prefix and title-casing: "EXECUTION_STATUS_RUNNING" -> "Running".
 */
export function humanizeEnum(value?: string | null, prefix?: string): string {
  if (!value) return '—'
  let v = value
  if (prefix && v.startsWith(prefix)) v = v.slice(prefix.length)
  // Also strip a generic trailing-segment prefix if no explicit one matched
  // (e.g. "MODEL_STAGE_PRODUCTION" with prefix "MODEL_STAGE_").
  return v
    .replace(/^_+/, '')
    .toLowerCase()
    .split('_')
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ')
}

/** Short id for display: "a1b2c3d4-...." -> "a1b2c3d4". */
export function shortId(id?: string | null): string {
  if (!id) return '—'
  return id.length > 8 ? id.slice(0, 8) : id
}
