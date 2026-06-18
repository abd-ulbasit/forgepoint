// ============================================================================
// NotificationsPage — read/unread notification list.
// ============================================================================
// GET /api/v1/notifications (the choreography-driven notification service). The
// BFF route is read-only here (no mark-read endpoint is exposed), so we render
// read/unread VISUALLY and offer an "Unread only" filter. Unread items get an
// accent bar + dot; the unreadCount drives the header badge.
//
// SECURITY: title/body are server strings rendered as TEXT (JSX escapes them) —
// never via dangerouslySetInnerHTML — so a malicious notification body can't
// inject markup.

import { useState } from 'react'
import { useNotifications } from '@/hooks/queries'
import { EmptyState, ErrorState, TableSkeleton } from '@/components/states'
import { Card, PageHeader } from '@/components/ui'
import { StatusPill } from '@/components/StatusPill'
import { NotificationsIcon } from '@/components/layout/icons'
import { formatRelative } from '@/lib/format'
import type { Notification } from '@/api/types'

export function NotificationsPage() {
  const [unreadOnly, setUnreadOnly] = useState(false)
  const { data, isLoading, isError, error, refetch } = useNotifications({ pageSize: 50, unreadOnly })

  const notifications = data?.notifications ?? []
  const unreadCount = data?.unreadCount ?? 0

  return (
    <div>
      <PageHeader
        title="Notifications"
        description="Platform events delivered to you by the notification service."
        actions={
          <label className="flex cursor-pointer select-none items-center gap-2 text-sm text-ink-600">
            <input
              type="checkbox"
              className="h-4 w-4 rounded border-ink-300 text-brand-600 focus:ring-brand-500"
              checked={unreadOnly}
              onChange={(e) => setUnreadOnly(e.target.checked)}
            />
            Unread only
            {unreadCount > 0 && (
              <span className="rounded-full bg-brand-100 px-2 py-0.5 text-xs font-semibold text-brand-700">
                {unreadCount}
              </span>
            )}
          </label>
        }
      />

      <Card>
        {isLoading ? (
          <TableSkeleton rows={6} cols={2} />
        ) : isError ? (
          <ErrorState message={error instanceof Error ? error.message : undefined} onRetry={() => refetch()} />
        ) : notifications.length === 0 ? (
          <EmptyState
            title={unreadOnly ? 'No unread notifications' : 'No notifications'}
            description={unreadOnly ? "You're all caught up." : 'Platform events will show up here.'}
            icon={<NotificationsIcon className="h-6 w-6" />}
          />
        ) : (
          <ul className="divide-y divide-ink-100">
            {notifications.map((n) => (
              <NotificationRow key={n.id} notification={n} />
            ))}
          </ul>
        )}
      </Card>
    </div>
  )
}

function NotificationRow({ notification: n }: { notification: Notification }) {
  return (
    <li className={`relative flex gap-3 px-5 py-4 ${n.read ? '' : 'bg-brand-50/40'}`}>
      {/* Unread accent bar */}
      {!n.read && <span className="absolute inset-y-0 left-0 w-1 bg-brand-500" aria-hidden="true" />}
      {/* Unread dot */}
      <span className="mt-1.5 flex-shrink-0" aria-hidden="true">
        <span className={`block h-2 w-2 rounded-full ${n.read ? 'bg-transparent' : 'bg-brand-500'}`} />
      </span>
      <div className="min-w-0 flex-1">
        <div className="flex items-start justify-between gap-3">
          <p className={`text-sm ${n.read ? 'font-medium text-ink-700' : 'font-semibold text-ink-900'}`}>
            {n.title}
          </p>
          <div className="flex flex-shrink-0 items-center gap-2">
            <StatusPill value={n.severity} />
            <span className="whitespace-nowrap text-xs text-ink-400">{formatRelative(n.createdAt)}</span>
          </div>
        </div>
        {n.body && <p className="mt-1 text-sm text-ink-500">{n.body}</p>}
        {n.sourceService && (
          <p className="mt-1.5 text-xs text-ink-400">
            from <span className="font-medium text-ink-500">{n.sourceService}</span>
            {n.eventType ? ` · ${n.eventType}` : ''}
          </p>
        )}
      </div>
    </li>
  )
}
