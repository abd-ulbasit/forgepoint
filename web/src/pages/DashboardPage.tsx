// ============================================================================
// DashboardPage — the BFF /dashboard aggregate rendered as stat cards.
// ============================================================================
// This page is the consumer side of the BFF's fan-out: ONE request returns four
// independent tiles, each with its own {ok,error,data}. We render every tile's
// state INDEPENDENTLY — a degraded tile shows "Unavailable" while the others
// render normally. That partial-failure tolerance is the whole point of the
// BFF dashboard pattern, so the UI mirrors it rather than failing wholesale.

import { Link } from 'react-router-dom'
import { useDashboard } from '@/hooks/queries'
import { ErrorState, LoadingState } from '@/components/states'
import { Card, CardHeader, PageHeader, StatCard } from '@/components/ui'
import { StatusPill } from '@/components/StatusPill'
import { formatMoney, formatRelative, shortId } from '@/lib/format'
import {
  BillingIcon,
  ModelsIcon,
  MonitoringIcon,
  PipelinesIcon,
} from '@/components/layout/icons'
import type { DriftReport, Execution, GetUsageResponse } from '@/api/types'

export function DashboardPage() {
  const { data, isLoading, isError, error, refetch } = useDashboard()

  if (isLoading) return <LoadingState label="Loading dashboard…" />
  // A hard error here means the WHOLE /dashboard call failed (e.g. 401/network),
  // not a per-tile degradation — those are handled below as data.
  if (isError || !data) {
    return <ErrorState message={error instanceof Error ? error.message : undefined} onRetry={() => refetch()} />
  }

  const modelCount = data.models.ok ? (data.models.data?.total ?? 0) : null
  const activeExecs: Execution[] = data.activeExecutions.ok
    ? (data.activeExecutions.data?.executions ?? [])
    : []
  const driftReports: DriftReport[] = data.recentDrift.ok ? (data.recentDrift.data?.reports ?? []) : []
  const usage: GetUsageResponse | undefined = data.usage.ok ? data.usage.data : undefined

  const criticalDrift = driftReports.filter((r) => r.severity === 'DRIFT_SEVERITY_CRITICAL').length

  return (
    <div>
      <PageHeader title="Dashboard" description="A live overview of your ML platform, composed by the BFF in one call." />

      {/* ---- Stat tiles (each independently degradable) ---- */}
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          label="Registered models"
          value={modelCount ?? 0}
          icon={<ModelsIcon className="h-5 w-5" />}
          tone="brand"
          degraded={!data.models.ok}
          hint="Total in the registry"
        />
        <StatCard
          label="Active executions"
          value={activeExecs.length}
          icon={<PipelinesIcon className="h-5 w-5" />}
          tone="sky"
          degraded={!data.activeExecutions.ok}
          hint="Pipelines running now"
        />
        <StatCard
          label="Recent drift reports"
          value={driftReports.length}
          icon={<MonitoringIcon className="h-5 w-5" />}
          tone={criticalDrift > 0 ? 'amber' : 'emerald'}
          degraded={!data.recentDrift.ok}
          hint={criticalDrift > 0 ? `${criticalDrift} critical` : 'No critical drift'}
        />
        <StatCard
          label="Usage (grand total)"
          value={formatMoney(usage?.grandTotal)}
          icon={<BillingIcon className="h-5 w-5" />}
          tone="emerald"
          degraded={!data.usage.ok}
          hint={usage ? `${usage.summaries.length} team(s)` : undefined}
        />
      </div>

      {/* ---- Recent activity ---- */}
      <div className="mt-6 grid grid-cols-1 gap-6 lg:grid-cols-2">
        {/* Active executions */}
        <Card>
          <CardHeader
            title="Active executions"
            subtitle="Running pipeline sagas"
            action={
              <Link to="/pipelines" className="text-xs font-semibold text-brand-600 hover:text-brand-700">
                View all
              </Link>
            }
          />
          {!data.activeExecutions.ok ? (
            <DegradedPanel name="executions" />
          ) : activeExecs.length === 0 ? (
            <p className="px-5 py-10 text-center text-sm text-ink-400">No executions running right now.</p>
          ) : (
            <ul className="divide-y divide-ink-100">
              {activeExecs.slice(0, 6).map((ex) => (
                <li key={ex.id}>
                  <Link
                    to={`/executions/${ex.id}`}
                    className="flex items-center justify-between gap-3 px-5 py-3 transition-colors hover:bg-ink-50"
                  >
                    <div className="min-w-0">
                      <p className="truncate text-sm font-medium text-ink-800">{shortId(ex.id)}</p>
                      <p className="truncate text-xs text-ink-400">
                        pipeline {shortId(ex.pipelineId)} · started {formatRelative(ex.startedAt)}
                      </p>
                    </div>
                    <StatusPill value={ex.status} />
                  </Link>
                </li>
              ))}
            </ul>
          )}
        </Card>

        {/* Recent drift */}
        <Card>
          <CardHeader
            title="Recent drift"
            subtitle="Latest monitor reports — the closed loop"
            action={
              <Link to="/monitoring" className="text-xs font-semibold text-brand-600 hover:text-brand-700">
                View all
              </Link>
            }
          />
          {!data.recentDrift.ok ? (
            <DegradedPanel name="drift reports" />
          ) : driftReports.length === 0 ? (
            <p className="px-5 py-10 text-center text-sm text-ink-400">No recent drift detected.</p>
          ) : (
            <ul className="divide-y divide-ink-100">
              {driftReports.slice(0, 6).map((r) => (
                <li key={r.id} className="flex items-center justify-between gap-3 px-5 py-3">
                  <div className="min-w-0">
                    <p className="truncate text-sm font-medium text-ink-800">{r.modelName || shortId(r.id)}</p>
                    <p className="truncate text-xs text-ink-400">
                      {r.modelVersion ? `v${r.modelVersion} · ` : ''}
                      {formatRelative(r.createdAt)}
                    </p>
                  </div>
                  <StatusPill value={r.severity} />
                </li>
              ))}
            </ul>
          )}
        </Card>
      </div>
    </div>
  )
}

/** Inline degraded panel for a tile whose backing service failed. */
function DegradedPanel({ name }: { name: string }) {
  return (
    <div className="flex items-center gap-3 px-5 py-8 text-sm text-amber-700">
      <svg className="h-5 w-5 flex-shrink-0" fill="none" viewBox="0 0 24 24" strokeWidth={1.8} stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m9-.75a9 9 0 11-18 0 9 9 0 0118 0zm-9 3.75h.008v.008H12v-.008z" />
      </svg>
      <span>The {name} service is unavailable. Other panels are unaffected.</span>
    </div>
  )
}
