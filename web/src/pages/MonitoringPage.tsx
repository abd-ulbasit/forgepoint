// ============================================================================
// MonitoringPage — monitors + drift reports (the closed-loop story).
// ============================================================================
// Two sections from the model-monitor service:
//   * Monitors (GET /api/v1/monitors): per-model watchers, their state, and
//     whether AUTO-RETRAIN is wired — the "serve → monitor → retrain" loop.
//   * Drift reports (GET /api/v1/drift-reports): detected drift with PSI/KL/KS
//     scores and severity. We expand a report to show per-feature drift metrics
//     (baseline vs current vs score) — the quantitative evidence.

import { useState } from 'react'
import { useDriftReports, useMonitors } from '@/hooks/queries'
import { ErrorState, TableSkeleton } from '@/components/states'
import { Card, CardHeader, PageHeader, Table, Td, Th, Mono } from '@/components/ui'
import { StatusPill } from '@/components/StatusPill'
import { formatRelative, humanizeEnum } from '@/lib/format'
import type { DriftReport, MonitorEntry } from '@/api/types'

export function MonitoringPage() {
  const monitors = useMonitors({ pageSize: 50 })
  const drift = useDriftReports({ pageSize: 50 })

  return (
    <div>
      <PageHeader
        title="Monitoring"
        description="Streaming drift detection and the auto-retrain loop that closes serve → monitor → retrain."
      />

      {/* Monitors */}
      <Card className="mb-6">
        <CardHeader title="Monitors" subtitle="Per-model drift watchers and their auto-retrain wiring" />
        {monitors.isLoading ? (
          <TableSkeleton rows={4} cols={5} />
        ) : monitors.isError ? (
          <ErrorState
            message={monitors.error instanceof Error ? monitors.error.message : undefined}
            onRetry={() => monitors.refetch()}
          />
        ) : (monitors.data?.entries.length ?? 0) === 0 ? (
          <p className="px-5 py-10 text-center text-sm text-ink-400">No monitors configured.</p>
        ) : (
          <Table>
            <thead>
              <tr className="border-b border-ink-100">
                <Th>Model</Th>
                <Th>State</Th>
                <Th>Health</Th>
                <Th>Auto-retrain</Th>
                <Th>Baseline</Th>
              </tr>
            </thead>
            <tbody className="divide-y divide-ink-100">
              {monitors.data!.entries.map((entry) => (
                <MonitorRow key={entry.monitor.id} entry={entry} />
              ))}
            </tbody>
          </Table>
        )}
      </Card>

      {/* Drift reports */}
      <Card>
        <CardHeader title="Drift reports" subtitle="Detected distribution shifts with PSI / KL / KS scores" />
        {drift.isLoading ? (
          <TableSkeleton rows={5} cols={5} />
        ) : drift.isError ? (
          <ErrorState
            message={drift.error instanceof Error ? drift.error.message : undefined}
            onRetry={() => drift.refetch()}
          />
        ) : (drift.data?.reports.length ?? 0) === 0 ? (
          <p className="px-5 py-10 text-center text-sm text-ink-400">No drift reports yet.</p>
        ) : (
          <ul className="divide-y divide-ink-100">
            {drift.data!.reports.map((r) => (
              <DriftReportRow key={r.id} report={r} />
            ))}
          </ul>
        )}
      </Card>
    </div>
  )
}

function MonitorRow({ entry }: { entry: MonitorEntry }) {
  const m = entry.monitor
  return (
    <tr className="hover:bg-ink-50">
      <Td>
        <p className="font-medium text-ink-900">{m.modelName}</p>
        <p className="text-xs text-ink-400">{m.ownerTeam || '—'}</p>
      </Td>
      <Td>
        <StatusPill value={m.state} />
      </Td>
      <Td>
        {entry.health ? (
          <StatusPill value={entry.health.overallSeverity} />
        ) : (
          <span className="text-xs text-ink-400">—</span>
        )}
      </Td>
      <Td>
        {m.autoRetrain ? (
          <span className="pill bg-emerald-50 text-emerald-700 ring-emerald-200">
            <span className="h-1.5 w-1.5 rounded-full bg-current" /> Enabled
          </span>
        ) : (
          <span className="pill bg-ink-100 text-ink-500 ring-ink-200">
            <span className="h-1.5 w-1.5 rounded-full bg-current" /> Off
          </span>
        )}
      </Td>
      <Td className="text-ink-500">{m.baselineVersion ? `v${m.baselineVersion}` : '—'}</Td>
    </tr>
  )
}

function DriftReportRow({ report }: { report: DriftReport }) {
  const [expanded, setExpanded] = useState(false)
  const critical = report.severity === 'DRIFT_SEVERITY_CRITICAL'

  return (
    <li>
      <button
        type="button"
        onClick={() => setExpanded((e) => !e)}
        className="flex w-full items-center justify-between gap-3 px-5 py-3.5 text-left transition-colors hover:bg-ink-50"
        aria-expanded={expanded}
      >
        <div className="min-w-0">
          <p className="truncate text-sm font-medium text-ink-900">
            {report.modelName}
            {report.modelVersion ? <span className="text-ink-400"> · v{report.modelVersion}</span> : null}
          </p>
          <p className="truncate text-xs text-ink-400">
            {humanizeEnum(report.driftType, 'DRIFT_TYPE_')} drift · {report.sampleCount} samples ·{' '}
            {formatRelative(report.createdAt)}
          </p>
        </div>
        <div className="flex items-center gap-3">
          <StatusPill value={report.severity} />
          <svg
            className={`h-4 w-4 flex-shrink-0 text-ink-400 transition-transform ${expanded ? 'rotate-90' : ''}`}
            fill="none"
            viewBox="0 0 24 24"
            strokeWidth={2}
            stroke="currentColor"
          >
            <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
          </svg>
        </div>
      </button>

      {expanded && (
        <div className="border-t border-ink-100 bg-ink-50/60 px-5 py-4">
          {report.metrics.length === 0 ? (
            <p className="text-sm text-ink-400">No per-feature metrics in this report.</p>
          ) : (
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-xs uppercase tracking-wide text-ink-400">
                  <th className="pb-2 pr-4 font-semibold">Feature</th>
                  <th className="pb-2 pr-4 font-semibold">Method</th>
                  <th className="pb-2 pr-4 font-semibold text-right">Baseline</th>
                  <th className="pb-2 pr-4 font-semibold text-right">Current</th>
                  <th className="pb-2 pr-4 font-semibold text-right">Score</th>
                  <th className="pb-2 font-semibold">Severity</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-ink-200/60">
                {report.metrics.map((dm) => (
                  <tr key={dm.name}>
                    <td className="py-2 pr-4 font-medium text-ink-700">{dm.name}</td>
                    <td className="py-2 pr-4">
                      <Mono>{humanizeEnum(dm.method, 'DRIFT_METHOD_')}</Mono>
                    </td>
                    <td className="py-2 pr-4 text-right font-mono text-ink-600">{dm.baselineValue.toFixed(3)}</td>
                    <td className="py-2 pr-4 text-right font-mono text-ink-600">{dm.currentValue.toFixed(3)}</td>
                    <td className={`py-2 pr-4 text-right font-mono font-semibold ${critical ? 'text-red-600' : 'text-ink-900'}`}>
                      {dm.score.toFixed(4)}
                    </td>
                    <td className="py-2">
                      <StatusPill value={dm.severity} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}
    </li>
  )
}
