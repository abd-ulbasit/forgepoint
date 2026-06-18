// ============================================================================
// RunDetailPage — one run's params + final metrics (with a bar chart).
// ============================================================================
// GET /api/v1/runs/{id}. Params are key/value strings; final metrics are
// key/number pairs. We render params as a table and metrics as both a compact
// bar chart (recharts) and a table, so the numbers are scannable and visual.

import { Link, useParams } from 'react-router-dom'
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { useRun } from '@/hooks/queries'
import { ErrorState, LoadingState } from '@/components/states'
import { Card, CardHeader, KeyValue, PageHeader, Table, Td, Th, Mono } from '@/components/ui'
import { StatusPill } from '@/components/StatusPill'
import { formatDuration, formatTime, humanizeEnum, shortId } from '@/lib/format'

export function RunDetailPage() {
  const { id = '' } = useParams()
  const { data, isLoading, isError, error, refetch } = useRun(id)

  if (isLoading) return <LoadingState label="Loading run…" />
  if (isError || !data?.run) {
    return (
      <div>
        <BackLink />
        <ErrorState
          message={error instanceof Error ? error.message : 'Run not found.'}
          onRetry={() => refetch()}
        />
      </div>
    )
  }

  const run = data.run
  const metricData = run.finalMetrics.map((m) => ({ name: m.key, value: m.value }))

  return (
    <div>
      <BackLink />
      <PageHeader
        title={run.displayName || `Run ${shortId(run.id)}`}
        description={`Experiment ${shortId(run.experimentId)} · ${humanizeEnum(run.source, 'RUN_SOURCE_')}`}
        actions={<StatusPill value={run.status} />}
      />

      <Card className="mb-6">
        <div className="p-5">
          <KeyValue
            items={[
              { label: 'Run ID', value: <Mono>{shortId(run.id)}</Mono> },
              { label: 'Experiment', value: <Mono>{shortId(run.experimentId)}</Mono> },
              { label: 'Model version', value: run.modelVersionId ? <Mono>{shortId(run.modelVersionId)}</Mono> : '—' },
              { label: 'Duration', value: formatDuration(run.startedAt, run.endedAt) },
              { label: 'Started', value: formatTime(run.startedAt) },
              { label: 'Ended', value: run.endedAt ? formatTime(run.endedAt) : '—' },
            ]}
          />
        </div>
      </Card>

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
        {/* Params */}
        <Card>
          <CardHeader title="Parameters" subtitle="Hyperparameters logged for this run" />
          {run.params.length === 0 ? (
            <p className="px-5 py-10 text-center text-sm text-ink-400">No parameters logged.</p>
          ) : (
            <Table>
              <thead>
                <tr className="border-b border-ink-100">
                  <Th>Key</Th>
                  <Th>Value</Th>
                </tr>
              </thead>
              <tbody className="divide-y divide-ink-100">
                {run.params.map((p) => (
                  <tr key={p.key} className="hover:bg-ink-50">
                    <Td className="font-medium text-ink-700">{p.key}</Td>
                    <Td>
                      <Mono>{p.value}</Mono>
                    </Td>
                  </tr>
                ))}
              </tbody>
            </Table>
          )}
        </Card>

        {/* Metrics */}
        <Card>
          <CardHeader title="Final metrics" subtitle="Evaluation results" />
          {run.finalMetrics.length === 0 ? (
            <p className="px-5 py-10 text-center text-sm text-ink-400">No metrics logged.</p>
          ) : (
            <div className="p-5">
              <div className="h-48 w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={metricData} margin={{ top: 4, right: 8, bottom: 4, left: -16 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" vertical={false} />
                    <XAxis dataKey="name" tick={{ fontSize: 11, fill: '#64748b' }} tickLine={false} axisLine={{ stroke: '#e2e8f0' }} />
                    <YAxis tick={{ fontSize: 11, fill: '#64748b' }} tickLine={false} axisLine={false} />
                    <Tooltip
                      cursor={{ fill: '#f1f5f9' }}
                      contentStyle={{ fontSize: 12, borderRadius: 8, border: '1px solid #e2e8f0' }}
                    />
                    <Bar dataKey="value" fill="#6366f1" radius={[4, 4, 0, 0]} maxBarSize={48} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
              <table className="mt-4 w-full text-sm">
                <tbody className="divide-y divide-ink-100">
                  {run.finalMetrics.map((m) => (
                    <tr key={m.key}>
                      <td className="py-2 font-medium text-ink-700">{m.key}</td>
                      <td className="py-2 text-right font-mono text-ink-900">
                        {Number.isInteger(m.value) ? m.value : m.value.toFixed(4)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Card>
      </div>
    </div>
  )
}

function BackLink() {
  return (
    <Link to="/experiments" className="mb-3 inline-flex items-center gap-1 text-sm font-medium text-ink-500 hover:text-ink-800">
      <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 19.5L8.25 12l7.5-7.5" />
      </svg>
      Back to experiments
    </Link>
  )
}
