// ============================================================================
// ModelDetailPage — one model's metadata + its version history.
// ============================================================================
// Two parallel queries: GET /models/{id} and GET /models/{id}/versions. Each
// renders its own loading/error state so a slow versions call doesn't block the
// header. Version metrics arrive as a google.protobuf.Struct (arbitrary JSON),
// which we flatten into small metric chips.

import { Link, useParams } from 'react-router-dom'
import { useModel, useModelVersions } from '@/hooks/queries'
import { ErrorState, LoadingState, TableSkeleton } from '@/components/states'
import { Card, CardHeader, KeyValue, PageHeader, Table, Td, Th, Mono } from '@/components/ui'
import { StatusPill } from '@/components/StatusPill'
import { formatBytes, formatTime, shortId } from '@/lib/format'
import type { Struct } from '@/api/types'

export function ModelDetailPage() {
  const { id = '' } = useParams()
  const model = useModel(id)
  const versions = useModelVersions(id)

  if (model.isLoading) return <LoadingState label="Loading model…" />
  if (model.isError || !model.data?.model) {
    return (
      <div>
        <BackLink />
        <ErrorState
          message={model.error instanceof Error ? model.error.message : 'Model not found.'}
          onRetry={() => model.refetch()}
        />
      </div>
    )
  }

  const m = model.data.model

  return (
    <div>
      <BackLink />
      <PageHeader
        title={m.name}
        description={m.description || 'No description provided.'}
      />

      {/* Metadata */}
      <Card className="mb-6">
        <div className="p-5">
          <KeyValue
            items={[
              { label: 'Model ID', value: <Mono>{shortId(m.id)}</Mono> },
              { label: 'Team', value: m.team || '—' },
              { label: 'Framework', value: m.framework ? <Mono>{m.framework}</Mono> : '—' },
              { label: 'Task type', value: m.taskType || '—' },
              {
                label: 'Production version',
                value: m.productionVersion ? (
                  <span className="inline-flex items-center gap-2">
                    <StatusPill value="MODEL_STAGE_PRODUCTION" /> v{m.productionVersion}
                  </span>
                ) : (
                  <span className="text-ink-400">none deployed</span>
                ),
              },
              { label: 'Latest version', value: m.latestVersion ? `v${m.latestVersion}` : '—' },
              { label: 'Created', value: formatTime(m.createdAt) },
              { label: 'Updated', value: formatTime(m.updatedAt) },
            ]}
          />
          {Object.keys(m.tags ?? {}).length > 0 && (
            <div className="mt-5 border-t border-ink-100 pt-4">
              <p className="mb-2 text-xs font-medium uppercase tracking-wide text-ink-400">Tags</p>
              <div className="flex flex-wrap gap-2">
                {Object.entries(m.tags).map(([k, v]) => (
                  <span key={k} className="rounded-md bg-ink-100 px-2 py-1 text-xs text-ink-600">
                    <span className="font-medium text-ink-700">{k}</span>: {v}
                  </span>
                ))}
              </div>
            </div>
          )}
        </div>
      </Card>

      {/* Versions */}
      <Card>
        <CardHeader title="Versions" subtitle="Every registered artifact for this model" />
        {versions.isLoading ? (
          <TableSkeleton rows={4} cols={5} />
        ) : versions.isError ? (
          <ErrorState
            message={versions.error instanceof Error ? versions.error.message : undefined}
            onRetry={() => versions.refetch()}
          />
        ) : (versions.data?.versions.length ?? 0) === 0 ? (
          <p className="px-5 py-10 text-center text-sm text-ink-400">No versions registered yet.</p>
        ) : (
          <Table>
            <thead>
              <tr className="border-b border-ink-100">
                <Th>Version</Th>
                <Th>Stage</Th>
                <Th>Status</Th>
                <Th>Size</Th>
                <Th>Metrics</Th>
                <Th>Created</Th>
              </tr>
            </thead>
            <tbody className="divide-y divide-ink-100">
              {versions.data!.versions.map((v) => (
                <tr key={v.id} className="hover:bg-ink-50">
                  <Td className="font-medium text-ink-900">v{v.version}</Td>
                  <Td>
                    <StatusPill value={v.stage} />
                  </Td>
                  <Td>
                    <StatusPill value={v.status} />
                  </Td>
                  <Td className="text-ink-500">{formatBytes(v.sizeBytes)}</Td>
                  <Td>
                    <MetricChips metrics={v.metrics} />
                  </Td>
                  <Td className="text-ink-500">{formatTime(v.createdAt)}</Td>
                </tr>
              ))}
            </tbody>
          </Table>
        )}
      </Card>
    </div>
  )
}

function BackLink() {
  return (
    <Link to="/models" className="mb-3 inline-flex items-center gap-1 text-sm font-medium text-ink-500 hover:text-ink-800">
      <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 19.5L8.25 12l7.5-7.5" />
      </svg>
      Back to models
    </Link>
  )
}

/** Flatten a metrics Struct into up to 4 numeric chips. */
function MetricChips({ metrics }: { metrics: Struct | null }) {
  if (!metrics || Object.keys(metrics).length === 0) {
    return <span className="text-xs text-ink-400">—</span>
  }
  const entries = Object.entries(metrics).slice(0, 4)
  return (
    <div className="flex flex-wrap gap-1.5">
      {entries.map(([k, v]) => (
        <span key={k} className="rounded bg-brand-50 px-1.5 py-0.5 text-xs text-brand-700">
          {k}:{' '}
          <span className="font-semibold">
            {typeof v === 'number' ? (Number.isInteger(v) ? v : v.toFixed(3)) : String(v)}
          </span>
        </span>
      ))}
    </div>
  )
}
