// ============================================================================
// ExperimentsPage — experiment runs list.
// ============================================================================
// Lists runs from GET /api/v1/runs (the event-driven experiment tracker). Each
// row links to the run detail, which shows params + final metrics. Runs can be
// API-triggered or EVENT-sourced (RUN_SOURCE_EVENT) — we surface the source so
// the async-ingestion story is visible.

import { Link } from 'react-router-dom'
import { useRuns } from '@/hooks/queries'
import { usePagination } from '@/hooks/usePagination'
import { EmptyState, ErrorState, TableSkeleton } from '@/components/states'
import { Card, PageHeader, PaginationBar, Table, Td, Th, Mono } from '@/components/ui'
import { StatusPill } from '@/components/StatusPill'
import { ExperimentsIcon } from '@/components/layout/icons'
import { formatDuration, formatRelative, humanizeEnum, shortId } from '@/lib/format'

export function ExperimentsPage() {
  const { page, pageSize, pageToken, goNext, reset } = usePagination(20)
  const { data, isLoading, isError, error, refetch, isFetching } = useRuns({ pageSize, pageToken })

  const runs = data?.runs ?? []
  const total = data?.pagination.totalCount ?? 0
  const nextToken = data?.pagination.nextPageToken ?? ''

  return (
    <div>
      <PageHeader title="Experiments" description="Training runs with their parameters and metrics." />

      <Card>
        {isLoading ? (
          <TableSkeleton rows={6} cols={5} />
        ) : isError ? (
          <ErrorState message={error instanceof Error ? error.message : undefined} onRetry={() => refetch()} />
        ) : runs.length === 0 ? (
          <EmptyState
            title="No runs yet"
            description="Training runs appear here as they're logged to the experiment tracker."
            icon={<ExperimentsIcon className="h-6 w-6" />}
          />
        ) : (
          <>
            <Table>
              <thead>
                <tr className="border-b border-ink-100">
                  <Th>Run</Th>
                  <Th>Status</Th>
                  <Th>Source</Th>
                  <Th>Duration</Th>
                  <Th>Started</Th>
                  <Th className="text-right">{''}</Th>
                </tr>
              </thead>
              <tbody className="divide-y divide-ink-100">
                {runs.map((r) => (
                  <tr key={r.id} className="hover:bg-ink-50">
                    <Td>
                      <Link to={`/runs/${r.id}`} className="font-medium text-ink-900 hover:text-brand-700">
                        {r.displayName || shortId(r.id)}
                      </Link>
                      <p className="text-xs text-ink-400">
                        {r.params.length} param{r.params.length === 1 ? '' : 's'} ·{' '}
                        {r.finalMetrics.length} metric{r.finalMetrics.length === 1 ? '' : 's'}
                      </p>
                    </Td>
                    <Td>
                      <StatusPill value={r.status} />
                    </Td>
                    <Td>
                      <Mono>{humanizeEnum(r.source, 'RUN_SOURCE_')}</Mono>
                    </Td>
                    <Td className="text-ink-500">{formatDuration(r.startedAt, r.endedAt)}</Td>
                    <Td className="text-ink-500">{formatRelative(r.startedAt)}</Td>
                    <Td className="text-right">
                      <Link to={`/runs/${r.id}`} className="text-sm font-semibold text-brand-600 hover:text-brand-700">
                        Details
                      </Link>
                    </Td>
                  </tr>
                ))}
              </tbody>
            </Table>
            <PaginationBar
              total={total}
              shown={runs.length}
              hasNext={Boolean(nextToken) && !isFetching}
              onNext={() => goNext(nextToken)}
              onReset={reset}
              page={page}
            />
          </>
        )}
      </Card>
    </div>
  )
}
