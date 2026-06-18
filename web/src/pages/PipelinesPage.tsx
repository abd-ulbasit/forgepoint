// ============================================================================
// PipelinesPage — pipeline definitions + a one-click trigger.
// ============================================================================
// Lists pipeline definitions from GET /api/v1/pipelines. "Run" POSTs to
// /api/v1/pipelines/{id}/start (202 Accepted, runs async) and then navigates
// straight to the new execution's detail page, where the SSE stream shows the
// saga executing live — the headline flow.

import { useNavigate } from 'react-router-dom'
import { usePipelines, useStartPipeline } from '@/hooks/queries'
import { usePagination } from '@/hooks/usePagination'
import { useToast } from '@/components/Toast'
import { ApiError } from '@/api/client'
import { EmptyState, ErrorState, Spinner, TableSkeleton } from '@/components/states'
import { Card, PageHeader, PaginationBar, Table, Td, Th, Mono } from '@/components/ui'
import { StatusPill } from '@/components/StatusPill'
import { PipelinesIcon, PlayIcon } from '@/components/layout/icons'
import { formatTime, humanizeEnum, shortId } from '@/lib/format'
import type { PipelineDefinition } from '@/api/types'

export function PipelinesPage() {
  const { page, pageSize, pageToken, goNext, reset } = usePagination(20)
  const { data, isLoading, isError, error, refetch, isFetching } = usePipelines({ pageSize, pageToken })

  const pipelines = data?.pipelines ?? []
  const total = data?.pagination.totalCount ?? 0
  const nextToken = data?.pagination.nextPageToken ?? ''

  return (
    <div>
      <PageHeader
        title="Pipelines"
        description="Saga & DAG definitions. Trigger one to watch its steps execute live."
      />

      <Card>
        {isLoading ? (
          <TableSkeleton rows={6} cols={4} />
        ) : isError ? (
          <ErrorState message={error instanceof Error ? error.message : undefined} onRetry={() => refetch()} />
        ) : pipelines.length === 0 ? (
          <EmptyState
            title="No pipelines defined"
            description="Pipeline definitions are created via the orchestrator API or the fp CLI."
            icon={<PipelinesIcon className="h-6 w-6" />}
          />
        ) : (
          <>
            <Table>
              <thead>
                <tr className="border-b border-ink-100">
                  <Th>Name</Th>
                  <Th>Type</Th>
                  <Th>Steps</Th>
                  <Th>Created</Th>
                  <Th className="text-right">Action</Th>
                </tr>
              </thead>
              <tbody className="divide-y divide-ink-100">
                {pipelines.map((p) => (
                  <PipelineRow key={p.id} pipeline={p} />
                ))}
              </tbody>
            </Table>
            <PaginationBar
              total={total}
              shown={pipelines.length}
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

function PipelineRow({ pipeline }: { pipeline: PipelineDefinition }) {
  const navigate = useNavigate()
  const toast = useToast()
  const start = useStartPipeline()

  async function handleRun() {
    try {
      const resp = await start.mutateAsync({ pipelineId: pipeline.id })
      const execId = resp.execution?.id
      toast.success(`Started ${pipeline.name}.`)
      if (execId) {
        // Jump to the live execution view immediately.
        navigate(`/executions/${execId}`)
      }
    } catch (err) {
      const msg = err instanceof ApiError ? err.message : 'Failed to start pipeline.'
      toast.error(msg)
    }
  }

  // Type badge tone: saga = info, training = neutral, batch = neutral.
  const typeTone = pipeline.type === 'PIPELINE_TYPE_DEPLOYMENT_SAGA' ? 'info' : 'neutral'

  return (
    <tr className="hover:bg-ink-50">
      <Td>
        <p className="font-medium text-ink-900">{pipeline.name}</p>
        <p className="text-xs text-ink-400">
          <Mono>{shortId(pipeline.id)}</Mono>
          {pipeline.team ? ` · ${pipeline.team}` : ''}
        </p>
      </Td>
      <Td>
        <StatusPill value={pipeline.type} toneOverride={typeTone} />
      </Td>
      <Td className="text-ink-600">
        {pipeline.steps.length} step{pipeline.steps.length === 1 ? '' : 's'}
        <p className="mt-0.5 max-w-xs truncate text-xs text-ink-400">
          {pipeline.steps.map((s) => humanizeEnum(s.type, 'STEP_TYPE_')).join(' → ')}
        </p>
      </Td>
      <Td className="text-ink-500">{formatTime(pipeline.createdAt)}</Td>
      <Td className="text-right">
        <button type="button" className="btn-primary px-3 py-1.5" onClick={handleRun} disabled={start.isPending}>
          {start.isPending ? <Spinner className="h-4 w-4 text-white" /> : <PlayIcon className="h-4 w-4" />}
          Run
        </button>
      </Td>
    </tr>
  )
}
